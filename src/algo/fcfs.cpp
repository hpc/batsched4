#include "fcfs.hpp"
#include <iostream>

#include "../pempek_assert.hpp"

FCFS::FCFS(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{}

FCFS::~FCFS()
{}

void FCFS::on_simulation_start(double date,
    const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;

    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);
}

void FCFS::on_simulation_end(double date)
{
    (void) date;
}

void FCFS::make_decisions(double date,
    SortableJobOrder::UpdateInformation *update_info,
    SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    // This algorithm is a version of FCFS without backfilling.
    // It is meant to be fast in the usual case, not to handle corner cases.
    // It is not meant to be easily readable or hackable ;).

    // This fast FCFS variant in a few words:
    // - only handles the FCFS queue order
    // - only handles finite jobs (no switchoff)
    // - only handles time as floating-point (-> precision errors).

    bool job_ended = false;

    // Handle newly finished jobs
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        job_ended = true;

        Job * finished_job = (*_workload)[ended_job_id];

        // Update data structures
        _available_machines.insert(_current_allocations[ended_job_id]);
        _nb_available_machines += finished_job->nb_requested_resources;
        _current_allocations.erase(ended_job_id);
    }

    // If jobs have finished, execute jobs as long as they fit
    if (job_ended)
    {
        for (auto job_it = _pending_jobs.begin();
             job_it != _pending_jobs.end(); )
        {
            Job * pending_job = *job_it;
            IntervalSet machines;

            if (_selector->fit(pending_job, _available_machines, machines))
           {
                _decision->add_execute_job(pending_job->id,
                    machines, date);

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= pending_job->nb_requested_resources;
                _current_allocations[pending_job->id] = machines;
                job_it = _pending_jobs.erase(job_it);

            }
            else
            {
                // The job becomes priority!
                // As there is no backfilling, we can simply leave this loop.
                break;
            }
        }
    }

    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Is this job valid?
        if (new_job->nb_requested_resources > _nb_machines)
        {
            // Invalid!
            _decision->add_reject_job(new_job_id, date);
            continue;
        }

        // Is there a waiting job?
        if (!_pending_jobs.empty())
        {
            // Yes. The new job is queued up.
            _pending_jobs.push_back(new_job);
        }
        else
        {
            // No, the queue is empty.
            // Can the new job be executed now?
            if (new_job->nb_requested_resources <= _nb_available_machines)
            {
                // Yes, the job can be executed right away!
                IntervalSet machines = _available_machines.left(
                    new_job->nb_requested_resources);
                _decision->add_execute_job(new_job_id, machines, date);

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= new_job->nb_requested_resources;
                _current_allocations[new_job_id] = machines;
            }
            else
            {
                // No. The job is queued up.
                _pending_jobs.push_back(new_job);
            }
        }
    }
}
