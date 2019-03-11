#include "easy_bf_fast.hpp"

//#include <loguru.hpp>

#include "../pempek_assert.hpp"

EasyBackfillingFast::EasyBackfillingFast(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{}

EasyBackfillingFast::~EasyBackfillingFast()
{}

void EasyBackfillingFast::on_simulation_start(double date,
    const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;

    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);
}

void EasyBackfillingFast::on_simulation_end(double date)
{
    (void) date;
}

void EasyBackfillingFast::make_decisions(double date,
    SortableJobOrder::UpdateInformation *update_info,
    SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    // This algorithm is a fast version of EASY backfilling.
    // It is meant to be fast in the usual case, not to handle corner cases
    // (use the other easy backfilling available in batsched for this purpose).
    // It is not meant to be easily readable or hackable ;).

    // This fast EASY backfilling variant in a few words:
    // - only handles the FCFS queue order
    // - only handles the basic resource selection policy
    // - only handles finite jobs (no switchoff), with walltimes
    // - only handles one priority job (the first of the queue)
    // - only handles time as floating-point (-> precision errors).

    // Hacks:
    // - uses priority job's completion time to store its expected starting time

    bool job_ended = false;

    // Handle newly finished jobs
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        job_ended = true;

        Job * finished_job = (*_workload)[ended_job_id];
        const Allocation & alloc = _current_allocations[ended_job_id];

        // Update data structures
        _available_machines.insert(alloc.machines);
        _nb_available_machines += finished_job->nb_requested_resources;
        _horizons.erase(alloc.horizon_it);
        _current_allocations.erase(ended_job_id);
    }

    // If jobs have finished, let's execute jobs as long as they are priority
    if (job_ended)
    {
        if (_priority_job != nullptr)
        {
            Allocation alloc;
            FinishedHorizonPoint point;

            if (_priority_job->nb_requested_resources <= _nb_available_machines)
            {
                //LOG_F(INFO, "Priority job fits!");
                alloc.machines = _available_machines.left(
                    _priority_job->nb_requested_resources);
                _decision->add_execute_job(_priority_job->id, alloc.machines,
                    date);

                point.nb_released_machines = _priority_job->nb_requested_resources;
                point.date = date + (double)_priority_job->walltime;
                alloc.horizon_it = insert_horizon_point(point);

                // Update data structures
                _available_machines -= alloc.machines;
                _nb_available_machines -= _priority_job->nb_requested_resources;
                _current_allocations[_priority_job->id] = alloc;
                _priority_job = nullptr;

                // Execute the whole queue until a priority job cannot fit
                for (auto job_it = _pending_jobs.begin();
                     job_it != _pending_jobs.end(); )
                {
                    Job * pending_job = *job_it;
                    if (pending_job->nb_requested_resources <= _nb_available_machines)
                    {
                        alloc.machines = _available_machines.left(
                            pending_job->nb_requested_resources);
                        _decision->add_execute_job(pending_job->id,
                            alloc.machines, date);

                        point.nb_released_machines = pending_job->nb_requested_resources;
                        point.date = date + (double)pending_job->walltime;
                        alloc.horizon_it = insert_horizon_point(point);

                        // Update data structures
                        _available_machines -= alloc.machines;
                        _nb_available_machines -= pending_job->nb_requested_resources;
                        _current_allocations[pending_job->id] = alloc;
                        job_it = _pending_jobs.erase(job_it);
                    }
                    else
                    {
                        // The job becomes priority!
                        _priority_job = pending_job;
                        _priority_job->completion_time = compute_priority_job_expected_earliest_starting_time();
                        _pending_jobs.erase(job_it);

                        // Stop first queue traversal.
                        break;
                    }
                }
            }

            // Backfill jobs that does not hinder priority job.
            if (_nb_available_machines > 0)
            {
                for (auto job_it = _pending_jobs.begin();
                     job_it != _pending_jobs.end(); )
                {
                    const Job * pending_job = *job_it;
                    // Can the job be executed now ?
                    if (pending_job->nb_requested_resources <= _nb_available_machines &&
                        date + pending_job->walltime <= _priority_job->completion_time)
                    {
                        // Yes, it can be backfilled!
                        alloc.machines = _available_machines.left(
                            pending_job->nb_requested_resources);
                        _decision->add_execute_job(pending_job->id,
                            alloc.machines, date);

                        point.nb_released_machines = pending_job->nb_requested_resources;
                        point.date = date + (double)pending_job->walltime;
                        alloc.horizon_it = insert_horizon_point(point);

                        // Update data structures
                        _available_machines -= alloc.machines;
                        _nb_available_machines -= pending_job->nb_requested_resources;
                        _current_allocations[pending_job->id] = alloc;
                        job_it = _pending_jobs.erase(job_it);

                        // Directly get out of the backfilling loop if all machines are busy.
                        if (_nb_available_machines <= 0)
                            break;
                    }
                    else
                    {
                        ++job_it;
                    }
                }
            }
        }
    }

    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Can the job be executed right now?
        if (new_job->nb_requested_resources <= _nb_available_machines)
        {
            //LOG_F(INFO, "There are enough available resources (%d) to execute job %s", _nb_available_machines, new_job->id.c_str());
            // Can it be executed now (without hindering priority job?)
            if (_priority_job == nullptr ||
                date + new_job->walltime <= _priority_job->completion_time)
            {
                //LOG_F(INFO, "Job %s can be started right away!", new_job->id.c_str());
                // Yes, the job can be executed right away!
                Allocation alloc;

                alloc.machines = _available_machines.left(
                    new_job->nb_requested_resources);
                _decision->add_execute_job(new_job_id, alloc.machines, date);

                FinishedHorizonPoint point;
                point.nb_released_machines = new_job->nb_requested_resources;
                point.date = date + (double)new_job->walltime;
                alloc.horizon_it = insert_horizon_point(point);

                // Update data structures
                _available_machines -= alloc.machines;
                _nb_available_machines -= new_job->nb_requested_resources;
                _current_allocations[new_job_id] = alloc;
            }
            else
            {
                // No, the job cannot be executed (hinders priority job.)
                /*LOG_F(INFO, "Not enough time to execute job %s (walltime=%g, priority job expected starting time=%g)",
                      new_job->id.c_str(), (double)new_job->walltime, _priority_job->completion_time);*/
                _pending_jobs.push_back(new_job);
            }
        }
        else
        {
            // The job is too big to fit now.

            // Is the job valid on this platform?
            if (new_job->nb_requested_resources > _nb_machines)
            {
                /*LOG_F(INFO, "Rejecing job %s (required %d machines, while platform size is %d)",
                      new_job->id.c_str(), new_job->nb_requested_resources, _nb_machines);*/
                _decision->add_reject_job(new_job_id, date);
            }
            else
            {
                if (_priority_job == nullptr)
                {
                    // The job becomes priority.
                    _priority_job = new_job;
                    _priority_job->completion_time = compute_priority_job_expected_earliest_starting_time();
                }
                else
                {
                    // The job is queued up.
                    _pending_jobs.push_back(new_job);
                }
            }
        }
    }
}

double EasyBackfillingFast::compute_priority_job_expected_earliest_starting_time()
{
    int nb_available = _nb_available_machines;
    int required = _priority_job->nb_requested_resources;

    for (auto it = _horizons.begin(); it != _horizons.end(); ++it)
    {
        nb_available += it->nb_released_machines;

        if (nb_available >= required)
        {
            return it->date;
        }
    }

    PPK_ASSERT_ERROR(false, "The job will never be executable.");
    return 0;
}

std::list<EasyBackfillingFast::FinishedHorizonPoint>::iterator EasyBackfillingFast::insert_horizon_point(const EasyBackfillingFast::FinishedHorizonPoint &point)
{
    // The data structure is sorted, we can therefore traverse it in order
    // until finding an insertion point.
    for (auto it = _horizons.begin(); it != _horizons.end(); ++it)
    {
        if (point.date < it->date)
        {
            // Insertion point is before the current iterator.
            return _horizons.insert(it, point);
        }
    }

    // Insertion point not found. Insertion at end.
    return _horizons.insert(_horizons.end(), point);
}
