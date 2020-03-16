#include "conservative_bf.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

ConservativeBackfilling::ConservativeBackfilling(Workload *workload, SchedulingDecision *decision,
                                                 Queue *queue, ResourceSelector * selector, double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    if (variant_options->HasMember("dump_previsional_schedules"))
    {
        PPK_ASSERT_ERROR((*variant_options)["dump_previsional_schedules"].IsBool(),
                "Invalid options: 'dump_previsional_schedules' should be a boolean");
        _dump_provisional_schedules = (*variant_options)["dump_previsional_schedules"].GetBool();
    }

    if (variant_options->HasMember("dump_prefix"))
    {
        PPK_ASSERT_ERROR((*variant_options)["dump_prefix"].IsString(),
                "Invalid options: 'dump_prefix' should be a string");
        _dump_prefix = (*variant_options)["dump_prefix"].GetString();
    }
}

ConservativeBackfilling::~ConservativeBackfilling()
{
}

void ConservativeBackfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    _schedule = Schedule(_nb_machines, date);
    (void) batsim_config;
}

void ConservativeBackfilling::on_simulation_end(double date)
{
    (void) date;
}

void ConservativeBackfilling::make_decisions(double date,
                                             SortableJobOrder::UpdateInformation *update_info,
                                             SortableJobOrder::CompareInformation *compare_info)
{
    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job((*_workload)[ended_job_id]);

    // Let's handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
        {
            _decision->add_reject_job(new_job_id, date);
        }
        else if (!new_job->has_walltime)
        {
            LOG_SCOPE_FUNCTION(INFO);
            LOG_F(INFO, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
            _decision->add_reject_job(new_job_id, date);
        }
        else
        {
            _queue->append_job(new_job, update_info);
            recently_queued_jobs.push_back(new_job_id);
        }
    }

    // Let's update the schedule's present
    _schedule.update_first_slice(date);

    // Queue sorting
    _queue->sort_queue(update_info, compare_info);

    // If no resources have been released, we can just insert the new jobs into the schedule
    if (_jobs_ended_recently.empty())
    {
        for (const string & new_job_id : recently_queued_jobs)
        {
            const Job * new_job = (*_workload)[new_job_id];
            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);

            // If the job should start now, let's say it to the resource manager
            if (alloc.started_in_first_slice)
            {
                _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                _queue->remove_job(new_job);
            }
        }
    }
    else
    {
        // Since some resources have been freed,
        // Let's compress the schedule following conservative backfilling rules:
        // For each non running job j
        //   Remove j from the schedule
        //   Add j into the schedule
        //   If j should be executed now
        //     Take the decision to run j now
        for (auto job_it = _queue->begin(); job_it != _queue->end(); )
        {
            const Job * job = (*job_it)->job;

            _schedule.remove_job_if_exists(job);
//            if (_dump_provisional_schedules)
//                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);
//            if (_dump_provisional_schedules)
//                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);

            if (alloc.started_in_first_slice)
            {
                _decision->add_execute_job(job->id, alloc.used_machines, date);
                job_it = _queue->remove_job(job_it);
            }
            else
                ++job_it;
        }
    }

    // And now let's see if we can estimate some waiting times
    
    for (const std::string & job_id : _jobs_whose_waiting_time_estimation_has_been_requested_recently)
    {
        const Job * new_job = (*_workload)[job_id];
        double answer = _schedule.query_wait(new_job->nb_requested_resources, new_job->walltime, _selector);
            _decision->add_answer_estimate_waiting_time(job_id, answer, date);
    }

    if (_dump_provisional_schedules)
        _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
}
