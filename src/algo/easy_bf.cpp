#include "easy_bf.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EasyBackfilling::EasyBackfilling(Workload * workload,
                                 SchedulingDecision * decision,
                                 Queue * queue,
                                 ResourceSelector * selector,
                                 double rjms_delay,
                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
}

EasyBackfilling::~EasyBackfilling()
{

}

void EasyBackfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    _schedule = Schedule(_nb_machines, date);
    (void) batsim_config;
}

void EasyBackfilling::on_simulation_end(double date)
{
    (void) date;
}

void EasyBackfilling::make_decisions(double date,
                                     SortableJobOrder::UpdateInformation *update_info,
                                     SortableJobOrder::CompareInformation *compare_info)
{
    const Job * priority_job_before = _queue->first_job_or_nullptr();

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
    const Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info, compare_info);

    // If no resources have been released, we can just try to backfill the newly-released jobs
    if (_jobs_ended_recently.empty())
    {
        int nb_available_machines = _schedule.begin()->available_machines.size();

        for (unsigned int i = 0; i < recently_queued_jobs.size() && nb_available_machines > 0; ++i)
        {
            const string & new_job_id = recently_queued_jobs[i];
            const Job * new_job = (*_workload)[new_job_id];

            // The job could have already been executed by sort_queue_while_handling_priority_job,
            // that's why we check whether the queue contains the job.
            if (_queue->contains_job(new_job) &&
                new_job != priority_job_after &&
                new_job->nb_requested_resources <= nb_available_machines)
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);
                if ( alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(new_job_id, alloc.used_machines, date);
                    _queue->remove_job(new_job);
                    nb_available_machines -= new_job->nb_requested_resources;
                }
                else
                    _schedule.remove_job(new_job);
            }
        }
    }
    else
    {
        // Some resources have been released, the whole queue should be traversed.
        auto job_it = _queue->begin();
        int nb_available_machines = _schedule.begin()->available_machines.size();

        // Let's try to backfill all the jobs
        while (job_it != _queue->end() && nb_available_machines > 0)
        {
            const Job * job = (*job_it)->job;

            if (_schedule.contains_job(job))
                _schedule.remove_job(job);

            if (job == priority_job_after) // If the current job is priority
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it); // Updating job_it to remove on traversal
                    priority_job_after = _queue->first_job_or_nullptr();
                }
                else
                    ++job_it;
            }
            else // The job is not priority
            {
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it);
                }
                else
                {
                    _schedule.remove_job(job);
                    ++job_it;
                }
            }
        }
    }
}


void EasyBackfilling::sort_queue_while_handling_priority_job(const Job * priority_job_before,
                                                             const Job *& priority_job_after,
                                                             SortableJobOrder::UpdateInformation * update_info,
                                                             SortableJobOrder::CompareInformation * compare_info)
{
    if (_debug)
        LOG_F(1, "sort_queue_while_handling_priority_job beginning, %s", _schedule.to_string().c_str());

    // Let's sort the queue
    _queue->sort_queue(update_info, compare_info);

    // Let the new priority job be computed
    priority_job_after = _queue->first_job_or_nullptr();

    // If the priority job has changed
    if (priority_job_after != priority_job_before)
    {
        // If there was a priority job before, let it be removed from the schedule
        if (priority_job_before != nullptr)
            _schedule.remove_job_if_exists(priority_job_before);

        // Let us ensure the priority job is in the schedule.
        // To do so, while the priority job can be executed now, we keep on inserting it into the schedule
        for (bool could_run_priority_job = true; could_run_priority_job && priority_job_after != nullptr; )
        {
            could_run_priority_job = false;

            // Let's add the priority job into the schedule
            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(priority_job_after, _selector);

            if (alloc.started_in_first_slice)
            {
                _decision->add_execute_job(priority_job_after->id, alloc.used_machines, (double)update_info->current_date);
                _queue->remove_job(priority_job_after);
                priority_job_after = _queue->first_job_or_nullptr();
                could_run_priority_job = true;
            }
        }
    }

    if (_debug)
        LOG_F(1, "sort_queue_while_handling_priority_job ending, %s", _schedule.to_string().c_str());
}
