#include "sequencer.hpp"

#include "../pempek_assert.hpp"

Sequencer::Sequencer(Workload *workload,
                   SchedulingDecision *decision,
                   Queue *queue,
                   ResourceSelector *selector,
                   double rjms_delay,
                   rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Sequencer::~Sequencer()
{

}

void Sequencer::on_simulation_start(double date, const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;

    _machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(_machines.size() == (unsigned int) _nb_machines);
}

void Sequencer::on_simulation_end(double date)
{
    (void) date;
}

void Sequencer::make_decisions(double date,
                              SortableJobOrder::UpdateInformation *update_info,
                              SortableJobOrder::CompareInformation *compare_info)
{
    // This algorithm executes all the jobs, one after the other.
    // At any time, either 0 or 1 job is running on the platform.
    // The order of the sequence depends on the queue order.

    // Up to one job finished since last call.
    PPK_ASSERT_ERROR(_jobs_ended_recently.size() <= 1);
    if (!_jobs_ended_recently.empty())
    {
        PPK_ASSERT_ERROR(_isJobRunning);
        _isJobRunning = false;
    }

    // Add valid jobs into the queue
    for (const std::string & job_id : _jobs_released_recently)
    {
        const Job * job = (*_workload)[job_id];

        if (job->nb_requested_resources <= _nb_machines)
            _queue->append_job(job, update_info);
        else
            _decision->add_reject_job(job->id, date);
    }

    // Sort queue if needed
    _queue->sort_queue(update_info, compare_info);

    // Execute the first job if the machine is empty
    const Job * job = _queue->first_job_or_nullptr();
    if (job != nullptr && !_isJobRunning)
    {
        _decision->add_execute_job(job->id,
                                   _machines.left(job->nb_requested_resources),
                                   date);
        _isJobRunning = true;
        _queue->remove_job(job);
    }
}
