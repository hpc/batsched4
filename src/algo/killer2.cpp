#include "killer2.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

Killer2::Killer2(Workload *workload,
               SchedulingDecision *decision,
               Queue *queue,
               ResourceSelector *selector,
               double rjms_delay,
               rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Killer2::~Killer2()
{

}

void Killer2::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    (void) date;
    (void) batsim_config;

    available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(available_machines.size() == (unsigned int) _nb_machines);
}

void Killer2::on_simulation_end(double date)
{
    (void) date;
}

void Killer2::make_decisions(double date,
                            SortableJobOrder::UpdateInformation *update_info,
                            SortableJobOrder::CompareInformation *compare_info)
{
    LOG_F(1, "Date: %g. Available machines: %s", date, available_machines.to_string_brackets().c_str());

    // Let's update available machines
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        int nb_available_before = available_machines.size();
        available_machines.insert(current_allocations[ended_job_id]);
        PPK_ASSERT_ERROR(nb_available_before + (int)current_allocations[ended_job_id].size() == (int)available_machines.size());
        current_allocations.erase(ended_job_id);
    }

    LOG_F(1, "Date: %g. Available machines: %s", date, available_machines.to_string_brackets().c_str());

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
            _decision->add_reject_job(new_job_id, date);
        else
            _queue->append_job(new_job, update_info);
    }

    // Queue sorting
    _queue->sort_queue(update_info, compare_info);

    if (!_queue->is_empty())
    {
        const Job * job = _queue->first_job();

        // Kill the job if it is running
        if (current_allocations.size() > 0)
        {
            PPK_ASSERT_ERROR(current_allocations.size() == 1);
            string running_job = current_allocations.begin()->first;

            _decision->add_kill_job({running_job}, date);
            available_machines.insert(current_allocations[running_job]);
            current_allocations.erase(running_job);
            date += 5; // The execution will be 5 seconds after the kill
        }

        // Run the priority job
        IntervalSet used_machines;
        bool fitted = _selector->fit(job, available_machines, used_machines);
        PPK_ASSERT_ERROR(fitted);

        _decision->add_execute_job(job->id, used_machines, date);
        current_allocations[job->id] = used_machines;

        available_machines.remove(used_machines);
        _queue->remove_job(job);
    }

    LOG_F(1, "Date: %g. Available machines: %s", date, available_machines.to_string_brackets().c_str());
}
