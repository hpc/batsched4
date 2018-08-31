#include "killer.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

Killer::Killer(Workload *workload,
               SchedulingDecision *decision,
               Queue *queue,
               ResourceSelector *selector,
               double rjms_delay,
               rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    if (variant_options->HasMember("nb_kills_per_job"))
    {
        PPK_ASSERT_ERROR((*variant_options)["nb_kills_per_job"].IsInt(),
                "Bad algo options: nb_kills_per_job should be an integer");
        nb_kills_per_job = (*variant_options)["nb_kills_per_job"].GetInt();
        PPK_ASSERT_ERROR(nb_kills_per_job >= 0,
                         "Bad algo options: nb_kills_per_job should be positive (got %d)",
                         nb_kills_per_job);
    }

    if (variant_options->HasMember("delay_before_kill"))
    {
        PPK_ASSERT_ERROR((*variant_options)["delay_before_kill"].IsNumber(),
                "Bad algo options: delay_before_kill should be an integer");
        delay_before_kill = (*variant_options)["delay_before_kill"].GetDouble();
        PPK_ASSERT_ERROR(delay_before_kill >= 0,
                         "Bad algo options: delay_before_kill should be positive (got %g)",
                         delay_before_kill);
    }

    LOG_SCOPE_FUNCTION(INFO);
    LOG_F(INFO, "nb_kills_per_job: %d", nb_kills_per_job);
    LOG_F(INFO, "delay_before_kill: %g", delay_before_kill);
}

Killer::~Killer()
{

}

void Killer::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    (void) date;
    (void) batsim_config;

    available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(available_machines.size() == (unsigned int) _nb_machines);
}

void Killer::on_simulation_end(double date)
{
    (void) date;
}

void Killer::make_decisions(double date,
                            SortableJobOrder::UpdateInformation *update_info,
                            SortableJobOrder::CompareInformation *compare_info)
{
    LOG_F(1, "Date: %g. Available machines: %s", date, available_machines.to_string_brackets().c_str());

    // Let's update available machines
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        int nb_available_before = available_machines.size();
        available_machines.insert(current_allocations[ended_job_id]);
        PPK_ASSERT_ERROR(nb_available_before + (*_workload)[ended_job_id]->nb_requested_resources == (int)available_machines.size());
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

    // Let's now do a scheduling pass.
    // This algorithm can only execute one job by call, the priority one.
    // It is executed if it there are enough available resources.
    // The job kill is then queued 10 secondes after its starting.

    if (!_queue->is_empty())
    {
        const Job * job = _queue->first_job();

        if (job->nb_requested_resources <= (int)available_machines.size())
        {
            IntervalSet used_machines;
            if (_selector->fit(job, available_machines, used_machines))
            {
                _decision->add_execute_job(job->id, used_machines, date);
                current_allocations[job->id] = used_machines;
                available_machines.remove(used_machines);
                _queue->remove_job(job);

                for (int i = 0; i < nb_kills_per_job; ++i)
                {
                    date += delay_before_kill;
                    _decision->add_kill_job({job->id}, date);
                }
            }
        }
    }

    LOG_F(1, "Date: %g. Available machines: %s", date, available_machines.to_string_brackets().c_str());
}
