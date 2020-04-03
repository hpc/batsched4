#include "energy_watcher.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EnergyWatcher::EnergyWatcher(Workload * workload,
               SchedulingDecision * decision,
               Queue * queue,
               ResourceSelector * selector,
               double rjms_delay,
               rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

EnergyWatcher::~EnergyWatcher()
{

}

void EnergyWatcher::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    (void) date;

    _machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(_machines.size() == (unsigned int) _nb_machines);

    // TODO: print warning if time sharing is disabled
    (void) batsim_config;
}

void EnergyWatcher::on_simulation_end(double date)
{
    (void) date;
}

void EnergyWatcher::make_decisions(double date,
                            SortableJobOrder::UpdateInformation *update_info,
                            SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    /* This algorithm execute the jobs in order in arrival in a sequence.
       It queries about the energy consumption at each job arrival and checks
       that it is non-decreasing. */

    if (_consumed_joules_updated_recently)
    {
        if (_previous_energy < 0)
            _previous_energy = _consumed_joules;

        PPK_ASSERT_ERROR(_consumed_joules - _previous_energy >= -1e-6,
                         "Energy consumption inconsistency: it should be non-decreasing. "
                         "Received %g but previous value is %g.",
                         _consumed_joules, _previous_energy);
        LOG_F(INFO, "Updating consumed joules. Now=%g. Before=%g.",
               _consumed_joules, _previous_energy);
        _previous_energy = _consumed_joules;
    }

    PPK_ASSERT_ERROR(_jobs_killed_recently.size() == 0,
                     "Jobs have been killed, which should not happen with this algorithm.");

    if (_jobs_ended_recently.size() > 0)
    {
        PPK_ASSERT_ERROR(_is_machine_busy == true);
        _is_machine_busy = false;
    }

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
        {
            // The job is too big for the machine -> reject
            _decision->add_reject_job(new_job_id, date);
        }
        else
        {
            // THe job fits in the machine -> added in queue
            _queue->append_job(new_job, update_info);
        }
    }

    // Query consumed energy if new jobs have been submitted
    if (_jobs_released_recently.size() > 0)
    {
        _decision->add_query_energy_consumption(date);
    }

    // Execute a job if the machine is completely idle
    execute_job_if_whole_machine_is_idle(date);
}

void EnergyWatcher::execute_job_if_whole_machine_is_idle(double date)
{
    const Job * job = _queue->first_job_or_nullptr();

    // If all the machines are available and that there are jobs to compute
    if (!_is_machine_busy && job != nullptr)
    {
        IntervalSet machines_to_use = _machines.left(job->nb_requested_resources);
        _decision->add_execute_job(job->id, machines_to_use, date);

        _is_machine_busy = true;
        _queue->remove_job(job);
    }
}

