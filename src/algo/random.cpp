#include "random.hpp"

#include "../pempek_assert.hpp"

using namespace std;

Random::Random(Workload * workload,
               SchedulingDecision * decision,
               Queue * queue,
               ResourceSelector * selector,
               double rjms_delay,
               rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Random::~Random()
{

}

void Random::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    (void) date;

    machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(machines.size() == (unsigned int) _nb_machines);

    // TODO: print warning if time sharing is disabled
    (void) batsim_config;
}

void Random::on_simulation_end(double date)
{
    (void) date;
}

void Random::make_decisions(double date,
                            SortableJobOrder::UpdateInformation *update_info,
                            SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    /* This algorithm does not care whether machines are in use, it does not even store it.
     * Jobs are allocated to random machines as soon as they are submitted. */

    PPK_ASSERT_ERROR(_jobs_killed_recently.size() == 0,
                     "Jobs have been killed, which should not happen with this algorithm.");

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
            // THe job fits in the machine -> executed right now
            _decision->add_execute_job(new_job->id,
                                       machines.random_pick(new_job->nb_requested_resources),
                                       date);
        }
    }
}

