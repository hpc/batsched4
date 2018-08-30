#include "sleeper.hpp"

#include <iostream>

#include "../pempek_assert.hpp"

using namespace std;

Sleeper::Sleeper(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                 double rjms_delay, rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    PPK_ASSERT_ERROR(variant_options->HasMember("pstate_compute"), "Invalid options JSON object: Member 'pstate_compute' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["pstate_compute"].IsInt(), "Invalid options JSON object: Member 'pstate_compute' must be integral");
    int pstate_compute = (*variant_options)["pstate_compute"].GetInt();
    PPK_ASSERT_ERROR(pstate_compute >= 0, "Invalid options JSON object: Member 'pstate_compute' value must be positive (got %d)", pstate_compute);
    compute_pstate = pstate_compute;

    PPK_ASSERT_ERROR(variant_options->HasMember("pstate_sleep"), "Invalid options JSON object: Member 'pstate_sleep' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["pstate_sleep"].IsInt(), "Invalid options JSON object: Member 'pstate_sleep' must be integral");
    int pstate_sleep = (*variant_options)["pstate_sleep"].GetInt();
    PPK_ASSERT_ERROR(pstate_sleep >= 0, "Invalid options JSON object: Member 'pstate_sleep' value must be positive (got %d)", pstate_sleep);
    sleep_pstate = pstate_sleep;
}

Sleeper::~Sleeper()
{

}

void Sleeper::on_simulation_start(double date, const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;

    all_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    available_machines = all_machines;
}

void Sleeper::on_simulation_end(double date)
{
    (void) date;

    simulation_finished = true;
}

void Sleeper::make_decisions(double date, SortableJobOrder::UpdateInformation *update_info, SortableJobOrder::CompareInformation *compare_info)
{
    // Let's handle recently ended jobs
    PPK_ASSERT_ERROR(_jobs_ended_recently.size() == 0 || _jobs_ended_recently.size() == 1);
    if (_jobs_ended_recently.size() > 0)
    {
        string ended_job_id = _jobs_ended_recently[0];
        const Job * finished_job = (*_workload)[ended_job_id];
        PPK_ASSERT_ERROR(job_being_computed == ended_job_id);

        job_being_computed = "";

        PPK_ASSERT_ERROR(finished_job->nb_requested_resources == (int) computing_machines.size());
        available_machines.insert(computing_machines);
        computing_machines.clear();
    }

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
            _decision->add_reject_job(new_job_id, date);
        else
            _queue->append_job(new_job, update_info);
    }

    // Let's handle machine power state alterations
    for (auto mit : _machines_whose_pstate_changed_recently)
    {
        const int & new_pstate = mit.first;
        const IntervalSet & machines = mit.second;

        PPK_ASSERT_ERROR(new_pstate == compute_pstate || new_pstate == sleep_pstate);

        if (new_pstate == compute_pstate)
        {
            PPK_ASSERT_ERROR(machines == (machines & machines_being_switched_on));

            machines_being_switched_on.remove(machines);
            available_machines.insert(machines);
        }
        else if (new_pstate == sleep_pstate)
        {
            PPK_ASSERT_ERROR(machines == (machines & machines_being_switched_off));

            machines_being_switched_off.remove(machines);
            sleeping_machines.insert(machines);
        }
    }

    // Queue sorting
    _queue->sort_queue(update_info, compare_info);

    /* This algorithm will ensure that 0 or 1 job is being computed at any given time.
     * Only the first job of the queue can be computed.
     *
     * Unneeded resources will be put to sleep.
     * Resources can be awakened in order to run a job.
     */

    if (!_queue->is_empty())
    {
        // There are jobs to compute
        const Job * job = _queue->first_job();

        if (job_being_computed == "")
        {
            PPK_ASSERT_ERROR(computing_machines.size() == 0);
            // No job is being computed now. We should then run the job!
            if ((int)available_machines.size() >= job->nb_requested_resources)
            {
                // The job can be executed right now
                IntervalSet alloc = available_machines.left(job->nb_requested_resources);
                _decision->add_execute_job(job->id, alloc, date);

                // Let's update machine states
                available_machines.remove(alloc);
                computing_machines.insert(alloc);

                // Let's put unused machines into sleep
                if (available_machines.size() > 0)
                {
                    _decision->add_set_resource_state(available_machines, sleep_pstate, date);
                    machines_being_switched_off.insert(available_machines);
                    available_machines.clear();
                }

                job_being_computed = job->id;
                _queue->remove_job(job);
            }
            else
            {
                // There are not enough available machine now to run the job
                int future_nb_avail = available_machines.size() + machines_being_switched_on.size();

                if (future_nb_avail < job->nb_requested_resources)
                {
                    // Some machines must be ordered to wake up so the job can run.
                    int nb_machines_to_wake_up = job->nb_requested_resources - future_nb_avail;
                    int nb_machines_to_order_to_wake_up = std::min((int)sleeping_machines.size(), nb_machines_to_wake_up);

                    if (nb_machines_to_order_to_wake_up > 0)
                    {
                        // Decision
                        IntervalSet machines_to_order_to_wake_up = sleeping_machines.left(nb_machines_to_order_to_wake_up);
                        _decision->add_set_resource_state(machines_to_order_to_wake_up, compute_pstate, date);

                        // Machines state update
                        sleeping_machines.remove(machines_to_order_to_wake_up);
                        machines_being_switched_on.insert(machines_to_order_to_wake_up);
                    }
                }
                else
                {
                    // Machines are already being waken up, there is nothing to do.
                }
            }
        }
        else
        {
            // A job is already being computed, there is nothing to do but to put as much machines as possible to sleep

            // Let's put unused machines into sleep
            if (available_machines.size() > 0)
            {
                _decision->add_set_resource_state(available_machines, sleep_pstate, date);
                machines_being_switched_off.insert(available_machines);
                available_machines.clear();
            }
        }
    }
    else if (!simulation_finished)
    {
        // There are no jobs to compute at the moment.

        // Let's put unused machines into sleep
        if (available_machines.size() > 0)
        {
            _decision->add_set_resource_state(available_machines, sleep_pstate, date);
            machines_being_switched_off.insert(available_machines);
            available_machines.clear();
        }
    }
}
