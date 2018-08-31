#include "energy_bf_dicho.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EnergyBackfillingDichotomy::EnergyBackfillingDichotomy(Workload *workload, SchedulingDecision *decision, Queue *queue,
                                                       ResourceSelector *selector, double rjms_delay,
                                                       rapidjson::Document *variant_options) :
    EnergyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options)
{
    _str_to_comparison_type["switch_on"] = SWITCH_ON;
    _str_to_comparison_type["remove_sleep_jobs"] = REMOVE_SLEEP_JOBS;

    // Let's get the tolerated slowdown loss ratio from the variant options
    PPK_ASSERT_ERROR(variant_options->HasMember("tolerated_slowdown_loss_ratio"),
                     "Invalid options JSON object: Member 'tolerated_slowdown_loss_ratio' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["tolerated_slowdown_loss_ratio"].IsNumber(),
                     "Invalid options JSON object: Member 'tolerated_slowdown_loss_ratio' should be a number");
    _tolerated_slowdown_loss_ratio = (*variant_options)["tolerated_slowdown_loss_ratio"].GetDouble();
    PPK_ASSERT_ERROR(_tolerated_slowdown_loss_ratio >= 0,
                     "Invalid options JSON object: Member 'tolerated_slowdown_loss_ratio' should be positive or null (got %g)",
                     (double) _tolerated_slowdown_loss_ratio);

    if (variant_options->HasMember("comparison_type"))
    {
        PPK_ASSERT_ERROR((*variant_options)["comparison_type"].IsString(),
                         "Invalid options JSON object: Member 'comparison_type' should be a string");
        string comp_type = (*variant_options)["comparison_type"].GetString();
        PPK_ASSERT_ERROR(_str_to_comparison_type.count(comp_type) == 1,
                         "Invalid options JSON object: invalid 'comparison_type' value (%s)",
                         comp_type.c_str());
        _comparison_type = _str_to_comparison_type.at(comp_type);
    }
}

EnergyBackfillingDichotomy::~EnergyBackfillingDichotomy()
{

}

void EnergyBackfillingDichotomy::make_decisions(double date,
                                                SortableJobOrder::UpdateInformation *update_info,
                                                SortableJobOrder::CompareInformation *compare_info)
{
    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        const Job * ended_job = (*_workload)[ended_job_id];
        ++_nb_jobs_completed;

        PPK_ASSERT_ERROR(_schedule.contains_job(ended_job),
                         "Invalid schedule: job '%s' just finished, "
                         "but it not in the schedule...\n%s",
                         ended_job_id.c_str(), _schedule.to_string().c_str());
        PPK_ASSERT_ERROR(!_queue->contains_job(ended_job),
                         "Job '%s' just ended, but it is still in the "
                         "queue...\nQueue : %s",
                         ended_job_id.c_str(),
                         _queue->to_string().c_str());

        // Let's remove the finished job from the schedule
        _schedule.remove_job(ended_job);
    }

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];
        ++_nb_jobs_submitted;

        PPK_ASSERT_ERROR(!_schedule.contains_job(new_job),
                         "Invalid schedule: job '%s' already exists.\n%s",
                         new_job->id.c_str(), _schedule.to_string().c_str());
        PPK_ASSERT_ERROR(!_queue->contains_job(new_job),
                         "Job '%s' is already in the queue!\nQueue:%s",
                         new_job->id.c_str(), _queue->to_string().c_str());

        if (new_job->nb_requested_resources > _nb_machines)
        {
            _decision->add_reject_job(new_job_id, date);
            ++_nb_jobs_completed;
        }
        else
            _queue->append_job(new_job, update_info);
    }

    // Let's update the first slice of the schedule
    update_first_slice_taking_sleep_jobs_into_account(date);

    // Let's update the sorting of the queue
    _queue->sort_queue(update_info, compare_info);

    // Let's find the schedule which:
    //  - minimises the estimated energy consumption
    //  - respects the slowdown constraint: not worse in avg bds (for jobs in schedule) than
    //    tolerated_slowdown_loss_ratio times the bds of the schedule in which all nodes are awaken first.

    // *******************************************************************
    // Let's compute global informations about the current online schedule
    // *******************************************************************
    const Schedule::TimeSlice & online_first_slice = *_schedule.begin();
    IntervalSet sleeping_machines = compute_sleeping_machines(online_first_slice);
    IntervalSet awakenable_sleeping_machines = compute_potentially_awaken_machines(online_first_slice);
    const IntervalSet & available_machines = online_first_slice.available_machines;

    // ****************************************************************
    // Let's first compute the schedule in which all nodes are awakened
    // ****************************************************************
    Schedule awakened_schedule = _schedule;

    if (_comparison_type == SWITCH_ON)
    {
        // Let's awaken every machine at the earliest moment for each machine
        for (auto machine_it = sleeping_machines.elements_begin(); machine_it != sleeping_machines.elements_end(); ++machine_it)
        {
            int machine_id = *machine_it;
            Rational wake_up_moment = find_earliest_moment_to_awaken_machines(awakened_schedule, IntervalSet(machine_id));
            awaken_machine(awakened_schedule, machine_id, wake_up_moment);
        }
    }
    else if (_comparison_type == REMOVE_SLEEP_JOBS)
    {
        // Let's simply remove sleep jobs from the schedule!
        for (auto machine_it = sleeping_machines.elements_begin(); machine_it != sleeping_machines.elements_end(); ++machine_it)
        {
            int machine_id = *machine_it;
            MachineInformation *minfo = _machine_informations.at(machine_id);
            while (awakened_schedule.remove_job_if_exists(minfo->ensured_sleep_job));
            while (awakened_schedule.remove_job_if_exists(minfo->potential_sleep_job));
        }
    }

    // Let's add jobs into the schedule
    put_jobs_into_schedule(awakened_schedule);

    // *********************************************************************************
    // Let's determine if the online schedule respects our avg bds constraint.
    // If so, we should try to sedate machines. Otherwise, we should try to awaken some.
    // *********************************************************************************
    Schedule online_schedule = _schedule;
    put_jobs_into_schedule(online_schedule);

    //Rational online_awakened_comparison_horizon = max(awakened_schedule.finite_horizon(), online_schedule.finite_horizon());

    // Let's compute the jobs metrics and energy of the two schedules
    ScheduleMetrics online_sched_metrics = compute_metrics_of_schedule(online_schedule);
    ScheduleMetrics awakened_sched_metrics = compute_metrics_of_schedule(awakened_schedule);
    //Rational online_sched_energy = estimate_energy_of_schedule(online_schedule, online_awakened_comparison_horizon);
    //Rational awakened_sched_energy = estimate_energy_of_schedule(awakened_schedule, online_awakened_comparison_horizon);

    bool should_sedate_machines = false;
    if (online_sched_metrics.mean_slowdown <= awakened_sched_metrics.mean_slowdown * _tolerated_slowdown_loss_ratio)
        should_sedate_machines = true;

    // *****************************************************************************************
    // Let's do a dichotomy on the number of machines to awaken/sedate to find the best solution
    // *****************************************************************************************

    LOG_F(INFO, "should_sedate_machines=%d", should_sedate_machines);

    if (should_sedate_machines)
    {
         // Sleeping machines are not available since they compute "fake" jobs
        IntervalSet sedatable_machines = available_machines;

        int nb_to_sedate_min = 1; // Online schedule is nb_to_sedate = 0
        int nb_to_sedate_max = sedatable_machines.size();

        Schedule best_schedule = online_schedule;
        ScheduleMetrics best_sched_metrics = online_sched_metrics;
        //Rational best_sched_energy = online_sched_energy;
        int best_nb_to_sedate = 0;

        // Dichotomy
        while (nb_to_sedate_min < nb_to_sedate_max)
        {
            // Select machines to sedate
            int nb_to_sedate = (nb_to_sedate_min + nb_to_sedate_max) / 2;

            IntervalSet machines_to_sedate;
            _selector->select_resources_to_sedate(nb_to_sedate, available_machines, sedatable_machines, machines_to_sedate);

            // Create the schedule with the desired sedated machines
            Schedule schedule = _schedule;

            for (auto machine_it = machines_to_sedate.elements_begin(); machine_it != machines_to_sedate.elements_end(); ++machine_it)
            {
                int machine_id = *machine_it;
                sedate_machine(schedule, machine_id, schedule.begin());
            }

            put_jobs_into_schedule(schedule);

            // Let's compute jobs metrics about the current schedule
            ScheduleMetrics sched_metrics = compute_metrics_of_schedule(schedule);

            // If the schedule respects the avg slowdown constraint
            if (sched_metrics.mean_slowdown <= awakened_sched_metrics.mean_slowdown * _tolerated_slowdown_loss_ratio)
            {
                // Let's compute the energy of both schedules to compare them
                Rational comparison_horizon = max(best_schedule.finite_horizon(), schedule.finite_horizon());
                Rational best_sched_energy = estimate_energy_of_schedule(best_schedule, comparison_horizon);
                Rational sched_energy = estimate_energy_of_schedule(schedule, comparison_horizon);

                LOG_F(INFO, "Current schedule respects the mean slowdown constraint. "
                     "(best_energy, curr_energy) : (%g, %g)",
                     (double)best_sched_energy, (double)sched_energy);

                // Let's update the best solution if needed
                if ((sched_energy < best_sched_energy) ||
                    ((sched_energy == best_sched_energy) && (nb_to_sedate > best_nb_to_sedate)))
                {
                    best_schedule = schedule;
                    best_sched_metrics = sched_metrics;
                    best_sched_energy = sched_energy;
                    best_nb_to_sedate = nb_to_sedate;
                }

                nb_to_sedate_min = nb_to_sedate + 1;
            }
            else
                nb_to_sedate_max = nb_to_sedate - 1;
        }

        // Let's apply the decisions of the best schedule
        make_decisions_of_schedule(best_schedule);
    }
    else
    {
        int nb_to_awaken_min = 1; // online schedule is nb_to_awaken = 0
        int nb_to_awaken_max = awakenable_sleeping_machines.size();

        Schedule best_schedule = online_schedule;
        ScheduleMetrics best_sched_metrics = online_sched_metrics;
        int best_nb_to_awaken = 0;

        // Dichotomy
        while (nb_to_awaken_min < nb_to_awaken_max)
        {
            // Select machines to awaken
            int nb_to_awaken = (nb_to_awaken_min + nb_to_awaken_max) / 2;
            IntervalSet machines_to_awaken;
            _selector->select_resources_to_awaken(nb_to_awaken, available_machines, awakenable_sleeping_machines, machines_to_awaken);

            // Create the schedule with the desired awakened machines
            Schedule schedule = _schedule;

            for (auto machine_it = machines_to_awaken.elements_begin(); machine_it != machines_to_awaken.elements_end(); ++machine_it)
            {
                int machine_id = *machine_it;
                Rational wake_up_date = find_earliest_moment_to_awaken_machines(schedule, IntervalSet(machine_id));
                awaken_machine(schedule, machine_id, wake_up_date);
            }

            put_jobs_into_schedule(schedule);

            ScheduleMetrics sched_metrics = compute_metrics_of_schedule(schedule);

            // If the schedule respects our avg slowdown constraint
            if (sched_metrics.mean_slowdown <= awakened_sched_metrics.mean_slowdown * _tolerated_slowdown_loss_ratio)
            {
                // Let's compute the energy of both schedules to compare them
                Rational comparison_horizon = max(best_schedule.finite_horizon(), schedule.finite_horizon());
                Rational best_sched_energy = estimate_energy_of_schedule(best_schedule, comparison_horizon);
                Rational sched_energy = estimate_energy_of_schedule(schedule, comparison_horizon);

                LOG_F(INFO, "Current schedule respects the mean slowdown constraint. "
                     "(best_energy, curr_energy) : (%g, %g)",
                     (double)best_sched_energy, (double)sched_energy);

                // Let's update the best solution if needed
                if ((sched_energy < best_sched_energy) ||
                    ((sched_energy == best_sched_energy) && (nb_to_awaken < best_nb_to_awaken)))
                {
                    best_schedule = schedule;
                    best_sched_metrics = sched_metrics;
                    best_nb_to_awaken = nb_to_awaken;
                }

                nb_to_awaken_max = nb_to_awaken - 1;
            }
            else
                nb_to_awaken_min = nb_to_awaken + 1;
        }

        // Let's apply the decisions of the best schedule
        make_decisions_of_schedule(best_schedule);
    }
}
