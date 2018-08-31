#include "energy_bf_idle_sleeper.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EnergyBackfillingIdleSleeper::EnergyBackfillingIdleSleeper(Workload *workload,
                                                           SchedulingDecision *decision,
                                                           Queue *queue,
                                                           ResourceSelector *selector,
                                                           double rjms_delay,
                                                           rapidjson::Document *variant_options) :
    EnergyBackfillingMonitoringInertialShutdown(workload, decision, queue, selector, rjms_delay, variant_options)
{
}

EnergyBackfillingIdleSleeper::~EnergyBackfillingIdleSleeper()
{

}

void EnergyBackfillingIdleSleeper::on_monitoring_stage(double date)
{
    update_first_slice_taking_sleep_jobs_into_account(date);
    _inertial_schedule = _schedule;

    update_idle_states(date, _inertial_schedule, _all_machines, _idle_machines,
                       _machines_idle_start_date);
    make_idle_sleep_decisions(date);
}

void EnergyBackfillingIdleSleeper::select_idle_machines_to_sedate(Rational current_date,
                                                                  const IntervalSet &idle_machines,
                                                                  const IntervalSet &machines_awake_soon,
                                                                  const Job *priority_job,
                                                                  const std::map<int, Rational> idle_machines_start_date,
                                                                  Rational minimum_idle_time_to_sedate,
                                                                  IntervalSet &machines_to_sedate,
                                                                  bool take_priority_job_into_account)
{
    int nb_awake_soon = machines_awake_soon.size();

    int nb_needed_for_priority_job = 0;
    if (priority_job != nullptr && take_priority_job_into_account)
        nb_needed_for_priority_job = priority_job->nb_requested_resources;

    Rational sedate_thresh = current_date - minimum_idle_time_to_sedate;
    IntervalSet sedatable_idle_machines;

    for (auto machine_it = idle_machines.elements_begin();
         machine_it != idle_machines.elements_end();
         ++machine_it)
    {
        const int machine_id = *machine_it;
        if (idle_machines_start_date.at(machine_id) <= sedate_thresh)
            sedatable_idle_machines.insert(machine_id);
    }

    // To avoid blocking the priority job with switches OFF, let's reduce
    // the number of machines to switch OFF if needed.
    int nb_machines_to_sedate = max(0, min(nb_awake_soon - nb_needed_for_priority_job,
                                           (int)sedatable_idle_machines.size()));

    machines_to_sedate.clear();
    if (nb_machines_to_sedate > 0)
        machines_to_sedate = sedatable_idle_machines.left(nb_machines_to_sedate);
}

void EnergyBackfillingIdleSleeper::select_idle_machines_to_awaken(const Queue *queue,
                                                                  const Schedule &schedule,
                                                                  ResourceSelector * priority_job_selector,
                                                                  const IntervalSet &idle_machines,
                                                                  AwakeningPolicy policy,
                                                                  int maximum_nb_machines_to_awaken,
                                                                  IntervalSet &machines_to_awaken,
                                                                  bool take_priority_job_into_account)
{
    PPK_ASSERT_ERROR(maximum_nb_machines_to_awaken >= 0);
    machines_to_awaken.clear();

    // If there are no jobs to compute, there is nothing more to do.
    if (queue->nb_jobs() <= 0)
        return;

    // If we only awaken for the priority job, this is already done by Inertial Shutdown.
    if (policy == AWAKEN_FOR_PRIORITY_JOB_ONLY)
        return;

    Schedule schedule_copy = schedule;

    // Let's try to backfill some jobs into the awakenable machines, and wake them up if needed.
    IntervalSet awakable_machines = compute_potentially_awaken_machines(*schedule_copy.begin());

    if (awakable_machines.size() > (unsigned int)maximum_nb_machines_to_awaken)
        awakable_machines = awakable_machines.left(maximum_nb_machines_to_awaken);

    IntervalSet usable_machines = idle_machines + awakable_machines;
    IntervalSet usable_idle_machines = idle_machines;

    // Let's find the priority job and related stuff to avoid penalizing the priority job

    const Job * priority_job;
    bool priority_job_needs_awakenings;
    Schedule::JobAlloc priority_job_alloc;
    IntervalSet priority_job_reserved_machines;
    IntervalSet machines_that_can_be_used_by_the_priority_job;
    compute_priority_job_and_related_stuff(schedule_copy, queue, priority_job,
                                           priority_job_selector,
                                           priority_job_needs_awakenings,
                                           priority_job_alloc,
                                           priority_job_reserved_machines,
                                           machines_that_can_be_used_by_the_priority_job);

    if (policy == AWAKEN_FOR_ALL_JOBS_RESPECTING_PRIORITY_JOB && take_priority_job_into_account)
    {
        // Let's remove the priority_job_reserved_machines from the usable_machines
        usable_idle_machines -= priority_job_reserved_machines;
        usable_machines -= priority_job_reserved_machines;
    }

    auto job_it = queue->begin();
    while (usable_machines.size() > 0 && job_it != queue->end())
    {
        const Job * job = (*job_it)->job;
        if (job->nb_requested_resources <= (int)usable_machines.size())
        {
            IntervalSet machines_to_use_for_this_job;

            if (usable_idle_machines.size() > 0)
            {
                int nb_idle_machines_to_use = min((int)usable_idle_machines.size(), job->nb_requested_resources);
                machines_to_use_for_this_job = usable_idle_machines.left(nb_idle_machines_to_use);
                usable_idle_machines -= machines_to_use_for_this_job;
            }

            machines_to_use_for_this_job += usable_machines.left(job->nb_requested_resources - (int)machines_to_use_for_this_job.size());
            IntervalSet machines_to_awaken_for_this_job = (awakable_machines & machines_to_use_for_this_job);

            usable_machines -= machines_to_use_for_this_job;
            awakable_machines -= machines_to_awaken_for_this_job;
            machines_to_awaken += machines_to_awaken_for_this_job;
        }

        ++job_it;
    }

    PPK_ASSERT_ERROR((int)machines_to_awaken.size() <= maximum_nb_machines_to_awaken);
}

void EnergyBackfillingIdleSleeper::update_idle_states(Rational current_date,
                                                      const Schedule & schedule,
                                                      const IntervalSet & all_machines,
                                                      IntervalSet & idle_machines,
                                                      map<int,Rational> & machines_idle_start_date)
{
    PPK_ASSERT_ERROR(schedule.nb_slices() > 0);
    PPK_ASSERT_ERROR(schedule.first_slice_begin() == Rational(current_date));

    const Schedule::TimeSlice & slice = *schedule.begin();

    IntervalSet machines_newly_available = (slice.available_machines & (all_machines - idle_machines));

    for (auto machine_it = machines_newly_available.elements_begin();
         machine_it != machines_newly_available.elements_end();
         ++machine_it)
    {
        int machine_id = *machine_it;
        machines_idle_start_date[machine_id] = current_date;
    }

    IntervalSet machines_newly_busy = ((all_machines - slice.available_machines) & idle_machines);

    for (auto machine_it = machines_newly_busy.elements_begin();
         machine_it != machines_newly_busy.elements_end();
         ++machine_it)
    {
        int machine_id = *machine_it;
        machines_idle_start_date[machine_id] = schedule.infinite_horizon();
    }

    PPK_ASSERT_ERROR((machines_newly_available & machines_newly_busy) == IntervalSet::empty_interval_set(),
                     "machines_newly_available=%s. machines_newly_busy=%s",
                     machines_newly_available.to_string_brackets().c_str(),
                     machines_newly_busy.to_string_brackets().c_str());

    PPK_ASSERT_ERROR((machines_newly_available & idle_machines) == IntervalSet::empty_interval_set(),
                     "machines_newly_available=%s. _idle_machines=%s",
                     machines_newly_available.to_string_brackets().c_str(),
                     idle_machines.to_string_brackets().c_str());

    PPK_ASSERT_ERROR((machines_newly_busy & idle_machines) == machines_newly_busy,
                     "machines_newly_busy=%s. _idle_machines=%s",
                     machines_newly_busy.to_string_brackets().c_str(),
                     idle_machines.to_string_brackets().c_str());

    idle_machines += machines_newly_available;
    idle_machines -= machines_newly_busy;
}

void EnergyBackfillingIdleSleeper::make_idle_sleep_decisions(double date)
{
    const Job * priority_job = _queue->first_job_or_nullptr();
    IntervalSet machines_awake_soon = (_awake_machines + _switching_on_machines - _switching_off_machines)
                                       + _machines_to_awaken - _machines_to_sedate;

    IntervalSet machines_to_sedate;
    select_idle_machines_to_sedate(date, _idle_machines, machines_awake_soon,
                                   priority_job, _machines_idle_start_date,
                                   _needed_amount_of_idle_time_to_be_sedated,
                                   machines_to_sedate);

    if (machines_to_sedate.size() > 0)
    {
        _machines_to_sedate += machines_to_sedate;
        _nb_machines_sedated_for_being_idle += machines_to_sedate.size();

        IntervalSet machines_sedated_this_turn, machines_awakened_this_turn;
        handle_queued_switches(_inertial_schedule, _machines_to_sedate, _machines_to_awaken,
                               machines_sedated_this_turn, machines_awakened_this_turn);

        if (machines_sedated_this_turn.size() > 0)
            LOG_F(INFO, "Date=%g. Those machines should be put to sleep now: %s",
                   date, machines_sedated_this_turn.to_string_brackets().c_str());
        if (machines_awakened_this_turn.size() > 0)
            LOG_F(INFO, "Date=%g. Those machines should be awakened now: %s",
                   date, machines_awakened_this_turn.to_string_brackets().c_str());

        _machines_to_sedate -= machines_sedated_this_turn;
        _machines_to_awaken -= machines_awakened_this_turn;
        _machines_sedated_since_last_monitoring_stage_inertia = machines_sedated_this_turn;
        _machines_awakened_since_last_monitoring_stage_inertia = machines_awakened_this_turn;

        PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == IntervalSet::empty_interval_set());

        make_decisions_of_schedule(_inertial_schedule, false);
    }
}
