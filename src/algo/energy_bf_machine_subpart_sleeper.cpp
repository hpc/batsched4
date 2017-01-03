#include "energy_bf_machine_subpart_sleeper.hpp"

#include "../pempek_assert.hpp"

using namespace std;

EnergyBackfillingMachineSubpartSleeper::EnergyBackfillingMachineSubpartSleeper(Workload *workload, SchedulingDecision *decision,
                                                                               Queue *queue, ResourceSelector *selector,
                                                                               double rjms_delay, rapidjson::Document *variant_options) :
    EnergyBackfillingMonitoringInertialShutdown(workload, decision, queue, selector, rjms_delay, variant_options)
{
    PPK_ASSERT_ERROR(variant_options->HasMember("fraction_of_machines_to_let_awake"),
                     "Invalid options JSON object: Member 'fraction_of_machines_to_let_awake' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["fraction_of_machines_to_let_awake"].IsNumber(),
                     "Invalid options JSON object: Member 'fraction_of_machines_to_let_awake' must be a number");
    _fraction_of_machines_to_let_awake = (*variant_options)["fraction_of_machines_to_let_awake"].GetDouble();

    PPK_ASSERT_ERROR(_fraction_of_machines_to_let_awake >= 0 && _fraction_of_machines_to_let_awake <= 1,
                     "Invalid options JSON object: Member 'fraction_of_machines_to_let_awake' has an invalid "
                     "value (%g)", (double) _fraction_of_machines_to_let_awake);

    printf("Fraction of machines to let awake: %g\n",
           (double) _fraction_of_machines_to_let_awake);
}

EnergyBackfillingMachineSubpartSleeper::~EnergyBackfillingMachineSubpartSleeper()
{

}

void EnergyBackfillingMachineSubpartSleeper::on_monitoring_stage(double date)
{
    update_first_slice_taking_sleep_jobs_into_account(date);

    _inertial_schedule = _schedule;

    const Job * priority_job = nullptr;
    MachineRange machines_awake_soon = _awake_machines + _switching_on_machines + _machines_to_awaken
                                        - _switching_off_machines - _machines_to_sedate;
    int nb_machines_to_let_awakened = (int) (_fraction_of_machines_to_let_awake * _all_machines.size());
    if (_inertial_shutdown_debug)
        printf("Date=%g. nb_machines_to_let_awakened=%d\n", date, nb_machines_to_let_awakened);

    MachineRange machines_that_can_be_used_by_the_priority_job;
    Schedule::JobAlloc priority_job_alloc;

    if (_inertial_shutdown_debug)
        printf("Schedule without priority_job.%s\n", _inertial_schedule.to_string().c_str());

    // Let's find in which time space the priority job should be executed
    if (!_queue->is_empty())
    {
        priority_job = _queue->first_job();
        // To do so, let's insert the priority job into the schedule.

        PPK_ASSERT_ERROR(!_inertial_schedule.contains_job(priority_job),
                         "The priority job is not supposed to be in the schedule, "
                         "but it is. Priority job = '%s'.\n%s",
                         priority_job->id.c_str(), _inertial_schedule.to_string().c_str());
        priority_job_alloc = _inertial_schedule.add_job_first_fit(priority_job, _selector);

        // Now we want to determine which machines the priority job can use at this period of time.
        // To do so, let's remove it from the schedule then compute all available machines during
        // this period of time.
        _inertial_schedule.remove_job(priority_job);

        machines_that_can_be_used_by_the_priority_job = _inertial_schedule.available_machines_during_period(priority_job_alloc.begin, priority_job_alloc.end);

        // Let's also update the number of machines to let awakened if needed
        nb_machines_to_let_awakened = max(nb_machines_to_let_awakened, priority_job->nb_requested_resources);
        if (_inertial_shutdown_debug)
            printf("Date=%g. nb_machines_to_let_awakened=%d\n", date, nb_machines_to_let_awakened);
    }

    int nb_machines_to_sedate = (int)machines_awake_soon.size() - nb_machines_to_let_awakened;
    if (_inertial_shutdown_debug)
        printf("Date=%g. nb_machines_to_sedate=%d\n", date, nb_machines_to_sedate);
    if (nb_machines_to_sedate > 0)
    {
        if (_inertial_shutdown_debug)
            printf("Date=%g. nb_machines_to_sedate=%d. machines_awake_soon=%s. "
                   "machines_that_can_be_used_by_the_priority_job=%s\n",
                   date, nb_machines_to_sedate,
                   machines_awake_soon.to_string_brackets().c_str(),
                   machines_that_can_be_used_by_the_priority_job.to_string_brackets().c_str());

        MachineRange machines_to_sedate;
        select_machines_to_sedate(nb_machines_to_sedate, machines_awake_soon,
                                  machines_that_can_be_used_by_the_priority_job,
                                  machines_to_sedate, priority_job);

        if (_inertial_shutdown_debug)
            printf("Date=%g. machines_to_sedate=%s",
                   date, machines_to_sedate.to_string_brackets().c_str());
        _machines_to_sedate += machines_to_sedate;

        printf("Machines to sedate are now %s\n", _machines_to_sedate.to_string_brackets().c_str());

        MachineRange machines_sedated_this_turn, machines_awakened_this_turn, empty_range;
        handle_queued_switches(_inertial_schedule, _machines_to_sedate,
                               empty_range, machines_sedated_this_turn,
                               machines_awakened_this_turn);

        PPK_ASSERT_ERROR(machines_awakened_this_turn == MachineRange::empty_range());

        if (machines_sedated_this_turn.size() > 0)
            printf("Date=%g. Those machines should be put to sleep now: %s\n",
                   date, machines_sedated_this_turn.to_string_brackets().c_str());

        _machines_to_sedate -= machines_sedated_this_turn;
        _nb_machines_sedated_by_inertia += (int) machines_to_sedate.size();

        PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == MachineRange::empty_range());

        make_decisions_of_schedule(_inertial_schedule, false);
    }

    MachineRange machines_asleep_soon = _asleep_machines + _switching_off_machines + _machines_to_sedate
                                        - _machines_to_awaken - _switching_on_machines;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
}
