#include "energy_bf_machine_subpart_sleeper.hpp"

#include "energy_bf_idle_sleeper.hpp"
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

    const Job * priority_job = _queue->first_job_or_nullptr();
    MachineRange machines_awake_soon = _awake_machines + _switching_on_machines + _machines_to_awaken
                                        - _switching_off_machines - _machines_to_sedate;
    int nb_machines_to_let_awakened = (int) (_fraction_of_machines_to_let_awake * _all_machines.size());
    if (_inertial_shutdown_debug)
        printf("Date=%g. nb_machines_to_let_awakened=%d\n", date, nb_machines_to_let_awakened);

    MachineRange machines_that_can_be_used_by_the_priority_job;
    Schedule::JobAlloc priority_job_alloc;
    MachineRange priority_job_reserved_machines;
    bool priority_job_needs_awakenings = false;

    if (_inertial_shutdown_debug)
        printf("Schedule without priority_job.%s\n", _inertial_schedule.to_string().c_str());

    compute_priority_job_and_related_stuff(_inertial_schedule, _queue, priority_job,
                                           _selector,
                                           priority_job_needs_awakenings,
                                           priority_job_alloc,
                                           priority_job_reserved_machines,
                                           machines_that_can_be_used_by_the_priority_job);

    if (!priority_job_needs_awakenings)
    {
        if (priority_job != nullptr)
            nb_machines_to_let_awakened = max(nb_machines_to_let_awakened, priority_job->nb_requested_resources);

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


        // Let's now try to sedate the machines which have been idle for too long
        EnergyBackfillingIdleSleeper::update_idle_states(date, _inertial_schedule, _all_machines,
                                                         _idle_machines, _machines_idle_start_date);
        machines_awake_soon = _awake_machines + _switching_on_machines +
                              _machines_to_awaken - _machines_to_sedate;
        MachineRange machines_to_sedate_for_being_idle;
        EnergyBackfillingIdleSleeper::select_idle_machines_to_sedate(date,
                                        _idle_machines, machines_awake_soon,
                                        priority_job, _machines_idle_start_date,
                                        _needed_amount_of_idle_time_to_be_sedated,
                                        machines_to_sedate_for_being_idle);

        if (machines_to_sedate_for_being_idle.size() > 0)
        {
            // Let's handle queue switches
            MachineRange machines_sedated_this_turn;
            MachineRange machines_awakened_this_turn;
            MachineRange empty_range;

            handle_queued_switches(_inertial_schedule, machines_to_sedate_for_being_idle, empty_range,
                                   machines_sedated_this_turn, machines_awakened_this_turn);

            // A subset of those machines can be sedated (not all of them because it might be jobs
            // in the future on some resources)
            PPK_ASSERT_ERROR((machines_sedated_this_turn & machines_to_sedate_for_being_idle) ==
                             machines_sedated_this_turn,
                             "The sedated machines are not the expected ones.Sedated=%s.\nExpected=subset of %s",
                             machines_sedated_this_turn.to_string_brackets().c_str(),
                             machines_to_sedate_for_being_idle.to_string_brackets().c_str());

            PPK_ASSERT_ERROR(machines_awakened_this_turn == MachineRange::empty_range(),
                             "The awakened machines are not the expected ones.Awakened=%s.\nExpected=%s",
                             machines_awakened_this_turn.to_string_brackets().c_str(),
                             MachineRange::empty_range().to_string_brackets().c_str());

            printf("Date=%g. Those machines should be put to sleep now for being idle: %s\n",
                       date, machines_sedated_this_turn.to_string_brackets().c_str());


            PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == MachineRange::empty_range());

            if (_inertial_shutdown_debug)
            {
                printf("Date=%g. Before make_decisions_of_schedule. %s\n",
                       date, _inertial_schedule.to_string().c_str());
                write_schedule_debug("_on_monitoring_before_make_decisions_of_schedule");
            }

            // Let's finally make the idle decisions!
            make_decisions_of_schedule(_inertial_schedule, false);

            _nb_machines_sedated_for_being_idle += machines_sedated_this_turn.size();
            PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle <= _nb_machines);
        }
    }
}
