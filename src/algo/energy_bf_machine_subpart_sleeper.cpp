#include "energy_bf_machine_subpart_sleeper.hpp"

#include <loguru.hpp>

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

    LOG_SCOPE_FUNCTION(INFO);
    LOG_F(INFO, "Fraction of machines to let awake: %g",
           (double) _fraction_of_machines_to_let_awake);
}

EnergyBackfillingMachineSubpartSleeper::~EnergyBackfillingMachineSubpartSleeper()
{

}

void EnergyBackfillingMachineSubpartSleeper::on_monitoring_stage(double date)
{
    update_first_slice_taking_sleep_jobs_into_account(date);
    _inertial_schedule = _schedule;

    // Let's check whether the asleep machines are consistent
    IntervalSet machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                        + _machines_to_sedate - _machines_to_awaken;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);

    // Let's clear the machines to sedate, to possibly take new ones which would be sedated before
    if (_machines_to_sedate.size() > 0)
    {
        // The machines sedated for being idle should never be done in the future, so
        // cancelled switches OFF are inertial ones. However, for coherency's sake,
        // the number of idle-sedated machines might also be decreased if there is no
        // inertially-sedated machines left.
        int nb_cancelled_switches_off = (int) _machines_to_sedate.size();
        int nb_cancelled_inertially_sedated_machines = min(nb_cancelled_switches_off,
                                                           _nb_machines_sedated_by_inertia);

        int nb_cancelled_idle_sedated_machines = 0;
        if (nb_cancelled_inertially_sedated_machines < nb_cancelled_switches_off)
            nb_cancelled_idle_sedated_machines = nb_cancelled_switches_off -
                                                 nb_cancelled_inertially_sedated_machines;

        PPK_ASSERT_ERROR(nb_cancelled_inertially_sedated_machines +
                         nb_cancelled_idle_sedated_machines == nb_cancelled_switches_off);

        _nb_machines_sedated_by_inertia -= nb_cancelled_inertially_sedated_machines;
        _nb_machines_sedated_for_being_idle -= nb_cancelled_idle_sedated_machines;

        PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia >= 0);
        PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0);

        _machines_to_sedate.clear();

        // Let's check whether the asleep machines are consistent
        machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                               + _machines_to_sedate - _machines_to_awaken;
        PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                             _nb_machines_sedated_for_being_idle,
                         "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                         "nb_sedated_idle=%d\n",
                         (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                         _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
    }

    const Job * priority_job = _queue->first_job_or_nullptr();
    IntervalSet machines_awake_soon = (_awake_machines + _switching_on_machines - _switching_off_machines)
                                       + _machines_to_awaken - _machines_to_sedate;
    int nb_machines_to_let_awakened = (int) (_fraction_of_machines_to_let_awake * _all_machines.size());
    if (_inertial_shutdown_debug)
        LOG_F(1, "Date=%g. nb_machines_to_let_awakened=%d", date, nb_machines_to_let_awakened);

    IntervalSet machines_that_can_be_used_by_the_priority_job;
    Schedule::JobAlloc priority_job_alloc;
    IntervalSet priority_job_reserved_machines;
    bool priority_job_needs_awakenings = false;

    if (_inertial_shutdown_debug)
        LOG_F(1, "Schedule without priority_job.%s", _inertial_schedule.to_string().c_str());

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
            LOG_F(1, "Date=%g. nb_machines_to_sedate=%d", date, nb_machines_to_sedate);
        if (nb_machines_to_sedate > 0)
        {
            if (_inertial_shutdown_debug)
                LOG_F(1, "Date=%g. nb_machines_to_sedate=%d. machines_awake_soon=%s. "
                       "machines_that_can_be_used_by_the_priority_job=%s",
                       date, nb_machines_to_sedate,
                       machines_awake_soon.to_string_brackets().c_str(),
                       machines_that_can_be_used_by_the_priority_job.to_string_brackets().c_str());

            // Let's "steal" some machines sedated for being idle if needed
            int nb_idle_machines_to_steal = 0;
            if (_nb_machines_sedated_for_being_idle > 0)
            {
                nb_idle_machines_to_steal = min(_nb_machines_sedated_for_being_idle,
                                                    (int) nb_machines_to_sedate);
                _nb_machines_sedated_by_inertia += nb_idle_machines_to_steal;
                _nb_machines_sedated_for_being_idle -= nb_idle_machines_to_steal;
            }

            IntervalSet machines_to_sedate;
            select_machines_to_sedate(nb_machines_to_sedate, machines_awake_soon,
                                      machines_that_can_be_used_by_the_priority_job,
                                      machines_to_sedate, priority_job);
            LOG_F(INFO, "Date=%g. Machines to sedate were %s", date,
                   _machines_to_sedate.to_string_brackets().c_str());
            LOG_F(INFO, "Date=%g. Machines asleep soon were %s", date,
                   machines_asleep_soon.to_string_brackets().c_str());

            if (_inertial_shutdown_debug)
                LOG_F(1, "Date=%g. machines_to_sedate=%s",
                       date, machines_to_sedate.to_string_brackets().c_str());

            _machines_to_sedate += machines_to_sedate;
            _nb_machines_sedated_by_inertia += (int) machines_to_sedate.size();

            LOG_F(INFO, "Machines to sedate are now %s", _machines_to_sedate.to_string_brackets().c_str());

            IntervalSet machines_sedated_this_turn, machines_awakened_this_turn, empty_range;
            handle_queued_switches(_inertial_schedule, _machines_to_sedate,
                                   empty_range, machines_sedated_this_turn,
                                   machines_awakened_this_turn);

            PPK_ASSERT_ERROR(machines_awakened_this_turn == IntervalSet::empty_interval_set());

            if (machines_sedated_this_turn.size() > 0)
                LOG_F(INFO, "Date=%g. Those machines should be put to sleep now: %s",
                       date, machines_sedated_this_turn.to_string_brackets().c_str());

            _machines_to_sedate -= machines_sedated_this_turn;

            PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == IntervalSet::empty_interval_set());

            make_decisions_of_schedule(_inertial_schedule, false);

            // Let's make sure the asleep machines are still consistent
            machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                   + _machines_to_sedate - _machines_to_awaken;
            PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                                 _nb_machines_sedated_for_being_idle,
                             "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                             "nb_sedated_idle=%d\n",
                             (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                             _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
        }

        // Let's now try to sedate the machines which have been idle for too long
        EnergyBackfillingIdleSleeper::update_idle_states(date, _inertial_schedule, _all_machines,
                                                         _idle_machines, _machines_idle_start_date);
        machines_awake_soon = _awake_machines + _switching_on_machines +
                              _machines_to_awaken - _machines_to_sedate;

        // Guard to prevent switch ON/OFF cycles
        if (_switching_on_machines.size() == 0 && _machines_to_awaken.size() == 0)
        {
            IntervalSet machines_to_sedate_for_being_idle;
            EnergyBackfillingIdleSleeper::select_idle_machines_to_sedate(date,
                                            _idle_machines, machines_awake_soon,
                                            priority_job, _machines_idle_start_date,
                                            _needed_amount_of_idle_time_to_be_sedated,
                                            machines_to_sedate_for_being_idle);

            if (machines_to_sedate_for_being_idle.size() > 0)
            {
                // Let's handle queue switches
                IntervalSet machines_sedated_this_turn;
                IntervalSet machines_awakened_this_turn;
                IntervalSet empty_range;

                handle_queued_switches(_inertial_schedule, machines_to_sedate_for_being_idle, empty_range,
                                       machines_sedated_this_turn, machines_awakened_this_turn);

                // A subset of those machines can be sedated (not all of them because it might be jobs
                // in the future on some resources)
                PPK_ASSERT_ERROR((machines_sedated_this_turn & machines_to_sedate_for_being_idle) ==
                                 machines_sedated_this_turn,
                                 "The sedated machines are not the expected ones.Sedated=%s.\nExpected=subset of %s",
                                 machines_sedated_this_turn.to_string_brackets().c_str(),
                                 machines_to_sedate_for_being_idle.to_string_brackets().c_str());

                PPK_ASSERT_ERROR(machines_awakened_this_turn == IntervalSet::empty_interval_set(),
                                 "The awakened machines are not the expected ones.Awakened=%s.\nExpected=%s",
                                 machines_awakened_this_turn.to_string_brackets().c_str(),
                                 IntervalSet::empty_interval_set().to_string_brackets().c_str());

                LOG_F(INFO, "Date=%g. Those machines should be put to sleep now for being idle: %s",
                           date, machines_sedated_this_turn.to_string_brackets().c_str());


                PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == IntervalSet::empty_interval_set());

                if (_inertial_shutdown_debug)
                {
                    LOG_F(1, "Date=%g. Before make_decisions_of_schedule. %s",
                           date, _inertial_schedule.to_string().c_str());
                    write_schedule_debug("_on_monitoring_before_make_decisions_of_schedule");
                }

                // Let's finally make the idle decisions!
                make_decisions_of_schedule(_inertial_schedule, false);

                _nb_machines_sedated_for_being_idle += machines_sedated_this_turn.size();
                PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle <= _nb_machines);

                // Let's make sure the asleep machines are still consistent
                machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                       + _machines_to_sedate - _machines_to_awaken;
                PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                                     _nb_machines_sedated_for_being_idle,
                                 "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                                 "nb_sedated_idle=%d\n",
                                 (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                                 _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
            }
        }
    }

    PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia <= _nb_machines - nb_machines_to_let_awakened);

    // Let's update the reason why machines are sedated if needed
    if (_nb_machines_sedated_for_being_idle > nb_machines_to_let_awakened)
    {
        int nb_machines_to_transfer = _nb_machines_sedated_for_being_idle - nb_machines_to_let_awakened;
        PPK_ASSERT_ERROR(nb_machines_to_transfer > 0);

        _nb_machines_sedated_for_being_idle -= nb_machines_to_transfer;
        _nb_machines_sedated_by_inertia += nb_machines_to_transfer;

        PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0);
        PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia <= _nb_machines);
    }

    PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle <= nb_machines_to_let_awakened);
}
