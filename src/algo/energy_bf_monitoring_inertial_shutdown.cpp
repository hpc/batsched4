#include "energy_bf_monitoring_inertial_shutdown.hpp"
#include "easy_bf_plot_liquid_load_horizon.hpp"

#include <boost/regex.hpp>

#include <loguru.hpp>

#include "../pempek_assert.hpp"
#include "energy_bf_idle_sleeper.hpp"

using namespace std;

EnergyBackfillingMonitoringInertialShutdown::EnergyBackfillingMonitoringInertialShutdown(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    EnergyBackfillingMonitoringPeriod(workload, decision, queue, selector, rjms_delay, variant_options)
{
    PPK_ASSERT_ERROR(variant_options->HasMember("trace_output_filename"),
                     "Invalid options JSON object: Member 'trace_output_filename' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["trace_output_filename"].IsString(),
            "Invalid options JSON object: Member 'trace_output_filename' must be a string");
    string trace_output_filename = (*variant_options)["trace_output_filename"].GetString();

    _output_file.open(trace_output_filename);
    PPK_ASSERT_ERROR(_output_file.is_open(), "Couldn't open file %s", trace_output_filename.c_str());

    string buf = "date,nb_jobs_in_queue,first_job_size,priority_job_expected_waiting_time,load_in_queue,liquid_load_horizon\n";
    _output_file.write(buf.c_str(), buf.size());

    if (variant_options->HasMember("allow_future_switches"))
    {
        PPK_ASSERT_ERROR((*variant_options)["allow_future_switches"].IsBool(),
                         "Invalid options JSON object: Member 'allow_future_switches' must be a boolean");

        _allow_future_switches = (*variant_options)["allow_future_switches"].GetBool();
    }

    if (variant_options->HasMember("upper_llh_threshold"))
    {
        PPK_ASSERT_ERROR((*variant_options)["upper_llh_threshold"].IsNumber(),
                         "Invalid options JSON object: Member 'upper_llh_threshold' must be a number");
        _upper_llh_threshold = (*variant_options)["upper_llh_threshold"].GetDouble();

        PPK_ASSERT_ERROR(_upper_llh_threshold >= 0,
                         "Invalid options JSON object: Member 'upper_llh_threshold' must be non-negative.");
    }

    if (variant_options->HasMember("inertial_alteration"))
    {
        PPK_ASSERT_ERROR((*variant_options)["inertial_alteration"].IsString(),
                         "Invalid options JSON object: Member 'inertial_alteration' must be a string");

        string inertial_alteration = (*variant_options)["inertial_alteration"].GetString();

        boost::regex r("\\s*(x|p)\\s*(\\d+|\\d+\\.\\d+)\\s*");

        boost::match_results<std::string::const_iterator> results;
        bool matched = boost::regex_match(inertial_alteration, results, r);
        PPK_ASSERT_ERROR(matched, "Invalid options JSON object: Member 'inertial_alteration' has an invalid format");

        string alteration_type_string = results[1];
        string alteration_number_string = results[2];

        PPK_ASSERT_ERROR(alteration_type_string.size() == 1);
        char alteration_type_char = alteration_type_string[0];
        double alteration_number = std::stod(alteration_number_string);

        if (alteration_type_char == 'p')
            _alteration_type = SUM;
        else if (alteration_type_char == 'x')
            _alteration_type = PRODUCT;
        else
            PPK_ASSERT_ERROR(false);

        PPK_ASSERT_ERROR(alteration_number >= 0,
                         "Invalid options JSON object: Member 'inertial_alteration' "
                         "has an invalid value ('%s' -> %g)",
                         alteration_number_string.c_str(),
                         alteration_number);
        _inertial_alteration_number = alteration_number;
    }

    if (variant_options->HasMember("idle_time_to_sedate"))
    {
        _needed_amount_of_idle_time_to_be_sedated = (*variant_options)["idle_time_to_sedate"].GetDouble();

        PPK_ASSERT_ERROR(_needed_amount_of_idle_time_to_be_sedated >= 0,
                         "Invalid options JSON object: Member 'idle_time_to_sedate' "
                         "has an invalid value (%g)", _needed_amount_of_idle_time_to_be_sedated);
    }

    if (variant_options->HasMember("sedate_idle_on_classical_events"))
    {
        _sedate_idle_on_classical_events = (*variant_options)["sedate_idle_on_classical_events"].GetBool();
    }

    LOG_SCOPE_FUNCTION(INFO);
    LOG_F(INFO, "needed amount of idle time to be sedated: %g", _needed_amount_of_idle_time_to_be_sedated);
    LOG_F(INFO, "Sedate on classical events: %s", _sedate_idle_on_classical_events ? "true" : "false");
}

void EnergyBackfillingMonitoringInertialShutdown::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    EnergyBackfillingMonitoringPeriod::on_simulation_start(date, batsim_config);

    _inertial_schedule = _schedule;

    for (int i = 0; i < _nb_machines; ++i)
        _machines_idle_start_date[i] = date;

    _idle_machines = _all_machines;
}
// break energy_bf_monitoring_inertial_shutdown.cpp:119 if date >= 3300
void EnergyBackfillingMonitoringInertialShutdown::make_decisions(double date,
                                                                 SortableJobOrder::UpdateInformation * update_info,
                                                                 SortableJobOrder::CompareInformation * compare_info)
{
    PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0 &&
                     _nb_machines_sedated_for_being_idle <= (int)_all_machines.size(),
                     "Invalid nb_machines_sedated_for_being_idle value: %d\n",
                     _nb_machines_sedated_for_being_idle);

    if (!_jobs_ended_recently.empty())
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

        // Stop sending CALL_ME_LATER if all jobs have been executed.
        if (_no_more_static_job_to_submit_received &&
            _queue->is_empty() &&
            !EnergyBackfilling::contains_any_nonfake_job(_schedule))
            _stop_sending_call_me_later = true;
    }

    // Let's update the first slice of the schedule
    update_first_slice_taking_sleep_jobs_into_account(date);

    // We can now (after updating the first slice AND before adding new jobs into the queue),
    // compute a  LLH value to know how much the LLH decreased since the last computation.
    Rational llh = EasyBackfillingPlotLiquidLoadHorizon::compute_liquid_load_horizon(_inertial_schedule,
                                                                                     _queue, date);

    Rational trapezoid_area = ((_last_llh_value + llh) / 2) * (date - _last_llh_date);
    PPK_ASSERT_ERROR(trapezoid_area >= 0,
                     "Invalid area computed (%g): Cannot be negative.",
                     (double) trapezoid_area);

    // In order to be visualized properly, this value is stored as date-epsilon
    Rational time_diff_from_last_llh_computation = date - _last_llh_date;

    if (time_diff_from_last_llh_computation > 0)
    {
        Rational epsilon = min(Rational(1e-6), time_diff_from_last_llh_computation / 2);
        if (_write_output_file)
        {
            int first_job_size = -1;
            const Job * first_job = _queue->first_job_or_nullptr();
            if (first_job != nullptr)
                first_job_size = first_job->nb_requested_resources;

            write_output_file((double)(Rational(date) - epsilon), _queue->nb_jobs(),
                              first_job_size,
                              (double) _queue->compute_load_estimation(),
                              (double) llh);
        }
    }

    _llh_integral_since_last_monitoring_stage += trapezoid_area;
    _last_llh_value = llh;
    _last_llh_date = date;

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];
        PPK_ASSERT_ERROR(new_job->has_walltime,
                         "This scheduler only supports jobs with walltimes.");
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

    // Let's update the sorting of the queue
    _queue->sort_queue(update_info, compare_info);

    // Simple Easy Backfilling
    _inertial_schedule = _schedule;
    const Job * priority_job = nullptr;
    IntervalSet priority_job_reserved_machines;
    bool priority_job_can_be_started_soon = true;

    Rational time_to_wake_up = (*_variant_options)["time_switch_on"].GetDouble();
    Rational soon_horizon = time_to_wake_up * 2 + date;

    if (_inertial_shutdown_debug)
    {
        LOG_SCOPE_FUNCTION(1);
        LOG_F(1, "Date=%g. Beginning of make_decisions. Queue: %s.\n%s",
               date, _queue->to_string().c_str(), _inertial_schedule.to_string().c_str());
        write_schedule_debug("_make_decisions_begin");
    }
    else
    {
        LOG_SCOPE_FUNCTION(INFO);
    }

    set<const Job *> allocated_jobs;

    IntervalSet machines_sedated_this_turn, machines_awakened_this_turn;
    handle_queued_switches(_inertial_schedule, _machines_to_sedate, _machines_to_awaken,
                           machines_sedated_this_turn, machines_awakened_this_turn);

    if (_queue->is_empty())
    {
        _machines_to_sedate -= machines_sedated_this_turn;
        _machines_to_awaken -= machines_awakened_this_turn;

        _priority_job_starting_time_expectancy = date;
    }
    else
    {
        bool priority_job_launched_now = true;

        auto job_it = _queue->begin();
        while (priority_job_launched_now && job_it != _queue->end())
        {
            const Job * job = (*job_it)->job;
            priority_job = job;

            if (_inertial_shutdown_debug)
                LOG_F(1, "Date=%g. make_decisions, priority loop, trying to insert priority job '%s'.%s",
                       date, priority_job->id.c_str(), _inertial_schedule.to_string().c_str());

            Schedule::JobAlloc alloc = _inertial_schedule.add_job_first_fit(priority_job, _selector, false);

            if (alloc.has_been_inserted)
            {
                priority_job_reserved_machines = alloc.used_machines;

                if (alloc.started_in_first_slice)
                {
                    priority_job = nullptr;
                    priority_job_reserved_machines.clear();
                    _priority_job_starting_time_expectancy = alloc.begin;

                    priority_job_launched_now = true;
                    allocated_jobs.insert(job);
                }
                else
                {
                    priority_job_launched_now = false;

                    _priority_job_starting_time_expectancy = compute_priority_job_starting_time_expectancy(_inertial_schedule, priority_job);
                    priority_job_can_be_started_soon = _priority_job_starting_time_expectancy <= soon_horizon;
                    priority_job_can_be_started_soon = true; // REMOVEME. Forces the previous behaviour.
                }
            }
            else
            {
                priority_job_launched_now = false;

                // The priority job couldn't be inserted into the Schedule... It means some machines must be awakened first.

                // Before doing anything else, let's compute when the priority job would start if all machines were to be awakened.
                _priority_job_starting_time_expectancy = compute_priority_job_starting_time_expectancy(_inertial_schedule, priority_job);
                priority_job_can_be_started_soon = _priority_job_starting_time_expectancy <= soon_horizon;
                priority_job_can_be_started_soon = true; // REMOVEME. Forces the previous behaviour.

                // If the job can be started "soon", we must make sure is is runnable.
                if (priority_job_can_be_started_soon)
                {
                    // Let's first check whether this was caused by a decision made this turn, so we can cancel the decision before applying it.
                    // To do so, we'll first remove the fake jobs of the machines which have been switched OFF this turn, hopefully to create empty room
                    // for the priority job
                    LOG_F(INFO, "In order to make priority job '%s' fit, canceling the switches OFF of machines %s",
                           priority_job->id.c_str(),
                           _machines_to_sedate.to_string_brackets().c_str());

                    for (auto mit = _machines_to_sedate.elements_begin(); mit != _machines_to_sedate.elements_end(); ++mit)
                    {
                        int machine_id = *mit;
                        MachineInformation * minfo = _machine_informations[machine_id];

                        _inertial_schedule.remove_job_last_occurence(minfo->switch_off_job);
                        if (minfo->ensured_sleep_job->walltime > 0)
                            _inertial_schedule.remove_job_last_occurence(minfo->ensured_sleep_job);
                        _inertial_schedule.remove_job_last_occurence(minfo->potential_sleep_job);
                    }

                    alloc = _inertial_schedule.add_job_first_fit(priority_job, _selector, false);
                    if (alloc.has_been_inserted)
                    {
                        // The job could fit by only cancelling some switches OFF \o/
                        // Let's perserve the switches OFF that do not disturb the priority job.
                        IntervalSet non_disturbing_machines_switches_off = _machines_to_sedate - alloc.used_machines;
                        IntervalSet disturbing_machines_switches_off = (_machines_to_sedate & alloc.used_machines);

                        // The machines sedated for being idle should never be done in the future, so
                        // cancelled switches OFF are inertial ones. However, for coherency's sake,
                        // the number of idle-sedated machines might also be decreased if there is no
                        // inertially-sedated machines left.
                        int nb_cancelled_switches_off = (int) disturbing_machines_switches_off.size();
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

                        LOG_F(INFO, "The priority job '%s' could be inserted by cancelling some switches OFF (%s). "
                               "Let's reinsert a subpart of these switches off : %s",
                               priority_job->id.c_str(),
                               disturbing_machines_switches_off.to_string_brackets().c_str(),
                               non_disturbing_machines_switches_off.to_string_brackets().c_str());

                        // Let's put these machines to sleep.
                        IntervalSet thrash, machines_sedated_this_turn_tmp, empty_range;
                        handle_queued_switches(_inertial_schedule, non_disturbing_machines_switches_off,
                                               empty_range, machines_sedated_this_turn_tmp, thrash);

                        PPK_ASSERT_ERROR((machines_sedated_this_turn_tmp &
                                          non_disturbing_machines_switches_off) == machines_sedated_this_turn_tmp,
                                         "The machines that have been sedated are not the expected ones.\n"
                                         "sedated=%s\nexpected=subset of %s",
                                         machines_sedated_this_turn_tmp.to_string_brackets().c_str(),
                                         non_disturbing_machines_switches_off.to_string_brackets().c_str());

                        PPK_ASSERT_ERROR(thrash == IntervalSet::empty_interval_set(),
                                         "The machines that have been awakeend by this call are not the "
                                         "expected ones.\nawakened=%s\nexpected=%s",
                                         thrash.to_string_elements().c_str(),
                                         IntervalSet::empty_interval_set().to_string_brackets().c_str());

                        machines_sedated_this_turn -= disturbing_machines_switches_off;
                        _machines_to_sedate -= disturbing_machines_switches_off;
                    }
                    else
                    {
                        LOG_F(INFO, "Cancelling some switches OFF was not enough... Some machines must be awakened.");

                        // Cancelling some switches OFF was not enough... Some machines must be awakened.
                        // Let's determine how many of them should be awakened.
                        IntervalSet machines_awake_in_the_prediction_schedule = _awake_machines +
                                _switching_on_machines + _machines_to_awaken; // no machines to sedate, because they were already canceled

                        if (_inertial_shutdown_debug)
                        {
                            LOG_F(1, "_awake_machines: %s", _awake_machines.to_string_brackets().c_str());
                            LOG_F(1, "_switching_on_machines: %s", _switching_on_machines.to_string_brackets().c_str());
                            LOG_F(1, "_switching_off_machines: %s", _switching_off_machines.to_string_brackets().c_str());
                            LOG_F(1, "_machines_to_awaken: %s", _machines_to_awaken.to_string_brackets().c_str());
                            LOG_F(1, "_machines_to_sedate: %s", _machines_to_sedate.to_string_brackets().c_str());

                            LOG_F(1, "Awake machines in prediction schedule: %s (size=%d)",
                                   machines_awake_in_the_prediction_schedule.to_string_brackets().c_str(),
                                   (int)machines_awake_in_the_prediction_schedule.size());
                        }

                        int minimum_nb_machines_to_awaken = job->nb_requested_resources - (int)machines_awake_in_the_prediction_schedule.size();
                        PPK_ASSERT_ERROR(minimum_nb_machines_to_awaken > 0,
                                         "Schedule seems to be inconsistent.\n"
                                         "job->nb_requested_resources=%d. machines_awake_in_the_prediction_schedule=%d\n"
                                         "awake machines: %s. machines being switched ON: %s. Machines to awaken:%s\n%s",
                                         job->nb_requested_resources,
                                         (int)machines_awake_in_the_prediction_schedule.size(),
                                         _awake_machines.to_string_brackets().c_str(),
                                         _switching_on_machines.to_string_brackets().c_str(),
                                         _machines_to_awaken.to_string_brackets().c_str(),
                                         _inertial_schedule.to_string().c_str());

                        LOG_F(INFO, "Date=%g. make_decisions. Would like to awaken %d machines to execute job '%s', "
                               "which requests %d resources.",
                               date, minimum_nb_machines_to_awaken, priority_job->id.c_str(),
                               priority_job->nb_requested_resources);

                        // Let's select which machines should be awakened
                        IntervalSet machines_to_awaken_to_make_priority_job_fit;
                        select_machines_to_awaken(minimum_nb_machines_to_awaken, _asleep_machines + _switching_off_machines,
                                                  machines_to_awaken_to_make_priority_job_fit);

                        // Let's awaken the machines in the schedule
                        IntervalSet thrash, machines_really_awakened_to_make_priority_job_fit;
                        handle_queued_switches(_inertial_schedule, IntervalSet(),
                                               machines_to_awaken_to_make_priority_job_fit,
                                               thrash, machines_really_awakened_to_make_priority_job_fit);
                        PPK_ASSERT_ERROR(thrash == IntervalSet::empty_interval_set(),
                                         "The machines that have been sedated by this call are not the "
                                         "expected ones.\nsedated=%s\nexpected=%s",
                                         thrash.to_string_elements().c_str(),
                                         IntervalSet::empty_interval_set().to_string_brackets().c_str());
                        PPK_ASSERT_ERROR((machines_really_awakened_to_make_priority_job_fit &
                                          machines_to_awaken_to_make_priority_job_fit) ==
                                         machines_really_awakened_to_make_priority_job_fit,
                                         "The machines that have been awakened by this call are not the"
                                         "expected ones.\nawakened=%s\nexpected=subset of %s",
                                         machines_really_awakened_to_make_priority_job_fit.to_string_brackets().c_str(),
                                         machines_to_awaken_to_make_priority_job_fit.to_string_brackets().c_str());

                        // Now the priority job should fit the platform. Let's insert it into it
                        alloc = _inertial_schedule.add_job_first_fit(priority_job, _selector, false);
                        PPK_ASSERT_ERROR(alloc.has_been_inserted,
                                         "Cannot insert the priority job, which should not happen now! "
                                         "priority_job='%s', nb_res=%d\n%s",
                                         priority_job->id.c_str(), priority_job->nb_requested_resources,
                                         _inertial_schedule.to_string().c_str());
                        PPK_ASSERT_ERROR(alloc.begin > _inertial_schedule.first_slice_begin());

                        // Let's update which machines are sedated now.
                        // The machines sedated for being idle should never be done in the future, so
                        // cancelled switches OFF and newly awakened machines are inertial ones.
                        // However, for coherency's sake,
                        // the number of idle-sedated machines might also be decreased if there is no
                        // inertially-sedated machines left.
                        int nb_cancelled_switches_off = (int)_machines_to_sedate.size();
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

                        // Update after awakenings
                        int nb_awakened_machines = (int) machines_to_awaken_to_make_priority_job_fit.size();
                        int nb_awakened_inertially_sedated_machines = min(nb_awakened_machines,
                                                                          _nb_machines_sedated_by_inertia);

                        int nb_awakened_idle_sedated_machines = 0;
                        if (nb_awakened_inertially_sedated_machines < nb_awakened_machines)
                            nb_awakened_idle_sedated_machines = nb_awakened_machines -
                                                                nb_awakened_inertially_sedated_machines;

                        PPK_ASSERT_ERROR(nb_awakened_idle_sedated_machines +
                                         nb_awakened_inertially_sedated_machines == nb_awakened_machines);

                        _nb_machines_sedated_by_inertia -= nb_awakened_inertially_sedated_machines;
                        _nb_machines_sedated_for_being_idle -= nb_awakened_idle_sedated_machines;

                        PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia >= 0);
                        PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0);

                        machines_sedated_this_turn -= _machines_to_sedate;
                        _machines_to_sedate.clear();
                        machines_awakened_this_turn += machines_really_awakened_to_make_priority_job_fit;
                        _machines_to_awaken += (machines_to_awaken_to_make_priority_job_fit - machines_really_awakened_to_make_priority_job_fit);
                    }
                }
            }

            job_it++;
        } // end of priority job management loop

        _machines_to_sedate -= machines_sedated_this_turn;
        _machines_to_awaken -= machines_awakened_this_turn;
        _machines_sedated_since_last_monitoring_stage_inertia += machines_sedated_this_turn;
        _machines_awakened_since_last_monitoring_stage_inertia += machines_awakened_this_turn;

        PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == IntervalSet::empty_interval_set(),
                         "The machines to awaken and those to sedate should be distinct...\n"
                         "machines_to_awaken=%s\nmachines_to_sedate=%s",
                         _machines_to_awaken.to_string_brackets().c_str(),
                         _machines_to_sedate.to_string_brackets().c_str());

        if (machines_sedated_this_turn.size() > 0)
            LOG_F(INFO, "Date=%g. Those machines should be put to sleep now: %s", date,
                   machines_sedated_this_turn.to_string_brackets().c_str());

        if (machines_awakened_this_turn.size() > 0)
            LOG_F(INFO, "Date=%g. Those machines should be awakened now: %s", date,
                   machines_awakened_this_turn.to_string_brackets().c_str());

        if (_inertial_shutdown_debug)
        {
            LOG_F(1, "Date=%g. After priority job loop. %s",
                   date, _inertial_schedule.to_string().c_str());
            write_schedule_debug("_make_decisions_after_priority_job_loop");
        }

        int nb_machines_available_now = (int) _inertial_schedule.begin()->available_machines.size();

        job_it = _queue->begin();
        while (job_it != _queue->end() && nb_machines_available_now > 0)
        {
            const Job * job = (*job_it)->job;
            if (job != priority_job && // Not the priority job
                allocated_jobs.count(job) == 0 && // Not already scheduled
                job->nb_requested_resources <= nb_machines_available_now) // Thin enough to fit the first hole
            {
                Schedule::JobAlloc alloc = _inertial_schedule.add_job_first_fit(job, _selector, false);
                if (alloc.has_been_inserted)
                {
                    if (alloc.started_in_first_slice)
                    {
                        allocated_jobs.insert(job);
                        nb_machines_available_now -= job->nb_requested_resources;
                        PPK_ASSERT_ERROR(nb_machines_available_now >= 0);
                    }
                    else
                        _inertial_schedule.remove_job(job);
                }
            }

            job_it++;
        }
    }

    if (_inertial_shutdown_debug)
    {
        LOG_F(1, "Date=%g. End of make_decisions. %s", date, _inertial_schedule.to_string().c_str());
        write_schedule_debug("_make_decisions_end");
    }

    // Let's make the first decision parts (executing new jobs / awakening some machines for the priority job)
    make_decisions_of_schedule(_inertial_schedule, false);

    IntervalSet machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                        + _machines_to_sedate - _machines_to_awaken;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);

    // Let's make sure all the jobs we marked as executed were removed from the queue
    for (const Job * allocated_job : allocated_jobs)
        PPK_ASSERT_ERROR(!_queue->contains_job(allocated_job),
                         "Inconsistency: job '%s' is marked as allocated but it has not "
                         "been removed from the queue.", allocated_job->id.c_str());

    // Let's awake sedated idle machines if needed
    EnergyBackfillingIdleSleeper::update_idle_states(date, _inertial_schedule, _all_machines,
                                                     _idle_machines, _machines_idle_start_date);

    IntervalSet machines_to_awaken_for_being_idle;
    EnergyBackfillingIdleSleeper::select_idle_machines_to_awaken(_queue,
        _inertial_schedule, _selector,
        _idle_machines,
        EnergyBackfillingIdleSleeper::AwakeningPolicy::AWAKEN_FOR_ALL_JOBS_RESPECTING_PRIORITY_JOB,
        _nb_machines_sedated_for_being_idle,
        machines_to_awaken_for_being_idle,
        true); // <--------------------------------------------------------------------------------- Choice to make.

    if (machines_to_awaken_for_being_idle.size() > 0)
    {
        _machines_to_awaken += machines_to_awaken_for_being_idle;

        // The awakened machines are the previously idle ones
        _nb_machines_sedated_for_being_idle -= machines_to_awaken_for_being_idle.size();
        PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0,
                         "Invalid nb_machines_sedated_for_being_idle value: %d\n",
                         _nb_machines_sedated_for_being_idle);

        IntervalSet machines_sedated_this_turn, machines_awakened_this_turn, empty_range;
        machines_sedated_this_turn.clear();
        machines_awakened_this_turn.clear();
        handle_queued_switches(_inertial_schedule, empty_range, machines_to_awaken_for_being_idle,
                               machines_sedated_this_turn, machines_awakened_this_turn);

        PPK_ASSERT_ERROR(machines_awakened_this_turn == machines_to_awaken_for_being_idle,
                         "Unexpected awakened machines.\n Awakened=%s.\nExpected=%s.",
                         machines_awakened_this_turn.to_string_brackets().c_str(),
                         machines_to_awaken_for_being_idle.to_string_brackets().c_str());

        PPK_ASSERT_ERROR(machines_sedated_this_turn == IntervalSet::empty_interval_set(),
                         "The sedated machines are not the expected ones.Sedated=%s.\nExpected=%s",
                         machines_sedated_this_turn.to_string_brackets().c_str(),
                         IntervalSet::empty_interval_set().to_string_brackets().c_str());

        LOG_F(INFO, "Date=%g. Those machines should be awakened now (previously idle ones): %s",
               date, machines_awakened_this_turn.to_string_brackets().c_str());

        _machines_to_awaken -= machines_awakened_this_turn;

        PPK_ASSERT_ERROR((_machines_to_awaken & _machines_to_sedate) == IntervalSet::empty_interval_set());

        // Let's make the new decisions (switches ON)!
        make_decisions_of_schedule(_inertial_schedule, false);

        machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                               + _machines_to_sedate - _machines_to_awaken;
        PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                             _nb_machines_sedated_for_being_idle,
                         "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                         "nb_sedated_idle=%d\n",
                         (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                         _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
    }
    else if (_sedate_idle_on_classical_events &&
             // Guard to prevent switch ON/OFF cycles
             _switching_on_machines.size() == 0 && _machines_to_awaken.size() == 0)
    {
        // If no machine has been awakened for being idle AND
        // if we should sedate on classical events AND
        // if no machine should be awakened (currently or in the future),
        // we can try to sedate idle machines now.

        // Let's try to sedate the machines which have been idle for too long
        EnergyBackfillingIdleSleeper::update_idle_states(date, _inertial_schedule, _all_machines,
                                                         _idle_machines, _machines_idle_start_date);

        IntervalSet machines_awake_soon = (_awake_machines + _switching_on_machines - _switching_off_machines)
                                           + _machines_to_awaken - _machines_to_sedate;

        IntervalSet machines_to_sedate_for_being_idle;
        EnergyBackfillingIdleSleeper::select_idle_machines_to_sedate(date,
                                        _idle_machines, machines_awake_soon,
                                        priority_job, _machines_idle_start_date,
                                        _needed_amount_of_idle_time_to_be_sedated,
                                        machines_to_sedate_for_being_idle,
                                        true); // <------------------------------------------------- Choice to make.

        if (machines_to_sedate_for_being_idle.size() > 0)
        {
            // Let's handle queue switches
            IntervalSet empty_range;
            machines_sedated_this_turn.clear();
            machines_awakened_this_turn.clear();
            handle_queued_switches(_inertial_schedule, machines_to_sedate_for_being_idle, empty_range,
                                   machines_sedated_this_turn, machines_awakened_this_turn);


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

            // Let's finally make the decisions!
            make_decisions_of_schedule(_inertial_schedule, false);

            _nb_machines_sedated_for_being_idle += machines_sedated_this_turn.size();
            PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle <= (int)_all_machines.size());
        }
    }

    // Now that the decisions have been made, let's compute the LLH again.
    llh = EasyBackfillingPlotLiquidLoadHorizon::compute_liquid_load_horizon(_inertial_schedule,
                                                                            _queue, date);
    if (_write_output_file)
    {
        int first_job_size = -1;
        const Job * first_job = _queue->first_job_or_nullptr();
        if (first_job != nullptr)
            first_job_size = first_job->nb_requested_resources;

        write_output_file(date, _queue->nb_jobs(),
                          first_job_size,
                          (double) _queue->compute_load_estimation(),
                          (double) llh);
    }

    _last_llh_value = llh;
    _last_llh_date = date;

    machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                           + _machines_to_sedate - _machines_to_awaken;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);
}







// break energy_bf_monitoring_inertial_shutdown.cpp:691 if date >= 3600
void EnergyBackfillingMonitoringInertialShutdown::on_monitoring_stage(double date)
{
    PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0 &&
                     _nb_machines_sedated_for_being_idle <= (int)_all_machines.size(),
                     "Invalid nb_machines_sedated_for_being_idle value: %d\n",
                     _nb_machines_sedated_for_being_idle);
    LOG_SCOPE_FUNCTION(INFO);

    // Let's update the first slice of the schedule
    update_first_slice_taking_sleep_jobs_into_account(date);

    // Let's compute a first LLH value
    Rational llh = EasyBackfillingPlotLiquidLoadHorizon::compute_liquid_load_horizon(_inertial_schedule,
                                                                                     _queue, date);

    Rational trapezoid_area = ((_last_llh_value + llh) / 2) * (date - _last_llh_date);
    PPK_ASSERT_ERROR(trapezoid_area >= 0,
                     "Invalid area computed (%g): Cannot be negative.",
                     (double) trapezoid_area);

    // In order to be visualized properly, this value is stored as date-epsilon
    Rational time_diff_from_last_llh_computation = date - _last_llh_date;

    if (time_diff_from_last_llh_computation > 0)
    {
        Rational epsilon = min(Rational(1e-6), time_diff_from_last_llh_computation / 2);
        if (_write_output_file)
        {
            int first_job_size = -1;
            const Job * first_job = _queue->first_job_or_nullptr();
            if (first_job != nullptr)
                first_job_size = first_job->nb_requested_resources;

            write_output_file((double)(Rational(date) - epsilon), _queue->nb_jobs(),
                              first_job_size,
                              (double) _queue->compute_load_estimation(),
                              (double) llh);
        }
    }

    _llh_integral_since_last_monitoring_stage += trapezoid_area;
    _last_llh_value = llh;
    _last_llh_date = date;

    _inertial_schedule = _schedule;
    const Job * priority_job = nullptr;
    IntervalSet priority_job_reserved_machines;
    bool priority_job_can_be_started_soon = true;
    (void) priority_job_can_be_started_soon;

    Rational time_to_wake_up = (*_variant_options)["time_switch_on"].GetDouble();
    Rational soon_horizon = time_to_wake_up * 2 + date;

    if (_inertial_shutdown_debug)
        LOG_F(1, "Date=%g. Begin of on_monitoring_stage.", date);

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
    }

    //_machines_to_awaken.clear();

    if (_inertial_shutdown_debug)
    {
        LOG_F(1, "LLH computed. %s", _inertial_schedule.to_string().c_str());
        write_schedule_debug("_on_monitoring_after_llh");
    }

    IntervalSet machines_that_can_be_used_by_the_priority_job;
    Schedule::JobAlloc priority_job_alloc;
    bool priority_job_needs_awakenings = false;

    if (_inertial_shutdown_debug)
        LOG_F(1, "Schedule without priority_job.%s", _inertial_schedule.to_string().c_str());

    compute_priority_job_and_related_stuff(_inertial_schedule, _queue, priority_job,
                                           _selector,
                                           priority_job_needs_awakenings,
                                           priority_job_alloc,
                                           priority_job_reserved_machines,
                                           machines_that_can_be_used_by_the_priority_job);

    if (priority_job != nullptr)
    {
        Rational priority_job_starting_time_expectancy = _priority_job_starting_time_expectancy;
        priority_job_can_be_started_soon = priority_job_starting_time_expectancy <= soon_horizon;
        priority_job_can_be_started_soon = true; // REMOVEME. Forces the previous behaviour.
    }
    else
    {
        _priority_job_starting_time_expectancy = date;
    }

    if (_first_monitoring_stage)
    {
        _first_monitoring_stage = false;
        _last_decision = AWAKEN_MACHINES;
        _inertial_number = 0;
    }
    else if (!priority_job_needs_awakenings)
    {
        Rational mean_llh_over_last_period = _llh_integral_since_last_monitoring_stage / period();

        if (mean_llh_over_last_period >= _upper_llh_threshold)
        {
            // If the threshold is met, the next decision is forced by hacking some variables.
            if (_last_decision == SEDATE_MACHINES)
            {
                _last_decision = AWAKEN_MACHINES;
                _inertial_number = 0;
            }

            _llh_integral_of_preceding_monitoring_stage_slice = 0;
        }

        if (_last_decision == AWAKEN_MACHINES)
        {
            if (_llh_integral_since_last_monitoring_stage > _llh_integral_of_preceding_monitoring_stage_slice)
            {
                // The LLH's integral is still increasing! We may want to awaken more machines!
                IntervalSet awakable_machines = _asleep_machines - _machines_to_awaken;

                if (!_allow_future_switches)
                    awakable_machines -= _non_wakable_asleep_machines;

                int nb_machines_to_awaken_by_inertia = 0;
                if (_alteration_type == PRODUCT)
                    nb_machines_to_awaken_by_inertia = (int)((int)_machines_awakened_since_last_monitoring_stage_inertia.size() *
                                                       _inertial_alteration_number);
                else if (_alteration_type == SUM)
                    nb_machines_to_awaken_by_inertia = (int)((int)_machines_awakened_since_last_monitoring_stage_inertia.size() +
                                                       _inertial_alteration_number);
                else
                    PPK_ASSERT_ERROR(false);

                _inertial_number = min(max(nb_machines_to_awaken_by_inertia,1),
                                       (int)awakable_machines.size());

                if (_inertial_number > 0)
                {
                    LOG_F(INFO, "Date=%g. on_monitoring_stage. Would like to awaken %d machines.",
                           date, _inertial_number);

                    IntervalSet machines_to_awaken;
                    select_machines_to_awaken(_inertial_number, awakable_machines, machines_to_awaken);

                    LOG_F(INFO, "Date=%g. Decided to awaken machines %s", date, machines_to_awaken.to_string_brackets().c_str());

                    _machines_to_awaken += machines_to_awaken;

                    // The awakened machines are the inertially-sedated ones.
                    // If there is no inertially-sedated machines left, the idle-sedated ones are impacted
                    int nb_awakened_inertially_sedated_machines = min((int)machines_to_awaken.size(),
                                                                      _nb_machines_sedated_by_inertia);
                    int nb_awakened_idle_sedated_machines = 0;
                    if (nb_awakened_inertially_sedated_machines < (int)machines_to_awaken.size())
                        nb_awakened_idle_sedated_machines = (int)machines_to_awaken.size() -
                                                            nb_awakened_inertially_sedated_machines;

                    _nb_machines_sedated_by_inertia -= nb_awakened_inertially_sedated_machines;
                    _nb_machines_sedated_for_being_idle -= nb_awakened_idle_sedated_machines;

                    PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia >= 0);
                    PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0);
                }
            }
            else
            {
                // The LLH has decreased, let's switch to sedate mode :)
                _last_decision = SEDATE_MACHINES;
                _inertial_number = 0;

                LOG_F(INFO, "Date=%g. Decided to do nothing now, but switching to sedate mode!", date);
            }
        } // end if (_last_decision == AWAKEN_MACHINES)
        else // if (_last_decision == SEDATE_MACHINES)
        {
            if (_llh_integral_since_last_monitoring_stage <= _llh_integral_of_preceding_monitoring_stage_slice)
            {
                // The LLH's integral is still decreasing, let's sedate more machines!

                IntervalSet sedatable_machines = _awake_machines - _machines_to_sedate;

                if (!_allow_future_switches)
                {
                    PPK_ASSERT_ERROR(_inertial_schedule.nb_slices() > 0);
                    auto slice_it = _inertial_schedule.begin();
                    const Schedule::TimeSlice & slice = *slice_it;

                    sedatable_machines -= (_all_machines - slice.available_machines);
                }

                int nb_sedatable_machines = sedatable_machines.size();

                if (priority_job != nullptr)
                    nb_sedatable_machines = max(0, (int)sedatable_machines.size() - (int)priority_job_reserved_machines.size());

                int nb_machines_to_sedate_by_inertia = 0;
                if (_alteration_type == PRODUCT)
                    nb_machines_to_sedate_by_inertia = (int)((int)_machines_sedated_since_last_monitoring_stage_inertia.size() *
                                                       _inertial_alteration_number);
                else if (_alteration_type == SUM)
                    nb_machines_to_sedate_by_inertia = (int)((int)_machines_sedated_since_last_monitoring_stage_inertia.size() +
                                                       _inertial_alteration_number);
                else
                    PPK_ASSERT_ERROR(false);

                _inertial_number = min(max(nb_machines_to_sedate_by_inertia,1),
                                       nb_sedatable_machines);

                if (_inertial_number > 0)
                {
                    IntervalSet machines_to_sedate;
                    int nb_idle_machines_to_steal = 0;
                    int nb_new_machines_to_sedate = _inertial_number;

                    // Let's just use already sedated machines (for being idle) as if they were inertially sedated
                    if (_nb_machines_sedated_for_being_idle > 0)
                    {
                        nb_idle_machines_to_steal = min(_nb_machines_sedated_for_being_idle,
                                                            (int) _inertial_number);
                        nb_new_machines_to_sedate = _inertial_number - nb_idle_machines_to_steal;

                        PPK_ASSERT_ERROR(nb_idle_machines_to_steal >= 0 &&
                                         nb_idle_machines_to_steal <= _nb_machines_sedated_for_being_idle);
                        PPK_ASSERT_ERROR(nb_new_machines_to_sedate >= 0);
                    }

                    if (nb_new_machines_to_sedate > 0)
                        select_machines_to_sedate(_inertial_number, sedatable_machines,
                                                  machines_that_can_be_used_by_the_priority_job,
                                                  machines_to_sedate, priority_job);

                    LOG_F(INFO, "Date=%g. Decided to sedate machines %s",
                           date, machines_to_sedate.to_string_brackets().c_str());
                    if (nb_idle_machines_to_steal > 0)
                        LOG_F(INFO, "... and to steal %d idle-sedated machines",
                               nb_idle_machines_to_steal);

                    _nb_machines_sedated_by_inertia += machines_to_sedate.size() + nb_idle_machines_to_steal;
                    _nb_machines_sedated_for_being_idle -= nb_idle_machines_to_steal;
                    _machines_to_sedate += machines_to_sedate;

                    PPK_ASSERT_ERROR(_nb_machines_sedated_by_inertia + _nb_machines_sedated_for_being_idle <= _nb_machines);
                    PPK_ASSERT_ERROR(_nb_machines_sedated_for_being_idle >= 0);
                }
            }
            else
            {
                // The LLH has increased, let's switch to awakening mode :)
                _last_decision = AWAKEN_MACHINES;
                _inertial_number = 0;

                LOG_F(INFO, "Date=%g. Decided to do nothing now, but switching to awakening mode!", date);
            }
        }
    }

    if (priority_job != nullptr && !priority_job_needs_awakenings)
    {
        // Let's make sure the priority job has not been delayed by the choices we made.
        Schedule::JobAlloc priority_job_alloc2 = _inertial_schedule.add_job_first_fit(priority_job, _selector);

        if (_inertial_shutdown_debug)
        {
            LOG_F(1, "After decisions.%s", _inertial_schedule.to_string().c_str());
        }

        PPK_ASSERT_ERROR(priority_job_alloc2.has_been_inserted &&
                         priority_job_alloc2.begin <= priority_job_alloc.begin,
                         "Invalid energy-related decisions have been made: "
                         "These decisions delayed the priority job. "
                         "Starting time before:%g. Starting time after:%g\n",
                         (double)priority_job_alloc.begin, (double)priority_job_alloc2.begin);
    }

    _last_llh_value = llh;
    _last_llh_date = date;
    _llh_integral_of_preceding_monitoring_stage_slice = _llh_integral_since_last_monitoring_stage;
    _llh_integral_since_last_monitoring_stage = 0;

    // Let's handle queue switches (first run, to make the first decisions)
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

    // Let's make the inertial decisions
    make_decisions_of_schedule(_inertial_schedule, false);

    IntervalSet machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                        + _machines_to_sedate - _machines_to_awaken;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);

    if (priority_job_needs_awakenings)
    {
        // In this case, we can only try to add the priority job after doing the previously requested awakenings.
        PPK_ASSERT_ERROR(priority_job != nullptr);

        _inertial_schedule.add_job_first_fit(priority_job, _selector, false);
    }

    // Let's now try to sedate the machines which have been idle for too long
    EnergyBackfillingIdleSleeper::update_idle_states(date, _inertial_schedule, _all_machines,
                                                     _idle_machines, _machines_idle_start_date);
    IntervalSet machines_awake_soon = (_awake_machines + _switching_on_machines - _switching_off_machines)
                                       + _machines_to_awaken - _machines_to_sedate;

    // Guard to prevent switch ON/OFF cycles
    if (_switching_on_machines.size() == 0 && _machines_to_awaken.size() == 0)
    {
        IntervalSet machines_to_sedate_for_being_idle;
        EnergyBackfillingIdleSleeper::select_idle_machines_to_sedate(date,
                                        _idle_machines, machines_awake_soon,
                                        priority_job, _machines_idle_start_date,
                                        _needed_amount_of_idle_time_to_be_sedated,
                                        machines_to_sedate_for_being_idle,
                                        true); // <------------------------------------------------- Choice to make.

        if (machines_to_sedate_for_being_idle.size() > 0)
        {
            // Let's handle queue switches
            machines_sedated_this_turn.clear();
            machines_awakened_this_turn.clear();
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
        }
    }

    if (_inertial_shutdown_debug)
        LOG_F(1, "Date=%g. End of on_monitoring_stage", date);

    // Now that the decisions have been made, let's compute the LLH again.
    llh = EasyBackfillingPlotLiquidLoadHorizon::compute_liquid_load_horizon(_inertial_schedule,
                                                                            _queue, date);
    if (_write_output_file)
    {
        int first_job_size = -1;
        const Job * first_job = _queue->first_job_or_nullptr();
        if (first_job != nullptr)
            first_job_size = first_job->nb_requested_resources;

        write_output_file(date, _queue->nb_jobs(),
                          first_job_size,
                          (double) _queue->compute_load_estimation(),
                          (double) llh);
    }

    machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                           + _machines_to_sedate - _machines_to_awaken;
    PPK_ASSERT_ERROR((int)machines_asleep_soon.size() == _nb_machines_sedated_by_inertia +
                                                         _nb_machines_sedated_for_being_idle,
                     "Asleep machines inconsistency. nb_asleep_soon=%d (%s). nb_sedated_inertia=%d. "
                     "nb_sedated_idle=%d\n",
                     (int)machines_asleep_soon.size(), machines_asleep_soon.to_string_brackets().c_str(),
                     _nb_machines_sedated_by_inertia, _nb_machines_sedated_for_being_idle);

    _last_llh_value = llh;
    _last_llh_date = date;
}

void EnergyBackfillingMonitoringInertialShutdown::select_machines_to_sedate(int nb_machines,
                                                                            const IntervalSet &sedatable_machines,
                                                                            const IntervalSet &machines_that_can_be_used_by_the_priority_job,
                                                                            IntervalSet &machines_to_sedate,
                                                                            const Job *priority_job) const
{
    PPK_ASSERT_ERROR(nb_machines <= (int)sedatable_machines.size());
    PPK_ASSERT_ERROR((sedatable_machines & (_awake_machines + _switching_on_machines)) == sedatable_machines);

    if (_sedating_policy == SEDATE_FIRST_MACHINES)
    {
        select_first_machines_to_sedate(nb_machines, _inertial_schedule, sedatable_machines,
                                        machines_that_can_be_used_by_the_priority_job, machines_to_sedate,
                                        priority_job);
    }
    else
        PPK_ASSERT_ERROR(false, "Unknown sedating policy");

    PPK_ASSERT_ERROR(machines_to_sedate.size() == (unsigned int)nb_machines);
    PPK_ASSERT_ERROR((machines_to_sedate & sedatable_machines) == machines_to_sedate);
}

void EnergyBackfillingMonitoringInertialShutdown::select_machines_to_awaken(int nb_machines,
                                                                            const IntervalSet &awakable_machines,
                                                                            IntervalSet &machines_to_awaken) const
{
    PPK_ASSERT_ERROR(nb_machines <= (int)awakable_machines.size());
    PPK_ASSERT_ERROR((awakable_machines & (_asleep_machines + _switching_off_machines)) == awakable_machines,
                     "awakable_machines should be a subset of union(_asleep_machines,_switching_off_machines).\n"
                     "awakable_machines=%s\n_asleep_machines=%s\n_switching_off_machines=%s\n"
                     "union(_asleep_machines,_switching_off_machines)=%s\n",
                     awakable_machines.to_string_brackets().c_str(),
                     _asleep_machines.to_string_brackets().c_str(),
                     _switching_off_machines.to_string_brackets().c_str(),
                     (_asleep_machines + _switching_off_machines).to_string_brackets().c_str());

    if (_awakening_policy == AWAKEN_FIRST_MACHINES)
        select_first_machines_to_awaken(nb_machines, _inertial_schedule, awakable_machines, machines_to_awaken);
    else
        PPK_ASSERT_ERROR(false, "Unknown awakening policy");

    PPK_ASSERT_ERROR(machines_to_awaken.size() == (unsigned int) nb_machines);
    PPK_ASSERT_ERROR((machines_to_awaken & awakable_machines) == machines_to_awaken);
}

void EnergyBackfillingMonitoringInertialShutdown::select_first_machines_to_sedate(int nb_machines, const Schedule & schedule,
                                                                                  const IntervalSet & sedatable_machines,
                                                                                  const IntervalSet & machines_that_can_be_used_by_the_priority_job,
                                                                                  IntervalSet &machines_to_sedate,
                                                                                  const Job * priority_job)
{
    machines_to_sedate.clear();

    // If there is a priority job, in order to make sure we don't delay it, we can compute how many machines we can "steal"
    // from the "hole" in which it is supposed to be executed
    IntervalSet stolen_machines;
    IntervalSet really_stolable_machines = sedatable_machines & machines_that_can_be_used_by_the_priority_job;
    int priority_job_nb_requested_resources = 0;

    if (priority_job != nullptr)
        priority_job_nb_requested_resources = priority_job->nb_requested_resources;

    int nb_machines_that_can_be_stolen_from_priority_job = max(0, (int)(really_stolable_machines.size() - priority_job_nb_requested_resources));

    auto slice_it = schedule.begin();
    while (slice_it != schedule.end())
    {
        const Schedule::TimeSlice & slice = *slice_it;
        IntervalSet sedatable_machines_now = (slice.available_machines & sedatable_machines) - machines_to_sedate;

        // The sedatable machines might annoy the priority job...
        IntervalSet annoying_sedatable_machines = (sedatable_machines_now & (really_stolable_machines - stolen_machines));
        if (annoying_sedatable_machines != IntervalSet::empty_interval_set())
        {
            // Let's see if we can "steal" some machines from the priority job.
            int nb_machines_stolable_now = nb_machines_that_can_be_stolen_from_priority_job - stolen_machines.size();
            if (nb_machines_stolable_now > 0)
            {
                IntervalSet stolable_machines = annoying_sedatable_machines.left(min(nb_machines_stolable_now, (int)annoying_sedatable_machines.size()));

                int nb_machines_i_want_to_steal = min(stolable_machines.size(), nb_machines - machines_to_sedate.size());
                IntervalSet stolen_machines_now = stolable_machines.left(nb_machines_i_want_to_steal);

                stolen_machines += stolen_machines_now;
                machines_to_sedate += stolen_machines_now;
            }
        }

        sedatable_machines_now -= annoying_sedatable_machines;

        if (sedatable_machines_now.size() + machines_to_sedate.size() >= (unsigned int)nb_machines)
        {
            machines_to_sedate += sedatable_machines_now.left(nb_machines - machines_to_sedate.size());
            return;
        }
        else
            machines_to_sedate += sedatable_machines_now;

        slice_it++;
    }

    PPK_ASSERT_ERROR(false, "Couldn't select %d machines to sedate :(.\n"
                     "sedatable_machines=%s.\n"
                     "priority_job=%p.\n"
                     "priority_job_nb_requested_resources=%d.\n"
                     "machines_that_can_be_used_by_the_priority_job=%s.\n%s\n",
                     nb_machines, sedatable_machines.to_string_brackets().c_str(),
                     priority_job, priority_job_nb_requested_resources,
                     machines_that_can_be_used_by_the_priority_job.to_string_brackets().c_str(),
                     schedule.to_string().c_str());
}

void EnergyBackfillingMonitoringInertialShutdown::select_first_machines_to_awaken(int nb_machines, const Schedule & schedule,
                                                                                  const IntervalSet &awakable_machines,
                                                                                  IntervalSet &machines_to_awaken)
{
    machines_to_awaken.clear();

    auto slice_it = schedule.begin();
    while (slice_it != schedule.end())
    {
        const Schedule::TimeSlice & slice = *slice_it;
        IntervalSet awakable_machines_now = (compute_potentially_awaken_machines(slice) & awakable_machines) - machines_to_awaken;

        if (awakable_machines_now.size() + machines_to_awaken.size() >= (unsigned int)nb_machines)
        {
            machines_to_awaken += awakable_machines_now.left(nb_machines - machines_to_awaken.size());
            return;
        }
        else
            machines_to_awaken += awakable_machines_now;

        slice_it++;
    }

    PPK_ASSERT_ERROR(false, "Couldn't select %d machines to awaken :(. %s\n", nb_machines, schedule.to_string().c_str());
}

void EnergyBackfillingMonitoringInertialShutdown::handle_queued_switches(Schedule & schedule,
                                                                         const IntervalSet & machines_to_sedate,
                                                                         const IntervalSet & machines_to_awaken,
                                                                         IntervalSet & machines_sedated_now,
                                                                         IntervalSet & machines_awakened_now)
{
    if (_inertial_shutdown_debug)
    {
        LOG_F(1, "handle_queued_switches, begin.\n"
               "machines_to_awaken: %s\nmachines_to_sedate: %s\n"
               "_awake_machines: %s\n_asleep_machines: %s\n"
               "_wakable_asleep_machines: %s\n"
               "_non_wakable_asleep_machines: %s\n%s",
               machines_to_awaken.to_string_brackets().c_str(),
               machines_to_sedate.to_string_brackets().c_str(),
               _awake_machines.to_string_brackets().c_str(),
               _asleep_machines.to_string_brackets().c_str(),
               _wakable_asleep_machines.to_string_brackets().c_str(),
               _non_wakable_asleep_machines.to_string_brackets().c_str(),
               _inertial_schedule.to_string().c_str());
        write_schedule_debug("_handle_queued_switches_begin");
    }

    // Let's sedate and awaken the desired machines into the _inertial_schedule
    for (auto machine_it = machines_to_awaken.elements_begin(); machine_it != machines_to_awaken.elements_end(); machine_it++)
    {
        int machine_id = *machine_it;
        Rational awakening_date = awaken_machine_as_soon_as_possible(schedule, machine_id);

        if (awakening_date == schedule.first_slice_begin())
            machines_awakened_now += machine_id;
    }

    if (_inertial_shutdown_debug)
    {
        LOG_F(1, "handle_queued_switches, after awakenings.\n"
               "machines_to_sedate: %s\n%s",
               machines_to_sedate.to_string_brackets().c_str(),
               _inertial_schedule.to_string().c_str());
        write_schedule_debug("_handle_queued_switches_after_awakenings");
    }

    for (auto machine_it = machines_to_sedate.elements_begin(); machine_it != machines_to_sedate.elements_end(); machine_it++)
    {
        int machine_id = *machine_it;

        Rational sedating_date = sedate_machines_at_the_furthest_moment(schedule, machine_id);

        if (sedating_date == schedule.first_slice_begin())
            machines_sedated_now += machine_id;
    }

    if (_inertial_shutdown_debug)
    {
        LOG_F(1, "handle_queued_switches, end.\n"
               "machines_sedated_now = %s\n"
               "machines_awakened_now = %s\n%s",
               machines_sedated_now.to_string_brackets().c_str(),
               machines_awakened_now.to_string_brackets().c_str(),
               _inertial_schedule.to_string().c_str());
        write_schedule_debug("_handle_queued_switches_end");
    }

}

void EnergyBackfillingMonitoringInertialShutdown::write_output_file(double date,
                                                                    int nb_jobs_in_queue,
                                                                    int first_job_size,
                                                                    double load_in_queue,
                                                                    double liquid_load_horizon)
{
    PPK_ASSERT_ERROR(_output_file.is_open());

    const int buf_size = 256;
    int nb_printed;
    char * buf = (char*) malloc(sizeof(char) * buf_size);

    if (first_job_size != -1)
        nb_printed = snprintf(buf, buf_size, "%g,%d,%d,%g,%g,%g\n",
                              date, nb_jobs_in_queue, first_job_size,
                              (double)_priority_job_starting_time_expectancy - date,
                              load_in_queue, liquid_load_horizon);
    else
        nb_printed = snprintf(buf, buf_size, "%g,%d,NA,NA,%g,%g\n",
                              date, nb_jobs_in_queue,
                              load_in_queue, liquid_load_horizon);
    PPK_ASSERT_ERROR(nb_printed < buf_size - 1,
                     "Buffer too small, some information might have been lost");

    _output_file.write(buf, strlen(buf));

    free(buf);
}

void EnergyBackfillingMonitoringInertialShutdown::write_schedule_debug(const string &filename_suffix)
{
    if (_really_write_svg_files)
    {
        char output_filename[256];
        snprintf(output_filename, 256, "%s/inertial_schedule_%06d%s.svg",
                 _output_dir.c_str(), _debug_output_id, filename_suffix.c_str());

        _inertial_schedule.write_svg_to_file(output_filename);
    }

    ++_debug_output_id;
}

void EnergyBackfillingMonitoringInertialShutdown::compute_priority_job_and_related_stuff(Schedule &schedule,
                                                                                         const Queue * queue,
                                                                                         const Job *&priority_job,
                                                                                         ResourceSelector * priority_job_selector,
                                                                                         bool & priority_job_needs_awakenings,
                                                                                         Schedule::JobAlloc &first_insertion_alloc,
                                                                                         IntervalSet &priority_job_reserved_machines,
                                                                                         IntervalSet &machines_that_can_be_used_by_the_priority_job)
{
    priority_job = nullptr;
    first_insertion_alloc.has_been_inserted = false;
    priority_job_reserved_machines.clear();
    machines_that_can_be_used_by_the_priority_job.clear();

    // Let's find in which time space the priority job should be executed
    if (!queue->is_empty())
    {
        priority_job = queue->first_job();
        // To do so, let's insert the priority job into the schedule.

        if (schedule.contains_job(priority_job))
            schedule.remove_job(priority_job);

        first_insertion_alloc = schedule.add_job_first_fit(priority_job, priority_job_selector, false);

        if (!first_insertion_alloc.has_been_inserted)
        {
            // The priority job has not been inserted. This is probably because some machines must be awakened first.
            priority_job_needs_awakenings = true;
        }
        else
        {
            priority_job_reserved_machines = first_insertion_alloc.used_machines;

            // Now we want to determine which machines the priority job can use at this period of time.
            // To do so, let's remove it from the schedule then compute all available machines during
            // this period of time.
            PPK_ASSERT_ERROR(schedule.contains_job(priority_job));
            schedule.remove_job(priority_job);
            PPK_ASSERT_ERROR(!schedule.contains_job(priority_job));

            machines_that_can_be_used_by_the_priority_job = schedule.available_machines_during_period(first_insertion_alloc.begin, first_insertion_alloc.end);
            // Coherency checks: the previous allocation should be a subset of these machines
            PPK_ASSERT_ERROR(((first_insertion_alloc.used_machines & machines_that_can_be_used_by_the_priority_job) == first_insertion_alloc.used_machines) &&
                             ((first_insertion_alloc.used_machines + machines_that_can_be_used_by_the_priority_job) == machines_that_can_be_used_by_the_priority_job),
                             "The priority job '%s' has been allocated in an invalid place. "
                             "It should have been among %s, but it is in %s.\n%s",
                             priority_job->id.c_str(), machines_that_can_be_used_by_the_priority_job.to_string_brackets().c_str(),
                             first_insertion_alloc.used_machines.to_string_brackets().c_str(),
                             schedule.to_string().c_str());
        }
    }
}

Rational EnergyBackfillingMonitoringInertialShutdown::compute_priority_job_starting_time_expectancy(const Schedule &schedule,
                                                                                                    const Job *priority_job)
{
    if (priority_job == nullptr)
        return -1;

    Schedule copy = schedule;

    // Let's remove the job from the schedule if it exists
    copy.remove_job_if_exists(priority_job);

    // Let's remove every sleeping machine from it
    IntervalSet machines_asleep_soon = (_asleep_machines + _switching_off_machines - _switching_on_machines)
                                        + _machines_to_sedate - _machines_to_awaken;

    for (auto mit = machines_asleep_soon.elements_begin(); mit != machines_asleep_soon.elements_end(); ++mit)
    {
        int machine_id = *mit;
        EnergyBackfilling::awaken_machine_as_soon_as_possible(copy, machine_id);
    }

    Schedule::JobAlloc alloc = copy.add_job_first_fit(priority_job, _selector, true);

    PPK_ASSERT_ERROR(alloc.has_been_inserted);
    return alloc.begin;
}
