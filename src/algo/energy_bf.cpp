#include "energy_bf.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EnergyBackfilling::MachineInformation::MachineInformation(int machine_id) :
    machine_number(machine_id)
{
    PPK_ASSERT_ERROR(machine_id >= 0);
    create_selector();
}

EnergyBackfilling::MachineInformation::~MachineInformation()
{
    free_jobs();
    free_selector();
}

void EnergyBackfilling::MachineInformation::create_jobs(double rjms_delay,
                                                        Rational ensured_sleep_time_lower_bound,
                                                        Rational ensured_sleep_time_upper_bound)
{
    PPK_ASSERT_ERROR(rjms_delay >= 0);
    PPK_ASSERT_ERROR(ensured_sleep_time_lower_bound <= ensured_sleep_time_upper_bound);
    PPK_ASSERT_ERROR(switch_on_job == nullptr);
    PPK_ASSERT_ERROR(switch_off_job == nullptr);
    PPK_ASSERT_ERROR(ensured_sleep_job == nullptr);
    PPK_ASSERT_ERROR(potential_sleep_job == nullptr);
    PPK_ASSERT_ERROR(machine_number > -1);
    PPK_ASSERT_ERROR(switch_on_seconds > 0);
    PPK_ASSERT_ERROR(switch_off_seconds > 0);
    PPK_ASSERT_ERROR(switch_on_energy > 0);
    PPK_ASSERT_ERROR(switch_off_energy > 0);
    PPK_ASSERT_ERROR(idle_epower > 0);
    PPK_ASSERT_ERROR(sleep_epower >= 0);
    PPK_ASSERT_ERROR(idle_epower > sleep_epower);

    switch_on_job = new Job;
    switch_on_job->id = "fakejob_son_" + to_string(machine_number);
    switch_on_job->nb_requested_resources = 1;
    switch_on_job->submission_time = 0;
    switch_on_job->walltime = (double) switch_on_seconds + rjms_delay;

    switch_off_job = new Job;
    switch_off_job->id = "fakejob_soff_" + to_string(machine_number);
    switch_off_job->nb_requested_resources = 1;
    switch_off_job->submission_time = 0;
    switch_off_job->walltime = (double) switch_off_seconds + rjms_delay;

    /* Let us determine the minimum time that should be waited to avoid pure loss of energy via switchON + switchOFF.
     * When a machine is switched OFF then ON, the time can be split in 3 parts :
     *   - a, the switching OFF part. In this part, the power is pOFF
     *   - s, the asleep part. In this part, the power is pSLEEP
     *   - b, the switching ON part. In this part, the power is pON
     *
     * pIDLE is the power used by the machine in idle state.
     * We want to ensure that the switch OFF then ON does not take more energy than staying in idle state:
     *   a * pOFF + s * pSLEEP + b * pON <= (a+s+b) * pIDLE
     *   (a * pOFF) + (s * pSLEEP) + (b * pON) <= ((a+b) * pIDLE) + (s * pIDLE)
     *   (s * pSLEEP) - (s * pIDLE) <= ((a+b) * pIDLE) - (a * pOFF) - (b * pON)
     *   s * (pSLEEP - pIDLE) <= ((a+b) * pIDLE) - (a * pOFF) - (b * pON)
     *   s >= (((a+b) * pIDLE) - (a * pOFF) - (b * pON)) / (pSLEEP - pIDLE) because (pSLEEP - pIDLE) is negative <=> pIDLE > pSLEEP
     */

    Rational minimum_sleeping_time = ((switch_off_seconds + switch_on_seconds + 2 * rjms_delay) * idle_epower - switch_off_energy - switch_on_energy) / (sleep_epower - idle_epower);

    minimum_sleeping_time = min(max(minimum_sleeping_time,
                                    ensured_sleep_time_lower_bound),
                                ensured_sleep_time_upper_bound);

    ensured_sleep_job = new Job;
    ensured_sleep_job->id = "fakejob_esleep_" + to_string(machine_number);
    ensured_sleep_job->nb_requested_resources = 1;
    ensured_sleep_job->submission_time = 0;
    ensured_sleep_job->walltime = (double) minimum_sleeping_time;

    potential_sleep_job = new Job;
    potential_sleep_job->id = "fakejob_psleep_" + to_string(machine_number);
    potential_sleep_job->nb_requested_resources = 1;
    potential_sleep_job->submission_time = 0;
    potential_sleep_job->walltime = std::numeric_limits<double>::max();
}

void EnergyBackfilling::MachineInformation::free_jobs()
{
    if (switch_on_job)
    {
        delete switch_on_job;
        switch_on_job = nullptr;
    }

    if (switch_off_job)
    {
        delete switch_off_job;
        switch_off_job = nullptr;
    }

    if (ensured_sleep_job)
    {
        delete ensured_sleep_job;
        ensured_sleep_job = nullptr;
    }

    if (potential_sleep_job)
    {
        delete potential_sleep_job;
        potential_sleep_job = nullptr;
    }
}

void EnergyBackfilling::MachineInformation::create_selector()
{
    PPK_ASSERT_ERROR(limited_resource_selector  == nullptr);
    PPK_ASSERT_ERROR(machine_number >= 0);

    limited_resource_selector  = new LimitedRangeResourceSelector(IntervalSet(machine_number));
}

void EnergyBackfilling::MachineInformation::free_selector()
{
    if (limited_resource_selector != nullptr)
    {
        delete limited_resource_selector;
        limited_resource_selector = nullptr;
    }
}



string EnergyBackfilling::machine_state_to_string(const EnergyBackfilling::MachineState &state)
{
    switch (state)
    {
    case AWAKE: return "awake";
    case ASLEEP: return "asleep";
    case SWITCHING_OFF: return "switching_off";
    case SWITCHING_ON: return "switching_on";
    }

    PPK_ASSERT_ERROR(false, "Unhandled case");
    return "unhandled_case";
}



EnergyBackfilling::EnergyBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                                     double rjms_delay, rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
}

EnergyBackfilling::~EnergyBackfilling()
{
    clear_machine_informations();
}

void EnergyBackfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    _schedule = Schedule(_nb_machines, date);
    (void) batsim_config;

    generate_machine_informations(_nb_machines);

    _all_machines = IntervalSet::ClosedInterval(0, _nb_machines - 1);
    PPK_ASSERT_ERROR((int)(_all_machines.size()) == _nb_machines);

    _awake_machines.insert(_all_machines);
}

void EnergyBackfilling::on_simulation_end(double date)
{
    (void) date;
    // TODO: do something about this?
}

void EnergyBackfilling::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
    if (_debug)
    {
        LOG_F(1, "on_machine_state_changed beginning, schedule : %s", _schedule.to_string().c_str());
    }

    // Let's update the current schedule to take the machine state change into account
    IntervalSet switched_off_machines;

    // Let's remove all related switch jobs from the online schedule
    for (auto machine_it = machines.elements_begin(); machine_it != machines.elements_end(); ++machine_it)
    {
        int machine_id = *machine_it;
        MachineInformation * machine_info = _machine_informations.at(machine_id);

        PPK_ASSERT_ERROR(new_state == machine_info->compute_pstate ||
                         new_state == machine_info->sleep_pstate,
                         "Machine %d just changed its pstate to %d, "
                         "but this pstate is unknown. compute_pstate=%d, "
                         "sleep_pstate=%d.", machine_id, new_state,
                         machine_info->compute_pstate,
                         machine_info->sleep_pstate);

        if (new_state == machine_info->compute_pstate)
        {
            // A Switch ON has been acknowledged. Let's remove the switch job from the schedule
            bool switch_on_job_removed = _schedule.remove_job_if_exists(machine_info->switch_on_job);
            PPK_ASSERT_ERROR(switch_on_job_removed,
                             "Machine %d has just been switched ON, but there was "
                             "no switch ON job for it in the schedule...\n%s",
                             machine_id, _schedule.to_string().c_str());

            // Update machine state
            PPK_ASSERT_ERROR(_switching_on_machines.contains(machine_id),
                             "Machine %d has just been switched ON, but the "
                             "machine was not marked as a switching ON one. "
                             "switching_on_machines=%s",
                             machine_id,
                             _switching_on_machines.to_string_brackets().c_str());
            _switching_on_machines.remove(machine_id);

            PPK_ASSERT_ERROR(!_awake_machines.contains(machine_id),
                             "Machine %d has just been switched ON, but the "
                             "machine was already marked as an awake machine... "
                             "awake_machines=%s",
                             machine_id,
                             _awake_machines.to_string_brackets().c_str());
            _awake_machines.insert(machine_id);
        }
        else
        {
            // A Switch OFF has been acknowledged. Let's remove the switch job from the schedule
            bool switch_off_job_removed = _schedule.remove_job_if_exists(machine_info->switch_off_job);
            PPK_ASSERT_ERROR(switch_off_job_removed,
                             "Machine %d has just been switched OFF, but there was "
                             "no switch OFF job for it in the schedule...\n%s",
                             machine_id, _schedule.to_string().c_str());

            // Update machine state
            PPK_ASSERT_ERROR(_switching_off_machines.contains(machine_id),
                             "Machine %d has just been switched OFF, but the "
                             "machine was not marked as a switching OFF one. "
                             "switching_off_machines=%s",
                             machine_id,
                             _switching_off_machines.to_string_brackets().c_str());
            _switching_off_machines.remove(machine_id);

            PPK_ASSERT_ERROR(!_asleep_machines.contains(machine_id),
                             "Machine %d has just been switched OFF, but the "
                             "machine was already marked as an asleep one... "
                             "asleep_machines=%s",
                             machine_id, _asleep_machines.to_string_brackets().c_str());
            _asleep_machines.insert(machine_id);

            if (_schedule.contains_job(machine_info->ensured_sleep_job))
                _non_wakable_asleep_machines.insert(machine_id);
            else
            {
                PPK_ASSERT_ERROR(_schedule.contains_job(machine_info->potential_sleep_job),
                                 "Machine %d has just been switched OFF, but there "
                                 "was no ensured nor potential sleep job in the schedule "
                                 "for this machine.\n%s", machine_id,
                                 _schedule.to_string().c_str());
                _wakable_asleep_machines.insert(machine_id);
            }

            // Let's mark that the sleep jobs of this machine should be translated to the left
            switched_off_machines.insert(machine_id);
        }
    }

    if (_debug)
    {
        LOG_F(1, "on_machine_state_changed, before sleep jobs translation. %s", _schedule.to_string().c_str());
    }

    // Let's translate to the left (-> present) the sleep jobs that comes from newly asleep machines
    for (auto machine_it = switched_off_machines.elements_begin(); machine_it != switched_off_machines.elements_end(); ++machine_it)
    {
        int machine_id = *machine_it;
        MachineInformation * machine_info = _machine_informations.at(machine_id);

        // Let's remove previously existing sleep jobs
        _schedule.remove_job_if_exists(machine_info->ensured_sleep_job);

        bool removed_potential_sleep = _schedule.remove_job_if_exists(machine_info->potential_sleep_job);
        PPK_ASSERT_ERROR(removed_potential_sleep,
                         "Machine %d has just been switched OFF, but there was no "
                         "potential sleep job in the schedule for this machine.\n%s",
                         machine_id, _schedule.to_string().c_str());

        // Let's insert them back in the schedule at the right place
        sedate_machine_without_switch(_schedule, machine_id, date);
    }

    if (_debug)
    {
        LOG_F(1, "on_machine_state_changed before update_first_slice, schedule : %s", _schedule.to_string().c_str());
    }
}

void EnergyBackfilling::on_requested_call(double date)
{
    (void) date;
    PPK_ASSERT_ERROR(_nb_call_me_later_running > 0,
                     "Received a REQUESTED_CALL message from Batsim while there "
                     "was no running call_me_later request.");
    _nb_call_me_later_running--;
}

void EnergyBackfilling::generate_machine_informations(int nb_machines)
{
    PPK_ASSERT_ERROR(nb_machines > 0);

    // Let's parse the options file
    PPK_ASSERT_ERROR(_variant_options->HasMember("power_compute"), "Invalid options JSON object: Member 'power_compute' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["power_compute"].IsNumber(), "Invalid options JSON object: Member 'power_compute' must be a number");
    Rational power_compute = (*_variant_options)["power_compute"].GetDouble();
    PPK_ASSERT_ERROR(power_compute > 0, "Invalid options JSON object: Member 'power_compute' must be strictly positive (got %g)", (double)power_compute);

    PPK_ASSERT_ERROR(_variant_options->HasMember("power_idle"), "Invalid options JSON object: Member 'power_idle' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["power_idle"].IsNumber(), "Invalid options JSON object: Member 'power_idle' must be a number");
    Rational power_idle = (*_variant_options)["power_idle"].GetDouble();
    PPK_ASSERT_ERROR(power_idle > 0, "Invalid options JSON object: Member 'power_idle' must be strictly positive (got %g)", (double)power_idle);

    PPK_ASSERT_ERROR(_variant_options->HasMember("power_sleep"), "Invalid options JSON object: Member 'power_sleep' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["power_sleep"].IsNumber(), "Invalid options JSON object: Member 'power_sleep' must be a number");
    Rational power_sleep = (*_variant_options)["power_sleep"].GetDouble();
    PPK_ASSERT_ERROR(power_sleep > 0, "Invalid options JSON object: Member 'power_sleep' must be strictly positive (got %g)", (double)power_sleep);


    PPK_ASSERT_ERROR(_variant_options->HasMember("pstate_compute"), "Invalid options JSON object: Member 'pstate_compute' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["pstate_compute"].IsInt(), "Invalid options JSON object: Member 'pstate_compute' must be integral");
    int pstate_compute = (*_variant_options)["pstate_compute"].GetInt();
    PPK_ASSERT_ERROR(pstate_compute >= 0, "Invalid options JSON object: Member 'pstate_compute' value must be positive (got %d)", pstate_compute);

    PPK_ASSERT_ERROR(_variant_options->HasMember("pstate_sleep"), "Invalid options JSON object: Member 'pstate_sleep' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["pstate_sleep"].IsInt(), "Invalid options JSON object: Member 'pstate_sleep' must be integral");
    int pstate_sleep = (*_variant_options)["pstate_sleep"].GetInt();
    PPK_ASSERT_ERROR(pstate_sleep >= 0, "Invalid options JSON object: Member 'pstate_sleep' value must be positive (got %d)", pstate_sleep);


    PPK_ASSERT_ERROR(_variant_options->HasMember("time_switch_on"), "Invalid options JSON object: Member 'time_switch_on' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["time_switch_on"].IsNumber(), "Invalid options JSON object: Member 'time_switch_on' must be a number");
    Rational time_switch_on = (*_variant_options)["time_switch_on"].GetDouble();
    PPK_ASSERT_ERROR(time_switch_on > 0, "Invalid options JSON object: Member 'time_switch_on' value must be strictly positive (got %g)", (double)time_switch_on);

    PPK_ASSERT_ERROR(_variant_options->HasMember("time_switch_off"), "Invalid options JSON object: Member 'time_switch_off' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["time_switch_off"].IsNumber(), "Invalid options JSON object: Member 'time_switch_off' must be a number");
    Rational time_switch_off = (*_variant_options)["time_switch_off"].GetDouble();
    PPK_ASSERT_ERROR(time_switch_off > 0, "Invalid options JSON object: Member 'time_switch_off' value must be strictly positive (got %g)", (double)time_switch_off);


    PPK_ASSERT_ERROR(_variant_options->HasMember("energy_switch_on"), "Invalid options JSON object: Member 'energy_switch_on' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["energy_switch_on"].IsNumber(), "Invalid options JSON object: Member 'energy_switch_on' must be a number");
    Rational energy_switch_on = (*_variant_options)["energy_switch_on"].GetDouble();
    PPK_ASSERT_ERROR(energy_switch_on > 0, "Invalid options JSON object: Member 'energy_switch_on' value must be strictly positive (got %g)", (double)energy_switch_on);

    PPK_ASSERT_ERROR(_variant_options->HasMember("energy_switch_off"), "Invalid options JSON object: Member 'energy_switch_off' cannot be found");
    PPK_ASSERT_ERROR((*_variant_options)["energy_switch_off"].IsNumber(), "Invalid options JSON object: Member 'energy_switch_off' must be a number");
    Rational energy_switch_off = (*_variant_options)["energy_switch_off"].GetDouble();
    PPK_ASSERT_ERROR(energy_switch_off > 0, "Invalid options JSON object: Member 'energy_switch_off' value must be strictly positive (got %g)", (double)energy_switch_off);


    Rational ensured_sleep_time_lower_bound = 0;
    if (_variant_options->HasMember("ensured_sleep_time_lower_bound"))
    {
        PPK_ASSERT_ERROR((*_variant_options)["ensured_sleep_time_lower_bound"].IsNumber(),
                "Invalid options JSON object: Member 'ensured_sleep_time_lower_bound' is not a number");
        ensured_sleep_time_lower_bound = (*_variant_options)["ensured_sleep_time_lower_bound"].GetDouble();
        PPK_ASSERT_ERROR(ensured_sleep_time_lower_bound >= 0,
                         "Invalid options JSON object: Member 'ensured_sleep_time_lower_bound' must "
                         "be positive (got %g)", (double) ensured_sleep_time_lower_bound);
    }

    Rational ensured_sleep_time_upper_bound = 1e8;
    if (_variant_options->HasMember("ensured_sleep_time_upper_bound"))
    {
        PPK_ASSERT_ERROR((*_variant_options)["ensured_sleep_time_upper_bound"].IsNumber(),
                "Invalid options JSON object: Member 'ensured_sleep_time_upper_bound' is not a number");
        ensured_sleep_time_upper_bound = (*_variant_options)["ensured_sleep_time_upper_bound"].GetDouble();
        PPK_ASSERT_ERROR(ensured_sleep_time_upper_bound >= 0,
                         "Invalid options JSON object: Member 'ensured_sleep_time_upper_bound' must "
                         "be positive (got %g)", (double) ensured_sleep_time_upper_bound);
    }

    PPK_ASSERT_ERROR(ensured_sleep_time_lower_bound <= ensured_sleep_time_upper_bound,
                     "Invalid options JSON object: ensured_sleep_time_lower_bound (%g) must be "
                     "lower than or equal to ensured_sleep_time_upper_bound (%g), which is not "
                     "the case here.", (double) ensured_sleep_time_lower_bound,
                     (double) ensured_sleep_time_upper_bound);

    for (int i = 0; i < nb_machines; ++i)
    {
        MachineInformation * minfo = new MachineInformation(i);

        minfo->compute_pstate = pstate_compute;
        minfo->sleep_pstate = pstate_sleep;
        minfo->compute_epower = power_compute;
        minfo->idle_epower = power_idle;
        minfo->sleep_epower = power_sleep;
        minfo->switch_on_energy = energy_switch_on;
        minfo->switch_off_energy = energy_switch_off;
        minfo->switch_on_seconds = time_switch_on;
        minfo->switch_off_seconds = time_switch_off;
        minfo->switch_on_electrical_power = energy_switch_on / time_switch_on;
        minfo->switch_off_electrical_power = energy_switch_off / time_switch_off;

        minfo->create_jobs(_rjms_delay, ensured_sleep_time_lower_bound,
                           ensured_sleep_time_upper_bound);

        _machine_informations[minfo->machine_number] = minfo;
    }

    LOG_F(INFO, "Ensured sleep length of the first machine : %g seconds.",
           (double) _machine_informations[0]->ensured_sleep_job->walltime);
}

void EnergyBackfilling::clear_machine_informations()
{
    for (auto mit : _machine_informations)
    {
        MachineInformation * minfo = mit.second;
        delete minfo;
    }

    _machine_informations.clear();
}

void EnergyBackfilling::make_decisions(double date,
                                       SortableJobOrder::UpdateInformation * update_info,
                                       SortableJobOrder::CompareInformation * compare_info)
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

    Schedule current_schedule = _schedule;

    if (_debug)
    {
        LOG_F(1, "Schedule before put_jobs_into_schedule: %s", current_schedule.to_string().c_str());
    }

    put_jobs_into_schedule(current_schedule);

    if (_debug)
    {
        LOG_F(1, "Schedule before sedate_machines_at_the_furthest_moment: %s", current_schedule.to_string().c_str());
    }

    sedate_machines_at_the_furthest_moment(current_schedule, _awake_machines);

    make_decisions_of_schedule(current_schedule);
}

void EnergyBackfilling::make_decisions_of_schedule(const Schedule &schedule,
                                                   bool run_call_me_later_on_nothing_to_do)
{
    bool did_something = false;

    PPK_ASSERT_ERROR(schedule.nb_slices() > 0);
    const Schedule::TimeSlice & slice = *schedule.begin();
    PPK_ASSERT_ERROR(slice.begin == _schedule.first_slice_begin());

    map<int, IntervalSet> state_switches_to_do;

    for (auto mit : slice.allocated_jobs)
    {
        const Job * job = mit.first;
        const IntervalSet & job_machines = mit.second;

        // If the job is a fake one
        if (is_fake_job(job->id))
        {
            PPK_ASSERT_ERROR(job_machines.size() == 1);
            MachineInformation * machine_info = _machine_informations.at(job_machines.first_element());
            int machine_id = machine_info->machine_number;

            if (is_switch_on_job(job->id))
            {
                // If the switch ON is new
                if (!_schedule.contains_job(job))
                {
                    // Let's remove the sleep jobs from the online schedule
                    bool ensured_removed = _schedule.remove_job_if_exists(machine_info->ensured_sleep_job);
                    bool potential_removed = _schedule.remove_job_if_exists(machine_info->potential_sleep_job);
                    PPK_ASSERT_ERROR(ensured_removed || potential_removed);

                    // Let's insert the switch ON into the online schedule
                    Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, machine_info->limited_resource_selector);
                    PPK_ASSERT_ERROR(alloc.begin == slice.begin);

                    // Let's register that this machine should be switched ON now
                    state_switches_to_do[machine_info->compute_pstate].insert(machine_info->machine_number);

                    // Let's update machine states
                    PPK_ASSERT_ERROR(_asleep_machines.contains(machine_id));
                    _asleep_machines.remove(machine_id);

                    PPK_ASSERT_ERROR(_wakable_asleep_machines.contains(machine_id));
                    _wakable_asleep_machines.remove(machine_id);

                    PPK_ASSERT_ERROR(!_switching_on_machines.contains(machine_id));
                    _switching_on_machines.insert(machine_info->machine_number);
                }
            }
            else if (is_switch_off_job(job->id))
            {
                // If the switch OFF is new
                if (!_schedule.contains_job(job))
                {
                    // Let's sedate the machine into the online schedule
                    sedate_machine(_schedule, machine_info->machine_number, _schedule.begin());

                    // Let's register that this machine should be switched OFF now
                    state_switches_to_do[machine_info->sleep_pstate].insert(machine_info->machine_number);

                    // Let's update machine states
                    PPK_ASSERT_ERROR(_awake_machines.contains(machine_id));
                    _awake_machines.remove(machine_id);

                    PPK_ASSERT_ERROR(!_switching_off_machines.contains(machine_id));
                    _switching_off_machines.insert(machine_info->machine_number);
                }
            }
        }
        else // The job is a real one
        {
            // If the job is a new one
            if (_queue->contains_job(job))
            {
                // Let's append it to the online schedule
                LimitedRangeResourceSelector selector(job_machines);
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, &selector);
                PPK_ASSERT_ERROR(alloc.started_in_first_slice);
                PPK_ASSERT_ERROR(alloc.begin == slice.begin);

                // Let's tell the RJMS this job should be executed now
                _decision->add_execute_job(job->id, job_machines, (double)alloc.begin);
                did_something = true;

                // Let's remove it from the queue
                _queue->remove_job(job);
            }
        }
    }

    // Let's make state switch decisions now
    for (auto mit : state_switches_to_do)
    {
        int target_pstate = mit.first;
        const IntervalSet & machines = mit.second;

        _decision->add_set_resource_state(machines, target_pstate, (double) slice.begin);
        did_something = true;
    }

    if (run_call_me_later_on_nothing_to_do)
    {
        if (!did_something && (_nb_jobs_completed < _workload->nb_jobs()) && (_nb_call_me_later_running == 0))
        {
            // To avoid Batsim's deadlock, we should tell it to wait that we are ready.

            PPK_ASSERT_ERROR(_schedule.nb_slices() >= 1);
            _decision->add_call_me_later((double) _schedule.begin()->end + 1, (double) _schedule.begin()->begin);
            _nb_call_me_later_running++;
        }
    }
}

void EnergyBackfilling::update_first_slice_taking_sleep_jobs_into_account(Rational date)
{
    PPK_ASSERT_ERROR(_schedule.nb_slices() > 0);

    if (_debug)
        LOG_F(1, "update_first_slice... Date=%f\n%s",
               (double) date, _schedule.to_string().c_str());

    // Since we are not only using "real" jobs, the usual assumption that a slice cannot
    // end prematurely (because of walltimes being greater than execution times) does not stand.

    // We are sure that "Switch ON" and "Switch OFF" jobs behave the same that usual walltime jobs.
    // However this is not the case for the two sleeping jobs (ensured and potential).

    // Ensured sleeping jobs are used to ensure that a machine is not awaken too soon, which
    // would cause a pure loss of energy (w.r.t. letting the machine idle). Hence, those jobs
    // only exists in the scheduler view and Batsim won't tell when these jobs are finished.
    // Thus, finition of those jobs must be detected by the scheduler.

    // Potential sleeping jobs are used to make sure an asleep machine is not used for
    // computing jobs. These jobs have an infinite length and their termination should
    // not be a problem.


    // Hence, the update is valid if:
    //  - No slice ended prematurely
    //  - If slices ended prematurely, they were only related to ensured->potential sleep jobs.

    auto slice_it = _schedule.begin();
    bool slices_have_been_ended = false;

    // Complex tests will only be done if some slices ended prematurely
    if (date >= slice_it->end)
    {
        slices_have_been_ended = true;

        // Let's make sure all jobs but ensured-sleep related ones remain in the same
        // state in the scheduling, from the first slice to the current date
        IntervalSet sleeping_machines;
        set<string> non_sleep_jobs;

        const Schedule::TimeSlice & first_slice = *slice_it;
        for (const auto & mit : first_slice.allocated_jobs)
        {
            const Job * job = mit.first;
            if (!is_ensured_sleep_job(job->id) && !is_potential_sleep_job(job->id))
                non_sleep_jobs.insert(job->id);
            else
                sleeping_machines.insert(mit.second);
        }

        while (slice_it->end <= date)
        {
            set<string> non_sleep_jobs_in_slice;
            IntervalSet sleeping_machines_in_slice;
            const Schedule::TimeSlice & slice = *slice_it;

            // Let's check that aside from ensured sleep jobs, the jobs
            // are the same in the involved slices (those ended + the merge slice)
            for (const auto & mit : slice.allocated_jobs)
            {
                const Job * job = mit.first;
                if (!is_ensured_sleep_job(job->id) && !is_potential_sleep_job(job->id))
                {
                    PPK_ASSERT_ERROR(non_sleep_jobs.find(job->id) != non_sleep_jobs.end(),
                                     "A time slice ended prematurely, which is only "
                                     "allowed if the ended slices only exist because of "
                                     "ensured sleep jobs, which is not the case here, "
                                     "since job '%s' is not in all involved slices. Date=%g. %s",
                                     job->id.c_str(),
                                     (double) date,
                                     _schedule.to_string().c_str());
                    non_sleep_jobs_in_slice.insert(job->id);
                }
                else
                    sleeping_machines_in_slice.insert(mit.second);

            }

            PPK_ASSERT_ERROR(non_sleep_jobs_in_slice == non_sleep_jobs,
                             "A time slice ended prematurely, which is only allowed "
                             "if the ended slices only exist because of ensured->potential "
                             "sleep jobs, which is not the case here, because other jobs "
                             "appeared/vanished in slice '%s'. Date=%g.\n"
                             "Non sleep jobs in first slice: %s\n"
                             "Non sleep jobs in current slice: %s\n%s",
                             slice.to_string().c_str(),
                             (double) date,
                             boost::algorithm::join(non_sleep_jobs, ",").c_str(),
                             boost::algorithm::join(non_sleep_jobs_in_slice, ",").c_str(),
                             _schedule.to_string().c_str());

            PPK_ASSERT_ERROR(sleeping_machines_in_slice == sleeping_machines,
                             "A time slice ended prematurely, which is only allowed "
                             "if the ended slices only exist because of ensured->potential "
                             "sleep jobs, which is not the case here, since the sleeping "
                             "machines are not the same in the first slice (%s) and in"
                             "the current one (%s).",
                             sleeping_machines.to_string_brackets().c_str(),
                             sleeping_machines_in_slice.to_string_brackets().c_str());

            if (_debug)
            {
                LOG_F(1, "The slice will be removed because everything seems fine.\n"
                       "sleeping_machines = %s\nnon_sleep_jobs=%s\n%s",
                       sleeping_machines.to_string_brackets().c_str(),
                       boost::algorithm::join(non_sleep_jobs, ",").c_str(),
                       slice.to_string().c_str());
            }

            ++slice_it;
        }
    }

    // The schedule seems to be fine.
    _schedule.update_first_slice_removing_remaining_jobs(date);

    if (slices_have_been_ended)
    {
        // Let's update whether machines are wakable.
        slice_it = _schedule.begin();

        IntervalSet wakable_machines_now, non_wakable_machines_now;

        for (const auto mit : slice_it->allocated_jobs)
        {
            const Job * job = mit.first;
            const IntervalSet & alloc = mit.second;
            if (is_ensured_sleep_job(job->id))
                non_wakable_machines_now.insert(alloc);
            else if (is_potential_sleep_job(job->id))
                wakable_machines_now.insert(alloc);
        }

        // Let's make sure these machines are valid.
        PPK_ASSERT_ERROR((wakable_machines_now & non_wakable_machines_now) == IntervalSet::empty_interval_set(),
                         "Invalid schedule update: the new wakable and non-wakable machines are not"
                         "distinct. New wakable: %s. New non wakable: %s.",
                         wakable_machines_now.to_string_brackets().c_str(),
                         non_wakable_machines_now.to_string_brackets().c_str());
        PPK_ASSERT_ERROR((wakable_machines_now + non_wakable_machines_now) == _asleep_machines,
                         "Invalid schedule update: the asleep machines have changed. "
                         "Asleep machines before: %s. Asleep machines now: %s.",
                         _asleep_machines.to_string_brackets().c_str(),
                         (wakable_machines_now + non_wakable_machines_now).to_string_brackets().c_str());

        _wakable_asleep_machines = wakable_machines_now;
        _non_wakable_asleep_machines = non_wakable_machines_now;
    }

}

void EnergyBackfilling::put_jobs_into_schedule(Schedule &schedule) const
{
    Rational initial_infinite_horizon = schedule.infinite_horizon();

    for (auto job_it = _queue->begin(); job_it != _queue->end(); ++job_it)
    {
        const Job * job = (*job_it)->job;

        // Let's try to put the job into the schedule at the first available slot.
        Schedule::JobAlloc alloc = schedule.add_job_first_fit(job, _selector, false);

        // Because of sleeping machines, the allocation might be impossible.
        // If this happens, some machines must be awaken
        if (!alloc.has_been_inserted)
        {
            // The nodes we will try to switch ON are the ones in a potential sleep state between
            // the finite horizon and the infinite one. Let's find which ones should be awakened.
            auto slice_it = schedule.end();
            --slice_it;
            const Schedule::TimeSlice & last_slice = *slice_it;

            // Let's compute which machines can be awakened in the last slice
            IntervalSet machines_that_can_be_awakened = compute_potentially_awaken_machines(last_slice);

            // Let's find which machines to awaken
            IntervalSet machines_to_awaken;
            _selector->select_resources_to_awaken_to_make_job_fit(job, last_slice.available_machines, machines_that_can_be_awakened, machines_to_awaken);
            PPK_ASSERT_ERROR(machines_to_awaken.size() > 0);

            // Let's find the first moment in time (the earliest) when all the machines to awaken can be awakened
            Rational earliest_awakening_time = find_earliest_moment_to_awaken_machines(schedule, machines_to_awaken);

            // Let's modify the future schedule of the machines which should be used for computing the job
            //   1. Let's reduce the length (or remove) the potential sleep jobs they have
            //   2. Let's insert a wake up job for each sleeping machine
            for (auto machine_it = machines_to_awaken.elements_begin(); machine_it != machines_to_awaken.elements_end(); ++machine_it)
            {
                int machine_id = *machine_it;
                awaken_machine(schedule, machine_id, earliest_awakening_time);
            }

            if (_debug)
            {
                LOG_F(1, "schedule before job insertion: %s", schedule.to_string().c_str());
                LOG_F(1, "job : (id='%s', walltime=%g)", job->id.c_str(), (double) job->walltime);
            }

            // Let's finally insert the job
            auto job_alloc = schedule.add_job_first_fit_after_time(job, earliest_awakening_time, _selector);

            if (_debug)
            {
                LOG_F(1, "schedule after job insertion: %s", schedule.to_string().c_str());
            }

            // Let's make sure the machine awakening was not useless
            PPK_ASSERT_ERROR((job_alloc.used_machines & machines_to_awaken) != IntervalSet::empty_interval_set());

            // Let's make sure the infinite horizon has not been changed
            PPK_ASSERT_ERROR(initial_infinite_horizon == schedule.infinite_horizon());
        }
    }
}

Rational EnergyBackfilling::sedate_machines_at_the_furthest_moment(Schedule &schedule, const IntervalSet &machines_to_sedate) const
{
    PPK_ASSERT_ERROR(schedule.nb_slices() >= 1);

    auto time_slice_it = schedule.end();
    --time_slice_it;

    PPK_ASSERT_ERROR(time_slice_it->end == schedule.infinite_horizon());
    PPK_ASSERT_ERROR(time_slice_it->begin == schedule.finite_horizon());

    Rational earliest_sedating_date = schedule.infinite_horizon();

    // Let's store awaken_machines into one variable
    IntervalSet awaken_machines = time_slice_it->available_machines; // Since sleep jobs targets the infinite horizon, the sleeping machines are not available in the last time slice

    while ((machines_to_sedate & awaken_machines) != IntervalSet::empty_interval_set())
    {
        if (time_slice_it != schedule.begin())
            --time_slice_it;

        // Let's find which machines which should be sedated just after the current time slice.
        // Since time slices are traversed from future to past, we know that if a machine is still awaken,
        // it does nothing in the future. Then, if a machine in the awaken_machines set does something in the
        // current time slice, it should be sedated at the end of the time slice currently being traversed.
        IntervalSet machines_to_sedate_now;

        if (_debug)
        {
            LOG_F(1, "Schedule : %s", schedule.to_string().c_str());
            LOG_F(1, "Current time slice : %s", time_slice_it->to_string().c_str());
        }


        for (auto mit : time_slice_it->allocated_jobs)
        {
            const Job * job = mit.first;
            const IntervalSet & job_machines = mit.second;

            // If the job is a fake one
            if (is_fake_job(job->id))
            {
                PPK_ASSERT_ERROR(job_machines.size() == 1);

                int machine_id = job_machines.first_element();
                boost::regex e("fakejob_(.*)_(\\d+)");

                boost::match_results<std::string::const_iterator> results;
                bool matched = boost::regex_match(job->id, results, e);
                PPK_ASSERT_ERROR(matched);

                int machine_id_from_job_id = stoi(results[2]);
                PPK_ASSERT_ERROR(machine_id == machine_id_from_job_id);

                // If the fake job is a switch ON one
                if (results[1] == "son")
                    machines_to_sedate_now.insert(machine_id);
            }
            else
                machines_to_sedate_now.insert(job_machines);
        }

        // Only machines that are awaken should be sedated now
        machines_to_sedate_now &= awaken_machines;

        // Only machines that need to be sedated should be sedated now
        machines_to_sedate_now &= machines_to_sedate;

        // Let's sedate all the machines that must be
        for (auto machine_it = machines_to_sedate_now.elements_begin(); machine_it != machines_to_sedate_now.elements_end(); ++machine_it)
        {
            int machine_id = *machine_it;
            sedate_machine(schedule, machine_id, time_slice_it, false);
            earliest_sedating_date = time_slice_it->end;
            // Fortunately, inserting after the current time slice can only change the iterators after (-> future) the current iterator,
            // not the ones before (-> past), which makes the traversal work
        }

        // Let's mark machines that will be sedated as not awaken
        awaken_machines -= machines_to_sedate_now;

        if (time_slice_it == schedule.begin())
        {
            // If we reached the schedule's beginning, all awaken machines that should be sedated should be put into sleep now.
            machines_to_sedate_now = awaken_machines & machines_to_sedate;

            for (auto machine_it = machines_to_sedate_now.elements_begin(); machine_it != machines_to_sedate_now.elements_end(); ++machine_it)
            {
                int machine_id = *machine_it;
                sedate_machine(schedule, machine_id, time_slice_it, true);
                earliest_sedating_date = time_slice_it->begin;
                // In this case, the sleep jobs should be inserted into the given time slice, not just after it
            }

            awaken_machines -= machines_to_sedate_now;

            PPK_ASSERT_ERROR((awaken_machines & machines_to_sedate) == IntervalSet::empty_interval_set());
        }
    }

    return earliest_sedating_date;
}

void EnergyBackfilling::sedate_machine(Schedule &schedule,
                                       int machine_id,
                                       std::list<Schedule::TimeSlice>::iterator time_slice,
                                       bool insert_in_slice) const
{
    // Let's retrieve the MachineInformation
    MachineInformation * minfo = _machine_informations.at(machine_id);

    if (_debug)
    {
        LOG_F(1, "\n-----\n");
        LOG_F(1, "Machine to sedate: %d", machine_id);
        LOG_F(1, "Schedule before switch_off_alloc : %s", schedule.to_string().c_str());
    }

    // Let's add the switch OFF job into the schedule
    Schedule::JobAlloc switch_off_alloc = schedule.add_job_first_fit_after_time_slice(minfo->switch_off_job, time_slice, minfo->limited_resource_selector);

    if (_debug)
    {
        LOG_F(1, "Schedule after switch_off_alloc : %s", schedule.to_string().c_str());
    }

    if (insert_in_slice)
        PPK_ASSERT_ERROR(switch_off_alloc.begin == time_slice->begin,
                         "switch_off_alloc.begin = %g, time_slice->begin = %g,",
                         (double) switch_off_alloc.begin, (double) time_slice->begin);
    else
        PPK_ASSERT_ERROR(switch_off_alloc.begin == time_slice->end,
                         "switch_off_alloc.begin = %g, time_slice->end = %g,",
                         (double) switch_off_alloc.begin, (double) time_slice->end);

    sedate_machine_without_switch(schedule, machine_id, switch_off_alloc.end);
}

void EnergyBackfilling::sedate_machine_without_switch(Schedule &schedule,
                                                      int machine_id,
                                                      Rational when_it_should_start) const
{
    // Let's retrieve the MachineInformation
    MachineInformation * minfo = _machine_informations.at(machine_id);

    Rational when_ensured_sleep_job_finishes = when_it_should_start;

    if (minfo->ensured_sleep_job->walltime > 0)
    {
        // Let's add the ensured sleep job into the schedule, right after the previous one
        Schedule::JobAlloc ensured_sleep_alloc = schedule.add_job_first_fit_after_time(minfo->ensured_sleep_job, when_it_should_start, minfo->limited_resource_selector);

        if (_debug)
        {
            LOG_F(1, "Schedule after ensured_sleep_alloc : %s", schedule.to_string().c_str());
        }

        PPK_ASSERT_ERROR(ensured_sleep_alloc.begin == when_it_should_start,
                         "ensured_sleep_alloc.begin = %g, when_it_should_start = %g",
                         (double) ensured_sleep_alloc.begin, (double) when_it_should_start);

        when_ensured_sleep_job_finishes = ensured_sleep_alloc.end;
    }

    // Let's change the walltime of the potential sleep job to make sure it perfectly reaches the infinite horizon
    minfo->potential_sleep_job->walltime = schedule.infinite_horizon() - when_ensured_sleep_job_finishes;
    Rational infinite_horizon_before = schedule.infinite_horizon();

    // Let's add the potential sleep job into the schedule, right after the previous one
    Schedule::JobAlloc potential_sleep_alloc = schedule.add_job_first_fit_after_time(minfo->potential_sleep_job, when_it_should_start, minfo->limited_resource_selector);

    if (_debug)
    {
        LOG_F(1, "Schedule after potential_sleep_alloc : %s", schedule.to_string().c_str());
    }

    PPK_ASSERT_ERROR(potential_sleep_alloc.begin == when_ensured_sleep_job_finishes);
    PPK_ASSERT_ERROR(potential_sleep_alloc.end == schedule.infinite_horizon());
    PPK_ASSERT_ERROR(schedule.infinite_horizon() == infinite_horizon_before);
}

void EnergyBackfilling::awaken_machine(Schedule &schedule, int machine_id, Rational awakening_date) const
{
    MachineInformation * machine_info = _machine_informations.at(machine_id);

    // Let's find when the potential sleep job starts
    auto potential_job_beginning_slice = schedule.find_last_occurence_of_job(machine_info->potential_sleep_job, schedule.begin());
    Rational previously_potential_job_beginning_time = potential_job_beginning_slice->begin;

    // Let's remove the previous potential sleep job from the schedule
    schedule.remove_job_last_occurence(machine_info->potential_sleep_job);

    // If the machine can still have a potential sleep job (of reduced length), one is added into the schedule
    Rational potential_sleep_maximum_length = awakening_date - previously_potential_job_beginning_time;
    PPK_ASSERT_ERROR(potential_sleep_maximum_length >= 0);

    if (_debug)
    {
        LOG_F(1, "EnergyBackfilling::awaken_machine.\n"
               "potential_sleep_maximum_length = %f\n%s",
               (double) potential_sleep_maximum_length,
               schedule.to_string().c_str());
    }

    if (potential_sleep_maximum_length > 0)
    {
        Job * job = machine_info->potential_sleep_job;
        job->walltime = potential_sleep_maximum_length;
        auto potential_sleep_alloc = schedule.add_job_first_fit_after_time(job, previously_potential_job_beginning_time, machine_info->limited_resource_selector);
        PPK_ASSERT_ERROR(potential_sleep_alloc.begin == previously_potential_job_beginning_time);

        if (_debug)
        {
            LOG_F(1, "EnergyBackfilling::awaken_machine.\n"
                   "The potential job has been inserted back into the schedule.\n%s",
                   schedule.to_string().c_str());
        }
    }

    // Let's wake the machine up
    auto wake_up_alloc = schedule.add_job_first_fit_after_time(machine_info->switch_on_job, previously_potential_job_beginning_time, machine_info->limited_resource_selector);
    PPK_ASSERT_ERROR(wake_up_alloc.begin == awakening_date);

    if (_debug)
    {
        LOG_F(1, "EnergyBackfilling::awaken_machine after wake up\n%s",
               schedule.to_string().c_str());
    }
}

Rational EnergyBackfilling::awaken_machine_as_soon_as_possible(Schedule &schedule, int machine_id) const
{
    MachineInformation * machine_info = _machine_informations.at(machine_id);

    // Let's find when the potential sleep job starts
    auto potential_job_beginning_slice = schedule.find_last_occurence_of_job(machine_info->potential_sleep_job, schedule.begin());
    PPK_ASSERT_ERROR(potential_job_beginning_slice != schedule.end(),
                     "Cannot awaken machine %d: The machine is not sleeping\n", machine_id);

    Rational insertion_time = potential_job_beginning_slice->begin;

    awaken_machine(schedule, machine_id, insertion_time);
    return insertion_time;
}

EnergyBackfilling::ScheduleMetrics EnergyBackfilling::compute_metrics_of_schedule(const Schedule &schedule, Rational min_job_length) const
{
    PPK_ASSERT_ERROR(schedule.nb_slices() > 0);
    ScheduleMetrics ret;

    ret.makespan = schedule.finite_horizon();

    struct JobInfo
    {
        bool traversed_left = false;
        bool traversed_right = false;

        Rational submission_time;
        Rational starting_time;
        Rational ending_time;

        Rational waiting_time;
        Rational turnaround_time;
        Rational slowdown;
        Rational bounded_slowdown;
    };

    map<string, JobInfo> jobs_info;

    // Past -> Future traversal to find starting times
    for (auto time_slice_it = schedule.begin(); time_slice_it != schedule.end(); ++time_slice_it)
    {
        const Schedule::TimeSlice & slice = *time_slice_it;

        for (auto mit : slice.allocated_jobs)
        {
            const Job * job = mit.first;

            if (!is_fake_job(job->id))
            {
                if (jobs_info.count(job->id) == 0)
                    jobs_info[job->id] = JobInfo();

                JobInfo & info = jobs_info[job->id];

                // If the job has not been taken into account
                if (!info.traversed_right)
                {
                    info.submission_time = job->submission_time;
                    info.starting_time = slice.begin;

                    info.traversed_right = true;
                }
            }
        }
    }

    // Future -> Past traversal to find expected ending times
    bool should_continue_traversal = true;
    auto last_slice = schedule.end();
    --last_slice;
    for (auto time_slice_it = last_slice; should_continue_traversal; --time_slice_it)
    {
        const Schedule::TimeSlice & slice = *time_slice_it;

        for (auto mit : slice.allocated_jobs)
        {
            const Job * job = mit.first;

            if (! is_fake_job(job->id))
            {
                JobInfo & info = jobs_info[job->id];

                // If the job has not been taken into account
                if (!info.traversed_left)
                {
                    info.ending_time = slice.end;

                    // Now that both starting and end times are known, metrics can be computed
                    info.waiting_time = info.starting_time - info.submission_time;
                    info.turnaround_time = info.ending_time - info.submission_time;
                    info.slowdown = info.turnaround_time / (info.ending_time - info.starting_time);
                    info.bounded_slowdown = info.turnaround_time / std::max(min_job_length, Rational(info.ending_time - info.starting_time));

                    // Let's compute the sum of the metrics there
                    ret.mean_waiting_time += info.waiting_time;
                    ret.mean_turnaround_time += info.turnaround_time;
                    ret.mean_slowdown += info.slowdown;
                    ret.mean_bounded_slowdown += info.bounded_slowdown;

                    // The same with the max of each metrics
                    ret.max_waiting_time = std::max(ret.max_waiting_time, info.waiting_time);
                    ret.max_turnaround_time = std::max(ret.max_turnaround_time, info.turnaround_time);
                    ret.max_slowdown = std::max(ret.max_slowdown, info.slowdown);
                    ret.max_bounded_slowdown = std::max(ret.max_bounded_slowdown, info.bounded_slowdown);

                    info.traversed_left = true;
                }
            }
        }

        if (time_slice_it == schedule.begin())
            should_continue_traversal = false;
    }

    // Let's divide the sum of each metrics by the number of jobs to get the mean
    if (jobs_info.size() > 0)
    {
        ret.mean_waiting_time /= jobs_info.size();
        ret.mean_turnaround_time /= jobs_info.size();
        ret.mean_slowdown /= jobs_info.size();
        ret.mean_bounded_slowdown /= jobs_info.size();
    }

    return ret;
}

IntervalSet EnergyBackfilling::compute_potentially_awaken_machines(const Schedule::TimeSlice &time_slice)
{
    // Computes the machines that can be awaken in one time slice.
    // These machines are the one on which potential sleep jobs are allocated

    IntervalSet res;

    for (auto mit : time_slice.allocated_jobs)
    {
        const Job * job = mit.first;
        const IntervalSet & job_machines = mit.second;

        if (is_potential_sleep_job(job->id))
            res.insert(job_machines);
    }

    return res;
}

IntervalSet EnergyBackfilling::compute_sleeping_machines(const Schedule::TimeSlice &time_slice)
{
    // Computes the machines that are sleeping in one time slice:
    //   - those in ensured sleep
    //   - those in potential sleep

    IntervalSet res;

    for (auto mit : time_slice.allocated_jobs)
    {
        const Job * job = mit.first;
        const IntervalSet & job_machines = mit.second;

        if (is_potential_sleep_job(job->id) || is_ensured_sleep_job(job->id))
            res.insert(job_machines);
    }

    return res;
}

Rational EnergyBackfilling::find_earliest_moment_to_awaken_machines(Schedule &schedule, const IntervalSet &machines_to_awaken) const
{
    // Let's traverse the schedule Future -> Past
    // As soon as we find any machine to awaken not computing a potential sleep job,
    // the moment of time we seek is found.
    PPK_ASSERT_ERROR(schedule.nb_slices() > 0);

    auto slice_it = schedule.end(); // Finite horizon -> Infinite horizon
    --slice_it;

    // Let's make sure this method is called correctly by checking that all machines are sleeping in the last time slice
    IntervalSet sleeping_machines = compute_potentially_awaken_machines(*slice_it);
    PPK_ASSERT_ERROR((sleeping_machines & machines_to_awaken) == machines_to_awaken);

    do
    {
        if (slice_it != schedule.begin())
            --slice_it;

        sleeping_machines = compute_potentially_awaken_machines(*slice_it);

        // If all the machines are not in a potential sleep yet
        if ((sleeping_machines & machines_to_awaken) != machines_to_awaken)
            return slice_it->end;

    } while (slice_it != schedule.begin());

    return schedule.first_slice_begin();
}

Rational EnergyBackfilling::estimate_energy_of_schedule(const Schedule &schedule, Rational horizon) const
{
    PPK_ASSERT_ERROR(horizon < schedule.infinite_horizon());
    Rational energy = 0;

    // Let's iterate the slices
    for (auto slice_it = schedule.begin(); (slice_it->begin < horizon) && (slice_it != schedule.end()); ++slice_it)
    {
        const Schedule::TimeSlice & slice = *slice_it;

        Rational length = slice.length;
        if (slice_it->end > horizon)
            length = horizon - slice_it->begin;

        IntervalSet idle_machines = _all_machines;

        for (auto mit : slice.allocated_jobs)
        {
            const Job * job = mit.first;
            const IntervalSet & mr = mit.second;

            idle_machines -= mr;
            Rational job_power_in_slice = 0;

            if (is_fake_job(job->id))
            {
                if (is_switch_on_job(job->id))
                    for (auto machine_it = mr.elements_begin(); machine_it != mr.elements_end(); ++machine_it)
                        job_power_in_slice += _machine_informations.at(*machine_it)->switch_on_electrical_power;
                else if (is_switch_off_job(job->id))
                    for (auto machine_it = mr.elements_begin(); machine_it != mr.elements_end(); ++machine_it)
                        job_power_in_slice += _machine_informations.at(*machine_it)->switch_off_electrical_power;
                else if (is_ensured_sleep_job(job->id))
                    for (auto machine_it = mr.elements_begin(); machine_it != mr.elements_end(); ++machine_it)
                        job_power_in_slice += _machine_informations.at(*machine_it)->sleep_epower;
                else if (is_potential_sleep_job(job->id))
                    for (auto machine_it = mr.elements_begin(); machine_it != mr.elements_end(); ++machine_it)
                        job_power_in_slice += _machine_informations.at(*machine_it)->sleep_epower;
                else
                    PPK_ASSERT_ERROR(false);
            }
            else
            {
                for (auto machine_it = mr.elements_begin(); machine_it != mr.elements_end(); ++machine_it)
                    job_power_in_slice += _machine_informations.at(*machine_it)->compute_epower;
            }

            energy += job_power_in_slice * length;
        }

        // Let's add the energy of idle machines
        for (auto machine_it = idle_machines.elements_begin(); machine_it != idle_machines.elements_end(); ++ machine_it)
        {
            int machine_id = *machine_it;
            MachineInformation * minfo = _machine_informations.at(machine_id);

            energy += minfo->idle_epower * length;
        }
    }

    return energy;
}

bool EnergyBackfilling::is_switch_on_job(const std::string & job_id)
{
    return boost::starts_with(job_id, "fakejob_son_");
}

bool EnergyBackfilling::is_switch_off_job(const std::string & job_id)
{
    return boost::starts_with(job_id, "fakejob_soff_");
}

bool EnergyBackfilling::is_ensured_sleep_job(const std::string & job_id)
{
    return boost::starts_with(job_id, "fakejob_esleep_");
}

bool EnergyBackfilling::is_potential_sleep_job(const std::string & job_id)
{
    return boost::starts_with(job_id, "fakejob_psleep_");
}

bool EnergyBackfilling::is_fake_job(const std::string & job_id)
{
    return boost::starts_with(job_id, "fakejob_");
}

bool EnergyBackfilling::contains_any_fake_job(const Schedule &schedule)
{
    for (auto slice_it = schedule.begin(); slice_it != schedule.end(); ++slice_it)
    {
        for (auto mit : slice_it->allocated_jobs)
        {
            const Job * job = mit.first;
            if (is_fake_job(job->id))
                return true;
        }
    }
    return false;
}

bool EnergyBackfilling::contains_any_nonfake_job(const Schedule &schedule)
{
    for (auto slice_it = schedule.begin(); slice_it != schedule.end(); ++slice_it)
    {
        for (auto mit : slice_it->allocated_jobs)
        {
            const Job * job = mit.first;
            if (!is_fake_job(job->id))
                return true;
        }
    }
    return false;
}
