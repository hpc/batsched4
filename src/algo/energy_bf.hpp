#pragma once

#include <list>
#include <map>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"

class EnergyBackfilling : public ISchedulingAlgorithm
{
public:
    enum MachineState
    {
        AWAKE,
        ASLEEP,
        SWITCHING_OFF,
        SWITCHING_ON
    };

    struct ScheduleMetrics
    {
        Rational makespan = 0;

        Rational mean_waiting_time = 0;
        Rational max_waiting_time = 0;

        Rational mean_turnaround_time = 0;
        Rational max_turnaround_time = 0;

        Rational mean_slowdown = 0;
        Rational max_slowdown = 0;

        Rational mean_bounded_slowdown = 0;
        Rational max_bounded_slowdown = 0;
    };

    std::string machine_state_to_string(const MachineState & state);

    struct MachineInformation
    {
        MachineInformation(int machine_number);
        ~MachineInformation();

        void create_jobs(double rjms_delay,
                         Rational ensured_sleep_time_lower_bound,
                         Rational ensured_sleep_time_upper_bound);
        void free_jobs();

    private:
        void create_selector();
        void free_selector();

    public:
        int machine_number = -1; //! The machine number the MachineInformation corresponds to
        LimitedRangeResourceSelector * limited_resource_selector = nullptr;
        MachineState state = AWAKE;

        int compute_pstate = 0;
        int sleep_pstate = 1;

        Rational compute_epower = 0;
        Rational idle_epower = 0;
        Rational sleep_epower = 0;

        Rational switch_on_seconds = 0;
        Rational switch_on_energy = 0;
        Rational switch_on_electrical_power = 0;

        Rational switch_off_seconds = 0;
        Rational switch_off_energy = 0;
        Rational switch_off_electrical_power = 0;

        Job * switch_on_job = nullptr; //! This job corresponds to the switching ON state of the machine
        Job * switch_off_job = nullptr; //! This job corresponds to the switching OFF state of the machine

        Job * ensured_sleep_job = nullptr; //! This job corresponds to the sleeping state of the machine. It cannot be stopped. It is used to avoid pure loss of energy via too frequent switches OFF and ON
        Job * potential_sleep_job = nullptr; //! This job corresponds to the sleeping state of the machine. It can be stopped if a job cannot fit in the schedule otherwise
    };

public:
    EnergyBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                      double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EnergyBackfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void on_machine_state_changed(double date, IntervalSet machines, int newState);

    virtual void on_requested_call(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

protected:
    void generate_machine_informations(int nb_machines);
    void clear_machine_informations();

    void make_decisions_of_schedule(const Schedule & schedule, bool run_call_me_later_on_nothing_to_do = true);

    void update_first_slice_taking_sleep_jobs_into_account(Rational date);

    void put_jobs_into_schedule(Schedule & schedule) const;

    /**
     * @brief Sedates machines as soon as they remain idle.
     * @param[in] schedule The Schedule into which the sedatings should be done
     * @param[in] machines_to_sedate The machines to sedate
     * @return The earliest sedating decision date
     */
    Rational sedate_machines_at_the_furthest_moment(Schedule & schedule,
                                                    const IntervalSet & machines_to_sedate) const;
    void sedate_machine(Schedule & schedule,
                        int machine_id,
                        std::list<Schedule::TimeSlice>::iterator time_slice,
                        bool insert_in_slice = true) const;
    void sedate_machine_without_switch(Schedule & schedule,
                                       int machine_id,
                                       Rational when_it_should_start) const;

    void awaken_machine(Schedule & schedule,
                        int machine_id,
                        Rational awakening_date) const;

    /**
     * @brief Awakens a machine as soon as possible
     * @param[in] schedule The Schedule into which the awakening should be done
     * @param[in] machine_id The machine to awaken
     * @return The moment at which the machine should be awakened
     */
    Rational awaken_machine_as_soon_as_possible(Schedule & schedule,
                                                int machine_id) const;

    ScheduleMetrics compute_metrics_of_schedule(const Schedule & schedule, Rational min_job_length = 30) const;

    /**
     * @brief Computes the machines that can be awakened inside a given time slice.
     * @details These machines are the ones on which potential sleep jobs are allocated
     * @param[in] time_slice The time slice
     * @return The machines that can be awaked inside the given time slice
     */
    static IntervalSet compute_potentially_awaken_machines(const Schedule::TimeSlice & time_slice);
    static IntervalSet compute_sleeping_machines(const Schedule::TimeSlice & time_slice);

    Rational find_earliest_moment_to_awaken_machines(Schedule & schedule, const IntervalSet & machines_to_awaken) const;

    Rational estimate_energy_of_schedule(const Schedule & schedule, Rational horizon) const;

    static bool is_switch_on_job(const std::string & job_id);
    static bool is_switch_off_job(const std::string & job_id);
    static bool is_ensured_sleep_job(const std::string & job_id);
    static bool is_potential_sleep_job(const std::string & job_id);
    static bool is_fake_job(const std::string & job_id);

    static bool contains_any_fake_job(const Schedule & schedule);
    static bool contains_any_nonfake_job(const Schedule & schedule);

protected:
    Schedule _schedule;
    bool _debug = false;

    int _nb_call_me_later_running = 0;

    int _nb_jobs_submitted = 0;
    int _nb_jobs_completed = 0;

    std::map<int, MachineInformation*> _machine_informations;

    IntervalSet _all_machines; //!< All the machines that can be used for computing jobs
    IntervalSet _switching_on_machines; //!< The machines currently being switched ON
    IntervalSet _switching_off_machines; //!< The machines currently being switched OFF
    IntervalSet _asleep_machines; //!< The machines currently in a sleepy state. They cannot be used to compute jobs now. This is the union of _wakable_asleep_machines and _non_wakable_asleep_machines
    IntervalSet _wakable_asleep_machines; //!< Subset of _asleep_machines. Those machines have been sleeping for enough time, they can be awakened.
    IntervalSet _non_wakable_asleep_machines; //!< Subset of _asleep_machines. Those machines have NOT been sleeping for enough time, they cannot be awakened yet.
    IntervalSet _awake_machines; //!< The machines currently in a computation pstate. They can represent the machines which compute jobs, or idle machines.
};
