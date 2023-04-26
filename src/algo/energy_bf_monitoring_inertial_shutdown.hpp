#pragma once

#include "energy_bf_monitoring_period.hpp"

class EnergyBackfillingMonitoringInertialShutdown : public EnergyBackfillingMonitoringPeriod
{
public:
    enum DecisionType
    {
        SEDATE_MACHINES,
        AWAKEN_MACHINES,
    };

    enum MachinesSedatingPolicy
    {
        SEDATE_FIRST_MACHINES
    };

    enum MachinesAwakeningPolicy
    {
        AWAKEN_FIRST_MACHINES
    };

    enum InertialAlterationType
    {
        SUM,
        PRODUCT
    };

public:
    EnergyBackfillingMonitoringInertialShutdown(Workload * workload,
                                                SchedulingDecision * decision,
                                                Queue * queue,
                                                ResourceSelector * selector,
                                                double rjms_delay,
                                                rapidjson::Document * variant_options);

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

    virtual void on_monitoring_stage(double date);


    void select_machines_to_sedate(int nb_machines,
                                   const IntervalSet & sedatable_machines,
                                   const IntervalSet & machines_that_can_be_used_by_the_priority_job,
                                   IntervalSet & machines_to_sedate,
                                   const Job * priority_job) const;

    void select_machines_to_awaken(int nb_machines,
                                   const IntervalSet & awakable_machines,
                                   IntervalSet & machines_to_awaken) const;

    static void select_first_machines_to_sedate(int nb_machines,
                                                const Schedule & schedule,
                                                const IntervalSet & sedatable_machines,
                                                const IntervalSet &machines_that_can_be_used_by_the_priority_job,
                                                IntervalSet & machines_to_sedate,
                                                const Job * priority_job = nullptr);

    static void select_first_machines_to_awaken(int nb_machines,
                                                const Schedule & schedule,
                                                const IntervalSet & awakable_machines,
                                                IntervalSet & machines_to_awaken);

    void write_schedule_debug(const std::string & filename_suffix = "");

    static void compute_priority_job_and_related_stuff(Schedule & schedule,
                                                       const Queue * queue,
                                                       const Job *& priority_job,
                                                       ResourceSelector * priority_job_selector,
                                                       bool & priority_job_needs_awakenings,
                                                       Schedule::JobAlloc & first_insertion_alloc,
                                                       IntervalSet & priority_job_reserved_machines,
                                                       IntervalSet & machines_that_can_be_used_by_the_priority_job);

    Rational compute_priority_job_starting_time_expectancy(const Schedule & schedule,
                                                           const Job * priority_job);


protected:
    /**

     * @details This method will add fake jobs into _inertial_schedule and update _machines_to_sedate
     *          and _machines_to_awaken if the switches have been done in the first slice.
     */

    /**
     * @brief Handles the switches ON/OFF that must be done but that couldn't be done earlier.
     * @param[in,out] schedule The Schedule on which the modifications should be done
     * @param[in] machines_to_sedate The machines that must be sedated as soon as possible
     * @param[in] machines_to_awaken The machines that must be awakened as soon as possible
     * @param[out] machines_sedated_now The machines that have been sedated in the first time slice
     * @param[out] machines_awakened_now The machines that have been awakened in the first time slice
     */
    void handle_queued_switches(Schedule & schedule,
                                const IntervalSet & machines_to_sedate,
                                const IntervalSet & machines_to_awaken,
                                IntervalSet & machines_sedated_now,
                                IntervalSet & machines_awakened_now);

    void write_output_file(double date,
                           int nb_jobs_in_queue,
                           int first_job_size,
                           double load_in_queue,
                           double liquid_load_horizon);

protected:
    Schedule _inertial_schedule;

private:
    std::ofstream _output_file;

protected:
    bool _inertial_shutdown_debug = false;
    bool _really_write_svg_files = false;
    bool _write_output_file = true;
    int _debug_output_id = 0;

    // Algorithm real (scientific) parameters:
    bool _allow_future_switches = true;
    Rational _upper_llh_threshold = 1e100;
    InertialAlterationType _alteration_type = PRODUCT;
    Rational _inertial_alteration_number = 2;

    bool _sedate_idle_on_classical_events = false;
    double _needed_amount_of_idle_time_to_be_sedated = 1e18;
    int _nb_machines_sedated_for_being_idle = 0;
    int _nb_machines_sedated_by_inertia = 0;
    std::map<int, Rational> _machines_idle_start_date;
    IntervalSet _idle_machines;

    bool _first_monitoring_stage = true;

    Rational _priority_job_starting_time_expectancy = 0;

    DecisionType _last_decision;
    int _inertial_number;
    Rational _last_llh_value = 0; //!< The value of the latest computed Liquid Load Horizon
    Rational _last_llh_date = 0; //!< The date at which the latest Liquid Load Horizon has been computed
    Rational _llh_integral_since_last_monitoring_stage = 0; //!< The sum over time of the Liquid Load Horizon. This sum is computed by the sum of the areas of trapezoids.
    Rational _llh_integral_of_preceding_monitoring_stage_slice = 0; //!< The precedent value of _llh_integral_since_last_monitoring_stage

    MachinesSedatingPolicy _sedating_policy = SEDATE_FIRST_MACHINES;
    MachinesAwakeningPolicy _awakening_policy = AWAKEN_FIRST_MACHINES;

    IntervalSet _machines_to_sedate;
    IntervalSet _machines_to_awaken;

    // The pstates changes that were asked since the last monitoring stage
    IntervalSet _machines_awakened_since_last_monitoring_stage_inertia;
    IntervalSet _machines_sedated_since_last_monitoring_stage_inertia;
};
