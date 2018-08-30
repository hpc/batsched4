#pragma once

#include "energy_bf_monitoring_inertial_shutdown.hpp"

#include <map>

class EnergyBackfillingIdleSleeper : public EnergyBackfillingMonitoringInertialShutdown
{
public:
    enum AwakeningPolicy
    {
        AWAKEN_FOR_PRIORITY_JOB_ONLY,
        AWAKEN_FOR_ALL_JOBS_WITHOUT_RESPECTING_PRIORITY_JOB,
        AWAKEN_FOR_ALL_JOBS_RESPECTING_PRIORITY_JOB
    };

public:
    EnergyBackfillingIdleSleeper(Workload * workload, SchedulingDecision * decision,
                                           Queue * queue, ResourceSelector * selector,
                                           double rjms_delay, rapidjson::Document * variant_options);

    virtual ~EnergyBackfillingIdleSleeper();

    virtual void on_monitoring_stage(double date);

public:
    /**
     * @brief Selects which machines should be sedated for being idle for too long
     * @param[in] current_date The current date
     * @param[in] idle_machines The machines currently idle
     * @param[in] machines_awake_soon The machines that will be awake soon (those awake now and those switching ON)
     * @param[in] priority_job The priority job (nullptr if there is no priority job)
     * @param[in] idle_machines_start_date A map which associates machine_ids to the starting time of their idle state
     * @param[in] minimum_idle_time_to_sedate The machines must be idle for a longer time than idle_sedate_thresh to be sedated
     * @param[out] machines_to_sedate The machines that can be sedated for being idle for too long (output parameter)
     */
    static void select_idle_machines_to_sedate(Rational current_date,
                                               const IntervalSet & idle_machines,
                                               const IntervalSet & machines_awake_soon,
                                               const Job * priority_job,
                                               const std::map<int, Rational> idle_machines_start_date,
                                               Rational minimum_idle_time_to_sedate,
                                               IntervalSet & machines_to_sedate,
                                               bool take_priority_job_into_account = true);

    /**
     * @brief Selects which machines should be awakened to compute some jobs
     * @param[in] queue The queue which contains jobs (or not)
     * @param[in] schedule The current schedule. It should not be modified by this function.
     * @param[in,out] priority_job_selector The ResourceSelector to insert the priority_job into the schedule
     * @param[in] idle_machines The machines currently idle
     * @param[in] policy The AwakeningPolicy to apply
     * @param[in] maximum_nb_machines_to_awaken The maximum number of machines to awaken for this call
     * @param[out] machines_to_awaken The machines that can be awakened to execute jobs (output parameter)
     */
    static void select_idle_machines_to_awaken(const Queue *queue,
                                               const Schedule &schedule,
                                               ResourceSelector * priority_job_selector,
                                               const IntervalSet &idle_machines,
                                               AwakeningPolicy policy,
                                               int maximum_nb_machines_to_awaken,
                                               IntervalSet &machines_to_awaken,
                                               bool take_priority_job_into_account = true);

    /**
     * @brief Updates whichever machines are idle and their idle starting time if needed,
     *        based on the first slice of the given schedule
     * @param[in] current_date The current date
     * @param[in] schedule The current schedule. Its first slice must reflect the platform usage
     * @param[in] all_machines All computing machines
     * @param[in,out] idle_machines The machines currently being idle
     * @param[in,out] machines_idle_start_date The starting time of the idle period of the idle machines
     */
    static void update_idle_states(Rational current_date,
                                   const Schedule & schedule,
                                   const IntervalSet & all_machines,
                                   IntervalSet & idle_machines,
                                   std::map<int,Rational> & machines_idle_start_date);

protected:
    void make_idle_sleep_decisions(double date);
};
