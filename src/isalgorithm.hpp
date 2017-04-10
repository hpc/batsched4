#pragma once

#include <rapidjson/document.h>
#include <vector>

#include "decision.hpp"
#include "queue.hpp"

/**
 * @brief The base abstract class of (scheduling & machine state) decision algorithms
 */
class ISchedulingAlgorithm
{
public:
    /**
     * @brief Builds a ISchedulingAlgorithm
     * @param[in,out] workload The Workload instance
     * @param[in,out] decision The Decision instance
     * @param[in,out] queue The Queue instance
     * @param[in,out] selector The ResourceSelector instance
     * @param[in] rjms_delay The maximum amount of time the RJMS is supposed to take to do some actions like killing jobs.
     * @param[in,out] variant_options The variant-dependent options
     */
    ISchedulingAlgorithm(Workload * workload,
                         SchedulingDecision * decision,
                         Queue * queue,
                         ResourceSelector * selector,
                         double rjms_delay,
                         rapidjson::Document * variant_options);

    /**
     * @brief Destroys a ISchedulingAlgorithm
     */
    virtual ~ISchedulingAlgorithm() {}

    /**
     * @brief This function is called when the simulation is about to begin
     * @param[in] date The date at which the simulation is about to begin
     */
    virtual void on_simulation_start(double date) = 0;
    /**
     * @brief This function is called when the simulation is about to finish
     * @param[in] date The date at which the simulation is about to finish
     */
    virtual void on_simulation_end(double date) = 0;

    /**
     * @brief This function is called when jobs have been released
     * @param[in] date The date at which the jobs have been released
     * @param[in] job_ids The identifiers of the jobs which have been released
     */
    virtual void on_job_release(double date, const std::vector<std::string> & job_ids);
    /**
     * @brief This function is called when jobs have been finished
     * @param[in] date The date at which the jobs have been finished
     * @param[in] job_ids The identifiers of the jobs which have been finished
     */
    virtual void on_job_end(double date, const std::vector<std::string> & job_ids);

    /**
     * @brief This function is called when jobs have been killed (resulting from a decision, not a timeout)
     * @param[in] date The date at which the jobs have been killed
     * @param[in] job_ids The identifiers of the jobs which have been finished
     */
    virtual void on_job_killed(double date, const std::vector<std::string> & job_ids);

    /**
     * @brief This function is called when the power state of some machines have been changed
     * @param[in] date The date at which the power state alteration has occured
     * @param[in] machines The machines involved in the power state alteration
     * @param[in] new_state The new power state of the machines: The state they are in, after the alteration
     */
    virtual void on_machine_state_changed(double date, MachineRange machines, int new_state);

    /**
     * @brief This function is called when some machines failed
     * @param[in] date The date at which the machines failed
     * @param[in] machines The machines which failed
     */
    virtual void on_failure(double date, MachineRange machines);
    /**
     * @brief This function is called when some machines are no longer in a failure state
     * @param[in] date The date at which the machines have been fixed
     * @param[in] machines The machines which have been fixed
     */
    virtual void on_failure_end(double date, MachineRange machines);

    /**
     * @brief This function is called when a NOP message is received
     * @param[in] date The date at which the NOP message have been received
     */
    virtual void on_nop(double date);

    /**
     * @brief This function is called when some decisions need to be made
     * @param[in] date The current date
     * @param[in,out] update_info Some information to sort the jobs
     * @param[in,out] compare_info Some information to sort the jobs
     */
    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info) = 0;

    /**
     * @brief Allows to set the total number of machines in the platform
     * @details This function is called just before on_simulation_start
     * @param[in] nb_machines The total number of machines in the platform
     */
    void set_nb_machines(int nb_machines);

    /**
     * @brief Clears data structures used to store what happened between two make_decisions calls
     * @details This function should be called between make_decisions calls!
     */
    void clear_recent_data_structures();

protected:
    Workload * _workload;
    SchedulingDecision * _decision;
    Queue * _queue;
    ResourceSelector * _selector;
    double _rjms_delay;
    rapidjson::Document * _variant_options;
    int _nb_machines = -1;

    struct Failure
    {
        Failure(double date, MachineRange machines, bool failed);

        double date;
        MachineRange machines;
        bool failed;
    };

protected:
    std::vector<std::string> _jobs_released_recently;
    std::vector<std::string> _jobs_ended_recently;
    std::vector<std::string> _jobs_killed_recently;
    std::map<int, MachineRange> _machines_whose_pstate_changed_recently;
    std::vector<Failure> _recent_failstate_changes;
    bool _nopped_recently;
};
