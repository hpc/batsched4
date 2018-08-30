#pragma once

#include "../isalgorithm.hpp"

class EnergyWatcher : public ISchedulingAlgorithm
{
public:
    EnergyWatcher(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
           double rjms_delay, rapidjson::Document * variant_options);

    virtual ~EnergyWatcher();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

    void execute_job_if_whole_machine_is_idle(double date);

private:
    IntervalSet _machines;
    bool _is_machine_busy = false;
    double _previous_energy = -1;
};
