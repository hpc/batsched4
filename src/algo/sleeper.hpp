#pragma once

#include <list>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"

class Sleeper : public ISchedulingAlgorithm
{
public:
    Sleeper(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
            double rjms_delay, rapidjson::Document * variant_options);

    virtual ~Sleeper();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    void make_decisions(double date,
                        SortableJobOrder::UpdateInformation * update_info,
                        SortableJobOrder::CompareInformation * compare_info);


private:
    IntervalSet all_machines;

    IntervalSet available_machines;
    IntervalSet computing_machines;
    IntervalSet sleeping_machines;
    IntervalSet machines_being_switched_on;
    IntervalSet machines_being_switched_off;

    std::string job_being_computed = "";

    int compute_pstate;
    int sleep_pstate;
    bool simulation_finished = false;
};
