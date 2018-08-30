#pragma once

#include "../isalgorithm.hpp"

class Random : public ISchedulingAlgorithm
{
public:
    Random(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
           double rjms_delay, rapidjson::Document * variant_options);

    virtual ~Random();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    IntervalSet machines;
};
