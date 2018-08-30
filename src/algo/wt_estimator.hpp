#pragma once

#include "../isalgorithm.hpp"

#include "../locality.hpp"
#include <intervalset.hpp>

class Workload;
class SchedulingDecision;

class WaitingTimeEstimator : public ISchedulingAlgorithm
{
public:
    WaitingTimeEstimator(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                         double rjms_delay, rapidjson::Document * variant_options);

    virtual ~WaitingTimeEstimator();

    void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);
};
