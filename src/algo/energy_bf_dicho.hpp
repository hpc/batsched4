#pragma once

#include "energy_bf.hpp"

class EnergyBackfillingDichotomy : public EnergyBackfilling
{
    enum AwakeningComparisonType
    {
        SWITCH_ON,
        REMOVE_SLEEP_JOBS
    };

public:
    EnergyBackfillingDichotomy(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                               double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EnergyBackfillingDichotomy();

protected:
    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    Rational _tolerated_slowdown_loss_ratio;
    AwakeningComparisonType _comparison_type = REMOVE_SLEEP_JOBS;

    std::map<std::string, AwakeningComparisonType> _str_to_comparison_type;
};
