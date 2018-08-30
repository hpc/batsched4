#pragma once

#include "../isalgorithm.hpp"

#include <vector>
#include <set>
#include <list>
#include <map>

#include "../locality.hpp"
#include <intervalset.hpp>

class Workload;
class SchedulingDecision;

class Killer2 : public ISchedulingAlgorithm
{
public:
    Killer2(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
           double rjms_delay, rapidjson::Document * variant_options);

    virtual ~Killer2();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    IntervalSet available_machines;
    std::map<std::string, IntervalSet> current_allocations;
};
