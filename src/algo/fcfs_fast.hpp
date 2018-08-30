#pragma once

#include <unordered_map>
#include <list>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"

class FCFSFast : public ISchedulingAlgorithm
{
public:
    FCFSFast(Workload * workload, SchedulingDecision * decision,
        Queue * queue, ResourceSelector * selector,
        double rjms_delay,
        rapidjson::Document * variant_options);
    virtual ~FCFSFast();

    virtual void on_simulation_start(double date,
        const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
        SortableJobOrder::UpdateInformation * update_info,
        SortableJobOrder::CompareInformation * compare_info);

private:
    // Machines currently available
    IntervalSet _available_machines;
    int _nb_available_machines = -1;

    // Pending jobs (queue)
    std::list<Job *> _pending_jobs;

    // Allocations of running jobs
    std::unordered_map<std::string, IntervalSet> _current_allocations;
};
