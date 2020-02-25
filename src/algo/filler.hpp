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

class Filler : public ISchedulingAlgorithm
{
public:
    Filler(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
           double rjms_delay, rapidjson::Document * variant_options);

    virtual ~Filler();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);
private:
    void fill(double date);

private:
    double fraction_of_machines_to_use = 1; //! In ]0,1]. If job requests 42 machines, the scheduler will allocate ceil(42*rho) machines.
    bool set_job_metadata = false; //! If set to true, metadata will be associated to jobs when they are started.
    bool custom_mapping = true;

    IntervalSet available_machines; // Corresponds to classical availability: no job is running on those machines.
    IntervalSet unavailable_machines; // This is NOT the complement of available_machines! This correspond to user-supplied events, that may overlap strangely with job executions as I write these lines.
    std::map<std::string, IntervalSet> current_allocations;
    bool _debug = true;
};
