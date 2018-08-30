#pragma once

#include <unordered_map>
#include <list>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"

class EasyBackfillingFast : public ISchedulingAlgorithm
{
public:
    EasyBackfillingFast(Workload * workload, SchedulingDecision * decision,
        Queue * queue, ResourceSelector * selector,
        double rjms_delay,
        rapidjson::Document * variant_options);
    virtual ~EasyBackfillingFast();

    virtual void on_simulation_start(double date,
        const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
        SortableJobOrder::UpdateInformation * update_info,
        SortableJobOrder::CompareInformation * compare_info);

private:
    struct FinishedHorizonPoint
    {
        double date;
        int nb_released_machines;
    };

    struct Allocation
    {
        IntervalSet machines;
        std::list<FinishedHorizonPoint>::iterator horizon_it;
    };

private:
    double compute_priority_job_expected_earliest_starting_time();
    std::list<FinishedHorizonPoint>::iterator insert_horizon_point(const FinishedHorizonPoint & point);

private:
    // Machines currently available
    IntervalSet _available_machines;
    int _nb_available_machines = -1;

    // Pending jobs (queue; without the priority job)
    std::list<Job *> _pending_jobs;

    // Allocations of running jobs
    std::unordered_map<std::string, Allocation> _current_allocations;

    // When running jobs are expected to finish.
    // Always sorted by increasing date.
    std::list<FinishedHorizonPoint> _horizons;

    // At any time, null if there is no priority job (no waiting job)
    Job * _priority_job = nullptr;
};
