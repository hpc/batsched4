#pragma once

#include "../isalgorithm.hpp"

#include <vector>
#include <set>
#include <list>
#include <map>

#include "../locality.hpp"
#include "../machine_range.hpp"

class Workload;
class SchedulingDecision;

class Submitter : public ISchedulingAlgorithm
{
public:
    Submitter(Workload * workload, SchedulingDecision * decision, Queue * queue,
              ResourceSelector * selector, double rjms_delay,
              rapidjson::Document * variant_options);

    virtual ~Submitter();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

protected:
    void submit_delay_job(double delay, double date);

private:
    MachineRange available_machines;
    std::map<std::string, MachineRange> current_allocations;
    int nb_submitted_jobs = 0; //!< The number of jobs submitted from this algorithm
    int nb_jobs_to_submit = 10; //!< The number of jobs to submit
    bool redis_enabled;
    bool finished_submitting_sent = false;
};
