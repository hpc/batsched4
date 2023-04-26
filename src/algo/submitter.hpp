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
    IntervalSet available_machines;
    std::map<std::string, IntervalSet> current_allocations;
    int nb_submitted_jobs = 0; //!< The number of jobs submitted from this algorithm
    int nb_jobs_to_submit = 10; //!< The number of jobs to submit
    bool increase_jobs_duration = true; //!< Whether the duration of the submitted jobs increases or not. If false, the same profile will be used by all the submitted jobs.
    bool send_profile_if_already_sent = false; //!< Whether already transmitted profiles should be sent again to Batsim or not.
    bool send_profiles_in_separate_event = true; //!< Whether profiles should be sent in a separate message or not
    bool set_job_metadata = false; //! If set to true, metadata will be associated to jobs when they are submitted.

    bool dyn_submit_ack;
    bool redis_enabled;
    bool finished_submitting_sent = false;

    std::set<std::string> profiles_already_sent;
};
