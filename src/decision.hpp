#pragma once

#include <vector>
#include <string>

#include <intervalset.hpp>

class AbstractProtocolWriter;

class SchedulingDecision
{
public:
    SchedulingDecision();
    ~SchedulingDecision();

    void add_execute_job(const std::string &job_id, const IntervalSet & machine_ids, double date,
                         std::vector<int> executor_to_allocated_resource_mapping = {});
    void add_reject_job(const std::string &job_id, double date);
    void add_kill_job(const std::vector<std::string> & job_ids, double date);

    /**
     * @brief add_submit_jobs
     * @param workload_name
     * @param job_id Job identifier (WITHOUT WORKLOAD! PREFIX)
     * @param profile_name Profile name (WITHOUT WORKLOAD! PREFIX)
     * @param job_json_description
     * @param profile_json_description
     * @param date
     */
    void add_submit_job(const std::string & workload_name,
                        const std::string & job_id,
                        const std::string & profile_name,
                        const std::string & job_json_description,
                        const std::string & profile_json_description,
                        double date,
                        bool send_profile = true);

    void add_submit_profile(const std::string & workload_name,
                            const std::string & profile_name,
                            const std::string & profile_json_description,
                            double date);

    void add_set_resource_state(IntervalSet machines, int new_state, double date);

    void add_set_job_metadata(const std::string & job_id,
                              const std::string & metadata,
                              double date);

    void add_call_me_later(double future_date, double date);
    void add_scheduler_finished_submitting_jobs(double date);

    void add_query_energy_consumption(double date);
    void add_answer_estimate_waiting_time(const std::string & job_id,
                                          double estimated_waiting_time,
                                          double date);

    void clear();

    std::string content(double date);
    double last_date() const;

private:
    AbstractProtocolWriter * _proto_writer = nullptr;
};
