#pragma once

#include <vector>
#include <string>

#include <intervalset.hpp>
#include "json_workload.hpp"
#include "batsched_tools.hpp"
#include <utility>

class AbstractProtocolWriter;
class RedisStorage;

class SchedulingDecision
{
public:
    SchedulingDecision();
    ~SchedulingDecision();

    void add_execute_job(const std::string &job_id, const IntervalSet & machine_ids, double date,
                         std::vector<int> executor_to_allocated_resource_mapping = {});
    void handle_resubmission(std::unordered_map<std::string,batsched_tools::Job_Message *> recently_killed_jobs,Workload * workload,double date);
    void push_back_job_still_needed_to_be_killed(batsched_tools::Job_Message * jm);
    void add_reject_job(double date, const std::string &job_id, batsched_tools::REJECT_TYPES forWhat );
    void add_kill_job(const std::vector<batsched_tools::Job_Message *> & job_msgs, double date);

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

    void add_call_me_later(double date, double future_date,batsched_tools::CALL_ME_LATERS cml);
    void add_scheduler_finished_submitting_jobs(double date);
    void add_scheduler_continue_submitting_jobs(double date);
    void add_generic_notification(const std::string &type,const std::string &notify_data,double date);

    void add_query_energy_consumption(double date);
    void add_answer_estimate_waiting_time(const std::string & job_id,
                                          double estimated_waiting_time,
                                          double date);
    double remove_call_me_later(batsched_tools::CALL_ME_LATERS cml_in, double date,Workload * w0);
    std::map<int,batsched_tools::CALL_ME_LATERS> get_call_me_laters();
    int get_nb_call_me_laters();
    void set_call_me_laters(std::map<int,batsched_tools::CALL_ME_LATERS> & cml,double date,bool dispatch=false);
    void set_blocked_call_me_laters(std::set<batsched_tools::call_me_later_types> &blocked_cmls);
    void clear_blocked_call_me_laters();
    void add_blocked_call_me_later(batsched_tools::call_me_later_types type);
    void remove_blocked_call_me_later(batsched_tools::call_me_later_types type);
            

    void clear();

    std::string content(double date);
    double last_date() const;
    void set_nb_call_me_laters(int nb);
    void set_redis(bool enabled, RedisStorage * redis);
    std::string to_json_desc(rapidjson::Document * doc);


private:
    void get_meta_data_from_delay(std::pair<std::string,batsched_tools::Job_Message *> killed_map, 
                                                        rapidjson::Document & profile_doc,
                                                        rapidjson::Document & job_doc,
                                                        Workload * w0);
    void get_meta_data_from_parallel_homogeneous(std::pair<std::string,batsched_tools::Job_Message *> killed_map,
                                                        rapidjson::Document & profile_doc,
                                                        rapidjson::Document & job_doc,
                                                        Workload* w0);
    
    
    AbstractProtocolWriter * _proto_writer = nullptr;
    bool _redis_enabled = false;
    RedisStorage * _redis = nullptr;
    int _nb_call_me_laters=0;
    std::map<int,batsched_tools::CALL_ME_LATERS> _call_me_laters;
    std::set<batsched_tools::call_me_later_types> _blocked_cmls;
    std::vector<batsched_tools::Job_Message *> _jobs_still_needed_to_be_killed;
    
};
