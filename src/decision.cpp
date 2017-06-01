#include "decision.hpp"

#include "network.hpp"
#include "pempek_assert.hpp"
#include "protocol.hpp"
#include "data_storage.hpp"

namespace n = network;
using namespace std;

SchedulingDecision::SchedulingDecision()
{
    _proto_writer = new JsonProtocolWriter;
}

SchedulingDecision::~SchedulingDecision()
{
    delete _proto_writer;
    _proto_writer = nullptr;
}

void SchedulingDecision::add_execute_job(const std::string & job_id, const MachineRange &machine_ids, double date)
{
    _proto_writer->append_execute_job(job_id, machine_ids, date);
}

void SchedulingDecision::add_reject_job(const std::string & job_id, double date)
{
    _proto_writer->append_reject_job(job_id, date);
}

void SchedulingDecision::add_kill_job(const vector<string> &job_ids, double date)
{
    _proto_writer->append_kill_job(job_ids, date);
}

void SchedulingDecision::add_submit_job(const string & workload_name,
                                        const string & job_id,
                                        const string & profile_name,
                                        const string & job_json_description,
                                        const string & profile_json_description,
                                        double date,
                                        bool send_profile)
{
    string complete_job_id = workload_name + '!' + job_id;

    if (_redis_enabled)
    {
        string job_key = RedisStorage::job_key(workload_name, job_id);
        string profile_key = RedisStorage::profile_key(workload_name, profile_name);

        PPK_ASSERT_ERROR(_redis != nullptr);
        _redis->set(job_key, job_json_description);
        _redis->set(profile_key, profile_json_description);

        _proto_writer->append_submit_job(complete_job_id, date, "", "", send_profile);
    }
    else
        _proto_writer->append_submit_job(complete_job_id, date,
                                         job_json_description,
                                         profile_json_description,
                                         send_profile);
}

void SchedulingDecision::add_set_resource_state(MachineRange machines, int new_state, double date)
{
    _proto_writer->append_set_resource_state(machines, std::to_string(new_state), date);
}

void SchedulingDecision::add_call_me_later(double future_date, double date)
{
    _proto_writer->append_call_me_later(future_date, date);
}

void SchedulingDecision::add_scheduler_finished_submitting_jobs(double date)
{
    _proto_writer->append_scheduler_finished_submitting_jobs(date);
}

void SchedulingDecision::clear()
{
    _proto_writer->clear();
}

string SchedulingDecision::content(double date)
{
    return _proto_writer->generate_current_message(date);
}

double SchedulingDecision::last_date() const
{
    return _proto_writer->last_date();
}

void SchedulingDecision::set_redis(bool enabled, RedisStorage *redis)
{
    _redis_enabled = enabled;
    _redis = redis;
}
