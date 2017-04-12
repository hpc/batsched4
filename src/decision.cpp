#include "decision.hpp"

#include "network.hpp"
#include "pempek_assert.hpp"
#include "protocol.hpp"

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

void SchedulingDecision::add_submit_job(const string &job_id, const string &job_json_description,
                                        const string &profile_json_description, double date)
{
    _proto_writer->append_submit_job(job_id, date, job_json_description, profile_json_description);
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
