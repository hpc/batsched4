#pragma once

#include <vector>
#include <string>

#include "machine_range.hpp"

class AbstractProtocolWriter;

// TODO: remove class?
class SchedulingDecision
{
public:
    SchedulingDecision();
    ~SchedulingDecision();

    void add_execute_job(const std::string &job_id, const MachineRange & machine_ids, double date);
    void add_reject_job(const std::string &job_id, double date);
    void add_kill_job(const std::vector<std::string> & job_ids, double date);

    void add_set_resource_state(MachineRange machines, int new_state, double date);

    void add_call_me_later(double future_date, double date);

    void clear();

    std::string content(double date);
    double last_date() const;

private:
    AbstractProtocolWriter * _proto_writer = nullptr;
};
