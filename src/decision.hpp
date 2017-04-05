#pragma once

#include <vector>
#include <string>

#include "machine_range.hpp"

class SchedulingDecision
{
public:
    void add_allocation(const std::string &job_id, const MachineRange & machineIDs, double date);
    void add_rejection(const std::string &job_id, double date);
    void add_kill(const std::string & job_id, double date);

    void add_change_machine_state(MachineRange machines, int newPState, double date);

    void add_nop_me_later(double future_date, double date);

    void clear();

    const std::string & content() const { return _content; }
    double last_date() const { return _lastDate; }

private:
    std::string _content;
    double _lastDate = -1;
    bool _display_decisions = true;
};
