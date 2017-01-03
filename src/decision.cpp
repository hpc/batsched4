#include "decision.hpp"

#include "network.hpp"
#include "pempek_assert.hpp"

namespace n = network;
using namespace std;

void SchedulingDecision::add_allocation(const std::string & job_id, const MachineRange &machineIDs, double date)
{
    int nbMachines = machineIDs.size();
    PPK_ASSERT(nbMachines > 0);
    PPK_ASSERT(date >= _lastDate);

    _lastDate = date;

    _content += n::separator0 + to_string(date) + n::separator1 + n::jobAllocation + n::separator1;
    _content += job_id + n::separator3;

    _content += machineIDs.to_string_hyphen();

    if (_display_decisions)
        printf("Date=%g. Made decision to run job '%s' on machines %s\n", date, job_id.c_str(), machineIDs.to_string_hyphen().c_str());
}

void SchedulingDecision::add_rejection(const std::string & job_id, double date)
{
    _lastDate = date;
    _content += n::separator0 + to_string(date) + n::separator1 + n::jobRejection + n::separator1 + job_id;

    if (_display_decisions)
        printf("Made decision to reject job '%s'\n", job_id.c_str());
}

void SchedulingDecision::add_change_machine_state(MachineRange machines, int newPState, double date)
{
    PPK_ASSERT(date >= _lastDate);
    _lastDate = date;

    _content += n::separator0 + to_string(date) + n::separator1 + n::machinePStateChangeRequest + n::separator1;
    _content += machines.to_string_hyphen() + n::separator3 + to_string(newPState);

    if (_display_decisions)
        printf("Date=%g. Made decision to change the pstate of machines %s to %d\n", date, machines.to_string_hyphen().c_str(), newPState);
}

void SchedulingDecision::add_nop_me_later(double future_date, double date)
{
    PPK_ASSERT(date >= _lastDate);
    PPK_ASSERT(future_date >= date);

    _content += n::separator0 + to_string(date) + n::separator1 + n::nopMeLater + n::separator1;
    _content += to_string(future_date);

    if (_display_decisions)
        printf("Date=%g. Made decision to be nopped later at date=%g\n", date, future_date);
}

void SchedulingDecision::clear()
{
    _content.clear();
    _lastDate = -1;
}
