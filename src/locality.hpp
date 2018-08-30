#pragma once

#include <intervalset.hpp>
struct Job;

class ResourceSelector
{
public:
    ResourceSelector() {}
    virtual ~ResourceSelector() {}

    virtual bool fit(const Job * job, const MachineRange & available, MachineRange & allocated) = 0;
    virtual void select_resources_to_sedate(int nb_resources, const MachineRange & available, const MachineRange & potentially_sedated, MachineRange & to_sedate) = 0;
    virtual void select_resources_to_awaken(int nb_resources, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken) = 0;
    virtual void select_resources_to_awaken_to_make_job_fit(const Job * job, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken) = 0;
};

class BasicResourceSelector : public ResourceSelector
{
public:
    BasicResourceSelector() {}
    ~BasicResourceSelector() {}

    bool fit(const Job * job, const MachineRange & available, MachineRange & allocated);
    void select_resources_to_sedate(int nb_resources, const MachineRange & available, const MachineRange & potentially_sedated, MachineRange & to_sedate);
    void select_resources_to_awaken(int nb_resources, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);
};

class ContiguousResourceSelector : public ResourceSelector
{
public:
    ContiguousResourceSelector() {}
    ~ContiguousResourceSelector() {}

    bool fit(const Job * job, const MachineRange & available, MachineRange & allocated);
    void select_resources_to_sedate(int nb_resources, const MachineRange & available, const MachineRange & potentially_sedated, MachineRange & to_sedate);
    void select_resources_to_awaken(int nb_resources, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);
};

class LimitedRangeResourceSelector : public ResourceSelector
{
public:
    LimitedRangeResourceSelector() {}
    LimitedRangeResourceSelector(const MachineRange & limited_range) : _limited_range(limited_range) {}
    ~LimitedRangeResourceSelector() {}

    bool fit(const Job *job, const MachineRange &available, MachineRange &allocated);
    void select_resources_to_sedate(int nb_resources, const MachineRange & available, const MachineRange & potentially_sedated, MachineRange & to_sedate);
    void select_resources_to_awaken(int nb_resources, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const MachineRange & available, const MachineRange & potentially_awaken, MachineRange & to_awaken);

public:
    MachineRange _limited_range;
};
