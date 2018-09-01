#pragma once

#include <intervalset.hpp>
struct Job;

class ResourceSelector
{
public:
    ResourceSelector();
    virtual ~ResourceSelector();

    virtual bool fit(const Job * job, const IntervalSet & available, IntervalSet & allocated) = 0;
    virtual void select_resources_to_sedate(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_sedated, IntervalSet & to_sedate) = 0;
    virtual void select_resources_to_awaken(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken) = 0;
    virtual void select_resources_to_awaken_to_make_job_fit(const Job * job, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken) = 0;
};

class BasicResourceSelector : public ResourceSelector
{
public:
    BasicResourceSelector();
    ~BasicResourceSelector();

    bool fit(const Job * job, const IntervalSet & available, IntervalSet & allocated);
    void select_resources_to_sedate(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_sedated, IntervalSet & to_sedate);
    void select_resources_to_awaken(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);
};

class ContiguousResourceSelector : public ResourceSelector
{
public:
    ContiguousResourceSelector();
    ~ContiguousResourceSelector();

    bool fit(const Job * job, const IntervalSet & available, IntervalSet & allocated);
    void select_resources_to_sedate(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_sedated, IntervalSet & to_sedate);
    void select_resources_to_awaken(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);
};

class LimitedRangeResourceSelector : public ResourceSelector
{
public:
    LimitedRangeResourceSelector();
    LimitedRangeResourceSelector(const IntervalSet & limited_range);
    ~LimitedRangeResourceSelector();

    bool fit(const Job *job, const IntervalSet &available, IntervalSet &allocated);
    void select_resources_to_sedate(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_sedated, IntervalSet & to_sedate);
    void select_resources_to_awaken(int nb_resources, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);
    void select_resources_to_awaken_to_make_job_fit(const Job * job, const IntervalSet & available, const IntervalSet & potentially_awaken, IntervalSet & to_awaken);

public:
    IntervalSet _limited_range;
};
