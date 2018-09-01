#pragma once

#include <list>

struct Job;

#include "schedule.hpp"

struct SortableJob
{
    const Job * job;
    Rational release_date;
    Rational slowdown;
    Rational bounded_slowdown;

    void update_slowdown(Rational current_date);
    void update_bounded_slowdown(Rational current_date, Rational execution_time_lower_bound);
};

class SortableJobOrder
{
public:
    struct CompareInformation
    {
        virtual ~CompareInformation() = 0;
    };

    struct UpdateInformation
    {
        UpdateInformation(Rational current_date);
        virtual ~UpdateInformation();

        Rational current_date;
    };

public:
    virtual ~SortableJobOrder();
    virtual bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const = 0;
    virtual void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const = 0;
};

class FCFSOrder : public SortableJobOrder
{
public:
    ~FCFSOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class LCFSOrder : public SortableJobOrder
{
public:
    ~LCFSOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class DescendingBoundedSlowdownOrder : public SortableJobOrder
{
public:
    DescendingBoundedSlowdownOrder(Rational min_job_length);
    ~DescendingBoundedSlowdownOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;

private:
    Rational _min_job_length;
};

class DescendingSlowdownOrder : public SortableJobOrder
{
public:
    ~DescendingSlowdownOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class AscendingSizeOrder : public SortableJobOrder
{
public:
    ~AscendingSizeOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class DescendingSizeOrder : public SortableJobOrder
{
public:
    ~DescendingSizeOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class AscendingWalltimeOrder : public SortableJobOrder
{
public:
    ~AscendingWalltimeOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class DescendingWalltimeOrder : public SortableJobOrder
{
public:
    ~DescendingWalltimeOrder();
    bool compare(const SortableJob * j1, const SortableJob * j2, const CompareInformation * info = nullptr) const;
    void updateJob(SortableJob * job, const UpdateInformation * info = nullptr) const;
};

class Queue
{
public:
    Queue(SortableJobOrder * order);
    ~Queue();

    void append_job(const Job * job, SortableJobOrder::UpdateInformation * update_info);
    std::list<SortableJob *>::iterator remove_job(const Job * job);
    std::list<SortableJob *>::iterator remove_job(std::list<SortableJob *>::iterator job_it);
    void sort_queue(SortableJobOrder::UpdateInformation * update_info, SortableJobOrder::CompareInformation * compare_info = nullptr);

    const Job * first_job() const;
    const Job * first_job_or_nullptr() const;
    bool contains_job(const Job * job) const;

    bool is_empty() const;
    int nb_jobs() const;
    Rational compute_load_estimation() const;

    std::string to_string() const;

    std::list<SortableJob *>::iterator begin();
    std::list<SortableJob *>::iterator end();

    std::list<SortableJob *>::const_iterator begin() const;
    std::list<SortableJob *>::const_iterator end() const;

private:
    std::list<SortableJob *> _jobs;
    SortableJobOrder * _order;
};
