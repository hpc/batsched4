#pragma once

#include <list>

#include "json_workload.hpp"
#include "exact_numbers.hpp"

struct QueueingTheoryWaitingTimeEstimator
{
    void add_submitted_job(const Job * job);
    void add_completed_job(const Job * job);

    void remove_old(Rational old_date_thresh);
    Rational estimate_waiting_time(Rational period_length);
    Rational estimate_waiting_time_by_area(Rational period_length, int nb_awake_machines);

private:
    std::list<const Job *> _recently_submitted_jobs;
    std::list<const Job *> _recently_completed_jobs;
};
