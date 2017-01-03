#include "queueing_theory_waiting_time_estimator.hpp"

#include "pempek_assert.hpp"

void QueueingTheoryWaitingTimeEstimator::add_submitted_job(const Job *job)
{
    _recently_submitted_jobs.push_back(job);
}

void QueueingTheoryWaitingTimeEstimator::add_completed_job(const Job *job)
{
    _recently_completed_jobs.push_back(job);
}

void QueueingTheoryWaitingTimeEstimator::remove_old(Rational old_date_thresh)
{
    for (auto submit_lit = _recently_submitted_jobs.begin(); submit_lit != _recently_submitted_jobs.end(); )
    {
        const Job * job = *submit_lit;
        if (job->submission_time < old_date_thresh)
            submit_lit = _recently_submitted_jobs.erase(submit_lit);
        else
            break;
    }

    for (auto complete_lit = _recently_completed_jobs.begin(); complete_lit != _recently_completed_jobs.end(); )
    {
        const Job * job = *complete_lit;
        if (job->completion_time < old_date_thresh)
            complete_lit = _recently_completed_jobs.erase(complete_lit);
        else
            break;
    }
}

Rational QueueingTheoryWaitingTimeEstimator::estimate_waiting_time(Rational period_length)
{
    PPK_ASSERT_ERROR(period_length > 0);
    Rational arrival_rate = _recently_submitted_jobs.size() / period_length;
    Rational service_rate = _recently_completed_jobs.size() / period_length;

    if (arrival_rate != service_rate && service_rate != 0)
        return (1/(service_rate-arrival_rate)) - (1/service_rate);
    else
        return std::numeric_limits<Rational>::infinity();
}

Rational QueueingTheoryWaitingTimeEstimator::estimate_waiting_time_by_area(Rational period_length, int nb_awake_machines)
{
    (void) period_length;
    (void) nb_awake_machines;
    PPK_ASSERT_ERROR(false, "Not implemented");
    return 0;
}
