#include "locality.hpp"

#include "schedule.hpp"

#include "json_workload.hpp"
#include "pempek_assert.hpp"


ResourceSelector::ResourceSelector()
{

}

ResourceSelector::~ResourceSelector()
{

}

BasicResourceSelector::BasicResourceSelector()
{

}

BasicResourceSelector::~BasicResourceSelector()
{

}

bool BasicResourceSelector::fit(const Job *job, const IntervalSet &available, IntervalSet &allocated)
{
    if (job->nb_requested_resources <= (int) available.size())
    {
        allocated = available.left(job->nb_requested_resources);
        PPK_ASSERT_ERROR(allocated.size() == (unsigned int)job->nb_requested_resources);
        return true;
    }

    return false;
}

void BasicResourceSelector::select_resources_to_sedate(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_sedated, IntervalSet &to_sedate)
{
    (void) available;
    PPK_ASSERT_ERROR(nb_resources <= (int)potentially_sedated.size());

    to_sedate = potentially_sedated.left(nb_resources);
    PPK_ASSERT_ERROR(nb_resources == (int)to_sedate.size());
}

void BasicResourceSelector::select_resources_to_awaken(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    (void) available;
    PPK_ASSERT_ERROR(nb_resources <= (int)potentially_awaken.size());

    to_awaken = potentially_awaken.left(nb_resources);
    PPK_ASSERT_ERROR(nb_resources == (int)to_awaken.size());
}

void BasicResourceSelector::select_resources_to_awaken_to_make_job_fit(const Job *job, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    select_resources_to_awaken(job->nb_requested_resources - available.size(), available, potentially_awaken, to_awaken);
    IntervalSet u;
    PPK_ASSERT_ERROR(fit(job, (available + to_awaken), u));
}

ContiguousResourceSelector::ContiguousResourceSelector()
{

}

ContiguousResourceSelector::~ContiguousResourceSelector()
{

}

bool ContiguousResourceSelector::fit(const Job *job, const IntervalSet &available, IntervalSet &allocated)
{
    for (auto it = available.intervals_begin(); it != available.intervals_end(); ++it)
    {
        int interval_length = it->upper() - it->lower() + 1;

        if (job->nb_requested_resources <= interval_length)
        {
            allocated = *it;
            allocated = allocated.left(job->nb_requested_resources);
            PPK_ASSERT_ERROR(allocated.size() == (unsigned int)job->nb_requested_resources);
            return true;
        }
    }

    return false;
}

void ContiguousResourceSelector::select_resources_to_sedate(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_sedated, IntervalSet &to_sedate)
{
    (void) available;
    PPK_ASSERT_ERROR(nb_resources <= (int)potentially_sedated.size());

    to_sedate = potentially_sedated.left(nb_resources);
    PPK_ASSERT_ERROR(nb_resources == (int)to_sedate.size());
}

void ContiguousResourceSelector::select_resources_to_awaken(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    (void) available;
    PPK_ASSERT_ERROR(nb_resources <= (int)potentially_awaken.size());

    to_awaken = potentially_awaken.left(nb_resources);
    PPK_ASSERT_ERROR(nb_resources == (int)to_awaken.size());
}

void ContiguousResourceSelector::select_resources_to_awaken_to_make_job_fit(const Job *job, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    // The union of the available and potentially_awaken ranges are not necessarily the full machine and we should keep it in mind.
    // Since we want the job to be allocated on a contiguous range, let's only work on the biggest interval of the union of
    // available and potentially_awaken.

    IntervalSet fully_awaken_range = available + potentially_awaken;
    IntervalSet work_range = *fully_awaken_range.biggest_interval();

    // Let's make sure the job can fit in the work range
    PPK_ASSERT_ERROR(job->nb_requested_resources <= (int)work_range.size());

    // If the job can already fit the machine, there is nothing to do
    IntervalSet u;
    if (!fit(job, available, u))
    {
        // Let's find which machines are already available and which ones can potentially be awaken inside the work_range
        IntervalSet work_range_available = work_range & available;

        // This heuristic increases the size of the biggest available hole until the job can fit in it
        auto left_interval_it = work_range_available.biggest_interval();
        auto right_interval_it = left_interval_it;

        int hole_size = left_interval_it->upper() - left_interval_it->lower() + 1;

        while (hole_size < job->nb_requested_resources)
        {
            // We can either increase the hole size by extending it to the left or the right
            // Let's always choose the one which allows to obtain the greater amount of available machines
            int left_potential_awaken = 0;
            int left_potential_available = 0;

            int right_potential_awaken = 0;
            int right_potential_available = 0;

            auto left_next_interval_it = left_interval_it;
            --left_next_interval_it;

            // If there are machines that can be awaken on the left
            if (left_interval_it != work_range_available.intervals_begin())
            {
                // [3, 5] u [8, 12] -> 2 (= 8 - 5 - 1) resources can be awaken in the middle
                int interval_size = left_next_interval_it->upper() - left_next_interval_it->lower() + 1;
                left_potential_awaken = left_interval_it->lower() - left_next_interval_it->upper() - 1;
                left_potential_available = left_potential_awaken + interval_size;
            }

            auto right_next_interval_it = right_interval_it;
            ++right_next_interval_it;

            // If there are machines that can be awaken on the right
            if (right_next_interval_it != work_range_available.intervals_end())
            {
                // [3, 5] u [8, 12] -> 2 (= 8 - 5 - 1) resources can be awaken in the middle

                int interval_size = right_next_interval_it->upper() - right_next_interval_it->lower() + 1;
                right_potential_awaken = right_next_interval_it->lower() - right_interval_it->upper() - 1;
                right_potential_available = right_potential_awaken + interval_size;
            }

            bool extend_left;

            if (left_potential_awaken == 0) // If we can only extend to the right
            {
                PPK_ASSERT_ERROR(right_potential_awaken > 0);
                extend_left = false;
            }
            else if (right_potential_awaken == 0) // If we can only extend to the left
            {
                PPK_ASSERT_ERROR(left_potential_awaken > 0);
                extend_left = true;
            }
            else // If one side has to be chosen
            {
                // Let's choose the side that maximises the (nb_available / nb_awaken) ratio
                Rational left_ratio =  left_potential_available / left_potential_awaken;
                Rational right_ratio = right_potential_available / right_potential_awaken;

                extend_left = left_ratio < right_ratio;
            }

            // If the hole is extended towards the left
            if (extend_left)
            {
                // If all the machines between the two intervals do not have to be awaken
                if (hole_size + right_potential_awaken >= job->nb_requested_resources)
                {
                    int nb_machines_to_awaken = job->nb_requested_resources - hole_size;
                    IntervalSet machines_to_awaken = IntervalSet::ClosedInterval(right_interval_it->upper() + 1, right_interval_it->upper() + 1 + nb_machines_to_awaken);
                    PPK_ASSERT_ERROR(machines_to_awaken == nb_machines_to_awaken);
                    to_awaken += machines_to_awaken;

                    PPK_ASSERT_ERROR((to_awaken & potentially_awaken) == to_awaken);
                    PPK_ASSERT_ERROR(fit(job, available & to_awaken, u));
                    return;
                }
                else
                {
                    IntervalSet machines_to_awaken = IntervalSet::ClosedInterval(right_interval_it->upper() + 1, right_next_interval_it->lower() - 1);
                    PPK_ASSERT_ERROR(machines_to_awaken.size() > 0);
                    to_awaken += machines_to_awaken;

                    hole_size += machines_to_awaken.size();
                    right_interval_it = right_next_interval_it;
                }
            }
            else // If the hole is extended towards the right
            {
                // If all the machines between the two intervals do not have to be awaken
                if (hole_size + left_potential_awaken >= job->nb_requested_resources)
                {
                    int nb_machines_to_awaken = job->nb_requested_resources - hole_size;
                    IntervalSet machines_to_awaken = IntervalSet::ClosedInterval(left_next_interval_it->upper() + 1, left_next_interval_it->upper() + 1 + nb_machines_to_awaken);
                    PPK_ASSERT_ERROR(machines_to_awaken == nb_machines_to_awaken);
                    to_awaken += machines_to_awaken;

                    PPK_ASSERT_ERROR((to_awaken & potentially_awaken) == to_awaken);
                    PPK_ASSERT_ERROR(fit(job, available & to_awaken, u));
                    return;
                }
                else
                {
                    IntervalSet machines_to_awaken = IntervalSet::ClosedInterval(left_next_interval_it->upper() + 1, left_interval_it->lower() - 1);
                    PPK_ASSERT_ERROR(machines_to_awaken.size() > 0);
                    to_awaken += machines_to_awaken;

                    hole_size += machines_to_awaken.size();
                    left_interval_it = left_next_interval_it;
                }
            }
        }
    }

    PPK_ASSERT_ERROR((to_awaken & potentially_awaken) == to_awaken);
    PPK_ASSERT_ERROR(fit(job, available & to_awaken, u));
    return;
}

LimitedRangeResourceSelector::LimitedRangeResourceSelector()
{

}

LimitedRangeResourceSelector::LimitedRangeResourceSelector(const IntervalSet &limited_range) :
    _limited_range(limited_range)
{

}

LimitedRangeResourceSelector::~LimitedRangeResourceSelector()
{

}

bool LimitedRangeResourceSelector::fit(const Job *job, const IntervalSet &available, IntervalSet &allocated)
{
    IntervalSet limited_available = available;
    limited_available &= _limited_range;

    if (job->nb_requested_resources <= (int) limited_available.size())
    {
        allocated = limited_available.left(job->nb_requested_resources);
        PPK_ASSERT_ERROR(allocated.size() == (unsigned int)job->nb_requested_resources);
        return true;
    }

    return false;
}

void LimitedRangeResourceSelector::select_resources_to_sedate(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_sedated, IntervalSet &to_sedate)
{
    (void) nb_resources;
    (void) available;
    (void) potentially_sedated;
    (void) to_sedate;

    PPK_ASSERT_ERROR(false, "The LimitedRangeResourceSelector is not meant to be used to select resources to sedate");
}

void LimitedRangeResourceSelector::select_resources_to_awaken(int nb_resources, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    (void) nb_resources;
    (void) available;
    (void) potentially_awaken;
    (void) to_awaken;

    PPK_ASSERT_ERROR(false, "The LimitedRangeResourceSelector is not meant to be used to select resources to awaken");
}

void LimitedRangeResourceSelector::select_resources_to_awaken_to_make_job_fit(const Job *job, const IntervalSet &available, const IntervalSet &potentially_awaken, IntervalSet &to_awaken)
{
    (void) job;
    (void) available;
    (void) potentially_awaken;
    (void) to_awaken;

    PPK_ASSERT_ERROR(false, "The LimitedRangeResourceSelector is not meant to be used to select resources to awaken to make a job fit");
}
