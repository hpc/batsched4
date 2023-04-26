#pragma once



namespace myBatsched{
    struct Job;
struct Profile;
typedef std::shared_ptr<Job> JobPtr; //!< A smart pointer towards a Job.
typedef std::shared_ptr<Profile> ProfilePtr; //!< A smart pointer towards a Profile.

}
