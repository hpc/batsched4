#pragma once

#include <map>

#include <rapidjson/document.h>

#include "exact_numbers.hpp"
#include "data_storage.hpp"

struct Job
{
    std::string id;
    int unique_number;
    int nb_requested_resources;
    Rational walltime;
    double submission_time = 0;
    double completion_time = -1;
};

struct JobComparator
{
    bool operator()(const Job * j1, const Job * j2) const
    {
        return j1->id < j2->id;
    }
};

class Workload
{
public:
    ~Workload();

    Job * operator[] (std::string jobID);
    const Job * operator[] (std::string jobID) const;
    int nb_jobs() const { return _jobs.size(); }

    void set_rjms_delay(Rational rjms_delay);

    void add_job_from_redis(RedisStorage &storage, const std::string & job_id, double submission_time);
    void add_job_from_json_object(const rapidjson::Value & object, const std::string & job_id, double submission_time);

    Job * job_from_json_description_string(const std::string & json_string);
    Job * job_from_json_object(const rapidjson::Value & object);

private:
    void put_file_into_buffer(const std::string & filename);
    void deallocate();

private:
    char * _fileContents = nullptr;
    std::map<std::string, Job*> _jobs;
    Rational _rjms_delay = 0;
    int _job_number = 0;
};
