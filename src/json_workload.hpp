#pragma once

#include <map>

#include <rapidjson/document.h>

#include "exact_numbers.hpp"
#include "data_storage.hpp"
#include <intervalset.hpp>
#include "./external/batsched_profile.hpp"


struct JobAlloc;

struct Job
{
    std::string id;
    int unique_number;
    int nb_requested_resources;
    Rational walltime;
    bool has_walltime = true;
    double submission_time = 0;
    std::vector<double> submission_times ;  //can possibly use for handling resubmitted jobs
    double completion_time = -1;
    mutable std::map<Rational, JobAlloc*> allocations;
    int cores=1;
    std::string purpose = "job";
    IntervalSet future_allocations;
    double start = -1;
    myBatsched::ProfilePtr profile = nullptr;
    std::string json_description;
    double checkpoint_interval;
    double dump_time;
    double read_time;
    
};


struct JobAlloc
{
  Rational begin;
  Rational end;
  bool started_in_first_slice;
  bool has_been_inserted = true;
  const Job * job;
  IntervalSet used_machines;
};


struct JobComparator
{
    bool operator()(const Job * j1, const Job * j2) const;
};

class Workload
{
public:
    ~Workload();

    Job * operator[] (std::string jobID);
    const Job * operator[] (std::string jobID) const;
    int nb_jobs() const;

    void set_rjms_delay(Rational rjms_delay);

    void add_job_from_redis(RedisStorage &storage, const std::string & job_id, double submission_time);
    void add_job_from_json_object(const rapidjson::Value & object, const std::string & job_id, double submission_time);
    void add_job_from_json_description_string(const std::string & json_string, const std::string & job_id, double submission_time);

    Job * job_from_json_description_string(const std::string & json_string);
    Job * job_from_json_object(const rapidjson::Value & object);
    Job * job_from_json_object(const rapidjson::Value & job_object, const rapidjson::Value & profile_object);

private:
    void put_file_into_buffer(const std::string & filename);
    void deallocate();
public:
    bool _checkpointing_on = false;
    bool _compute_checkpointing = false;
    double _MTBF = -1.0;
    double _SMTBF = -1.0;
    double _repair_time = 0.0;
    double _host_speed;
    double _fixed_failures = -1.0;
    double _checkpointing_interval = -1.0;
    bool _seed_failures = false;
    int _queue_depth = -1;
private:
    char * _fileContents = nullptr;
    std::map<std::string, Job*> _jobs;
    Rational _rjms_delay = 0;
    int _job_number = 0;
   
};

