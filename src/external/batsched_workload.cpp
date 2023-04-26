#include <loguru.hpp>
#include <fstream>
#include <streambuf>
#include <string>
#include <algorithm>
#include <regex>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include "batsched_workload.hpp"
#include "batsched_profile.hpp"
#include "batsched_job.hpp"

using namespace rapidjson;
namespace r = rapidjson;
namespace myBatsched{

Workload *Workload::new_static_workload(const std::string & workload_name,
                                        const std::string & workload_file,
                                        bool checkpointing_on
                                       )
{
    Workload * workload = new Workload;

    workload->jobs = new Jobs;
    workload->profiles = new Profiles;

    workload->jobs->set_profiles(workload->profiles);
    workload->jobs->set_workload(workload);
    workload->name = workload_name;
    workload->file = workload_file;
    workload->_checkpointing_on = checkpointing_on;
    if (! (workload_file == "dynamic"))
        workload->_is_static = true;
    return workload;
}

Workload *Workload::new_dynamic_workload(const std::string & workload_name,bool checkpointing_on)
{
    Workload * workload = new_static_workload(workload_name, "dynamic",checkpointing_on);

    workload->_is_static = false;
    return workload;
}

void Workload::load_from_batsim(const std::string &json_filename, const r::Value & job_json, const r::Value & profile_json)
{
        LOG_F(INFO,"Loading batsim workload %s %s",name.c_str(),json_filename.c_str());
        Document d;
        Value jobs_array,profiles_array;
        
        jobs_array.CopyFrom(job_json,d.GetAllocator());
        profiles_array.CopyFrom(profile_json,d.GetAllocator());
        
        d.SetObject();
        d.AddMember("jobs",jobs_array,d.GetAllocator());
        d.AddMember("profiles",profiles_array,d.GetAllocator());
       
        profiles->load_from_json(d,json_filename);
       
        jobs->load_from_json(d,json_filename);
       
        LOG_F(INFO,"JSON workload parsed sucessfully. Read %d jobs and %d profiles.",
             jobs->nb_jobs(), profiles->nb_profiles());
}

void Workload::load_from_json(const std::string &json_filename)
{
    LOG_F(INFO,"Loading JSON workload '%s'...", json_filename.c_str());
    // Let the file content be placed in a string
    std::ifstream ifile(json_filename);
    CHECK_F(ifile.is_open(), "Cannot read file '%s'", json_filename.c_str());
    std::string content;

    ifile.seekg(0, std::ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, std::ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());

    // JSON document creation
    Document doc;
    doc.Parse(content.c_str());
    CHECK_F(!doc.HasParseError(), "Invalid JSON file '%s': could not be parsed", json_filename.c_str());
    CHECK_F(doc.IsObject(), "Invalid JSON file '%s': not a JSON object", json_filename.c_str());

    // Let's try to read the number of machines in the JSON document
    CHECK_F(doc.HasMember("nb_res"), "Invalid JSON file '%s': the 'nb_res' field is missing", json_filename.c_str());
    const Value & nb_res_node = doc["nb_res"];
    CHECK_F(nb_res_node.IsInt(), "Invalid JSON file '%s': the 'nb_res' field is not an integer", json_filename.c_str());
    int nb_machines = nb_res_node.GetInt();
    CHECK_F(nb_machines > 0, "Invalid JSON file '%s': the value of the 'nb_res' field is invalid (%d)",
               json_filename.c_str(), nb_machines);

    profiles->load_from_json(doc, json_filename);
    jobs->load_from_json(doc, json_filename);

    LOG_F(INFO,"JSON workload parsed sucessfully. Read %d jobs and %d profiles.",
             jobs->nb_jobs(), profiles->nb_profiles());
    LOG_F(INFO,"Checking workload validity...");
    check_validity();
    LOG_F(INFO,"Workload seems to be valid.");
   
}
void Workload::check_validity()
{
    // Let's check that every SEQUENCE-typed profile points to existing profiles
    // And update the refcounting of these profiles
    for (auto mit : profiles->profiles())
    {
        ProfilePtr profile = mit.second;
        if (profile->type == ProfileType::SEQUENCE)
        {
            auto * data = static_cast<SequenceProfileData *>(profile->data);
            data->profile_sequence.reserve(data->sequence.size());
            for (const auto & prof : data->sequence)
            {
                (void) prof; // Avoids a warning if assertions are ignored
                CHECK_F(profiles->exists(prof),
                           "Invalid composed profile '%s': the used profile '%s' does not exist",
                           mit.first.c_str(), prof.c_str());
                // Adds one to the refcounting for the profile 'prof'
                data->profile_sequence.push_back(profiles->at(prof));
            }
        }
    }

    // TODO : check that there are no circular calls between composed profiles...
    // TODO: compute the constraint of the profile number of resources, to check if it matches the jobs that use it

    // Let's check the profile validity of each job
    for (const auto & mit : jobs->jobs())
    {
        check_single_job_validity(mit.second);
    }
}

void Workload::check_single_job_validity(const JobPtr job)
{
    //TODO This is already checked during creation of the job in Job::from_json
    CHECK_F(profiles->exists(job->profile->name),
               "Invalid job %s: the associated profile '%s' does not exist",
               job->id.to_cstring(), job->profile->name.c_str());

    if (job->profile->type == ProfileType::PARALLEL)
    {
        auto * data = static_cast<ParallelProfileData *>(job->profile->data);
        (void) data; // Avoids a warning if assertions are ignored
        CHECK_F(data->nb_res == job->requested_nb_res,
                   "Invalid job %s: the requested number of resources (%d) do NOT match"
                   " the number of resources of the associated profile '%s' (%d)",
                   job->id.to_cstring(), job->requested_nb_res, job->profile->name.c_str(), data->nb_res);
    }
    /*else if (job->profile->type == ProfileType::SEQUENCE)
    {
        // TODO: check if the number of resources matches a resource-constrained composed profile
    }*/
}
bool Workload::is_static() const
{
    return _is_static;
}
Workload::~Workload()
{
    delete jobs;
    delete profiles;

    jobs = nullptr;
    profiles = nullptr;
}
Workloads::~Workloads()
{
    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;
        delete workload;
    }
    _workloads.clear();
}
Workload *Workloads::operator[](const std::string &workload_name)
{
    return at(workload_name);
}
Workload *Workloads::at(const std::string &workload_name)
{
    return _workloads.at(workload_name);
}
const Workload *Workloads::at(const std::string &workload_name) const
{
    return _workloads.at(workload_name);
}
unsigned int Workloads::nb_workloads() const
{
    return static_cast<unsigned int>(_workloads.size());
}

unsigned int Workloads::nb_static_workloads() const
{
    unsigned int count = 0;

    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;

        count += static_cast<unsigned int>(workload->is_static());
    }

    return count;
}
JobPtr Workloads::job_at(const JobIdentifier &job_id)
{
    return at(job_id.workload_name())->jobs->at(job_id);
}

const JobPtr Workloads::job_at(const JobIdentifier &job_id) const
{
    return at(job_id.workload_name())->jobs->at(job_id);
}
void Workloads::delete_jobs(const std::vector<JobIdentifier> & job_ids,
                            const bool & garbage_collect_profiles)
{
    for (const JobIdentifier & job_id : job_ids)
    {
        at(job_id.workload_name())->jobs->delete_job(job_id, garbage_collect_profiles);
    }
}
bool Workloads::exists(const std::string &workload_name) const
{
    return _workloads.count(workload_name) == 1;
}
std::map<std::string, Workload *> &Workloads::workloads()
{
    return _workloads;
}
bool Workloads::job_is_registered(const JobIdentifier &job_id)
{
    return at(job_id.workload_name())->jobs->exists(job_id);
}

bool Workloads::job_profile_is_registered(const JobIdentifier &job_id)
{
    //TODO this could be improved/simplified
    auto job = at(job_id.workload_name())->jobs->at(job_id);
    return at(job_id.workload_name())->profiles->exists(job->profile->name);
}
void Workloads::insert_workload(const std::string &workload_name, Workload *workload)
{
    CHECK_F(!exists(workload_name));
    CHECK_F(!exists(workload->name));

    workload->name = workload_name;
    _workloads[workload_name] = workload;
}
}

