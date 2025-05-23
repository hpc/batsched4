#include "json_workload.hpp"

#include <stdexcept>
#include <fstream>
#include <vector>
#include <limits>
#include <regex>
#include <loguru.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include "batsched_tools.hpp"

#include "pempek_assert.hpp"

using namespace rapidjson;
using namespace std;

Workload::~Workload()
{
    deallocate();
}

Job * Workload::operator[](string jobID)
{
    PPK_ASSERT_ERROR(_jobs.count(jobID) == 1, "Job '%s' does not exist", jobID.c_str());
    return _jobs.at(jobID);
}

const Job * Workload::operator[](string jobID) const
{
    return _jobs.at(jobID);
}

int Workload::nb_jobs() const
{
    return (int) _jobs.size();
}

void Workload::set_rjms_delay(Rational rjms_delay)
{
    PPK_ASSERT_ERROR(rjms_delay >= 0);
    _rjms_delay = rjms_delay;
}
std::map<std::string,Job*>& Workload::get_jobs()
{
    return _jobs;
}

void Workload::add_job_from_redis(RedisStorage & storage, const string &job_id, double submission_time)
{
    string job_json_desc_str = storage.get_job_json_string(job_id);
    PPK_ASSERT_ERROR(job_json_desc_str != "", "Cannot retrieve job '%s'", job_id.c_str());

    Job * job = job_from_json_description_string(job_json_desc_str);
    job->id = job_id;
    job->submission_time = submission_time;

    // Let's apply the RJMS delay on the job
    job->walltime += _rjms_delay;

    PPK_ASSERT_ERROR(_jobs.count(job_id) == 0, "Job '%s' already exists in the Workload", job_id.c_str());
    _jobs[job_id] = job;
}

void Workload::add_job_from_json_object(const Value &object, const string & job_id, double submission_time)
{
    CLOG_F(CCU_DEBUG_ALL,"here");
    Job * job = job_from_json_object(object["job"],object["profile"]);
    CLOG_F(CCU_DEBUG_ALL,"here");
    job->id = job_id;
    //this submission time is now gathered from job_from_json_object(), and potentially changed to original_submit
    //job->submission_time = submission_time;


    // Let's apply the RJMS delay on the job
    job->walltime += _rjms_delay;

    PPK_ASSERT_ERROR(_jobs.count(job_id) == 0, "Job '%s' already exists in the Workload", job_id.c_str());
    _jobs[job_id] = job;
}

void Workload::add_job_from_json_description_string(const string &json_string, const string &job_id, double submission_time)
{
    Job * job = job_from_json_description_string(json_string);
    job->id = job_id;
    //this submission time is now gathered from job_from_json_object(), and potentially changed to original_submit
    //job->submission_time = submission_time;

    // Let's apply the RJMS delay on the job
    job->walltime += _rjms_delay;

    PPK_ASSERT_ERROR(_jobs.count(job_id) == 0, "Job '%s' already exists in the Workload", job_id.c_str());
    _jobs[job_id] = job;
}

void Workload::put_file_into_buffer(const string &filename)
{
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open())
        throw runtime_error("Cannot open file '" + filename + "'");

    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    vector<char> buffer(size);
    if (!file.read(buffer.data(), size))
        throw runtime_error("Cannot read file '" + filename + "'");

    _fileContents = new char[size + 1];
    memcpy(_fileContents, buffer.data(), size);
    _fileContents[size] = '\0';
}

void Workload::deallocate()
{
    // Let's delete all allocated jobs
    for (auto& mit : _jobs)
    {
        delete mit.second;
    }

    _jobs.clear();

    delete _fileContents;
    _fileContents = nullptr;
}

Job* Workload::job_from_json_description_string(const string &json_string)
{
    Document document;

    if (document.Parse(json_string.c_str()).HasParseError())
        throw runtime_error("Invalid json string '" + json_string + "'");

    return job_from_json_object(document);
}

Job *Workload::job_from_json_object(const Value &object)
{
    CLOG_F(CCU_DEBUG_ALL,"here");
    PPK_ASSERT_ERROR(object.IsObject(), "Invalid json object: not an object");

    PPK_ASSERT_ERROR(object.HasMember("id"), "Invalid json object: no 'id' member");
    PPK_ASSERT_ERROR(object["id"].IsString(), "Invalid json object: 'id' member is not a string");
    PPK_ASSERT_ERROR(object.HasMember("res"), "Invalid json object: no 'res' member");
    PPK_ASSERT_ERROR(object["res"].IsInt(), "Invalid json object: 'res' member is not an integer");
    CLOG_F(CCU_DEBUG_ALL,"here");
    Job * j = new Job;
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->id = object["id"].GetString();
    CLOG_F(CCU_DEBUG_ALL,"here");
    //start out with walltime = -1 and has_walltime = true, will change as needed
    j->walltime = -1;
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->has_walltime = true;
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->nb_requested_resources = object["res"].GetInt();
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->unique_number = _job_number++;
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->checkpoint_interval = -1.0;
    CLOG_F(CCU_DEBUG_ALL,"here");
    PPK_ASSERT_ERROR(object.HasMember("from_workload"),"%s: job '%s' has no 'from_workload' field",j->id.c_str());
    j->from_workload = object["from_workload"].GetBool();
    PPK_ASSERT_ERROR(object.HasMember("subtime"), "%s: job '%s' has no 'subtime' field",j->id.c_str());
    j->submission_time = object["subtime"].GetDouble();
    if (start_from_checkpoint->started_from_checkpoint)
    {
        CLOG_F(CCU_DEBUG_ALL,"here");
        j->checkpoint_job_data = new batsched_tools::checkpoint_job_data();
        PPK_ASSERT_ERROR(object.HasMember("allocation"), "%s: job '%s' has no 'allocation' field"
        ", but we are starting-from-checkpoint",j->id.c_str());
        std::string interval = object["allocation"].GetString();
        if (interval != "null")
            j->checkpoint_job_data->allocation = IntervalSet::from_string_hyphen(interval);
        CLOG_F(CCU_DEBUG_ALL,"here");
        PPK_ASSERT_ERROR(object.HasMember("progress"), "%s: job '%s' has no 'progress' field"
        ", but we are starting-from-checkpoint",j->id.c_str());
        j->checkpoint_job_data->progress = object["progress"].GetDouble();
        CLOG_F(CCU_DEBUG_ALL,"here");
        PPK_ASSERT_ERROR(object.HasMember("state"), "%s: job '%s' has no 'state' field"
        ", but we are starting-from-checkpoint",j->id.c_str());
        int state = object["state"].GetInt();
        j->checkpoint_job_data->state = static_cast<batsched_tools::JobState>(state);
        CLOG_F(CCU_DEBUG_ALL,"here");
        PPK_ASSERT_ERROR(object.HasMember("jitter"), "%s: job '%s' has no 'jitter' field"
        ", but we are starting-from-checkpoint",j->id.c_str());
        j->checkpoint_job_data->jitter = object["jitter"].GetString();
        CLOG_F(CCU_DEBUG_ALL,"here");
        PPK_ASSERT_ERROR(object.HasMember("runtime"), "%s: job '%s' has no 'runtime' field"
        ", but we are starting-from-checkpoint",j->id.c_str());
        j->checkpoint_job_data->runtime = object["runtime"].GetDouble();
        CLOG_F(CCU_DEBUG_ALL,"here");
        //this is so sorting by submit time will work whether you use the FCFS or ORIGINAL-FCFS queuing policy
        //basically the scheduler doesn't need to know the submit time in the start-from-checkpoint simulation, it needs to know the original
        
        
    }
    PPK_ASSERT_ERROR(object.HasMember("original_submit"), "%s: job '%s' has no 'original_submit' field",j->id.c_str());
    j->original_submit = object["original_submit"].GetDouble();
    CLOG_F(CCU_DEBUG_ALL,"here");
    if (object.HasMember("walltime"))
    {
        PPK_ASSERT_ERROR(object["walltime"].IsNumber(), "Invalid json object: 'walltime' member is not a number");
        
        j->walltime = object["walltime"].GetDouble();
        CLOG_F(CCU_DEBUG_ALL,"walltime %g",(double)j->walltime);
    }

    PPK_ASSERT_ERROR(j->walltime == -1 || j->walltime > 0,
                     "Invalid json object: 'walltime' should either be -1 (no walltime) "
                     "or strictly positive.");
    if (object.HasMember("original_walltime"))
    {
        PPK_ASSERT_ERROR(object["original_walltime"].IsNumber(), "Invalid json object: 'original_walltime' member is not a number");
        
        j->original_walltime = object["original_walltime"].GetDouble();
    }

    if (j->walltime == -1)
        j->has_walltime = false;
    if (object.HasMember("cores"))
    {
        PPK_ASSERT_ERROR(object["cores"].IsInt(), "Invalid json object: 'cores' member is not an integer");
        j->cores = object["cores"].GetInt();
    }
    if (object.HasMember("purpose"))
    {
        PPK_ASSERT_ERROR(object["purpose"].IsString(), "Invalid json object: 'purpose' member is not a string");
        j->purpose = object["purpose"].GetString();
    }
    if (object.HasMember("start"))
    {
        PPK_ASSERT_ERROR(object["start"].IsNumber(), "Invalid json object: 'start' member is not a number");
        j->start = object["start"].GetDouble();
    }
    if (object.HasMember("profile"))
    {

    }
    if (object.HasMember("alloc"))
    {
        PPK_ASSERT_ERROR(object["alloc"].IsString(), "Invalid json object: 'alloc' member is not a string");
        j->future_allocations = IntervalSet::from_string_hyphen(object["alloc"].GetString()," ","-");
    }
    else
        j->future_allocations = IntervalSet::empty_interval_set(); //make this empty if no allocation
    
    if (object.HasMember("submission_times"))
    {
        const Value & submission_times = object["submission_times"];
        for (const auto& time : submission_times.GetArray())
            j->submission_times.push_back(time.GetDouble());
    }
    if (object.HasMember("dumptime"))
    {
        j->dump_time = object["dumptime"].GetDouble();
    }
    if (object.HasMember("readtime"))
    {
        j->read_time = object["readtime"].GetDouble();
    }
    if (object.HasMember("checkpoint_interval"))
    {
        j->checkpoint_interval = object["checkpoint_interval"].GetDouble();
    }
    StringBuffer buffer;
    rapidjson::Writer<StringBuffer> writer(buffer);
    object.Accept(writer);

    j->json_description = buffer.GetString();
    
    return j;
}
Job *Workload::job_from_json_object(const Value &job_object,const Value &profile_object)
{
    Job * j = job_from_json_object(job_object);
    CLOG_F(CCU_DEBUG_ALL,"here");
    j->profile = myBatsched::Profile::from_json(j->id,profile_object);
    return j;
}



std::string JobAlloc::to_string()const
{
    return batsched_tools::to_string(this);

}
