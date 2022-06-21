#include "json_workload.hpp"

#include <stdexcept>
#include <fstream>
#include <vector>
#include <limits>
#include <loguru.hpp>

#include <rapidjson/document.h>

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
    Job * job = job_from_json_object(object);
    job->id = job_id;
    job->submission_time = submission_time;

    // Let's apply the RJMS delay on the job
    job->walltime += _rjms_delay;

    PPK_ASSERT_ERROR(_jobs.count(job_id) == 0, "Job '%s' already exists in the Workload", job_id.c_str());
    _jobs[job_id] = job;
}

void Workload::add_job_from_json_description_string(const string &json_string, const string &job_id, double submission_time)
{
    Job * job = job_from_json_description_string(json_string);
    job->id = job_id;
    job->submission_time = submission_time;

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
    PPK_ASSERT_ERROR(object.IsObject(), "Invalid json object: not an object");

    PPK_ASSERT_ERROR(object.HasMember("id"), "Invalid json object: no 'id' member");
    PPK_ASSERT_ERROR(object["id"].IsString(), "Invalid json object: 'id' member is not a string");
    PPK_ASSERT_ERROR(object.HasMember("res"), "Invalid json object: no 'res' member");
    PPK_ASSERT_ERROR(object["res"].IsInt(), "Invalid json object: 'res' member is not an integer");

    Job * j = new Job;
    j->id = object["id"].GetString();
    j->walltime = -1;
    j->has_walltime = true;
    j->nb_requested_resources = object["res"].GetInt();
    j->unique_number = _job_number++;

    if (object.HasMember("walltime"))
    {
        PPK_ASSERT_ERROR(object["walltime"].IsNumber(), "Invalid json object: 'walltime' member is not a number");
        
        j->walltime = object["walltime"].GetDouble();
        LOG_F(INFO,"walltime %g",(double)j->walltime);
    }

    PPK_ASSERT_ERROR(j->walltime == -1 || j->walltime > 0,
                     "Invalid json object: 'walltime' should either be -1 (no walltime) "
                     "or strictly positive.");

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
    if (object.HasMember("alloc"))
    {
        PPK_ASSERT_ERROR(object["alloc"].IsString(), "Invalid json object: 'alloc' member is not a string");
        j->future_allocations = IntervalSet::from_string_hyphen(object["alloc"].GetString()," ","-");
    }

    return j;
}

bool JobComparator::operator()(const Job *j1, const Job *j2) const
{
    return j1->id < j2->id;
}
