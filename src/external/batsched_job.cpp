#include <string>
#include <fstream>
#include <streambuf>
#include <algorithm>
#include <regex>
#include <math.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <loguru.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include "batsched_job.hpp"
#include "batsched_profile.hpp"
#include "batsched_workload.hpp"

using namespace rapidjson;
namespace myBatsched{
Jobs::~Jobs()
{
    _jobs.clear();
}

JobIdentifier::JobIdentifier(const std::string & workload_name,
                             const std::string & job_name) :
    _workload_name(workload_name),
    _job_name(job_name)
{
    _representation = representation();
}

JobIdentifier::JobIdentifier(const std::string & job_id_str)
{
    // Split the job_identifier by '!'
    std::vector<std::string> job_identifier_parts;
    boost::split(job_identifier_parts, job_id_str,
                 boost::is_any_of("!"), boost::token_compress_on);

    CHECK_F(job_identifier_parts.size() == 2,
               "Invalid string job identifier '%s': should be formatted as two '!'-separated "
               "parts, the second one being any string without '!'. Example: 'some_text!42'.",
               job_id_str.c_str());

    this->_workload_name = job_identifier_parts[0];
    this->_job_name = job_identifier_parts[1];
    _representation = representation();
}
std::string JobIdentifier::to_string() const
{
    return _representation;
}
const char *JobIdentifier::to_cstring() const
{
    return _representation.c_str();
}
std::string JobIdentifier::representation() const
{
    return _workload_name + '!' + _job_name;
}
bool operator<(const JobIdentifier &ji1, const JobIdentifier &ji2)
{
    return ji1.to_string() < ji2.to_string();
}

bool operator==(const JobIdentifier &ji1, const JobIdentifier &ji2)
{
    return ji1.to_string() == ji2.to_string();
}
std::size_t JobIdentifierHasher::operator()(const JobIdentifier & id) const
{
    return std::hash<std::string>()(id.to_string());
}
std::string JobIdentifier::workload_name() const
{
    return _workload_name;
}

std::string JobIdentifier::job_name() const
{
    return _job_name;
}

void Jobs::set_profiles(Profiles *profiles)
{
    _profiles = profiles;
}

void Jobs::set_workload(Workload *workload)
{
    _workload = workload;
}
void Jobs::add_job(JobPtr job)
{
 _jobs[job->id]=job;
 _jobs_met.insert({job->id,true}); // not sure about this, whether it's really needed or not    
    
}
void Jobs::load_from_json(const rapidjson::Document &doc, const std::string &filename)
{
    std::string error_prefix = "Invalid JSON file '" + filename + "'";

    CHECK_F(doc.IsObject(), "%s: not a JSON object", error_prefix.c_str());
    CHECK_F(doc.HasMember("jobs"), "%s: the 'jobs' array is missing", error_prefix.c_str());
    const Value & jobs = doc["jobs"];
    if (jobs.IsArray())
    {

        for (SizeType i = 0; i < jobs.Size(); i++) // Uses SizeType instead of size_t
        {
            //const Value & job_json_description = jobs[i];
            std::string job_desc = jobs[i].GetString();
            LOG_F(INFO,"our string: %s",job_desc.c_str());
            job_desc=Job::not_escaped(job_desc);
            LOG_F(INFO,"after not_escaped");
            Document d;
            d.Parse(job_desc.c_str());
            const rapidjson::Value & job_json_description = d.GetObject();
            auto j = Job::from_json(job_json_description, _workload, error_prefix);
            LOG_F(INFO,"after from_json");
            CHECK_F(!exists(j->id), "%s: duplication of job id '%s'",
                    error_prefix.c_str(), j->id.to_string().c_str());
            _jobs[j->id] = j;
            _jobs_met.insert({j->id, true});
        }
    }
    
}

std::string Job::not_escaped(const std::string& input)
{
    std::string output;
    output.reserve(input.size());
    for (const char c: input) {
        switch (c) {
            case '\\':  output += "";        break;
            default:    output += c;            break;
        }
    }

    return output;
}
std::string Job::to_json_desc(rapidjson::Document * doc)
{
  rapidjson::StringBuffer buffer;

  buffer.Clear();

  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc->Accept(writer);

  return std::string( buffer.GetString() );
}
JobPtr Job::from_json(const rapidjson::Value & json_desc,
                     Workload * workload,
                     const std::string & error_prefix)
{
        
    // Create and initialize with default values
    auto j =std::make_shared<Job>();
    j->workload = workload;
    j->starting_time = -1;
    j->runtime = -1;
    j->state = JobState::JOB_STATE_NOT_SUBMITTED;
    j->cores = 1;
    j->purpose = "job";
    j->start = -1.0;
    j->future_allocation;
    

    CHECK_F(json_desc.IsObject(), "%s: one job is not an object", error_prefix.c_str());

    // Get job id and create a JobIdentifier
    CHECK_F(json_desc.HasMember("id"), "%s: one job has no 'id' field", error_prefix.c_str());
    CHECK_F(json_desc["id"].IsString() || json_desc["id"].IsInt(), "%s: on job id field is invalid, it should be a string or an integer", error_prefix.c_str());
    std::string job_id_str;
    if (json_desc["id"].IsString())
    {
        job_id_str = json_desc["id"].GetString();
    }
    else if (json_desc["id"].IsInt())
    {
        job_id_str = std::to_string(json_desc["id"].GetInt());
    }

    if (job_id_str.find(workload->name) == std::string::npos)
    {
        // the workload name is not present in the job id string
        j->id = JobIdentifier(workload->name, job_id_str);
    }
    else
    {
        j->id = JobIdentifier(job_id_str);
    }

    // Get submission time
    CHECK_F(json_desc.HasMember("subtime"), "%s: job '%s' has no 'subtime' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    CHECK_F(json_desc["subtime"].IsNumber(), "%s: job '%s' has a non-number 'subtime' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    j->submission_time = static_cast<long double>(json_desc["subtime"].GetDouble());

    // Get walltime (optional)
    if (!json_desc.HasMember("walltime"))
    {
        LOG_F(INFO,"job '%s' has no 'walltime' field", j->id.to_string().c_str());
    }
    else
    {
        CHECK_F(json_desc["walltime"].IsNumber(), "%s: job %s has a non-number 'walltime' field",
                   error_prefix.c_str(), j->id.to_string().c_str());
        j->walltime = static_cast<long double>(json_desc["walltime"].GetDouble());
    }
    CHECK_F(j->walltime == -1 || j->walltime > 0,
               "%s: job '%s' has an invalid walltime (%Lg). It should either be -1 (no walltime) "
               "or a strictly positive number.",
               error_prefix.c_str(), j->id.to_string().c_str(), j->walltime);

    // Get number of requested resources
    CHECK_F(json_desc.HasMember("res"), "%s: job %s has no 'res' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    CHECK_F(json_desc["res"].IsInt(), "%s: job %s has a non-number 'res' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    CHECK_F(json_desc["res"].GetInt() >= 0, "%s: job %s has a negative 'res' field (%d)",
               error_prefix.c_str(), j->id.to_string().c_str(), json_desc["res"].GetInt());
    j->requested_nb_res = static_cast<unsigned int>(json_desc["res"].GetInt());

    // Get the job profile
    CHECK_F(json_desc.HasMember("profile"), "%s: job %s has no 'profile' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    CHECK_F(json_desc["profile"].IsString(), "%s: job %s has a non-string 'profile' field",
               error_prefix.c_str(), j->id.to_string().c_str());

    // TODO raise exception when the profile does not exist.
    std::string profile_name = json_desc["profile"].GetString();
    CHECK_F(workload->profiles->exists(profile_name), "%s: the profile %s for job %s does not exist",
               error_prefix.c_str(), profile_name.c_str(), j->id.to_string().c_str());
    j->profile = workload->profiles->at(profile_name);
    
    if (json_desc.HasMember("cores"))
    {
        CHECK_F(json_desc["cores"].IsInt(), "%s: job %s has a non-number 'cores' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        CHECK_F(json_desc["cores"].GetInt() >= 0, "%s: job %s has a negative 'cores' field (%d)",
               error_prefix.c_str(), j->id.to_string().c_str(), json_desc["cores"].GetInt());
        j->cores = json_desc["cores"].GetInt();
        
    }
     if (json_desc.HasMember("purpose"))
    {
        CHECK_F(json_desc["purpose"].IsString(), "%s: job %s has a non-string 'purpose' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        j->purpose = json_desc["purpose"].GetString();
        
    }
    if (json_desc.HasMember("start"))
    {
        CHECK_F(json_desc["start"].IsNumber(), "%s: job %s has a non-number 'start' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        j->start = json_desc["start"].GetDouble();
    }
    if (json_desc.HasMember("alloc"))
    {
        CHECK_F(json_desc["alloc"].IsString(), "%s: job %s has a non-string 'alloc' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        j->future_allocation = IntervalSet::from_string_hyphen(json_desc["alloc"].GetString()," ","-");
    }
    if(workload->_checkpointing_on)
    {
    
        //***** got rid of CHECK_F statements as checkpointing is optional so, so are the fields
        /*CHECK_F(json_desc.HasMember("checkpoint"),"%s: job %s has no 'checkpoint' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        CHECK_F(json_desc["checkpoint"].IsNumber(), "%s: job %s has non double 'checkpoint' field",
                error_prefix.c_str(),j->id.to_string().c_str()); */
        if (json_desc.HasMember("checkpoint") && json_desc["checkpoint"].IsNumber())
            j->checkpoint_interval=json_desc["checkpoint"].GetDouble();
        /*
        CHECK_F(json_desc.HasMember("dumptime"),"%s: job %s has no 'dumptime' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        CHECK_F(json_desc["dumptime"].IsNumber(), "%s: job %s has non double 'dumptime' field",
                error_prefix.c_str(),j->id.to_string().c_str());   */
        if (json_desc.HasMember("dumptime") && json_desc["dumptime"].IsNumber())
            j->dump_time=json_desc["dumptime"].GetDouble();
        /*
        CHECK_F(json_desc.HasMember("readtime"),"%s: job %s has no 'readtime' field",
                error_prefix.c_str(),j->id.to_string().c_str());
        CHECK_F(json_desc["readtime"].IsNumber(), "%s: job %s has non double 'readtime' field",
                error_prefix.c_str(),j->id.to_string().c_str()); */
        if (json_desc.HasMember("readtime") && json_desc["readtime"].IsNumber())
            j->read_time=json_desc["readtime"].GetDouble();
        
        /*if (j->id.job_name().find("#")== std::string::npos)
        {    
            Document profile_doc;
            profile_doc.Parse(j->profile->json_description.c_str());
            
            DelayProfileData * data =static_cast<DelayProfileData *>(j->profile->data);
            double delay = data->delay;
            int subtract = 0;
            data->real_delay = delay;
            if (std::fmod(delay,j->checkpoint_interval) == 0)
                subtract = 1;
            delay = (floor(delay / j->checkpoint_interval) - subtract )* j->dump_time + delay;
            data->delay = delay;
            j->profile->data = data;
            profile_doc["delay"]=delay;
            j->profile->json_description = Job::to_json_desc(& profile_doc);
           LOG_F(INFO,"DEBUG delay: %f",delay);
           LOG_F(INFO,"DEBUG profile data %f",(static_cast<DelayProfileData *>(j->profile->data))->delay);
        }
        }*/
    
    }
    LOG_F(INFO,"Profile name %s and '%s'", profile_name.c_str(), j->profile->name.c_str());
    // Let's get the JSON string which originally described the job
    // (to conserve potential fields unused by Batsim)
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json_desc.Accept(writer);

    // Let's replace the job ID by its WLOAD!NUMBER counterpart if needed
    // in the json raw description
    std::string json_description_tmp(buffer.GetString(), buffer.GetSize());
    /// @cond DOXYGEN_FAILS_PARSING_THIS_REGEX
    std::regex r(R"("id"\s*:\s*(?:"*[^(,|})]*"*)\s*)");
    /// @endcond
    std::string replacement_str = "\"id\":\"" + j->id.to_string() + "\"";
    // LOG_F(INFO,"Before regexp: %s", json_description_tmp.c_str());
    j->json_description = std::regex_replace(json_description_tmp, r, replacement_str);

    // Let's check that the new description is a valid JSON string
    rapidjson::Document check_doc;
    check_doc.Parse(j->json_description.c_str());
    CHECK_F(!check_doc.HasParseError(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart:"
               "The output string '%s' is not valid JSON.", j->json_description.c_str());
    CHECK_F(check_doc.IsObject(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output string '%s' is not valid JSON.", j->json_description.c_str());
    CHECK_F(check_doc.HasMember("id"),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'id' field.", j->json_description.c_str());
    CHECK_F(check_doc["id"].IsString(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has a non-string 'id' field.", j->json_description.c_str());
    CHECK_F(check_doc.HasMember("subtime") && check_doc["subtime"].IsNumber(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'subtime' field (or it is not a number)",
               j->json_description.c_str());
    CHECK_F((check_doc.HasMember("walltime") && check_doc["walltime"].IsNumber())
               || (!check_doc.HasMember("walltime")),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'walltime' field (or it is not a number)",
               j->json_description.c_str());
    CHECK_F(check_doc.HasMember("res") && check_doc["res"].IsInt(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'res' field (or it is not an integer)",
               j->json_description.c_str());
    CHECK_F(check_doc.HasMember("profile") && check_doc["profile"].IsString(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'profile' field (or it is not a string)",
               j->json_description.c_str());

    if (json_desc.HasMember("smpi_ranks_to_hosts_mapping"))
    {
        CHECK_F(json_desc["smpi_ranks_to_hosts_mapping"].IsArray(),
                "%s: job '%s' has a non-array 'smpi_ranks_to_hosts_mapping' field",
                error_prefix.c_str(), j->id.to_string().c_str());

        const auto & mapping_array = json_desc["smpi_ranks_to_hosts_mapping"];
        j->smpi_ranks_to_hosts_mapping.resize(mapping_array.Size());

        for (unsigned int i = 0; i < mapping_array.Size(); ++i)
        {
            CHECK_F(mapping_array[i].IsInt(),
                       "%s: job '%s' has a bad 'smpi_ranks_to_hosts_mapping' field: rank "
                       "%d does not point to an integral number",
                       error_prefix.c_str(), j->id.to_string().c_str(), i);
            int host_number = mapping_array[i].GetInt();
            CHECK_F(host_number >= 0 && static_cast<unsigned int>(host_number) < j->requested_nb_res,
                       "%s: job '%s' has a bad 'smpi_ranks_to_hosts_mapping' field: rank "
                       "%d has an invalid value %d : should be in [0,%d[",
                       error_prefix.c_str(), j->id.to_string().c_str(),
                       i, host_number, j->requested_nb_res);

            j->smpi_ranks_to_hosts_mapping[i] = host_number;
        }
    }

    LOG_F(INFO,"Job '%s' Loaded", j->id.to_string().c_str());
    return j;
}

// Do NOT remove namespaces in the arguments (to avoid doxygen warnings)
JobPtr Job::from_json(const std::string & json_str,
                     Workload * workload,
                     const std::string & error_prefix)
{
    
    Document doc;
    doc.Parse(json_str.c_str());
    LOG_F(INFO,"after parse");
    CHECK_F(!doc.HasParseError(),
               "%s: Cannot be parsed. Content (between '##'):\n#%s#",
               error_prefix.c_str(), json_str.c_str());

    return Job::from_json(doc, workload, error_prefix);
}
int Jobs::nb_jobs() const
{
    return static_cast<int>(_jobs.size());
}
bool Jobs::exists(const JobIdentifier & job_id) const
{
    auto it = _jobs_met.find(job_id);
    return it != _jobs_met.end();
}
std::unordered_map<JobIdentifier, JobPtr,JobIdentifierHasher> &Jobs::jobs()
{
    return _jobs;
}

JobPtr Jobs::operator[](JobIdentifier job_id)
{
    auto it = _jobs.find(job_id);
    CHECK_F(it != _jobs.end(), "Cannot get job '%s': it does not exist",
               job_id.to_cstring());
    return it->second;
}
void Jobs::delete_job(const JobIdentifier & job_id, const bool & garbage_collect_profiles)
{
    CHECK_F(exists(job_id),
               "Bad Jobs::delete_job call: The job with name='%s' does not exist.",
               job_id.to_cstring());

    std::string profile_name = _jobs[job_id]->profile->name;
    _jobs.erase(job_id);
    if (garbage_collect_profiles)
    {
        _workload->profiles->remove_profile(profile_name);
    }
}

const JobPtr Jobs::operator[](JobIdentifier job_id) const
{
    auto it = _jobs.find(job_id);
    CHECK_F(it != _jobs.end(), "Cannot get job '%s': it does not exist",
               job_id.to_cstring());
    return it->second;
}

JobPtr Jobs::at(JobIdentifier job_id)
{
    return operator[](job_id);
}

const JobPtr Jobs::at(JobIdentifier job_id) const
{
    return operator[](job_id);
}
}

