#include "protocol.hpp"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "pempek_assert.hpp"

using namespace rapidjson;
using namespace std;

JsonProtocolWriter::JsonProtocolWriter() :
    _alloc(_doc.GetAllocator())
{
    _doc.SetObject();
}

JsonProtocolWriter::~JsonProtocolWriter()
{

}

void JsonProtocolWriter::append_query_consumed_energy(double date)
{
    /* {
      "timestamp": 10.0,
      "type": "QUERY",
      "data": {
        "requests": {"consumed_energy": {}}
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value empty_object(rapidjson::kObjectType);
    Value requests_object(rapidjson::kObjectType);
    requests_object.AddMember("consumed_energy", empty_object, _alloc);

    Value data(rapidjson::kObjectType);
    data.AddMember("requests", requests_object, _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("QUERY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_answer_estimate_waiting_time(const string &job_id,
                                                             double estimated_waiting_time,
                                                             double date)
{
    /* {
      "timestamp": 10.0,
      "type": "ANSWER",
      "data": {
        "estimate_waiting_time": {
          "job_id": "workflow_submitter0!potential_job17",
          "estimated_waiting_time": 56
        }
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value estimation(rapidjson::kObjectType);
    estimation.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    estimation.AddMember("estimated_waiting_time", Value().SetDouble(estimated_waiting_time), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("ANSWER"), _alloc);
    event.AddMember("data", Value().SetObject().AddMember("estimate_waiting_time", estimation, _alloc), _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_register_job(const string &job_id,
                                           double date,
                                           const string &job_description,
                                           const string &profile_description,
                                           bool send_profile)
{
    /* Without redis: {
      "timestamp": 10.0,
      "type": "REGISTER_JOB",
      "data": {
        "job_id": "w12!45",
      }
    }
    With redis: {
      "timestamp": 10.0,
      "type": "REGISTER_JOB",
      "data": {
        "job_id": "dyn!my_new_job",
        "job":{
          "profile": "delay_10s",
          "res": 1,
          "id": "my_new_job",
          "walltime": 12.0
        },
        "profile":{
          "type": "delay",
          "delay": 10
        }
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    (void) job_description;
    (void) profile_description;

    if (!job_description.empty())
    {
        Document job_doc;
        job_doc.Parse(job_description.c_str());
        PPK_ASSERT_ERROR(!job_doc.HasParseError(), "Invalid JSON job ###%s###",
                         job_description.c_str());

        data.AddMember("job", Value().CopyFrom(job_doc, _alloc), _alloc);
    }

    if (!profile_description.empty() && send_profile)
    {
        Document profile_doc;
        profile_doc.Parse(profile_description.c_str());
        PPK_ASSERT_ERROR(!profile_doc.HasParseError(), "Invalid JSON profile ###%s###",
                         profile_description.c_str());

        data.AddMember("profile", Value().CopyFrom(profile_doc, _alloc), _alloc);
    }

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("REGISTER_JOB"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_register_profile(const string &workload_name,
                                               const string &profile_name,
                                               const string &profile_description,
                                               double date)
{
    /* {
      "timestamp": 10.0,
      "type": "REGISTER_PROFILE",
      "data": {
        "workload_name": "dyn_wl1",
        "profile_name":  "delay_10s",
        "profile": {
          "type": "delay",
          "delay": 10
        }
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("workload_name", Value().SetString(workload_name.c_str(), _alloc), _alloc);
    data.AddMember("profile_name", Value().SetString(profile_name.c_str(), _alloc), _alloc);

    PPK_ASSERT_ERROR(!profile_description.empty());
    {
        Document profile_doc;
        profile_doc.Parse(profile_description.c_str());
        PPK_ASSERT_ERROR(!profile_doc.HasParseError(), "Invalid JSON profile ###%s###",
                         profile_description.c_str());

        data.AddMember("profile", Value().CopyFrom(profile_doc, _alloc), _alloc);
    }

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("REGISTER_PROFILE"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_execute_job(const string &job_id,
                                            const IntervalSet &allocated_resources,
                                            double date,
                                            const vector<int> & executor_to_allocated_resource_mapping)
{
    /* {
      "timestamp": 10.0,
      "type": "EXECUTE_JOB",
      "data": {
        "job_id": "w12!45",
        "alloc": "2-3",
        "mapping": {"0": "0", "1": "0", "2": "1", "3": "1"}
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    data.AddMember("alloc", Value().SetString(allocated_resources.to_string_hyphen(" ", "-").c_str(), _alloc), _alloc);

    if (!executor_to_allocated_resource_mapping.empty())
    {
        Value mapping_object(rapidjson::kObjectType);

        for (int i = 0; i < (int) executor_to_allocated_resource_mapping.size(); ++i)
        {
            string executor_str = std::to_string(i);
            string resource_str = std::to_string(executor_to_allocated_resource_mapping[i]);
            mapping_object.AddMember(Value().SetString(executor_str.c_str(), _alloc),
                                     Value().SetString(resource_str.c_str(), _alloc), _alloc);
        }

        data.AddMember("mapping", mapping_object, _alloc);
    }

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("EXECUTE_JOB"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_reject_job(const string &job_id,
                                           double date)
{
    /* {
      "timestamp": 10.0,
      "type": "REJECT_JOB",
      "data": { "job_id": "w12!45" }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("REJECT_JOB"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_kill_job(const vector<string> &job_ids,
                                         double date)
{
    /* {
      "timestamp": 10.0,
      "type": "KILL_JOB",
      "data": {"job_ids": ["w0!1", "w0!2"]}
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value job_ids_array(rapidjson::kArrayType);
    job_ids_array.Reserve(job_ids.size(), _alloc);
    for (const string & job_id : job_ids)
        job_ids_array.PushBack(Value().SetString(job_id.c_str(), _alloc), _alloc);

    Value data(rapidjson::kObjectType);
    data.AddMember("job_ids", job_ids_array, _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("KILL_JOB"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_set_resource_state(IntervalSet resources,
                                                   const string &new_state,
                                                   double date)
{
    /* {
      "timestamp": 10.0,
      "type": "SET_RESOURCE_STATE",
      "data": {"resources": "1 2 3-5", "state": "42"}
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("resources", Value().SetString(resources.to_string_hyphen(" ", "-").c_str(), _alloc), _alloc);
    data.AddMember("state", Value().SetString(new_state.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SET_RESOURCE_STATE"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_set_job_metadata(const string & job_id,
                                                 const string & metadata,
                                                 double date)
{
    /* {
      "timestamp": 13.0,
      "type": "SET_JOB_METADATA",
      "data": {
        "job_id": "wload!42",
        "metadata": "scheduler-defined string"
      }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    data.AddMember("metadata", Value().SetString(metadata.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SET_JOB_METADATA"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_call_me_later(double future_date,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "CALL_ME_LATER",
      "data": {"timestamp": 25.5}
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("timestamp", Value().SetDouble(future_date), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("CALL_ME_LATER"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_scheduler_finished_submitting_jobs(double date)
{
    /* {
      "timestamp": 42.0,
      "type": "NOTIFY",
      "data": { "type": "registration_finished" }
    } */

    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("type", Value().SetString("registration_finished", _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("NOTIFY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}



void JsonProtocolWriter::clear()
{
    _is_empty = true;

    _doc.RemoveAllMembers();
    _events.SetArray();
}

string JsonProtocolWriter::generate_current_message(double date)
{
    PPK_ASSERT_ERROR(date >= _last_date, "Date inconsistency");
    PPK_ASSERT_ERROR(_events.IsArray(),
               "Successive calls to JsonProtocolWriter::generate_current_message without calling "
               "the clear() method is not supported");

    // Generating the content
    _doc.AddMember("now", Value().SetDouble(date), _alloc);
    _doc.AddMember("events", _events, _alloc);

    // Dumping the content to a buffer
    StringBuffer buffer;
    Writer<rapidjson::StringBuffer> writer(buffer);
    _doc.Accept(writer);

    // Returning the buffer as a string
    return string(buffer.GetString(), buffer.GetSize());
}

bool JsonProtocolWriter::is_empty()
{
    return _is_empty;
}

double JsonProtocolWriter::last_date()
{
    return _last_date;
}

AbstractProtocolWriter::~AbstractProtocolWriter() {}
