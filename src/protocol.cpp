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

void JsonProtocolWriter::append_nop(double date)
{
    PPK_ASSERT(date >= _last_date, "Date inconsistency");
   _last_date = date;
   _is_empty = false;
}

void JsonProtocolWriter::append_submit_job(const string &job_id,
                                           double date,
                                           const string &job_description,
                                           const string &profile_description,
                                           bool send_profile)
{
    /* Without redis: {
      "timestamp": 10.0,
      "type": "SUBMIT_JOB",
      "data": {
        "job_id": "w12!45",
      }
    }
    With redis: {
      "timestamp": 10.0,
      "type": "SUBMIT_JOB",
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

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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
    event.AddMember("type", Value().SetString("SUBMIT_JOB"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_execute_job(const string &job_id,
                                            const MachineRange &allocated_resources,
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

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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

void JsonProtocolWriter::append_set_resource_state(MachineRange resources,
                                                   const string &new_state,
                                                   double date)
{
    /* {
      "timestamp": 10.0,
      "type": "SET_RESOURCE_STATE",
      "data": {"resources": "1 2 3-5", "state": "42"}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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

void JsonProtocolWriter::append_call_me_later(double future_date,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "CALL_ME_LATER",
      "data": {"timestamp": 25.5}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
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
      "data": { "type": "submission_finished" }
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("type", Value().SetString("submission_finished", _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("NOTIFY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_query_request(void *anything,
                                              double date)
{
    PPK_ASSERT(false, "Unimplemented!");
    (void) anything;
    (void) date;
}



void JsonProtocolWriter::append_simulation_begins(int nb_resources, double date)
{
    /* {
      "timestamp": 0.0,
      "type": "SIMULATION_BEGINS",
      "data": {}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("nb_resources", Value().SetInt(nb_resources), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SIMULATION_BEGINS"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_simulation_ends(double date)
{
    /* {
      "timestamp": 0.0,
      "type": "SIMULATION_ENDS",
      "data": {}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SIMULATION_ENDS"), _alloc);
    event.AddMember("data", Value().SetObject(), _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_job_submitted(const string & job_id,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "JOB_SUBMITTED",
      "data": {
        "job_ids": ["w0!1", "w0!2"]
      }
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_SUBMITTED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_job_completed(const string & job_id,
                                              const string & job_status,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "JOB_COMPLETED",
      "data": {"job_id": "w0!1", "status": "SUCCESS"}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    PPK_ASSERT(std::find(accepted_completion_statuses.begin(), accepted_completion_statuses.end(), job_status) != accepted_completion_statuses.end(),
               "Unsupported job status '%s'!", job_status.c_str());
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    data.AddMember("status", Value().SetString(job_status.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_COMPLETED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_job_killed(const vector<string> & job_ids,
                                           double date)
{
    /* {
      "timestamp": 10.0,
      "type": "JOB_KILLED",
      "data": {"job_ids": ["w0!1", "w0!2"]}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_KILLED"), _alloc);

    Value jobs(rapidjson::kArrayType);
    jobs.Reserve(job_ids.size(), _alloc);
    for (const string & job_id : job_ids)
        jobs.PushBack(Value().SetString(job_id.c_str(), _alloc), _alloc);

    event.AddMember("data", Value().SetObject().AddMember("job_ids", jobs, _alloc), _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_resource_state_changed(const MachineRange & resources,
                                                       const string & new_state,
                                                       double date)
{
    /* {
      "timestamp": 10.0,
      "type": "RESOURCE_STATE_CHANGED",
      "data": {"resources": "1 2 3-5", "state": "42"}
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("resources",
                   Value().SetString(resources.to_string_hyphen(" ", "-").c_str(), _alloc), _alloc);
    data.AddMember("state", Value().SetString(new_state.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("RESOURCE_STATE_CHANGED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_query_reply_energy(double consumed_energy,
                                                   double date)
{
    /* {
      "timestamp": 10.0,
      "type": "QUERY_REPLY",
      "data": {"energy_consumed": "12500" }
    } */

    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("QUERY_REPLY"), _alloc);
    event.AddMember("data", Value().SetObject().AddMember("energy_consumed", Value().SetDouble(consumed_energy), _alloc), _alloc);

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
    PPK_ASSERT(date >= _last_date, "Date inconsistency");
    PPK_ASSERT(_events.IsArray(),
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

bool test_json_writer()
{
    AbstractProtocolWriter * proto_writer = new JsonProtocolWriter;
    printf("EMPTY content:\n%s\n", proto_writer->generate_current_message(0).c_str());
    proto_writer->clear();

    proto_writer->append_simulation_begins(4, 10);
    printf("SIM_BEGINS content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_simulation_ends(10);
    printf("SIM_ENDS content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_job_submitted({"w0!j0", "w0!j1"}, 10);
    printf("JOB_SUBMITTED content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_job_completed("w0!j0", "SUCCESS", 10);
    printf("JOB_COMPLETED content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_job_killed({"w0!j0", "w0!j1"}, 10);
    printf("JOB_KILLED content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_resource_state_changed(MachineRange::from_string_hyphen("1,3-5"), "42", 10);
    printf("RESOURCE_STATE_CHANGED content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    proto_writer->append_query_reply_energy(65535, 10);
    printf("QUERY_REPLY (energy) content:\n%s\n", proto_writer->generate_current_message(42).c_str());
    proto_writer->clear();

    delete proto_writer;

    return true;
}
