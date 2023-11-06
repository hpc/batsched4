#include "decision.hpp"

#include "network.hpp"
#include "pempek_assert.hpp"
#include "protocol.hpp"
#include "data_storage.hpp"
#include "batsched_tools.hpp"
#include "json_workload.hpp"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <utility>
#include <loguru.hpp>

namespace n = network;
using namespace std;

SchedulingDecision::SchedulingDecision()
{
    _proto_writer = new JsonProtocolWriter;
}

SchedulingDecision::~SchedulingDecision()
{
    delete _proto_writer;
    _proto_writer = nullptr;
}

void SchedulingDecision::add_execute_job(const std::string & job_id, const IntervalSet &machine_ids, double date, vector<int> executor_to_allocated_resource_mapping)
{
    if (executor_to_allocated_resource_mapping.size() == 0)
        _proto_writer->append_execute_job(job_id, machine_ids, date);
    else
        _proto_writer->append_execute_job(job_id, machine_ids, date, executor_to_allocated_resource_mapping);
}

void SchedulingDecision::add_reject_job(const std::string & job_id, double date)
{
    _proto_writer->append_reject_job(job_id, date);
}

void SchedulingDecision::add_kill_job(const vector<batsched_tools::Job_Message *> &job_msgs, double date)
{
    _proto_writer->append_kill_job(job_msgs, date);
}

void SchedulingDecision::add_submit_job(const string & workload_name,
                                        const string & job_id,
                                        const string & profile_name,
                                        const string & job_json_description,
                                        const string & profile_json_description,
                                        double date,
                                        bool send_profile)
{
    string complete_job_id = workload_name + '!' + job_id;

    if (_redis_enabled)
    {
        string job_key = RedisStorage::job_key(workload_name, job_id);
        string profile_key = RedisStorage::profile_key(workload_name, profile_name);

        PPK_ASSERT_ERROR(_redis != nullptr);
        _redis->set(job_key, job_json_description);
        _redis->set(profile_key, profile_json_description);

        _proto_writer->append_register_job(complete_job_id, date, "", "", send_profile);
    }
    else
        _proto_writer->append_register_job(complete_job_id, date,
                                         job_json_description,
                                         profile_json_description,
                                         send_profile);
}
void SchedulingDecision::handle_resubmission(std::unordered_map<std::string,batsched_tools::Job_Message *> jobs_killed_recently,
                                            Workload *w0,
                                            double date)
{
    
    for(const auto & killed_map:jobs_killed_recently)
    {
        /*not implementing this but it is a potential problem. see batsim/src/server.cpp server_on_kill_jobs()
        if (killed_map.second->progress == 1.0) //if the progress is 100% then it shouldn't be resubmitted
            continue;
        */
        std::string killed_job=killed_map.first;
        
    
        Job * job_to_queue = (*w0)[killed_job];

        //auto parts = batsched_tools::get_job_parts(killed_job);
        auto sep = batsched_tools::tools::separate_id(killed_job);
        
        //auto start = killed_job.find("!")+1;
        //auto end = killed_job.find("#");
        //std::string basename = (end ==std::string::npos) ? killed_job.substr(start) : killed_job.substr(start,end-start); 

        //const std::string workload_str = killed_job.substr(0,start-1); 
        
        //const std::string workload_str = killed_job.substr(0,start-1);  //used when having multiple workloads
        //get the conversion from seconds to cpu instructions
            
        //get the job identifier of the job that was killed
        //std::string jid = killed_job;
        std::string profile_jd=job_to_queue->profile->json_description;
        rapidjson::Document profile_doc;
        profile_doc.Parse(profile_jd.c_str());
        rapidjson::Document job_doc;
        job_doc.Parse(job_to_queue->json_description.c_str());
        
        LOG_F(INFO,"here");
        if (job_to_queue->profile->type == myBatsched::ProfileType::DELAY )
        {
            get_meta_data_from_delay(killed_map,profile_doc,job_doc,w0);
        }
        else if (job_to_queue->profile->type == myBatsched::ProfileType::PARALLEL_HOMOGENEOUS)
        {
            get_meta_data_from_parallel_homogeneous(killed_map,profile_doc,job_doc,w0);
        }
        LOG_F(INFO,"here");
        job_doc["subtime"]=date;
        LOG_F(INFO,"here");
        job_doc["original_submit"]=date;
        LOG_F(INFO,"here");
        job_doc["original_start"]=-1.0;
        LOG_F(INFO,"here");
        rapidjson::Document::AllocatorType & myAlloc(job_doc.GetAllocator());
        job_doc["submission_times"].PushBack(date,myAlloc);
        LOG_F(INFO,"here");

        
        /*        
        //check if resubmitted and get the next resubmission number
        int resubmit = 1;
        if (end!=std::string::npos) //if job name has # in it...was resubmitted b4
        {
            resubmit = std::stoi(killed_job.substr(end+1));   // then get the resubmitted number
            resubmit++; // and add 1 to it
        }
        std::string resubmit_str = std::to_string(resubmit);
        */
        
        //std::string profile_name = basename + "#" + resubmit_str;
        std::string profile_name = sep.next_profile_name;
        std::string job_name = sep.next_job_name;
        std::string job_id = sep.next_resubmit_string;
        std::string workload_name = sep.workload;
        job_doc["profile"].SetString(profile_name.data(), profile_name.size(), myAlloc);
        job_doc["id"].SetString(job_id.data(),job_id.size(),myAlloc);
        LOG_F(INFO,"here");
        std::string error_prefix = "Invalid JSON job '" + killed_job + "'";
        profile_jd = to_json_desc(&profile_doc);
        std::string job_jd = to_json_desc(&job_doc);
        //LOG_F(INFO,"workload: %s  job: %s, profile: %s",workload_name.c_str(),job_name.c_str(),profile_name.c_str());
        add_submit_profile(workload_name,
                                    profile_name,
                                    profile_jd,
                                    date);
                                    
        add_submit_job(workload_name,
                                    job_name,
                                    profile_name,
                                    job_jd,
                                    profile_jd,
                                    date,
                                    true);
        if (job_doc.HasMember("metadata"))
        {
            std::string meta = job_doc["metadata"].GetString();
            //must replace double quotes with single quotes.  Remember to
            //replace single quotes with double quotes before parsing metadata
            std::replace( meta.begin(), meta.end(), '\"', '\'');
        add_set_job_metadata(job_id,
                                        meta,
                                        date);
        }                               
    }            
 

 
}
std::string SchedulingDecision::to_json_desc(rapidjson::Document * doc){

    rapidjson::StringBuffer buffer;

    buffer.Clear();

    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc->Accept(writer);

    return std::string( buffer.GetString() );

}
 
 void SchedulingDecision::get_meta_data_from_parallel_homogeneous(std::pair<std::string,batsched_tools::Job_Message *> killed_map,
                                                                rapidjson::Document & profile_doc,
                                                                rapidjson::Document & job_doc,
                                                                Workload* w0)
 {
        double one_second = w0->_host_speed;
        std::string killed_job = killed_map.first;
        double progress = killed_map.second->progress;
        //get the job that was killed
        Job * job_to_queue =(*w0)[killed_job];
        LOG_F(INFO,"ccu chkpt_interval %f",job_to_queue->checkpoint_interval);
  
    if (w0->_checkpointing_on)
    {
        double progress_time = 0;
        if (progress > 0)
        {
            
            
            progress_time =(progress * profile_doc["cpu"].GetDouble())/one_second;
            progress_time += job_to_queue->checkpoint_job_data->runtime;

            LOG_F(INFO,"job %s progress is > 0  progress: %f  progress_time: %f",job_to_queue->id.c_str(),progress,progress_time);
            //LOG_F(INFO,"profile_doc[cpu]: %f    , one_second: %f",profile_doc["cpu"].GetDouble(),one_second);
            
            bool has_checkpointed = false;
            
            std::string meta_str = "null";
            int num_checkpoints_completed = 0;
            rapidjson::Document meta_doc;
            //check whether there is a checkpointed value and set has_checkpointed if so
            if (job_doc.HasMember("metadata"))
            {
                
                meta_str = job_doc["metadata"].GetString();
                std::replace(meta_str.begin(),meta_str.end(),'\'','\"');
                meta_doc.Parse(meta_str.c_str());
                if (meta_doc.HasMember("checkpointed"))
                {
                    has_checkpointed = meta_doc["checkpointed"].GetBool();
                  
                    
                }
            }
            //if has checkpointed we need to alter how we check num_checkpoints_completed and progress time
            if (has_checkpointed)
            {
                
                //progress_time must be subtracted by read_time to see how many checkpoints we have gone through
                num_checkpoints_completed = floor((progress_time-job_to_queue->read_time)/(job_to_queue->checkpoint_interval + job_to_queue->dump_time));
                if (meta_doc.HasMember("work_progress"))
                {
                    double work = meta_doc["work_progress"].GetDouble();
                    if (num_checkpoints_completed > 0)
                        work += num_checkpoints_completed * job_to_queue->checkpoint_interval;
                    work = work * one_second;
                    meta_doc["work_progress"] = work;
                }
                else if (num_checkpoints_completed > 0)
                {
                    meta_doc.AddMember("work_progress",rapidjson::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval*one_second),meta_doc.GetAllocator());
                }
                if (meta_doc.HasMember("num_dumps"))
                {
                        int num_dumps = meta_doc["num_dumps"].GetInt();
                        if (num_checkpoints_completed > 0)
                            num_dumps += num_checkpoints_completed;
                        meta_doc["num_dumps"] = num_dumps;
                        
                }
                else if (num_checkpoints_completed > 0)
                {
                        meta_doc.AddMember("num_dumps",rapidjson::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                    
                }
                std::string meta_str = to_json_desc(&meta_doc);
                job_doc["metadata"].SetString(meta_str.c_str(),job_doc.GetAllocator());
                // the progress_time needs to add back in the read_time
                progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time) + job_to_queue->read_time;
            
            }
            else // there hasn't been any checkpoints in the past, do normal check on num_checkpoints_completed
            {
                num_checkpoints_completed = floor(progress_time/(job_to_queue->checkpoint_interval + job_to_queue->dump_time ));
                progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time);
                LOG_F(INFO,"line 258 num_checkpoints_completed %d",num_checkpoints_completed);
                LOG_F(INFO,"progress time %f  checkpoint interval: %f dump time %f  num check: %f",
                progress_time,job_to_queue->checkpoint_interval,job_to_queue->dump_time,
                progress_time/(job_to_queue->checkpoint_interval + job_to_queue->dump_time ));
                
                //if a checkpoint has completed set the metadata to reflect this
                if (num_checkpoints_completed > 0)
                {
                    meta_doc.SetObject();
                    //if there was previous metadata make sure to include it
                    if (meta_str!="null")
                    {
                        meta_doc.Parse(meta_str.c_str());
                    }    
                    rapidjson::Document::AllocatorType& myAlloc = meta_doc.GetAllocator();
                    meta_doc.AddMember("checkpointed",rapidjson::Value().SetBool(true),myAlloc);
                    meta_doc.AddMember("num_dumps",rapidjson::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                    meta_doc.AddMember("work_progress",rapidjson::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval * one_second),meta_doc.GetAllocator());
                    std::string myString = to_json_desc(&meta_doc);
                    rapidjson::Document::AllocatorType& myAlloc2 = job_doc.GetAllocator();
                                        
                    if (meta_str=="null")
                        job_doc.AddMember("metadata",rapidjson::Value().SetString(myString.c_str(),myAlloc2),myAlloc2);
                    else
                        job_doc["metadata"].SetString(myString.c_str(),myAlloc2);
                }

            }
            myBatsched::ParallelHomogeneousProfileData * data = static_cast<myBatsched::ParallelHomogeneousProfileData *>(job_to_queue->profile->data);
            if (data->original_cpu != -1.0)
                profile_doc["cpu"]=data->original_cpu;
            if (job_to_queue->original_walltime != -1.0)
                job_doc["walltime"]=job_to_queue->original_walltime.convert_to<double>();
            //only if a new checkpoint has been reached does the delay time change
            //LOG_F(INFO,"REPAIR num_checkpoints_completed: %d",num_checkpoints_completed);
            if (num_checkpoints_completed > 0)
            {
                LOG_F(INFO,"job %s num_checkpoints_completed: %d",job_to_queue->id.c_str(),num_checkpoints_completed);
                //if we have checkpointed, the walltime can be reduced.  Reduce by the num of checkpoints. if _subtract_progress_from_walltime is false, skip
                if (job_to_queue->walltime > 0 && job_doc.HasMember("walltime") && w0->_subtract_progress_from_walltime) 
                {
                    LOG_F(INFO,"decision line 288");
                    job_doc["walltime"].SetDouble( (double)job_to_queue->walltime - 
                                         (num_checkpoints_completed * (job_to_queue->checkpoint_interval+job_to_queue->dump_time) - job_to_queue->read_time));
                }
                double cpu = profile_doc["cpu"].GetDouble();
                double cpu_time = cpu / one_second;
                cpu_time = cpu_time - progress_time + job_to_queue->read_time;
                //LOG_F(INFO,"REPAIR cpu_time: %f  readtime: %f",cpu_time,job_to_queue->read_time);
                profile_doc["cpu"].SetDouble(cpu_time*one_second);
                
            }
        }
    
    }
    else
    {
        myBatsched::ParallelHomogeneousProfileData * data = static_cast<myBatsched::ParallelHomogeneousProfileData *>(job_to_queue->profile->data);
        if (data->original_cpu != -1.0)
            profile_doc["cpu"]=data->original_cpu;
        if (job_to_queue->original_walltime != -1.0)
            job_doc["walltime"]=job_to_queue->original_walltime.convert_to<double>();
    }
}
void SchedulingDecision::get_meta_data_from_delay(std::pair<std::string,batsched_tools::Job_Message *> killed_map,
                                                rapidjson::Document & profile_doc,
                                                rapidjson::Document & job_doc,
                                                Workload * w0)
{
    double one_second = w0->_host_speed;
    std::string killed_job = killed_map.first;
    double progress = killed_map.second->progress;
    //get the job that was killed
    Job * job_to_queue =(*w0)[killed_job];

    if (w0->_checkpointing_on)
    {
        double progress_time = 0;
        if (progress > 0)
        {
            
            
            progress_time =progress * profile_doc["delay"].GetDouble();
            progress_time += job_to_queue->checkpoint_job_data->runtime;
            //LOG_F(INFO,"REPAIR progress is > 0  progress: %f  progress_time: %f",progress,progress_time);
            
            bool has_checkpointed = false;
            std::string meta_str = "null";
            int num_checkpoints_completed = 0;
            rapidjson::Document meta_doc;
            //check whether there is a checkpointed value and set has_checkpointed if so
            if (job_doc.HasMember("metadata"))
            {
                
                meta_str = job_doc["metadata"].GetString();
                std::replace(meta_str.begin(),meta_str.end(),'\'','\"');
                meta_doc.Parse(meta_str.c_str());
                if (meta_doc.HasMember("checkpointed"))
                {
                    has_checkpointed = meta_doc["checkpointed"].GetBool();
                    
                }
            }
            //if has checkpointed we need to alter how we check num_checkpoints_completed and progress time
            if (has_checkpointed)
            {
                
                //progress_time must be subtracted by read_time to see how many checkpoints we have gone through
                num_checkpoints_completed = floor((progress_time-job_to_queue->read_time)/(job_to_queue->checkpoint_interval + job_to_queue->dump_time));
                if (meta_doc.HasMember("work_progress"))
                {
                    double work = meta_doc["work_progress"].GetDouble();
                    if (num_checkpoints_completed > 0)
                        work += num_checkpoints_completed * job_to_queue->checkpoint_interval;
                    meta_doc["work_progress"] = work;
                }
                else if (num_checkpoints_completed > 0)
                {
                    meta_doc.AddMember("work_progress",rapidjson::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval),meta_doc.GetAllocator());
                }
                if (meta_doc.HasMember("num_dumps"))
                {
                        int num_dumps = meta_doc["num_dumps"].GetInt();
                        if (num_checkpoints_completed > 0)
                            num_dumps += num_checkpoints_completed;
                        meta_doc["num_dumps"] = num_dumps;
                        
                }
                else if (num_checkpoints_completed > 0)
                {
                        meta_doc.AddMember("num_dumps",rapidjson::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                    
                }
                std::string meta_str = to_json_desc(&meta_doc);
                job_doc["metadata"].SetString(meta_str.c_str(),job_doc.GetAllocator());
                // the progress_time needs to add back in the read_time
                progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time) + job_to_queue->read_time;
            
            }
            else // there hasn't been any checkpoints in the past, do normal check on num_checkpoints_completed
            {
                num_checkpoints_completed = floor(progress_time/(job_to_queue->checkpoint_interval + job_to_queue->dump_time ));
                progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time);
                
                
                //if a checkpoint has completed set the metadata to reflect this
                if (num_checkpoints_completed > 0)
                {
                    meta_doc.SetObject();
                    //if there was previous metadata make sure to include it
                    if (meta_str!="null")
                    {
                        meta_doc.Parse(meta_str.c_str());
                    }    
                    rapidjson::Document::AllocatorType& myAlloc = meta_doc.GetAllocator();
                    meta_doc.AddMember("checkpointed",rapidjson::Value().SetBool(true),myAlloc);
                    meta_doc.AddMember("num_dumps",rapidjson::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                    meta_doc.AddMember("work_progress",rapidjson::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval),meta_doc.GetAllocator());
                    std::string myString = to_json_desc(&meta_doc);
                    rapidjson::Document::AllocatorType& myAlloc2 = job_doc.GetAllocator();
                                        
                    if (meta_str=="null")
                        job_doc.AddMember("metadata",rapidjson::Value().SetString(myString.c_str(),myAlloc2),myAlloc2);
                    else
                        job_doc["metadata"].SetString(myString.c_str(),myAlloc2);
                }

            }        
            //only if a new checkpoint has been reached does the delay time change
            //LOG_F(INFO,"REPAIR num_checkpoints_completed: %d",num_checkpoints_completed);
            if (num_checkpoints_completed > 0)
            {
                //if we have checkpointed, the walltime can be reduced.  Reduce by the num of checkpoints. if _subtract_progress_from_walltime is false, skip
                if (job_to_queue->walltime > 0 && job_doc.HasMember("walltime") && w0->_subtract_progress_from_walltime) 
                {
                    job_doc["walltime"].SetDouble( (double)job_to_queue->walltime - 
                                         (num_checkpoints_completed * (job_to_queue->checkpoint_interval+job_to_queue->dump_time) - job_to_queue->read_time));
                }
                double delay = profile_doc["delay"].GetDouble() - progress_time + job_to_queue->read_time;
                //LOG_F(INFO,"REPAIR delay: %f  readtime: %f",delay,job_to_queue->read_time);
                profile_doc["delay"].SetDouble(delay);
                
                
            }
        }
    
    }
                                    
            
}

void SchedulingDecision::add_submit_profile(const string &workload_name,
                                            const string &profile_name,
                                            const string &profile_json_description,
                                            double date)
{
    _proto_writer->append_register_profile(workload_name,
                                         profile_name,
                                         profile_json_description,
                                         date);
}

void SchedulingDecision::add_set_resource_state(IntervalSet machines, int new_state, double date)
{
    _proto_writer->append_set_resource_state(machines, std::to_string(new_state), date);
}

void SchedulingDecision::add_set_job_metadata(const string &job_id,
                                              const string &metadata,
                                              double date)
{
    _proto_writer->append_set_job_metadata(job_id, metadata, date);
}

void SchedulingDecision::add_call_me_later(batsched_tools::call_me_later_types forWhat, int id,double future_date, double date,std::string job_id)
{
    _proto_writer->append_call_me_later(forWhat,_nb_call_me_laters, future_date, date);
    CALL_ME_LATERS call_me_later;
    call_me_later.forWhat = forWhat;
    call_me_later.job_id = job_id;
    call_me_later.time = future_date;
    call_me_later.id = _nb_call_me_laters;
    _call_me_laters[_nb_call_me_laters]=call_me_later;
    _nb_call_me_laters++;
}
double SchedulingDecision::remove_call_me_later(batsched_tools::call_me_later_types forWhat, int id, double date, Workload * w0)
{
    CALL_ME_LATERS call_me_later = _call_me_laters[id];
    _call_me_laters.erase(id);
    if (date > call_me_later.time)
    {
        if (call_me_later.forWhat == batsched_tools::call_me_later_types::RESERVATION_START)
            ((*w0)[call_me_later.job_id])->walltime -=( date - call_me_later.time);
        return date - call_me_later.time;
    }
    else
        return 0;
}
void SchedulingDecision::set_nb_call_me_laters(int nb)
{
    _nb_call_me_laters = nb;
}

void SchedulingDecision::add_generic_notification(const std::string &type,const std::string & notify_data,double date)
{
    _proto_writer->append_generic_notification(type,notify_data,date);
}

void SchedulingDecision::add_scheduler_finished_submitting_jobs(double date)
{
    _proto_writer->append_scheduler_finished_submitting_jobs(date);
}

void SchedulingDecision::add_scheduler_continue_submitting_jobs(double date)
{
    _proto_writer->append_scheduler_continue_submitting_jobs(date);
}

void SchedulingDecision::add_query_energy_consumption(double date)
{
    _proto_writer->append_query_consumed_energy(date);
}

void SchedulingDecision::add_answer_estimate_waiting_time(const string &job_id,
                                                          double estimated_waiting_time,
                                                          double date)
{
    _proto_writer->append_answer_estimate_waiting_time(job_id, estimated_waiting_time, date);
}

void SchedulingDecision::clear()
{
    _proto_writer->clear();
}

string SchedulingDecision::content(double date)
{
    return _proto_writer->generate_current_message(date);
}

double SchedulingDecision::last_date() const
{
    return _proto_writer->last_date();
}

void SchedulingDecision::set_redis(bool enabled, RedisStorage *redis)
{
    _redis_enabled = enabled;
    _redis = redis;
}
