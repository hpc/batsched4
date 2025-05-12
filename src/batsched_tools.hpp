#ifndef BATSCHED_TOOLS_HPP
#define BATSCHED_TOOLS_HPP
#include <unordered_map>
#include <string>
#include <cstdarg>
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <memory>
#include "json_workload.hpp"
#include "schedule.hpp"
#include <set>
#include "pempek_assert.hpp"
#include "loguru.hpp"
struct JobAlloc;
struct Job;

//ingestMacro
#define ingestM(variable,outervariable,json) PPK_ASSERT_ERROR(json.HasMember(#variable),"ingesting '%s' failed, no '%s' in json",#outervariable,#variable); variable = ingest(variable,json[#variable])
//ingestDateMacro
#define ingestDM(variable,outervariable,json) PPK_ASSERT_ERROR(json.HasMember(#variable),"ingesting '%s' failed, no '%s' in json",#outervariable,#variable); variable = ingest(variable,json[#variable],date)
//ingestVoidDateMacro
#define ingestVDM(json) PPK_ASSERT_ERROR(json.HasMember("_call_me_laters"),"ingesting 'failures' failed, no '_call_me_laters' in json"); ingestCMLS(json["_call_me_laters"],date)
//ingestTypeTestMacro
#define ingestTTM(variable,outervariable,json,type) PPK_ASSERT_ERROR(json.HasMember(#variable),"ingesting '%s' failed, no '%s' in json",#outervariable,#variable); PPK_ASSERT_ERROR(json[#variable].Is##type(),"ingesting '%s' failed, field '%s' is not '%s' type",#outervariable,#variable,#type); variable = ingest(variable,json[#variable])

#define CCU_INFO 1
#define CCU_DEBUG_FIN 2
#define CCU_DEBUG 3
#define CCU_DEBUG_ALL 4
#define CCU_DEBUG_MAX 9
#define CLOG_F(verbosity_level, ...) LOG_F(verbosity_level,__VA_ARGS__)

#define BLOG_F(log_type,fmt,...) B_LOG_INSTANCE->blog(log_type,date,fmt,## __VA_ARGS__)
class b_log{
    
public:
b_log();
~b_log();
void blog(std::string type,double date,std::string fmt,...);
void add_log_file(std::string file, std::string type,std::string open_method,bool csv = false,std::string separator = ",");
void add_header(std::string type,std::string header);
void copy_file(std::string file, std::string type,std::string copy_location);

std::unordered_map<std::string,FILE*> _files;
std::unordered_map<std::string,bool> _csv_status;
std::unordered_map<std::string,std::string> _csv_sep;
};
namespace blog_types
    {
        const std::string SOFT_ERRORS="SOFT_ERRORS";
        const std::string FAILURES="FAILURES";
    };
namespace blog_open_method
    {
        const std::string APPEND = "APPEND";
        const std::string OVERWRITE = "OVERWRITE";
    };
namespace blog_failure_event
    {
        const std::string MACHINE_REPAIR = "MACHINE_REPAIR";
        const std::string MACHINE_INSTANT_DOWN_UP = "MACHINE_INSTANT_DOWN_UP";
        const std::string REPAIR_TIME = "REPAIR_TIME";
        const std::string KILLING_JOBS = "KILLING_JOBS";
        const std::string FAILURE = "FAILURE";
        const std::string REPAIR_DONE = "REPAIR_DONE";
        const std::string MACHINE_ALREADY_DOWN = "MACHINE_ALREADY_DOWN";

    };


namespace batsched_tools{
    
    enum class reservation_types{RESERVATION,REPAIR}; //not used yet
    enum class call_me_later_types 
    {
        FIXED_FAILURE
        ,SMTBF
        ,MTBF
        ,REPAIR_DONE
        ,RESERVATION_START
        ,CHECKPOINT_SYNC
        ,CHECKPOINT_BATSCHED
        ,RECOVER_FROM_CHECKPOINT
        ,METRICS
    };
    enum class KILL_TYPES 
    {
        NONE
        ,FIXED_FAILURE
        ,SMTBF
        ,MTBF
        ,RESERVATION
    };
    enum class REJECT_TYPES
    {
        NOT_ENOUGH_RESOURCES
        ,NOT_ENOUGH_AVAILABLE_RESOURCES
        ,NO_WALLTIME
        ,NO_RESERVATION_ALLOCATION
    };
    enum class JobState
{
     JOB_STATE_NOT_SUBMITTED                //!< The job exists but cannot be scheduled yet.
    ,JOB_STATE_SUBMITTED                    //!< The job has been submitted, it can now be scheduled.
    ,JOB_STATE_RUNNING                      //!< The job has been scheduled and is currently being processed.
    ,JOB_STATE_COMPLETED_SUCCESSFULLY       //!< The job execution finished before its walltime successfully.
    ,JOB_STATE_COMPLETED_FAILED             //!< The job execution finished before its walltime but the job failed.
    ,JOB_STATE_COMPLETED_WALLTIME_REACHED   //!< The job has reached its walltime and has been killed.
    ,JOB_STATE_COMPLETED_KILLED             //!< The job has been killed.
    ,JOB_STATE_REJECTED                     //!< The job has been rejected by the scheduler.
};
    
    struct failure_tuple{
        int machine_down;
        batsched_tools::call_me_later_types type;
        std::string method;
    };
    struct id_separation{
        std::string basename;
        std::string resubmit_string;
        int resubmit_number;
        int next_resubmit_number;
        std::string next_profile_name;
        std::string next_job_name;
        std::string next_resubmit_string;
        std::string workload;
        std::string nb_checkpoint;
        };
    struct Job_Message{
        std::string id;
        std::string progress_str;
        double progress;
        batsched_tools::KILL_TYPES forWhat = batsched_tools::KILL_TYPES::NONE;
    };
    struct FinishedHorizonPoint
    {
        double date;
        int nb_released_machines;
        IntervalSet machines; //used if share-packing
        int index = -1; //only set during a checkpoint, not any other use
    };

    struct Allocation
    {
        IntervalSet machines;
        std::list<FinishedHorizonPoint>::iterator horizon_it;
        bool has_horizon = true;

    };


    //easy_bf3 structs
    struct Scheduled_Job
    {
        std::string id;
        int requested_resources;
        double wall_time;
        double start_time;
        double est_finish_time;
        IntervalSet allocated_machines;
    };
    struct Priority_Job
    {
        std::string id;
        int requested_resources;
        int extra_resources;
        double shadow_time;
        double est_finish_time;
    };
    //end easy_bf3 structs
    struct tools{
        static id_separation separate_id(const std::string job_id);
    };
    
     struct job_parts{
        int job_number;   //the job number part
        int job_resubmit; //the job resubmit number
        int job_checkpoint; // the job checkpoint number
        std::string workload; // the job's workload part
        std::string next_checkpoint; //the whole job name with the next_checkpoint tacked on
        std::string next_resubmit; //the whole job name with the next_resubmit tacked on

    };
    struct batsched_tools::job_parts get_job_parts(std::string job_id);
    struct checkpoint_job_data{
      batsched_tools::JobState state = batsched_tools::JobState::JOB_STATE_NOT_SUBMITTED;    //state when the job was checkpointed
      double progress = -1;  //if state was JOB_STATE_RUNNING (2) the progress that had been made from 0 to 1.0
      double runtime = 0;    //if state was JOB_STATE_RUNNING (2) the amount of total runtime the job had made
      IntervalSet allocation = IntervalSet::empty_interval_set();  //if state was JOB_STATE_RUNNING, which nodes were allocated to it
      long double consumed_energy = -1.0; //the consumed energy at the time of checkpoint
      std::string jitter = "null"; //the jitter

    };
    struct start_from_chkpt{
      int nb_folder = 0;  //the folder to get checkpoint info from
      int nb_checkpoint=0;  // the amount of times we started from a checkpoint
      int nb_original_jobs = 0; //the amount of jobs the original workload had
      int nb_actually_completed=0; //the amount of jobs actually completed
      int nb_previously_completed=0; //the amount of jobs previously completed
      bool started_from_checkpoint=false; //whether or not we started from a checkpoint
      std::string checkpoint_folder="null"; //the actual folder to read in from
      bool received_submitted_jobs = false;
      bool start_adding_to_queue = false;
      std::set<std::string> jobs_that_should_have_been_submitted_already = {};
      double first_submitted_time=0;
    };
    struct CALL_ME_LATERS{
        double time;  //time to call back
        int id; //id of call me later.
        batsched_tools::call_me_later_types forWhat; //what is the callback for? So we know how to deal with it.
        std::string extra_data="{}"; //any extra json data?  repair done uses this for the machine number that is done.
    };

    

    struct memInfo{
        unsigned long long total,free,available,used;
    };
    memInfo get_node_memory_usage();
    pid_t get_batsched_pid();
    struct pid_mem{
      unsigned long long USS=0;
      unsigned long long PSS=0;  
      unsigned long long RSS=0;
    };
    pid_mem get_pid_memory_usage(pid_t pid);

    template<typename ... Args>
    std::string string_format( std::string format, Args ... args )
    {
        int size_s = std::snprintf( nullptr, 0, format.c_str(), args ... ) + 1; // Extra space for '\0'
        if( size_s <= 0 ){ throw std::runtime_error( "Error during formatting." ); }
        auto size = static_cast<size_t>( size_s );
        std::unique_ptr<char[]> buf( new char[ size ] );
        std::snprintf( buf.get(), size, format.c_str(), args ... );
        return std::string( buf.get(), buf.get() + size - 1 ); // We don't want the '\0' inside
    }  
    //std::string to_string(int value);
    std::string to_string(const int value);
    //std::string to_string(unsigned int value);
    std::string to_string(const unsigned int value);
    //std::string to_string(long value);
    std::string to_string(const long value);
    std::string to_string(double value);
    std::string to_string(Rational value);
    //std::string to_string(std::string value);
    std::string to_string(const std::string value);
    //std::string to_string(Job* value);
    std::string to_string(const Job* value);
    //std::string to_string(JobAlloc * alloc);
    std::string to_string(const JobAlloc * alloc);
    std::string to_string(batsched_tools::KILL_TYPES kt);
    //std::string to_string(Schedule::ReservedTimeSlice rts);
    //std::string to_string(const Schedule::ReservedTimeSlice rts);
    //std::string to_string(Schedule::ReservedTimeSlice * rts);
    //std::string to_string(const Schedule::ReservedTimeSlice * rts);
    //std::string to_string(Schedule::ReservedTimeSlice & rts);
    //std::string to_string(const Schedule::ReservedTimeSlice & rts);
    //std::string to_json_string(int value);
    std::string to_json_string(const int value);
    //std::string to_json_string(unsigned int value);
    std::string to_json_string(const unsigned int value);
    //std::string to_json_string(long value);
    std::string to_json_string(const long value);
    std::string to_json_string(double value);
    std::string to_json_string(Rational value);
    //std::string to_json_string(std::string value);
    std::string to_json_string(const std::string value);
    std::string to_json_string(Job* value);
    std::string to_json_string(const Job* value);
    //std::string to_json_string(JobAlloc * alloc);
    std::string to_json_string(const JobAlloc * alloc);
    std::string to_json_string(batsched_tools::KILL_TYPES kt);
    std::string to_json_string(const IntervalSet is);
    std::string to_json_string(const batsched_tools::Job_Message * jm);
    std::string to_json_string(const batsched_tools::Allocation * alloc);
    std::string to_json_string(const batsched_tools::Allocation & alloc);
    std::string to_json_string(const batsched_tools::FinishedHorizonPoint * fhp);
    std::string to_json_string(const batsched_tools::FinishedHorizonPoint & fhp);
    std::string to_json_string(const batsched_tools::CALL_ME_LATERS &cml);
    std::string to_json_string(const std::chrono::_V2::system_clock::time_point &tp);
    std::string to_json_string(const batsched_tools::Scheduled_Job* sj);
    std::string to_json_string(const batsched_tools::Scheduled_Job& sj);
    std::string to_json_string(const batsched_tools::Priority_Job* pj);
    std::string to_json_string(const batsched_tools::Priority_Job& pj);
    //std::string to_json_string(Schedule::ReservedTimeSlice rts);
    //std::string to_json_string(const Schedule::ReservedTimeSlice rts);
    //std::string to_json_string(Schedule::ReservedTimeSlice * rts);
    //std::string to_json_string(const Schedule::ReservedTimeSlice * rts);
    //std::string to_json_string(Schedule::ReservedTimeSlice & rts);
    //std::string to_json_string(const Schedule::ReservedTimeSlice & rts);
   



    template<typename K,typename V>    
    std::string pair_to_string(std::pair<K,V> pair)
    {
        return batsched_tools::to_string(pair.first)+":"+batsched_tools::to_string(pair.second);
    }
    template<typename T>
    std::string vector_to_string(std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
            ourString += ", \n"; first = false;
        ourString = ourString + batsched_tools::to_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_string(std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString +  batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_string(std::map<K,V> &m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_string(std::map<K,V> *m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K, typename V>
    std::string unordered_map_to_string(std::unordered_map<K,V> &um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K, typename V>
    std::string unordered_map_to_string(std::unordered_map<K,V> *um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_string(std::list<T> &list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_string(std::list<T> *list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string vector_to_string(const std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
            ourString += ", \n"; first = false;
        ourString = ourString + "\"" + batsched_tools::to_string(value) + "\"";
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_string(const std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:*v)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + "\"" + batsched_tools::to_string(value) + "\"";
        }
        ourString = ourString + "]";
        return ourString;
    }


    template<typename K, typename V>
    std::string map_to_string(const std::map<K,V> &m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K, typename V>
    std::string map_to_string(const std::map<K,V> *m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(const std::unordered_map<K,V> &um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(const std::unordered_map<K,V> *um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::pair_to_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_string(const std::list<T> &list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_string(const std::list<T> *list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", \n"; first = false;
            ourString = ourString + batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }











    template<typename K,typename V>
    std::string pair_to_json_string(std::pair<K,V> pair)
    {
        return "{\"key\":"+batsched_tools::to_json_string(pair.first)+", \"value\":"+batsched_tools::to_json_string(pair.second)+"}";
    }
    template<typename K,typename V>
    std::string const_pair_to_string(const std::pair<K,V> pair)
    {
        return "{\"key\":"+batsched_tools::to_json_string(pair.first)+", \"value\":"+batsched_tools::to_json_string(pair.second)+"}";
    }
    template<typename K,typename V>
    std::string pair_to_simple_json_string(std::pair<K,V> pair)
    {
        return batsched_tools::to_json_string(pair.first) + ":" + batsched_tools::to_json_string(pair.second);
    }
    template<typename K,typename V>
    std::string const_pair_to_simple_string(const std::pair<K,V> pair)
    {
         return batsched_tools::to_json_string(pair.first) + ":" + batsched_tools::to_json_string(pair.second);
    }

    template<typename T>
    std::string vector_to_json_string(std::vector<T> &v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first && newline)
            ourString += ", \n";
        else if (!first)
            ourString += ",";
        first = false;
        ourString = ourString + batsched_tools::to_json_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(std::vector<T> *v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(const std::vector<T> &v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first && newline)
            ourString += ", \n";
        else if (!first)
            ourString += ",";
        first = false;
        ourString = ourString + batsched_tools::to_json_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(const std::vector<T> *v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    
    template<typename K, typename V>
    std::string vector_pair_to_json_string(const std::vector<std::pair<K,V>> &v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
    for (std::pair<K,V> kv_pair:v)
    {
        if (!first && newline)
            ourString += ", \n";
        else if (!first)
            ourString += ",";
        first = false;
        ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename K, typename V>
    std::string vector_pair_to_json_string(const std::vector<std::pair<K,V>> *v,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:(*v))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(std::map<K,V> &m,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
                
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(std::map<K,V> *m,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(const std::map<K,V> &m,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(const std::map<K,V> *m,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(std::unordered_map<K,V> &um,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(std::unordered_map<K,V> *um,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(const std::unordered_map<K,V> &um,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(const std::unordered_map<K,V> *um,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(std::list<T> &list,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(std::list<T> *list,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*list))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(const std::list<T> &list,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(const std::list<T> *list,bool newline=true)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*list))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string unordered_set_to_json_string(std::unordered_set<T> &set,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (T value:set)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string unordered_set_to_json_string(std::unordered_set<T> *set,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (T value:(*set))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string unordered_set_to_json_string(const std::unordered_set<T> &set,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (T value:set)
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string unordered_set_to_json_string(const std::unordered_set<T> *set,bool newline=true)
    {
        LOG_F(INFO,"here");
        std::string ourString="[";
        bool first = true;
        for (T value:(*set))
        {
            if (!first && newline)
                ourString += ", \n";
            else if (!first)
                ourString += ",";
            first = false;
            LOG_F(INFO,"here");
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        LOG_F(INFO,"here");
        ourString = ourString + "]";
        return ourString;
    }

    template<typename T>
    void from_json_to_vector(const rapidjson::Value & json, std::vector<T> & vec)
    {
        using namespace rapidjson;
        for(SizeType i=0;i<json.Size();i++)
        {
            vec.push_back(json[i]);
        }
        
    }

   
   

    
};



#endif