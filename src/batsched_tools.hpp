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
struct JobAlloc;
struct Job;







#define BLOG_F(log_type,fmt,...) B_LOG_INSTANCE->blog(log_type,fmt,date,## __VA_ARGS__)
class b_log{
    
public:
b_log();
~b_log();
enum logging_type{FAILURES};
const char * logging_types = {"FAILURES"};
void blog(logging_type type,std::string fmt, double date,...);
void add_log_file(std::string file, logging_type type);

std::unordered_map<logging_type,FILE*> _files;
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
        ,CHECKPOINT_BATSCHED
        ,RECOVER_FROM_CHECKPOINT
    };
    enum class KILL_TYPES 
    {
        NONE
        ,FIXED_FAILURE
        ,SMTBF
        ,MTBF
        ,RESERVATION
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
    struct tools{
        static id_separation separate_id(const std::string job_id);
    };
     struct job_parts{
        int job_number;
        int job_resubmit;
        int job_checkpoint;
        std::string workload;
        std::string next_checkpoint;
        std::string next_resubmit;

    };
    struct checkpoint_job_data{
      int state = -1;
      double progress = -1;
      double runtime = 0;
      std::string allocation = "null";
      long double consumed_energy = -1.0;
      std::string jitter = "null";

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
    };

    struct batsched_tools::job_parts get_job_parts(std::string job_id);

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
        ourString += ", "; first = false;
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
            ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
        ourString += ", "; first = false;
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
            ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
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
                ourString += ", "; first = false;
            ourString = ourString + batsched_tools::to_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }











    template<typename K,typename V>
    std::string pair_to_json_string(std::pair<K,V> pair)
    {
        return "{ \"key\":"+batsched_tools::to_json_string(pair.first)+", \"value\":"+batsched_tools::to_json_string(pair.second)+"}";
    }
    template<typename K,typename V>
    std::string const_pair_to_string(const std::pair<K,V> pair)
    {
        return "{ \"key\":"+batsched_tools::to_json_string(pair.first)+", \"value\":"+batsched_tools::to_json_string(pair.second)+"}";
    }

    template<typename T>
    std::string vector_to_json_string(std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
            ourString += ", "; first = false;
        ourString = ourString + batsched_tools::to_json_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first)
                ourString += ", "; first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(const std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
            ourString += ", "; 
        first = false;
        ourString = ourString + batsched_tools::to_json_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_json_string(const std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K, typename V>
    std::string vector_pair_to_json_string(const std::vector<std::pair<K,V>> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (std::pair<K,V> kv_pair:v)
    {
        if (!first)
            ourString += ", "; 
        first = false;
        ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename K, typename V>
    std::string vector_pair_to_json_string(const std::vector<std::pair<K,V>> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:(*v))
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(std::map<K,V> &m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(std::map<K,V> *m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(const std::map<K,V> &m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_json_string(const std::map<K,V> *m)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(std::unordered_map<K,V> &um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(std::unordered_map<K,V> *um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(const std::unordered_map<K,V> &um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_json_string(const std::unordered_map<K,V> *um)
    {
        std::string ourString="[";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::pair_to_json_string(kv_pair);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(std::list<T> &list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(std::list<T> *list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*list))
        {
            if(!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(const std::list<T> &list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:list)
        {
            if(!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
        ourString = ourString + "]";
        return ourString;
    }
    template<typename T>
    std::string list_to_json_string(const std::list<T> *list)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*list))
        {
            if(!first)
                ourString += ", "; 
            first = false;
            ourString = ourString + batsched_tools::to_json_string(value);
        }
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