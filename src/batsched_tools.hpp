#ifndef BATSCHED_TOOLS_HPP
#define BATSCHED_TOOLS_HPP
#include <unordered_map>
#include <string>
#include <cstdarg>
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>



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
    enum class call_me_later_types {FIXED_FAILURE,SMTBF,MTBF,REPAIR_DONE,RESERVATION_START,CHECKPOINT_BATSCHED,RECOVER_FROM_CHECKPOINT};
    enum class KILL_TYPES {NONE,FIXED_FAILURE,SMTBF,MTBF,RESERVATION};
    struct id_separation{
        std::string basename;
        std::string resubmit_string;
        int resubmit_number;
        std::string next_resubmit_string;
        std::string workload;
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
}


#endif