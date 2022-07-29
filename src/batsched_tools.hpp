#ifndef BATSCHED_TOOLS_HPP
#define BATSCHED_TOOLS_HPP
#include <unordered_map>
#include <string>
#include <cstdarg>
#include <cstdio>



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
    enum class call_me_later_types {FIXED_FAILURE,SMTBF,MTBF,REPAIR_DONE,RESERVATION_START};
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
}


#endif