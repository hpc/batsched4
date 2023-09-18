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
    enum class reservation_types{RESERVATION,REPAIR};
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
};


#endif