#ifndef BATSCHED_TOOLS_HPP
#define BATSCHED_TOOLS_HPP
#include <unordered_map>
#include <string>
#include <cstdarg>
#include <cstdio>


namespace batsched_tools{
enum call_me_later_types {FIXED_FAILURE,SMTBF,MTBF,REPAIR_DONE};
}
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



#endif