#include <cstdarg>
#include <string>
#include "batsched_tools.hpp"
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
b_log::b_log(){

}
b_log::~b_log(){
 for (auto key_value:_files)
    fclose(key_value.second);
}
void b_log::add_log_file(std::string file,logging_type type){
    FILE* myFile=fopen(file.c_str(),"w");
    _files[type]=myFile;
}
void b_log::blog(logging_type type, std::string fmt, double date, ...){
    
    if (_files.size() > 0 && _files.find(type) != _files.end()){
        va_list args;
        va_start(args,date);
        FILE* file = _files[type];                                                                                                                                                           
        std::fprintf(file,"%-60f ||",date);
        fmt=fmt + "\n";
        std::vfprintf(file,fmt.c_str(),args);
        va_end(args);
    }
    
}
batsched_tools::id_separation batsched_tools::tools::separate_id(const std::string job_id){
    batsched_tools::id_separation separation;
    auto start = job_id.find("!")+1;
    auto end = job_id.find("#");
    separation.basename = (end ==std::string::npos) ? job_id.substr(start) : job_id.substr(start,end-start); 
    separation.workload = job_id.substr(0,start-1);
    separation.resubmit_number = 1;
    int next_number = 1;
    if (end!=std::string::npos) //if job name has # in it...was resubmitted b4
    {
            separation.resubmit_number = std::stoi(job_id.substr(end+1));   // then get the resubmitted number
            next_number = separation.resubmit_number + 1;
    }                                                                                                                                                                                                                                                                                                                                                                                    
    separation.resubmit_string = std::to_string(separation.resubmit_number);
    separation.next_resubmit_string = separation.workload + "!" +
                                      separation.basename + "#" +
                                      std::to_string(next_number);
    return separation;
}
batsched_tools::memInfo batsched_tools::get_node_memory_usage()
{
 FILE* file;
    struct memInfo meminfo;
    
/*
#instead of doing fscanf, we should really use fgets for line by line compare
#and use strtok or strtok_r  (strtok 'string token' has a state and you initially set it to a string with a token to search)
#subsequent calls to it are passed with a NULL for string and searches the rest of the string
#strtok_r you pass it a buffer for the rest of the string and it stores the rest in this buffer.
#then subsequent calls to it are passed the rest as a string and the rest buffer to put the rest in 
*/
    file = fopen("/proc/meminfo", "r");
    fscanf(file, "MemTotal: %llu kB\n",&meminfo.total);
    fscanf(file, "MemFree: %llu kB\n",&meminfo.free);
    fscanf(file, "MemAvailable: %llu kB\n",&meminfo.available);
    fclose(file);
    
   
    meminfo.used=0;
    return meminfo;
}
pid_t batsched_tools::get_batsched_pid()
{
    return getpid();
}




batsched_tools::pid_mem batsched_tools::get_pid_memory_usage(pid_t pid=0)
{
    std::ifstream ifs;
    if (pid == 0)
        ifs.open("/proc/self/smaps", std::ios_base::in);
    else
        ifs.open("/proc/"+std::to_string(pid)+"/smaps",std::ios_base::in);
    std::string type,value;
    int sumUSS=0;
    int sumPSS=0;
    int sumRSS=0;
    while (ifs>>type>>value)
    {
            if (type.find("Private")!=std::string::npos)
            {
              sumUSS+=std::stoi(value);
            }
            if (type.find("Pss")!=std::string::npos)
            {
              sumPSS+=std::stoi(value);
            }
            if (type.find("Rss")!=std::string::npos)
            {
              sumRSS+=std::stoi(value);
            }
    }
    batsched_tools::pid_mem memory_struct;
    memory_struct.USS=sumUSS;
    memory_struct.PSS=sumPSS;
    memory_struct.RSS=sumRSS;
    ifs.close();
    return memory_struct;

}