#include <cstdarg>
#include <string>
#include "batsched_tools.hpp"
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <loguru.hpp>
//#include "schedule.cpp"

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
//a helper function to seperate_id
batsched_tools::job_parts batsched_tools::get_job_parts(std::string job_id)
{
        batsched_tools::job_parts parts;

        auto startDollar = job_id.find("$");
        parts.job_checkpoint = (startDollar != std::string::npos) ? std::stoi(job_id.substr(startDollar+1)) : -1;
        if (startDollar == std::string::npos)
            startDollar = job_id.size();
        //ok we got the checkpoint number, now the resubmit number
        auto startPound = job_id.find("#");
        parts.job_resubmit = (startPound != std::string::npos) ? std::stoi(job_id.substr(startPound+1,startPound+1 - startDollar)): -1;
        if (startPound == std::string::npos)
            startPound = startDollar;
        auto startExclamation = job_id.find("!");
        parts.job_number = (startExclamation != std::string::npos) ? std::stoi(job_id.substr(startExclamation+1,startExclamation+1 - startPound)) : std::stoi(job_id.substr(0,startPound));
        parts.workload = (startExclamation != std::string::npos) ? job_id.substr(0,startExclamation) : "null";
        std::string checkpoint = ((parts.job_checkpoint != -1) ? "$" + std::to_string(parts.job_checkpoint) : "");
        std::string resubmit = ((parts.job_resubmit != -1) ? "#" + std::to_string(parts.job_resubmit+1) : "#1");
        parts.next_resubmit = parts.workload + "!"+
                     std::to_string(parts.job_number) +
                     resubmit +
                     checkpoint;
        checkpoint = ((parts.job_checkpoint != -1) ? "$" + std::to_string(parts.job_checkpoint+1) : "$1");
        resubmit = ((parts.job_resubmit != -1) ? "#" + std::to_string(parts.job_resubmit) : "");
        parts.next_checkpoint = parts.workload + "!"+
                     std::to_string(parts.job_number) +
                     resubmit +
                     checkpoint;

        return parts;
}
batsched_tools::id_separation batsched_tools::tools::separate_id(const std::string job_id){
    batsched_tools::id_separation separation;
    batsched_tools::job_parts parts = batsched_tools::get_job_parts(job_id);
    int next_number;
    separation.basename = std::to_string(parts.job_number);
    separation.workload = parts.workload;
    separation.resubmit_number = (parts.job_resubmit != -1) ? parts.job_resubmit : 0;
    separation.nb_checkpoint = (parts.job_checkpoint != -1) ? "$"+std::to_string(parts.job_checkpoint) : "";
    next_number = separation.resubmit_number + 1;
    separation.next_resubmit_number = next_number;
    separation.resubmit_string = std::to_string(separation.resubmit_number);
    separation.next_resubmit_string = separation.workload + "!" +
                                      separation.basename + "#" +
                                      std::to_string(next_number) +
                                      separation.nb_checkpoint;
    separation.next_profile_name = separation.basename + "#" +
                                   std::to_string(next_number) +
                                   separation.nb_checkpoint;
    separation.next_job_name = separation.next_profile_name;
    LOG_F(INFO,"job_id: '%s' next: '%s'",job_id.c_str(),separation.next_resubmit_string.c_str());
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

    
    std::string batsched_tools::to_string(const int value)
    {
        return std::to_string(value);
    }
    
    std::string batsched_tools::to_string(const unsigned int value)
    {
        return std::to_string(value);
    }
    std::string batsched_tools::to_string(long value)
    {
        return std::to_string(value);
    }
    std::string batsched_tools::to_string(double value)
    {
        return batsched_tools::string_format("%.15f",value);
    }
    std::string batsched_tools::to_string(Rational value)
    {
        return batsched_tools::string_format("%.15f",value.convert_to<double>());
    }
    
   
 
    
    std::string batsched_tools::to_string(const std::string value)
    {
        return value;
    }
    
    
    
    std::string batsched_tools::to_string(const Job* job)
    {
        return job->id;
    }

    
    std::string batsched_tools::to_string(batsched_tools::KILL_TYPES kt)
    {
        return std::to_string(int(kt));
    }


    
    
    
    std::string batsched_tools::to_string(const JobAlloc * alloc)
    {
        std::string s = "{";
            s+="{\"begin\":\""+alloc->begin.str()+"\"";
            s+=",\"end\":\""+alloc->end.str()+"\"";
            s+=",\"has_been_inserted\":"+std::string((alloc->has_been_inserted ? "true":"false"));
            s+=",\"started_in_first_slice\":"+std::string((alloc->started_in_first_slice ? "1":"0"));
            s+=",\"used_machines\":\""+alloc->used_machines.to_string_hyphen()+"\"";
            s+=",\"job:\":\""+alloc->job->id+"\"";
        s+="}";
        return s;
    }
    
   
    
    /*std::string batsched_tools::to_string(const Schedule::ReservedTimeSlice *rts){
        return rts->to_string();
        
    }
    
   
    
    std::string batsched_tools::to_string(const Schedule::ReservedTimeSlice &rts){
        return rts.to_string();
    }
    */
   
    
    
    std::string batsched_tools::to_json_string(const int value)
    {
        return std::to_string(value);
    }
    
    std::string batsched_tools::to_json_string(const unsigned int value)
    {
        return std::to_string(value);
    }
    std::string batsched_tools::to_json_string(long value)
    {
        return std::to_string(value);
    }
    
    std::string batsched_tools::to_json_string(double value)
    {
        LOG_F(INFO,"here");
        return batsched_tools::string_format("%.15f",value);
    }
    std::string batsched_tools::to_json_string(Rational value)
    {
        return batsched_tools::string_format("%.15f",value.convert_to<double>());
    }
    

    
    std::string batsched_tools::to_json_string(const std::string value)
    {
        return "\""+value+"\"";
    }
    
    std::string batsched_tools::to_json_string(Job* job)
    {
        return "\""+job->id+"\"";
    }
    
    std::string batsched_tools::to_json_string(const Job* job)
    {
        return "\""+job->id+"\"";
    }
    
    std::string batsched_tools::to_json_string(batsched_tools::KILL_TYPES kt)
    {
        return batsched_tools::to_json_string(int(kt));
    }
    std::string batsched_tools::to_json_string(const IntervalSet is)
    {
        return is.to_string_hyphen();
    }
    std::string batsched_tools::to_json_string(const Job_Message * jm)
    {
        std::string ret;
        ret = "{";
        ret += "\"id\":"                +  batsched_tools::to_json_string(jm->id)                   + ",";
        ret += "\"progress_str\":"      +  batsched_tools::to_json_string(jm->progress_str)         + ",";
        ret += "\"progress\":"          +  batsched_tools::to_json_string(jm->progress)                 + ",";
        ret += "\"forWhat\":"           +  batsched_tools::to_json_string(jm->forWhat)              + "}";
        return ret;
    }

    std::string id;
        std::string progress_str;
        double progress;
        batsched_tools::KILL_TYPES forWhat = batsched_tools::KILL_TYPES::NONE;
    
    
    std::string batsched_tools::to_json_string(const JobAlloc * alloc)
    {
        std::string s = "{";
        
            s+="\"begin\":\""+batsched_tools::to_json_string(alloc->begin.convert_to<double>())+"\"";
           
            s+=",\"end\":\""+batsched_tools::to_json_string(alloc->end.convert_to<double>())+"\"";
           
            s+=",\"has_been_inserted\":"+std::string((alloc->has_been_inserted ? "true":"false"));
           
            s+=",\"started_in_first_slice\":"+std::string((alloc->started_in_first_slice ? "true":"false"));
           
            s+=",\"used_machines\":\""+alloc->used_machines.to_string_hyphen()+"\"";
           
            s+=",\"job\":\""+alloc->job->id+"\"";
        s+="}";
        return s;
    }
    
    
    
    /*std::string batsched_tools::to_json_string(const Schedule::ReservedTimeSlice *rts){
        return rts->to_json_string();
    }
    

    
    std::string batsched_tools::to_json_string(const Schedule::ReservedTimeSlice &rts){
        return rts.to_json_string();
    }
*/



    




    
   

