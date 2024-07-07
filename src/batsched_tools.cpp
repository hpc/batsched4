#include <cstdarg>
#include <string>
#include "batsched_tools.hpp"
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <loguru.hpp>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif
//#include "schedule.cpp"
#include <sstream>
#include <iomanip>

b_log::b_log(){

}
b_log::~b_log(){
 for (auto key_value:_files)
    fclose(key_value.second);
}
void b_log::add_log_file(std::string file,std::string type, std::string open_method,bool csv){
    LOG_F(INFO,"here");
    FILE* myFile;
    if (open_method == blog_open_method::OVERWRITE)
        myFile=fopen(file.c_str(),"w");
    else if (open_method == blog_open_method::APPEND)
        myFile=fopen(file.c_str(),"a");
    else
        PPK_ASSERT_ERROR(false,"ERROR opening file %s, type: %s, open_method: %s",file.c_str(),type.c_str(),open_method.c_str());
    LOG_F(INFO,"here");
    std::fprintf(myFile,"");
    LOG_F(INFO,"here");
    _files[type]=myFile;
    _csv_status[type]=csv;
}
void b_log::copy_file(std::string file, std::string type, std::string copy_location)
{
    //first get file
    FILE* myFile = _files[type];
    //then flush file
    fflush(myFile);
    fclose(myFile);
    fs::copy(file,copy_location);
    myFile = fopen(file.c_str(),"a");
    _files[type]=myFile;

}
void b_log::add_header(std::string type,std::string header){
    FILE* file = _files[type];
    std::fprintf(file,"%s\n",header.c_str());
    fflush(file);

}
void b_log::blog(std::string type, double date,std::string fmt, ...){
    
    
    if (_files.size() > 0 && _files.find(type) != _files.end()){
        va_list args;
        LOG_F(INFO,"here");
        va_start(args,date);
        LOG_F(INFO,"here");
        FILE* file = _files[type];
        LOG_F(INFO,"here");
        if(!_csv_status[type])
        {
            LOG_F(INFO,"here");
            std::fprintf(file,"%-60f ||",date);
            LOG_F(INFO,"here");
        }
        else
        {
            
            LOG_F(INFO,"here");
            std::fprintf(file,"%f,",date);
            LOG_F(INFO,"here");
        }
        LOG_F(INFO,"here");
        fmt=fmt + "\n";
        std::vfprintf(file,fmt.c_str(),args);
        LOG_F(INFO,"here");
        va_end(args);
        fflush(file);
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
        return batsched_tools::string_format("%.15f",value);
    }
    std::string batsched_tools::to_json_string(Rational value)
    {
        return batsched_tools::string_format("%.15f",value.convert_to<double>());
    }
    

    
    std::string batsched_tools::to_json_string(const std::string value)
    {
        std::stringstream ss;
        std::string str;
        const char delim='"';
        const char escape='\\';
        ss<<std::quoted(value,delim,escape);
        ss>>str;
        return str;
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
        return "\"" + is.to_string_hyphen() + "\"";
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

    std::string batsched_tools::to_json_string(const batsched_tools::Allocation *alloc)
    {
        using namespace std;
        std::string ret;
        ret = "{";
        LOG_F(INFO,"here");
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("machines",alloc->machines.to_string_hyphen())) + ",";
        LOG_F(INFO,"here");
        if (alloc->has_horizon)
            ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("horizon_it",alloc->horizon_it->index)) + ",";
        else
            ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("horizon_it",-1)) + ",";
        LOG_F(INFO,"here");
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,bool>("has_horizon",alloc->has_horizon)) + "}";
        return ret;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::Allocation &alloc)
    {
        using namespace std;
        std::string ret;
        ret = "{";
        LOG_F(INFO,"here");
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("machines",alloc.machines.to_string_hyphen())) + ",";
        LOG_F(INFO,"here");
        if (alloc.has_horizon)
            ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("horizon_it",alloc.horizon_it->index)) + ",";
        else
            ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("horizon_it",-1)) + ",";
        LOG_F(INFO,"here");
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,bool>("has_horizon",alloc.has_horizon)) + "}";
        return ret;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::FinishedHorizonPoint * fhp)
    {
        using namespace std;
        std::string ret;
        ret = "{";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,double>("date",fhp->date)) + ",";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("nb_released_machines",fhp->nb_released_machines)) + ",";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("machines",fhp->machines.to_string_hyphen())) + "}";
        return ret;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::FinishedHorizonPoint & fhp)
    {
        using namespace std;
        std::string ret;
        ret = "{";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,double>("date",fhp.date)) + ",";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("nb_released_machines",fhp.nb_released_machines)) + ",";
        ret += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("machines",fhp.machines.to_string_hyphen())) + "}";
        return ret;
    }
    
    std::string batsched_tools::to_json_string(const JobAlloc * alloc)
    {
        std::string s = "{";
        
            s+="\"begin\":"+batsched_tools::to_json_string(alloc->begin.convert_to<std::string>());
           
            s+=",\"end\":"+batsched_tools::to_json_string(alloc->end.convert_to<std::string>());
            //for visualizing the Rational only
            s+=",\"dbl_begin\":\""+batsched_tools::to_json_string(alloc->begin.convert_to<double>())+"\"";
            s+=",\"dbl_end\":\""+batsched_tools::to_json_string(alloc->end.convert_to<double>())+"\"";
           
            s+=",\"has_been_inserted\":"+std::string((alloc->has_been_inserted ? "true":"false"));
           
            s+=",\"started_in_first_slice\":"+std::string((alloc->started_in_first_slice ? "true":"false"));
           
            s+=",\"used_machines\":\""+alloc->used_machines.to_string_hyphen()+"\"";
           
            s+=",\"job\":\""+alloc->job->id+"\"";
        s+="}";
        return s;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::CALL_ME_LATERS &cml)
    {
        std::string s;
            s = "{";
            s += "\"time\":"        +   batsched_tools::to_json_string(cml.time)                                        + ",";
            s += "\"id\":"          +   batsched_tools::to_json_string(cml.id)                                          + ",";
            s += "\"forWhat\":"     +   batsched_tools::to_json_string(static_cast<int>(cml.forWhat))                   + ",";
            s += "\"extra_data\":"      +   batsched_tools::to_json_string(cml.extra_data)                                           ;
            s += "}";
        return s;
    }
    std::string batsched_tools::to_json_string(const std::chrono::_V2::system_clock::time_point &tp)
    {
        std::time_t myTime = std::chrono::system_clock::to_time_t(tp);
        char timeString[std::size("yyyy-mm-dd HH:MM:SS")];
        std::strftime(std::data(timeString),std::size(timeString),"%F %T\n",std::localtime(&myTime));
        return "\""+std::string(timeString)+"\"";
    }
    std::string batsched_tools::to_json_string(const batsched_tools::Scheduled_Job* sj){
        std::string s;
            s = "{";
            LOG_F(INFO,"id");
            s += "\"id\":"                      +   batsched_tools::to_json_string(sj->id)                              + ",";
            LOG_F(INFO,"requested_resources");
            s += "\"requested_resources\":"     +   batsched_tools::to_json_string(sj->requested_resources)             + ",";
            LOG_F(INFO,"wall_time");
            s += "\"wall_time\":"               +   batsched_tools::to_json_string(sj->wall_time)                       + ",";
            LOG_F(INFO,"start_time");
            s += "\"start_time\":"              +   batsched_tools::to_json_string(sj->start_time)                      + ",";
            LOG_F(INFO,"est_finish_time");
            s += "\"est_finish_time\":"         +   batsched_tools::to_json_string(sj->est_finish_time)                 + ",";
            LOG_F(INFO,"allocated_machines");
            s += "\"allocated_machines\":"      +   batsched_tools::to_json_string(sj->allocated_machines);
            s += "}";
        return s;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::Scheduled_Job& sj){
        std::string s;
            s = "{";
            s += "\"id\":"                      +   batsched_tools::to_json_string(sj.id)                              + ",";
            s += "\"requested_resources\":"     +   batsched_tools::to_json_string(sj.requested_resources)             + ",";
            s += "\"wall_time\":"               +   batsched_tools::to_json_string(sj.wall_time)                       + ",";
            s += "\"start_time\":"              +   batsched_tools::to_json_string(sj.start_time)                      + ",";
            s += "\"est_finish_time\":"         +   batsched_tools::to_json_string(sj.est_finish_time)                 + ",";
            s += "\"allocated_machines\":"      +   batsched_tools::to_json_string(sj.allocated_machines);
            s += "}";
        return s;
    }
    std::string batsched_tools::to_json_string(const batsched_tools::Priority_Job* pj){
        std::string s;
            s = "{";
            s += "\"id\":"                      +   batsched_tools::to_json_string(pj->id)                              + ",";
            s += "\"requested_resources\":"     +   batsched_tools::to_json_string(pj->requested_resources)             + ",";
            s += "\"extra_resources\":"         +   batsched_tools::to_json_string(pj->extra_resources)                 + ",";
            s += "\"shadow_time\":"             +   batsched_tools::to_json_string(pj->shadow_time)                     + ",";
            s += "\"est_finish_time\":"         +   batsched_tools::to_json_string(pj->est_finish_time);
            s += "}";
        return s;

    }
    std::string batsched_tools::to_json_string(const batsched_tools::Priority_Job& pj){
        std::string s;
            s = "{";
            s += "\"id\":"                      +   batsched_tools::to_json_string(pj.id)                              + ",";
            s += "\"requested_resources\":"     +   batsched_tools::to_json_string(pj.requested_resources)             + ",";
            s += "\"extra_resources\":"         +   batsched_tools::to_json_string(pj.extra_resources)                 + ",";
            s += "\"shadow_time\":"             +   batsched_tools::to_json_string(pj.shadow_time)                     + ",";
            s += "\"est_finish_time\":"         +   batsched_tools::to_json_string(pj.est_finish_time);
            s += "}";
        return s;
    }
    
    
    /*std::string batsched_tools::to_json_string(const Schedule::ReservedTimeSlice *rts){
        return rts->to_json_string();
    }
    

    
    std::string batsched_tools::to_json_string(const Schedule::ReservedTimeSlice &rts){
        return rts.to_json_string();
    }
*/



    




    
   

