#include <cstdarg>
#include <string>
#include "batsched_tools.hpp"
#include <cstdio>
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