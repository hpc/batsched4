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