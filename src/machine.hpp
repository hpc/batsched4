
#ifndef MACHINE_HPP
#define MACHINE_HPP
#include <map>
#include <string>
#include <vector>

#include <rapidjson/document.h>



struct Machine{
    double speed;
    double repair_time;
    std::string name;
    std::string prefix;
    int id;
    std::string role;
    std::string core_count;
    int cores_available;
};
class Machines{
    public:
            ~Machines();
            Machine * operator[](std::string machine_name);
            Machine * operator[](int machine_number);
            void add_machine_from_json_object(const rapidjson::Value & object);
    private:
        std::map<std::string,Machine *> _machinesM;
        std::map<std::string,std::map<int,Machine *> *> _machinesByPrefix;
        std::vector<Machine *> _machinesV;

};

#endif