
#ifndef MACHINE_HPP
#define MACHINE_HPP
#include <map>
#include <string>
#include <vector>
#include <intervalset.hpp>

#include <rapidjson/document.h>



struct Machine{
    double speed;
    double repair_time = -1.0;
    std::string name;
    int int_name;
    std::string prefix;
    int id;
    std::string role;
    int core_count = -1;
    int cores_available;
    std::string to_json_string();
};
struct Prefix{
    std::map<int,Machine *> * machinesInPrefix;
    IntervalSet machineIdsAvailable;
    IntervalSet machineIdsUnavailable;
    IntervalSet machineIds;
    IntervalSet repair_machines;

};
class Machines{
    public:
            ~Machines();
            void ingest(const rapidjson::Value & json);
            Machine * operator[](std::string machine_name);
            Machine * operator[](int machine_number);
            Machine * operator()(std::string prefix_name,int machine_number);
            void set_core_percent(double core_percent);
            void add_machine_from_json_object(const rapidjson::Value & object);
            void update_machine_from_json_object(const rapidjson::Value & object);
            std::string to_json_string();
    private:
        std::map<std::string,Machine *> _machinesM; //machine by name
        std::map<std::string,Prefix *> _machinesByPrefix;
        // removed std::vector<Machine *> _machinesV; // a vector of all machines,not sure if useful.  machine id does not line up with vector index
        std::map<int,Machine*> _machinesById;
        double _core_percent = 1.0;
};

#endif