#include "machine.hpp"
#include "pempek_assert.hpp"
#include <loguru.hpp>
#include <regex>
#include <string>

Machines::~Machines(){

}
Machine * Machines::operator[](std::string machine_name){
    PPK_ASSERT_ERROR(_machinesM.count(machine_name) == 1, "Machine '%s' does not exist", machine_name.c_str());
    return _machinesM.at(machine_name);
}
Machine * Machines::operator[](int machine_number){
    PPK_ASSERT_ERROR(machine_number < _machinesV.size(),"Machine with position '%d' does not exist",machine_number);
    return _machinesV[machine_number];
}
void Machines::add_machine_from_json_object(const rapidjson::Value & object){
    Machine* new_machine = new Machine;
    
    PPK_ASSERT_ERROR(object.HasMember("name"),"machine has no name");
    new_machine->name = object["name"].GetString();
    
    std::regex r("[0-9]+");
    std::smatch m;
    //std::regex_match(new_machine->name, m, r);
    //new_machine->id = std::stoi(m.str(m.size()));
    //new_machine->prefix = new_machine->name.substr(0,m.position(m.size()));
    for(std::sregex_iterator i = std::sregex_iterator(new_machine->name.begin(), new_machine->name.end(), r);
                            i != std::sregex_iterator();
                            ++i )
    {
        m = *i;
    }
    new_machine->id = std::stoi(m.str());
    new_machine->prefix = new_machine->name.substr(0,m.position());
    


    PPK_ASSERT_ERROR(object.HasMember("core_count"),"machine %s has no core_count",new_machine->name.c_str());
    new_machine->core_count = object["core_count"].GetInt();

    PPK_ASSERT_ERROR(object.HasMember("speed"),"machine %s has no speed",new_machine->name.c_str());
    new_machine->speed = object["speed"].GetDouble();

    PPK_ASSERT_ERROR(object.HasMember("repair-time"),"machine %s has no repair-time",new_machine->name.c_str());
    new_machine->repair_time = object["repair-time"].GetDouble();

    _machinesM[new_machine->name] = new_machine;
    _machinesV.push_back(new_machine);
}
    
