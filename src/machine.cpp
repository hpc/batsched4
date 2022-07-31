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
    
    std::regex r(".*[a-zA-Z]+([0-9]+)");//all characters then at least one letter, then capture all numbers
    //matches m[0](m[1])[not matched]  a30b(109)  @d(309)   b(12)   ab(309)[bc] 
    //non-matches [1089]  --no characters
    //non-matches [bbb@], [ccbb]  --no numbers
    
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
    PPK_ASSERT_ERROR(m.size()==2,"Machine name should be any characters followed by at least one letter followed by numbers.  This should generate a regex match of size 2.  "
        "This is not the case with machine '%s'",new_machine->name.c_str());
    new_machine->id = std::stoi(m[1]);
    new_machine->prefix = new_machine->name.substr(0,m.position(1));
    


    PPK_ASSERT_ERROR(object.HasMember("core_count"),"machine %s has no core_count",new_machine->name.c_str());
    new_machine->core_count = object["core_count"].GetInt();

    PPK_ASSERT_ERROR(object.HasMember("speed"),"machine %s has no speed",new_machine->name.c_str());
    new_machine->speed = object["speed"].GetDouble();

    PPK_ASSERT_ERROR(object.HasMember("repair-time"),"machine %s has no repair-time",new_machine->name.c_str());
    new_machine->repair_time = object["repair-time"].GetDouble();

    _machinesM[new_machine->name] = new_machine; //all machines in a map.  Can get the machine if you know the machine name
    _machinesV.push_back(new_machine);//all machines in a vector.  can get machines by index.
    //_machinesByPrefix  // all machines separated by prefix name into maps separated by machine id.
    //check if the prefix has already been made
    if (_machinesByPrefix.count(new_machine->prefix) == 1)
    {
        //the prefix has already been made
        //get the map associated with that prefix and add the machine
        std::map<int,Machine *>* aMap=_machinesByPrefix.at(new_machine->prefix);
        (*aMap)[new_machine->id]=new_machine;

    }
    else
    {
        //the prefix has not already been made
        //make a new map and add the machine to that map
        std::map<int,Machine *> * aMap = new std::map<int,Machine *>;
        (*aMap)[new_machine->id]=new_machine;
        _machinesByPrefix[new_machine->prefix]=aMap;

    }
}
    
