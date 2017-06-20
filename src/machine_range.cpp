#include "machine_range.hpp"

#include <vector>

#include <boost/algorithm/string.hpp>

#include "pempek_assert.hpp"

using namespace std;

MachineRange::MachineRange()
{
    PPK_ASSERT_ERROR(size() == 0);
}

MachineRange::MachineRange(const MachineRange::ClosedInterval &interval)
{
    insert(interval);
}

MachineRange::MachineRange(const MachineRange &other)
{
    set = other.set;
}

MachineRange::MachineRange(int machine_id)
{
    insert(machine_id);
}

MachineRange::Set::element_iterator MachineRange::elements_begin()
{
    return boost::icl::elements_begin(set);
}

MachineRange::Set::element_const_iterator MachineRange::elements_begin() const
{
    return boost::icl::elements_begin(set);
}

MachineRange::Set::element_iterator MachineRange::elements_end()
{
    return boost::icl::elements_end(set);
}

MachineRange::Set::element_const_iterator MachineRange::elements_end() const
{
    return boost::icl::elements_end(set);
}

MachineRange::Set::iterator MachineRange::intervals_begin()
{
    return set.begin();
}

MachineRange::Set::const_iterator MachineRange::intervals_begin() const
{
    return set.begin();
}

MachineRange::Set::iterator MachineRange::intervals_end()
{
    return set.end();
}

MachineRange::Set::const_iterator MachineRange::intervals_end() const
{
    return set.end();
}


void MachineRange::clear()
{
    set.clear();
    PPK_ASSERT_ERROR(size() == 0);
}

void MachineRange::insert(const MachineRange &range)
{
    for (auto it = range.intervals_begin(); it != range.intervals_end(); ++it)
        set.insert(*it);
}

void MachineRange::insert(ClosedInterval interval)
{
    set.insert(interval);
}

void MachineRange::insert(int machine_id)
{
    set.insert(machine_id);
}

void MachineRange::remove(const MachineRange &range)
{
    //printf("set=%s\n", to_string_elements().c_str());
    //printf("range=%s\n", range.to_string_elements().c_str());
    set -= range.set;
    //printf("set=%s\n", to_string_elements().c_str());
}

void MachineRange::remove(ClosedInterval interval)
{
    set -= interval;
}

void MachineRange::remove(int machine_id)
{
    set -= machine_id;
}

MachineRange MachineRange::left(int nb_machines) const
{
    PPK_ASSERT_ERROR(set.size() >= (unsigned int)nb_machines,
                     "Invalid MachineRange::left call: looking for %d machines in a set of size %lu",
                     nb_machines, set.size());

    // Let's find the value of the nth element
    int nb_inserted = 0;
    MachineRange res;

    for (auto it = intervals_begin(); it != intervals_end() && nb_inserted < nb_machines; ++it)
    {
        // The size of the current interval
        int interval_size = it->upper() - it->lower() + 1;
        int nb_to_add = std::min(interval_size, nb_machines - nb_inserted);

        res.insert(ClosedInterval(it->lower(), it->lower() + nb_to_add - 1));
        nb_inserted += nb_to_add;
        PPK_ASSERT_ERROR(res.size() == (unsigned int) nb_inserted, "Invalid MachineRange size: got %u, expected %d",
                         res.size(), nb_inserted);

        //printf("left, res=%s (%s)\n", res.to_string_hyphen().c_str(), res.to_string_elements().c_str());
    }

    PPK_ASSERT_ERROR(res.size() == (unsigned int) nb_machines, "Invalid MachineRange size : got %u, expected %d", res.size(), nb_machines);
    /*printf("left, available=%s (%s), available_size=%u, nb_machines=%d, res=%s (%s), res_size=%u\n",
           to_string_hyphen().c_str(), to_string_elements().c_str(), size(), nb_machines,
           res.to_string_hyphen().c_str(), res.to_string_elements().c_str(), res.size());*/
    return res;
}

MachineRange::Set::const_iterator MachineRange::biggest_interval() const
{
    if (size() == 0)
        return intervals_end();

    auto res = intervals_begin();
    int res_size = res->upper() - res->lower() + 1;

    for (auto interval_it = ++intervals_begin(); interval_it != intervals_end(); ++interval_it)
    {
        int interval_size = interval_it->upper() - interval_it->lower() + 1;

        if (interval_size > res_size)
        {
            res = interval_it;
            res_size = interval_size;
        }
    }

    return res;
}

int MachineRange::first_element() const
{
    PPK_ASSERT_ERROR(size() > 0);
    return *elements_begin();
}

unsigned int MachineRange::size() const
{
    return set.size();
}

bool MachineRange::contains(int machine_id) const
{
    return boost::icl::contains(set, machine_id);
}

std::string MachineRange::to_string_brackets(const std::string & union_str,
                                             const std::string & opening_bracket,
                                             const std::string & closing_bracket,
                                             const std::string & sep) const
{
    vector<string> machine_id_strings;

    if (size() == 0)
        machine_id_strings.push_back(opening_bracket + closing_bracket);
    else
    {
        for (auto it = intervals_begin(); it != intervals_end(); ++it)
            if (it->lower() == it->upper())
                machine_id_strings.push_back(opening_bracket + to_string(it->lower()) + closing_bracket);
            else
                machine_id_strings.push_back(opening_bracket + to_string(it->lower()) + sep + to_string(it->upper()) + closing_bracket);
    }

    return boost::algorithm::join(machine_id_strings, union_str);
}

std::string MachineRange::to_string_hyphen(const std::string &sep, const std::string &joiner) const
{
    vector<string> machine_id_strings;
    for (auto it = intervals_begin(); it != intervals_end(); ++it)
        if (it->lower() == it->upper())
            machine_id_strings.push_back(to_string(it->lower()));
        else
            machine_id_strings.push_back(to_string(it->lower()) + joiner + to_string(it->upper()));

    return boost::algorithm::join(machine_id_strings, sep);
}

string MachineRange::to_string_elements(const string &sep) const
{
    vector<string> machine_id_strings;
    for (auto it = elements_begin(); it != elements_end(); ++it)
        machine_id_strings.push_back(to_string(*it));

    return boost::algorithm::join(machine_id_strings, sep);
}

MachineRange &MachineRange::operator=(const MachineRange &other)
{
    set = other.set;
    return *this;
}

MachineRange &MachineRange::operator=(const MachineRange::ClosedInterval &interval)
{
    set.clear();
    PPK_ASSERT_ERROR(set.size() == 0);
    set.insert(interval);
    return *this;
}

bool MachineRange::operator==(const MachineRange &other) const
{
    return set == other.set;
}

bool MachineRange::operator!=(const MachineRange &other) const
{
    return set != other.set;
}

MachineRange & MachineRange::operator&=(const MachineRange &other)
{
    set &= other.set;
    return *this;
}

MachineRange &MachineRange::operator-=(const MachineRange &other)
{
    set -= other.set;
    return *this;
}

MachineRange &MachineRange::operator+=(const MachineRange &other)
{
    set += other.set;
    return *this;
}

MachineRange MachineRange::operator-(const MachineRange &other) const
{
    MachineRange ret = *this;
    ret -= other;
    return ret;
}

MachineRange MachineRange::operator+(const MachineRange &other) const
{
    MachineRange ret = *this;
    ret += other;
    return ret;
}

MachineRange MachineRange::operator&(const MachineRange &other) const
{
    MachineRange ret = *this;
    ret &= other;
    return ret;
}

MachineRange MachineRange::empty_range()
{
    return MachineRange();
}

MachineRange MachineRange::from_string_hyphen(const string &str, const string &sep, const string &joiner, const string & error_prefix)
{
    MachineRange res;

    // Let us do a split by sep to get all the parts
    vector<string> parts;
    boost::split(parts, str, boost::is_any_of(sep), boost::token_compress_on);

    for (const string & part : parts)
    {
        // Since each machineIDk can either be a single machine or a closed interval, let's try to split on joiner
        vector<string> interval_parts;
        boost::split(interval_parts, part, boost::is_any_of(joiner), boost::token_compress_on);
        PPK_ASSERT_ERROR(interval_parts.size() >= 1 && interval_parts.size() <= 2,
                         "%s: the MIDk '%s' should either be a single machine ID"
                         " (syntax: MID to represent the machine ID MID) or a closed interval (syntax: MIDa-MIDb to represent"
                         " the machine interval [MIDA,MIDb])", error_prefix.c_str(), part.c_str());

        if (interval_parts.size() == 1)
        {
            int machine_id = std::stoi(interval_parts[0]);
            res.insert(machine_id);
        }
        else
        {
            int machineIDa = std::stoi(interval_parts[0]);
            int machineIDb = std::stoi(interval_parts[1]);

            PPK_ASSERT_ERROR(machineIDa <= machineIDb, "%s: the MIDk '%s' is composed of two"
                             " parts (1:%d and 2:%d) but the first value must be lesser than or equal to the second one",
                             error_prefix.c_str(), part.c_str(), machineIDa, machineIDb);

            res.insert(MachineRange::ClosedInterval(machineIDa, machineIDb));
        }
    }

    return res;
}

int MachineRange::operator[](int index) const
{
    PPK_ASSERT_ERROR(index >= 0 && index < (int)this->size(),
                     "Invalid call to MachineRange::operator[]: index (%d) should be in [0,%d[",
                     index, (int)this->size());

    // TODO: avoid O(n) retrieval
    auto machine_it = this->elements_begin();
    for (int i = 0; i < index; ++i)
        ++machine_it;

    return *machine_it;
}
