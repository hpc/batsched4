#pragma once

#include <string>

#include <boost/icl/interval_set.hpp>
#include <boost/icl/closed_interval.hpp>

struct MachineRange
{
public:
    typedef boost::icl::closed_interval<int, std::less> ClosedInterval;
    typedef boost::icl::interval_set<int,
        ICL_COMPARE_INSTANCE(ICL_COMPARE_DEFAULT, int),
        ClosedInterval> Set;

public:
    MachineRange();
    MachineRange(const ClosedInterval & interval);
    MachineRange(const MachineRange & other);
    MachineRange(int machine_id);

    Set::element_iterator elements_begin();
    Set::element_const_iterator elements_begin() const;

    Set::element_iterator elements_end();
    Set::element_const_iterator elements_end() const;

    Set::iterator intervals_begin();
    Set::const_iterator intervals_begin() const;

    Set::iterator intervals_end();
    Set::const_iterator intervals_end() const;

    void clear();
    void insert(const MachineRange & range);
    void insert(ClosedInterval interval);
    void insert(int machine_id);

    void remove(const MachineRange & range);
    void remove(ClosedInterval interval);
    void remove(int machine_id);

    MachineRange left(int nb_machines) const;

    Set::const_iterator biggest_interval() const;

    int first_element() const;
    unsigned int size() const;
    bool contains(int machine_id) const;

    std::string to_string_brackets(const std::string & union_str = "∪",
                                   const std::string & opening_bracket = "[",
                                   const std::string & closing_bracket = "]",
                                   const std::string & sep = ",") const;
    std::string to_string_hyphen(const std::string & sep = ",", const std::string & joiner = "-") const;
    std::string to_string_elements(const std::string & sep = ",") const;

    MachineRange & operator=(const MachineRange & other);
    MachineRange & operator=(const MachineRange::ClosedInterval & interval);

    bool operator==(const MachineRange & other) const;
    bool operator!=(const MachineRange & other) const;

    MachineRange & operator&=(const MachineRange & other); // a &= b  <==> a = intersection(a, b)
    MachineRange & operator-=(const MachineRange & other); // a -= b  <==> a = a \ b (set difference)
    MachineRange & operator+=(const MachineRange & other); // a += b  <==> a = union(a,b)

    MachineRange operator-(const MachineRange & other) const; // a - b <==> a \ b (set difference)
    MachineRange operator+(const MachineRange & other) const; // a + b <==> union(a,b)
    MachineRange operator&(const MachineRange & other) const; // a & b <==> intersection(a,b)

public:
    static MachineRange from_string_hyphen(const std::string & str,
                                           const std::string & sep = ",",
                                           const std::string & joiner = "-",
                                           const std::string & error_prefix = "Invalid machine range string");
    static MachineRange empty_range();

private:
    Set set;
};
