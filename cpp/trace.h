#ifndef _TRACE_H
#define _TRACE_H

#include <iostream>

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>

using namespace std;

template <typename T>
void trace_record(T t) {
    std::cout << t ;
}

// recursive variadic function {
template<typename T, typename... Args>
void trace_record(T t, Args... args) {
    std::cout << t << " ";
    trace_record(args...) ;
}

template<typename T, typename... Args>
void trace_debug(bool debug, T t, Args... args) {
#if DEBUG
        std::cerr << "DBG : ";
	trace_record(t, args...);
        std::cout << endl;
#endif        
}

template<typename T, typename... Args>
void trace_info(T t, Args... args) {
     std::cout << "INFO : ";
     trace_record(t, args...);
     std::cout<< endl;
}

template<typename T, typename... Args>
void trace_error(T t, Args... args) {
     std::cerr << "ERR : ";
     trace_record(t, args...);
     std::cerr << endl;
}
#endif
