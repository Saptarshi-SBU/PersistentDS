#ifndef _TRACE_H
#define _TRACE_H

#include <iostream>

using namespace std;

template <typename T>
void trace_record(T t) {
    std::cout << t ;
}

// recursive variadic function {
template<typename T, typename... Args>
void trace_record(T t, Args... args) {
    std::cout << t;
    trace_record(args...) ;
}

template<typename T, typename... Args>
void trace_record(bool debug, T t, Args... args) {
    if (debug) {
	trace_record(t, args...);
        std::cout << endl; 	
    }
}
#endif
