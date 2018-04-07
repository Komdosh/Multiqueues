//
// Created by komdosh on 4/7/18.
//
#ifndef MUTIQUEUES_MULTIQUEUES_H
#define MUTIQUEUES_MULTIQUEUES_H

#include "boost/heap/d_ary_heap.hpp"
#include <atomic>
#include <mutex>

class Multiqueues {
public:
    int numOfThreads;
    int numOfQueuesPerThread;
    int numOfQueues;
    typedef typename boost::heap::d_ary_heap<int, boost::heap::mutable_<true>, boost::heap::arity<2>> Queues;
    Queues *internalQueues;
    std::mutex *locks;

    Multiqueues();
    Multiqueues(int numOfThreads, int numOfQueuesPerThread);

    void insert(int insertNum);
    int deleteMax();
};


#endif //MUTIQUEUES_MULTIQUEUES_H
