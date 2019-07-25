//
// Created by komdosh on 4/7/18.
//
#ifndef MUTIQUEUES_MULTIQUEUES_H
#define MUTIQUEUES_MULTIQUEUES_H

#include "boost/heap/d_ary_heap.hpp"
#include <mutex>

class Multiqueues {
    int numOfThreads;
    int numOfQueuesPerThread;
    int numOfQueues = 2;
    unsigned int *seed = new unsigned int[1];
    typedef typename boost::heap::d_ary_heap<int, boost::heap::mutable_<true>, boost::heap::arity<2>> PriorityQueue;
    PriorityQueue *internalQueues;
    std::mutex *locks;

    int getRandomQueueIndexForHalf() const;

    int getRandomQueueIndex() const;

    int getQueIndexForDelete(int queueIndex, int secondQueueIndex) const;

    int getTopValue(int queueIndex) const;

public:
    void printSize();

    long getSize();

    void balance();

    void insert(int insertNum);
    void insertByThreadId(int insertNum, int threadId);
    int deleteMax();
    int deleteMaxByThreadId(int threadId);
    int deleteMaxByThreadOwn(int threadId);

    Multiqueues();
    Multiqueues(int numOfThreads, int numOfQueuesPerThread);

    ~Multiqueues();
};


#endif //MUTIQUEUES_MULTIQUEUES_H
