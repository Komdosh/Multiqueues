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
    typedef typename boost::heap::d_ary_heap<int, boost::heap::mutable_<true>, boost::heap::arity<2>> PriorityQueue;
    PriorityQueue *internalQueues;
    PriorityQueue prime;
    std::mutex *locks;
    std::mutex primeLock;

    Multiqueues();
    Multiqueues(int numOfThreads, int numOfQueuesPerThread);

    void insertIntoPrime(int insertNum);

    int deleteMaxPrime();
    void insert(int insertNum);
    void insertByThreadId(int insertNum, int threadId);
    int deleteMax();
    int deleteMaxByThreadId(int threadId);
    int deleteMaxByThreadOwn(int threadId);

    int getRandomQueueIndexForHalf() const;

    int getQueIndexForDelete(int queueIndex, int secondQueueIndex) const;

    void printSize();

    void balance();
};


#endif //MUTIQUEUES_MULTIQUEUES_H
