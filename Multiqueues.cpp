//
// Created by komdosh on 4/7/18.
//

#include "Multiqueues.h"

Multiqueues::Multiqueues(int numOfThreads, int numOfQueuesPerThread) {
    this->numOfThreads = numOfThreads;
    this->numOfQueuesPerThread = numOfQueuesPerThread;
    this->numOfQueues = numOfThreads*numOfQueuesPerThread;
    internalQueues = new Queues[numOfQueues];
    locks = new std::mutex[numOfQueues];
}

void Multiqueues::insert(int insertNum) {
    int queueIndex;
    do {
        queueIndex = rand()%this->numOfQueues;
    } while (!locks[queueIndex].try_lock());
    internalQueues[queueIndex].push(insertNum);
    locks[queueIndex].unlock();
}

int Multiqueues::deleteMax(){
    int queueIndex;
    int secondQueueIndex;
    do {
        queueIndex = rand()%this->numOfQueues;
        secondQueueIndex = rand()%this->numOfQueues;
        if(internalQueues[queueIndex].empty() || internalQueues[secondQueueIndex].empty()){
            continue;
        }
        if(internalQueues[queueIndex].top()>internalQueues[secondQueueIndex].top()){
            queueIndex = secondQueueIndex;
        }
    }while(!locks[queueIndex].try_lock());
    int topValue = internalQueues[queueIndex].top();
    internalQueues[queueIndex].pop();
    locks[queueIndex].unlock();
    return topValue;
}