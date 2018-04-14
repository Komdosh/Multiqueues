//
// Created by komdosh on 4/7/18.
//

#include "Multiqueues.h"
#include <iostream>
Multiqueues::Multiqueues(int numOfThreads, int numOfQueuesPerThread) {
    this->numOfThreads = numOfThreads;
    this->numOfQueuesPerThread = numOfQueuesPerThread;
    this->numOfQueues = numOfThreads * numOfQueuesPerThread;
    internalQueues = new Queues[numOfQueues];
    locks = new std::mutex[numOfQueues];
}

void Multiqueues::insert(int insertNum) {
    int queueIndex;
    do {
        queueIndex = rand() % this->numOfQueues;
    } while (!locks[queueIndex].try_lock());
    internalQueues[queueIndex].push(insertNum);
    locks[queueIndex].unlock();
}

void Multiqueues::insertByThreadId(int insertNum, int threadId) {
    int queueIndex;
    do {
        int halfOfThreads = this->numOfThreads / 2;

        if (threadId < halfOfThreads) {
            queueIndex = getRandomQueueIndexForHalf();
        } else {
            queueIndex = getRandomQueueIndexForHalf() + this->numOfQueues/2;
        }
    } while (!locks[queueIndex].try_lock());
    internalQueues[queueIndex].push(insertNum);
    locks[queueIndex].unlock();
}

int Multiqueues::deleteMax() {
    int queueIndex;
    int secondQueueIndex;
    do {
        queueIndex = rand() % this->numOfQueues;
        secondQueueIndex = rand() % this->numOfQueues;
        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
        if (queueIndex == -1) {
            continue;
        }
    } while (!locks[queueIndex].try_lock());
    int topValue = internalQueues[queueIndex].top();
    internalQueues[queueIndex].pop();
    locks[queueIndex].unlock();
    return topValue;
}

int Multiqueues::deleteMaxByThreadId(int threadId) {
    int queueIndex;
    int secondQueueIndex;
    do {
        int halfOfThreads = this->numOfThreads / 2;
        if (threadId < halfOfThreads) {
            queueIndex = getRandomQueueIndexForHalf();
            secondQueueIndex = getRandomQueueIndexForHalf();
        } else {
            queueIndex = getRandomQueueIndexForHalf() + this->numOfQueues/2;
            secondQueueIndex = getRandomQueueIndexForHalf() + this->numOfQueues/2;
        }

        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
        if (queueIndex == -1) {
            continue;
        }
    } while (!locks[queueIndex].try_lock());
    int topValue = internalQueues[queueIndex].top();
    internalQueues[queueIndex].pop();
    locks[queueIndex].unlock();
    return topValue;
}

int Multiqueues::deleteMaxByThreadOwn(int threadId) {
    int queueIndex;
    int secondQueueIndex;
    int it = 0;
    do {
        if (!it) {
            queueIndex = threadId * numOfQueuesPerThread;
            secondQueueIndex = threadId * numOfQueuesPerThread + 1;
            ++it;
        } else {
            queueIndex = rand() % this->numOfQueues;
            secondQueueIndex = rand() % this->numOfQueues;
        }

        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
        if (queueIndex == -1) {
            continue;
        }
    } while (!locks[queueIndex].try_lock());
    int topValue = internalQueues[queueIndex].top();
    internalQueues[queueIndex].pop();
    locks[queueIndex].unlock();
    return topValue;
}

void Multiqueues::printSize() {
    int over = 0;
    for (int i = 0; i < this->numOfQueues; ++i) {
        over+=internalQueues[i].size();
        std::cout << internalQueues[i].size() << std::endl;
    }
    std::cout<< "over: "<< over<<std::endl;
}

int Multiqueues::getQueIndexForDelete(int queueIndex, int secondQueueIndex) const {
    if (internalQueues[queueIndex].empty() && internalQueues[secondQueueIndex].empty()) {
        return -1;
    } else if (internalQueues[queueIndex].empty() && !internalQueues[secondQueueIndex].empty()) {
        queueIndex = secondQueueIndex;
    } else if (!internalQueues[secondQueueIndex].empty() &&
               internalQueues[queueIndex].top() > internalQueues[secondQueueIndex].top()) {
        queueIndex = secondQueueIndex;
    }

    if (queueIndex != -1 && internalQueues[queueIndex].empty()) {
        return -1;
    }
    return queueIndex;
}

int Multiqueues::getRandomQueueIndexForHalf() const { return rand() % (this->numOfQueues / 2); }