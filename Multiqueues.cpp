//
// Created by komdosh on 4/7/18.
//

#include "Multiqueues.h"
#include <iostream>

Multiqueues::Multiqueues(int numOfThreads, int numOfQueuesPerThread) {
    this->numOfThreads = numOfThreads;
    this->numOfQueuesPerThread = numOfQueuesPerThread;
    this->numOfQueues = numOfThreads * numOfQueuesPerThread;
    internalQueues = new PriorityQueue[numOfQueues];
    locks = new std::mutex[numOfQueues];
}

Multiqueues::~Multiqueues() {
    for (int i = 0; i < numOfQueues; ++i) {
        internalQueues[i].clear();
    }
    delete[] internalQueues;
    delete[] locks;
}

void Multiqueues::insert(int insertNum) {
    int queueIndex;
    do {
        queueIndex = getRandomQueueIndex();
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
            queueIndex = getRandomQueueIndexForHalf() + this->numOfQueues / 2;
        }
    } while (!locks[queueIndex].try_lock());
    internalQueues[queueIndex].push(insertNum);
    locks[queueIndex].unlock();
}

int Multiqueues::deleteMax() {
    int queueIndex;
    int secondQueueIndex;
    do {
        queueIndex = getRandomQueueIndex();
        secondQueueIndex = getRandomQueueIndex();
        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
    } while (queueIndex == -1 || !locks[queueIndex].try_lock());
    return getTopValue(queueIndex);
}

int Multiqueues::getTopValue(int queueIndex) const {
    int topValue = -1;
    if (!internalQueues[queueIndex].empty()) {
        internalQueues[queueIndex].top();
        internalQueues[queueIndex].pop();
    }
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
            queueIndex = getRandomQueueIndexForHalf() + this->numOfQueues / 2;
            secondQueueIndex = getRandomQueueIndexForHalf() + this->numOfQueues / 2;
        }

        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
    } while (queueIndex == -1 || !locks[queueIndex].try_lock());
    return getTopValue(queueIndex);
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
            queueIndex = getRandomQueueIndex();
            secondQueueIndex = getRandomQueueIndex();
        }

        queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
    } while (queueIndex == -1 || !locks[queueIndex].try_lock());
    return getTopValue(queueIndex);
}

int Multiqueues::getQueIndexForDelete(int queueIndex, int secondQueueIndex) const {
    if (internalQueues[queueIndex].empty() && internalQueues[secondQueueIndex].empty()) {
        return -1;
    } else if (internalQueues[queueIndex].empty() && !internalQueues[secondQueueIndex].empty()) {
        queueIndex = secondQueueIndex;
    } else {
        while (!locks[queueIndex].try_lock());
        int value = -1;
        if (!internalQueues[secondQueueIndex].empty())
            value = internalQueues[secondQueueIndex].top();
        locks[queueIndex].unlock();
        while (!locks[secondQueueIndex].try_lock());
        int value2 = -1;
        if (!internalQueues[secondQueueIndex].empty())
            value2 = internalQueues[secondQueueIndex].top();
        locks[secondQueueIndex].unlock();
        if (value2 > value) {
            queueIndex = secondQueueIndex;
        }
    }

    if (queueIndex != -1 && internalQueues[queueIndex].empty()) {
        return -1;
    }
    return queueIndex;
}

void Multiqueues::printSize() {
    for (int i = 0; i < this->numOfQueues; ++i) {
        std::cout << "Queue " << i << " has size " << internalQueues[i].size() << std::endl;
    }
};

void Multiqueues::balance() {
    int sumOfSizes = 0;
    int sizes[this->numOfQueues] = {0};
    int indexWithMax = 0, indexWithMin = 0;
    for (int i = 0; i < this->numOfQueues; ++i) {
        sizes[i] = internalQueues[i].size();
        sumOfSizes += sizes[i];
        if (sizes[indexWithMax] < sizes[i]) {
            indexWithMax = i;
        }
        if (i == 0 || sizes[indexWithMin] > sizes[i]) {
            indexWithMin = i;
        }
    }

    int averageSize = sumOfSizes / this->numOfQueues; //double is needless
    if (sizes[indexWithMax] > averageSize * 1.2) { // if max sized queue is 20% bigger and more than average
        while (!locks[indexWithMax].try_lock());
        while (!locks[indexWithMin].try_lock());
        int sizeOfTransfer = static_cast<int>(sizes[indexWithMax] * 0.3); // 30% elements transfer to smallest queue
        for (int i = 0; i < sizeOfTransfer; ++i) {
            internalQueues[indexWithMin].push(internalQueues[indexWithMax].top());
            internalQueues[indexWithMax].pop();
        }
        locks[indexWithMax].unlock();
        locks[indexWithMin].unlock();
    }
}

int Multiqueues::getRandomQueueIndexForHalf() const { return rand_r(this->seed) % (this->numOfQueues / 2); }

int Multiqueues::getRandomQueueIndex() const { return rand_r(this->seed) % this->numOfQueues; }

long Multiqueues::getSize() {
    long numOfElements = 0;
    for (int i = 0; i < this->numOfQueues; ++i) {
        numOfElements += internalQueues[i].size();
    }
    return numOfElements;
}
