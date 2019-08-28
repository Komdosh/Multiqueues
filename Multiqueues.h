//
// Created by komdosh on 4/7/18.
//
#ifndef MUTIQUEUES_MULTIQUEUES_H
#define MUTIQUEUES_MULTIQUEUES_H

#include "boost/heap/d_ary_heap.hpp"
#include <mutex>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <map>

template<typename T>
class Multiqueues {
    int numOfThreads;
    int numOfQueuesPerThread;
    int numOfQueues = 2;
    unsigned int *seed = new unsigned int[1];
    typedef typename boost::heap::d_ary_heap<T, boost::heap::mutable_<true>, boost::heap::arity<2>> PriorityQueue;
    PriorityQueue *internalQueues;
    std::mutex *locks;
    std::unordered_map<std::__thread_id, size_t> threadsMap;

    int getQueIndexForDelete(int queueIndex, int secondQueueIndex) const {
        if (internalQueues[queueIndex].empty() && internalQueues[secondQueueIndex].empty()) {
            return -1;
        } else if (internalQueues[queueIndex].empty() && !internalQueues[secondQueueIndex].empty()) {
            queueIndex = secondQueueIndex;
        } else {
            while (!locks[queueIndex].try_lock());
            int value = -1;
            if (!internalQueues[queueIndex].empty())
                value = internalQueues[queueIndex].top();
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

    inline int getRandomQueueIndexForHalf() const { return rand_r(this->seed) % (this->numOfQueues / 2); }


    inline int getRandomQueueIndex() const { return rand_r(this->seed) % this->numOfQueues; }

    T getTopValue(const int queueIndex) const {
        int topValue = -1;
        if (!internalQueues[queueIndex].empty()) {
            topValue = internalQueues[queueIndex].top();
            internalQueues[queueIndex].pop();
        }
        locks[queueIndex].unlock();
        return topValue;
    }

public:

    inline int getThreadId() {
        std::thread::id threadId = std::this_thread::get_id();
        std::hash<std::thread::id> hasher;
        return hasher(threadId) % this->numOfThreads;
    }

    Multiqueues(int numOfThreads, int numOfQueuesPerThread) {
        this->numOfThreads = numOfThreads;
        this->numOfQueuesPerThread = numOfQueuesPerThread;
        this->numOfQueues = numOfThreads * numOfQueuesPerThread;
        internalQueues = new PriorityQueue[numOfQueues];
        locks = new std::mutex[numOfQueues];
    }

    ~Multiqueues() {
        for (int i = 0; i < numOfQueues; ++i) {
            internalQueues[i].clear();
        }
        delete[] internalQueues;
        delete[] locks;
    }

    void insert(const T insertNum) {
        int queueIndex;
        do {
            queueIndex = getRandomQueueIndex();
        } while (!locks[queueIndex].try_lock());
        internalQueues[queueIndex].push(insertNum);
        locks[queueIndex].unlock();
    }

    void insertByThreadId(const T insertNum) {
        const int threadId = getThreadId();
        int queueIndex;
        do {
            const int halfOfThreads = this->numOfThreads / 2;

            if (threadId < halfOfThreads) {
                queueIndex = getRandomQueueIndexForHalf();
            } else {
                queueIndex = getRandomQueueIndexForHalf() + this->numOfQueues / 2;
            }
        } while (!locks[queueIndex].try_lock());
        internalQueues[queueIndex].push(insertNum);
        locks[queueIndex].unlock();
    }


    T deleteMax() {
        int queueIndex;
        int secondQueueIndex;
        do {
            queueIndex = getRandomQueueIndex();
            secondQueueIndex = getRandomQueueIndex();
            queueIndex = getQueIndexForDelete(queueIndex, secondQueueIndex);
        } while (queueIndex == -1 || !locks[queueIndex].try_lock());
        return getTopValue(queueIndex);
    }

    T deleteMaxByThreadId() {
        const int threadId = getThreadId();
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

    T deleteMaxByThreadOwn() {
        const int threadId = getThreadId();
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

    void balance() {
        int sumOfSizes = 0;
        int sizes[this->numOfQueues];
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

    void printSize() {
        for (int i = 0; i < this->numOfQueues; ++i) {
            std::cout << "Queue " << i << " has size " << internalQueues[i].size() << std::endl;
        }
    };

    long getSize() {
        long numOfElements = 0;
        for (int i = 0; i < this->numOfQueues; ++i) {
            numOfElements += internalQueues[i].size();
        }
        return numOfElements;
    }

};

#endif //MUTIQUEUES_MULTIQUEUES_H
