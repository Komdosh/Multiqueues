#include <iostream>
#include "Multiqueues.h"

#define MAX_INSERTED_NUM 1000000
#define INSERT_PER_THREAD 10000000
#define DELETE_PER_THREAD 500000
#define BALANCE 0
#define CPU_FRQ 2.5E9

using namespace std;

struct threadData {
    int threadId;
};

Multiqueues *multiqueues;

void printInfo(const string &type, int threadId, uint64_t start, long numOfElement) {
    double secs = (__rdtsc() - start) / CPU_FRQ;
    //cout << "[" << type << "] Thread " << threadId << " finish in " << secs << " sec" << endl;
    long throughput = static_cast<long>(numOfElement / secs);
    cout << "[" << type << "] Thread " << threadId << " have " << throughput << " throughput per sec" << endl;
}

void *RunMultiqueueExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;
    uint64_t start = __rdtsc();

    unsigned int seed = 0;
    for (int i = 0; i < INSERT_PER_THREAD; ++i) {
        int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
        multiqueues->insert(insertedNum);
        // multiqueues->insertByThreadId(insertedNum, threadData->threadId);
    }
    printInfo("INSERT", threadData->threadId, start, INSERT_PER_THREAD);

    start = __rdtsc();
    for (int i = 0; i < DELETE_PER_THREAD; ++i) {
        int relaxedMax = multiqueues->deleteMax();
        //int relaxedMax = multiqueues->deleteMaxByThreadId(threadData->threadId);
        //int relaxedMax = multiqueues->deleteMaxByThreadOwn(threadData->threadId);
    }
    printInfo("DELETE", threadData->threadId, start, DELETE_PER_THREAD);

    if (threadData->threadId == 0 && BALANCE) {
        start = __rdtsc();
        multiqueues->balance();
        printInfo("BALANCE", threadData->threadId, start, multiqueues->getSize());
    }

    pthread_exit(nullptr);
}

int main(int argc, char *argv[]) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < 4; i++)
        CPU_SET(i, &cpuset);

    int numOfThreads = atoi(argv[1]);
    int numOfQueuesPerThread = atoi(argv[2]);
    multiqueues = new Multiqueues(numOfThreads, numOfQueuesPerThread);
    pthread_t threads[multiqueues->numOfThreads];
    struct threadData td[multiqueues->numOfThreads];
    for (int i = 0; i < multiqueues->numOfThreads; i++) {
        td[i].threadId = i;
        int rc = pthread_create(&threads[i], nullptr, RunMultiqueueExperiment, (void *) &td[i]);

        int s = pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuset);
        if (s != 0) {
            printf("Thread %d affinities was not set", i);
            pthread_exit(nullptr);
        }

        if (rc) {
            cout << "Error: thread wasn't created," << rc << endl;
            exit(-1);
        }
    }

    pthread_exit(nullptr);
}