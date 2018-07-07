#include <iostream>
#include <x86intrin.h>
#include "Multiqueues.h"

#define MAX_INSERTED_NUM 1000000
#define INSERT_PER_THREAD 1000000
#define DELETE_PER_THREAD 100000
#define BALANCE 0
#define CPU_FRQ 2.5E9
#define CORES 4

using namespace std;

struct threadData {
    int threadId;
    int mode;
};

Multiqueues *multiqueues;
long* throughputsInsert;
long* throughputsDelete;

void printInfo(const string &type, int threadId, uint64_t start, long numOfElement) {
    double secs = (__rdtsc() - start) / CPU_FRQ;
    //cout << "[" << type << "] Thread " << threadId << " finish in " << secs << " sec" << endl;
    auto throughput = static_cast<long>(numOfElement / secs);
    cout << "[" << type << "] Thread " << threadId << (threadId > 9 ? "" : " ") << " have " << throughput
         << " throughput per sec" << endl;
    if(type == "DELETE"){
        throughputsDelete[threadId]=throughput;
    } else if(type == "INSERT"){
        throughputsInsert[threadId]=throughput;
    }

}

void *RunMultiqueueExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;
    uint64_t start = __rdtsc();

    unsigned int seed = 0;
    for (int i = 0; i < INSERT_PER_THREAD; ++i) {
        int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
        if (threadData->mode == 0) {
            multiqueues->insert(insertedNum);
        } else {
            multiqueues->insertByThreadId(insertedNum, threadData->threadId);
        }
    }
    printInfo("INSERT", threadData->threadId, start, INSERT_PER_THREAD);

    start = __rdtsc();
    for (int i = 0; i < DELETE_PER_THREAD; ++i) {
        if (threadData->mode == 0) {
            multiqueues->deleteMax();
        } else if (threadData->mode == 1) {
            multiqueues->deleteMaxByThreadId(threadData->threadId);
        } else {
            multiqueues->deleteMaxByThreadOwn(threadData->threadId);
        }
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
    cpu_set_t cpuset[CORES];

    for (int i = 0; i < CORES; i++) {
        CPU_ZERO(&cpuset[i]);
        CPU_SET(i, &cpuset[i]);
    }

    int numOfThreads = atoi(argv[1]);
    int numOfQueuesPerThread = atoi(argv[2]);
    multiqueues = new Multiqueues(numOfThreads, numOfQueuesPerThread);
    pthread_t threads[multiqueues->numOfThreads];
    struct threadData td[multiqueues->numOfThreads];
    throughputsDelete = new long[multiqueues->numOfThreads];
    throughputsInsert = new long[multiqueues->numOfThreads];
    for (int i = 0; i < multiqueues->numOfThreads; i++) {
        td[i].threadId = i;
        td[i].mode = 0;
        int rc = pthread_create(&threads[i], nullptr, RunMultiqueueExperiment, (void *) &td[i]);

        int s = pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuset[i % CORES]);
        if (s != 0) {
            printf("Thread %d affinities was not set", i);
            pthread_exit(nullptr);
        }

        if (rc) {
            cout << "Error: thread wasn't created," << rc << endl;
            exit(-1);
        }
    }

    long throughputDeleteSum = 0;
    long throughputInsertSum = 0;

    for (int i = 0; i < numOfThreads; i++) {
        pthread_join(threads[i], nullptr);
        throughputDeleteSum += throughputsDelete[i];
        throughputInsertSum += throughputsInsert[i];
    }

    cout<<"SUM THROUGHPUT INSERT: "<<throughputInsertSum<<endl;
    cout<<"SUM THROUGHPUT DELETE: "<<throughputDeleteSum<<endl;

    delete multiqueues;
    delete throughputsDelete;
    delete throughputsInsert;
    pthread_exit(nullptr);
}