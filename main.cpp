#include <iostream>
#include "Multiqueues.h"

#define MAX_INSERTED_NUM 1000000
#define INSERT_PER_THREAD 1000000
#define DELETE_PER_THREAD 500000
#define BALANCE 0

using namespace std;

struct threadData {
    int threadId;
};

Multiqueues *multiqueues;

void *RunMultiqueueExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;
    clock_t start;

    start = clock();
    for (int i = 0; i < INSERT_PER_THREAD; ++i) {
        int insertedNum = rand() % MAX_INSERTED_NUM;
        multiqueues->insert(insertedNum);
        //multiqueues->insertByThreadId(insertedNum, threadData->threadId);

        //multiqueues->insertIntoPrime(insertedNum); //For accuracy experiment
    }
    cout << "[INSERT] Thread " << threadData->threadId << " finish in " << clock() - start << " tick " << endl;

    start = clock();
    for (int i = 0; i < DELETE_PER_THREAD; ++i) {
        int relaxedMax = multiqueues->deleteMax();
        //int relaxedMax = multiqueues->deleteMaxByThreadId(threadData->threadId);
        //int relaxedMax = multiqueues->deleteMaxByThreadOwn(threadData->threadId);

        //int primeMax = multiqueues->deleteMaxPrime();
        //cout << "[MAX] Thread " << threadData->threadId << " relaxed " << relaxedMax << " prime "<< primeMax << " diff " << abs(relaxedMax-primeMax) << endl;
    }
    cout << "[DELETE] Thread " << threadData->threadId << " finish in " << clock() - start << " tick" << endl;

    if (threadData->threadId == 0 && BALANCE) {
        start = clock();
        multiqueues->balance();
        cout << "Thread " << threadData->threadId << " finish balance in " << clock() - start << " tick" << endl;
    }

    pthread_exit(nullptr);
}

int main(int argc, char *argv[]) {
    // srand(time(nullptr));
    srand(0);
    int numOfThreads = atoi(argv[1]);
    int numOfQueuesPerThread = atoi(argv[2]);
    multiqueues = new Multiqueues(numOfThreads, numOfQueuesPerThread);
    pthread_t threads[multiqueues->numOfThreads];
    struct threadData td[multiqueues->numOfThreads];

    for (int i = 0; i < multiqueues->numOfThreads; i++) {
        td[i].threadId = i;
        int rc = pthread_create(&threads[i], nullptr, RunMultiqueueExperiment, (void *) &td[i]);

        if (rc) {
            cout << "Error: thread wasn't created," << rc << endl;
            exit(-1);
        }
    }

    pthread_exit(nullptr);
}