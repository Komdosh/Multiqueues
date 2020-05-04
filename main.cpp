#include <iostream>
#include <x86intrin.h>
#include "macos/cpu_helper.h"
#include "macos/pthread_helper.h"
#include "Multiqueues.h"

#define MAX_INSERTED_NUM 1000000
#define INSERT_ELEMENTS 1000000
#define DELETE_ELEMENTS 500000
#define RANDOM_ELEMENTS 400000
#define BALANCE 0
#define CPU_FRQ 2.5E9
#define CORES 6
#define REPEATS 5

#define METHODS 3*3

using namespace std;

enum MultiqueueMode {
    ORIGINAL, OPTIMIZED_HALF, OPTIMIZED_EXACT
};

struct threadData {
    int threadId;
};

int threadsCount = CORES;
Multiqueues<int> *multiqueues;
long *throughputInsert;
long *throughputDelete;
long *throughputRandom;

long **throughputByThread;
long **throughputByQueue;

pthread_barrier_t barrier;

void initThroughputArrays(int threads, int queues) {
    throughputByThread = new long *[threads];
    for (int i = 0; i < threads; ++i) {
        throughputByThread[i] = new long[METHODS];
        for (int j = 0; j < METHODS; ++j) {
            throughputByThread[i][j] = 0;
        }
    }

    throughputByQueue = new long *[queues];
    for (int i = 0; i < queues; ++i) {
        throughputByQueue[i] = new long[METHODS];
        for (int j = 0; j < METHODS; ++j) {
            throughputByQueue[i][j] = 0;
        }
    }
}

void printCSVThroughputTable(int repeat, int max, long **throughput, bool isThreads) {
    cout << endl;
    if (isThreads) {
        cout << "=== Throughput by Threads ===" << endl;
    } else {
        cout << "=== Throughput by Queues ===" << endl;
    }
    cout << "Repeat: " << repeat << endl;
    cout << (isThreads ? "Thread" : "Queue")
         << ",OriginalInsert,OriginalDelete,OriginalRandom,OptimizedHalfInsert,OptimizedHalfDelete,OptimizedHalfRandom,OptimizedExactInsert,OptimizedExactDelete,OptimizedExactRandom"
         << endl;
    for (int i = 0; i < max; ++i) {
        cout << i + 1 << ",";
        for (int j = 0; j < METHODS; ++j) {
            cout << throughput[i][j] << ((j + 1 != METHODS) ? "," : "");
        }
        cout << endl;
    }
}


void saveThroughput(const string &type, int threadId, uint64_t start, uint64_t end, long numOfElement) {
    double secs = (static_cast<double>(end - start)) / CPU_FRQ;
    auto throughput = static_cast<long>(numOfElement / secs);
//    cout << "[" << type << "] " << throughput << " throughput per sec" << endl;
    if (type == "DELETE") {
        throughputDelete[threadId] = throughput;
    } else if (type == "INSERT") {
        throughputInsert[threadId] = throughput;
    } else if (type == "RANDOM") {
        throughputRandom[threadId] = throughput;
    }
}

void *RunOriginalExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;

    unsigned int seed = 0;
    int operationsCount = INSERT_ELEMENTS / threadsCount;

    uint64_t start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
        multiqueues->insert(insertedNum);
    }
    saveThroughput("INSERT", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    operationsCount = DELETE_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        multiqueues->deleteMax();
    }
    saveThroughput("DELETE", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    seed = 0;
    operationsCount = RANDOM_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int mode = rand_r(&seed) % 3;
        if (mode == 0) {
            multiqueues->deleteMax();
        } else {
            int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
            multiqueues->insert(insertedNum);
        }
    }
    saveThroughput("RANDOM", threadData->threadId, start, __rdtsc(), operationsCount);

    return nullptr;
}

void *RunOptimizedHalfExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;

    unsigned int seed = 0;
    int operationsCount = INSERT_ELEMENTS / threadsCount;

    uint64_t start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
        multiqueues->insertByThreadId(insertedNum);
    }
    saveThroughput("INSERT", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    operationsCount = DELETE_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        multiqueues->deleteMaxByThreadId();
    }
    saveThroughput("DELETE", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    seed = 0;
    operationsCount = RANDOM_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int mode = rand_r(&seed) % 3;
        if (mode == 0) {
            multiqueues->deleteMaxByThreadId();
        } else {
            int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
            multiqueues->insertByThreadId(insertedNum);
        }
    }
    saveThroughput("RANDOM", threadData->threadId, start, __rdtsc(), operationsCount);

    return nullptr;
}

void *RunOptimizedExactExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;

    unsigned int seed = 0;
    int operationsCount = INSERT_ELEMENTS / threadsCount;

    uint64_t start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
        multiqueues->insertByThreadId(insertedNum);
    }
    saveThroughput("INSERT", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    operationsCount = DELETE_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        multiqueues->deleteMaxByThreadOwn();
    }
    saveThroughput("DELETE", threadData->threadId, start, __rdtsc(), operationsCount);

    pthread_barrier_wait(&barrier);
    seed = 0;
    operationsCount = RANDOM_ELEMENTS / threadsCount;
    start = __rdtsc();
    for (int i = 0; i < operationsCount; ++i) {
        int mode = rand_r(&seed) % 3;
        if (mode == 0) {
            multiqueues->deleteMaxByThreadOwn();
        } else {
            int insertedNum = rand_r(&seed) % MAX_INSERTED_NUM;
            multiqueues->insertByThreadId(insertedNum);
        }
    }
    saveThroughput("RANDOM", threadData->threadId, start, __rdtsc(), operationsCount);

    return nullptr;
}

void runExperiment(const cpu_set_t *cpuset, int numOfThreads, int numOfQueues, bool isThreads, MultiqueueMode mode) {
    pthread_t threads[numOfThreads];
    struct threadData td[numOfThreads];
    throughputDelete = new long[numOfThreads]{0};
    throughputInsert = new long[numOfThreads]{0};
    throughputRandom = new long[numOfThreads]{0};
    void *(*routine)(void *);
    switch (mode) {
        case ORIGINAL:
            routine = RunOriginalExperiment;
            break;
        case OPTIMIZED_HALF:
            routine = RunOptimizedHalfExperiment;
            break;
        case OPTIMIZED_EXACT:
            routine = RunOptimizedExactExperiment;
            break;
    }


    for (int i = 0; i < numOfThreads; i++) {
        td[i].threadId = i;

        int rc = pthread_create(&threads[i], nullptr, routine, (void *) &td[i]);

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
    long throughputRandomSum = 0;

    for (int i = 0; i < numOfThreads; i++) {
        pthread_join(threads[i], nullptr);

        throughputDeleteSum += throughputDelete[i];
        throughputInsertSum += throughputInsert[i];
        throughputRandomSum += throughputRandom[i];
    }

    int dataOffset;
    switch (mode) {
        case ORIGINAL:
            dataOffset = 0;
            break;
        case OPTIMIZED_HALF:
            dataOffset = 3;
            break;
        case OPTIMIZED_EXACT:
            dataOffset = 6;
            break;
    }
    if (isThreads) {
        throughputByThread[numOfThreads - 1][dataOffset] += throughputInsertSum / REPEATS;
        throughputByThread[numOfThreads - 1][dataOffset + 1] += throughputDeleteSum / REPEATS;
        throughputByThread[numOfThreads - 1][dataOffset + 2] += throughputRandomSum / REPEATS;
    } else {
        throughputByQueue[numOfQueues - 1][dataOffset] += throughputInsertSum / REPEATS;
        throughputByQueue[numOfQueues - 1][dataOffset + 1] += throughputDeleteSum / REPEATS;
        throughputByQueue[numOfQueues - 1][dataOffset + 2] += throughputRandomSum / REPEATS;
    }
}

void runThreads(const cpu_set_t *cpuset, int numOfThreads, int startThreads, const MultiqueueMode &mode) {
    for (int threads = startThreads; threads < numOfThreads + 1; ++threads) {
        for (int repeat = 0; repeat < REPEATS; ++repeat) {
            multiqueues = new Multiqueues<int>(threads, 2);
            pthread_barrier_init(&barrier, nullptr, threads);
            runExperiment(cpuset, threads, 0, true, mode);
            pthread_barrier_destroy(&barrier);
            printCSVThroughputTable(repeat, numOfThreads, throughputByThread, true);
        }
    }
}

void runQueues(const cpu_set_t *cpuset, int numOfQueues, int startQueues, int queuesStep, const MultiqueueMode &mode) {
    for (int queues = startQueues; queues < numOfQueues + 1; queues += queuesStep) {
        for (int repeat = 0; repeat < REPEATS; ++repeat) {
            multiqueues = new Multiqueues<int>(CORES, queues);
            pthread_barrier_init(&barrier, nullptr, CORES);
            runExperiment(cpuset, CORES, queues, false, mode);
            pthread_barrier_destroy(&barrier);
            printCSVThroughputTable(repeat, numOfQueues, throughputByThread, true);
            printCSVThroughputTable(repeat, numOfQueues, throughputByQueue, false);
        }
    }
}

int main(int argc, char *argv[]) {

    cout
            << "Multiqueue"
               " [maxThreads: 8]"
               " [maxQueues: 4]"
               " [startModeThreads: 0 - Original\\1 - Optimized Half\\2 - Optimized Exact]"
               " [startModeQueues: 0 - Original\\1 - Optimized Half\\2 - Optimized Exact]"
               " [startThreads: 1..maxThreads]"
               " [startQueues: 1..maxQueues]"
               " [queuesStep: 1\\2\\3]"
            << endl
            << "Example 8 2 0 0 1 1 2"
            << endl;
    cpu_set_t cpuset[CORES];

    for (int i = 0; i < CORES; i++) {
        CPU_ZERO(&cpuset[i]);
        CPU_SET(i, &cpuset[i]);
    }

    int maxThreads = atoi(argv[1]);
    int maxQueues = atoi(argv[2]);
    int startModeThreads = atoi(argv[3]);
    int startModeQueues = atoi(argv[4]);
    int startThreads = atoi(argv[5]);
    int startQueues = atoi(argv[6]);
    int queuesStep = atoi(argv[7]);

    initThroughputArrays(maxThreads, maxQueues);

    threadsCount = maxThreads;
    cout
            << "Multiqueue" <<
            " [maxThreads: " << maxThreads << "]" <<
            " [maxQueues: " << maxQueues << "]" <<
            " [startModeThreads: " << startModeThreads << "]" <<
            " [startModeQueues:  " << startModeQueues << "]" <<
            " [startThreads:  " << startThreads << "]" <<
            " [startQueues:  " << startQueues << "]" <<
            " [queuesStep:  " << queuesStep << "]"
            << endl;

    if (startModeThreads == 0) {
        runThreads(cpuset, maxThreads, startThreads, ORIGINAL);
    }
    if (startModeThreads == 0 || startModeThreads == 1) {
        runThreads(cpuset, maxThreads, startThreads, OPTIMIZED_HALF);
    }
    if (startModeThreads == 0 || startModeThreads == 1 || startModeThreads == 2) {
        runThreads(cpuset, maxThreads, startThreads, OPTIMIZED_EXACT);
    }

    if (startModeQueues == 0) {
        runQueues(cpuset, maxQueues, startThreads, queuesStep, ORIGINAL);
    }

    if (startModeQueues == 0 || startModeQueues == 1) {
        runQueues(cpuset, maxQueues, startThreads, queuesStep, OPTIMIZED_HALF);
    }

    if (startModeQueues == 0 || startModeQueues == 1 || startModeQueues == 2) {
        runQueues(cpuset, maxQueues, startThreads, queuesStep, OPTIMIZED_EXACT);
    }

    pthread_exit(nullptr);
}