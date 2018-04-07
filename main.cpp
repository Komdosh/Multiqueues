#include <iostream>
#include "Multiqueues.h"

using namespace std;

struct threadData {
    int  threadId;
};

Multiqueues *multiqueues;

void *RunMultiqueueExperiment(void *threadarg) {
    struct threadData *threadData;
    threadData = (struct threadData *) threadarg;

    for(int i = 0; i<1E6; ++i){
        multiqueues->insert(rand()%15);
    }

    for(int i = 0; i<5000; ++i){
        cout<<multiqueues->deleteMax()<<endl;
    }

    pthread_exit(nullptr);
}

int main(int argc, char* argv[]) {
    srand(time(nullptr));
    multiqueues = new Multiqueues(atoi(argv[1]), atoi(argv[2]));
    pthread_t threads[multiqueues->numOfThreads];
    struct threadData td[multiqueues->numOfThreads];

    for(int i = 0; i < multiqueues->numOfThreads; i++ ) {
        td[i].threadId = i;
        int rc = pthread_create(&threads[i], nullptr, RunMultiqueueExperiment, (void *)&td[i]);

        if (rc) {
            cout << "Error: thread wasn't created," << rc << endl;
            exit(-1);
        }
    }

    pthread_exit(nullptr);
}