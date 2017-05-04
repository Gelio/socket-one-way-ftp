#ifndef WORKER_THREAD_H_INCLUDED
#define WORKER_THREAD_H_INCLUDED

#include <pthread.h>
#include "client-queue.h"
#include "worker-list.h"

typedef struct workerThreadArgs {
    int id;
    clientNode_t **clientQueue;
    pthread_mutex_t *clientQueueMutex;
    workerThreadNode_t **workerThreadsList;
    pthread_mutex_t *workerThreadsListMutex;
} workerThreadArgs_t;

void *workerThread(void *args);

#endif
