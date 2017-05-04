#include "common.h"
#include <string.h>

#include "worker-list.h"
#include "client-queue.h"
#include "server-helpers.h"

volatile sig_atomic_t shouldQuit = 0;

void usage(char *fileName)
{
    fprintf(stderr, "Usage: %s port\n", fileName);
    fprintf(stderr, "port > %d\n", PORT_MIN);
    exit(EXIT_FAILURE);
}

void sigIntHandler(int signal)
{
    shouldQuit = 1;
}

void parseArguments(int argc, char **argv, int16_t *port)
{
    if (argc != 2)
        usage(argv[0]);

    *port = atoi(argv[1]);
    if (*port <= PORT_MIN)
        usage(argv[0]);
}

void setSignalHandling(sigset_t *previousMask, sigset_t *previousMaskWithSigPipe)
{
    setHandler(SIGINT, sigIntHandler);

    sigset_t blockMask = prepareBlockMask();
    if (sigprocmask(SIG_BLOCK, &blockMask, previousMask) != 0)
        ERR("sigprocmask");
    memcpy(previousMaskWithSigPipe, previousMask, sizeof(sigset_t));
    sigaddset(previousMaskWithSigPipe, SIGPIPE);
}

void performCleanup(struct serverData *serverData)
{
    printf("[SERVER] Closing server socket\n");
    if (TEMP_FAILURE_RETRY(close(serverData->serverSocket)) < 0)
        ERR("close");

    // Join worker threads
    printf("[SERVER] Joining worker threads\n");
    joinWorkerThreads(*(serverData->workerThreadsList), serverData->workerThreadsListMutex);
    clearWorkerThreadList(serverData->workerThreadsList);

    if (pthread_mutex_destroy(serverData->workerThreadsListMutex) != 0)
        ERR("pthread_mutex_destroy");

    // Remove client queue
    printf("[SERVER] Removing client queue\n");
    if (pthread_mutex_destroy(serverData->clientQueueMutex) != 0)
        ERR("pthread_mutex_destroy");
    clearClientQueue(serverData->clientQueue);

    if (sigprocmask(SIG_SETMASK, serverData->previousMask, NULL) != 0)
        ERR("sigprocmask");
}
