#include "common.h"
#include "worker-thread.h"

void setWorkerThreadSignalHandling(sigset_t *previousMask)
{
    sigset_t blockMask = prepareBlockMask();
    if (pthread_sigmask(SIG_BLOCK, &blockMask, previousMask) < 0)
        ERR("pthread_sigmask");
}

void workerThreadCleanup(workerThreadArgs_t *workerArgs, sigset_t *previousMask)
{
    safeRemoveWorkerThreadFromList(workerArgs->workerThreadsList, pthread_self(), workerArgs->workerThreadsListMutex);


    free(workerArgs);
    if (pthread_sigmask(SIG_SETMASK, previousMask, NULL) < 0)
        ERR("pthread_sigmask");
}

void *workerThread(void *args)
{
    workerThreadArgs_t *workerArgs = (workerThreadArgs_t*)args;
    sigset_t previousMask;
    setWorkerThreadSignalHandling(&previousMask);
    printf("[%d] started\n", workerArgs->id);

    clientNode_t *client = safePopClientFromQueue(workerArgs->clientQueue, workerArgs->clientQueueMutex);

    printf("[%d] closing connection to the client\n", workerArgs->id);
    if (TEMP_FAILURE_RETRY(close(client->clientSocket)) < 0)
        ERR("close");
    free(client);

    int threadId = workerArgs->id;
    workerThreadCleanup(workerArgs, &previousMask);
    printf("[%d] terminated\n", threadId);
    return NULL;
}
