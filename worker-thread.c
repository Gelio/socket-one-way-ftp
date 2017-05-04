#include "common.h"
#include "worker-thread.h"
#include "server-helpers.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>

#define BUFFER_SIZE 400
#define MAX_FILE_NAME 100

void setWorkerThreadSignalHandling(sigset_t *previousMask)
{
    sigset_t blockMask = prepareBlockMask();
    if (pthread_sigmask(SIG_BLOCK, &blockMask, previousMask) < 0)
        ERR("pthread_sigmask");
}

void workerThreadCleanup(workerThreadArgs_t *workerArgs, sigset_t *previousMask)
{
    if (!shouldQuit)
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
    char *clientAddrReadable = inet_ntoa(client->clientAddr.sin_addr);
    if (clientAddrReadable == NULL)
    {
        printf("[%d] cannot determine clients IP", workerArgs->id);
        clientAddrReadable = "unknown";
    }

    printf("[%d] client connected from %s (port %d)\n", workerArgs->id, clientAddrReadable, ntohs(client->clientAddr.sin_port));
    char fileName[MAX_FILE_NAME];
    snprintf(fileName, MAX_FILE_NAME, "%s_%d.out", clientAddrReadable, ntohs(client->clientAddr.sin_port));
    int fileDes = open(fileName, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fileDes < 0)
        printf("[%d] cannot open file %s\n", workerArgs->id, fileName);
    else
    {
        char buffer[BUFFER_SIZE];
        while (1)
        {
            ssize_t bytesRead = bulkRead(client->clientSocket, buffer, BUFFER_SIZE);
            if (bytesRead < 0)
            {
                printf("[%d] read error\n", workerArgs->id);
                perror("read error");
                break;
            }
            else if (bytesRead == 0)
                break;

            ssize_t bytesWritten = bulkWrite(fileDes, buffer, bytesRead);
            if (bytesWritten <= 0)
            {
                printf("[%d] write error\n", workerArgs->id);
                perror("write error");
                break;
            }
        }
    }


    printf("[%d] closing connection to the client\n", workerArgs->id);
    if (TEMP_FAILURE_RETRY(close(client->clientSocket)) < 0)
        ERR("close client socket");
    free(client);
    if (fileDes >= 0 && TEMP_FAILURE_RETRY(close(fileDes)) < 0)
        ERR("close file descriptor");

    int threadId = workerArgs->id;
    workerThreadCleanup(workerArgs, &previousMask);
    printf("[%d] terminated\n", threadId);
    return NULL;
}
