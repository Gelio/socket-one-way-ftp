#ifndef SERVER_HELPERS_H_INCLUDED
#define SERVER_HELPERS_H_INCLUDED

#include <signal.h>

#include "common.h"
#include "worker-list.h"
#include "client-queue.h"

#define CLIENTS_MIN 2
#define CLIENTS_MAX 5
#define PORT_MIN 1000

struct serverData {
  int serverSocket;
  sigset_t *previousMask;
  sigset_t *previousMaskWithSigPipe;
  workerThreadNode_t **workerThreadsList;
  pthread_mutex_t *workerThreadsListMutex;
  clientNode_t **clientQueue;
  pthread_mutex_t *clientQueueMutex;
};

volatile sig_atomic_t shouldQuit;
void sigIntHandler(int signal);
void usage(char *fileName);
void parseArguments(int argc, char **argv, int *groupSize, int16_t *port);
void setSignalHandling(sigset_t *previousMask, sigset_t *previousMaskWithSigPipe);
void performCleanup(struct serverData *serverData);



#endif
