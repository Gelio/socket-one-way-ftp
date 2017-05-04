#ifndef CLIENT_QUEUE_H_INCLUDED
#define CLIENT_QUEUE_H_INCLUDED

#include <netinet/in.h>
#include <netinet/tcp.h>

typedef struct clientNode {
    int clientSocket;
    struct sockaddr_in clientAddr;
    socklen_t clientAddrLen;
    struct clientNode *next;
} clientNode_t;

void addClientToQueue(clientNode_t **head, int clientSocket, struct sockaddr_in *clientAddr, socklen_t *clientAddrLen);
void safeAddClientToQueue(clientNode_t **head, int clientSocket, struct sockaddr_in *clientAddr, socklen_t *clientAddrLen, pthread_mutex_t *mutex);
clientNode_t *popClientFromQueue(clientNode_t **head);
clientNode_t *safePopClientFromQueue(clientNode_t **head, pthread_mutex_t *mutex);
void clearClientQueue(clientNode_t **head);

#endif
