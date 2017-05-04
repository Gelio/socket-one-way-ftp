#ifndef CLIENT_QUEUE_H_INCLUDED
#define CLIENT_QUEUE_H_INCLUDED

typedef struct clientNode {
    int clientSocket;
    struct clientNode *next;
} clientNode_t;

void addClientToQueue(clientNode_t **head, int clientSocket);
void safeAddClientToQueue(clientNode_t **head, int clientSocket, pthread_mutex_t *mutex);
clientNode_t *popClientFromQueue(clientNode_t **head);
clientNode_t *safePopClientFromQueue(clientNode_t **head, pthread_mutex_t *mutex);
void clearClientQueue(clientNode_t **head);

#endif
