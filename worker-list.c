#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "worker-list.h"

void addWorkerThreadToList(workerThreadNode_t **head, pthread_t tid)
{
    workerThreadNode_t *newNode = malloc(sizeof(workerThreadNode_t));
    if (newNode == NULL)
        ERR("malloc");

    newNode->tid = tid;
    newNode->next = *head;
    *head = newNode;
}

void removeWorkerThreadFromList(workerThreadNode_t **head, pthread_t tid)
{
    if (*head == NULL)
        return;

    if ((*head)->tid == tid)
    {
        workerThreadNode_t *nodeToRemove = *head;
        *head = nodeToRemove->next;
        free(nodeToRemove);
        return;
    }

    workerThreadNode_t *previousNode = *head;
    while (previousNode->next != NULL)
    {
        workerThreadNode_t *currentNode = previousNode->next;
        if (currentNode->tid == tid)
        {
            previousNode->next = currentNode->next;
            free(currentNode);
            return;
        }
        previousNode = currentNode;
    }
}

void safeRemoveWorkerThreadFromList(workerThreadNode_t **head, pthread_t tid, pthread_mutex_t *mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
        ERR("pthread_mutex_lock");

    removeWorkerThreadFromList(head, tid);

    if (pthread_mutex_unlock(mutex) != 0)
        ERR("pthread_mutex_unlock");
}

void clearWorkerThreadList(workerThreadNode_t **head)
{
    while (*head != NULL)
    {
        workerThreadNode_t *next = (*head)->next;
        free(*head);
        *head = next;
    }
}

workerThreadNode_t *findWorkerThreadNode(workerThreadNode_t *head, pthread_t tid)
{
    while (head != NULL)
    {
        if (head->tid == tid)
            return head;
        head = head->next;
    }

    return NULL;
}

void _joinWorkerThreads(workerThreadNode_t *head)
{
    workerThreadNode_t *node = head;
    while (node != NULL)
    {
        if (pthread_join(node->tid, NULL) != 0)
            ERR("pthread_join");
        node = node->next;
    }
}

void joinWorkerThreads(workerThreadNode_t *head, pthread_mutex_t *listMutex)
{
    if (listMutex != NULL && pthread_mutex_lock(listMutex) != 0)
        ERR("pthread_mutex_lock");

    _joinWorkerThreads(head);

    if (listMutex != NULL && pthread_mutex_unlock(listMutex) != 0)
        ERR("pthread_mutex_unlock");
}
