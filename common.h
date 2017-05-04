#ifndef COMMON_H_INCLUDED
#define COMMON_H_INCLUDED

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

#define ERR(source) (fprintf(stderr, "%s:%d\n", __FILE__, __LINE__), \
                    perror(source), exit(EXIT_FAILURE) )

void setHandler(int signal, void (*handler)(int));
int makeSocket(int domain, int type);
void bindSocket(int serverSocket, int16_t port);
void bindSocketAndListen(int serverSocket, int16_t port, int backlog);
sigset_t prepareBlockMask();

ssize_t bulkRead(int fd, char *buf, size_t length);
ssize_t bulkWrite(int fd, char *buf, size_t length);

#endif
