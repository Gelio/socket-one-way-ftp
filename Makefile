CC=gcc
CFLAGS=-Wall -std=gnu99
LDLIBS= -lpthread

# SOURCES=$(wildcard *.c)
# HEADERS=$(wildcard *.h)
SOURCES=server.c

.PHONY: clean

all: server

clean:
	rm server

server: $(SOURCES) $(HEADERS)
	$(CC) $(SOURCES) -o server $(CFLAGS) $(LDLIBS)
