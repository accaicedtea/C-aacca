CC := gcc
CFLAGS := -Wall -Wextra -O2 -pthread
SRCS := main.c discovery.c peer_manager.c file_transfer.c
OBJS := $(SRCS:.c=.o)
TARGET := p2pchat

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c discovery.h peer_manager.h file_transfer.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean