CC := gcc
CFLAGS := -Wall -Wextra -O2 -pthread
SRCS := main.c discovery.c peer_manager.c file_transfer.c
OBJS := $(SRCS:.c=.o)
TARGET := p2pchat

# Rileva sistema operativo per Windows
UNAME_S := $(shell uname -s)
LDFLAGS :=

ifneq (,$(findstring MINGW,$(UNAME_S)))
    LDFLAGS := -lws2_32
    TARGET := p2pchat.exe
endif
ifneq (,$(findstring MSYS,$(UNAME_S)))
    LDFLAGS := -lws2_32
    TARGET := p2pchat.exe
endif

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c discovery.h peer_manager.h file_transfer.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET) p2pchat.exe

.PHONY: all clean