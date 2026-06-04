# Makefile
CFLAGS = -Wall -Wextra -O2 -pthread `pkg-config --cflags gtk4`
LDFLAGS = `pkg-config --libs gtk4`

all: p2pchat

p2pchat: p2pchat.c
	gcc $(CFLAGS) -o p2pchat p2pchat.c $(LDFLAGS) -lm

clean:
	rm -f p2pchat