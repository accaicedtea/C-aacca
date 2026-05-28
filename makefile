all:
	gcc -Wall -Wextra -O2 -pthread -o p2pchat p2pchat.c

clean:
	rm -f p2pchat