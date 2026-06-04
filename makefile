CFLAGS = -Wall -Wextra -O2 -pthread `pkg-config --cflags gtk4`
LDFLAGS = `pkg-config --libs gtk4`

p2pchat: p2p_proto.o p2p_gui.o
	gcc -o p2pchat p2p_proto.o p2p_gui.o $(LDFLAGS) -lm

p2p_proto.o: p2p_proto.c p2p_proto.h
	gcc $(CFLAGS) -c p2p_proto.c

p2p_gui.o: p2p_gui.c p2p_proto.h
	gcc $(CFLAGS) -c p2p_gui.c

clean:
	rm -f *.o p2pchat