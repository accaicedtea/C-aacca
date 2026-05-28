#include "discovery.h"
#include "peer_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #define CLOSESOCKET closesocket
    typedef int socklen_t;
#else
    #define CLOSESOCKET close
    #define SOCKET int
    #define INVALID_SOCKET -1
#endif

static int udp_sock = -1;
static pthread_t beacon_thread, listener_thread;
static volatile int running = 0;
static unsigned short my_tcp_port;
static char my_ip[16];

static void get_my_ip(char *ip_buf) {
    // Ottiene l'IP primario (semplificato)
    struct sockaddr_in dummy;
    socklen_t len = sizeof(dummy);
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    dummy.sin_family = AF_INET;
    dummy.sin_addr.s_addr = inet_addr("8.8.8.8");
    dummy.sin_port = htons(53);
    connect(s, (struct sockaddr*)&dummy, sizeof(dummy));
    getsockname(s, (struct sockaddr*)&dummy, &len);
    inet_ntop(AF_INET, &dummy.sin_addr, ip_buf, 16);
    CLOSESOCKET(s);
}

static void *beacon_sender(void *arg) {
    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(DISCOVERY_PORT);
    broadcast_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

    char msg[64];
    snprintf(msg, sizeof(msg), "P2P:%s:%u", my_ip, my_tcp_port);

    while (running) {
        sendto(udp_sock, msg, strlen(msg), 0,
               (struct sockaddr*)&broadcast_addr, sizeof(broadcast_addr));
        sleep(3);
    }
    return NULL;
}

static void handle_beacon(const char *msg) {
    if (strncmp(msg, "P2P:", 4) != 0) return;
    char ip[16];
    unsigned int port;
    if (sscanf(msg + 4, "%15[^:]:%u", ip, &port) != 2) return;
    if (strcmp(ip, my_ip) == 0 && port == my_tcp_port) return; // ignora sé stesso

    peer_add(ip, (unsigned short)port);
}

static void *beacon_listener(void *arg) {
    char buf[128];
    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);

    while (running) {
        int n = recvfrom(udp_sock, buf, sizeof(buf)-1, 0,
                         (struct sockaddr*)&sender_addr, &addr_len);
        if (n > 0) {
            buf[n] = '\0';
            handle_beacon(buf);
        }
    }
    return NULL;
}

void discovery_start(unsigned short tcp_port) {
    my_tcp_port = tcp_port;
    get_my_ip(my_ip);
    running = 1;

    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    int broadcast = 1;
    setsockopt(udp_sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DISCOVERY_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(udp_sock, (struct sockaddr*)&addr, sizeof(addr));

    pthread_create(&beacon_thread, NULL, beacon_sender, NULL);
    pthread_create(&listener_thread, NULL, beacon_listener, NULL);
}

void discovery_stop(void) {
    running = 0;
    CLOSESOCKET(udp_sock);
    pthread_join(beacon_thread, NULL);
    pthread_join(listener_thread, NULL);
}