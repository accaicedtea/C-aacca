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

static int udp_sock = -1;
static pthread_t beacon_thread, listener_thread;
static volatile int running = 0;
static char my_name[32];
static unsigned short my_tcp_port;
static char my_ip[16];

static void get_my_ip(char *ip_buf) {
    struct sockaddr_in dummy;
    socklen_t len = sizeof(dummy);
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) {
        strcpy(ip_buf, "127.0.0.1");
        return;
    }
    dummy.sin_family = AF_INET;
    dummy.sin_addr.s_addr = inet_addr("8.8.8.8");
    dummy.sin_port = htons(53);
    connect(s, (struct sockaddr*)&dummy, sizeof(dummy));
    getsockname(s, (struct sockaddr*)&dummy, &len);
    inet_ntop(AF_INET, &dummy.sin_addr, ip_buf, 16);
    close(s);
}

static void *beacon_sender(void *arg) {
    (void)arg;
    
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) return NULL;
    
    int broadcast = 1;
    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(DISCOVERY_PORT);
    broadcast_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

    char msg[128];
    
    while (running) {
        snprintf(msg, sizeof(msg), "P2P|%s|%s|%u", my_name, my_ip, my_tcp_port);
        sendto(sock, msg, strlen(msg), 0, 
               (struct sockaddr*)&broadcast_addr, sizeof(broadcast_addr));
        sleep(2);  // Beacon ogni 2 secondi
    }
    
    close(sock);
    return NULL;
}

static void handle_beacon(const char *msg, const char *sender_ip) {
    // Formato: P2P|name|ip|port
    if (strncmp(msg, "P2P|", 4) != 0) return;
    
    char name[32], ip[16];
    unsigned int port;
    
    if (sscanf(msg + 4, "%31[^|]|%15[^|]|%u", name, ip, &port) != 3) return;
    
    // Ignora se stesso
    if (strcmp(ip, my_ip) == 0 && port == my_tcp_port) return;
    
    // Usa l'IP del mittente reale (ignora quello nel messaggio per sicurezza)
    peer_add(name, sender_ip, (unsigned short)port);
}

static void *beacon_listener(void *arg) {
    (void)arg;
    
    char buf[256];
    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);

    while (running) {
        int n = recvfrom(udp_sock, buf, sizeof(buf)-1, 0,
                         (struct sockaddr*)&sender_addr, &addr_len);
        if (n > 0) {
            buf[n] = '\0';
            char sender_ip[16];
            inet_ntop(AF_INET, &sender_addr.sin_addr, sender_ip, sizeof(sender_ip));
            handle_beacon(buf, sender_ip);
        }
    }
    return NULL;
}

void discovery_start(const char *name, unsigned short tcp_port) {
    strncpy(my_name, name, sizeof(my_name)-1);
    my_name[sizeof(my_name)-1] = '\0';
    my_tcp_port = tcp_port;
    get_my_ip(my_ip);
    running = 1;

    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) {
        perror("UDP socket");
        return;
    }

    int broadcast = 1;
    setsockopt(udp_sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));
    setsockopt(udp_sock, SOL_SOCKET, SO_REUSEADDR, &broadcast, sizeof(broadcast));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DISCOVERY_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(udp_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("UDP bind");
        close(udp_sock);
        return;
    }

    pthread_create(&beacon_thread, NULL, beacon_sender, NULL);
    pthread_create(&listener_thread, NULL, beacon_listener, NULL);
    
    printf("Discovery avviato. Nome: %s, IP: %s, Porta TCP: %u\n", 
           my_name, my_ip, my_tcp_port);
}

void discovery_stop(void) {
    running = 0;
    if (udp_sock >= 0) close(udp_sock);
    pthread_join(beacon_thread, NULL);
    pthread_join(listener_thread, NULL);
}