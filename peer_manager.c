#include "peer_manager.h"
#include "file_transfer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef _WIN32
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
#endif

// Definizione delle variabili globali
pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;
peer_t *peers = NULL;
int peer_count = 0;

void peer_init(void) {
    pthread_mutex_init(&list_mutex, NULL);
}

void peer_add(const char *ip, unsigned short port) {
    pthread_mutex_lock(&list_mutex);
    
    // Controlla se già connesso
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            pthread_mutex_unlock(&list_mutex);
            return;
        }
    }

    // Nuova connessione TCP
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        perror("socket");
        pthread_mutex_unlock(&list_mutex);
        return;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect to peer");
        closesocket(sock);
        pthread_mutex_unlock(&list_mutex);
        return;
    }

    peers = realloc(peers, (peer_count + 1) * sizeof(peer_t));
    peers[peer_count].sock = sock;
    strncpy(peers[peer_count].ip, ip, 15);
    peers[peer_count].ip[15] = '\0';
    peers[peer_count].port = port;
    peer_count++;

    printf("\n✓ Connesso a peer %s:%u\n", ip, port);
    pthread_mutex_unlock(&list_mutex);
}

void peer_remove(SOCKET sock) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].sock == sock) {
            closesocket(sock);
            printf("\n✗ Peer %s:%u disconnesso\n", peers[i].ip, peers[i].port);
            if (i != peer_count - 1)
                memmove(&peers[i], &peers[i+1], (peer_count - i - 1) * sizeof(peer_t));
            peer_count--;
            peers = realloc(peers, peer_count * sizeof(peer_t));
            break;
        }
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_broadcast_message(const char *text) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        transfer_send_message(peers[i].sock, text);
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_broadcast_file(const char *filename) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        transfer_send_file(peers[i].sock, filename);
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_shutdown_all(void) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        transfer_send_quit(peers[i].sock);
        closesocket(peers[i].sock);
    }
    free(peers);
    peers = NULL;
    peer_count = 0;
    pthread_mutex_unlock(&list_mutex);
}

void peer_list_print(void) {
    pthread_mutex_lock(&list_mutex);
    printf("Peer connessi (%d):\n", peer_count);
    for (int i = 0; i < peer_count; i++) {
        printf("  %s:%u\n", peers[i].ip, peers[i].port);
    }
    if (peer_count == 0) printf("  (nessun peer connesso)\n");
    pthread_mutex_unlock(&list_mutex);
}