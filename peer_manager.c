#include "peer_manager.h"
#include "file_transfer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;
peer_t *peers = NULL;
int peer_count = 0;

void peer_init(void) {
    pthread_mutex_init(&list_mutex, NULL);
}

void peer_add(const char *name, const char *ip, unsigned short port) {
    pthread_mutex_lock(&list_mutex);
    
    // Controlla se già esiste
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            // Aggiorna il nome se cambiato
            snprintf(peers[i].name, sizeof(peers[i].name), "%s", name);
            pthread_mutex_unlock(&list_mutex);
            return;
        }
    }

    // Nuova connessione TCP
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        pthread_mutex_unlock(&list_mutex);
        return;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        closesocket(sock);
        pthread_mutex_unlock(&list_mutex);
        return;
    }

    peers = realloc(peers, (peer_count + 1) * sizeof(peer_t));
    snprintf(peers[peer_count].name, sizeof(peers[peer_count].name), "%s", name);
    snprintf(peers[peer_count].ip, sizeof(peers[peer_count].ip), "%s", ip);
    peers[peer_count].port = port;
    peers[peer_count].sock = sock;
    peers[peer_count].connected = 1;
    peer_count++;

    printf("\n✓ Connesso a %s (%s:%u)\n> ", name, ip, port);
    fflush(stdout);
    pthread_mutex_unlock(&list_mutex);
}

void peer_remove(SOCKET sock) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].sock == sock) {
            printf("\n✗ Disconnesso da %s\n> ", peers[i].name);
            fflush(stdout);
            closesocket(sock);
            if (i != peer_count - 1)
                memmove(&peers[i], &peers[i+1], (peer_count - i - 1) * sizeof(peer_t));
            peer_count--;
            peers = realloc(peers, peer_count * sizeof(peer_t));
            break;
        }
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_remove_by_index(int index) {
    pthread_mutex_lock(&list_mutex);
    if (index >= 0 && index < peer_count) {
        closesocket(peers[index].sock);
        if (index != peer_count - 1)
            memmove(&peers[index], &peers[index+1], (peer_count - index - 1) * sizeof(peer_t));
        peer_count--;
        peers = realloc(peers, peer_count * sizeof(peer_t));
    }
    pthread_mutex_unlock(&list_mutex);
}

int peer_find_by_name(const char *name) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (strcmp(peers[i].name, name) == 0) {
            pthread_mutex_unlock(&list_mutex);
            return i;
        }
    }
    pthread_mutex_unlock(&list_mutex);
    return -1;
}

void peer_list_print(void) {
    pthread_mutex_lock(&list_mutex);
    printf("\n┌─ Peer connessi (%d) ─────────────────\n", peer_count);
    if (peer_count == 0) {
        printf("│  (nessun peer)\n");
    } else {
        for (int i = 0; i < peer_count; i++) {
            printf("│ [%d] %s @ %s:%u\n", i, peers[i].name, peers[i].ip, peers[i].port);
        }
    }
    printf("└──────────────────────────────────────\n> ");
    fflush(stdout);
    pthread_mutex_unlock(&list_mutex);
}

int peer_send_message_to(int index, const char *text) {
    pthread_mutex_lock(&list_mutex);
    int ret = -1;
    if (index >= 0 && index < peer_count && peers[index].connected) {
        ret = transfer_send_message(peers[index].sock, text);
    }
    pthread_mutex_unlock(&list_mutex);
    return ret;
}

int peer_send_file_to(int index, const char *filename) {
    pthread_mutex_lock(&list_mutex);
    int ret = -1;
    if (index >= 0 && index < peer_count && peers[index].connected) {
        ret = transfer_send_file(peers[index].sock, filename);
    }
    pthread_mutex_unlock(&list_mutex);
    return ret;
}

void peer_broadcast_message(const char *text) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected)
            transfer_send_message(peers[i].sock, text);
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_broadcast_file(const char *filename) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected)
            transfer_send_file(peers[i].sock, filename);
    }
    pthread_mutex_unlock(&list_mutex);
}

void peer_shutdown_all(void) {
    pthread_mutex_lock(&list_mutex);
    for (int i = 0; i < peer_count; i++) {
        if (peers[i].connected) {
            transfer_send_quit(peers[i].sock);
            closesocket(peers[i].sock);
        }
    }
    free(peers);
    peers = NULL;
    peer_count = 0;
    pthread_mutex_unlock(&list_mutex);
}