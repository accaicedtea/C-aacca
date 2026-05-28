#ifndef PEER_MANAGER_H
#define PEER_MANAGER_H

#include <pthread.h>

#ifdef _WIN32
    #include <winsock2.h>
#else
    #define SOCKET int
    #define INVALID_SOCKET -1
    #define closesocket(s) close(s)
#endif

typedef struct {
    SOCKET sock;
    char ip[16];
    unsigned short port;
} peer_t;

// Variabili globali accessibili da altri moduli
extern pthread_mutex_t list_mutex;
extern peer_t *peers;
extern int peer_count;

// Funzioni pubbliche
void peer_init(void);
void peer_add(const char *ip, unsigned short port);
void peer_remove(SOCKET sock);
void peer_broadcast_message(const char *text);
void peer_broadcast_file(const char *filename);
void peer_shutdown_all(void);
void peer_list_print(void);

#endif