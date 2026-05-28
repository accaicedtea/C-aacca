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
    char name[32];
    char ip[16];
    unsigned short port;
    int connected;  // 1 se la connessione TCP è attiva
} peer_t;

extern pthread_mutex_t list_mutex;
extern peer_t *peers;
extern int peer_count;

void peer_init(void);
void peer_add(const char *name, const char *ip, unsigned short port);
void peer_remove(SOCKET sock);
void peer_remove_by_index(int index);
int peer_find_by_name(const char *name);
void peer_list_print(void);

// Invio a peer specifici
int peer_send_message_to(int index, const char *text);
int peer_send_file_to(int index, const char *filename);

// Broadcast
void peer_broadcast_message(const char *text);
void peer_broadcast_file(const char *filename);

void peer_shutdown_all(void);

#endif