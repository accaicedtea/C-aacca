#ifndef P2P_PROTO_H
#define P2P_PROTO_H

#include <stdint.h>
#include <pthread.h>
#include <netinet/in.h>

#define NAME_LEN 48

typedef struct {
    int sock;
    char name[NAME_LEN];
    char ip[INET_ADDRSTRLEN];
    int port;
    int active;
    time_t last_seen;
    int awaiting_pong;
    uint64_t bytes_sent, bytes_recv;
    time_t connected_at;
} Peer;

typedef struct {
    Peer peers[32];
    pthread_mutex_t peers_lock;
    volatile int running;
    char myname[NAME_LEN];
    char myip[INET_ADDRSTRLEN];
    int myport;

    volatile uint64_t xfer_total;
    volatile uint64_t xfer_done;
    volatile int xfer_active;
    char xfer_name[256];
} AppData;

extern AppData g_data;

int  p2p_init(const char *name, int port, const char *ip);
void p2p_start(void);
void p2p_stop(void);
int  p2p_send_text(int sock, const char *msg);
int  p2p_send_file(int sock, const char *path);
void p2p_connect(const char *ip, int port);

#endif