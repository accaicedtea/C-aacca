#ifndef P2P_PROTO_H
#define P2P_PROTO_H

#include <stdint.h>
#include <pthread.h>
#include <netinet/in.h>
#include <time.h>

#define NAME_LEN   48
#define MAX_PEERS  32

typedef struct {
    int      sock;
    char     name[NAME_LEN];
    char     ip[INET_ADDRSTRLEN];
    int      port;              /* remote listening port */
    int      active;
    time_t   last_seen;
    int      awaiting_pong;
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    time_t   connected_at;
} Peer;

/* ── Callbacks ────────────────────────────────────────────────────────────
 * ALL callbacks are invoked from background threads.
 * Any GTK call inside them MUST be dispatched via g_idle_add().
 * ──────────────────────────────────────────────────────────────────────── */

/* Text message received */
typedef void (*OnMessageCb)(const char *peer_name, const char *msg, void *userdata);

/* Peer connected (connected=1) or disconnected (connected=0) */
typedef void (*OnPeerEventCb)(int connected, const char *peer_name,
                               const char *ip, void *userdata);

/* Generic log line */
typedef void (*OnLogCb)(const char *text, void *userdata);

/* Incoming file request.  MUST BLOCK until the user decides.
 * Return: malloc'd absolute save path (caller frees), or NULL to reject. */
typedef char *(*OnFileInCb)(const char *peer_name, const char *filename,
                             uint64_t size_bytes, void *userdata);

/* ── AppData ────────────────────────────────────────────────────────────── */
typedef struct {
    Peer            peers[MAX_PEERS];
    pthread_mutex_t peers_lock;

    volatile int    running;
    char            myname[NAME_LEN];
    char            myip[INET_ADDRSTRLEN];
    int             myport;

    /* Transfer progress (updated from threads, read from GUI timer) */
    volatile uint64_t xfer_total;
    volatile uint64_t xfer_done;
    volatile int      xfer_active;
    char              xfer_name[256];
    char              xfer_direction;   /* 'S'=send  'R'=recv */

    /* Callbacks */
    OnMessageCb    on_message;
    OnPeerEventCb  on_peer_event;
    OnLogCb        on_log;
    OnFileInCb     on_file_incoming;
    void          *cb_userdata;
} AppData;

extern AppData g_data;

/* ── Public API ─────────────────────────────────────────────────────────── */
int  p2p_init    (const char *name, int port, const char *ip);
void p2p_start   (void);
void p2p_stop    (void);
int  p2p_send_text (int sock, const char *msg);
int  p2p_send_file (int sock, const char *path);
void p2p_connect   (const char *ip, int port);

#endif /* P2P_PROTO_H */