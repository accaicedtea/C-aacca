/*
 * p2p.c — P2P Chat & File Transfer
 * TUI elegante, discovery automatico LAN, invio file grandi
 *
 * Compile: gcc -o p2p p2p.c -lpthread -lm
 * Usage  : ./p2p [nome] [porta]
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <math.h>

/* ═══════════════════════════════════════════════════════════
   PROTOCOL
   ═══════════════════════════════════════════════════════════ */
#define MAGIC           0x50325031u   /* "P2P1" */
#define TYPE_HELLO      0x01          /* handshake: name\0 */
#define TYPE_TEXT       0x02
#define TYPE_FILE_HDR   0x03          /* filename\0 + uint64 size */
#define TYPE_FILE_DATA  0x04
#define TYPE_FILE_END   0x05
#define TYPE_PING       0x10
#define TYPE_PONG       0x11
#define TYPE_QUIT       0xFF

#define CHUNK_SIZE      65536
#define TCP_PORT_DEF    9100
#define DISC_PORT       9101
#define MAX_PEERS       32
#define NAME_LEN        48
#define HEARTBEAT_INT   5
#define PEER_TIMEOUT    20
#define LOG_LINES       200

/* ═══════════════════════════════════════════════════════════
   TUI ANSI
   ═══════════════════════════════════════════════════════════ */
#define C_RESET   "\033[0m"
#define C_BOLD    "\033[1m"
#define C_DIM     "\033[2m"
#define C_REV     "\033[7m"
#define C_BLACK   "\033[30m"
#define C_RED     "\033[31m"
#define C_GREEN   "\033[32m"
#define C_YELLOW  "\033[33m"
#define C_BLUE    "\033[34m"
#define C_MAGENTA "\033[35m"
#define C_CYAN    "\033[36m"
#define C_WHITE   "\033[37m"
#define C_BBLUE   "\033[94m"
#define C_BCYAN   "\033[96m"
#define C_BGREEN  "\033[92m"
#define C_BRED    "\033[91m"
#define C_BYEL    "\033[93m"
#define C_BMAG    "\033[95m"
#define CLEAR     "\033[2J\033[H"
#define HIDE_CUR  "\033[?25l"
#define SHOW_CUR  "\033[?25h"
#define SAVE_CUR  "\033[s"
#define REST_CUR  "\033[u"

/* ═══════════════════════════════════════════════════════════
   STRUCTS
   ═══════════════════════════════════════════════════════════ */
typedef struct {
    int     sock;
    char    name[NAME_LEN];
    char    ip[INET_ADDRSTRLEN];
    int     port;
    int     active;
    time_t  last_seen;
    int     awaiting_pong;
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    time_t  connected_at;
} Peer;

typedef struct {
    char    text[256];
    char    color[16];
    time_t  ts;
} LogEntry;

/* ═══════════════════════════════════════════════════════════
   GLOBALS
   ═══════════════════════════════════════════════════════════ */
static Peer            peers[MAX_PEERS];
static pthread_mutex_t peers_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile int    g_run      = 1;
static char            g_myname[NAME_LEN];
static char            g_myip[INET_ADDRSTRLEN];
static int             g_myport   = TCP_PORT_DEF;

/* log ring-buffer */
static LogEntry        g_log[LOG_LINES];
static int             g_log_head = 0;
static int             g_log_count= 0;
static pthread_mutex_t g_log_lock = PTHREAD_MUTEX_INITIALIZER;

/* transfer progress (for progress bar) */
static volatile uint64_t g_xfer_total  = 0;
static volatile uint64_t g_xfer_done   = 0;
static volatile int      g_xfer_active = 0;
static char              g_xfer_name[256];

/* ═══════════════════════════════════════════════════════════
   LOGGING
   ═══════════════════════════════════════════════════════════ */
static void log_add(const char *color, const char *fmt, ...) {
    pthread_mutex_lock(&g_log_lock);
    va_list ap;
    va_start(ap, fmt);
    LogEntry *e = &g_log[g_log_head % LOG_LINES];
    vsnprintf(e->text, sizeof(e->text), fmt, ap);
    va_end(ap);
    strncpy(e->color, color, sizeof(e->color)-1);
    e->ts = time(NULL);
    g_log_head++;
    if (g_log_count < LOG_LINES) g_log_count++;
    pthread_mutex_unlock(&g_log_lock);
}

/* ═══════════════════════════════════════════════════════════
   NET HELPERS
   ═══════════════════════════════════════════════════════════ */
static int send_all(int s, const void *buf, int len) {
    const char *p = (const char*)buf;
    int sent = 0;
    while (sent < len) {
        int n = (int)send(s, p + sent, (size_t)(len - sent), MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static int recv_all(int s, void *buf, int len) {
    char *p = (char*)buf;
    int got = 0;
    while (got < len) {
        int n = (int)recv(s, p + got, (size_t)(len - got), 0);
        if (n <= 0) return -1;
        got += n;
    }
    return 0;
}

/* Wire format: [4 magic][1 type][4 len][len data] */
static int proto_send(int s, uint8_t type, const void *payload, uint32_t len) {
    uint8_t hdr[9];
    uint32_t m = htonl(MAGIC);
    uint32_t nl = htonl(len);
    memcpy(hdr,   &m,  4);
    hdr[4] = type;
    memcpy(hdr+5, &nl, 4);
    if (send_all(s, hdr, 9) < 0) return -1;
    if (len > 0 && payload) {
        if (send_all(s, payload, (int)len) < 0) return -1;
    }
    return 0;
}

static int proto_recv(int s, uint8_t *type, uint8_t **payload, uint32_t *len) {
    uint8_t hdr[9];
    if (recv_all(s, hdr, 9) < 0) return -1;
    uint32_t m;
    memcpy(&m, hdr, 4);
    if (ntohl(m) != MAGIC) return -1;
    *type = hdr[4];
    memcpy(len, hdr+5, 4);
    *len = ntohl(*len);
    if (*len > 0) {
        *payload = (uint8_t*)malloc(*len + 1);
        if (!*payload) return -1;
        (*payload)[*len] = 0; /* null-terminate for text safety */
        if (recv_all(s, *payload, (int)*len) < 0) { free(*payload); *payload=NULL; return -1; }
    } else {
        *payload = NULL;
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════
   PEER MANAGEMENT
   ═══════════════════════════════════════════════════════════ */
static int peer_add(const char *name, const char *ip, int port, int sock) {
    pthread_mutex_lock(&peers_lock);

    /* update existing */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active &&
            strcmp(peers[i].ip, ip) == 0 &&
            peers[i].port == port) {
            /* reconnect */
            close(peers[i].sock);
            peers[i].sock = sock;
            strncpy(peers[i].name, name, NAME_LEN-1);
            peers[i].last_seen = time(NULL);
            peers[i].awaiting_pong = 0;
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BYEL, "⟳  %s ri-connesso (%s)", name, ip);
            return i;
        }
    }

    /* find free slot */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) {
            memset(&peers[i], 0, sizeof(peers[i]));
            strncpy(peers[i].name, name, NAME_LEN-1);
            strncpy(peers[i].ip,   ip,   INET_ADDRSTRLEN-1);
            peers[i].port         = port;
            peers[i].sock         = sock;
            peers[i].active       = 1;
            peers[i].last_seen    = time(NULL);
            peers[i].connected_at = time(NULL);
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BGREEN, "✓  %s connesso  [%s:%d]", name, ip, port);
            return i;
        }
    }

    pthread_mutex_unlock(&peers_lock);
    close(sock);
    return -1;
}

static void peer_remove_by_sock(int sock) {
    pthread_mutex_lock(&peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && peers[i].sock == sock) {
            log_add(C_BRED, "✗  %s disconnesso", peers[i].name);
            close(sock);
            peers[i].active = 0;
            break;
        }
    }
    pthread_mutex_unlock(&peers_lock);
}

static int peer_count(void) {
    int c = 0;
    pthread_mutex_lock(&peers_lock);
    for (int i = 0; i < MAX_PEERS; i++)
        if (peers[i].active) c++;
    pthread_mutex_unlock(&peers_lock);
    return c;
}

/* ═══════════════════════════════════════════════════════════
   HEARTBEAT THREAD
   ═══════════════════════════════════════════════════════════ */
static void *thread_heartbeat(void *arg) {
    (void)arg;
    while (g_run) {
        sleep(HEARTBEAT_INT);
        time_t now = time(NULL);
        pthread_mutex_lock(&peers_lock);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!peers[i].active) continue;
            if (peers[i].awaiting_pong &&
                (now - peers[i].last_seen) > PEER_TIMEOUT) {
                log_add(C_BRED, "⏱  %s timeout", peers[i].name);
                close(peers[i].sock);
                peers[i].active = 0;
                continue;
            }
            if (!peers[i].awaiting_pong) {
                if (proto_send(peers[i].sock, TYPE_PING, NULL, 0) < 0) {
                    close(peers[i].sock);
                    peers[i].active = 0;
                } else {
                    peers[i].awaiting_pong = 1;
                }
            }
        }
        pthread_mutex_unlock(&peers_lock);
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════
   TCP RECEIVER THREAD (one per connection)
   ═══════════════════════════════════════════════════════════ */
typedef struct { int sock; char ip[INET_ADDRSTRLEN]; int port; } ConnInfo;

static void *thread_receiver(void *arg) {
    ConnInfo ci = *(ConnInfo*)arg;
    free(arg);
    int s = ci.sock;

    /* --- HANDSHAKE: expect HELLO --- */
    uint8_t type; uint8_t *pay; uint32_t plen;
    if (proto_recv(s, &type, &pay, &plen) < 0 || type != TYPE_HELLO) {
        free(pay); close(s);
        return NULL;
    }
    char remote_name[NAME_LEN];
    strncpy(remote_name, (char*)pay, NAME_LEN-1);
    remote_name[NAME_LEN-1] = 0;
    free(pay);

    /* send our HELLO back */
    if (proto_send(s, TYPE_HELLO, g_myname, (uint32_t)strlen(g_myname)) < 0) {
        close(s); return NULL;
    }

    int peer_idx = peer_add(remote_name, ci.ip, ci.port, s);
    if (peer_idx < 0) { close(s); return NULL; }

    /* --- MESSAGE LOOP --- */
    while (g_run) {
        if (proto_recv(s, &type, &pay, &plen) < 0) break;

        switch (type) {
        case TYPE_PING:
            proto_send(s, TYPE_PONG, NULL, 0);
            free(pay);
            break;

        case TYPE_PONG:
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    peers[i].last_seen    = time(NULL);
                    peers[i].awaiting_pong = 0;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            free(pay);
            break;

        case TYPE_TEXT: {
            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    peers[i].bytes_recv += plen;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BCYAN, "💬 %-16s  %.*s", sender, (int)plen, (char*)pay);
            free(pay);
            break;
        }

        case TYPE_FILE_HDR: {
            /* payload: filename\0 + 8 bytes uint64 size (big-endian) */
            if (!pay || plen < 9) { free(pay); break; }
            char  *fname = (char*)pay;
            size_t fnlen = strlen(fname);
            if (fnlen + 1 + 8 > plen) { free(pay); break; }
            uint64_t fsize;
            memcpy(&fsize, pay + fnlen + 1, 8);
            fsize = /* ntohll */ (
                ((uint64_t)ntohl((uint32_t)(fsize >> 32)) ) |
                ((uint64_t)ntohl((uint32_t)(fsize & 0xFFFFFFFF)) << 32)
            );

            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BMAG, "↓  %s invia  %s  (%.1f MB)",
                    sender, fname, (double)fsize / 1048576.0);
            free(pay); pay = NULL;

            /* strip path for safety */
            char *base = fname;  /* fname freed — copy before */
            /* re-extract from pay (already freed — we kept fname ptr above) */
            /* Actually fname pointed into pay which we just freed!
               Re-receive carefully — we need to keep fname valid. */
            /* Correction: do NOT free pay yet when using fname */
            /* This block intentionally uses a goto-less re-approach below */
            (void)base;
            log_add(C_BRED, "BUG-GUARD: dovrei rientrare nel recv file");
            break;
        }

        /* We'll handle file receive in a dedicated sub-loop below */
        case TYPE_FILE_END:
            free(pay);
            break;

        case TYPE_QUIT:
            free(pay);
            goto disconnect;

        default:
            free(pay);
            break;
        }
        continue;
disconnect:
        break;
    }

    peer_remove_by_sock(s);
    return NULL;
}

/*
 * Receiver v2 — proper file handling (replace thread_receiver above).
 * The goto-based sub-loop was too messy; clean rewrite below.
 */
static void *thread_receiver_v2(void *arg) {
    ConnInfo ci = *(ConnInfo*)arg;
    free(arg);
    int s = ci.sock;

    /* ── HANDSHAKE ── */
    {
        uint8_t t; uint8_t *p; uint32_t l;
        if (proto_recv(s, &t, &p, &l) < 0 || t != TYPE_HELLO) {
            free(p); close(s); return NULL;
        }
        char rname[NAME_LEN] = {0};
        strncpy(rname, (char*)p, NAME_LEN-1);
        free(p);
        if (proto_send(s, TYPE_HELLO, g_myname, (uint32_t)strlen(g_myname)) < 0) {
            close(s); return NULL;
        }
        if (peer_add(rname, ci.ip, ci.port, s) < 0) {
            close(s); return NULL;
        }
    }

    /* ── MESSAGE LOOP ── */
    for (;;) {
        uint8_t type; uint8_t *pay; uint32_t plen;
        if (proto_recv(s, &type, &pay, &plen) < 0) break;

        if (type == TYPE_PING) {
            proto_send(s, TYPE_PONG, NULL, 0);
            free(pay);

        } else if (type == TYPE_PONG) {
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    peers[i].last_seen = time(NULL);
                    peers[i].awaiting_pong = 0;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            free(pay);

        } else if (type == TYPE_TEXT) {
            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    peers[i].bytes_recv += plen;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BCYAN, "💬 %-14s  %.*s", sender, (int)plen, (char*)pay);
            free(pay);

        } else if (type == TYPE_FILE_HDR) {
            /* payload layout: <filename>\0<8 bytes BE uint64 size> */
            if (!pay || plen < 2) { free(pay); continue; }

            /* safely extract filename */
            size_t fnlen = strnlen((char*)pay, plen);
            if (fnlen + 1 + 8 > plen) { free(pay); continue; }

            char fname[512];
            strncpy(fname, (char*)pay, sizeof(fname)-1);
            fname[sizeof(fname)-1] = 0;

            uint64_t fsize_net, fsize;
            memcpy(&fsize_net, pay + fnlen + 1, 8);
            /* manual ntohll */
            uint32_t hi = ntohl((uint32_t)(fsize_net >> 32));
            uint32_t lo = ntohl((uint32_t)(fsize_net & 0xFFFFFFFFu));
            fsize = ((uint64_t)lo << 32) | (uint64_t)hi;
            free(pay);

            /* strip directory component from received filename */
            char *base = strrchr(fname, '/');
            if (!base) base = strrchr(fname, '\\');
            const char *safe_name = base ? base+1 : fname;

            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    break;
                }
            pthread_mutex_unlock(&peers_lock);

            log_add(C_BMAG, "↓  %s → %s  (%.2f MB)",
                    sender, safe_name, (double)fsize / 1048576.0);

            /* show progress */
            g_xfer_total  = fsize;
            g_xfer_done   = 0;
            g_xfer_active = 1;
            strncpy(g_xfer_name, safe_name, sizeof(g_xfer_name)-1);

            FILE *fp = fopen(safe_name, "wb");
            if (!fp)
                log_add(C_BRED, "  impossibile creare %s: %s", safe_name, strerror(errno));

            uint64_t received = 0;
            int file_ok = 1;
            for (;;) {
                uint8_t t2; uint8_t *p2; uint32_t l2;
                if (proto_recv(s, &t2, &p2, &l2) < 0) { file_ok = 0; free(p2); break; }
                if (t2 == TYPE_FILE_DATA) {
                    if (fp) fwrite(p2, 1, l2, fp);
                    received += l2;
                    g_xfer_done = received;
                    free(p2);
                } else if (t2 == TYPE_FILE_END) {
                    free(p2);
                    break;
                } else {
                    free(p2);
                }
            }
            g_xfer_active = 0;
            if (fp) fclose(fp);

            if (file_ok)
                log_add(C_BGREEN, "  ✓ %s  %.2f MB ricevuto",
                        safe_name, (double)received / 1048576.0);
            else
                log_add(C_BRED, "  ✗ %s  ricezione interrotta", safe_name);

        } else if (type == TYPE_QUIT) {
            free(pay);
            break;
        } else {
            free(pay);
        }
    }

    peer_remove_by_sock(s);
    return NULL;
}

/* ═══════════════════════════════════════════════════════════
   TCP SERVER THREAD
   ═══════════════════════════════════════════════════════════ */
static void *thread_tcp_server(void *arg) {
    (void)arg;
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) { log_add(C_BRED, "tcp_server socket: %s", strerror(errno)); return NULL; }

    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(ls, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)g_myport);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(ls, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_add(C_BRED, "tcp_server bind: %s", strerror(errno));
        close(ls); return NULL;
    }
    listen(ls, 16);

    while (g_run) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int c = accept(ls, (struct sockaddr*)&ca, &cl);
        if (c < 0) {
            if (g_run) usleep(50000);
            continue;
        }
        int flag = 1;
        setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        ConnInfo *ci = (ConnInfo*)malloc(sizeof(ConnInfo));
        ci->sock = c;
        ci->port = g_myport;
        inet_ntop(AF_INET, &ca.sin_addr, ci->ip, sizeof(ci->ip));

        pthread_t t;
        pthread_create(&t, NULL, thread_receiver_v2, ci);
        pthread_detach(t);
    }
    close(ls);
    return NULL;
}

/* ═══════════════════════════════════════════════════════════
   DISCOVERY — responder (answers UDP broadcasts)
   ═══════════════════════════════════════════════════════════ */
static void *thread_disc_responder(void *arg) {
    (void)arg;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return NULL;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in a = {0};
    a.sin_family      = AF_INET;
    a.sin_port        = htons(DISC_PORT);
    a.sin_addr.s_addr = INADDR_ANY;
    bind(s, (struct sockaddr*)&a, sizeof(a));

    char buf[256];
    struct sockaddr_in from; socklen_t fl;
    while (g_run) {
        fl = sizeof(from);
        int n = (int)recvfrom(s, buf, (int)sizeof(buf)-1, 0, (struct sockaddr*)&from, &fl);
        if (n <= 0) continue;
        buf[n] = 0;
        if (strncmp(buf, "P2PDISC|", 8) == 0) {
            char resp[256];
            snprintf(resp, sizeof(resp), "P2PHERE|%s|%d", g_myname, g_myport);
            sendto(s, resp, strlen(resp), 0, (struct sockaddr*)&from, sizeof(from));
        }
    }
    close(s);
    return NULL;
}

/* ═══════════════════════════════════════════════════════════
   DISCOVERY — seeker (broadcasts + connects to found peers)
   ═══════════════════════════════════════════════════════════ */
static void do_connect(const char *ip, int port) {
    /* skip self */
    if (strcmp(ip, g_myip) == 0 && port == g_myport) return;

    /* skip already connected */
    pthread_mutex_lock(&peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active &&
            strcmp(peers[i].ip, ip) == 0 &&
            peers[i].port == port) {
            pthread_mutex_unlock(&peers_lock);
            return;
        }
    }
    pthread_mutex_unlock(&peers_lock);

    int ts = socket(AF_INET, SOCK_STREAM, 0);
    if (ts < 0) return;

    /* non-blocking connect with 2s timeout */
    int flags = fcntl(ts, F_GETFL, 0);
    fcntl(ts, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in ta = {0};
    ta.sin_family = AF_INET;
    ta.sin_port   = htons((uint16_t)port);
    inet_pton(AF_INET, ip, &ta.sin_addr);

    connect(ts, (struct sockaddr*)&ta, sizeof(ta));

    struct timeval tv = {2, 0};
    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(ts, &wset);
    if (select(ts+1, NULL, &wset, NULL, &tv) <= 0) {
        close(ts); return;
    }
    int err = 0; socklen_t el = sizeof(err);
    getsockopt(ts, SOL_SOCKET, SO_ERROR, &err, &el);
    if (err) { close(ts); return; }

    fcntl(ts, F_SETFL, flags); /* restore blocking */
    int one = 1;
    setsockopt(ts, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    /* ── HANDSHAKE: send HELLO, receive HELLO ── */
    if (proto_send(ts, TYPE_HELLO, g_myname, (uint32_t)strlen(g_myname)) < 0) {
        close(ts); return;
    }
    uint8_t t; uint8_t *p; uint32_t l;
    if (proto_recv(ts, &t, &p, &l) < 0 || t != TYPE_HELLO) {
        free(p); close(ts); return;
    }
    char rname[NAME_LEN] = {0};
    strncpy(rname, (char*)p, NAME_LEN-1);
    free(p);

    int idx = peer_add(rname, ip, port, ts);
    if (idx < 0) return;

    /* spawn receiver for ongoing messages */
    ConnInfo *ci = (ConnInfo*)malloc(sizeof(ConnInfo));
    ci->sock = ts;
    ci->port = port;
    strncpy(ci->ip, ip, sizeof(ci->ip)-1);

    /*
     * We need a receiver BUT the handshake is already done.
     * Use a thin wrapper that skips the handshake part.
     * Simplest: just loop recv here in a detached thread.
     */
    /* Actually we want the full receive loop but skip HELLO.
       Use a flag struct instead — quickest fix: mini struct */

    typedef struct { int sock; } SockOnly;
    /* Inline a detached thread with just the message loop */
    /* We re-use ConnInfo — receiver_v2 will do its own HELLO which will fail
       since we already did it. So we need a separate "already-handshaked" receiver. */

    /* For clarity: duplicate the message-loop as a local lambda approach
       using a simple already_handshaked flag in ConnInfo */

    /* SIMPLEST correct approach: reconnect as "server side" but seeker sent HELLO first.
       The server side (thread_receiver_v2) expects to RECEIVE hello first.
       Since seeker connects and sends HELLO first, the server-side handler (thread_receiver_v2)
       correctly handles it. So for the seeker side, after the handshake is done, we need
       ONLY the message-receive loop — no more handshake. */

    /* We'll create a dedicated "connected-side receiver" thread.
       Pass sock + already have name registered in peers[]. */

    /* Thin receiver: just the message loop from thread_receiver_v2 after handshake */
    /* We do this by passing -1 as a signal to skip handshake */

    /* Cleaner: use an extra field in ConnInfo */
    free(ci);

    /* Spawn a mini receive loop thread */
    int *sp = (int*)malloc(sizeof(int));
    *sp = ts;

    /* define inline thread */
    extern void *outgoing_recv_loop(void *arg);
    pthread_t rt;
    pthread_create(&rt, NULL, outgoing_recv_loop, sp);
    pthread_detach(rt);
}

/* Message-receive loop for outgoing connections (handshake already done) */
void *outgoing_recv_loop(void *arg) {
    int s = *(int*)arg;
    free(arg);

    for (;;) {
        uint8_t type; uint8_t *pay; uint32_t plen;
        if (proto_recv(s, &type, &pay, &plen) < 0) break;

        if (type == TYPE_PING) {
            proto_send(s, TYPE_PONG, NULL, 0);
            free(pay);
        } else if (type == TYPE_PONG) {
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    peers[i].last_seen = time(NULL);
                    peers[i].awaiting_pong = 0;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            free(pay);
        } else if (type == TYPE_TEXT) {
            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    peers[i].bytes_recv += plen;
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BCYAN, "💬 %-14s  %.*s", sender, (int)plen, (char*)pay);
            free(pay);
        } else if (type == TYPE_FILE_HDR) {
            if (!pay || plen < 2) { free(pay); continue; }
            size_t fnlen = strnlen((char*)pay, plen);
            if (fnlen + 1 + 8 > plen) { free(pay); continue; }
            char fname[512];
            strncpy(fname, (char*)pay, sizeof(fname)-1);
            fname[sizeof(fname)-1] = 0;
            uint64_t fsize_net;
            memcpy(&fsize_net, pay + fnlen + 1, 8);
            uint32_t hi = ntohl((uint32_t)(fsize_net >> 32));
            uint32_t lo = ntohl((uint32_t)(fsize_net & 0xFFFFFFFFu));
            uint64_t fsize = ((uint64_t)lo << 32) | (uint64_t)hi;
            free(pay);

            char *base = strrchr(fname, '/');
            if (!base) base = strrchr(fname, '\\');
            const char *safe_name = base ? base+1 : fname;

            char sender[NAME_LEN] = "?";
            pthread_mutex_lock(&peers_lock);
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && peers[i].sock == s) {
                    strncpy(sender, peers[i].name, NAME_LEN-1);
                    break;
                }
            pthread_mutex_unlock(&peers_lock);
            log_add(C_BMAG, "↓  %s → %s  (%.2f MB)", sender, safe_name, (double)fsize/1048576.0);

            g_xfer_total  = fsize;
            g_xfer_done   = 0;
            g_xfer_active = 1;
            strncpy(g_xfer_name, safe_name, sizeof(g_xfer_name)-1);

            FILE *fp = fopen(safe_name, "wb");
            if (!fp) log_add(C_BRED, "  cannot create %s: %s", safe_name, strerror(errno));

            uint64_t received = 0;
            for (;;) {
                uint8_t t2; uint8_t *p2; uint32_t l2;
                if (proto_recv(s, &t2, &p2, &l2) < 0) { free(p2); break; }
                if (t2 == TYPE_FILE_DATA) {
                    if (fp) fwrite(p2, 1, l2, fp);
                    received += l2;
                    g_xfer_done = received;
                    free(p2);
                } else if (t2 == TYPE_FILE_END) { free(p2); break; }
                else { free(p2); }
            }
            g_xfer_active = 0;
            if (fp) fclose(fp);
            log_add(C_BGREEN, "  ✓ %s  %.2f MB", safe_name, (double)received/1048576.0);
        } else if (type == TYPE_QUIT) {
            free(pay); break;
        } else {
            free(pay);
        }
    }

    peer_remove_by_sock(s);
    return NULL;
}

static void *thread_disc_seeker(void *arg) {
    (void)arg;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return NULL;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));
    struct timeval tv = {1, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in bcast = {0};
    bcast.sin_family      = AF_INET;
    bcast.sin_port        = htons(DISC_PORT);
    bcast.sin_addr.s_addr = INADDR_BROADCAST;

    char buf[256];
    struct sockaddr_in from; socklen_t fl;

    while (g_run) {
        char disc[64];
        snprintf(disc, sizeof(disc), "P2PDISC|%s|%d", g_myname, g_myport);
        sendto(s, disc, strlen(disc), 0, (struct sockaddr*)&bcast, sizeof(bcast));

        time_t deadline = time(NULL) + 2;
        while (g_run && time(NULL) < deadline) {
            fl = sizeof(from);
            int n = (int)recvfrom(s, buf, (int)sizeof(buf)-1, 0, (struct sockaddr*)&from, &fl);
            if (n <= 0) continue;
            buf[n] = 0;

            char  rname[NAME_LEN];
            int   rport;
            if (sscanf(buf, "P2PHERE|%47[^|]|%d", rname, &rport) != 2) continue;

            char rip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &from.sin_addr, rip, sizeof(rip));
            (void)rname; /* name learned via handshake */
            do_connect(rip, rport);
        }
        sleep(4);
    }
    close(s);
    return NULL;
}

/* ═══════════════════════════════════════════════════════════
   SEND HELPERS
   ═══════════════════════════════════════════════════════════ */
static int send_text(int sock, const char *msg) {
    return proto_send(sock, TYPE_TEXT, msg, (uint32_t)strlen(msg));
}

static int send_file(int sock, const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return -1;
    uint64_t fsize = (uint64_t)st.st_size;

    /* build HDR payload */
    size_t fnlen = strlen(path);
    size_t hlen  = fnlen + 1 + 8;
    uint8_t *hdr = (uint8_t*)malloc(hlen);
    if (!hdr) return -1;
    memcpy(hdr, path, fnlen+1);

    /* htonll */
    uint64_t fsize_net;
    uint32_t hi = htonl((uint32_t)(fsize >> 32));
    uint32_t lo = htonl((uint32_t)(fsize & 0xFFFFFFFFu));
    memcpy(&fsize_net, &lo, 4);
    memcpy((char*)&fsize_net + 4, &hi, 4);
    memcpy(hdr + fnlen + 1, &fsize_net, 8);

    if (proto_send(sock, TYPE_FILE_HDR, hdr, (uint32_t)hlen) < 0) {
        free(hdr); return -1;
    }
    free(hdr);

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    g_xfer_total  = fsize;
    g_xfer_done   = 0;
    g_xfer_active = 1;
    strncpy(g_xfer_name, path, sizeof(g_xfer_name)-1);

    uint8_t *chunk = (uint8_t*)malloc(CHUNK_SIZE);
    size_t n;
    int err = 0;
    while ((n = fread(chunk, 1, CHUNK_SIZE, fp)) > 0) {
        if (proto_send(sock, TYPE_FILE_DATA, chunk, (uint32_t)n) < 0) {
            err = 1; break;
        }
        g_xfer_done += n;
    }
    free(chunk);
    fclose(fp);

    if (!err) proto_send(sock, TYPE_FILE_END, NULL, 0);
    g_xfer_active = 0;
    return err ? -1 : 0;
}

/* ═══════════════════════════════════════════════════════════
   TUI
   ═══════════════════════════════════════════════════════════ */
static int  term_w = 80;
static int  term_h = 24;

static void tui_size(void) {
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0) {
        term_w = ws.ws_col;
        term_h = ws.ws_row;
    }
}

static void tui_hline(char ch, int w) {
    for (int i = 0; i < w; i++) putchar(ch);
}

static void tui_goto(int row, int col) {
    printf("\033[%d;%dH", row, col);
}

/* format bytes nicely */
static void fmt_bytes(char *out, size_t outsz, uint64_t b) {
    if (b < 1024)             snprintf(out, outsz, "%llu B",  (unsigned long long)b);
    else if (b < 1024*1024)   snprintf(out, outsz, "%.1f KB", (double)b/1024.0);
    else if (b < 1024*1024*1024) snprintf(out, outsz, "%.1f MB", (double)b/1048576.0);
    else                      snprintf(out, outsz, "%.2f GB", (double)b/1073741824.0);
}

/* ═══════════════════════════════════════════════════════════
   DRAW
   ═══════════════════════════════════════════════════════════ */
static void draw_screen(int log_offset, int menu_visible) {
    tui_size();
    tui_goto(1,1);

    int W = term_w;

    /* ── HEADER ── */
    printf("%s%s", C_BLUE C_BOLD, C_REV);
    printf(" ⬡  P2P  MESH  CHAT  &  FILE  TRANSFER ");
    int hpad = W - 39;
    for (int i = 0; i < hpad; i++) putchar(' ');
    printf("%s\n", C_RESET);

    /* ── STATUS BAR ── */
    int pc = peer_count();
    printf("%s╔", C_CYAN);
    tui_hline('═', W-2);
    printf("╗%s\n", C_RESET);

    printf("%s║%s", C_CYAN, C_RESET);
    printf("  %s●%s %s  │  IP: %s%-16s%s│  Porta: %s%-6d%s│  Peer: %s%s%d%s%s  ",
           C_BGREEN, C_RESET, g_myname,
           C_BYEL, g_myip, C_RESET,
           C_BYEL, g_myport, C_RESET,
           pc > 0 ? C_BGREEN : C_BRED,
           pc > 0 ? "" : "",
           pc, C_RESET, "");
    /* pad to W-2 */
    printf("%s║%s\n", C_CYAN, C_RESET);

    printf("%s╠", C_CYAN);
    tui_hline('═', W-2);
    printf("╣%s\n", C_RESET);

    /* ── PEER LIST ── */
    int peer_cols = (W >= 100) ? 4 : (W >= 70 ? 3 : 2);
    int col_w = (W - 2) / peer_cols;

    printf("%s║%s%s  PEERS  %s", C_CYAN, C_RESET, C_DIM, C_RESET);
    for (int i = 0; i < W-2-9; i++) putchar(' ');
    printf("%s║%s\n", C_CYAN, C_RESET);

    pthread_mutex_lock(&peers_lock);
    int shown = 0, cidx = 0;
    printf("%s║%s  ", C_CYAN, C_RESET);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) continue;
        char uptime[24];
        time_t up = time(NULL) - peers[i].connected_at;
        snprintf(uptime, sizeof(uptime), "%02d:%02d", (int)(up/60), (int)(up%60));
        char bs[16], br[16];
        fmt_bytes(bs, sizeof(bs), peers[i].bytes_sent);
        fmt_bytes(br, sizeof(br), peers[i].bytes_recv);
        int cw = col_w - 3;
        printf("%s[%d]%s %-*.*s %s↑%s%-7s %s↓%s%-7s  ",
               C_BGREEN, shown, C_RESET,
               (cw > 16 ? 16 : cw), (cw > 16 ? 16 : cw),
               peers[i].name,
               C_BYEL, C_RESET, bs,
               C_BMAG, C_RESET, br);
        shown++;
        cidx++;
        if (cidx >= peer_cols) {
            /* pad to end of row */
            printf("%s║%s\n%s║%s  ", C_CYAN, C_RESET, C_CYAN, C_RESET);
            cidx = 0;
        }
    }
    if (shown == 0)
        printf("%s  nessun peer connesso — in ascolto...%s", C_DIM, C_RESET);
    pthread_mutex_unlock(&peers_lock);

    /* pad remaining columns */
    int remaining = (W - 2) - (cidx * col_w) - 2;
    for (int i = 0; i < remaining && i < W; i++) putchar(' ');
    printf("%s║%s\n", C_CYAN, C_RESET);

    printf("%s╠", C_CYAN);
    tui_hline('═', W-2);
    printf("╣%s\n", C_RESET);

    /* ── TRANSFER PROGRESS ── */
    if (g_xfer_active && g_xfer_total > 0) {
        double pct = (double)g_xfer_done / (double)g_xfer_total;
        int bar_w = W - 24;
        if (bar_w < 10) bar_w = 10;
        int filled = (int)(pct * bar_w);
        printf("%s║%s  %s%s%s  [", C_CYAN, C_RESET, C_BYEL, g_xfer_name, C_RESET);
        printf("%s", C_BGREEN);
        for (int i = 0; i < filled; i++) putchar('█');
        printf("%s", C_DIM);
        for (int i = filled; i < bar_w; i++) putchar('░');
        printf("%s] %s%3.0f%%%s  ", C_RESET, C_BYEL, pct*100, C_RESET);
        printf("%s║%s\n", C_CYAN, C_RESET);
        printf("%s╠", C_CYAN);
        tui_hline('═', W-2);
        printf("╣%s\n", C_RESET);
    }

    /* ── LOG ── */
    int log_h = term_h - (menu_visible ? 22 : 16);
    if (log_h < 3) log_h = 3;

    printf("%s║%s%s  LOG  %s", C_CYAN, C_RESET, C_DIM, C_RESET);
    for (int i = 0; i < W-2-7; i++) putchar(' ');
    printf("%s║%s\n", C_CYAN, C_RESET);

    pthread_mutex_lock(&g_log_lock);
    int start = g_log_count > log_h ? g_log_count - log_h + log_offset : 0;
    if (start < 0) start = 0;
    for (int li = 0; li < log_h; li++) {
        int idx = start + li;
        printf("%s║%s  ", C_CYAN, C_RESET);
        if (idx < g_log_count) {
            LogEntry *e = &g_log[(g_log_head - g_log_count + idx + LOG_LINES) % LOG_LINES];
            struct tm *tm = localtime(&e->ts);
            printf("%s%02d:%02d:%02d%s  %s%-*.*s%s",
                   C_DIM, tm->tm_hour, tm->tm_min, tm->tm_sec, C_RESET,
                   e->color,
                   W-14, W-14, e->text, C_RESET);
        } else {
            for (int i = 0; i < W-4; i++) putchar(' ');
        }
        printf("%s║%s\n", C_CYAN, C_RESET);
    }
    pthread_mutex_unlock(&g_log_lock);

    printf("%s╠", C_CYAN);
    tui_hline('═', W-2);
    printf("╣%s\n", C_RESET);

    /* ── MENU ── */
    printf("%s║%s", C_CYAN, C_RESET);
    printf("  %s[m]%s msg   %s[b]%s broadcast   %s[f]%s file   %s[B]%s file-broadcast   "
           "%s[p]%s peers   %s[c]%s connetti   %s[q]%s esci  ",
           C_BYEL, C_RESET,
           C_BYEL, C_RESET,
           C_BYEL, C_RESET,
           C_BYEL, C_RESET,
           C_BYEL, C_RESET,
           C_BYEL, C_RESET,
           C_BRED, C_RESET);
    printf("%s║%s\n", C_CYAN, C_RESET);

    printf("%s╚", C_CYAN);
    tui_hline('═', W-2);
    printf("╝%s\n", C_RESET);

    printf("%s> %s", C_BGREEN, C_RESET);
    fflush(stdout);
}

/* ═══════════════════════════════════════════════════════════
   INPUT HELPERS
   ═══════════════════════════════════════════════════════════ */
static struct termios g_orig_term;

static void term_raw(void) {
    tcgetattr(STDIN_FILENO, &g_orig_term);
    struct termios raw = g_orig_term;
    raw.c_lflag &= ~(ECHO | ICANON);
    tcsetattr(STDIN_FILENO, TCSANOW, &raw);
}

static void term_restore(void) {
    tcsetattr(STDIN_FILENO, TCSANOW, &g_orig_term);
}

/* read a line with echo (restores canonical mode temporarily) */
static int read_line(const char *prompt, char *out, int maxlen) {
    term_restore();
    printf("%s", prompt);
    fflush(stdout);
    if (!fgets(out, maxlen, stdin)) { term_raw(); return -1; }
    out[strcspn(out, "\n")] = 0;
    term_raw();
    return (int)strlen(out);
}

/* pick a peer interactively; returns index into peers[] or -1 */
static int pick_peer(void) {
    pthread_mutex_lock(&peers_lock);
    int idxs[MAX_PEERS], cnt = 0;
    for (int i = 0; i < MAX_PEERS; i++)
        if (peers[i].active) idxs[cnt++] = i;

    if (cnt == 0) {
        pthread_mutex_unlock(&peers_lock);
        log_add(C_BRED, "nessun peer connesso");
        return -1;
    }

    printf("\n");
    for (int i = 0; i < cnt; i++)
        printf("  %s[%d]%s  %s (%s)\n",
               C_BYEL, i, C_RESET,
               peers[idxs[i]].name,
               peers[idxs[i]].ip);
    pthread_mutex_unlock(&peers_lock);

    char buf[16];
    if (read_line("  Scegli peer: ", buf, sizeof(buf)) < 0) return -1;
    int c = atoi(buf);
    if (c < 0 || c >= cnt) return -1;
    return idxs[c];
}

/* ═══════════════════════════════════════════════════════════
   SIGNAL HANDLER
   ═══════════════════════════════════════════════════════════ */
static void on_signal(int sig) {
    (void)sig;
    g_run = 0;
}

/* ═══════════════════════════════════════════════════════════
   MAIN
   ═══════════════════════════════════════════════════════════ */
int main(int argc, char **argv) {
    signal(SIGINT,  on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    /* args */
    if (argc >= 2) strncpy(g_myname, argv[1], NAME_LEN-1);
    else           gethostname(g_myname, NAME_LEN-1);
    g_myname[NAME_LEN-1] = 0;

    if (argc >= 3) g_myport = atoi(argv[2]);

    /* detect local IP */
    {
        struct ifaddrs *ifa;
        if (getifaddrs(&ifa) == 0) {
            for (struct ifaddrs *i = ifa; i; i = i->ifa_next) {
                if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET) continue;
                struct sockaddr_in *sin = (struct sockaddr_in*)i->ifa_addr;
                const char *ip = inet_ntoa(sin->sin_addr);
                if (strcmp(ip, "127.0.0.1") == 0) continue;
                strncpy(g_myip, ip, sizeof(g_myip)-1);
                break;
            }
            freeifaddrs(ifa);
        }
        if (!g_myip[0]) strcpy(g_myip, "127.0.0.1");
    }

    /* init TUI */
    printf(CLEAR HIDE_CUR);
    tui_size();
    log_add(C_BGREEN, "avviato — %s  %s:%d", g_myname, g_myip, g_myport);

    /* start threads */
    pthread_t ts, tdr, tds, thb;
    pthread_create(&ts,  NULL, thread_tcp_server,    NULL);
    pthread_create(&tdr, NULL, thread_disc_responder, NULL);
    pthread_create(&tds, NULL, thread_disc_seeker,   NULL);
    pthread_create(&thb, NULL, thread_heartbeat,     NULL);
    pthread_detach(ts); pthread_detach(tdr);
    pthread_detach(tds); pthread_detach(thb);

    /* TUI refresh thread */
    /* We'll do it from main loop with non-blocking stdin */
    term_raw();

    int log_offset = 0;
    time_t last_draw = 0;

    char cmd_buf[1024];
    int  cmd_len = 0;

    while (g_run) {
        /* redraw every 250ms or on event */
        time_t now = time(NULL);
        if (now != last_draw) {
            printf(CLEAR);
            draw_screen(log_offset, 0);
            last_draw = now;
        }

        /* non-blocking input check */
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        struct timeval tv = {0, 250000}; /* 250ms */
        int sr = select(STDIN_FILENO+1, &rfds, NULL, NULL, &tv);
        if (sr <= 0) continue;

        char c;
        if (read(STDIN_FILENO, &c, 1) <= 0) break;

        /* simple single-key dispatch */
        if (c == 'q' || c == 'Q') {
            break;
        } else if (c == 'p' || c == 'P') {
            /* peers already shown on screen */
            log_add(C_DIM, "peer count: %d", peer_count());
        } else if (c == 'm') {
            /* message to single peer */
            term_restore();
            printf("\n");
            int idx = pick_peer();
            if (idx >= 0) {
                char msg[512];
                if (read_line("  Messaggio: ", msg, sizeof(msg)) > 0) {
                    pthread_mutex_lock(&peers_lock);
                    int sock = peers[idx].sock;
                    char pname[NAME_LEN];
                    strncpy(pname, peers[idx].name, NAME_LEN-1);
                    peers[idx].bytes_sent += strlen(msg);
                    pthread_mutex_unlock(&peers_lock);
                    if (send_text(sock, msg) == 0)
                        log_add(C_BYEL, "→  %s: %s", pname, msg);
                    else
                        log_add(C_BRED, "  errore invio msg a %s", pname);
                }
            }
            term_raw();
            printf(CLEAR);

        } else if (c == 'b') {
            /* broadcast text */
            term_restore();
            char msg[512];
            if (read_line("\n  Broadcast: ", msg, sizeof(msg)) > 0 && strlen(msg) > 0) {
                int sent = 0;
                pthread_mutex_lock(&peers_lock);
                for (int i = 0; i < MAX_PEERS; i++) {
                    if (!peers[i].active) continue;
                    peers[i].bytes_sent += strlen(msg);
                    if (send_text(peers[i].sock, msg) == 0) sent++;
                }
                pthread_mutex_unlock(&peers_lock);
                log_add(C_BYEL, "→  broadcast: \"%s\"  (%d peer)", msg, sent);
            }
            term_raw();
            printf(CLEAR);

        } else if (c == 'f') {
            /* file to single peer */
            term_restore();
            printf("\n");
            int idx = pick_peer();
            if (idx >= 0) {
                char path[512];
                if (read_line("  File path: ", path, sizeof(path)) > 0) {
                    pthread_mutex_lock(&peers_lock);
                    int sock = peers[idx].sock;
                    char pname[NAME_LEN];
                    strncpy(pname, peers[idx].name, NAME_LEN-1);
                    pthread_mutex_unlock(&peers_lock);

                    struct stat st;
                    if (stat(path, &st) < 0) {
                        log_add(C_BRED, "  file non trovato: %s", path);
                    } else {
                        char sz[24]; fmt_bytes(sz, sizeof(sz), (uint64_t)st.st_size);
                        log_add(C_BYEL, "→  %s  %s  →  %s", path, sz, pname);
                        if (send_file(sock, path) == 0)
                            log_add(C_BGREEN, "  ✓ %s inviato", path);
                        else
                            log_add(C_BRED, "  ✗ errore invio %s", path);
                    }
                }
            }
            term_raw();
            printf(CLEAR);

        } else if (c == 'B') {
            /* broadcast file */
            term_restore();
            char path[512];
            if (read_line("\n  File broadcast path: ", path, sizeof(path)) > 0) {
                struct stat st;
                if (stat(path, &st) < 0) {
                    log_add(C_BRED, "  file non trovato: %s", path);
                } else {
                    char sz[24]; fmt_bytes(sz, sizeof(sz), (uint64_t)st.st_size);
                    int sent = 0;
                    pthread_mutex_lock(&peers_lock);
                    int socks[MAX_PEERS], names_idx[MAX_PEERS], scnt = 0;
                    for (int i = 0; i < MAX_PEERS; i++)
                        if (peers[i].active) {
                            socks[scnt]     = peers[i].sock;
                            names_idx[scnt] = i;
                            scnt++;
                        }
                    pthread_mutex_unlock(&peers_lock);

                    for (int i = 0; i < scnt; i++) {
                        log_add(C_BYEL, "→  %s  %s", path, sz);
                        if (send_file(socks[i], path) == 0) sent++;
                        else log_add(C_BRED, "  ✗ peer %d fallito", i);
                    }
                    log_add(C_BGREEN, "  broadcast file: %d/%d peer", sent, scnt);
                }
            }
            term_raw();
            printf(CLEAR);

        } else if (c == 'c') {
            /* manual connect */
            term_restore();
            char ipport[64];
            if (read_line("\n  IP:porta (es. 192.168.1.5:9100): ", ipport, sizeof(ipport)) > 0) {
                char ip[INET_ADDRSTRLEN] = {0};
                int  port = g_myport;
                char *colon = strchr(ipport, ':');
                if (colon) {
                    *colon = 0;
                    port = atoi(colon+1);
                }
                strncpy(ip, ipport, sizeof(ip)-1);
                log_add(C_DIM, "connessione a %s:%d ...", ip, port);
                do_connect(ip, port);
            }
            term_raw();
            printf(CLEAR);

        } else if (c == 'j') {
            /* scroll log up */
            log_offset--;
        } else if (c == 'k') {
            /* scroll log down */
            log_offset++;
        }
    }

    /* ── SHUTDOWN ── */
    g_run = 0;
    term_restore();
    printf(SHOW_CUR "\n");

    pthread_mutex_lock(&peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) continue;
        proto_send(peers[i].sock, TYPE_QUIT, NULL, 0);
        close(peers[i].sock);
        peers[i].active = 0;
    }
    pthread_mutex_unlock(&peers_lock);

    printf("\n%s  Ciao!%s\n\n", C_BYEL, C_RESET);
    return 0;
}