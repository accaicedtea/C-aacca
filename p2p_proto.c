#define _GNU_SOURCE
#include "p2p_proto.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <net/if.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

/* ── Protocol constants ──────────────────────────────────────────────────── */
#define MAGIC           0x50325031u
#define TYPE_HELLO      0x01
#define TYPE_TEXT       0x02
#define TYPE_FILE_HDR   0x03
#define TYPE_FILE_DATA  0x04
#define TYPE_FILE_END   0x05
#define TYPE_PING       0x10
#define TYPE_PONG       0x11
#define TYPE_QUIT       0xFF

#define CHUNK_SIZE      65536
#define DISC_PORT       9101
#define HEARTBEAT_INT   5
#define PEER_TIMEOUT    120   /* generous: user may need time for file dialog */

/* ── Global state ────────────────────────────────────────────────────────── */
AppData g_data;

/* ── Internal log helper ─────────────────────────────────────────────────── */
static void p2p_log(const char *fmt, ...) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (g_data.on_log)
        g_data.on_log(buf, g_data.cb_userdata);
}

/* ── Network helpers ─────────────────────────────────────────────────────── */
static int send_all(int s, const void *buf, int len) {
    const char *p = (const char *)buf;
    int sent = 0;
    while (sent < len) {
        int n = (int)send(s, p + sent, (size_t)(len - sent), MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static int recv_all(int s, void *buf, int len) {
    char *p = (char *)buf;
    int got = 0;
    while (got < len) {
        int n = (int)recv(s, p + got, (size_t)(len - got), 0);
        if (n <= 0) return -1;
        got += n;
    }
    return 0;
}

/* Wire: [4 magic BE][1 type][4 len BE][len bytes payload] */
static int proto_send(int s, uint8_t type, const void *payload, uint32_t len) {
    uint8_t hdr[9];
    uint32_t m = htonl(MAGIC), nl = htonl(len);
    memcpy(hdr, &m, 4);
    hdr[4] = type;
    memcpy(hdr + 5, &nl, 4);
    if (send_all(s, hdr, 9) < 0) return -1;
    if (len > 0 && payload)
        if (send_all(s, payload, (int)len) < 0) return -1;
    return 0;
}

static int proto_recv(int s, uint8_t *type, uint8_t **payload, uint32_t *len) {
    uint8_t hdr[9];
    if (recv_all(s, hdr, 9) < 0) return -1;
    uint32_t m;
    memcpy(&m, hdr, 4);
    if (ntohl(m) != MAGIC) return -1;
    *type = hdr[4];
    memcpy(len, hdr + 5, 4);
    *len = ntohl(*len);
    if (*len > 0) {
        *payload = (uint8_t *)malloc(*len + 1);
        if (!*payload) return -1;
        (*payload)[*len] = 0; /* null-terminate for safe string use */
        if (recv_all(s, *payload, (int)*len) < 0) {
            free(*payload);
            *payload = NULL;
            return -1;
        }
    } else {
        *payload = NULL;
    }
    return 0;
}

/* ── HELLO handshake ─────────────────────────────────────────────────────
 * Payload: <name bytes><NUL><4-byte BE uint32 listening port>
 * Total: strlen(name)+1+4 bytes
 * ──────────────────────────────────────────────────────────────────────── */
static int hello_send(int s) {
    size_t   nlen = strlen(g_data.myname);
    uint32_t port_be = htonl((uint32_t)g_data.myport);
    uint8_t *buf = (uint8_t *)malloc(nlen + 1 + 4);
    if (!buf) return -1;
    memcpy(buf, g_data.myname, nlen + 1);
    memcpy(buf + nlen + 1, &port_be, 4);
    int r = proto_send(s, TYPE_HELLO, buf, (uint32_t)(nlen + 1 + 4));
    free(buf);
    return r;
}

static int hello_recv(int s, char *name_out, int *port_out) {
    uint8_t  type;
    uint8_t *pay;
    uint32_t len;
    if (proto_recv(s, &type, &pay, &len) < 0) return -1;
    if (type != TYPE_HELLO || !pay || len < 5) { free(pay); return -1; }
    size_t nlen = strnlen((char *)pay, len);
    if (nlen + 1 + 4 > len) { free(pay); return -1; }
    strncpy(name_out, (char *)pay, NAME_LEN - 1);
    name_out[NAME_LEN - 1] = 0;
    uint32_t port_be;
    memcpy(&port_be, pay + nlen + 1, 4);
    *port_out = (int)ntohl(port_be);
    free(pay);
    return 0;
}

/* ── Peer management ─────────────────────────────────────────────────────── */
static int peer_add(const char *name, const char *ip, int port, int sock) {
    pthread_mutex_lock(&g_data.peers_lock);

    /* Dedup by IP — one connection per host */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active && strcmp(g_data.peers[i].ip, ip) == 0) {
            close(g_data.peers[i].sock);
            g_data.peers[i].sock = sock;
            g_data.peers[i].port = port;
            strncpy(g_data.peers[i].name, name, NAME_LEN - 1);
            g_data.peers[i].last_seen    = time(NULL);
            g_data.peers[i].awaiting_pong = 0;
            pthread_mutex_unlock(&g_data.peers_lock);
            p2p_log("⟳ %s reconnected (%s)", name, ip);
            if (g_data.on_peer_event)
                g_data.on_peer_event(1, name, ip, g_data.cb_userdata);
            return i;
        }
    }

    /* New peer — find empty slot */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!g_data.peers[i].active) {
            memset(&g_data.peers[i], 0, sizeof(Peer));
            strncpy(g_data.peers[i].name, name, NAME_LEN - 1);
            strncpy(g_data.peers[i].ip,   ip,   INET_ADDRSTRLEN - 1);
            g_data.peers[i].port         = port;
            g_data.peers[i].sock         = sock;
            g_data.peers[i].active       = 1;
            g_data.peers[i].last_seen    = time(NULL);
            g_data.peers[i].connected_at = time(NULL);
            pthread_mutex_unlock(&g_data.peers_lock);
            p2p_log("✓ %s connected [%s:%d]", name, ip, port);
            if (g_data.on_peer_event)
                g_data.on_peer_event(1, name, ip, g_data.cb_userdata);
            return i;
        }
    }

    pthread_mutex_unlock(&g_data.peers_lock);
    close(sock);
    return -1; /* no room */
}

static void peer_remove_by_sock(int sock) {
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active && g_data.peers[i].sock == sock) {
            char name[NAME_LEN], ip[INET_ADDRSTRLEN];
            strncpy(name, g_data.peers[i].name, NAME_LEN - 1);
            strncpy(ip,   g_data.peers[i].ip,   INET_ADDRSTRLEN - 1);
            close(sock);
            g_data.peers[i].active = 0;
            pthread_mutex_unlock(&g_data.peers_lock);
            p2p_log("✗ %s disconnected", name);
            if (g_data.on_peer_event)
                g_data.on_peer_event(0, name, ip, g_data.cb_userdata);
            return;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
}

/* ── Heartbeat thread ─────────────────────────────────────────────────────── */
static void *thread_heartbeat(void *arg) {
    (void)arg;
    while (g_data.running) {
        sleep(HEARTBEAT_INT);
        time_t now = time(NULL);
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!g_data.peers[i].active) continue;

            if (g_data.peers[i].awaiting_pong &&
                (now - g_data.peers[i].last_seen) > PEER_TIMEOUT) {
                char name[NAME_LEN];
                strncpy(name, g_data.peers[i].name, NAME_LEN - 1);
                char ip[INET_ADDRSTRLEN];
                strncpy(ip, g_data.peers[i].ip, INET_ADDRSTRLEN - 1);
                int s = g_data.peers[i].sock;
                g_data.peers[i].active = 0;
                close(s);
                pthread_mutex_unlock(&g_data.peers_lock);
                p2p_log("⏱ %s timeout", name);
                if (g_data.on_peer_event)
                    g_data.on_peer_event(0, name, ip, g_data.cb_userdata);
                pthread_mutex_lock(&g_data.peers_lock);
                i = -1; /* restart scan */
                continue;
            }
            if (!g_data.peers[i].awaiting_pong) {
                if (proto_send(g_data.peers[i].sock, TYPE_PING, NULL, 0) < 0) {
                    char name[NAME_LEN];
                    strncpy(name, g_data.peers[i].name, NAME_LEN - 1);
                    char ip[INET_ADDRSTRLEN];
                    strncpy(ip, g_data.peers[i].ip, INET_ADDRSTRLEN - 1);
                    int s = g_data.peers[i].sock;
                    g_data.peers[i].active = 0;
                    close(s);
                    pthread_mutex_unlock(&g_data.peers_lock);
                    p2p_log("✗ %s: send error", name);
                    if (g_data.on_peer_event)
                        g_data.on_peer_event(0, name, ip, g_data.cb_userdata);
                    pthread_mutex_lock(&g_data.peers_lock);
                    i = -1;
                } else {
                    g_data.peers[i].awaiting_pong = 1;
                }
            }
        }
        pthread_mutex_unlock(&g_data.peers_lock);
    }
    return NULL;
}

/* ── Per-peer message loop ───────────────────────────────────────────────── */
static void *handle_peer_messages(void *arg) {
    int s = *(int *)arg;
    free(arg);

    uint8_t  type;
    uint8_t *pay;
    uint32_t plen;

    /* State for streaming file receive */
    int      in_file    = 0;
    FILE    *fp         = NULL;
    uint64_t fsize      = 0;
    uint64_t received   = 0;
    char     safe_name[512] = {0};

    while (g_data.running) {
        if (proto_recv(s, &type, &pay, &plen) < 0) break;

        /* ── Inside a file stream ── */
        if (in_file) {
            if (type == TYPE_FILE_DATA) {
                if (fp && plen > 0) fwrite(pay, 1, plen, fp);
                received              += plen;
                g_data.xfer_done       = received;
                free(pay);
            } else if (type == TYPE_FILE_END) {
                free(pay);
                in_file           = 0;
                g_data.xfer_active = 0;
                if (fp) { fclose(fp); fp = NULL; }
                p2p_log("✓ received  %s  (%.2f MB)",
                        safe_name, (double)received / 1048576.0);
            } else if (type == TYPE_PING) {
                proto_send(s, TYPE_PONG, NULL, 0);
                free(pay);
            } else {
                free(pay);
            }
            continue;
        }

        /* ── Normal messages ── */
        switch (type) {

        case TYPE_PING:
            proto_send(s, TYPE_PONG, NULL, 0);
            free(pay);
            break;

        case TYPE_PONG:
            pthread_mutex_lock(&g_data.peers_lock);
            for (int i = 0; i < MAX_PEERS; i++) {
                if (g_data.peers[i].active && g_data.peers[i].sock == s) {
                    g_data.peers[i].last_seen    = time(NULL);
                    g_data.peers[i].awaiting_pong = 0;
                    break;
                }
            }
            pthread_mutex_unlock(&g_data.peers_lock);
            free(pay);
            break;

        case TYPE_TEXT: {
            char peer_name[NAME_LEN] = "?";
            pthread_mutex_lock(&g_data.peers_lock);
            for (int i = 0; i < MAX_PEERS; i++) {
                if (g_data.peers[i].active && g_data.peers[i].sock == s) {
                    strncpy(peer_name, g_data.peers[i].name, NAME_LEN - 1);
                    g_data.peers[i].bytes_recv += plen;
                    break;
                }
            }
            pthread_mutex_unlock(&g_data.peers_lock);
            /* ── KEY FIX: actually call the message callback ── */
            if (g_data.on_message)
                g_data.on_message(peer_name, (char *)pay, g_data.cb_userdata);
            free(pay);
            break;
        }

        case TYPE_FILE_HDR: {
            if (!pay || plen < 6) { free(pay); break; }
            size_t fnlen = strnlen((char *)pay, plen);
            if (fnlen + 1 + 8 > plen) { free(pay); break; }

            char fname[512];
            strncpy(fname, (char *)pay, sizeof(fname) - 1);
            fname[sizeof(fname) - 1] = 0;

            /* Parse 8-byte BE file size: hi(4) | lo(4) */
            uint32_t hi_be, lo_be;
            memcpy(&hi_be, pay + fnlen + 1, 4);
            memcpy(&lo_be, pay + fnlen + 5, 4);
            fsize = ((uint64_t)ntohl(hi_be) << 32) | (uint64_t)ntohl(lo_be);
            free(pay);

            /* Strip directory component for safety */
            char *base = strrchr(fname, '/');
            if (!base) base = strrchr(fname, '\\');
            snprintf(safe_name, sizeof(safe_name), "%s", base ? base + 1 : fname);

            /* Find sender name */
            char peer_name[NAME_LEN] = "?";
            pthread_mutex_lock(&g_data.peers_lock);
            for (int i = 0; i < MAX_PEERS; i++) {
                if (g_data.peers[i].active && g_data.peers[i].sock == s) {
                    strncpy(peer_name, g_data.peers[i].name, NAME_LEN - 1);
                    break;
                }
            }
            pthread_mutex_unlock(&g_data.peers_lock);

            /* Ask GUI/user where to save — THIS CALL BLOCKS */
            char *save_path = NULL;
            if (g_data.on_file_incoming) {
                save_path = g_data.on_file_incoming(peer_name, safe_name,
                                                     fsize, g_data.cb_userdata);
            } else {
                save_path = strdup(safe_name);
            }

            if (!save_path) {
                p2p_log("✗ file rejected: %s from %s", safe_name, peer_name);
                fp = NULL; /* drain data but don't write */
            } else {
                fp = fopen(save_path, "wb");
                if (!fp)
                    p2p_log("✗ cannot create %s: %s", save_path, strerror(errno));
                free(save_path);
            }

            g_data.xfer_total     = fsize;
            g_data.xfer_done      = 0;
            g_data.xfer_active    = 1;
            g_data.xfer_direction = 'R';
            snprintf(g_data.xfer_name, sizeof(g_data.xfer_name), "%.255s", safe_name);
            received = 0;
            in_file  = 1;

            p2p_log("↓ receiving  %s  from %s  (%.2f MB)",
                    safe_name, peer_name, (double)fsize / 1048576.0);
            break;
        }

        case TYPE_QUIT:
            free(pay);
            goto done;

        default:
            free(pay);
            break;
        }
        continue;
done:
        break;
    }

    if (fp) fclose(fp);
    peer_remove_by_sock(s);
    return NULL;
}

/* ── TCP server thread ───────────────────────────────────────────────────── */
static void *thread_tcp_server(void *arg) {
    (void)arg;
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) { p2p_log("tcp_server socket: %s", strerror(errno)); return NULL; }

    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(ls, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in a = {0};
    a.sin_family      = AF_INET;
    a.sin_port        = htons((uint16_t)g_data.myport);
    a.sin_addr.s_addr = INADDR_ANY;
    if (bind(ls, (struct sockaddr *)&a, sizeof(a)) < 0) {
        p2p_log("tcp_server bind port %d: %s", g_data.myport, strerror(errno));
        close(ls); return NULL;
    }
    listen(ls, 16);

    while (g_data.running) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int c = accept(ls, (struct sockaddr *)&ca, &cl);
        if (c < 0) { if (g_data.running) usleep(50000); continue; }

        int flag = 1;
        setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        /* Handshake: receive peer's HELLO, send ours */
        char rname[NAME_LEN]; int rport;
        if (hello_recv(c, rname, &rport) < 0 || hello_send(c) < 0) {
            close(c); continue;
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof(ip));

        if (peer_add(rname, ip, rport, c) < 0) { close(c); continue; }

        int *ps = (int *)malloc(sizeof(int));
        *ps = c;
        pthread_t thr;
        pthread_create(&thr, NULL, handle_peer_messages, ps);
        pthread_detach(thr);
    }
    close(ls);
    return NULL;
}

/* ── Discovery responder (answers UDP probe) ────────────────────────────── */
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
    if (bind(s, (struct sockaddr *)&a, sizeof(a)) < 0) { close(s); return NULL; }

    char buf[256]; struct sockaddr_in from; socklen_t fl;
    while (g_data.running) {
        fl = sizeof(from);
        int n = (int)recvfrom(s, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&from, &fl);
        if (n <= 0) continue;
        buf[n] = 0;
        if (strncmp(buf, "P2PDISC|", 8) == 0) {
            char resp[256];
            snprintf(resp, sizeof(resp), "P2PHERE|%s|%d", g_data.myname, g_data.myport);
            sendto(s, resp, strlen(resp), 0, (struct sockaddr *)&from, sizeof(from));
        }
    }
    close(s);
    return NULL;
}

/* ── Broadcast on every active non-loopback interface ───────────────────────
 * This is the KEY fix for USB network support.  A USB tethering connection
 * (usb0, rndis0, ecm0…) appears as a regular network interface.  Sending
 * the discovery probe on its directed broadcast address ensures we find peers
 * on USB cables just as reliably as on Wi-Fi or Ethernet.
 * ──────────────────────────────────────────────────────────────────────── */
static void broadcast_all_ifaces(int udp_sock, const char *msg) {
    struct ifaddrs *ifa_list;
    size_t msg_len = strlen(msg);

    if (getifaddrs(&ifa_list) != 0) {
        /* fallback: global limited broadcast */
        struct sockaddr_in b = {0};
        b.sin_family      = AF_INET;
        b.sin_port        = htons(DISC_PORT);
        b.sin_addr.s_addr = INADDR_BROADCAST;
        sendto(udp_sock, msg, msg_len, 0, (struct sockaddr *)&b, sizeof(b));
        return;
    }

    for (struct ifaddrs *ifa = ifa_list; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        if (ifa->ifa_flags & IFF_LOOPBACK) continue;
        if (!(ifa->ifa_flags & IFF_UP)) continue;
        if (!(ifa->ifa_flags & IFF_BROADCAST)) continue;
        if (!ifa->ifa_broadaddr) continue;

        struct sockaddr_in bcast;
        memcpy(&bcast, ifa->ifa_broadaddr, sizeof(bcast));
        bcast.sin_port = htons(DISC_PORT);
        sendto(udp_sock, msg, msg_len, 0, (struct sockaddr *)&bcast, sizeof(bcast));
    }
    freeifaddrs(ifa_list);
}

/* ── Discovery seeker (broadcasts probes, connects to replies) ─────────── */
static void *thread_disc_seeker(void *arg) {
    (void)arg;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return NULL;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));
    struct timeval tv = {1, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in local = {0};
    local.sin_family      = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    bind(s, (struct sockaddr *)&local, sizeof(local)); /* bind to any port */

    char buf[256]; struct sockaddr_in from; socklen_t fl;

    while (g_data.running) {
        char disc[64];
        snprintf(disc, sizeof(disc), "P2PDISC|%s|%d", g_data.myname, g_data.myport);
        broadcast_all_ifaces(s, disc);

        time_t deadline = time(NULL) + 2;
        while (g_data.running && time(NULL) < deadline) {
            fl = sizeof(from);
            int n = (int)recvfrom(s, buf, sizeof(buf) - 1, 0,
                                  (struct sockaddr *)&from, &fl);
            if (n <= 0) continue;
            buf[n] = 0;

            char rname[NAME_LEN]; int rport;
            if (sscanf(buf, "P2PHERE|%47[^|]|%d", rname, &rport) != 2) continue;

            char rip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &from.sin_addr, rip, sizeof(rip));
            p2p_connect(rip, rport); /* no-op if already connected */
        }
        sleep(4);
    }
    close(s);
    return NULL;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

int p2p_init(const char *name, int port, const char *ip) {
    memset(&g_data, 0, sizeof(g_data));
    pthread_mutex_init(&g_data.peers_lock, NULL);

    strncpy(g_data.myname, name ? name : "User", NAME_LEN - 1);
    g_data.myport = (port > 0) ? port : 9100;

    if (ip && *ip) {
        strncpy(g_data.myip, ip, INET_ADDRSTRLEN - 1);
    } else {
        /* auto-detect: first non-loopback IPv4 */
        struct ifaddrs *ifa;
        if (getifaddrs(&ifa) == 0) {
            for (struct ifaddrs *i = ifa; i; i = i->ifa_next) {
                if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET) continue;
                struct sockaddr_in *sin = (struct sockaddr_in *)i->ifa_addr;
                const char *a = inet_ntoa(sin->sin_addr);
                if (strcmp(a, "127.0.0.1") == 0) continue;
                strncpy(g_data.myip, a, INET_ADDRSTRLEN - 1);
                break;
            }
            freeifaddrs(ifa);
        }
        if (!g_data.myip[0]) strcpy(g_data.myip, "127.0.0.1");
    }
    return 0;
}

void p2p_start(void) {
    g_data.running = 1;
    signal(SIGPIPE, SIG_IGN);

    pthread_t ts, tdr, tds, thb;
    pthread_create(&ts,  NULL, thread_tcp_server,    NULL);
    pthread_create(&tdr, NULL, thread_disc_responder, NULL);
    pthread_create(&tds, NULL, thread_disc_seeker,   NULL);
    pthread_create(&thb, NULL, thread_heartbeat,     NULL);
    pthread_detach(ts); pthread_detach(tdr);
    pthread_detach(tds); pthread_detach(thb);
}

void p2p_stop(void) {
    g_data.running = 0;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active) {
            proto_send(g_data.peers[i].sock, TYPE_QUIT, NULL, 0);
            close(g_data.peers[i].sock);
            g_data.peers[i].active = 0;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
}

int p2p_send_text(int sock, const char *msg) {
    int r = proto_send(sock, TYPE_TEXT, msg, (uint32_t)strlen(msg));
    if (r == 0) {
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < MAX_PEERS; i++)
            if (g_data.peers[i].active && g_data.peers[i].sock == sock)
                g_data.peers[i].bytes_sent += strlen(msg);
        pthread_mutex_unlock(&g_data.peers_lock);
    }
    return r;
}

int p2p_send_file(int sock, const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return -1;
    uint64_t fsize = (uint64_t)st.st_size;

    /* Build HDR payload: path\0  hi(4BE)  lo(4BE) */
    size_t  fnlen = strlen(path);
    size_t  hlen  = fnlen + 1 + 8;
    uint8_t *hdr  = (uint8_t *)malloc(hlen);
    if (!hdr) return -1;
    memcpy(hdr, path, fnlen + 1);
    uint32_t hi = htonl((uint32_t)(fsize >> 32));
    uint32_t lo = htonl((uint32_t)(fsize & 0xFFFFFFFFu));
    memcpy(hdr + fnlen + 1, &hi, 4);
    memcpy(hdr + fnlen + 5, &lo, 4);

    if (proto_send(sock, TYPE_FILE_HDR, hdr, (uint32_t)hlen) < 0) {
        free(hdr); return -1;
    }
    free(hdr);

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    g_data.xfer_total     = fsize;
    g_data.xfer_done      = 0;
    g_data.xfer_active    = 1;
    g_data.xfer_direction = 'S';
    snprintf(g_data.xfer_name, sizeof(g_data.xfer_name), "%.255s", path);

    uint8_t *chunk = (uint8_t *)malloc(CHUNK_SIZE);
    size_t   n;
    int      err = 0;

    while ((n = fread(chunk, 1, CHUNK_SIZE, fp)) > 0) {
        if (proto_send(sock, TYPE_FILE_DATA, chunk, (uint32_t)n) < 0) {
            err = 1; break;
        }
        g_data.xfer_done += n;
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < MAX_PEERS; i++)
            if (g_data.peers[i].active && g_data.peers[i].sock == sock)
                g_data.peers[i].bytes_sent += n;
        pthread_mutex_unlock(&g_data.peers_lock);
    }
    free(chunk);
    fclose(fp);

    if (!err) proto_send(sock, TYPE_FILE_END, NULL, 0);
    g_data.xfer_active = 0;
    return err ? -1 : 0;
}

void p2p_connect(const char *ip, int port) {
    /* Skip self (compare against ALL local interfaces) */
    if (port == g_data.myport) {
        struct ifaddrs *ifa;
        if (getifaddrs(&ifa) == 0) {
            for (struct ifaddrs *i = ifa; i; i = i->ifa_next) {
                if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET) continue;
                struct sockaddr_in *sin = (struct sockaddr_in *)i->ifa_addr;
                if (strcmp(inet_ntoa(sin->sin_addr), ip) == 0) {
                    freeifaddrs(ifa);
                    return; /* that's us */
                }
            }
            freeifaddrs(ifa);
        }
    }

    /* Already connected? */
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active && strcmp(g_data.peers[i].ip, ip) == 0) {
            pthread_mutex_unlock(&g_data.peers_lock);
            return;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);

    /* Non-blocking connect with 2-second timeout */
    int ts = socket(AF_INET, SOCK_STREAM, 0);
    if (ts < 0) return;

    int fl = fcntl(ts, F_GETFL, 0);
    fcntl(ts, F_SETFL, fl | O_NONBLOCK);

    struct sockaddr_in ta = {0};
    ta.sin_family = AF_INET;
    ta.sin_port   = htons((uint16_t)port);
    inet_pton(AF_INET, ip, &ta.sin_addr);
    connect(ts, (struct sockaddr *)&ta, sizeof(ta));

    struct timeval tv = {2, 0};
    fd_set wset; FD_ZERO(&wset); FD_SET(ts, &wset);
    if (select(ts + 1, NULL, &wset, NULL, &tv) <= 0) { close(ts); return; }

    int err = 0; socklen_t el = sizeof(err);
    getsockopt(ts, SOL_SOCKET, SO_ERROR, &err, &el);
    if (err) { close(ts); return; }

    fcntl(ts, F_SETFL, fl); /* restore blocking */
    int one = 1;
    setsockopt(ts, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    /* Handshake: send our HELLO, receive peer's HELLO */
    if (hello_send(ts) < 0) { close(ts); return; }
    char rname[NAME_LEN]; int rport;
    if (hello_recv(ts, rname, &rport) < 0) { close(ts); return; }

    if (peer_add(rname, ip, rport, ts) < 0) { close(ts); return; }

    int *ps = (int *)malloc(sizeof(int));
    *ps = ts;
    pthread_t rt;
    pthread_create(&rt, NULL, handle_peer_messages, ps);
    pthread_detach(rt);
}