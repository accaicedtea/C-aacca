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
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

#define MAGIC          0x50325031u
#define TYPE_HELLO     0x01
#define TYPE_TEXT      0x02
#define TYPE_FILE_HDR  0x03
#define TYPE_FILE_DATA 0x04
#define TYPE_FILE_END  0x05
#define TYPE_PING      0x10
#define TYPE_PONG      0x11
#define TYPE_QUIT      0xFF

#define CHUNK_SIZE     65536
#define DISC_PORT      9101
#define HEARTBEAT_INT  5
#define PEER_TIMEOUT   20

AppData g_data = {0};

static int send_all(int s, const void *buf, int len) {
    const char *p = buf;
    int sent = 0;
    while (sent < len) {
        int n = send(s, p + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static int recv_all(int s, void *buf, int len) {
    char *p = buf;
    int got = 0;
    while (got < len) {
        int n = recv(s, p + got, len - got, 0);
        if (n <= 0) return -1;
        got += n;
    }
    return 0;
}

static int proto_send(int s, uint8_t type, const void *payload, uint32_t len) {
    uint8_t hdr[9];
    uint32_t m = htonl(MAGIC), nl = htonl(len);
    memcpy(hdr, &m, 4);
    hdr[4] = type;
    memcpy(hdr + 5, &nl, 4);
    if (send_all(s, hdr, 9) < 0) return -1;
    if (len > 0 && payload)
        if (send_all(s, payload, len) < 0) return -1;
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
        *payload = malloc(*len + 1);
        if (!*payload) return -1;
        (*payload)[*len] = 0;
        if (recv_all(s, *payload, *len) < 0) {
            free(*payload);
            return -1;
        }
    } else {
        *payload = NULL;
    }
    return 0;
}

static int peer_add(const char *name, const char *ip, int port, int sock) {
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active && !strcmp(g_data.peers[i].ip, ip) && g_data.peers[i].port == port) {
            close(g_data.peers[i].sock);
            g_data.peers[i].sock = sock;
            strncpy(g_data.peers[i].name, name, NAME_LEN - 1);
            g_data.peers[i].last_seen = time(NULL);
            g_data.peers[i].awaiting_pong = 0;
            pthread_mutex_unlock(&g_data.peers_lock);
            return i;
        }
    }
    for (int i = 0; i < 32; i++) {
        if (!g_data.peers[i].active) {
            memset(&g_data.peers[i], 0, sizeof(Peer));
            strncpy(g_data.peers[i].name, name, NAME_LEN - 1);
            strncpy(g_data.peers[i].ip, ip, INET_ADDRSTRLEN - 1);
            g_data.peers[i].port = port;
            g_data.peers[i].sock = sock;
            g_data.peers[i].active = 1;
            g_data.peers[i].last_seen = time(NULL);
            g_data.peers[i].connected_at = time(NULL);
            pthread_mutex_unlock(&g_data.peers_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
    close(sock);
    return -1;
}

static void peer_remove_by_sock(int sock) {
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active && g_data.peers[i].sock == sock) {
            close(sock);
            g_data.peers[i].active = 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
}

static void *thread_heartbeat(void *arg) {
    (void)arg;
    while (g_data.running) {
        sleep(HEARTBEAT_INT);
        time_t now = time(NULL);
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < 32; i++) {
            if (!g_data.peers[i].active) continue;
            if (g_data.peers[i].awaiting_pong && (now - g_data.peers[i].last_seen) > PEER_TIMEOUT) {
                close(g_data.peers[i].sock);
                g_data.peers[i].active = 0;
            } else if (!g_data.peers[i].awaiting_pong) {
                if (proto_send(g_data.peers[i].sock, TYPE_PING, NULL, 0) < 0) {
                    close(g_data.peers[i].sock);
                    g_data.peers[i].active = 0;
                } else {
                    g_data.peers[i].awaiting_pong = 1;
                }
            }
        }
        pthread_mutex_unlock(&g_data.peers_lock);
    }
    return NULL;
}

static void *handle_peer_messages(void *arg) {
    int s = *(int *)arg;
    free(arg);
    uint8_t type;
    uint8_t *pay;
    uint32_t plen;
    int file_receiving = 0;
    FILE *fp = NULL;
    uint64_t fsize = 0, received = 0;
    char safe_name[512] = {0};

    while (g_data.running) {
        if (proto_recv(s, &type, &pay, &plen) < 0) break;

        if (file_receiving) {
            if (type == TYPE_FILE_DATA) {
                if (fp && plen > 0) fwrite(pay, 1, plen, fp);
                received += plen;
                g_data.xfer_done = received;
                free(pay);
            } else if (type == TYPE_FILE_END) {
                free(pay);
                file_receiving = 0;
                g_data.xfer_active = 0;
                if (fp) {
                    fclose(fp);
                    fp = NULL;
                }
                g_data.xfer_total = 0;
            } else if (type == TYPE_PING) {
                proto_send(s, TYPE_PONG, NULL, 0);
                free(pay);
            } else if (type == TYPE_PONG) {
                free(pay);
            } else {
                free(pay);
            }
            continue;
        }

        switch (type) {
        case TYPE_PING:
            proto_send(s, TYPE_PONG, NULL, 0);
            free(pay);
            break;
        case TYPE_PONG:
            free(pay);
            break;
        case TYPE_TEXT:
            free(pay);
            break;
        case TYPE_FILE_HDR: {
            if (!pay || plen < 2) { free(pay); continue; }
            size_t fnlen = strnlen((char *)pay, plen);
            if (fnlen + 1 + 8 > plen) { free(pay); continue; }

            char fname[512];
            strncpy(fname, (char *)pay, sizeof(fname) - 1);
            fname[sizeof(fname) - 1] = 0;

            uint32_t hi_net, lo_net;
            memcpy(&hi_net, pay + fnlen + 1, 4);
            memcpy(&lo_net, pay + fnlen + 5, 4);
            fsize = ((uint64_t)ntohl(hi_net) << 32) | ntohl(lo_net);
            free(pay);

            char *base = strrchr(fname, '/');
            if (!base) base = strrchr(fname, '\\');
            snprintf(safe_name, sizeof(safe_name), "%s", base ? base + 1 : fname);

            g_data.xfer_total = fsize;
            g_data.xfer_done = 0;
            g_data.xfer_active = 1;
            // troncamento sicuro
            snprintf(g_data.xfer_name, sizeof(g_data.xfer_name), "%.255s", safe_name);

            fp = fopen(safe_name, "wb");
            if (!fp) {
                g_data.xfer_active = 0;
                g_data.xfer_total = 0;
            }
            received = 0;
            file_receiving = 1;
            break;
        }
        case TYPE_QUIT:
            free(pay);
            goto disconnect;
        default:
            free(pay);
            break;
        }
    }
disconnect:
    if (fp) fclose(fp);
    peer_remove_by_sock(s);
    return NULL;
}

static void *thread_tcp_server(void *arg) {
    (void)arg;
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) return NULL;
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(ls, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_data.myport);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(ls);
        return NULL;
    }
    listen(ls, 16);

    while (g_data.running) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof(ca);
        int c = accept(ls, (struct sockaddr *)&ca, &cl);
        if (c < 0) {
            if (g_data.running) usleep(50000);
            continue;
        }
        int flag = 1;
        setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        uint8_t mtype; uint8_t *mpayload; uint32_t mlen;
        if (proto_recv(c, &mtype, &mpayload, &mlen) < 0 || mtype != TYPE_HELLO) {
            free(mpayload);
            close(c);
            continue;
        }
        char rname[NAME_LEN] = {0};
        strncpy(rname, (char *)mpayload, NAME_LEN - 1);
        free(mpayload);
        if (proto_send(c, TYPE_HELLO, g_data.myname, strlen(g_data.myname)) < 0) {
            close(c);
            continue;
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof(ip));
        if (peer_add(rname, ip, g_data.myport, c) < 0) {
            close(c);
            continue;
        }

        int *psock = malloc(sizeof(int));
        *psock = c;
        pthread_t thr;
        pthread_create(&thr, NULL, handle_peer_messages, psock);
        pthread_detach(thr);
    }
    close(ls);
    return NULL;
}

static void *thread_disc_responder(void *arg) {
    (void)arg;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return NULL;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons(DISC_PORT);
    a.sin_addr.s_addr = INADDR_ANY;
    bind(s, (struct sockaddr *)&a, sizeof(a));
    char buf[256];
    struct sockaddr_in from;
    socklen_t fl;
    while (g_data.running) {
        fl = sizeof(from);
        int n = recvfrom(s, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&from, &fl);
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

static void *thread_disc_seeker(void *arg) {
    (void)arg;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return NULL;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));
    struct timeval tv = {1, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_in bcast = {0};
    bcast.sin_family = AF_INET;
    bcast.sin_port = htons(DISC_PORT);
    bcast.sin_addr.s_addr = INADDR_BROADCAST;

    char buf[256];
    struct sockaddr_in from;
    socklen_t fl;
    while (g_data.running) {
        char disc[64];
        snprintf(disc, sizeof(disc), "P2PDISC|%s|%d", g_data.myname, g_data.myport);
        sendto(s, disc, strlen(disc), 0, (struct sockaddr *)&bcast, sizeof(bcast));

        time_t deadline = time(NULL) + 2;
        while (g_data.running && time(NULL) < deadline) {
            fl = sizeof(from);
            int n = recvfrom(s, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&from, &fl);
            if (n <= 0) continue;
            buf[n] = 0;
            char rname[NAME_LEN];
            int rport;
            if (sscanf(buf, "P2PHERE|%47[^|]|%d", rname, &rport) != 2) continue;
            char rip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &from.sin_addr, rip, sizeof(rip));
            p2p_connect(rip, rport);
        }
        sleep(4);
    }
    close(s);
    return NULL;
}

/* ---------- API pubbliche ---------- */
int p2p_init(const char *name, int port, const char *ip) {
    strncpy(g_data.myname, name ? name : "User", NAME_LEN - 1);
    g_data.myname[NAME_LEN - 1] = '\0';
    g_data.myport = port ? port : 9100;
    if (ip) {
        strncpy(g_data.myip, ip, INET_ADDRSTRLEN - 1);
    } else {
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
    pthread_t ts, tdr, tds, thb;
    pthread_create(&ts, NULL, thread_tcp_server, NULL);
    pthread_create(&tdr, NULL, thread_disc_responder, NULL);
    pthread_create(&tds, NULL, thread_disc_seeker, NULL);
    pthread_create(&thb, NULL, thread_heartbeat, NULL);
    pthread_detach(ts);
    pthread_detach(tdr);
    pthread_detach(tds);
    pthread_detach(thb);
}

void p2p_stop(void) {
    g_data.running = 0;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active) {
            proto_send(g_data.peers[i].sock, TYPE_QUIT, NULL, 0);
            close(g_data.peers[i].sock);
            g_data.peers[i].active = 0;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
}

int p2p_send_text(int sock, const char *msg) {
    return proto_send(sock, TYPE_TEXT, msg, strlen(msg));
}

int p2p_send_file(int sock, const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return -1;
    uint64_t fsize = st.st_size;

    size_t fnlen = strlen(path);
    size_t hlen = fnlen + 1 + 8;
    uint8_t *hdr = malloc(hlen);
    if (!hdr) return -1;
    memcpy(hdr, path, fnlen + 1);

    uint32_t hi = htonl((uint32_t)(fsize >> 32));
    uint32_t lo = htonl((uint32_t)(fsize & 0xFFFFFFFF));
    memcpy(hdr + fnlen + 1, &hi, 4);
    memcpy(hdr + fnlen + 5, &lo, 4);

    if (proto_send(sock, TYPE_FILE_HDR, hdr, hlen) < 0) {
        free(hdr);
        return -1;
    }
    free(hdr);

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    g_data.xfer_total = fsize;
    g_data.xfer_done = 0;
    g_data.xfer_active = 1;
    snprintf(g_data.xfer_name, sizeof(g_data.xfer_name), "%.255s", path);

    uint8_t *chunk = malloc(CHUNK_SIZE);
    size_t n;
    int err = 0;
    while ((n = fread(chunk, 1, CHUNK_SIZE, fp)) > 0) {
        if (proto_send(sock, TYPE_FILE_DATA, chunk, n) < 0) {
            err = 1;
            break;
        }
        g_data.xfer_done += n;
    }
    free(chunk);
    fclose(fp);
    if (!err) proto_send(sock, TYPE_FILE_END, NULL, 0);
    g_data.xfer_active = 0;
    return err ? -1 : 0;
}

void p2p_connect(const char *ip, int port) {
    if (!strcmp(ip, g_data.myip) && port == g_data.myport) return;

    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active && !strcmp(g_data.peers[i].ip, ip) && g_data.peers[i].port == port) {
            pthread_mutex_unlock(&g_data.peers_lock);
            return;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);

    int ts = socket(AF_INET, SOCK_STREAM, 0);
    if (ts < 0) return;

    int flags = fcntl(ts, F_GETFL, 0);
    fcntl(ts, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in ta = {0};
    ta.sin_family = AF_INET;
    ta.sin_port = htons(port);
    inet_pton(AF_INET, ip, &ta.sin_addr);
    connect(ts, (struct sockaddr *)&ta, sizeof(ta));

    struct timeval tv = {2, 0};
    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(ts, &wset);
    if (select(ts + 1, NULL, &wset, NULL, &tv) <= 0) {
        close(ts);
        return;
    }
    int err = 0;
    socklen_t el = sizeof(err);
    getsockopt(ts, SOL_SOCKET, SO_ERROR, &err, &el);
    if (err) {
        close(ts);
        return;
    }

    fcntl(ts, F_SETFL, flags);
    int one = 1;
    setsockopt(ts, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    if (proto_send(ts, TYPE_HELLO, g_data.myname, strlen(g_data.myname)) < 0) {
        close(ts);
        return;
    }
    uint8_t mtype;
    uint8_t *mpayload;
    uint32_t mlen;
    if (proto_recv(ts, &mtype, &mpayload, &mlen) < 0 || mtype != TYPE_HELLO) {
        free(mpayload);
        close(ts);
        return;
    }
    char rname[NAME_LEN] = {0};
    strncpy(rname, (char *)mpayload, NAME_LEN - 1);
    free(mpayload);

    if (peer_add(rname, ip, port, ts) < 0) {
        close(ts);
        return;
    }

    int *psock = malloc(sizeof(int));
    *psock = ts;
    pthread_t rt;
    pthread_create(&rt, NULL, handle_peer_messages, psock);
    pthread_detach(rt);
}