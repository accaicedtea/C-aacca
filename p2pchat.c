#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <errno.h>

// ==================== PROTOCOLLO ====================
#define PROTO_MAGIC     0x50325000
#define TYPE_TEXT       0x01
#define TYPE_FILE_REQ   0x02
#define TYPE_FILE_DATA  0x03
#define TYPE_FILE_END   0x04
#define TYPE_QUIT       0xFF
#define TYPE_PING       0x10
#define TYPE_PONG       0x11
#define CHUNK_SIZE      8192

// ==================== CONFIGURAZIONE ====================
#define TCP_PORT        9000
#define DISCOVERY_PORT  9001
#define MAX_PEERS       32
#define MAX_NAME        32
#define HEARTBEAT_INTERVAL  5
#define HEARTBEAT_TIMEOUT   15

// ==================== TIPI ====================
typedef struct {
    int sock;
    char name[MAX_NAME];
    char ip[16];
    unsigned short port;
    int active;
    time_t last_seen;
    int awaiting_pong;
} Peer;

// ==================== STATO GLOBALE ====================
static Peer peers[MAX_PEERS];
static int peer_count = 0;
static pthread_mutex_t peer_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int running = 1;
static char my_name[MAX_NAME];
static char my_ip[16] = {0};

// ==================== UTILITÀ ====================
static int send_all(int sock, const void *buf, int len) {
    int sent = 0;
    while (sent < len) {
        int n = send(sock, (const char*)buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static int recv_all(int sock, void *buf, int len) {
    int received = 0;
    while (received < len) {
        int n = recv(sock, (char*)buf + received, len - received, 0);
        if (n <= 0) return -1;
        received += n;
    }
    return 0;
}

static int send_msg(int sock, uint8_t type, const void *payload, uint32_t len) {
    uint32_t magic = htonl(PROTO_MAGIC);
    uint32_t netlen = htonl(len);
    uint8_t hdr[9];
    memcpy(hdr, &magic, 4);
    hdr[4] = type;
    memcpy(hdr + 5, &netlen, 4);
    if (send_all(sock, hdr, 9) < 0) return -1;
    if (len > 0 && send_all(sock, payload, len) < 0) return -1;
    return 0;
}

static int recv_msg(int sock, uint8_t *type, uint8_t **payload, uint32_t *len) {
    uint8_t hdr[9];
    if (recv_all(sock, hdr, 9) < 0) return -1;
    uint32_t magic;
    memcpy(&magic, hdr, 4);
    magic = ntohl(magic);
    if (magic != PROTO_MAGIC) return -1;
    *type = hdr[4];
    memcpy(len, hdr + 5, 4);
    *len = ntohl(*len);
    if (*len > 0) {
        *payload = malloc(*len);
        if (!*payload) return -1;
        if (recv_all(sock, *payload, *len) < 0) { free(*payload); return -1; }
    } else {
        *payload = NULL;
    }
    return 0;
}

// ==================== PEER ====================
static int add_peer(const char *name, const char *ip, unsigned short port, int sock) {
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            snprintf(peers[i].name, MAX_NAME, "%s", name);
            close(peers[i].sock);
            peers[i].sock = sock;
            peers[i].last_seen = time(NULL);
            peers[i].awaiting_pong = 0;
            pthread_mutex_unlock(&peer_mutex);
            return i;
        }
    }
    int idx = -1;
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) { idx = i; break; }
    }
    if (idx == -1) { close(sock); pthread_mutex_unlock(&peer_mutex); return -1; }
    snprintf(peers[idx].name, MAX_NAME, "%s", name);
    snprintf(peers[idx].ip, 16, "%s", ip);
    peers[idx].port = port;
    peers[idx].sock = sock;
    peers[idx].active = 1;
    peers[idx].last_seen = time(NULL);
    peers[idx].awaiting_pong = 0;
    peer_count++;
    printf("\n✅ %s connesso (%s:%d)\n", name, ip, port);
    fflush(stdout);
    pthread_mutex_unlock(&peer_mutex);
    return idx;
}

static void remove_peer(int sock) {
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && peers[i].sock == sock) {
            printf("\n❌ %s disconnesso\n", peers[i].name);
            fflush(stdout);
            close(peers[i].sock);
            peers[i].active = 0;
            peer_count--;
            break;
        }
    }
    pthread_mutex_unlock(&peer_mutex);
}

// ==================== HEARTBEAT ====================
static void *heartbeat_thread(void *arg) {
    (void)arg;
    while (running) {
        sleep(HEARTBEAT_INTERVAL);
        pthread_mutex_lock(&peer_mutex);
        time_t now = time(NULL);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!peers[i].active) continue;
            if (peers[i].awaiting_pong && (now - peers[i].last_seen) > HEARTBEAT_TIMEOUT) {
                printf("\n⚠️  %s timeout\n", peers[i].name);
                fflush(stdout);
                close(peers[i].sock);
                peers[i].active = 0;
                peer_count--;
                continue;
            }
            if (!peers[i].awaiting_pong) {
                if (send_msg(peers[i].sock, TYPE_PING, NULL, 0) == 0)
                    peers[i].awaiting_pong = 1;
                else {
                    close(peers[i].sock);
                    peers[i].active = 0;
                    peer_count--;
                }
            }
        }
        pthread_mutex_unlock(&peer_mutex);
    }
    return NULL;
}

// ==================== DISCOVERY ====================
static void *discovery_responder(void *arg) {
    (void)arg;
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DISCOVERY_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(sock, (struct sockaddr*)&addr, sizeof(addr));
    char buf[256];
    struct sockaddr_in sender;
    socklen_t sender_len;
    while (running) {
        sender_len = sizeof(sender);
        int n = recvfrom(sock, buf, sizeof(buf)-1, 0, (struct sockaddr*)&sender, &sender_len);
        if (n <= 0) continue;
        buf[n] = '\0';
        if (strcmp(buf, "P2P_DISCOVER") == 0) {
            char response[128];
            snprintf(response, sizeof(response), "P2P_HERE|%s|%d", my_name, TCP_PORT);
            sendto(sock, response, strlen(response), 0, (struct sockaddr*)&sender, sizeof(sender));
        }
    }
    close(sock);
    return NULL;
}

static void *discovery_seeker(void *arg) {
    (void)arg;
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    int broadcast = 1;
    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));
    struct timeval tv = {1, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_in baddr;
    memset(&baddr, 0, sizeof(baddr));
    baddr.sin_family = AF_INET;
    baddr.sin_port = htons(DISCOVERY_PORT);
    baddr.sin_addr.s_addr = inet_addr("255.255.255.255");
    char buf[256];
    struct sockaddr_in resp;
    socklen_t resp_len;
    while (running) {
        sendto(sock, "P2P_DISCOVER", 12, 0, (struct sockaddr*)&baddr, sizeof(baddr));
        time_t start = time(NULL);
        while (time(NULL) - start < 2) {
            resp_len = sizeof(resp);
            int n = recvfrom(sock, buf, sizeof(buf)-1, 0, (struct sockaddr*)&resp, &resp_len);
            if (n <= 0) continue;
            buf[n] = '\0';
            char name[MAX_NAME], ip[16];
            unsigned int port;
            if (sscanf(buf, "P2P_HERE|%31[^|]|%u", name, &port) != 2) continue;
            inet_ntop(AF_INET, &resp.sin_addr, ip, sizeof(ip));
            if (port == TCP_PORT && strcmp(ip, my_ip) == 0) continue;
            pthread_mutex_lock(&peer_mutex);
            int exists = 0;
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port)
                    exists = 1;
            pthread_mutex_unlock(&peer_mutex);
            if (exists) continue;
            int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in tcp_addr;
            memset(&tcp_addr, 0, sizeof(tcp_addr));
            tcp_addr.sin_family = AF_INET;
            tcp_addr.sin_port = htons(port);
            inet_pton(AF_INET, ip, &tcp_addr.sin_addr);
            if (connect(tcp_sock, (struct sockaddr*)&tcp_addr, sizeof(tcp_addr)) == 0)
                add_peer(name, ip, port, tcp_sock);
            else
                close(tcp_sock);
        }
        sleep(3);
    }
    close(sock);
    return NULL;
}

// ==================== TCP ====================
static void *tcp_receiver(void *arg) {
    int sock = *(int*)arg;
    free(arg);
    while (running) {
        uint8_t type;
        uint8_t *payload;
        uint32_t plen;
        if (recv_msg(sock, &type, &payload, &plen) < 0) break;
        switch (type) {
            case TYPE_PING:
                send_msg(sock, TYPE_PONG, NULL, 0);
                free(payload);
                break;
            case TYPE_PONG:
                pthread_mutex_lock(&peer_mutex);
                for (int i = 0; i < MAX_PEERS; i++)
                    if (peers[i].active && peers[i].sock == sock) {
                        peers[i].last_seen = time(NULL);
                        peers[i].awaiting_pong = 0;
                        break;
                    }
                pthread_mutex_unlock(&peer_mutex);
                free(payload);
                break;
            case TYPE_TEXT:
                printf("\n💬 %.*s\n> ", plen, (char*)payload);
                fflush(stdout);
                free(payload);
                break;
            case TYPE_FILE_REQ: {
                uint16_t name_len;
                memcpy(&name_len, payload, 2);
                char filename[256];
                memcpy(filename, payload+2, name_len);
                filename[name_len] = '\0';
                uint64_t fsize;
                memcpy(&fsize, payload+2+name_len, 8);
                free(payload);
                printf("\n📥 Ricezione: %s (%llu byte)\n", filename, (unsigned long long)fsize);
                fflush(stdout);
                FILE *f = fopen(filename, "wb");
                uint64_t received = 0;
                while (received < fsize) {
                    uint8_t t2; uint8_t *p2; uint32_t l2;
                    if (recv_msg(sock, &t2, &p2, &l2) < 0) break;
                    if (t2 == TYPE_FILE_DATA) { if (f) fwrite(p2,1,l2,f); received += l2; }
                    else if (t2 == TYPE_FILE_END) { free(p2); break; }
                    free(p2);
                }
                if (f) fclose(f);
                printf("✅ %s ricevuto\n> ", filename);
                fflush(stdout);
                break;
            }
            case TYPE_QUIT:
                free(payload);
                goto done;
            default:
                free(payload);
        }
    }
done:
    remove_peer(sock);
    return NULL;
}

static void *tcp_server(void *arg) {
    (void)arg;
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TCP_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr));
    listen(listen_sock, 10);
    while (running) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof(ca);
        int client = accept(listen_sock, (struct sockaddr*)&ca, &cl);
        if (client < 0) continue;
        pthread_t tid;
        int *sp = malloc(sizeof(int)); *sp = client;
        pthread_create(&tid, NULL, tcp_receiver, sp);
        pthread_detach(tid);
    }
    close(listen_sock);
    return NULL;
}

// ==================== MENU ====================
static void show_peers(void) {
    pthread_mutex_lock(&peer_mutex);
    printf("\n┌─ Peer connessi (%d) ─────────────────────┐\n", peer_count);
    if (peer_count == 0) {
        printf("│  Nessuno                                  │\n");
    } else {
        int n = 0;
        for (int i = 0; i < MAX_PEERS; i++) {
            if (peers[i].active)
                printf("│ [%d] %-20s %-15s │\n", n++, peers[i].name, peers[i].ip);
        }
    }
    printf("└───────────────────────────────────────────┘\n");
    pthread_mutex_unlock(&peer_mutex);
}

static int select_peer(void) {
    pthread_mutex_lock(&peer_mutex);
    int indices[MAX_PEERS], count = 0;
    for (int i = 0; i < MAX_PEERS; i++)
        if (peers[i].active) indices[count++] = i;
    pthread_mutex_unlock(&peer_mutex);
    
    if (count == 0) {
        printf("⚠️  Nessun peer disponibile\n");
        return -1;
    }
    
    printf("Peer disponibili:\n");
    for (int i = 0; i < count; i++)
        printf("  [%d] %s\n", i, peers[indices[i]].name);
    printf("Scegli (0-%d): ", count-1);
    fflush(stdout);
    
    char buf[16];
    if (!fgets(buf, sizeof(buf), stdin)) return -1;
    int c = atoi(buf);
    return (c >= 0 && c < count) ? indices[c] : -1;
}

// ==================== MAIN ====================
int main(void) {
    gethostname(my_name, sizeof(my_name));
    my_name[sizeof(my_name)-1] = '\0';
    
    struct ifaddrs *ifaddr;
    if (getifaddrs(&ifaddr) == 0) {
        for (struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
                struct sockaddr_in *sin = (struct sockaddr_in*)ifa->ifa_addr;
                char *ip = inet_ntoa(sin->sin_addr);
                if (strcmp(ip, "127.0.0.1") != 0) {
                    strncpy(my_ip, ip, 15);
                    break;
                }
            }
        }
        freeifaddrs(ifaddr);
    }
    if (my_ip[0] == '\0') strcpy(my_ip, "127.0.0.1");
    
    printf("╔══════════════════════════════════════════╗\n");
    printf("║       P2P Chat & File Transfer          ║\n");
    printf("║  Nome: %-32s ║\n", my_name);
    printf("║  IP:   %-32s ║\n", my_ip);
    printf("╚══════════════════════════════════════════╝\n\n");
    
    pthread_t stid, dr, ds, hb;
    pthread_create(&stid, NULL, tcp_server, NULL);
    pthread_create(&dr, NULL, discovery_responder, NULL);
    pthread_create(&ds, NULL, discovery_seeker, NULL);
    pthread_create(&hb, NULL, heartbeat_thread, NULL);
    sleep(1);
    
    char input[1024];
    
    while (running) {
        printf("\n┌─ MENU ───────────────────────────────────┐\n");
        printf("│ 1. Mostra peer                            │\n");
        printf("│ 2. Invia messaggio a peer                 │\n");
        printf("│ 3. Invia file a peer                      │\n");
        printf("│ 4. Broadcast messaggio                    │\n");
        printf("│ 5. Broadcast file                         │\n");
        printf("│ 0. Esci                                   │\n");
        printf("└───────────────────────────────────────────┘\n");
        printf("> ");
        fflush(stdout);
        
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = '\0';
        int choice = atoi(input);
        
        switch (choice) {
            case 0: running = 0; break;
            case 1: show_peers(); break;
            case 2: {
                int idx = select_peer();
                if (idx >= 0) {
                    printf("Messaggio: ");
                    fflush(stdout);
                    if (fgets(input, sizeof(input), stdin)) {
                        input[strcspn(input, "\n")] = '\0';
                        if (strlen(input) > 0) {
                            if (send_msg(peers[idx].sock, TYPE_TEXT, input, strlen(input)) == 0)
                                printf("✅ Inviato\n");
                            else
                                printf("❌ Fallito\n");
                        }
                    }
                }
                break;
            }
            case 3: {
                int idx = select_peer();
                if (idx >= 0) {
                    printf("File: ");
                    fflush(stdout);
                    if (fgets(input, sizeof(input), stdin)) {
                        input[strcspn(input, "\n")] = '\0';
                        FILE *f = fopen(input, "rb");
                        if (!f) { printf("❌ File non trovato\n"); break; }
                        fseek(f, 0, SEEK_END);
                        long fsize = ftell(f);
                        rewind(f);
                        uint16_t nl = strlen(input);
                        uint8_t *req = malloc(2+nl+8);
                        memcpy(req, &nl, 2);
                        memcpy(req+2, input, nl);
                        uint64_t fs = fsize;
                        memcpy(req+2+nl, &fs, 8);
                        if (send_msg(peers[idx].sock, TYPE_FILE_REQ, req, 2+nl+8) == 0) {
                            free(req);
                            uint8_t chunk[CHUNK_SIZE];
                            size_t n;
                            while ((n = fread(chunk,1,CHUNK_SIZE,f)) > 0)
                                send_msg(peers[idx].sock, TYPE_FILE_DATA, chunk, n);
                            send_msg(peers[idx].sock, TYPE_FILE_END, NULL, 0);
                            printf("✅ File inviato\n");
                        }
                        fclose(f);
                    }
                }
                break;
            }
            case 4:
                printf("Broadcast: ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin)) {
                    input[strcspn(input, "\n")] = '\0';
                    if (strlen(input) > 0) {
                        int sent = 0;
                        pthread_mutex_lock(&peer_mutex);
                        for (int i = 0; i < MAX_PEERS; i++)
                            if (peers[i].active)
                                sent += (send_msg(peers[i].sock, TYPE_TEXT, input, strlen(input)) == 0);
                        pthread_mutex_unlock(&peer_mutex);
                        printf("✅ Inviato a %d peer\n", sent);
                    }
                }
                break;
            case 5:
                printf("File broadcast: ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin)) {
                    input[strcspn(input, "\n")] = '\0';
                    FILE *f = fopen(input, "rb");
                    if (!f) { printf("❌ File non trovato\n"); break; }
                    fseek(f, 0, SEEK_END);
                    long fsize = ftell(f);
                    rewind(f);
                    int sent = 0;
                    pthread_mutex_lock(&peer_mutex);
                    for (int i = 0; i < MAX_PEERS; i++) {
                        if (!peers[i].active) continue;
                        uint16_t nl = strlen(input);
                        uint8_t *req = malloc(2+nl+8);
                        memcpy(req, &nl, 2);
                        memcpy(req+2, input, nl);
                        uint64_t fs = fsize;
                        memcpy(req+2+nl, &fs, 8);
                        if (send_msg(peers[i].sock, TYPE_FILE_REQ, req, 2+nl+8) == 0) {
                            rewind(f);
                            uint8_t chunk[CHUNK_SIZE];
                            size_t n;
                            while ((n = fread(chunk,1,CHUNK_SIZE,f)) > 0)
                                send_msg(peers[i].sock, TYPE_FILE_DATA, chunk, n);
                            send_msg(peers[i].sock, TYPE_FILE_END, NULL, 0);
                            sent++;
                        }
                        free(req);
                    }
                    pthread_mutex_unlock(&peer_mutex);
                    fclose(f);
                    printf("✅ Inviato a %d peer\n", sent);
                }
                break;
        }
    }
    
    running = 0;
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < MAX_PEERS; i++)
        if (peers[i].active) {
            send_msg(peers[i].sock, TYPE_QUIT, NULL, 0);
            close(peers[i].sock);
        }
    pthread_mutex_unlock(&peer_mutex);
    sleep(1);
    printf("\n👋 Arrivederci!\n");
    return 0;
}