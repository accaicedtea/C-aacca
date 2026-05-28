#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <errno.h>

// Protocollo
#define MAGIC         0x50325000
#define TYPE_TEXT     0x01
#define TYPE_FILE_REQ 0x02
#define TYPE_FILE_DATA 0x03
#define TYPE_FILE_END 0x04
#define TYPE_QUIT     0xFF
#define TYPE_PING     0x10
#define TYPE_PONG     0x11
#define CHUNK_SIZE    8192

// Config
#define TCP_PORT      9000
#define DISCOVERY_PORT 9001
#define MAX_PEERS     32
#define NAME_LEN      32
#define HEARTBEAT     5
#define TIMEOUT       15

typedef struct {
    int sock;
    char name[NAME_LEN];
    char ip[16];
    int port;
    int active;
    time_t last;
    int waiting;
} Peer;

static Peer peers[MAX_PEERS];
static int pcount = 0;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static volatile int run = 1;
static char myname[NAME_LEN];
static char myip[16];

// ==== NET HELPERS ====
int send_all(int s, const void *b, int l) {
    int t = 0;
    while (t < l) {
        int n = send(s, (char*)b+t, l-t, 0);
        if (n <= 0) return -1;
        t += n;
    }
    return 0;
}

int recv_all(int s, void *b, int l) {
    int t = 0;
    while (t < l) {
        int n = recv(s, (char*)b+t, l-t, 0);
        if (n <= 0) return -1;
        t += n;
    }
    return 0;
}

int send_msg(int s, uint8_t type, const void *p, uint32_t len) {
    uint32_t m = htonl(MAGIC);
    uint32_t nl = htonl(len);
    uint8_t h[9];
    memcpy(h, &m, 4);
    h[4] = type;
    memcpy(h+5, &nl, 4);
    if (send_all(s, h, 9) < 0) return -1;
    if (len > 0 && send_all(s, p, len) < 0) return -1;
    return 0;
}

int recv_msg(int s, uint8_t *type, uint8_t **p, uint32_t *len) {
    uint8_t h[9];
    if (recv_all(s, h, 9) < 0) return -1;
    uint32_t m;
    memcpy(&m, h, 4);
    if (ntohl(m) != MAGIC) return -1;
    *type = h[4];
    memcpy(len, h+5, 4);
    *len = ntohl(*len);
    if (*len > 0) {
        *p = malloc(*len);
        if (recv_all(s, *p, *len) < 0) { free(*p); return -1; }
    } else *p = NULL;
    return 0;
}

// ==== PEER ====
void add_peer(const char *name, const char *ip, int port, int sock) {
    pthread_mutex_lock(&lock);
    // check duplicate
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
            strncpy(peers[i].name, name, NAME_LEN-1);
            close(peers[i].sock);
            peers[i].sock = sock;
            peers[i].last = time(NULL);
            peers[i].waiting = 0;
            pthread_mutex_unlock(&lock);
            printf("\n🔄 %s riconnesso\n> ", name);
            fflush(stdout);
            return;
        }
    }
    // find empty
    int idx = -1;
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) { idx = i; break; }
    }
    if (idx < 0) { close(sock); pthread_mutex_unlock(&lock); return; }
    
    strncpy(peers[idx].name, name, NAME_LEN-1);
    strncpy(peers[idx].ip, ip, 15);
    peers[idx].port = port;
    peers[idx].sock = sock;
    peers[idx].active = 1;
    peers[idx].last = time(NULL);
    peers[idx].waiting = 0;
    pcount++;
    pthread_mutex_unlock(&lock);
    printf("\n✅ %s connesso! (%s)\n> ", name, ip);
    fflush(stdout);
}

void remove_peer(int sock) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && peers[i].sock == sock) {
            printf("\n❌ %s disconnesso\n> ", peers[i].name);
            fflush(stdout);
            close(sock);
            peers[i].active = 0;
            pcount--;
            break;
        }
    }
    pthread_mutex_unlock(&lock);
}

// ==== HEARTBEAT ====
void *heartbeat(void *a) {
    (void)a;
    while (run) {
        sleep(HEARTBEAT);
        pthread_mutex_lock(&lock);
        time_t now = time(NULL);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!peers[i].active) continue;
            if (peers[i].waiting && (now - peers[i].last) > TIMEOUT) {
                printf("\n⚠️  %s timeout\n> ", peers[i].name);
                fflush(stdout);
                close(peers[i].sock);
                peers[i].active = 0;
                pcount--;
                continue;
            }
            if (!peers[i].waiting) {
                if (send_msg(peers[i].sock, TYPE_PING, NULL, 0) == 0)
                    peers[i].waiting = 1;
                else {
                    close(peers[i].sock);
                    peers[i].active = 0;
                    pcount--;
                }
            }
        }
        pthread_mutex_unlock(&lock);
    }
    return NULL;
}

// ==== DISCOVERY ====
void *discovery_responder(void *a) {
    (void)a;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    int r = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &r, sizeof(r));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DISCOVERY_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(s, (struct sockaddr*)&addr, sizeof(addr));
    
    char buf[256];
    struct sockaddr_in from;
    socklen_t flen;
    
    while (run) {
        flen = sizeof(from);
        int n = recvfrom(s, buf, 255, 0, (struct sockaddr*)&from, &flen);
        if (n <= 0) continue;
        buf[n] = 0;
        if (strcmp(buf, "P2P_DISCOVER") == 0) {
            char resp[128];
            snprintf(resp, sizeof(resp), "P2P_HERE|%s|%d", myname, TCP_PORT);
            sendto(s, resp, strlen(resp), 0, (struct sockaddr*)&from, sizeof(from));
        }
    }
    close(s);
    return NULL;
}

void *discovery_seeker(void *a) {
    (void)a;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    int b = 1;
    setsockopt(s, SOL_SOCKET, SO_BROADCAST, &b, sizeof(b));
    struct timeval tv = {1, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    struct sockaddr_in baddr = {0};
    baddr.sin_family = AF_INET;
    baddr.sin_port = htons(DISCOVERY_PORT);
    baddr.sin_addr.s_addr = inet_addr("255.255.255.255");
    
    char buf[256];
    struct sockaddr_in resp;
    socklen_t rlen;
    
    while (run) {
        sendto(s, "P2P_DISCOVER", 12, 0, (struct sockaddr*)&baddr, sizeof(baddr));
        time_t start = time(NULL);
        while (time(NULL) - start < 2) {
            rlen = sizeof(resp);
            int n = recvfrom(s, buf, 255, 0, (struct sockaddr*)&resp, &rlen);
            if (n <= 0) continue;
            buf[n] = 0;
            
            char name[NAME_LEN], ip[16];
            int port;
            if (sscanf(buf, "P2P_HERE|%31[^|]|%d", name, &port) != 2) continue;
            inet_ntop(AF_INET, &resp.sin_addr, ip, sizeof(ip));
            if (port == TCP_PORT && strcmp(ip, myip) == 0) continue;
            
            // check if already connected
            pthread_mutex_lock(&lock);
            int found = 0;
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port)
                    found = 1;
            pthread_mutex_unlock(&lock);
            if (found) continue;
            
            int ts = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in ta = {0};
            ta.sin_family = AF_INET;
            ta.sin_port = htons(port);
            inet_pton(AF_INET, ip, &ta.sin_addr);
            
            if (connect(ts, (struct sockaddr*)&ta, sizeof(ta)) == 0)
                add_peer(name, ip, port, ts);
            else
                close(ts);
        }
        sleep(3);
    }
    close(s);
    return NULL;
}

// ==== TCP RECEIVER ====
void *tcp_receiver(void *a) {
    (void)a;
    int s = *(int*)a;
    free(a);
    
    while (run) {
        uint8_t t;
        uint8_t *p;
        uint32_t l;
        if (recv_msg(s, &t, &p, &l) < 0) break;
        
        switch (t) {
            case TYPE_PING:
                send_msg(s, TYPE_PONG, NULL, 0);
                free(p);
                break;
            case TYPE_PONG:
                pthread_mutex_lock(&lock);
                for (int i = 0; i < MAX_PEERS; i++)
                    if (peers[i].active && peers[i].sock == s) {
                        peers[i].last = time(NULL);
                        peers[i].waiting = 0;
                        break;
                    }
                pthread_mutex_unlock(&lock);
                free(p);
                break;
            case TYPE_TEXT:
                printf("\n💬 %.*s\n> ", l, (char*)p);
                fflush(stdout);
                free(p);
                break;
            case TYPE_FILE_REQ: {
                uint16_t nl;
                memcpy(&nl, p, 2);
                char fn[256];
                memcpy(fn, p+2, nl);
                fn[nl] = 0;
                uint64_t fs;
                memcpy(&fs, p+2+nl, 8);
                free(p);
                printf("\n📥 %s (%llu B)\n> ", fn, (unsigned long long)fs);
                fflush(stdout);
                
                FILE *f = fopen(fn, "wb");
                uint64_t rc = 0;
                while (rc < fs) {
                    uint8_t t2; uint8_t *p2; uint32_t l2;
                    if (recv_msg(s, &t2, &p2, &l2) < 0) break;
                    if (t2 == TYPE_FILE_DATA) { if (f) fwrite(p2,1,l2,f); rc += l2; }
                    else if (t2 == TYPE_FILE_END) { free(p2); break; }
                    free(p2);
                }
                if (f) fclose(f);
                printf("✅ %s ok\n> ", fn);
                fflush(stdout);
                break;
            }
            case TYPE_QUIT:
                free(p);
                goto out;
            default:
                free(p);
        }
    }
out:
    remove_peer(s);
    return NULL;
}

void *tcp_server(void *a) {
    (void)a;
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    int r = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &r, sizeof(r));
    
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TCP_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(ls, (struct sockaddr*)&addr, sizeof(addr));
    listen(ls, 10);
    
    while (run) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof(ca);
        int c = accept(ls, (struct sockaddr*)&ca, &cl);
        if (c < 0) continue;
        pthread_t t;
        int *sp = malloc(sizeof(int));
        *sp = c;
        pthread_create(&t, NULL, tcp_receiver, sp);
        pthread_detach(t);
    }
    close(ls);
    return NULL;
}

// ==== MENU ====
void show_peers(void) {
    pthread_mutex_lock(&lock);
    printf("\n╔══════════════════════════════════╗\n");
    printf("║  Peer connessi: %-2d              ║\n", pcount);
    printf("╠══════════════════════════════════╣\n");
    if (pcount == 0) {
        printf("║  (nessuno)                       ║\n");
    } else {
        int n = 0;
        for (int i = 0; i < MAX_PEERS; i++) {
            if (peers[i].active) {
                printf("║ [%d] %-20s     ║\n", n, peers[i].name);
                n++;
            }
        }
    }
    printf("╚══════════════════════════════════╝\n");
    pthread_mutex_unlock(&lock);
}

// ==== MAIN ====
int main(void) {
    gethostname(myname, NAME_LEN);
    myname[NAME_LEN-1] = 0;
    
    // get IP
    struct ifaddrs *ifa;
    if (getifaddrs(&ifa) == 0) {
        for (struct ifaddrs *i = ifa; i; i = i->ifa_next) {
            if (i->ifa_addr && i->ifa_addr->sa_family == AF_INET) {
                struct sockaddr_in *s = (struct sockaddr_in*)i->ifa_addr;
                char *ip = inet_ntoa(s->sin_addr);
                if (strcmp(ip, "127.0.0.1") != 0) {
                    strncpy(myip, ip, 15);
                    break;
                }
            }
        }
        freeifaddrs(ifa);
    }
    if (!myip[0]) strcpy(myip, "127.0.0.1");
    
    printf("\n");
    printf("╔══════════════════════════════════════╗\n");
    printf("║        P2P CHAT & FILE              ║\n");
    printf("║  Host: %-28s ║\n", myname);
    printf("║  IP:   %-28s ║\n", myip);
    printf("╚══════════════════════════════════════╝\n\n");
    
    // start threads
    pthread_t st, dr, ds, hb;
    pthread_create(&st, NULL, tcp_server, NULL);
    pthread_create(&dr, NULL, discovery_responder, NULL);
    pthread_create(&ds, NULL, discovery_seeker, NULL);
    pthread_create(&hb, NULL, heartbeat, NULL);
    sleep(1);
    
    printf("In attesa di peer...\n");
    printf("Comandi: 1=peers 2=msg 3=file 4=all msg 5=all file 0=exit\n\n");
    
    char buf[1024];
    
    while (run) {
        printf("> ");
        fflush(stdout);
        if (!fgets(buf, sizeof(buf), stdin)) break;
        buf[strcspn(buf, "\n")] = 0;
        
        if (strcmp(buf, "0") == 0 || strcmp(buf, "q") == 0 || strcmp(buf, "quit") == 0) {
            run = 0;
            break;
        }
        else if (strcmp(buf, "1") == 0 || strcmp(buf, "p") == 0) {
            show_peers();
        }
        else if (strcmp(buf, "2") == 0 || strcmp(buf, "m") == 0) {
            // scegli peer
            pthread_mutex_lock(&lock);
            int idxs[MAX_PEERS], cnt = 0;
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active) idxs[cnt++] = i;
            pthread_mutex_unlock(&lock);
            
            if (cnt == 0) { printf("Nessun peer\n"); continue; }
            
            printf("Peer:\n");
            for (int i = 0; i < cnt; i++)
                printf(" [%d] %s\n", i, peers[idxs[i]].name);
            printf("Chi? ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            int c = atoi(buf);
            if (c < 0 || c >= cnt) continue;
            int idx = idxs[c];
            
            printf("Msg: ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            buf[strcspn(buf, "\n")] = 0;
            if (strlen(buf) > 0) {
                if (send_msg(peers[idx].sock, TYPE_TEXT, buf, strlen(buf)) == 0)
                    printf("✅ ok\n");
                else
                    printf("❌ fail\n");
            }
        }
        else if (strcmp(buf, "3") == 0 || strcmp(buf, "f") == 0) {
            pthread_mutex_lock(&lock);
            int idxs[MAX_PEERS], cnt = 0;
            for (int i = 0; i < MAX_PEERS; i++)
                if (peers[i].active) idxs[cnt++] = i;
            pthread_mutex_unlock(&lock);
            
            if (cnt == 0) { printf("Nessun peer\n"); continue; }
            
            printf("Peer:\n");
            for (int i = 0; i < cnt; i++)
                printf(" [%d] %s\n", i, peers[idxs[i]].name);
            printf("Chi? ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            int c = atoi(buf);
            if (c < 0 || c >= cnt) continue;
            int idx = idxs[c];
            
            printf("File: ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            buf[strcspn(buf, "\n")] = 0;
            
            FILE *f = fopen(buf, "rb");
            if (!f) { printf("❌ File non trovato\n"); continue; }
            
            fseek(f, 0, SEEK_END);
            long fs = ftell(f);
            rewind(f);
            
            uint16_t nl = strlen(buf);
            uint8_t *req = malloc(2+nl+8);
            memcpy(req, &nl, 2);
            memcpy(req+2, buf, nl);
            uint64_t fss = fs;
            memcpy(req+2+nl, &fss, 8);
            
            if (send_msg(peers[idx].sock, TYPE_FILE_REQ, req, 2+nl+8) == 0) {
                free(req);
                uint8_t ch[CHUNK_SIZE];
                size_t n;
                while ((n = fread(ch,1,CHUNK_SIZE,f)) > 0)
                    send_msg(peers[idx].sock, TYPE_FILE_DATA, ch, n);
                send_msg(peers[idx].sock, TYPE_FILE_END, NULL, 0);
                printf("✅ File inviato\n");
            }
            fclose(f);
        }
        else if (strcmp(buf, "4") == 0) {
            printf("Broadcast: ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            buf[strcspn(buf, "\n")] = 0;
            if (strlen(buf) > 0) {
                int sent = 0;
                pthread_mutex_lock(&lock);
                for (int i = 0; i < MAX_PEERS; i++)
                    if (peers[i].active)
                        sent += (send_msg(peers[i].sock, TYPE_TEXT, buf, strlen(buf)) == 0);
                pthread_mutex_unlock(&lock);
                printf("✅ %d peer\n", sent);
            }
        }
        else if (strcmp(buf, "5") == 0) {
            printf("File broadcast: ");
            fflush(stdout);
            if (!fgets(buf, sizeof(buf), stdin)) continue;
            buf[strcspn(buf, "\n")] = 0;
            
            FILE *f = fopen(buf, "rb");
            if (!f) { printf("❌ File non trovato\n"); continue; }
            fseek(f, 0, SEEK_END);
            long fs = ftell(f);
            rewind(f);
            
            int sent = 0;
            pthread_mutex_lock(&lock);
            for (int i = 0; i < MAX_PEERS; i++) {
                if (!peers[i].active) continue;
                uint16_t nl = strlen(buf);
                uint8_t *req = malloc(2+nl+8);
                memcpy(req, &nl, 2);
                memcpy(req+2, buf, nl);
                uint64_t fss = fs;
                memcpy(req+2+nl, &fss, 8);
                if (send_msg(peers[i].sock, TYPE_FILE_REQ, req, 2+nl+8) == 0) {
                    rewind(f);
                    uint8_t ch[CHUNK_SIZE];
                    size_t n;
                    while ((n = fread(ch,1,CHUNK_SIZE,f)) > 0)
                        send_msg(peers[i].sock, TYPE_FILE_DATA, ch, n);
                    send_msg(peers[i].sock, TYPE_FILE_END, NULL, 0);
                    sent++;
                }
                free(req);
            }
            pthread_mutex_unlock(&lock);
            fclose(f);
            printf("✅ %d peer\n", sent);
        }
        else {
            printf("? 1=peers 2=msg 3=file 4=all 5=allfile 0=quit\n");
        }
    }
    
    // cleanup
    run = 0;
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX_PEERS; i++)
        if (peers[i].active) {
            send_msg(peers[i].sock, TYPE_QUIT, NULL, 0);
            close(peers[i].sock);
        }
    pthread_mutex_unlock(&lock);
    sleep(1);
    printf("\nCiao!\n");
    return 0;
}