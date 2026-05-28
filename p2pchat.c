#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

// ==================== PROTOCOLLO ====================
#define PROTO_MAGIC     0x50325000
#define TYPE_TEXT       0x01
#define TYPE_FILE_REQ   0x02
#define TYPE_FILE_DATA  0x03
#define TYPE_FILE_END   0x04
#define TYPE_QUIT       0xFF
#define CHUNK_SIZE      8192

// ==================== CONFIGURAZIONE ====================
#define TCP_PORT        9000
#define DISCOVERY_PORT  9001
#define MAX_PEERS       32
#define MAX_NAME        32

// ==================== TIPI ====================
typedef struct {
    int sock;
    char name[MAX_NAME];
    char ip[16];
    unsigned short port;
    int active;
} Peer;

// ==================== STATO GLOBALE ====================
static Peer peers[MAX_PEERS];
static int peer_count = 0;
static pthread_mutex_t peer_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int running = 1;
static char my_name[MAX_NAME];

// ==================== UTILITÀ ====================
static uint32_t htonl_local(uint32_t x) { return htonl(x); }
static uint32_t ntohl_local(uint32_t x) { return ntohl(x); }

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

// ==================== PROTOCOLLO MESSAGGI ====================
static int send_msg(int sock, uint8_t type, const void *payload, uint32_t len) {
    uint32_t magic = htonl_local(PROTO_MAGIC);
    uint32_t netlen = htonl_local(len);
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
    if (ntohl_local(magic) != PROTO_MAGIC) return -1;
    
    *type = hdr[4];
    memcpy(len, hdr + 5, 4);
    *len = ntohl_local(*len);
    
    if (*len > 0) {
        *payload = malloc(*len);
        if (!*payload) return -1;
        if (recv_all(sock, *payload, *len) < 0) {
            free(*payload);
            return -1;
        }
    } else {
        *payload = NULL;
    }
    return 0;
}

// ==================== GESTIONE PEER ====================

static int add_peer(const char *name, const char *ip, unsigned short port, int sock) {
    pthread_mutex_lock(&peer_mutex);
    
    // Cerca slot libero
    int idx = -1;
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!peers[i].active) {
            idx = i;
            break;
        }
    }
    
    if (idx == -1) {
        pthread_mutex_unlock(&peer_mutex);
        return -1;
    }
    
    snprintf(peers[idx].name, MAX_NAME, "%s", name);
    snprintf(peers[idx].ip, 16, "%s", ip);
    peers[idx].port = port;
    peers[idx].sock = sock;
    peers[idx].active = 1;
    peer_count++;
    
    printf("\n✅ Connesso a: %s (%s:%d)\n", name, ip, port);
    printf("> ");
    fflush(stdout);
    
    pthread_mutex_unlock(&peer_mutex);
    return idx;
}

static void remove_peer_by_sock(int sock) {
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active && peers[i].sock == sock) {
            printf("\n❌ Disconnesso: %s\n> ", peers[i].name);
            fflush(stdout);
            close(peers[i].sock);
            peers[i].active = 0;
            peer_count--;
            break;
        }
    }
    pthread_mutex_unlock(&peer_mutex);
}

// ==================== DISCOVERY UDP ====================
static void *discovery_sender(void *arg) {
    (void)arg;
    
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    int broadcast = 1;
    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(DISCOVERY_PORT);
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    
    char msg[128];
    
    while (running) {
        snprintf(msg, sizeof(msg), "P2P|%s|%d", my_name, TCP_PORT);
        sendto(sock, msg, strlen(msg), 0, (struct sockaddr*)&addr, sizeof(addr));
        sleep(2);
    }
    
    close(sock);
    return NULL;
}

static void *discovery_listener(void *arg) {
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
        int n = recvfrom(sock, buf, sizeof(buf) - 1, 0, 
                        (struct sockaddr*)&sender, &sender_len);
        if (n <= 0) continue;
        
        buf[n] = '\0';
        
        char name[MAX_NAME], ip[16];
        unsigned int port;
        
        if (sscanf(buf, "P2P|%31[^|]|%u", name, &port) != 2) continue;
        
        inet_ntop(AF_INET, &sender.sin_addr, ip, sizeof(ip));
        
        // Ignora se stessi
        if (strcmp(ip, "127.0.0.1") == 0 && port == TCP_PORT) continue;
        
        pthread_mutex_lock(&peer_mutex);
        int exists = 0;
        for (int i = 0; i < MAX_PEERS; i++) {
            if (peers[i].active && strcmp(peers[i].ip, ip) == 0 && peers[i].port == port) {
                exists = 1;
                break;
            }
        }
        pthread_mutex_unlock(&peer_mutex);
        
        if (exists) continue;
        
        // Connetti via TCP
        int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in tcp_addr;
        memset(&tcp_addr, 0, sizeof(tcp_addr));
        tcp_addr.sin_family = AF_INET;
        tcp_addr.sin_port = htons(port);
        inet_pton(AF_INET, ip, &tcp_addr.sin_addr);
        
        if (connect(tcp_sock, (struct sockaddr*)&tcp_addr, sizeof(tcp_addr)) == 0) {
            add_peer(name, ip, port, tcp_sock);
        } else {
            close(tcp_sock);
        }
    }
    
    close(sock);
    return NULL;
}

// ==================== SERVER TCP ====================
static void *tcp_receiver(void *arg) {
    int sock = *(int*)arg;
    free(arg);
    
    while (running) {
        uint8_t type;
        uint8_t *payload;
        uint32_t plen;
        
        if (recv_msg(sock, &type, &payload, &plen) < 0) break;
        
        switch (type) {
            case TYPE_TEXT:
                printf("\n💬 Messaggio: %.*s\n> ", plen, (char*)payload);
                fflush(stdout);
                break;
                
            case TYPE_FILE_REQ: {
                uint16_t name_len;
                memcpy(&name_len, payload, 2);
                char filename[256];
                memcpy(filename, payload + 2, name_len);
                filename[name_len] = '\0';
                
                uint64_t fsize;
                memcpy(&fsize, payload + 2 + name_len, 8);
                
                printf("\n📥 File in arrivo: %s (%llu bytes)\n> ", 
                       filename, (unsigned long long)fsize);
                fflush(stdout);
                
                FILE *f = fopen(filename, "wb");
                uint64_t received = 0;
                
                while (received < fsize) {
                    uint8_t t2;
                    uint8_t *p2;
                    uint32_t l2;
                    
                    if (recv_msg(sock, &t2, &p2, &l2) < 0) break;
                    
                    if (t2 == TYPE_FILE_DATA) {
                        if (f) fwrite(p2, 1, l2, f);
                        received += l2;
                    } else if (t2 == TYPE_FILE_END) {
                        free(p2);
                        break;
                    }
                    free(p2);
                }
                
                if (f) fclose(f);
                printf("✅ File ricevuto: %s\n> ", filename);
                fflush(stdout);
                break;
            }
                
            case TYPE_QUIT:
                free(payload);
                goto done;
        }
        free(payload);
    }
    
done:
    remove_peer_by_sock(sock);
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
    
    printf("Server TCP in ascolto sulla porta %d\n", TCP_PORT);
    
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client = accept(listen_sock, (struct sockaddr*)&client_addr, &len);
        if (client < 0) continue;
        
        char ip[16];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        
        pthread_t tid;
        int *sock_ptr = malloc(sizeof(int));
        *sock_ptr = client;
        pthread_create(&tid, NULL, tcp_receiver, sock_ptr);
        pthread_detach(tid);
    }
    
    close(listen_sock);
    return NULL;
}

// ==================== MENU PRINCIPALE ====================
static void show_peers(void) {
    pthread_mutex_lock(&peer_mutex);
    printf("\n┌─ Peer connessi ─────────────────────────────┐\n");
    if (peer_count == 0) {
        printf("│  Nessun peer connesso                       │\n");
    } else {
        int idx = 0;
        for (int i = 0; i < MAX_PEERS; i++) {
            if (peers[i].active) {
                printf("│ [%d] %-20s %-15s │\n", 
                       idx++, peers[i].name, peers[i].ip);
            }
        }
    }
    printf("└──────────────────────────────────────────────┘\n");
    pthread_mutex_unlock(&peer_mutex);
}

static int select_peer(void) {
    pthread_mutex_lock(&peer_mutex);
    
    // Costruisci array di peer attivi
    int active_indices[MAX_PEERS];
    int active_count = 0;
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active) {
            active_indices[active_count++] = i;
        }
    }
    
    pthread_mutex_unlock(&peer_mutex);
    
    if (active_count == 0) {
        printf("\n⚠️  Nessun peer disponibile\n");
        return -1;
    }
    
    // Mostra elenco
    printf("\n┌─ Seleziona peer ────────────────────────────┐\n");
    for (int i = 0; i < active_count; i++) {
        int idx = active_indices[i];
        printf("│ [%d] %-30s │\n", i, peers[idx].name);
    }
    printf("└──────────────────────────────────────────────┘\n");
    printf("Numero (0-%d): ", active_count - 1);
    fflush(stdout);
    
    char buf[16];
    if (!fgets(buf, sizeof(buf), stdin)) return -1;
    int choice = atoi(buf);
    
    if (choice >= 0 && choice < active_count)
        return active_indices[choice];
    
    return -1;
}

// ==================== MAIN ====================
int main(void) {
    // Nome host
    gethostname(my_name, sizeof(my_name));
    my_name[sizeof(my_name) - 1] = '\0';
    
    printf("╔══════════════════════════════════════════╗\n");
    printf("║  P2P Chat & File Transfer               ║\n");
    printf("║  Nome: %-32s ║\n", my_name);
    printf("╚══════════════════════════════════════════╝\n\n");
    
    // Avvia server TCP
    pthread_t server_tid;
    pthread_create(&server_tid, NULL, tcp_server, NULL);
    
    // Avvia discovery
    pthread_t disc_send, disc_recv;
    pthread_create(&disc_send, NULL, discovery_sender, NULL);
    pthread_create(&disc_recv, NULL, discovery_listener, NULL);
    
    sleep(1);
    
    // Menu
    char input[1024];
    
    while (running) {
        printf("\n┌─ Menu ────────────────────────────────────┐\n");
        printf("│ 1. Mostra peer                            │\n");
        printf("│ 2. Invia messaggio                        │\n");
        printf("│ 3. Invia file                             │\n");
        printf("│ 4. Broadcast messaggio                    │\n");
        printf("│ 5. Broadcast file                         │\n");
        printf("│ 0. Esci                                   │\n");
        printf("└────────────────────────────────────────────┘\n");
        printf("> ");
        fflush(stdout);
        
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = '\0';
        
        int choice = atoi(input);
        
        switch (choice) {
            case 0:
                running = 0;
                break;
                
            case 1:
                show_peers();
                break;
                
            case 2: {
                int idx = select_peer();
                if (idx >= 0) {
                    printf("Messaggio: ");
                    fflush(stdout);
                    if (fgets(input, sizeof(input), stdin)) {
                        input[strcspn(input, "\n")] = '\0';
                        if (strlen(input) > 0) {
                            send_msg(peers[idx].sock, TYPE_TEXT, input, strlen(input));
                            printf("✅ Inviato a %s\n", peers[idx].name);
                        }
                    }
                }
                break;
            }
                
            case 3: {
                int idx = select_peer();
                if (idx >= 0) {
                    printf("Nome file: ");
                    fflush(stdout);
                    if (fgets(input, sizeof(input), stdin)) {
                        input[strcspn(input, "\n")] = '\0';
                        if (strlen(input) > 0) {
                            FILE *f = fopen(input, "rb");
                            if (!f) {
                                printf("❌ File non trovato\n");
                            } else {
                                fseek(f, 0, SEEK_END);
                                long fsize = ftell(f);
                                rewind(f);
                                
                                // Header
                                uint16_t name_len = strlen(input);
                                uint8_t *req = malloc(2 + name_len + 8);
                                memcpy(req, &name_len, 2);
                                memcpy(req + 2, input, name_len);
                                uint64_t fsize64 = fsize;
                                memcpy(req + 2 + name_len, &fsize64, 8);
                                send_msg(peers[idx].sock, TYPE_FILE_REQ, req, 2 + name_len + 8);
                                free(req);
                                
                                // Dati
                                uint8_t chunk[CHUNK_SIZE];
                                size_t n;
                                while ((n = fread(chunk, 1, CHUNK_SIZE, f)) > 0) {
                                    send_msg(peers[idx].sock, TYPE_FILE_DATA, chunk, n);
                                }
                                send_msg(peers[idx].sock, TYPE_FILE_END, NULL, 0);
                                fclose(f);
                                printf("✅ File inviato a %s\n", peers[idx].name);
                            }
                        }
                    }
                }
                break;
            }
                
            case 4: {
                printf("Messaggio broadcast: ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin)) {
                    input[strcspn(input, "\n")] = '\0';
                    if (strlen(input) > 0) {
                        pthread_mutex_lock(&peer_mutex);
                        for (int i = 0; i < MAX_PEERS; i++) {
                            if (peers[i].active) {
                                send_msg(peers[i].sock, TYPE_TEXT, input, strlen(input));
                            }
                        }
                        pthread_mutex_unlock(&peer_mutex);
                        printf("✅ Broadcast inviato\n");
                    }
                }
                break;
            }
                
            case 5: {
                printf("File broadcast: ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin)) {
                    input[strcspn(input, "\n")] = '\0';
                    if (strlen(input) > 0) {
                        FILE *f = fopen(input, "rb");
                        if (!f) {
                            printf("❌ File non trovato\n");
                        } else {
                            fseek(f, 0, SEEK_END);
                            long fsize = ftell(f);
                            rewind(f);
                            
                            pthread_mutex_lock(&peer_mutex);
                            for (int i = 0; i < MAX_PEERS; i++) {
                                if (peers[i].active) {
                                    uint16_t name_len = strlen(input);
                                    uint8_t *req = malloc(2 + name_len + 8);
                                    memcpy(req, &name_len, 2);
                                    memcpy(req + 2, input, name_len);
                                    uint64_t fsize64 = fsize;
                                    memcpy(req + 2 + name_len, &fsize64, 8);
                                    send_msg(peers[i].sock, TYPE_FILE_REQ, req, 2 + name_len + 8);
                                    free(req);
                                    
                                    rewind(f);
                                    uint8_t chunk[CHUNK_SIZE];
                                    size_t n;
                                    while ((n = fread(chunk, 1, CHUNK_SIZE, f)) > 0) {
                                        send_msg(peers[i].sock, TYPE_FILE_DATA, chunk, n);
                                    }
                                    send_msg(peers[i].sock, TYPE_FILE_END, NULL, 0);
                                }
                            }
                            pthread_mutex_unlock(&peer_mutex);
                            fclose(f);
                            printf("✅ File broadcast inviato\n");
                        }
                    }
                }
                break;
            }
        }
    }
    
    // Cleanup
    running = 0;
    
    pthread_mutex_lock(&peer_mutex);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (peers[i].active) {
            send_msg(peers[i].sock, TYPE_QUIT, NULL, 0);
            close(peers[i].sock);
        }
    }
    pthread_mutex_unlock(&peer_mutex);
    
    pthread_join(server_tid, NULL);
    
    printf("\n👋 Arrivederci!\n");
    return 0;
}