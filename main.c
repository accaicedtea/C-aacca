#include "discovery.h"
#include "peer_manager.h"
#include "file_transfer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#ifdef _WIN32
    #include <winsock2.h>
    void usleep(__int64 usec) { Sleep(usec/1000); }
    #define sleep(x) Sleep(1000*(x))
#else
    #include <netinet/in.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
#endif

#define TCP_PORT 9000

static volatile int keep_running = 1;

void int_handler(int sig) {
    (void)sig;  // evita warning unused parameter
    keep_running = 0;
}

void *tcp_server_thread(void *arg) {
    (void)arg;  // evita warning unused parameter
    
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("socket");
        return NULL;
    }
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TCP_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    
    int reuse = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        closesocket(listen_sock);
        return NULL;
    }
    
    listen(listen_sock, 10);
    printf("Server TCP in ascolto sulla porta %d\n", TCP_PORT);

    while (keep_running) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        SOCKET client = accept(listen_sock, (struct sockaddr*)&client_addr, &len);
        if (client < 0) {
            if (keep_running) perror("accept");
            continue;
        }

        char ip[16];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        unsigned short port = ntohs(client_addr.sin_port);
        printf("\n✓ Nuova connessione TCP da %s:%u\n", ip, port);

        // Aggiunge il peer alla lista
        pthread_mutex_lock(&list_mutex);
        peers = realloc(peers, (peer_count+1)*sizeof(peer_t));
        peers[peer_count].sock = client;
        strncpy(peers[peer_count].ip, ip, 16);
        peers[peer_count].port = port;
        peer_count++;
        pthread_mutex_unlock(&list_mutex);

        // Avvia thread di ricezione per questo socket
        pthread_t tid;
        SOCKET *sock_arg = malloc(sizeof(SOCKET));
        *sock_arg = client;
        pthread_create(&tid, NULL, transfer_receive_loop, sock_arg);
        pthread_detach(tid);
    }
    
    closesocket(listen_sock);
    return NULL;
}

int main(int argc, char **argv) {
    (void)argc;  // evita warning unused parameter
    (void)argv;  // evita warning unused parameter
    
    signal(SIGINT, int_handler);
    signal(SIGTERM, int_handler);

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);
#endif

    peer_init();
    discovery_start(TCP_PORT);

    pthread_t server_tid;
    pthread_create(&server_tid, NULL, tcp_server_thread, NULL);

    printf("\n=== P2P Chat & File Transfer ===\n");
    printf("Comandi:\n"
           "  testo          -> invia messaggio a tutti i peer\n"
           "  /peers         -> elenco peer connessi\n"
           "  /file <nome>   -> invia file a tutti i peer\n"
           "  /quit          -> esce\n\n> ");
    fflush(stdout);

    char input[1024];
    while (keep_running && fgets(input, sizeof(input), stdin)) {
        size_t len = strlen(input);
        if (len > 0 && input[len-1] == '\n') input[len-1] = '\0';

        if (strcmp(input, "/quit") == 0) break;
        else if (strcmp(input, "/peers") == 0) peer_list_print();
        else if (strncmp(input, "/file ", 6) == 0)
            peer_broadcast_file(input + 6);
        else if (strlen(input) > 0)
            peer_broadcast_message(input);
        
        printf("> ");
        fflush(stdout);
    }

    keep_running = 0;
    peer_shutdown_all();
    discovery_stop();
    pthread_cancel(server_tid);
    pthread_join(server_tid, NULL);

#ifdef _WIN32
    WSACleanup();
#endif

    printf("\nProgramma terminato.\n");
    return 0;
}