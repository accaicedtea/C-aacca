#include "discovery.h"
#include "peer_manager.h"
#include "file_transfer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <termios.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define TCP_PORT 9000
#define MENU_ITEMS 6

static volatile int keep_running = 1;

typedef struct {
    const char *label;
    char shortcut;
} MenuItem;

static MenuItem main_menu[MENU_ITEMS] = {
    {"📋 Mostra peer connessi", 'p'},
    {"💬 Invia messaggio a peer", 'm'},
    {"📁 Invia file a peer", 'f'},
    {"📢 Broadcast messaggio", 'b'},
    {"📦 Broadcast file", 'x'},
    {"❌ Esci", 'q'}
};

static int selected = 0;
static struct termios orig_termios;
static int raw_mode_enabled = 0;

static void enable_raw_mode(void) {
    if (raw_mode_enabled) return;
    tcgetattr(STDIN_FILENO, &orig_termios);
    struct termios raw = orig_termios;
    raw.c_lflag &= ~(ECHO | ICANON | ISIG);  // ISIG disabilita Ctrl+C/Z
    raw.c_cc[VMIN] = 0;
    raw.c_cc[VTIME] = 1;
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);
    raw_mode_enabled = 1;
}

static void disable_raw_mode(void) {
    if (!raw_mode_enabled) return;
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &orig_termios);
    raw_mode_enabled = 0;
}

static void cleanup_and_exit(void) {
    disable_raw_mode();
    keep_running = 0;
    peer_shutdown_all();
    discovery_stop();
    printf("\n👋 Arrivederci!\n");
    exit(0);
}

void int_handler(int sig) {
    (void)sig;
    cleanup_and_exit();
}

void *tcp_server_thread(void *arg) {
    (void)arg;
    
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) return NULL;
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TCP_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    
    int reuse = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(listen_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        closesocket(listen_sock);
        return NULL;
    }
    
    listen(listen_sock, 10);

    while (keep_running) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        SOCKET client = accept(listen_sock, (struct sockaddr*)&client_addr, &len);
        if (client < 0) {
            if (!keep_running) break;
            continue;
        }

        char ip[16];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));

        pthread_mutex_lock(&list_mutex);
        peers = realloc(peers, (peer_count+1)*sizeof(peer_t));
        snprintf(peers[peer_count].ip, sizeof(peers[peer_count].ip), "%s", ip);
        peers[peer_count].port = ntohs(client_addr.sin_port);
        peers[peer_count].sock = client;
        peers[peer_count].connected = 1;
        snprintf(peers[peer_count].name, sizeof(peers[peer_count].name), "Unknown");
        peer_count++;
        pthread_mutex_unlock(&list_mutex);

        pthread_t tid;
        SOCKET *sock_arg = malloc(sizeof(SOCKET));
        *sock_arg = client;
        pthread_create(&tid, NULL, transfer_receive_loop, sock_arg);
        pthread_detach(tid);
    }
    
    closesocket(listen_sock);
    return NULL;
}

static int kbhit(void) {
    struct timeval tv = {0, 0};
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(STDIN_FILENO, &fds);
    return select(STDIN_FILENO+1, &fds, NULL, NULL, &tv) > 0;
}

static int getch(void) {
    char c;
    if (read(STDIN_FILENO, &c, 1) == 1) return c;
    return -1;
}

static void clear_screen(void) {
    printf("\033[2J\033[H");
    fflush(stdout);
}

static void draw_menu(void) {
    clear_screen();
    printf("╔══════════════════════════════════════════════╗\n");
    printf("║     P2P Chat & File Transfer                ║\n");
    printf("╠══════════════════════════════════════════════╣\n");
    
    for (int i = 0; i < MENU_ITEMS; i++) {
        if (i == selected)
            printf("║ \033[7m %c - %-40s \033[0m║\n", main_menu[i].shortcut, main_menu[i].label);
        else
            printf("║   %c - %-40s ║\n", main_menu[i].shortcut, main_menu[i].label);
    }
    
    printf("╚══════════════════════════════════════════════╝\n");
    printf("\n↑↓ naviga  ENTER seleziona  Q esci  P peer\n");
    fflush(stdout);
}

static int select_peer_interactive(void) {
    pthread_mutex_lock(&list_mutex);
    int count = peer_count;
    pthread_mutex_unlock(&list_mutex);
    
    if (count == 0) {
        printf("\n⚠️  Nessun peer connesso!\n");
        fflush(stdout);
        sleep(1);
        return -1;
    }
    
    int sel = 0;
    
    while (keep_running) {
        clear_screen();
        printf("┌─ Seleziona peer (ESC per annullare) ────────┐\n");
        
        pthread_mutex_lock(&list_mutex);
        for (int i = 0; i < peer_count; i++) {
            if (i == sel)
                printf("│ \033[7m [%d] %-20s @ %-15s \033[0m│\n", 
                       i, peers[i].name, peers[i].ip);
            else
                printf("│  [%d] %-20s @ %-15s │\n", 
                       i, peers[i].name, peers[i].ip);
        }
        pthread_mutex_unlock(&list_mutex);
        
        printf("└──────────────────────────────────────────────┘\n");
        fflush(stdout);
        
        while (!kbhit() && keep_running) {
            usleep(10000); // 10ms
        }
        if (!keep_running) return -1;
        
        int c = getch();
        if (c == -1) continue;
        
        if (c == '\033') {
            if (kbhit()) {
                c = getch();
                if (c == '[' && kbhit()) {
                    c = getch();
                    if (c == 'A' && sel > 0) sel--;
                    else if (c == 'B' && sel < count-1) sel++;
                }
            } else {
                return -1; // ESC
            }
        } else if (c == '\n' || c == '\r') {
            return sel;
        } else if (c == 'q' || c == 'Q') {
            return -1;
        }
    }
    
    return -1;
}

static void read_line(char *buf, int size) {
    disable_raw_mode();
    printf("\n> ");
    fflush(stdout);
    if (!fgets(buf, size, stdin)) buf[0] = '\0';
    buf[strcspn(buf, "\n")] = '\0';
    enable_raw_mode();
}

static void run_interactive_menu(void) {
    char input[2048];
    
    while (keep_running) {
        draw_menu();
        
        while (!kbhit() && keep_running) {
            usleep(10000);
        }
        if (!keep_running) return;
        
        int c = getch();
        if (c == -1) continue;
        
        // ESC sequence
        if (c == '\033') {
            if (kbhit()) {
                c = getch();
                if (c == '[' && kbhit()) {
                    c = getch();
                    if (c == 'A' && selected > 0) selected--;
                    else if (c == 'B' && selected < MENU_ITEMS-1) selected++;
                }
            }
            continue;
        }
        
        // ENTER
        if (c == '\n' || c == '\r') {
            switch (selected) {
                case 0: // Mostra peer
                    clear_screen();
                    peer_list_print();
                    printf("\nPremi un tasto per continuare...");
                    fflush(stdout);
                    while (!kbhit() && keep_running) usleep(10000);
                    if (keep_running) getch();
                    break;
                    
                case 1: { // Invia messaggio
                    int idx = select_peer_interactive();
                    if (idx >= 0 && keep_running) {
                        pthread_mutex_lock(&list_mutex);
                        char name[32];
                        snprintf(name, sizeof(name), "%s", peers[idx].name);
                        pthread_mutex_unlock(&list_mutex);
                        
                        disable_raw_mode();
                        printf("\nMessaggio per %s: ", name);
                        fflush(stdout);
                        if (!fgets(input, sizeof(input), stdin)) input[0] = '\0';
                        input[strcspn(input, "\n")] = '\0';
                        enable_raw_mode();
                        
                        if (strlen(input) > 0) {
                            peer_send_message_to(idx, input);
                            printf("✅ Inviato!\n");
                            fflush(stdout);
                            sleep(1);
                        }
                    }
                    break;
                }
                    
                case 2: { // Invia file
                    int idx = select_peer_interactive();
                    if (idx >= 0 && keep_running) {
                        pthread_mutex_lock(&list_mutex);
                        char name[32];
                        snprintf(name, sizeof(name), "%s", peers[idx].name);
                        pthread_mutex_unlock(&list_mutex);
                        
                        disable_raw_mode();
                        printf("\nFile per %s: ", name);
                        fflush(stdout);
                        if (!fgets(input, sizeof(input), stdin)) input[0] = '\0';
                        input[strcspn(input, "\n")] = '\0';
                        enable_raw_mode();
                        
                        if (strlen(input) > 0) {
                            if (peer_send_file_to(idx, input) == 0)
                                printf("✅ File inviato!\n");
                            else
                                printf("❌ Errore!\n");
                            fflush(stdout);
                            sleep(1);
                        }
                    }
                    break;
                }
                    
                case 3: // Broadcast messaggio
                    read_line(input, sizeof(input));
                    if (strlen(input) > 0) {
                        peer_broadcast_message(input);
                        printf("✅ Inviato a tutti!\n");
                        fflush(stdout);
                        sleep(1);
                    }
                    break;
                    
                case 4: // Broadcast file
                    read_line(input, sizeof(input));
                    if (strlen(input) > 0) {
                        peer_broadcast_file(input);
                        printf("✅ File inviato a tutti!\n");
                        fflush(stdout);
                        sleep(1);
                    }
                    break;
                    
                case 5: // Esci
                    keep_running = 0;
                    return;
            }
        }
        // Tasti rapidi
        else if (c == 'q' || c == 'Q') {
            keep_running = 0;
            return;
        }
        else if (c == 'p' || c == 'P') {
            clear_screen();
            peer_list_print();
            printf("\nPremi un tasto...");
            fflush(stdout);
            while (!kbhit() && keep_running) usleep(10000);
            if (keep_running) getch();
        }
    }
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;
    
    char my_name[64];
    if (gethostname(my_name, sizeof(my_name)) != 0) {
        snprintf(my_name, sizeof(my_name), "Peer-%d", getpid());
    }
    my_name[sizeof(my_name)-1] = '\0';
    
    printf("Nome host: %s\nAvvio...\n", my_name);

    // Registra handler per SIGINT e SIGTERM
    signal(SIGINT, int_handler);
    signal(SIGTERM, int_handler);

    // Inizializza
    peer_init();
    discovery_start(my_name, TCP_PORT);

    pthread_t server_tid;
    pthread_create(&server_tid, NULL, tcp_server_thread, NULL);
    sleep(1);

    // Modalità interattiva
    enable_raw_mode();
    run_interactive_menu();
    disable_raw_mode();

    // Pulizia
    keep_running = 0;
    peer_shutdown_all();
    discovery_stop();
    pthread_join(server_tid, NULL);

    printf("\n👋 Arrivederci!\n");
    return 0;
}