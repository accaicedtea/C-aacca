#ifndef FILE_TRANSFER_H
#define FILE_TRANSFER_H

#ifdef _WIN32
    #include <winsock2.h>
#else
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #define SOCKET int
    #define INVALID_SOCKET -1
    #define closesocket(s) close(s)
#endif

int transfer_send_message(SOCKET sock, const char *text);
int transfer_send_file(SOCKET sock, const char *filename);
void *transfer_receive_loop(void *arg);  // firma corretta per pthread
int transfer_send_quit(SOCKET sock);

#endif