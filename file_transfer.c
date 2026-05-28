#include "file_transfer.h"
#include "peer_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PROTO_MAGIC     0x50325000
#define TYPE_TEXT       0x01
#define TYPE_FILE_REQ   0x02
#define TYPE_FILE_DATA  0x03
#define TYPE_FILE_END   0x04
#define TYPE_QUIT       0xFF
#define CHUNK_SIZE      8192

static int send_msg(SOCKET sock, uint8_t type, const void *payload, uint32_t len) {
    uint32_t magic = htonl(PROTO_MAGIC);
    uint32_t netlen = htonl(len);
    uint8_t hdr[9];
    memcpy(hdr, &magic, 4);
    hdr[4] = type;
    memcpy(hdr+5, &netlen, 4);
    if (send(sock, (const char*)hdr, 9, 0) != 9) return -1;
    if (len > 0 && send(sock, (const char*)payload, len, 0) != (int)len) return -1;
    return 0;
}

static int recv_msg(SOCKET sock, uint8_t *type, void **payload, uint32_t *len) {
    uint8_t hdr[9];
    int total = 0;
    while (total < 9) {
        int r = recv(sock, (char*)hdr + total, 9 - total, 0);
        if (r <= 0) return -1;
        total += r;
    }
    uint32_t magic;
    memcpy(&magic, hdr, 4);
    if (ntohl(magic) != PROTO_MAGIC) return -1;
    *type = hdr[4];
    memcpy(len, hdr+5, 4);
    *len = ntohl(*len);
    if (*len > 0) {
        *payload = malloc(*len);
        if (!*payload) return -1;
        uint32_t received = 0;
        while (received < *len) {
            int r = recv(sock, (char*)*payload + received, *len - received, 0);
            if (r <= 0) { free(*payload); return -1; }
            received += r;
        }
    } else {
        *payload = NULL;
    }
    return 0;
}

int transfer_send_message(SOCKET sock, const char *text) {
    return send_msg(sock, TYPE_TEXT, text, strlen(text));
}

int transfer_send_file(SOCKET sock, const char *filename) {
    FILE *f = fopen(filename, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    rewind(f);

    uint16_t name_len = strlen(filename);
    uint8_t *req = malloc(2 + name_len + 8);
    memcpy(req, &name_len, 2);
    memcpy(req+2, filename, name_len);
    uint64_t fsize64 = (uint64_t)fsize;
    memcpy(req+2+name_len, &fsize64, 8);
    if (send_msg(sock, TYPE_FILE_REQ, req, 2+name_len+8) < 0) {
        free(req); fclose(f); return -1;
    }
    free(req);

    uint8_t chunk[CHUNK_SIZE];
    size_t n;
    while ((n = fread(chunk, 1, CHUNK_SIZE, f)) > 0) {
        if (send_msg(sock, TYPE_FILE_DATA, chunk, n) < 0) { fclose(f); return -1; }
    }
    send_msg(sock, TYPE_FILE_END, NULL, 0);
    fclose(f);
    return 0;
}

int transfer_send_quit(SOCKET sock) {
    return send_msg(sock, TYPE_QUIT, NULL, 0);
}

void *transfer_receive_loop(void *arg) {
    SOCKET sock = *(SOCKET*)arg;
    free(arg);

    while (1) {
        uint8_t type;
        void *payload = NULL;
        uint32_t plen;
        if (recv_msg(sock, &type, &payload, &plen) < 0) break;
        
        switch (type) {
            case TYPE_TEXT:
                printf("\n💬 [Messaggio]: %.*s\n> ", plen, (char*)payload);
                fflush(stdout);
                break;
                
            case TYPE_FILE_REQ: {
                uint16_t name_len;
                memcpy(&name_len, payload, 2);
                char filename[256];
                memcpy(filename, (uint8_t*)payload+2, name_len);
                filename[name_len] = '\0';
                uint64_t fsize;
                memcpy(&fsize, (uint8_t*)payload+2+name_len, 8);
                printf("\n📥 Ricezione file: %s (%llu byte)\n", filename, (unsigned long long)fsize);
                
                FILE *f = fopen(filename, "wb");
                if (!f) {
                    uint64_t remaining = fsize;
                    while (remaining > 0) {
                        uint8_t t2; void *p2; uint32_t len2;
                        if (recv_msg(sock, &t2, &p2, &len2) < 0) break;
                        if (t2 == TYPE_FILE_DATA && len2 <= remaining) remaining -= len2;
                        else if (t2 == TYPE_FILE_END) { free(p2); break; }
                        free(p2);
                    }
                } else {
                    uint64_t received = 0;
                    while (received < fsize) {
                        uint8_t t2; void *p2; uint32_t len2;
                        if (recv_msg(sock, &t2, &p2, &len2) < 0) break;
                        if (t2 == TYPE_FILE_DATA) {
                            fwrite(p2, 1, len2, f);
                            received += len2;
                        } else if (t2 == TYPE_FILE_END) {
                            free(p2);
                            break;
                        }
                        free(p2);
                    }
                    fclose(f);
                    printf("✅ File %s ricevuto (%llu/%llu byte)\n> ", 
                           filename, (unsigned long long)received, (unsigned long long)fsize);
                }
                fflush(stdout);
                break;
            }
            
            case TYPE_QUIT:
                printf("\n👋 Peer ha chiuso la connessione.\n> ");
                fflush(stdout);
                free(payload);
                goto cleanup;
        }
        free(payload);
    }
cleanup:
    peer_remove(sock);
    return NULL;
}