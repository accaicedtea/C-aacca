#ifndef DISCOVERY_H
#define DISCOVERY_H

#define DISCOVERY_PORT 9001  // Porta UDP per discovery

// Avvia il discovery (beacon + listener)
// my_name: nome identificativo del peer
// tcp_port: porta TCP su cui si ascolta
void discovery_start(const char *my_name, unsigned short tcp_port);

// Ferma il discovery
void discovery_stop(void);

#endif