#ifndef DISCOVERY_H
#define DISCOVERY_H

#define DISCOVERY_PORT 9000   // UDP broadcast port

// Avvia il thread di discovery (beacon + listener).
// tcp_port: porta su cui il peer ascolta le connessioni TCP.
void discovery_start(unsigned short tcp_port);

// Ferma il discovery e libera le risorse.
void discovery_stop(void);

#endif