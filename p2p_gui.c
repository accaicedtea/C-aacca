#include "p2p_proto.h"
#include <gtk/gtk.h>
#include <stdio.h>
#include <string.h>

static GtkWidget *window;
static GtkWidget *listbox_peers;
static GtkWidget *textview_log;
static GtkTextBuffer *log_buffer;
static GtkWidget *entry_msg;
static GtkWidget *progress_bar;

static void log_to_gui(const char *line) {
    GtkTextIter end;
    gtk_text_buffer_get_end_iter(log_buffer, &end);
    gtk_text_buffer_insert(log_buffer, &end, line, -1);
    gtk_text_buffer_insert(log_buffer, &end, "\n", -1);
    GtkTextMark *mark = gtk_text_buffer_create_mark(log_buffer, NULL, &end, FALSE);
    gtk_text_view_scroll_to_mark(GTK_TEXT_VIEW(textview_log), mark, 0.0, TRUE, 0.0, 0.0);
}

/* ---------- aggiornamento periodico della lista peer e della barra ---------- */
static gboolean update_peer_list(gpointer data) {
    (void)data;
    GtkWidget *child = gtk_widget_get_first_child(listbox_peers);
    while (child) {
        GtkWidget *next = gtk_widget_get_next_sibling(child);
        gtk_list_box_remove(GTK_LIST_BOX(listbox_peers), child);
        child = next;
    }

    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s (%s)", g_data.peers[i].name, g_data.peers[i].ip);
            GtkWidget *label = gtk_label_new(buf);
            gtk_list_box_append(GTK_LIST_BOX(listbox_peers), label);
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
    return G_SOURCE_REMOVE;
}

static gboolean update_progress(gpointer data) {
    (void)data;
    if (g_data.xfer_active && g_data.xfer_total > 0) {
        double pct = (double)g_data.xfer_done / g_data.xfer_total;
        gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress_bar), pct);
        char text[64];
        snprintf(text, sizeof(text), "%.0f%%", pct * 100.0);
        gtk_progress_bar_set_text(GTK_PROGRESS_BAR(progress_bar), text);
        gtk_widget_set_visible(progress_bar, TRUE);
    } else {
        gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(progress_bar), 0.0);
        gtk_progress_bar_set_text(GTK_PROGRESS_BAR(progress_bar), "");
        gtk_widget_set_visible(progress_bar, FALSE);
    }
    return G_SOURCE_REMOVE;
}

static gboolean periodic(gpointer data) {
    (void)data;
    g_idle_add(update_peer_list, NULL);
    g_idle_add(update_progress, NULL);
    return G_SOURCE_CONTINUE;
}

/* ---------- ottenere il socket del peer selezionato ---------- */
static int get_selected_peer_socket(void) {
    GtkListBoxRow *row = gtk_list_box_get_selected_row(GTK_LIST_BOX(listbox_peers));
    if (!row) return -1;
    int idx = gtk_list_box_row_get_index(row);
    int sock = -1, cnt = 0;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++) {
        if (g_data.peers[i].active) {
            if (cnt == idx) {
                sock = g_data.peers[i].sock;
                break;
            }
            cnt++;
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
    return sock;
}

/* ---------- pulsanti messaggio ---------- */
static void on_send_clicked(GtkWidget *widget, gpointer user_data) {
    (void)widget;
    (void)user_data;
    const char *msg = gtk_editable_get_text(GTK_EDITABLE(entry_msg));
    if (!msg || strlen(msg) == 0) return;
    int sock = get_selected_peer_socket();
    if (sock < 0) {
        log_to_gui("No peer selected.");
        return;
    }
    if (p2p_send_text(sock, msg) == 0) {
        char buf[256];
        snprintf(buf, sizeof(buf), "Me: %s", msg);
        log_to_gui(buf);
    } else {
        log_to_gui("Send failed.");
    }
    gtk_editable_set_text(GTK_EDITABLE(entry_msg), "");
}

static void on_broadcast_clicked(GtkWidget *widget, gpointer user_data) {
    (void)widget;
    (void)user_data;
    const char *msg = gtk_editable_get_text(GTK_EDITABLE(entry_msg));
    if (!msg || strlen(msg) == 0) return;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < 32; i++)
        if (g_data.peers[i].active) p2p_send_text(g_data.peers[i].sock, msg);
    pthread_mutex_unlock(&g_data.peers_lock);
    char buf[256];
    snprintf(buf, sizeof(buf), "Me (broadcast): %s", msg);
    log_to_gui(buf);
    gtk_editable_set_text(GTK_EDITABLE(entry_msg), "");
}

/* ---------- file dialog (GtkFileDialog, moderno) ---------- */
static void file_dialog_response(GObject *source, GAsyncResult *result, gpointer user_data) {
    gboolean broadcast = GPOINTER_TO_INT(user_data);
    GtkFileDialog *dialog = GTK_FILE_DIALOG(source);
    GFile *file = gtk_file_dialog_open_finish(dialog, result, NULL);
    if (!file) return;

    char *path = g_file_get_path(file);
    g_object_unref(file);
    if (!path) return;

    if (broadcast) {
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < 32; i++)
            if (g_data.peers[i].active) p2p_send_file(g_data.peers[i].sock, path);
        pthread_mutex_unlock(&g_data.peers_lock);
        char buf[256];
        snprintf(buf, sizeof(buf), "Broadcasting file: %s", path);
        log_to_gui(buf);
    } else {
        int sock = get_selected_peer_socket();
        if (sock >= 0) {
            char buf[256];
            snprintf(buf, sizeof(buf), "Sending file: %s", path);
            log_to_gui(buf);
            p2p_send_file(sock, path);
        } else {
            log_to_gui("No peer selected.");
        }
    }
    g_free(path);
}

static void on_send_file_clicked(GtkWidget *widget, gpointer user_data) {
    (void)widget;
    (void)user_data;
    GtkFileDialog *dialog = gtk_file_dialog_new();
    gtk_file_dialog_open(dialog, GTK_WINDOW(window), NULL, file_dialog_response, GINT_TO_POINTER(0));
    g_object_unref(dialog);
}

static void on_broadcast_file_clicked(GtkWidget *widget, gpointer user_data) {
    (void)widget;
    (void)user_data;
    GtkFileDialog *dialog = gtk_file_dialog_new();
    gtk_file_dialog_open(dialog, GTK_WINDOW(window), NULL, file_dialog_response, GINT_TO_POINTER(1));
    g_object_unref(dialog);
}

/* ---------- finestra di connessione manuale ---------- */
static void connect_dialog_response(GtkWidget *dialog, int response, GtkWidget *entry) {
    if (response == GTK_RESPONSE_ACCEPT) {
        const char *ip_port = gtk_editable_get_text(GTK_EDITABLE(entry));
        char ip[INET_ADDRSTRLEN] = {0};
        int port = g_data.myport;
        const char *colon = strchr(ip_port, ':');
        if (colon) {
            size_t len = colon - ip_port;
            if (len >= sizeof(ip)) len = sizeof(ip) - 1;
            memcpy(ip, ip_port, len);
            ip[len] = '\0';
            port = atoi(colon + 1);
        } else {
            strncpy(ip, ip_port, sizeof(ip) - 1);
        }
        if (strlen(ip) > 0) {
            p2p_connect(ip, port);
            char buf[256];
            snprintf(buf, sizeof(buf), "Connecting to %s:%d ...", ip, port);
            log_to_gui(buf);
        }
    }
    gtk_window_destroy(GTK_WINDOW(dialog));
}

static void on_connect_clicked(GtkWidget *widget, gpointer user_data) {
    (void)widget;
    (void)user_data;
    GtkWidget *dialog = gtk_window_new();
    gtk_window_set_title(GTK_WINDOW(dialog), "Connect to peer");
    gtk_window_set_modal(GTK_WINDOW(dialog), TRUE);
    gtk_window_set_transient_for(GTK_WINDOW(dialog), GTK_WINDOW(window));

    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 5);
    gtk_window_set_child(GTK_WINDOW(dialog), box);

    GtkWidget *entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(entry), "IP:port (e.g. 192.168.42.2:9100)");
    gtk_box_append(GTK_BOX(box), entry);

    GtkWidget *btn_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 2);
    gtk_box_append(GTK_BOX(box), btn_box);

    GtkWidget *cancel = gtk_button_new_with_label("Cancel");
    g_signal_connect_swapped(cancel, "clicked", G_CALLBACK(gtk_window_destroy), dialog);
    gtk_box_append(GTK_BOX(btn_box), cancel);

    GtkWidget *ok = gtk_button_new_with_label("Connect");
    g_signal_connect(ok, "clicked", G_CALLBACK(connect_dialog_response), entry);
    gtk_box_append(GTK_BOX(btn_box), ok);

    gtk_window_present(GTK_WINDOW(dialog));
}

/* ---------- attivazione applicazione GTK ---------- */
static void activate(GtkApplication *app, gpointer user_data) {
    (void)user_data;
    window = gtk_application_window_new(app);
    gtk_window_set_title(GTK_WINDOW(window), "P2P Chat & File Transfer");
    gtk_window_set_default_size(GTK_WINDOW(window), 700, 500);

    GtkWidget *vbox = gtk_box_new(GTK_ORIENTATION_VERTICAL, 5);
    gtk_window_set_child(GTK_WINDOW(window), vbox);

    char info[128];
    snprintf(info, sizeof(info), "Name: %s  IP: %s  Port: %d", g_data.myname, g_data.myip, g_data.myport);
    gtk_box_append(GTK_BOX(vbox), gtk_label_new(info));

    /* peer list */
    GtkWidget *scrolled_peers = gtk_scrolled_window_new();
    gtk_scrolled_window_set_min_content_height(GTK_SCROLLED_WINDOW(scrolled_peers), 100);
    listbox_peers = gtk_list_box_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scrolled_peers), listbox_peers);
    gtk_box_append(GTK_BOX(vbox), scrolled_peers);

    /* log */
    GtkWidget *scrolled_log = gtk_scrolled_window_new();
    gtk_scrolled_window_set_min_content_height(GTK_SCROLLED_WINDOW(scrolled_log), 150);
    textview_log = gtk_text_view_new();
    gtk_text_view_set_editable(GTK_TEXT_VIEW(textview_log), FALSE);
    log_buffer = gtk_text_view_get_buffer(GTK_TEXT_VIEW(textview_log));
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scrolled_log), textview_log);
    gtk_box_append(GTK_BOX(vbox), scrolled_log);

    /* progress bar */
    progress_bar = gtk_progress_bar_new();
    gtk_progress_bar_set_show_text(GTK_PROGRESS_BAR(progress_bar), TRUE);
    gtk_widget_set_visible(progress_bar, FALSE);
    gtk_box_append(GTK_BOX(vbox), progress_bar);

    /* messaggio e pulsanti */
    GtkWidget *hbox = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 2);
    entry_msg = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(entry_msg), "Type message...");
    gtk_box_append(GTK_BOX(hbox), entry_msg);
    gtk_widget_set_hexpand(entry_msg, TRUE);

    GtkWidget *btn_send = gtk_button_new_with_label("Send");
    g_signal_connect(btn_send, "clicked", G_CALLBACK(on_send_clicked), NULL);
    gtk_box_append(GTK_BOX(hbox), btn_send);

    GtkWidget *btn_bcast = gtk_button_new_with_label("Broadcast");
    g_signal_connect(btn_bcast, "clicked", G_CALLBACK(on_broadcast_clicked), NULL);
    gtk_box_append(GTK_BOX(hbox), btn_bcast);

    gtk_box_append(GTK_BOX(vbox), hbox);

    /* pulsanti file e connetti */
    GtkWidget *hbox2 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 2);
    GtkWidget *btn_file = gtk_button_new_with_label("Send File...");
    g_signal_connect(btn_file, "clicked", G_CALLBACK(on_send_file_clicked), NULL);
    gtk_box_append(GTK_BOX(hbox2), btn_file);

    GtkWidget *btn_bfile = gtk_button_new_with_label("Broadcast File...");
    g_signal_connect(btn_bfile, "clicked", G_CALLBACK(on_broadcast_file_clicked), NULL);
    gtk_box_append(GTK_BOX(hbox2), btn_bfile);

    GtkWidget *btn_conn = gtk_button_new_with_label("Connect...");
    g_signal_connect(btn_conn, "clicked", G_CALLBACK(on_connect_clicked), NULL);
    gtk_box_append(GTK_BOX(hbox2), btn_conn);

    gtk_box_append(GTK_BOX(vbox), hbox2);

    gtk_window_present(GTK_WINDOW(window));

    /* aggiornamento periodico */
    g_timeout_add(1000, periodic, NULL);
}

int main(int argc, char **argv) {
    p2p_init(argc > 1 ? argv[1] : NULL,
             argc > 2 ? atoi(argv[2]) : 9100,
             argc > 3 ? argv[3] : NULL);
    p2p_start();

    GtkApplication *app = gtk_application_new("org.p2pchat.app", G_APPLICATION_DEFAULT_FLAGS);
    g_signal_connect(app, "activate", G_CALLBACK(activate), NULL);
    int status = g_application_run(G_APPLICATION(app), 0, NULL);
    p2p_stop();
    g_object_unref(app);
    return status;
}