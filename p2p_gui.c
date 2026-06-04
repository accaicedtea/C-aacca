/* ═══════════════════════════════════════════════════════════════════════════
 * gui.c — Modern GTK4 interface for P2P Chat & File Transfer
 *
 * Design decisions vs. original:
 *   • Dark slate theme with CSS (no GTK theme dependency)
 *   • Bubble-style chat log instead of a plain GtkTextView
 *   • Peer sidebar with online dot + IP:port
 *   • File-incoming: BLOCKING dialog on main thread (pthread_cond_wait)
 *     The receiver thread waits until the user clicks Accept or Decline.
 *     If the user accepts, a GtkFileDialog lets them pick the save path.
 *   • File-sending: dispatched to a dedicated pthread so the GUI stays
 *     responsive during long transfers.
 *   • All p2p callbacks reach GTK only via g_idle_add() — thread-safe.
 *
 * Requires: GTK 4.10+  (GtkFileDialog, gtk_paned_set_start/end_child)
 *           GTK 4.12+  (gtk_css_provider_load_from_string)
 *           → falls back to load_from_data for GTK 4.10/4.11 via #ifdef
 * ═══════════════════════════════════════════════════════════════════════════ */
#define _GNU_SOURCE
#include "p2p_proto.h"
#include <gtk/gtk.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 1 · CSS THEME
 * ───────────────────────────────────────────────────────────────────────── */
static const char *APP_CSS =

/* ── Base ── */
"window { background-color: #0f172a; }"
"* { -gtk-icon-shadow: none; }"

/* ── Header bar ── */
".app-header {"
"  background-color: #0a0f1e;"
"  border-bottom: 1px solid #1e293b;"
"  padding: 0 18px;"
"  min-height: 50px;"
"}"
".app-title {"
"  color: #f8fafc;"
"  font-size: 15px;"
"  font-weight: 700;"
"  letter-spacing: -0.3px;"
"}"
".app-info {"
"  color: #475569;"
"  font-size: 11px;"
"  font-family: monospace;"
"}"

/* ── Sidebar ── */
".sidebar {"
"  background-color: #0d1424;"
"  border-right: 1px solid #1e293b;"
"}"
".section-label {"
"  color: #334155;"
"  font-size: 10px;"
"  font-weight: 700;"
"  letter-spacing: 1.5px;"
"  padding: 14px 14px 6px;"
"  font-family: monospace;"
"}"
"row {"
"  padding: 0;"
"  background: transparent;"
"}"
"row:selected { background: transparent; }"
"row:selected .peer-card { background-color: #1e3a5f; }"
"row:hover .peer-card { background-color: #1e293b; }"
".peer-card {"
"  padding: 9px 12px;"
"  border-radius: 8px;"
"  margin: 2px 6px;"
"  transition: background-color 120ms ease;"
"}"
".dot-on { color: #22c55e; font-size: 9px; margin-right: 6px; }"
".peer-name { color: #e2e8f0; font-size: 13px; font-weight: 600; }"
".peer-addr { color: #475569; font-size: 11px; font-family: monospace; }"
".btn-connect {"
"  background-color: transparent;"
"  color: #38bdf8;"
"  border: 1px dashed #1e3a5f;"
"  border-radius: 8px;"
"  padding: 7px 0;"
"  margin: 6px 8px 10px;"
"  font-size: 12px;"
"  transition: all 120ms ease;"
"}"
".btn-connect:hover {"
"  background-color: #0f2035;"
"  border-color: #38bdf8;"
"}"

/* ── Chat header ── */
".chat-header {"
"  background-color: #0a0f1e;"
"  border-bottom: 1px solid #1e293b;"
"  padding: 10px 16px;"
"  min-height: 44px;"
"}"
".chat-peer-name { color: #f8fafc; font-size: 14px; font-weight: 600; }"

/* ── Messages area ── */
".messages-area { background-color: #0f172a; }"
".system-msg {"
"  color: #334155;"
"  font-size: 11px;"
"  font-style: italic;"
"  margin: 6px 0;"
"}"

/* ── Message bubbles ── */
".bubble-me {"
"  background-color: #1d4ed8;"
"  border-radius: 16px 16px 4px 16px;"
"  padding: 8px 13px;"
"  color: #eff6ff;"
"  font-size: 13px;"
"}"
".bubble-peer {"
"  background-color: #1e293b;"
"  border-radius: 16px 16px 16px 4px;"
"  padding: 8px 13px;"
"  color: #e2e8f0;"
"  font-size: 13px;"
"}"
".msg-sender { color: #38bdf8; font-size: 10px; font-weight: 700; margin-bottom: 2px; }"
".msg-time   { color: #334155; font-size: 10px; margin-top: 3px; }"

/* ── Input bar ── */
".input-bar {"
"  background-color: #0a0f1e;"
"  border-top: 1px solid #1e293b;"
"  padding: 10px 14px;"
"}"
".action-row { margin-bottom: 6px; }"
".btn-tool {"
"  background-color: #1e293b;"
"  color: #94a3b8;"
"  border: 1px solid #334155;"
"  border-radius: 7px;"
"  padding: 5px 11px;"
"  font-size: 11px;"
"  transition: all 100ms ease;"
"}"
".btn-tool:hover { background-color: #334155; color: #f1f5f9; border-color: #475569; }"
"entry.msg-input {"
"  background-color: #1e293b;"
"  color: #f1f5f9;"
"  border: 1px solid #334155;"
"  border-radius: 22px;"
"  padding: 8px 16px;"
"  font-size: 13px;"
"  caret-color: #38bdf8;"
"}"
"entry.msg-input:focus { border-color: #1d4ed8; }"
".btn-send {"
"  background-color: #1d4ed8;"
"  color: white;"
"  border: none;"
"  border-radius: 50%;"
"  min-width: 40px;"
"  min-height: 40px;"
"  font-size: 18px;"
"  padding: 0;"
"  margin-left: 6px;"
"  transition: background-color 100ms ease;"
"}"
".btn-send:hover { background-color: #2563eb; }"
".btn-send:active { background-color: #1e40af; }"

/* ── Transfer bar ── */
".xfer-bar {"
"  background-color: #060c18;"
"  border-top: 1px solid #1e293b;"
"  padding: 5px 16px;"
"  min-height: 28px;"
"}"
".xfer-label { color: #64748b; font-size: 11px; font-family: monospace; min-width: 260px; }"
"progressbar.xfer-pg trough  { background-color: #1e293b; border-radius: 3px; min-height: 4px; border: none; }"
"progressbar.xfer-pg progress { background: linear-gradient(90deg,#1d4ed8,#0ea5e9); border-radius: 3px; }"

/* ── File-incoming dialog ── */
".fdlg-card {"
"  background-color: #1e293b;"
"  border-radius: 14px;"
"  border: 1px solid #334155;"
"  padding: 28px 32px 24px;"
"}"
".fdlg-icon  { font-size: 42px; }"
".fdlg-title { color: #f8fafc; font-size: 16px; font-weight: 700; }"
".fdlg-info  { color: #94a3b8; font-size: 12px; line-height: 1.6; }"
".btn-accept {"
"  background-color: #22c55e;"
"  color: white;"
"  border: none;"
"  border-radius: 8px;"
"  padding: 9px 22px;"
"  font-weight: 700;"
"  font-size: 13px;"
"  transition: background-color 100ms ease;"
"}"
".btn-accept:hover { background-color: #16a34a; }"
".btn-decline {"
"  background-color: #334155;"
"  color: #94a3b8;"
"  border: none;"
"  border-radius: 8px;"
"  padding: 9px 22px;"
"  font-size: 13px;"
"  transition: all 100ms ease;"
"}"
".btn-decline:hover { background-color: #475569; color: #f1f5f9; }"

/* ── Connect dialog ── */
".conn-card { background-color: #1e293b; border-radius: 12px; padding: 22px 24px 18px; }"
".conn-title { color: #f8fafc; font-size: 14px; font-weight: 600; margin-bottom: 6px; }"
"entry.conn-input {"
"  background-color: #0f172a;"
"  color: #f1f5f9;"
"  border: 1px solid #334155;"
"  border-radius: 8px;"
"  padding: 8px 12px;"
"  font-family: monospace;"
"  font-size: 13px;"
"}"
"entry.conn-input:focus { border-color: #1d4ed8; }"
;

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 2 · APPLICATION WIDGET STATE
 * ───────────────────────────────────────────────────────────────────────── */
static GtkWindow *main_window;
static GtkWidget *sidebar_list;
static GtkWidget *chat_peer_label;
static GtkWidget *messages_box;
static GtkWidget *messages_scroll;
static GtkWidget *entry_msg;
static GtkWidget *xfer_bar;
static GtkWidget *xfer_label;
static GtkWidget *xfer_pg;

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 3 · IDLE-DISPATCH DATA STRUCTURES
 * ───────────────────────────────────────────────────────────────────────── */
typedef struct { char peer[NAME_LEN]; char msg[4096]; } MsgData;

/* Blocking file-incoming request (lives on the heap, shared with GTK callbacks) */
typedef struct {
    char     peer_name[NAME_LEN];
    char     filename[512];
    uint64_t size;
    char    *result_path;       /* malloc'd path or NULL → reject */
    pthread_mutex_t mtx;
    pthread_cond_t  cond;
    int      done;
} FileReq;

/* Context passed through the GTK save-dialog async chain */
typedef struct {
    FileReq   *req;
    GtkWindow *parent;
} SaveCtx;

/* Context for the fire-and-forget file-send thread */
typedef struct { int sock; char path[4096]; int broadcast; } FileSendTask;

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 4 · MESSAGE / PEER DISPLAY (main-thread helpers)
 * ───────────────────────────────────────────────────────────────────────── */

static gboolean do_scroll_bottom(gpointer ud) {
    (void)ud;
    GtkAdjustment *adj =
        gtk_scrolled_window_get_vadjustment(GTK_SCROLLED_WINDOW(messages_scroll));
    gtk_adjustment_set_value(adj,
        gtk_adjustment_get_upper(adj) - gtk_adjustment_get_page_size(adj));
    return G_SOURCE_REMOVE;
}

/* Append a chat bubble.  is_me: 1=mine, 0=peer, -1=system */
static void append_message(const char *sender, const char *text, int is_me) {
    if (is_me < 0) {
        /* System / event line — centered grey italic */
        GtkWidget *lbl = gtk_label_new(text);
        gtk_label_set_xalign(GTK_LABEL(lbl), 0.5f);
        gtk_widget_add_css_class(lbl, "system-msg");
        gtk_widget_set_margin_top(lbl, 4);
        gtk_widget_set_margin_bottom(lbl, 4);
        gtk_box_append(GTK_BOX(messages_box), lbl);
        g_idle_add(do_scroll_bottom, NULL);
        return;
    }

    /* Outer row determines alignment */
    GtkWidget *outer = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *inner = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);

    /* Sender label (only for peer messages) */
    if (!is_me) {
        GtkWidget *sl = gtk_label_new(sender);
        gtk_label_set_xalign(GTK_LABEL(sl), 0.0f);
        gtk_widget_add_css_class(sl, "msg-sender");
        gtk_box_append(GTK_BOX(inner), sl);
    }

    /* Bubble */
    GtkWidget *bubble = gtk_label_new(text);
    gtk_label_set_wrap(GTK_LABEL(bubble), TRUE);
    gtk_label_set_wrap_mode(GTK_LABEL(bubble), PANGO_WRAP_WORD_CHAR);
    gtk_label_set_xalign(GTK_LABEL(bubble), 0.0f);
    gtk_label_set_selectable(GTK_LABEL(bubble), TRUE);
    gtk_widget_add_css_class(bubble, is_me ? "bubble-me" : "bubble-peer");
    gtk_widget_set_hexpand(bubble, FALSE);
    gtk_widget_set_halign(bubble, GTK_ALIGN_START);
    gtk_box_append(GTK_BOX(inner), bubble);

    /* Timestamp */
    time_t now = time(NULL);
    char ts[8];
    strftime(ts, sizeof(ts), "%H:%M", localtime(&now));
    GtkWidget *tl = gtk_label_new(ts);
    gtk_widget_add_css_class(tl, "msg-time");
    gtk_label_set_xalign(GTK_LABEL(tl), is_me ? 1.0f : 0.0f);
    gtk_box_append(GTK_BOX(inner), tl);

    /* Spacer pushes bubble to correct side */
    GtkWidget *spacer = gtk_label_new("");
    gtk_widget_set_hexpand(spacer, TRUE);

    if (is_me) {
        gtk_widget_set_margin_top(inner, 3);
        gtk_widget_set_margin_bottom(inner, 3);
        gtk_widget_set_margin_start(inner, 60);
        gtk_widget_set_margin_end(inner, 10);
        gtk_widget_set_halign(inner, GTK_ALIGN_END);
        gtk_box_append(GTK_BOX(outer), spacer);
        gtk_box_append(GTK_BOX(outer), inner);
    } else {
        gtk_widget_set_margin_top(inner, 3);
        gtk_widget_set_margin_bottom(inner, 3);
        gtk_widget_set_margin_start(inner, 10);
        gtk_widget_set_margin_end(inner, 60);
        gtk_widget_set_halign(inner, GTK_ALIGN_START);
        gtk_box_append(GTK_BOX(outer), inner);
        gtk_box_append(GTK_BOX(outer), spacer);
    }

    gtk_box_append(GTK_BOX(messages_box), outer);
    g_idle_add(do_scroll_bottom, NULL);
}

/* Rebuild the peer list (runs on main thread) */
static gboolean idle_rebuild_peers(gpointer ud) {
    (void)ud;
    GtkWidget *child = gtk_widget_get_first_child(sidebar_list);
    while (child) {
        GtkWidget *next = gtk_widget_get_next_sibling(child);
        gtk_list_box_remove(GTK_LIST_BOX(sidebar_list), child);
        child = next;
    }
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!g_data.peers[i].active) continue;

        GtkWidget *card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 3);
        gtk_widget_add_css_class(card, "peer-card");

        GtkWidget *top = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
        GtkWidget *dot = gtk_label_new("●");
        gtk_widget_add_css_class(dot, "dot-on");
        GtkWidget *nm = gtk_label_new(g_data.peers[i].name);
        gtk_widget_add_css_class(nm, "peer-name");
        gtk_label_set_xalign(GTK_LABEL(nm), 0.0f);
        gtk_box_append(GTK_BOX(top), dot);
        gtk_box_append(GTK_BOX(top), nm);
        gtk_box_append(GTK_BOX(card), top);

        char addr[64];
        snprintf(addr, sizeof(addr), "%s:%d",
                 g_data.peers[i].ip, g_data.peers[i].port);
        GtkWidget *ip = gtk_label_new(addr);
        gtk_widget_add_css_class(ip, "peer-addr");
        gtk_label_set_xalign(GTK_LABEL(ip), 0.0f);
        gtk_box_append(GTK_BOX(card), ip);

        gtk_list_box_append(GTK_LIST_BOX(sidebar_list), card);
    }
    pthread_mutex_unlock(&g_data.peers_lock);
    return G_SOURCE_REMOVE;
}

/* Update transfer progress bar (runs on main thread) */
static gboolean idle_update_xfer(gpointer ud) {
    (void)ud;
    if (g_data.xfer_active && g_data.xfer_total > 0) {
        double pct = (double)g_data.xfer_done / (double)g_data.xfer_total;
        gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(xfer_pg), pct);
        double mb_done  = (double)g_data.xfer_done  / 1048576.0;
        double mb_total = (double)g_data.xfer_total / 1048576.0;
        char label[300];
        snprintf(label, sizeof(label), "%c  %s   %.1f / %.1f MB  (%.0f%%)",
                 g_data.xfer_direction == 'S' ? (char)0x2191 : (char)0x2193,
                 g_data.xfer_name, mb_done, mb_total, pct * 100.0);
        gtk_label_set_text(GTK_LABEL(xfer_label), label);
        gtk_widget_set_visible(xfer_bar, TRUE);
    } else {
        gtk_widget_set_visible(xfer_bar, FALSE);
    }
    return G_SOURCE_REMOVE;
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 5 · BACKGROUND-THREAD CALLBACKS  (all → g_idle_add)
 * ───────────────────────────────────────────────────────────────────────── */

static gboolean _idle_msg(gpointer ptr) {
    MsgData *m = ptr;
    append_message(m->peer, m->msg, 0);
    free(m);
    return G_SOURCE_REMOVE;
}

static void cb_on_message(const char *peer, const char *msg, void *ud) {
    (void)ud;
    MsgData *m = malloc(sizeof(MsgData));
    strncpy(m->peer, peer, NAME_LEN - 1);
    strncpy(m->msg,  msg,  sizeof(m->msg) - 1);
    m->peer[NAME_LEN - 1] = m->msg[sizeof(m->msg) - 1] = '\0';
    g_idle_add(_idle_msg, m);
}

static gboolean _idle_sys(gpointer ptr) {
    append_message("", (char *)ptr, -1);
    free(ptr);
    return G_SOURCE_REMOVE;
}

static void cb_on_log(const char *text, void *ud) {
    (void)ud;
    g_idle_add(_idle_sys, strdup(text));
}

static void cb_on_peer_event(int connected, const char *peer,
                              const char *ip, void *ud) {
    (void)ud;
    char *msg = NULL;
    asprintf(&msg, connected ? "⟢ %s (%s) connected" : "⟢ %s disconnected",
             peer, ip);
    if (msg) g_idle_add(_idle_sys, msg);
    g_idle_add(idle_rebuild_peers, NULL);
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 6 · FILE-INCOMING BLOCKING DIALOG
 *
 *  The receiver thread calls cb_on_file_incoming() which schedules the
 *  GTK dialog via g_idle_add(), then sleeps on a condition variable.
 *  The GTK callbacks (Decline button / save-dialog completion) signal
 *  the condition and store the chosen path.  The receiver thread then
 *  wakes up and returns to the protocol engine.
 * ───────────────────────────────────────────────────────────────────────── */

/* Called when the GtkFileDialog save completes (or is cancelled) */
static void on_save_chosen(GObject *src, GAsyncResult *res, gpointer ptr) {
    SaveCtx *ctx = ptr;
    GtkFileDialog *dlg = GTK_FILE_DIALOG(src);
    GFile *f = gtk_file_dialog_save_finish(dlg, res, NULL);
    if (f) {
        char *p = g_file_get_path(f);
        ctx->req->result_path = p ? strdup(p) : NULL;
        if (p) g_free(p);
        g_object_unref(f);
    } else {
        ctx->req->result_path = NULL;  /* user cancelled save dialog → reject */
    }
    pthread_mutex_lock(&ctx->req->mtx);
    ctx->req->done = 1;
    pthread_cond_signal(&ctx->req->cond);
    pthread_mutex_unlock(&ctx->req->mtx);
    free(ctx);
}

/* "Accept & Save…" button */
static void on_accept_clicked(GtkWidget *btn, gpointer ptr) {
    SaveCtx *ctx = ptr;
    /* Close the confirm window first */
    GtkWidget *win = gtk_widget_get_ancestor(btn, GTK_TYPE_WINDOW);
    if (win) gtk_window_destroy(GTK_WINDOW(win));
    /* Open a save dialog — async, will call on_save_chosen */
    GtkFileDialog *sd = gtk_file_dialog_new();
    gtk_file_dialog_set_initial_name(sd, ctx->req->filename);
    gtk_file_dialog_save(sd, ctx->parent, NULL, on_save_chosen, ctx);
    g_object_unref(sd);
}

/* "Decline" button */
static void on_decline_clicked(GtkWidget *btn, gpointer ptr) {
    SaveCtx *ctx = ptr;
    GtkWidget *win = gtk_widget_get_ancestor(btn, GTK_TYPE_WINDOW);
    if (win) gtk_window_destroy(GTK_WINDOW(win));
    ctx->req->result_path = NULL;
    pthread_mutex_lock(&ctx->req->mtx);
    ctx->req->done = 1;
    pthread_cond_signal(&ctx->req->cond);
    pthread_mutex_unlock(&ctx->req->mtx);
    free(ctx);
}

/* Runs on main thread via g_idle_add — builds the confirmation dialog */
static gboolean show_incoming_dialog(gpointer ptr) {
    FileReq *req = ptr;

    GtkWidget *dlg = gtk_window_new();
    gtk_window_set_title(GTK_WINDOW(dlg), "Incoming File");
    gtk_window_set_modal(GTK_WINDOW(dlg), TRUE);
    gtk_window_set_transient_for(GTK_WINDOW(dlg), main_window);
    gtk_window_set_resizable(GTK_WINDOW(dlg), FALSE);
    gtk_window_set_deletable(GTK_WINDOW(dlg), FALSE); /* force Accept/Decline */

    /* Card */
    GtkWidget *card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 14);
    gtk_widget_add_css_class(card, "fdlg-card");
    gtk_widget_set_margin_top(card, 12);
    gtk_widget_set_margin_bottom(card, 12);
    gtk_widget_set_margin_start(card, 12);
    gtk_widget_set_margin_end(card, 12);
    gtk_window_set_child(GTK_WINDOW(dlg), card);

    /* Icon */
    GtkWidget *icon = gtk_label_new("📥");
    gtk_widget_add_css_class(icon, "fdlg-icon");
    gtk_label_set_xalign(GTK_LABEL(icon), 0.5f);
    gtk_box_append(GTK_BOX(card), icon);

    /* Title */
    GtkWidget *title = gtk_label_new("Incoming File Transfer");
    gtk_widget_add_css_class(title, "fdlg-title");
    gtk_box_append(GTK_BOX(card), title);

    /* Info */
    char info[512];
    double sz = (double)req->size;
    const char *unit;
    if (sz >= 1073741824.0)    { sz /= 1073741824.0; unit = "GB"; }
    else if (sz >= 1048576.0)  { sz /= 1048576.0;    unit = "MB"; }
    else if (sz >= 1024.0)     { sz /= 1024.0;        unit = "KB"; }
    else                        { unit = "B"; }

    snprintf(info, sizeof(info),
             "From:  %s\nFile:  %s\nSize:  %.2f %s",
             req->peer_name, req->filename, sz, unit);
    GtkWidget *info_lbl = gtk_label_new(info);
    gtk_widget_add_css_class(info_lbl, "fdlg-info");
    gtk_label_set_justify(GTK_LABEL(info_lbl), GTK_JUSTIFY_LEFT);
    gtk_label_set_xalign(GTK_LABEL(info_lbl), 0.0f);
    gtk_box_append(GTK_BOX(card), info_lbl);

    /* Button row */
    GtkWidget *btn_row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    gtk_widget_set_halign(btn_row, GTK_ALIGN_END);
    gtk_box_append(GTK_BOX(card), btn_row);

    SaveCtx *ctx = malloc(sizeof(SaveCtx));
    ctx->req    = req;
    ctx->parent = main_window;

    GtkWidget *b_decline = gtk_button_new_with_label("Decline");
    gtk_widget_add_css_class(b_decline, "btn-decline");
    g_signal_connect(b_decline, "clicked", G_CALLBACK(on_decline_clicked), ctx);
    gtk_box_append(GTK_BOX(btn_row), b_decline);

    GtkWidget *b_accept = gtk_button_new_with_label("Accept & Save…");
    gtk_widget_add_css_class(b_accept, "btn-accept");
    g_signal_connect(b_accept, "clicked", G_CALLBACK(on_accept_clicked), ctx);
    gtk_box_append(GTK_BOX(btn_row), b_accept);

    gtk_window_present(GTK_WINDOW(dlg));
    return G_SOURCE_REMOVE;
}

/* Called from receiver thread — BLOCKS until user decides */
static char *cb_on_file_incoming(const char *peer_name, const char *filename,
                                  uint64_t size, void *ud) {
    (void)ud;
    FileReq *req = calloc(1, sizeof(FileReq));
    strncpy(req->peer_name, peer_name, NAME_LEN - 1);
    strncpy(req->filename,  filename,  sizeof(req->filename) - 1);
    req->size = size;
    pthread_mutex_init(&req->mtx, NULL);
    pthread_cond_init(&req->cond, NULL);

    g_idle_add(show_incoming_dialog, req);   /* schedule GTK dialog */

    pthread_mutex_lock(&req->mtx);
    while (!req->done)
        pthread_cond_wait(&req->cond, &req->mtx);
    pthread_mutex_unlock(&req->mtx);

    char *path = req->result_path;
    pthread_mutex_destroy(&req->mtx);
    pthread_cond_destroy(&req->cond);
    free(req);
    return path;   /* caller frees; NULL = reject */
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 7 · UTILITY: get selected peer socket
 * ───────────────────────────────────────────────────────────────────────── */
static int get_selected_sock(void) {
    GtkListBoxRow *row = gtk_list_box_get_selected_row(GTK_LIST_BOX(sidebar_list));
    if (!row) return -1;
    int idx = gtk_list_box_row_get_index(row);
    int sock = -1, cnt = 0;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active) {
            if (cnt++ == idx) { sock = g_data.peers[i].sock; break; }
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
    return sock;
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 8 · FILE SEND THREAD (keep GUI responsive during transfer)
 * ───────────────────────────────────────────────────────────────────────── */
static void *file_send_thread(void *ptr) {
    FileSendTask *t = ptr;
    if (t->broadcast) {
        pthread_mutex_lock(&g_data.peers_lock);
        for (int i = 0; i < MAX_PEERS; i++) {
            if (g_data.peers[i].active)
                p2p_send_file(g_data.peers[i].sock, t->path);
        }
        pthread_mutex_unlock(&g_data.peers_lock);
    } else {
        p2p_send_file(t->sock, t->path);
    }
    free(t);
    return NULL;
}

static void dispatch_file_send(int sock, const char *path, int broadcast) {
    FileSendTask *t = malloc(sizeof(FileSendTask));
    t->sock      = sock;
    t->broadcast = broadcast;
    strncpy(t->path, path, sizeof(t->path) - 1);
    pthread_t thr;
    pthread_create(&thr, NULL, file_send_thread, t);
    pthread_detach(thr);
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 9 · BUTTON / EVENT HANDLERS
 * ───────────────────────────────────────────────────────────────────────── */

static void do_send_message(void) {
    const char *txt = gtk_editable_get_text(GTK_EDITABLE(entry_msg));
    if (!txt || !*txt) return;
    int sock = get_selected_sock();
    if (sock < 0) { append_message("", "⚠  No peer selected.", -1); return; }
    if (p2p_send_text(sock, txt) == 0)
        append_message("Me", txt, 1);
    else
        append_message("", "⚠  Send failed.", -1);
    gtk_editable_set_text(GTK_EDITABLE(entry_msg), "");
}

static void on_send_clicked(GtkWidget *w, gpointer ud) {
    (void)w; (void)ud;
    do_send_message();
}

static gboolean on_key_pressed(GtkEventControllerKey *eck, guint key,
                                guint kc, GdkModifierType mods, gpointer ud) {
    (void)eck; (void)kc; (void)mods; (void)ud;
    if (key == GDK_KEY_Return || key == GDK_KEY_KP_Enter) {
        do_send_message();
        return TRUE;
    }
    return FALSE;
}

static void on_broadcast_msg(GtkWidget *w, gpointer ud) {
    (void)w; (void)ud;
    const char *txt = gtk_editable_get_text(GTK_EDITABLE(entry_msg));
    if (!txt || !*txt) return;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++)
        if (g_data.peers[i].active) p2p_send_text(g_data.peers[i].sock, txt);
    pthread_mutex_unlock(&g_data.peers_lock);
    append_message("Me → all", txt, 1);
    gtk_editable_set_text(GTK_EDITABLE(entry_msg), "");
}

/* File open dialog callback */
static void on_file_open_done(GObject *src, GAsyncResult *res, gpointer ptr) {
    FileSendTask *hint = ptr;          /* contains broadcast flag + sock */
    GtkFileDialog *dlg = GTK_FILE_DIALOG(src);
    GFile *f = gtk_file_dialog_open_finish(dlg, res, NULL);
    if (!f) { free(hint); return; }
    char *path = g_file_get_path(f);
    g_object_unref(f);
    if (!path) { free(hint); return; }

    char buf[512];
    if (hint->broadcast) {
        snprintf(buf, sizeof(buf), "Broadcasting file: %s", path);
        dispatch_file_send(-1, path, 1);
    } else {
        int sock = hint->sock;
        if (sock < 0) {
            append_message("", "⚠  No peer selected.", -1);
            g_free(path); free(hint); return;
        }
        snprintf(buf, sizeof(buf), "Sending file: %s", path);
        dispatch_file_send(sock, path, 0);
    }
    append_message("", buf, -1);
    g_free(path);
    free(hint);
}

static void on_send_file(GtkWidget *w, gpointer ud) {
    (void)w;
    FileSendTask *hint = calloc(1, sizeof(FileSendTask));
    hint->broadcast = GPOINTER_TO_INT(ud);
    hint->sock      = hint->broadcast ? -1 : get_selected_sock();
    GtkFileDialog *dlg = gtk_file_dialog_new();
    gtk_file_dialog_open(dlg, main_window, NULL, on_file_open_done, hint);
    g_object_unref(dlg);
}

/* Manual connect dialog */
static void on_do_connect(GtkWidget *btn, gpointer ud) {
    GtkWidget *entry = GTK_WIDGET(ud);
    const char *s = gtk_editable_get_text(GTK_EDITABLE(entry));
    char ip[INET_ADDRSTRLEN] = {0};
    int port = g_data.myport;
    const char *colon = strchr(s, ':');
    if (colon) {
        size_t l = (size_t)(colon - s);
        if (l >= sizeof(ip)) l = sizeof(ip) - 1;
        memcpy(ip, s, l); ip[l] = '\0';
        port = atoi(colon + 1);
    } else {
        strncpy(ip, s, sizeof(ip) - 1);
    }
    if (*ip) {
        p2p_connect(ip, port);
        char msg[256];
        snprintf(msg, sizeof(msg), "⟢ Connecting to %s:%d …", ip, port);
        append_message("", msg, -1);
    }
    gtk_window_destroy(GTK_WINDOW(gtk_widget_get_ancestor(btn, GTK_TYPE_WINDOW)));
}

static void on_connect_clicked(GtkWidget *w, gpointer ud) {
    (void)w; (void)ud;
    GtkWidget *dlg = gtk_window_new();
    gtk_window_set_title(GTK_WINDOW(dlg), "Connect to peer");
    gtk_window_set_modal(GTK_WINDOW(dlg), TRUE);
    gtk_window_set_transient_for(GTK_WINDOW(dlg), main_window);
    gtk_window_set_default_size(GTK_WINDOW(dlg), 340, -1);
    gtk_window_set_resizable(GTK_WINDOW(dlg), FALSE);

    GtkWidget *card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 12);
    gtk_widget_add_css_class(card, "conn-card");
    gtk_widget_set_margin_top(card, 10); gtk_widget_set_margin_bottom(card, 10);
    gtk_widget_set_margin_start(card, 10); gtk_widget_set_margin_end(card, 10);
    gtk_window_set_child(GTK_WINDOW(dlg), card);

    GtkWidget *lbl = gtk_label_new("Connect to a peer");
    gtk_widget_add_css_class(lbl, "conn-title");
    gtk_label_set_xalign(GTK_LABEL(lbl), 0.0f);
    gtk_box_append(GTK_BOX(card), lbl);

    GtkWidget *entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(entry), "192.168.1.10:9100");
    gtk_widget_add_css_class(entry, "conn-input");
    gtk_box_append(GTK_BOX(card), entry);

    GtkWidget *row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    gtk_widget_set_halign(row, GTK_ALIGN_END);
    gtk_box_append(GTK_BOX(card), row);

    GtkWidget *b_cancel = gtk_button_new_with_label("Cancel");
    gtk_widget_add_css_class(b_cancel, "btn-decline");
    g_signal_connect_swapped(b_cancel, "clicked", G_CALLBACK(gtk_window_destroy), dlg);
    gtk_box_append(GTK_BOX(row), b_cancel);

    GtkWidget *b_ok = gtk_button_new_with_label("Connect");
    gtk_widget_add_css_class(b_ok, "btn-accept");
    g_signal_connect(b_ok, "clicked", G_CALLBACK(on_do_connect), entry);
    gtk_box_append(GTK_BOX(row), b_ok);

    gtk_window_present(GTK_WINDOW(dlg));
}

/* Sidebar row selection → update chat header */
static void on_peer_row_selected(GtkListBox *lb, GtkListBoxRow *row, gpointer ud) {
    (void)lb; (void)ud;
    if (!row) { gtk_label_set_text(GTK_LABEL(chat_peer_label), ""); return; }
    int idx = gtk_list_box_row_get_index(row), cnt = 0;
    pthread_mutex_lock(&g_data.peers_lock);
    for (int i = 0; i < MAX_PEERS; i++) {
        if (g_data.peers[i].active) {
            if (cnt++ == idx) {
                char buf[64];
                snprintf(buf, sizeof(buf), "● %s", g_data.peers[i].name);
                gtk_label_set_text(GTK_LABEL(chat_peer_label), buf);
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_data.peers_lock);
}

/* Periodic timer — dispatches peer list + progress bar refreshes */
static gboolean on_timer(gpointer ud) {
    (void)ud;
    g_idle_add(idle_rebuild_peers, NULL);
    g_idle_add(idle_update_xfer,   NULL);
    return G_SOURCE_CONTINUE;
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 10 · BUILD THE UI
 * ───────────────────────────────────────────────────────────────────────── */
static void activate(GtkApplication *app, gpointer ud) {
    (void)ud;

    /* ── CSS ── */
    GtkCssProvider *css = gtk_css_provider_new();
#if GTK_CHECK_VERSION(4, 12, 0)
    gtk_css_provider_load_from_string(css, APP_CSS);
#else
    gtk_css_provider_load_from_data(css, APP_CSS, -1, NULL);
#endif
    gtk_style_context_add_provider_for_display(
        gdk_display_get_default(),
        GTK_STYLE_PROVIDER(css),
        GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);

    /* ── Window ── */
    main_window = GTK_WINDOW(gtk_application_window_new(app));
    gtk_window_set_title(main_window, "P2P Chat");
    gtk_window_set_default_size(main_window, 900, 580);

    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_window_set_child(main_window, root);

    /* ── Header bar ── */
    GtkWidget *hbar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_widget_add_css_class(hbar, "app-header");
    gtk_widget_set_hexpand(hbar, TRUE);
    gtk_box_append(GTK_BOX(root), hbar);

    GtkWidget *title = gtk_label_new("P2P Chat");
    gtk_widget_add_css_class(title, "app-title");
    gtk_box_append(GTK_BOX(hbar), title);

    GtkWidget *sp = gtk_label_new("");
    gtk_widget_set_hexpand(sp, TRUE);
    gtk_box_append(GTK_BOX(hbar), sp);

    char info[128];
    snprintf(info, sizeof(info), "%s  ·  %s:%d",
             g_data.myname, g_data.myip, g_data.myport);
    GtkWidget *info_lbl = gtk_label_new(info);
    gtk_widget_add_css_class(info_lbl, "app-info");
    gtk_box_append(GTK_BOX(hbar), info_lbl);

    /* ── Paned: sidebar | chat ── */
    GtkWidget *paned = gtk_paned_new(GTK_ORIENTATION_HORIZONTAL);
    gtk_paned_set_position(GTK_PANED(paned), 210);
    gtk_widget_set_vexpand(paned, TRUE);
    gtk_box_append(GTK_BOX(root), paned);

    /* ─ Sidebar ─ */
    GtkWidget *sidebar = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_widget_add_css_class(sidebar, "sidebar");
    gtk_widget_set_size_request(sidebar, 180, -1);
    gtk_paned_set_start_child(GTK_PANED(paned), sidebar);
    gtk_paned_set_shrink_start_child(GTK_PANED(paned), FALSE);

    GtkWidget *sec_lbl = gtk_label_new("PEERS");
    gtk_widget_add_css_class(sec_lbl, "section-label");
    gtk_label_set_xalign(GTK_LABEL(sec_lbl), 0.0f);
    gtk_box_append(GTK_BOX(sidebar), sec_lbl);

    GtkWidget *peer_scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(peer_scroll),
                                   GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
    gtk_widget_set_vexpand(peer_scroll, TRUE);
    gtk_box_append(GTK_BOX(sidebar), peer_scroll);

    sidebar_list = gtk_list_box_new();
    gtk_list_box_set_selection_mode(GTK_LIST_BOX(sidebar_list), GTK_SELECTION_SINGLE);
    gtk_widget_add_css_class(sidebar_list, "sidebar");
    gtk_list_box_set_activate_on_single_click(GTK_LIST_BOX(sidebar_list), TRUE);
    g_signal_connect(sidebar_list, "row-selected",
                     G_CALLBACK(on_peer_row_selected), NULL);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(peer_scroll), sidebar_list);

    GtkWidget *btn_conn = gtk_button_new_with_label("＋  Connect…");
    gtk_widget_add_css_class(btn_conn, "btn-connect");
    g_signal_connect(btn_conn, "clicked", G_CALLBACK(on_connect_clicked), NULL);
    gtk_box_append(GTK_BOX(sidebar), btn_conn);

    /* ─ Chat panel ─ */
    GtkWidget *chat = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_widget_set_hexpand(chat, TRUE);
    gtk_paned_set_end_child(GTK_PANED(paned), chat);
    gtk_paned_set_shrink_end_child(GTK_PANED(paned), FALSE);

    /* Chat header */
    GtkWidget *chdr = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    gtk_widget_add_css_class(chdr, "chat-header");
    gtk_box_append(GTK_BOX(chat), chdr);

    chat_peer_label = gtk_label_new("");
    gtk_widget_add_css_class(chat_peer_label, "chat-peer-name");
    gtk_label_set_xalign(GTK_LABEL(chat_peer_label), 0.0f);
    gtk_widget_set_hexpand(chat_peer_label, TRUE);
    gtk_box_append(GTK_BOX(chdr), chat_peer_label);

    /* Messages scroll */
    messages_scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(messages_scroll),
                                   GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
    gtk_widget_set_vexpand(messages_scroll, TRUE);
    gtk_widget_add_css_class(messages_scroll, "messages-area");
    gtk_box_append(GTK_BOX(chat), messages_scroll);

    messages_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);
    gtk_widget_set_margin_top(messages_box, 12);
    gtk_widget_set_margin_bottom(messages_box, 12);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(messages_scroll), messages_box);

    /* Input bar */
    GtkWidget *ibar = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    gtk_widget_add_css_class(ibar, "input-bar");
    gtk_box_append(GTK_BOX(chat), ibar);

    /* Tool buttons row */
    GtkWidget *arow = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_add_css_class(arow, "action-row");
    gtk_box_append(GTK_BOX(ibar), arow);

    GtkWidget *b_file = gtk_button_new_with_label("📎  Send File");
    gtk_widget_add_css_class(b_file, "btn-tool");
    g_signal_connect(b_file, "clicked", G_CALLBACK(on_send_file), GINT_TO_POINTER(0));
    gtk_box_append(GTK_BOX(arow), b_file);

    GtkWidget *b_bfile = gtk_button_new_with_label("📢  Broadcast File");
    gtk_widget_add_css_class(b_bfile, "btn-tool");
    g_signal_connect(b_bfile, "clicked", G_CALLBACK(on_send_file), GINT_TO_POINTER(1));
    gtk_box_append(GTK_BOX(arow), b_bfile);

    GtkWidget *b_bmsg = gtk_button_new_with_label("💬  Broadcast Msg");
    gtk_widget_add_css_class(b_bmsg, "btn-tool");
    g_signal_connect(b_bmsg, "clicked", G_CALLBACK(on_broadcast_msg), NULL);
    gtk_box_append(GTK_BOX(arow), b_bmsg);

    /* Message entry + send */
    GtkWidget *mrow = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_box_append(GTK_BOX(ibar), mrow);

    entry_msg = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(entry_msg), "Type a message…");
    gtk_widget_add_css_class(entry_msg, "msg-input");
    gtk_widget_set_hexpand(entry_msg, TRUE);
    gtk_box_append(GTK_BOX(mrow), entry_msg);

    GtkEventController *kc = gtk_event_controller_key_new();
    g_signal_connect(kc, "key-pressed", G_CALLBACK(on_key_pressed), NULL);
    gtk_widget_add_controller(entry_msg, kc);

    GtkWidget *b_send = gtk_button_new_with_label("↑");
    gtk_widget_add_css_class(b_send, "btn-send");
    g_signal_connect(b_send, "clicked", G_CALLBACK(on_send_clicked), NULL);
    gtk_box_append(GTK_BOX(mrow), b_send);

    /* ── Transfer bar (hidden when idle) ── */
    xfer_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    gtk_widget_add_css_class(xfer_bar, "xfer-bar");
    gtk_widget_set_visible(xfer_bar, FALSE);
    gtk_box_append(GTK_BOX(root), xfer_bar);

    xfer_label = gtk_label_new("");
    gtk_widget_add_css_class(xfer_label, "xfer-label");
    gtk_box_append(GTK_BOX(xfer_bar), xfer_label);

    xfer_pg = gtk_progress_bar_new();
    gtk_widget_add_css_class(xfer_pg, "xfer-pg");
    gtk_widget_set_hexpand(xfer_pg, TRUE);
    gtk_widget_set_valign(xfer_pg, GTK_ALIGN_CENTER);
    gtk_box_append(GTK_BOX(xfer_bar), xfer_pg);

    /* ── Periodic updates ── */
    g_timeout_add(1000, on_timer, NULL);

    gtk_window_present(main_window);

    /* Show welcome message */
    append_message("", "P2P Chat started — waiting for peers…", -1);
}

/* ───────────────────────────────────────────────────────────────────────────
 *  SECTION 11 · MAIN
 * ───────────────────────────────────────────────────────────────────────── */
int main(int argc, char **argv) {
    p2p_init(argc > 1 ? argv[1] : NULL,
             argc > 2 ? atoi(argv[2]) : 9100,
             argc > 3 ? argv[3] : NULL);

    /* Register all callbacks before p2p_start() */
    g_data.on_message       = cb_on_message;
    g_data.on_peer_event    = cb_on_peer_event;
    g_data.on_log           = cb_on_log;
    g_data.on_file_incoming = cb_on_file_incoming;
    g_data.cb_userdata      = NULL;

    p2p_start();

    GtkApplication *app =
        gtk_application_new("org.p2pchat.app", G_APPLICATION_DEFAULT_FLAGS);
    g_signal_connect(app, "activate", G_CALLBACK(activate), NULL);
    int status = g_application_run(G_APPLICATION(app), 0, NULL);

    p2p_stop();
    g_object_unref(app);
    return status;
}