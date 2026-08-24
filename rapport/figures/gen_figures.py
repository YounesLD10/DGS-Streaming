#!/usr/bin/env python3
"""Generate the rapport's matplotlib figures (conceptual diagrams + styled
terminal-output captures from live cluster data). Run once from this dir."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import textwrap

BG = "#0D1117"
PANEL = "#161B22"
BLUE = "#58A6FF"
GREEN = "#3FB950"
RED = "#F85149"
ORANGE = "#D29922"
GREY = "#8B949E"
WHITE = "#E6EDF3"

plt.rcParams["font.family"] = "DejaVu Sans"


def new_fig(w, h):
    fig, ax = plt.subplots(figsize=(w, h), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.axis("off")
    return fig, ax


def box(ax, x, y, w, h, text, color=BLUE, textcolor="black", fontsize=9.5, lw=1.4):
    r = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                        linewidth=lw, edgecolor=color, facecolor=color, alpha=0.85, zorder=2)
    ax.add_patch(r)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
             fontsize=fontsize, color=textcolor, weight="bold", zorder=3, wrap=True)
    return (x + w / 2, y + h / 2)


def arrow(ax, p1, p2, color=GREY, label=None, label_color=WHITE, lw=1.3, style="-"):
    a = FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=14,
                         color=color, linewidth=lw, linestyle=style, zorder=1)
    ax.add_patch(a)
    if label:
        mx, my = (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2
        ax.text(mx + 0.15, my, label, fontsize=8, color=label_color, ha="left", va="center")


# ---------------------------------------------------------------------------
# 1. etats.png — diagramme d'etats-transitions (9 etats)
# ---------------------------------------------------------------------------
def gen_etats():
    fig, ax = new_fig(8, 9.8)
    ax.set_xlim(0, 9.5)
    ax.set_ylim(3.6, 24)

    states = [
        ("Produit", "Lu depuis le CSV", BLUE, 21.6),
        ("Chiffre", "Fernet (AES-128-CBC\n+ HMAC-SHA256)", BLUE, 19.1),
        ("Publie", "Topic Kafka 'payments'", BLUE, 16.6),
        ("Decrypte", "Job1 - ecrit Bronze", BLUE, 14.1),
    ]
    centers = {}
    for name, sub, color, y in states:
        c = box(ax, 2.5, y, 5, 1.8, f"{name}\n{sub}", color=color, fontsize=9)
        centers[name] = c

    # Valide branch
    c_valide = box(ax, 2.5, 11.6, 5, 1.6, "Valide\nJob2 - 9 regles ISO respectees", color=GREEN, fontsize=9)
    c_normalise = box(ax, 2.5, 9.2, 5, 1.6, "Normalise\nJob3 - enrichi, ecrit Silver", color=GREEN, fontsize=9)
    c_optimise = box(ax, 2.5, 6.8, 5, 1.6, "Optimise\nJob4 - score + deduplique, ecrit Gold", color=GREEN, fontsize=9)
    c_sync = box(ax, 2.5, 4.2, 5, 1.8,
                 "Synchronise\ngold-flattener -> JDBC Sink\n-> gold_transactions",
                 color="#2EA043", fontsize=8.5)

    # Rejete (terminal, rouge) — colonne separee a gauche, pas de croisement
    c_rejete = box(ax, 0.2, 9.5, 2.0, 2.4, "Rejete\n\nRoute vers\npayments.dlq", color=RED, fontsize=8.5)

    arrow(ax, (centers["Produit"][0], 21.6), (centers["Chiffre"][0], 20.9))
    arrow(ax, (centers["Chiffre"][0], 19.1), (centers["Publie"][0], 18.4))
    arrow(ax, (centers["Publie"][0], 16.6), (centers["Decrypte"][0], 15.9), label="Token Fernet valide")
    arrow(ax, (centers["Decrypte"][0], 14.1), (c_valide[0], 13.2), label="9 regles respectees")

    # Fleches rouges courtes, horizontales, vers la colonne Rejete (gauche)
    arrow(ax, (centers["Publie"][0] - 2.5, 17.2), (1.2, 11.5), color=RED, label=None)
    ax.text(1.3, 13.7, "Token invalide", fontsize=7.3, color=RED, ha="left", va="center", rotation=58)
    arrow(ax, (centers["Decrypte"][0] - 2.5, 14.7), (1.2, 11.3), color=RED, label=None)
    ax.text(2.65, 13.55, "Regle ISO\nechouee", fontsize=7.3, color=RED, ha="left", va="center")

    arrow(ax, (c_valide[0], 11.6), (c_normalise[0], 10.8))
    arrow(ax, (c_normalise[0], 9.2), (c_optimise[0], 8.4))
    arrow(ax, (c_optimise[0], 6.8), (c_sync[0], 6.0))

    ax.text(4.85, 23.2, "Diagramme d'etats-transitions du pipeline (9 etats)",
            ha="center", fontsize=11.5, color=WHITE, weight="bold")
    fig.tight_layout()
    fig.savefig("etats.png", facecolor=BG)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 2. activite.png — diagramme d'activite (9 regles Job2)
# ---------------------------------------------------------------------------
def gen_activite():
    fig, ax = new_fig(7, 11.2)
    ax.set_xlim(0, 8)
    ax.set_ylim(11.3, 30)

    rules = [
        ("① MESSAGE_TYPE a 4 chiffres ?", "MESSAGE_TYPE_INVALIDE"),
        ("② AMOUNT numerique > 0 ?", "MONTANT_INVALIDE"),
        ("③④⑤ DEVISE / BANQUE / CARD_TYPE presents ?", "CHAMP_MANQUANT"),
        ("⑥ REJECT_CODE vide ?", "REJETE_PAR_BANQUE"),
        ("⑦ ISO 8583 - MTI connu ?", "ISO8583_MTI_INCONNU"),
        ("⑧ ISO 4217 - devise valide (23) ?", "ISO4217_DEVISE_INVALIDE"),
        ("⑨ ISO 7812 - Luhn valide sur le PAN ?", "ISO7812_LUHN_ECHOUE"),
    ]

    y = 28.5
    box(ax, 2.5, y, 3, 1.1, "Recoit la transaction\ndecryptee (Job1)", color=BLUE, fontsize=8)
    y -= 1.9
    prev_c = (4, y + 1.9)
    for cond, reject in rules:
        cy = box(ax, 1.8, y, 4.4, 1.2, cond, color=ORANGE, textcolor="black", fontsize=8)
        arrow(ax, prev_c, (cy[0], y + 1.2), label="non" if prev_c[1] != 28.5 else None)
        rx = box(ax, 6.7, y - 0.1, 1.2, 1.0, reject, color=RED, fontsize=6.0)
        arrow(ax, (cy[0] + 2.2, y + 0.6), (6.65, rx[1]), color=RED)
        ax.text(6.42, y + 0.78, "oui", fontsize=7.5, color=RED, ha="center", va="bottom")
        prev_c = (cy[0], y)
        y -= 1.9

    final = box(ax, 2.3, y - 0.3, 3.4, 1.2, "payments.validated\n(9 regles respectees)", color=GREEN, fontsize=8.5)
    arrow(ax, prev_c, (final[0], y + 0.9))

    ax.text(4, 29.6, "Diagramme d'activite — validation Job2 (9 regles)",
            ha="center", fontsize=11, color=WHITE, weight="bold")
    fig.tight_layout()
    fig.savefig("activite.png", facecolor=BG)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 3. star_schema.png — table gold_transactions (modele relationnel plat)
# ---------------------------------------------------------------------------
def gen_star_schema():
    fig, ax = new_fig(7, 8)
    ax.set_xlim(0, 8)
    ax.set_ylim(0, 11)

    fields = [
        ("authorization_code", "TEXT", "PK"),
        ("message_type", "TEXT", ""),
        ("transaction_amount", "NUMERIC(18,2)", ""),
        ("currency_code / currency_alpha", "TEXT", ""),
        ("issuing_bank", "TEXT", ""),
        ("card_type / card_scheme", "TEXT", ""),
        ("payment_channel", "TEXT", ""),
        ("risk_score", "TEXT", ""),
        ("mti_name / mcc_description", "TEXT", ""),
        ("matching_status / reject_code", "TEXT", ""),
        ("processed_at", "TEXT", ""),
        ("source_system / pipeline_version", "TEXT", ""),
        ("loaded_at", "TIMESTAMPTZ", "DEFAULT now()"),
    ]

    box(ax, 1, 9.8, 6, 0.9, "gold_transactions", color="#2EA043", fontsize=13)
    y = 9.7
    for name, typ, note in fields:
        y -= 0.68
        r = FancyBboxPatch((1, y), 6, 0.6, boxstyle="square,pad=0", linewidth=0.8,
                            edgecolor=GREY, facecolor=PANEL, zorder=2)
        ax.add_patch(r)
        label = f"{name}" + ("  — PK" if note == "PK" else "")
        ax.text(1.15, y + 0.3, label, fontsize=8.3, color=WHITE, va="center")
        ax.text(6.85, y + 0.3, typ, fontsize=7.6, color=BLUE, va="center", ha="right")

    ax.text(4, 10.6, "Table gold_transactions — modele relationnel plat",
            ha="center", fontsize=11.5, color=WHITE, weight="bold")
    ax.text(4, 0.55, "Pas de tables de dimension : un seul champ par colonne du flux\n"
                     "produit par scripts/gold_flattener.py (payments.gold.flat)",
            ha="center", fontsize=8, color=GREY, style="italic")
    fig.tight_layout()
    fig.savefig("star_schema.png", facecolor=BG)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 4. components.png — diagramme de composants du pipeline
# ---------------------------------------------------------------------------
def gen_components():
    fig, ax = new_fig(13, 9)
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 11)

    p = box(ax, 0.3, 8.7, 2, 1, "producer.py", color=BLUE, fontsize=8.5)
    t_pay = box(ax, 2.8, 8.8, 1.6, 0.8, "payments", color=ORANGE, textcolor="black", fontsize=7.5)
    j1 = box(ax, 4.9, 8.7, 1.8, 1, "Job1\nDecrypt", color=GREEN, fontsize=8)
    t_dec = box(ax, 7.2, 8.8, 1.9, 0.8, "payments.\ndecrypted", color=ORANGE, textcolor="black", fontsize=6.8)
    j2 = box(ax, 9.6, 8.7, 1.8, 1, "Job2\nValidate", color=GREEN, fontsize=8)
    t_val = box(ax, 11.9, 9.3, 1.9, 0.7, "payments.\nvalidated", color=ORANGE, textcolor="black", fontsize=6.5)
    t_dlq = box(ax, 11.9, 8.2, 1.9, 0.7, "payments.dlq", color=RED, fontsize=6.8)
    j3 = box(ax, 14.3, 8.7, 1.5, 1, "Job3\nNormalize", color=GREEN, fontsize=7.5)

    t_norm = box(ax, 14.0, 6.9, 1.9, 0.8, "payments.\nnormalized", color=ORANGE, textcolor="black", fontsize=6.5)
    j4 = box(ax, 11.4, 6.9, 1.8, 1, "Job4\nOptimize", color=GREEN, fontsize=8)
    t_gold = box(ax, 9.0, 6.9, 1.9, 0.8, "payments.gold", color=ORANGE, textcolor="black", fontsize=7)
    flat = box(ax, 6.4, 6.9, 1.9, 1, "gold-\nflattener", color="#2EA043", fontsize=8)
    t_flat = box(ax, 4.0, 6.9, 1.9, 0.8, "payments.\ngold.flat", color=ORANGE, textcolor="black", fontsize=6.8)
    jdbc = box(ax, 1.4, 6.9, 2.2, 1, "Kafka Connect\nJDBC Sink", color="#2EA043", fontsize=7.5)
    pg = box(ax, 0.3, 5.0, 1.9, 0.9, "PostgreSQL\ngold_transactions", color="#9D4EDD", fontsize=6.8)

    arrow(ax, (2.3, 9.2), (2.8, 9.2))
    arrow(ax, (4.4, 9.2), (4.9, 9.2))
    arrow(ax, (6.7, 9.2), (7.2, 9.2))
    arrow(ax, (9.1, 9.2), (9.6, 9.2))
    arrow(ax, (11.4, 9.5), (11.9, 9.65), color=GREEN)
    arrow(ax, (11.4, 8.9), (11.9, 8.55), color=RED)
    arrow(ax, (13.8, 9.65), (14.3, 9.3), color=GREEN)
    arrow(ax, (15.05, 8.7), (14.95, 7.7))
    arrow(ax, (14.0, 7.3), (11.7, 7.3))
    arrow(ax, (11.4, 7.3), (10.9, 7.3))
    arrow(ax, (9.0, 7.3), (8.3, 7.3))
    arrow(ax, (6.4, 7.3), (5.9, 7.3))
    arrow(ax, (4.0, 7.3), (3.6, 7.3))
    arrow(ax, (2.3, 6.9), (1.6, 5.9))

    # MinIO branch
    minio_b = box(ax, 0.3, 3.4, 1.9, 0.8, "MinIO\nBronze", color="#FF8800", textcolor="black", fontsize=7)
    minio_s = box(ax, 2.5, 3.4, 1.9, 0.8, "MinIO\nSilver", color="#FF8800", textcolor="black", fontsize=7)
    minio_g = box(ax, 4.7, 3.4, 1.9, 0.8, "MinIO\nGold", color="#FF8800", textcolor="black", fontsize=7)
    arrow(ax, (5.5, 8.7), (1.2, 4.2), color="#FF8800", style="--")
    arrow(ax, (14.6, 6.9), (3.4, 4.2), color="#FF8800", style="--")
    arrow(ax, (9.5, 6.9), (5.6, 4.2), color="#FF8800", style="--")

    # Monitoring branch
    exp = box(ax, 7.6, 3.4, 1.9, 0.8, "swam_\nexporter", color="#9D4EDD", fontsize=7.5)
    prom = box(ax, 9.8, 3.4, 1.9, 0.8, "Prometheus", color="#9D4EDD", fontsize=7.5)
    graf = box(ax, 12.0, 3.4, 1.9, 0.8, "Grafana", color="#9D4EDD", fontsize=7.5)
    arrow(ax, (8.5, 3.4), (8.5, 4.9), color="#9D4EDD", style=":")
    arrow(ax, (9.5, 3.8), (9.8, 3.8))
    arrow(ax, (11.7, 3.8), (12.0, 3.8))

    ax.text(8, 10.4, "Diagramme de composants — pipeline de streaming temps reel",
            ha="center", fontsize=12.5, color=WHITE, weight="bold")
    ax.text(8, 2.2,
            "NB : une tentative de connecter directement le JDBC Sink (Debezium) sur payments.gold a echoue\n"
            "(NullPointerException dans SinkRecordDescriptor.Builder.isFlattened() sur JSON schemaless).\n"
            "Solution retenue : gold-flattener republie un message a schema explicite avant le sink.",
            ha="center", fontsize=8, color=GREY, style="italic")
    fig.tight_layout()
    fig.savefig("components.png", facecolor=BG)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Terminal-style capture renderer (real CLI output -> styled image)
# ---------------------------------------------------------------------------
def render_terminal(filename, title, text, w=11, fontsize=9.2):
    lines = text.rstrip("\n").split("\n")
    h = 0.9 + 0.26 * len(lines)
    fig, ax = plt.subplots(figsize=(w, h), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor("#010409")
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    # title bar
    bar = mpatches.FancyBboxPatch((0, 1 - 0.9 / h), 1, 0.9 / h, boxstyle="square,pad=0",
                                   facecolor=PANEL, edgecolor="none", transform=ax.transAxes)
    ax.add_patch(bar)
    for i, c in enumerate([RED, ORANGE, GREEN]):
        ax.add_patch(plt.Circle((0.02 + i * 0.025, 1 - 0.45 / h), 0.007, color=c, transform=ax.transAxes))
    ax.text(0.5, 1 - 0.45 / h, title, ha="center", va="center", fontsize=10,
            color=WHITE, weight="bold", transform=ax.transAxes)

    y0 = 1 - 1.05 / h
    for i, line in enumerate(lines):
        ax.text(0.015, y0 - i * (0.26 / h), line, ha="left", va="top",
                 fontsize=fontsize, color="#7EE787" if line.strip().startswith("===") else WHITE,
                 family="monospace", transform=ax.transAxes)
    fig.tight_layout(pad=0.3)
    fig.savefig(filename, facecolor=BG)
    plt.close(fig)


def gen_terminal_captures():
    caps = [
        ("terraform-apply.png", "terraform plan — terraform/", "/tmp/cap_terraform.txt"),
        ("pods-running.png", "minikube kubectl -- get pods -A", "/tmp/cap_pods.txt"),
        ("kafka-cluster.png", "minikube kubectl -- get kafka -n kafka", "/tmp/cap_kafka.txt"),
        ("kafka-topics.png", "minikube kubectl -- get kafkatopic -n kafka", "/tmp/cap_topics.txt"),
        ("debezium-running.png", "GET /connectors/gold-transactions-sink/status", "/tmp/cap_connector.txt"),
        ("datamart-rows.png", "SELECT * FROM gold_transactions ORDER BY loaded_at DESC LIMIT 8;", "/tmp/cap_rows.txt"),
    ]
    for fname, title, src in caps:
        with open(src) as f:
            text = f.read()
        render_terminal(fname, title, text)


if __name__ == "__main__":
    gen_etats()
    gen_activite()
    gen_star_schema()
    gen_components()
    gen_terminal_captures()
    print("All figures generated.")
