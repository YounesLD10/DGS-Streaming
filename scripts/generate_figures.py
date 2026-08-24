"""
Generate 5 PNG architecture figures for the PFE SWAM report.
Output: ~/hps-rt-poc/rapport/figures/
"""

import os
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle, Polygon, Arc
import matplotlib.patheffects as pe
import numpy as np

OUT = os.path.expanduser("~/hps-rt-poc/rapport/figures")
os.makedirs(OUT, exist_ok=True)

# ─── Palette ─────────────────────────────────────────────────────────────────
VERT   = '#1B5E20'
BLEU   = '#1565C0'
ORANGE = '#E65100'
ROUGE  = '#C62828'
VIOLET = '#6A1B9A'
TEAL   = '#00695C'
JAUNE  = '#F9A825'
BRONZE = '#8D6E63'
SILVER = '#78909C'
GRIS   = '#424242'
BLANC  = '#FFFFFF'
LBLEU  = '#E3F2FD'
LVERT  = '#E8F5E9'
LGRIS  = '#F5F5F5'

# ─── Logo helpers ─────────────────────────────────────────────────────────────

def draw_kafka_logo(ax, cx, cy, r=0.25):
    """Orange square with stylised K."""
    sq = FancyBboxPatch((cx - r, cy - r), 2*r, 2*r,
                        boxstyle="round,pad=0.02", linewidth=0,
                        facecolor=ORANGE, zorder=5)
    ax.add_patch(sq)
    ax.text(cx, cy, 'K', ha='center', va='center',
            fontsize=r*52, fontweight='bold', color='white', zorder=6)

def draw_flink_logo(ax, cx, cy, r=0.25):
    """Red triangle with F."""
    pts = np.array([[cx, cy+r], [cx-r*0.86, cy-r*0.5], [cx+r*0.86, cy-r*0.5]])
    tri = plt.Polygon(pts, closed=True, facecolor=ROUGE, edgecolor='white',
                      linewidth=1.5, zorder=5)
    ax.add_patch(tri)
    ax.text(cx, cy-r*0.05, 'F', ha='center', va='center',
            fontsize=r*40, fontweight='bold', color='white', zorder=6)

def draw_minio_logo(ax, cx, cy, w=0.55, h=0.22):
    """Red rectangle with MinIO text."""
    rect = FancyBboxPatch((cx - w/2, cy - h/2), w, h,
                          boxstyle="round,pad=0.02", linewidth=0,
                          facecolor=ROUGE, zorder=5)
    ax.add_patch(rect)
    ax.text(cx, cy, 'MinIO', ha='center', va='center',
            fontsize=h*46, fontweight='bold', color='white', zorder=6)

def draw_postgres_logo(ax, cx, cy, r=0.25):
    """Blue circle with PG."""
    circ = Circle((cx, cy), r, facecolor=BLEU, edgecolor='#0D47A1',
                  linewidth=1.5, zorder=5)
    ax.add_patch(circ)
    ax.text(cx, cy, 'PG', ha='center', va='center',
            fontsize=r*38, fontweight='bold', color='white', zorder=6)

def draw_prometheus_logo(ax, cx, cy, r=0.25):
    """Orange circle with flame."""
    circ = Circle((cx, cy), r, facecolor='#E87722', edgecolor='#BF360C',
                  linewidth=1.5, zorder=5)
    ax.add_patch(circ)
    # Outer flame
    flame_pts = np.array([[cx, cy+r*0.68], [cx-r*0.38, cy-r*0.25],
                          [cx-r*0.15, cy-r*0.05], [cx-r*0.25, cy-r*0.45],
                          [cx, cy-r*0.2], [cx+r*0.25, cy-r*0.45],
                          [cx+r*0.15, cy-r*0.05], [cx+r*0.38, cy-r*0.25]])
    flame = plt.Polygon(flame_pts, closed=True, facecolor='white', zorder=6)
    ax.add_patch(flame)

def draw_grafana_logo(ax, cx, cy, r=0.25):
    """Orange hexagon with G."""
    angles = [math.pi/2 + i*math.pi/3 for i in range(6)]
    pts = [(cx + r*math.cos(a), cy + r*math.sin(a)) for a in angles]
    hex_patch = plt.Polygon(pts, closed=True, facecolor='#F46800',
                            edgecolor='#D84315', linewidth=1.5, zorder=5)
    ax.add_patch(hex_patch)
    ax.text(cx, cy, 'G', ha='center', va='center',
            fontsize=r*42, fontweight='bold', color='white', zorder=6)

def draw_k8s_logo(ax, cx, cy, r=0.25):
    """Blue wheel (circle + 7 spokes)."""
    circ = Circle((cx, cy), r, facecolor=BLEU, edgecolor='#0D47A1',
                  linewidth=1.5, zorder=5)
    ax.add_patch(circ)
    inner = Circle((cx, cy), r*0.35, facecolor='white', zorder=6)
    ax.add_patch(inner)
    for i in range(7):
        a = i * 2 * math.pi / 7
        x1, y1 = cx + r*0.38*math.cos(a), cy + r*0.38*math.sin(a)
        x2, y2 = cx + r*0.85*math.cos(a), cy + r*0.85*math.sin(a)
        ax.plot([x1, x2], [y1, y2], color='white', lw=2, zorder=7)

def draw_terraform_logo(ax, cx, cy, r=0.22):
    """Purple diamond with TF."""
    diamond_pts = np.array([[cx, cy+r], [cx+r*0.7, cy], [cx, cy-r], [cx-r*0.7, cy]])
    d = plt.Polygon(diamond_pts, closed=True, facecolor=VIOLET,
                    edgecolor='#4A148C', linewidth=1.5, zorder=5)
    ax.add_patch(d)
    ax.text(cx, cy, 'TF', ha='center', va='center',
            fontsize=r*38, fontweight='bold', color='white', zorder=6)

def draw_debezium_logo(ax, cx, cy, w=0.50, h=0.22):
    """Red rectangle with DBZ."""
    rect = FancyBboxPatch((cx - w/2, cy - h/2), w, h,
                          boxstyle="round,pad=0.02", linewidth=0,
                          facecolor=ROUGE, zorder=5)
    ax.add_patch(rect)
    ax.text(cx, cy, 'DBZ', ha='center', va='center',
            fontsize=h*42, fontweight='bold', color='white', zorder=6)

def draw_python_logo(ax, cx, cy, r=0.22):
    """Blue/yellow circle with Py."""
    circ_b = Circle((cx - r*0.15, cy), r*0.85, facecolor='#3776AB',
                    edgecolor='#FFD43B', linewidth=2, zorder=5)
    ax.add_patch(circ_b)
    ax.text(cx, cy, 'Py', ha='center', va='center',
            fontsize=r*38, fontweight='bold', color='white', zorder=6)

def arrow(ax, x1, y1, x2, y2, color='#333333', lw=1.5, style='->', head=12):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=f'->', color=color,
                                lw=lw, mutation_scale=head))

def rounded_box(ax, x, y, w, h, color, label=None, label_color='white',
                fontsize=10, alpha=1.0, linewidth=0, edgecolor='none', zorder=3):
    box = FancyBboxPatch((x, y), w, h,
                         boxstyle="round,pad=0.04",
                         facecolor=color, edgecolor=edgecolor,
                         linewidth=linewidth, alpha=alpha, zorder=zorder)
    ax.add_patch(box)
    if label:
        ax.text(x + w/2, y + h/2, label, ha='center', va='center',
                fontsize=fontsize, color=label_color,
                fontweight='bold', zorder=zorder+1)

def section_box(ax, x, y, w, h, title, bg, title_bg=None, fontsize=11):
    """A rounded container with a darker header strip."""
    if title_bg is None:
        title_bg = bg
    rounded_box(ax, x, y, w, h, bg, edgecolor=title_bg, linewidth=2, zorder=2)
    rounded_box(ax, x, y + h - 0.5, w, 0.5, title_bg,
                label=title, fontsize=fontsize, zorder=4)


# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 1 — Flux de données
# ═══════════════════════════════════════════════════════════════════════════════

def fig_flux_donnees():
    fig, ax = plt.subplots(figsize=(20, 13))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 13)
    ax.axis('off')
    fig.patch.set_facecolor('white')

    # Title
    ax.text(10, 12.4, "Flux de Données — Pipeline SWAM Temps Réel",
            ha='center', va='center', fontsize=16, fontweight='bold',
            color=VERT)
    ax.axhline(12.1, color=VERT, linewidth=2, xmin=0.02, xmax=0.98)

    # ── Row 1 : PostgreSQL → Debezium → Kafka payments ──────────────────────
    # PostgreSQL SWAM
    rounded_box(ax, 0.3, 10.0, 2.5, 1.6, LBLEU, edgecolor=BLEU, linewidth=2, zorder=2)
    draw_postgres_logo(ax, 1.0, 10.95, r=0.28)
    ax.text(1.9, 11.05, 'PostgreSQL\nSWAM', ha='center', va='center',
            fontsize=9, color=GRIS)
    ax.text(1.55, 10.3, 'transactions\n(CDC source)', ha='center', va='center',
            fontsize=7.5, color='#666')

    arrow(ax, 2.8, 10.8, 4.0, 10.8, color=BLEU, lw=2)
    ax.text(3.4, 11.05, 'CDC', ha='center', fontsize=8, color=BLEU, style='italic')

    # Debezium
    rounded_box(ax, 4.0, 10.0, 2.5, 1.6, '#F3E5F5', edgecolor=VIOLET, linewidth=2, zorder=2)
    draw_debezium_logo(ax, 4.75, 11.0, w=0.5, h=0.22)
    ax.text(5.5, 11.0, 'Debezium\nConnector', ha='center', va='center',
            fontsize=9, color=GRIS)
    ax.text(5.25, 10.3, 'Capture des\nchangements', ha='center', va='center',
            fontsize=7.5, color='#666')

    arrow(ax, 6.5, 10.8, 7.6, 10.8, color=ORANGE, lw=2)
    ax.text(7.05, 11.05, 'produce', ha='center', fontsize=8, color=ORANGE, style='italic')

    # Topic payments (raw)
    rounded_box(ax, 7.6, 10.0, 2.6, 1.6, '#FFF3E0', edgecolor=ORANGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 8.2, 10.95, r=0.22)
    ax.text(9.15, 10.95, 'payments\n(raw encrypted)', ha='center', va='center',
            fontsize=9, color=GRIS)
    ax.text(8.9, 10.25, 'raw_encrypted\nAES-256', ha='center', va='center',
            fontsize=7.5, color='#666')

    # ── Row 2 : Job1 Decrypt ─────────────────────────────────────────────────
    arrow(ax, 8.9, 10.0, 8.9, 9.15, color=VERT, lw=2)
    ax.text(9.2, 9.55, 'consume', ha='left', fontsize=8, color=VERT, style='italic')

    # Job1
    rounded_box(ax, 7.3, 8.2, 3.2, 1.6, LVERT, edgecolor=VERT, linewidth=2, zorder=2)
    draw_flink_logo(ax, 8.0, 9.05, r=0.25)
    ax.text(9.1, 9.1, 'Job 1 — Decrypt', ha='center', va='center',
            fontsize=9.5, color=VERT, fontweight='bold')
    ax.text(9.0, 8.55, 'Déchiffrement AES\nDLQ → payments.dlq', ha='center', va='center',
            fontsize=7.5, color='#555')

    # DLQ arrow
    arrow(ax, 10.5, 8.8, 12.2, 8.8, color=ROUGE, lw=1.5)
    ax.text(11.35, 9.05, 'reject', ha='center', fontsize=8, color=ROUGE, style='italic')
    rounded_box(ax, 12.2, 8.2, 2.8, 1.2, '#FFEBEE', edgecolor=ROUGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 12.8, 8.8, r=0.2)
    ax.text(13.9, 8.8, 'payments\n.dlq', ha='center', va='center',
            fontsize=9, color=ROUGE)

    # ── Row 3 : Topic decrypted → Job2 Validate ──────────────────────────────
    arrow(ax, 8.9, 8.2, 8.9, 7.35, color=VERT, lw=2)

    # Topic decrypted
    rounded_box(ax, 7.3, 6.5, 3.2, 1.6, '#FFF3E0', edgecolor=ORANGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 7.95, 7.4, r=0.22)
    ax.text(9.05, 7.4, 'payments\n.decrypted', ha='center', va='center',
            fontsize=9, color=GRIS)
    ax.text(8.9, 6.75, 'déchiffrées, en clair', ha='center', va='center',
            fontsize=7.5, color='#666')

    arrow(ax, 8.9, 6.5, 8.9, 5.65, color=VERT, lw=2)

    # Job2
    rounded_box(ax, 7.3, 4.8, 3.2, 1.6, LVERT, edgecolor=VERT, linewidth=2, zorder=2)
    draw_flink_logo(ax, 7.95, 5.65, r=0.25)
    ax.text(9.1, 5.65, 'Job 2 — Validate', ha='center', va='center',
            fontsize=9.5, color=VERT, fontweight='bold')
    ax.text(9.0, 5.1, 'ISO 8583/4217/7812\nvalidation + DLQ', ha='center', va='center',
            fontsize=7.5, color='#555')

    # DLQ arrow from Job2
    arrow(ax, 10.5, 5.4, 12.2, 5.4, color=ROUGE, lw=1.5)
    ax.text(11.35, 5.65, 'invalid', ha='center', fontsize=8, color=ROUGE, style='italic')
    rounded_box(ax, 12.2, 4.9, 2.8, 1.0, '#FFEBEE', edgecolor=ROUGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 12.8, 5.4, r=0.2)
    ax.text(13.9, 5.4, 'payments\n.dlq', ha='center', va='center',
            fontsize=9, color=ROUGE)

    # ── Row 4 : Topic validated → Job3 Normalize ─────────────────────────────
    arrow(ax, 8.9, 4.8, 8.9, 3.95, color=VERT, lw=2)

    # Topic validated
    rounded_box(ax, 7.3, 3.1, 3.2, 1.6, '#FFF3E0', edgecolor=ORANGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 7.95, 4.0, r=0.22)
    ax.text(9.05, 4.0, 'payments\n.validated', ha='center', va='center',
            fontsize=9, color=GRIS)
    ax.text(8.9, 3.35, 'validées, ISO-compliant', ha='center', va='center',
            fontsize=7.5, color='#666')

    arrow(ax, 8.9, 3.1, 8.9, 2.25, color=VERT, lw=2)

    # Job3 + Silver
    rounded_box(ax, 5.0, 1.3, 3.0, 1.6, LVERT, edgecolor=VERT, linewidth=2, zorder=2)
    draw_flink_logo(ax, 5.6, 2.15, r=0.22)
    ax.text(6.75, 2.15, 'Job 3 — Normalize', ha='center', va='center',
            fontsize=9, color=VERT, fontweight='bold')
    ax.text(6.5, 1.55, 'Normalisation +\nMinIO Silver', ha='center', va='center',
            fontsize=7.5, color='#555')

    # Silver MinIO
    arrow(ax, 5.0, 2.1, 3.5, 2.1, color=SILVER, lw=2)
    ax.text(4.25, 2.35, 'write', ha='center', fontsize=8, color=SILVER, style='italic')
    rounded_box(ax, 0.3, 1.5, 2.8, 1.2, '#ECEFF1', edgecolor=SILVER, linewidth=2, zorder=2)
    draw_minio_logo(ax, 1.7, 2.0, w=0.5, h=0.2)
    ax.text(1.7, 1.75, 'MinIO Silver\nsilver/', ha='center', va='center',
            fontsize=8.5, color=SILVER)

    # Topic normalized
    rounded_box(ax, 7.3, 1.3, 3.2, 1.6, '#FFF3E0', edgecolor=ORANGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 7.95, 2.15, r=0.22)
    ax.text(9.05, 2.15, 'payments\n.normalized', ha='center', va='center',
            fontsize=9, color=GRIS)

    # Job4 + Gold
    arrow(ax, 10.5, 2.1, 11.2, 2.1, color=VERT, lw=2)
    rounded_box(ax, 11.2, 1.3, 3.0, 1.6, LVERT, edgecolor=VERT, linewidth=2, zorder=2)
    draw_flink_logo(ax, 11.8, 2.15, r=0.22)
    ax.text(12.95, 2.15, 'Job 4 — Optimize', ha='center', va='center',
            fontsize=9, color=VERT, fontweight='bold')
    ax.text(12.7, 1.55, 'Dédup + Risk Score\nMinIO Gold + PG', ha='center', va='center',
            fontsize=7.5, color='#555')

    # Gold MinIO
    arrow(ax, 14.2, 2.5, 15.5, 2.8, color=JAUNE, lw=2)
    rounded_box(ax, 15.5, 2.2, 2.8, 1.2, '#FFFDE7', edgecolor=JAUNE, linewidth=2, zorder=2)
    draw_minio_logo(ax, 16.9, 2.75, w=0.5, h=0.2)
    ax.text(16.9, 2.45, 'MinIO Gold\ngold/', ha='center', va='center',
            fontsize=8.5, color='#F57F17')

    # Bronze MinIO (from Job1)
    arrow(ax, 7.3, 9.0, 4.5, 9.0, color=BRONZE, lw=1.5)
    ax.text(5.9, 9.25, 'write', ha='center', fontsize=8, color=BRONZE, style='italic')
    rounded_box(ax, 1.5, 8.4, 2.8, 1.2, '#EFEBE9', edgecolor=BRONZE, linewidth=2, zorder=2)
    draw_minio_logo(ax, 2.9, 9.0, w=0.5, h=0.2)
    ax.text(2.9, 8.65, 'MinIO Bronze\nbronze/', ha='center', va='center',
            fontsize=8.5, color=BRONZE)

    # PostgreSQL gold_transactions
    arrow(ax, 14.2, 1.9, 15.5, 1.4, color=BLEU, lw=2)
    rounded_box(ax, 15.5, 0.7, 2.8, 1.4, LBLEU, edgecolor=BLEU, linewidth=2, zorder=2)
    draw_postgres_logo(ax, 16.1, 1.35, r=0.25)
    ax.text(17.0, 1.35, 'gold_transactions\nPostgreSQL', ha='center', va='center',
            fontsize=8.5, color=BLEU)

    # Prometheus / Grafana
    arrow(ax, 16.9, 0.7, 16.9, 0.35, color=TEAL, lw=1.5)
    rounded_box(ax, 14.7, 0.05, 1.7, 0.6, '#E0F2F1', edgecolor=TEAL, linewidth=1.5, zorder=2)
    draw_prometheus_logo(ax, 15.1, 0.35, r=0.18)
    ax.text(15.7, 0.35, 'Prometheus', ha='center', va='center', fontsize=8, color=TEAL)

    arrow(ax, 16.4, 0.35, 17.1, 0.35, color='#F46800', lw=1.5)
    rounded_box(ax, 17.1, 0.05, 1.7, 0.6, '#FFF3E0', edgecolor='#F46800', linewidth=1.5, zorder=2)
    draw_grafana_logo(ax, 17.5, 0.35, r=0.18)
    ax.text(18.1, 0.35, 'Grafana', ha='center', va='center', fontsize=8, color='#F46800')

    # Legend
    legend_items = [
        mpatches.Patch(color=ORANGE, label='Topic Kafka'),
        mpatches.Patch(color=VERT, label='Job Flink'),
        mpatches.Patch(color=BRONZE, label='MinIO Bronze'),
        mpatches.Patch(color=SILVER, label='MinIO Silver'),
        mpatches.Patch(color=JAUNE, label='MinIO Gold'),
        mpatches.Patch(color=ROUGE, label='Dead Letter Queue'),
        mpatches.Patch(color=BLEU, label='PostgreSQL'),
    ]
    ax.legend(handles=legend_items, loc='lower left', fontsize=8,
              ncol=4, framealpha=0.9, bbox_to_anchor=(0.0, 0.0))

    plt.tight_layout()
    path = os.path.join(OUT, 'flux_donnees.png')
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    sz = os.path.getsize(path) // 1024
    print(f"✓ flux_donnees.png  ({sz} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — Architecture Technique (5 colonnes)
# ═══════════════════════════════════════════════════════════════════════════════

def fig_archi_technique():
    fig, ax = plt.subplots(figsize=(20, 11))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 11)
    ax.axis('off')
    fig.patch.set_facecolor('white')

    ax.text(10, 10.55, "Architecture Technique — SWAM Real-Time Payments PoC",
            ha='center', va='center', fontsize=16, fontweight='bold', color=VERT)
    ax.axhline(10.25, color=VERT, linewidth=2, xmin=0.02, xmax=0.98)

    def _logo(fn, ax, cx, cy, r):
        """Call any logo fn, mapping r to w/h for flat-rect logos."""
        if fn in (draw_debezium_logo, draw_minio_logo):
            fn(ax, cx, cy, w=r*2.2, h=r*0.88)
        else:
            fn(ax, cx, cy, r=r)

    columns = [
        # (x_start, width, bg_color, header_color, title, components, namespace)
        (0.2, 3.5, '#EDE7F6', VIOLET, 'SOURCE',
         [('Debezium', draw_debezium_logo, VIOLET),
          ('Producer\nPython', draw_python_logo, BLEU)],
         'ns: kafka-connect'),
        (3.9, 3.5, '#FFF3E0', ORANGE, 'KAFKA',
         [('6 Topics\nKafka', draw_kafka_logo, ORANGE),
          ('Strimzi\nOperator', draw_k8s_logo, BLEU)],
         'ns: kafka'),
        (7.6, 3.5, '#E8F5E9', VERT, 'FLINK',
         [('4 Flink\nJobs', draw_flink_logo, VERT),
          ('Job Manager\n+ Task Mgr', draw_flink_logo, '#2E7D32')],
         'ns: flink'),
        (11.3, 3.5, '#FFF8E1', '#8D6000', 'MINIO',
         [('MinIO S3\n3 couches', draw_minio_logo, ROUGE),
          ('Bronze / Silver\n/ Gold', draw_minio_logo, '#5D4037')],
         'ns: minio'),
        (15.0, 4.7, '#E3F2FD', BLEU, 'POSTGRES + MONITORING',
         [('PostgreSQL\ngold_transactions', draw_postgres_logo, BLEU),
          ('Prometheus\n+ Grafana', draw_prometheus_logo, TEAL)],
         'ns: kafka-connect\n+ monitoring'),
    ]

    for (x, w, bg, hdr, title, comps, ns) in columns:
        # outer box
        outer = FancyBboxPatch((x, 0.5), w, 9.5,
                               boxstyle="round,pad=0.05",
                               facecolor=bg, edgecolor=hdr,
                               linewidth=3, zorder=2)
        ax.add_patch(outer)
        # header
        rounded_box(ax, x, 9.25, w, 0.75, hdr, label=title,
                    fontsize=12, zorder=4)
        # namespace
        ax.text(x + w/2, 0.75, ns, ha='center', va='center',
                fontsize=8, color='#666', style='italic')

        # components
        y_pos = [7.8, 5.2]
        for i, (name, logo_fn, color) in enumerate(comps):
            cy = y_pos[i]
            # component box
            comp_box = FancyBboxPatch((x+0.25, cy-0.9), w-0.5, 1.8,
                                      boxstyle="round,pad=0.05",
                                      facecolor='white', edgecolor=color,
                                      linewidth=2, alpha=0.9, zorder=3)
            ax.add_patch(comp_box)
            _logo(logo_fn, ax, x + 0.75, cy + 0.35, r=0.28 if 'minio' not in name.lower() else 0.25)
            ax.text(x + w/2 + 0.1, cy + 0.35, name, ha='center', va='center',
                    fontsize=10, color=GRIS, fontweight='bold')

        # separator
        ax.plot([x+0.3, x+w-0.3], [4.5, 4.5], color=hdr, lw=1, ls='--', alpha=0.5)

    # Arrows between columns
    for x_arr, y_arr in [(3.7, 7.2), (7.4, 7.2), (11.1, 7.2), (14.8, 7.2)]:
        ax.annotate('', xy=(x_arr, y_arr), xytext=(x_arr - 0.1, y_arr),
                    arrowprops=dict(arrowstyle='->', color=GRIS, lw=2,
                                    mutation_scale=14))
    for x_arr, y_arr in [(3.7, 5.0), (7.4, 5.0), (11.1, 5.0), (14.8, 5.0)]:
        ax.annotate('', xy=(x_arr, y_arr), xytext=(x_arr - 0.1, y_arr),
                    arrowprops=dict(arrowstyle='->', color=GRIS, lw=2,
                                    mutation_scale=14))

    # K8s label
    k8s_box = FancyBboxPatch((0.1, 0.1), 19.8, 10.1,
                             boxstyle="round,pad=0.05",
                             facecolor='none', edgecolor=BLEU,
                             linewidth=2, linestyle='dashed', zorder=1)
    ax.add_patch(k8s_box)
    draw_k8s_logo(ax, 0.55, 0.5, r=0.2)
    ax.text(1.0, 0.5, 'Minikube — 1 Nœud (6 vCPU / 6 Go RAM)', ha='left', va='center',
            fontsize=9, color=BLEU)

    plt.tight_layout()
    path = os.path.join(OUT, 'archiTechnique.png')
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    sz = os.path.getsize(path) // 1024
    print(f"✓ archiTechnique.png  ({sz} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 3 — Architecture Détaillée
# ═══════════════════════════════════════════════════════════════════════════════

def fig_archi_detaillee():
    fig, ax = plt.subplots(figsize=(22, 14))
    ax.set_xlim(0, 22)
    ax.set_ylim(0, 14)
    ax.axis('off')
    fig.patch.set_facecolor('white')

    ax.text(11, 13.5, "Architecture Détaillée — SWAM Real-Time Payments PoC",
            ha='center', va='center', fontsize=16, fontweight='bold', color=VERT)
    ax.axhline(13.2, color=VERT, linewidth=2, xmin=0.02, xmax=0.98)

    def detail_box(ax, x, y, w, h, bg, border, header, header_color, items, item_color='#333'):
        # outer
        outer = FancyBboxPatch((x, y), w, h,
                               boxstyle="round,pad=0.05",
                               facecolor=bg, edgecolor=border, linewidth=3, zorder=2)
        ax.add_patch(outer)
        # header
        rounded_box(ax, x, y+h-0.55, w, 0.55, header_color, label=header,
                    fontsize=12, zorder=4)
        # items
        step = (h - 0.6) / (len(items) + 0.5)
        for j, item in enumerate(items):
            iy = y + h - 0.75 - (j+0.5)*step
            ax.text(x + 0.18, iy, '▸', ha='left', va='center',
                    fontsize=9, color=border)
            ax.text(x + 0.45, iy, item, ha='left', va='center',
                    fontsize=8.5, color=item_color)

    # SOURCE
    detail_box(ax, 0.2, 0.5, 4.1, 12.5, '#EDE7F6', VIOLET, 'SOURCE', VIOLET, [
        'Debezium Connector v2.7',
        '└ transactions table',
        '└ logical replication',
        '',
        'Producer Python (simulator)',
        '└ 100 tx/min (configurable)',
        '└ AES-256 encrypted payload',
        '└ ISO 8583 format',
        '',
        'kafka-connect namespace',
        'PostgreSQL SWAM :5432',
        '└ table: transactions',
    ])
    draw_debezium_logo(ax, 1.1, 12.4, w=0.6, h=0.25)
    draw_python_logo(ax, 2.1, 12.4, r=0.22)

    # KAFKA
    detail_box(ax, 4.6, 0.5, 4.1, 12.5, '#FFF3E0', ORANGE, 'KAFKA / STRIMZI', ORANGE, [
        'Strimzi Operator 0.51.0',
        'KRaft Mode (no ZooKeeper)',
        '1 Broker · 3 partitions',
        '',
        '6 Topics :',
        '  payments             (raw AES)',
        '  payments.decrypted   (clear)',
        '  payments.validated   (ISO OK)',
        '  payments.normalized  (normal.)',
        '  payments.gold        (enriched)',
        '  payments.dlq         (rejects)',
        'Rétention : 7 jours',
    ])
    draw_kafka_logo(ax, 5.5, 12.4, r=0.22)

    # FLINK
    detail_box(ax, 9.0, 0.5, 4.1, 12.5, '#E8F5E9', VERT, 'FLINK JOBS', VERT, [
        'Apache Flink 1.19.1',
        'PyFlink DataStream API',
        '1 JobManager · 3 TaskManagers',
        '',
        'Job 1 — Decrypt',
        '  AES-256 déchiffrement',
        '  DLQ si échec crypto',
        '',
        'Job 2 — Validate',
        '  ISO 8583 / 4217 / 7812',
        '  DLQ si données invalides',
        '',
        'Job 3 — Normalize',
        '  Normalisation champs',
        '  → Silver MinIO',
        '',
        'Job 4 — Optimize',
        '  Déduplication',
        '  Risk scoring (LOW/MED/HIGH)',
        '  → Gold MinIO + PostgreSQL',
    ])
    draw_flink_logo(ax, 9.85, 12.4, r=0.22)

    # MINIO
    detail_box(ax, 13.4, 0.5, 4.1, 12.5, '#FFF8E1', '#8D6000', 'MINIO MEDALLION', '#8D6000', [
        'MinIO Single Node',
        'Bucket : rt-payments',
        '',
        '▸ Bronze  (bronze/)',
        '   Format : JSON',
        '   Source : Job 1 (décrypté)',
        '   ~50 Ko/tx',
        '',
        '▸ Silver  (silver/)',
        '   Format : JSON',
        '   Source : Job 3 (normalisé)',
        '   ~45 Ko/tx',
        '',
        '▸ Gold    (gold/)',
        '   Format : JSON',
        '   Source : Job 4 (enrichi)',
        '   ~60 Ko/tx (risk + canal)',
    ])
    draw_minio_logo(ax, 14.3, 12.4, w=0.55, h=0.22)

    # MONITORING
    detail_box(ax, 17.7, 0.5, 4.1, 12.5, '#E3F2FD', BLEU, 'MONITORING', BLEU, [
        'Prometheus 2.x  :9090',
        '└ node-exporter',
        '└ kube-state-metrics',
        '└ cAdvisor',
        '└ hps_exporter :8888',
        '└ Flink reporter',
        '',
        'Grafana 11.x  :3000',
        '8 Dashboards :',
        '  Executive Overview',
        '  Kafka Monitoring',
        '  Flink Monitoring',
        '  MinIO Monitoring',
        '  K8s Cluster',
        '  Business Analytics',
        '  Data Quality',
        '  Business & Data Quality',
        '',
        'PostgreSQL gold_transactions',
        '└ UPSERT via gold-sink',
        '└ ~10K rows',
    ])
    draw_prometheus_logo(ax, 18.65, 12.4, r=0.22)
    draw_grafana_logo(ax, 19.5, 12.4, r=0.22)

    # Arrows between columns
    for xa in [4.3, 8.8, 13.2, 17.5]:
        ax.annotate('', xy=(xa, 6.75), xytext=(xa - 0.2, 6.75),
                    arrowprops=dict(arrowstyle='->', color=GRIS, lw=2,
                                    mutation_scale=16))

    # K8s border
    k8s_box = FancyBboxPatch((0.1, 0.1), 21.8, 13.1,
                             boxstyle="round,pad=0.05",
                             facecolor='none', edgecolor='#1976D2',
                             linewidth=2, linestyle='dashed', zorder=1)
    ax.add_patch(k8s_box)
    draw_k8s_logo(ax, 0.55, 0.3, r=0.18)
    ax.text(1.0, 0.3, 'Minikube (Docker driver) — WSL2 Ubuntu 24.04',
            ha='left', va='center', fontsize=9, color=BLEU)

    plt.tight_layout()
    path = os.path.join(OUT, 'archiDetaillee.png')
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    sz = os.path.getsize(path) // 1024
    print(f"✓ archiDetaillee.png  ({sz} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 4 — Architecture Medallion
# ═══════════════════════════════════════════════════════════════════════════════

def fig_medallion():
    fig, ax = plt.subplots(figsize=(18, 10))
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 10)
    ax.axis('off')
    fig.patch.set_facecolor('white')

    ax.text(9, 9.55, "Architecture Medallion — MinIO Bronze / Silver / Gold",
            ha='center', va='center', fontsize=15, fontweight='bold', color=VERT)
    ax.axhline(9.25, color=VERT, linewidth=2, xmin=0.02, xmax=0.98)

    def medallion_layer(ax, x, y, w, h, color, title, subtitle, written_by, format_str, content_items):
        outer = FancyBboxPatch((x, y), w, h,
                               boxstyle="round,pad=0.08",
                               facecolor=color + '22', edgecolor=color,
                               linewidth=3, zorder=2)
        ax.add_patch(outer)
        # header
        rounded_box(ax, x, y+h-0.65, w, 0.65, color, label=title, fontsize=14, zorder=4)
        ax.text(x + w/2, y+h-0.9, subtitle, ha='center', va='center',
                fontsize=9, color='#555', style='italic')
        # "Écrit par"
        rounded_box(ax, x+0.2, y+h-1.55, w-0.4, 0.4, color+'44',
                    label=f'Écrit par : {written_by}', fontsize=9,
                    label_color=GRIS, zorder=3)
        # format
        rounded_box(ax, x+0.2, y+h-2.1, w-0.4, 0.4, color+'33',
                    label=f'Format : {format_str}', fontsize=9,
                    label_color=GRIS, zorder=3)
        # content
        for i, item in enumerate(content_items):
            iy = y + h - 2.7 - i * 0.55
            ax.text(x + 0.35, iy, '• ' + item, ha='left', va='center',
                    fontsize=8.5, color=GRIS)

    # Bronze
    medallion_layer(ax, 0.3, 1.0, 4.5, 7.8, BRONZE, 'BRONZE',
                    'Données brutes déchiffrées', 'Job 1 — Decrypt', 'JSON',
                    ['Payload AES-256 déchiffré',
                     'Champs bruts non validés',
                     'Timestamp ingestion',
                     'Transaction ID (original)',
                     'Chemin: bronze/{date}/tx.json',
                     'Rétention: illimitée',
                     'Taille moy: ~50 Ko/tx'])
    draw_minio_logo(ax, 1.5, 7.95, w=0.55, h=0.22)

    # Arrow bronze → silver
    arrow(ax, 4.8, 5.0, 5.6, 5.0, color=SILVER, lw=3)
    ax.text(5.2, 5.35, 'Normalisation', ha='center', fontsize=8.5, color=SILVER, fontweight='bold')

    # Silver
    medallion_layer(ax, 5.6, 1.0, 4.5, 7.8, SILVER, 'SILVER',
                    'Données validées et normalisées', 'Job 3 — Normalize', 'JSON',
                    ['Validation ISO 8583/4217/7812',
                     'Champs normalisés (snake_case)',
                     'Montant converti en centimes',
                     'Devise validée (ISO 4217)',
                     'BIC normalisé (ISO 7812)',
                     'Chemin: silver/{date}/tx.json',
                     'Taille moy: ~45 Ko/tx'])
    draw_minio_logo(ax, 6.75, 7.95, w=0.55, h=0.22)

    # Arrow silver → gold
    arrow(ax, 10.1, 5.0, 10.9, 5.0, color=JAUNE, lw=3)
    ax.text(10.5, 5.35, 'Enrichissement', ha='center', fontsize=8.5,
            color='#F57F17', fontweight='bold')

    # Gold
    medallion_layer(ax, 10.9, 1.0, 4.5, 7.8, JAUNE, 'GOLD',
                    'Données enrichies et dédupliquées', 'Job 4 — Optimize', 'JSON',
                    ['Déduplication (tx_id unique)',
                     'Risk Score: LOW/MED/HIGH',
                     'Canal paiement détecté',
                     'Enrichissement ISO complet',
                     'Chemin: gold/{date}/tx.json',
                     'Taille moy: ~60 Ko/tx',
                     '→ Sink vers PostgreSQL'])
    draw_minio_logo(ax, 12.05, 7.95, w=0.55, h=0.22)

    # Arrow gold → PostgreSQL
    arrow(ax, 13.15, 1.0, 13.15, 0.55, color=BLEU, lw=2.5)
    rounded_box(ax, 10.9, 0.05, 4.5, 0.9, LBLEU, edgecolor=BLEU, linewidth=2, zorder=2)
    draw_postgres_logo(ax, 11.5, 0.5, r=0.22)
    ax.text(13.15, 0.5, 'gold_transactions  PostgreSQL\nUPSERT via gold-sink Kafka Connect',
            ha='center', va='center', fontsize=9, color=BLEU)

    # DLQ panel (bottom left)
    dlq_box = FancyBboxPatch((0.3, 0.05), 9.8, 0.85,
                             boxstyle="round,pad=0.05",
                             facecolor='#FFEBEE', edgecolor=ROUGE, linewidth=2.5, zorder=2)
    ax.add_patch(dlq_box)
    draw_kafka_logo(ax, 0.85, 0.5, r=0.2)
    ax.text(5.2, 0.5, 'Dead Letter Queue — payments.dlq\n'
                       'Job 1 (déchiffrement échoué) + Job 2 (validation ISO échouée)',
            ha='center', va='center', fontsize=9, color=ROUGE)

    # Kafka input
    rounded_box(ax, 0.3, 8.9, 17.4, 0.65, '#FFF3E0', edgecolor=ORANGE, linewidth=2, zorder=2)
    draw_kafka_logo(ax, 0.9, 9.22, r=0.2)
    ax.text(9, 9.22, 'Input : Topic Kafka — payments / payments.decrypted / payments.validated / payments.normalized',
            ha='center', va='center', fontsize=9.5, color=ORANGE)

    # Arrows from Kafka band
    for xa in [2.55, 7.85, 13.15]:
        arrow(ax, xa, 8.9, xa, 8.8, color=ORANGE, lw=2)

    plt.tight_layout()
    path = os.path.join(OUT, 'medallion.png')
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    sz = os.path.getsize(path) // 1024
    print(f"✓ medallion.png  ({sz} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 5 — Déploiement K8s
# ═══════════════════════════════════════════════════════════════════════════════

def fig_deploiement():
    fig, ax = plt.subplots(figsize=(20, 13))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 13)
    ax.axis('off')
    fig.patch.set_facecolor('white')

    ax.text(10, 12.55, "Diagramme de Déploiement — Infrastructure Kubernetes SWAM",
            ha='center', va='center', fontsize=15, fontweight='bold', color=VERT)
    ax.axhline(12.25, color=VERT, linewidth=2, xmin=0.02, xmax=0.98)

    # ── Outer: WSL2 Host ──────────────────────────────────────────────────────
    wsl2 = FancyBboxPatch((0.15, 0.1), 19.7, 12.0,
                          boxstyle="round,pad=0.08",
                          facecolor='#FAFAFA', edgecolor='#37474F',
                          linewidth=3, zorder=1)
    ax.add_patch(wsl2)
    ax.text(0.55, 12.0, '[HOST]  Machine Hôte — WSL2 Ubuntu 24.04  (6 vCPU / 16 Go RAM)',
            ha='left', va='center', fontsize=10, color='#37474F', fontweight='bold')

    # Host processes (top)
    host_procs = [
        (0.4, 10.8, 3.0, 0.9, draw_python_logo, 'producer.py\n100 tx/min', '#E8EAF6', '#3949AB'),
        (3.7, 10.8, 3.5, 0.9, draw_python_logo, 'hps_exporter.py\n:8888 Prometheus', '#E8EAF6', '#3949AB'),
        (7.5, 10.8, 3.0, 0.9, draw_terraform_logo, 'Terraform\nIaC provisioning', '#F3E5F5', VIOLET),
    ]
    for (hx, hy, hw, hh, logo_fn, label, bg, color) in host_procs:
        b = FancyBboxPatch((hx, hy), hw, hh, boxstyle="round,pad=0.05",
                           facecolor=bg, edgecolor=color, linewidth=2, zorder=3)
        ax.add_patch(b)
        logo_fn(ax, hx + 0.45, hy + hh/2, r=0.2)
        ax.text(hx + hw/2 + 0.15, hy + hh/2, label, ha='center', va='center',
                fontsize=9, color=GRIS)

    # ── Inner: Minikube node ──────────────────────────────────────────────────
    mink = FancyBboxPatch((0.3, 0.2), 19.4, 10.4,
                          boxstyle="round,pad=0.08",
                          facecolor='#F1F8FF', edgecolor=BLEU,
                          linewidth=2.5, linestyle='dashed', zorder=2)
    ax.add_patch(mink)
    draw_k8s_logo(ax, 0.75, 10.4, r=0.22)
    ax.text(1.15, 10.4,
            'Nœud Minikube  (driver: Docker)  —  6 vCPU / 6 Go RAM alloués',
            ha='left', va='center', fontsize=9.5, color=BLEU, fontweight='bold')

    def ns_box(ax, x, y, w, h, color, title, pods):
        """Namespace box with pod list."""
        outer = FancyBboxPatch((x, y), w, h,
                               boxstyle="round,pad=0.06",
                               facecolor=color + '18', edgecolor=color,
                               linewidth=2.5, zorder=3)
        ax.add_patch(outer)
        hdr = FancyBboxPatch((x, y+h-0.52), w, 0.52,
                             boxstyle="square,pad=0",
                             facecolor=color, edgecolor='none', zorder=4)
        ax.add_patch(hdr)
        ax.text(x + w/2, y+h-0.26, title, ha='center', va='center',
                fontsize=10, fontweight='bold', color='white', zorder=5)
        step = (h - 0.62) / (len(pods) + 0.3)
        for i, (pod, port) in enumerate(pods):
            py = y + h - 0.75 - i * step
            circ_s = Circle((x + 0.35, py), 0.1,
                            facecolor=color, edgecolor='none', zorder=5)
            ax.add_patch(circ_s)
            ax.text(x + 0.55, py, pod, ha='left', va='center',
                    fontsize=8.5, color='#333', zorder=5)
            if port:
                ax.text(x + w - 0.15, py, port, ha='right', va='center',
                        fontsize=7.5, color=color, style='italic', zorder=5)

    # ── ns: kafka ─────────────────────────────────────────────────────────────
    ns_box(ax, 0.5, 4.8, 4.5, 5.2, ORANGE, 'ns: kafka', [
        ('Strimzi Operator', ''),
        ('my-cluster-kafka-0', ':9092'),
        ('my-cluster-entity-op.', ''),
        ('KRaft Controller', ':9093'),
        ('6 KafkaTopics', ''),
        ('3 Partitions / topic', ''),
    ])
    draw_kafka_logo(ax, 1.2, 9.55, r=0.28)

    # ── ns: flink ─────────────────────────────────────────────────────────────
    ns_box(ax, 5.2, 4.8, 4.5, 5.2, VERT, 'ns: flink', [
        ('flink-jobmanager', ':8081'),
        ('flink-taskmanager-0', ''),
        ('flink-taskmanager-1', ''),
        ('flink-taskmanager-2', ''),
        ('Job1: Decrypt', ''),
        ('Job2: Validate', ''),
        ('Job3: Normalize', ''),
        ('Job4: Optimize', ''),
    ])
    draw_flink_logo(ax, 5.9, 9.55, r=0.28)

    # ── ns: minio ─────────────────────────────────────────────────────────────
    ns_box(ax, 9.9, 4.8, 4.5, 5.2, ROUGE, 'ns: minio', [
        ('minio-0', ':9000'),
        ('Console Web', ':9001'),
        ('Bucket: rt-payments', ''),
        ('  bronze/', ''),
        ('  silver/', ''),
        ('  gold/', ''),
    ])
    draw_minio_logo(ax, 11.0, 9.55, w=0.6, h=0.22)

    # ── ns: kafka-connect ─────────────────────────────────────────────────────
    ns_box(ax, 0.5, 0.4, 4.5, 4.1, VIOLET, 'ns: kafka-connect', [
        ('Debezium Connector', ''),
        ('  ├ source: transactions', ''),
        ('gold-sink connector', ''),
        ('  └ sink: gold_tx', ''),
        ('PostgreSQL SWAM', ':5432'),
        ('PostgreSQL gold_tx', ':5432'),
    ])
    draw_debezium_logo(ax, 1.3, 3.35, w=0.55, h=0.22)
    draw_postgres_logo(ax, 2.4, 3.35, r=0.22)

    # ── ns: monitoring ────────────────────────────────────────────────────────
    ns_box(ax, 5.2, 0.4, 9.2, 4.1, TEAL, 'ns: monitoring', [
        ('Prometheus',               ':9090'),
        ('  ├ node-exporter',        ''),
        ('  ├ kube-state-metrics',   ''),
        ('  ├ cAdvisor',             ''),
        ('  └ hps_exporter (host)',  ':8888'),
        ('Grafana',                  ':3000'),
        ('  └ 8 dashboards SWAM',    ''),
    ])
    draw_prometheus_logo(ax, 5.9, 3.35, r=0.22)
    draw_grafana_logo(ax, 6.9, 3.35, r=0.22)

    # ── Network arrows ────────────────────────────────────────────────────────
    # producer → kafka
    ax.annotate('', xy=(2.75, 10.8), xytext=(2.75, 9.8),
                arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.5, mutation_scale=12))
    ax.text(3.1, 10.3, ':9092', fontsize=7.5, color=ORANGE, style='italic')

    # Kafka → Flink
    ax.annotate('', xy=(5.2, 7.4), xytext=(5.0, 7.4),
                arrowprops=dict(arrowstyle='->', color=GRIS, lw=1.5, mutation_scale=10))

    # Flink → MinIO
    ax.annotate('', xy=(9.9, 7.4), xytext=(9.7, 7.4),
                arrowprops=dict(arrowstyle='->', color=GRIS, lw=1.5, mutation_scale=10))

    # Flink → kafka-connect (gold-sink)
    ax.annotate('', xy=(3.0, 4.5), xytext=(6.5, 4.5),
                arrowprops=dict(arrowstyle='->', color=VIOLET, lw=1.5,
                                mutation_scale=10, connectionstyle='arc3,rad=-0.2'))
    ax.text(4.5, 4.9, 'UPSERT gold', fontsize=7.5, color=VIOLET, style='italic')

    # hps_exporter → Prometheus
    ax.annotate('', xy=(6.2, 4.5), xytext=(5.5, 10.8),
                arrowprops=dict(arrowstyle='->', color=TEAL, lw=1.5,
                                mutation_scale=10, connectionstyle='arc3,rad=0.3'))
    ax.text(4.2, 7.5, 'scrape\n:8888', fontsize=7.5, color=TEAL, style='italic')

    # Port-forward legend
    legend_items = [
        mpatches.Patch(color=ORANGE, label='ns: kafka  (:9092 KRaft)'),
        mpatches.Patch(color=VERT, label='ns: flink  (:8081 UI)'),
        mpatches.Patch(color=ROUGE, label='ns: minio  (:9000/:9001)'),
        mpatches.Patch(color=VIOLET, label='ns: kafka-connect  (:5432)'),
        mpatches.Patch(color=TEAL, label='ns: monitoring  (:9090/:3000)'),
    ]
    ax.legend(handles=legend_items, loc='upper right', fontsize=8.5,
              framealpha=0.95, bbox_to_anchor=(0.99, 0.98))

    plt.tight_layout()
    path = os.path.join(OUT, 'deploiement.png')
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    sz = os.path.getsize(path) // 1024
    print(f"✓ deploiement.png  ({sz} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print(f"Generating figures → {OUT}\n")
    fig_flux_donnees()
    fig_archi_technique()
    fig_archi_detaillee()
    fig_medallion()
    fig_deploiement()
    print("\nAll figures generated.")

    # Copy to /mnt/user-data/outputs/ if it exists
    alt = '/mnt/user-data/outputs'
    if os.path.isdir(alt):
        import shutil
        for f in ['flux_donnees.png', 'archiTechnique.png', 'archiDetaillee.png',
                  'medallion.png', 'deploiement.png']:
            shutil.copy(os.path.join(OUT, f), os.path.join(alt, f))
        print(f"Copied to {alt}")
