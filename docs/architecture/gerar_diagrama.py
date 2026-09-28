"""
gerar_diagrama.py
------------------
Gera a imagem do desenho de arquitetura da pipeline (arquitetura.png),
para incluir na entrega zipada do Tech Challenge.
"""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from matplotlib.patches import ConnectionStyle

fig, ax = plt.subplots(figsize=(14, 7.5))
ax.set_xlim(0, 14)
ax.set_ylim(0, 7.5)
ax.axis("off")

COLOR_COMPUTE = "#FF9900"   # laranja AWS
COLOR_STORAGE = "#3B48CC"   # azul
COLOR_CATALOG = "#7AA116"   # verde
COLOR_QUERY = "#8C4FFF"     # roxo
COLOR_VIZ = "#232F3E"       # cinza escuro
COLOR_SRC = "#146EB4"       # azul claro

boxes = {}


def add_box(name, x, y, w, h, label, color, fontsize=10, text_color="white"):
    box = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.06,rounding_size=0.08",
        linewidth=1.4, edgecolor=color, facecolor=color, alpha=0.92,
    )
    ax.add_patch(box)
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
             fontsize=fontsize, color=text_color, weight="bold", wrap=True)
    boxes[name] = (x, y, w, h)


def add_arrow(a, b, text="", rad=0.0, color="#444444"):
    ax1, ay1, aw, ah = boxes[a]
    bx1, by1, bw, bh = boxes[b]
    start = (ax1 + aw, ay1 + ah / 2)
    end = (bx1, by1 + bh / 2)
    arrow = FancyArrowPatch(
        start, end, connectionstyle=ConnectionStyle(f"arc3,rad={rad}"),
        arrowstyle="-|>", mutation_scale=18, linewidth=1.6, color=color,
    )
    ax.add_patch(arrow)
    if text:
        mx, my = (start[0] + end[0]) / 2, (start[1] + end[1]) / 2 + 0.28
        ax.text(mx, my, text, ha="center", fontsize=8.5, color=color, style="italic")


# Linha 1 — Ingestão
add_box("fonte", 0.3, 5.6, 1.9, 1.2, "Site / API\nIpeadata\n(EIA366_PBRENT366)", COLOR_SRC)
add_box("scraper", 2.7, 5.6, 2.0, 1.2, "Scraper Python\n(AWS CloudShell)", COLOR_COMPUTE)
add_box("s3raw", 5.3, 5.6, 2.1, 1.2, "S3 - RAW\nparquet, partição diária\n(ano/mes/dia)", COLOR_STORAGE)

# Linha 2 — Transformação
add_box("glue", 5.3, 3.5, 2.1, 1.2, "AWS Glue Job\n(PySpark)\nmédia móvel 7d", COLOR_COMPUTE)
add_box("s3curated", 7.9, 3.5, 2.1, 1.2, "S3 - CURATED\nparquet\npartição diária", COLOR_STORAGE)
add_box("catalog", 5.3, 1.4, 2.1, 1.2, "Glue Data\nCatalog\n(db: default)", COLOR_CATALOG)

# Linha 3 — Consulta e visualização
add_box("athena", 10.5, 3.5, 1.9, 1.2, "Amazon\nAthena\n(SQL)", COLOR_QUERY)
add_box("viz", 10.5, 1.4, 1.9, 1.2, "Visualização\n(Python /\nQuickSight)", COLOR_VIZ)

# Setas
add_arrow("fonte", "scraper")
add_arrow("scraper", "s3raw")
add_arrow("s3raw", "glue", rad=-0.35)
add_arrow("glue", "s3curated")
add_arrow("glue", "catalog", rad=-0.3)
add_arrow("s3curated", "athena", rad=-0.2)
add_arrow("catalog", "athena", rad=0.35)
add_arrow("athena", "viz", rad=-0.2)

ax.text(7, 7.1, "Arquitetura da Pipeline Batch — Preço do Petróleo (Ipea)",
        ha="center", fontsize=15, weight="bold", color="#232F3E")
ax.text(7, 0.3, "Ingestão (scraping) → S3 Raw → Glue (transformação + catálogo) → S3 Curated → Athena → Visualização",
        ha="center", fontsize=9.5, color="#555555")

plt.tight_layout()
plt.savefig("arquitetura.png", dpi=200, bbox_inches="tight")
print("Diagrama salvo em arquitetura.png")
