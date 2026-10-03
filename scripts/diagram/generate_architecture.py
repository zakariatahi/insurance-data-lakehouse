"""Generate the project architecture diagram as an editable SVG."""

from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "screenshots" / "smart_claims_architecture.svg"

INK = "#243247"
MUTED = "#5B6B7D"
LINE = "#738297"
BLUE = "#1677D2"
BLUE_LIGHT = "#E8F4FF"
BRONZE = "#FFE7C7"
SILVER = "#EBF0F5"
GOLD = "#FFF0AF"
GREEN = "#E0F6E8"

svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="2400" height="1400" viewBox="0 0 2400 1400" role="img" aria-labelledby="title desc">',
    '<title id="title">Smart Claims insurance data lakehouse architecture</title>',
    '<desc id="desc">Azure SQL, Event Hubs, and Unity Catalog volumes feed Databricks bronze and silver tables. Gold views combine claim, customer, policy, and vehicle telematics data.</desc>',
    '<defs>',
    '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" markerHeight="9" orient="auto-start-reverse"><path d="M 1 1 L 9 5 L 1 9 z" fill="#66788D"/></marker>',
    '<filter id="shadow" x="-20%" y="-30%" width="140%" height="160%"><feDropShadow dx="0" dy="3" stdDeviation="6" flood-color="#20304A" flood-opacity="0.10"/></filter>',
    '<style>text{font-family:Segoe UI,Arial,sans-serif;fill:#243247}.title{font-size:44px;font-weight:750}.subtitle{font-size:20px;fill:#5B6B7D}.panel-title{font-size:25px;font-weight:750;letter-spacing:1.5px}.lane{font-size:15px;font-weight:700;letter-spacing:2px;fill:#8290A1}.box-title{font-size:23px;font-weight:720}.box-detail{font-size:16px;fill:#5B6B7D}.table{font-size:21px;font-weight:650}.table-small{font-size:18px;font-weight:650}.note{font-size:17px;fill:#5B6B7D}.footer{font-size:19px;font-weight:650}</style>',
    '</defs>',
]


def rect(x, y, w, h, fill, stroke="none", radius=18, width=1.5, dash=None, shadow=False):
    extra = ' filter="url(#shadow)"' if shadow else ""
    if dash:
        extra += f' stroke-dasharray="{dash}"'
    svg.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{width}"{extra}/>'
    )


def text(x, y, content, klass=None, anchor=None, fill=None, font_size=None):
    attrs = f' class="{klass}"' if klass else ""
    attrs += f' text-anchor="{anchor}"' if anchor else ""
    attrs += f' fill="{fill}"' if fill else ""
    attrs += f' style="font-size:{font_size}px"' if font_size else ""
    svg.append(f'<text x="{x}" y="{y}"{attrs}>{escape(content)}</text>')


def path(d, color=LINE, width=2.4, arrow=True, dash=None):
    attrs = ' marker-end="url(#arrow)"' if arrow else ""
    attrs += f' stroke-dasharray="{dash}"' if dash else ""
    svg.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linecap="round" stroke-linejoin="round"{attrs}/>' )


def box(x, y, w, h, title, detail, fill, stroke, accent, title_size=None):
    rect(x, y, w, h, fill, stroke, 17, 1.8, shadow=True)
    rect(x, y, 8, h, accent, radius=4)
    text(x + 24, y + 35, title, "box-title", font_size=title_size)
    if detail:
        text(x + 24, y + 62, detail, "box-detail")


def table(x, y, w, name, fill, stroke, small=False):
    rect(x, y, w, 62, fill, stroke, 14, 1.7, shadow=True)
    text(x + w / 2, y + 40, name, "table-small" if small else "table", anchor="middle")


# Canvas and title.
rect(0, 0, 2400, 1400, "#FFFFFF", radius=0)
text(42, 64, "Smart Claims", "title")
text(42, 102, "Azure insurance data lakehouse  •  ingestion to analytics", "subtitle")
rect(2030, 32, 320, 70, BLUE_LIGHT, "#9CCCF4", 19, 1.4)
text(2190, 77, "Microsoft Azure  +  Databricks", "footer", anchor="middle")

# Four large stages with reference-style dashed boundaries.
panels = [
    (40, 690, "DATA INGESTION", "#F5FAFF", "#A9CFEF"),
    (750, 365, "BRONZE", "#FFF8EF", "#E7C494"),
    (1135, 390, "SILVER", "#F7FAFC", "#C9D5DF"),
    (1545, 525, "GOLD", "#FFFCED", "#E7D178"),
]
for x, w, heading, fill, border in panels:
    rect(x, 145, w, 1070, fill, border, 29, 2.0, "12 10")
    text(x + w / 2, 191, heading, "panel-title", anchor="middle")
    path(f"M{x + 26} 215 H{x + w - 26}", border, 1.5, arrow=False)

# Horizontal guide lanes.
for y in (468, 824):
    path(f"M65 {y} H2045", "#D4DCE5", 1.5, arrow=False, dash="8 10")
text(70, 242, "STREAMING TELEMATICS", "lane")
text(70, 500, "AZURE SQL CHANGE INGESTION", "lane")
text(70, 855, "IMAGE AND METADATA INGESTION", "lane")

# Telematics ingestion path.
box(75, 280, 175, 88, "Parquet", "vehicle events", GREEN, "#9BCBAB", "#4BAE70")
box(282, 280, 158, 88, "Python replay", "batched sends", BLUE_LIGHT, "#9CCCF4", BLUE)
box(474, 280, 220, 88, "Event Hubs", "telematics hub", BLUE_LIGHT, "#9CCCF4", BLUE)
path("M250 324 H282")
path("M440 324 H474")
path("M694 324 H785")
table(785, 291, 295, "telematics", BRONZE, "#D9A967")
path("M1080 322 H1170")
table(1170, 291, 320, "telematics", SILVER, "#ABBBC9")
text(1330, 378, "timestamps • coordinates", "note", anchor="middle")
path("M1490 322 H1580")
box(1580, 268, 455, 105, "aggregated_telematics", "speed metrics • event count / vehicle", GOLD, "#D4BB53", "#E2B534")

# Azure SQL source and bronze/silver paths.
for y, label in ((525, "claims.csv"), (609, "customers.csv"), (693, "policies.csv")):
    table(75, y, 182, label, GREEN, "#9BCBAB", small=True)
    path(f"M257 {y + 31} H286", arrow=False)
path("M286 556 V724", arrow=False)
path("M286 640 H311")
box(311, 595, 168, 90, "Azure SQL", "source tables", BLUE_LIGHT, "#9CCCF4", BLUE)
path("M479 640 H514")
box(514, 595, 182, 90, "Lakeflow", "Connect", "#FFEDE8", "#F0A994", "#F17657")
path("M696 640 H764", arrow=False)
path("M764 557 V725", arrow=False)
for y, name in ((526, "claims"), (610, "customers"), (694, "policies")):
    path(f"M764 {y + 31} H785")
    table(785, y, 295, name, BRONZE, "#D9A967")
    path(f"M1080 {y + 31} H1170")
    table(1170, y, 320, name, SILVER, "#ABBBC9")
    path(f"M1490 {y + 31} H1532", arrow=False)
path("M1532 557 V725", arrow=False)
path("M1532 641 H1580")
box(1580, 588, 455, 105, "customer_claim_policy", "claims + policies + customers", GOLD, "#D4BB53", "#E2B534")

# Object storage route. Claim metadata has no silver/gold table in this repo.
table(75, 898, 182, "training images", GREEN, "#9BCBAB", small=True)
table(75, 1002, 182, "claim metadata", GREEN, "#9BCBAB", small=True)
path("M257 929 H286", arrow=False)
path("M257 1033 H286", arrow=False)
path("M286 929 V1033", arrow=False)
path("M286 981 H311")
box(311, 936, 168, 90, "UC Volume", "landing files", BLUE_LIGHT, "#9CCCF4", BLUE)
path("M479 981 H514")
box(514, 936, 182, 90, "Auto Loader", "files to tables", "#FFEDE8", "#F0A994", "#F17657")
path("M696 981 H764", arrow=False)
path("M764 929 V1033", arrow=False)
path("M764 929 H785")
table(785, 898, 295, "training_images", BRONZE, "#D9A967", small=True)
path("M764 1033 H785")
table(785, 1002, 295, "claim_images_meta", BRONZE, "#D9A967", small=True)
path("M1080 929 H1170")
table(1170, 898, 320, "training_images", SILVER, "#ABBBC9", small=True)
text(1330, 1001, "damage label extracted from filename", "note", anchor="middle")
rect(1170, 1050, 320, 80, "#FFFFFF", "#CED8E1", 14, 1.3)
text(1330, 1082, "Image data stops at silver", "table-small", anchor="middle")
text(1330, 1111, "No image model in this repository", "note", anchor="middle")

# Gold combination, with two explicit upstream dependencies.
path("M2035 320 H2060 V875 H1985 V909")
path("M1807 693 V909")
box(1580, 909, 455, 112, "customer_claim_policy_telematics", "joined on chassis_no", GOLD, "#D4BB53", "#E2B534", title_size=19)
path("M2035 965 H2110")
box(2110, 919, 245, 95, "Claims analytics", "gold view for queries", "#E9F7F1", "#A1D9BD", "#48AD7C")
text(1807, 1070, "Gold tables are materialized views", "note", anchor="middle")

# Platform band echoes the reference image and makes the catalog boundary explicit.
rect(750, 1240, 1320, 78, "#EAF5FF", "#8EC3EC", 18, 1.8)
text(1410, 1289, "Unity Catalog  •  smart_claims_catalog", "footer", anchor="middle")
rect(750, 1330, 1320, 42, "#FFF0EA", "#F3B09C", 13, 1.4)
text(1410, 1358, "Lakeflow Connect   •   Spark Declarative Pipelines   •   scheduled workflow", "footer", anchor="middle")

svg.append("</svg>")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text("\n".join(svg) + "\n", encoding="utf-8")
print(OUTPUT)
