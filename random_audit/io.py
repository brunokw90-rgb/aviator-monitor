import re
from pathlib import Path
import csv

def sql_to_csv(sql_path, csv_path):
    text = Path(sql_path).read_text(encoding="utf-8", errors="ignore")
    rows = re.findall(r"\((\d+),\s*'([\d\.]+)'\s*,\s*'([^']+)'\)", text)
    out = Path(csv_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","valor","ts"])
        w.writerows(rows)
    return out, len(rows)