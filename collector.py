import os
import time
import json
import csv
import requests
from datetime import datetime, timezone

# ========= CONFIG =========
ENDPOINT_URL = "https://dashdocifrao.com.br/api/buscar_velas_cache.php?fonte=apostaganha&limite=120"
POLL_SECONDS = 1                         # intervalo entre coletas
OUT_DIR = os.path.join("audit_out")      # pasta de saída
RAW_JSONL = os.path.join(OUT_DIR, "live_raw.jsonl")
ROLLUP_CSV = os.path.join(OUT_DIR, "live_rollup.csv")
STATE_JSON = os.path.join(OUT_DIR, "collector_state.json")  # guarda últimas rodadas já vistas
TIMEOUT = 10
# ==========================

os.makedirs(OUT_DIR, exist_ok=True)

def load_state():
    if os.path.exists(STATE_JSON):
        try:
            with open(STATE_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"seen_ids": []}
    return {"seen_ids": []}

def save_state(state):
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_JSON)

def fetch_endpoint():
    r = requests.get(ENDPOINT_URL, timeout=TIMEOUT)
    r.raise_for_status()
    # a resposta que você mostrou é JSON “compacto” com campos pt-BR
    return r.json()

def normalize_item(item):
    """
    Converte um registro do endpoint para um dicionário padronizado.
    Campos esperados que vimos no print:
      - "multiplicador": "1.43x"
      - "rodada": 3108885
      - "date": "2025-09-20"
      - "end": "21:12:46"  (fim da rodada)
      - "minuto": "11"
      - "total": "1" (às vezes)
    """
    # valor do multiplicador como float, removendo 'x' e vírgulas
    mult_raw = str(item.get("multiplicador", "")).lower().replace("x", "").strip()
    try:
        multiplicador = float(mult_raw.replace(",", "."))
    except Exception:
        multiplicador = None

    rodada = str(item.get("rodada", "")).strip()
    data = str(item.get("date", "")).strip()
    hora_fim = str(item.get("end", "")).strip()

    # timestamp UTC aproximado (se vier só data/hora local, mantemos string)
    ts_iso = None
    if data and hora_fim:
        try:
            dt = datetime.fromisoformat(f"{data} {hora_fim}".replace("T", " "))
            # se não tiver timezone, não forçamos — apenas formatamos
            ts_iso = dt.isoformat()
        except Exception:
            ts_iso = None

    return {
        "rodada": rodada,
        "multiplicador": multiplicador,
        "multiplicador_str": item.get("multiplicador"),
        "date": data,
        "time_end": hora_fim,
        "timestamp": ts_iso,   # melhor para séries temporais depois
        "minuto": item.get("minuto"),
        "total": item.get("total"),
        "raw": item,           # guardamos o bruto por segurança
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

def append_jsonl(rows):
    with open(RAW_JSONL, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def append_csv(rows):
    # colunas estáveis para análises
    fieldnames = [
        "rodada", "timestamp", "date", "time_end",
        "multiplicador", "multiplicador_str", "minuto", "total", "ingested_at"
    ]
    file_exists = os.path.exists(ROLLUP_CSV)
    with open(ROLLUP_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

def main():
    state = load_state()
    seen = set(state.get("seen_ids", []))

    print(f"[collector] Iniciando. Saída em '{OUT_DIR}'. Intervalo={POLL_SECONDS}s")
    print(f"[collector] Endpoint: {ENDPOINT_URL}")

    while True:
        try:
            payload = fetch_endpoint()
            resultados = payload.get("resultados") or payload.get("results") or []

            # alguns endpoints vêm como lista de dicts dentro de uma string — tratamos os dois casos
            if isinstance(resultados, str):
                try:
                    resultados = json.loads(resultados)
                except Exception:
                    resultados = []

            novos = []
            for item in resultados:
                rid = str(item.get("rodada", "")).strip()
                if not rid:
                    continue
                if rid in seen:
                    continue

                row = normalize_item(item)
                novos.append(row)
                seen.add(rid)

            if novos:
                append_jsonl(novos)
                append_csv(novos)
                # mantemos só os últimos 100k ids na memória/arquivo de estado
                if len(seen) > 100_000:
                    seen = set(list(seen)[-80_000:])
                save_state({"seen_ids": list(seen)})
                print(f"[collector] +{len(novos)} novos | total_ids={len(seen)}")
            else:
                print("[collector] nenhum novo registro.")

        except requests.RequestException as e:
            print(f"[collector][http] erro: {e}")
        except Exception as e:
            print(f"[collector][fatal] {type(e)._name_}: {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()