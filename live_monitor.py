# live_monitor.py
import os
import time
import json
from collections import deque
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# =================== CONFIG ===================
CSV_PATH = os.path.join("audit_out", "live_rollup.csv")
OUT_DIR = "audit_out"
LOG_TXT = os.path.join(OUT_DIR, "live_signals.log")
LOG_JSONL = os.path.join(OUT_DIR, "live_signals.jsonl")

# cortes a monitorar e limites de "k baixas" para ligar alerta
CUTS = [2.0, 5.0, 10.0, 20.0]
K_TRIGGERS = {  # ajuste livre
    2.0: 8,
    5.0: 15,
    10.0: 20,
    20.0: 25,
}

# janela de frequência (quantas últimas rodadas para % altas)
ROLL_WIN = 400

# intervalo entre leituras do arquivo
SLEEP_SEC = 2

# para ler incrementalmente:
CHUNK_READ_ROWS = 1000   # lê só o final do arquivo
# ==============================================

os.makedirs(OUT_DIR, exist_ok=True)

def tail_csv(path: str, n_rows: int) -> pd.DataFrame:
    """Lê apenas o final do CSV para ser rápido."""
    if not os.path.exists(path):
        return pd.DataFrame()
    # tentativa 1: ler inteiro se o arquivo ainda é pequeno
    try:
        df = pd.read_csv(path)
        if len(df) <= n_rows:
            return df
        return df.tail(n_rows).reset_index(drop=True)
    except Exception:
        # fallback: tenta engine python
        try:
            df = pd.read_csv(path, engine="python")
            if len(df) <= n_rows:
                return df
            return df.tail(n_rows).reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

def compute_runs(series_bool: np.ndarray) -> int:
    """retorna o tamanho do último run de False (baixas) antes do último valor."""
    # queremos run de 'multiplicador < cut' (baixas) terminando no fim da série
    if series_bool.size == 0:
        return 0
    k = 0
    for v in series_bool[::-1]:
        if v:  # True == baixa
            k += 1
        else:
            break
    return k

def log_event(event: dict):
    stamp = datetime.now(timezone.utc).isoformat()
    line = f"[{stamp}] {event['msg']}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps({**event, "time": stamp}, ensure_ascii=False) + "\n")

def fmt_pct(x):
    return f"{x*100:.2f}%"

def main():
    print("[monitor] iniciando… aguardando CSV do collector em:", CSV_PATH)
    last_size = -1
    last_alert = {}  # evita spams por corte

    while True:
        if not os.path.exists(CSV_PATH):
            time.sleep(SLEEP_SEC)
            continue

        size = os.path.getsize(CSV_PATH)
        if size == last_size:
            # nada mudou
            time.sleep(SLEEP_SEC)
            continue
        last_size = size

        df = tail_csv(CSV_PATH, CHUNK_READ_ROWS)
        if df.empty or "multiplicador" not in df.columns:
            time.sleep(SLEEP_SEC)
            continue

        # garante ordem temporal por 'ingested_at' se existir, senão por índice
        if "ingested_at" in df.columns:
            df = df.sort_values("ingested_at").reset_index(drop=True)

        mult = pd.to_numeric(df["multiplicador"], errors="coerce")
        mult = mult.dropna()
        if mult.empty:
            time.sleep(SLEEP_SEC)
            continue

        # janela para frequências
        win = mult.tail(ROLL_WIN).values

        print("\n" + "="*72)
        print(f"[monitor] registros lidos: {len(df)} | última={mult.iloc[-1]:.2f}x  | janela freq={min(len(win), ROLL_WIN)}")
        print("-"*72)

        for cut in CUTS:
            lows_mask = (mult < cut).values
            highs_mask = ~lows_mask

            # run de baixas no final
            k_run_low = compute_runs(lows_mask)

            # frequência de altas na janela
            w = (win >= cut)
            p_high = w.mean() if w.size else 0.0

            # impressão de painel
            dash = f"cut={cut:>4.1f}x | run_baixas={k_run_low:>3} | freq_altas({len(win):>3})={fmt_pct(p_high)}"
            print(dash)

            # regra simples de alerta
            trig = K_TRIGGERS.get(cut, None)
            if trig is not None and k_run_low >= trig:
                key = f"{cut}"
                msg = f"ALERTA cut={cut}x | run de baixas >= {trig} (k={k_run_low}) | freq_altas({len(win)})={fmt_pct(p_high)}"
                # só registra se mudou de estado (para não duplicar a cada loop)
                if last_alert.get(key) != k_run_low:
                    log_event({"cut": cut, "k_run_low": int(k_run_low), "p_high_win": float(p_high), "msg": msg})
                    last_alert[key] = k_run_low

        print("="*72)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()