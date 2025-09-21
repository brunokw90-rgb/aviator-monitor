# scripts/run_trends.py
# -- coding: utf-8 --

import math
from pathlib import Path
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# --------------------------
# Configurações principais
# --------------------------
CSV_PATH = Path("audit_out") / "dados.csv"         # entrada
OUT_DIR  = Path("audit_out") / "trend_out"         # saída
HIGH_THRESHOLD = 10.0                               # “alta” = multiplicador >= 10
WINDOW_W = 5                                       # janela futura: P(>=1 alta nas próximas W rodadas)
MAX_K = 15                                         # k máximo para as curvas condicionais


# --------------------------
# Utilidades
# --------------------------
def pick_numeric_series(df: pd.DataFrame) -> pd.Series:
    """Escolhe a coluna numérica (float/int) a usar. Prioriza 'valor' se existir."""
    if "valor" in df.columns:
        s = pd.to_numeric(df["valor"], errors="coerce")
        s = s.dropna().astype(float)
        if s.size > 0:
            return s.reset_index(drop=True)

    # tenta a primeira coluna numérica
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not num_cols:
        raise ValueError("Nenhuma coluna numérica encontrada em dados.csv.")
    s = pd.to_numeric(df[num_cols[0]], errors="coerce").dropna().astype(float)
    return s.reset_index(drop=True)


def to_binary_highs(x: pd.Series, thr: float) -> np.ndarray:
    """Converte série numérica em 0/1: 1 se valor >= thr (alta), 0 caso contrário."""
    return (x.values >= thr).astype(np.int8)


def runs_lengths(bits: np.ndarray, target: int) -> List[int]:
    """Comprimentos das sequências consecutivas de 'target' (0 ou 1) em 'bits'."""
    lens = []
    count = 0
    for b in bits:
        if b == target:
            count += 1
        else:
            if count > 0:
                lens.append(count)
            count = 0
    if count > 0:
        lens.append(count)
    return lens


def prob_at_least_one_high(next_window: np.ndarray) -> float:
    """
    Dada uma janela 0/1 representando as próximas W rodadas,
    retorna P(>=1 alta) = 1 - P(0 altas) = 1 - prod(1 - bit).
    """
    if next_window.size == 0:
        return np.nan
    # se a janela tem 1s e 0s, 1 - produto(1 - bit)
    return float(1.0 - np.prod(1 - next_window))


def conditional_curve(bits: np.ndarray, k: int, w: int, condition_on: int) -> Tuple[int, float]:
    """
    Calcula P(>=1 alta nas próximas w), condicionado a 'k' rodadas anteriores serem todas 'condition_on'.
    - condition_on = 0 → “k baixas seguidas”
    - condition_on = 1 → “k altas seguidas”
    Retorna: (amostras, probabilidade)
    """
    n = bits.size
    if n <= k + w:
        return 0, np.nan

    hits = 0.0
    count = 0

    for i in range(k, n - w):
        prev_k = bits[i - k:i]
        if prev_k.size == k and np.all(prev_k == condition_on):
            nxt = bits[i:i + w]      # próximas w
            p = prob_at_least_one_high(nxt)
            if not np.isnan(p):
                hits += p
                count += 1

    if count == 0:
        return 0, np.nan
    return count, float(hits / count)


def baseline_palta_em_w(bits: np.ndarray, w: int) -> float:
    """
    Baseline teórica sob independência:
    p = média de 'alta' (1's) → P(>=1 alta em w) = 1 - (1 - p)^w
    """
    p = float(bits.mean()) if bits.size > 0 else np.nan
    if np.isnan(p):
        return np.nan
    return 1.0 - (1.0 - p) ** w


def save_lineplot(xs: List[int], ys: List[float], baseline: float, title: str, xlabel: str, ylabel: str, path: Path):
    plt.figure()
    plt.plot(xs, ys, marker="o")
    if not (baseline is None or np.isnan(baseline)):
        plt.axhline(baseline, linestyle="--")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=120)
    plt.close()


def save_hist(lens: List[int], title: str, xlabel: str, path: Path, bins: int = 30):
    plt.figure()
    plt.hist(lens, bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("frequência")
    plt.grid(True)
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=120)
    plt.close()


# --------------------------
# Pipeline principal
# --------------------------
def main():
    # 1) Carrega CSV e escolhe a coluna numérica
    df = pd.read_csv(CSV_PATH)
    series = pick_numeric_series(df)

    # 2) Converte em 0/1 conforme threshold
    bits = to_binary_highs(series, HIGH_THRESHOLD)

    # 3) Baseline: P(>=1 alta em W) sob independência
    base = baseline_palta_em_w(bits, WINDOW_W)

    # 4) Curvas condicionais para k = 2..MAX_K
    ks = list(range(2, MAX_K + 1))

    # 4.a) Condição: k baixas seguidas (0)
    lows_counts = []
    lows_probs = []
    for k in ks:
        count, prob = conditional_curve(bits, k=k, w=WINDOW_W, condition_on=0)
        lows_counts.append(count)
        lows_probs.append(prob)

    # 4.b) Condição: k altas seguidas (1)
    highs_counts = []
    highs_probs = []
    for k in ks:
        count, prob = conditional_curve(bits, k=k, w=WINDOW_W, condition_on=1)
        highs_counts.append(count)
        highs_probs.append(prob)

    # 5) Distribuição de tamanhos de runs (0 e 1)
    low_runs  = runs_lengths(bits, target=0)
    high_runs = runs_lengths(bits, target=1)

    # 6) Salva CSVs-resumo
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cond_df = pd.DataFrame({
        "k": ks,
        "n_cond_low": lows_counts,
        f"P(>=1 alta em {WINDOW_W} | k baixas)": lows_probs,
        "n_cond_high": highs_counts,
        f"P(>=1 alta em {WINDOW_W} | k altas)": highs_probs,
        "baseline_indep": [base] * len(ks),
    })
    cond_csv = OUT_DIR / "cond_curves.csv"
    cond_df.to_csv(cond_csv, index=False)

    runs_df = pd.DataFrame({
        "run_len_low": pd.Series(low_runs),
        "run_len_high": pd.Series(high_runs),
    })
    runs_csv = OUT_DIR / "runs_distribution.csv"
    runs_df.to_csv(runs_csv, index=False)

    # 7) Gera gráficos
    save_lineplot(
        xs=ks,
        ys=lows_probs,
        baseline=base,
        title=f"P(alta em {WINDOW_W}) | k baixas seguidas",
        xlabel="k baixas consecutivas",
        ylabel=f"P(alta em {WINDOW_W})",
        path=OUT_DIR / "cond_k_lows.png",
    )

    save_lineplot(
        xs=ks,
        ys=highs_probs,
        baseline=base,
        title=f"P(alta em {WINDOW_W}) | k altas seguidas",
        xlabel="k altas consecutivas",
        ylabel=f"P(alta em {WINDOW_W})",
        path=OUT_DIR / "cond_k_highs.png",
    )

    if len(low_runs) > 0:
        save_hist(
            lens=low_runs,
            title="Distribuição de tamanhos de runs (baixas)",
            xlabel="tamanho da sequência de baixas",
            path=OUT_DIR / "runs_low_hist.png",
            bins=50,
        )

    if len(high_runs) > 0:
        save_hist(
            lens=high_runs,
            title="Distribuição de tamanhos de runs (altas)",
            xlabel="tamanho da sequência de altas",
            path=OUT_DIR / "runs_high_hist.png",
            bins=50,
        )

    # 8) Log no console
    p_high = float(bits.mean()) if bits.size > 0 else float("nan")
    print(f"[Frequências globais] alta(>= {HIGH_THRESHOLD}): {p_high:.6f}")
    print(f"[Baseline indep] P(>=1 alta em {WINDOW_W}): {base:.6f}")
    print(f"[OK] CSVs: {cond_csv.name}, {runs_csv.name}")
    print(f"[OK] Gráficos: cond_k_lows.png, cond_k_highs.png, runs_low_hist.png, runs_high_hist.png")
    print(f"[OK] Pasta: {OUT_DIR}")


if __name__ == "__main__":
    main()