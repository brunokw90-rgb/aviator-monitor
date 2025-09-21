# random_audit/trend.py
# ------------------------------------------------------------
# Ferramentas para:
#  - Classificar rodadas em baixa/média/alta
#  - Calcular probabilidades condicionais de “alta”
#  - Procurar padrões (ex.: K baixas seguidas) e medir
#    P(alta nas próximas H rodadas)
#  - Fazer uma simulação simples de “janelas deslizantes”
# ------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -------------------------
# 1) Classificação
# -------------------------

def to_categories(
    series: pd.Series,
    low_th: float = 2.0,
    high_th: float = 10.0
) -> np.ndarray:
    """
    Converte a série numérica em classes:
      0 = baixa  (< low_th)
      1 = média  [low_th, high_th)
      2 = alta   (>= high_th)
    """
    x = pd.to_numeric(series, errors="coerce").dropna().astype(float).values
    cats = np.zeros_like(x, dtype=int)
    cats[(x >= low_th) & (x < high_th)] = 1
    cats[x >= high_th] = 2
    return cats


# -------------------------
# 2) Probabilidades básicas
# -------------------------

def class_frequencies(cats: np.ndarray) -> Dict[int, float]:
    """Frequência relativa de cada classe {0,1,2}."""
    n = len(cats)
    return {c: float((cats == c).sum())/n for c in (0, 1, 2)}


def p_high_after_k_lows(
    cats: np.ndarray,
    k: int,
    horizon: int = 5
) -> Tuple[int, int, float]:
    """
    P(alta nas próximas 'horizon' rodadas | ocorreram K 'baixas' consecutivas)
    Retorna: (ocorrencias_padroes, sucessos, prob)
    """
    assert k >= 1 and horizon >= 1
    n = len(cats)
    hits = 0
    total = 0
    for i in range(k, n - horizon):
        # checa se os k últimos foram '0' (baixa):
        if np.all(cats[i-k:i] == 0):
            total += 1
            if np.any(cats[i:i+horizon] == 2):
                hits += 1
    prob = hits/total if total > 0 else np.nan
    return total, hits, prob


def p_high_after_pattern(
    cats: np.ndarray,
    pattern: Iterable[int],
    horizon: int = 5
) -> Tuple[int, int, float]:
    """
    P(alta nas próximas 'horizon' rodadas | padrão 'pattern' acabou de ocorrer)
    pattern: sequência de classes, ex.: [0,0,0,0] (4 baixas seguidas)
    """
    patt = np.array(list(pattern), dtype=int)
    m = len(patt)
    n = len(cats)
    hits = 0
    total = 0
    for i in range(m, n - horizon):
        if np.all(cats[i-m:i] == patt):
            total += 1
            if np.any(cats[i:i+horizon] == 2):
                hits += 1
    prob = hits/total if total > 0 else np.nan
    return total, hits, prob


# -------------------------
# 3) Janelas deslizantes
# -------------------------

@dataclass
class SlidingConfig:
    window: int = 50        # quantas últimas rodadas observar
    horizon: int = 5        # quantas futuras avaliar
    low_th: float = 2.0
    high_th: float = 10.0
    min_k_lows: int = 8     # “sinal” se houve pelo menos K baixas seguidas dentro da janela


@dataclass
class SlidingResult:
    positions: List[int]        # índices onde a janela gerou “sinal”
    hit: List[int]              # 1 se houve alta no horizonte, 0 se não
    prob_empirica: float        # média dos acertos
    total_sinais: int


def sliding_signal_k_lows(
    series: pd.Series,
    cfg: SlidingConfig = SlidingConfig()
) -> SlidingResult:
    """
    Estratégia simples: em cada ponto t, olhe a janela [t-window, t).
    Se existir algum trecho com pelo menos 'min_k_lows' baixas consecutivas,
    emitimos “sinal” e verificamos se haverá uma 'alta' (classe 2) nas próximas
    'horizon' rodadas.
    """
    cats = to_categories(series, cfg.low_th, cfg.high_th)
    n = len(cats)
    pos = []
    hits = []
    for t in range(cfg.window, n - cfg.horizon):
        win = cats[t-cfg.window:t]
        # detecta sequência de pelo menos K baixas:
        max_streak = 0
        cur = 0
        for c in win:
            if c == 0:
                cur += 1
                max_streak = max(max_streak, cur)
            else:
                cur = 0
        if max_streak >= cfg.min_k_lows:
            pos.append(t)
            future = cats[t:t+cfg.horizon]
            hits.append(int(np.any(future == 2)))
    prob = float(np.mean(hits)) if len(hits) > 0 else np.nan
    return SlidingResult(positions=pos, hit=hits, prob_empirica=prob, total_sinais=len(pos))


# -------------------------
# 4) Relatórios/Plots
# -------------------------

def plot_conditional_curve_k_lows(
    cats: np.ndarray,
    k_values: Iterable[int] = range(2, 15),
    horizon: int = 5,
    out_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Gera tabela P(alta | k baixas), k=2..14 por padrão.
    Salva um gráfico opcionalmente.
    """
    rows = []
    for k in k_values:
        total, hits, p = p_high_after_k_lows(cats, k=k, horizon=horizon)
        rows.append({"k": k, "total": total, "hits": hits, "p_high_within_H": p})
    df = pd.DataFrame(rows)

    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        plt.figure()
        plt.plot(df["k"], df["p_high_within_H"], marker="o")
        plt.axhline( (cats==2).mean(), linestyle="--")  # baseline: frequência global de altas
        plt.xlabel("k baixas consecutivas")
        plt.ylabel(f"P(alta em {horizon})")
        plt.title("P(alta | k baixas seguidas)")
        plt.grid(True)
        plt.savefig(out_dir / "cond_k_lows.png", dpi=130)
        plt.close()

    return df


def export_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)