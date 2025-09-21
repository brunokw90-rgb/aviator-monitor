# scripts/run_trend_sweep.py
# -- coding: utf-8 --
"""
Varredura de cortes (2x, 5x, 10x, 20x...) + condicionais e heatmaps,
com export de estatísticas (média, std, variância, amplitude) por corte (h=5).

Como rodar:
  (venv) PS C:\...\random_audit_proj> python .\scripts\run_trend_sweep.py
"""

from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---- NOVO: import do resumo de estatísticas
from random_audit.trend_stats import stats_table_for_cuts

# --------------------------
# Parâmetros
# --------------------------
CSV_PATH      = Path("audit_out") / "dados.csv"
OUT_DIR       = Path("audit_out") / "trend_out"
CUTS          = [2.0, 5.0, 10.0, 20.0]
HORIZONS      = [1, 2, 3, 4, 5]          # h (próximas h rodadas)
K_MAX_LOWS    = 30                        # k máximo p/ sequência de baixas
K_MAX_HIGHS   = 10                        # k máximo p/ sequência de altas
MIN_COUNT     = 20                        # amostras mínimas para considerar ponto
WINDOW_H      = 5                         # h padrão para curvas/estatística
# --------------------------


def _pick_value_column(df: pd.DataFrame) -> pd.Series:
    """Escolhe a coluna numérica (prioriza 'valor')."""
    if "valor" in df.columns:
        s = pd.to_numeric(df["valor"], errors="coerce").dropna().astype(float)
        if s.size > 0:
            return s.reset_index(drop=True)
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not num_cols:
        raise ValueError("Nenhuma coluna numérica encontrada em dados.csv.")
    s = pd.to_numeric(df[num_cols[0]], errors="coerce").dropna().astype(float)
    return s.reset_index(drop=True)


def _to_bool_high(x: np.ndarray, cut: float) -> np.ndarray:
    """True para 'alta' (>= cut), False caso contrário."""
    return x >= cut


def _runs_lengths(flags: np.ndarray, value: bool) -> np.ndarray:
    """Comprimentos das runs do valor desejado (True/False)."""
    if flags.size == 0:
        return np.array([], dtype=int)
    changes = np.flatnonzero(np.diff(flags.astype(int)) != 0)
    starts = np.r_[0, changes + 1]
    ends = np.r_[changes, flags.size - 1]
    keep = flags[starts] == value
    lengths = (ends - starts + 1)[keep]
    return lengths


def _prob_high_within_h_after_k(flags_high: np.ndarray, k: int, h: int, want_after_lows=True):
    """
    P(>=1 alta nas próximas h), condicionada a k consecutivas ANTES:
      - want_after_lows=True  => k baixas seguidas
      - want_after_lows=False => k altas seguidas
    Retorna (p, n_amostras).
    """
    n = flags_high.size
    hits = 0
    total = 0
    for t in range(k, n - h):
        prev = flags_high[t - k:t]
        cond_ok = (prev == (False if want_after_lows else True)).all()
        if not cond_ok:
            continue
        future = flags_high[t:t + h]
        total += 1
        if future.any():
            hits += 1
    p = (hits / total) if total > 0 else np.nan
    return p, total


def _plot_line(x, y, title, xlabel, ylabel, path, yref=None):
    plt.figure()
    plt.plot(x, y, marker="o")
    if yref is not None and not np.isnan(yref):
        plt.axhline(yref, linestyle="--", linewidth=1)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=120)
    plt.close()


def _plot_hist(lengths, title, xlabel, path, bins=50):
    plt.figure()
    plt.hist(lengths, bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("frequência")
    plt.grid(True)
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=120)
    plt.close()


def _plot_heatmap(matrix: np.ndarray, x_ticks, y_ticks, title, xlabel, ylabel, path):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    im = ax.imshow(matrix, aspect="auto", origin="lower")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(np.arange(len(x_ticks)))
    ax.set_xticklabels([str(v) for v in x_ticks])
    ax.set_yticks(np.arange(len(y_ticks)))
    ax.set_yticklabels([str(v) for v in y_ticks])
    fig.colorbar(im, ax=ax, label="P(alta)")
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=120)
    plt.close()


def main():
    # 1) Carregar dados
    df = pd.read_csv(CSV_PATH)
    series = _pick_value_column(df)
    x = series.values
    x = x[np.isfinite(x)]

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2) BASELINE (opcional, para referência de gráfico com h=WINDOW_H)
    def baseline_palta_em_w(flags: np.ndarray, w: int) -> float:
        # 1 - P(nenhuma alta em w) ≈ 1 - prod(1 - bit) por janela (estimativa):
        hits, total = 0, 0
        for t in range(0, flags.size - w):
            block = flags[t:t + w]
            total += 1
            if block.any():
                hits += 1
        return (hits / total) if total > 0 else np.nan

    # 3) results: {cut: {h: {k: prob}}}  (APENAS curva "k baixas" para o resumo)
    results = {}

    # 4) Loop por cortes
    for cut in CUTS:
        cut_dir = OUT_DIR / f"cut_{str(cut).replace('.','_')}"
        cut_dir.mkdir(parents=True, exist_ok=True)

        flags = _to_bool_high(x, cut)
        base_ref = baseline_palta_em_w(flags, WINDOW_H)

        # ---- (a) Condicionais em k BAIXAS (linha)
        ks_l = np.arange(2, K_MAX_LOWS + 1)
        ps_l = []
        for k in ks_l:
            p, n = _prob_high_within_h_after_k(flags, k=k, h=WINDOW_H, want_after_lows=True)
            ps_l.append(np.nan if (n < MIN_COUNT) else p)
        _plot_line(
            ks_l, ps_l,
            title=f"P(alta em {WINDOW_H}) | k baixas seguidas  (cut={cut})",
            xlabel="k baixas consecutivas",
            ylabel=f"P(alta em {WINDOW_H})",
            path=cut_dir / "cond_k_lows.png",
            yref=base_ref
        )

        # ---- (b) Condicionais em k ALTAS (linha)
        ks_h = np.arange(2, K_MAX_HIGHS + 1)
        ps_h = []
        for k in ks_h:
            p, n = _prob_high_within_h_after_k(flags, k=k, h=WINDOW_H, want_after_lows=False)
            ps_h.append(np.nan if (n < MIN_COUNT) else p)
        _plot_line(
            ks_h, ps_h,
            title=f"P(alta em {WINDOW_H}) | k altas seguidas  (cut={cut})",
            xlabel="k altas consecutivas",
            ylabel=f"P(alta em {WINDOW_H})",
            path=cut_dir / "cond_k_highs.png",
            yref=base_ref
        )

        # ---- (c) Hist de runs
        highs_len = _runs_lengths(flags, True)
        lows_len  = _runs_lengths(flags, False)
        _plot_hist(highs_len,
                   title=f"Distribuição de runs (altas)  (cut={cut})",
                   xlabel="tamanho da sequência de altas",
                   path=cut_dir / "runs_high_hist.png",
                   bins=min(50, max(10, int(np.sqrt(max(1, highs_len.size))))))
        _plot_hist(lows_len,
                   title=f"Distribuição de runs (baixas)  (cut={cut})",
                   xlabel="tamanho da sequência de baixas",
                   path=cut_dir / "runs_low_hist.png",
                   bins=min(80, max(10, int(np.sqrt(max(1, lows_len.size))))))

        # ---- (d) HEATMAPS: P(alta em h) | k baixas/altas
        ks = np.arange(1, K_MAX_LOWS + 1)
        hs = np.array(HORIZONS, dtype=int)
        mat_l = np.full((len(ks), len(hs)), np.nan, dtype=float)
        for i, k in enumerate(ks):
            for j, h in enumerate(hs):
                p, n = _prob_high_within_h_after_k(flags, k=k, h=h, want_after_lows=True)
                mat_l[i, j] = np.nan if (n < MIN_COUNT) else p
        _plot_heatmap(mat_l, x_ticks=hs, y_ticks=ks,
                      title=f"P(alta em h) | k baixas (cut={cut})",
                      xlabel="h (próximas h rodadas)", ylabel="k baixas seguidas",
                      path=cut_dir / "heat_low_k_h.png")

        ks2 = np.arange(1, K_MAX_HIGHS + 1)
        mat_h = np.full((len(ks2), len(hs)), np.nan, dtype=float)
        for i, k in enumerate(ks2):
            for j, h in enumerate(hs):
                p, n = _prob_high_within_h_after_k(flags, k=k, h=h, want_after_lows=False)
                mat_h[i, j] = np.nan if (n < MIN_COUNT) else p
        _plot_heatmap(mat_h, x_ticks=hs, y_ticks=ks2,
                      title=f"P(alta em h) | k altas (cut={cut})",
                      xlabel="h (próximas h rodadas)", ylabel="k altas seguidas",
                      path=cut_dir / "heat_high_k_h.png")

        # ---- (e) PREPARO para ESTATÍSTICA: guardar curva "k baixas" em results
        # Estrutura esperada por stats_table_for_cuts: results[cut][h][k] = prob
        # -> vamos popular APENAS para h = WINDOW_H (5), usando ks_l / ps_l
        results.setdefault(float(cut), {})
        results[float(cut)].setdefault(int(WINDOW_H), {})
        for k, p in zip(ks_l, ps_l):
            if np.isnan(p):
                continue
            results[float(cut)][int(WINDOW_H)][int(k)] = float(p)

    # 5) ===== NOVO BLOCO: gerar estatísticas por corte (h=5) =====
    stats_df = stats_table_for_cuts(results, h_target=int(WINDOW_H))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stats_csv = OUT_DIR / "trend_stats_h5.csv"
    stats_json = OUT_DIR / "trend_stats_h5.json"

    stats_df.to_csv(stats_csv, index=False, encoding="utf-8")
    with open(stats_json, "w", encoding="utf-8") as f:
        json.dump(stats_df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    print(f"[OK] Varredura finalizada. Saídas em: {OUT_DIR}")
    print(f"[OK] Estatísticas (h={WINDOW_H}) salvas:\n- {stats_csv}\n- {stats_json}")


if __name__ == "__main__":
    main()