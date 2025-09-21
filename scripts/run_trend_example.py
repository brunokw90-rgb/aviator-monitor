# scripts/run_trend_example.py
from pathlib import Path
import pandas as pd
from random_audit.trend import (
    to_categories, class_frequencies, p_high_after_k_lows,
    p_high_after_pattern, SlidingConfig, sliding_signal_k_lows,
    plot_conditional_curve_k_lows, export_csv
)

# Caminhos (ajuste se precisar)
CSV_PATH = Path("./audit_out/dados.csv")  # gerado pelo random-audit
OUT_DIR  = Path("./audit_out/trend_out")

def main():
    df = pd.read_csv(CSV_PATH)
    # supondo que sua coluna numérica seja "valor"
    series = df["valor"]

    # 1) Frequências globais
    cats = to_categories(series, low_th=2.0, high_th=10.0)
    freqs = class_frequencies(cats)
    print("[Frequências globais] baixa, média, alta:", freqs)

    # 2) P(alta | k baixas seguidas), horizon=5
    cond_df = plot_conditional_curve_k_lows(
        cats, k_values=range(2, 15), horizon=5, out_dir=OUT_DIR
    )
    export_csv(cond_df, OUT_DIR / "p_high_given_k_lows.csv")
    print("[OK] CSV e gráfico de P(alta|k baixas) salvos em:", OUT_DIR)

    # 3) Padrão genérico: 4 baixas seguidas depois 1 média (exemplo)
    total, hits, p = p_high_after_pattern(cats, pattern=[0,0,0,0,1], horizon=5)
    print(f"P(alta | padrão [0,0,0,0,1]) em 5 rodadas: total={total}, hits={hits}, p={p}")

    # 4) Janela deslizante (estilo “Guru”)
    cfg = SlidingConfig(window=50, horizon=5, min_k_lows=8)
    slide_res = sliding_signal_k_lows(series, cfg)
    print(f"[Sliding] sinais={slide_res.total_sinais}, "
          f"acerto_médio={slide_res.prob_empirica:.4f}")

if __name__ == "__main__":
    main()