from .plotting import plot_hist, plot_acf as plot_acf_fig, plot_qq, plot_bds
from .io import sql_to_csv
from .core import compute_basic_metrics
from .report import write_markdown_report, write_html_report
from .stats import (dist_summary, chi_square_uniform, ks_test_scaled_uniform, runs_test, runs_up_down, acf, ljung_box, jarque_bera, bds_test)
from dataclasses import asdict
from pathlib import Path
import argparse, json, pandas as pd
import numpy as np

def shannon_entropy_bits(x: np.ndarray, bins: int = 256):
    """
    Entropia de Shannon (em bits) da série, com binning em [0,1].
    Retorna None se não houver dados válidos.
    """
    x = np.asarray(x)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return None

    mn = float(np.min(x))
    mx = float(np.max(x))
    if mx == mn:
        # série constante -> entropia 0
        return 0.0

    # normaliza para [0,1] e faz histograma
    xn = (x - mn) / (mx - mn)
    hist, _ = np.histogram(xn, bins=bins, range=(0.0, 1.0))
    if hist.sum() == 0:
        return None

    p = hist / hist.sum()
    p = p[p > 0]  # evita log2(0)
    H = -np.sum(p * np.log2(p))
    return float(H)

def main():
    # -------------------------
    # Args
    # -------------------------
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True, help="Caminho do arquivo SQL")
    ap.add_argument("--out", required=True, help="Pasta de saída")
    ap.add_argument("--col", default="valor", help="Nome da coluna numérica (default: valor)")
    ap.add_argument("--bins", type=int, default=60, help="Bins para histograma/qui-quadrado (default: 60)")
    ap.add_argument("--lags", type=int, default=24, help="Lags p/ ACF (default: 24)")
    ap.add_argument("--lb-lags", type=int, default=24, help="Lags p/ Ljung-Box (default: 24)")
    ap.add_argument("--run-cut", default="median", help="corte para runs_test: número, 'median' ou 'mean'")
    ap.add_argument("--no-plots", action="store_true", help="Não gerar gráficos")
    ap.add_argument("--html", action="store_true", help="Gerar relatório em HTML além do Markdown")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # 1) Converter SQL -> CSV
    # -------------------------
    csv_path = out_dir / "dados.csv"
    csv_path, n_rows = sql_to_csv(args.sql, csv_path)
    print(f"[OK] Convertido SQL -> CSV: {csv_path} ({n_rows} linhas)")

    # -------------------------
    # 2) Carregar série
    # -------------------------
    df = pd.read_csv(csv_path)
    if args.col not in df.columns:
        raise ValueError(f"Coluna '{args.col}' não encontrada no CSV. Colunas: {list(df.columns)}")
    series = pd.to_numeric(df[args.col], errors="coerce").dropna()

    metrics_dict = {}

    # -------------------------
    # 3) Métricas básicas
    # -------------------------
    basics = compute_basic_metrics(series)
    metrics_dict["basic"] = asdict(basics)

    # -------------------------
    # 4) Testes de distribuição
    # -------------------------
    chi = chi_square_uniform(series, bins=args.bins)
    ks  = ks_test_scaled_uniform(series)
    jb  = jarque_bera(series)
    metrics_dict["chi_square_uniform"] = asdict(chi)
    metrics_dict["ks_scaled_uniform"]   = asdict(ks)
    metrics_dict["jarque_bera"]         = asdict(jb)

    # -------------------------
    # 5) Ordem/Dependência
    # -------------------------
    # 5.1 Runs (Wald–Wolfowitz) com corte
    cut = args.run_cut
    try:
        cut_val = float(cut)
    except Exception:
        cut_val = cut  # 'median' ou 'mean'
    rtest = runs_test(series, cut=cut_val)
    metrics_dict["runs_test"] = asdict(rtest)

    # 5.2 Runs up-and-down (monotonia)
    rud = runs_up_down(series)
    metrics_dict["runs_up_down"] = asdict(rud)

    # 5.3 ACF
    acf_res = acf(series, lags=args.lags)
    metrics_dict["acf"] = asdict(acf_res)

    # 5.4 Ljung-Box
    lb_res = ljung_box(series, lags=args.lb_lags)
    metrics_dict["ljung_box"] = asdict(lb_res)

    # -------------------------
    # 6) BDS (dependência não-linear) m=2..5
    # -------------------------
    metrics_dict["bds"] = {}
    for m in [2, 3, 4, 5]:
        try:
            bds_res = bds_test(series, m=m)  # usa versão amostral em stats.py
            metrics_dict["bds"][f"m={m}"] = asdict(bds_res)
        except Exception as e:
            metrics_dict["bds"][f"m={m}"] = {"erro": str(e)}

    # -------------------------
    # 7) Gráficos
    # -------------------------
    plots_dir = out_dir / "plots"
    metrics_dict["plots"] = {}
    if not args.no_plots:
        hist_path = plot_hist(series, plots_dir, bins=args.bins)
        acf_path  = plot_acf_fig(acf_res.lags, acf_res.acf, acf_res.ci_approx, plots_dir)
        qq_path   = plot_qq(series, plots_dir)

        # BDS plot (Z vs m)
        bds_plot_path = None
        try:
            bds_plot_path = plot_bds(metrics_dict.get("bds", {}), plots_dir)
        except Exception as e:
            print(f"[ERRO] Não foi possível gerar gráfico BDS: {e}")
            bds_plot_path = None

        metrics_dict["plots"] = {
            "hist": str(hist_path),
            "acf":  str(acf_path),
            "qq":   str(qq_path),
        }
        if bds_plot_path:
            metrics_dict["plots"]["bds"] = str(bds_plot_path)

    # -------------------------
    # 8) Salvar JSON
    # -------------------------
    metrics_json = out_dir / "metrics.json"
    metrics_json.write_text(
        json.dumps(metrics_dict, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[OK] metrics.json salvo: {metrics_json}")

    # -------------------------
    # 9) Relatórios
    # -------------------------
    report_path = write_markdown_report(out_dir, metrics_dict)
    print(f"[OK] report.md salvo: {report_path}")

    if args.html:
        html_path = write_html_report(out_dir, metrics_dict)
        print(f"[OK] report.html salvo: {html_path}")