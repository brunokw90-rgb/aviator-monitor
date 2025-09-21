from pathlib import Path
from typing import Dict, Any


def _fmt_float(val, nd=6):
    try:
        return f"{float(val):.{nd}f}"
    except Exception:
        return "NA"


def write_markdown_report(out_dir, metrics: Dict[str, Any]):
    """
    Gera um report.md simples com todas as seções disponíveis no dict 'metrics'.
    Espera chaves como: n, mean, median, std, min, max,
    dist_summary, chi_square_uniform, ks_uniform, runs_test, acf.
    """
    out = Path(out_dir) / "report.md"
    out.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# Random Audit - Relatório\n")

    # ------------------ Métricas básicas ------------------
    lines += [
        "## Métricas Básicas",
        f"- n: {metrics.get('n', 'NA')}",
        f"- média: {_fmt_float(metrics.get('mean'))}" if 'mean' in metrics else "- média: NA",
        f"- mediana: {_fmt_float(metrics.get('median'))}" if 'median' in metrics else "- mediana: NA",
        f"- desvio padrão: {_fmt_float(metrics.get('std'))}" if 'std' in metrics else "- desvio padrão: NA",
        f"- min: {_fmt_float(metrics.get('min'))}" if 'min' in metrics else "- min: NA",
        f"- max: {_fmt_float(metrics.get('max'))}" if 'max' in metrics else "- max: NA",
        ""
    ]

    # ------------------ Distribuição ------------------
    if "dist_summary" in metrics and isinstance(metrics["dist_summary"], dict):
        d = metrics["dist_summary"]
        lines += [
            "## Distribuição (skew/kurt/entropia)",
            f"- assimetria (skewness): {_fmt_float(d.get('skewness'))}",
            f"- curtose (excesso): {_fmt_float(d.get('kurtosis_excess'))}",
            f"- entropia (bits): {_fmt_float(d.get('entropy_bits'))}",
            ""
        ]

    # ------------------ Qui-quadrado ------------------
    if "chi_square_uniform" in metrics and isinstance(metrics["chi_square_uniform"], dict):
        c = metrics["chi_square_uniform"]
        lines += [
            "## Qui-quadrado (uniformidade por bins)",
            f"- bins: {c.get('bins', 'NA')}",
            f"- χ²: {_fmt_float(c.get('statistic'))} | gl: {c.get('dof', 'NA')} | p-valor: {_fmt_float(c.get('pvalue'), nd=6)}",
            ""
        ]

    # ------------------ KS (scaled) ------------------
    if "ks_uniform" in metrics and isinstance(metrics["ks_uniform"], dict):
        k = metrics["ks_uniform"]
        note = k.get("note", "")
        lines += [
            "## KS contra Uniforme(0,1) (após min–max scaling)",
            f"- D: {_fmt_float(k.get('statistic'))} | p-valor: {_fmt_float(k.get('pvalue'), nd=6)}",
            f"- nota: {note}",
            ""
        ]

    # ------------------ Runs ------------------
    if "runs_test" in metrics and isinstance(metrics["runs_test"], dict):
        r = metrics["runs_test"]
        lines += [
            "## Runs (Wald–Wolfowitz)",
            f"- corte: {r.get('cut', 'NA')}",
            f"- runs: {r.get('runs', 'NA')} | acima: {r.get('n_above', 'NA')} | abaixo: {r.get('n_below', 'NA')}",
            f"- z: {_fmt_float(r.get('z'))} | p-valor: {_fmt_float(r.get('pvalue'), nd=6)}",
            ""
        ]

    # ------------------ Runs up-and-down ------------------
    if "runs_up_down" in metrics and isinstance(metrics["runs_up_down"], dict):
        rud = metrics["runs_up_down"]
        lines += [
            "## Runs up-and-down (monotonia)",
            f"- runs: {rud.get('runs','NA')} | subidas: {rud.get('n_up','NA')} | descidas: {rud.get('n_down','NA')}",
            f"- z: {_fmt_float(rud.get('z'))} | p-valor: {_fmt_float(rud.get('pvalue'), nd=6)}",
            ""
        ]

    # ------------------ ACF ------------------
    if "acf" in metrics and isinstance(metrics["acf"], dict):
        a = metrics["acf"]
        lags = a.get("lags", []) or []
        acf_vals = a.get("acf", []) or []
        try:
            pairs = list(zip(lags, acf_vals))
        except Exception:
            pairs = []
        preview = ", ".join(f"{lag}:{val:.3f}" for lag, val in pairs[:10]) if pairs else ""
        ci = a.get("ci_approx", None)

        lines += [
            "## Autocorrelação (ACF)",
            f"- lags mostrados (primeiros 10): {preview if preview else 'sem dados'}",
            f"- intervalo de confiança aprox.: ±{_fmt_float(ci, nd=3)}",
            ""
        ]
    
    # ------------------ Gráficos (arquivos gerados) ------------------
    if "plots" in metrics and isinstance(metrics["plots"], dict):
        p = metrics["plots"]
        lines += ["## Gráficos"]

        if p.get("hist"):
            lines.append(f"![Histograma]({p.get('hist')})")
        if p.get("acf"):
            lines.append(f"![ACF]({p.get('acf')})")
        if p.get("qq"):
            lines.append(f"![QQ-plot]({p.get('qq')})")

        lines.append("")

    # ------------------ Ljung–Box ------------------
    if "ljung_box" in metrics and isinstance(metrics["ljung_box"], dict):
        lb = metrics["ljung_box"]
        lines += [
            "## Ljung–Box (autocorrelação conjunta)",
            f"- lags (m): {lb.get('lags','NA')} | Q: {_fmt_float(lb.get('Q'))} | gl: {lb.get('dof','NA')} | p-valor: {_fmt_float(lb.get('pvalue'), nd=6)}",
            ""
        ]

    # ------------------ Jarque–Bera ------------------
    if "jarque_bera" in metrics and isinstance(metrics["jarque_bera"], dict):
        jb = metrics["jarque_bera"]
        lines += [
            "## Jarque–Bera (normalidade)",
            f"- JB: {_fmt_float(jb.get('JB'))} | p-valor: {_fmt_float(jb.get('pvalue'), nd=6)}",
            f"- skewness: {_fmt_float(jb.get('skewness'))} | kurtosis: {_fmt_float(jb.get('kurtosis'))}",
            ""
        ]

    # ------------------ BDS ------------------
    if "bds" in metrics and isinstance(metrics["bds"], dict):
        lines += ["## BDS (dependência não-linear)"]
        # Se veio no formato único {"m":..., "eps":..., "stat":..., "pvalue":...}
        if all(k in metrics["bds"] for k in ("m","eps","stat","pvalue")):
            b = metrics["bds"]
            lines += [
                f"- dimensão m: {b.get('m')} | raio eps: {_fmt_float(b.get('eps'))}",
                f"- Z: {_fmt_float(b.get('stat'))} | p-valor: {_fmt_float(b.get('pvalue'), nd=6)}",
                ""
            ]
        else:
            # Vários m: {"m=2": {...}, "m=3": {...}, ...}
            for key in sorted(metrics["bds"].keys()):
                b = metrics["bds"][key]
                if isinstance(b, dict) and "stat" in b:
                    lines += [
                        f"- {key}: Z={_fmt_float(b.get('stat'))} | p-valor={_fmt_float(b.get('pvalue'), nd=6)} | eps={_fmt_float(b.get('eps'))}"
                    ]
                else:
                    lines += [f"- {key}: {b}"]
            lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")
    return out

from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

def write_html_report(out_dir, metrics_dict):
    """
    Renderiza templates/report.html.j2 em HTML usando Jinja2.
    """
    out_dir = Path(out_dir)
    tpl_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(tpl_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("report.html.j2")

    # objeto simples pra acessar com ponto no template (m.chi_square_uniform...)
    class Obj(dict):
        __getattr__ = dict.get
        __setattr__ = dict.__setitem__   # <-- dois underscores
        __delattr__ = dict.__delitem__   # (opcional, mas ajuda)

    def to_obj(d):
        if isinstance(d, dict):
            o = Obj()
            for k, v in d.items():
                o[k] = to_obj(v)
            return o
        elif isinstance(d, list):
            return [to_obj(x) for x in d]
        else:
            return d

    html = tpl.render(m=to_obj(metrics_dict), now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    out_path = out_dir / "report.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path