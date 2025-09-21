from pathlib import Path
from typing import Optional, Sequence
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from pathlib import Path
import matplotlib.pyplot as plt

def plot_bds(bds_dict, plots_dir):
    """
    Gera gráfico 'BDS Z vs m'.
    - Aceita tanto formato único {"m":..,"stat":..} quanto múltiplos
      {"m=2": {...}, "m=3": {...}, ...}.
    - Retorna o Path do arquivo salvo (plots/bds.png) ou None.
    """
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    m_vals, z_vals = [], []

    # Formato único
    if isinstance(bds_dict, dict) and ("m" in bds_dict and "stat" in bds_dict):
        try:
            m_vals = [int(bds_dict.get("m"))]
            z_vals = [float(bds_dict.get("stat"))]
        except Exception:
            return None
    else:
        # Formato com chaves "m=2", "m=3", ...
        for k, v in (bds_dict or {}).items():
            if not isinstance(v, dict):
                continue
            if "m" in v and "stat" in v:
                try:
                    m_vals.append(int(v["m"]))
                    z_vals.append(float(v["stat"]))
                except Exception:
                    continue

    if not m_vals:
        return None

    # Ordena por m
    order = sorted(range(len(m_vals)), key=lambda i: m_vals[i])
    m_ord = [m_vals[i] for i in order]
    z_ord = [z_vals[i] for i in order]

    plt.figure()
    plt.plot(m_ord, z_ord, marker="o")
    plt.axhline(0, linestyle="--")
    plt.title("BDS: estatística Z por dimensão m")
    plt.xlabel("m")
    plt.ylabel("Z")
    out = plots_dir / "bds.png"
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()
    return out


def _to_numeric(series: pd.Series) -> np.ndarray:
    x = pd.to_numeric(series, errors="coerce").dropna().astype(float).values
    return x


def plot_hist(series: pd.Series, out_dir: Path, bins: int = 30, title: str = "Histograma") -> Path:
    x = _to_numeric(series)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "histograma.png"

    plt.figure()
    plt.hist(x, bins=bins, density=False)
    plt.title(title)
    plt.xlabel("valor")
    plt.ylabel("contagem")
    plt.tight_layout()
    plt.savefig(path, dpi=120)
    plt.close()
    return path


def plot_acf(lags: Sequence[int], acf_vals: Sequence[float], ci: float, out_dir: Path,
             title: str = "ACF") -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "acf.png"

    lags = list(lags or [])
    vals = list(acf_vals or [])

    plt.figure()
    plt.stem(lags, vals)
    if np.isfinite(ci):
        plt.axhline(ci, linestyle="--")
        plt.axhline(-ci, linestyle="--")
    plt.axhline(0.0, linewidth=1)
    plt.title(title)
    plt.xlabel("lag")
    plt.ylabel("autocorrelação")
    plt.tight_layout()
    plt.savefig(path, dpi=120)
    plt.close()
    return path


def plot_qq(series: pd.Series, out_dir: Path, dist: str = "norm", title: str = "QQ-plot (Normal)") -> Path:
    """
    QQ-plot simples contra Normal(0,1) após padronizar a série.
    """
    x = _to_numeric(series)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "qqplot.png"

    if x.size == 0:
        # cria imagem vazia informativa
        plt.figure()
        plt.text(0.5, 0.5, "Sem dados", ha="center", va="center")
        plt.axis("off")
        plt.savefig(path, dpi=120)
        plt.close()
        return path

    x = (x - np.mean(x)) / (np.std(x) if np.std(x) > 0 else 1.0)
    plt.figure()
    stats.probplot(x, dist="norm", plot=plt)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=120)
    plt.close()
    return path


from pathlib import Path
import matplotlib.pyplot as plt

def plot_bds(bds_dict, plots_dir):
    """
    Gera e salva o gráfico 'BDS: estatística Z por dimensão m'.
    Aceita:
      - formato único: {"m":..,"stat":..,"pvalue":..,"eps":..}
      - formato múltiplo: {"m=2": {...}, "m=3": {...}, ...}
    Retorna: Path do arquivo salvo (plots/bds.png) ou None.
    """
    plots_dir = Path(plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    # extrai (m, Z)
    m_vals, z_vals = [], []
    if isinstance(bds_dict, dict) and ("m" in bds_dict and "stat" in bds_dict):
        # formato simples
        try:
            m_vals = [int(bds_dict["m"])]
            z_vals = [float(bds_dict["stat"])]
        except Exception:
            return None
    else:
        # múltiplos "m=2", "m=3", ...
        for _, v in (bds_dict or {}).items():
            if isinstance(v, dict) and ("m" in v and "stat" in v):
                try:
                    m_vals.append(int(v["m"]))
                    z_vals.append(float(v["stat"]))
                except Exception:
                    continue

    if not m_vals:
        return None

    # ordena por m
    order = sorted(range(len(m_vals)), key=lambda i: m_vals[i])
    m_ord = [m_vals[i] for i in order]
    z_ord = [z_vals[i] for i in order]

    plt.figure()
    plt.plot(m_ord, z_ord, marker="o")
    plt.axhline(0, linestyle="--")
    plt.title("BDS: estatística Z por dimensão m")
    plt.xlabel("m")
    plt.ylabel("Z")
    out = plots_dir / "bds.png"
    plt.tight_layout()
    plt.savefig(out, dpi=120)
    plt.close()
    return out