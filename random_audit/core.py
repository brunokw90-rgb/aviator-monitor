from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class SummaryMetrics:
    n: int
    mean: float
    median: float
    std: float
    min: float
    max: float
    # novo campo
    entropy: Optional[float] = None


def _shannon_entropy_bits(x: np.ndarray, bins: int = 256) -> Optional[float]:
    """
    Entropia de Shannon em bits para a série x.
    - Normaliza x para [0, 1] e calcula histograma com 'bins' caixas.
    - Retorna None se não houver dados válidos; 0.0 se a série for constante.
    """
    x = np.asarray(x)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return None

    mn = float(np.min(x))
    mx = float(np.max(x))
    if mx == mn:
        # Série constante → sem incerteza
        return 0.0

    xn = (x - mn) / (mx - mn)  # normaliza para [0,1]
    hist, _ = np.histogram(xn, bins=bins, range=(0.0, 1.0))
    total = int(hist.sum())
    if total == 0:
        return None

    p = hist.astype(float) / total
    p = p[p > 0]  # evita log2(0)
    H = -np.sum(p * np.log2(p))
    return float(H)


def compute_basic_metrics(series: pd.Series) -> SummaryMetrics:
    # garante array float limpo
    x = pd.to_numeric(series, errors="coerce").dropna().astype(float).values
    if x.size == 0:
        nan = float("nan")
        return SummaryMetrics(0, nan, nan, nan, nan, nan, None)

    return SummaryMetrics(
        n=int(x.size),
        mean=float(np.mean(x)),
        median=float(np.median(x)),
        std=float(np.std(x, ddof=0)),
        min=float(np.min(x)),
        max=float(np.max(x)),
        entropy=_shannon_entropy_bits(x),  # <- calcula e preenche
    )