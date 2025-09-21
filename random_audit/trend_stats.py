# random_audit/trend_stats.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Iterable, Dict, Any, Tuple
import numpy as np
import pandas as pd

@dataclass
class TrendStats:
    cut: float
    h: int
    n_k: int                 # quantos k válidos entraram no cálculo
    mean: float
    std: float
    var: float
    min_val: float
    max_val: float
    amplitude: float

    def asdict(self) -> Dict[str, Any]:
        d = asdict(self)
        # arredonda pra deixar bonito no HTML/CSV
        for k in ("mean","std","var","min_val","max_val","amplitude"):
            d[k] = float(np.round(d[k], 8))
        return d

def summarize_prob_curve(prob_by_k: Dict[int, float],
                         cut: float,
                         h: int) -> TrendStats:
    """
    prob_by_k: dict {k: P(alta em h | k consecutivos)}
    """
    if not prob_by_k:
        return TrendStats(cut=cut, h=h, n_k=0,
                          mean=np.nan, std=np.nan, var=np.nan,
                          min_val=np.nan, max_val=np.nan, amplitude=np.nan)
    xs = np.array(list(prob_by_k.values()), dtype=float)
    return TrendStats(
        cut=cut,
        h=h,
        n_k=int(xs.size),
        mean=float(xs.mean()),
        std=float(xs.std(ddof=0)),
        var=float(xs.var(ddof=0)),
        min_val=float(xs.min()),
        max_val=float(xs.max()),
        amplitude=float(xs.max() - xs.min()),
    )

def stats_table_for_cuts(results: Dict[float, Dict[int, Dict[int, float]]],
                         h_target: int = 5) -> pd.DataFrame:
    """
    results estrutura esperada:
        {cut: {h: {k: prob}}}
    Retorna um DataFrame com uma linha por corte (para h=h_target).
    """
    rows = []
    for cut, by_h in results.items():
        curve = by_h.get(h_target, {})
        st = summarize_prob_curve(curve, cut=float(cut), h=h_target).asdict()
        rows.append(st)
    df = pd.DataFrame(rows).sort_values("cut")
    return df