from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Tuple, List, Union

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class DistSummary:
    skewness: float
    kurtosis_excess: float
    entropy_bits: float


@dataclass
class ChiSquareUniform:
    bins: int
    statistic: float
    pvalue: float
    dof: int
    observed: List[int]
    expected: List[float]


@dataclass
class KSTest:
    statistic: float
    pvalue: float
    note: str  # dica sobre normalização


@dataclass
class RunsTest:
    runs: int
    n_above: int
    n_below: int
    z: float
    pvalue: float
    cut: Union[str, float]


@dataclass
class ACFResult:
    lags: List[int]
    acf: List[float]
    ci_approx: float  # ~ 1.96 / sqrt(n)


@dataclass
class LjungBoxResult:
    lags: int
    Q: float
    pvalue: float
    dof: int
    rhos: List[float]  # autocorrelações ρ1..ρm


@dataclass
class RunsUpDownResult:
    runs: int
    n_up: int
    n_down: int
    z: float
    pvalue: float


@dataclass
class JarqueBeraResult:
    JB: float
    pvalue: float
    skewness: float
    kurtosis: float


@dataclass
class BDSResult:
    m: int        # dimensão de incorporação
    eps: float    # raio
    n: int        # tamanho da amostra
    stat: float   # estatística Z
    pvalue: float


def _to_numeric(series: pd.Series) -> np.ndarray:
    x = pd.to_numeric(series, errors="coerce").dropna().astype(float).values
    return x


def dist_summary(series: pd.Series, bins: int = 30) -> DistSummary:
    x = _to_numeric(series)
    if x.size == 0:
        return DistSummary(np.nan, np.nan, np.nan)
    skew = stats.skew(x, bias=False)
    kurt_excess = stats.kurtosis(x, fisher=True, bias=False)
    # Entropia baseada em histograma (probabilidades)
    hist, _ = np.histogram(x, bins=bins)
    p = hist / hist.sum() if hist.sum() > 0 else np.zeros_like(hist, dtype=float)
    ent_nat = stats.entropy(p)  # nats
    ent_bits = float(ent_nat / np.log(2))  # bits
    return DistSummary(float(skew), float(kurt_excess), float(ent_bits))


def chi_square_uniform(series: pd.Series, bins: int = 30) -> ChiSquareUniform:
    """
    Testa se a distribuição é 'próxima' de uniforme por contagem em bins.
    H0: frequências por bin = n/bins (uniforme).
    """
    x = _to_numeric(series)
    if x.size == 0:
        return ChiSquareUniform(bins, np.nan, np.nan, 0, [], [])
    hist, edges = np.histogram(x, bins=bins)
    observed = hist.astype(int)
    expected = np.full(bins, fill_value=x.size / bins, dtype=float)
    # χ² = sum (O-E)^2/E
    with np.errstate(divide="ignore", invalid="ignore"):
        stat = np.nansum((observed - expected) ** 2 / expected)
    dof = bins - 1
    p = 1.0 - stats.chi2.cdf(stat, dof)
    return ChiSquareUniform(bins, float(stat), float(p), int(dof), observed.tolist(), expected.tolist())


def ks_test_scaled_uniform(series: pd.Series) -> KSTest:
    """
    KS contra Uniforme(0,1) após ESCALAR a série para [0,1] usando min-max.
    Observação: isso 'ajusta' limites; serve como verificação grosseira de forma.
    """
    x = _to_numeric(series)
    if x.size == 0:
        return KSTest(np.nan, np.nan, "série vazia")
    xmin, xmax = np.min(x), np.max(x)
    note = "min-max scaled to [0,1]"
    if xmax == xmin:
        return KSTest(np.nan, np.nan, "todos valores iguais; KS inválido")
    z = (x - xmin) / (xmax - xmin)
    stat, p = stats.kstest(z, "uniform")
    return KSTest(float(stat), float(p), note)


def runs_test(series: pd.Series, cut: Union[str, float] = "median") -> RunsTest:
    """
    Wald–Wolfowitz runs test acima/abaixo do corte.
    """
    x = _to_numeric(series)
    if x.size == 0:
        return RunsTest(np.nan, 0, 0, np.nan, np.nan, cut)

    if isinstance(cut, (int, float)):
        thr = float(cut)
    elif cut == "median":
        thr = float(np.median(x))
    elif cut == "mean":
        thr = float(np.mean(x))
    else:
        thr = float(np.median(x))

    # Binariza: acima = 1, abaixo = 0; empates são descartados
    mask_above = x > thr
    mask_below = x < thr
    keep = mask_above | mask_below
    s = np.where(mask_above[keep], 1, 0)

    n1 = int(np.sum(s == 1))
    n0 = int(np.sum(s == 0))
    if n1 == 0 or n0 == 0:
        return RunsTest(runs=1, n_above=n1, n_below=n0, z=np.nan, pvalue=np.nan, cut=cut)

    # Conta runs
    runs = 1 + int(np.sum(s[1:] != s[:-1]))

    # Esperança e variância sob H0
    mu = 1 + (2 * n1 * n0) / (n1 + n0)
    var = (2 * n1 * n0 * (2 * n1 * n0 - n1 - n0)) / (((n1 + n0) ** 2) * (n1 + n0 - 1))
    z = (runs - mu) / np.sqrt(var) if var > 0 else np.nan
    p = 2 * (1 - stats.norm.cdf(abs(z))) if np.isfinite(z) else np.nan

    return RunsTest(int(runs), n1, n0, float(z), float(p), cut)


def runs_up_down(series: pd.Series) -> RunsUpDownResult:
    """
    Runs-up-and-down (monotonia): usa o sinal das diferenças consecutivas.
    H0: ordem aleatória (sem tendência de subir/descer).
    """
    x = _to_numeric(series)
    if x.size < 2:
        return RunsUpDownResult(runs=np.nan, n_up=0, n_down=0, z=np.nan, pvalue=np.nan)

    d = np.diff(x)
    mask = d != 0
    if not np.any(mask):
        # tudo igual, não dá pra testar
        return RunsUpDownResult(runs=1, n_up=0, n_down=0, z=np.nan, pvalue=np.nan)

    s = (d[mask] > 0).astype(int)      # 1 = sobe, 0 = desce
    n1 = int(np.sum(s == 1))           # subidas
    n0 = int(np.sum(s == 0))           # descidas
    if n1 == 0 or n0 == 0:
        return RunsUpDownResult(runs=1, n_up=n1, n_down=n0, z=np.nan, pvalue=np.nan)

    runs = 1 + int(np.sum(s[1:] != s[:-1]))
    # mesma esperança/variância do runs acima/abaixo (Wald–Wolfowitz)
    mu = 1 + (2 * n1 * n0) / (n1 + n0)
    var = (2 * n1 * n0 * (2 * n1 * n0 - n1 - n0)) / (((n1 + n0) ** 2) * (n1 + n0 - 1))
    z = (runs - mu) / np.sqrt(var) if var > 0 else np.nan
    p = 2 * (1 - stats.norm.cdf(abs(z))) if np.isfinite(z) else np.nan
    return RunsUpDownResult(int(runs), n1, n0, float(z), float(p))


def acf(series: pd.Series, lags: int = 20) -> ACFResult:
    """
    ACF simples (não viesada), lags 1..L. IC aproximado +-1.96/sqrt(n).
    """
    x = _to_numeric(series)
    if x.size == 0:
        return ACFResult([], [], np.nan)

    x = x - np.mean(x)
    var = np.dot(x, x) / x.size
    acf_vals = []
    for k in range(1, lags + 1):
        num = np.dot(x[:-k], x[k:]) / x.size
        acf_vals.append(float(num / var) if var > 0 else np.nan)
    ci = 1.96 / np.sqrt(x.size)
    return ACFResult(list(range(1, lags + 1)), acf_vals, float(ci))


def ljung_box(series: pd.Series, lags: int = 20) -> LjungBoxResult:
    """
    Teste Ljung–Box:
    H0: não há autocorrelação até o lag 'm' (processo ruído branco).
    Q ~ χ²(m) sob H0.
    Fórmula: Q = n(n+2) * sum_{k=1..m} (ρ_k^2 / (n-k))
    """
    x = _to_numeric(series)
    n = x.size
    if n == 0 or lags <= 0:
        return LjungBoxResult(lags=int(lags), Q=np.nan, pvalue=np.nan, dof=int(lags), rhos=[])

    x = x - np.mean(x)
    var = np.dot(x, x) / n
    rhos = []
    for k in range(1, lags + 1):
        num = np.dot(x[:-k], x[k:]) / n
        rhos.append(float(num / var) if var > 0 else np.nan)

    Q = n * (n + 2) * np.nansum([(rhos[k-1] ** 2) / (n - k) for k in range(1, lags + 1)])
    p = 1.0 - stats.chi2.cdf(Q, df=lags)
    return LjungBoxResult(lags=int(lags), Q=float(Q), pvalue=float(p), dof=int(lags), rhos=rhos)


def jarque_bera(series: pd.Series) -> JarqueBeraResult:
    """
    Teste de normalidade Jarque–Bera.
    H0: dados seguem distribuição normal.
    Usa skewness e kurtosis.
    """
    x = _to_numeric(series)
    n = x.size
    if n == 0:
        return JarqueBeraResult(JB=np.nan, pvalue=np.nan, skewness=np.nan, kurtosis=np.nan)

    s = stats.skew(x, nan_policy="omit")
    k = stats.kurtosis(x, fisher=False, nan_policy="omit")  # kurtosis normal = 3
    jb = (n / 6.0) * (s*2 + (1.0 / 4.0) * ((k - 3.0) * 2))
    p = 1.0 - stats.chi2.cdf(jb, df=2)
    return JarqueBeraResult(float(jb), float(p), float(s), float(k))


def bds_test(series: pd.Series, m: int = 2, eps: float = None, max_pairs: int = 50000) -> BDSResult:
    """
    Teste BDS para independência não-linear (versão amostral).
    H0: série é i.i.d.
    m: dimensão de incorporação (2..6)
    eps: raio (default = 0.7 * desvio-padrão)
    max_pairs: número máximo de pares aleatórios para estimativa
    """
    rng = np.random.default_rng(123)
    x = _to_numeric(series).astype(float)
    n = len(x)
    if n < 50:
        return BDSResult(m, np.nan, n, np.nan, np.nan)

    if eps is None:
        eps = 0.7 * np.std(x)

    # Função de correlação estimada via amostragem
    def corr_m_sample(x, m, eps, max_pairs):
        n = len(x) - m + 1
        if n <= 1:
            return 0.0
        embeds = np.array([x[i:i+m] for i in range(n)])
        # sorteia pares de índices
        idx_i = rng.integers(0, n, size=max_pairs)
        idx_j = rng.integers(0, n, size=max_pairs)
        dists = np.max(np.abs(embeds[idx_i] - embeds[idx_j]), axis=1)
        return np.mean(dists < eps)

    c1 = corr_m_sample(x, 1, eps, max_pairs)
    cm = corr_m_sample(x, m, eps, max_pairs)

    var = (4.0 * c1 * (2 * m - 1) * (1 - c1) * 2) / n if n > 0 else np.nan
    z = (cm - c1 ** m) / np.sqrt(var) if var > 0 else np.nan
    p = 2 * (1 - stats.norm.cdf(abs(z))) if np.isfinite(z) else np.nan

    return BDSResult(m, float(eps), n, float(z), float(p))


# Helpers para serialização
def asdict_safe(obj):
    if hasattr(obj, "_dict_"):
        try:
            return asdict(obj)
        except Exception:
            pass
    return obj