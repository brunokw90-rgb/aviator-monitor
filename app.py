import os
import math
from datetime import datetime
from functools import wraps

import pandas as pd
import requests
from flask import (
    Flask, request, redirect, url_for, session,
    render_template, render_template_string, flash, jsonify
)

import re

def _parse_multiplier(x):
    """Extrai número de strings como '1.0JS:1' -> 1.0; aceita int/float direto."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(',', '.')  # vírgula -> ponto, se vier assim
    m = re.search(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', s)
    return float(m.group()) if m else None

# =========================
# Config (variáveis de ambiente)
# =========================
# Secret key obrigatória para sessões no Flask
_secret = (os.getenv("SECRET_KEY") or "").strip()
if not _secret:
    # fallback para não quebrar se não houver SECRET_KEY no Render
    _secret = os.urandom(32).hex()
SECRET_KEY = _secret

# Credenciais (aceita USERNAME/PASSWORD ou APP_USER/APP_PASS)
APP_USER = (os.getenv("USERNAME") or os.getenv("APP_USER", "admin")).strip()
APP_PASS = (os.getenv("PASSWORD") or os.getenv("APP_PASS", "admin123")).strip()

# Fonte de dados (priorize JSON; deixe CSV vazio se não usar)
JSON_URL = (os.getenv("DASHBOARD_JSON_URL", "")).strip()
CSV_PATH = (os.getenv("CSV_PATH", "audit_out/live_rollup.csv")).strip()

# Janela para cálculo de frequências
try:
    WINDOW = int((os.getenv("FREQ_WINDOW", "500")).strip())
except Exception:
    WINDOW = 500

# =========================
# Flask
# =========================
app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

@app.after_request
def no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# log simples para debug (remova em produção)
print("FLASK SECRET_KEY set?", bool(app.config.get("SECRET_KEY")))
print("Using JSON_URL?" , bool(JSON_URL), "| CSV_PATH:", CSV_PATH)

# =========================
# Helpers de autenticação
# =========================
def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapper


# =========================
# Leitura de dados (corrigidas p/ 'resultados' e 'multiplicador')
# =========================
def load_df_from_json(url: str) -> pd.DataFrame:
    """
    Lê o endpoint JSON. Aceita:
    - lista direta: [{"multiplicador": 1.2, "date": "...", "end": "..."}]
    - objeto com lista: {"data": [...]}, {"result": [...]}, {"resultados": [...]}, etc.
    Normaliza p/ DataFrame com colunas ['multiplier', 'datetime', 'round'].
    """
    if not url:
        raise ValueError("JSON_URL vazio")

    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    # Extrair a lista de registros
    if isinstance(data, dict):
        # inclui 'resultados' (observado no seu endpoint)
        for k in ("data", "result", "results", "resultados", "velas", "candles", "items"):
            if k in data and isinstance(data[k], list):
                data = data[k]
                break

    if not isinstance(data, list):
        raise ValueError("JSON não está em formato de lista de registros")

    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue

        mult_raw = (
            item.get("multiplier")
            or item.get("multiplicador")
            or item.get("mult")
            or item.get("m")
            or item.get("valor")
            or item.get("value")
            or item.get("x")
        )
        mult = _parse_multiplier(mult_raw)  # <-- usa o parser novo

        # data/hora
        date_part = item.get("datetime") or item.get("timestamp") or item.get("date") or item.get("data")
        time_part = item.get("time") or item.get("hora") or item.get("end")
        if date_part and time_part and isinstance(date_part, str) and isinstance(time_part, str):
            dt = f"{date_part} {time_part}"
        else:
            dt = date_part or time_part

        rows.append({
            "multiplier": mult,
            "datetime": dt,
            "round": item.get("round") or item.get("rodada") or item.get("id")
        })

    df = pd.DataFrame(rows)

    # Já vem limpo; só descarta NaN
    return df.dropna(subset=["multiplier"])


def load_df(csv_path: str, json_url: str) -> pd.DataFrame:
    """
    Tenta JSON primeiro; se não vier nada, tenta CSV local.
    """
    # 1) JSON
    if json_url:
        try:
            df = load_df_from_json(json_url)
            if not df.empty:
                return df
        except Exception as e:
            print("load_df JSON error:", e)

    # 2) CSV
    try:
        if csv_path and os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            # detectar coluna de multiplicador (inclui 'multiplicador')
            for c in df.columns:
                if c.lower() in ("multiplier", "multiplicador", "mult", "m", "valor", "value", "x"):
                    df = df.rename(columns={c: "multiplier"})
                    df["multiplier"] = pd.to_numeric(df["multiplier"], errors="coerce")
                    df = df.dropna(subset=["multiplier"])
                    return df
    except Exception as e:
        print("load_df CSV error:", e)

    # Se nada deu, retorna DF vazio com a coluna esperada
    return pd.DataFrame(columns=["multiplier"])


def get_multiplier_series(df: pd.DataFrame) -> pd.Series:
    """
    Retorna a Series com os multiplicadores (float).
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if "multiplier" in df.columns:
        s = pd.to_numeric(df["multiplier"], errors="coerce")
        return s.dropna()
    # fallback defensivo
    for c in df.columns:
        if c.lower() in ("multiplier", "multiplicador", "mult", "m", "valor", "value", "x"):
            return pd.to_numeric(df[c], errors="coerce").dropna()
    return pd.Series(dtype=float)


def compute_freqs(series: pd.Series, window: int = 500) -> dict:
    """
    Calcula frequências e estatísticas básicas para cortes 2x, 5x, 10x, 20x
    na janela mais recente (window).
    """
    if series is None or series.empty:
        return {
            "window": 0,
            "n": 0,
            "cuts": {},
            "mean": None,
            "std": None,
            "min": None,
            "p50": None,
            "p90": None,
            "p99": None,
        }

    s = series.dropna()
    if window and len(s) > window:
        s = s.iloc[-window:]

    def pct(th):
        if len(s) == 0:
            return 0.0
        return round((s >= th).mean() * 100, 2)

    cuts = {
        "2x":  pct(2.0),
        "5x":  pct(5.0),
        "10x": pct(10.0),
        "20x": pct(20.0),
    }

    stats = {
        "window": int(window if window and len(series) >= window else len(s)),
        "n": int(len(s)),
        "cuts": cuts,
        "mean": round(s.mean(), 4) if len(s) else None,
        "std":  round(s.std(ddof=1), 4) if len(s) > 1 else None,
        "min":  float(s.min()) if len(s) else None,
        "p50":  float(s.quantile(0.5)) if len(s) else None,
        "p90":  float(s.quantile(0.9)) if len(s) else None,
        "p99":  float(s.quantile(0.99)) if len(s) else None,
    }
    return stats


# =========================
# Templates inline
# =========================
LOGIN_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>Login • Aviator Monitor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial,sans-serif;background:#0b1220;color:#e5eeff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
    .card{background:#111a2e;padding:32px;border-radius:14px;box-shadow:0 10px 30px rgba(0,0,0,.35);width:320px}
    h1{margin:0 0 16px 0;font-size:20px}
    label{display:block;margin:10px 0 6px 0;font-size:13px;color:#b6c2e2}
    input{width:100%;padding:10px;border-radius:8px;border:1px solid #233052;background:#0e1628;color:#eaeef5}
    button{margin-top:16px;width:100%;padding:10px;border:0;border-radius:8px;background:#3b82f6;color:white;font-weight:600;cursor:pointer}
    .err{color:#ff7b7b;margin-top:8px;font-size:13px}
  </style>
</head>
<body>
  <form class="card" method="post">
    <h1>Aviator Monitor</h1>
    <label>Usuário</label>
    <input name="username" autocomplete="username" required>
    <label>Senha</label>
    <input name="password" type="password" autocomplete="current-password" required>
    <button type="submit">Entrar</button>
    {% if error %}<div class="err">{{error}}</div>{% endif %}
  </form>
</body>
</html>
"""

DASH_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>Aviator Monitor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{--bg:#0b1220;--panel:#0f1a31;--muted:#9fb0d3;--hi:#22c55e;--warn:#f59e0b;--danger:#ef4444;--card:#111d38}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial,sans-serif;background:var(--bg);color:#eaf1ff;margin:0}
    header{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;background:var(--panel);position:sticky;top:0}
    .wrap{max-width:1000px;margin:20px auto;padding:0 16px}
    h1{font-size:18px;margin:0}
    .grid{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:12px;margin-top:16px}
    .card{background:var(--card);padding:14px;border-radius:10px}
    .muted{color:var(--muted);font-size:12px}
    .big{font-size:24px;font-weight:700}
    .ok{color:var(--hi)} .warn{color:var(--warn)} .bad{color:var(--danger)}
    table{width:100%;border-collapse:collapse;margin-top:18px}
    th,td{border-bottom:1px solid #223055;padding:8px;text-align:left;font-size:13px}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,"Courier New",monospace}
    .pill{font-size:12px;padding:4px 8px;border-radius:999px;background:#223055;color:#cfe1ff}
    .btn{background:#334155;border:0;color:#fff;padding:8px 12px;border-radius:8px;cursor:pointer;text-decoration:none}
  </style>
</head>
<body>
  <header>
    <h1>Aviator Monitor</h1>
    <div class="muted">Janela: {{window}} • Fonte: {{source_data}}</div>
    <div><a class="btn" href="{{url_for('logout')}}">Sair</a></div>
  </header>

  <div class="wrap">
    <div class="grid">
  <div class="card"><div class="muted">Total (n)</div><div class="big" id="n">{{ freqs.n }}</div></div>
  <div class="card"><div class="muted">Média</div><div class="big mono" id="mean">{{ freqs.mean }}</div></div>
  <div class="card"><div class="muted">Desvio</div><div class="big mono" id="std">{{ freqs.std }}</div></div>
  <div class="card"><div class="muted">P90</div><div class="big mono" id="p90">{{ freqs.p90 }}</div></div>
</div>

<div class="grid" style="margin-top:12px">
  <div class="card"><div class="muted">≥ 2x</div><div class="big ok"   id="ge2">{{  freqs.cuts["2x"]  }}%</div></div>
  <div class="card"><div class="muted">≥ 5x</div><div class="big warn" id="ge5">{{  freqs.cuts["5x"]  }}%</div></div>
  <div class="card"><div class="muted">≥ 10x</div><div class="big warn" id="ge10">{{ freqs.cuts["10x"] }}%</div></div>
  <div class="card"><div class="muted">≥ 20x</div><div class="big bad"  id="ge20">{{ freqs.cuts["20x"] }}%</div></div>
</div>

<div class="card" style="margin-top:12px">
  <div class="muted">Atualizado em</div>
  <div class="mono" id="updated_at">{{ updated_at }}</div>

  <div class="muted" style="margin-top:8px">Últimos 50 registros</div>
  <div id="table_wrap">{{ table_html | safe }}</div>
</div>
  </div>

<script>
const t = document.getElementById('table_html');
  if (t && j.table_html) t.innerHTML = j.table_html;   // <-- atualiza a tabela
const $ = id => document.getElementById(id);
const fmtNum = v => (v == null || v === '') ? '—' : Number(v).toFixed(4);
const fmtPct = v => {
  if (v == null || v === '') return '—';
  const n = Number(v);
  if (isNaN(n)) return '—';
  // Se vier 0–1 (fração), converte; se já vier em % (ex. 54.1), mantém
  const perc = n <= 1 ? n * 100 : n;
  return perc.toFixed(2) + '%';
};

<script>
(function(){
  const IDs = ['n','mean','std','p90','ge2','ge5','ge10','ge20','updated_at','table_wrap'];

  function hasAll(){ return IDs.every(id => document.getElementById(id)); }
  function setText(id, v){ const el = document.getElementById(id); if (el && v!=null) el.textContent = v; }
  function fmt4(x){ return (x==null||isNaN(x)) ? '' : Number(x).toFixed(4); }
  function pct(x){ return (x==null||isNaN(x)) ? '' : (Number(x).toFixed(2)+'%'); }

  async function refreshLive(){
    try {
      const res = await fetch('/api/live?ts='+Date.now(), { cache:'no-store' });
      const j = await res.json();
      console.log('tick /api/live =>', j);     // <-- veja no Console
      if (!j || !j.ok) return;

      const f = j.freqs || {};
      const cuts = f.cuts || {};

      setText('n',   f.n);
      setText('mean',fmt4(f.mean));
      setText('std', fmt4(f.std));
      setText('p90', fmt4(f.p90));
      setText('ge2',  pct(cuts['2x']  ?? f.ge_2x));
      setText('ge5',  pct(cuts['5x']  ?? f.ge_5x));
      setText('ge10', pct(cuts['10x'] ?? f.ge_10x));
      setText('ge20', pct(cuts['20x'] ?? f.ge_20x));

      const wrap = document.getElementById('table_wrap');
      if (wrap && j.table_html) wrap.innerHTML = j.table_html;

      setText('updated_at', new Date().toLocaleString());
    } catch (err) {
      console.error('refresh error:', err);
    }
  }

  function start(){
    if (!hasAll()){ console.warn('IDs faltando no HTML'); return; }
    refreshLive();
    window._liveTimer && clearInterval(window._liveTimer);
    window.__liveTimer = setInterval(refreshLive, 2000);
  }

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refreshLive();
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
</script>
</body>
</html>
"""
def build_table_html(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<em>Sem dados</em>"

    preferred = (
        "multiplier", "mult", "m", "valor", "value", "x",
        "datetime", "data", "date", "hora", "time",
        "round", "rodada", "id"
    )
    cols = [c for c in df.columns if c.lower() in preferred] or list(df.columns)

    # tenta ordenar por alguma coluna temporal/ID
    sort_candidates = ["datetime", "date", "hora", "time", "round", "rodada", "id"]
    sort_col = next((c for c in sort_candidates if c in df.columns), None)

    df_view = df
    if sort_col:
        df_view = df.sort_values(by=sort_col, ascending=False)

    show = df_view[cols].head(50)
    return show.to_html(index=False, classes="mono")

# =========================
# Rotas
# =========================
@app.get("/health")
def health():
    return "ok", 200


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if u == APP_USER and p == APP_PASS:
            session["logged_in"] = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        error = "Usuário ou senha inválidos."
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    flash("Você saiu da sessão.", "info")
    return redirect(url_for("login"))


@app.get("/")
@login_required
def dashboard():
    df = load_df(CSV_PATH, JSON_URL)
    freqs = compute_freqs(df)
    table_html = build_table_html(df)
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source = JSON_URL if JSON_URL else CSV_PATH

    return render_template_string(
        DASH_HTML,
        freqs=freqs,
        updated_at=updated_at,
        window=WINDOW,
        source_data=source,
        table_html=table_html   # <- garanta que esse campo existe aqui
    )


@app.get("/api/live")
def api_live():
    df = load_df(CSV_PATH, JSON_URL)
    freqs = compute_freqs(df)
    table_html = build_table_html(df)

    return jsonify({
        "ok": True,
        "freqs": freqs,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "window": WINDOW,
        # aqui precisa devolver a tabela também!
        "table_html": table_html
    })

# =========================
# Debug helpers (somente para diagnóstico)
# =========================
@app.get("/ping")
def ping():
    return "pong", 200

@app.get("/debug/source")
def dbg_source():
    return jsonify({
        "json_url": JSON_URL or None,
        "csv_path": CSV_PATH or None,
        "using_json": bool(JSON_URL),
        "using_csv": bool(CSV_PATH and not JSON_URL),
        "window": WINDOW,
    })

@app.get("/debug/sample")
def dbg_sample():
    try:
        df = load_df(CSV_PATH, JSON_URL)
        rows = df.head(5).to_dict(orient="records") if not df.empty else []
        return jsonify({
            "ok": True,
            "shape": list(df.shape),
            "columns": list(df.columns),
            "rows": rows
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/debug/raw")
def dbg_raw():
    if not JSON_URL:
        return jsonify({"ok": False, "error": "JSON_URL vazio"}), 400
    try:
        r = requests.get(JSON_URL, timeout=15)
        info = {
            "status_code": r.status_code,
            "headers_content_type": r.headers.get("Content-Type"),
            "text_prefix": r.text[:1000],   # primeiras 1000 chars p/ inspeção
            "length": len(r.text),
        }
        return jsonify({"ok": True, "info": info})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================
# Main (local)
# =========================
if __name__ == "__main__":
    # debug=True só localmente
    app.run(host="0.0.0.0", port=5000, debug=True)