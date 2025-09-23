# app.py
import os
import math
from datetime import datetime
from functools import wraps

import pandas as pd
import requests
from flask import (
    Flask, request, redirect, url_for, session,
    render_template_string, flash, jsonify
)

# =========================
# Config (variáveis de ambiente)
# =========================
# SECRET_KEY obrigatório para sessões
_secret = os.getenv("SECRET_KEY")
if not _secret or not _secret.strip():
    # fallback seguro (não exponha em logs)
    _secret = os.urandom(32).hex()
SECRET_KEY = _secret

# Credenciais de login (ambas aceitas: USERNAME/PASSWORD ou APP_USER/APP_PASS)
APP_USER = os.getenv("USERNAME", os.getenv("APP_USER", "admin"))
APP_PASS = os.getenv("PASSWORD", os.getenv("APP_PASS", "admin123"))

# Fonte de dados
JSON_URL = os.getenv("DASHBOARD_JSON_URL", "").strip()
CSV_PATH = os.getenv("CSV_PATH", "audit_out/live_rollup.csv").strip()

# Janela (tamanho do "rollup" para métricas)
WINDOW = int(os.getenv("FREQ_WINDOW", "500"))

import sys
print(f"🐍 Python version being used: {sys.version}")

# =========================
# Database (Postgres via SQLAlchemy)
# =========================
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    BigInteger, Numeric, Text, DateTime, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func

# =========================
# Config Banco de Dados
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")

def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        # Força psycopg2 para compatibilidade
        if "+psycopg2" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            url = url.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
        
        # Forçar IPv4 e melhorar conectividade
        if "?" in url:
            url += "&target_session_attrs=read-write&keepalives_idle=600&connect_timeout=10"
        else:
            url += "?sslmode=require&target_session_attrs=read-write&keepalives_idle=600&connect_timeout=10"
            
        return url

    # fallback por partes (se precisar)
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
    
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError("Defina DATABASE_URL ou todas as variáveis do banco.")
    return (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}&target_session_attrs=read-write&keepalives_idle=600&connect_timeout=10"
    )

# Variável global para engine
_engine = None
_db_enabled = True  # Flag para controlar se o banco está funcionando

# metadata global (usado para criar tabelas etc.)
metadata = MetaData()

# Tabela central para persistir os resultados
multipliers = Table(
    "multipliers",
    metadata,  # CORRIGIDO: era _metadata
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("round", BigInteger, nullable=True),
    Column("multiplier", Numeric(20, 8), nullable=False),
    Column("datetime", DateTime(timezone=True), server_default=func.now()),
    Column("source", Text, nullable=True),
    Column("raw", Text, nullable=True),
    UniqueConstraint("round", name="uq_multipliers_round"),
    Index("ix_multipliers_datetime", "datetime"),
)

def db_engine() -> Engine:
    global _engine, _db_enabled
    if _engine is None and _db_enabled:
        try:
            _engine = create_engine(
                _db_url(), 
                pool_pre_ping=True, 
                pool_size=5, 
                max_overflow=5,
                connect_args={
                    "connect_timeout": 10,
                    "server_settings": {"application_name": "aviator_monitor"},
                    "options": "-c default_transaction_isolation=read_committed"
                }
            )
            metadata.create_all(_engine)
        except Exception as e:
            print(f"[DB] Desabilitando banco devido a erro de driver: {e}")
            _db_enabled = False
            return None
    return _engine if _db_enabled else None

def save_rows(rows: list[dict], source: str | None = None) -> int:
    if not _db_enabled:
        print("[DB] Banco desabilitado - dados não salvos")
        return 0
        
    if not rows:
        return 0

    if source:
        for r in rows:
            r.setdefault("source", source)

    eng = db_engine()
    if not eng:
        return 0
        
    inserted = 0

    try:
        with eng.begin() as conn:
            has_round = any(r.get("round") is not None for r in rows)
            if has_round:
                stmt = pg_insert(multipliers).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["round"])
                result = conn.execute(stmt)
                inserted = result.rowcount if result.rowcount and result.rowcount > 0 else 0
            else:
                result = conn.execute(multipliers.insert(), rows)
                inserted = result.rowcount or 0
    except Exception as e:
        print(f"[DB] Erro ao salvar (banco será desabilitado): {e}")
        _db_enabled = False
        return 0

    return inserted

# Teste de conexão ao carregar o módulo
try:
    engine = db_engine()
    print("[DB] Conexão criada com sucesso:", engine.url)
except Exception as e:
    print("[DB] Erro ao conectar:", e)

# =========================
# Flask
# =========================
app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

# =========================
# Auth helpers
# =========================
def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            nxt = request.path if request.method == "GET" else url_for("dashboard")
            return redirect(url_for("login", next=nxt))
        return view(*args, **kwargs)
    return wrapper

# =========================
# Leitura de dados
# =========================
def load_df_from_json(url: str) -> pd.DataFrame:
    """
    Tenta ler dados do endpoint JSON. Aceita formatos:
    - lista de objetos: [{"multiplier": 1.2, "time": "..."}]
    - objeto com lista: {"data": [...]} / {"result": [...]} / {"velas": [...]} etc.
    Normaliza para um DataFrame com as colunas possíveis.
    """
    if not url:
        raise ValueError("JSON_URL vazio")

    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    # Extrair a lista
    if isinstance(data, dict):
        # Adicione "resultados" à lista de chaves possíveis
        for k in ("data", "result", "results", "velas", "candles", "items", "resultados"):
            if k in data and isinstance(data[k], list):
                data = data[k]
                break

    if not isinstance(data, list):
        raise ValueError("JSON não está em formato de lista de registros")

    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue

        # Adicione "multiplicador" às chaves possíveis
        mult = (
            item.get("multiplier")
            or item.get("mult")
            or item.get("m")
            or item.get("valor")
            or item.get("value")
            or item.get("x")
            or item.get("multiplicador")
        )

        # Data/hora: tente montar a string com "date" e "end" se existirem
        dt = (
            item.get("timestamp")
            or item.get("data")
            or item.get("date")
            or item.get("hora")
            or item.get("time")
            or (f'{item.get("date", "")} {item.get("end", "")}'.strip() if item.get("date") and item.get("end") else None)
        )

        rows.append({
            "multiplier": mult,
            "datetime": dt,
            "round": item.get("round") or item.get("rodada") or item.get("id")
        })

    df = pd.DataFrame(rows)
    if "multiplier" in df.columns:
        df["multiplier"] = pd.to_numeric(df["multiplier"], errors="coerce")
    return df.dropna(subset=["multiplier"])


def load_df(csv_path: str, json_url: str) -> pd.DataFrame:
    """
    Tenta JSON primeiro; se falhar, tenta CSV local.
    Retorna DataFrame com coluna 'multiplier' e, se existentes, 'datetime' e 'round'.
    """
    # 1) JSON
    if json_url:
        try:
            df = load_df_from_json(json_url)
            if not df.empty:
                return df
        except Exception as e:
            print("JSON load failed:", e)

    # 2) CSV local
    try:
        if csv_path and os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            # normaliza multiplier
            for c in df.columns:
                if c.lower() in ("multiplier", "multiplicador", "mult", "m", "valor", "value", "x"):
                    df = df.rename(columns={c: "multiplier"})
                    df["multiplier"] = pd.to_numeric(df["multiplier"], errors="coerce")
                    break

            # normaliza datetime (se houver)
            for c in df.columns:
                if c.lower() in ("datetime", "timestamp", "data", "date", "hora", "time", "end"):
                    try:
                        df = df.rename(columns={c: "datetime"})
                        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                    except Exception:
                        pass
                    break

            # normaliza round (se houver)
            for c in df.columns:
                if c.lower() in ("round", "rodada", "id"):
                    df = df.rename(columns={c: "round"})
                    break

            df = df.dropna(subset=["multiplier"])
            return df
    except Exception as e:
        print("CSV load failed:", e)

    # vazio como fallback
    return pd.DataFrame(columns=["multiplier", "datetime", "round"])


def get_multiplier_series(df: pd.DataFrame) -> pd.Series:
    """Retorna a série dos multiplicadores como float."""
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if "multiplier" in df.columns:
        s = pd.to_numeric(df["multiplier"], errors="coerce")
        return s.dropna()
    # tentativa por nomes
    for c in df.columns:
        if c.lower() in ("multiplier", "multiplicador", "mult", "m", "valor", "value", "x"):
            return pd.to_numeric(df[c], errors="coerce").dropna()
    return pd.Series(dtype=float)

# =========================
# Métricas
# =========================
def compute_freqs(s: pd.Series, window: int = 500) -> dict:
    """
    Calcula estatísticas e cortes ≥2x, ≥5x, ≥10x, ≥20x.
    Retorna dicionário pronto para JSON/template.
    """
    if s is None or len(s) == 0:
        return {
            "n": 0, "min": None, "mean": None, "std": None,
            "p50": None, "p90": None, "p99": None, "window": window,
            "cuts": {"2x": 0, "5x": 0, "10x": 0, "20x": 0}
        }

    if window and len(s) > window:
        s = s.iloc[-window:]

    n = int(s.count())
    mn = float(s.min())
    mean = float(s.mean())
    std = float(s.std(ddof=1)) if n > 1 else 0.0
    p50 = float(s.quantile(0.50))
    p90 = float(s.quantile(0.90))
    p99 = float(s.quantile(0.99))

    def pct_ge(th):
        return round(100.0 * float((s >= th).mean()), 2)

    cuts = {
        "2x": pct_ge(2.0),
        "5x": pct_ge(5.0),
        "10x": pct_ge(10.0),
        "20x": pct_ge(20.0),
    }

    return {
        "n": n, "min": mn, "mean": round(mean, 4), "std": round(std, 4),
        "p50": round(p50, 4), "p90": round(p90, 4), "p99": round(p99, 4),
        "window": window, "cuts": cuts
    }

def build_table_html(df: pd.DataFrame, limit: int = 50) -> str:
    """Gera HTML da tabela com últimas N linhas (prioriza colunas úteis, mais recentes primeiro)."""
    if df is None or df.empty:
        return "<em>Sem dados</em>"

    preferred = ("multiplier", "mult", "m", "valor", "value",
                 "datetime", "data", "date", "hora", "time", "end",
                 "round", "rodada", "id")
    cols = [c for c in df.columns if c.lower() in preferred]

    # Tenta ordenar pelo campo de data/hora, se existir
    order_col = None
    for c in ("datetime", "data", "date", "hora", "time", "end"):
        if c in df.columns:
            order_col = c
            break

    if order_col:
        show = df[cols].sort_values(order_col, ascending=False).head(limit) if cols else df.sort_values(order_col, ascending=False).head(limit)
    else:
        show = df[cols].iloc[::-1].head(limit) if cols else df.iloc[::-1].head(limit)  # Se não tiver coluna de data, inverte a ordem

    # garante ordenação do mais novo para o mais antigo (se tiver datetime como tipo datetime)
    if "datetime" in show.columns and pd.api.types.is_datetime64_any_dtype(show["datetime"]):
        show = show.sort_values("datetime", ascending=False)

    return show.to_html(index=False, classes="mono")

# =========================
# Template (Dashboard HTML)
# =========================
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
    .source{font-size:12px;color:#a9bbdf}
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
(function(){
  const IDs = ['n','mean','std','p90','ge2','ge5','ge10','ge20','updated_at','table_wrap'];
  function hasAll(){ return IDs.every(id => document.getElementById(id)); }
  function setText(id, v){ const el=document.getElementById(id); if(el!=null && v!=null) el.textContent=v; }
  function fmt4(x){ return (x==null||isNaN(x)) ? '' : Number(x).toFixed(4); }
  function pct(x){ return (x==null||isNaN(x)) ? '' : (Number(x).toFixed(2)+'%'); }

  async function refreshLive(){
    try{
      const r = await fetch('/api/live?ts='+Date.now(), { cache:'no-store' });
      const j = await r.json();
      if(!j || !j.ok) return;

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
    }catch(e){ console.error('refreshLive error:', e); }
  }

  function start(){
    if (!hasAll()){ console.warn('IDs faltando no HTML'); return; }
    refreshLive();
    window._liveTimer && clearInterval(window._liveTimer);
    window.__liveTimer = setInterval(refreshLive, 2000);
  }

  document.addEventListener('visibilitychange', ()=>{ if(!document.hidden) refreshLive(); });
  if(document.readyState==='loading'){ document.addEventListener('DOMContentLoaded', start); } else { start(); }
})();
</script>
</body>
</html>
"""

# =========================
# Rotas
# =========================

import traceback
_last_error = {"trace": None}

@app.errorhandler(Exception)
def on_any_error(e):
    # guarda último stacktrace
    _last_error["trace"] = traceback.format_exc()
    raise e  # deixa o Flask/Log tratar normalmente

@app.get("/debug/last-trace")
def last_trace():
    t = _last_error.get("trace")
    return ("<pre>"+t+"</pre>") if t else "Sem stacktrace capturado."

@app.get("/health")
def health():
    return "OK"

@app.get("/ping")
def ping():
    return "pong"

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd  = request.form.get("password", "")
        if user == APP_USER and pwd == APP_PASS:
            session["logged_in"] = True
            nxt = request.args.get("next") or url_for("dashboard")
            return redirect(nxt)
        error = "Credenciais inválidas."
    LOGIN_HTML = """
    <!doctype html>
    <html lang="pt-br"><head><meta charset="utf-8"><title>Login</title>
    <style>body{font-family:system-ui;background:#0b1220;color:#eaf1ff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
    .card{background:#111d38;padding:24px;border-radius:10px;width:320px}label{display:block;margin:10px 0 4px}
    input{width:100%;padding:10px;border-radius:8px;border:1px solid #223055;background:#0f1a31;color:#fff}button{margin-top:14px;width:100%;padding:10px;border:0;border-radius:8px;background:#334155;color:#fff;cursor:pointer}
    .err{color:#ef4444;margin-top:10px}</style></head><body>
    <div class="card"><h2>Entrar</h2>
      <form method="post">
        <label>Usuário</label><input name="username" autocomplete="username" required>
        <label>Senha</label><input name="password" type="password" autocomplete="current-password" required>
        <button type="submit">Acessar</button>
        {% if error %}<div class="err">{{error}}</div>{% endif %}
      </form>
    </div></body></html>
    """
    return render_template_string(LOGIN_HTML, error=error)

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.get("/")
@login_required
def dashboard():
    try:
        df = load_df(CSV_PATH, JSON_URL)
        s = get_multiplier_series(df)
        freqs = compute_freqs(s, window=WINDOW)
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = JSON_URL if JSON_URL else CSV_PATH
        table_html = build_table_html(df)
    except Exception as e:
        # Loga stacktrace completo no Render
        app.logger.exception("Erro no dashboard")
        # Fallback bem simples para não quebrar
        freqs = {
            "n": 0, "min": None, "mean": None, "std": None,
            "p50": None, "p90": None, "p99": None,
            "window": WINDOW,
            "cuts": {"2x": 0, "5x": 0, "10x": 0, "20x": 0}
        }
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = JSON_URL if JSON_URL else CSV_PATH
        table_html = "<em>Erro ao carregar dados (veja logs)</em>"

    return render_template_string(
        DASH_HTML,
        freqs=freqs,
        updated_at=updated_at,
        window=WINDOW,
        source_data=source,
        table_html=table_html
    )

@app.get("/api/live")
@login_required
def api_live():
    df = load_df(CSV_PATH, JSON_URL)
    s = get_multiplier_series(df)
    freqs = compute_freqs(s, window=WINDOW)
    table_html = build_table_html(df)
# --- Persistência leve (salva últimas leituras) ---
    try:
        # prepara linhas para salvar
        rows = []
        if not df.empty:
            # escolhe colunas que existirão no seu DataFrame
            col_mult = next((c for c in df.columns if c.lower() in ("multiplier","mult","m","valor","value","x")), None)
            col_round = next((c for c in df.columns if c.lower() in ("round","rodada","id")), None)
            col_dt    = next((c for c in df.columns if c.lower() in ("datetime","data","date","hora","time")), None)

            # usamos somente as N últimas para não forçar o free tier
            for _, r in df.tail(50).iterrows():
                rows.append({
                    "multiplier": float(r[col_mult]) if col_mult and pd.notna(r[col_mult]) else None,
                    "round": int(r[col_round]) if col_round and pd.notna(r[col_round]) else None,
                    "datetime": pd.to_datetime(r[col_dt], errors="coerce") if col_dt else None,
                    "raw": None,  # se quiser, json.dumps(r.to_dict(), ensure_ascii=False)
                })

            # limpa inválidos
            rows = [x for x in rows if x["multiplier"] is not None]

        # salva (upsert por round quando houver)
        inserted = save_rows(rows, source=JSON_URL or CSV_PATH)
        # log simples
        print(f"[DB] inseridas: {inserted}")
    except Exception as e:
        # não quebrar o endpoint em caso de falha do banco
        print(f"[DB] erro ao inserir: {e}")
    return jsonify({
        "ok": True,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "window": WINDOW,
        "freqs": freqs,
        "table_html": table_html
    })

# =========================
# Debug helpers
# =========================
@app.get("/debug/source")
def dbg_source():
    return jsonify({
        "using_json": bool(JSON_URL),
        "using_csv": bool(CSV_PATH),
        "json_url": JSON_URL or None,
        "csv_path": CSV_PATH or None,
        "window": WINDOW
    })

@app.get("/debug/sample")
def dbg_sample():
    try:
        df = load_df(CSV_PATH, JSON_URL)
        shape = [int(df.shape[0]), int(df.shape[1])]
        cols = list(df.columns)
        rows = df.head(5).to_dict(orient="records")
        return jsonify({"ok": True, "shape": shape, "columns": cols, "rows": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# --- DEBUG: inspeciona a URL do banco sem vazar senha
@app.get("/debug/db_url")
def dbg_db_url():
    try:
        import os
        from urllib.parse import urlsplit

        raw = os.getenv("DATABASE_URL", "")
        raw = raw.strip() if raw else ""

        # Mostra quais variáveis existem (sem valores)
        flags = {
            "has_DATABASE_URL": bool(raw),
            "has_DB_HOST": bool(os.getenv("DB_HOST")),
            "has_DB_PORT": bool(os.getenv("DB_PORT")),
            "has_DB_NAME": bool(os.getenv("DB_NAME")),
            "has_DB_USER": bool(os.getenv("DB_USER")),
            "has_DB_PASSWORD": bool(os.getenv("DB_PASSWORD")),
        }

        if not raw:
            return {"ok": False, "error": "DATABASE_URL ausente", "vars": flags}, 200

        p = urlsplit(raw)
        # Nunca devolva senha
        return {
            "ok": True,
            "vars": flags,
            "scheme": p.scheme,           # deve ser "postgresql+psycopg2"
            "username": p.username,       # ex.: postgres.<project-ref>
            "hostname": p.hostname,       # ESPERADO: aws-1-sa-east-1.pooler.supabase.com
            "port": p.port,               # 5432
            "path": p.path,               # /postgres
            "query": p.query,             # sslmode=require
        }, 200
    except Exception as e:
        return {"ok": False, "error": repr(e)}, 200