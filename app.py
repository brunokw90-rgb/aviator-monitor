# app.py
import os
import math
import traceback
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
from sqlalchemy.sql import func, text

# =========================
# Config Banco de Dados
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")

def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        # psycopg 3 - driver Python puro, sem compilação
        if "+psycopg" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
            url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        
        # Parâmetros simples para psycopg 3
        if "?" in url:
            url += "&application_name=aviator_monitor"
        else:
            url += "?sslmode=require&application_name=aviator_monitor"
            
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
        f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"
    )

# Variável global para engine
_engine = None

# metadata global (usado para criar tabelas etc.)
metadata = MetaData()

# Tabela central para persistir os resultados
multipliers = Table(
    "multipliers",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("round", BigInteger, nullable=True),
    Column("multiplier", Numeric(20, 8), nullable=False),
    Column("datetime", DateTime(timezone=True), server_default=func.now()),
    Column("source", Text, nullable=True),
    Column("raw", Text, nullable=True),
    UniqueConstraint("round", name="uq_multipliers_round"),
    Index("ix_multipliers_datetime", "datetime"),
    Index("ix_multipliers_round", "round"),
)

def db_engine() -> Engine:
    global _engine
    if _engine is None:
        try:
            # Debug da variável de ambiente
            database_url = os.getenv("DATABASE_URL")
            print(f"[DB] DATABASE_URL detectada: {bool(database_url)}")
            
            # Detectar driver disponível
            try:
                import psycopg
                driver_name = "psycopg3"
                print("[DB] psycopg 3 detectado")
            except ImportError:
                try:
                    import psycopg2
                    driver_name = "psycopg2"
                    print("[DB] psycopg2-binary detectado - usando driver estável")
                except ImportError:
                    print("[DB] ERRO: Nenhum driver PostgreSQL disponível")
                    return None
            
            url = _db_url()
            
            # Ajustar URL para o driver correto
            if driver_name == "psycopg2":
                url = url.replace("postgresql+psycopg://", "postgresql+psycopg2://")
                url = url.replace("postgresql://", "postgresql+psycopg2://")
                print(f"[DB] Conectando via psycopg2: {url[:60]}...")
            else:
                url = url.replace("postgresql+psycopg2://", "postgresql+psycopg://")
                url = url.replace("postgresql://", "postgresql+psycopg://")
                print(f"[DB] Conectando via psycopg3: {url[:60]}...")
            
            _engine = create_engine(
                url, 
                pool_pre_ping=True, 
                pool_size=10, 
                max_overflow=20,
                echo=False
            )
            metadata.create_all(_engine)
            print("[DB] SUCESSO! Engine criada e tabelas verificadas")
        except Exception as e:
            print(f"[DB] ERRO: {type(e).__name__}: {e}")
            return None
    return _engine

def save_rows(rows: list[dict], source: str | None = None) -> int:
    """CORRIGIDO: Salva apenas registros novos + adiciona timestamp atual"""
    if not rows:
        return 0

    if source:
        for r in rows:
            r.setdefault("source", source)

    try:
        eng = db_engine()
        if not eng:
            print("[DB] Engine retornou None - banco não disponível")
            return 0
        
        # FILTRO CRÍTICO: Pegar apenas rounds realmente novos
        if rows:
            # Primeiro, descobre qual é o maior round já no banco
            with eng.connect() as conn:
                result = conn.execute(text("SELECT COALESCE(MAX(round), 0) FROM multipliers"))
                max_round_db = result.fetchone()[0] or 0
            
            # Filtra apenas rounds MAIORES que o que já temos
            valid_rows = []
            current_time = datetime.now()  # Captura horário atual
            
            for r in rows:
                round_num = r.get('round')
                if round_num and round_num > max_round_db:
                    # NOVO: Define timestamp atual para novos registros
                    r['datetime'] = current_time
                    valid_rows.append(r)
            
            # Ordena por round decrescente e pega só os mais recentes
            if valid_rows:
                valid_rows = sorted(valid_rows, key=lambda x: x.get('round', 0) or 0, reverse=True)
                # Limita a 20 novos registros por vez para evitar spam
                rows = valid_rows[:20]
                print(f"[DB] Filtrados {len(rows)} registros novos (rounds > {max_round_db}) com timestamp atual")
            else:
                rows = []
                print(f"[DB] Nenhum registro novo (max round DB: {max_round_db})")
            
        inserted = 0

        if rows:  # Só tenta inserir se há dados válidos
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

        print(f"[DB] SUCESSO! Inseridas {inserted} linhas com horário atual")
        return inserted
    except Exception as e:
        print(f"[DB] ERRO AO SALVAR: {type(e).__name__}: {e}")
        return 0

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
# Error handling
# =========================
_last_error = {"trace": None}

@app.errorhandler(Exception)
def on_any_error(e):
    # guarda último stacktrace
    _last_error["trace"] = traceback.format_exc()
    raise e  # deixa o Flask/Log tratar normalmente

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
    """CORRIGIDO: Gera HTML com formatação de horário melhorada"""
    if df is None or df.empty:
        return "<em>Sem dados</em>"

    # Usar ROUND como campo de ordenação principal (mais confiável que datetime)
    round_col = None
    for c in df.columns:
        if c.lower() in ("round", "rodada", "id"):
            round_col = c
            break
    
    if round_col and round_col in df.columns:
        # Ordena por round DECRESCENTE (maior = mais recente)
        show = df.sort_values(round_col, ascending=False).head(limit).copy()
        print(f"[TABLE] Ordenado por {round_col} decrescente - primeiro round: {show.iloc[0][round_col] if not show.empty else 'N/A'}")
    else:
        # Fallback: usar index invertido
        show = df.iloc[::-1].head(limit).copy()
        print("[TABLE] Usando ordenação por index invertido")
    
    # NOVO: Formatar datetime para mostrar só hora:minuto:segundo se for de hoje
    if 'datetime' in show.columns:
        def format_datetime(dt):
            if pd.isna(dt):
                return "N/A"
            if hasattr(dt, 'strftime'):
                # Se for de hoje, mostra só hora
                if dt.date() == datetime.now().date():
                    return dt.strftime("%H:%M:%S")
                else:
                    return dt.strftime("%d/%m %H:%M:%S")
            return str(dt)
        
        show['datetime'] = show['datetime'].apply(format_datetime)

    return show.to_html(index=False, classes="mono", escape=False)

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

      <div class="muted" style="margin-top:8px">Últimos registros (round decrescente)</div>
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
    window.__liveTimer = setInterval(refreshLive, 3000); // Reduzido para 3s
  }

  document.addEventListener('visibilitychange', ()=>{ if(!document.hidden) refreshLive(); });
  if(document.readyState==='loading'){ document.addEventListener('DOMContentLoaded', start); } else { start(); }
})();
</script>
</body>
</html>
"""

# =========================
# Rotas do Banco de Dados
# =========================

@app.get("/db/count")
@login_required
def db_count():
    """Retorna contagem total de registros no banco"""
    try:
        eng = db_engine()
        if not eng:
            return jsonify({"ok": False, "error": "Banco não disponível"}), 500
            
        with eng.connect() as conn:
            # Usando text() para SQL raw
            result = conn.execute(text("SELECT COUNT(*) as total FROM multipliers"))
            total = result.fetchone()[0]
            
            # Também pega estatísticas básicas
            stats_result = conn.execute(text("""
                SELECT 
                    MIN(multiplier) as min_mult,
                    MAX(multiplier) as max_mult,
                    AVG(multiplier) as avg_mult,
                    COUNT(*) as total_records,
                    MAX(datetime) as last_update,
                    MAX(round) as max_round
                FROM multipliers 
                WHERE multiplier IS NOT NULL
            """))
            stats = stats_result.fetchone()
            
        return jsonify({
            "ok": True,
            "total_records": total,
            "stats": {
                "min_multiplier": float(stats[0]) if stats[0] else None,
                "max_multiplier": float(stats[1]) if stats[1] else None,
                "avg_multiplier": round(float(stats[2]), 4) if stats[2] else None,
                "last_update": stats[4].isoformat() if stats[4] else None,
                "max_round": int(stats[5]) if stats[5] else None
            }
        })
        
    except Exception as e:
        print(f"[DB COUNT] Erro: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/db/last")
@login_required
def db_last():
    """CORRIGIDO: Retorna os últimos N registros ORDENADOS POR ROUND DECRESCENTE"""
    try:
        limit = request.args.get("limit", "50")
        limit = max(1, min(1000, int(limit)))
        
        eng = db_engine()
        if not eng:
            return jsonify({"ok": False, "error": "Banco não disponível"}), 500
            
        with eng.connect() as conn:
            # CRÍTICO: Ordenar por round DESC (maior primeiro)
            result = conn.execute(text("""
                SELECT id, round, multiplier, datetime, source 
                FROM multipliers 
                ORDER BY round DESC NULLS LAST, id DESC 
                LIMIT :limit_val
            """), {"limit_val": limit})
            
            records = []
            for row in result:
                records.append({
                    "id": row[0],
                    "round": row[1],
                    "multiplier": float(row[2]) if row[2] else None,
                    "datetime": row[3].isoformat() if row[3] else None,
                    "source": row[4]
                })
            
        return jsonify({
            "ok": True,
            "limit": limit,
            "count": len(records),
            "records": records
        })
        
    except Exception as e:
        print(f"[DB LAST] Erro: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/db/stats")
@login_required
def db_stats():
    """CORRIGIDO: Retorna estatísticas dos registros mais recentes por ROUND"""
    try:
        window = request.args.get("window", str(WINDOW))
        window = max(1, int(window))
        
        eng = db_engine()
        if not eng:
            return jsonify({"ok": False, "error": "Banco não disponível"}), 500
            
        with eng.connect() as conn:
            # CRÍTICO: Ordenar por round DESC
            result = conn.execute(text("""
                SELECT multiplier
                FROM multipliers 
                WHERE multiplier IS NOT NULL
                ORDER BY round DESC NULLS LAST, id DESC 
                LIMIT :window_val
            """), {"window_val": window})
            
            multipliers = [float(row[0]) for row in result]
            
        if not multipliers:
            return jsonify({
                "ok": True,
                "window": window,
                "stats": {"n": 0, "message": "Nenhum dado encontrado"}
            })
            
        s = pd.Series(multipliers)
        freqs = compute_freqs(s, window=window)
        
        return jsonify({
            "ok": True,
            "window": window,
            "stats": freqs
        })
        
    except Exception as e:
        print(f"[DB STATS] Erro: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================
# Rotas principais
# =========================

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
    """CORRIGIDO: Dashboard com horários formatados"""
    try:
        # Buscar dados diretamente do banco para garantir consistência
        eng = db_engine()
        if eng:
            with eng.connect() as conn:
                # Pegar os últimos registros por round para exibir
                result = conn.execute(text("""
                    SELECT multiplier, datetime, round 
                    FROM multipliers 
                    WHERE multiplier IS NOT NULL
                    ORDER BY round DESC NULLS LAST 
                    LIMIT 100
                """))
                
                rows = []
                for row in result:
                    rows.append({
                        "multiplier": float(row[0]),
                        "datetime": row[1] if row[1] else datetime.now(),  # Fallback para agora
                        "round": row[2]
                    })
                
                df = pd.DataFrame(rows)
        else:
            # Fallback para fonte externa se banco não disponível
            df = load_df(CSV_PATH, JSON_URL)
            
        s = get_multiplier_series(df)
        freqs = compute_freqs(s, window=WINDOW)
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = "Banco PostgreSQL" if eng else (JSON_URL if JSON_URL else CSV_PATH)
        table_html = build_table_html(df)
        
    except Exception as e:
        # Loga stacktrace completo
        app.logger.exception("Erro no dashboard")
        # Fallback bem simples para não quebrar
        freqs = {
            "n": 0, "min": None, "mean": None, "std": None,
            "p50": None, "p90": None, "p99": None,
            "window": WINDOW,
            "cuts": {"2x": 0, "5x": 0, "10x": 0, "20x": 0}
        }
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = "Erro: " + str(e)[:100]
        table_html = "<em>Erro ao carregar dados</em>"

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
    """CORRIGIDO: API que prioriza dados do banco mas ainda coleta novos dados"""
    # Primeiro, tenta coletar dados novos da fonte externa
    try:
        df_source = load_df(CSV_PATH, JSON_URL)
        if not df_source.empty:
            # Prepara dados para salvar (só os válidos)
            rows = []
            col_mult = next((c for c in df_source.columns if c.lower() in ("multiplier","mult","m","valor","value","x")), None)
            col_round = next((c for c in df_source.columns if c.lower() in ("round","rodada","id")), None)
            col_dt = next((c for c in df_source.columns if c.lower() in ("datetime","data","date","hora","time")), None)

            if col_mult and col_round:  # Precisa ter pelo menos multiplier e round
                for _, r in df_source.iterrows():
                    if pd.notna(r[col_mult]) and pd.notna(r[col_round]):
                        rows.append({
                            "multiplier": float(r[col_mult]),
                            "round": int(r[col_round]),
                            "datetime": pd.to_datetime(r[col_dt], errors="coerce") if col_dt else None,
                            "raw": None,
                        })

            # Salva novos dados (função já filtra por round > max_round)
            inserted = save_rows(rows, source=JSON_URL or CSV_PATH)
            print(f"[API_LIVE] Processados {len(rows)} registros, inseridos: {inserted}")
    except Exception as e:
        print(f"[API_LIVE] Erro ao coletar dados externos: {e}")
        inserted = 0

    # Agora busca dados do banco para exibir (sempre atualizado)
    try:
        eng = db_engine()
        if eng:
            with eng.connect() as conn:
                # Buscar dados do banco ordenados corretamente
                result = conn.execute(text("""
                    SELECT multiplier, datetime, round 
                    FROM multipliers 
                    WHERE multiplier IS NOT NULL
                    ORDER BY round DESC NULLS LAST 
                    LIMIT :limit_val
                """), {"limit_val": WINDOW})
                
                rows = []
                for row in result:
                    rows.append({
                        "multiplier": float(row[0]),
                        "datetime": row[1] if row[1] else None,
                        "round": row[2]
                    })
                
                df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame(columns=["multiplier", "datetime", "round"])
            
    except Exception as e:
        print(f"[API_LIVE] Erro ao buscar do banco: {e}")
        df = pd.DataFrame(columns=["multiplier", "datetime", "round"])

    # Calcula métricas e monta resposta
    s = get_multiplier_series(df)
    freqs = compute_freqs(s, window=WINDOW)
    table_html = build_table_html(df)
        
    return jsonify({
        "ok": True,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "window": WINDOW,
        "freqs": freqs,
        "table_html": table_html,
        "inserted_new": inserted  # Para debug
    })

# =========================
# Debug helpers
# =========================

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

@app.get("/debug/python")
def debug_python():
    import sys, platform
    return {
        "python_version": sys.version,
        "python_version_info": list(sys.version_info),
        "platform": platform.platform(),
        "executable": sys.executable
    }

@app.get("/debug/sample")
def dbg_sample():
    try:
        df = load_df(CSV_PATH, JSON_URL)
        shape = [int(df.shape[0]), int(df.shape[1])]
        cols = list(df.columns)
        
        # Ordena por round decrescente para debug
        if "round" in df.columns:
            df_sorted = df.sort_values("round", ascending=False)
            rows = df_sorted.head(10).to_dict(orient="records")
        else:
            rows = df.head(10).to_dict(orient="records")
            
        return jsonify({
            "ok": True, 
            "shape": shape, 
            "columns": cols, 
            "rows": rows,
            "note": "Ordenado por round decrescente para debug"
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

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
            "scheme": p.scheme,
            "username": p.username,
            "hostname": p.hostname,
            "port": p.port,
            "path": p.path,
            "query": p.query,
        }, 200
    except Exception as e:
        return {"ok": False, "error": repr(e)}, 200


@app.get("/db/reset")
@login_required  
def db_reset():
    """Reset completo da tabela - USE COM CUIDADO"""
    try:
        eng = db_engine()
        if not eng:
            return jsonify({"ok": False, "error": "Banco não disponível"}), 500
            
        with eng.begin() as conn:
            result = conn.execute(text("DELETE FROM multipliers"))
            deleted = result.rowcount or 0
            
        return jsonify({
            "ok": True,
            "deleted_records": deleted,
            "message": f"Tabela resetada! {deleted} registros removidos"
        })
        
    except Exception as e:
        print(f"[DB RESET] Erro: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================
# Main
# =========================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)