# app.py - Versão Enxugada para Fly.io
import os
import traceback
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

import pandas as pd
import requests
from flask import Flask, request, redirect, url_for, session, render_template_string, jsonify

from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Numeric, Text, DateTime, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import func, text

# =========================
# Configuração
# =========================
SECRET_KEY = os.getenv("SECRET_KEY", os.urandom(32).hex())
APP_USER = os.getenv("USERNAME", os.getenv("APP_USER", "admin"))
APP_PASS = os.getenv("PASSWORD", os.getenv("APP_PASS", "admin123"))
JSON_URL = os.getenv("DASHBOARD_JSON_URL", "").strip()
WINDOW = int(os.getenv("FREQ_WINDOW", "500"))

# Variáveis globais
_engine = None
_collector_running = False
_collector_thread = None
_last_error = {"trace": None}

# =========================
# Database
# =========================
metadata = MetaData()

multipliers = Table(
    "multipliers", metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("round", BigInteger, nullable=True),
    Column("multiplier", Numeric(20, 8), nullable=False),
    Column("datetime", DateTime(timezone=True), server_default=func.now()),
    Column("source", Text, nullable=True),
    UniqueConstraint("round", name="uq_multipliers_round"),
    Index("ix_multipliers_round", "round"),
)

def db_engine():
    global _engine
    if _engine is None:
        try:
            url = os.getenv("DATABASE_URL")
            if not url:
                raise RuntimeError("DATABASE_URL não definida")
                
            # Ajusta para psycopg2 (driver mais estável no Fly.io)
            if "postgresql://" in url and "+psycopg" not in url:
                url = url.replace("postgresql://", "postgresql+psycopg2://")
            
            _engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
            metadata.create_all(_engine)
            print("[DB] Conectado com sucesso")
        except Exception as e:
            print(f"[DB] Erro: {e}")
            return None
    return _engine

def save_rows(rows: list[dict], source: str = None) -> int:
    if not rows:
        return 0
    
    try:
        eng = db_engine()
        if not eng:
            return 0
        
        # Filtra apenas rounds novos
        with eng.connect() as conn:
            result = conn.execute(text("SELECT COALESCE(MAX(round), 0) FROM multipliers"))
            max_round_db = result.fetchone()[0] or 0
        
        valid_rows = [r for r in rows if r.get('round', 0) > max_round_db]
        
        if valid_rows:
            # Distribui timestamps retroativos
            valid_rows.sort(key=lambda x: x.get('round', 0))
            current_time = datetime.now()
            
            for i, r in enumerate(valid_rows):
                seconds_ago = (len(valid_rows) - i - 1) * 25
                r['datetime'] = current_time - timedelta(seconds=seconds_ago)
                r['source'] = source or 'AUTO'
            
            # Limita inserções
            rows = valid_rows[:50]
            
            with eng.begin() as conn:
                stmt = pg_insert(multipliers).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["round"])
                result = conn.execute(stmt)
                inserted = result.rowcount or 0
                
            print(f"[DB] Inseridas {inserted} linhas")
            return inserted
    except Exception as e:
        print(f"[DB] Erro ao salvar: {e}")
        
    return 0

# =========================
# Flask
# =========================
app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

@app.errorhandler(Exception)
def on_error(e):
    _last_error["trace"] = traceback.format_exc()
    raise e

def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapper

# =========================
# Data Loading
# =========================
def load_data_from_api(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
        
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        
        # Extrai lista do JSON
        if isinstance(data, dict):
            for k in ("data", "result", "results", "items"):
                if k in data and isinstance(data[k], list):
                    data = data[k]
                    break
        
        if not isinstance(data, list):
            return pd.DataFrame()
        
        rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
                
            mult = (item.get("multiplier") or item.get("mult") or 
                   item.get("m") or item.get("value"))
            round_num = (item.get("round") or item.get("rodada") or 
                        item.get("id"))
            
            if mult and round_num:
                rows.append({
                    "multiplier": float(mult),
                    "round": int(round_num)
                })
        
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# =========================
# Coletor Automático
# =========================
def collect_background():
    global _collector_running
    print("[COLLECTOR] Iniciado")
    
    while _collector_running:
        try:
            df = load_data_from_api(JSON_URL)
            if not df.empty:
                rows = df.to_dict('records')
                inserted = save_rows(rows, "AUTO")
                if inserted > 0:
                    print(f"[COLLECTOR] Coletados {inserted} registros")
        except Exception as e:
            print(f"[COLLECTOR] Erro: {e}")
        
        time.sleep(30)

def start_collector():
    global _collector_running, _collector_thread
    if not _collector_running:
        _collector_running = True
        _collector_thread = threading.Thread(target=collect_background, daemon=True)
        _collector_thread.start()
        return True
    return False

def stop_collector():
    global _collector_running
    if _collector_running:
        _collector_running = False
        return True
    return False

# =========================
# Métricas
# =========================
def compute_stats(multipliers: list, window: int = 500) -> dict:
    if not multipliers:
        return {"n": 0, "mean": None, "std": None, "p90": None, 
                "cuts": {"2x": 0, "5x": 0, "10x": 0, "20x": 0}}
    
    s = pd.Series(multipliers[-window:] if len(multipliers) > window else multipliers)
    
    cuts = {
        "2x": round(100 * (s >= 2.0).mean(), 2),
        "5x": round(100 * (s >= 5.0).mean(), 2),
        "10x": round(100 * (s >= 10.0).mean(), 2),
        "20x": round(100 * (s >= 20.0).mean(), 2),
    }
    
    return {
        "n": len(s),
        "mean": round(s.mean(), 4),
        "std": round(s.std(), 4),
        "p90": round(s.quantile(0.90), 4),
        "cuts": cuts
    }

# =========================
# Rotas
# =========================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        if user == APP_USER and pwd == APP_PASS:
            session["logged_in"] = True
            return redirect(url_for("dashboard"))
        return render_template_string(LOGIN_TEMPLATE, error="Credenciais inválidas")
    return render_template_string(LOGIN_TEMPLATE)

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.get("/")
@login_required
def dashboard():
    try:
        eng = db_engine()
        if eng:
            with eng.connect() as conn:
                result = conn.execute(text("""
                    SELECT multiplier, datetime, round 
                    FROM multipliers 
                    ORDER BY round DESC NULLS LAST 
                    LIMIT 100
                """))
                
                data = [{"multiplier": float(r[0]), "datetime": r[1], "round": r[2]} 
                       for r in result]
        else:
            data = []
        
        multipliers = [d["multiplier"] for d in data]
        stats = compute_stats(multipliers, WINDOW)
        
        # Tabela HTML simples
        table_rows = ""
        for d in data[:20]:  # Só primeiros 20
            dt = d["datetime"].strftime("%H:%M:%S") if d["datetime"] else "N/A"
            table_rows += f"<tr><td>{d['multiplier']}</td><td>{dt}</td><td>{d['round']}</td></tr>"
        
        return render_template_string(DASHBOARD_TEMPLATE, 
                                     stats=stats, 
                                     table_rows=table_rows,
                                     updated_at=datetime.now().strftime("%H:%M:%S"))
    except Exception as e:
        return f"Erro: {e}"

@app.get("/api/live")
@login_required
def api_live():
    try:
        # Coleta novos dados
        df = load_data_from_api(JSON_URL)
        inserted = 0
        if not df.empty:
            rows = df.to_dict('records')
            inserted = save_rows(rows, "LIVE")
        
        # Busca dados do banco
        eng = db_engine()
        multipliers = []
        if eng:
            with eng.connect() as conn:
                result = conn.execute(text("""
                    SELECT multiplier FROM multipliers 
                    ORDER BY round DESC NULLS LAST 
                    LIMIT :limit
                """), {"limit": WINDOW})
                multipliers = [float(r[0]) for r in result]
        
        stats = compute_stats(multipliers, WINDOW)
        
        return jsonify({
            "ok": True,
            "stats": stats,
            "inserted": inserted,
            "updated_at": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# Rotas do coletor
@app.get("/collector/start")
@login_required
def collector_start():
    success = start_collector()
    return jsonify({"ok": success, "message": "Iniciado" if success else "Já rodando"})

@app.get("/collector/stop")
@login_required
def collector_stop():
    success = stop_collector()
    return jsonify({"ok": success, "message": "Parado" if success else "Não estava rodando"})

@app.get("/collector/status")
@login_required
def collector_status():
    return jsonify({
        "ok": True,
        "running": _collector_running,
        "thread_alive": _collector_thread.is_alive() if _collector_thread else False
    })

# Rotas de dados
@app.get("/db/count")
@login_required
def db_count():
    try:
        eng = db_engine()
        with eng.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*), MAX(round) FROM multipliers"))
            total, max_round = result.fetchone()
        return jsonify({"ok": True, "total": total, "max_round": max_round})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.get("/db/reset")
@login_required
def db_reset():
    try:
        eng = db_engine()
        with eng.begin() as conn:
            result = conn.execute(text("DELETE FROM multipliers"))
            deleted = result.rowcount or 0
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# Rotas básicas
@app.get("/health")
def health():
    return "OK"

@app.get("/debug/trace")
def debug_trace():
    trace = _last_error.get("trace")
    return f"<pre>{trace}</pre>" if trace else "Sem erros"

# =========================
# Templates Inline
# =========================
LOGIN_TEMPLATE = """
<!doctype html>
<html><head><title>Login</title>
<style>body{font-family:system-ui;background:#0b1220;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.card{background:#111d38;padding:24px;border-radius:10px;width:300px}
input{width:100%;padding:10px;margin:8px 0;border-radius:6px;border:1px solid #333;background:#222;color:#fff}
button{width:100%;padding:10px;border:0;border-radius:6px;background:#0066cc;color:#fff;cursor:pointer}
.err{color:#ff4444;margin-top:10px}</style></head>
<body><div class="card"><h2>Aviator Monitor</h2>
<form method="post">
<input name="username" placeholder="Usuário" required>
<input name="password" type="password" placeholder="Senha" required>
<button>Entrar</button>
{% if error %}<div class="err">{{error}}</div>{% endif %}
</form></div></body></html>
"""

DASHBOARD_TEMPLATE = """
<!doctype html>
<html><head><title>Aviator Monitor</title>
<style>body{font-family:system-ui;background:#0b1220;color:#fff;margin:0;padding:20px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}
.card{background:#111d38;padding:14px;border-radius:8px}
.big{font-size:24px;font-weight:bold}
.muted{color:#999;font-size:12px}
table{width:100%;border-collapse:collapse;margin-top:20px}
th,td{padding:8px;text-align:left;border-bottom:1px solid #333}
.ok{color:#22c55e} .warn{color:#f59e0b} .bad{color:#ef4444}
.btn{background:#0066cc;border:0;color:#fff;padding:8px 12px;border-radius:4px;text-decoration:none;display:inline-block}
</style></head>
<body>
<div style="display:flex;justify-content:space-between;align-items:center">
<h1>Aviator Monitor</h1>
<div><a class="btn" href="/logout">Sair</a></div>
</div>

<div class="grid">
<div class="card"><div class="muted">Total</div><div class="big">{{stats.n}}</div></div>
<div class="card"><div class="muted">Média</div><div class="big">{{stats.mean}}</div></div>
<div class="card"><div class="muted">Desvio</div><div class="big">{{stats.std}}</div></div>
<div class="card"><div class="muted">P90</div><div class="big">{{stats.p90}}</div></div>
</div>

<div class="grid">
<div class="card"><div class="muted">≥ 2x</div><div class="big ok">{{stats.cuts["2x"]}}%</div></div>
<div class="card"><div class="muted">≥ 5x</div><div class="big warn">{{stats.cuts["5x"]}}%</div></div>
<div class="card"><div class="muted">≥ 10x</div><div class="big warn">{{stats.cuts["10x"]}}%</div></div>
<div class="card"><div class="muted">≥ 20x</div><div class="big bad">{{stats.cuts["20x"]}}%</div></div>
</div>

<div class="card">
<div class="muted">Atualizado: {{updated_at}}</div>
<table><tr><th>Multiplicador</th><th>Horário</th><th>Round</th></tr>
{{table_rows|safe}}
</table>
</div>

<script>
setInterval(async () => {
  try {
    const r = await fetch('/api/live');
    const data = await r.json();
    if (data.ok) location.reload();
  } catch(e) {}
}, 5000);
</script>
</body></html>
"""

# =========================
# Inicialização
# =========================
if __name__ == "__main__":
    start_collector()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
else:
    # Produção - inicia coletor após delay
    def delayed_start():
        time.sleep(10)
        start_collector()
    threading.Thread(target=delayed_start, daemon=True).start()