import os
import math
import pandas as pd
from datetime import datetime
from flask import Flask, request, redirect, url_for, session, render_template_string, jsonify, abort

# ======== Config por variáveis de ambiente ========
CSV_PATH   = os.getenv("CSV_PATH", "audit_out/live_rollup.csv")
APP_USER   = os.getenv("APP_USER", "admin")
APP_PASS   = os.getenv("APP_PASS", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")  # troque em produção!
WINDOW     = int(os.getenv("FREQ_WINDOW", "500"))  # tamanho da janela p/ cálculo das freq

# ======== Flask ========
app = Flask(__name__)
app.secret_key = SECRET_KEY

# ======== Helpers ========
def login_required(view):
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    wrapper.__name__ = view.__name__
    return wrapper

def load_df(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        # tenta com separador ; se vier assim
        df = pd.read_csv(csv_path, sep=";")
    return df

def get_multiplier_series(df: pd.DataFrame):
    if df.empty:
        return pd.Series(dtype=float)
    # tenta detectar o nome da coluna do multiplicador
    for col in ["multiplicador", "valor", "value", "mult"]:
        if col in df.columns:
            s = df[col]
            break
    else:
        # nenhuma coluna conhecida encontrada
        return pd.Series(dtype=float)

    # Normaliza para float (ex.: "1.43x" -> 1.43)
    def to_float(x):
        if isinstance(x, str):
            x = x.lower().replace("x", "").replace(",", ".").strip()
        try:
            return float(x)
        except Exception:
            return math.nan

    s = s.map(to_float)
    s = s.dropna()
    return s.astype(float)

def compute_freqs(s: pd.Series, cuts=(2,5,10,20), window=500):
    if s.empty:
        return {c: None for c in cuts}, 0
    s_window = s.tail(window)
    total = len(s_window)
    res = {}
    for c in cuts:
        res[c] = round(float((s_window >= c).mean())*100, 2) if total else None
    return res, total

# ======== Rotas ========
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if u == APP_USER and p == APP_PASS:
            session["logged_in"] = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        return render_template_string(LOGIN_HTML, error="Credenciais inválidas.")
    return render_template_string(LOGIN_HTML, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_required
def dashboard():
    df = load_df(CSV_PATH)
    s = get_multiplier_series(df)
    freqs, total = compute_freqs(s, window=WINDOW)
    last_val = s.iloc[-1] if len(s) else None
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # tabela com últimos 50 registros pra inspecionar
    last_rows = None
    if not df.empty:
        show_cols = [c for c in df.columns if c.lower() in ("multiplicador","valor","value","mult","data","date","hora","time","rodada","id","round")]
        last_rows = df[show_cols].tail(50) if show_cols else df.tail(50)

    return render_template_string(DASH_HTML,
        freqs=freqs, total=total, last_val=last_val,
        csv_path=CSV_PATH, updated_at=updated_at,
        window=WINDOW, table_html=(last_rows.to_html(index=False) if isinstance(last_rows, pd.DataFrame) else "<em>Sem dados</em>")
    )

@app.route("/api/live")
@login_required
def api_live():
    df = load_df(CSV_PATH)
    s = get_multiplier_series(df)
    freqs, total = compute_freqs(s, window=WINDOW)
    last_val = s.iloc[-1] if len(s) else None
    return jsonify({
        "csv_path": CSV_PATH,
        "window": WINDOW,
        "total_in_window": total,
        "last_multiplier": last_val,
        "freq_percent_ge": {f"{k}x": v for k, v in freqs.items()}
    })

# ======== Templates (inline) ========
LOGIN_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>Login • Aviator Monitor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial,sans-serif;background:#0b1220;color:#eaeef5;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
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
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial,sans-serif;background:var(--bg);color:#eaeef5;margin:0;padding:24px}
    .wrap{max-width:1100px;margin:0 auto}
    header{display:flex;align-items:center;gap:12px;justify-content:space-between}
    .pill{background:var(--panel);padding:8px 12px;border-radius:999px;color:var(--muted);font-size:12px}
    .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-top:16px}
    .kpi{background:var(--card);border-radius:12px;padding:16px}
    .kpi h3{margin:0;font-size:14px;color:var(--muted)}
    .kpi .v{font-size:28px;font-weight:800;margin-top:8px}
    .kpi .v.ok{color:var(--hi)} .kpi .v.mid{color:var(--warn)} .kpi .v.bad{color:var(--danger)}
    .panel{background:var(--panel);padding:16px;border-radius:12px;margin-top:18px}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{padding:8px;border-bottom:1px solid rgba(255,255,255,.06)}
    th{color:#b6c2e2;text-align:left}
    a, a:visited{color:#93c5fd}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1 style="margin:0;font-size:22px">Aviator Monitor</h1>
        <div class="pill">CSV: {{csv_path}} • janela={{window}} • atual: {{updated_at}}</div>
      </div>
      <div><a href="{{ url_for('logout') }}">Sair</a></div>
    </header>

    <div class="grid">
      <div class="kpi">
        <h3>Último multiplicador</h3>
        <div class="v">{{ '—' if last_val is none else ('%.2fx' % last_val) }}</div>
      </div>
      {% for c,label in [(2,'≥ 2x'),(5,'≥ 5x'),(10,'≥ 10x'),(20,'≥ 20x')] %}
      <div class="kpi">
        <h3>{{label}} (janela {{total}})</h3>
        {% set p = freqs.get(c) %}
        {% set cls = 'ok' if p is not none and p>=20 else ('mid' if p is not none and p>=10 else 'bad') %}
        <div class="v {{cls}}">{{ '—' if p is none else ('%.2f%%' % p) }}</div>
      </div>
      {% endfor %}
    </div>

    <div class="panel">
      <h3 style="margin-top:0;color:#b6c2e2">Últimos registros</h3>
      {{ table_html | safe }}
    </div>
  </div>
</body>
</html>
"""

if __name__ == "__main__":
    # Para rodar local: python app.py
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

@app.route("/")
def home():
    return "Aviator Monitor está rodando 🚀"