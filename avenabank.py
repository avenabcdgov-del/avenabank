"""AvenaBank - fictional national bank of Avena. Currency: Aurum (₳). Not real money.
SINGLE FILE. Setup:  pip install flask "psycopg[binary]"
  1) python avenabank.py create-president      (prompts for the President's password)
  2) python avenabank.py                       (opens on http://127.0.0.1:5000)
Hosting with a free Postgres (e.g. Neon): set DATABASE_URL and SECRET_KEY (any long random text). Without DATABASE_URL it uses a local SQLite file.
Other settings: HOST=0.0.0.0, PORT, AVENABANK_HTTPS=1 and AVENABANK_DB=/persistent/path/avenabank.db
"""
import os, re, sys, sqlite3, secrets, json, getpass, html
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from functools import wraps
from flask import Flask, g, request, session, jsonify
from werkzeug.security import generate_password_hash, check_password_hash

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PG = bool(DATABASE_URL)
if PG:
    import psycopg
    INTEGRITY = (psycopg.errors.UniqueViolation,)
else:
    INTEGRITY = (sqlite3.IntegrityError,)

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("AVENABANK_DB", os.path.join(BASE, "instance", "avenabank.db"))
os.makedirs(os.path.dirname(DB), exist_ok=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL, pw_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN('citizen','official','president')), perms TEXT NOT NULL DEFAULT '[]',
  preferred_name TEXT NOT NULL, citizen_id TEXT UNIQUE, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts(id INTEGER PRIMARY KEY, public_id TEXT UNIQUE NOT NULL,
  user_id INTEGER UNIQUE NOT NULL REFERENCES users(id), status TEXT NOT NULL DEFAULT 'Active'
  CHECK(status IN('Active','Suspended','Closed')), note TEXT DEFAULT '', created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY, ref TEXT UNIQUE NOT NULL, description TEXT NOT NULL,
  note TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'draft', created_by INTEGER NOT NULL, created_at TEXT NOT NULL, executed_at TEXT);
CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, ref TEXT UNIQUE NOT NULL, idem_key TEXT UNIQUE,
  type TEXT NOT NULL, amount INTEGER NOT NULL CHECK(amount>0), sender_id INTEGER REFERENCES accounts(id),
  recipient_id INTEGER REFERENCES accounts(id), status TEXT NOT NULL DEFAULT 'Completed', description TEXT NOT NULL,
  initiator_id INTEGER NOT NULL REFERENCES users(id), batch_id INTEGER REFERENCES batches(id),
  reversal_of INTEGER UNIQUE REFERENCES transactions(id), created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY, tx_id INTEGER NOT NULL REFERENCES transactions(id),
  account_id INTEGER NOT NULL REFERENCES accounts(id), delta INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS ix_ledger_acc ON ledger(account_id);
CREATE INDEX IF NOT EXISTS ix_tx_created ON transactions(created_at);
CREATE TRIGGER IF NOT EXISTS ledger_no_update BEFORE UPDATE ON ledger BEGIN SELECT RAISE(ABORT,'ledger is immutable'); END;
CREATE TRIGGER IF NOT EXISTS ledger_no_delete BEFORE DELETE ON ledger BEGIN SELECT RAISE(ABORT,'ledger is immutable'); END;
CREATE TRIGGER IF NOT EXISTS tx_no_delete BEFORE DELETE ON transactions BEGIN SELECT RAISE(ABORT,'transactions are immutable'); END;
CREATE TABLE IF NOT EXISTS batch_items(id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL REFERENCES batches(id),
  account_id INTEGER NOT NULL REFERENCES accounts(id), amount INTEGER NOT NULL CHECK(amount>0),
  status TEXT NOT NULL DEFAULT 'pending', error TEXT, UNIQUE(batch_id, account_id));
CREATE TABLE IF NOT EXISTS cards(id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL REFERENCES accounts(id),
  last4 TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'Active' CHECK(status IN('Active','Frozen','Inactive','Replaced')),
  expiry TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
  title TEXT NOT NULL, body TEXT NOT NULL, link TEXT, read INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disputes(id INTEGER PRIMARY KEY, tx_id INTEGER NOT NULL REFERENCES transactions(id),
  user_id INTEGER NOT NULL REFERENCES users(id), message TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'Pending',
  response TEXT, resolved_at TEXT, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY, actor_id INTEGER, action TEXT NOT NULL, target TEXT,
  reason TEXT, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT OR IGNORE INTO settings VALUES('max_transfer','1000000'),('large_issue','1000000');
"""

PG_SCHEMA = re.sub(r"CREATE TRIGGER[^\n]*\n", "", SCHEMA).replace("INTEGER PRIMARY KEY", "BIGSERIAL PRIMARY KEY").replace("INTEGER", "BIGINT")
PG_SCHEMA = PG_SCHEMA.replace("INSERT OR IGNORE INTO settings VALUES('max_transfer','1000000'),('large_issue','1000000');", "INSERT INTO settings VALUES('max_transfer','1000000'),('large_issue','1000000') ON CONFLICT DO NOTHING;")
PG_TRIG = """
CREATE OR REPLACE FUNCTION avb_immutable() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'records are immutable'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ledger_imm ON ledger;
CREATE TRIGGER ledger_imm BEFORE UPDATE OR DELETE ON ledger FOR EACH ROW EXECUTE FUNCTION avb_immutable();
DROP TRIGGER IF EXISTS tx_imm ON transactions;
CREATE TRIGGER tx_imm BEFORE DELETE ON transactions FOR EACH ROW EXECUTE FUNCTION avb_immutable();
"""

class Row(dict):
    def __getitem__(self, k): return list(self.values())[k] if isinstance(k, int) else dict.__getitem__(self, k)
class PGCur:
    def __init__(self, cur):
        self.rowcount, self.rows = cur.rowcount, []
        if cur.description:
            cols = [c.name for c in cur.description]
            self.rows = [Row((k, int(v) if isinstance(v, Decimal) else v) for k, v in zip(cols, r)) for r in cur.fetchall()]
        self.lastrowid = self.rows[0]["id"] if self.rows and "id" in self.rows[0] else None
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows
    def __iter__(self): return iter(self.rows)
class PGConn:
    def __init__(self): self.c = psycopg.connect(DATABASE_URL, autocommit=True)
    def execute(self, sql, p=()):
        sql = sql.replace("?", "%s").replace(" LIKE ", " ILIKE ").replace("BEGIN IMMEDIATE", "BEGIN")
        if sql.lstrip().upper().startswith("INSERT INTO") and "INTO settings" not in sql and "RETURNING" not in sql: sql += " RETURNING id"
        return PGCur(self.c.execute(sql, list(p) if p else None))
    def close(self): self.c.close()

app = Flask(__name__, static_folder=None)
_kf = os.path.join(os.path.dirname(DB), "secret_key")
if not os.environ.get("SECRET_KEY") and not os.path.exists(_kf):
    open(_kf, "w").write(secrets.token_hex(32)); os.chmod(_kf, 0o600)
app.secret_key = os.environ.get("SECRET_KEY") or open(_kf).read().strip()
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Strict",
                  SESSION_COOKIE_SECURE=os.environ.get("AVENABANK_HTTPS") == "1")

class ApiError(Exception):
    def __init__(s, msg, code=400): s.msg, s.code = msg, code

@app.errorhandler(ApiError)
def _api_err(e): return jsonify(error=e.msg), e.code
@app.errorhandler(Exception)
def _any_err(e):
    if hasattr(e, "code") and isinstance(e.code, int): return jsonify(error=e.name), e.code
    app.logger.exception(e); return jsonify(error="Something went wrong. Nothing was changed."), 500

def db():
    if "db" not in g and PG: g.db = PGConn()
    if "db" not in g:
        g.db = sqlite3.connect(DB, isolation_level=None, timeout=15)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys=ON"); g.db.execute("PRAGMA journal_mode=WAL")
    return g.db
@app.teardown_appcontext
def _close(_):
    d = g.pop("db", None)
    if d: d.close()

def init_db():
    if PG:
        c = psycopg.connect(DATABASE_URL, autocommit=True); c.execute(PG_SCHEMA); c.execute(PG_TRIG); c.close()
    else:
        c = sqlite3.connect(DB); c.executescript(SCHEMA); c.close()

@contextmanager
def txn():
    d = db(); d.execute("BEGIN IMMEDIATE")
    if PG: d.execute("SELECT pg_advisory_xact_lock(7001)")  # one money-moving transaction at a time
    try: yield d; d.execute("COMMIT")
    except BaseException: d.execute("ROLLBACK"); raise

now = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def money(v):
    try: d = Decimal(str(v).strip())
    except InvalidOperation: raise ApiError("Enter a valid amount.")
    if d <= 0 or d.as_tuple().exponent < -2 or d > Decimal("1000000000"): raise ApiError("Amount must be positive with at most 2 decimals.")
    return int(d * 100)
def setting(k): return int(db().execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()[0])
def balance(aid): return db().execute("SELECT COALESCE(SUM(delta),0) FROM ledger WHERE account_id=?", (aid,)).fetchone()[0]
def audit(action, target="", reason=""):
    db().execute("INSERT INTO audit(actor_id,action,target,reason,created_at) VALUES(?,?,?,?,?)", (session.get("uid"), action, target, reason, now()))
def notify(user_id, title, body, link=""):
    db().execute("INSERT INTO notifications(user_id,title,body,link,created_at) VALUES(?,?,?,?,?)", (user_id, title, body, link, now()))
def body():
    if not request.is_json: raise ApiError("JSON required.", 415)
    return request.get_json(silent=True) or {}

def me():
    u = db().execute("SELECT * FROM users WHERE id=?", (session.get("uid"),)).fetchone() if session.get("uid") else None
    if not u: raise ApiError("Please sign in.", 401)
    return u
def my_account():
    a = db().execute("SELECT * FROM accounts WHERE user_id=?", (me()["id"],)).fetchone()
    if not a: raise ApiError("No bank account is linked to your profile.", 404)
    return a
def login_required(f):
    @wraps(f)
    def w(*a, **k): me(); return f(*a, **k)
    return w
def need(perm):
    def deco(f):
        @wraps(f)
        def w(*a, **k):
            u = me()  # role and perms always come from the database, never the browser
            if u["role"] != "president" and not (u["role"] == "official" and perm in json.loads(u["perms"])):
                raise ApiError("You do not have permission for this action.", 403)
            return f(*a, **k)
        return w
    return deco

def post_tx(type_, amount, sender, recipient, desc, initiator, idem=None, batch=None, reversal_of=None):
    """Caller must hold txn(). Writes the transaction and its ledger entries atomically."""
    if idem:
        old = db().execute("SELECT * FROM transactions WHERE idem_key=?", (idem,)).fetchone()
        if old: return old, True
    ref = "AVB-" + secrets.token_hex(6).upper()
    cur = db().execute("INSERT INTO transactions(ref,idem_key,type,amount,sender_id,recipient_id,description,initiator_id,batch_id,reversal_of,created_at)"
                       " VALUES(?,?,?,?,?,?,?,?,?,?,?)", (ref, idem, type_, amount, sender, recipient, desc, initiator, batch, reversal_of, now()))
    for acc, delta in ((sender, -amount), (recipient, amount)):
        if acc: db().execute("INSERT INTO ledger(tx_id,account_id,delta) VALUES(?,?,?)", (cur.lastrowid, acc, delta))
    return db().execute("SELECT * FROM transactions WHERE id=?", (cur.lastrowid,)).fetchone(), False

def acct_user(aid): return db().execute("SELECT user_id FROM accounts WHERE id=?", (aid,)).fetchone()[0]
def fmt(c): return f"₳{c/100:,.2f}"

def tx_dict(r, viewer_acc=None):
    d = dict(r); d["amount_fmt"] = fmt(r["amount"])
    for k in ("sender_id", "recipient_id"):
        a = db().execute("SELECT a.public_id,u.preferred_name FROM accounts a JOIN users u ON u.id=a.user_id WHERE a.id=?", (r[k],)).fetchone() if r[k] else None
        d[k[:-3]] = dict(a) if a else None
    if viewer_acc: d["direction"] = "out" if r["sender_id"] == viewer_acc else "in"
    return d

# ---------- auth ----------
@app.post("/api/login")
def login():
    d = body(); u = db().execute("SELECT * FROM users WHERE username=?", (str(d.get("username", "")).strip().lower(),)).fetchone()
    if not u or not check_password_hash(u["pw_hash"], str(d.get("password", ""))): raise ApiError("Incorrect username or password.", 401)
    session.clear(); session["uid"] = u["id"]; return jsonify(ok=True)
@app.post("/api/logout")
def logout(): session.clear(); return jsonify(ok=True)
@app.get("/api/me")
def whoami():
    u = me(); a = db().execute("SELECT * FROM accounts WHERE user_id=?", (u["id"],)).fetchone()
    unread = db().execute("SELECT COUNT(*) FROM notifications WHERE user_id=? AND read=0", (u["id"],)).fetchone()[0]
    return jsonify(name=u["preferred_name"], role=u["role"], perms=json.loads(u["perms"]), unread=unread,
                   account=dict(public_id=a["public_id"], status=a["status"], balance=balance(a["id"]), balance_fmt=fmt(balance(a["id"]))) if a else None)

# ---------- citizen ----------
@app.get("/api/transactions")
@login_required
def history():
    a = my_account(); q = request.args; w, p = ["(t.sender_id=? OR t.recipient_id=?)"], [a["id"], a["id"]]
    return jsonify(list_tx(w, p, q, a["id"]))

def list_tx(w, p, q, viewer=None):
    if q.get("q"): w.append("(t.ref LIKE ? OR t.description LIKE ?)"); p += [f"%{q['q']}%"] * 2
    for k, col in (("type", "t.type"), ("status", "t.status")):
        if q.get(k): w.append(f"{col}=?"); p.append(q[k])
    if q.get("from"): w.append("t.created_at>=?"); p.append(q["from"])
    if q.get("to"): w.append("t.created_at<=?"); p.append(q["to"] + "T23:59:59Z")
    if q.get("min"): w.append("t.amount>=?"); p.append(money(q["min"]))
    if q.get("max"): w.append("t.amount<=?"); p.append(money(q["max"]))
    order = {"date_asc": "t.id ASC", "amount_desc": "t.amount DESC", "amount_asc": "t.amount ASC"}.get(q.get("sort"), "t.id DESC")
    page = max(int(q.get("page", 1) or 1), 1); sql = " AND ".join(w)
    total = db().execute(f"SELECT COUNT(*) FROM transactions t WHERE {sql}", p).fetchone()[0]
    rows = db().execute(f"SELECT t.* FROM transactions t WHERE {sql} ORDER BY {order} LIMIT 20 OFFSET ?", p + [(page - 1) * 20]).fetchall()
    return dict(total=total, page=page, items=[tx_dict(r, viewer) for r in rows])

@app.get("/api/transactions/<ref>")
@login_required
def tx_detail(ref):
    a = my_account(); r = db().execute("SELECT * FROM transactions WHERE ref=? AND (sender_id=? OR recipient_id=?)", (ref, a["id"], a["id"])).fetchone()
    if not r: raise ApiError("Transaction not found.", 404)  # same answer whether it exists or not
    return jsonify(tx_dict(r, a["id"]))

@app.post("/api/transfer")
@login_required
def transfer():
    d = body(); amt = money(d.get("amount")); key = str(d.get("idem_key", ""))[:80]
    if not key: raise ApiError("Missing request key.")
    desc = str(d.get("description", "")).strip()[:140] or "Transfer"
    with txn():
        u = me(); s = my_account()
        old = db().execute("SELECT * FROM transactions WHERE idem_key=?", (f"t:{u['id']}:{key}",)).fetchone()
        if old: return jsonify(tx=tx_dict(old, s["id"]), duplicate=True)
        r = db().execute("SELECT * FROM accounts WHERE public_id=?", (str(d.get("to", "")).strip().upper(),)).fetchone()
        if not r or r["status"] != "Active": raise ApiError("Recipient account is not available.")
        if s["status"] != "Active": raise ApiError("Your account is restricted and cannot send Aurum.", 403)
        if r["id"] == s["id"]: raise ApiError("You cannot send Aurum to yourself.")
        if amt > setting("max_transfer"): raise ApiError("Amount exceeds the transfer limit.")
        if amt > balance(s["id"]): raise ApiError("Insufficient balance.")
        t, _ = post_tx("transfer", amt, s["id"], r["id"], desc, u["id"], f"t:{u['id']}:{key}")
        notify(u["id"], "Aurum sent", f"{fmt(amt)} sent to {r['public_id']}.", t["ref"])
        notify(r["user_id"], "Aurum received", f"{fmt(amt)} received.", t["ref"])
    return jsonify(tx=tx_dict(t, s["id"]))

@app.get("/api/notifications")
@login_required
def notifs():
    u = me(); rows = [dict(r) for r in db().execute("SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 50", (u["id"],))]
    db().execute("UPDATE notifications SET read=1 WHERE user_id=?", (u["id"],)); return jsonify(rows)

@app.get("/api/cards")
@login_required
def cards():
    a = my_account(); u = me()
    return jsonify([dict(c, holder=u["preferred_name"]) for c in db().execute("SELECT * FROM cards WHERE account_id=? ORDER BY id DESC", (a["id"],))])
@app.post("/api/cards")
@login_required
def card_request():
    a = my_account()
    if a["status"] != "Active": raise ApiError("Account is restricted.", 403)
    with txn():
        if db().execute("SELECT 1 FROM cards WHERE account_id=? AND status IN('Active','Frozen')", (a["id"],)).fetchone(): raise ApiError("You already have a live card.")
        yr = int(now()[:4]) + 4
        db().execute("INSERT INTO cards(account_id,last4,expiry,created_at) VALUES(?,?,?,?)", (a["id"], f"{secrets.randbelow(10000):04d}", f"{now()[5:7]}/{yr}", now()))
    return jsonify(ok=True)
@app.post("/api/cards/<int:cid>/<act>")
@login_required
def card_act(cid, act):
    a = my_account(); c = db().execute("SELECT * FROM cards WHERE id=? AND account_id=?", (cid, a["id"])).fetchone()
    if not c: raise ApiError("Card not found.", 404)
    new = {"freeze": ("Active", "Frozen"), "unfreeze": ("Frozen", "Active")}.get(act)
    if not new or c["status"] != new[0]: raise ApiError("That card action is not available.")
    with txn():
        db().execute("UPDATE cards SET status=? WHERE id=?", (new[1], cid)); notify(me()["id"], "Card status changed", f"Card ••{c['last4']} is now {new[1]}.")
    return jsonify(ok=True)

@app.post("/api/disputes")
@login_required
def dispute():
    d = body(); a = my_account(); t = db().execute("SELECT id FROM transactions WHERE ref=? AND (sender_id=? OR recipient_id=?)", (d.get("ref"), a["id"], a["id"])).fetchone()
    if not t or not str(d.get("message", "")).strip(): raise ApiError("Choose one of your transactions and describe the problem.")
    db().execute("INSERT INTO disputes(tx_id,user_id,message,created_at) VALUES(?,?,?,?)", (t[0], me()["id"], str(d["message"])[:1000], now())); return jsonify(ok=True)

@app.get("/statement")
@login_required
def statement():
    a = my_account(); q = request.args; data = list_tx(["(t.sender_id=? OR t.recipient_id=?)"], [a["id"], a["id"]], dict(q, sort="date_asc"), a["id"])["items"]
    e = html.escape
    rows = "".join(f"<tr><td>{e(t['created_at'])}</td><td>{e(t['ref'])}</td><td>{e(t['description'])}</td><td style='text-align:right'>{'−' if t['direction']=='out' else '+'}{e(t['amount_fmt'])}</td></tr>" for t in data)
    return (f"<!doctype html><title>AvenaBank statement</title><body style='font-family:Georgia;max-width:760px;margin:2em auto'><h1>AvenaBank ₳ — Statement</h1>"
            f"<p><b>AvenaBank internal record. Fictional Aurum; not real money.</b><br>Account {e(a['public_id'])} · Currency Aurum (₳) · Range {e(q.get('from','start'))} to {e(q.get('to','today'))} · Generated {now()} · Balance {fmt(balance(a['id']))}</p>"
            f"<table width=100% border=1 cellspacing=0 cellpadding=6>{rows}</table><p><button onclick=print()>Print</button></p>")

# ---------- admin ----------
def eligible(aid):
    a = db().execute("SELECT status FROM accounts WHERE id=?", (aid,)).fetchone(); return bool(a and a["status"] == "Active")

@app.get("/api/admin/stats")
@need("view_stats")
def stats():
    c = lambda s, p=(): db().execute(s, p).fetchone()[0]; since = request.args.get("since", "1970-01-01")
    return jsonify(circulation=fmt(c("SELECT COALESCE(SUM(delta),0) FROM ledger")),
        active=c("SELECT COUNT(*) FROM accounts WHERE status='Active'"), suspended=c("SELECT COUNT(*) FROM accounts WHERE status='Suspended'"),
        cards=c("SELECT COUNT(*) FROM cards"), pending_disputes=c("SELECT COUNT(*) FROM disputes WHERE status='Pending'"),
        issued=fmt(c("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type IN('issue','income') AND created_at>=?", (since,))),
        transfer_volume=fmt(c("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='transfer' AND created_at>=?", (since,))),
        recent=[tx_dict(r) for r in db().execute("SELECT * FROM transactions ORDER BY id DESC LIMIT 8")],
        batches=[dict(r) for r in db().execute("SELECT * FROM batches ORDER BY id DESC LIMIT 5")],
        audit=[dict(r) for r in db().execute("SELECT * FROM audit ORDER BY id DESC LIMIT 8")])

@app.get("/api/admin/accounts")
@need("manage_accounts")
def adm_accounts():
    q = f"%{request.args.get('q','')}%"
    return jsonify([dict(r, balance_fmt=fmt(balance(r["id"]))) for r in db().execute(
        "SELECT a.id,a.public_id,a.status,u.username,u.preferred_name,u.role FROM accounts a JOIN users u ON u.id=a.user_id WHERE u.username LIKE ? OR u.preferred_name LIKE ? OR a.public_id LIKE ? ORDER BY a.id DESC LIMIT 200", (q, q, q))])
@app.post("/api/admin/accounts")
@need("manage_accounts")
def adm_create_account():
    d = body(); un = str(d.get("username", "")).strip().lower(); pw = str(d.get("password", ""))
    if not un.isalnum() or len(pw) < 10 or not str(d.get("name", "")).strip(): raise ApiError("Username (letters/numbers), preferred name and a 10+ character temporary password are required.")
    with txn():
        try: uid = db().execute("INSERT INTO users(username,pw_hash,role,preferred_name,citizen_id,created_at) VALUES(?,?,'citizen',?,?,?)",
                (un, generate_password_hash(pw), d["name"].strip()[:60], str(d.get("citizen_id") or "").strip() or None, now())).lastrowid
        except INTEGRITY: raise ApiError("That username or citizen ID is already in use.")
        pub = "AVN-" + secrets.token_hex(5).upper()
        db().execute("INSERT INTO accounts(public_id,user_id,created_at) VALUES(?,?,?)", (pub, uid, now())); audit("account.create", pub, "new citizen account")
    return jsonify(public_id=pub)
@app.post("/api/admin/accounts/<pub>/status")
@need("manage_accounts")
def adm_status(pub):
    d = body(); st = d.get("status"); reason = str(d.get("reason", "")).strip()
    if st not in ("Active", "Suspended", "Closed") or not reason: raise ApiError("A valid status and a reason are required.")
    if st in ("Suspended", "Closed") and d.get("confirm") is not True: raise ApiError("Please confirm this sensitive action.")
    with txn():
        a = db().execute("SELECT * FROM accounts WHERE public_id=?", (pub,)).fetchone()
        if not a or a["status"] == "Closed": raise ApiError("Account not found or already closed.", 404)
        db().execute("UPDATE accounts SET status=?, note=? WHERE id=?", (st, reason, a["id"]))
        audit(f"account.{st.lower()}", pub, reason); notify(a["user_id"], "Account status changed", f"Your account is now {st}.")
    return jsonify(ok=True)

def run_items(items, type_, desc, batch=None, key=""):
    """Each recipient is its own atomic unit; a failure never affects others and a retry never double-pays."""
    out = []
    for aid, amt, item_id in items:
        try:
            with txn():
                if not eligible(aid): raise ApiError("Account is not eligible.")
                t, dup = post_tx(type_, amt, None, aid, desc, session["uid"], f"{key}:{aid}", batch)
                if not dup: notify(acct_user(aid), "Aurum received", f"{fmt(amt)} — {desc}", t["ref"])
                if item_id: db().execute("UPDATE batch_items SET status='completed',error=NULL WHERE id=?", (item_id,))
            out.append(dict(account=aid, ok=True, ref=t["ref"]))
        except ApiError as e:
            if item_id: db().execute("UPDATE batch_items SET status='failed',error=? WHERE id=?", (e.msg, item_id))
            out.append(dict(account=aid, ok=False, error=e.msg))
    return out

def resolve(pub):
    a = db().execute("SELECT id FROM accounts WHERE public_id=?", (str(pub).upper(),)).fetchone()
    if not a: raise ApiError(f"Unknown account {pub}.")
    return a[0]

@app.post("/api/admin/issue")
@need("issue")
def issue():
    d = body(); reason = str(d.get("reason", "")).strip(); key = str(d.get("idem_key", ""))[:80]
    rec = [(resolve(r.get("account")), money(r.get("amount"))) for r in d.get("recipients", [])]
    if not reason or not rec or not key: raise ApiError("Recipients, a reason and a request key are required.")
    if sum(a for _, a in rec) >= setting("large_issue") and d.get("confirm_large") is not True: raise ApiError("Large issuance: explicit confirmation required.", 409)
    res = run_items([(a, m, None) for a, m in rec], "issue", reason, key=f"issue:{key}")
    audit("issue", f"{len(rec)} account(s), {fmt(sum(m for _, m in rec))}", reason); return jsonify(results=res)

@app.post("/api/admin/batches")
@need("income")
def batch_create():
    d = body(); desc = str(d.get("description", "")).strip()
    if not desc: raise ApiError("A description is required.")
    if d.get("all"): amt = money(d.get("amount")); rec = [(r[0], amt) for r in db().execute("SELECT id FROM accounts WHERE status='Active'")]
    else: rec = [(resolve(r.get("account")), money(r.get("amount"))) for r in d.get("recipients", [])]
    if not rec: raise ApiError("Choose at least one recipient.")
    with txn():
        bid = db().execute("INSERT INTO batches(ref,description,note,created_by,created_at) VALUES(?,?,?,?,?)", ("INC-" + secrets.token_hex(5).upper(), desc[:100], str(d.get("note", ""))[:500], session["uid"], now())).lastrowid
        for aid, amt in dict(rec).items():
            db().execute("INSERT INTO batch_items(batch_id,account_id,amount,status,error) VALUES(?,?,?,?,?)", (bid, aid, amt, *(("pending", None) if eligible(aid) else ("ineligible", "Account is not active"))))
        audit("batch.create", str(bid), desc)
    return jsonify(batch_view(bid))
def batch_view(bid):
    b = db().execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
    if not b: raise ApiError("Batch not found.", 404)
    items = [dict(r, amount_fmt=fmt(r["amount"])) for r in db().execute("SELECT i.*,a.public_id FROM batch_items i JOIN accounts a ON a.id=i.account_id WHERE batch_id=?", (bid,))]
    ok = [i for i in items if i["status"] in ("pending", "completed")]
    return dict(batch=dict(b), items=items, recipients=len(ok), total_fmt=fmt(sum(i["amount"] for i in ok)), ineligible=len(items) - len(ok))
@app.get("/api/admin/batches")
@need("income")
def batch_list(): return jsonify([dict(r) for r in db().execute("SELECT * FROM batches ORDER BY id DESC LIMIT 50")])
@app.get("/api/admin/batches/<int:bid>")
@need("income")
def batch_get(bid): return jsonify(batch_view(bid))
@app.post("/api/admin/batches/<int:bid>/execute")
@need("income")
def batch_exec(bid):
    d = body(); v = batch_view(bid)
    if v["batch"]["status"] in ("completed", "partial"): return jsonify(dict(v, duplicate=True))
    if d.get("confirm") is not True: raise ApiError("Final confirmation required.")
    with txn():
        db().execute("UPDATE batches SET status='executing' WHERE id=? AND status IN('draft','executing')", (bid,))
    b = v["batch"]; todo = [(i["account_id"], i["amount"], i["id"]) for i in v["items"] if i["status"] in ("pending", "failed")]
    run_items(todo, "income", b["description"], bid, key=f"batch:{bid}")  # idempotency keys make retries safe
    v = batch_view(bid); failed = any(i["status"] == "failed" for i in v["items"])
    db().execute("UPDATE batches SET status=?, executed_at=? WHERE id=?", ("partial" if failed else "completed", now(), bid))
    audit("batch.execute", b["ref"], b["description"]); return jsonify(batch_view(bid))

@app.post("/api/admin/adjust")
@need("adjust")
def adjust():
    d = body(); reason = str(d.get("reason", "")).strip(); amt = money(d.get("amount")); aid = resolve(d.get("account"))
    if not reason: raise ApiError("A reason is required.")
    with txn():
        if amt > balance(aid): raise ApiError("Deduction exceeds the account balance.")
        t, _ = post_tx("deduction", amt, aid, None, reason, session["uid"], f"adj:{d.get('idem_key')}" if d.get("idem_key") else None)
        audit("deduction", t["ref"], reason); notify(acct_user(aid), "Account adjusted", f"{fmt(amt)} deducted: {reason}", t["ref"])
    return jsonify(ref=t["ref"])
@app.post("/api/admin/transactions/<ref>/reverse")
@need("adjust")
def reverse(ref):
    reason = str(body().get("reason", "")).strip()
    if not reason: raise ApiError("A reason is required.")
    with txn():
        o = db().execute("SELECT * FROM transactions WHERE ref=?", (ref,)).fetchone()
        if not o or o["type"] == "reversal" or db().execute("SELECT 1 FROM transactions WHERE reversal_of=?", (o["id"],)).fetchone(): raise ApiError("This transaction cannot be reversed.")
        if o["recipient_id"] and o["amount"] > balance(o["recipient_id"]): raise ApiError("Recipient balance is too low to reverse.")
        t, _ = post_tx("reversal", o["amount"], o["recipient_id"], o["sender_id"], f"Reversal of {ref}: {reason}", session["uid"], reversal_of=o["id"])
        audit("reversal", ref, reason)
    return jsonify(ref=t["ref"])

@app.get("/api/admin/transactions")
@need("view_transactions")
def adm_tx(): return jsonify(list_tx(["1=1"], [], request.args))
@app.get("/api/admin/audit")
@need("view_audit")
def adm_audit(): return jsonify([dict(r) for r in db().execute("SELECT a.*,u.username actor FROM audit a LEFT JOIN users u ON u.id=a.actor_id ORDER BY a.id DESC LIMIT 200")])
@app.get("/api/admin/disputes")
@need("disputes")
def adm_disputes(): return jsonify([dict(r) for r in db().execute("SELECT d.*,t.ref FROM disputes d JOIN transactions t ON t.id=d.tx_id ORDER BY d.id DESC")])
@app.post("/api/admin/disputes/<int:did>")
@need("disputes")
def adm_resolve(did):
    d = body(); st = d.get("status"); resp = str(d.get("response", "")).strip()
    if st not in ("Upheld", "Rejected") or not resp: raise ApiError("Decision and response are required.")
    with txn():
        x = db().execute("SELECT * FROM disputes WHERE id=? AND status='Pending'", (did,)).fetchone()
        if not x: raise ApiError("Dispute not found or already resolved.", 404)
        db().execute("UPDATE disputes SET status=?,response=?,resolved_at=? WHERE id=?", (st, resp, now(), did)); audit("dispute.resolve", str(did), f"{st}: {resp}"); notify(x["user_id"], "Dispute update", f"Your dispute was {st}.")
    return jsonify(ok=True)
@app.get("/api/admin/cards")
@need("manage_cards")
def adm_cards(): return jsonify([dict(r) for r in db().execute("SELECT c.*,a.public_id FROM cards c JOIN accounts a ON a.id=c.account_id ORDER BY c.id DESC LIMIT 200")])
@app.post("/api/admin/cards/<int:cid>")
@need("manage_cards")
def adm_card(cid):
    st = body().get("status")
    if st not in ("Active", "Frozen", "Inactive", "Replaced"): raise ApiError("Invalid status.")
    with txn():
        c = db().execute("SELECT * FROM cards WHERE id=?", (cid,)).fetchone()
        if not c: raise ApiError("Card not found.", 404)
        db().execute("UPDATE cards SET status=? WHERE id=?", (st, cid)); audit("card.status", str(cid), st); notify(acct_user(c["account_id"]), "Card status changed", f"Card ••{c['last4']} is now {st}.")
    return jsonify(ok=True)

@app.get("/")
def index(): return PAGE

def cli():
    init_db()
    if sys.argv[1:2] == ["create-president"]:
        un = input("President username: ").strip().lower(); pw = os.environ.get("AVENABANK_ADMIN_PASSWORD") or getpass.getpass("Password (10+ chars): ")
        if len(pw) < 10: sys.exit("Password too short.")
        with app.app_context():
            c = db(); uid = c.execute("INSERT INTO users(username,pw_hash,role,preferred_name,created_at) VALUES(?,?,'president','President',?)", (un, generate_password_hash(pw), now())).lastrowid
            c.execute("INSERT INTO accounts(public_id,user_id,created_at) VALUES(?,?,?)", ("AVN-" + secrets.token_hex(5).upper(), uid, now())); print("President created.")
    else:
        app.run(host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", 5000)))



PAGE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AvenaBank — National Bank of Avena</title>
<style>
:root{--bg:#0d1a14;--panel:#14261d;--line:#2a4034;--ink:#e9efe9;--mute:#9db1a5;--gold:#c9a24b;--green:#1f5a3f;--bad:#e08a7a}
@media(prefers-color-scheme:light){:root{--bg:#f3f6f3}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 system-ui,sans-serif}
a{color:var(--gold)}:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.wm{font:700 1.35rem Georgia,serif;letter-spacing:.04em}.wm b{color:var(--gold)}
#app{display:flex;min-height:100vh}nav{width:230px;background:#0a130e;border-right:1px solid var(--line);padding:1rem;display:flex;flex-direction:column;gap:.25rem}
nav a{padding:.55rem .7rem;border-radius:8px;color:var(--ink);text-decoration:none}nav a.on,nav a:hover{background:var(--green)}
nav h4{margin:1rem 0 .2rem;color:var(--gold);font-size:.75rem;letter-spacing:.12em;text-transform:uppercase}
main{flex:1;padding:1.5rem;max-width:1000px}.top{display:none}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:1rem;margin-bottom:1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:1rem}.big{font:600 2rem Georgia,serif;color:var(--gold)}
.mute{color:var(--mute);font-size:.9rem}h2{font-family:Georgia,serif;margin-top:0}
input,select,textarea,button{font:inherit;color:var(--ink);background:#0b1812;border:1px solid var(--line);border-radius:8px;padding:.55rem .7rem}
label{display:block;margin:.6rem 0 .2rem;color:var(--mute);font-size:.9rem}input,select,textarea{width:100%}
button{cursor:pointer;background:var(--green);border-color:var(--green)}button.alt{background:none;border-color:var(--gold);color:var(--gold)}button[disabled]{opacity:.5}
table{width:100%;border-collapse:collapse}td,th{padding:.5rem;border-bottom:1px solid var(--line);text-align:left;font-size:.92rem}.wrap{overflow-x:auto}
.in{color:#7fd1a0}.out{color:var(--bad)}.tag{border:1px solid var(--line);border-radius:99px;padding:0 .5rem;font-size:.8rem}
.vcard{width:340px;max-width:100%;aspect-ratio:1.6;border-radius:16px;padding:1.1rem;background:linear-gradient(135deg,#1f5a3f,#0f2b1f);border:1px solid var(--gold);display:flex;flex-direction:column;justify-content:space-between}
.vcard.Frozen{filter:grayscale(.8);opacity:.7}.row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}.row>*{flex:1;min-width:120px}
#msg{position:fixed;bottom:1rem;right:1rem;max-width:340px}#msg div{background:var(--panel);border:1px solid var(--gold);border-radius:8px;padding:.6rem .9rem;margin-top:.4rem}#msg .e{border-color:var(--bad)}
#login{max-width:360px;margin:12vh auto;padding:1rem}
@media(max-width:760px){#app{flex-direction:column}nav{width:100%;flex-direction:row;flex-wrap:wrap;position:sticky;top:0;z-index:2}nav h4{display:none}}
@media print{nav,#msg,button{display:none!important}}
</style></head><body>
<div id="root"></div><div id="msg" role="status" aria-live="polite"></div>
<script>
const $=s=>document.querySelector(s), esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let ME=null, KEY=null; const newKey=()=>crypto.randomUUID();
function toast(t,bad){const d=document.createElement('div');d.textContent=t;if(bad)d.className='e';$('#msg').append(d);setTimeout(()=>d.remove(),5000)}
async function api(path,data){const r=await fetch('/api'+path,data===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const j=await r.json().catch(()=>({}));if(!r.ok){if(r.status===401&&ME){ME=null;boot()}throw new Error(j.error||'Request failed')}return j}
const act=async(btn,fn)=>{btn.disabled=true;try{await fn()}catch(e){toast(e.message,1)}finally{btn.disabled=false}};
const qs=o=>new URLSearchParams(Object.fromEntries(Object.entries(o).filter(([,v])=>v))).toString();
const val=id=>$('#'+id).value;
const can=p=>ME.role==='president'||(ME.role==='official'&&ME.perms.includes(p));
const mask=a=>a.slice(0,4)+'••••'+a.slice(-3);

async function boot(){try{ME=await api('/me')}catch{ME=null}
  if(!ME){$('#root').innerHTML=`<form id="login" class="card"><div class="wm">Avena<b>Bank</b> ₳</div><p class="mute">National Bank of Avena · fictional Aurum economy, not real money</p>
   <label for="u">Username</label><input id="u" autocomplete="username" required><label for="p">Password</label><input id="p" type="password" autocomplete="current-password" required>
   <p><button>Sign in</button></p></form>`;
   $('#login').onsubmit=e=>{e.preventDefault();act(e.submitter,async()=>{await api('/login',{username:val('u'),password:val('p')});boot()})};return}
  const L=[['dash','Dashboard'],['send','Send Aurum'],['hist','History'],['cards','Cards'],['notes','Notifications'+(ME.unread?` (${ME.unread})`:'')]];
  const A=[['console','Console','view_stats'],['accts','Accounts','manage_accounts'],['issue','Issue Aurum','issue'],['income','Income Payments','income'],['atx','Transactions','view_transactions'],['disp','Disputes','disputes'],['acards','Cards','manage_cards'],['audit','Audit Log','view_audit']].filter(x=>can(x[2]));
  $('#root').innerHTML=`<div id="app"><nav aria-label="Main"><div class="wm">Avena<b>Bank</b> ₳</div>${ME.account?L.map(l=>`<a href="#${l[0]}" data-r="${l[0]}">${l[1]}</a>`).join(''):''}
   ${A.length?'<h4>Administration</h4>'+A.map(l=>`<a href="#${l[0]}" data-r="${l[0]}">${l[1]}</a>`).join(''):''}<a href="#" id="out">Sign out</a></nav><main id="main" tabindex="-1"></main></div>`;
  $('#out').onclick=async e=>{e.preventDefault();await api('/logout',{});boot()};route()}
addEventListener('hashchange',()=>ME&&route());
async function route(){const r=location.hash.slice(1)||(ME.account?'dash':'console');KEY=newKey();
  document.querySelectorAll('nav a[data-r]').forEach(a=>a.classList.toggle('on',a.dataset.r===r));
  $('#main').innerHTML='<p class="mute">Loading…</p>';
  try{$('#main').innerHTML=await (V[r]||V.dash)()}catch(e){$('#main').innerHTML=`<div class="card">${esc(e.message)}</div>`}
  if(W[r])W[r]();}
const txTable=(items,adm)=>items.length?`<div class="wrap"><table><tr><th>Date</th><th>Ref</th><th>Type</th><th>Description</th><th>Amount</th><th>Status</th></tr>${items.map(t=>`<tr><td>${esc(t.created_at.replace('T',' ').slice(0,16))}</td><td>${esc(t.ref)}</td><td>${esc(t.type)}</td><td>${esc(t.description)}${adm?` <span class="mute">${esc(t.sender?.public_id||'—')}→${esc(t.recipient?.public_id||'—')}</span>`:''}</td>
 <td class="${t.direction}">${t.direction?(t.direction==='in'?'▲ +':'▼ −'):''}${esc(t.amount_fmt)}</td><td><span class="tag">${esc(t.status)}</span></td></tr>`).join('')}</table></div>`:'<p class="mute">No transactions yet.</p>';
const V={},W={};
V.dash=async()=>{const t=await api('/transactions?page=1'),a=ME.account;return`<h2>Welcome, ${esc(ME.name)}</h2><div class="grid"><div class="card"><div class="mute">Available balance</div><div class="big">${esc(a.balance_fmt)}</div><div class="mute">Account ${esc(mask(a.public_id))} · <span class="tag">${esc(a.status)}</span></div></div></div>
 <div class="card"><h3>Recent transactions</h3>${txTable(t.items.slice(0,6))}<p><a href="#send">Send Aurum</a> · <a href="#cards">View cards</a> · <a href="#hist">Full history</a></p></div>`};
V.send=()=>`<h2>Send Aurum</h2><div class="card"><div id="s1"><label for="to">Recipient account ID</label><input id="to" placeholder="AVN-XXXXXXXXXX"><label for="am">Amount (₳)</label><input id="am" inputmode="decimal">
 <label for="ds">Description</label><input id="ds" maxlength="140"><p><button id="rev">Review</button></p></div><div id="s2" hidden></div></div>`;
W.send=()=>{$('#rev').onclick=()=>{if(!val('to')||!(parseFloat(val('am'))>0))return toast('Enter a recipient and a positive amount.',1);
  $('#s1').hidden=1;$('#s2').hidden=0;$('#s2').innerHTML=`<p>Send <b>₳${esc(val('am'))}</b> to <b>${esc(val('to').toUpperCase())}</b>?<br><span class="mute">${esc(val('ds'))}</span></p><button id="go">Confirm and send</button> <button class="alt" id="back">Back</button>`;
  $('#back').onclick=()=>{$('#s1').hidden=0;$('#s2').hidden=1};
  $('#go').onclick=e=>act(e.target,async()=>{const r=await api('/transfer',{to:val('to'),amount:val('am'),description:val('ds'),idem_key:KEY});toast('Sent. Ref '+r.tx.ref);ME=await api('/me');location.hash='#hist';route()})}};
V.hist=()=>`<h2>Transaction history</h2><div class="card"><div class="row"><div><label for="fq">Search</label><input id="fq"></div><div><label for="ft">Type</label><select id="ft"><option value="">Any</option><option>transfer</option><option>issue</option><option>income</option><option>deduction</option><option>reversal</option></select></div>
 <div><label for="ff">From</label><input id="ff" type="date"></div><div><label for="fe">To</label><input id="fe" type="date"></div><div><label for="fmn">Min ₳</label><input id="fmn"></div><div><label for="fmx">Max ₳</label><input id="fmx"></div>
 <div><label for="fs">Sort</label><select id="fs"><option value="">Newest</option><option value="date_asc">Oldest</option><option value="amount_desc">Amount ↓</option><option value="amount_asc">Amount ↑</option></select></div></div>
 <p><button id="fgo">Apply</button> <button class="alt" id="stm">Print statement</button></p><div id="hl"></div><p><button class="alt" id="pv">‹ Prev</button> <span id="pg"></span> <button class="alt" id="nx">Next ›</button></p></div>
 <div class="card"><h3>Dispute a transaction</h3><div class="row"><input id="dr" placeholder="Transaction reference" aria-label="Transaction reference"><input id="dm" placeholder="What went wrong?" aria-label="Dispute message"><button id="dg">Submit</button></div></div>`;
W.hist=()=>{let page=1;const f=()=>({q:val('fq'),type:val('ft'),from:val('ff'),to:val('fe'),min:val('fmn'),max:val('fmx'),sort:val('fs')});
  const load=async()=>{try{const r=await api('/transactions?'+qs({...f(),page}));$('#hl').innerHTML=txTable(r.items);$('#pg').textContent=`Page ${r.page} of ${Math.max(1,Math.ceil(r.total/20))} · ${r.total} found`;$('#nx').disabled=r.page*20>=r.total;$('#pv').disabled=page<=1}catch(e){toast(e.message,1)}};
  $('#fgo').onclick=()=>{page=1;load()};$('#nx').onclick=()=>{page++;load()};$('#pv').onclick=()=>{page--;load()};
  $('#stm').onclick=()=>open('/statement?'+qs({from:val('ff'),to:val('fe')}));
  $('#dg').onclick=e=>act(e.target,async()=>{await api('/disputes',{ref:val('dr'),message:val('dm')});toast('Dispute submitted.')});load()};
V.cards=async()=>{const c=await api('/cards');return`<h2>Virtual cards</h2><p class="mute">Fictional identification cards for Avena internal use only. They are not payment cards.</p>${c.map(x=>`<div class="card"><div class="vcard ${x.status}"><div class="row"><div class="wm">Avena<b>Bank</b></div><div style="text-align:right;font-size:1.5rem;color:var(--gold)">₳</div></div>
 <div style="font-size:1.2rem;letter-spacing:.12em">•••• •••• •••• ${esc(x.last4)}</div><div class="row"><div>${esc(x.holder)}</div><div style="text-align:right">Exp ${esc(x.expiry)}<br><span class="tag">${esc(x.status)}</span></div></div></div>
 ${['Active','Frozen'].includes(x.status)?`<p><button data-c="${x.id}" data-a="${x.status==='Active'?'freeze':'unfreeze'}">${x.status==='Active'?'Freeze':'Unfreeze'} card</button></p>`:''}</div>`).join('')||'<p class="mute">You have no card yet.</p>'}<button id="rq">Request a card</button>`};
W.cards=()=>{$('#rq').onclick=e=>act(e.target,async()=>{await api('/cards',{});route()});document.querySelectorAll('[data-c]').forEach(b=>b.onclick=()=>act(b,async()=>{await api(`/cards/${b.dataset.c}/${b.dataset.a}`,{});route()}))};
V.notes=async()=>{const n=await api('/notifications');ME=await api('/me');return`<h2>Notifications</h2>${n.map(x=>`<div class="card"><b>${esc(x.title)}</b><div>${esc(x.body)}</div><div class="mute">${esc(x.created_at)}</div></div>`).join('')||'<p class="mute">Nothing yet.</p>'}`};
// ---- admin ----
V.console=async()=>{const s=await api('/admin/stats');return`<h2>Banking console</h2><div class="grid">${[['In circulation',s.circulation],['Active accounts',s.active],['Suspended',s.suspended],['Cards issued',s.cards],['Issued (all time)',s.issued],['Transfer volume',s.transfer_volume],['Pending disputes',s.pending_disputes]].map(x=>`<div class="card"><div class="mute">${x[0]}</div><div class="big">${esc(x[1])}</div></div>`).join('')}</div>
 <div class="card"><h3>Recent transactions</h3>${txTable(s.recent,1)}</div><div class="card"><h3>Recent income batches</h3>${s.batches.map(b=>`<div>${esc(b.ref)} · ${esc(b.description)} · <span class="tag">${esc(b.status)}</span></div>`).join('')||'<span class="mute">None.</span>'}</div>
 <div class="card"><h3>Recent admin actions</h3>${s.audit.map(a=>`<div>${esc(a.created_at)} · ${esc(a.action)} · ${esc(a.target)}</div>`).join('')||'<span class="mute">None.</span>'}</div>`};
V.accts=()=>`<h2>Accounts</h2><div class="card"><h3>Create citizen account</h3><div class="row"><input id="nu" placeholder="Username" aria-label="Username"><input id="nn" placeholder="Preferred name" aria-label="Preferred name"><input id="nc" placeholder="Avena ID number (optional)" aria-label="Citizen ID"><input id="np" type="password" placeholder="Temporary password (10+)" aria-label="Temporary password"><button id="nb">Create</button></div></div>
 <div class="card"><input id="aq" placeholder="Search accounts" aria-label="Search accounts"><div id="al"></div></div>`;
W.accts=()=>{const load=async()=>{const a=await api('/admin/accounts?q='+encodeURIComponent(val('aq')));$('#al').innerHTML=`<div class="wrap"><table><tr><th>Account</th><th>Name</th><th>Balance</th><th>Status</th><th></th></tr>${a.map(x=>`<tr><td>${esc(x.public_id)}</td><td>${esc(x.preferred_name)}</td><td>${esc(x.balance_fmt)}</td><td><span class="tag">${esc(x.status)}</span></td><td>${x.status==='Closed'?'':['Active','Suspended','Closed'].filter(s=>s!==x.status).map(s=>`<button class="alt" data-p="${x.public_id}" data-s="${s}">${s==='Active'?'Reactivate':s}</button>`).join(' ')}</td></tr>`).join('')}</table></div>`;
   document.querySelectorAll('[data-p]').forEach(b=>b.onclick=()=>act(b,async()=>{const r=prompt(`Reason to set ${b.dataset.p} to ${b.dataset.s}:`);if(!r)return;if(b.dataset.s!=='Active'&&!confirm(`Really set ${b.dataset.s}?`))return;
     await api(`/admin/accounts/${b.dataset.p}/status`,{status:b.dataset.s,reason:r,confirm:true});load()}))};
  $('#aq').oninput=load;$('#nb').onclick=e=>act(e.target,async()=>{const r=await api('/admin/accounts',{username:val('nu'),name:val('nn'),citizen_id:val('nc'),password:val('np')});toast('Created '+r.public_id);load()});load()};
V.issue=()=>`<h2>Issue Aurum</h2><div class="card"><label for="ia">Recipient account IDs (one per line, optionally "ID, amount")</label><textarea id="ia" rows="4"></textarea><label for="iam">Default amount per recipient (₳)</label><input id="iam"><label for="ir">Reason (required)</label><input id="ir"><p><button id="ib">Review and issue</button></p><div id="io"></div></div>`;
const parseRecs=(txt,def)=>txt.split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{const [account,amount]=l.split(/[ ,]+/);return{account,amount:amount||def}});
W.issue=()=>{$('#ib').onclick=e=>act(e.target,async()=>{const recipients=parseRecs(val('ia'),val('iam'));const tot=recipients.reduce((s,r)=>s+parseFloat(r.amount||0),0);
  if(!recipients.length||!val('ir'))throw new Error('Recipients and a reason are required.');if(!confirm(`Issue ₳${tot.toFixed(2)} to ${recipients.length} account(s)?`))return;
  const body={recipients,reason:val('ir'),idem_key:KEY};let r;try{r=await api('/admin/issue',body)}catch(x){if(!/Large/.test(x.message))throw x;if(!confirm('Large issuance. Confirm again?'))return;r=await api('/admin/issue',{...body,confirm_large:true})}
  $('#io').innerHTML=r.results.map(x=>`<div class="${x.ok?'in':'out'}">${x.ok?'✓ '+esc(x.ref):'✗ '+esc(x.error)}</div>`).join('')})};
V.income=async()=>{const b=await api('/admin/batches');return`<h2>Income payments</h2><div class="card"><h3>New batch</h3><label for="bd">Description</label><input id="bd" placeholder="Citizen Income">
 <label for="bn">Internal note</label><input id="bn"><label for="ba">Recipients (one per line, "ID, amount") or leave blank for all eligible</label><textarea id="ba" rows="3"></textarea><label for="bm">Amount per recipient (₳)</label><input id="bm"><p><button id="bc">Create and review</button></p><div id="bv"></div></div>
 <div class="card"><h3>History</h3>${b.map(x=>`<div><a href="#" data-b="${x.id}">${esc(x.ref)}</a> · ${esc(x.description)} · <span class="tag">${esc(x.status)}</span></div>`).join('')||'<span class="mute">No batches.</span>'}</div>`};
W.income=()=>{const show=async v=>{$('#bv').innerHTML=`<p><b>${v.recipients}</b> recipients · total <b>${esc(v.total_fmt)}</b> · ineligible: ${v.ineligible} · status <span class="tag">${esc(v.batch.status)}</span></p><div class="wrap"><table>${v.items.map(i=>`<tr><td>${esc(i.public_id)}</td><td>${esc(i.amount_fmt)}</td><td>${esc(i.status)} ${esc(i.error||'')}</td></tr>`).join('')}</table></div>
  ${['draft','executing'].includes(v.batch.status)?'<button id="bx">Confirm and execute</button>':''}`;
  if($('#bx'))$('#bx').onclick=e=>act(e.target,async()=>{if(!confirm(`Execute ${v.batch.ref}: ${v.total_fmt} to ${v.recipients} recipients?`))return;show(await api(`/admin/batches/${v.batch.id}/execute`,{confirm:true}))})};
  $('#bc').onclick=e=>act(e.target,async()=>{const t=val('ba').trim();show(await api('/admin/batches',{description:val('bd'),note:val('bn'),...(t?{recipients:parseRecs(t,val('bm'))}:{all:true,amount:val('bm')})}))});
  document.querySelectorAll('[data-b]').forEach(a=>a.onclick=async e=>{e.preventDefault();show(await api('/admin/batches/'+a.dataset.b))})};
V.atx=async()=>{const r=await api('/admin/transactions');return`<h2>All transactions</h2><div class="card">${txTable(r.items,1)}</div>${can('adjust')?`<div class="card"><h3>Corrections</h3><div class="row"><input id="rr" placeholder="Reference to reverse" aria-label="Reference to reverse"><input id="rn" placeholder="Reason" aria-label="Reason"><button id="rb">Reverse</button></div>
 <div class="row"><input id="da" placeholder="Account ID" aria-label="Account ID"><input id="dv" placeholder="₳ amount" aria-label="Amount"><input id="dn" placeholder="Reason" aria-label="Deduction reason"><button id="db">Deduct</button></div></div>`:''}`};
W.atx=()=>{if(!$('#rb'))return;$('#rb').onclick=e=>act(e.target,async()=>{if(!confirm('Reverse this transaction?'))return;await api(`/admin/transactions/${val('rr')}/reverse`,{reason:val('rn')});toast('Reversed.');route()});
  $('#db').onclick=e=>act(e.target,async()=>{if(!confirm('Deduct from this account?'))return;await api('/admin/adjust',{account:val('da'),amount:val('dv'),reason:val('dn'),idem_key:KEY});toast('Deducted.');route()})};
V.disp=async()=>{const d=await api('/admin/disputes');return`<h2>Disputes</h2>${d.map(x=>`<div class="card"><b>${esc(x.ref)}</b> <span class="tag">${esc(x.status)}</span><p>${esc(x.message)}</p>${x.status==='Pending'?`<input id="r${x.id}" placeholder="Response" aria-label="Response"> <button data-d="${x.id}" data-s="Upheld">Uphold</button> <button class="alt" data-d="${x.id}" data-s="Rejected">Reject</button>`:`<div class="mute">${esc(x.response)} · ${esc(x.resolved_at)}</div>`}</div>`).join('')||'<p class="mute">No disputes.</p>'}`};
W.disp=()=>document.querySelectorAll('[data-d]').forEach(b=>b.onclick=()=>act(b,async()=>{await api('/admin/disputes/'+b.dataset.d,{status:b.dataset.s,response:val('r'+b.dataset.d)});route()}));
V.acards=async()=>{const c=await api('/admin/cards');return`<h2>Cards</h2><div class="card"><div class="wrap"><table>${c.map(x=>`<tr><td>${esc(x.public_id)}</td><td>••${esc(x.last4)}</td><td><select data-k="${x.id}" aria-label="Card status">${['Active','Frozen','Inactive','Replaced'].map(s=>`<option${s===x.status?' selected':''}>${s}</option>`).join('')}</select></td></tr>`).join('')}</table></div></div>`};
W.acards=()=>document.querySelectorAll('[data-k]').forEach(s=>s.onchange=()=>act(s,async()=>{await api('/admin/cards/'+s.dataset.k,{status:s.value});toast('Updated.')}));
V.audit=async()=>{const a=await api('/admin/audit');return`<h2>Audit log</h2><div class="card wrap"><table><tr><th>When</th><th>Actor</th><th>Action</th><th>Target</th><th>Reason</th></tr>${a.map(x=>`<tr><td>${esc(x.created_at)}</td><td>${esc(x.actor)}</td><td>${esc(x.action)}</td><td>${esc(x.target)}</td><td>${esc(x.reason)}</td></tr>`).join('')}</table></div>`};
boot();
</script></body></html>
'''


if __name__ == "__main__": cli()
else: init_db()
