"""Monitor de planta: app aparte de MetalERP.

Recibe del router una señal por cada pieza que termina una máquina (o el
contador del CNC), guarda cada pieza en su propia base y una vez por hora
manda el paquete a MetalERP por la API. Si MetalERP no responde, el paquete
queda guardado y se reintenta cada 5 minutos, sin duplicar nada.

Arranque: INICIAR_MONITOR.bat (o python monitor.py). Pantalla: http://<pc>:5100
"""
import json, os, socket, sqlite3, threading, time, uuid
import urllib.request, urllib.error
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory, g

AQUI = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(AQUI, "monitor.db")
CONFIG_PATH = os.path.join(AQUI, "config.json")
CONFIG_DEFECTO = {
    "metalerp_url": "http://localhost:5000",
    "api_key": "",
    "intervalo_min": 60,          # cada cuánto se manda el paquete (alineado al reloj)
    "puerto": 5100,
    "parada_seg": 180,            # sin piezas durante este tiempo: la máquina figura parada
    "senal_token": "",            # si se completa, el router tiene que mandarlo (?token=...)
}
REINTENTO_MIN = 5
MAX_PIEZAS_PAQUETE = 5000
MAX_SALTO_CONTADOR = 500          # un salto mayor del contador se toma como reinicio

app = Flask(__name__, template_folder=os.path.join(AQUI, "templates"))
_lock_cfg = threading.Lock()
_aviso_envio = threading.Event()
_estado = {"metalerp_ok": None, "metalerp_error": None, "metalerp_visto": None,
           "maquinas": [], "ultimo_envio": None, "proximo_envio": None, "enviando": False}
_lock_contador = threading.Lock()


def ahora():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Configuración ────────────────────────────────────────────────────────────
def cfg():
    with _lock_cfg:
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                c = json.load(f)
        except (OSError, ValueError):
            c = {}
        return {**CONFIG_DEFECTO, **c}


def guardar_cfg(nuevo):
    with _lock_cfg:
        c = {**CONFIG_DEFECTO}
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                c.update(json.load(f))
        except (OSError, ValueError):
            pass
        c.update(nuevo)
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(c, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_PATH)
        return c


# ── Base propia ──────────────────────────────────────────────────────────────
ESQUEMA = """
CREATE TABLE IF NOT EXISTS piezas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT UNIQUE NOT NULL,
    maquina TEXT NOT NULL,
    hora TEXT NOT NULL,
    ciclo_seg REAL,
    contador INTEGER,
    origen TEXT,
    paquete TEXT,                 -- NULL: todavía no entró en un paquete
    creado TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS ix_piezas_paquete ON piezas(paquete, id);
CREATE INDEX IF NOT EXISTS ix_piezas_maq ON piezas(maquina, hora);
CREATE TABLE IF NOT EXISTS paquetes (
    uuid TEXT PRIMARY KEY,
    creado TEXT DEFAULT (datetime('now','localtime')),
    desde TEXT, hasta TEXT,
    piezas INTEGER DEFAULT 0,
    estado TEXT DEFAULT 'Por enviar',   -- Por enviar / Enviado / Error
    intentos INTEGER DEFAULT 0,
    proximo_intento TEXT,
    enviado TEXT,
    respuesta TEXT,
    error TEXT
);
CREATE TABLE IF NOT EXISTS contadores (
    maquina TEXT PRIMARY KEY,
    contador INTEGER,
    hora TEXT
);
CREATE TABLE IF NOT EXISTS ultima_pieza (
    maquina TEXT PRIMARY KEY,
    hora TEXT
);
"""


def conectar():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA busy_timeout = 30000")
    return db


def get_db():
    if "db" not in g:
        g.db = conectar()
    return g.db


@app.teardown_appcontext
def _cerrar(e=None):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    db = conectar()
    db.executescript(ESQUEMA)
    db.commit()
    db.close()


# ── Señales del router ───────────────────────────────────────────────────────
def registrar_piezas(db, maquina, n, hora_fin, contador=None, origen="senal"):
    """Guarda n piezas de la máquina terminadas hasta hora_fin, repartidas
    entre la pieza anterior y esta. El ciclo es el tiempo entre piezas."""
    maquina = maquina.strip().upper()
    fin = datetime.strptime(hora_fin, "%Y-%m-%d %H:%M:%S")
    r = db.execute("SELECT hora FROM ultima_pieza WHERE maquina=?", (maquina,)).fetchone()
    anterior = datetime.strptime(r["hora"], "%Y-%m-%d %H:%M:%S") if r else None
    paso = (fin - anterior).total_seconds() / n if anterior and fin > anterior else None
    # Un hueco de más de 30 min no es un ciclo (almuerzo, parada): sin ciclo.
    if paso is not None and paso > 1800:
        paso = None
    filas = []
    for i in range(n):
        h = fin - timedelta(seconds=(paso or 0) * (n - 1 - i))
        filas.append((str(uuid.uuid4()), maquina, h.strftime("%Y-%m-%d %H:%M:%S"),
                       round(paso, 1) if paso else None,
                       (contador - (n - 1 - i)) if contador is not None else None, origen))
    db.executemany("INSERT INTO piezas (uuid, maquina, hora, ciclo_seg, contador, origen) VALUES (?,?,?,?,?,?)", filas)
    db.execute("INSERT INTO ultima_pieza (maquina, hora) VALUES (?,?) ON CONFLICT(maquina) DO UPDATE SET hora=excluded.hora",
               (maquina, hora_fin))
    return n


def _hora_senal(v):
    s = str(v or "").strip().replace("T", " ")[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            h = datetime.strptime(s, fmt)
            # Reloj del router muy corrido: se usa la hora de la PC.
            if abs((h - datetime.now()).total_seconds()) < 12 * 3600:
                return h.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return ahora()


@app.route("/senal", methods=["GET", "POST"])
def senal():
    """Lo llama el router (o el programa puente) por cada pieza:
         GET  /senal?maquina=T16                 -> 1 pieza
         GET  /senal?maquina=T16&contador=18452   -> las piezas desde el último contador
         GET  /senal?maquina=T16&piezas=3         -> 3 piezas
         POST /senal  {"maquina": "T16", "contador": 18452, "hora": "..."}  (o una lista de esos)
    """
    c = cfg()
    datos = request.get_json(silent=True) if request.method == "POST" else None
    if datos is None:
        datos = dict(request.values)
    lista = datos if isinstance(datos, list) else [datos]
    if c.get("senal_token") and (request.values.get("token") or (lista[0] or {}).get("token")) != c["senal_token"]:
        return jsonify({"error": "token inválido"}), 403
    total, res = 0, []
    db = get_db()
    with _lock_contador:
        for d in lista:
            if not isinstance(d, dict):
                continue
            maq = str(d.get("maquina") or "").strip().upper()
            if not maq:
                res.append({"error": "falta maquina"}); continue
            hora = _hora_senal(d.get("hora"))
            n = 1
            cont = d.get("contador")
            if cont not in (None, ""):
                try:
                    cont = int(float(cont))
                except (TypeError, ValueError):
                    res.append({"maquina": maq, "error": "contador inválido"}); continue
                prev = db.execute("SELECT contador FROM contadores WHERE maquina=?", (maq,)).fetchone()
                db.execute("""INSERT INTO contadores (maquina, contador, hora) VALUES (?,?,?)
                    ON CONFLICT(maquina) DO UPDATE SET contador=excluded.contador, hora=excluded.hora""", (maq, cont, hora))
                if prev is None or prev["contador"] is None:
                    n = 0          # primera lectura: solo se toma como referencia
                else:
                    n = cont - prev["contador"]
                    if n < 0 or n > MAX_SALTO_CONTADOR:
                        n = 0      # el contador se reinició o saltó: nueva referencia
            elif d.get("piezas") not in (None, ""):
                try:
                    n = max(0, min(MAX_SALTO_CONTADOR, int(float(d.get("piezas")))))
                except (TypeError, ValueError):
                    n = 0
            else:
                cont = None
            if n:
                registrar_piezas(db, maq, n, hora, cont if isinstance(cont, int) else None,
                                 origen=str(d.get("origen") or "router")[:20])
            total += n
            res.append({"maquina": maq, "piezas": n})
        db.commit()
    return jsonify({"ok": True, "piezas": total, "detalle": res})


# ── Conexión con MetalERP ────────────────────────────────────────────────────
def llamar_metalerp(metodo, ruta, cuerpo=None, timeout=60, url_base=None, sin_clave=False):
    c = cfg()
    if not sin_clave and not c.get("api_key"):
        raise RuntimeError("La app todavía no está conectada con MetalERP (Configuración → Conectar)")
    url = (url_base or c["metalerp_url"]).rstrip("/") + ruta
    data = json.dumps(cuerpo).encode("utf-8") if cuerpo is not None else None
    cab = {"Content-Type": "application/json"}
    if not sin_clave:
        cab["X-Api-Key"] = c["api_key"]
    req = urllib.request.Request(url, data=data, method=metodo, headers=cab)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error")
        except Exception:
            msg = None
        if e.code == 404 and not msg:
            msg = "MetalERP no tiene la conexión para la app: reinicialo con INICIAR_SERVIDOR.bat para que tome la versión nueva"
        raise RuntimeError(msg or f"MetalERP respondió {e.code}")
    except (urllib.error.URLError, socket.timeout, ConnectionError) as e:
        raise RuntimeError(f"No se pudo conectar con MetalERP en {url.split('/api/')[0]}: ¿está abierto el servidor? ({getattr(e, 'reason', e)})")


def armar_paquetes(db):
    """Las piezas que todavía no están en un paquete pasan a uno nuevo."""
    while True:
        ids = [r["id"] for r in db.execute(
            "SELECT id FROM piezas WHERE paquete IS NULL ORDER BY id LIMIT ?", (MAX_PIEZAS_PAQUETE,)).fetchall()]
        if not ids:
            return
        u = str(uuid.uuid4())
        marcas = ",".join("?" * len(ids))
        db.execute(f"UPDATE piezas SET paquete=? WHERE id IN ({marcas})", [u] + ids)
        r = db.execute("SELECT MIN(hora) d, MAX(hora) h, COUNT(*) n FROM piezas WHERE paquete=?", (u,)).fetchone()
        db.execute("INSERT INTO paquetes (uuid, desde, hasta, piezas) VALUES (?,?,?,?)", (u, r["d"], r["h"], r["n"]))
        db.commit()


def enviar_pendientes(forzar=False):
    """Manda los paquetes por enviar (y los que fallaron, cuando toca)."""
    db = conectar()
    _estado["enviando"] = True
    try:
        armar_paquetes(db)
        filtro = "estado!='Enviado'" if forzar else "estado='Por enviar' OR (estado='Error' AND (proximo_intento IS NULL OR proximo_intento<=?))"
        args = () if forzar else (ahora(),)
        for k in db.execute(f"SELECT * FROM paquetes WHERE {filtro} ORDER BY creado, uuid", args).fetchall():
            piezas = [dict(r) for r in db.execute(
                "SELECT uuid, maquina, hora, ciclo_seg, contador FROM piezas WHERE paquete=? ORDER BY id", (k["uuid"],))]
            try:
                r = llamar_metalerp("POST", "/api/planta/v1/paquetes",
                                    {"uuid": k["uuid"], "desde": k["desde"], "hasta": k["hasta"], "piezas": piezas}, timeout=120)
                db.execute("""UPDATE paquetes SET estado='Enviado', enviado=?, respuesta=?, error=NULL,
                    intentos=intentos+1 WHERE uuid=?""", (ahora(), json.dumps(r, ensure_ascii=False)[:4000], k["uuid"]))
                db.commit()
                _estado.update(metalerp_ok=True, metalerp_error=None, metalerp_visto=ahora(), ultimo_envio=ahora())
            except Exception as e:
                prox = (datetime.now() + timedelta(minutes=REINTENTO_MIN)).strftime("%Y-%m-%d %H:%M:%S")
                db.execute("""UPDATE paquetes SET estado='Error', error=?, intentos=intentos+1, proximo_intento=?
                    WHERE uuid=?""", (str(e)[:500], prox, k["uuid"]))
                db.commit()
                _estado.update(metalerp_ok=False, metalerp_error=str(e))
                break           # si uno falla, los siguientes esperan (van en orden)
    finally:
        _estado["enviando"] = False
        db.close()


def proximo_envio(desde=None):
    """Próximo envío alineado al reloj: con 60 min, a cada hora en punto."""
    m = max(1, int(cfg().get("intervalo_min") or 60))
    d = desde or datetime.now()
    base = d.replace(hour=0, minute=0, second=0, microsecond=0)
    pasados = int((d - base).total_seconds() // 60)
    return base + timedelta(minutes=(pasados // m + 1) * m)


def hilo_envios():
    prox = proximo_envio()
    _estado["proximo_envio"] = prox.strftime("%Y-%m-%d %H:%M:%S")
    while True:
        forzado = _aviso_envio.wait(30)
        _aviso_envio.clear()
        try:
            if forzado or datetime.now() >= prox:
                enviar_pendientes(forzar=forzado)
                if datetime.now() >= prox:
                    prox = proximo_envio()
            else:
                # Reintentos de paquetes que fallaron (cada 5 min), sin esperar la hora.
                db = conectar()
                hay = db.execute("SELECT 1 FROM paquetes WHERE estado='Error' AND proximo_intento<=?", (ahora(),)).fetchone()
                db.close()
                if hay:
                    enviar_pendientes()
            _estado["proximo_envio"] = prox.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"  [envíos] {e}")


def hilo_maquinas():
    """Lee de MetalERP las máquinas con su operación y operario, cada 30 s."""
    while True:
        try:
            if cfg().get("api_key"):
                _estado["maquinas"] = llamar_metalerp("GET", "/api/planta/v1/maquinas", timeout=20)
                _estado.update(metalerp_ok=True, metalerp_error=None, metalerp_visto=ahora())
        except Exception as e:
            _estado.update(metalerp_ok=False, metalerp_error=str(e))
        time.sleep(30)


# ── Pantalla ─────────────────────────────────────────────────────────────────
_IPS_PROPIAS = set()


def es_local():
    """La configuración se cambia solo desde la PC donde corre la app (por
    localhost o por la IP de la propia PC)."""
    if not _IPS_PROPIAS:
        _IPS_PROPIAS.update({"127.0.0.1", "::1", "localhost", ip_local()})
        try:
            _IPS_PROPIAS.update(socket.gethostbyname_ex(socket.gethostname())[2])
        except OSError:
            pass
    return request.remote_addr in _IPS_PROPIAS


@app.route("/")
def inicio():
    return send_from_directory(os.path.join(AQUI, "templates"), "monitor.html")


@app.route("/api/estado")
def api_estado():
    db = get_db()
    c = cfg()
    hoy = datetime.now().strftime("%Y-%m-%d")
    stats = {r["maquina"]: dict(r) for r in db.execute("""SELECT maquina,
            SUM(CASE WHEN substr(hora,1,10)=? THEN 1 ELSE 0 END) hoy,
            SUM(CASE WHEN paquete IS NULL THEN 1 ELSE 0 END) sin_paquete,
            MAX(hora) ultima,
            AVG(CASE WHEN hora>=? THEN ciclo_seg END) ciclo_prom
        FROM piezas WHERE hora>=? OR paquete IS NULL GROUP BY maquina""",
        (hoy, (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"), hoy)).fetchall()}
    sin_enviar = db.execute("""SELECT COUNT(*) FROM piezas p LEFT JOIN paquetes k ON k.uuid=p.paquete
        WHERE p.paquete IS NULL OR k.estado!='Enviado'""").fetchone()[0]
    maquinas = []
    vistos = set()
    for m in _estado["maquinas"] or []:
        s = stats.get(m["codigo"].upper(), {})
        vistos.add(m["codigo"].upper())
        maquinas.append({**m, "local": s})
    for cod, s in stats.items():   # señales de máquinas que MetalERP no conoce
        if cod not in vistos:
            maquinas.append({"codigo": cod, "nombre": "(no está en MetalERP)", "desconocida": True, "local": s})
    return jsonify({
        "hora": ahora(), "config": {k: c[k] for k in ("metalerp_url", "intervalo_min", "parada_seg", "puerto")},
        "tiene_clave": bool(c.get("api_key")), "es_local": es_local(),
        "metalerp_ok": _estado["metalerp_ok"], "metalerp_error": _estado["metalerp_error"],
        "metalerp_visto": _estado["metalerp_visto"], "ultimo_envio": _estado["ultimo_envio"],
        "proximo_envio": _estado["proximo_envio"], "enviando": _estado["enviando"],
        "sin_enviar": sin_enviar, "maquinas": maquinas,
    })


@app.route("/api/paquetes")
def api_paquetes():
    filas = get_db().execute("SELECT * FROM paquetes ORDER BY creado DESC LIMIT 100").fetchall()
    res = []
    for r in filas:
        x = dict(r)
        try:
            x["respuesta"] = json.loads(r["respuesta"]) if r["respuesta"] else None
        except ValueError:
            pass
        res.append(x)
    return jsonify(res)


@app.route("/api/piezas")
def api_piezas():
    w, p = ["1=1"], []
    if request.args.get("maquina"):
        w.append("maquina=?"); p.append(request.args["maquina"].upper())
    filas = get_db().execute(f"""SELECT p.*, k.estado paquete_estado FROM piezas p
        LEFT JOIN paquetes k ON k.uuid=p.paquete WHERE {' AND '.join(w)} ORDER BY p.id DESC LIMIT 300""", p).fetchall()
    return jsonify([dict(r) for r in filas])


@app.route("/api/enviar_ahora", methods=["POST"])
def api_enviar_ahora():
    _aviso_envio.set()
    return jsonify({"ok": True})


@app.route("/api/config", methods=["GET", "PUT"])
def api_config():
    if request.method == "GET":
        c = cfg()
        return jsonify({**{k: v for k, v in c.items() if k != "api_key"},
                        "api_key_fin": (c.get("api_key") or "")[-4:], "es_local": es_local()})
    if not es_local():
        return jsonify({"error": "La configuración solo se cambia desde la PC donde corre la app"}), 403
    d = request.get_json(silent=True) or {}
    nuevo = {}
    if "metalerp_url" in d:
        u = str(d["metalerp_url"]).strip().rstrip("/")
        if not u.startswith(("http://", "https://")):
            return jsonify({"error": "La dirección tiene que empezar con http://"}), 400
        nuevo["metalerp_url"] = u
    if d.get("api_key"):
        nuevo["api_key"] = str(d["api_key"]).strip()
    for k, lo, hi in (("intervalo_min", 1, 1440), ("parada_seg", 30, 7200)):
        if k in d:
            try:
                nuevo[k] = max(lo, min(hi, int(d[k])))
            except (TypeError, ValueError):
                return jsonify({"error": f"{k} inválido"}), 400
    if "senal_token" in d:
        nuevo["senal_token"] = str(d["senal_token"] or "").strip()
    guardar_cfg(nuevo)
    _estado["proximo_envio"] = proximo_envio().strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"ok": True})


@app.route("/api/vincular", methods=["POST"])
def api_vincular():
    """Conecta la app con MetalERP con un usuario administrador: MetalERP da
    una clave nueva y queda guardada acá. No hace falta copiar nada."""
    if not es_local():
        return jsonify({"ok": False, "error": "Conectá la app desde la PC donde está instalada"}), 403
    d = request.get_json(silent=True) or {}
    url = str(d.get("metalerp_url") or cfg()["metalerp_url"]).strip().rstrip("/")
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        r = llamar_metalerp("POST", "/api/planta/v1/vincular",
                            {"username": d.get("username"), "password": d.get("password"), "nombre": "Monitor de planta"},
                            timeout=15, url_base=url, sin_clave=True)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    guardar_cfg({"metalerp_url": url, "api_key": r["clave"]})
    try:
        _estado["maquinas"] = llamar_metalerp("GET", "/api/planta/v1/maquinas", timeout=20)
        _estado.update(metalerp_ok=True, metalerp_error=None, metalerp_visto=ahora())
    except Exception:
        pass
    return jsonify({"ok": True, "app": r.get("nombre")})


@app.route("/api/probar", methods=["POST"])
def api_probar():
    try:
        r = llamar_metalerp("GET", "/api/planta/v1/estado", timeout=10)
        _estado.update(metalerp_ok=True, metalerp_error=None, metalerp_visto=ahora())
        try:
            _estado["maquinas"] = llamar_metalerp("GET", "/api/planta/v1/maquinas", timeout=20)
        except Exception:
            pass
        return jsonify({"ok": True, "app": r.get("app")})
    except Exception as e:
        _estado.update(metalerp_ok=False, metalerp_error=str(e))
        return jsonify({"ok": False, "error": str(e)})


def ip_local():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "127.0.0.1"


if __name__ == "__main__":
    init_db()
    c = cfg()
    if not os.path.exists(CONFIG_PATH):
        guardar_cfg({})
    threading.Thread(target=hilo_envios, name="envios", daemon=True).start()
    threading.Thread(target=hilo_maquinas, name="maquinas", daemon=True).start()
    puerto = int(c.get("puerto") or 5100)
    print("=" * 52 + "\n   Monitor de planta (conectado a MetalERP)\n" + "=" * 52)
    print(f"\n   Pantalla:  http://localhost:{puerto}")
    print(f"   En la red: http://{ip_local()}:{puerto}")
    print(f"   Señal del router: http://{ip_local()}:{puerto}/senal?maquina=T16")
    print(f"   MetalERP:  {c['metalerp_url']}  (paquete cada {c['intervalo_min']} min)\n")
    app.run(host="0.0.0.0", port=puerto, debug=False, threaded=True)
