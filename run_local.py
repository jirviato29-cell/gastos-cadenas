"""
run_local.py — versión SQLite para pruebas locales.
Usa la misma carpeta templates/ que main.py.
Ejecutar: python run_local.py
"""
import sqlite3
import os
from flask import Flask, render_template, request, jsonify, redirect
from datetime import date, datetime
from io import BytesIO
import openpyxl

app = Flask(__name__, template_folder='templates')
app.config['SECRET_KEY'] = 'local-test-2026'

DB_PATH = os.path.join(os.path.dirname(__file__), 'local_gastos.db')


# ── SQLite helpers ────────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def ql(rows): return [dict(r) for r in rows]
def q1(row):  return dict(row) if row else None


# ── Cálculos (idénticos a main.py) ───────────────────────────────────────────

def dias_vacaciones_ley(anos):
    if anos < 1:  return 0
    if anos < 2:  return 12
    if anos < 3:  return 14
    if anos < 4:  return 16
    if anos < 5:  return 18
    if anos < 10: return 20
    return 22


def calcular_gasto_promotor(p):
    sueldo = float(p['sueldo'] or 0)
    sd  = sueldo / 30
    ss  = round(sueldo / 4.33, 2)
    fi  = p['fecha_ingreso']
    if isinstance(fi, str):
        fi = datetime.strptime(fi, '%Y-%m-%d').date()
    anos    = max(0, (date.today() - fi).days / 365.25)
    dias_vac = dias_vacaciones_ley(int(anos))
    ag  = round((sd * 15) / 52, 2)
    vac = round((dias_vac * sd) / 52, 2)
    pv  = round(vac * 0.25, 2)
    seg = 696.42 if p.get('tiene_seguro') else 0.0
    isn, imp, gi, fc = 61.54, 417.05, 274.0, 27.0
    total = round(ss + seg + isn + imp + gi + fc + ag + vac + pv, 2)
    return dict(sueldo_semanal=ss, seguro=seg, isn=isn, impuestos=imp,
                gastos_indirectos=gi, fondo_contingencia=fc,
                aguinaldo=ag, vacaciones=vac, prima_vacacional=pv,
                total=total, anos=round(anos, 1), dias_vac=dias_vac)


# ── Init DB ───────────────────────────────────────────────────────────────────

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS tiendas (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre          TEXT NOT NULL,
            cadena          TEXT NOT NULL,
            clave_principal TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS promotores (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre                  TEXT NOT NULL,
            tienda_id               INTEGER REFERENCES tiendas(id),
            sueldo                  REAL NOT NULL DEFAULT 0,
            fecha_ingreso           TEXT NOT NULL,
            tiene_seguro            INTEGER DEFAULT 0,
            dias_vacaciones_tomados INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS subclaves_telcel (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            tienda_id INTEGER REFERENCES tiendas(id) ON DELETE CASCADE,
            subclave  TEXT NOT NULL,
            UNIQUE(tienda_id, subclave)
        );
        CREATE TABLE IF NOT EXISTS semanas (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_inicio TEXT NOT NULL,
            fecha_fin    TEXT NOT NULL,
            UNIQUE(fecha_inicio, fecha_fin)
        );
        CREATE TABLE IF NOT EXISTS gastos_semana (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            semana_id          INTEGER REFERENCES semanas(id)   ON DELETE CASCADE,
            promotor_id        INTEGER REFERENCES promotores(id) ON DELETE CASCADE,
            sueldo_semanal     REAL DEFAULT 0,
            comisiones         REAL DEFAULT 0,
            seguro             REAL DEFAULT 0,
            isn                REAL DEFAULT 0,
            impuestos          REAL DEFAULT 0,
            gastos_indirectos  REAL DEFAULT 0,
            fondo_contingencia REAL DEFAULT 0,
            aguinaldo          REAL DEFAULT 0,
            vacaciones         REAL DEFAULT 0,
            prima_vacacional   REAL DEFAULT 0,
            total              REAL DEFAULT 0,
            UNIQUE(semana_id, promotor_id)
        );
        CREATE TABLE IF NOT EXISTS pagos_telcel (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            semana_id INTEGER REFERENCES semanas(id) ON DELETE CASCADE,
            subclave  TEXT NOT NULL,
            producto  TEXT,
            comision  REAL DEFAULT 0
        );
    """)
    db.commit()
    db.close()
    print("SQLite DB inicializada —", DB_PATH)


# ── TIENDAS ───────────────────────────────────────────────────────────────────

@app.route('/tiendas')
def tiendas():
    db = get_db()
    ts   = ql(db.execute("SELECT * FROM tiendas ORDER BY cadena, nombre").fetchall())
    subs = ql(db.execute("SELECT * FROM subclaves_telcel ORDER BY id").fetchall())
    db.close()
    smap = {}
    for s in subs:
        smap.setdefault(s['tienda_id'], []).append(s['subclave'])
    for t in ts:
        t['subclaves'] = smap.get(t['id'], [])
    return render_template('tiendas.html', tiendas=ts)


@app.route('/api/tiendas', methods=['POST'])
def add_tienda():
    d  = request.json
    db = get_db()
    try:
        cur = db.execute(
            "INSERT INTO tiendas (nombre, cadena, clave_principal) VALUES (?,?,?)",
            (d['nombre'].strip(), d['cadena'].strip(), d['clave_principal'].strip()))
        tid = cur.lastrowid
        for sub in d.get('subclaves', []):
            if sub.strip():
                db.execute("INSERT OR IGNORE INTO subclaves_telcel (tienda_id, subclave) VALUES (?,?)",
                           (tid, sub.strip()))
        db.commit()
        return jsonify({'ok': True, 'id': tid})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


@app.route('/api/tiendas/<int:tid>', methods=['PUT'])
def edit_tienda(tid):
    d  = request.json
    db = get_db()
    try:
        db.execute("UPDATE tiendas SET nombre=?, cadena=?, clave_principal=? WHERE id=?",
                   (d['nombre'].strip(), d['cadena'].strip(), d['clave_principal'].strip(), tid))
        db.execute("DELETE FROM subclaves_telcel WHERE tienda_id=?", (tid,))
        for sub in d.get('subclaves', []):
            if sub.strip():
                db.execute("INSERT INTO subclaves_telcel (tienda_id, subclave) VALUES (?,?)",
                           (tid, sub.strip()))
        db.commit()
        return jsonify({'ok': True})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


@app.route('/api/tiendas/<int:tid>', methods=['DELETE'])
def del_tienda(tid):
    db = get_db()
    try:
        db.execute("DELETE FROM tiendas WHERE id=?", (tid,))
        db.commit(); return jsonify({'ok': True})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


# ── PROMOTORES ────────────────────────────────────────────────────────────────

@app.route('/promotores')
def promotores():
    db = get_db()
    proms   = ql(db.execute("""
        SELECT p.*, t.nombre AS tienda_nombre, t.cadena
        FROM promotores p LEFT JOIN tiendas t ON t.id = p.tienda_id
        ORDER BY t.cadena, t.nombre, p.nombre
    """).fetchall())
    tiendas = ql(db.execute(
        "SELECT id, nombre, cadena FROM tiendas ORDER BY cadena, nombre").fetchall())
    db.close()
    result = []
    for p in proms:
        p['tiene_seguro']     = bool(p['tiene_seguro'])
        p['fecha_ingreso_str'] = p['fecha_ingreso'] or ''
        g = calcular_gasto_promotor(p)
        p.update(g)
        result.append(p)
    return render_template('promotores.html', promotores=result, tiendas=tiendas)


@app.route('/api/promotores', methods=['POST'])
def add_promotor():
    d  = request.json
    db = get_db()
    try:
        cur = db.execute("""
            INSERT INTO promotores
                (nombre, tienda_id, sueldo, fecha_ingreso, tiene_seguro, dias_vacaciones_tomados)
            VALUES (?,?,?,?,?,?)
        """, (d['nombre'].strip(), d.get('tienda_id') or None,
              float(d['sueldo']), d['fecha_ingreso'],
              1 if d.get('tiene_seguro') else 0,
              int(d.get('dias_vacaciones_tomados', 0))))
        db.commit()
        return jsonify({'ok': True, 'id': cur.lastrowid})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


@app.route('/api/promotores/<int:pid>', methods=['PUT'])
def edit_promotor(pid):
    d  = request.json
    db = get_db()
    try:
        db.execute("""
            UPDATE promotores SET nombre=?, tienda_id=?, sueldo=?,
                fecha_ingreso=?, tiene_seguro=?, dias_vacaciones_tomados=?
            WHERE id=?
        """, (d['nombre'].strip(), d.get('tienda_id') or None,
              float(d['sueldo']), d['fecha_ingreso'],
              1 if d.get('tiene_seguro') else 0,
              int(d.get('dias_vacaciones_tomados', 0)), pid))
        db.commit(); return jsonify({'ok': True})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


@app.route('/api/promotores/<int:pid>', methods=['DELETE'])
def del_promotor(pid):
    db = get_db()
    try:
        db.execute("DELETE FROM promotores WHERE id=?", (pid,))
        db.commit(); return jsonify({'ok': True})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


# ── SEMANAS Y GASTOS ──────────────────────────────────────────────────────────

@app.route('/gastos')
def gastos():
    db      = get_db()
    semanas = ql(db.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC").fetchall())
    db.close()
    return render_template('gastos.html', semanas=semanas)


@app.route('/api/semanas', methods=['GET'])
def list_semanas():
    db   = get_db()
    rows = ql(db.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC").fetchall())
    db.close()
    return jsonify(rows)


@app.route('/api/semanas', methods=['POST'])
def add_semana():
    d  = request.json
    db = get_db()
    try:
        cur = db.execute("INSERT INTO semanas (fecha_inicio, fecha_fin) VALUES (?,?)",
                         (d['fecha_inicio'], d['fecha_fin']))
        db.commit(); return jsonify({'ok': True, 'id': cur.lastrowid})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        db.close()


@app.route('/api/gastos/generar', methods=['POST'])
def generar_gastos():
    semana_id = int(request.json['semana_id'])
    db = get_db()
    try:
        proms = ql(db.execute("SELECT * FROM promotores").fetchall())

        com_rows = ql(db.execute("""
            SELECT st.tienda_id, SUM(pt.comision) AS total
            FROM pagos_telcel pt
            JOIN subclaves_telcel st ON st.subclave = pt.subclave
            WHERE pt.semana_id = ?
            GROUP BY st.tienda_id
        """, (semana_id,)).fetchall())
        com_tienda = {r['tienda_id']: float(r['total'] or 0) for r in com_rows}

        cnt_rows = ql(db.execute("""
            SELECT tienda_id, COUNT(*) AS cnt FROM promotores
            WHERE tienda_id IS NOT NULL GROUP BY tienda_id
        """).fetchall())
        cnt_tienda = {r['tienda_id']: int(r['cnt']) for r in cnt_rows}

        for p in proms:
            p['tiene_seguro'] = bool(p['tiene_seguro'])
            g   = calcular_gasto_promotor(p)
            tid = p['tienda_id']
            cnt = cnt_tienda.get(tid, 1) or 1
            com = round(com_tienda.get(tid, 0) / cnt, 2)
            db.execute("""
                INSERT INTO gastos_semana
                    (semana_id, promotor_id, sueldo_semanal, comisiones, seguro, isn,
                     impuestos, gastos_indirectos, fondo_contingencia,
                     aguinaldo, vacaciones, prima_vacacional, total)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(semana_id, promotor_id) DO UPDATE SET
                    sueldo_semanal=excluded.sueldo_semanal,
                    comisiones=excluded.comisiones,
                    seguro=excluded.seguro,
                    isn=excluded.isn,
                    impuestos=excluded.impuestos,
                    gastos_indirectos=excluded.gastos_indirectos,
                    fondo_contingencia=excluded.fondo_contingencia,
                    aguinaldo=excluded.aguinaldo,
                    vacaciones=excluded.vacaciones,
                    prima_vacacional=excluded.prima_vacacional,
                    total=excluded.total
            """, (semana_id, p['id'], g['sueldo_semanal'], com, g['seguro'],
                  g['isn'], g['impuestos'], g['gastos_indirectos'],
                  g['fondo_contingencia'], g['aguinaldo'],
                  g['vacaciones'], g['prima_vacacional'], g['total']))

        db.commit()
        return jsonify({'ok': True, 'promotores': len(proms)})
    except Exception as e:
        db.rollback(); return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        db.close()


@app.route('/api/gastos/<int:semana_id>')
def get_gastos(semana_id):
    db   = get_db()
    rows = ql(db.execute("""
        SELECT t.cadena, t.nombre AS tienda,
               COUNT(gs.id)               AS promotores,
               SUM(gs.sueldo_semanal)     AS sueldos,
               SUM(gs.comisiones)         AS comisiones,
               SUM(gs.seguro)             AS seguro,
               SUM(gs.isn)                AS isn,
               SUM(gs.impuestos)          AS impuestos,
               SUM(gs.aguinaldo)          AS aguinaldo,
               SUM(gs.vacaciones)         AS vacaciones,
               SUM(gs.prima_vacacional)   AS prima_vacacional,
               SUM(gs.gastos_indirectos)  AS gastos_indirectos,
               SUM(gs.fondo_contingencia) AS fondo_contingencia,
               SUM(gs.total)              AS total
        FROM gastos_semana gs
        JOIN promotores p ON p.id = gs.promotor_id
        JOIN tiendas t ON t.id = p.tienda_id
        WHERE gs.semana_id = ?
        GROUP BY t.id, t.cadena, t.nombre
        ORDER BY t.cadena, t.nombre
    """, (semana_id,)).fetchall())
    db.close()
    for r in rows:
        for k, v in r.items():
            if k not in ('cadena', 'tienda') and v is not None:
                try: r[k] = round(float(v), 2)
                except: pass
    return jsonify(rows)


# ── TELCEL ────────────────────────────────────────────────────────────────────

@app.route('/telcel')
def telcel():
    db      = get_db()
    semanas = ql(db.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC").fetchall())
    db.close()
    return render_template('telcel.html', semanas=semanas)


@app.route('/api/telcel/upload', methods=['POST'])
def upload_telcel():
    semana_id = request.form.get('semana_id')
    if not semana_id:
        return jsonify({'ok': False, 'error': 'Selecciona una semana'}), 400
    f = request.files.get('archivo')
    if not f:
        return jsonify({'ok': False, 'error': 'No se recibió archivo'}), 400
    try:
        wb      = openpyxl.load_workbook(BytesIO(f.read()), data_only=True)
        ws      = wb.active
        headers = [str(c.value).strip().lower() if c.value is not None else ''
                   for c in next(ws.iter_rows(min_row=1, max_row=1))]

        def col_idx(*names):
            for n in names:
                if n in headers: return headers.index(n)
            return None

        idx_sub  = col_idx('subclave', 'fzavta', 'sub_clave', 'clave')
        idx_prod = col_idx('producto', 'product', 'descripcion', 'descripción')
        idx_com  = col_idx('comis', 'comision', 'comisión', 'comisiones', 'importe')

        if idx_sub is None or idx_com is None:
            return jsonify({'ok': False,
                            'error': f'Columnas no encontradas. Encabezados detectados: {headers}'}), 400

        rows_data = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            sub  = str(row[idx_sub]).strip()  if row[idx_sub]  is not None else ''
            prod = str(row[idx_prod]).strip() if idx_prod is not None and row[idx_prod] is not None else ''
            try:   com = float(row[idx_com] or 0)
            except: com = 0.0
            if sub and sub.lower() not in ('none', 'nan', ''):
                rows_data.append((sub, prod, com))

        if not rows_data:
            return jsonify({'ok': False, 'error': 'El archivo no contiene datos válidos'}), 400

        db = get_db()
        db.execute("DELETE FROM pagos_telcel WHERE semana_id=?", (semana_id,))
        db.executemany(
            "INSERT INTO pagos_telcel (semana_id, subclave, producto, comision) VALUES (?,?,?,?)",
            [(semana_id, r[0], r[1], r[2]) for r in rows_data])
        db.commit()

        resumen = ql(db.execute("""
            SELECT pt.subclave,
                   COALESCE(t.nombre, 'Sin tienda asignada') AS tienda,
                   COALESCE(t.cadena, '')                    AS cadena,
                   SUM(pt.comision)                          AS total
            FROM pagos_telcel pt
            LEFT JOIN subclaves_telcel st ON st.subclave = pt.subclave
            LEFT JOIN tiendas t ON t.id = st.tienda_id
            WHERE pt.semana_id = ?
            GROUP BY pt.subclave, t.nombre, t.cadena
            ORDER BY total DESC
        """, (semana_id,)).fetchall())
        db.close()
        for r in resumen:
            r['total'] = round(float(r['total'] or 0), 2)
        return jsonify({'ok': True, 'filas': len(rows_data), 'resumen': resumen})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── RESUMEN ───────────────────────────────────────────────────────────────────

@app.route('/resumen')
def resumen():
    db      = get_db()
    semanas = ql(db.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC").fetchall())
    db.close()
    return render_template('resumen.html', semanas=semanas)


@app.route('/api/resumen/<int:semana_id>')
def get_resumen(semana_id):
    db   = get_db()
    gast = ql(db.execute("""
        SELECT t.id, t.cadena, t.nombre AS tienda,
               SUM(gs.total) AS gastos, COUNT(gs.id) AS promotores
        FROM gastos_semana gs
        JOIN promotores p ON p.id = gs.promotor_id
        JOIN tiendas t ON t.id = p.tienda_id
        WHERE gs.semana_id = ?
        GROUP BY t.id, t.cadena, t.nombre
    """, (semana_id,)).fetchall())

    result = {}
    for r in gast:
        result[r['id']] = {
            'id': r['id'], 'cadena': r['cadena'], 'tienda': r['tienda'],
            'gastos': round(float(r['gastos'] or 0), 2),
            'promotores': int(r['promotores']), 'ingresos': 0.0
        }

    ing = ql(db.execute("""
        SELECT st.tienda_id, t.cadena, t.nombre AS tienda,
               SUM(pt.comision) AS ingresos
        FROM pagos_telcel pt
        JOIN subclaves_telcel st ON st.subclave = pt.subclave
        JOIN tiendas t ON t.id = st.tienda_id
        WHERE pt.semana_id = ?
        GROUP BY st.tienda_id, t.cadena, t.nombre
    """, (semana_id,)).fetchall())
    db.close()

    for r in ing:
        tid = r['tienda_id']
        v   = round(float(r['ingresos'] or 0), 2)
        if tid in result:
            result[tid]['ingresos'] = v
        else:
            result[tid] = {'id': tid, 'cadena': r['cadena'], 'tienda': r['tienda'],
                           'gastos': 0.0, 'promotores': 0, 'ingresos': v}

    out = sorted(result.values(), key=lambda x: (x['cadena'], x['tienda']))
    for r in out:
        r['utilidad'] = round(r['ingresos'] - r['gastos'], 2)
    return jsonify(out)


@app.route('/')
def index():
    return redirect('/gastos')


# ── Arranque ──────────────────────────────────────────────────────────────────

init_db()

if __name__ == '__main__':
    print()
    print("=" * 52)
    print("  gastos-cadenas  —  modo LOCAL (SQLite)")
    print("  Abre en tu navegador: http://127.0.0.1:5000")
    print("=" * 52)
    print()
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)
