from flask import Flask, render_template, request, jsonify, redirect
import os
from datetime import date, datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import openpyxl
from io import BytesIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'gastosCadenas2026'

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/gastos"
)

connection_pool = pool.ThreadedConnectionPool(2, 10, DATABASE_URL)


def get_conn():
    conn = connection_pool.getconn()
    conn.cursor_factory = RealDictCursor
    return conn


def release_conn(conn):
    connection_pool.putconn(conn)


# ── Cálculos ─────────────────────────────────────────────────────────────────

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
    sd = sueldo / 30
    ss = round(sueldo / 4.33, 2)

    fi = p['fecha_ingreso']
    if isinstance(fi, str):
        fi = datetime.strptime(fi, '%Y-%m-%d').date()
    anos = max(0, (date.today() - fi).days / 365.25)
    dias_vac = dias_vacaciones_ley(int(anos))

    aguinaldo       = round((sd * 15) / 52, 2)
    vacaciones      = round((dias_vac * sd) / 52, 2)
    prima_vacacional = round(vacaciones * 0.25, 2)
    seguro           = 696.42 if p.get('tiene_seguro') else 0.0
    isn              = 61.54
    impuestos        = 417.05
    gastos_indirectos   = 274.0
    fondo_contingencia  = 27.0
    total = round(ss + seguro + isn + impuestos + gastos_indirectos +
                  fondo_contingencia + aguinaldo + vacaciones + prima_vacacional, 2)
    return dict(sueldo_semanal=ss, seguro=seguro, isn=isn, impuestos=impuestos,
                gastos_indirectos=gastos_indirectos, fondo_contingencia=fondo_contingencia,
                aguinaldo=aguinaldo, vacaciones=vacaciones,
                prima_vacacional=prima_vacacional, total=total,
                anos=round(anos, 1), dias_vac=dias_vac)


# ── Init DB ───────────────────────────────────────────────────────────────────

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT pg_advisory_lock(88888)")
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tiendas (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                cadena TEXT NOT NULL,
                clave_principal TEXT NOT NULL UNIQUE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS promotores (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                tienda_id INTEGER REFERENCES tiendas(id),
                sueldo FLOAT NOT NULL DEFAULT 0,
                fecha_ingreso DATE NOT NULL,
                tiene_seguro BOOLEAN DEFAULT FALSE,
                dias_vacaciones_tomados INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subclaves_telcel (
                id SERIAL PRIMARY KEY,
                tienda_id INTEGER REFERENCES tiendas(id) ON DELETE CASCADE,
                subclave TEXT NOT NULL,
                UNIQUE(tienda_id, subclave)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS semanas (
                id SERIAL PRIMARY KEY,
                fecha_inicio DATE NOT NULL,
                fecha_fin DATE NOT NULL,
                UNIQUE(fecha_inicio, fecha_fin)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gastos_semana (
                id SERIAL PRIMARY KEY,
                semana_id INTEGER REFERENCES semanas(id) ON DELETE CASCADE,
                promotor_id INTEGER REFERENCES promotores(id) ON DELETE CASCADE,
                sueldo_semanal FLOAT DEFAULT 0,
                comisiones FLOAT DEFAULT 0,
                seguro FLOAT DEFAULT 0,
                isn FLOAT DEFAULT 0,
                impuestos FLOAT DEFAULT 0,
                gastos_indirectos FLOAT DEFAULT 0,
                fondo_contingencia FLOAT DEFAULT 0,
                aguinaldo FLOAT DEFAULT 0,
                vacaciones FLOAT DEFAULT 0,
                prima_vacacional FLOAT DEFAULT 0,
                total FLOAT DEFAULT 0,
                UNIQUE(semana_id, promotor_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pagos_telcel (
                id SERIAL PRIMARY KEY,
                semana_id INTEGER REFERENCES semanas(id) ON DELETE CASCADE,
                subclave TEXT NOT NULL,
                producto TEXT,
                comision FLOAT DEFAULT 0
            )
        """)
        conn.commit()
        print("DB gastos-cadenas OK")
    finally:
        cur.execute("SELECT pg_advisory_unlock(88888)")
        cur.close()
        release_conn(conn)


# ── TIENDAS ───────────────────────────────────────────────────────────────────

@app.route('/tiendas')
def tiendas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.*,
               COALESCE(json_agg(s.subclave ORDER BY s.id)
                        FILTER (WHERE s.id IS NOT NULL), '[]') AS subclaves
        FROM tiendas t
        LEFT JOIN subclaves_telcel s ON s.tienda_id = t.id
        GROUP BY t.id ORDER BY t.cadena, t.nombre
    """)
    rows = cur.fetchall()
    cur.close()
    release_conn(conn)
    return render_template('tiendas.html', tiendas=rows)


@app.route('/api/tiendas', methods=['POST'])
def add_tienda():
    d = request.json
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO tiendas (nombre, cadena, clave_principal) VALUES (%s,%s,%s) RETURNING id",
            (d['nombre'].strip(), d['cadena'].strip(), d['clave_principal'].strip()))
        tid = cur.fetchone()['id']
        for sub in d.get('subclaves', []):
            if sub.strip():
                cur.execute(
                    "INSERT INTO subclaves_telcel (tienda_id, subclave) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (tid, sub.strip()))
        conn.commit()
        return jsonify({'ok': True, 'id': tid})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/tiendas/<int:tid>', methods=['PUT'])
def edit_tienda(tid):
    d = request.json
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE tiendas SET nombre=%s, cadena=%s, clave_principal=%s WHERE id=%s",
            (d['nombre'].strip(), d['cadena'].strip(), d['clave_principal'].strip(), tid))
        cur.execute("DELETE FROM subclaves_telcel WHERE tienda_id=%s", (tid,))
        for sub in d.get('subclaves', []):
            if sub.strip():
                cur.execute(
                    "INSERT INTO subclaves_telcel (tienda_id, subclave) VALUES (%s,%s)",
                    (tid, sub.strip()))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/tiendas/<int:tid>', methods=['DELETE'])
def del_tienda(tid):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM tiendas WHERE id=%s", (tid,))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


# ── PROMOTORES ────────────────────────────────────────────────────────────────

@app.route('/promotores')
def promotores():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*, t.nombre AS tienda_nombre, t.cadena
        FROM promotores p LEFT JOIN tiendas t ON t.id = p.tienda_id
        ORDER BY t.cadena NULLS LAST, t.nombre NULLS LAST, p.nombre
    """)
    proms = cur.fetchall()
    cur.execute("SELECT id, nombre, cadena FROM tiendas ORDER BY cadena, nombre")
    tiendas = cur.fetchall()
    cur.close()
    release_conn(conn)

    # Enriquecer con cálculos
    resultado = []
    for p in proms:
        p = dict(p)
        g = calcular_gasto_promotor(p)
        p.update(g)
        if p['fecha_ingreso']:
            p['fecha_ingreso_str'] = p['fecha_ingreso'].strftime('%Y-%m-%d')
        resultado.append(p)

    return render_template('promotores.html', promotores=resultado, tiendas=tiendas)


@app.route('/api/promotores', methods=['POST'])
def add_promotor():
    d = request.json
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO promotores (nombre, tienda_id, sueldo, fecha_ingreso,
                                    tiene_seguro, dias_vacaciones_tomados)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
        """, (d['nombre'].strip(), d.get('tienda_id') or None,
              float(d['sueldo']), d['fecha_ingreso'],
              bool(d.get('tiene_seguro', False)),
              int(d.get('dias_vacaciones_tomados', 0))))
        pid = cur.fetchone()['id']
        conn.commit()
        return jsonify({'ok': True, 'id': pid})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/promotores/<int:pid>', methods=['PUT'])
def edit_promotor(pid):
    d = request.json
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE promotores SET nombre=%s, tienda_id=%s, sueldo=%s,
                fecha_ingreso=%s, tiene_seguro=%s, dias_vacaciones_tomados=%s
            WHERE id=%s
        """, (d['nombre'].strip(), d.get('tienda_id') or None,
              float(d['sueldo']), d['fecha_ingreso'],
              bool(d.get('tiene_seguro', False)),
              int(d.get('dias_vacaciones_tomados', 0)), pid))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/promotores/<int:pid>', methods=['DELETE'])
def del_promotor(pid):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM promotores WHERE id=%s", (pid,))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


# ── GASTOS ────────────────────────────────────────────────────────────────────

@app.route('/gastos')
def gastos():
    return render_template('gastos.html')


@app.route('/api/semanas', methods=['POST'])
def add_semana():
    d = request.json
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO semanas (fecha_inicio, fecha_fin) VALUES (%s,%s) RETURNING id",
            (d['fecha_inicio'], d['fecha_fin']))
        sid = cur.fetchone()['id']
        conn.commit()
        return jsonify({'ok': True, 'id': sid})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 400
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/semanas', methods=['GET'])
def list_semanas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC")
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r['fecha_inicio'] = str(r['fecha_inicio'])
        r['fecha_fin'] = str(r['fecha_fin'])
    cur.close()
    release_conn(conn)
    return jsonify(rows)


@app.route('/api/gastos/generar', methods=['POST'])
def generar_gastos():
    semana_id = int(request.json['semana_id'])
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM promotores")
        proms = cur.fetchall()

        # Comisiones Telcel totales por tienda para esta semana
        cur.execute("""
            SELECT st.tienda_id, SUM(pt.comision) AS total
            FROM pagos_telcel pt
            JOIN subclaves_telcel st ON st.subclave = pt.subclave
            WHERE pt.semana_id = %s
            GROUP BY st.tienda_id
        """, (semana_id,))
        com_tienda = {r['tienda_id']: float(r['total'] or 0) for r in cur.fetchall()}

        # Promotores por tienda (para repartir comisiones proporcionalmente)
        cur.execute("""
            SELECT tienda_id, COUNT(*) AS cnt FROM promotores
            WHERE tienda_id IS NOT NULL GROUP BY tienda_id
        """)
        cnt_tienda = {r['tienda_id']: int(r['cnt']) for r in cur.fetchall()}

        for p in proms:
            g = calcular_gasto_promotor(p)
            tid = p['tienda_id']
            cnt = cnt_tienda.get(tid, 1) or 1
            comisiones = round(com_tienda.get(tid, 0) / cnt, 2)
            cur.execute("""
                INSERT INTO gastos_semana
                    (semana_id, promotor_id, sueldo_semanal, comisiones, seguro, isn,
                     impuestos, gastos_indirectos, fondo_contingencia,
                     aguinaldo, vacaciones, prima_vacacional, total)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (semana_id, promotor_id) DO UPDATE SET
                    sueldo_semanal=EXCLUDED.sueldo_semanal,
                    comisiones=EXCLUDED.comisiones,
                    seguro=EXCLUDED.seguro, isn=EXCLUDED.isn,
                    impuestos=EXCLUDED.impuestos,
                    gastos_indirectos=EXCLUDED.gastos_indirectos,
                    fondo_contingencia=EXCLUDED.fondo_contingencia,
                    aguinaldo=EXCLUDED.aguinaldo, vacaciones=EXCLUDED.vacaciones,
                    prima_vacacional=EXCLUDED.prima_vacacional, total=EXCLUDED.total
            """, (semana_id, p['id'], g['sueldo_semanal'], comisiones, g['seguro'],
                  g['isn'], g['impuestos'], g['gastos_indirectos'],
                  g['fondo_contingencia'], g['aguinaldo'],
                  g['vacaciones'], g['prima_vacacional'], g['total']))

        conn.commit()
        return jsonify({'ok': True, 'promotores': len(proms)})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        cur.close()
        release_conn(conn)


@app.route('/api/gastos/<int:semana_id>')
def get_gastos(semana_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
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
        WHERE gs.semana_id = %s
        GROUP BY t.id, t.cadena, t.nombre
        ORDER BY t.cadena, t.nombre
    """, (semana_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    release_conn(conn)
    # Convertir Decimal/float a float
    for r in rows:
        for k, v in r.items():
            if v is not None and k not in ('cadena', 'tienda'):
                try:
                    r[k] = round(float(v), 2)
                except Exception:
                    pass
    return jsonify(rows)


@app.route('/api/gastos/<int:semana_id>/detalle')
def get_gastos_detalle(semana_id):
    """Gastos por promotor individual para las pestañas de la UI."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT gs.sueldo_semanal, gs.comisiones, gs.seguro, gs.isn,
               gs.impuestos, gs.gastos_indirectos, gs.fondo_contingencia,
               gs.aguinaldo, gs.vacaciones, gs.prima_vacacional, gs.total,
               p.nombre AS promotor,
               t.nombre AS tienda,
               t.cadena
        FROM gastos_semana gs
        JOIN promotores p ON p.id = gs.promotor_id
        JOIN tiendas t ON t.id = p.tienda_id
        WHERE gs.semana_id = %s
        ORDER BY t.cadena, t.nombre, p.nombre
    """, (semana_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    release_conn(conn)
    for r in rows:
        for k, v in r.items():
            if k not in ('promotor', 'tienda', 'cadena') and v is not None:
                try:
                    r[k] = round(float(v), 2)
                except Exception:
                    pass
    return jsonify(rows)


# ── TELCEL ────────────────────────────────────────────────────────────────────

@app.route('/telcel')
def telcel():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC")
    semanas = cur.fetchall()
    cur.close()
    release_conn(conn)
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
        wb = openpyxl.load_workbook(BytesIO(f.read()), data_only=True)
        ws = wb.active
        headers = []
        for c in next(ws.iter_rows(min_row=1, max_row=1)):
            headers.append(str(c.value).strip().lower() if c.value is not None else '')

        def col_idx(*names):
            for n in names:
                if n in headers:
                    return headers.index(n)
            return None

        idx_sub  = col_idx('subclave', 'fzavta', 'sub_clave', 'sub clave', 'clave')
        idx_prod = col_idx('producto', 'product', 'descripcion', 'descripción')
        idx_com  = col_idx('comis', 'comision', 'comisión', 'comisiones', 'commission', 'importe')

        if idx_sub is None or idx_com is None:
            return jsonify({'ok': False,
                            'error': f'Columnas no encontradas. Encabezados: {headers}'}), 400

        rows_data = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            sub  = str(row[idx_sub]).strip()  if row[idx_sub]  is not None else ''
            prod = str(row[idx_prod]).strip() if idx_prod is not None and row[idx_prod] is not None else ''
            try:
                com = float(row[idx_com] or 0)
            except Exception:
                com = 0.0
            if sub and sub.lower() not in ('none', 'nan', ''):
                rows_data.append((sub, prod, com))

        if not rows_data:
            return jsonify({'ok': False, 'error': 'El archivo no contiene datos válidos'}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM pagos_telcel WHERE semana_id=%s", (semana_id,))
        cur.executemany(
            "INSERT INTO pagos_telcel (semana_id, subclave, producto, comision) VALUES (%s,%s,%s,%s)",
            [(semana_id, r[0], r[1], r[2]) for r in rows_data])
        conn.commit()

        cur.execute("""
            SELECT pt.subclave,
                   COALESCE(t.nombre,'Sin tienda asignada') AS tienda,
                   COALESCE(t.cadena,'') AS cadena,
                   SUM(pt.comision) AS total
            FROM pagos_telcel pt
            LEFT JOIN subclaves_telcel st ON st.subclave = pt.subclave
            LEFT JOIN tiendas t ON t.id = st.tienda_id
            WHERE pt.semana_id = %s
            GROUP BY pt.subclave, t.nombre, t.cadena
            ORDER BY total DESC
        """, (semana_id,))
        resumen = [dict(r) for r in cur.fetchall()]
        for r in resumen:
            r['total'] = round(float(r['total'] or 0), 2)
        cur.close()
        release_conn(conn)

        return jsonify({'ok': True, 'filas': len(rows_data), 'resumen': resumen})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── RESUMEN ───────────────────────────────────────────────────────────────────

@app.route('/resumen')
def resumen():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM semanas ORDER BY fecha_inicio DESC")
    semanas = cur.fetchall()
    cur.close()
    release_conn(conn)
    return render_template('resumen.html', semanas=semanas)


@app.route('/api/resumen/<int:semana_id>')
def get_resumen(semana_id):
    conn = get_conn()
    cur = conn.cursor()

    # Gastos por tienda
    cur.execute("""
        SELECT t.id, t.cadena, t.nombre AS tienda,
               SUM(gs.total) AS gastos, COUNT(gs.id) AS promotores
        FROM gastos_semana gs
        JOIN promotores p ON p.id = gs.promotor_id
        JOIN tiendas t ON t.id = p.tienda_id
        WHERE gs.semana_id = %s
        GROUP BY t.id, t.cadena, t.nombre
    """, (semana_id,))
    result = {}
    for r in cur.fetchall():
        result[r['id']] = {
            'id': r['id'], 'cadena': r['cadena'], 'tienda': r['tienda'],
            'gastos': round(float(r['gastos'] or 0), 2),
            'promotores': int(r['promotores']), 'ingresos': 0.0
        }

    # Ingresos Telcel por tienda (via subclaves)
    cur.execute("""
        SELECT st.tienda_id, t.cadena, t.nombre AS tienda, SUM(pt.comision) AS ingresos
        FROM pagos_telcel pt
        JOIN subclaves_telcel st ON st.subclave = pt.subclave
        JOIN tiendas t ON t.id = st.tienda_id
        WHERE pt.semana_id = %s
        GROUP BY st.tienda_id, t.cadena, t.nombre
    """, (semana_id,))
    for r in cur.fetchall():
        tid = r['tienda_id']
        ing = round(float(r['ingresos'] or 0), 2)
        if tid in result:
            result[tid]['ingresos'] = ing
        else:
            result[tid] = {
                'id': tid, 'cadena': r['cadena'], 'tienda': r['tienda'],
                'gastos': 0.0, 'promotores': 0, 'ingresos': ing
            }

    rows = sorted(result.values(), key=lambda x: (x['cadena'], x['tienda']))
    for r in rows:
        r['utilidad'] = round(r['ingresos'] - r['gastos'], 2)

    cur.close()
    release_conn(conn)
    return jsonify(rows)


# ── Root ──────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return redirect('/gastos')


try:
    init_db()
except Exception as e:
    print(f"init_db: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
