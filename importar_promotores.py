"""
importar_promotores.py
Columnas del Excel:
  A=Cadena  B=Tienda  C=ID (ej: C10-CAROLINA)  D=Nombre
  E=Fecha de ingreso  F=Seguro IMSS (SI/NO)  G=Comisiones  H=Sueldo
"""
import sqlite3
import openpyxl
from datetime import datetime

EXCEL_PATH = r'C:\Users\Administrador\Desktop\TIENDAS\promotores.xlsx'
DB_PATH    = r'C:\Users\Administrador\Desktop\gastos-cadenas\local_gastos.db'

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
db.execute("PRAGMA foreign_keys=ON")

# Índice de tiendas: (cadena_lower, nombre_lower) -> id  y  nombre_lower -> id
tiendas_all = db.execute("SELECT id, cadena, nombre FROM tiendas").fetchall()
idx_exacto  = {(t['cadena'].strip().lower(), t['nombre'].strip().lower()): t['id'] for t in tiendas_all}
idx_nombre  = {t['nombre'].strip().lower(): t['id'] for t in tiendas_all}

def buscar_tienda(cadena_excel, nombre_excel):
    c = cadena_excel.strip().lower()
    n = nombre_excel.strip().lower()
    # 1. Coincidencia exacta cadena+nombre
    if (c, n) in idx_exacto:
        return idx_exacto[(c, n)]
    # 2. Coincidencia exacta solo por nombre
    if n in idx_nombre:
        return idx_nombre[n]
    # 3. Coincidencia parcial: nombre de la DB termina en el del Excel
    for (dc, dn), tid in idx_exacto.items():
        if n and dn.endswith(n):
            if not c or dc == c:
                return tid
    return None

wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
ws = wb.active

insertados = actualizados = omitidos = 0
sin_tienda = []

for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
    cadena_val  = str(row[0]).strip() if row[0] is not None else ''
    tienda_val  = str(row[1]).strip() if row[1] is not None else ''
    promotor_id = str(row[2]).strip() if row[2] is not None else ''
    nombre      = str(row[3]).strip() if row[3] is not None else ''
    fecha_raw   = row[4]
    seguro_raw    = str(row[5]).strip().upper() if row[5] is not None else ''
    comisiones_raw = row[6]
    sueldo_raw    = row[7]

    # Saltar filas vacías o encabezados repetidos
    if not promotor_id or not nombre or promotor_id.upper() in ('ID', 'CLAVE', 'NONE', ''):
        omitidos += 1
        continue

    # Fecha
    if isinstance(fecha_raw, datetime):
        fecha_str = fecha_raw.strftime('%Y-%m-%d')
    elif fecha_raw:
        try:
            fecha_str = str(fecha_raw).strip()[:10]
        except Exception:
            fecha_str = None
    else:
        fecha_str = None

    tiene_seguro = 1 if seguro_raw in ('SI', 'SÍ', 'S', 'YES', '1', 'TRUE') else 0

    try:
        comisiones = float(comisiones_raw or 0)
    except (TypeError, ValueError):
        comisiones = 0.0

    try:
        sueldo = float(sueldo_raw or 0)
    except (TypeError, ValueError):
        sueldo = 0.0

    tienda_id = buscar_tienda(cadena_val, tienda_val)
    if tienda_id is None and tienda_val:
        sin_tienda.append(f'  Fila {i}: {promotor_id} — tienda "{cadena_val}/{tienda_val}" no encontrada')

    existing = db.execute(
        "SELECT id FROM promotores WHERE promotor_id=? LIMIT 1", (promotor_id,)
    ).fetchone()

    if existing:
        db.execute("""
            UPDATE promotores
               SET nombre=?, tienda_id=?, sueldo=?, comisiones=?, fecha_ingreso=?, tiene_seguro=?
             WHERE id=?
        """, (nombre, tienda_id, sueldo, comisiones, fecha_str, tiene_seguro, existing['id']))
        actualizados += 1
    else:
        db.execute("""
            INSERT INTO promotores
                (promotor_id, nombre, tienda_id, sueldo, comisiones, fecha_ingreso,
                 tiene_seguro, dias_vacaciones_tomados)
            VALUES (?,?,?,?,?,?,?,0)
        """, (promotor_id, nombre, tienda_id, sueldo, comisiones, fecha_str, tiene_seguro))
        insertados += 1

db.commit()
total = db.execute("SELECT COUNT(*) FROM promotores").fetchone()[0]
db.close()

print(f"Insertados : {insertados}")
print(f"Actualizados: {actualizados}")
print(f"Omitidos   : {omitidos}  (filas vacías / encabezados)")
print(f"Total en DB: {total}")
if sin_tienda:
    print(f"\nSin tienda asignada ({len(sin_tienda)}):")
    for m in sin_tienda[:20]:
        print(m)
