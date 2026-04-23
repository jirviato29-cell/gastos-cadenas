"""
Importa TIENDAS.xlsx a la base de datos local SQLite (local_gastos.db).
Estructura del Excel:
  Col A = CADENA, Col B = TIENDA, Col C = CLAVE PRINCIPAL, Col D = SUBCLAVES
  Cuando A/B/C son None -> la fila es una subclave adicional de la tienda anterior.
"""
import sqlite3
import openpyxl

XLSX_PATH = 'TIENDAS.xlsx'
DB_PATH   = 'local_gastos.db'

wb = openpyxl.load_workbook(XLSX_PATH)
ws = wb.active

# Agrupar filas: {(cadena, nombre, clave_principal): [subclave1, subclave2, ...]}
tiendas = {}
orden   = []
current = None

for row in ws.iter_rows(min_row=2, values_only=True):
    cadena  = str(row[0]).strip() if row[0] else None
    nombre  = str(row[1]).strip() if row[1] else None
    clave   = str(row[2]).strip() if row[2] else None
    sub     = str(row[3]).strip() if row[3] else None

    if cadena and nombre and clave:
        current = (cadena, nombre, clave)
        if current not in tiendas:
            tiendas[current] = []
            orden.append(current)
        if clave and clave not in tiendas[current]:
            tiendas[current].append(clave)
    elif current and sub:
        if sub not in tiendas[current]:
            tiendas[current].append(sub)

# Importar a SQLite
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

tiendas_nuevas = 0
subclaves_nuevas = 0

for (cadena, nombre, clave_principal) in orden:
    # Upsert tienda
    existing = db.execute(
        "SELECT id FROM tiendas WHERE clave_principal=?", (clave_principal,)
    ).fetchone()

    if existing:
        tienda_id = existing['id']
    else:
        cur = db.execute(
            "INSERT INTO tiendas (cadena, nombre, clave_principal) VALUES (?,?,?)",
            (cadena, nombre, clave_principal)
        )
        tienda_id = cur.lastrowid
        tiendas_nuevas += 1

    # Insertar subclaves que no existan
    for sub in tiendas[(cadena, nombre, clave_principal)]:
        exists = db.execute(
            "SELECT 1 FROM subclaves_telcel WHERE subclave=?", (sub,)
        ).fetchone()
        if not exists:
            db.execute(
                "INSERT INTO subclaves_telcel (tienda_id, subclave) VALUES (?,?)",
                (tienda_id, sub)
            )
            subclaves_nuevas += 1

db.commit()

total_tiendas   = db.execute("SELECT COUNT(*) FROM tiendas").fetchone()[0]
total_subclaves = db.execute("SELECT COUNT(*) FROM subclaves_telcel").fetchone()[0]
db.close()

print(f"Tiendas nuevas insertadas : {tiendas_nuevas}")
print(f"Subclaves nuevas insertadas: {subclaves_nuevas}")
print(f"-----------------------------")
print(f"Total tiendas en DB        : {total_tiendas}")
print(f"Total subclaves en DB      : {total_subclaves}")
