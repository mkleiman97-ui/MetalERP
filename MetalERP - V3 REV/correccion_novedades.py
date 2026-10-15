#!/usr/bin/env python3
"""
Corrige el rendimiento de novedades de producción que quedaron mal
calculadas por un bug ya corregido en server.py.

CONTEXTO DEL BUG:
  tiempo_real     = tiempo EFECTIVAMENTE trabajado (ya neto).
  tiempo_perdido  = tiempo APARTE que no se trabajó (turno = tiempo_real + tiempo_perdido).
  El sistema calculaba mal el rendimiento restando tiempo_perdido de
  tiempo_real (t_productivo = tiempo_real - tiempo_perdido), como si
  tiempo_perdido estuviera incluido dentro de tiempo_real. Como no lo
  está, esa resta daba valores chicos o negativos y el rendimiento se
  disparaba a números absurdos (ej: 170624%, o el tope de emergencia 999.9%).

CORRECCIÓN:
  El rendimiento se recalcula usando tiempo_real SOLO (sin restar nada),
  que es la misma fórmula que ya quedó corregida en server.py.

USO:
  python3 fix_novedades.py --db "database\\metalerp.db"            -> DRY-RUN (solo muestra)
  python3 fix_novedades.py --db "database\\metalerp.db" --apply    -> aplica la corrección
"""
import sqlite3
import argparse
import sys

def find_db_path(explicit):
    if explicit:
        return explicit
    for candidate in ("produccion.db", "database.db", "app.db", "database/metalerp.db"):
        try:
            with open(candidate):
                return candidate
        except FileNotFoundError:
            continue
    return "database/metalerp.db"  # fallback (ruta real usada por server.py)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=None, help="Ruta a la base de datos sqlite (ej: database\\metalerp.db)")
    parser.add_argument("--apply", action="store_true", help="Aplicar los cambios (por defecto solo simula)")
    args = parser.parse_args()

    db_path = find_db_path(args.db)
    print(f"Usando base de datos: {db_path}\n")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    tablas = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    if "novedades_produccion" not in tablas:
        print("ERROR: esta base de datos no tiene la tabla 'novedades_produccion'.")
        print("Tablas encontradas en este archivo:")
        for t in tablas:
            print(f"  - {t}")
        print("\nProbablemente le estás apuntando al archivo .db equivocado.")
        print("La ruta que usa el servidor es: database/metalerp.db (relativa a la carpeta del proyecto).")
        sys.exit(1)

    rows = cur.execute("""
        SELECT n.id, n.numero, n.cantidad_producida, n.tiempo_real_min, n.tiempo_perdido,
               n.rendimiento_pct, n.desvio,
               COALESCE(NULLIF(oo.tiempo_ciclo_min,0), NULLIF(po.tiempo_ciclo_min,0), 1) AS std_ciclo
        FROM novedades_produccion n
        JOIN orden_operaciones oo ON oo.id = n.orden_operacion_id
        LEFT JOIN proceso_operaciones po ON po.id = oo.proceso_op_id
        WHERE n.tiempo_real_min > 0
        ORDER BY n.id
    """).fetchall()

    if not rows:
        print("No se encontraron novedades para revisar.")
        return

    updates = []
    afectadas = 0
    for r in rows:
        std_ciclo = r["std_ciclo"] or 1
        t_real = r["tiempo_real_min"]
        qty = r["cantidad_producida"]

        # Fórmula corregida: rendimiento en base a tiempo_real solo (sin restar tiempo_perdido)
        t_productivo = max(t_real, 0.1)
        rendimiento_nuevo = round((qty / t_productivo) / (1 / std_ciclo) * 100, 1)
        rendimiento_nuevo = min(rendimiento_nuevo, 999.9)
        desvio_nuevo = 1 if rendimiento_nuevo < 80 else 0

        if abs((r["rendimiento_pct"] or 0) - rendimiento_nuevo) < 0.05 and r["desvio"] == desvio_nuevo:
            continue

        afectadas += 1
        print(f"  Novedad {r['numero']} (id={r['id']})")
        print(f"    tiempo_real={t_real} min | tiempo_perdido={r['tiempo_perdido']} min")
        print(f"    rendimiento actual : {r['rendimiento_pct']}%  (desvio={r['desvio']})")
        print(f"    rendimiento nuevo  : {rendimiento_nuevo}%  (desvio={desvio_nuevo})")
        print()
        updates.append((rendimiento_nuevo, desvio_nuevo, r["id"]))

    if afectadas == 0:
        print("No se encontraron novedades cuyo rendimiento cambie con la fórmula corregida.")
        return

    print(f"Total de novedades a corregir: {afectadas}\n")

    if not args.apply:
        print("Modo DRY-RUN: no se modificó nada. Corré con --apply para aplicar estos cambios.")
        return

    cur.executemany(
        "UPDATE novedades_produccion SET rendimiento_pct=?, desvio=? WHERE id=?",
        updates
    )
    conn.commit()
    print(f"✔ Se actualizaron {len(updates)} novedad(es).")

if __name__ == "__main__":
    main()