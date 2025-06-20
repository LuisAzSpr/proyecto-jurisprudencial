from fastapi import FastAPI, HTTPException, Query
from google.cloud import storage
from datetime import timedelta
import psycopg2
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
from typing import Optional, List

load_dotenv()

app = FastAPI()


LISTA_ORGANO_PERMITIDOS = [
    "SEGUNDA SALA DE DERECHO CONSTITUCIONAL Y SOCIAL TRANSITORIA",
    "CUARTA SALA DE DERECHO CONSTITUCIONAL Y SOCIAL TRANSITORIA"
]

# Configuración de Google Cloud Storage
credentials = service_account.Credentials.from_service_account_file('credenciales.json')
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("automatizacion-casillero")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB-HOST"),
        port=os.getenv("DB-PORT"),
        dbname=os.getenv("DB-NAME"),
        user=os.getenv("USERNAME-DB"),
        password=os.getenv("PASSWORD-DB"),
        options='-c statement_timeout=10000'  # 10 segundos
    )

@app.get("/descargar/{ndetalle}")
def generar_url(ndetalle: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT url FROM sentencias_y_autos WHERE ndetalle = %s", (ndetalle,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result or result[0] is None:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    blob_name = result[0]
    blob = bucket.blob(blob_name)

    signed_url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=180),
        method="GET"
    )

    return {"url": signed_url}


@app.get("/filters")
def obtener_filtros():
    conn = get_db_connection()
    cur = conn.cursor()

    #cur.execute("SELECT DISTINCT organo_detalle FROM sentencias_y_autos WHERE organo_detalle IS NOT NULL;")

    cur.execute("""
        SELECT DISTINCT j.nombre_juez
        FROM jueces j
        JOIN sentencias_jueces sj ON sj.codigo = j.codigo
        JOIN sentencias_y_autos s ON s.ndetalle = sj.ndetalle
        WHERE j.nombre_juez IS NOT NULL;
    """)
    lista_juez = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()
    return {
        "organo_detalle": sorted(LISTA_ORGANO_PERMITIDOS),
        "nombre_juez": sorted(lista_juez)
    }

@app.get("/statistics")
def estadisticas(
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    lista_jueces: Optional[List[str]] = Query(None)
):
    conn = get_db_connection()
    cur = conn.cursor()

    filtros_where = []
    params: List = []

    if fecha_desde:
        filtros_where.append("s.fecha_resolucion >= %s")
        params.append(fecha_desde)
    if fecha_hasta:
        filtros_where.append("s.fecha_resolucion <= %s")
        params.append(fecha_hasta)
    if lista_jueces:
        filtros_where.append("j.nombre_juez = ANY(%s)")
        params.append(lista_jueces)

    from_clause = """
        sentencias_y_autos s
        LEFT JOIN sentencias_jueces sj ON sj.ndetalle = s.ndetalle
        LEFT JOIN jueces j ON j.codigo = sj.codigo
    """

    where_sql = ""
    if filtros_where:
        where_sql = "WHERE " + " AND ".join(filtros_where)

    select_query = f"""
        SELECT
            j.nombre_juez,
            COUNT(*) AS total,
            COUNT(*) - COUNT(s.url) AS nulos
        FROM 
            {from_clause}
            {where_sql}
        GROUP BY
            j.codigo, j.nombre_juez
        ORDER BY
            j.nombre_juez ASC;
    """

    cur.execute(select_query, tuple(params))
    filas = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"juez": row[0], "total": row[1], "nulos": row[2]}
        for row in filas
    ]


@app.get("/search")
def buscar_sentencias(
    organo_detalle: Optional[str] = Query(None),
    nombre_juez: Optional[str] = Query(None),
    fecha_desde: Optional[str] = Query(None),
    fecha_hasta: Optional[str] = Query(None),
    clasificacion_fundada: Optional[bool] = False,
    limit: int = 100,
    offset: int = 0
):
    conn = get_db_connection()
    cur = conn.cursor()

    filtros_where = []
    params: List = []

    if organo_detalle:
        if organo_detalle not in LISTA_ORGANO_PERMITIDOS:
            raise HTTPException(status_code=400, detail="Órgano no permitido.")
        filtros_where.append("s.organo_detalle = ANY(%s)")
        params.append(LISTA_ORGANO_PERMITIDOS)
    if nombre_juez:
        filtros_where.append("j.nombre_juez = %s")
        params.append(nombre_juez)
    if fecha_desde:
        filtros_where.append("s.fecha_resolucion >= %s")
        params.append(fecha_desde)
    if fecha_hasta:
        filtros_where.append("s.fecha_resolucion <= %s")
        params.append(fecha_hasta)
    if clasificacion_fundada:
        filtros_where.append("s.clasificacion IN (%s, %s)")
        params.extend(["fundado", "infundado"])

    from_clause = "sentencias_y_autos s LEFT JOIN sentencias_jueces sj ON sj.ndetalle = s.ndetalle LEFT JOIN jueces j ON j.codigo = sj.codigo"

    where_sql = ""
    filtros_where.append("s.url IS NOT NULL")
    if filtros_where:
        where_sql = "WHERE " + " AND ".join(filtros_where)

    count_query = f"""
        SELECT COUNT(DISTINCT s.ndetalle)
        FROM {from_clause}
        {where_sql};
    """
    cur.execute(count_query, tuple(params))
    total_count = cur.fetchone()[0]

    select_query = f"""
        SELECT DISTINCT s.ndetalle, s.url, s.clasificacion, s.fecha_resolucion
        FROM {from_clause}
        {where_sql}
        ORDER BY s.fecha_resolucion DESC
        LIMIT %s OFFSET %s;
    """
    cur.execute(select_query, tuple(params + [limit, offset]))
    filas = cur.fetchall()
    items = [
        {"ndetalle": row[0], "url": row[1], "clasificacion": row[2]}
        for row in filas
    ]

    cur.close()
    conn.close()

    return {
        "total_count": total_count,
        "items": items
    }

