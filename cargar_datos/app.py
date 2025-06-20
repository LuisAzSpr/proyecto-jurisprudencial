from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
import psycopg2
import os
import json
import re
from tqdm import tqdm
import logging
import time
from clasificacion import extraer_texto_pdf
from clasificacion import clasificar_archivo_pdf

load_dotenv()


# --------------------- Conexiones --------------------------
# -----------------------------------------------------
def configurar_logger():
    logger = logging.getLogger()
    if not logger.hasHandlers():  # Evitar agregar múltiples handlers
        logging.basicConfig(
            level=logging.INFO,  # Cambia esto a INFO para ocultar los DEBUG
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),  # Salida estándar
                logging.FileHandler("logs.log", mode="a")  # Archivo de logs
            ]
        )
    return logger

logger = configurar_logger()


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB-HOST"),
        port=os.getenv("DB-PORT"),
        dbname=os.getenv("DB-NAME"),
        user=os.getenv("USERNAME-DB"),
        password=os.getenv("PASSWORD-DB"),
        options='-c statement_timeout=10000'  # 10 segundos
    )

credentials = service_account.Credentials.from_service_account_file('credenciales.json')
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("automatizacion-casillero")

# -----------------------------------------------------


# ------------------------- UTILS ---------------------
# -----------------------------------------------------

def obtener_fecha_mas_reciente(prefijo_folder):
    try:
        blobs = bucket.list_blobs(prefix=prefijo_folder)

        # Filtramos y obtenemos la fecha de actualización más reciente
        fechas = [(blob.name, blob.updated) for blob in blobs if not blob.name.endswith('/')]
        if not fechas:
            logger.info(f"No se encontraron archivos en el folder: {prefijo_folder}")
            return None

        archivo_mas_reciente = max(fechas, key=lambda x: x[1])
        logger.info(f"Archivo más reciente: {archivo_mas_reciente[0]}, actualizado en: {archivo_mas_reciente[1]}")
        return archivo_mas_reciente

    except Exception as e:
        logger.info(f"Error al obtener la fecha más reciente en {prefijo_folder}: {e}")
        raise

def procesar_archivo_json(lista):
    lista_data = []
    for x in lista:
        lista_data += x['lista']
    lista_norep = eliminar_diccionarios_repetidos(lista_data)
    return lista_norep

def eliminar_diccionarios_repetidos(lista):
    vistos = set()
    resultado = []
    for dic in lista:
        clave = json.dumps(dic, sort_keys=True)  # convierte a string con claves ordenadas
        if clave not in vistos:
            vistos.add(clave)
            resultado.append(dic)
    return resultado

def leer_json(nombre_remoto_json):
    try:
        blob = bucket.blob(nombre_remoto_json)
        contenido = blob.download_as_text()
        data = json.loads(contenido)
        return data
    except Exception as e:
        logger.info(f"No se pudo leer el JSON desde el bucket: {e}")
        raise

# -----------------------------------------------------


# ----------------- CARGAR JSON A BASE DE DATOS -------------------------
# ---------------------------------------------------------------------

def filtrar_precargado_json():

    # 0. obtenemos el archivo json mas reciente  y lo leemos
    archivo, _ = obtener_fecha_mas_reciente('data')
    json_ids = leer_json(archivo)

    # 1. Leer lista de ndetalles ya existentes
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT ndetalle FROM sentencias_y_autos;")
    pdfs_ya_subidos = {row[0] for row in cur.fetchall()}

    # 2. preprocesamos, eliminamos los repetidos, etc.
    json_ids_procesados = procesar_archivo_json(json_ids)
    json_filtrado = [item for item in json_ids_procesados if str(item.get("ndetalle")) not in pdfs_ya_subidos]
    return json_filtrado

def cargar_json_a_database(data_filtrada):

    # creamos una conexion
    conn = get_db_connection()
    cur = conn.cursor()

    # comenzamos a insetar los campos del json en la base de datos
    for i,item in enumerate(data_filtrada):
        logger.info(f"[INFO] Procesadas {i}/{len(data_filtrada)} sentencias -> {i}")

        # Insertar en sentencias_y_autos
        cur.execute("""
            INSERT INTO sentencias_y_autos (
                ndetalle, acto_procesal, anio_expe, anio_recurso_expe,
                anio_resolucion, codigo_distrito, codigo_organo, codigo_recurso,
                desc_documento, desc_tipo_recurso_expe, distrito_judicial_expe,
                especialidad_expe, fecha_ingreso_expe, fecha_resolucion,
                instancia_detalle, instancia_expe, juez_firma_resolucion,
                mostrar_botones, nexpedeinte, norma_derecho_interno_expe,
                numero_en_letras, numero_recurso_expe, numero_resolucion,
                organo_detalle, organo_expe, proceso_exp, sede_detalle,
                sumilla, tipo_documento, xformato_expe, url, clasificacion,
                subclasificacion, fecha_real
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s)
            ON CONFLICT (ndetalle) DO NOTHING;
        """, (
            item.get("ndetalle"),
            item.get("actoProcesal"),
            item.get("anioExpe"),
            item.get("anioRecursoExpe"),
            item.get("anioResolucion"),
            item.get("codigoDistrito"),
            item.get("codigoOrgano"),
            item.get("codigoRecurso"),
            item.get("descDocumento"),
            item.get("descTipoRecursoExpe"),
            item.get("distritoJudicialExpe"),
            item.get("especialidadExpe"),
            item.get("fechaIngresoExpe"),
            item.get("fechaResolucion"),
            item.get("instanciaDetalle"),
            item.get("instanciaExpe"),
            item.get("juezFirmaResolucion"),
            item.get("mostrarBotones"),
            item.get("nexpediente"),
            item.get("normaDerechoInternoExpe"),
            item.get("numeroEnLetras"),
            item.get("numeroRecursoExpe"),
            item.get("numeroResolucion"),
            item.get("organoDetalle"),
            item.get("organoExpe"),
            item.get("procesoExp"),
            item.get("sedeDetalle"),
            item.get("sumilla"),
            item.get("tipoDocumento"),
            item.get("xformatoExpe"),
            item.get("url"),
            item.get("clasificacion"),
            item.get("subclasificacion"),
            item.get("fecha_real")
        ))

        # Insertar jueces y la relación
        for juez in item.get("magistrados", []):
            codigo = juez.get("codigo")
            nombre = juez.get("valor")
            if codigo and nombre:
                # Insertar juez
                cur.execute("""
                    INSERT INTO jueces (codigo, nombre_juez)
                    VALUES (%s, %s)
                    ON CONFLICT (codigo) DO NOTHING;
                """, (codigo, nombre))

                # Insertar relación sentencia-juez
                cur.execute("""
                    INSERT INTO sentencias_jueces (ndetalle, codigo)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (item["ndetalle"], codigo))

        if i % 20 == 0:
            conn.commit()

# ---------------------------------------------------------------------


# ----------------- CARGAR RUTA BUCKET A URL EN BASE DE DATOS -------------------------
# -------------------------------------------------------------------------------------

def enrutar_pdfs():

    # Conexion a la base de datos y extraccion de los archivos pdfs
    conn = get_db_connection()
    cur = conn.cursor()
    prefix = "descargas_pdf/"
    blobs = list(bucket.list_blobs(prefix=prefix))  # Convertir a lista para contar

    # Regex para extraer el ID del nombre del archivo
    pattern = r"id=(\d+)\.pdf$"

    # Listamos los ndetalles que se encuentra en el bucket
    ndetalles_bucket = {}
    for blob in blobs:
        match = re.search(pattern, blob.name)
        if match:
            ndetalles_bucket[match.group(1)] = blob.name

    # Extraemos los ndetalles que posean valores nulos en la "url"
    cur.execute('''
        SELECT
            ndetalle
        FROM sentencias_y_autos
        WHERE url IS NULL;
    ''')

    ndetalles_faltantes = set([x[0] for  x in cur.fetchall()])

    # Filtramos solo los ndetalles que se encuentran en ambos
    ndetalles_a_subir = list(ndetalles_faltantes & set(ndetalles_bucket.keys()))

    # Para cada ndetalle a subir
    for i,ndet in enumerate(ndetalles_a_subir):
        time.sleep(0.1)
        filename = ndetalles_bucket[ndet] # obtenemos el nombre en el bucket
        logger.info(f"-> {filename} : {i} / {len(ndetalles_a_subir)}")
        # actualizamos
        cur.execute(
            "UPDATE sentencias_y_autos SET url = %s WHERE ndetalle = %s",
            (filename, ndet)
        )
        conn.commit()
        logger.info("Comiteado ----")

        # cada 40 realizamos un commit y mostramos el progreso
        if i % 40 == 0:
            time.sleep(1)
            logger.info(f"Progreso: {i}/{len(ndetalles_a_subir)}")

    # Guardar cambios finales
    conn.commit()
    cur.close()
    conn.close()

def clasificar_archivos():

    conn = get_db_connection()
    cur = conn.cursor()

    # Extraemos los ndetalles que posean valores nulos en la "clasificacion" pero
    # que si tengan un pdf asociado en la "url"
    cur.execute('''
        SELECT
            ndetalle,
            url
        FROM sentencias_y_autos
        WHERE
            url IS NOT NULL
            AND
            clasificacion IS NULL
    ''')

    filas = cur.fetchall()

    contador = 0
    for ndetalle, url in filas:
        contador += 1
        try:
            logger.info(f"Procesando {ndetalle}...")

            blob = bucket.blob(url)
            logger.info("Obteniendo pdf-bytes")
            pdf_bytes = blob.download_as_bytes(timeout=15)  # <= AÑADE timeout

            logger.info(f"Obteniendo texto")
            texto = extraer_texto_pdf(pdf_bytes)

            
            logger.info(f"Empeando la clasificacion")
            resultado = clasificar_archivo_pdf(texto, ndetalle)
            clasificacion = resultado.get('clase', 'desconocido')

            cur.execute(
                "UPDATE sentencias_y_autos SET clasificacion = %s WHERE ndetalle = %s",
                (clasificacion, ndetalle)
            )
            if contador%40==0:
                logger.info(" Commit !! ")
                conn.commit()
            logger.info(f"{ndetalle} clasificado como {clasificacion} : {contador} / {len(filas)}")

        except Exception as e:
            logger.error(f"Error procesando {ndetalle}: {e}")

    conn.commit()
    cur.close()
    conn.close()

# -------------------------------------------------------------------------------------


def main():

    logger.info("------------------ Empezando guardado de datos ---------------")
    # 1. cargar jsons a base de datos
    logger.info("1.1. Empezando filtrado para cargar json en base de datos ")
    json_filtrados = filtrar_precargado_json()
    logger.info("1.2. Empezando carga de json a base de datos")
    cargar_json_a_database(json_filtrados)

    # 2. cargar ruta del bucket al campo "url" de la base de datos.
    logger.info("2. Empezando enrutado de pdfs en base de datos.")
    enrutar_pdfs()

    # 3. Clasificar los archivos pdfs
    logger.info("3. Empezando clasificacion de los pdfs")
    clasificar_archivos()


if __name__=='__main__':
    main()
