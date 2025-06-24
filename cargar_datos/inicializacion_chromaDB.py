import os
from dotenv import load_dotenv
import psycopg2
from google.oauth2 import service_account
from google.cloud import storage
import pdfplumber
import io
import json
import logging

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


credentials = service_account.Credentials.from_service_account_file('credenciales.json')
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("automatizacion-casillero")

load_dotenv(verbose=False)

conn = psycopg2.connect(
        host=os.getenv("DB-HOST"),
        port=os.getenv("DB-PORT"),
        dbname=os.getenv("DB-NAME"),
        user=os.getenv("USERNAME-DB"),
        password=os.getenv("PASSWORD-DB")
)

cur = conn.cursor()
cur.execute(
    '''
    SELECT
            ndetalle,
            url,
            clasificacion,
            organo_detalle
    FROM sentencias_y_autos
    WHERE
        clasificacion IN ('fundado','infundado')
        AND
        organo_detalle in ('CUARTA SALA DE DERECHO CONSTITUCIONAL Y SOCIAL TRANSITORIA','SEGUNDA SALA DE DERECHO CONSTITUCIONAL Y SOCIAL TRANSITORIA')
    '''
)
total = cur.fetchall()



def leer_paginas_pdf_como_lineas(pdf_key , num_paginas=1):
    blob = bucket.blob(pdf_key)
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    resultado = []
    with pdfplumber.open(buffer) as pdf:
        total_paginas = len(pdf.pages)
        for i in range(min(num_paginas, total_paginas)):
            texto = pdf.pages[i].extract_text()
            lineas = texto.splitlines() if texto else []
            resultado.append(lineas)

    return resultado if resultado else None


total = {}
contador = 0
for ndetalle,url in total:
    pagina1 = leer_paginas_pdf_como_lineas(
        pdf_key = url,
        num_paginas = 1
    )
    total[ndetalle] = pagina1[:5]
    if contador%50==0:
        with open("encabezado.json", "w", encoding="utf-8") as f:
            json.dump(total, f, ensure_ascii=False, indent=2)
            print(f"{contador}/{len(total)}")
    contador += 1

with open("encabezado.json", "w", encoding="utf-8") as f:
    json.dump(total, f, ensure_ascii=False, indent=2)