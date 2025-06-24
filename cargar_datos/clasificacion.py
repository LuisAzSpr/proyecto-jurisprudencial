from abc import abstractmethod, ABC
import numpy as np
import re
import pdfplumber
import io
import logging

logging.getLogger("pdfminer").setLevel(logging.ERROR)

PRIMERAS_PALABRAS = [
    'declarar', 'declararon', 'declare', 'DECLARARON', 'DECLARAR',
    'Declararon', 'Declare', 'declarando','Declarar'
]
SEGUNDAS_PALABRAS = [
    'PROCEDENTE', 'IMPROCEDENTE', 'INFUNDADO', 'FUNDADO',
    'FUNDADA', 'INFUNDADA', 'ADMISIBLE', 'INADMISIBLE'
]
DICCIONARIO = {
    '+FUNDADA': 'fundado', '+FUNDADO': 'fundado',
    '+INFUNDADA': 'infundado', '+INFUNDADO': 'infundado',
    '+PROCEDENTE': 'procedente', '+IMPROCEDENTE': 'improcedente'
}

def generar_formas(base_palabras):
    terminaciones = ["", ":", ": ", ":  ", ":\n", ":\t", " ", "\n", "\t"]
    formas = []
    for base in base_palabras:
        for sufijo in terminaciones:
            formas.append(f"{base}{sufijo}")
    return list(set(formas))  # eliminar duplicados

def combinar_listas_regex(primeras_bases, segundas_palabras):
    primeras = generar_formas(primeras_bases)
    patrones = []
    for p1 in primeras:
        for p2 in segundas_palabras:
            p1_limpia = re.escape(p1.strip())
            p2_limpia = re.escape(p2.strip())
            patron = rf"{p1_limpia}\s+{p2_limpia}"
            patrones.append(patron)
    return patrones

class Manejador(ABC):
    def __init__(self, siguiente=None):
        self._siguiente = siguiente

    @abstractmethod
    def manejar(self, solicitud):
        pass

    def pasar(self, solicitud):
        if self._siguiente:
            return self._siguiente.manejar(solicitud)
        # Si no hay siguiente, se podría retornar None, o lanzar una excepción
        return None

class Manejador1(Manejador):
    def manejar(self, mensaje_json):
        nombre_pdf = mensaje_json['nombre_pdf']
        if len(mensaje_json) == 2:
            tipo = list(mensaje_json.keys())[1]
            posicion = list(mensaje_json.values())[1]
            if posicion > 0.6:
                print(f"Subir el archivo {nombre_pdf} al directorio {tipo}")
                return tipo
            else:
                return self.pasar(mensaje_json)  # Agregar return aquí
        else:
            return self.pasar(mensaje_json)  # Agregar return aquí

class Manejador2(Manejador):
    def manejar(self, mensaje_json):
        nombre_pdf = mensaje_json['nombre_pdf']
        if len(mensaje_json) > 2:
            palabras = list(mensaje_json.keys())[1:]
            posiciones = list(mensaje_json.values())[1:]
            indices = np.argsort(posiciones)[-2:]
            if posiciones[indices[1]] - posiciones[indices[0]] > 0.20:
                tipo = palabras[indices[1]]
                print(f"Subir el archivo {nombre_pdf} al directorio {tipo}")
                return tipo
            else:
                print(f"posiciones juntas {posiciones}")
                return self.pasar(mensaje_json)  # Agregar return aquí
        else:
            return self.pasar(mensaje_json)  # Agregar return aquí

class Manejador3(Manejador):
    def manejar(self, mensaje_json):
        nombre_pdf = mensaje_json['nombre_pdf']
        print(f"Subir el archivo {nombre_pdf} al directorio DESCONOCIDO")
        return 'desconocido'

def estructura_pdf(contenido):
    claves = {}
    patrones_regex = combinar_listas_regex(PRIMERAS_PALABRAS, SEGUNDAS_PALABRAS)
    tamano_texto = len(contenido)

    for patron in patrones_regex:
        coincidencias = re.findall(patron, contenido)
        if coincidencias:
            coincidencias_unicas = list(set(coincidencias))
            for coincidencia in coincidencias_unicas:
                claves[patron] = contenido.rfind(coincidencia)/tamano_texto

    return claves

def formateo_estructura(claves):
    claves_diccionario = {}
    for key, value in DICCIONARIO.items():
        for palabra_clave, valor in claves.items():
            if key not in palabra_clave:
                continue
            claves_diccionario[value] = valor
    return claves_diccionario


def clasificar_archivo_pdf(contenido, id_val):
    handler_chain = Manejador1(Manejador2(Manejador3()))
    claves = estructura_pdf(contenido)
    claves_diccionario = formateo_estructura(claves)

    json_final = {'nombre_pdf':id_val}
    json_final.update(claves_diccionario)
    clasificacion = handler_chain.manejar(json_final)

    return {
        'ndetalle': id_val,
        'clase': clasificacion
    }

def extraer_texto_pdf(pdf_bytes):
    texto = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for pagina in pdf.pages:
            texto += pagina.extract_text() or ""
    return texto