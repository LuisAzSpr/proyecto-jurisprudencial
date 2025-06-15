import streamlit as st
import requests
import os
from dotenv import load_dotenv

load_dotenv(verbose=True)
API_BASE_URL = os.getenv("URL_API")

st.set_page_config(layout="wide")
st.title("Buscador de Sentencias PDF")

@st.cache_data
def cargar_filtros():
    try:
        resp = requests.get(f"{API_BASE_URL}/filters")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"Error al cargar filtros: {e}")
        return {
            "organo_detalle": [], "nombre_juez": []
        }

filtros = cargar_filtros()
def opciones_con_todos(lista): return ["Todos"] + lista

opts_organo_det = opciones_con_todos(filtros["organo_detalle"])
opts_juez = opciones_con_todos(filtros["nombre_juez"])

# Diseño de filtros
col1, col2, col3, col4 = st.columns(4)

with col1:
    fecha_desde = st.date_input("Fecha Desde")
with col2:
    fecha_hasta = st.date_input("Fecha Hasta")
with col3:
    sel_organo_det = st.selectbox("Órgano Detalle", opts_organo_det)
with col4:
    sel_juez = st.selectbox("Nombre Juez", opts_juez)

filtro_clasificacion = st.checkbox("Solo sentencias")

if st.button("Buscar"):
    params = {
        "fecha_desde": fecha_desde.isoformat(),
        "fecha_hasta": fecha_hasta.isoformat()
    }
    if sel_organo_det != "Todos":
        params["organo_detalle"] = sel_organo_det
    if sel_juez != "Todos":
        params["nombre_juez"] = sel_juez
    if filtro_clasificacion:
        params["clasificacion_fundada"] = filtro_clasificacion

    with st.spinner("Consultando sentencias..."):
        try:
            resp = requests.get(f"{API_BASE_URL}/search", params=params)
            resp.raise_for_status()
            resultado = resp.json()
        except Exception as e:
            st.error(f"Error al consultar datos: {e}")
            st.stop()

    total = resultado.get("total_count", 0)
    items = resultado.get("items", {})

    st.markdown(f"**Total de PDFs que coinciden con los filtros:** {total}")
    if total == 0:
        st.warning("No se encontraron PDF con esos filtros.")
    else:
        tabla_md = "| pdf | Descargar |\n| ---- | -------- |\n"
        for nd, ruta in items.items():
            url_pdf = None
            try:
                d = requests.get(f"{API_BASE_URL}/descargar/{nd}")
                if d.status_code == 200:
                    url_pdf = d.json().get("url")
            except:
                pass
            link = f"[📄 Descargar]({url_pdf})" if url_pdf else "Error al generar URL"
            nombre = ruta.split("/")[-1].split(",")[0] + ".pdf" if ruta else "None"
            tabla_md += f"| {nombre} | {link} |\n"

        st.markdown("#### Primeros 100 resultados:")
        st.markdown(tabla_md, unsafe_allow_html=True)