# app_front.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
API_BASE_URL = os.getenv("URL_API")

st.set_page_config(layout="wide")

# -----------------------
# Carga de filtros
# -----------------------
@st.cache_data
def cargar_filtros():
    """Obtiene los valores disponibles para filtros desde el backend."""
    try:
        resp = requests.get(f"{API_BASE_URL}/filters")
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        st.error(f"Error al cargar filtros: {e}")
        return {"organo_detalle": [], "nombre_juez": []}

# -----------------------
# Parámetros de consulta
# -----------------------
def build_params(fecha_desde, fecha_hasta, sel_organo, sel_juez, solo_sentencias, extra=None):
    """Construye el dict de parámetros para las peticiones al backend."""
    params = {
        "fecha_desde": fecha_desde.isoformat(),
        "fecha_hasta": fecha_hasta.isoformat(),
    }
    if extra:
        params.update(extra)
    if sel_organo and sel_organo != "Todos":
        params["organo_detalle"] = sel_organo
    if sel_juez and sel_juez != "Todos":
        params["nombre_juez"] = sel_juez
    if solo_sentencias:
        params["clasificacion_fundada"] = True
    return params

# -----------------------
# Páginas de visualización
# -----------------------
def display_search_page():
    """Muestra la interfaz de búsqueda de PDFs."""
    filtros = cargar_filtros()
    opts_organo = ["Todos"] + filtros.get("organo_detalle", [])
    opts_juez = ["Todos"] + filtros.get("nombre_juez", [])

    # Controles de filtros
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_desde = st.date_input("Fecha Desde")
    with col2:
        fecha_hasta = st.date_input("Fecha Hasta")
    with col3:
        sel_organo = st.selectbox("Órgano Detalle", opts_organo)
    with col4:
        sel_juez = st.selectbox("Nombre Juez", opts_juez)

    solo_sentencias = st.checkbox("Solo sentencias")

    if st.button("Buscar"):
        st.session_state.pagina_actual = 1

    page = st.session_state.get("pagina_actual", 1)
    limit = 100
    offset = (page - 1) * limit

    params = build_params(fecha_desde, fecha_hasta, sel_organo, sel_juez, solo_sentencias,
                          extra={"limit": limit, "offset": offset})
    with st.spinner("Consultando sentencias..."):
        resultado = fetch_data("/search", params)

    show_search_results(resultado, page, limit)


def show_search_results(resultado, pagina, limite):
    """Renderiza los resultados de búsqueda con paginación."""
    total = resultado.get("total_count", 0)
    items = resultado.get("items", [])

    st.markdown(f"**Total de PDFs encontrados:** {total}")
    if total == 0:
        st.warning("No se encontraron resultados.")
        return

    # Tabla Markdown
    tabla = ["| PDF | Clasificación | Descargar |", "| --- | ------------- | -------- |"]
    for item in items:
        nd = item.get("ndetalle")
        ruta = item.get("url") or ""
        clasif = item.get("clasificacion", "N/A")
        link = build_download_link(nd)
        nombre = ruta.split("/")[-1].split(",")[0] + ".pdf" if ruta else "-"
        tabla.append(f"| {nombre} | {clasif} | {link} |")

    st.markdown("\n".join(tabla), unsafe_allow_html=True)
    pages = (total - 1) // limite + 1
    st.markdown(f"#### Página {pagina} de {pages}")

    col_prev, _, col_next = st.columns(3)
    with col_prev:
        if st.button("⬅ Página anterior") and pagina > 1:
            st.session_state.pagina_actual -= 1
            st.experimental_rerun()
    with col_next:
        if st.button("Página siguiente ➡") and pagina < pages:
            st.session_state.pagina_actual += 1
            st.experimental_rerun()

# -----------------------
# Estadísticas
# -----------------------
def display_stats_page():
    """Muestra la sección de estadísticas de URLs."""
    st.header("📊 Estadísticas de URLs nulas")

    filtros = cargar_filtros()
    opts_juez = ["Todos"] + filtros.get("nombre_juez", [])

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_desde = st.date_input("Desde", key="stats_desde")
    with col2:
        fecha_hasta = st.date_input("Hasta", key="stats_hasta")
    with col3:
        sel_juez = st.selectbox("Filtrar por Juez", opts_juez)

    if st.button("Generar estadísticas"):
        params = build_params(fecha_desde, fecha_hasta, None, sel_juez, False)
        with st.spinner("Obteniendo datos..."):
            stats = fetch_data("/statistics", params)
        render_stats(stats)


def render_stats(stats):
    """Dibuja gráficos de barras para las estadísticas."""
    # Por día
    by_day = stats.get("by_day", [])
    if by_day:
        df_day = pd.DataFrame(by_day)
        df_day["day"] = pd.to_datetime(df_day["day"])
        st.subheader("% Nulos por día")
        st.bar_chart(df_day.set_index("day")["null_percentage"])

    # Por juez
    by_juez = stats.get("by_juez", [])
    if by_juez:
        df_juez = pd.DataFrame(by_juez)
        st.subheader("% Nulos por juez")
        st.bar_chart(df_juez.set_index("juez")["null_percentage"])

# -----------------------
# Helpers
# -----------------------
def fetch_data(endpoint, params):
    """Petición GET al backend, maneja errores y retorna JSON o diccionario vacío."""
    try:
        resp = requests.get(f"{API_BASE_URL}{endpoint}", params=params)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        st.error(f"Error al obtener datos: {e}")
        return {}


def build_download_link(ndetalle):
    """Genera el enlace de descarga firmado para un PDF dado su ID."""
    try:
        resp = requests.get(f"{API_BASE_URL}/descargar/{ndetalle}")
        resp.raise_for_status()
        return f"[📄 Descargar]({resp.json().get('url')})"
    except requests.RequestException:
        return "Error al generar URL"

# -----------------------
# Main
# -----------------------
def main():
    # Inicialización de estado
    if "page" not in st.session_state:
        st.session_state.page = "search"
    if "pagina_actual" not in st.session_state:
        st.session_state.pagina_actual = 1

    # Barra de navegación
    col_nav1, col_nav2 = st.columns([1, 1])
    with col_nav1:
        if st.button("🔍 Buscar PDF"):
            st.session_state.page = "search"
    with col_nav2:
        if st.button("📊 Estadísticas"):
            st.session_state.page = "stats"

    # Renderizado de páginas
    if st.session_state.page == "stats":
        display_stats_page()
    else:
        display_search_page()


if __name__ == "__main__":
    main()
