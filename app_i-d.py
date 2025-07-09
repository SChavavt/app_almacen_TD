
import streamlit as st
import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, date

# --- Configuración inicial ---
st.set_page_config(page_title="Panel de Almacén", layout="wide")
st_autorefresh(interval=5 * 1000, key="auto_refresh")

st.markdown("""
    <h1 style="color: white; font-size: 2.5rem; margin-bottom: 2rem;">
        <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
    </h1>
""", unsafe_allow_html=True)

st.markdown("""
    <style>
    .dataframe td {
        white-space: unset !important;
        word-break: break-word;
    }
    </style>
""", unsafe_allow_html=True)

# --- Autenticación con Google Sheets desde secrets ---
if "gsheets" not in st.secrets:
    st.error("Faltan credenciales de Google Sheets.")
    st.stop()

creds_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def formatear_fecha_consistente(fecha_str):
    if pd.isna(fecha_str) or str(fecha_str).strip() in ["", "Sin fecha"]:
        return "Sin fecha"
    try:
        if isinstance(fecha_str, str) and "/" in fecha_str:
            return fecha_str
        elif isinstance(fecha_str, str) and "-" in fecha_str:
            return datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        elif hasattr(fecha_str, 'strftime'):
            return fecha_str.strftime("%d/%m/%Y")
    except:  # noqa: E722
        return "Sin fecha"
    return "Sin fecha"

def cargar_datos():
    SHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
    SHEET_NAME = "datos_pedidos"
    client = get_gspread_client()
    worksheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    if 'Fecha_Entrega' in df.columns:
        df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(formatear_fecha_consistente)
    if 'Fecha_Completado' in df.columns:
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
    if 'Hora' in df.columns:
        df['Hora'] = pd.to_datetime(df['Hora'], errors='coerce')
    return df

def mostrar_resumen_estados(df):
    st.markdown("### 📊 Resumen General")
    hoy = date.today()
    total_demorados = df[df["Estado"] == "🔴 Demorado"].shape[0]
    total_en_proceso = df[df["Estado"] == "🔵 En proceso"].shape[0]
    total_pendientes = df[df["Estado"] == "📥 Pendiente"].shape[0]
    total_completados_hoy = df[
        (df["Estado"] == "🟢 Completado") & 
        (pd.to_datetime(df["Fecha_Completado"]).dt.date == hoy)
    ].shape[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🔴 Demorados", total_demorados)
    col2.metric("🔵 En proceso", total_en_proceso)
    col3.metric("📥 Pendientes", total_pendientes)
    col4.metric("🟢 Completados Hoy", total_completados_hoy)

# --- MAIN ---
df = cargar_datos()
if "Tipo" in df.columns:
    df_pedidos = df[df["Tipo"] == "📦 Pedido"].copy()
else:
    st.warning("La columna 'Tipo' no existe en los datos. Se mostrarán todos los registros.")
    df_pedidos = df.copy()

if not df_pedidos.empty:
    df_completados_hoy = df_pedidos[
        (df_pedidos["Estado"] == "🟢 Completado") & 
        (pd.to_datetime(df_pedidos["Fecha_Completado"]).dt.date == date.today())
    ].copy()

    df_activos = df_pedidos[
        df_pedidos["Estado"].isin(["📥 Pendiente", "🔵 En proceso", "🔴 Demorado"])
    ].copy()

    df_visualizacion = pd.concat([df_activos, df_completados_hoy], ignore_index=True)

    if not df_visualizacion.empty:
        df_visualizacion["Orden"] = df_visualizacion["Estado"].apply(
            lambda x: 0 if x == "🔴 Demorado" else (
                1 if x == "🔵 En proceso" else (
                    2 if x == "📥 Pendiente" else 3
                )
            )
        )
        df_visualizacion["Fecha_Entrega"] = df_visualizacion["Fecha_Entrega"].apply(formatear_fecha_consistente)
        df_visualizacion["Tipo_Orden"] = df_visualizacion["Tipo_Envio"].apply(
            lambda x: 0 if x == "Local-Mañana" else (
                1 if x == "Local-Tarde" else (
                    2 if x == "Saltillo" else (
                        3 if x == "Pasa a Bodega" else (
                            4 if x == "Foráneo" else 5
                        )
                    )
                )
            )
        )
        df_visualizacion = df_visualizacion.sort_values(by=["Orden", "Tipo_Orden", "Hora"])
        mostrar_resumen_estados(df_visualizacion)

        locales_manana = df_visualizacion[df_visualizacion["Tipo_Envio"] == "Local-Mañana"]
        locales_tarde = df_visualizacion[df_visualizacion["Tipo_Envio"] == "Local-Tarde"]

        grupos_locales = []
        for tipo_local, emoji, df_local in [
            ("Local-Mañana", "☀️", locales_manana),
            ("Local-Tarde", "🌙", locales_tarde)
        ]:
            for fecha, grupo in df_local.groupby("Fecha_Entrega"):
                fecha_fmt = formatear_fecha_consistente(fecha)
                titulo = f"{emoji} {tipo_local.replace('-', ' ')} ({fecha_fmt})"
                grupos_locales.append((titulo, grupo.copy()))

        otros = df_visualizacion[
            ~df_visualizacion["Tipo_Envio"].isin(["Local-Mañana", "Local-Tarde"])
        ]
        otros_grupos = []
        for tipo, grupo in otros.groupby("Tipo_Envio"):
            if tipo == "Saltillo":
                emoji = "⛰️"
                titulo = f"{emoji} Saltillo"
            elif tipo == "Pasa a Bodega":
                emoji = "📦"
                titulo = f"{emoji} Pasa a Bodega"
            elif tipo == "Foráneo":
                emoji = "🌍"
                titulo = f"{emoji} Pedidos Foráneos"
            else:
                emoji = "❓"
                titulo = f"{emoji} Local Sin clasificar"
            otros_grupos.append((titulo, grupo.copy()))

        todos_grupos = grupos_locales + otros_grupos
        if todos_grupos:
            cols = st.columns(len(todos_grupos))
            for i, (titulo, df_grupo) in enumerate(todos_grupos):
                with cols[i]:
                    st.markdown(f"#### {titulo}")
                    columnas_mostrar = ["Cliente", "Hora", "Estado", "Surtidor"] 
                    if "Tipo_Envio" in df_grupo.columns and "❓" in titulo:
                        columnas_mostrar.append("Tipo_Envio")
                    st.dataframe(
                        df_grupo[columnas_mostrar]
                        .rename(columns={"Hora": "Fecha", "Tipo_Envio": "Envío"})
                        .reset_index(drop=True),
                        use_container_width=True,
                        hide_index=True
                    )
        else:
            st.info("No hay pedidos para mostrar según los criterios.")
    else:
        st.info("No hay pedidos activos ni completados hoy.")
else:
    st.info("No hay pedidos cargados.")
