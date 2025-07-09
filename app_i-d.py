
import streamlit as st
import pandas as pd
from datetime import datetime, date
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh

# --- Configuración inicial ---
st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")
st_autorefresh(interval=5 * 1000, key="autorefresh")

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

# --- Cargar credenciales desde secrets ---
if "gsheets" not in st.secrets:
    st.error("❌ No se encontraron credenciales de Google Sheets en secrets.")
    st.stop()

creds_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def formatear_fecha(fecha_str):
    if pd.isna(fecha_str) or str(fecha_str).strip() in ["", "Sin fecha"]:
        return "Sin fecha"
    try:
        if isinstance(fecha_str, str) and '/' in fecha_str:
            return fecha_str
        elif isinstance(fecha_str, str) and '-' in fecha_str:
            return datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        elif hasattr(fecha_str, 'strftime'):
            return fecha_str.strftime("%d/%m/%Y")
    except:  # noqa: E722
        return "Sin fecha"
    return "Sin fecha"

@st.cache_data(ttl=60)
def cargar_datos_gsheets():
    SHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
    SHEET_NAME = "datos_pedidos"
    client = get_gspread_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    if df.empty:
        return df

    if "Fecha_Entrega" in df.columns:
        df["Fecha_Entrega"] = df["Fecha_Entrega"].apply(formatear_fecha)

    df["Fecha_Completado"] = pd.to_datetime(df["Fecha_Completado"], errors='coerce')
    df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors='coerce')

    return df

def mostrar_resumen(df):
    st.markdown("### 📊 Resumen General")
    hoy = date.today()

    total_demorados = df[df["Estado"] == "🔴 Demorado"].shape[0]
    total_proceso = df[df["Estado"] == "🔵 En proceso"].shape[0]
    total_pendientes = df[df["Estado"] == "📥 Pendiente"].shape[0]
    total_hoy = df[(df["Estado"] == "🟢 Completado") & (df["Fecha_Completado"].dt.date == hoy)].shape[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 Demorados", total_demorados)
    c2.metric("🔵 En proceso", total_proceso)
    c3.metric("📥 Pendientes", total_pendientes)
    c4.metric("🟢 Completados Hoy", total_hoy)

def visualizar_pedidos(df):
    activos = df[df["Estado"].isin(["📥 Pendiente", "🔵 En proceso", "🔴 Demorado"])].copy()
    completados_hoy = df[(df["Estado"] == "🟢 Completado") & (df["Fecha_Completado"].dt.date == date.today())].copy()
    df_vis = pd.concat([activos, completados_hoy], ignore_index=True)

    if df_vis.empty:
        st.info("No hay pedidos activos ni completados hoy.")
        return

    df_vis["Orden"] = df_vis["Estado"].map({
        "🔴 Demorado": 0, "🔵 En proceso": 1, "📥 Pendiente": 2
    }).fillna(3)

    tipo_envio_orden = {
        "Local-Mañana": 0, "Local-Tarde": 1, "Saltillo": 2,
        "Pasa a Bodega": 3, "Foráneo": 4
    }

    df_vis["Tipo_Orden"] = df_vis["Tipo_Envio"].map(tipo_envio_orden).fillna(5)
    df_vis = df_vis.sort_values(by=["Orden", "Tipo_Orden", "Hora_Registro"])

    locales = {
        "☀️ Local Mañana": df_vis[df_vis["Tipo_Envio"] == "Local-Mañana"],
        "🌙 Local Tarde": df_vis[df_vis["Tipo_Envio"] == "Local-Tarde"]
    }

    otros = df_vis[~df_vis["Tipo_Envio"].isin(["Local-Mañana", "Local-Tarde"])]

    grupos = []
    for titulo, df_local in locales.items():
        for fecha, grupo in df_local.groupby("Fecha_Entrega"):
            fecha_fmt = formatear_fecha(fecha)
            grupos.append((f"{titulo} ({fecha_fmt})", grupo.copy()))

    for tipo, grupo in otros.groupby("Tipo_Envio"):
        if tipo == "Saltillo":
            grupos.append(("⛰️ Saltillo", grupo))
        elif tipo == "Pasa a Bodega":
            grupos.append(("📦 Pasa a Bodega", grupo))
        elif tipo == "Foráneo":
            grupos.append(("🌍 Pedidos Foráneos", grupo))

    if not grupos:
        st.info("No hay grupos para mostrar.")
        return

    cols = st.columns(len(grupos))
    for i, (titulo, grupo) in enumerate(grupos):
        with cols[i]:
            st.markdown(f"#### {titulo}")
            mostrar = ["Cliente", "Hora_Registro", "Estado", "Surtidor"]
            df_disp = grupo[mostrar].copy()
            df_disp.rename(columns={"Hora_Registro": "Fecha"}, inplace=True)
            st.dataframe(
                df_disp.reset_index(drop=True),
                use_container_width=True,
                hide_index=True
            )

# --- MAIN ---
df = cargar_datos_gsheets()
if not df.empty:
    mostrar_resumen(df)
    visualizar_pedidos(df)
else:
    st.info("No hay datos cargados.")
