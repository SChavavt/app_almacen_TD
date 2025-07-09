import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
from streamlit_autorefresh import st_autorefresh

# --- Configuración de la página ---
st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")
st_autorefresh(interval=5 * 1000, key="datarefresh_integrated")

st.markdown("""
    <h1 style="color: white; font-size: 2.5rem; margin-bottom: 2rem;">
        <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real (Integrado)
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
    st.error("❌ Faltan credenciales de Google Sheets. Asegúrate de definirlas en .streamlit/secrets.toml")
    st.stop()

creds_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

g_spread_client = get_gspread_client()

# --- AWS S3 Configuration ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws"]["aws_region"]
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Falta la clave: {e}")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

@st.cache_resource
def get_s3_client():
    try:
        return boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                  region_name=AWS_REGION)
    except Exception as e:
        st.error(f"❌ Error al inicializar el cliente S3: {e}")
        st.stop()

s3_client = get_s3_client()

# --- Constantes ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

def load_data_from_gsheets(sheet_id, worksheet_name):
    try:
        worksheet = g_spread_client.open_by_key(sheet_id).worksheet(worksheet_name)
        all_data = worksheet.get_all_values()
        if not all_data:
            return pd.DataFrame(), worksheet

        headers = all_data[0]
        data_rows = all_data[1:]
        df = pd.DataFrame(data_rows, columns=headers)
        df['_gsheet_row_index'] = df.index + 2

        expected_columns = [
            'ID_Pedido', 'Folio_Factura', 'Hora_Registro', 'Vendedor_Registro', 'Cliente',
            'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Notas', 'Modificacion_Surtido',
            'Adjuntos', 'Adjuntos_Surtido', 'Estado', 'Estado_Pago', 'Fecha_Completado',
            'Hora_Proceso', 'Turno', 'Surtidor'
        ]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = ''

        df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(lambda x: str(x) if pd.notna(x) and str(x).strip() != '' else '')
        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce')

        df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
        df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
        df['Turno'] = df['Turno'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()

        return df, worksheet
    except Exception as e:
        st.error(f"❌ Error al cargar los datos desde Google Sheets: {e}")
        st.stop()

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
    except:
        return "Sin fecha"
    return "Sin fecha"

def mostrar_resumen_estados(df):
    st.markdown("### 📊 Resumen General")
    total_demorados = df[df["Estado"] == "🔴 Demorado"].shape[0]
    total_en_proceso = df[df["Estado"] == "🔵 En Proceso"].shape[0]
    total_pendientes = df[df["Estado"] == "📥 Pendiente"].shape[0]
    total_completados_hoy = df[(df["Estado"] == "🟢 Completado") & (pd.to_datetime(df["Fecha_Completado"]).dt.date == date.today())].shape[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🔴 Demorados", total_demorados)
    col2.metric("🔵 En Proceso", total_en_proceso)
    col3.metric("📥 Pendientes", total_pendientes)
    col4.metric("🟢 Completados Hoy", total_completados_hoy)

def main():
    df, worksheet = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

    if not df.empty:
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce')
        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')

        mostrar_resumen_estados(df)

        st.sidebar.header("Filtros de Pedidos")
        mostrar_completados = st.sidebar.checkbox("Mostrar Pedidos Completados", value=False)
        mostrar_cancelados = st.sidebar.checkbox("Mostrar Pedidos Cancelados", value=False)

        df_filtrado = df.copy()
        if not mostrar_completados:
            df_filtrado = df_filtrado[df_filtrado["Estado"] != "🟢 Completado"]
        if not mostrar_cancelados:
            df_filtrado = df_filtrado[df_filtrado["Estado"] != "⚫ Cancelado"]

        df_filtrado = df_filtrado.sort_values(by="Hora_Registro", ascending=True)

        todos_grupos = []
        for tipo_envio, grupo in df_filtrado.groupby("Tipo_Envio"):
            if grupo.empty:
                continue
            titulo = f"📦 {tipo_envio if tipo_envio else 'Sin tipo'}"
            todos_grupos.append((titulo, grupo))

        if todos_grupos:
            cols = st.columns(len(todos_grupos))
            for i, (titulo, df_grupo) in enumerate(todos_grupos):
                with cols[i]:
                    st.markdown(f"#### {titulo}")
                    columnas_mostrar = ["Cliente", "Hora_Registro", "Estado", "Surtidor"]
                    df_display = df_grupo[columnas_mostrar].copy()
                    df_display = df_display.rename(columns={"Hora_Registro": "Registro"})
                    df_display['Registro'] = df_display['Registro'].apply(
                        lambda x: x.strftime("%H:%M") if pd.notna(x) and x.date() == date.today() else x.strftime("%d/%m %H:%M") if pd.notna(x) else "")
                    st.dataframe(df_display.reset_index(drop=True), use_container_width=True, hide_index=True)
        else:
            st.info("No hay pedidos para mostrar según los criterios de filtro.")
    else:
        st.info("No hay pedidos cargados.")

if __name__ == "__main__":
    main()
