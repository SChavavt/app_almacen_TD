import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")
st_autorefresh(interval=5 * 1000, key="datarefresh_integrated")

col_title, col_button = st.columns([0.7, 0.3])
with col_title:
    st.markdown("""
        <h1 style="color: white; font-size: 2.5rem; margin-bottom: 0rem;">
            <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
        </h1>
    """, unsafe_allow_html=True)

with col_button:
    st.markdown("<div style='padding-top: 25px;'>", unsafe_allow_html=True)
    if 'show_recent_completed' not in st.session_state:
        st.session_state['show_recent_completed'] = False

    button_label = "👁️ Mostrar Completados (24h)" if not st.session_state['show_recent_completed'] else "👁️ Ocultar Completados"
    if st.button(button_label):
        st.session_state['show_recent_completed'] = not st.session_state['show_recent_completed']
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("---")

st.markdown("""
    <style>
    .dataframe td {
        white-space: unset !important;
        word-break: break-word;
    }
    </style>
""", unsafe_allow_html=True)

GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(_credentials_json_dict)
        if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.stop()

try:
    if "aws" not in st.secrets:
        st.error("❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets.")
        st.stop()

    AWS_CREDENTIALS = st.secrets["aws"]
    AWS_ACCESS_KEY_ID = AWS_CREDENTIALS["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = AWS_CREDENTIALS["aws_secret_access_key"]
    AWS_REGION = AWS_CREDENTIALS["aws_region"]
    S3_BUCKET_NAME = AWS_CREDENTIALS["s3_bucket_name"]

except Exception as e:
    st.error(f"❌ Error al cargar las credenciales de AWS S3: {e}")
    st.stop()

@st.cache_resource
def get_s3_client():
    try:
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        st.stop()

try:
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets.")
        st.stop()

    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()

    spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
    worksheet_main = spreadsheet.worksheet(GOOGLE_SHEET_WORKSHEET_NAME)

except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()

@st.cache_data(ttl=30)
def load_data_from_gsheets(sheet_id, worksheet_name):
    try:
        spreadsheet = g_spread_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
        if not data:
            return pd.DataFrame()

        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        df['gsheet_row_index'] = df.index + 2

        numerical_cols = ['ID_Pedido']
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        date_time_cols = ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado', 'Fecha_Pago_Comprobante', 'Hora_Proceso']
        for col in date_time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'Turno' in df.columns:
            df['Turno'] = df['Turno'].astype(str).replace({'nan': '', '': None}).fillna('')
        else:
            df['Turno'] = ''

        return df
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Google Sheets: {e}")
        st.stop()

def get_s3_file_url(s3_object_key):
    if not s3_object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_object_key},
            ExpiresIn=3600
        )
    except Exception as e:
        st.error(f"❌ Error URL S3: {e}")
        return None

def display_attachments(adjuntos_str, s3_client_instance):
    if pd.isna(adjuntos_str) or not adjuntos_str.strip():
        return "N/A"
    try:
        file_keys = [fk.strip() for fk in adjuntos_str.split(',') if fk.strip()]
        links = []
        for fk in file_keys:
            url = get_s3_file_url(fk)
            if url:
                file_name = fk.split('/')[-1]
                links.append(f"[{file_name}]({url})")
            else:
                links.append(f"❌ {fk} (Error URL)")
        return " | ".join(links)
    except Exception as e:
        return f"Error adjuntos: {e}"

def display_dataframe_with_formatting(df_to_display):
    columnas_base = ["Cliente", "Hora_Registro", "Estado"]
    if 'Surtidor' in df_to_display.columns:
        columnas_base.append("Surtidor")
    elif 'Vendedor_Registro' in df_to_display.columns:
        columnas_base.append("Vendedor_Registro")

    existing_columns = [col for col in columnas_base if col in df_to_display.columns]
    if not existing_columns:
        st.info("No hay columnas relevantes.")
        return

    df_display_final = df_to_display[existing_columns].copy()
    rename_map = {}
    if "Hora_Registro" in df_display_final.columns:
        rename_map["Hora_Registro"] = "Fecha"
    if "Vendedor_Registro" in df_display_final.columns and "Surtidor" not in df_display_final.columns:
        rename_map["Vendedor_Registro"] = "Surtidor"

    df_display_final = df_display_final.rename(columns=rename_map)
    if 'Fecha' in df_display_final.columns:
        df_display_final['Fecha'] = df_display_final['Fecha'].apply(
            lambda x: x.strftime("%H:%M") if pd.notna(x) and x.date() == date.today() else x.strftime("%d/%m %H:%M") if pd.notna(x) else ""
        )
    st.dataframe(
        df_display_final,
        use_container_width=True,
        column_config={col: st.column_config.Column(width="small") for col in df_display_final.columns},
        hide_index=True
    )

# --- Lógica principal ---

df_all_data = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

if 'ID_Pedido' in df_all_data.columns:
    df_all_data['ID_Pedido'] = df_all_data['ID_Pedido'].astype(str)

if 'Adjuntos' in df_all_data.columns:
    df_all_data['Adjuntos_Enlaces'] = df_all_data['Adjuntos'].apply(
        lambda x: display_attachments(x, s3_client)
    )

if not df_all_data.empty:
    df_display_data = df_all_data.copy()
    time_threshold = datetime.now() - timedelta(hours=24)

    if not st.session_state['show_recent_completed']:
        df_display_data = df_display_data[df_display_data['Estado'] != '🟢 Completado'].copy()
    else:
        df_display_data = df_display_data[
            (df_display_data['Estado'] != '🟢 Completado') |
            ((df_display_data['Estado'] == '🟢 Completado') & 
             (df_display_data['Fecha_Completado'].notna()) &
             (df_display_data['Fecha_Completado'] >= time_threshold))
        ].copy()

    grupos_a_mostrar = []
    df_foraneos = df_display_data[df_display_data['Turno'] == ''].copy() 
    if not df_foraneos.empty:
        grupos_a_mostrar.append((f"🌍 Pedidos Foráneos ({len(df_foraneos)})", df_foraneos))
    
    unique_turns = [t for t in df_display_data['Turno'].unique() if t != '']
    preferred_order = ['☀️ Local Mañana', '🌙 Local Tarde', '📦 Pasa a Bodega', '🌵 Saltillo']
    sorted_unique_turns = [t for t in preferred_order if t in unique_turns] + sorted(set(unique_turns) - set(preferred_order))

    for turno_val in sorted_unique_turns:
        df_grupo = df_display_data[df_display_data['Turno'] == turno_val].copy()
        if not df_grupo.empty:
            grupos_a_mostrar.append((f"{turno_val} ({len(df_grupo)})", df_grupo))

    if grupos_a_mostrar:
        num_cols_per_row = 3
        for i in range(0, len(grupos_a_mostrar), num_cols_per_row):
            row = grupos_a_mostrar[i:i+num_cols_per_row]
            cols = st.columns(len(row))
            for j, (titulo, df_grupo) in enumerate(row):
                with cols[j]:
                    st.markdown(f"#### {titulo}")
                    if 'Hora_Registro' in df_grupo.columns:
                        df_grupo = df_grupo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                    display_dataframe_with_formatting(df_grupo)
    else:
        st.info("No hay pedidos para mostrar.")
else:
    st.info("No hay pedidos cargados desde Google Sheets.")
