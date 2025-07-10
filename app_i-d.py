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

# 🔄 Refrescar cada 5 segundos automáticamente
st_autorefresh(interval=5 * 1000, key="datarefresh_integrated")

# --- Título con emoji y botón a la derecha ---
col_title, col_button = st.columns([0.7, 0.3]) # Ajustar proporciones de columnas según sea necesario

with col_title:
    st.markdown(
        """
        <h1 style="color: white; font-size: 2.5rem; margin-bottom: 0rem;">
            <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
        </h1>
        """,
        unsafe_allow_html=True,
    )

with col_button:
    # Ajustar el padding top para alinear con el título si es necesario
    st.markdown("<div style='padding-top: 25px;'>", unsafe_allow_html=True) 
    
    # Inicializar estado de sesión: False = solo activos (por defecto, ocultar completados de 24h)
    # True = mostrar activos + completados de 24h
    if 'show_recent_completed' not in st.session_state:
        st.session_state['show_recent_completed'] = False

    # Etiqueta del botón basada en el estado
    button_label = "👁️ Mostrar Completados (24h)" if not st.session_state['show_recent_completed'] else "👁️ Ocultar Completados"
    if st.button(button_label):
        st.session_state['show_recent_completed'] = not st.session_state['show_recent_completed']
        st.rerun() # Fuerza una recarga para aplicar el filtro
    st.markdown("</div>", unsafe_allow_html=True)

# Añadir línea separadora
st.markdown("---")

# Inyectar CSS para el word-wrap en las celdas del dataframe
st.markdown(
    """
    <style>
    .dataframe td {
        white-space: unset !important;
        word-break: break-word;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Google Sheets Constants ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    """
    Autentica con Google Sheets usando las credenciales de la cuenta de servicio
    y retorna un cliente de gspread.
    """
    try:
        # Usando el scope más compatible para gspread
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(_credentials_json_dict)

        if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()

        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.info("ℹ️ Verifica que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tus credenciales de servicio en `secrets.toml` sean válidas.")
        st.stop()

# --- AWS S3 Configuration ---
try:
    if "aws" not in st.secrets:
        st.error("❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [aws].")
        st.info("Falta la clave: 'st.secrets has no key \"aws\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? More info: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
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
    """
    Inicializa y retorna un cliente S3 de boto3.
    """
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        return s3
    except Exception as e:
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        st.stop()

# Inicializar clientes globalmente
try:
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [gsheets].")
        st.info("Falta la clave: 'st.secrets has no key \"gsheets\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? More info: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
        st.stop()

    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()

    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet_main = spreadsheet.worksheet(GOOGLE_SHEET_WORKSHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Error: La hoja de cálculo con ID '{GOOGLE_SHEET_ID}' no se encontró. Verifica el ID y los permisos de la cuenta de servicio.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error: La pestaña '{GOOGLE_SHEET_WORKSHEET_NAME}' no se encontró en la hoja de cálculo. Verifica el nombre de la pestaña y los permisos.")
        st.stop()

except Exception as e:
    st.error(f"❌ Error general al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud. También, revisa tus credenciales de AWS S3 y Google Sheets en `.streamlit/secrets.toml` o en la interfaz de Streamlit Cloud.")
    st.stop()

def load_data_from_gsheets(sheet_id, worksheet_name):
    """
    Carga todos los datos de una hoja de cálculo de Google Sheets en un DataFrame de Pandas
    y añade el índice de fila de la hoja de cálculo.
    """
    try:
        spreadsheet = g_spread_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        data = worksheet.get_all_values()
        if not data:
            return pd.DataFrame()

        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)

        df['gsheet_row_index'] = df.index + 2

        # Convertir columnas a tipos adecuados, verificando su existencia
        numerical_cols = ['ID_Pedido']
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        date_time_cols = ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado', 'Fecha_Pago_Comprobante', 'Hora_Proceso']
        for col in date_time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Asegurarse de que la columna 'Turno' se maneje correctamente como string y nulos
        if 'Turno' in df.columns:
            df['Turno'] = df['Turno'].astype(str).replace({'nan': '', '': None}).fillna('')
        else:
            df['Turno'] = '' # Si no existe, crearla vacía

        return df
    except gspread.exceptions.APIError as e:
        st.error(f"❌ Error de API de Google Sheets al cargar datos: {e}")
        st.info("Verifica los permisos de la cuenta de servicio en Google Sheets.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Google Sheets: {e}")
        st.stop()

# --- Funciones de S3 ---
def get_s3_file_url(s3_object_key):
    """Genera una URL pre-firmada para acceder a un objeto S3."""
    if not s3_object_key:
        return None
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_object_key},
            ExpiresIn=3600
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para {s3_object_key}: {e}")
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
        return f"Error al procesar adjuntos: {e}"

# Helper function to encapsulate dataframe display logic
def display_dataframe_with_formatting(df_to_display):
    # Columnas a mostrar: Cliente, Hora_Registro (como Fecha), Estado, Vendedor_Registro (como Surtidor)
    columnas_base = ["Cliente", "Hora_Registro", "Estado"]
    
    # Decidir si usar 'Surtidor' o 'Vendedor_Registro'
    if 'Surtidor' in df_to_display.columns:
        columnas_base.append("Surtidor")
    elif 'Vendedor_Registro' in df_to_display.columns:
        columnas_base.append("Vendedor_Registro")

    existing_columns = [col for col in columnas_base if col in df_to_display.columns]

    if not existing_columns:
        st.info("No hay columnas relevantes para mostrar en este subgrupo.")
        return

    df_display_final = df_to_display[existing_columns].copy()

    rename_map = {}
    if "Hora_Registro" in df_display_final.columns:
        rename_map["Hora_Registro"] = "Fecha"
    if "Vendedor_Registro" in df_display_final.columns and "Surtidor" not in df_display_final.columns:
        rename_map["Vendedor_Registro"] = "Surtidor"

    df_display_final = df_display_final.rename(columns=rename_map)

    # Formatear la columna de 'Fecha' (originalmente 'Hora_Registro')
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

# --- Lógica principal de la aplicación ---

# Cargar todos los datos
df_all_data = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

# Convertir 'ID_Pedido' y columnas de fecha/hora (robusto, aunque ya hecho en load_data)
if 'ID_Pedido' in df_all_data.columns:
    df_all_data['ID_Pedido'] = df_all_data['ID_Pedido'].astype(str)
if 'Hora_Registro' in df_all_data.columns:
    df_all_data['Hora_Registro'] = pd.to_datetime(df_all_data['Hora_Registro'], errors='coerce')
if 'Fecha_Entrega' in df_all_data.columns:
    df_all_data['Fecha_Entrega'] = pd.to_datetime(df_all_data['Fecha_Entrega'], errors='coerce')
if 'Fecha_Completado' in df_all_data.columns:
    df_all_data['Fecha_Completado'] = pd.to_datetime(df_all_data['Fecha_Completado'], errors='coerce')

# Procesar adjuntos para crear enlaces
if 'Adjuntos' in df_all_data.columns:
    df_all_data['Adjuntos_Enlaces'] = df_all_data['Adjuntos'].apply(
        lambda x: display_attachments(x, s3_client)
    )

# --- Visualización de Datos por columna 'Turno' ---
if not df_all_data.empty:
    df_display_data = df_all_data.copy()
    time_threshold = datetime.now() - timedelta(hours=24)

    # Lógica de filtrado basada en el estado del botón
    if not st.session_state['show_recent_completed']:
        # Estado: False -> Botón dice "Mostrar Completados (24h)"
        # Acción: Mostrar solo pedidos NO completados (ocultar TODOS los completados).
        df_display_data = df_display_data[df_display_data['Estado'] != '🟢 Completado'].copy()
    else:
        # Estado: True -> Botón dice "Ocultar Completados"
        # Acción: Mostrar pedidos NO completados Y completados de las últimas 24h.
        df_display_data = df_display_data[
            (df_display_data['Estado'] != '🟢 Completado') |
            ((df_display_data['Estado'] == '🟢 Completado') & 
             (df_display_data['Fecha_Completado'].notna()) &
             (df_display_data['Fecha_Completado'] >= time_threshold))
        ].copy()

    grupos_a_mostrar = []
    # 1. Pedidos Foráneos: Si 'Turno' está vacío (None o string vacío después de limpieza)
    df_foraneos = df_display_data[df_display_data['Turno'] == ''].copy() 
    if not df_foraneos.empty:
        grupos_a_mostrar.append((f"🌍 Pedidos Foráneos ({len(df_foraneos)})", df_foraneos))
    
    # 2. Otros grupos basados en valores únicos de la columna 'Turno' (excluyendo vacíos)
    unique_turns = [t for t in df_display_data['Turno'].unique() if t != ''] 
    
    preferred_order = [
        '☀️ Local Mañana',
        '🌙 Local Tarde',
        '📦 Pasa a Bodega',
        '🌵 Saltillo'
    ]

    sorted_unique_turns = []
    for p_t in preferred_order:
        if p_t in unique_turns:
            sorted_unique_turns.append(p_t)
            unique_turns.remove(p_t)
    sorted_unique_turns.extend(sorted(unique_turns)) 

    for turno_val in sorted_unique_turns:
        df_grupo = df_display_data[df_display_data['Turno'] == turno_val].copy() 
        if not df_grupo.empty:
            titulo_grupo = turno_val
            grupos_a_mostrar.append((f"{titulo_grupo} ({len(df_grupo)})", df_grupo))

    if grupos_a_mostrar:
        num_cols_per_row = 3
        for row_index_start in range(0, len(grupos_a_mostrar), num_cols_per_row):
            current_row_groups = grupos_a_mostrar[row_index_start : row_index_start + num_cols_per_row]
            cols = st.columns(len(current_row_groups))
            for i, (titulo, df_grupo) in enumerate(current_row_groups):
                with cols[i]:
                    st.markdown(f"#### {titulo}")
                    # Ordenar por Hora_Registro para tener los más recientes arriba en cada grupo
                    if 'Hora_Registro' in df_grupo.columns:
                        df_grupo = df_grupo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                    display_dataframe_with_formatting(df_grupo)
    else:
        st.info("No hay pedidos para mostrar según los criterios actuales.")
else:
    st.info("No hay pedidos en la hoja de cálculo.")

if __name__ == '__main__':
    pass
