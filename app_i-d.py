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

# Título con emoji colorido
st.markdown(
    """
    <h1 style="color: white; font-size: 2.5rem; margin-bottom: 2rem;">
        <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real (Integrado)
    </h1>
    """,
    unsafe_allow_html=True,
)

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

        date_time_cols = ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado']
        for col in date_time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Asegurarse de que 'Surtidor' esté presente y no sea None si 'Vendedor_Registro' es usado como 'Surtidor' en la vista
        # Si 'Surtidor' ya es una columna, úsala directamente. Si no, y 'Vendedor_Registro' existe, úsala como fallback.
        if 'Surtidor' not in df.columns and 'Vendedor_Registro' in df.columns:
            df['Surtidor'] = df['Vendedor_Registro'] # Temporal para la lógica de abajo, será renombrada para display

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

# Helper function to encapsulate dataframe display logic, mirroring app_almacen.py's column display
def display_dataframe_with_formatting(df_to_display):
    # Columnas a mostrar, exactamente como en app_almacen.py: Cliente, Hora, Estado, Surtidor
    # y si tiene Tipo_Envio y es '?', se añade Tipo_Envio
    
    # Asegurarse de que todas las columnas existan antes de seleccionarlas
    # Vendedor_Registro es lo que debe ser 'Surtidor' en la vista
    columnas_base = ["Cliente", "Hora_Registro", "Estado"]
    
    # Si 'Surtidor' existe en el DF (directamente de GSheets), la usamos.
    # Si no, pero existe 'Vendedor_Registro', la usamos como 'Surtidor' para la visualización.
    if 'Surtidor' in df_to_display.columns:
        columnas_base.append("Surtidor")
    elif 'Vendedor_Registro' in df_to_display.columns:
        columnas_base.append("Vendedor_Registro")

    existing_columns = [col for col in columnas_base if col in df_to_display.columns]

    if not existing_columns:
        st.info("No hay columnas relevantes para mostrar en este subgrupo.")
        return

    df_display_final = df_to_display[existing_columns].copy()

    # Renombrar columnas para la visualización: Hora_Registro a Fecha, Vendedor_Registro a Surtidor
    rename_map = {}
    if "Hora_Registro" in df_display_final.columns:
        rename_map["Hora_Registro"] = "Fecha"
    if "Vendedor_Registro" in df_display_final.columns and "Surtidor" not in df_display_final.columns:
        rename_map["Vendedor_Registro"] = "Surtidor" # Renombrar si Vendedor_Registro es la fuente de Surtidor

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

# --- Visualización de Datos (según app_almacen.py) ---
st.header("Todos los Pedidos por Tipo de Envío y Turno")

if not df_all_data.empty:
    st.info(f"Mostrando todos los {len(df_all_data)} pedidos.")

    # Reorganizar los datos en grupos lógicos para visualización, replicando app_almacen.py
    grupos_a_mostrar = []
    handled_order_ids_for_grouping = set() # Track IDs that have been explicitly grouped

    # Pedidos Locales (Mañana, Tarde)
    df_local = df_all_data[df_all_data['Tipo_Envio'] == 'Local'].copy()
    
    df_manana = df_local[df_local['Turno'] == '☀️ Local Mañana'].copy()
    if not df_manana.empty:
        grupos_a_mostrar.append((f"☀️ Local Mañana ({len(df_manana)})", df_manana))
        handled_order_ids_for_grouping.update(df_manana['ID_Pedido'].tolist())

    df_tarde = df_local[df_local['Turno'] == '🌙 Local Tarde'].copy()
    if not df_tarde.empty:
        grupos_a_mostrar.append((f"🌙 Local Tarde ({len(df_tarde)})", df_tarde))
        handled_order_ids_for_grouping.update(df_tarde['ID_Pedido'].tolist())
    
    # Otros Tipo_Envio específicos en el orden deseado
    ordered_other_types = ['Foráneos', 'Pasa a Bodega', 'Saltillo']

    for tipo_envio_key in ordered_other_types:
        df_grupo = df_all_data[(df_all_data['Tipo_Envio'] == tipo_envio_key) & (~df_all_data['ID_Pedido'].isin(handled_order_ids_for_grouping))].copy()
        if not df_grupo.empty:
            if tipo_envio_key == 'Pasa a Bodega':
                grupos_a_mostrar.append((f"📦 Pasa a Bodega ({len(df_grupo)})", df_grupo))
            elif tipo_envio_key == 'Saltillo':
                grupos_a_mostrar.append((f"🌵 Saltillo ({len(df_grupo)})", df_grupo))
            elif tipo_envio_key == 'Foráneos':
                grupos_a_mostrar.append((f"🌍 Pedidos Foráneos ({len(df_grupo)})", df_grupo))
            handled_order_ids_for_grouping.update(df_grupo['ID_Pedido'].tolist())


    # Manejar otros tipos de envío no clasificados explícitamente arriba
    # Esto busca cualquier Tipo_Envio que tenga pedidos no manejados por los grupos anteriores.
    df_remaining_general = df_all_data[~df_all_data['ID_Pedido'].isin(handled_order_ids_for_grouping)].copy()

    if not df_remaining_general.empty:
        unique_remaining_types = df_remaining_general['Tipo_Envio'].dropna().unique()
        for tipo_envio in sorted(unique_remaining_types):
            # Exclude 'Local' here as its specific turns should be handled
            if tipo_envio == 'Local':
                df_local_remaining = df_remaining_general[df_remaining_general['Tipo_Envio'] == 'Local'].copy()
                unique_local_remaining_turns = df_local_remaining['Turno'].dropna().unique()
                for turno in sorted(unique_local_remaining_turns):
                    df_grupo_turno = df_local_remaining[df_local_remaining['Turno'] == turno].copy()
                    if not df_grupo_turno.empty:
                        grupos_a_mostrar.append((f"❓ Local ({turno}) ({len(df_grupo_turno)})", df_grupo_turno))
            else: # Other general types not explicitly covered
                df_grupo = df_remaining_general[df_remaining_general['Tipo_Envio'] == tipo_envio].copy()
                if not df_grupo.empty:
                    grupos_a_mostrar.append((f"❓ Otros ({tipo_envio}) ({len(df_grupo)})", df_grupo))

    if grupos_a_mostrar:
        # Mostrar columnas dinámicamente, una al lado de la otra
        cols = st.columns(len(grupos_a_mostrar))
        for i, (titulo, df_grupo) in enumerate(grupos_a_mostrar):
            with cols[i]:
                st.markdown(f"#### {titulo}")
                # Ordenar por Hora_Registro si existe la columna
                if 'Hora_Registro' in df_grupo.columns:
                    df_grupo = df_grupo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                display_dataframe_with_formatting(df_grupo)
    else:
        st.info("No hay pedidos para mostrar según los criterios.")
else:
    st.info("No hay pedidos para mostrar en la hoja de cálculo.")

if __name__ == '__main__':
    pass
