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
# Esto asegura que la aplicación cargue los datos más recientes de Google Sheets y S3
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

        numerical_cols = ['ID_Pedido']
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        date_time_cols = ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado']
        for col in date_time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

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

# Helper function to encapsulate dataframe display logic, mimicking app_almacen.py's column display
def display_dataframe_with_formatting(df_to_display):
    # Columnas a mostrar, reflejando app_almacen.py: Cliente, Hora, Estado, Surtidor
    # Usamos los nombres originales de las columnas del DF cargado y luego renombramos para la visualización
    columnas_originales_a_mostrar = ["Cliente", "Hora_Registro", "Estado", "Vendedor_Registro"]
    
    # Asegurarse de que todas las columnas existan antes de seleccionarlas
    existing_columns = [col for col in columnas_originales_a_mostrar if col in df_to_display.columns]
    
    if not existing_columns:
        st.info("No hay columnas relevantes para mostrar en este subgrupo.")
        return

    df_display_final = df_to_display[existing_columns].copy()

    # Renombrar columnas para la visualización, como en app_almacen.py
    rename_map = {
        "Hora_Registro": "Fecha",
        "Vendedor_Registro": "Surtidor"
    }
    df_display_final = df_display_final.rename(columns={k: v for k, v in rename_map.items() if k in df_display_final.columns})

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

# Convertir 'ID_Pedido' y columnas de fecha/hora
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

# --- Visualización de Datos ---
st.header("Todos los Pedidos por Tipo de Envío y Turno")

if not df_all_data.empty:
    st.info(f"Mostrando todos los {len(df_all_data)} pedidos.")

    # Definir las categorías para el diseño de 2 columnas principales con apilamiento (según image_4f345c.png)
    # y la asignación explícita de cada tabla a una columna.
    # Orden importa para que coincida con la imagen y la expectativa del usuario.
    primary_display_structure = [
        # Columna 1
        {"header": "☀️ Local Mañana", "filter_func": lambda df: (df['Tipo_Envio'] == 'Local') & (df['Turno'] == '☀️ Local Mañana'), "target_col_idx": 0},
        {"header": "Foráneos", "filter_func": lambda df: df['Tipo_Envio'] == 'Foráneos', "target_col_idx": 0},
        {"header": "🌵 Saltillo", "filter_func": lambda df: df['Tipo_Envio'] == 'Saltillo', "target_col_idx": 0}, # Añadido aquí explícitamente

        # Columna 2
        {"header": "🌙 Local Tarde", "filter_func": lambda df: (df['Tipo_Envio'] == 'Local') & (df['Turno'] == '🌙 Local Tarde'), "target_col_idx": 1},
        {"header": "📦 Pasa a Bodega", "filter_func": lambda df: df['Tipo_Envio'] == 'Pasa a Bodega', "target_col_idx": 1}
    ]

    # Crear las dos columnas principales
    col1, col2 = st.columns(2)
    
    # Mapeo de índices de columna a objetos de columna Streamlit
    cols_map = {0: col1, 1: col2}

    # Conjunto para rastrear los ID_Pedido que ya han sido mostrados
    handled_order_ids = set()

    # Procesar las categorías de visualización principales
    for category in primary_display_structure:
        with cols_map[category["target_col_idx"]]:
            st.markdown(f"#### {category['header']}") # Usar #### para el título como en app_almacen.py
            df_filtered = df_all_data[category["filter_func"](df_all_data)].copy()
            
            if not df_filtered.empty:
                # Ordenar por Hora_Registro
                if 'Hora_Registro' in df_filtered.columns:
                    df_filtered = df_filtered.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                
                display_dataframe_with_formatting(df_filtered) # Usar la función de formato simplificada
                handled_order_ids.update(df_filtered['ID_Pedido'].tolist()) # Registrar los IDs de pedido mostrados
            else:
                st.info("No hay pedidos.")
            st.markdown("---") # Separador entre tablas dentro de la columna

    # --- Manejar cualquier otro pedido que no fue clasificado en la estructura principal ---
    st.subheader("Otros Pedidos / Categorías No Clasificadas")
    
    # Filtrar pedidos que no han sido mostrados
    df_unhandled = df_all_data[~df_all_data['ID_Pedido'].isin(handled_order_ids)].copy()

    if not df_unhandled.empty:
        # Agrupar por Tipo_Envio y luego por Turno (si aplica) para los no manejados
        unique_unhandled_types = df_unhandled['Tipo_Envio'].dropna().unique().tolist()
        
        for tipo_envio in sorted(unique_unhandled_types):
            df_remaining_by_tipo = df_unhandled[df_unhandled['Tipo_Envio'] == tipo_envio].copy()
            
            if tipo_envio == 'Local' and 'Turno' in df_remaining_by_tipo.columns:
                # Para pedidos 'Local' no manejados, agrupar por Turno
                unique_unhandled_turns = df_remaining_by_tipo['Turno'].dropna().unique().tolist()
                if not unique_unhandled_turns: # Si hay 'Local' pero sin Turno definido
                    st.markdown(f"**🚚 Pedidos: {tipo_envio} (Sin Turno Definido)**")
                    if 'Hora_Registro' in df_remaining_by_tipo.columns:
                        df_remaining_by_tipo = df_remaining_by_tipo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                    display_dataframe_with_formatting(df_remaining_by_tipo)
                else:
                    for turno in sorted(unique_unhandled_turns):
                        st.markdown(f"**🚚 Pedidos: {tipo_envio} - Turno: {turno}**")
                        df_remaining_by_turno = df_remaining_by_tipo[df_remaining_by_tipo['Turno'] == turno].copy()
                        if 'Hora_Registro' in df_remaining_by_turno.columns:
                            df_remaining_by_turno = df_remaining_by_turno.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                        display_dataframe_with_formatting(df_remaining_by_turno)
            else: # Para otros Tipo_Envio no manejados
                st.markdown(f"**🚚 Pedidos: {tipo_envio}**")
                if 'Hora_Registro' in df_remaining_by_tipo.columns:
                    df_remaining_by_tipo = df_remaining_by_tipo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                display_dataframe_with_formatting(df_remaining_by_tipo)
    else:
        st.info("Todos los pedidos han sido clasificados en las categorías principales.")
else:
    st.info("No hay pedidos para mostrar en la hoja de cálculo.")

if __name__ == '__main__':
    pass
