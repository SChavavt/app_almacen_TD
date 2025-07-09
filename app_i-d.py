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
# VERIFICA Y REEMPLAZA ESTOS VALORES CON LOS REALES DE TU HOJA DE CÁLCULO.
# 1. GOOGLE_SHEET_ID: Se encuentra en la URL de tu hoja de cálculo.
#    Ejemplo: Si tu URL es https://docs.google.com/spreadsheets/d/12345ABCDE_YOUR_ID_HERE_FGHIJKL/edit#gid=0
#    Entonces el ID es '12345ABCDE_YOUR_ID_HERE_FGHIJKL'
# 2. GOOGLE_SHEET_WORKSHEET_NAME: Es el nombre EXACTO de la pestaña (hoja) dentro de tu documento de Google Sheets.
#    Ejemplo: Si la pestaña se llama "DatosPedidos", usa 'DatosPedidos'. ¡Respeta mayúsculas y minúsculas!
# 3. PERMISOS: Asegúrate de haber COMPARTIDO tu Google Sheet con la dirección de correo electrónico
#    de la "client_email" que se encuentra dentro de tus credenciales de servicio.
#    Dale al menos permiso de "Lector" o "Editor".
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY' # <--- ¡VERIFICA Y REEMPLAZA SI ES NECESARIO!
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos' # <--- ¡VERIFICA Y REEMPLAZA SI ES NECESARIO!

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    """
    Autentica con Google Sheets usando las credenciales de la cuenta de servicio
    y retorna un cliente de gspread.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(_credentials_json_dict)

        # Asegúrate de que private_key tenga los saltos de línea correctos y sin espacios en blanco alrededor.
        # Esto es CRÍTICO para evitar el error 'Incorrect padding'.
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
    # Obtener credenciales de Google Sheets de st.secrets
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [gsheets].")
        st.info("Falta la clave: 'st.secrets has no key \"gsheets\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? More info: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
        st.stop()

    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    # Asegurarse de que la clave privada tenga los saltos de línea correctos
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()

    # Abrir la hoja de cálculo por ID y nombre de pestaña
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

# Eliminamos @st.cache_resource para que siempre cargue lo último
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

        # Añadir el índice de fila de gsheets
        df['gsheet_row_index'] = df.index + 2 # +2 porque los headers están en la fila 1 y el índice de pandas es 0-based

        # Convertir columnas a tipos apropiados
        numerical_cols = ['ID_Pedido'] # Añade aquí más columnas numéricas si es necesario
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
        # La URL pre-firmada expirará en 1 hora (3600 segundos)
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_object_key},
            ExpiresIn=3600
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para {s3_object_key}: {e}")
        return None

# Función para cargar adjuntos
def display_attachments(adjuntos_str, s3_client_instance):
    if pd.isna(adjuntos_str) or not adjuntos_str.strip():
        return "N/A"
    try:
        # Asumiendo que los adjuntos están separados por coma y espacio, como "file1.pdf, file2.jpg"
        file_keys = [fk.strip() for fk in adjuntos_str.split(',') if fk.strip()]
        links = []
        for fk in file_keys:
            # Asegurarse de que el key sea completo si está usando subcarpetas por ID de pedido
            # Por ejemplo, si los adjuntos están en 'adjuntos/ID_PEDIDO/nombre_archivo.ext'
            # y adjuntos_str solo contiene 'nombre_archivo.ext', se necesitaría reconstruir el key completo.
            # Por simplicidad, asumiremos que adjuntos_str ya contiene el key completo de S3.
            url = get_s3_file_url(fk)
            if url:
                file_name = fk.split('/')[-1] # Obtener solo el nombre del archivo
                links.append(f"[{file_name}]({url})")
            else:
                links.append(f"❌ {fk} (Error URL)")
        return " | ".join(links)
    except Exception as e:
        return f"Error al procesar adjuntos: {e}"


# --- Lógica principal de la aplicación ---

# Filtrar por vendedor, estado y rango de fechas
st.sidebar.header("Filtros")

# Cargar todos los datos al inicio para obtener listas de opciones
df_all_data = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

# Convertir 'Hora_Registro' a datetime para filtrar por fecha
if 'Hora_Registro' in df_all_data.columns:
    df_all_data['Hora_Registro'] = pd.to_datetime(df_all_data['Hora_Registro'], errors='coerce')

# Convertir 'Fecha_Entrega' a datetime
if 'Fecha_Entrega' in df_all_data.columns:
    df_all_data['Fecha_Entrega'] = pd.to_datetime(df_all_data['Fecha_Entrega'], errors='coerce')

# Opciones de filtro para Vendedor (incluir todos)
all_vendedores = ['Todos'] + sorted(df_all_data['Vendedor_Registro'].dropna().unique().tolist())
selected_vendedor = st.sidebar.selectbox("Filtrar por Vendedor:", all_vendedores)

# Opciones de filtro para Estado (incluir todos)
all_estados = ['Todos'] + sorted(df_all_data['Estado'].dropna().unique().tolist())
selected_estado = st.sidebar.selectbox("Filtrar por Estado:", all_estados, index=all_estados.index('Activo') if 'Activo' in all_estados else 0)

# Filtro por tipo de envío
all_tipos_envio = ['Todos'] + sorted(df_all_data['Tipo_Envio'].dropna().unique().tolist())
selected_tipo_envio = st.sidebar.selectbox("Filtrar por Tipo de Envío:", all_tipos_envio)

# Filtro de fecha de registro
today = date.today()
default_start_date = today - timedelta(days=7) # Últimos 7 días por defecto
date_range = st.sidebar.date_input(
    "Rango de Fechas de Registro:",
    value=(default_start_date, today),
    max_value=today,
    format="DD/MM/YYYY"
)

start_date = date_range[0]
end_date = date_range[1] if len(date_range) > 1 else date_range[0]

# --- Lógica de filtrado ---
df_filtered = df_all_data.copy()

if selected_vendedor != 'Todos':
    df_filtered = df_filtered[df_filtered['Vendedor_Registro'] == selected_vendedor]

if selected_estado != 'Todos':
    df_filtered = df_filtered[df_filtered['Estado'] == selected_estado]

if selected_tipo_envio != 'Todos':
    df_filtered = df_filtered[df_filtered['Tipo_Envio'] == selected_tipo_envio]

# Filtrar por rango de fechas de registro
if 'Hora_Registro' in df_filtered.columns:
    df_filtered = df_filtered[
        (df_filtered['Hora_Registro'].dt.date >= start_date) &
        (df_filtered['Hora_Registro'].dt.date <= end_date)
    ]

# Ordenar los datos: Activos primero, luego por Hora_Registro más reciente
df_activos = df_filtered[df_filtered['Estado'] == 'Activo'].sort_values(by='Hora_Registro', ascending=False)
df_completados = df_filtered[df_filtered['Estado'] == 'Completado'].sort_values(by='Hora_Registro', ascending=False)

# Unir ambos DataFrames
df_display = pd.concat([df_activos, df_completados])

# --- Visualización de Datos ---
st.header("Pedidos Filtrados")

if not df_display.empty:
    st.info(f"Se encontraron {len(df_display)} pedidos con los filtros aplicados.")

    if 'ID_Pedido' in df_display.columns:
        df_display['ID_Pedido'] = df_display['ID_Pedido'].astype(str)

    if 'Adjuntos' in df_display.columns:
        df_display['Adjuntos_Enlaces'] = df_display['Adjuntos'].apply(
            lambda x: display_attachments(x, s3_client)
        )

    # Convertir a datetime antes de formatear
    if 'Hora_Registro' in df_display.columns:
        df_display['Hora_Registro'] = pd.to_datetime(df_display['Hora_Registro'], errors='coerce')
    if 'Fecha_Completado' in df_display.columns:
        df_display['Fecha_Completado'] = pd.to_datetime(df_display['Fecha_Completado'], errors='coerce')

    # Columnas a mostrar y sus nuevos nombres
    display_cols_mapping = {
        'ID_Pedido': 'ID_Pedido',
        'Cliente': 'Cliente',
        'Estado': 'Estado',
        'Vendedor_Registro': 'Vendedor',
        'Tipo_Envio': 'Envío',
        'Fecha_Entrega': 'Entrega',
        'Hora_Registro': 'Registro',
        'Notas': 'Notas',
        'Adjuntos_Enlaces': 'Adjuntos'
    }

    # Añadir 'Fecha_Completado' si existe en el DataFrame
    if 'Fecha_Completado' in df_display.columns:
        display_cols_mapping['Fecha_Completado'] = 'Completado'

    # Filtrar las columnas que no existen en el DataFrame actual
    cols_to_use = {original: new for original, new in display_cols_mapping.items() if original in df_display.columns}
    df_display_renamed = df_display[list(cols_to_use.keys())].rename(columns=cols_to_use)

    # Formatear la columna de registro a solo hora si es del día actual, sino fecha y hora
    if 'Registro' in df_display_renamed.columns:
        df_display_renamed['Registro'] = df_display_renamed['Registro'].apply(
            lambda x: x.strftime("%H:%M") if pd.notna(x) and x.date() == date.today() else x.strftime("%d/%m %H:%M") if pd.notna(x) else ""
        )
    # Formatear la columna de completado a solo fecha
    if 'Completado' in df_display_renamed.columns:
        df_display_renamed['Completado'] = df_display_renamed['Completado'].apply(
            lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else ""
        )

    st.dataframe(
        df_display_renamed.reset_index(drop=True),
        use_container_width=True,
        column_config={
            "Adjuntos": st.column_config.Column(
                "Adjuntos",
                help="Enlaces a los archivos adjuntos en S3",
                width="large"
            ),
            **{col: st.column_config.Column(width="small") for col in df_display_renamed.columns if col != "Adjuntos"}
        },
        hide_index=True
    )
else:
    st.info("No hay pedidos para mostrar según los criterios de filtro.")

if __name__ == '__main__':
    # No hay código adicional aquí, la aplicación de Streamlit se ejecuta directamente.
    pass
