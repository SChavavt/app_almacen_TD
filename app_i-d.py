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

# Helper function to encapsulate dataframe display logic
def display_dataframe_with_formatting(df_to_display):
    display_cols_mapping = {
        'ID_Pedido': 'ID_Pedido',
        'Cliente': 'Cliente',
        'Estado': 'Estado',
        'Vendedor_Registro': 'Vendedor',
        'Tipo_Envio': 'Envío',
        'Fecha_Entrega': 'Entrega',
        'Hora_Registro': 'Registro',
        'Notas': 'Notas',
        'Adjuntos_Enlaces': 'Adjuntos',
        'Turno': 'Turno' # Incluir Turno en el mapeo
    }

    # Añadir 'Fecha_Completado' si existe en el DataFrame específico
    if 'Fecha_Completado' in df_to_display.columns:
        display_cols_mapping['Fecha_Completado'] = 'Completado'

    # Filtrar las columnas que no existen en el DataFrame actual antes de renombrar
    cols_to_use = {original: new for original, new in display_cols_mapping.items() if original in df_to_display.columns}
    df_display_final = df_to_display[list(cols_to_use.keys())].rename(columns=cols_to_use)

    # Formatear la columna de registro a solo hora si es del día actual, sino fecha y hora
    if 'Registro' in df_display_final.columns:
        df_display_final['Registro'] = df_display_final['Registro'].apply(
            lambda x: x.strftime("%H:%M") if pd.notna(x) and x.date() == date.today() else x.strftime("%d/%m %H:%M") if pd.notna(x) else ""
        )
    # Formatear la columna de completado a solo fecha
    if 'Completado' in df_display_final.columns:
        df_display_final['Completado'] = df_display_final['Completado'].apply(
            lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else ""
        )

    st.dataframe(
        df_display_final,
        use_container_width=True,
        column_config={
            "Adjuntos": st.column_config.Column(
                "Adjuntos",
                help="Enlaces a los archivos adjuntos en S3",
                width="large"
            ),
            **{col: st.column_config.Column(width="small") for col in df_display_final.columns if col != "Adjuntos"}
        },
        hide_index=True
    )


# --- Lógica principal de la aplicación ---

# Cargar todos los datos
df_all_data = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

# Convertir 'Hora_Registro' y 'Fecha_Entrega' a datetime
if 'Hora_Registro' in df_all_data.columns:
    df_all_data['Hora_Registro'] = pd.to_datetime(df_all_data['Hora_Registro'], errors='coerce')
if 'Fecha_Entrega' in df_all_data.columns:
    df_all_data['Fecha_Entrega'] = pd.to_datetime(df_all_data['Fecha_Entrega'], errors='coerce')


# --- Visualización de Datos ---
st.header("Todos los Pedidos por Tipo de Envío y Turno")

if not df_all_data.empty:
    st.info(f"Mostrando todos los {len(df_all_data)} pedidos.")

    # Convertir 'ID_Pedido' a string
    if 'ID_Pedido' in df_all_data.columns:
        df_all_data['ID_Pedido'] = df_all_data['ID_Pedido'].astype(str)

    # Procesar adjuntos para crear enlaces
    if 'Adjuntos' in df_all_data.columns:
        df_all_data['Adjuntos_Enlaces'] = df_all_data['Adjuntos'].apply(
            lambda x: display_attachments(x, s3_client)
        )

    # Convertir a datetime antes de formatear para todas las columnas de tiempo
    if 'Hora_Registro' in df_all_data.columns:
        df_all_data['Hora_Registro'] = pd.to_datetime(df_all_data['Hora_Registro'], errors='coerce')
    if 'Fecha_Completado' in df_all_data.columns:
        df_all_data['Fecha_Completado'] = pd.to_datetime(df_all_data['Fecha_Completado'], errors='coerce')

    # Obtener tipos de envío únicos y ordenarlos para una visualización consistente
    unique_tipos_envio = sorted(df_all_data['Tipo_Envio'].dropna().unique().tolist())

    if not unique_tipos_envio:
        st.warning("No se encontraron tipos de envío definidos en los pedidos.")
    else:
        for tipo_envio in unique_tipos_envio:
            if tipo_envio == 'Local':
                st.subheader(f"🚚 Pedidos: {tipo_envio}") # General header for all local
                
                # Define the specific turnos and map them to columns
                # Asegúrate de que estos nombres de turno coincidan exactamente con tus datos en Google Sheets
                local_turnos_order = ['☀️ Local Mañana', '🌙 Local Tarde', '🌵 Saltillo', '📦 Pasa a Bodega']
                
                # Crear columnas para cada turno específico
                # Se crean 4 columnas ya que son 4 tipos de turnos específicos
                cols = st.columns(len(local_turnos_order))
                
                for i, turno_name in enumerate(local_turnos_order):
                    with cols[i]:
                        st.markdown(f"**{turno_name}**") # Un encabezado más pequeño dentro de la columna
                        df_current_turno = df_all_data[
                            (df_all_data['Tipo_Envio'] == 'Local') & 
                            (df_all_data['Turno'] == turno_name)
                        ].copy()

                        if not df_current_turno.empty:
                            if 'Hora_Registro' in df_current_turno.columns:
                                df_current_turno = df_current_turno.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                            display_dataframe_with_formatting(df_current_turno)
                        else:
                            st.info("No hay pedidos.")
                
                # Manejar pedidos 'Local' con turnos faltantes o no listados explícitamente
                # Filtra todos los turnos que son 'Local' pero que NO están en `local_turnos_order`
                other_local_turnos_df = df_all_data[
                    (df_all_data['Tipo_Envio'] == 'Local') &
                    (~df_all_data['Turno'].isin(local_turnos_order) | df_all_data['Turno'].isna())
                ].copy()
                
                if not other_local_turnos_df.empty:
                    st.markdown("---") # Separador visual
                    st.subheader(f"🚚 Pedidos: {tipo_envio} - Otros Turnos/Sin Turno Definido")
                    if 'Hora_Registro' in other_local_turnos_df.columns:
                        other_local_turnos_df = other_local_turnos_df.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                    display_dataframe_with_formatting(other_local_turnos_df)
                # else: No es necesario un st.info aquí, ya que si está vacío, no se mostrará nada.

            else:
                # Para otros tipos de envío (ej. Foráneos), mostrar directamente en secciones verticales
                st.subheader(f"🚚 Pedidos: {tipo_envio}")
                df_tipo_envio_group = df_all_data[df_all_data['Tipo_Envio'] == tipo_envio].copy()
                if not df_tipo_envio_group.empty:
                    if 'Hora_Registro' in df_tipo_envio_group.columns:
                        df_tipo_envio_group = df_tipo_envio_group.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)
                    display_dataframe_with_formatting(df_tipo_envio_group)
                else:
                    st.info(f"No hay pedidos para el tipo de envío '{tipo_envio}'.")
else:
    st.info("No hay pedidos para mostrar en la hoja de cálculo.")

if __name__ == '__main__':
    # No hay código adicional aquí, la aplicación de Streamlit se ejecuta directamente.
    pass
