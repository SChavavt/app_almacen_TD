import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import os
import gspread.utils
from streamlit_autorefresh import st_autorefresh

from app_a import SERVICE_ACCOUNT_FILE, load_credentials_from_file

# --- Configuración de la página ---


# --- Google Sheets Authentication desde st.secrets ---
if "gsheets" not in st.secrets:
    st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que `.streamlit/secrets.toml` contenga la sección [gsheets].")
    st.stop()

import json
GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")

@st.cache_resource
def get_gspread_client(credentials_dict):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    return gspread.authorize(creds)

g_spread_client = get_gspread_client(GSHEETS_CREDENTIALS)

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

# --- Google Sheets Configuration ---
# Asegúrate de que este archivo de credenciales JSON esté en la misma carpeta que tu app de Streamlit

# ====================================================================================================
# IMPORTANTE: Por favor, VERIFICA Y REEMPLAZA ESTOS VALORES CON LOS REALES DE TU HOJA DE CÁLCULO.
# Los valores a continuación son EJEMPLOS tomados de tu archivo 'app_almacen.py' proporcionado.
# Si tu hoja de cálculo actual tiene un ID o nombre de pestaña diferente, DEBES ACTUALIZARLOS AQUÍ.
#
# 1. GOOGLE_SHEET_ID: Se encuentra en la URL de tu hoja de cálculo.
#    Ejemplo: Si tu URL es https://docs.google.com/spreadsheets/d/12345ABCDE_YOUR_ID_HERE_FGHIJKL/edit#gid=0
#    Entonces el ID es '12345ABCDE_YOUR_ID_HERE_FGHIJKL'
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY' # <--- ¡VERIFICA Y REEMPLAZA SI ES NECESARIO!

# 2. GOOGLE_SHEET_WORKSHEET_NAME: Es el nombre EXACTO de la pestaña (hoja) dentro de tu documento de Google Sheets.
#    Ejemplo: Si la pestaña se llama "DatosPedidos", usa 'DatosPedidos'. ¡Respeta mayúsculas y minúsculas!
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos' # <--- ¡VERIFICA Y REEMPLAZA SI ES NECESARIO!

# 3. PERMISOS: Asegúrate de haber COMPARTIDO tu Google Sheet con la dirección de correo electrónico
#    de la "client_email" que se encuentra dentro de tu archivo 'sistema-pedidos-td-e80e1a9633c2.json'.
#    Dale al menos permiso de "Lector" o "Editor".
# ====================================================================================================


# --- AWS S3 Configuration ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws"]["aws_region"]
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente. Falta la clave: {e}")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- Cached Clients for Google Sheets and AWS S3 ---
@st.cache_resource
def get_gspread_client(credentials_json_dict):
    """
    Autentica con Google Sheets usando las credenciales de la cuenta de servicio
    y retorna un cliente de gspread.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.info("ℹ️ Verifica que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tu archivo de credenciales sea válido.")
        st.stop()

@st.cache_resource
def get_s3_client():
    """
    Inicializa y retorna un cliente de S3, usando credenciales globales.
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
        st.error(f"❌ Error al inicializar el cliente S3: {e}")
        st.info("ℹ️ Revisa tus credenciales de AWS en `st.secrets['aws']` y la configuración de la región.")
        st.stop()

# Initialize clients globally
try:
    google_creds_dict = load_credentials_from_file(SERVICE_ACCOUNT_FILE)  # noqa: F821
    g_spread_client = get_gspread_client(google_creds_dict)
    s3_client = get_s3_client()
except Exception as e:
    st.error(f"❌ Error general al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que el archivo de credenciales de Google Sheets esté en la raíz de tu proyecto y que las APIs de Google Sheets y Drive estén habilitadas. También, revisa tus credenciales de AWS S3 en `.streamlit/secrets.toml`.")
    st.stop()


# --- Data Loading from Google Sheets ---
# Eliminamos @st.cache_resource para que siempre cargue lo último
def load_data_from_gsheets(sheet_id, worksheet_name):
    """
    Carga todos los datos de una hoja de cálculo de Google Sheets en un DataFrame de Pandas
    y añade el índice de fila de la hoja de cálculo.
    """
    try:
        spreadsheet = g_spread_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Obtener todos los valores incluyendo los encabezados para poder calcular el índice de fila
        all_data = worksheet.get_all_values()
        if not all_data:
            return pd.DataFrame(), worksheet

        headers = all_data[0]
        data_rows = all_data[1:]

        df = pd.DataFrame(data_rows, columns=headers)

        # Añadir el índice de fila de Google Sheet (basado en 1)
        # Asumiendo que el encabezado está en la fila 1, la primera fila de datos es la fila 2.
        df['_gsheet_row_index'] = df.index + 2

        # Define las columnas esperadas y asegúrate de que existan
        expected_columns = [
            'ID_Pedido', 'Folio_Factura', 'Hora_Registro', 'Vendedor_Registro', 'Cliente',
            'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Notas', 'Modificacion_Surtido',
            'Adjuntos', 'Adjuntos_Surtido', 'Estado', 'Estado_Pago', 'Fecha_Completado',
            'Hora_Proceso', 'Turno', 'Surtidor'
        ]

        for col in expected_columns:
            if col not in df.columns:
                df[col] = '' # Inicializa columnas faltantes como cadena vacía

        # Asegura que las columnas de fecha/hora se manejen correctamente
        df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(
            lambda x: str(x) if pd.notna(x) and str(x).strip() != '' else ''
        )

        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce') # Ensure Hora_Proceso is datetime

        # IMPORTANT: Strip whitespace from key columns to ensure correct filtering and finding
        df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
        df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
        df['Turno'] = df['Turno'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()

        return df, worksheet

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Error: La hoja de cálculo con ID '{sheet_id}' no se encontró. Verifica el ID.")
        st.info("ℹ️ Asegúrate de que el GOOGLE_SHEET_ID en tu código sea exactamente el mismo que el de la URL de tu hoja de cálculo.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error: La pestaña '{worksheet_name}' no se encontró en la hoja de cálculo. Verifica el nombre de la pestaña.")
        st.info("ℹ️ Asegúrate de que el GOOGLE_SHEET_WORKSHEET_NAME en tu código sea exactamente el mismo que el nombre de la pestaña en tu hoja de cálculo.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar los datos desde Google Sheets: {e}")
        st.info("ℹ️ Revisa los permisos de tu cuenta de servicio. Debe tener acceso de 'Lector' o 'Editor' a la hoja de cálculo.")
        st.stop()

# --- Data Saving/Updating to Google Sheets ---
def batch_update_gsheet_cells(worksheet, updates_list):
    """
    Realiza múltiples actualizaciones de celdas en una sola solicitud por lotes a Google Sheets
    utilizando worksheet.update_cells().
    updates_list: Lista de diccionarios, cada uno con las claves 'range' y 'values'.
                  Ej: [{'range': 'A1', 'values': [['nuevo_valor']]}, ...]
    """
    try:
        if not updates_list:
            return False

        cell_list = []
        for update_item in updates_list:
            range_str = update_item['range']
            value = update_item['values'][0][0] # Asumiendo un único valor como [['valor']]

            # Convertir la notación A1 (ej. 'A1') a índice de fila y columna (base 1)
            row, col = gspread.utils.a1_to_rowcol(range_str)
            # Crear un objeto Cell y añadirlo a la lista
            cell_list.append(gspread.Cell(row=row, col=col, value=value))

        if cell_list:
            worksheet.update_cells(cell_list) # Este es el método correcto para batch update en el worksheet
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False

# --- Helper Functions for S3 (from app_a.py, not directly used in display but useful) ---
def find_pedido_subfolder_prefix(s3_client_param, parent_prefix, folder_name):
    if not s3_client_param:
        return None

    possible_prefixes = [
        f"{parent_prefix}{folder_name}/",
        f"{parent_prefix}{folder_name}",
        f"adjuntos_pedidos/{folder_name}/",
        f"adjuntos_pedidos/{folder_name}",
        f"{folder_name}/",
        folder_name
    ]

    for pedido_prefix in possible_prefixes:
        try:
            response = s3_client_param.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pedido_prefix,
                MaxKeys=1
            )

            if 'Contents' in response and response['Contents']:
                return pedido_prefix

        except Exception:
            continue

    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=100
        )

        if 'Contents' in response:
            for obj in response['Contents']:
                if folder_name in obj['Key']:
                    if '/' in obj['Key']:
                        prefix_parts = obj['Key'].split('/')[:-1]
                        return '/'.join(prefix_parts) + '/'

    except Exception:
        pass

    return None

def get_files_in_s3_prefix(s3_client_param, prefix):
    if not s3_client_param or not prefix:
        return []

    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=100
        )

        files = []
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'):
                    file_name = item['Key'].split('/')[-1]
                    if file_name:
                        files.append({
                            'title': file_name,
                            'key': item['Key'],
                            'size': item['Size'],
                            'last_modified': item['LastModified']
                        })
        return files

    except Exception as e:
        st.error(f"❌ Error al obtener archivos del prefijo S3 '{prefix}': {e}")
        return []

def get_s3_file_download_url(s3_client_param, object_key):
    if not s3_client_param or not object_key:
        return "#"

    try:
        url = s3_client_param.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=7200
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para '{object_key}': {e}")
        return "#"


# --- Helper Functions from app_almacen.py (adapted for GSheets data) ---

def formatear_fecha_consistente(fecha_str):
    """Convierte cualquier formato de fecha al formato dd/mm/yyyy"""
    if pd.isna(fecha_str) or str(fecha_str).strip() == "Sin fecha" or str(fecha_str).strip() == "":
        return "Sin fecha"

    try:
        # Si ya está en formato dd/mm/yyyy, devolverlo tal como está
        if isinstance(fecha_str, str) and "/" in fecha_str and len(fecha_str.split("/")[2]) == 4:
            return fecha_str

        # Intentar parsear diferentes formatos
        if isinstance(fecha_str, str):
            # Formato YYYY-MM-DD
            if "-" in fecha_str and len(fecha_str.split("-")[0]) == 4:
                fecha_obj = datetime.strptime(fecha_str, "%Y-%m-%d")
                return fecha_obj.strftime("%d/%m/%Y")
            # Formato dd/mm/yyyy (ya correcto)
            elif "/" in fecha_str:
                return fecha_str

        # Si es un objeto datetime o timestamp
        if hasattr(fecha_str, 'strftime'):
            return fecha_str.strftime("%d/%m/%Y")

        return "Sin fecha"
    except Exception:
        return "Sin fecha"

def check_and_update_demorados(df_to_check, worksheet):
    """
    Checks for orders in 'En Proceso' status that have exceeded 1 hour and updates their status to 'Demorado'
    in the DataFrame and Google Sheets. Utiliza actualización por lotes para mayor eficiencia.
    """
    updates_to_perform = []
    updated_indices_df = []
    current_time = datetime.now()
    one_hour_ago = current_time - timedelta(hours=1)
    headers = worksheet.row_values(1) # Obtener los encabezados una sola vez

    try:
        estado_col_index = headers.index('Estado') + 1
    except ValueError:
        st.error("❌ Error interno: Columna 'Estado' no encontrada en los encabezados de Google Sheets.")
        return df_to_check, False

    for idx, row in df_to_check.iterrows():
        if row['Estado'] == "🔵 En Proceso" and pd.notna(row['Hora_Proceso']):
            hora_proceso_dt = pd.to_datetime(row['Hora_Proceso'], errors='coerce')
            if pd.notna(hora_proceso_dt) and hora_proceso_dt < one_hour_ago:
                gsheet_row_index = row.get('_gsheet_row_index') # Usar el índice pre-calculado
                if gsheet_row_index is not None:
                    updates_to_perform.append({
                        'range': f"{gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_index)}",
                        'values': [["🔴 Demorado"]]
                    })
                    df_to_check.loc[idx, "Estado"] = "🔴 Demorado"
                    updated_indices_df.append(idx)
                else:
                    st.warning(f"⚠️ ID_Pedido '{row['ID_Pedido']}' no tiene '_gsheet_row_index' o no se encontró en Google Sheets. No se pudo actualizar el estado a 'Demorado'.")

    if updates_to_perform:
        if batch_update_gsheet_cells(worksheet, updates_to_perform):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            return df_to_check, True
        else:
            st.error("❌ Falló la actualización por lotes de pedidos a 'Demorado'.")
            return df_to_check, False
    return df_to_check, False


def mostrar_resumen_estados(df):
    st.markdown("### 📊 Resumen General")
    total_demorados = df[df["Estado"] == "🔴 Demorado"].shape[0]
    total_en_proceso = df[df["Estado"] == "🔵 En proceso"].shape[0]
    total_pendientes = df[df["Estado"] == "📥 Pendiente"].shape[0]
    total_completados_hoy = df[
        (df["Estado"] == "🟢 Completado") &
        (pd.to_datetime(df["Fecha_Completado"], errors='coerce').dt.date == date.today())
    ].shape[0]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🔴 Demorados", total_demorados)
    with col2:
        st.metric("🔵 En Proceso", total_en_proceso)
    with col3:
        st.metric("📥 Pendientes", total_pendientes)
    with col4:
        st.metric("🟢 Completados Hoy", total_completados_hoy)


# --- Main Application Logic ---
def main():
    df_main, worksheet_main = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

    if not df_main.empty:
        # Asegurarse de que 'Fecha_Completado' sea datetime para el filtro
        df_main['Fecha_Completado'] = pd.to_datetime(df_main['Fecha_Completado'], errors='coerce')
        df_main['Hora_Proceso'] = pd.to_datetime(df_main['Hora_Proceso'], errors='coerce')
        df_main['Hora_Registro'] = pd.to_datetime(df_main['Hora_Registro'], errors='coerce')


        # Actualizar estados a "Demorado" si aplica
        df_main, updated_demorados = check_and_update_demorados(df_main, worksheet_main)
        if updated_demorados:
            # Si hubo actualizaciones, recargar los datos para reflejar los cambios
            # Esto es importante porque check_and_update_demorados modifica el DF localmente
            # y también la hoja de cálculo, pero la visualización necesita el DF actualizado.
            df_main, worksheet_main = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)


        mostrar_resumen_estados(df_main)

        # Filtros para la visualización
        st.sidebar.header("Filtros de Pedidos")
        mostrar_completados = st.sidebar.checkbox("Mostrar Pedidos Completados", value=False)
        mostrar_cancelados = st.sidebar.checkbox("Mostrar Pedidos Cancelados", value=False)

        df_filtrado = df_main.copy()

        if not mostrar_completados:
            df_filtrado = df_filtrado[df_filtrado["Estado"] != "🟢 Completado"]
        if not mostrar_cancelados:
            df_filtrado = df_filtrado[df_filtrado["Estado"] != "⚫ Cancelado"]

        # Ordenar por Hora_Registro para asegurar el orden de llegada
        df_filtrado = df_filtrado.sort_values(by="Hora_Registro", ascending=True)

        # Agrupar pedidos
        grupos_locales = []
        otros_grupos = []

        # Pedidos Locales (Pendientes, En Proceso, Demorados)
        estados_locales = ["📥 Pendiente", "🔵 En Proceso", "🔴 Demorado"]
        df_locales = df_filtrado[
            (df_filtrado["Tipo_Envio"] == "🛵 Local") |
            (df_filtrado["Tipo_Envio"] == "") |
            (pd.isna(df_filtrado["Tipo_Envio"]))
        ].copy()

        for estado in estados_locales:
            grupo = df_locales[df_locales["Estado"] == estado].copy()
            if not grupo.empty:
                emoji = ""
                if estado == "📥 Pendiente":
                    emoji = "📥"
                elif estado == "🔵 En Proceso":
                    emoji = "🔵"
                elif estado == "🔴 Demorado":
                    emoji = "🔴"
                grupos_locales.append((f"{emoji} Local {estado}", grupo))

        # Otros tipos de envío y estados (incluyendo "Completado" y "Cancelado" si se seleccionan)
        otros_tipos_envio = df_filtrado[
            (df_filtrado["Tipo_Envio"] != "🛵 Local") &
            (df_filtrado["Tipo_Envio"] != "") &
            (pd.notna(df_filtrado["Tipo_Envio"]))
        ]["Tipo_Envio"].unique()

        for tipo_envio in otros_tipos_envio:
            grupo = df_filtrado[df_filtrado["Tipo_Envio"] == tipo_envio].copy()
            if not grupo.empty:
                emoji = "📦" # Emoji por defecto para otros tipos de envío
                if tipo_envio == "📬 Solicitud de guía":
                    emoji = "📬"
                elif tipo_envio == "🌍 Foráneo":
                    emoji = "🌍"
                otros_grupos.append((f"{emoji} {tipo_envio}", grupo))

        # Añadir grupos de "Completado" y "Cancelado" si se seleccionaron
        if mostrar_completados:
            grupo_completado = df_filtrado[df_filtrado["Estado"] == "🟢 Completado"].copy()
            if not grupo_completado.empty:
                # Ordenar completados por Fecha_Completado de más reciente a más antiguo
                grupo_completado = grupo_completado.sort_values(by="Fecha_Completado", ascending=False)
                otros_grupos.append(("🟢 Completado", grupo_completado))

        if mostrar_cancelados:
            grupo_cancelado = df_filtrado[df_filtrado["Estado"] == "⚫ Cancelado"].copy()
            if not grupo_cancelado.empty:
                otros_grupos.append(("⚫ Cancelado", grupo_cancelado))

        # Mostrar columnas
        todos_grupos = grupos_locales + otros_grupos
        if todos_grupos:
            cols = st.columns(len(todos_grupos))
            for i, (titulo, df_grupo) in enumerate(todos_grupos):
                with cols[i]:
                    st.markdown(f"#### {titulo}")
                    # Columnas a mostrar
                    columnas_mostrar = ["Cliente", "Hora_Registro", "Estado", "Surtidor"]
                    if "Tipo_Envio" in df_grupo.columns and ("Foráneo" in titulo or "Solicitud de guía" in titulo):
                        columnas_mostrar.append("Tipo_Envio")
                    if "Fecha_Entrega" in df_grupo.columns and ("Foráneo" in titulo or "Solicitud de guía" in titulo):
                        columnas_mostrar.append("Fecha_Entrega")
                    if "Fecha_Completado" in df_grupo.columns and "Completado" in titulo:
                        columnas_mostrar.append("Fecha_Completado")

                    # Renombrar columnas para la visualización
                    df_display = df_grupo[columnas_mostrar].copy()
                    df_display = df_display.rename(columns={
                        "Hora_Registro": "Registro",
                        "Tipo_Envio": "Envío",
                        "Fecha_Entrega": "Entrega",
                        "Fecha_Completado": "Completado"
                    })

                    # Formatear la columna de registro a solo hora si es del día actual, sino fecha y hora
                    df_display['Registro'] = df_display['Registro'].apply(
                        lambda x: x.strftime("%H:%M") if pd.notna(x) and x.date() == date.today() else x.strftime("%d/%m %H:%M") if pd.notna(x) else ""
                    )
                    # Formatear la columna de completado a solo fecha
                    if 'Completado' in df_display.columns:
                        df_display['Completado'] = df_display['Completado'].apply(
                            lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else ""
                        )


                    st.dataframe(
                        df_display.reset_index(drop=True),
                        use_container_width=True,
                        column_config={col: st.column_config.Column(width="small") for col in df_display.columns},
                        hide_index=True
                    )
        else:
            st.info("No hay pedidos para mostrar según los criterios de filtro.")
    else:
        st.info("No hay pedidos activos ni completados en la hoja de cálculo.")

if __name__ == "__main__":
    main()
