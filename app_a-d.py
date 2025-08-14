
import time
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import re
import gspread.utils
import json # Import json for parsing credentials
import os
import uuid
from pytz import timezone


st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

# 🔁 Restaurar pestañas activas si venimos de una acción que modificó datos
if "preserve_main_tab" in st.session_state:
    st.session_state["active_main_tab_index"] = st.session_state.pop("preserve_main_tab", 0)
    st.session_state["active_subtab_local_index"] = st.session_state.pop("preserve_local_tab", 0)
    st.session_state["active_date_tab_m_index"] = st.session_state.pop("preserve_date_tab_m", 0)
    st.session_state["active_date_tab_t_index"] = st.session_state.pop("preserve_date_tab_t", 0)

st.title("📬 Bandeja de Pedidos TD")

# ✅ Versión única con claves para evitar errores de duplicado
col_recarga, col_reintento = st.columns([1, 1])

with col_recarga:
    if st.button("🔄 Recargar Pedidos (seguro)", help="Actualiza datos sin reiniciar pestañas ni scroll", key="btn_recargar_seguro"):
        st.session_state["reload_pedidos_soft"] = True
        st.cache_data.clear()
        st.cache_resource.clear()

with col_reintento:
    if st.button("❌ Reparar Conexión", help="Borra todos los caches y recarga la app", key="btn_reparar_conexion"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()


# --- Google Sheets Constants (pueden venir de st.secrets si se prefiere) ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

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
except KeyError as e:
    st.error(f"❌ Error: Problema al acceder a una clave de AWS S3 en Streamlit secrets. Falta la clave: {e}")
    st.info("Asegúrate de que todas las claves (aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name) estén presentes en la sección [aws].")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- Initialize Session State for tab persistence ---
if "active_main_tab_index" not in st.session_state:
    st.session_state["active_main_tab_index"] = 0
if "active_subtab_local_index" not in st.session_state:
    st.session_state["active_subtab_local_index"] = 0
if "active_date_tab_m_index" not in st.session_state:
    st.session_state["active_date_tab_m_index"] = 0
if "active_date_tab_t_index" not in st.session_state:
    st.session_state["active_date_tab_t_index"] = 0
if "expanded_pedidos" not in st.session_state:
    st.session_state["expanded_pedidos"] = {}
    st.session_state["expanded_attachments"] = {}

# --- Soft reload si el usuario presionó "Recargar Pedidos (seguro)"
if st.session_state.get("reload_pedidos_soft"):
    st.session_state["reload_pedidos_soft"] = False
    st.rerun()  # 🔁 Solo recarga los datos sin perder el estado de pestañas


# --- Cached Clients for Google Sheets and AWS S3 ---

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = dict(_credentials_json_dict)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    try:
        _ = client.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError:
        # Token expirado o inválido → limpiar y regenerar
        st.cache_resource.clear()
        st.warning("🔁 Token expirado. Reintentando autenticación...")

        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        _ = client.open_by_key(GOOGLE_SHEET_ID)

    return client


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
        st.info("ℹ️ Revisa tus credenciales de AWS en st.secrets['aws'] y la configuración de la región.")
        st.stop()

# Initialize clients globally
try:
    # Obtener credenciales de Google Sheets de st.secrets
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [gsheets].")
        st.info("Falta la clave: 'st.secrets has no key \"gsheets\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? More info: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
        st.stop()

    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")


    try:
        g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        s3_client = get_s3_client()
    except gspread.exceptions.APIError as e:
        if "ACCESS_TOKEN_EXPIRED" in str(e) or "UNAUTHENTICATED" in str(e):
            st.cache_resource.clear()
            st.warning("🔄 La sesión con Google Sheets expiró. Reconectando...")
            time.sleep(1)
            g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
            s3_client = get_s3_client()
        else:
            st.error(f"❌ Error al autenticar clientes: {e}")
            st.stop()


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
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud. También, revisa tus credenciales de AWS S3 y Google Sheets en .streamlit/secrets.toml o en la interfaz de Streamlit Cloud.")
    st.stop()


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_data(ttl=60)
def get_raw_sheet_data(sheet_id: str, worksheet_name: str, credentials: dict) -> list[list[str]]:
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials["private_key"] = credentials["private_key"].replace("\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        return worksheet.get_all_values()
    except gspread.exceptions.APIError:
        st.cache_data.clear()  # 🔁 Limpiar la caché en caso de error de token/API
        st.warning("🔁 Token expirado o error de conexión. Reintentando...")
        time.sleep(1)  # Pequeña pausa antes de reintentar  # noqa: F821
        # Reautenticamos
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        return worksheet.get_all_values()


def process_sheet_data(all_data: list[list[str]]) -> tuple[pd.DataFrame, list[str]]:
    """
    Convierte los datos en crudo de Google Sheets en un DataFrame procesado.
    """
    if not all_data:
        return pd.DataFrame(), []

    headers = all_data[0]
    data_rows = all_data[1:]
    df = pd.DataFrame(data_rows, columns=headers)
    df['_gsheet_row_index'] = df.index + 2

    expected_columns = [
        'ID_Pedido', 'Folio_Factura', 'Hora_Registro', 'Vendedor_Registro', 'Cliente',
        'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Modificacion_Surtido',
        'Adjuntos', 'Adjuntos_Surtido', 'Adjuntos_Guia',
        'Estado', 'Estado_Pago', 'Fecha_Completado', 'Hora_Proceso', 'Turno'
    ]



    for col in expected_columns:
        if col not in df.columns:
            df[col] = ''

    df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(
        lambda x: str(x) if pd.notna(x) and str(x).strip() != '' else ''
    )

    df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
    df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
    df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce')

    df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
    df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
    df['Turno'] = df['Turno'].astype(str).str.strip()
    df['Estado'] = df['Estado'].astype(str).str.strip()

    return df, headers


def update_gsheet_cell(worksheet, headers, row_index, col_name, value):
    """
    Actualiza una celda específica en Google Sheets.
    row_index es el índice de fila de gspread (base 1).
    col_name es el nombre de la columna.
    headers es la lista de encabezados obtenida previamente.
    """
    try:
        if col_name not in headers:
            st.error(f"❌ Error: La columna '{col_name}' no se encontró en Google Sheets para la actualización. Verifica los encabezados.")
            return False
        col_index = headers.index(col_name) + 1 # Convertir a índice base 1 de gspread
        worksheet.update_cell(row_index, col_index, value)
        # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la celda ({row_index}, {col_name}) en Google Sheets: {e}")
        return False
    
def cargar_pedidos_desde_google_sheet(sheet_id, worksheet_name):
    """
    Carga los datos de una hoja de Google Sheets y devuelve un DataFrame y los encabezados.
    """
    try:
        client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
        headers = worksheet.row_values(1)

        if headers:
            df = pd.DataFrame(worksheet.get_all_records())
            return df, headers
        else:
            return pd.DataFrame(), []
    except Exception as e:
        st.error(f"❌ Error al cargar la hoja {worksheet_name}: {e}")
        return pd.DataFrame(), []


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
            # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False
    

# --- AWS S3 Helper Functions (Copied from app_admin.py directly) ---
def upload_file_to_s3(s3_client_param, bucket_name, file_obj, s3_key):
    try:
        s3_client_param.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_obj.getvalue()
        )

        # Ya no necesitamos presigned_url, podemos armar la URL pública directa:
        url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, url

    except Exception as e:
        st.error(f"❌ Error al subir archivo a S3: {e}")
        return False, None

# --- AWS S3 Helper Functions (Copied from app_admin.py directly) ---

def find_pedido_subfolder_prefix(s3_client_param, parent_prefix, folder_name):
    """
    Finds the correct S3 prefix for a given order folder.
    Searches for various possible prefix formats.
    """
    if not s3_client_param:
        return None

    possible_prefixes = [
        f"{parent_prefix}{folder_name}/",
        f"{parent_prefix}{folder_name}",
        f"adjuntos_pedidos/{folder_name}/", # Fallback if parent_prefix is not correctly set
        f"adjuntos_pedidos/{folder_name}",
        f"{folder_name}/", # Even more general fallback
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
            # Continue to the next prefix if there's an error with the current one
            continue

    # If direct prefix search fails, try a broader search
    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=100 # Adjust MaxKeys or implement pagination if many objects are expected
        )

        if 'Contents' in response:
            for obj in response['Contents']:
                if folder_name in obj['Key']:
                    if '/' in obj['Key']:
                        prefix_parts = obj['Key'].split('/')[:-1]
                        return '/'.join(prefix_parts) + '/'

    except Exception:
        pass # Silently fail if broader search also has issues

    return None

def get_files_in_s3_prefix(s3_client_param, prefix):
    """
    Retrieves a list of files within a given S3 prefix.
    """
    if not s3_client_param or not prefix:
        return []

    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=100 # Adjust MaxKeys or implement pagination if many files are expected
        )

        files = []
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'): # Exclude folders
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
    """
    Retorna una URL pública permanente para archivos subidos con ACL='public-read'.
    """
    return f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{object_key}"


# --- Helper Functions (existing in app.py) ---

def ordenar_pedidos_custom(df_pedidos_filtrados):
    """
    Ordena el DataFrame con:
    1. Modificación de Surtido (sin importar hora)
    2. Demorados
    3. Pendientes / En Proceso (los más viejos arriba)
    """
    if df_pedidos_filtrados.empty:
        return df_pedidos_filtrados

    # Asegurar datetime para ordenar por antigüedad
    df_pedidos_filtrados['Hora_Registro_dt'] = pd.to_datetime(df_pedidos_filtrados['Hora_Registro'], errors='coerce')

    def get_sort_key(row):
        mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
        refact_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
        tiene_modificacion_sin_confirmar = (
            mod_texto and
            not mod_texto.endswith("[✔CONFIRMADO]") and
            refact_tipo != "Datos Fiscales"
        )


        if tiene_modificacion_sin_confirmar:
            return (0, pd.Timestamp.min)  # Arriba del todo si no está confirmada

        if row["Estado"] == "🔴 Demorado":
            return (1, pd.Timestamp.min)  # Justo debajo

        return (2, row['Hora_Registro_dt'] if pd.notna(row['Hora_Registro_dt']) else pd.Timestamp.max)


    df_pedidos_filtrados['custom_sort_key'] = df_pedidos_filtrados.apply(get_sort_key, axis=1)

    df_sorted = df_pedidos_filtrados.sort_values(by='custom_sort_key', ascending=True)

    return df_sorted.drop(columns=['custom_sort_key', 'Hora_Registro_dt'])

def check_and_update_demorados(df_to_check, worksheet, headers):
    """
    Revisa pedidos en estado '🟡 Pendiente' que lleven más de 1 hora desde su registro
    y los actualiza a '🔴 Demorado'.
    """
    updates_to_perform = []
    zona_mexico = timezone("America/Mexico_City")
    current_time = datetime.now(zona_mexico)

    try:
        estado_col_index = headers.index('Estado') + 1
    except ValueError:
        st.error("❌ Error interno: Columna 'Estado' no encontrada en los encabezados de Google Sheets.")
        return df_to_check, False

    changes_made = False

    for idx, row in df_to_check.iterrows():
        if row['Estado'] != "🟡 Pendiente":
            continue

        hora_registro = pd.to_datetime(row.get('Hora_Registro'), errors='coerce')
        gsheet_row_index = row.get('_gsheet_row_index')

        if pd.notna(hora_registro):
            hora_registro = hora_registro.tz_localize("America/Mexico_City") if hora_registro.tzinfo is None else hora_registro
            if (current_time - hora_registro).total_seconds() > 3600 and gsheet_row_index is not None:
                updates_to_perform.append({
                    'range': f"{gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_index)}",
                    'values': [["🔴 Demorado"]]
                })
                df_to_check.loc[idx, "Estado"] = "🔴 Demorado"
                changes_made = True

    if updates_to_perform:
        if batch_update_gsheet_cells(worksheet, updates_to_perform):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            return df_to_check, changes_made
        else:
            st.error("❌ Falló la actualización por lotes a 'Demorado'.")
            return df_to_check, False

    return df_to_check, False

def fijar_estado_pestanas_guia(row, origen_tab):
    st.session_state["pedido_editado"] = row['ID_Pedido']
    st.session_state["fecha_seleccionada"] = row.get("Fecha_Entrega", "")
    st.session_state["subtab_local"] = origen_tab
    st.session_state["active_main_tab_index"] = st.session_state.get("active_main_tab_index", 0)
    st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
    st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
    st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)

def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers, s3_client_param):
    """
    Displays a single order with its details, actions, and attachments.
    Includes logic for updating status, surtidor, notes, and handling attachments.
    """

    surtido_files_in_s3 = []  # ✅ Garantiza que la variable exista siempre
    pedido_folder_prefix = None  # ✅ Garantiza que esté definido aunque no se haya expandido adjuntos

    gsheet_row_index = row.get('_gsheet_row_index')
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{row['ID_Pedido']}'.")
        return

    folio = row.get("Folio_Factura", "").strip() or row['ID_Pedido']
    st.markdown(f'<a name="pedido_{row["ID_Pedido"]}"></a>', unsafe_allow_html=True)
    with st.expander(f"{row['Estado']} - {folio} - {row['Cliente']}", expanded=st.session_state["expanded_pedidos"].get(row['ID_Pedido'], False)):  
        st.markdown("---")
        mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
        hay_modificacion = mod_texto != ""


        # --- Cambiar Fecha y Turno ---
        if row['Estado'] != "🟢 Completado" and row.get("Tipo_Envio") in ["📍 Pedido Local", "🚚 Pedido Foráneo"]:
            st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
            st.markdown("##### 📅 Cambiar Fecha y Turno")
            col_current_info_date, col_current_info_turno, col_inputs = st.columns([1, 1, 2])

            fecha_actual_str = row.get("Fecha_Entrega", "")
            fecha_actual_dt = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            fecha_mostrar = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else "Sin fecha"
            col_current_info_date.info(f"**Fecha actual:** {fecha_mostrar}")

            current_turno = row.get("Turno", "")
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                col_current_info_turno.info(f"**Turno actual:** {current_turno}")
            else:
                col_current_info_turno.empty()

            today = datetime.now().date()
            default_fecha = fecha_actual_dt.date() if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today else today

            fecha_key = f"new_fecha_{row['ID_Pedido']}"
            turno_key = f"new_turno_{row['ID_Pedido']}"

            if fecha_key not in st.session_state:
                st.session_state[fecha_key] = default_fecha
            if turno_key not in st.session_state:
                st.session_state[turno_key] = current_turno

            st.date_input(
                "Nueva fecha:",
                value=st.session_state[fecha_key],
                min_value=today,
                max_value=today + timedelta(days=365),
                format="DD/MM/YYYY",
                key=fecha_key,
            )

            if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                turno_options = ["", "☀️ Local Mañana", "🌙 Local Tarde"]
                if st.session_state[turno_key] not in turno_options:
                    st.session_state[turno_key] = turno_options[0]

                st.selectbox(
                    "Clasificar turno como:",
                    options=turno_options,
                    key=turno_key,
                )

            if st.button("✅ Aplicar Cambios de Fecha/Turno", key=f"btn_apply_{row['ID_Pedido']}"):
                st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                cambios = []
                nueva_fecha_str = st.session_state[fecha_key].strftime('%Y-%m-%d')

                if nueva_fecha_str != fecha_actual_str:
                    col_idx = headers.index("Fecha_Entrega") + 1
                    cambios.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx), 'values': [[nueva_fecha_str]]})

                if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                    nuevo_turno = st.session_state[turno_key]
                    if nuevo_turno != current_turno:
                        col_idx = headers.index("Turno") + 1
                        cambios.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx), 'values': [[nuevo_turno]]})

                if cambios:
                    if batch_update_gsheet_cells(worksheet, cambios):
                        if "Fecha_Entrega" in headers:
                            df.at[idx, "Fecha_Entrega"] = nueva_fecha_str
                        if "Turno" in headers and row.get("Tipo_Envio") == "📍 Pedido Local":
                            df.at[idx, "Turno"] = st.session_state[turno_key]

                        st.toast(f"📅 Pedido {row['ID_Pedido']} actualizado.", icon="✅")
                    else:
                        st.error("❌ Falló la actualización en Google Sheets.")
                else:
                    st.info("No hubo cambios para aplicar.")


        
        st.markdown("---")

        # --- Main Order Layout ---
        # This section displays the core information of the order
        disabled_if_completed = (row['Estado'] == "🟢 Completado")

        col_order_num, col_client, col_time, col_status, col_vendedor, col_print_btn, col_complete_btn = st.columns([0.5, 2, 1.5, 1, 1.2, 1, 1])
        # --- Mostrar Comentario (si existe)
        comentario = str(row.get("Comentario", "")).strip()
        if comentario:
            st.markdown("##### 📝 Comentario del Pedido")
            st.info(comentario)


        col_order_num.write(f"**{orden}**")
        folio_factura = row.get("Folio_Factura", "").strip()
        cliente = row.get("Cliente", "").strip()
        col_client.markdown(f"📄 **{folio_factura}**  \n🤝 **{cliente}**")

        hora_registro_dt = pd.to_datetime(row['Hora_Registro'], errors='coerce')
        if pd.notna(hora_registro_dt):
            col_time.write(f"🕒 {hora_registro_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            col_time.write("")

        col_status.write(f"{row['Estado']}")

        vendedor_registro = row.get("Vendedor_Registro", "")
        col_vendedor.write(f"👤 {vendedor_registro}")



        # ✅ PRINT and UPDATE TO "IN PROCESS"
        # 🧠 Preservar pestañas activas para evitar cambio visual
        st.session_state["preserve_main_tab"] = st.session_state.get("active_main_tab_index", 0)
        st.session_state["preserve_local_tab"] = st.session_state.get("active_subtab_local_index", 0)
        st.session_state["preserve_date_tab_m"] = st.session_state.get("active_date_tab_m_index", 0)
        st.session_state["preserve_date_tab_t"] = st.session_state.get("active_date_tab_t_index", 0)

        if col_print_btn.button("🖨 Imprimir", key=f"print_{row['ID_Pedido']}_{origen_tab}"):
            # ✅ Expandir el pedido y sus adjuntos
            st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
            st.session_state["expanded_attachments"][row['ID_Pedido']] = True

            # ✅ Solo actualizar si estaba en Pendiente o Demorado
            if row["Estado"] in ["🟡 Pendiente", "🔴 Demorado"]:
                zona_mexico = timezone("America/Mexico_City")
                now = datetime.now(zona_mexico)
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")

                estado_col_idx = headers.index("Estado") + 1
                hora_proc_col_idx = headers.index("Hora_Proceso") + 1

                updates = [
                    {'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx), 'values': [["🔵 En Proceso"]]},
                    {'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proc_col_idx), 'values': [[now_str]]}
                ]
                if batch_update_gsheet_cells(worksheet, updates):
                    df.at[idx, "Estado"] = "🔵 En Proceso"
                    df.at[idx, "Hora_Proceso"] = now_str
                    row["Estado"] = "🔵 En Proceso"  # ✅ Refleja el cambio en pantalla
                    st.toast("📄 Estado actualizado a 'En Proceso'", icon="📌")
                else:
                    st.error("❌ Falló la actualización del estado a 'En Proceso'.")



        # This block displays attachments if they are expanded
        if st.session_state["expanded_attachments"].get(row["ID_Pedido"], False):
            st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")
            pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client_param, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                files_in_folder = get_files_in_s3_prefix(s3_client_param, pedido_folder_prefix)
                if files_in_folder:
                    filtered_files_to_display = [
                        f for f in files_in_folder
                        if "comprobante" not in f['title'].lower() and "surtido" not in f['title'].lower()
                    ]
                    if filtered_files_to_display:
                        for file_info in filtered_files_to_display:
                            file_url = get_s3_file_download_url(s3_client_param, file_info['key'])
                            display_name = file_info['title']
                            if row['ID_Pedido'] in display_name:
                                display_name = display_name.replace(row['ID_Pedido'], "").replace("__", "_").replace("_-", "_").replace("-_", "_").strip('_').strip('-')
                            st.markdown(f"- 📄 **{display_name}** ([🔗 Ver/Descargar]({file_url}))")
                    else:
                        st.info("No hay adjuntos para mostrar (excluyendo comprobantes y surtidos).")
                else:
                    st.info("No se encontraron archivos en la carpeta del pedido en S3.")
            else:
                st.error(f"❌ No se encontró la carpeta (prefijo S3) del pedido '{row['ID_Pedido']}'.")


        # Complete Button
        if col_complete_btn.button("🟢 Completar", key=f"complete_button_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            try:
                updates = []
                estado_col_idx = headers.index('Estado') + 1
                fecha_completado_col_idx = headers.index('Fecha_Completado') + 1

                zona_mexico = timezone("America/Mexico_City")
                now = datetime.now(zona_mexico)
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")


                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                    'values': [["🟢 Completado"]]
                })
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                    'values': [[now_str]]
                })

                if batch_update_gsheet_cells(worksheet, updates):
                    df.loc[idx, "Estado"] = "🟢 Completado"
                    df.loc[idx, "Fecha_Completado"] = now
                    st.success(f"✅ Pedido {row['ID_Pedido']} completado exitosamente.")

                    # 🔁 Mantener pestaña activa
                    st.session_state["pedido_editado"] = row['ID_Pedido']
                    st.session_state["fecha_seleccionada"] = row.get("Fecha_Entrega", "")
                    st.session_state["subtab_local"] = origen_tab

                    st.cache_data.clear()

                    st.session_state["active_main_tab_index"] = st.session_state.get("active_main_tab_index", 0)
                    st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
                    st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
                    st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)
                    st.rerun()
                else:
                    st.error("❌ No se pudo completar el pedido.")
            except Exception as e:
                st.error(f"Error al completar el pedido: {e}")

                
        # ✅ BOTÓN PROCESAR MODIFICACIÓN - Solo para pedidos con estado 🛠 Modificación
        if row['Estado'] == "🛠 Modificación":
            col_process_mod = st.columns(1)[0]  # Crear columna para el botón
            if col_process_mod.button("🔧 Procesar Modificación", key=f"process_mod_{row['ID_Pedido']}_{origen_tab}"):
                try:
                    # 🧠 Preservar pestañas activas
                    st.session_state["preserve_main_tab"] = st.session_state.get("active_main_tab_index", 0)
                    st.session_state["preserve_local_tab"] = st.session_state.get("active_subtab_local_index", 0)
                    st.session_state["preserve_date_tab_m"] = st.session_state.get("active_date_tab_m_index", 0)
                    st.session_state["preserve_date_tab_t"] = st.session_state.get("active_date_tab_t_index", 0)
                    
                    # ✅ Expandir el pedido
                    st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                    
                    # 🔄 Actualizar solo el estado a "En Proceso"
                    estado_col_idx = headers.index("Estado") + 1
                    updates = [
                        {'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx), 'values': [["🔵 En Proceso"]]}
                    ]
                    
                    if batch_update_gsheet_cells(worksheet, updates):
                        # ✅ Actualizar el DataFrame y la fila localmente
                        df.at[idx, "Estado"] = "🔵 En Proceso"
                        row["Estado"] = "🔵 En Proceso"  # Refleja el cambio en pantalla
                        
                        st.toast("🔧 Modificación procesada - Estado actualizado a 'En Proceso'", icon="✅")
                        
                        # 🔁 Mantener pestañas activas
                        st.session_state["active_main_tab_index"] = st.session_state.get("active_main_tab_index", 0)
                        st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
                        st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
                        st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)
                        
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Falló la actualización del estado a 'En Proceso'.")
                        
                except Exception as e:
                    st.error(f"❌ Error al procesar la modificación: {e}")

        # --- Adjuntar archivos de guía ---
        if row['Estado'] != "🟢 Completado":
            with st.expander("📦 Subir Archivos de Guía"):
                upload_key = f"file_guia_{row['ID_Pedido']}"

                archivos_guia = st.file_uploader(
                    "📎 Subir guía(s) del pedido",
                    type=["pdf", "jpg", "jpeg", "png"],
                    accept_multiple_files=True,
                    key=upload_key
                )

                if archivos_guia:
                    fijar_estado_pestanas_guia(row, origen_tab)
                    st.session_state["expanded_pedidos"][row['ID_Pedido']] = True  # ✅ se mantiene expandido

                    # 🧠 Preservar pestañas activas antes de subir guía
                    st.session_state["preserve_main_tab"] = st.session_state.get("active_main_tab_index", 0)
                    st.session_state["preserve_local_tab"] = st.session_state.get("active_subtab_local_index", 0)
                    st.session_state["preserve_date_tab_m"] = st.session_state.get("active_date_tab_m_index", 0)
                    st.session_state["preserve_date_tab_t"] = st.session_state.get("active_date_tab_t_index", 0)

                    if st.button("📤 Subir Guía", key=f"btn_subir_guia_{row['ID_Pedido']}"):

                        st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                        st.session_state["expanded_attachments"][row['ID_Pedido']] = True
                        uploaded_urls = []

                        for archivo in archivos_guia:
                            ext = os.path.splitext(archivo.name)[1]
                            s3_key = f"{row['ID_Pedido']}/guia_{uuid.uuid4().hex[:6]}{ext}"
                            success, url = upload_file_to_s3(s3_client_param, S3_BUCKET_NAME, archivo, s3_key)
                            if success:
                                uploaded_urls.append(url)

                        if uploaded_urls:
                            anterior = str(row.get("Adjuntos_Guia", "")).strip()
                            nueva_lista = anterior + ", " + ", ".join(uploaded_urls) if anterior else ", ".join(uploaded_urls)

                            success = update_gsheet_cell(worksheet, headers, gsheet_row_index, "Adjuntos_Guia", nueva_lista)
                            if success:
                                df.at[idx, "Adjuntos_Guia"] = nueva_lista
                                row["Adjuntos_Guia"] = nueva_lista  # ✅ refleja el cambio localmente
                                st.toast(f"📤 {len(uploaded_urls)} guía(s) subida(s) con éxito.", icon="📦")
                                st.success(f"📦 Se subieron correctamente {len(uploaded_urls)} archivo(s) de guía.")
                            else:
                                st.error("❌ No se pudo actualizar el Google Sheet con los archivos de guía.")
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")


        refact_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
        refact_subtipo = str(row.get("Refacturacion_Subtipo", "")).strip()

        if hay_modificacion:
            # 🟡 Si NO es refacturación por Datos Fiscales
            if refact_tipo != "Datos Fiscales":
                if mod_texto.endswith('[✔CONFIRMADO]'):
                    st.info(f"🟡 Modificación de Surtido:\n{mod_texto}")
                else:
                    st.warning(f"🟡 Modificación de Surtido:\n{mod_texto}")
                    if st.button("✅ Confirmar Cambios de Surtido", key=f"confirm_mod_{row['ID_Pedido']}"):
                        st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                        st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
                        nuevo_texto = mod_texto + " [✔CONFIRMADO]"
                        success = update_gsheet_cell(worksheet, headers, gsheet_row_index, "Modificacion_Surtido", nuevo_texto)
                        if success:
                            st.success("✅ Cambios de surtido confirmados.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ No se pudo confirmar la modificación.")
                
                # Mostrar info adicional si es refacturación por material
                if refact_tipo == "Material":
                    st.markdown("#### 🔁 Refacturación por Material")
                    st.info(f"📌 Tipo: **{refact_tipo}**  \n🔧 Subtipo: **{refact_subtipo}**")

            # ℹ️ Si es refacturación por Datos Fiscales
            elif refact_tipo == "Datos Fiscales":
                st.info("ℹ️ Esta modificación fue marcada como **Datos Fiscales**. Se muestra como referencia pero no requiere confirmación.")
                if mod_texto:
                    st.info(f"✉️ Modificación (Datos Fiscales):\n{mod_texto}")

            # Archivos mencionados en el texto
            mod_surtido_archivos_mencionados_raw = []
            for linea in mod_texto.split('\n'):
                match = re.search(r'\(Adjunto: (.+?)\)', linea)
                if match:
                    mod_surtido_archivos_mencionados_raw.extend([f.strip() for f in match.group(1).split(',')])

            # Buscar en S3
            if pedido_folder_prefix is None:
                pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client_param, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            surtido_files_in_s3 = []
            if pedido_folder_prefix:
                all_files_in_folder = get_files_in_s3_prefix(s3_client_param, pedido_folder_prefix)
                surtido_files_in_s3 = [
                    f for f in all_files_in_folder
                    if "surtido" in f['title'].lower()
                ]

            all_surtido_related_files = []
            for f_name in mod_surtido_archivos_mencionados_raw:
                cleaned_f_name = f_name.split('/')[-1]
                all_surtido_related_files.append({
                    'title': cleaned_f_name,
                    'key': f"{pedido_folder_prefix}{cleaned_f_name}"
                })

            for s_file in surtido_files_in_s3:
                if not any(s_file['title'] == existing_f['title'] for existing_f in all_surtido_related_files):
                    all_surtido_related_files.append(s_file)

            if all_surtido_related_files:
                st.markdown("Adjuntos de Modificación (Surtido/Relacionados):")
                archivos_ya_mostrados_para_mod = set()

                for file_info in all_surtido_related_files:
                    file_name_to_display = file_info['title']
                    object_key_to_download = file_info['key']

                    if file_name_to_display in archivos_ya_mostrados_para_mod:
                        continue

                    try:
                        if not object_key_to_download.startswith(S3_ATTACHMENT_PREFIX) and pedido_folder_prefix:
                            object_key_to_download = f"{pedido_folder_prefix}{file_name_to_display}"

                        if not pedido_folder_prefix and not object_key_to_download.startswith(S3_BUCKET_NAME):
                            st.warning(f"⚠️ No se pudo determinar la ruta S3 para: {file_name_to_display}")
                            continue

                        presigned_url = get_s3_file_download_url(s3_client_param, object_key_to_download)
                        if presigned_url and presigned_url != "#":
                            st.markdown(f"- 📄 [{file_name_to_display}]({presigned_url})")
                        else:
                            st.warning(f"⚠️ No se pudo generar el enlace para: {file_name_to_display}")
                    except Exception as e:
                        st.warning(f"⚠️ Error al procesar adjunto de modificación '{file_name_to_display}': {e}")

                    archivos_ya_mostrados_para_mod.add(file_name_to_display)
            else:
                st.info("No hay adjuntos específicos para esta modificación de surtido mencionados en el texto.")


    # --- Scroll automático al pedido impreso (si corresponde) ---
    if st.session_state.get("scroll_to_pedido_id") == row["ID_Pedido"]:
        import streamlit.components.v1 as components
        components.html(f"""
            <script>
                const el = document.querySelector('a[name="pedido_{row["ID_Pedido"]}"]');
                if (el) {{
                    el.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                }}
            </script>
        """, height=0)
        st.session_state["scroll_to_pedido_id"] = None

def mostrar_pedido_solo_guia(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers, s3_client_param):
    """
    Render minimalista SOLO para subir guía y marcar como completado.
    - Sin botones de imprimir/completar
    - Sin lógica de modificación de surtido
    - El bloque de guía siempre visible
    - Muestra el comentario del pedido si existe
    - Al subir guía => actualiza Adjuntos_Guia y cambia a 🟢 Completado + Fecha_Completado
    """
    gsheet_row_index = row.get('_gsheet_row_index')
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se obtuvo _gsheet_row_index para '{row.get('ID_Pedido','?')}'.")
        return

    folio = (row.get("Folio_Factura", "") or "").strip() or row['ID_Pedido']
    st.markdown(f'<a name="pedido_{row["ID_Pedido"]}"></a>', unsafe_allow_html=True)

    # Expander simple con info básica (sin acciones extra)
    with st.expander(f"{row['Estado']} - {folio} - {row.get('Cliente','')}", expanded=True):
        st.markdown("---")

        # Cabecera compacta
        col_order_num, col_client, col_time, col_status, col_vendedor = st.columns([0.5, 2, 1.6, 1, 1.2])
        col_order_num.write(f"**{orden}**")
        col_client.markdown(f"📄 **{folio}**  \n🤝 **{row.get('Cliente','')}**")

        hora_registro_dt = pd.to_datetime(row.get('Hora_Registro', ''), errors='coerce')
        col_time.write(f"🕒 {hora_registro_dt.strftime('%Y-%m-%d %H:%M:%S')}" if pd.notna(hora_registro_dt) else "")
        col_status.write(f"{row['Estado']}")
        col_vendedor.write(f"👤 {row.get('Vendedor_Registro','')}")

        # 📝 Comentario del pedido (NUEVO)
        comentario = str(row.get("Comentario", "")).strip()
        if comentario:
            st.markdown("##### 📝 Comentario del Pedido")
            st.info(comentario)

        st.markdown("---")
        st.markdown("### 📦 Subir Archivos de Guía")

        # Uploader siempre visible (sin expander)
        upload_key = f"file_guia_only_{row['ID_Pedido']}"
        archivos_guia = st.file_uploader(
            "📎 Subir guía(s) del pedido",
            type=["pdf", "jpg", "jpeg", "png"],
            accept_multiple_files=True,
            key=upload_key
        )

        # --- Botón para subir guía y completar ---
        if st.button("📤 Subir Guía y Completar", key=f"btn_subir_guia_only_{row['ID_Pedido']}"):
            # ✅ Validación: al menos un archivo
            if not archivos_guia:
                st.warning("⚠️ Primero sube al menos un archivo de guía.")
                st.stop()

            uploaded_urls = []
            for archivo in archivos_guia:
                ext = os.path.splitext(archivo.name)[1]
                s3_key = f"{row['ID_Pedido']}/guia_{uuid.uuid4().hex[:6]}{ext}"
                success, url = upload_file_to_s3(s3_client_param, S3_BUCKET_NAME, archivo, s3_key)
                if success and url:
                    uploaded_urls.append(url)

            # Construir nueva lista de URLs
            nueva_lista = str(row.get("Adjuntos_Guia", "")).strip()
            if uploaded_urls:
                nueva_lista = (nueva_lista + ", " if nueva_lista else "") + ", ".join(uploaded_urls)

            # Preparar updates a Google Sheets
            updates = []

            if "Adjuntos_Guia" in headers:
                col_idx = headers.index("Adjuntos_Guia") + 1
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx),
                    'values': [[nueva_lista]]
                })

            if "Estado" in headers:
                col_idx = headers.index("Estado") + 1
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx),
                    'values': [["🟢 Completado"]]
                })

            mx_now = datetime.now(timezone("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
            if "Fecha_Completado" in headers:
                col_idx = headers.index("Fecha_Completado") + 1
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx),
                    'values': [[mx_now]]
                })

            # Ejecutar actualización en lote
            if updates and batch_update_gsheet_cells(worksheet, updates):
                # Refrescar DataFrame local para reflejo inmediato
                if uploaded_urls:
                    df.at[idx, "Adjuntos_Guia"] = nueva_lista
                    row["Adjuntos_Guia"] = nueva_lista
                df.at[idx, "Estado"] = "🟢 Completado"
                df.at[idx, "Fecha_Completado"] = mx_now

                st.toast(f"📤 {len(uploaded_urls)} guía(s) subida(s). Pedido completado.", icon="✅")
                st.success("✅ Pedido marcado como **🟢 Completado**.")

                # 🔒 Permanecer en 📋 Solicitudes de Guía (índice 3)
                st.session_state["active_main_tab_index"] = 3
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("❌ No se pudo actualizar Google Sheets con la guía y/o el estado.")


# --- Main Application Logic ---

raw_data = get_raw_sheet_data(
    sheet_id=GOOGLE_SHEET_ID,
    worksheet_name=GOOGLE_SHEET_WORKSHEET_NAME,
    credentials=GSHEETS_CREDENTIALS
)
df_main, headers_main = process_sheet_data(raw_data)


if not df_main.empty:
    df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
    if changes_made_by_demorado_check:
        st.cache_data.clear()

        st.session_state["active_main_tab_index"] = st.session_state.get("active_main_tab_index", 0)
        st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
        st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
        st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)

        st.rerun()

    # --- 🔔 Alerta de Modificación de Surtido ---  
    mod_surtido_df = df_main[
        (df_main['Modificacion_Surtido'].astype(str).str.strip() != '') &
        (~df_main['Modificacion_Surtido'].astype(str).str.endswith('[✔CONFIRMADO]')) &
        (df_main['Estado'] != '🟢 Completado') &
        (df_main['Refacturacion_Tipo'].fillna("").str.strip() != "Datos Fiscales")
    ]


    mod_surtido_count = len(mod_surtido_df)

    if mod_surtido_count > 0:
        ubicaciones = []
        for _, row in mod_surtido_df.iterrows():
            tipo = row.get("Tipo_Envio", "")
            turno = row.get("Turno", "")
            if tipo == "📍 Pedido Local":
                if "Mañana" in turno:
                    ubicaciones.append("📍 Local / Mañana")
                elif "Tarde" in turno:
                    ubicaciones.append("📍 Local / Tarde")
                elif "Saltillo" in turno:
                    ubicaciones.append("📍 Local / Saltillo")
                elif "Bodega" in turno:
                    ubicaciones.append("📍 Local / Bodega")
                else:
                    ubicaciones.append("📍 Local")
            elif tipo == "🚚 Pedido Foráneo":
                ubicaciones.append("🚚 Foráneo")
            elif tipo == "🔁 Devolución":
                ubicaciones.append("🔁 Devolución")
            elif tipo == "🛠 Garantía":
                ubicaciones.append("🛠 Garantía")

        ubicaciones = sorted(set(ubicaciones))
        ubicaciones_str = ", ".join(ubicaciones)

        st.warning(f"⚠️ Hay {mod_surtido_count} pedido(s) con **Modificación de Surtido** ➤ {ubicaciones_str}")

    df_pendientes_proceso_demorado = df_main[df_main["Estado"].isin(["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado", "🛠 Modificación"])].copy()
    df_completados_historial = df_main[df_main["Estado"] == "🟢 Completado"].copy()

    st.markdown("### 📊 Resumen de Estados")

    # Contador corregido que excluye completados ya limpiados
    completados_visibles = df_main[
        (df_main['Estado'] == '🟢 Completado') &
        (df_main.get('Completados_Limpiado', '').astype(str).str.lower() != 'sí')
    ]

    # Contadores por estado (incluye 🟣 Cancelado)
    estado_counts = {
        '🟡 Pendiente': (df_main['Estado'] == '🟡 Pendiente').sum(),
        '🔵 En Proceso': (df_main['Estado'] == '🔵 En Proceso').sum(),
        '🔴 Demorado': (df_main['Estado'] == '🔴 Demorado').sum(),
        '🛠 Modificación': (df_main['Estado'] == '🛠 Modificación').sum(),
        '🟣 Cancelado': (df_main['Estado'] == '🟣 Cancelado').sum(),
        '🟢 Completado': len(completados_visibles),
    }

    # Total de pedidos sumando todos los estados
    total_pedidos_estados = sum(estado_counts.values())

    # Estados siempre visibles
    estados_fijos = ['🟡 Pendiente', '🔵 En Proceso', '🟢 Completado']

    # Estados dinámicos (solo si > 0)
    estados_condicionales = ['🔴 Demorado', '🛠 Modificación', '🟣 Cancelado']

    # Construcción de la lista a mostrar
    estados_a_mostrar = []

    # Total primero
    estados_a_mostrar.append(("📦 Total Pedidos", total_pedidos_estados))

    # Fijos (siempre)
    for estado in estados_fijos:
        estados_a_mostrar.append((estado, estado_counts[estado]))

    # Dinámicos (solo si hay > 0)
    for estado in estados_condicionales:
        cantidad = estado_counts.get(estado, 0)
        if cantidad > 0:
            estados_a_mostrar.append((estado, cantidad))

    # Render de métricas
    cols = st.columns(len(estados_a_mostrar))
    for col, (nombre_estado, cantidad) in zip(cols, estados_a_mostrar):
        col.metric(nombre_estado, int(cantidad))


    # === CASOS ESPECIALES (Devoluciones/Garantías) ===
    df_casos, headers_casos = cargar_pedidos_desde_google_sheet(GOOGLE_SHEET_ID, "casos_especiales")
    gspread_client_tmp = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    worksheet_casos = gspread_client_tmp.open_by_key(GOOGLE_SHEET_ID).worksheet("casos_especiales")

    # Asegurar columnas clave para no fallar si faltan
    for col in ["ID_Pedido","Cliente","Folio_Factura","Tipo_Envio","Estado_Caso",
                "Fecha_Recepcion_Devolucion","Estado_Recepcion",
                "Nota_Credito_URL","Documento_Adicional_URL","Comentarios_Admin_Devolucion"]:
        if col not in df_casos.columns:
            df_casos[col] = ""


    # --- Implementación de Pestañas con st.tabs ---
    tab_options = [
        "📍 Pedidos Locales",
        "🚚 Pedidos Foráneos",
        "🏙️ Pedidos CDMX",
        "📋 Solicitudes de Guía",
        "🔁 Devoluciones",
        "🛠 Garantías",
        "✅ Historial Completados",
    ]
    main_tabs = st.tabs(tab_options)

    with main_tabs[0]: # 📍 Pedidos Locales
        st.markdown("### 📋 Pedidos Locales")
        subtab_options_local = ["🌅 Mañana", "🌇 Tarde", "⛰️ Saltillo", "📦 En Bodega"]
        
        subtabs_local = st.tabs(subtab_options_local)

        with subtabs_local[0]: # 🌅 Mañana
            pedidos_m_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "☀️ Local Mañana")
            ].copy()
            if not pedidos_m_display.empty:
                pedidos_m_display['Fecha_Entrega_dt'] = pd.to_datetime(pedidos_m_display['Fecha_Entrega'], errors='coerce')
                fechas_unicas_dt = sorted(pedidos_m_display["Fecha_Entrega_dt"].dropna().unique())

                if fechas_unicas_dt:
                    date_tab_labels = [f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}" for fecha in fechas_unicas_dt]
                    
                    date_tabs_m = st.tabs(date_tab_labels)
                    
                    for i, fecha_dt in enumerate(fechas_unicas_dt):
                        date_label = f"📅 {pd.to_datetime(fecha_dt).strftime('%d/%m/%Y')}"
                        with date_tabs_m[i]:
                            pedidos_fecha = pedidos_m_display[pedidos_m_display["Fecha_Entrega_dt"] == fecha_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌅 Pedidos Locales - Mañana - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Mañana", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
                                
                else: # Added: Message if no orders for morning shift
                    st.info("No hay pedidos para el turno mañana.")
            else: # Added: Message if no orders for morning shift
                st.info("No hay pedidos para el turno mañana.")
                                
        with subtabs_local[1]:  # 🌇 Tarde
            pedidos_t_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌙 Local Tarde")
            ].copy()
            if not pedidos_t_display.empty:
                pedidos_t_display['Fecha_Entrega_dt'] = pd.to_datetime(pedidos_t_display['Fecha_Entrega'], errors='coerce')
                fechas_unicas_dt = sorted(pedidos_t_display["Fecha_Entrega_dt"].dropna().unique())

                if fechas_unicas_dt:
                    date_tab_labels = [f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}" for fecha in fechas_unicas_dt]
                    
                    date_tabs_t = st.tabs(date_tab_labels)
                    for i, date_label in enumerate(date_tab_labels):
                        with date_tabs_t[i]:
                            current_selected_date_dt_str = date_label.replace("📅 ", "")
                            current_selected_date_dt = pd.to_datetime(current_selected_date_dt_str, format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_t_display[pedidos_t_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌇 Pedidos Locales - Tarde - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Tarde", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
                else:
                    st.info("No hay pedidos para el turno tarde.")
            else:
                st.info("No hay pedidos para el turno tarde.")

        with subtabs_local[2]: # ⛰️ Saltillo
            pedidos_s_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌵 Saltillo")
            ].copy()
            if not pedidos_s_display.empty:
                pedidos_s_display = ordenar_pedidos_custom(pedidos_s_display)
                st.markdown("#### ⛰️ Pedidos Locales - Saltillo")
                for orden, (idx, row) in enumerate(pedidos_s_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Saltillo", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos para Saltillo.")

        with subtabs_local[3]: # 📦 En Bodega
            pedidos_b_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "📦 Pasa a Bodega")
            ].copy()
            if not pedidos_b_display.empty:
                pedidos_b_display = ordenar_pedidos_custom(pedidos_b_display)
                st.markdown("#### 📦 Pedidos Locales - En Bodega")
                for orden, (idx, row) in enumerate(pedidos_b_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Pasa a Bodega", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos para pasar a bodega.")

    with main_tabs[1]: # 🚚 Pedidos Foráneos
        pedidos_foraneos_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🚚 Pedido Foráneo")
        ].copy()
        if not pedidos_foraneos_display.empty:
            pedidos_foraneos_display = ordenar_pedidos_custom(pedidos_foraneos_display)
            for orden, (idx, row) in enumerate(pedidos_foraneos_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Foráneo", "🚚 Pedidos Foráneos", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos foráneos.")

    with main_tabs[2]:  # 🏙️ Pedidos CDMX
        pedidos_cdmx_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🏙️ Pedido CDMX")
        ].copy()

        if not pedidos_cdmx_display.empty:
            pedidos_cdmx_display = ordenar_pedidos_custom(pedidos_cdmx_display)
            st.markdown("### 🏙️ Pedidos CDMX")
            for orden, (idx, row) in enumerate(pedidos_cdmx_display.iterrows(), start=1):
                # Reutiliza el mismo render que Foráneo (con tus botones de imprimir/completar, etc.)
                mostrar_pedido(df_main, idx, row, orden, "CDMX", "🏙️ Pedidos CDMX", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos CDMX.")

    with main_tabs[3]:  # 📋 Solicitudes de Guía
        solicitudes_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "📋 Solicitudes de Guía")
        ].copy()

        if not solicitudes_display.empty:
            solicitudes_display = ordenar_pedidos_custom(solicitudes_display)
            st.markdown("### 📋 Solicitudes de Guía")
            st.info("En esta pestaña solo puedes **subir la(s) guía(s)**. Al subir se marca el pedido como **🟢 Completado**.")
            for orden, (idx, row) in enumerate(solicitudes_display.iterrows(), start=1):
                # ✅ Render minimalista: solo guía + completar automático
                mostrar_pedido_solo_guia(df_main, idx, row, orden, "Solicitudes", "📋 Solicitudes de Guía", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay solicitudes de guía.")


    # --- TAB 3: 🔁 Devoluciones (casos_especiales) ---
    with main_tabs[4]:
        st.markdown("### 🔁 Devoluciones")

        # 1) Validaciones mínimas
        if 'df_casos' not in locals() and 'df_casos' not in globals():
            st.error("❌ No se encontró el DataFrame 'df_casos'. Asegúrate de haberlo cargado antes.")

        import os
        import json
        import math
        import re
        from datetime import datetime
        try:
            from zoneinfo import ZoneInfo
            _TZ = ZoneInfo("America/Mexico_City")
        except Exception:
            _TZ = None
        import pandas as pd

        # Detectar columna que indica el tipo de caso (Devoluciones)
        tipo_col = "Tipo_Caso" if "Tipo_Caso" in df_casos.columns else ("Tipo_Envio" if "Tipo_Envio" in df_casos.columns else None)
        if not tipo_col:
            st.error("❌ En 'casos_especiales' falta la columna 'Tipo_Caso' o 'Tipo_Envio'.")

        # 2) Filtrar SOLO devoluciones
        devoluciones_display = df_casos[df_casos[tipo_col].astype(str).str.contains("Devoluci", case=False, na=False)].copy()

        if devoluciones_display.empty:
            st.info("ℹ️ No hay devoluciones en 'casos_especiales'.")

        # 2.1 Excluir devoluciones ya completadas
        if "Estado" in devoluciones_display.columns:
            devoluciones_display = devoluciones_display[
                devoluciones_display["Estado"].astype(str).str.strip() != "🟢 Completado"
            ]

        if devoluciones_display.empty:
            st.success("🎉 No hay devoluciones pendientes. (Todas están 🟢 Completado)")

        # 3) Orden sugerido por Fecha_Registro (desc) o por Folio/Cliente
        if "Fecha_Registro" in devoluciones_display.columns:
            try:
                devoluciones_display["_FechaOrden"] = pd.to_datetime(devoluciones_display["Fecha_Registro"], errors="coerce")
                devoluciones_display = devoluciones_display.sort_values(by="_FechaOrden", ascending=False)
            except Exception:
                devoluciones_display = devoluciones_display.sort_values(by="Fecha_Registro", ascending=False)
        elif "ID_Pedido" in devoluciones_display.columns:
            devoluciones_display = devoluciones_display.sort_values(by="ID_Pedido", ascending=True)

        # 🔧 Helper para normalizar/extraer URLs desde texto o JSON
        def _normalize_urls(value):
            if value is None:
                return []
            if isinstance(value, float) and math.isnan(value):
                return []
            s = str(value).strip()
            if not s or s.lower() in ("nan", "none", "n/a"):
                return []
            urls = []
            try:
                obj = json.loads(s)
                if isinstance(obj, list):
                    for it in obj:
                        if isinstance(it, str) and it.strip():
                            urls.append(it.strip())
                        elif isinstance(it, dict):
                            u = it.get("url") or it.get("URL")
                            if u and str(u).strip():
                                urls.append(str(u).strip())
                elif isinstance(obj, dict):
                    for k in ("url", "URL", "link", "href"):
                        if obj.get(k):
                            urls.append(str(obj[k]).strip())
            except Exception:
                parts = re.split(r"[,\n;]+", s)
                for p in parts:
                    p = p.strip()
                    if p:
                        urls.append(p)
            seen = set()
            out = []
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    out.append(u)
            return out

        # 4) Recorrer cada devolución
        for _, row in devoluciones_display.iterrows():
            idp         = str(row.get("ID_Pedido", "")).strip()
            folio       = str(row.get("Folio_Factura", "")).strip()
            cliente     = str(row.get("Cliente", "")).strip()
            estado      = str(row.get("Estado", "Pendiente")).strip()
            vendedor    = str(row.get("Vendedor_Registro", "")).strip()
            estado_rec  = str(row.get("Estado_Recepcion", "N/A")).strip()
            area_resp   = str(row.get("Area_Responsable", "")).strip()

            if area_resp.lower() == "cliente":
                if estado.lower() == "aprobado" and estado_rec.lower() == "todo correcto":
                    emoji_estado = "✅"
                    aviso_extra  = " | Confirmado por administración: puede viajar la devolución"
                else:
                    emoji_estado = "🟡"
                    aviso_extra  = " | Pendiente de confirmación final"
                expander_title = f"🔁 {folio or 's/folio'} – {cliente or 's/cliente'} | Estado: {estado} | Estado_Recepcion: {estado_rec} {emoji_estado}{aviso_extra}"
            else:
                expander_title = f"🔁 {folio or 's/folio'} – {cliente or 's/cliente'} | Estado: {estado} | Estado_Recepcion: {estado_rec}"

            with st.expander(expander_title, expanded=False):
                st.markdown("#### 📋 Información de la Devolución")

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**👤 Vendedor:** {vendedor or 'N/A'}")
                    st.markdown(f"**📄 Factura de Origen:** {folio or 'N/A'}")
                    st.markdown(f"**🎯 Resultado Esperado:** {str(row.get('Resultado_Esperado', 'N/A')).strip()}")
                    st.markdown(f"**🆔 Número Cliente/RFC:** {str(row.get('Numero_Cliente_RFC', 'N/A')).strip()}")
                with col2:
                    st.markdown(f"**🏢 Área Responsable:** {area_resp or 'N/A'}")
                    st.markdown(f"**👥 Responsable del Error:** {str(row.get('Nombre_Responsable', 'N/A')).strip()}")
                    st.markdown(f"**🚚 Tipo Envío Original:** {str(row.get('Tipo_Envio_Original', 'N/A')).strip()}")

                coment_admin = str(row.get("Comentarios_Admin_Devolucion", "")).strip()
                if coment_admin:
                    st.markdown("**📝 Comentario Administrativo:**")
                    st.info(coment_admin)

                st.markdown("**📦 Material a Devolver / Detalle:**")
                material_devolver = str(row.get("Motivo_Detallado", "")).strip()
                if material_devolver:
                    st.text_area("", value=material_devolver, disabled=True, key=f"material_{folio}_{cliente}", height=120)
                else:
                    st.info("No se especificó material a devolver.")

                st.markdown("---")

                with st.expander("📎 Archivos del Caso", expanded=False):
                    adjuntos_urls = _normalize_urls(row.get("Adjuntos", ""))
                    nota_credito_url = str(row.get("Nota_Credito_URL", "")).strip()
                    documento_adic_url = str(row.get("Documento_Adicional_URL", "")).strip()

                    items = []
                    for u in adjuntos_urls:
                        file_name = os.path.basename(u)
                        items.append((file_name, u))

                    if nota_credito_url and nota_credito_url.lower() not in ("nan", "none", "n/a"):
                        items.append(("Nota de Crédito", nota_credito_url))
                    if documento_adic_url and documento_adic_url.lower() not in ("nan", "none", "n/a"):
                        items.append(("Documento Adicional", documento_adic_url))

                    if items:
                        for label, url in items:
                            st.markdown(f"- [{label}]({url})")
                    else:
                        st.info("No hay archivos registrados para esta devolución.")

                st.markdown("---")

                st.markdown("#### 📋 Documentación")
                guia_file = st.file_uploader(
                    "📋 Subir Guía de Retorno",
                    key=f"guia_{folio}_{cliente}",
                    help="Sube la guía de mensajería para el retorno del producto (PDF/JPG/PNG)"
                )

                if st.button("💾 Procesar Devolución", key=f"btn_proc_{folio}_{cliente}"):
                    try:
                        folder = idp or f"caso_{(folio or 'sfolio')}_{(cliente or 'scliente')}".replace(" ", "_")
                        guia_url = ""

                        if guia_file:
                            key_guia = f"{folder}/guia_retorno_{datetime.now().isoformat()[:19].replace(':','')}_{guia_file.name}"
                            _, guia_url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, guia_file, key_guia)

                        gsheet_row_idx = None
                        if "ID_Pedido" in df_casos.columns and idp:
                            matches = df_casos.index[df_casos["ID_Pedido"].astype(str).str.strip() == idp]
                            if len(matches) > 0:
                                gsheet_row_idx = int(matches[0]) + 2
                        if gsheet_row_idx is None:
                            filt = (
                                df_casos.get("Folio_Factura", pd.Series(dtype=str)).astype(str).str.strip().eq(folio) &
                                df_casos.get("Cliente", pd.Series(dtype=str)).astype(str).str.strip().eq(cliente)
                            )
                            matches = df_casos.index[filt] if hasattr(filt, "any") else []
                            if len(matches) > 0:
                                gsheet_row_idx = int(matches[0]) + 2

                        if gsheet_row_idx is None:
                            st.error("❌ No se encontró el caso en 'casos_especiales'.")

                        ok = True
                        if guia_url:
                            ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Hoja_Ruta_Mensajero", guia_url)

                        # Cambiar Estado a 🟢 Completado
                        ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Estado", "🟢 Completado")

                        # Guardar Fecha_Entrega
                        mx_now = datetime.now(_TZ).strftime("%Y-%m-%d %H:%M:%S") if _TZ else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        _ = update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Fecha_Entrega", mx_now)

                        if ok:
                            st.success("✅ Devolución procesada y marcada como 🟢 Completado.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ No se pudo procesar la devolución.")
                    except Exception as e:
                        st.error(f"❌ Error al procesar la devolución: {e}")

            st.markdown("---")


    with main_tabs[5]: #🛠 Garantías
        garantias_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🛠 Garantía")].copy()
        if not garantias_display.empty:
            garantias_display = ordenar_pedidos_custom(garantias_display)
            for orden, (idx, row) in enumerate(garantias_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Garantía", "🛠 Garantías", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay garantías.")

with main_tabs[6]:  # ✅ Historial Completados
    df_completados_historial = df_main[
        (df_main["Estado"] == "🟢 Completado") & 
        (df_main.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
    ].copy()

    df_completados_historial['_gsheet_row_index'] = df_completados_historial['_gsheet_row_index'].astype(int)

    col_titulo, col_btn = st.columns([0.75, 0.25])
    with col_titulo:
        st.markdown("### Historial de Pedidos Completados")
    with col_btn:
        if not df_completados_historial.empty and st.button("🧹 Limpiar Todos los Completados"):
            updates = []
            col_idx = headers_main.index("Completados_Limpiado") + 1
            for _, row in df_completados_historial.iterrows():
                g_row = row.get("_gsheet_row_index")
                if g_row:
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(g_row, col_idx),
                        'values': [["sí"]]
                    })
            if updates and batch_update_gsheet_cells(worksheet_main, updates):
                st.success(f"✅ {len(updates)} pedidos marcados como limpiados.")
                st.cache_data.clear()
                st.session_state["active_main_tab_index"] = 6
                st.rerun()

    # 🧹 Limpieza específica por grupo de completados locales
    df_completados_historial["Fecha_dt"] = pd.to_datetime(df_completados_historial["Fecha_Entrega"], errors='coerce')
    df_completados_historial["Grupo_Clave"] = df_completados_historial.apply(
        lambda row: f"{row['Turno']} – {row['Fecha_dt'].strftime('%d/%m')}" if row["Tipo_Envio"] == "📍 Pedido Local" else None,
        axis=1
    )

    grupos_locales = df_completados_historial[df_completados_historial["Grupo_Clave"].notna()]["Grupo_Clave"].unique().tolist()

    if grupos_locales:
        st.markdown("### 🧹 Limpieza Específica de Completados Locales")
        for grupo in grupos_locales:
            turno, fecha_str = grupo.split(" – ")
            fecha_dt = pd.to_datetime(fecha_str, format="%d/%m", errors='coerce').replace(year=datetime.now().year)

            # Verificar si hay incompletos en ese grupo
            hay_incompletos = df_main[
                (df_main["Turno"] == turno) &
                (pd.to_datetime(df_main["Fecha_Entrega"], errors='coerce').dt.date == fecha_dt.date()) &
                (df_main["Estado"].isin(["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"]))
            ]

            if hay_incompletos.empty:
                label_btn = f"🧹 Limpiar {turno.strip()} - {fecha_str}"
                if st.button(label_btn):
                    pedidos_a_limpiar = df_completados_historial[df_completados_historial["Grupo_Clave"] == grupo]
                    col_idx = headers_main.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            'range': gspread.utils.rowcol_to_a1(int(row["_gsheet_row_index"]), col_idx),
                            'values': [["sí"]]
                        }
                        for _, row in pedidos_a_limpiar.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_main, updates):
                        st.success(f"✅ {len(updates)} pedidos completados en {grupo} marcados como limpiados.")
                        st.cache_data.clear()
                        st.session_state["active_main_tab_index"] = 6
                        st.rerun()

    # Mostrar pedidos completados individuales
    if not df_completados_historial.empty:
            # 🧹 Botón de limpieza específico para foráneos
        completados_foraneos = df_completados_historial[
            df_completados_historial["Tipo_Envio"] == "🚚 Pedido Foráneo"
        ]

        if not completados_foraneos.empty:
            st.markdown("### 🧹 Limpieza de Completados Foráneos")
            if st.button("🧹 Limpiar Foráneos Completados"):
                col_idx = headers_main.index("Completados_Limpiado") + 1
                updates = [
                    {
                        'range': gspread.utils.rowcol_to_a1(int(row["_gsheet_row_index"]), col_idx),
                        'values': [["sí"]]
                    }
                    for _, row in completados_foraneos.iterrows()
                ]
                if updates and batch_update_gsheet_cells(worksheet_main, updates):
                    st.success(f"✅ {len(updates)} pedidos foráneos completados fueron marcados como limpiados.")
                    st.cache_data.clear()
                    st.session_state["active_main_tab_index"] = 6
                    st.rerun()

        df_completados_historial = df_completados_historial.sort_values(by="Fecha_Completado", ascending=False)
        for orden, (idx, row) in enumerate(df_completados_historial.iterrows(), start=1):
            mostrar_pedido(df_main, idx, row, orden, "Historial", "✅ Historial Completados", worksheet_main, headers_main, s3_client)
    else:
        st.info("No hay pedidos completados recientes o ya fueron limpiados.") 
