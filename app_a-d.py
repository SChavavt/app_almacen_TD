
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
from urllib.parse import urlparse, unquote
import streamlit.components.v1 as components

_MX_TZ = timezone("America/Mexico_City")

def mx_now():
    return datetime.now(_MX_TZ)           # objeto datetime tz-aware

def mx_now_str():
    return mx_now().strftime("%Y-%m-%d %H:%M:%S")

def mx_today():
    return mx_now().date()



st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

# 🔁 Restaurar pestañas activas si venimos de una acción que modificó datos
if "preserve_main_tab" in st.session_state:
    st.session_state["active_main_tab_index"] = st.session_state.pop("preserve_main_tab", 0)
    st.session_state["active_subtab_local_index"] = st.session_state.pop("preserve_local_tab", 0)
    st.session_state["active_date_tab_m_index"] = st.session_state.pop("preserve_date_tab_m", 0)
    st.session_state["active_date_tab_t_index"] = st.session_state.pop("preserve_date_tab_t", 0)
else:
    # 🧭 Leer pestaña activa desde parámetros de URL
    params = st.query_params
    if "tab" in params:
        try:
            tab_val = params["tab"]
            if isinstance(tab_val, list):
                tab_val = tab_val[0]
            st.session_state["active_main_tab_index"] = int(tab_val)
        except (ValueError, TypeError):
            st.session_state["active_main_tab_index"] = 0

st.query_params["tab"] = str(st.session_state.get("active_main_tab_index", 0))

st.title("📬 Bandeja de Pedidos TD")

# Flash message tras refresh
if "flash_msg" in st.session_state and st.session_state["flash_msg"]:
    st.success(st.session_state.pop("flash_msg"))


# ✅ Recarga segura que también repara la conexión si es necesario
if st.button(
    "🔄 Recargar Pedidos (seguro)",
    help="Actualiza datos sin reiniciar pestañas ni scroll",
    key="btn_recargar_seguro",
):
    # Guardamos cuántos pedidos teníamos antes de recargar
    st.session_state["prev_pedidos_count"] = st.session_state.get("last_pedidos_count", 0)
    st.session_state["prev_casos_count"] = st.session_state.get("last_casos_count", 0)
    st.session_state["need_compare"] = True
    st.session_state["reload_pedidos_soft"] = True
    st.cache_data.clear()
    st.cache_resource.clear()


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
    st.session_state["expanded_subir_guia"] = {}
if "expanded_devoluciones" not in st.session_state:
    st.session_state["expanded_devoluciones"] = {}
if "expanded_garantias" not in st.session_state:
    st.session_state["expanded_garantias"] = {}
if "last_pedidos_count" not in st.session_state:
    st.session_state["last_pedidos_count"] = 0
if "last_casos_count" not in st.session_state:
    st.session_state["last_casos_count"] = 0
if "prev_pedidos_count" not in st.session_state:
    st.session_state["prev_pedidos_count"] = 0
if "prev_casos_count" not in st.session_state:
    st.session_state["prev_casos_count"] = 0
if "need_compare" not in st.session_state:
    st.session_state["need_compare"] = False

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


def _reconnect_and_rerun():
    """Limpia cachés y fuerza un rerun de la aplicación."""
    st.cache_data.clear()
    st.cache_resource.clear()
    time.sleep(1)
    st.rerun()


def handle_auth_error(exc: Exception):
    """Intenta reparar la conexión ante errores comunes de autenticación o cuota."""
    err_text = str(exc)
    recoverable = [
        "401",
        "UNAUTHENTICATED",
        "ACCESS_TOKEN_EXPIRED",
        "RESOURCE_EXHAUSTED",
        "RATE_LIMIT",
        "429",
    ]
    if any(code in err_text for code in recoverable):
        st.warning("🔁 Error de autenticación o cuota. Reintentando conexión...")
        _reconnect_and_rerun()
    else:
        st.error(f"❌ Error general al autenticarse o inicializar clientes: {exc}")
        st.info(
            "ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud. También, revisa tus credenciales de AWS S3 y Google Sheets en .streamlit/secrets.toml o en la interfaz de Streamlit Cloud."
        )
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
    handle_auth_error(e)


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_data(ttl=60)
def get_raw_sheet_data(sheet_id: str, worksheet_name: str, credentials: dict) -> list[list[str]]:
    creds = dict(credentials)
    creds["private_key"] = creds["private_key"].replace("\\n", "\n")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    delay = 1
    for attempt in range(3):
        auth = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
        client = gspread.authorize(auth)

        try:
            sheet = client.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            return worksheet.get_all_values()
        except Exception as e:
            st.cache_data.clear()  # 🔁 Limpiar la caché en caso de error de token/API
            if attempt < 2:
                st.warning(
                    f"🔁 Error de conexión o token (intento {attempt + 1}/3). "
                    f"Reintentando en {delay}s..."
                )
                time.sleep(delay)
                delay *= 2
            else:
                st.error(
                    "❌ No se pudo conectar con Google Sheets después de varios intentos. "
                    "Verifica tu conectividad o tus credenciales de servicio."
                )
                raise e


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

def safe_update_estado(worksheet, gsheet_row_index, estado_col_idx, expected, new_value):
    """
    Actualiza la celda de ``Estado`` únicamente si su valor actual está en ``expected``.
    """
    try:
        current = worksheet.cell(gsheet_row_index, estado_col_idx).value
    except Exception as e:
        st.error(f"❌ Error al leer el estado actual: {e}")
        return False

    if current not in expected:
        st.warning(f"⚠️ Estado actual '{current}' no permite actualizar a '{new_value}'.")
        return False

    try:
        worksheet.update_cells(
            [gspread.Cell(row=gsheet_row_index, col=estado_col_idx, value=new_value)]
        )
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar el estado: {e}")
        return False

def get_column_indices(worksheet, column_names):
    """Obtain fresh column indices for the specified headers."""
    indices = {}
    for name in column_names:
        try:
            cell = worksheet.find(name)
            indices[name] = cell.col if cell else None
        except gspread.exceptions.CellNotFound:
            st.error(f"❌ Columna '{name}' no encontrada en la hoja.")
            indices[name] = None
    return indices

def ensure_columns(worksheet, headers, required_cols):
    """
    Asegura que la hoja tenga todas las columnas requeridas en la fila 1.
    Si faltan, las agrega al final. Devuelve headers actualizados.
    Es compatible con entornos donde Worksheet.update no existe.
    """
    missing = [c for c in required_cols if c not in headers]
    if not missing:
        return headers

    new_headers = headers + missing

    # 1) Intento con update (si existe en tu versión de gspread)
    try:
        # Algunas versiones requieren rango tipo '1:1'
        if hasattr(worksheet, "update"):
            try:
                worksheet.update('1:1', [new_headers])
            except TypeError:
                # En otras funciona 'A1'
                worksheet.update('A1', [new_headers])
            return new_headers
    except AttributeError:
        # No tiene update -> caemos al plan B
        pass
    except gspread.exceptions.APIError:
        # Si falla por dimensiones, intentamos plan B
        pass

    # 2) Plan B: update_cells (compatible con versiones viejas)
    try:
        # Asegurar que existan suficientes columnas
        try:
            # add_cols puede no estar en todas las versiones; si falla, seguimos
            if hasattr(worksheet, "add_cols"):
                extra = len(new_headers) - len(headers)
                if extra > 0:
                    worksheet.add_cols(extra)
        except Exception:
            pass

        cell_list = [gspread.Cell(row=1, col=i+1, value=val)
                     for i, val in enumerate(new_headers)]
        worksheet.update_cells(cell_list)
        return new_headers
    except Exception as e:
        st.error(f"❌ No se pudieron actualizar encabezados con compatibilidad: {e}")
        return headers



# --- AWS S3 Helper Functions (Copied from app_admin.py directly) ---
def upload_file_to_s3(s3_client_param, bucket_name, file_obj, s3_key):
    try:
        # La visibilidad pública se controla mediante la bucket policy.
        # Sólo añadimos el ContentType si está disponible para evitar
        # errores "AccessControlListNotSupported".
        put_kwargs = {
            "Bucket": bucket_name,
            "Key": s3_key,
            "Body": file_obj.getvalue(),
        }
        # Si Streamlit provee el content-type, pásalo (mejor vista/descarga en navegador)
        if hasattr(file_obj, "type") and file_obj.type:
            put_kwargs["ContentType"] = file_obj.type

        s3_client_param.put_object(**put_kwargs)

        permanent_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"

        # Return the permanent URL to the uploaded object so it can be stored directly.
        return True, permanent_url

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

def extract_s3_key(url_or_key: str) -> str:
    """Return a clean S3 object key from a raw key or a (possibly expired) URL."""
    if not isinstance(url_or_key, str):
        return url_or_key
    parsed = urlparse(url_or_key)
    if parsed.scheme and parsed.netloc:
        return unquote(parsed.path.lstrip("/"))
    return url_or_key


def get_s3_file_download_url(s3_client_param, object_key_or_url, expires_in=604800):
    """Genera y retorna una URL prefirmada para archivos almacenados en S3."""
    if not s3_client_param or not S3_BUCKET_NAME:
        st.error("❌ Configuración de S3 incompleta. Verifica el cliente y el nombre del bucket.")
        return "#"
    try:
        clean_key = extract_s3_key(object_key_or_url)
        return s3_client_param.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": clean_key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        st.error(f"❌ Error al generar URL prefirmada: {e}")
        return "#"


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


def fijar_y_preservar(row, origen_tab):
    """Preserva pestañas y expansores antes del rerun."""
    st.session_state["pedido_editado"] = row["ID_Pedido"]
    st.session_state["fecha_seleccionada"] = row.get("Fecha_Entrega", "")
    st.session_state["subtab_local"] = origen_tab

    # ✅ Mantener expander de pedido y adjuntos abierto
    st.session_state["expanded_pedidos"][row["ID_Pedido"]] = True
    st.session_state["expanded_attachments"][row["ID_Pedido"]] = True
    # ✅ Mantener expander de subir guía (si aplica)
    st.session_state["expanded_subir_guia"][row["ID_Pedido"]] = True

    # 🔄 Sincronizar la pestaña principal desde la URL antes de preservar
    tab_val = st.query_params.get("tab")
    if tab_val is not None:
        if isinstance(tab_val, list):
            tab_val = tab_val[0]
        try:
            st.session_state["active_main_tab_index"] = int(tab_val)
        except (ValueError, TypeError):
            st.session_state["active_main_tab_index"] = 0

    preserve_tab_state()


def preserve_tab_state():
    """Guarda las pestañas activas actuales para restaurarlas tras un rerun."""
    st.session_state["preserve_main_tab"] = st.session_state.get("active_main_tab_index", 0)
    st.session_state["preserve_local_tab"] = st.session_state.get("active_subtab_local_index", 0)
    st.session_state["preserve_date_tab_m"] = st.session_state.get("active_date_tab_m_index", 0)
    st.session_state["preserve_date_tab_t"] = st.session_state.get("active_date_tab_t_index", 0)


def set_active_main_tab(idx: int):
    """Actualiza la pestaña principal activa y sincroniza la URL."""
    st.session_state["active_main_tab_index"] = idx
    st.query_params["tab"] = str(idx)


def handle_generic_upload_change(row_id, expander_dict_name):
    """Mantiene expander y pestañas al seleccionar archivos.

    Parameters
    ----------
    row_id : Any
        Identificador de la fila asociada al pedido/caso.
    expander_dict_name : str
        Nombre del diccionario en ``st.session_state`` que controla la
        expansión del elemento (por ejemplo ``"expanded_pedidos"``).
    """
    if expander_dict_name in st.session_state:
        st.session_state[expander_dict_name][row_id] = True
    st.session_state["scroll_to_pedido_id"] = row_id
    preserve_tab_state()
    # El script se vuelve a ejecutar automáticamente después de este callback,
    # así que evitamos una llamada explícita a st.rerun().

def mostrar_pedido_detalle(
    df,
    idx,
    row,
    origen_tab,
    worksheet,
    headers,
    gsheet_row_index,
    col_print_btn,
):
    """Muestra el botón de impresión y actualiza el estado del pedido."""
    if col_print_btn.button(
        "🖨 Imprimir",
        key=f"print_{row['ID_Pedido']}_{origen_tab}",
        on_click=fijar_y_preservar,
        args=(row, origen_tab),
    ):
        if row["Estado"] in ["🟡 Pendiente", "🔴 Demorado", "🛠 Modificación"]:
            zona_mexico = timezone("America/Mexico_City")
            now = datetime.now(zona_mexico)
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")

            estado_col_idx = headers.index("Estado") + 1
            hora_proc_col_idx = headers.index("Hora_Proceso") + 1

            if safe_update_estado(
                worksheet,
                gsheet_row_index,
                estado_col_idx,
                ["🟡 Pendiente", "🔴 Demorado", "🛠 Modificación"],
                "🔵 En Proceso",
            ):
                try:
                    worksheet.update_cells([
                        gspread.Cell(row=gsheet_row_index, col=hora_proc_col_idx, value=now_str)
                    ])
                except Exception as e:
                    st.error(f"❌ Error al actualizar 'Hora_Proceso': {e}")

                df.at[idx, "Estado"] = "🔵 En Proceso"
                df.at[idx, "Hora_Proceso"] = now_str
                row["Estado"] = "🔵 En Proceso"
                st.toast("📄 Estado actualizado a 'En Proceso'", icon="📌")

        st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
        st.session_state["print_clicked"] = row["ID_Pedido"]

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
    if st.session_state.get("print_clicked") == row["ID_Pedido"]:
        st.session_state["expanded_pedidos"][row["ID_Pedido"]] = True
        st.session_state["expanded_attachments"][row["ID_Pedido"]] = True
        st.session_state.pop("print_clicked", None)

    folio = row.get("Folio_Factura", "").strip() or row['ID_Pedido']
    st.markdown(f'<a name="pedido_{row["ID_Pedido"]}"></a>', unsafe_allow_html=True)
    with st.expander(f"{row['Estado']} - {folio} - {row['Cliente']}", expanded=st.session_state["expanded_pedidos"].get(row['ID_Pedido'], False)):
        st.markdown("---")
        mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
        hay_modificacion = mod_texto != ""


        # --- Cambiar Fecha y Turno ---
        if row['Estado'] not in ["🟢 Completado", "✅ Viajó"] and row.get("Tipo_Envio") in ["📍 Pedido Local", "🚚 Pedido Foráneo"]:
            # Muestra los controles solo cuando el usuario lo solicite para evitar
            # renderizar innecesariamente muchos widgets (que pueden provocar el
            # error "Failed to fetch dynamically imported module").
            mostrar_cambio = st.checkbox(
                "📅 Cambiar Fecha y Turno",
                key=f"chk_fecha_{row['ID_Pedido']}",
            )

            if mostrar_cambio:
                col_current_info_date, col_current_info_turno, _ = st.columns([1, 1, 2])

                fecha_actual_str = row.get("Fecha_Entrega", "")
                fecha_actual_dt = (
                    pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
                )
                fecha_mostrar = (
                    fecha_actual_dt.strftime('%d/%m/%Y')
                    if pd.notna(fecha_actual_dt)
                    else "Sin fecha"
                )
                col_current_info_date.info(f"**Fecha actual:** {fecha_mostrar}")

                current_turno = row.get("Turno", "")
                if row.get("Tipo_Envio") == "📍 Pedido Local":
                    col_current_info_turno.info(f"**Turno actual:** {current_turno}")
                else:
                    col_current_info_turno.empty()

                today = datetime.now().date()
                default_fecha = (
                    fecha_actual_dt.date()
                    if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today
                    else today
                )

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

                if st.button(
                    "✅ Aplicar Cambios de Fecha/Turno",
                    key=f"btn_apply_{row['ID_Pedido']}",
                ):
                    st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                    cambios = []
                    nueva_fecha_str = st.session_state[fecha_key].strftime('%Y-%m-%d')

                    if nueva_fecha_str != fecha_actual_str:
                        col_idx = headers.index("Fecha_Entrega") + 1
                        cambios.append(
                            {
                                'range': gspread.utils.rowcol_to_a1(
                                    gsheet_row_index, col_idx
                                ),
                                'values': [[nueva_fecha_str]],
                            }
                        )

                    if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                        nuevo_turno = st.session_state[turno_key]
                        if nuevo_turno != current_turno:
                            col_idx = headers.index("Turno") + 1
                            cambios.append(
                                {
                                    'range': gspread.utils.rowcol_to_a1(
                                        gsheet_row_index, col_idx
                                    ),
                                    'values': [[nuevo_turno]],
                                }
                            )

                    if cambios:
                        if batch_update_gsheet_cells(worksheet, cambios):
                            if "Fecha_Entrega" in headers:
                                df.at[idx, "Fecha_Entrega"] = nueva_fecha_str
                            if (
                                "Turno" in headers
                                and row.get("Tipo_Envio") == "📍 Pedido Local"
                            ):
                                df.at[idx, "Turno"] = st.session_state[turno_key]

                            st.toast(
                                f"📅 Pedido {row['ID_Pedido']} actualizado.",
                                icon="✅",
                            )
                        else:
                            st.error("❌ Falló la actualización en Google Sheets.")
                    else:
                        st.info("No hubo cambios para aplicar.")


        
        st.markdown("---")

        # --- Main Order Layout ---
        # This section displays the core information of the order
        disabled_if_completed = (row['Estado'] in ["🟢 Completado", "✅ Viajó"])

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



        mostrar_pedido_detalle(
            df,
            idx,
            row,
            origen_tab,
            worksheet,
            headers,
            gsheet_row_index,
            col_print_btn,
        )



        # This block displays attachments inside an expander
        with st.expander(
            "📎 Archivos (Adjuntos y Guía)",
            expanded=st.session_state["expanded_attachments"].get(row["ID_Pedido"], False),
        ):
            if st.session_state["expanded_attachments"].get(row["ID_Pedido"], False):
                st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")
                pedido_folder_prefix = find_pedido_subfolder_prefix(
                    s3_client_param, S3_ATTACHMENT_PREFIX, row['ID_Pedido']
                )

                if pedido_folder_prefix:
                    files_in_folder = get_files_in_s3_prefix(
                        s3_client_param, pedido_folder_prefix
                    )
                    if files_in_folder:
                        filtered_files_to_display = [
                            f for f in files_in_folder
                            if "comprobante" not in f['title'].lower() and "surtido" not in f['title'].lower()
                        ]
                        if filtered_files_to_display:
                            for file_info in filtered_files_to_display:
                                file_url = get_s3_file_download_url(
                                    s3_client_param, file_info['key']
                                )
                                display_name = file_info['title']
                                if row['ID_Pedido'] in display_name:
                                    display_name = (
                                        display_name.replace(row['ID_Pedido'], "").replace("__", "_")
                                        .replace("_-", "_").replace("-_", "_").strip('_').strip('-')
                                    )
                                st.markdown(
                                    f'- 📄 **{display_name}** (<a href="{file_url}" target="_blank">🔗 Ver/Descargar</a>)',
                                    unsafe_allow_html=True,
                                )
                        else:
                            st.info("No hay adjuntos para mostrar (excluyendo comprobantes y surtidos).")
                    else:
                        st.info("No se encontraron archivos en la carpeta del pedido en S3.")
                else:
                    st.error(
                        f"❌ No se encontró la carpeta (prefijo S3) del pedido '{row['ID_Pedido']}'."
                    )


        # Complete Button with confirmation
        flag_key = f"confirm_complete_id_{row['ID_Pedido']}"
        if col_complete_btn.button(
            "🟢 Completar",
            key=f"complete_button_{row['ID_Pedido']}_{origen_tab}",
            disabled=disabled_if_completed,
        ):
            st.session_state[flag_key] = row["ID_Pedido"]

        if st.session_state.get(flag_key) == row["ID_Pedido"]:
            st.warning("¿Estás seguro de completar este pedido?")
            confirm_col, cancel_col = st.columns(2)
            with confirm_col:
                if st.button(
                    "Confirmar",
                    key=f"confirm_complete_{row['ID_Pedido']}_{origen_tab}",
                ):
                    try:
                        # Buscar el índice real de la fila en Google Sheets usando el ID_Pedido
                        cell = worksheet.find(str(row["ID_Pedido"]))
                        if not cell:
                            st.error(
                                f"❌ No se encontró el ID_Pedido '{row['ID_Pedido']}' en la hoja."
                            )
                        else:
                            gsheet_row_index = cell.row

                            estado_col_idx = headers.index("Estado") + 1
                            fecha_completado_col_idx = (
                                headers.index("Fecha_Completado") + 1
                            )

                            zona_mexico = timezone("America/Mexico_City")
                            now = datetime.now(zona_mexico)
                            now_str = now.strftime("%Y-%m-%d %H:%M:%S")

                            updates = []
                            updates.append(
                                {
                                    "range": gspread.utils.rowcol_to_a1(
                                        gsheet_row_index, estado_col_idx
                                    ),
                                    "values": [["🟢 Completado"]],
                                }
                            )
                            updates.append(
                                {
                                    "range": gspread.utils.rowcol_to_a1(
                                        gsheet_row_index, fecha_completado_col_idx
                                    ),
                                    "values": [[now_str]],
                                }
                            )

                            if batch_update_gsheet_cells(worksheet, updates):
                                df.loc[idx, "Estado"] = "🟢 Completado"
                                df.loc[idx, "Fecha_Completado"] = now
                                st.success(
                                    f"✅ Pedido {row['ID_Pedido']} completado exitosamente."
                                )

                                # 🔁 Mantener pestaña activa
                                st.session_state["pedido_editado"] = row["ID_Pedido"]
                                st.session_state["fecha_seleccionada"] = row.get(
                                    "Fecha_Entrega", ""
                                )
                                st.session_state["subtab_local"] = origen_tab
                                time.sleep(0.5)
                                estado_actual = worksheet.cell(
                                    gsheet_row_index, estado_col_idx
                                ).value
                                if estado_actual == "🟢 Completado":
                                    st.cache_data.clear()

                                    set_active_main_tab(
                                        st.session_state.get("active_main_tab_index", 0)
                                    )
                                    st.session_state["active_subtab_local_index"] = (
                                        st.session_state.get(
                                            "active_subtab_local_index", 0
                                        )
                                    )
                                    st.session_state["active_date_tab_m_index"] = (
                                        st.session_state.get(
                                            "active_date_tab_m_index", 0
                                        )
                                    )
                                    st.session_state["active_date_tab_t_index"] = (
                                        st.session_state.get(
                                            "active_date_tab_t_index", 0
                                        )
                                    )
                                    del st.session_state[flag_key]
                                    st.rerun()
                            else:
                                st.error("❌ No se pudo completar el pedido.")
                                if flag_key in st.session_state:
                                    del st.session_state[flag_key]
                    except Exception as e:
                        st.error(f"Error al completar el pedido: {e}")
                        if flag_key in st.session_state:
                            del st.session_state[flag_key]
            with cancel_col:
                if st.button(
                    "Cancelar",
                    key=f"cancel_complete_{row['ID_Pedido']}_{origen_tab}",
                ):
                    if flag_key in st.session_state:
                        del st.session_state[flag_key]

                
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
                    
                    # 🔄 Actualizar solo el estado a "En Proceso" si coincide con lo esperado
                    estado_col_idx = headers.index("Estado") + 1
                    if safe_update_estado(
                        worksheet,
                        gsheet_row_index,
                        estado_col_idx,
                        ["🟡 Pendiente", "🔴 Demorado", "🛠 Modificación"],
                        "🔵 En Proceso",
                    ):
                        # ✅ Actualizar el DataFrame y la fila localmente
                        df.at[idx, "Estado"] = "🔵 En Proceso"
                        row["Estado"] = "🔵 En Proceso"  # Refleja el cambio en pantalla

                        st.toast("🔧 Modificación procesada - Estado actualizado a 'En Proceso'", icon="✅")

                        # 🔁 Mantener pestañas activas
                        set_active_main_tab(st.session_state.get("active_main_tab_index", 0))
                        st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
                        st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
                        st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)

                        st.cache_data.clear()
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ Error al procesar la modificación: {e}")

        # --- Adjuntar archivos de guía ---
        if row['Estado'] not in ["🟢 Completado", "✅ Viajó"]:
            with st.expander(
                "📦 Subir Archivos de Guía",
                expanded=st.session_state["expanded_subir_guia"].get(row['ID_Pedido'], False),
            ):

                upload_key = f"file_guia_{row['ID_Pedido']}"
                archivos_guia = st.file_uploader(
                    "📎 Subir guía(s) del pedido",
                    type=["pdf", "jpg", "jpeg", "png"],
                    accept_multiple_files=True,
                    key=upload_key,
                    on_change=handle_generic_upload_change,
                    args=(row["ID_Pedido"], "expanded_subir_guia"),
                )

                if st.button(
                    "📤 Subir Guía",
                    key=f"btn_subir_guia_{row['ID_Pedido']}",
                    on_click=fijar_y_preservar,
                    args=(row, origen_tab),
                ):

                    if archivos_guia:
                        uploaded_keys = []
                        for archivo in archivos_guia:
                            ext = os.path.splitext(archivo.name)[1]
                            s3_key = f"{row['ID_Pedido']}/guia_{uuid.uuid4().hex[:6]}{ext}"
                            success, uploaded_key = upload_file_to_s3(
                                s3_client_param, S3_BUCKET_NAME, archivo, s3_key
                            )
                            if success and uploaded_key:
                                uploaded_keys.append(uploaded_key)

                        if uploaded_keys:
                            tipo_envio_str = str(row.get("Tipo_Envio", "")).lower()
                            use_hoja_ruta = ("devol" in tipo_envio_str) or ("garant" in tipo_envio_str)
                            target_col_for_guide = (
                                "Hoja_Ruta_Mensajero" if use_hoja_ruta else "Adjuntos_Guia"
                            )
                            if target_col_for_guide not in headers:
                                headers = ensure_columns(worksheet, headers, [target_col_for_guide])
                            anterior = str(row.get(target_col_for_guide, "")).strip()
                            nueva_lista = (
                                (anterior + ", " if anterior else "")
                                + ", ".join(uploaded_keys)
                            )
                            success = update_gsheet_cell(
                                worksheet, headers, gsheet_row_index, target_col_for_guide, nueva_lista
                            )
                            if success:
                                if target_col_for_guide == "Hoja_Ruta_Mensajero":
                                    df.at[idx, "Hoja_Ruta_Mensajero"] = nueva_lista
                                    row["Hoja_Ruta_Mensajero"] = nueva_lista
                                else:
                                    df.at[idx, "Adjuntos_Guia"] = nueva_lista
                                    row["Adjuntos_Guia"] = nueva_lista
                                st.toast(
                                    f"📤 {len(uploaded_keys)} guía(s) subida(s) con éxito.",
                                    icon="📦",
                                )
                                st.success(
                                    f"📦 Se subieron correctamente {len(uploaded_keys)} archivo(s) de guía."
                                )
                            else:
                                st.error(
                                    "❌ No se pudo actualizar el Google Sheet con los archivos de guía."
                                )
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")
                    else:
                        st.warning("⚠️ No seleccionaste archivos de guía.")
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
                            st.markdown(
                                f'- 📄 <a href="{presigned_url}" target="_blank">{file_name_to_display}</a>',
                                unsafe_allow_html=True,
                            )
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
            key=upload_key,
            on_change=handle_generic_upload_change,
            args=(row["ID_Pedido"], "expanded_pedidos"),
        )


        if st.button(
            "📤 Subir Guía",
            key=f"btn_subir_guia_only_{row['ID_Pedido']}",
            on_click=preserve_tab_state,
        ):
            if not archivos_guia:
                st.warning("⚠️ Primero sube al menos un archivo de guía.")
            else:
                uploaded_keys = []
                for archivo in archivos_guia:
                    ext = os.path.splitext(archivo.name)[1]
                    s3_key = f"{row['ID_Pedido']}/guia_{uuid.uuid4().hex[:6]}{ext}"
                    success, uploaded_key = upload_file_to_s3(s3_client_param, S3_BUCKET_NAME, archivo, s3_key)
                    if success and uploaded_key:
                        uploaded_keys.append(uploaded_key)

                if uploaded_keys:
                    nueva_lista = str(row.get("Adjuntos_Guia", "")).strip()
                    nueva_lista = (nueva_lista + ", " if nueva_lista else "") + ", ".join(uploaded_keys)
                    success = update_gsheet_cell(
                        worksheet, headers, gsheet_row_index, "Adjuntos_Guia", nueva_lista
                    )
                    if success:
                        df.at[idx, "Adjuntos_Guia"] = nueva_lista
                        row["Adjuntos_Guia"] = nueva_lista
                        st.toast(
                            f"📤 {len(uploaded_keys)} guía(s) subida(s) con éxito.",
                            icon="📦",
                        )
                        st.success(
                            f"📦 Se subieron correctamente {len(uploaded_keys)} archivo(s) de guía."
                        )
                        set_active_main_tab(3)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ No se pudo actualizar Google Sheets con la guía.")
                else:
                    st.warning("⚠️ No se subió ningún archivo válido.")

        flag_key = f"confirm_complete_id_{row['ID_Pedido']}"
        if st.button(
            "🟢 Completar",
            key=f"btn_completar_only_{row['ID_Pedido']}",
            on_click=preserve_tab_state,
        ):
            st.session_state[flag_key] = row["ID_Pedido"]

        if st.session_state.get(flag_key) == row["ID_Pedido"]:
            st.warning("¿Estás seguro de completar este pedido?")
            confirm_col, cancel_col = st.columns(2)
            with confirm_col:
                if st.button(
                    "Confirmar",
                    key=f"confirm_completar_only_{row['ID_Pedido']}",
                    on_click=preserve_tab_state,
                ):
                    if not str(row.get("Adjuntos_Guia", "")).strip():
                        st.warning("⚠️ Sube al menos una guía antes de completar.")
                        del st.session_state[flag_key]
                    else:
                        updates = []
                        if "Estado" in headers:
                            col_idx = headers.index("Estado") + 1
                            updates.append(
                                {
                                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx),
                                    'values': [["🟢 Completado"]],
                                }
                            )
                        mx_now = datetime.now(timezone("America/Mexico_City")).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if "Fecha_Completado" in headers:
                            col_idx = headers.index("Fecha_Completado") + 1
                            updates.append(
                                {
                                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx),
                                    'values': [[mx_now]],
                                }
                            )
                        if updates and batch_update_gsheet_cells(worksheet, updates):
                            df.at[idx, "Estado"] = "🟢 Completado"
                            df.at[idx, "Fecha_Completado"] = mx_now
                            st.success("✅ Pedido marcado como **🟢 Completado**.")
                            set_active_main_tab(3)
                            st.cache_data.clear()
                            del st.session_state[flag_key]
                            st.rerun()
                        else:
                            st.error("❌ No se pudo actualizar Google Sheets con el estado.")
                            if flag_key in st.session_state:
                                del st.session_state[flag_key]
            with cancel_col:
                if st.button(
                    "Cancelar",
                    key=f"cancel_completar_only_{row['ID_Pedido']}",
                    on_click=preserve_tab_state,
                ):
                    if flag_key in st.session_state:
                        del st.session_state[flag_key]

# --- Main Application Logic ---

def _load_pedidos():
    raw = get_raw_sheet_data(
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name=GOOGLE_SHEET_WORKSHEET_NAME,
        credentials=GSHEETS_CREDENTIALS,
    )
    return process_sheet_data(raw)

def _load_casos():
    raw = get_raw_sheet_data(
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name="casos_especiales",
        credentials=GSHEETS_CREDENTIALS,
    )
    return process_sheet_data(raw)

if st.session_state.get("need_compare"):
    prev_pedidos = st.session_state.get("prev_pedidos_count", 0)
    prev_casos = st.session_state.get("prev_casos_count", 0)
    for attempt in range(3):
        st.cache_data.clear()
        df_main, headers_main = _load_pedidos()
        df_casos, headers_casos = _load_casos()
        new_pedidos = len(df_main)
        new_casos = len(df_casos)
        if (new_pedidos > prev_pedidos or new_casos > prev_casos) or attempt == 2:
            break
        time.sleep(1)
    diff_ped = new_pedidos - prev_pedidos
    diff_casos = new_casos - prev_casos
    if diff_ped > 0:
        st.toast(f"✅ Se encontraron {diff_ped} pedidos nuevos.")
    else:
        st.toast("🔄 Pedidos actualizados. No hay nuevos registros.")
    if diff_casos > 0:
        st.toast(f"✅ Se encontraron {diff_casos} casos nuevos en 'casos_especiales'.")
    else:
        st.toast("🔄 'casos_especiales' actualizado. No hay nuevos registros.")
    st.session_state["last_pedidos_count"] = new_pedidos
    st.session_state["last_casos_count"] = new_casos
    st.session_state["need_compare"] = False
else:
    df_main, headers_main = _load_pedidos()
    df_casos, headers_casos = _load_casos()
    st.session_state["last_pedidos_count"] = len(df_main)
    st.session_state["last_casos_count"] = len(df_casos)

# --- Asegura que existan físicamente las columnas que vas a ESCRIBIR en datos_pedidos ---
required_cols_main = [
    "Estado", "Fecha_Completado", "Hora_Proceso",
    "Adjuntos_Guia", "Hoja_Ruta_Mensajero",
    "Completados_Limpiado",
    "Turno", "Fecha_Entrega", "Modificacion_Surtido"
]
headers_main = ensure_columns(worksheet_main, headers_main, required_cols_main)

# Y asegura que el DataFrame también tenga esas columnas en esta ejecución
for col in required_cols_main:
    if col not in df_main.columns:
        df_main[col] = ""


if not df_main.empty:
    df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
    if changes_made_by_demorado_check:
        st.cache_data.clear()

        set_active_main_tab(st.session_state.get("active_main_tab_index", 0))
        st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
        st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
        st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)

        st.rerun()

    # --- 🔔 Alerta de Modificación de Surtido ---  
    mod_surtido_df = df_main[
        (df_main['Modificacion_Surtido'].astype(str).str.strip() != '') &
        (~df_main['Modificacion_Surtido'].astype(str).str.endswith('[✔CONFIRMADO]')) &
        (~df_main['Estado'].isin(['🟢 Completado', '✅ Viajó'])) &
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

    # === CASOS ESPECIALES (Devoluciones/Garantías) ===
    try:
        worksheet_casos = g_spread_client.open_by_key(GOOGLE_SHEET_ID).worksheet("casos_especiales")
    except gspread.exceptions.APIError as e:
        st.error(f"❌ Error al abrir 'casos_especiales': {e}")
        st.cache_resource.clear()
        time.sleep(1)
        g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        worksheet_casos = g_spread_client.open_by_key(GOOGLE_SHEET_ID).worksheet("casos_especiales")

    # Asegurar físicamente en la hoja las columnas que vamos a escribir (si faltan, se agregan)
    required_cols_casos = [
        "Estado", "Fecha_Completado", "Hora_Proceso",
        "Modificacion_Surtido", "Adjuntos_Surtido", "Adjuntos",
        "Hoja_Ruta_Mensajero",  # para guía en devoluciones
        # (estas ayudan al render/orden; no pasa nada si ya existen)
        "Folio_Factura", "Cliente", "Vendedor_Registro",
        "Tipo_Envio", "Fecha_Entrega", "Comentario",
        # 👇 nuevas para clasificar envío/turno en devoluciones
        "Tipo_Envio_Original", "Turno",
        # Campos específicos de garantías
        "Numero_Serie", "Fecha_Compra",
        "Completados_Limpiado",
    ]
    headers_casos = ensure_columns(worksheet_casos, headers_casos, required_cols_casos)
    for c in ["Numero_Serie", "Fecha_Compra", "Completados_Limpiado"]:
        if c not in df_casos.columns:
            df_casos[c] = ""

    # 📊 Resumen de Estados combinando datos_pedidos y casos_especiales
    st.markdown("### 📊 Resumen de Estados")

    def _count_states(df):
        completados_visible = df[
            (df["Estado"] == "🟢 Completado") &
            (df.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
        ]
        cancelados_visible = df[
            (df["Estado"] == "🟣 Cancelado") &
            (df.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
        ]
        return {
            '🟡 Pendiente': (df["Estado"] == '🟡 Pendiente').sum(),
            '🔵 En Proceso': (df["Estado"] == '🔵 En Proceso').sum(),
            '🔴 Demorado': (df["Estado"] == '🔴 Demorado').sum(),
            '🛠 Modificación': (df["Estado"] == '🛠 Modificación').sum(),
            '🟣 Cancelado': len(cancelados_visible),
            '🟢 Completado': len(completados_visible),
        }

    counts_main = _count_states(df_main)
    counts_casos = _count_states(df_casos)
    estado_counts = {k: counts_main.get(k, 0) + counts_casos.get(k, 0)
                     for k in ['🟡 Pendiente', '🔵 En Proceso', '🔴 Demorado', '🛠 Modificación', '🟣 Cancelado', '🟢 Completado']}

    total_pedidos_estados = sum(estado_counts.values())
    estados_fijos = ['🟡 Pendiente', '🔵 En Proceso', '🟢 Completado']
    estados_condicionales = ['🔴 Demorado', '🛠 Modificación', '🟣 Cancelado']
    estados_a_mostrar = []
    estados_a_mostrar.append(("📦 Total Pedidos", total_pedidos_estados))
    for estado in estados_fijos:
        estados_a_mostrar.append((estado, estado_counts.get(estado, 0)))
    for estado in estados_condicionales:
        cantidad = estado_counts.get(estado, 0)
        if cantidad > 0:
            estados_a_mostrar.append((estado, cantidad))
    cols = st.columns(len(estados_a_mostrar))
    for col, (nombre_estado, cantidad) in zip(cols, estados_a_mostrar):
        col.metric(nombre_estado, int(cantidad))




    # --- Implementación de Pestañas con st.tabs ---
    tab_options = [
        "📍 Pedidos Locales",
        "🚚 Pedidos Foráneos",
        "🏙️ Pedidos CDMX",
        "📋 Solicitudes de Guía",
        "🎓 Cursos y Eventos",
        "🔁 Devoluciones",
        "🛠 Garantías",
        "✅ Historial Completados/Cancelados",
    ]
    main_tabs = st.tabs(tab_options)
    components.html(f"""
    <script>
    const tabs = window.parent.document.querySelectorAll('.stTabs [data-baseweb="tab"]');
    const activeIndex = {st.session_state.get("active_main_tab_index", 0)};
    if (tabs[activeIndex]) {{
        tabs[activeIndex].click();
    }}
    tabs.forEach((tab, idx) => {{
        tab.addEventListener('click', () => {{
            const params = new URLSearchParams(window.parent.location.search);
            params.set('tab', idx);
            window.parent.history.replaceState(null, '', window.parent.location.pathname + '?' + params.toString());
        }});
    }});
    </script>
    """, height=0)

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
                                mostrar_pedido(
                                    df_main,
                                    idx,
                                    row,
                                    orden,
                                    "Mañana",
                                    "📍 Pedidos Locales",
                                    worksheet_main,
                                    headers_main,
                                    s3_client,
                                )
                                
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
                                mostrar_pedido(
                                    df_main,
                                    idx,
                                    row,
                                    orden,
                                    "Tarde",
                                    "📍 Pedidos Locales",
                                    worksheet_main,
                                    headers_main,
                                    s3_client,
                                )
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
                    mostrar_pedido(
                        df_main,
                        idx,
                        row,
                        orden,
                        "Saltillo",
                        "📍 Pedidos Locales",
                        worksheet_main,
                        headers_main,
                        s3_client,
                    )
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
                    mostrar_pedido(
                        df_main,
                        idx,
                        row,
                        orden,
                        "Pasa a Bodega",
                        "📍 Pedidos Locales",
                        worksheet_main,
                        headers_main,
                        s3_client,
                    )
            else:
                st.info("No hay pedidos para pasar a bodega.")

    with main_tabs[1]: # 🚚 Pedidos Foráneos
        pedidos_foraneos_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🚚 Pedido Foráneo")
        ].copy()
        if not pedidos_foraneos_display.empty:
            pedidos_foraneos_display = ordenar_pedidos_custom(pedidos_foraneos_display)
            for orden, (idx, row) in enumerate(pedidos_foraneos_display.iterrows(), start=1):
                mostrar_pedido(
                    df_main,
                    idx,
                    row,
                    orden,
                    "Foráneo",
                    "🚚 Pedidos Foráneos",
                    worksheet_main,
                    headers_main,
                    s3_client,
                )
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
                mostrar_pedido(
                    df_main,
                    idx,
                    row,
                    orden,
                    "CDMX",
                    "🏙️ Pedidos CDMX",
                    worksheet_main,
                    headers_main,
                    s3_client,
                )
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


    with main_tabs[4]:  # 🎓 Cursos y Eventos
        pedidos_cursos_display = df_pendientes_proceso_demorado[
            df_pendientes_proceso_demorado["Tipo_Envio"] == "🎓 Cursos y Eventos"
        ].copy()
        if not pedidos_cursos_display.empty:
            pedidos_cursos_display = ordenar_pedidos_custom(pedidos_cursos_display)
            for orden, (idx, row) in enumerate(pedidos_cursos_display.iterrows(), start=1):
                mostrar_pedido(
                    df_main,
                    idx,
                    row,
                    orden,
                    "Cursos y Eventos",
                    "🎓 Cursos y Eventos",
                    worksheet_main,
                    headers_main,
                    s3_client,
                )
        else:
            st.info("No hay pedidos de Cursos y Eventos.")

# --- TAB 5: 🔁 Devoluciones (casos_especiales) ---
with main_tabs[5]:
    st.markdown("### 🔁 Devoluciones")

    # 1) Validaciones mínimas
    if 'df_casos' not in locals() and 'df_casos' not in globals():
        st.error("❌ No se encontró el DataFrame 'df_casos'. Asegúrate de haberlo cargado antes.")

    import os
    import json
    import math
    import re
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
            ~devoluciones_display["Estado"].astype(str).str.strip().isin(["🟢 Completado", "✅ Viajó"])
        ]

    if devoluciones_display.empty:
        st.success("🎉 No hay devoluciones pendientes. (Todas están 🟢 Completado o ✅ Viajó)")

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

    def render_caso_especial_devolucion(row):
        st.markdown("### 🧾 Caso Especial – 🔁 Devolución")

        folio_new = str(row.get("Folio_Factura", "")).strip() or "N/A"
        folio_err = str(row.get("Folio_Factura_Error", "")).strip() or "N/A"
        vendedor = str(row.get("Vendedor_Registro", "")).strip() or "N/A"
        hora = str(row.get("Hora_Registro", "")).strip() or "N/A"
        st.markdown(
            f"📄 Folio Nuevo: `{folio_new}` | 📄 Folio Error: `{folio_err}` | 🧑‍💼 Vendedor: `{vendedor}` | 🕒 Hora: `{hora}`"
        )

        cliente = str(row.get("Cliente", "")).strip() or "N/A"
        rfc = str(row.get("Numero_Cliente_RFC", "")).strip() or "N/A"
        st.markdown(f"👤 Cliente: {cliente} | RFC: {rfc}")

        estado = str(row.get("Estado", "")).strip() or "N/A"
        estado_caso = str(row.get("Estado_Caso", "")).strip() or "N/A"
        turno = str(row.get("Turno", "")).strip() or "N/A"
        st.markdown(f"Estado: {estado} | Estado del Caso: {estado_caso} | Turno: {turno}")

        r_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
        r_subtipo = str(row.get("Refacturacion_Subtipo", "")).strip()
        r_folio = str(row.get("Folio_Factura_Refacturada", "")).strip()
        if any([r_tipo, r_subtipo, r_folio]):
            st.markdown("#### ♻️ Refacturación")
            bullets = []
            if r_tipo:
                bullets.append(f"- Tipo: {r_tipo}")
            if r_subtipo:
                bullets.append(f"- Subtipo: {r_subtipo}")
            if r_folio:
                bullets.append(f"- Folio refacturado: {r_folio}")
            st.markdown("\n".join(bullets))

        resultado = str(row.get("Resultado_Esperado", "")).strip()
        if resultado:
            st.markdown(f"🎯 Resultado Esperado: {resultado}")

        motivo = str(row.get("Motivo_Detallado", "")).strip()
        if motivo:
            st.markdown("📝 Motivo / Descripción:")
            st.info(motivo)

        material = str(row.get("Material_Devuelto", "")).strip()
        if material:
            st.markdown("📦 Piezas / Material:")
            st.info(material)

        monto = str(row.get("Monto_Devuelto", "")).strip()
        if monto:
            st.markdown(f"💵 Monto (dev./estimado): {monto}")

        area_resp = str(row.get("Area_Responsable", "")).strip() or "N/A"
        resp_error = str(row.get("Nombre_Responsable", "")).strip() or "N/A"
        st.markdown(f"🏢 Área Responsable: {area_resp} | 👥 Responsable del Error: {resp_error}")

        fecha_entrega = str(row.get("Fecha_Entrega", "")).strip() or "N/A"
        fecha_rec = str(row.get("Fecha_Recepcion_Devolucion", "")).strip() or "N/A"
        estado_rec = str(row.get("Estado_Recepcion", "")).strip() or "N/A"
        st.markdown(
            f"📅 Fecha Entrega/Cierre: {fecha_entrega} | 📅 Recepción: {fecha_rec} | 📦 Recepción: {estado_rec}"
        )

        nota = str(row.get("Nota_Credito_URL", "")).strip()
        if nota:
            if nota.startswith("http"):
                st.markdown(
                    f'🧾 <a href="{nota}" target="_blank">Nota de Crédito</a>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f"🧾 Nota de Crédito: {nota}")

        doc_extra = str(row.get("Documento_Adicional_URL", "")).strip()
        if doc_extra:
            if doc_extra.startswith("http"):
                st.markdown(
                    f'📂 <a href="{doc_extra}" target="_blank">Documento Adicional</a>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f"📂 Documento Adicional: {doc_extra}")

        seguimiento = str(row.get("Seguimiento", "")).strip()
        comentario = str(row.get("Comentario", "")).strip()
        coment_admin = str(row.get("Comentarios_Admin_Devolucion", "")).strip()
        if coment_admin:
            st.markdown("🗒️ Comentario Administrativo:")
            st.info(coment_admin)

        st.markdown("📌 Seguimiento:")
        st.info(seguimiento or "")

        st.markdown("📝 Comentario:")
        st.info(comentario or "")

        mod_surtido = str(row.get("Modificacion_Surtido", "")).strip()
        adj_surtido = _normalize_urls(row.get("Adjuntos_Surtido", ""))
        if mod_surtido or adj_surtido:
            st.markdown("### 🛠 Modificación de surtido")
            if mod_surtido:
                st.info(mod_surtido)
            if adj_surtido:
                st.markdown("**Archivos de modificación:**")
                for u in adj_surtido:
                    nombre = os.path.basename(urlparse(u).path) or u
                    nombre = unquote(nombre)
                    url = get_s3_file_download_url(s3_client, u)
                    st.markdown(
                        f'- <a href="{url}" target="_blank">{nombre}</a>',
                        unsafe_allow_html=True,
                    )

        adjuntos = _normalize_urls(row.get("Adjuntos", ""))
        guias = _normalize_urls(row.get("Hoja_Ruta_Mensajero", ""))
        with st.expander("📎 Archivos (Adjuntos y Guía)", expanded=False):
            contenido = False
            if adjuntos:
                contenido = True
                st.markdown("**Adjuntos:**")
                for u in adjuntos:
                    nombre = os.path.basename(urlparse(u).path) or u
                    nombre = unquote(nombre)
                    url = get_s3_file_download_url(s3_client, u)
                    st.markdown(
                        f'- <a href="{url}" target="_blank">{nombre}</a>',
                        unsafe_allow_html=True,
                    )
            if guias:
                contenido = True
                st.markdown("**Guía:**")
                for g in guias:
                    nombre = os.path.basename(g)
                    if g == "#" or not g:
                        st.error("❌ Guía no disponible.")
                        continue
                    url = get_s3_file_download_url(s3_client, g)
                    if not url or url == "#":
                        st.error(f"❌ No se pudo generar la URL para la guía {nombre}.")
                        continue
                    st.markdown(
                        f'- <a href="{url}" target="_blank">{nombre}</a>',
                        unsafe_allow_html=True,
                    )
            if not contenido:
                st.info("Sin archivos registrados en la hoja.")

    # 4) Recorrer cada devolución
    for _, row in devoluciones_display.iterrows():
        idp         = str(row.get("ID_Pedido", "")).strip()
        folio       = str(row.get("Folio_Factura", "")).strip()
        cliente     = str(row.get("Cliente", "")).strip()
        estado      = str(row.get("Estado", "Pendiente")).strip()
        vendedor    = str(row.get("Vendedor_Registro", "")).strip()
        estado_rec  = str(row.get("Estado_Recepcion", "N/A")).strip()
        area_resp   = str(row.get("Area_Responsable", "")).strip()
        row_key     = (idp or f"{folio}_{cliente}").replace(" ", "_")

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

        with st.expander(expander_title, expanded=st.session_state["expanded_devoluciones"].get(row_key, False)):
            st.session_state["expanded_devoluciones"][row_key] = True
            render_caso_especial_devolucion(row)

            # === 🆕 NUEVO: Clasificar Tipo_Envio_Original, Turno y Fecha_Entrega (sin opción vacía y sin recargar) ===
            st.markdown("---")
            st.markdown("#### 🚦 Clasificar envío y fecha")

            # Valores actuales
            tipo_envio_actual = str(row.get("Tipo_Envio_Original", "")).strip()
            turno_actual      = str(row.get("Turno", "")).strip()
            fecha_actual_str  = str(row.get("Fecha_Entrega", "")).strip()
            fecha_actual_dt   = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            today_date        = (datetime.now(_TZ).date() if _TZ else datetime.now().date())

            # Claves únicas por caso (para que los widgets no “salten”)
            tipo_key   = f"tipo_envio_orig_{row_key}"
            turno_key  = f"turno_dev_{row_key}"
            fecha_key  = f"fecha_dev_{row_key}"

            # Opciones SIN vacío
            TIPO_OPTS  = ["📍 Pedido Local", "🚚 Pedido Foráneo"]
            TURNO_OPTS = ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"]

            # Inicializar valores en session_state (solo una vez)
            if tipo_key not in st.session_state:
                # Elegir por lo que ya trae la hoja; si no cuadra, por defecto Foráneo
                if tipo_envio_actual in TIPO_OPTS:
                    st.session_state[tipo_key] = tipo_envio_actual
                else:
                    low = tipo_envio_actual.lower()
                    st.session_state[tipo_key] = "📍 Pedido Local" if "local" in low else "🚚 Pedido Foráneo"

            if turno_key not in st.session_state:
                st.session_state[turno_key] = turno_actual if turno_actual in TURNO_OPTS else TURNO_OPTS[0]

            if fecha_key not in st.session_state:
                st.session_state[fecha_key] = (
                    fecha_actual_dt.date() if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today_date else today_date
                )

            # Selects y fecha (sin opción vacía). Cambiar aquí NO guarda en Sheets.
            c1, c2, c3 = st.columns([1.2, 1.2, 1])

            with c1:
                st.selectbox(
                    "Tipo de envío original",
                    options=TIPO_OPTS,
                    index=TIPO_OPTS.index(st.session_state[tipo_key]) if st.session_state[tipo_key] in TIPO_OPTS else 1,
                    key=tipo_key,
                    on_change=preserve_tab_state,
                )

            with c2:
                st.selectbox(
                    "Turno (si Local)",
                    options=TURNO_OPTS,
                    index=TURNO_OPTS.index(st.session_state[turno_key]) if st.session_state[turno_key] in TURNO_OPTS else 0,
                    key=turno_key,
                    disabled=(st.session_state[tipo_key] != "📍 Pedido Local"),
                    help="Solo aplica para Pedido Local",
                    on_change=preserve_tab_state,
                )

            with c3:
                st.date_input(
                    "Fecha de envío",
                    value=st.session_state[fecha_key],
                    min_value= today_date,
                    max_value= today_date + timedelta(days=365),
                    format="DD/MM/YYYY",
                    key=fecha_key,
                    on_change=preserve_tab_state,
                )

            # Botón aplicar (AQUÍ SÍ se guardan cambios). No cambiamos de pestaña.
            if st.button("✅ Aplicar cambios de envío/fecha", key=f"btn_aplicar_envio_fecha_{row_key}", on_click=preserve_tab_state):
                try:
                    # Por si acaso, preservar la pestaña actual (Devoluciones es índice 4)
                    st.session_state["preserve_main_tab"] = 4

                    # Resolver fila en gsheet
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
                    else:
                        updates = []
                        changed = False

                        # 1) Tipo_Envio_Original (sin opción vacía)
                        tipo_sel = st.session_state[tipo_key]
                        if "Tipo_Envio_Original" in headers_casos and tipo_sel != tipo_envio_actual:
                            col_idx = headers_casos.index("Tipo_Envio_Original") + 1
                            updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[tipo_sel]]})
                            changed = True

                        # 2) Turno (solo si Local)
                        if tipo_sel == "📍 Pedido Local":
                            turno_sel = st.session_state[turno_key]
                            if "Turno" in headers_casos and turno_sel != turno_actual:
                                col_idx = headers_casos.index("Turno") + 1
                                updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[turno_sel]]})
                                changed = True

                        # 3) Fecha_Entrega
                        fecha_sel = st.session_state[fecha_key]
                        fecha_sel_str = fecha_sel.strftime("%Y-%m-%d")
                        if "Fecha_Entrega" in headers_casos and fecha_sel_str != fecha_actual_str:
                            col_idx = headers_casos.index("Fecha_Entrega") + 1
                            updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[fecha_sel_str]]})
                            changed = True

                        if updates and changed:
                            if batch_update_gsheet_cells(worksheet_casos, updates):
                                # Reflejar en la UI sin recargar toda la app
                                row["Tipo_Envio_Original"] = tipo_sel
                                if tipo_sel == "📍 Pedido Local":
                                    row["Turno"] = st.session_state[turno_key]
                                row["Fecha_Entrega"] = fecha_sel_str

                                st.toast("✅ Cambios aplicados.", icon="✅")
                                # 🚫 Nada de st.rerun() ni cambio de pestaña
                            else:
                                st.error("❌ No se pudieron aplicar los cambios.")
                        else:
                            st.info("ℹ️ No hubo cambios que guardar.")
                except Exception as e:
                    st.error(f"❌ Error al aplicar cambios: {e}")


            # --- 🔧 Acciones rápidas (sin imprimir, sin cambiar pestaña) ---
            st.markdown("---")
            colA, colB = st.columns(2)

            # ⚙️ Procesar → 🔵 En Proceso + Hora_Proceso (si estaba Pendiente/Demorado/Modificación)
            if colA.button("⚙️ Procesar", key=f"procesar_caso_{idp or folio or cliente}"):
                try:
                    # Mantener la pestaña de Devoluciones
                    set_active_main_tab(5)

                    # Localiza la fila en 'casos_especiales'
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
                        st.error("❌ No se encontró el caso en 'casos_especiales' para actualizar.")
                    else:
                        if estado in ["🟡 Pendiente", "🔴 Demorado", "🛠 Modificación"]:
                            now_str = mx_now_str()
                            ok = True
                            if "Estado" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Estado", "🔵 En Proceso")
                            if "Hora_Proceso" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Hora_Proceso", now_str)

                            if ok:
                                # Reflejo inmediato local sin recargar
                                row["Estado"] = "🔵 En Proceso"
                                row["Hora_Proceso"] = now_str
                                st.toast("✅ Caso marcado como '🔵 En Proceso'.", icon="✅")
                            else:
                                st.error("❌ No se pudo actualizar a 'En Proceso'.")
                        else:
                            st.info("ℹ️ Este caso ya no está en Pendiente/Demorado/Modificación.")
                except Exception as e:
                    st.error(f"❌ Error al actualizar: {e}")



            # 🔧 Procesar Modificación → pasa a 🔵 En Proceso si está en 🛠 Modificación (sin recargar)
            if estado == "🛠 Modificación":
                if colB.button("🔧 Procesar Modificación", key=f"proc_mod_caso_{idp or folio or cliente}"):
                    try:
                        # Mantener la pestaña de Devoluciones
                        set_active_main_tab(5)

                        # Localiza la fila en 'casos_especiales'
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
                        else:
                            ok = True
                            if "Estado" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Estado", "🔵 En Proceso")

                            if ok:
                                # Reflejo inmediato en pantalla, sin recargar
                                row["Estado"] = "🔵 En Proceso"
                                st.toast("🔧 Modificación procesada - Estado actualizado a '🔵 En Proceso'", icon="✅")
                            else:
                                st.error("❌ Falló la actualización del estado a 'En Proceso'.")
                    except Exception as e:
                        st.error(f"❌ Error al procesar la modificación: {e}")


            # === Sección de Modificación de Surtido (mostrar/confirmar) ===
            mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
            refact_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
            refact_subtipo = str(row.get("Refacturacion_Subtipo", "")).strip()

            if mod_texto:
                st.markdown("#### 🛠 Modificación de Surtido")
                if refact_tipo != "Datos Fiscales":
                    if mod_texto.endswith('[✔CONFIRMADO]'):
                        st.info(mod_texto)
                    else:
                        st.warning(mod_texto)
                        if st.button("✅ Confirmar Cambios de Surtido", key=f"confirm_mod_caso_{idp or folio or cliente}"):
                            try:
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
                                    st.error("❌ No se encontró el caso para confirmar la modificación.")
                                else:
                                    nuevo_texto = mod_texto + " [✔CONFIRMADO]"
                                    ok = update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Modificacion_Surtido", nuevo_texto)
                                    if ok:
                                        st.success("✅ Cambios de surtido confirmados.")
                                        st.cache_data.clear()
                                        st.rerun()
                                    else:
                                        st.error("❌ No se pudo confirmar la modificación.")
                            except Exception as e:
                                st.error(f"❌ Error al confirmar la modificación: {e}")
                else:
                    st.info("ℹ️ Modificación marcada como **Datos Fiscales** (no requiere confirmación).")
                    st.info(mod_texto)

                if refact_tipo == "Material":
                    st.markdown("**🔁 Refacturación por Material**")
                    st.info(f"📌 Tipo: **{refact_tipo}**  \n🔧 Subtipo: **{refact_subtipo}**")

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
                        st.markdown(
                            f'- <a href="{url}" target="_blank">{label}</a>',
                            unsafe_allow_html=True,
                        )
                else:
                    st.info("No hay archivos registrados para esta devolución.")

            st.markdown("---")

            st.markdown("#### 📋 Documentación")
            st.caption("La guía es opcional; puedes completar la devolución sin subirla.")
            guia_files = st.file_uploader(
                "📋 Subir Guía de Retorno (opcional)",
                key=f"guia_{folio}_{cliente}",
                help="Opcional: sube la guía de mensajería para el retorno del producto (PDF/JPG/PNG)",
                on_change=handle_generic_upload_change,
                args=(row_key, "expanded_devoluciones"),
                accept_multiple_files=True,
            )


            if st.button(
                "📤 Subir Guía",
                key=f"btn_subir_guia_{folio}_{cliente}",
                on_click=preserve_tab_state,
            ):
                try:
                    if not guia_files:
                        st.warning("⚠️ Primero selecciona al menos un archivo de guía.")
                    else:
                        folder = idp or f"caso_{(folio or 'sfolio')}_{(cliente or 'scliente')}".replace(" ", "_")
                        guia_keys = []
                        for guia_file in guia_files:
                            key_guia = f"{folder}/guia_retorno_{datetime.now().isoformat()[:19].replace(':','')}_{guia_file.name}"
                            success, tmp_key = upload_file_to_s3(s3_client, S3_BUCKET_NAME, guia_file, key_guia)
                            if success and tmp_key:
                                guia_keys.append(tmp_key)
                        if guia_keys:
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
                            else:
                                existing = str(row.get("Hoja_Ruta_Mensajero", "")).strip()
                                if existing.lower() in ("nan", "none", "n/a"):
                                    existing = ""
                                new_keys = ", ".join(guia_keys)
                                guia_final = f"{existing}, {new_keys}" if existing else new_keys
                                ok = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Hoja_Ruta_Mensajero",
                                    guia_final,
                                )
                                if ok:
                                    row["Hoja_Ruta_Mensajero"] = guia_final
                                    st.toast(f"📤 {len(guia_keys)} guía(s) subida(s) con éxito.", icon="📦")
                                    st.success(f"📦 Se subieron correctamente {len(guia_keys)} archivo(s) de guía.")
                                    set_active_main_tab(5)
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error("❌ No se pudo actualizar la guía en Google Sheets.")
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")
                except Exception as e:
                    st.error(f"❌ Error al subir la guía: {e}")

            flag_key = f"confirm_complete_id_{row['ID_Pedido']}"
            if st.button(
                "🟢 Completar",
                key=f"btn_completar_{folio}_{cliente}",
                on_click=preserve_tab_state,
            ):
                st.session_state[flag_key] = row["ID_Pedido"]

            if st.session_state.get(flag_key) == row["ID_Pedido"]:
                st.warning("¿Estás seguro de completar este pedido?")
                confirm_col, cancel_col = st.columns(2)
                with confirm_col:
                    if st.button(
                        "Confirmar",
                        key=f"confirm_completar_{folio}_{cliente}",
                        on_click=preserve_tab_state,
                    ):
                        try:
                            if not str(row.get("Hoja_Ruta_Mensajero", "")).strip():
                                st.info("Completarás la devolución sin guía.")
                            gsheet_row_idx = None
                            if "ID_Pedido" in df_casos.columns and idp:
                                matches = df_casos.index[
                                    df_casos["ID_Pedido"].astype(str).str.strip() == idp
                                ]
                                if len(matches) > 0:
                                    gsheet_row_idx = int(matches[0]) + 2
                            if gsheet_row_idx is None:
                                filt = (
                                    df_casos.get("Folio_Factura", pd.Series(dtype=str)).astype(str).str.strip().eq(folio)
                                    & df_casos.get("Cliente", pd.Series(dtype=str)).astype(str).str.strip().eq(cliente)
                                )
                                matches = (
                                    df_casos.index[filt] if hasattr(filt, "any") else []
                                )
                                if len(matches) > 0:
                                    gsheet_row_idx = int(matches[0]) + 2
                            ok = True
                            if gsheet_row_idx is None:
                                st.error(
                                    "❌ No se encontró el caso en 'casos_especiales'."
                                )
                                ok = False
                            else:
                                tipo_sel = st.session_state.get(
                                    tipo_key, tipo_envio_actual
                                )
                                if "Tipo_Envio_Original" in headers_casos:
                                    ok &= update_gsheet_cell(
                                        worksheet_casos,
                                        headers_casos,
                                        gsheet_row_idx,
                                        "Tipo_Envio_Original",
                                        tipo_sel,
                                    )
                                    row["Tipo_Envio_Original"] = tipo_sel
                                if tipo_sel == "📍 Pedido Local":
                                    turno_sel = st.session_state.get(
                                        turno_key, turno_actual
                                    )
                                    if "Turno" in headers_casos:
                                        ok &= update_gsheet_cell(
                                            worksheet_casos,
                                            headers_casos,
                                            gsheet_row_idx,
                                            "Turno",
                                            turno_sel,
                                        )
                                        row["Turno"] = turno_sel
                                ok &= update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Estado",
                                    "🟢 Completado",
                                )
                                mx_now = mx_now_str()
                                _ = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Fecha_Completado",
                                    mx_now,
                                )
                                _ = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Fecha_Entrega",
                                    mx_now,
                                )
                            if ok:
                                st.session_state[
                                    "flash_msg"
                                ] = "✅ Devolución completada correctamente."
                                set_active_main_tab(5)
                                st.cache_data.clear()
                                del st.session_state[flag_key]
                                st.rerun()
                            else:
                                st.error("❌ No se pudo completar la devolución.")
                                if flag_key in st.session_state:
                                    del st.session_state[flag_key]
                        except Exception as e:
                            st.error(f"❌ Error al completar la devolución: {e}")
                            if flag_key in st.session_state:
                                del st.session_state[flag_key]
                with cancel_col:
                    if st.button(
                        "Cancelar",
                        key=f"cancel_completar_{folio}_{cliente}",
                        on_click=preserve_tab_state,
                    ):
                        if flag_key in st.session_state:
                            del st.session_state[flag_key]


    st.markdown("---")

with main_tabs[6]:  # 🛠 Garantías
    st.markdown("### 🛠 Garantías")

    import os, json, math, re
    import pandas as pd
    try:
        from zoneinfo import ZoneInfo
        _TZ = ZoneInfo("America/Mexico_City")
    except Exception:
        _TZ = None

    # 1) Validaciones mínimas
    if 'df_casos' not in locals() and 'df_casos' not in globals():
        st.error("❌ No se encontró el DataFrame 'df_casos'. Asegúrate de haberlo cargado antes.")
        st.stop()

    # Detectar columna de tipo
    tipo_col = "Tipo_Caso" if "Tipo_Caso" in df_casos.columns else ("Tipo_Envio" if "Tipo_Envio" in df_casos.columns else None)
    if not tipo_col:
        st.error("❌ En 'casos_especiales' falta la columna 'Tipo_Caso' o 'Tipo_Envio'.")
        st.stop()

    # 2) Filtrar SOLO garantías
    garantias_display = df_casos[df_casos[tipo_col].astype(str).str.contains("Garant", case=False, na=False)].copy()
    if garantias_display.empty:
        st.info("ℹ️ No hay garantías en 'casos_especiales'.")

    # 2.1 Excluir garantías ya completadas
    if "Estado" in garantias_display.columns:
        garantias_display = garantias_display[~garantias_display["Estado"].astype(str).str.strip().isin(["🟢 Completado", "✅ Viajó"])]

    if garantias_display.empty:
        st.success("🎉 No hay garantías pendientes. (Todas están 🟢 Completado o ✅ Viajó)")

    # 3) Orden sugerido por Hora_Registro (desc) o por ID
    if "Hora_Registro" in garantias_display.columns:
        try:
            garantias_display["_FechaOrden"] = pd.to_datetime(garantias_display["Hora_Registro"], errors="coerce")
            garantias_display = garantias_display.sort_values(by="_FechaOrden", ascending=False)
        except Exception:
            pass
    elif "ID_Pedido" in garantias_display.columns:
        garantias_display = garantias_display.sort_values(by="ID_Pedido", ascending=True)

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
        seen, out = set(), []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    # ====== RECORRER CADA GARANTÍA ======
    for _, row in garantias_display.iterrows():
        idp         = str(row.get("ID_Pedido", "")).strip()
        folio       = str(row.get("Folio_Factura", "")).strip()
        cliente     = str(row.get("Cliente", "")).strip()
        estado      = str(row.get("Estado", "🟡 Pendiente")).strip()
        vendedor    = str(row.get("Vendedor_Registro", "")).strip()
        estado_rec  = str(row.get("Estado_Recepcion", "N/A")).strip()
        area_resp   = str(row.get("Area_Responsable", "")).strip()
        numero_serie = str(row.get("Numero_Serie", "")).strip()
        fecha_compra = str(row.get("Fecha_Compra", "")).strip()
        row_key     = (idp or f"{folio}_{cliente}").replace(" ", "_")

        # Título del expander
        expander_title = f"🛠 {folio or 's/folio'} – {cliente or 's/cliente'} | Estado: {estado} | Estado_Recepcion: {estado_rec}"
        with st.expander(expander_title, expanded=st.session_state["expanded_garantias"].get(row_key, False)):
            st.session_state["expanded_garantias"][row_key] = True
            st.markdown("#### 📋 Información de la Garantía")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**👤 Vendedor:** {vendedor or 'N/A'}")
                st.markdown(f"**📄 Factura de Origen:** {folio or 'N/A'}")
                st.markdown(f"**🎯 Resultado Esperado:** {str(row.get('Resultado_Esperado', 'N/A')).strip()}")
                st.markdown(f"**🏷️ Número Cliente/RFC:** {str(row.get('Numero_Cliente_RFC', 'N/A')).strip()}")
            with col2:
                st.markdown(f"**🏢 Área Responsable:** {area_resp or 'N/A'}")
                st.markdown(f"**👥 Responsable del Error:** {str(row.get('Nombre_Responsable', 'N/A')).strip()}")
                st.markdown(f"**🔢 Número de Serie:** {numero_serie or 'N/A'}")
                st.markdown(f"**📅 Fecha de Compra:** {fecha_compra or 'N/A'}")

            # Motivo / piezas / monto (en garantía guardamos piezas en Material_Devuelto y monto estimado en Monto_Devuelto)
            st.markdown("**📝 Motivo / Descripción de la falla:**")
            st.info(str(row.get("Motivo_Detallado", "")).strip() or "N/A")

            st.markdown("**🧰 Piezas afectadas:**")
            st.info(str(row.get("Material_Devuelto", "")).strip() or "N/A")

            monto_txt = str(row.get("Monto_Devuelto", "")).strip()
            seguimiento_txt = str(row.get("Seguimiento", "")).strip()
            if monto_txt:
                st.markdown(f"**💵 Monto estimado (si aplica):** {monto_txt}")

            # Comentario administrativo (admin)
            coment_admin = str(row.get("Comentarios_Admin_Garantia", "")).strip() or str(row.get("Comentarios_Admin_Devolucion", "")).strip()
            if coment_admin:
                st.markdown("**📝 Comentario Administrativo:**")
                st.info(coment_admin)

            if seguimiento_txt:
                st.markdown("**📌 Seguimiento:**")
                st.info(seguimiento_txt)

            # === Clasificar envío/turno/fecha (igual que devoluciones) ===
            st.markdown("---")
            st.markdown("#### 🚦 Clasificar envío y fecha")

            # Valores actuales
            tipo_envio_actual = str(row.get("Tipo_Envio_Original", "")).strip()
            turno_actual      = str(row.get("Turno", "")).strip()
            fecha_actual_str  = str(row.get("Fecha_Entrega", "")).strip()
            fecha_actual_dt   = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            today_date        = (datetime.now(_TZ).date() if _TZ else datetime.now().date())

            tipo_key   = f"g_tipo_envio_orig_{row_key}"
            turno_key  = f"g_turno_{row_key}"
            fecha_key  = f"g_fecha_{row_key}"

            TIPO_OPTS  = ["📍 Pedido Local", "🚚 Pedido Foráneo"]
            TURNO_OPTS = ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"]

            # Inicialización en session_state
            if tipo_key not in st.session_state:
                if tipo_envio_actual in TIPO_OPTS:
                    st.session_state[tipo_key] = tipo_envio_actual
                else:
                    low = tipo_envio_actual.lower()
                    st.session_state[tipo_key] = "📍 Pedido Local" if "local" in low else "🚚 Pedido Foráneo"

            if turno_key not in st.session_state:
                st.session_state[turno_key] = turno_actual if turno_actual in TURNO_OPTS else TURNO_OPTS[0]

            if fecha_key not in st.session_state:
                st.session_state[fecha_key] = (
                    fecha_actual_dt.date() if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today_date else today_date
                )

            c1, c2, c3 = st.columns([1.2, 1.2, 1])
            with c1:
                st.selectbox(
                    "Tipo de envío original",
                    options=TIPO_OPTS,
                    index=TIPO_OPTS.index(st.session_state[tipo_key]) if st.session_state[tipo_key] in TIPO_OPTS else 1,
                    key=tipo_key,
                    on_change=preserve_tab_state,
                )
            with c2:
                st.selectbox(
                    "Turno (si Local)",
                    options=TURNO_OPTS,
                    index=TURNO_OPTS.index(st.session_state[turno_key]) if st.session_state[turno_key] in TURNO_OPTS else 0,
                    key=turno_key,
                    disabled=(st.session_state[tipo_key] != "📍 Pedido Local"),
                    help="Solo aplica para Pedido Local",
                    on_change=preserve_tab_state,
                )
            with c3:
                st.date_input(
                    "Fecha de envío",
                    value=st.session_state[fecha_key],
                    min_value=today_date,
                    max_value=today_date + timedelta(days=365),
                    format="DD/MM/YYYY",
                    key=fecha_key,
                    on_change=preserve_tab_state,
                )

            # Guardar cambios de envío/fecha
            if st.button("✅ Aplicar cambios de envío/fecha (Garantía)", key=f"btn_aplicar_envio_fecha_g_{row_key}", on_click=preserve_tab_state):
                try:
                    # Resolver fila en gsheet
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
                    else:
                        updates = []
                        changed = False

                        tipo_sel = st.session_state[tipo_key]
                        if "Tipo_Envio_Original" in headers_casos and tipo_sel != tipo_envio_actual:
                            col_idx = headers_casos.index("Tipo_Envio_Original") + 1
                            updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[tipo_sel]]})
                            changed = True

                        if tipo_sel == "📍 Pedido Local":
                            turno_sel = st.session_state[turno_key]
                            if "Turno" in headers_casos and turno_sel != turno_actual:
                                col_idx = headers_casos.index("Turno") + 1
                                updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[turno_sel]]})
                                changed = True

                        fecha_sel = st.session_state[fecha_key]
                        fecha_sel_str = fecha_sel.strftime("%Y-%m-%d")
                        if "Fecha_Entrega" in headers_casos and fecha_sel_str != fecha_actual_str:
                            col_idx = headers_casos.index("Fecha_Entrega") + 1
                            updates.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_idx, col_idx), 'values': [[fecha_sel_str]]})
                            changed = True

                        if updates and changed:
                            if batch_update_gsheet_cells(worksheet_casos, updates):
                                # Reflejar
                                row["Tipo_Envio_Original"] = tipo_sel
                                if tipo_sel == "📍 Pedido Local":
                                    row["Turno"] = st.session_state[turno_key]
                                row["Fecha_Entrega"] = fecha_sel_str
                                st.toast("✅ Cambios aplicados.", icon="✅")
                            else:
                                st.error("❌ No se pudieron aplicar los cambios.")
                        else:
                            st.info("ℹ️ No hubo cambios que guardar.")
                except Exception as e:
                    st.error(f"❌ Error al aplicar cambios: {e}")

            # --- Acciones rápidas ---
            st.markdown("---")
            colA, colB = st.columns(2)

            # ⚙️ Procesar
            if colA.button("⚙️ Procesar", key=f"procesar_g_{idp or folio or cliente}"):
                try:
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
                        st.error("❌ No se encontró el caso en 'casos_especiales' para actualizar.")
                    else:
                        if estado in ["🟡 Pendiente", "🔴 Demorado", "🛠 Modificación"]:
                            now_str = mx_now_str()
                            ok = True
                            if "Estado" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Estado", "🔵 En Proceso")
                            if "Hora_Proceso" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Hora_Proceso", now_str)

                            if ok:
                                row["Estado"] = "🔵 En Proceso"
                                row["Hora_Proceso"] = now_str
                                st.toast("✅ Caso marcado como '🔵 En Proceso'.", icon="✅")
                            else:
                                st.error("❌ No se pudo actualizar a 'En Proceso'.")
                        else:
                            st.info("ℹ️ Este caso ya no está en Pendiente/Demorado/Modificación.")
                except Exception as e:
                    st.error(f"❌ Error al actualizar: {e}")

            # 🔧 Procesar Modificación
            if estado == "🛠 Modificación":
                if colB.button("🔧 Procesar Modificación", key=f"proc_mod_g_{idp or folio or cliente}"):
                    try:
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
                        else:
                            ok = True
                            if "Estado" in headers_casos:
                                ok &= update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Estado", "🔵 En Proceso")

                            if ok:
                                row["Estado"] = "🔵 En Proceso"
                                st.toast("🔧 Modificación procesada - Estado actualizado a '🔵 En Proceso'", icon="✅")
                            else:
                                st.error("❌ Falló la actualización del estado a 'En Proceso'.")
                    except Exception as e:
                        st.error(f"❌ Error al procesar la modificación: {e}")

            # === Sección de Modificación de Surtido (similar a devoluciones) ===
            mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
            refact_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
            refact_subtipo = str(row.get("Refacturacion_Subtipo", "")).strip()

            if mod_texto:
                st.markdown("#### 🛠 Modificación de Surtido")
                if refact_tipo != "Datos Fiscales":
                    if mod_texto.endswith('[✔CONFIRMADO]'):
                        st.info(mod_texto)
                    else:
                        st.warning(mod_texto)
                        if st.button("✅ Confirmar Cambios de Surtido (Garantía)", key=f"confirm_mod_g_{idp or folio or cliente}"):
                            try:
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
                                    st.error("❌ No se encontró el caso para confirmar la modificación.")
                                else:
                                    nuevo_texto = mod_texto + " [✔CONFIRMADO]"
                                    ok = update_gsheet_cell(worksheet_casos, headers_casos, gsheet_row_idx, "Modificacion_Surtido", nuevo_texto)
                                    if ok:
                                        st.success("✅ Cambios de surtido confirmados.")
                                        st.cache_data.clear()
                                        st.rerun()
                                    else:
                                        st.error("❌ No se pudo confirmar la modificación.")
                            except Exception as e:
                                st.error(f"❌ Error al confirmar la modificación: {e}")
                else:
                    st.info("ℹ️ Modificación marcada como **Datos Fiscales** (no requiere confirmación).")
                    st.info(mod_texto)

                if refact_tipo == "Material":
                    st.markdown("**🔁 Refacturación por Material**")
                    st.info(f"📌 Tipo: **{refact_tipo}**  \n🔧 Subtipo: **{refact_subtipo}**")

            st.markdown("---")

            # === Archivos del Caso (Adjuntos + Dictamen/Nota + Adicional) ===
            with st.expander("📎 Archivos del Caso (Garantía)", expanded=False):
                adjuntos_urls = _normalize_urls(row.get("Adjuntos", ""))
                # Prioriza dictamen de garantía; si no existe, cae a Nota_Credito_URL
                dictamen_url = str(row.get("Dictamen_Garantia_URL", "")).strip()
                nota_credito_url = str(row.get("Nota_Credito_URL", "")).strip()
                principal_url = dictamen_url or nota_credito_url
                doc_adic_url = str(row.get("Documento_Adicional_URL", "")).strip()

                items = []
                for u in adjuntos_urls:
                    if u:
                        file_name = os.path.basename(urlparse(u).path) or u
                        file_name = unquote(file_name)
                        presigned = get_s3_file_download_url(s3_client, u)
                        items.append((file_name, presigned))
                if principal_url and principal_url.lower() not in ("nan", "none", "n/a"):
                    label_p = "Dictamen de Garantía" if dictamen_url else "Nota de Crédito"
                    presigned = get_s3_file_download_url(s3_client, principal_url)
                    items.append((label_p, presigned))
                if doc_adic_url and doc_adic_url.lower() not in ("nan", "none", "n/a"):
                    presigned = get_s3_file_download_url(s3_client, doc_adic_url)
                    items.append(("Documento Adicional", presigned))

                if items:
                    for label, url in items:
                        st.markdown(
                            f'- <a href="{url}" target="_blank">{label}</a>',
                            unsafe_allow_html=True,
                        )
                else:
                    st.info("No hay archivos registrados para esta garantía.")

            st.markdown("---")

            # === Guía y completar ===
            st.markdown("#### 📋 Documentación")
            st.caption("La guía es opcional; puedes completar la garantía sin subirla.")
            guia_files = st.file_uploader(
                "📋 Subir Guía de Envío/Retorno (Garantía) (opcional)",
                key=f"guia_g_{folio}_{cliente}",
                help="Opcional: sube la guía de mensajería para envío de reposición o retorno (PDF/JPG/PNG)",
                on_change=handle_generic_upload_change,
                args=(row_key, "expanded_garantias"),
                accept_multiple_files=True,
            )


            if st.button(
                "📤 Subir Guía",
                key=f"btn_subir_guia_g_{folio}_{cliente}",
                on_click=preserve_tab_state,
            ):
                try:
                    if not guia_files:
                        st.warning("⚠️ Primero selecciona al menos un archivo de guía.")
                    else:
                        folder = idp or f"garantia_{(folio or 'sfolio')}_{(cliente or 'scliente')}".replace(" ", "_")
                        guia_keys = []
                        for guia_file in guia_files:
                            key_guia = f"{folder}/guia_garantia_{datetime.now().isoformat()[:19].replace(':','')}_{guia_file.name}"
                            success, tmp_key = upload_file_to_s3(s3_client, S3_BUCKET_NAME, guia_file, key_guia)
                            if success and tmp_key:
                                guia_keys.append(tmp_key)
                        if guia_keys:
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
                            else:
                                existing = str(row.get("Hoja_Ruta_Mensajero", "")).strip()
                                if existing.lower() in ("nan", "none", "n/a"):
                                    existing = ""
                                new_keys = ", ".join(guia_keys)
                                guia_final = f"{existing}, {new_keys}" if existing else new_keys
                                ok = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Hoja_Ruta_Mensajero",
                                    guia_final,
                                )
                                if ok:
                                    row["Hoja_Ruta_Mensajero"] = guia_final
                                    st.toast(f"📤 {len(guia_keys)} guía(s) subida(s) con éxito.", icon="📦")
                                    st.success(f"📦 Se subieron correctamente {len(guia_keys)} archivo(s) de guía.")
                                    set_active_main_tab(6)
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error("❌ No se pudo actualizar la guía en Google Sheets.")
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")
                except Exception as e:
                    st.error(f"❌ Error al subir la guía: {e}")

            flag_key = f"confirm_complete_id_{row['ID_Pedido']}"
            if st.button(
                "🟢 Completar Garantía",
                key=f"btn_completar_g_{folio}_{cliente}",
                on_click=preserve_tab_state,
            ):
                st.session_state[flag_key] = row["ID_Pedido"]

            if st.session_state.get(flag_key) == row["ID_Pedido"]:
                st.warning("¿Estás seguro de completar este pedido?")
                confirm_col, cancel_col = st.columns(2)
                with confirm_col:
                    if st.button(
                        "Confirmar",
                        key=f"confirm_completar_g_{folio}_{cliente}",
                        on_click=preserve_tab_state,
                    ):
                        try:
                            if not str(row.get("Hoja_Ruta_Mensajero", "")).strip():
                                st.info("Completarás la garantía sin guía.")
                            gsheet_row_idx = None
                            if "ID_Pedido" in df_casos.columns and idp:
                                matches = df_casos.index[
                                    df_casos["ID_Pedido"].astype(str).str.strip() == idp
                                ]
                                if len(matches) > 0:
                                    gsheet_row_idx = int(matches[0]) + 2
                            if gsheet_row_idx is None:
                                filt = (
                                    df_casos.get("Folio_Factura", pd.Series(dtype=str)).astype(str).str.strip().eq(folio)
                                    & df_casos.get("Cliente", pd.Series(dtype=str)).astype(str).str.strip().eq(cliente)
                                )
                                matches = (
                                    df_casos.index[filt] if hasattr(filt, "any") else []
                                )
                                if len(matches) > 0:
                                    gsheet_row_idx = int(matches[0]) + 2
                            ok = True
                            if gsheet_row_idx is None:
                                st.error(
                                    "❌ No se encontró el caso en 'casos_especiales'."
                                )
                                ok = False
                            else:
                                tipo_sel = st.session_state.get(
                                    tipo_key, tipo_envio_actual
                                )
                                if "Tipo_Envio_Original" in headers_casos:
                                    ok &= update_gsheet_cell(
                                        worksheet_casos,
                                        headers_casos,
                                        gsheet_row_idx,
                                        "Tipo_Envio_Original",
                                        tipo_sel,
                                    )
                                    row["Tipo_Envio_Original"] = tipo_sel
                                if tipo_sel == "📍 Pedido Local":
                                    turno_sel = st.session_state.get(
                                        turno_key, turno_actual
                                    )
                                    if "Turno" in headers_casos:
                                        ok &= update_gsheet_cell(
                                            worksheet_casos,
                                            headers_casos,
                                            gsheet_row_idx,
                                            "Turno",
                                            turno_sel,
                                        )
                                        row["Turno"] = turno_sel
                                ok &= update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Estado",
                                    "🟢 Completado",
                                )
                                mx_now = mx_now_str()
                                _ = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Fecha_Completado",
                                    mx_now,
                                )
                                _ = update_gsheet_cell(
                                    worksheet_casos,
                                    headers_casos,
                                    gsheet_row_idx,
                                    "Fecha_Entrega",
                                    mx_now,
                                )
                            if ok:
                                st.session_state[
                                    "flash_msg"
                                ] = "✅ Garantía completada correctamente."
                                set_active_main_tab(6)
                                st.cache_data.clear()
                                del st.session_state[flag_key]
                                st.rerun()
                            else:
                                st.error("❌ No se pudo completar la garantía.")
                                if flag_key in st.session_state:
                                    del st.session_state[flag_key]
                        except Exception as e:
                            st.error(f"❌ Error al completar la garantía: {e}")
                            if flag_key in st.session_state:
                                del st.session_state[flag_key]
                with cancel_col:
                    if st.button(
                        "Cancelar",
                        key=f"cancel_completar_g_{folio}_{cliente}",
                        on_click=preserve_tab_state,
                    ):
                        if flag_key in st.session_state:
                            del st.session_state[flag_key]
                            

with main_tabs[7]:  # ✅ Historial Completados
    df_completados_historial = df_main[
        (df_main["Estado"] == "🟢 Completado") &
        (df_main.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
    ].copy()

    df_completados_historial['_gsheet_row_index'] = df_completados_historial['_gsheet_row_index'].astype(int)

    tipo_casos_col = None
    if 'Tipo_Caso' in df_casos.columns:
        tipo_casos_col = 'Tipo_Caso'
    elif 'Tipo_Envio' in df_casos.columns:
        tipo_casos_col = 'Tipo_Envio'
    df_casos_completados = df_casos[
        (df_casos["Estado"] == "🟢 Completado") &
        (df_casos.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
    ].copy()
    if not df_casos_completados.empty:
        df_casos_completados['_gsheet_row_index'] = df_casos_completados['_gsheet_row_index'].astype(int)

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
                set_active_main_tab(7)
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
                        set_active_main_tab(7)
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
                    set_active_main_tab(7)
                    st.rerun()

        df_completados_historial = df_completados_historial.sort_values(by="Fecha_Completado", ascending=False)
        for orden, (idx, row) in enumerate(df_completados_historial.iterrows(), start=1):
            mostrar_pedido(
                df_main,
                idx,
                row,
                orden,
                "Historial",
                "✅ Historial Completados",
                worksheet_main,
                headers_main,
                s3_client,
            )
    else:
        st.info("No hay pedidos completados recientes o ya fueron limpiados.")

    # === Casos Especiales Completados ===
    if tipo_casos_col:
        if not df_casos_completados.empty:
            def render_caso_especial_garantia_hist(row):
                st.markdown("### 🧾 Caso Especial – 🛠 Garantía")
                folio = str(row.get("Folio_Factura", "")).strip() or "N/A"
                vendedor = str(row.get("Vendedor_Registro", "")).strip() or "N/A"
                cliente = str(row.get("Cliente", "")).strip() or "N/A"
                st.markdown(f"📄 Factura: `{folio}` | 🧑‍💼 Vendedor: `{vendedor}`")
                st.markdown(f"👤 Cliente: {cliente}")
                estado = str(row.get("Estado", "")).strip() or "N/A"
                estado_rec = str(row.get("Estado_Recepcion", "")).strip() or "N/A"
                st.markdown(f"Estado: {estado} | Estado Recepción: {estado_rec}")
                numero_serie = str(row.get("Numero_Serie", "")).strip() or "N/A"
                fecha_compra = str(row.get("Fecha_Compra", "")).strip() or "N/A"
                st.markdown(f"🔢 Número de Serie: {numero_serie} | 📅 Fecha de Compra: {fecha_compra}")
                motivo = str(row.get("Motivo_Detallado", "")).strip()
                if motivo:
                    st.markdown("📝 Motivo / Descripción:")
                    st.info(motivo)
                piezas = str(row.get("Material_Devuelto", "")).strip()
                if piezas:
                    st.markdown("📦 Piezas afectadas:")
                    st.info(piezas)
                monto = str(row.get("Monto_Devuelto", "")).strip()
                if monto:
                    st.markdown(f"💵 Monto estimado: {monto}")
                adjuntos = _normalize_urls(row.get("Adjuntos", ""))
                guia = str(row.get("Hoja_Ruta_Mensajero", "")).strip()
                with st.expander("📎 Archivos del Caso", expanded=False):
                    contenido = False
                    if adjuntos:
                        contenido = True
                        st.markdown("**Adjuntos:**")
                        for u in adjuntos:
                            nombre = os.path.basename(urlparse(u).path) or u
                            nombre = unquote(nombre)
                            st.markdown(f"- [{nombre}]({u})")
                    if guia:
                        contenido = True
                        st.markdown("**Guía:**")
                        if guia.startswith("http"):
                            st.markdown(f"[Abrir guía]({guia})")
                        else:
                            st.markdown(guia)
                    if not contenido:
                        st.info("Sin archivos registrados en la hoja.")

            # Devoluciones completadas
            comp_dev = df_casos_completados[df_casos_completados[tipo_casos_col].astype(str).str.contains("Devoluci", case=False, na=False)]
            if not comp_dev.empty:
                st.markdown("### 🔁 Devoluciones Completadas")
                if st.button("🧹 Limpiar Devoluciones Completadas"):
                    col_idx = headers_casos.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            'range': gspread.utils.rowcol_to_a1(int(row['_gsheet_row_index']), col_idx),
                            'values': [["sí"]]
                        }
                        for _, row in comp_dev.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_casos, updates):
                        st.success(f"✅ {len(updates)} devoluciones marcadas como limpiadas.")
                        st.cache_data.clear()
                        set_active_main_tab(7)
                        st.rerun()
                comp_dev = comp_dev.sort_values(by="Fecha_Completado", ascending=False)
                for _, row in comp_dev.iterrows():
                    with st.expander(f"🔁 {row.get('Folio_Factura', 'N/A')} – {row.get('Cliente', 'N/A')}"):
                        render_caso_especial_devolucion(row)

            # Garantías completadas
            comp_gar = df_casos_completados[df_casos_completados[tipo_casos_col].astype(str).str.contains("Garant", case=False, na=False)]
            if not comp_gar.empty:
                st.markdown("### 🛠 Garantías Completadas")
                if st.button("🧹 Limpiar Garantías Completadas"):
                    col_idx = headers_casos.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            'range': gspread.utils.rowcol_to_a1(int(row['_gsheet_row_index']), col_idx),
                            'values': [["sí"]]
                        }
                        for _, row in comp_gar.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_casos, updates):
                        st.success(f"✅ {len(updates)} garantías marcadas como limpiadas.")
                        st.cache_data.clear()
                        set_active_main_tab(7)
                        st.rerun()
                comp_gar = comp_gar.sort_values(by="Fecha_Completado", ascending=False)
                for _, row in comp_gar.iterrows():
                    with st.expander(f"🛠 {row.get('Folio_Factura', 'N/A')} – {row.get('Cliente', 'N/A')}"):
                        render_caso_especial_garantia_hist(row)
        else:
            st.info("No hay casos especiales completados o ya fueron limpiados.")
