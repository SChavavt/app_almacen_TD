import streamlit as st
import pandas as pd
import numpy as np
import boto3
import gspread
import pdfplumber
import json
import re
import unicodedata
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse, unquote
from datetime import datetime, timedelta, date
import uuid
import urllib.parse
import urllib.request
import time
import calendar
from zoneinfo import ZoneInfo

# --- CONFIGURACIÓN DE STREAMLIT ---
st.set_page_config(page_title="📦 Panel de Gestión", layout="wide")
st.title("📦 Panel de Gestión")

MESES_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

MEXICO_CITY_TZ = ZoneInfo("America/Mexico_City")

def now_cdmx() -> datetime:
    """Fecha/hora actual en zona horaria de Ciudad de México."""
    return datetime.now(MEXICO_CITY_TZ)

# ===== SPREADSHEETS =====
SPREADSHEET_ID_MAIN = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SPREADSHEET_ID_ALEJANDRO = "1lWZEL228boUMH_tAdQ3_ZGkYHZZuEkfv"
_ALE_ID_CACHE = {}
_ALE_BOOTSTRAP_CACHE = {}
_MAIN_SPREADSHEET_CACHE = None
_COBRANZA_SPREADSHEET_CACHE = None
_COBRANZA_WS_CACHE = None
_COBRANZA_VALUES_CACHE = {}


def _cobranza_cache_key(ws):
    """Clave estable por spreadsheet+worksheet para evitar colisiones de cache."""
    ws_id = getattr(ws, "id", None)
    ss_id = getattr(getattr(ws, "spreadsheet", None), "id", None)
    return (ss_id, ws_id) if ws_id is not None else None

# --- CREDENCIALES DESDE SECRETS ---
try:
    credentials_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gspread_client = gspread.authorize(creds)
except Exception as e:
    st.error(f"❌ Error al autenticar con Google Sheets: {e}")
    st.stop()

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"]["aws_region"]
    )
    S3_BUCKET = st.secrets["aws"]["s3_bucket_name"]
    AWS_REGION = st.secrets["aws"]["aws_region"]
except Exception as e:
    st.error(f"❌ Error al autenticar con AWS S3: {e}")
    st.stop()

def get_worksheet():
    """Obtiene la hoja de cálculo principal de pedidos."""
    return get_main_worksheet("datos_pedidos")


def _is_transient_gspread_error(exc: Exception) -> bool:
    """Determina si un APIError de gspread parece transitorio (quota/rate/5xx)."""
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    text = str(exc).lower()
    transient_codes = {429, 500, 502, 503, 504}
    if status_code in transient_codes:
        return True
    # En Streamlit Cloud, algunos errores se redactan y llegan sin status_code legible.
    # En esos casos preferimos reintentar para evitar fallos espurios al recargar la app.
    if status_code is None and "redacted" in text:
        return True
    return any(token in text for token in ["quota", "ratelimit", "rate limit", "backend error", "timeout"])


def _retry_gspread_api_call(fn, retries: int = 5, base_delay: float = 0.8):
    """Ejecuta `fn` con reintentos ante APIError transitorio (o redacted sin status)."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            last_exc = exc
            if attempt == retries - 1:
                raise
            # Si luce permanente, damos un intento adicional corto y luego dejamos propagar.
            if (not _is_transient_gspread_error(exc)) and attempt >= 1:
                raise
            time.sleep(base_delay * (attempt + 1))

    if last_exc:
        raise last_exc


def get_main_spreadsheet(force_refresh: bool = False):
    """Abre y cachea el spreadsheet principal con reintentos para errores transitorios."""
    global _MAIN_SPREADSHEET_CACHE

    if _MAIN_SPREADSHEET_CACHE is not None and not force_refresh:
        return _MAIN_SPREADSHEET_CACHE

    _MAIN_SPREADSHEET_CACHE = _retry_gspread_api_call(
        lambda: gspread_client.open_by_key(SPREADSHEET_ID_MAIN),
        retries=4,
        base_delay=0.8,
    )
    return _MAIN_SPREADSHEET_CACHE


def get_main_worksheet(nombre_hoja: str):
    """Obtiene una worksheet del spreadsheet principal con fallback de recarga de metadata."""
    try:
        return _retry_gspread_api_call(
            lambda: get_main_spreadsheet().worksheet(nombre_hoja),
            retries=4,
            base_delay=0.8,
        )
    except gspread.exceptions.APIError as exc:
        if not _is_transient_gspread_error(exc):
            raise
        return _retry_gspread_api_call(
            lambda: get_main_spreadsheet(force_refresh=True).worksheet(nombre_hoja),
            retries=4,
            base_delay=1.0,
        )


def _get_all_records_with_retry(sheet, retries: int = 3):
    """Lee registros de una hoja con reintentos para errores transitorios de Google API."""
    return _retry_gspread_api_call(
        lambda: sheet.get_all_records(),
        retries=max(retries, 4),
        base_delay=0.9,
    )


PEDIDOS_SHEETS = ("datos_pedidos", "data_pedidos")
PEDIDOS_COLUMNAS_MINIMAS = [
    "ID_Pedido", "Hora_Registro", "Cliente", "Estado", "Vendedor_Registro", "Folio_Factura",
    "Comentario", "Comentarios", "Modificacion_Surtido", "Adjuntos_Surtido", "Adjuntos_Guia",
    "Adjuntos", "Direccion_Guia_Retorno", "Nota_Venta", "Tiene_Nota_Venta", "Motivo_NotaVenta",
    "Refacturacion_Tipo", "Refacturacion_Subtipo", "Folio_Factura_Refacturada", "fecha_modificacion", "Fecha_Modificacion"
]

# ===== ALEJANDRO DATA (Organizador) =====
ALE_SHEETS = (
    "CONFIG",
    "CITAS",
    "TAREAS",
    "COTIZACIONES",
    "CHECKLIST_TEMPLATE",
    "CHECKLIST_DAILY",
    "EVENT_LOG",
)

ALE_COLUMNAS = {
    "CONFIG": ["Key", "Value", "Descripcion", "Updated_At", "Updated_By"],

    "CITAS": [
        "Cita_ID","Created_At","Created_By","Fecha_Inicio","Fecha_Fin","Cliente_Persona","Empresa_Clinica",
        "Tipo","Prioridad","Estatus","Notas","Lugar","Telefono","Correo","Reminder_Minutes_Before",
        "Reminder_Status","Last_Updated_At","Last_Updated_By","Is_Deleted"
    ],

    "TAREAS": [
        "Tarea_ID","Created_At","Created_By","Titulo","Descripcion","Fecha_Limite","Prioridad","Estatus",
        "Cliente_Relacionado","Cotizacion_Folio_Relacionado","Tipo","Fecha_Completado","Notas_Resultado",
        "Last_Updated_At","Last_Updated_By","Is_Deleted"
    ],

    "COTIZACIONES": [
        "Cotizacion_ID","Folio","Created_At","Created_By","Fecha_Cotizacion","Cliente","Monto","Vendedor",
        "Estatus","Fecha_Proximo_Seguimiento","Ultimo_Seguimiento_Fecha","Dias_Sin_Seguimiento","Notas",
        "Resultado_Cierre","Convertida_A_Tarea_ID","Convertida_A_Cita_ID","Last_Updated_At","Last_Updated_By","Is_Deleted"
    ],

    "CHECKLIST_TEMPLATE": ["Item_ID","Orden","Item","Activo"],

    "CHECKLIST_DAILY": ["Fecha","Item_ID","Item","Completado","Completado_At","Completado_By","Notas"],

    "EVENT_LOG": ["Event_ID","Created_At","User","Modulo","Accion","Entidad_ID","Detalle"],
}


def _empty_pedidos_df(nombre_hoja: str) -> pd.DataFrame:
    """Construye un DataFrame vacío de pedidos con columnas mínimas + metadata."""
    df = pd.DataFrame(columns=PEDIDOS_COLUMNAS_MINIMAS)
    for c in PEDIDOS_COLUMNAS_MINIMAS:
        if c not in df.columns:
            df[c] = ""
    df["__hoja_origen"] = nombre_hoja
    df["__sheet_row"] = pd.Series(dtype="int")
    return df


def cargar_hoja_pedidos(nombre_hoja):
    """Carga una hoja de pedidos por nombre y garantiza columnas mínimas.

    Si falla por error transitorio/redacted en una hoja secundaria, intenta fallback
    y, en último caso, devuelve DataFrame vacío para no tumbar la app completa.
    """
    try:
        sheet = get_main_worksheet(nombre_hoja)
        data = _get_all_records_with_retry(sheet)
    except gspread.exceptions.APIError as e:
        # Fallback específico: si falla data_pedidos, intentar datos_pedidos.
        if nombre_hoja == "data_pedidos":
            try:
                sheet = get_main_worksheet("datos_pedidos")
                data = _get_all_records_with_retry(sheet)
                nombre_hoja = "datos_pedidos"
                st.warning("⚠️ No se pudo leer 'data_pedidos'. Se usó fallback a 'datos_pedidos'.")
            except Exception:
                st.warning(f"⚠️ No se pudo leer la hoja '{nombre_hoja}' (Google API). Se mostrará vacío temporalmente.")
                return _empty_pedidos_df(nombre_hoja)
        else:
            st.warning(f"⚠️ No se pudo leer la hoja '{nombre_hoja}' (Google API). Se mostrará vacío temporalmente.")
            return _empty_pedidos_df(nombre_hoja)
    except Exception:
        st.warning(f"⚠️ No se pudo leer la hoja '{nombre_hoja}'. Se mostrará vacío temporalmente.")
        return _empty_pedidos_df(nombre_hoja)

    df = pd.DataFrame(data)
    for c in PEDIDOS_COLUMNAS_MINIMAS:
        if c not in df.columns:
            df[c] = ""
    # Metadata interna para saber desde qué worksheet proviene cada pedido.
    # Se usa al momento de guardar cambios para escribir en la hoja correcta.
    df["__hoja_origen"] = nombre_hoja
    # Fila real en Google Sheets (considerando encabezado en fila 1).
    # Se usa para asegurar que las modificaciones se escriban en el pedido correcto.
    df["__sheet_row"] = df.index + 2
    return df

def _extract_sheet_id(value: str) -> str:
    """Extrae el spreadsheet_id si viene URL completa, si no devuelve el valor limpio."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw)
    if m:
        return m.group(1)
    return raw


def _is_truthy(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "si", "sí", "on"}


def _drive_api_get_file_meta(file_id: str) -> dict:
    """Consulta metadata de Drive sin googleapiclient."""
    token = creds.get_access_token().access_token
    params = urllib.parse.urlencode({
        "fields": "id,name,mimeType,shortcutDetails/targetId,shortcutDetails/targetMimeType",
        "supportsAllDrives": "true",
    })
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _drive_api_copy_as_gsheet(file_id: str, original_name: str = "") -> dict:
    """Copia un archivo de Drive convirtiéndolo a Google Sheet nativo."""
    token = creds.get_access_token().access_token
    new_name = (original_name or "alejandro_data")
    if new_name.lower().endswith('.xlsx'):
        new_name = new_name[:-5]
    body = {
        "name": f"{new_name} (AUTO-CONVERTED TD)",
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    params = urllib.parse.urlencode({
        "supportsAllDrives": "true",
        "fields": "id,name,mimeType",
    })
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}/copy?{params}"
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _create_bootstrap_alejandro_sheet(base_name: str = "alejandro_data") -> tuple[str, dict]:
    """Crea un Google Sheet nativo con las hojas/headers esperados para Organizador."""
    title = f"{base_name} (AUTO-BOOTSTRAP TD)"
    ss = gspread_client.create(title)

    first_name = ALE_SHEETS[0]
    ws0 = ss.sheet1
    ws0.update_title(first_name)
    cols0 = ALE_COLUMNAS.get(first_name, [])
    if cols0:
        ws0.append_row(cols0, value_input_option="USER_ENTERED")

    for name in ALE_SHEETS[1:]:
        cols = ALE_COLUMNAS.get(name, [])
        ws = ss.add_worksheet(title=name, rows=1000, cols=max(len(cols), 20))
        if cols:
            ws.append_row(cols, value_input_option="USER_ENTERED")

    meta = {
        "id": ss.id,
        "name": ss.title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "bootstrap_created": True,
    }
    return ss.id, meta


def _ensure_alejandro_structure_in_spreadsheet(spreadsheet_id: str):
    """Asegura hojas/headers de Alejandro dentro de un spreadsheet existente."""
    ss = gspread_client.open_by_key(spreadsheet_id)
    existing = {w.title: w for w in ss.worksheets()}

    for name in ALE_SHEETS:
        cols = ALE_COLUMNAS.get(name, [])
        if name not in existing:
            ws = ss.add_worksheet(title=name, rows=1000, cols=max(len(cols), 20))
            if cols:
                ws.append_row(cols, value_input_option="USER_ENTERED")
        else:
            ws = existing[name]
            if cols:
                current = [c.strip() for c in ws.row_values(1) if str(c).strip()]
                if not current:
                    ws.append_row(cols, value_input_option="USER_ENTERED")


def _resolve_alejandro_file_id(file_id: str) -> tuple[str, dict]:
    """Resuelve shortcut->target y, si viene Excel, intenta auto-convertir a Google Sheet."""
    if file_id in _ALE_ID_CACHE:
        return _ALE_ID_CACHE[file_id]

    gs = st.secrets.get("gsheets", {})
    allow_main_fallback = _is_truthy(gs.get("ALLOW_ALEJANDRO_MAIN_FALLBACK", "0"))
    allow_bootstrap = _is_truthy(gs.get("ALLOW_ALEJANDRO_BOOTSTRAP", "0"))

    try:
        meta = _drive_api_get_file_meta(file_id)
    except Exception as e:
        if "quotaExceeded" in str(e) or "Drive storage quota" in str(e):
            if allow_main_fallback:
                # Fallback opcional: usar spreadsheet principal existente.
                _ensure_alejandro_structure_in_spreadsheet(SPREADSHEET_ID_MAIN)
                fallback_meta = {
                    "id": SPREADSHEET_ID_MAIN,
                    "name": "MAIN_SPREADSHEET_FALLBACK",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "quota_fallback": True,
                }
                _ALE_ID_CACHE[file_id] = (SPREADSHEET_ID_MAIN, fallback_meta)
                return SPREADSHEET_ID_MAIN, fallback_meta
            raise Exception(
                "Drive rechazó el acceso por cuota (quotaExceeded) al archivo de Alejandro y el fallback a SPREADSHEET_ID_MAIN está desactivado. "
                "Para forzarlo temporalmente define gsheets.ALLOW_ALEJANDRO_MAIN_FALLBACK = 1."
            )
        raise

    mime = str(meta.get("mimeType", ""))

    if mime == "application/vnd.google-apps.shortcut":
        target_id = meta.get("shortcutDetails", {}).get("targetId", "")
        target_meta = _drive_api_get_file_meta(target_id) if target_id else {}
        target_mime = str(target_meta.get("mimeType", ""))
        if target_id and target_mime == "application/vnd.google-apps.spreadsheet":
            _ALE_ID_CACHE[file_id] = (target_id, target_meta)
            return target_id, target_meta
        raise Exception(
            f"Shortcut no apunta a Google Sheet. shortcut={file_id}, target={target_id or 'N/A'}, target_mime={target_mime or 'N/A'}"
        )

    if mime == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        try:
            converted = _drive_api_copy_as_gsheet(file_id, meta.get("name", "alejandro_data"))
            converted_id = str(converted.get("id", "")).strip()
            if not converted_id:
                raise Exception("Drive no devolvió id al convertir el archivo")
            conv_meta = _drive_api_get_file_meta(converted_id)
            _ALE_ID_CACHE[file_id] = (converted_id, conv_meta)
            return converted_id, conv_meta
        except Exception as conv_err:
            if ("quotaExceeded" in str(conv_err)) or ("403" in str(conv_err)):
                if allow_bootstrap:
                    # Fallback opcional: crear/usar bootstrap dedicado.
                    if file_id in _ALE_BOOTSTRAP_CACHE:
                        return _ALE_BOOTSTRAP_CACHE[file_id]
                    boot_id, boot_meta = _create_bootstrap_alejandro_sheet(meta.get("name", "alejandro_data"))
                    _ALE_BOOTSTRAP_CACHE[file_id] = (boot_id, boot_meta)
                    _ALE_ID_CACHE[file_id] = (boot_id, boot_meta)
                    return boot_id, boot_meta
                if allow_main_fallback:
                    # Fallback opcional: usar spreadsheet principal existente.
                    _ensure_alejandro_structure_in_spreadsheet(SPREADSHEET_ID_MAIN)
                    fallback_meta = {
                        "id": SPREADSHEET_ID_MAIN,
                        "name": "MAIN_SPREADSHEET_FALLBACK",
                        "mimeType": "application/vnd.google-apps.spreadsheet",
                        "quota_fallback": True,
                    }
                    _ALE_ID_CACHE[file_id] = (SPREADSHEET_ID_MAIN, fallback_meta)
                    return SPREADSHEET_ID_MAIN, fallback_meta
            raise Exception(
                "El archivo configurado es Excel (.xlsx) y no se pudo auto-convertir a Google Sheet con la service account. "
                f"name={meta.get('name','N/A')}, fileId={file_id}, detalle={conv_err}. "
                "Por seguridad ahora NO escribimos en datos_pedidos salvo que habilites gsheets.ALLOW_ALEJANDRO_MAIN_FALLBACK=1."
            )

    if mime != "application/vnd.google-apps.spreadsheet":
        raise Exception(
            "El archivo configurado no es Google Sheet nativo. "
            f"name={meta.get('name','N/A')}, mimeType={mime or 'N/A'}, fileId={file_id}."
        )

    _ALE_ID_CACHE[file_id] = (file_id, meta)
    return file_id, meta


def get_alejandro_spreadsheet_id() -> str:
    """Obtiene y valida el ID de Alejandro (resuelve shortcuts vía Drive API HTTP)."""
    gs = st.secrets.get("gsheets", {})
    candidate = (
        gs.get("spreadsheet_id_alejandro")
        or gs.get("SPREADSHEET_ID_ALEJANDRO")
        or SPREADSHEET_ID_ALEJANDRO
    )
    configured = _extract_sheet_id(candidate)
    resolved, _ = _resolve_alejandro_file_id(configured)
    return resolved


def get_alejandro_worksheet(nombre_hoja: str):
    """Abre una worksheet del spreadsheet alejandro_data por nombre."""
    spreadsheet_id = get_alejandro_spreadsheet_id()
    return gspread_client.open_by_key(spreadsheet_id).worksheet(nombre_hoja)


@st.cache_data(ttl=180, show_spinner=False)
def cargar_alejandro_hoja(nombre_hoja: str) -> pd.DataFrame:
    """Carga una hoja de alejandro_data y garantiza columnas mínimas."""
    sheet = get_alejandro_worksheet(nombre_hoja)
    data = _get_all_records_with_retry(sheet)
    df = pd.DataFrame(data)

    cols_min = ALE_COLUMNAS.get(nombre_hoja, [])
    for c in cols_min:
        if c not in df.columns:
            df[c] = ""

    return df


def now_iso():
    return now_cdmx().strftime("%Y-%m-%d %H:%M:%S")


def new_id(prefix: str) -> str:
    # Ej: CITA-20260224-AB12CD34
    return f"{prefix}-{now_cdmx().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"


def ensure_headers(sheet, nombre_hoja: str):
    """Asegura que la fila 1 tenga headers esperados."""
    expected = ALE_COLUMNAS.get(nombre_hoja, [])
    if not expected:
        return

    current = sheet.row_values(1)
    current = [c.strip() for c in current if str(c).strip()]

    if not current:
        # hoja vacía -> ponemos headers sin usar update(A1), que falla en algunos documentos
        try:
            sheet.append_row(expected, value_input_option="USER_ENTERED")
        except Exception:
            # fallback por si append no está permitido en esa hoja/documento
            sheet.insert_row(expected, index=1, value_input_option="USER_ENTERED")
        return

    # Si ya hay headers, pero faltan columnas, NO reescribimos (para no romper nada)
    faltan = [c for c in expected if c not in current]
    if faltan:
        # Solo avisamos (no rompemos), y trabajamos con expected para append
        # Nota: si quieres, luego hacemos "migración" de headers. Por ahora MVP.
        pass


def safe_append(nombre_hoja: str, row_dict: dict):
    """Append seguro por orden de ALE_COLUMNAS."""
    sheet = get_alejandro_worksheet(nombre_hoja)
    try:
        ensure_headers(sheet, nombre_hoja)
    except Exception:
        # No bloqueamos el alta si falla la validación/creación de headers
        pass

    cols = ALE_COLUMNAS.get(nombre_hoja, [])
    if not cols:
        raise Exception(f"No hay columnas definidas para {nombre_hoja}")

    row = [row_dict.get(c, "") for c in cols]
    try:
        sheet.append_row(row, value_input_option="USER_ENTERED")
        cargar_alejandro_hoja.clear()
    except Exception as e:
        msg = str(e)
        if "not supported for this document" in msg.lower():
            raise Exception(
                "Google API rechazó la escritura: ese ID apunta a un archivo no compatible (normalmente Excel en Drive sin convertir) "
                "o a un objeto que no es Google Sheet. Convierte el archivo a Google Sheets y usa su ID nativo en "
                "gsheets.spreadsheet_id_alejandro o gsheets.SPREADSHEET_ID_ALEJANDRO."
            ) from e
        raise


def safe_update_by_id(nombre_hoja: str, id_col: str, id_value: str, updates: dict):
    """
    Actualiza una fila en alejandro_data buscando por id_col == id_value.
    updates = {"Estatus": "Completada", "Fecha_Completado": "...", ...}
    """
    sheet = get_alejandro_worksheet(nombre_hoja)
    ensure_headers(sheet, nombre_hoja)

    headers = sheet.row_values(1)
    headers = [h.strip() for h in headers]

    if id_col not in headers:
        raise Exception(f"No existe la columna '{id_col}' en {nombre_hoja}")

    id_idx = headers.index(id_col) + 1  # 1-based

    # leer columna de IDs (desde fila 2)
    col_vals = sheet.col_values(id_idx)[1:]  # sin header
    try:
        pos0 = next(i for i, v in enumerate(col_vals) if str(v).strip() == str(id_value).strip())
    except StopIteration:
        raise Exception(f"No se encontró {id_col}={id_value} en {nombre_hoja}")

    row_number = pos0 + 2  # +2 porque col_vals arranca en fila 2

    # update en una sola llamada compatible con versiones viejas de gspread
    cells = []
    for k, v in updates.items():
        if k not in headers:
            continue
        col = headers.index(k) + 1
        cells.append(gspread.Cell(row=row_number, col=col, value=v))

    if not cells:
        return False

    sheet.update_cells(cells, value_input_option="USER_ENTERED")
    cargar_alejandro_hoja.clear()
    return True


def safe_delete_rows_by_filter(nombre_hoja: str, predicate) -> int:
    """Elimina filas de una hoja cuando predicate(record) == True. Devuelve cantidad eliminada."""
    sheet = get_alejandro_worksheet(nombre_hoja)
    ensure_headers(sheet, nombre_hoja)
    data = _get_all_records_with_retry(sheet)

    rows_to_delete = []
    for idx, rec in enumerate(data, start=2):  # start=2 por header en fila 1
        try:
            if predicate(rec):
                rows_to_delete.append(idx)
        except Exception:
            continue

    for row_num in sorted(rows_to_delete, reverse=True):
        # Compatibilidad gspread: algunas versiones solo exponen delete_row (singular)
        _retry_gspread_api_call(
            (lambda rn=row_num: sheet.delete_rows(rn))
            if hasattr(sheet, "delete_rows")
            else (lambda rn=row_num: sheet.delete_row(rn)),
            retries=4,
            base_delay=0.7,
        )
        # Pausa mínima para reducir picos de cuota al borrar múltiples filas consecutivas.
        time.sleep(0.12)

    if rows_to_delete:
        cargar_alejandro_hoja.clear()

    return len(rows_to_delete)


def debug_alejandro_documento() -> dict:
    """Diagnóstico de alejandro_data con metadata real de Drive + gspread."""
    gs = st.secrets.get("gsheets", {})
    configured_raw = (
        gs.get("spreadsheet_id_alejandro")
        or gs.get("SPREADSHEET_ID_ALEJANDRO")
        or SPREADSHEET_ID_ALEJANDRO
    )
    configured_id = _extract_sheet_id(configured_raw)

    out = {
        "spreadsheet_id": configured_id,
        "configured_raw": str(configured_raw),
        "resolved_spreadsheet_id": "",
        "drive_name": "",
        "drive_mimeType": "",
        "auto_converted": False,
        "bootstrap_created": False,
        "quota_fallback": False,
        "open_ok": False,
        "title": "",
        "url": "",
        "worksheets": [],
        "missing_expected_sheets": [],
    }

    try:
        resolved_id, meta = _resolve_alejandro_file_id(configured_id)
        out["resolved_spreadsheet_id"] = resolved_id
        out["drive_name"] = meta.get("name", "")
        out["drive_mimeType"] = meta.get("mimeType", "")
        out["url"] = f"https://docs.google.com/spreadsheets/d/{resolved_id}"
        out["auto_converted"] = (resolved_id != configured_id)
        out["bootstrap_created"] = bool(meta.get("bootstrap_created", False))
        out["quota_fallback"] = bool(meta.get("quota_fallback", False))

        ss = gspread_client.open_by_key(resolved_id)
        out["open_ok"] = True
        out["title"] = ss.title

        ws = ss.worksheets()
        names = []
        for w in ws:
            props = getattr(w, "_properties", {}) or {}
            names.append(w.title)
            out["worksheets"].append({
                "title": w.title,
                "id": props.get("sheetId"),
                "sheetType": props.get("sheetType", "UNKNOWN"),
                "rows": props.get("gridProperties", {}).get("rowCount"),
                "cols": props.get("gridProperties", {}).get("columnCount"),
            })
        out["missing_expected_sheets"] = [s for s in ALE_SHEETS if s not in names]

    except Exception as e:
        out["error"] = str(e)

    return out



# --- FUNCIONES ---
@st.cache_data(ttl=300)
def cargar_pedidos():
    """Carga y combina pedidos desde datos_pedidos + data_pedidos."""
    pedidos_frames = [cargar_hoja_pedidos(nombre_hoja) for nombre_hoja in PEDIDOS_SHEETS]
    if not pedidos_frames:
        return pd.DataFrame(columns=PEDIDOS_COLUMNAS_MINIMAS)
    return pd.concat(pedidos_frames, ignore_index=True, sort=False)


@st.cache_data(ttl=300)
def cargar_pedidos_modificables():
    """Carga solo pedidos de data_pedidos para la pestaña de modificación."""
    return cargar_hoja_pedidos("data_pedidos").copy()
@st.cache_data(ttl=300)
def cargar_casos_especiales():
    """
    Lee la hoja 'casos_especiales' y regresa un DataFrame.
    Si faltan columnas del ejemplo, las crea vacías para evitar KeyError.
    """
    sheet = get_main_worksheet("casos_especiales")
    data = _get_all_records_with_retry(sheet)
    df = pd.DataFrame(data)

    columnas_ejemplo = [
        "ID_Pedido","Hora_Registro","Vendedor_Registro","Cliente","Folio_Factura","Folio_Factura_Error","Tipo_Envio",
        "Fecha_Entrega","Comentario","Adjuntos","Estado","Resultado_Esperado","Material_Devuelto",
        "Monto_Devuelto","Motivo_Detallado","Area_Responsable","Nombre_Responsable","Fecha_Completado",
        "Completados_Limpiado","Estado_Caso","Hoja_Ruta_Mensajero","Numero_Cliente_RFC","Tipo_Envio_Original",
        "Tipo_Caso","Fecha_Recepcion_Devolucion","Estado_Recepcion","Nota_Credito_URL","Documento_Adicional_URL",
        "Seguimiento",
        "Comentarios_Admin_Devolucion","Modificacion_Surtido","Adjuntos_Surtido","Refacturacion_Tipo",
        "Refacturacion_Subtipo","Folio_Factura_Refacturada","Turno","Hora_Proceso","fecha_modificacion","Fecha_Modificacion",
        # Campos específicos de garantías
        "Numero_Serie","Fecha_Compra",
        "Comentario","Comentarios","Direccion_Guia_Retorno","Nota_Venta",
        "Tiene_Nota_Venta","Motivo_NotaVenta"
    ]
    for c in columnas_ejemplo:
        if c not in df.columns:
            df[c] = ""
    return df


@st.cache_data(ttl=300)
def cargar_todos_los_pedidos():
    """Carga todos los pedidos combinando datos_pedidos + data_pedidos."""
    return cargar_pedidos().copy()


def construir_descarga_completados_sin_limpieza():
    """
    Construye el DataFrame para la descarga "Solo pedidos 🟢 Completados sin limpiar".

    Incluye:
    - Todos los pedidos de la hoja data_pedidos.
    - Solo pedidos de casos_especiales con Completados_Limpiado vacío.

    También crea la columna '#' con numeración única según tipo de registro:
    - Foráneos (data_pedidos): 01, 02, ..., 10, 11, ...
    - Locales (data_pedidos): 101, 102, 103, ...
    - Casos especiales sin limpiar: 001, 002, 003, ...
    """
    columnas_salida = [
        "#", "Vendedor_Registro", "Folio_Factura", "Cliente", "Hora_Registro",
        "Tipo_Envio", "Turno", "Fecha_Entrega", "Estado"
    ]

    df_data = cargar_hoja_pedidos("data_pedidos").copy()
    df_casos = cargar_casos_especiales().copy()

    if "Completados_Limpiado" not in df_casos.columns:
        df_casos["Completados_Limpiado"] = ""
    df_casos = df_casos[df_casos["Completados_Limpiado"].astype(str).str.strip() == ""]

    for col in columnas_salida[1:]:
        if col not in df_data.columns:
            df_data[col] = ""
        if col not in df_casos.columns:
            df_casos[col] = ""

    foraneo_count = 1
    local_count = 101
    ids_data = []
    for _, row in df_data.iterrows():
        tipo_envio = normalizar(str(row.get("Tipo_Envio", "") or ""))
        if "foraneo" in tipo_envio:
            ids_data.append(f"{foraneo_count:02d}")
            foraneo_count += 1
        else:
            ids_data.append(str(local_count))
            local_count += 1
    df_data["#"] = ids_data

    df_casos = df_casos.reset_index(drop=True)
    df_casos["#"] = (df_casos.index + 1).map(lambda n: f"{n:03d}")

    salida = pd.concat(
        [df_data[columnas_salida], df_casos[columnas_salida]],
        ignore_index=True,
        sort=False,
    )
    return salida


def construir_descarga_flujo_por_categoria():
    """Construye pedidos en flujo separados por Locales, Foráneos y Casos especiales.

    En cada bloque se muestra primero el registro más reciente (última fila de la hoja),
    pero se conserva la numeración natural para que el más reciente tenga el número mayor.
    """
    columnas_salida = [
        "#", "Vendedor_Registro", "Folio_Factura", "Cliente", "Hora_Registro",
        "Tipo_Envio", "Turno", "Fecha_Entrega", "Estado"
    ]

    df_data = cargar_hoja_pedidos("data_pedidos").copy()
    df_casos = cargar_casos_especiales().copy()

    if "Completados_Limpiado" not in df_casos.columns:
        df_casos["Completados_Limpiado"] = ""
    df_casos = df_casos[df_casos["Completados_Limpiado"].astype(str).str.strip() == ""]

    for col in columnas_salida[1:]:
        if col not in df_data.columns:
            df_data[col] = ""
        if col not in df_casos.columns:
            df_casos[col] = ""

    tipos_normalizados = df_data["Tipo_Envio"].astype(str).map(normalizar)
    mask_foraneos = tipos_normalizados.str.contains("foraneo", na=False)

    df_foraneos = df_data[mask_foraneos].copy().reset_index(drop=True)
    df_locales = df_data[~mask_foraneos].copy().reset_index(drop=True)
    df_casos = df_casos.reset_index(drop=True)

    # Numeración en orden natural de captura (de arriba a abajo en la hoja)
    df_foraneos["#"] = (df_foraneos.index + 1).map(lambda n: f"{n:02d}")
    df_locales["#"] = (df_locales.index + 101).astype(str)
    df_casos["#"] = (df_casos.index + 1).map(lambda n: f"{n:03d}")

    # Orden visual: más reciente primero (última fila capturada)
    df_locales = df_locales.iloc[::-1].reset_index(drop=True)
    df_foraneos = df_foraneos.iloc[::-1].reset_index(drop=True)
    df_casos = df_casos.iloc[::-1].reset_index(drop=True)

    return {
        "locales": df_locales[columnas_salida],
        "foraneos": df_foraneos[columnas_salida],
        "casos": df_casos[columnas_salida],
    }


def construir_descarga_solo_completados():
    """
    Construye el DataFrame para la vista "🟢 Solo pedidos completados".

    Incluye:
    - Pedidos completados de data_pedidos.
    - Casos especiales completados con Completados_Limpiado vacío.
    """
    df_data = cargar_hoja_pedidos("data_pedidos").copy()
    df_casos = cargar_casos_especiales().copy()

    if "Estado" not in df_data.columns:
        df_data["Estado"] = ""
    if "Estado" not in df_casos.columns:
        df_casos["Estado"] = ""
    if "Completados_Limpiado" not in df_casos.columns:
        df_casos["Completados_Limpiado"] = ""

    mask_data_completados = df_data["Estado"].astype(str).str.lower().str.contains("complet", na=False)
    df_data = df_data[mask_data_completados]

    mask_casos_completados = df_casos["Estado"].astype(str).str.lower().str.contains("complet", na=False)
    mask_casos_no_limpiados = df_casos["Completados_Limpiado"].astype(str).str.strip() == ""
    df_casos = df_casos[mask_casos_completados & mask_casos_no_limpiados]

    return pd.concat([df_data, df_casos], ignore_index=True, sort=False)


def render_descarga_tabla(df_base, key_prefix, permitir_filtros=True, ordenar_por_id=True, mostrar_descarga=True):
    """Renderiza una tabla de descarga con filtros y botón de exportación."""
    df = df_base.copy()

    if df.empty:
        st.info("No hay datos disponibles para descargar.")
        return

    if "Hora_Registro" in df.columns:
        df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")

    if ordenar_por_id and "ID_Pedido" in df.columns:
        df["ID_Pedido"] = pd.to_numeric(df["ID_Pedido"], errors="coerce")
        df = df.sort_values(by="ID_Pedido", ascending=True)

    filtrado = df

    if permitir_filtros:
        rango_tiempo = st.selectbox(
            "Rango de tiempo",
            ["12 horas", "24 horas", "7 días", "Todos"],
            key=f"{key_prefix}_rango_tiempo",
        )
        estados_sel = st.multiselect(
            "Estado",
            sorted(df["Estado"].dropna().unique()) if "Estado" in df.columns else [],
            key=f"{key_prefix}_estado",
        )
        tipos_sel = st.multiselect(
            "Tipo de envío",
            sorted(df["Tipo_Envio"].dropna().unique()) if "Tipo_Envio" in df.columns else [],
            key=f"{key_prefix}_tipo_envio",
        )

        delta = None
        if rango_tiempo == "12 horas":
            delta = timedelta(hours=12)
        elif rango_tiempo == "24 horas":
            delta = timedelta(hours=24)
        elif rango_tiempo == "7 días":
            delta = timedelta(days=7)

        if delta is not None and "Hora_Registro" in filtrado.columns:
            filtrado = filtrado[filtrado["Hora_Registro"] >= datetime.now() - delta]
        if estados_sel and "Estado" in filtrado.columns:
            filtrado = filtrado[filtrado["Estado"].isin(estados_sel)]
        if tipos_sel and "Tipo_Envio" in filtrado.columns:
            filtrado = filtrado[filtrado["Tipo_Envio"].isin(tipos_sel)]

    filtrado = filtrado.drop(columns=["ID_Pedido"], errors="ignore")
    filtrado = filtrado.reset_index(drop=True)

    st.markdown(f"{len(filtrado)} registros encontrados")
    st.dataframe(filtrado, hide_index=True, use_container_width=True)

    if mostrar_descarga:
        buffer = BytesIO()
        with pd.ExcelWriter(
            buffer,
            engine="xlsxwriter",
            engine_kwargs={"options": {"strings_to_urls": False}},
        ) as writer:
            filtrado.to_excel(writer, index=False, sheet_name="Pedidos")
        buffer.seek(0)

        st.download_button(
            label="⬇️ Descargar Excel",
            data=buffer.getvalue(),
            file_name="pedidos_filtrados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_download_excel",
        )


def construir_excel_flujo_unificado(flujo_data):
    """Genera un Excel con 3 hojas (Foráneos, Locales y Casos especiales)."""
    buffer = BytesIO()
    with pd.ExcelWriter(
        buffer,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    ) as writer:
        for hoja, key in (("Foraneos", "foraneos"), ("Locales", "locales"), ("Casos_especiales", "casos")):
            df_hoja = flujo_data.get(key, pd.DataFrame()).copy()
            df_hoja = df_hoja.drop(columns=["ID_Pedido"], errors="ignore")
            df_hoja.to_excel(writer, index=False, sheet_name=hoja)
    buffer.seek(0)
    return buffer


def partir_urls(value):
    """
    Devuelve una lista de URLs limpia a partir de un string que puede venir
    como JSON, CSV, separado por ; o saltos de línea.
    """
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "n/a"):
        return []
    urls = []
    # Intento JSON
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, str) and it.strip():
                    urls.append(it.strip())
                elif isinstance(it, dict):
                    for k in ("url", "URL", "href", "link"):
                        if k in it and str(it[k]).strip():
                            urls.append(str(it[k]).strip())
        elif isinstance(obj, dict):
            for k in ("url", "URL", "href", "link"):
                if k in obj and str(obj[k]).strip():
                    urls.append(str(obj[k]).strip())
    except Exception:
        # Split por coma, punto y coma o salto de línea
        for p in re.split(r"[,\n;]+", s):
            p = p.strip()
            if p:
                urls.append(p)

    # Quitar duplicados preservando orden
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def obtener_prefijo_s3(pedido_id):
    posibles_prefijos = [
        f"{pedido_id}/", f"adjuntos_pedidos/{pedido_id}/",
        f"adjuntos_pedidos/{pedido_id}", f"{pedido_id}"
    ]
    for prefix in posibles_prefijos:
        try:
            respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
            if "Contents" in respuesta:
                return prefix if prefix.endswith("/") else prefix + "/"
        except Exception:
            continue
    return None

def obtener_archivos_pdf_validos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        archivos = respuesta.get("Contents", [])
        return [f for f in archivos if f["Key"].lower().endswith(".pdf") and any(x in f["Key"].lower() for x in ["guia", "guía", "descarga"])]
    except Exception as e:
        st.error(f"❌ Error al listar archivos en S3 para prefijo {prefix}: {e}")
        return []

def obtener_todos_los_archivos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        return respuesta.get("Contents", [])
    except Exception:
        return []

def extraer_texto_pdf(s3_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        with pdfplumber.open(BytesIO(response["Body"].read())) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        return f"[ERROR AL LEER PDF]: {e}"


# --- AWS S3 Helper Functions ---
INLINE_EXT = (".pdf", ".jpg", ".jpeg", ".png", ".webp")

def upload_file_to_s3(s3_client_param, bucket_name, file_obj, s3_key):
    try:
        lower_key = s3_key.lower() if isinstance(s3_key, str) else ""
        is_inline = lower_key.endswith(INLINE_EXT)
        put_kwargs = {
            "Bucket": bucket_name,
            "Key": s3_key,
            "Body": file_obj.getvalue(),
            "ContentDisposition": "inline" if is_inline else "attachment",  # FORCE INLINE VIEW
        }
        if hasattr(file_obj, "type") and file_obj.type:
            put_kwargs["ContentType"] = file_obj.type
        s3_client_param.put_object(**put_kwargs)
        permanent_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, permanent_url
    except Exception as e:
        st.error(f"❌ Error al subir archivo a S3: {e}")
        return False, None


def extract_s3_key(url_or_key: str) -> str:
    if not isinstance(url_or_key, str):
        return url_or_key
    parsed = urlparse(url_or_key)
    if parsed.scheme and parsed.netloc:
        return unquote(parsed.path.lstrip("/"))
    return url_or_key


def get_s3_file_download_url(s3_client_param, object_key_or_url, expires_in=604800):
    if not s3_client_param or not S3_BUCKET:
        st.error("❌ Configuración de S3 incompleta. Verifica el cliente y el nombre del bucket.")
        return "#"
    try:
        clean_key = extract_s3_key(object_key_or_url)
        params = {"Bucket": S3_BUCKET, "Key": clean_key}
        if isinstance(clean_key, str):
            lower_key = clean_key.lower()
            if lower_key.endswith(INLINE_EXT):
                filename = (clean_key.split("/")[-1] or "archivo").replace('"', "")
                params["ResponseContentDisposition"] = f'inline; filename="{filename}"'  # FORCE INLINE VIEW
                if lower_key.endswith(".pdf"):
                    params["ResponseContentType"] = "application/pdf"
                elif lower_key.endswith((".jpg", ".jpeg")):
                    params["ResponseContentType"] = "image/jpeg"
                elif lower_key.endswith(".png"):
                    params["ResponseContentType"] = "image/png"
                elif lower_key.endswith(".webp"):
                    params["ResponseContentType"] = "image/webp"

        return s3_client_param.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expires_in,
        )
    except Exception as e:
        st.error(f"❌ Error al generar URL prefirmada: {e}")
        return "#"


def resolver_nombre_y_enlace(valor, etiqueta_fallback):
    """Genera nombre legible y URL usable para valores guardados en la hoja.

    Si el valor es una URL S3/AWS, regenera una prefirmada para forzar
    cabeceras inline en PDF/imágenes y evitar descargas automáticas.
    """
    valor = str(valor).strip()
    if not valor:
        return None, None

    parsed = urlparse(valor)
    nombre_crudo = extract_s3_key(valor)
    nombre = nombre_crudo.split("/")[-1] if nombre_crudo else ""
    if not nombre:
        nombre = etiqueta_fallback

    if parsed.scheme and parsed.netloc:
        host = (parsed.netloc or "").lower()
        s3_domains = (
            ".amazonaws.com",
            ".s3.amazonaws.com",
        )
        if any(domain in host for domain in s3_domains):
            enlace = get_s3_file_download_url(s3_client, valor)
            if not enlace or enlace == "#":
                enlace = valor
        else:
            enlace = valor
    else:
        enlace = get_s3_file_download_url(s3_client, valor)
        if not enlace or enlace == "#":
            enlace = valor

    return nombre, enlace

def combinar_urls_existentes(existente, nuevas):
    """Combina listas de URLs respetando el formato previo (JSON o separado por comas/semicolons)."""
    existentes = partir_urls(existente)
    total = existentes + [u for u in nuevas if u not in existentes]
    existente = str(existente).strip()
    if existente.startswith('[') or existente.startswith('{'):
        return json.dumps(total, ensure_ascii=False)
    if ';' in existente:
        return '; '.join(total)
    return ', '.join(total)

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower()


def normalizar_folio(texto):
    """Normaliza folios ignorando acentos, mayúsculas y espacios."""
    if texto is None:
        return ""
    limpio = normalizar(str(texto).strip())
    limpio_sin_espacios = re.sub(r"\s+", "", limpio)
    return limpio_sin_espacios.upper()


def obtener_fecha_modificacion(row):
    """Devuelve la fecha de modificación sin importar el nombre exacto de la columna."""
    return str(row.get("Fecha_Modificacion") or row.get("fecha_modificacion") or "").strip()


def preparar_resultado_caso(row):
    """Convierte una fila de la hoja `casos_especiales` en un diccionario uniforme."""
    return {
        "__source": "casos",
        "ID_Pedido": str(row.get("ID_Pedido", "")).strip(),
        "Cliente": row.get("Cliente", ""),
        "Vendedor": row.get("Vendedor_Registro", ""),
        "Folio": row.get("Folio_Factura", ""),
        "Folio_Factura_Error": row.get("Folio_Factura_Error", ""),
        "Hora_Registro": row.get("Hora_Registro", ""),
        "Tipo_Envio": row.get("Tipo_Envio", ""),
        "Estado": row.get("Estado", ""),
        "Estado_Caso": row.get("Estado_Caso", ""),
        "Resultado_Esperado": row.get("Resultado_Esperado", ""),
        "Material_Devuelto": row.get("Material_Devuelto", ""),
        "Monto_Devuelto": row.get("Monto_Devuelto", ""),
        "Motivo_Detallado": row.get("Motivo_Detallado", ""),
        "Area_Responsable": row.get("Area_Responsable", ""),
        "Nombre_Responsable": row.get("Nombre_Responsable", ""),
        "Numero_Cliente_RFC": row.get("Numero_Cliente_RFC", ""),
        "Tipo_Envio_Original": row.get("Tipo_Envio_Original", ""),
        "Fecha_Entrega": row.get("Fecha_Entrega", ""),
        "Fecha_Recepcion_Devolucion": row.get("Fecha_Recepcion_Devolucion", ""),
        "Estado_Recepcion": row.get("Estado_Recepcion", ""),
        "Nota_Credito_URL": row.get("Nota_Credito_URL", ""),
        "Documento_Adicional_URL": row.get("Documento_Adicional_URL", ""),
        "Seguimiento": row.get("Seguimiento", ""),
        "Comentarios_Admin_Devolucion": row.get("Comentarios_Admin_Devolucion", ""),
        "Turno": row.get("Turno", ""),
        "Hora_Proceso": row.get("Hora_Proceso", ""),
        "Numero_Serie": row.get("Numero_Serie", ""),
        "Fecha_Compra": row.get("Fecha_Compra", ""),
        "Comentario": str(row.get("Comentario", "")).strip(),
        "Comentarios": str(row.get("Comentarios", "")).strip(),
        "Direccion_Guia_Retorno": str(row.get("Direccion_Guia_Retorno", "")).strip(),
        "Nota_Venta": str(row.get("Nota_Venta", "")).strip(),
        "Tiene_Nota_Venta": str(row.get("Tiene_Nota_Venta", "")).strip(),
        "Motivo_NotaVenta": str(row.get("Motivo_NotaVenta", "")).strip(),
        # 🛠 Modificación de surtido
        "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
        "Fecha_Modificacion_Surtido": obtener_fecha_modificacion(row),
        "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
        # ♻️ Refacturación
        "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo", "")).strip(),
        "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo", "")).strip(),
        "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada", "")).strip(),
        # Archivos del caso
        "Adjuntos_urls": partir_urls(row.get("Adjuntos", "")),
        "Guias_urls": partir_urls(row.get("Hoja_Ruta_Mensajero", "")),
    }


def render_caso_especial(res):
    """Renderiza en pantalla la información de un caso especial."""
    titulo = f"🧾 Caso Especial – {res.get('Tipo_Envio','') or 'N/A'}"
    st.markdown(f"### {titulo}")

    tipo_envio_val = str(res.get('Tipo_Envio',''))
    is_devolucion = (tipo_envio_val.strip() == "🔁 Devolución")
    is_garantia = "garant" in tipo_envio_val.lower()
    if is_devolucion:
        folio_nuevo = res.get("Folio","") or "N/A"
        folio_error = res.get("Folio_Factura_Error","") or "N/A"
        st.markdown(
            f"📄 **Folio Nuevo:** `{folio_nuevo}`  |  📄 **Folio Error:** `{folio_error}`  |  "
            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
        )
    else:
        st.markdown(
            f"📄 **Folio:** `{res.get('Folio','') or 'N/A'}`  |  "
            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
        )

    st.markdown(
        f"**👤 Cliente:** {res.get('Cliente','N/A')}  |  **RFC:** {res.get('Numero_Cliente_RFC','') or 'N/A'}"
    )
    st.markdown(
        f"**Estado:** {res.get('Estado','') or 'N/A'}  |  **Estado del Caso:** {res.get('Estado_Caso','') or 'N/A'}  |  **Turno:** {res.get('Turno','') or 'N/A'}"
    )
    if is_garantia:
        st.markdown(
            f"**🔢 Número de Serie:** {res.get('Numero_Serie','') or 'N/A'}  |  **📅 Fecha de Compra:** {res.get('Fecha_Compra','') or 'N/A'}"
        )

    comentario_txt = str(res.get("Comentario", "") or res.get("Comentarios", "")).strip()
    if comentario_txt:
        st.markdown("#### 📝 Comentarios del pedido")
        st.info(comentario_txt)

    direccion_retorno = str(res.get("Direccion_Guia_Retorno", "")).strip()
    if direccion_retorno:
        st.markdown("#### 📍 Dirección para guía de retorno")
        st.info(direccion_retorno)

    nota_venta_valor = str(res.get("Nota_Venta", "")).strip()
    tiene_nota_venta = str(res.get("Tiene_Nota_Venta", "")).strip()
    motivo_nota_venta = str(res.get("Motivo_NotaVenta", "")).strip()
    if nota_venta_valor or tiene_nota_venta or motivo_nota_venta:
        st.markdown("#### 🧾 Nota de venta")
        estado_texto = tiene_nota_venta or ("Sí" if nota_venta_valor else "No")
        st.markdown(f"- **¿Tiene nota de venta?:** {estado_texto}")
        if nota_venta_valor:
            st.markdown(f"- **Detalle:** {nota_venta_valor}")
        if motivo_nota_venta:
            st.markdown(f"- **Motivo:** {motivo_nota_venta}")

    ref_t = res.get("Refacturacion_Tipo","")
    ref_st = res.get("Refacturacion_Subtipo","")
    ref_f = res.get("Folio_Factura_Refacturada","")
    if any([ref_t, ref_st, ref_f]):
        st.markdown("**♻️ Refacturación:**")
        st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
        st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
        st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

    if str(res.get("Resultado_Esperado","")).strip():
        st.markdown(f"**🎯 Resultado Esperado:** {res.get('Resultado_Esperado','')}")
    if str(res.get("Motivo_Detallado","")).strip():
        st.markdown("**📝 Motivo / Descripción:**")
        st.info(str(res.get("Motivo_Detallado","")).strip())
    if str(res.get("Material_Devuelto","")).strip():
        st.markdown("**📦 Piezas / Material:**")
        st.info(str(res.get("Material_Devuelto","")).strip())
    if str(res.get("Monto_Devuelto","")).strip():
        st.markdown(f"**💵 Monto (dev./estimado):** {res.get('Monto_Devuelto','')}")

    st.markdown(
        f"**🏢 Área Responsable:** {res.get('Area_Responsable','') or 'N/A'}  |  **👥 Responsable del Error:** {res.get('Nombre_Responsable','') or 'N/A'}"
    )
    st.markdown(
        f"**📅 Fecha Entrega/Cierre (si aplica):** {res.get('Fecha_Entrega','') or 'N/A'}  |  "
        f"**📅 Recepción:** {res.get('Fecha_Recepcion_Devolucion','') or 'N/A'}  |  "
        f"**📦 Recepción:** {res.get('Estado_Recepcion','') or 'N/A'}"
    )
    st.markdown(
        f"**🧾 Nota de Crédito:** {res.get('Nota_Credito_URL','') or 'N/A'}  |  "
        f"**📂 Documento Adicional:** {res.get('Documento_Adicional_URL','') or 'N/A'}"
    )
    if str(res.get("Comentarios_Admin_Devolucion","")).strip():
        st.markdown("**🗒️ Comentario Administrativo:**")
        st.info(str(res.get("Comentarios_Admin_Devolucion","")).strip())

    seguimiento_txt = str(res.get("Seguimiento",""))
    if (is_devolucion or is_garantia) and seguimiento_txt.strip():
        st.markdown("**📌 Seguimiento:**")
        st.info(seguimiento_txt.strip())

    mod_txt = res.get("Modificacion_Surtido", "") or ""
    mod_fecha = res.get("Fecha_Modificacion_Surtido", "") or ""
    mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
    if mod_txt or mod_urls:
        st.markdown("#### 🛠 Modificación de surtido")
        if mod_fecha:
            st.caption(f"📅 Fecha de modificación: {mod_fecha}")
        if mod_txt:
            st.info(mod_txt)
        if mod_urls:
            st.markdown("**Archivos de modificación:**")
            for u in mod_urls:
                nombre = extract_s3_key(u).split("/")[-1]
                tmp = get_s3_file_download_url(s3_client, u)
                st.markdown(
                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                    unsafe_allow_html=True,
                )

    with st.expander("📎 Archivos (Adjuntos y Guía)", expanded=False):
        adj = res.get("Adjuntos_urls", []) or []
        guias = res.get("Guias_urls", []) or []
        if adj:
            st.markdown("**Adjuntos:**")
            for u in adj:
                nombre = extract_s3_key(u).split("/")[-1]
                tmp = get_s3_file_download_url(s3_client, u)
                st.markdown(
                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                    unsafe_allow_html=True,
                )
        if guias:
            st.markdown("**Guías:**")
            for idx, u in enumerate(guias, start=1):
                if not str(u).strip():
                    continue
                nombre = extract_s3_key(u).split("/")[-1]
                if not nombre:
                    nombre = f"Guía #{idx}"
                tmp = get_s3_file_download_url(s3_client, u)
                st.markdown(
                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                    unsafe_allow_html=True,
                )
        if not adj and not guias:
            st.info("Sin archivos registrados en la hoja.")

    st.markdown("---")

def _to_dt(series):
    return pd.to_datetime(series, errors="coerce")


def _to_date(series):
    return pd.to_datetime(series, errors="coerce").dt.date


def _safe_str(s):
    return "" if s is None else str(s).strip()


def _to_bool(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "sí", "si", "x", "ok", "✅", "completada"}


def _config_value(df_config: pd.DataFrame, key: str, default: str = "") -> str:
    if df_config is None or df_config.empty or "Key" not in df_config.columns:
        return default
    match = df_config[df_config["Key"].astype(str).str.strip().str.lower() == key.strip().lower()]
    if match.empty:
        return default
    return _safe_str(match.iloc[0].get("Value", default)) or default


def ensure_daily_checklist_items(hoy: date, df_template: pd.DataFrame, df_daily: pd.DataFrame) -> int:
    """Sincroniza CHECKLIST_DAILY del día con CHECKLIST_TEMPLATE (solo inserta faltantes)."""
    if df_template is None or df_template.empty:
        return 0

    activos = df_template.copy()
    if "Activo" in activos.columns:
        activos = activos[activos["Activo"].apply(_to_bool)]
    if "Orden" in activos.columns:
        activos["_orden"] = pd.to_numeric(activos["Orden"], errors="coerce")
        activos = activos.sort_values("_orden", ascending=True, na_position="last")

    existing_keys = set()
    if df_daily is not None and not df_daily.empty and {"Fecha", "Item_ID", "Item"}.issubset(df_daily.columns):
        daily = df_daily.copy()
        daily["_f"] = _to_date(daily["Fecha"])
        daily = daily[daily["_f"] == hoy]
        existing_keys = {
            (str(r.get("Item_ID", "")).strip(), str(r.get("Item", "")).strip().lower())
            for _, r in daily.iterrows()
        }

    inserted = 0
    for _, r in activos.iterrows():
        item_id = _safe_str(r.get("Item_ID", ""))
        item = _safe_str(r.get("Item", ""))
        if not item:
            continue
        key = (item_id, item.lower())
        if key in existing_keys:
            continue

        safe_append("CHECKLIST_DAILY", {
            "Fecha": hoy.strftime("%Y-%m-%d"),
            "Item_ID": item_id,
            "Item": item,
            "Completado": "0",
            "Completado_At": "",
            "Completado_By": "",
            "Notas": "",
        })
        inserted += 1

    return inserted


def get_checklist_daily_row_lookup(fecha_iso: str) -> dict:
    """Devuelve lookup para ubicar fila por (fecha+item) sin relecturas por cada guardado."""
    sheet = get_alejandro_worksheet("CHECKLIST_DAILY")
    ensure_headers(sheet, "CHECKLIST_DAILY")
    data = _get_all_records_with_retry(sheet)

    lookup = {}
    for idx, rec in enumerate(data, start=2):
        rec_fecha = _safe_str(rec.get("Fecha", ""))[:10]
        if rec_fecha != fecha_iso:
            continue
        rec_item_id = _safe_str(rec.get("Item_ID", ""))
        rec_item = _safe_str(rec.get("Item", "")).lower()
        if rec_item_id:
            lookup[(fecha_iso, rec_item_id, "")] = idx
        if rec_item:
            lookup[(fecha_iso, "", rec_item)] = idx
    return lookup


def update_checklist_daily_item(fecha_iso: str, item_id: str, item: str, completado: bool, notas: str = None, row_number: int = None, headers: list = None):
    """Actualiza una fila en CHECKLIST_DAILY por (Fecha + Item_ID/Item)."""
    sheet = get_alejandro_worksheet("CHECKLIST_DAILY")
    ensure_headers(sheet, "CHECKLIST_DAILY")
    if headers is None:
        headers = [h.strip() for h in sheet.row_values(1)]
    if row_number is None:
        data = _get_all_records_with_retry(sheet)
        for idx, rec in enumerate(data, start=2):
            rec_fecha = _safe_str(rec.get("Fecha", ""))[:10]
            rec_item_id = _safe_str(rec.get("Item_ID", ""))
            rec_item = _safe_str(rec.get("Item", "")).lower()
            if rec_fecha != fecha_iso:
                continue
            if item_id and rec_item_id == item_id:
                row_number = idx
                break
            if (not item_id) and rec_item == item.lower():
                row_number = idx
                break

    if row_number is None:
        raise Exception("No se encontró el ítem en CHECKLIST_DAILY")

    updates = {
        "Completado": "1" if completado else "0",
        "Completado_At": now_iso() if completado else "",
        "Completado_By": "ALEJANDRO" if completado else "",
    }
    notas_limpias = _safe_str(notas).strip()
    if notas_limpias:
        updates["Notas"] = notas_limpias

    cells = []
    for k, v in updates.items():
        if k in headers:
            cells.append(gspread.Cell(row=row_number, col=headers.index(k) + 1, value=v))

    # Si headers cacheados están desactualizados, reintenta leyendo headers actuales
    if not cells:
        fresh_headers = [h.strip() for h in sheet.row_values(1)]
        for k, v in updates.items():
            if k in fresh_headers:
                cells.append(gspread.Cell(row=row_number, col=fresh_headers.index(k) + 1, value=v))

    if cells:
        sheet.update_cells(cells, value_input_option="USER_ENTERED")
        cargar_alejandro_hoja.clear()


def build_hoy_alerts(hoy: date, df_citas: pd.DataFrame, df_tareas: pd.DataFrame, df_cot: pd.DataFrame, chk_hoy: pd.DataFrame, df_config: pd.DataFrame):
    alerts = []
    now_dt = datetime.now()
    alert_min_cita = int(_config_value(df_config, "alerta_cita_minutos", "60") or "60")
    cot_x_dias = int(_config_value(df_config, "cotizacion_sin_seguimiento_dias", "3") or "3")
    cierre_check_hora = int(_config_value(df_config, "checklist_alerta_hora", "18") or "18")

    if not df_tareas.empty and "Fecha_Limite" in df_tareas.columns:
        t = df_tareas.copy()
        t["_fl"] = _to_dt(t["Fecha_Limite"])
        vencidas = t[(t["_fl"].dt.date < hoy) & (t["Estatus"].astype(str).str.lower() != "completada")]
        if len(vencidas) > 0:
            alerts.append(("error", f"Hay {len(vencidas)} pendientes vencidos."))

    if not df_cot.empty:
        c = df_cot.copy()
        est = c.get("Estatus", "").astype(str).str.lower()
        no_cerradas = c[~est.str.contains("ganada|perdida", na=False)].copy()
        if "Fecha_Proximo_Seguimiento" in no_cerradas.columns:
            no_cerradas["_fps"] = _to_dt(no_cerradas["Fecha_Proximo_Seguimiento"])
            vencidas = no_cerradas[no_cerradas["_fps"].notna() & (no_cerradas["_fps"].dt.date < hoy)]
            if len(vencidas) > 0:
                alerts.append(("warning", f"Hay {len(vencidas)} cotización(es) con seguimiento vencido."))

        if "Ultimo_Seguimiento_Fecha" in no_cerradas.columns:
            no_cerradas["_usf"] = _to_dt(no_cerradas["Ultimo_Seguimiento_Fecha"])
            delta_days = (pd.Timestamp(now_dt) - no_cerradas["_usf"]).dt.days
            sin_seg = no_cerradas[no_cerradas["_usf"].isna() | (delta_days >= cot_x_dias)]
            if len(sin_seg) > 0:
                alerts.append(("warning", f"Hay {len(sin_seg)} cotización(es) sin seguimiento en {cot_x_dias}+ día(s)."))

    if not df_citas.empty and "Fecha_Inicio" in df_citas.columns:
        ci = df_citas.copy()
        ci["_fi"] = _to_dt(ci["Fecha_Inicio"])
        prox = ci[(ci["_fi"] >= now_dt) & (ci["_fi"] <= now_dt + timedelta(minutes=alert_min_cita))]
        prox = prox[~prox["Estatus"].astype(str).str.lower().isin(["realizada", "cancelada"])]
        if len(prox) > 0:
            alerts.append(("info", f"Hay {len(prox)} cita(s) en los próximos {alert_min_cita} minutos."))

    if not chk_hoy.empty and now_dt.hour >= cierre_check_hora:
        done = chk_hoy["Completado"].apply(_to_bool).sum() if "Completado" in chk_hoy.columns else 0
        total = len(chk_hoy)
        if total > 0 and done < total:
            alerts.append(("warning", f"Checklist incompleto: {done}/{total} completado al cierre del día."))

    return alerts



def _cobranza_clean_text(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _cobranza_norm_code(v) -> str:
    t = _cobranza_clean_text(v)
    if not t:
        return ""
    try:
        return str(int(float(t.replace(",", ""))))
    except Exception:
        return t


def _cobranza_is_valid_cliente_code(v) -> bool:
    """Valida que el código de cliente sea numérico (ej. 16982)."""
    t = _cobranza_clean_text(v)
    return bool(t) and t.isdigit()


def _cobranza_to_float(v) -> float:
    t = _cobranza_clean_text(v).replace(",", "").replace("$", "")
    if not t:
        return 0.0
    try:
        return float(t)
    except Exception:
        return 0.0


def _cobranza_to_date(v) -> str:
    dt = pd.to_datetime(v, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def _cobranza_mes_operativo(mes: str, estatus: str, fecha_proximo_pago: str) -> str:
    """Calcula mes operativo: usa mes de promesa activa; si no, conserva mes original."""
    mes_base = _cobranza_clean_text(mes)
    estatus_txt = _cobranza_clean_text(estatus).upper()
    fecha_txt = _cobranza_clean_text(fecha_proximo_pago)

    if estatus_txt == "PROMESA_PAGO" and fecha_txt:
        fecha_dt = pd.to_datetime(fecha_txt, errors="coerce")
        if not pd.isna(fecha_dt):
            return fecha_dt.strftime("%Y-%m")
    return mes_base


def get_cobranza_spreadsheet_id() -> str:
    gs = st.secrets.get("gsheets", {})
    spreadsheet_id = (
        gs.get("spreadsheet_id_cobranza")
        or gs.get("SPREADSHEET_ID_COBRANZA")
        or gs.get("spreadsheet_id")
    )
    if not spreadsheet_id:
        raise KeyError(
            "Falta definir gsheets.spreadsheet_id_cobranza o gsheets.spreadsheet_id en secrets."
        )
    return str(spreadsheet_id)


def get_cobranza_spreadsheet(force_refresh: bool = False):
    """Abre y cachea el spreadsheet de cobranza para reducir lecturas a la API."""
    global _COBRANZA_SPREADSHEET_CACHE

    if _COBRANZA_SPREADSHEET_CACHE is not None and not force_refresh:
        return _COBRANZA_SPREADSHEET_CACHE

    spreadsheet_id = get_cobranza_spreadsheet_id()
    _COBRANZA_SPREADSHEET_CACHE = _retry_gspread_api_call(
        lambda: gspread_client.open_by_key(spreadsheet_id),
        retries=4,
        base_delay=0.9,
    )
    return _COBRANZA_SPREADSHEET_CACHE


def get_cobranza_worksheet(nombre_hoja: str):
    try:
        return _retry_gspread_api_call(
            lambda: get_cobranza_spreadsheet().worksheet(nombre_hoja),
            retries=4,
            base_delay=0.9,
        )
    except gspread.exceptions.APIError as exc:
        if not _is_transient_gspread_error(exc):
            raise
        return _retry_gspread_api_call(
            lambda: get_cobranza_spreadsheet(force_refresh=True).worksheet(nombre_hoja),
            retries=4,
            base_delay=1.1,
        )


def cobranza_update_row_values(ws, row_number: int, values: list):
    """Actualiza una fila completa con compatibilidad para versiones viejas de gspread."""
    if hasattr(ws, "update"):
        start = gspread.utils.rowcol_to_a1(row_number, 1)
        end = gspread.utils.rowcol_to_a1(row_number, len(values))
        ws.update(f"{start}:{end}", [values])
        return

    cells = [
        gspread.Cell(row=row_number, col=idx + 1, value=values[idx])
        for idx in range(len(values))
    ]
    ws.update_cells(cells, value_input_option="USER_ENTERED")

def cobranza_ensure_headers(ws, expected_headers: list[str]):
    current = [
        str(x).strip()
        for x in _retry_gspread_api_call(
            lambda: ws.row_values(1),
            retries=4,
            base_delay=0.9,
        )
    ]
    if current != expected_headers:
        if not any(current):
            _retry_gspread_api_call(
                lambda: ws.append_row(expected_headers, value_input_option="USER_ENTERED"),
                retries=4,
                base_delay=0.9,
            )
        else:
            _retry_gspread_api_call(
                lambda: cobranza_update_row_values(ws, 1, expected_headers),
                retries=4,
                base_delay=0.9,
            )




def cobranza_migrar_comentarios_con_folio(ws):
    """Migra `cobranza_comentarios` a esquema actual sin perder datos."""
    expected = [
        "Mes", "Codigo", "Folio", "Dia", "Comentario", "Actualizado_por", "Timestamp",
        "Fecha_Proximo_Pago", "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre", "Mes_Operativo"
    ]
    legacy = ["Mes", "Codigo", "Dia", "Comentario", "Actualizado_por", "Timestamp"]
    with_folio = ["Mes", "Codigo", "Folio", "Dia", "Comentario", "Actualizado_por", "Timestamp"]
    prev_expected = [
        "Mes", "Codigo", "Folio", "Dia", "Comentario", "Actualizado_por", "Timestamp",
        "Fecha_Proximo_Pago", "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre"
    ]

    values = _retry_gspread_api_call(lambda: ws.get_all_values(), retries=4, base_delay=0.9)
    if not values:
        return False

    headers = [str(x).strip() for x in values[0]]
    if headers == expected:
        return False

    if headers == legacy:
        matrix = [expected]
        for row in values[1:]:
            row = row + [""] * (len(legacy) - len(row))
            mes, codigo, dia, comentario, actualizado_por, timestamp = row[:len(legacy)]
            mes_operativo = _cobranza_mes_operativo(mes, "", "")
            matrix.append([mes, codigo, "", dia, comentario, actualizado_por, timestamp, "", "", "", "", mes_operativo])
        _retry_gspread_api_call(lambda: cobranza_replace_matrix_values(ws, matrix), retries=4, base_delay=1.0)
        return True

    if headers == with_folio:
        matrix = [expected]
        for row in values[1:]:
            row = row + [""] * (len(with_folio) - len(row))
            mes, codigo, folio, dia, comentario, actualizado_por, timestamp = row[:len(with_folio)]
            mes_operativo = _cobranza_mes_operativo(mes, "", "")
            matrix.append([mes, codigo, folio, dia, comentario, actualizado_por, timestamp, "", "", "", "", mes_operativo])
        _retry_gspread_api_call(lambda: cobranza_replace_matrix_values(ws, matrix), retries=4, base_delay=1.0)
        return True

    if headers == prev_expected:
        matrix = [expected]
        for row in values[1:]:
            row = row + [""] * (len(prev_expected) - len(row))
            mes, codigo, folio, dia, comentario, actualizado_por, timestamp, fecha, recordatorio, estatus, fecha_cierre = row[:len(prev_expected)]
            mes_operativo = _cobranza_mes_operativo(mes, estatus, fecha)
            matrix.append([mes, codigo, folio, dia, comentario, actualizado_por, timestamp, fecha, recordatorio, estatus, fecha_cierre, mes_operativo])
        _retry_gspread_api_call(lambda: cobranza_replace_matrix_values(ws, matrix), retries=4, base_delay=1.0)
        return True

    # Fallback: reordena por nombre de columna para evitar corrimiento de datos.
    if all(h in expected for h in headers):
        matrix = [expected]
        for row in values[1:]:
            row = row + [""] * (len(headers) - len(row))
            rec = {headers[i]: row[i] for i in range(len(headers))}
            rec["Mes_Operativo"] = _cobranza_mes_operativo(
                rec.get("Mes", ""),
                rec.get("Estatus_Seguimiento", ""),
                rec.get("Fecha_Proximo_Pago", ""),
            )
            matrix.append([rec.get(h, "") for h in expected])
        _retry_gspread_api_call(lambda: cobranza_replace_matrix_values(ws, matrix), retries=4, base_delay=1.0)
        return True

    return False


def cobranza_backfill_mes_operativo(ws) -> int:
    """Rellena Mes_Operativo en filas históricas que lo tengan vacío."""
    recs = cobranza_load_records_with_rows(ws)
    if not recs:
        return 0

    updates = []
    for rec in recs:
        row_number = int(rec.get("__row", 0) or 0)
        if row_number <= 1:
            continue

        mes_operativo = _cobranza_clean_text(rec.get("Mes_Operativo", ""))
        if mes_operativo:
            continue

        nuevo_mes_operativo = _cobranza_mes_operativo(
            rec.get("Mes", ""),
            rec.get("Estatus_Seguimiento", ""),
            rec.get("Fecha_Proximo_Pago", ""),
        )
        if not nuevo_mes_operativo:
            continue

        updates.append((
            row_number,
            nuevo_mes_operativo,
            now_cdmx().strftime("%Y-%m-%d %H:%M:%S"),
        ))

    if not updates:
        return 0

    headers = [str(x).strip() for x in _retry_gspread_api_call(lambda: ws.row_values(1), retries=4, base_delay=0.9)]
    idx = {h: i for i, h in enumerate(headers)}
    if "Mes_Operativo" not in idx:
        return 0

    actualizado_por_idx = idx.get("Actualizado_por")
    timestamp_idx = idx.get("Timestamp")

    for row_number, mes_op, ts in updates:
        row_values = _retry_gspread_api_call(lambda rn=row_number: ws.row_values(rn), retries=4, base_delay=1.0)
        if len(row_values) < len(headers):
            row_values.extend([""] * (len(headers) - len(row_values)))
        row_values[idx["Mes_Operativo"]] = mes_op
        if actualizado_por_idx is not None and not _cobranza_clean_text(row_values[actualizado_por_idx]):
            row_values[actualizado_por_idx] = "sistema_backfill"
        if timestamp_idx is not None:
            row_values[timestamp_idx] = ts
        cobranza_update_row_values(ws, row_number, row_values)

    return len(updates)

def cobranza_replace_matrix_values(ws, matrix: list[list]):
    """Escribe una matriz completa con fallback para versiones viejas de gspread."""
    if not matrix or not matrix[0]:
        return

    rows = len(matrix)
    cols = len(matrix[0])

    if hasattr(ws, "update"):
        end_a1 = gspread.utils.rowcol_to_a1(rows, cols)
        ws.update(f"A1:{end_a1}", matrix, value_input_option="USER_ENTERED")
        _COBRANZA_VALUES_CACHE.pop(_cobranza_cache_key(ws), None)
        return

    cells = ws.range(1, 1, rows, cols)
    i = 0
    for r in range(rows):
        for c in range(cols):
            cells[i].value = matrix[r][c]
            i += 1
    ws.update_cells(cells, value_input_option="USER_ENTERED")
    _COBRANZA_VALUES_CACHE.pop(_cobranza_cache_key(ws), None)


def _cobranza_get_all_values_cached(ws, max_age_seconds: float = 20.0, use_cache: bool = True):
    """Lee valores de una worksheet con cache corto para bajar lecturas por minuto."""
    cache_key = _cobranza_cache_key(ws)
    now_ts = time.time()
    if use_cache and cache_key is not None:
        cache_entry = _COBRANZA_VALUES_CACHE.get(cache_key)
        if cache_entry:
            age = now_ts - cache_entry["ts"]
            if age <= max_age_seconds:
                return cache_entry["values"]

    values = _retry_gspread_api_call(lambda: ws.get_all_values(), retries=4, base_delay=1.0)
    if cache_key is not None:
        _COBRANZA_VALUES_CACHE[cache_key] = {"ts": now_ts, "values": values}
    return values


def cobranza_load_records_with_rows(ws, use_cache: bool = True) -> list[dict]:
    values = _cobranza_get_all_values_cached(ws, use_cache=use_cache)
    if not values:
        return []
    headers = values[0]
    out = []
    for i, row in enumerate(values[1:], start=2):
        row = row + [""] * (len(headers) - len(row))
        rec = {headers[j]: row[j] for j in range(len(headers))}
        # Compatibilidad interna: varias vistas usan "__row" para editar en Sheets.
        # Conservamos también "__row_number__" para no romper flujos existentes.
        rec["__row"] = i
        rec["__row_number__"] = i
        out.append(rec)
    return out


def cobranza_upsert_rows_by_key(ws, df: pd.DataFrame, key_cols: list[str], update_cols: list[str]):
    if df.empty:
        return
    headers = [str(h).strip() for h in _retry_gspread_api_call(lambda: ws.row_values(1), retries=4, base_delay=0.9)]
    recs = cobranza_load_records_with_rows(ws)
    # Guardamos índice por posición para poder escribir todo en un solo update
    # y evitar exceder cuota por demasiados writes por minuto.
    idx = {
        tuple(_cobranza_clean_text(r.get(k, "")) for k in key_cols): i
        for i, r in enumerate(recs)
    }
    for _, r in df.iterrows():
        key = tuple(_cobranza_clean_text(r.get(k, "")) for k in key_cols)
        payload = {h: _cobranza_clean_text(r.get(h, "")) for h in headers}
        if key in idx:
            old = recs[idx[key]]
            for c in set(key_cols + update_cols):
                if c in headers:
                    old[c] = payload.get(c, "")
        else:
            recs.append({h: payload.get(h, "") for h in headers})
            idx[key] = len(recs) - 1

    matrix = [headers] + [[rec.get(h, "") for h in headers] for rec in recs]
    _retry_gspread_api_call(
        lambda: cobranza_replace_matrix_values(ws, matrix),
        retries=4,
        base_delay=1.0,
    )
    _COBRANZA_VALUES_CACHE.pop(_cobranza_cache_key(ws), None)


def parse_reporte_cobranza_excel(file, mes: str) -> pd.DataFrame:
    raw = pd.read_excel(file, header=None)
    header_idx = None
    for i in range(len(raw)):
        vals = [_cobranza_clean_text(x).lower() for x in raw.iloc[i].tolist()]
        if any("codigo" in v or "código" in v for v in vals):
            header_idx = i
            break
    if header_idx is None:
        raise Exception("No se encontró encabezado 'Código' en REPORTE.xlsx")

    df = pd.read_excel(file, header=header_idx)
    rename = {}
    for c in df.columns:
        n = _cobranza_clean_text(c).lower().replace("ó", "o")
        if n == "codigo" or n == "código":
            rename[c] = "Codigo"
        elif "razon" in n and "social" in n:
            rename[c] = "Razon_Social"
        elif n == "saldo":
            rename[c] = "Saldo"
        elif "no vencido" in n:
            rename[c] = "No_Vencido"
        elif "vencido" in n:
            rename[c] = "Vencido"
    df = df.rename(columns=rename)
    for req in ["Codigo", "Razon_Social", "Saldo", "No_Vencido"]:
        if req not in df.columns:
            raise Exception(f"Falta columna requerida en REPORTE: {req}")
    if "Vencido" not in df.columns:
        df["Vencido"] = 0.0
    df = df[["Codigo", "Razon_Social", "Saldo", "No_Vencido", "Vencido"]].copy()
    df["Codigo"] = df["Codigo"].apply(_cobranza_norm_code)
    df = df[df["Codigo"].apply(_cobranza_is_valid_cliente_code)]
    for c in ["Saldo", "No_Vencido", "Vencido"]:
        df[c] = df[c].apply(_cobranza_to_float)
    df["Mes"] = mes
    return df[["Mes", "Codigo", "Razon_Social", "Saldo", "No_Vencido", "Vencido"]]


def parse_antiguedad_cobranza_excel(file, mes: str = "") -> pd.DataFrame:
    raw = pd.read_excel(file, header=None)
    rows = raw.fillna("").values.tolist()
    codigo = ""
    headers_idx = None
    out = []
    for row in rows:
        c0 = _cobranza_clean_text(row[0] if len(row) > 0 else "")
        c1 = _cobranza_clean_text(row[1] if len(row) > 1 else "")
        try:
            if c0 and c1 and float(c0.replace(",", "")) > 0:
                codigo = _cobranza_norm_code(c0)
                headers_idx = None
                continue
        except Exception:
            pass

        vals = [_cobranza_clean_text(x).lower() for x in row]
        if "folio" in vals and any("fecha venc" in v for v in vals):
            headers_idx = {i: v for i, v in enumerate(vals)}
            continue
        if not codigo or headers_idx is None:
            continue
        row_text = " ".join(vals)
        if "envio" in row_text or "total:" in row_text:
            continue

        i_folio = next((k for k, v in headers_idx.items() if v == "folio"), None)
        i_fv = next((k for k, v in headers_idx.items() if "fecha venc" in v), None)
        i_ff = next((k for k, v in headers_idx.items() if v == "fecha" or "fecha factura" in v), None)
        i_sal = next((k for k, v in headers_idx.items() if v.strip() == "saldo"), None)
        if i_sal is None:
            i_sal = next(
                (k for k, v in headers_idx.items() if "saldo" in v and "acumul" not in v),
                None,
            )
        i_cond = next((k for k, v in headers_idx.items() if "condicion" in v or "condición" in v), None)
        i_mon = next((k for k, v in headers_idx.items() if "moneda" in v), None)

        folio = _cobranza_clean_text(row[i_folio]) if i_folio is not None and i_folio < len(row) else ""
        fv = _cobranza_to_date(row[i_fv]) if i_fv is not None and i_fv < len(row) else ""
        ff = _cobranza_to_date(row[i_ff]) if i_ff is not None and i_ff < len(row) else ""
        saldo = _cobranza_to_float(row[i_sal]) if i_sal is not None and i_sal < len(row) else 0.0
        cond = _cobranza_clean_text(row[i_cond]) if i_cond is not None and i_cond < len(row) else ""
        mon = _cobranza_clean_text(row[i_mon]) if i_mon is not None and i_mon < len(row) else ""

        if not folio or not fv or saldo <= 0:
            continue
        mes_row = fv[:7] if fv else mes
        out.append({
            "Mes": mes_row,
            "Codigo": codigo,
            "Folio": folio,
            "Fecha_Factura": ff,
            "Fecha_Vencimiento": fv,
            "Saldo_Vence": saldo,
            "Condicion": cond,
            "Moneda": mon,
        })

    cols = ["Mes", "Codigo", "Folio", "Fecha_Factura", "Fecha_Vencimiento", "Saldo_Vence", "Condicion", "Moneda"]
    return pd.DataFrame(out, columns=cols)




def get_cobranza_worksheets_safe():
    """Abre hojas de cobranza con manejo robusto de APIError para no romper la app."""
    global _COBRANZA_WS_CACHE

    if _COBRANZA_WS_CACHE is not None:
        return _COBRANZA_WS_CACHE

    spreadsheet_id = get_cobranza_spreadsheet_id()
    service_email = str(credentials_dict.get("client_email", "(sin client_email en secrets)"))
    try:
        ss = get_cobranza_spreadsheet()
        ws_base = _retry_gspread_api_call(lambda: ss.worksheet("cobranza_base"), retries=4, base_delay=0.9)
        ws_venc = _retry_gspread_api_call(lambda: ss.worksheet("cobranza_vencimientos"), retries=4, base_delay=0.9)
        ws_com = _retry_gspread_api_call(lambda: ss.worksheet("cobranza_comentarios"), retries=4, base_delay=0.9)
        _COBRANZA_WS_CACHE = (ws_base, ws_venc, ws_com)
        return _COBRANZA_WS_CACHE
    except gspread.exceptions.WorksheetNotFound:
        st.error("❌ Faltan pestañas requeridas en el Google Sheet de Cobranza.")
        st.caption(
            "Revisa que existan exactamente: cobranza_base, cobranza_vencimientos y cobranza_comentarios."
        )
        st.caption(f"Spreadsheet usado: {spreadsheet_id}")
        return None, None, None
    except gspread.exceptions.APIError as e:
        if _is_transient_gspread_error(e):
            st.warning("⚠️ Google Sheets en límite temporal (429). Reintenta en unos segundos.")
            st.caption(f"Detalle técnico: {e}")
            return None, None, None
        st.error("❌ No fue posible abrir las hojas de Cobranza en Google Sheets (permiso o ID).")
        st.caption(f"Spreadsheet usado: {spreadsheet_id}")
        st.caption(f"Comparte el archivo con esta cuenta de servicio: {service_email}")
        st.caption(f"Detalle técnico: {e}")
        return None, None, None
    except Exception as e:
        st.error("❌ Error inesperado al abrir las hojas de Cobranza.")
        st.caption(f"Spreadsheet usado: {spreadsheet_id}")
        st.caption(f"Detalle técnico: {e}")
        return None, None, None


def _cobranza_meses_disponibles(base_df: pd.DataFrame) -> list[str]:
    """Devuelve todos los meses YYYY-MM disponibles (incluyendo mes actual)."""
    mes_actual = now_cdmx().strftime("%Y-%m")
    meses = []
    if not base_df.empty and "Mes" in base_df.columns:
        meses = sorted({
            m for m in base_df["Mes"].astype(str)
            if re.match(r"^\d{4}-\d{2}$", m)
        })
    if mes_actual not in meses:
        meses.append(mes_actual)
    return sorted(meses)


def _cobranza_meses_con_comentarios(com_df: pd.DataFrame) -> list[str]:
    """Devuelve meses operativos válidos (YYYY-MM) presentes en comentarios."""
    if com_df.empty:
        return []

    meses = sorted({
        _cobranza_clean_text(m)
        for m in com_df.get("Mes_Operativo", "").astype(str).tolist()
        if re.match(r"^\d{4}-\d{2}$", _cobranza_clean_text(m))
    })
    return meses


def _cobranza_meses_hojas_creadas(ss) -> list[str]:
    """Lista meses con hoja mensual ya creada en Drive (Cobranza_YYYY-MM)."""
    meses = []
    worksheets = _retry_gspread_api_call(lambda: ss.worksheets(), retries=4, base_delay=0.9)
    for ws in worksheets:
        title = _cobranza_clean_text(getattr(ws, "title", ""))
        if not title.startswith("Cobranza_"):
            continue
        mes = title.replace("Cobranza_", "", 1)
        if re.match(r"^\d{4}-\d{2}$", mes):
            meses.append(mes)
    return sorted(set(meses))


def _cobranza_sheet_title_safe(title: str) -> str:
    t = re.sub(r"[\[\]\*\?/\\:]", "-", str(title or "").strip())
    return (t[:100] if t else f"Cobranza_{datetime.now().strftime('%Y%m%d_%H%M%S')}")




def _cobranza_aplicar_formato_drive(ss, ws, total_rows: int, total_cols: int):
    """Aplica formato de legibilidad en la hoja mensual de Drive."""
    if total_rows < 2 or total_cols <= 0:
        return

    # Limpia reglas condicionales heredadas (versiones anteriores pintaban celdas completas en verde).
    # Si no se eliminan explícitamente, Google Sheets conserva esas reglas al actualizar valores.
    requests = []
    try:
        metadata = _retry_gspread_api_call(lambda: ss.fetch_sheet_metadata(), retries=3, base_delay=0.7)
        sheets_meta = metadata.get("sheets", []) if isinstance(metadata, dict) else []
        ws_meta = next((sh for sh in sheets_meta if sh.get("properties", {}).get("sheetId") == ws.id), None)
        cond_rules = ws_meta.get("conditionalFormats", []) if isinstance(ws_meta, dict) else []
        for idx in range(len(cond_rules) - 1, -1, -1):
            requests.append({
                "deleteConditionalFormatRule": {
                    "sheetId": ws.id,
                    "index": idx,
                }
            })
    except Exception:
        # Si falla la lectura de metadata continuamos con el formato base.
        pass

    anchos_px = {
        0: 90,
        1: 260,
        2: 170,
        3: 110,
        4: 130,
        5: 130,
        6: 90,
        7: 120,
    }

    requests.extend([
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "gridProperties": {
                        "frozenRowCount": 2,
                        "frozenColumnCount": 2,
                    },
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.88, "blue": 0.95},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 2,
                    "endRowIndex": total_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": total_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": total_cols,
                    }
                }
            }
        },
    ])

    for col_idx in range(total_cols):
        pixel_size = anchos_px.get(col_idx, 185 if col_idx >= 8 else 100)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": col_idx,
                    "endIndex": col_idx + 1,
                },
                "properties": {"pixelSize": pixel_size},
                "fields": "pixelSize",
            }
        })

    _retry_gspread_api_call(lambda: ss.batch_update({"requests": requests}), retries=4, base_delay=1.0)
def _cobranza_guardar_en_drive_por_mes(spreadsheet_id: str, mes: str, out_df: pd.DataFrame) -> tuple[str, bool]:
    """Guarda reporte en una hoja mensual; actualiza la existente si ya fue creada."""
    configured_id = get_cobranza_spreadsheet_id()
    if not spreadsheet_id or str(spreadsheet_id).strip() == str(configured_id).strip():
        ss = get_cobranza_spreadsheet()
    else:
        ss = _retry_gspread_api_call(
            lambda: gspread_client.open_by_key(str(spreadsheet_id)),
            retries=4,
            base_delay=0.9,
        )
    title = _cobranza_sheet_title_safe(f"Cobranza_{mes}")
    creada = False

    try:
        ws_target = _retry_gspread_api_call(lambda: ss.worksheet(title), retries=4, base_delay=0.9)
    except gspread.exceptions.WorksheetNotFound:
        ws_target = _retry_gspread_api_call(
            lambda: ss.add_worksheet(
                title=title,
                rows=max(len(out_df) + 5, 50),
                cols=max(len(out_df.columns) + 2, 20),
            ),
            retries=4,
            base_delay=1.0,
        )
        creada = True

    encabezado = [f"Fecha De Generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"] + [""] * (len(out_df.columns) - 1)
    matrix = [encabezado, list(out_df.columns)] + out_df.fillna("").astype(str).values.tolist()
    _retry_gspread_api_call(lambda: cobranza_replace_matrix_values(ws_target, matrix), retries=4, base_delay=1.0)
    _cobranza_aplicar_formato_drive(ss, ws_target, total_rows=len(matrix), total_cols=len(out_df.columns))
    return title, creada


def _cobranza_es_pago_completo(texto: str) -> bool:
    txt_raw = _cobranza_clean_text(texto).lower()
    if not txt_raw:
        return False

    txt = "".join(
        c for c in unicodedata.normalize("NFD", txt_raw)
        if unicodedata.category(c) != "Mn"
    )
    variantes = [
        "pago completo",
        "cliente liquido factura",
        "liquido",
        "liquidado",
    ]
    return any(v in txt for v in variantes)


def _cobranza_etiqueta_pago_completo(texto: str) -> str:
    """Agrega una marca visual en verde al comentario cuando ya fue liquidado/pagado completo."""
    txt = _cobranza_clean_text(texto)
    if not txt:
        return ""
    if not _cobranza_es_pago_completo(txt):
        return txt
    if "🟩" in txt:
        return txt
    return f"🟩 {txt}"


def _cobranza_codigos_liquidados_mes(com_df: pd.DataFrame, mes_objetivo: str) -> set[str]:
    """Obtiene códigos cuyo último registro del mes quedó como pagado completo/liquidado."""
    if com_df.empty:
        return set()

    com_mes = com_df[com_df.get("Mes", "").astype(str) == str(mes_objetivo)].copy()
    if com_mes.empty:
        return set()

    if "__row" not in com_mes.columns:
        com_mes["__row"] = np.arange(len(com_mes))

    com_mes["Codigo"] = com_mes.get("Codigo", "").astype(str).str.strip()
    com_mes = com_mes[com_mes["Codigo"] != ""]
    if com_mes.empty:
        return set()

    com_mes["_timestamp_sort"] = pd.to_datetime(com_mes.get("Timestamp", ""), errors="coerce")
    com_mes = com_mes.sort_values(by=["Codigo", "_timestamp_sort", "__row"]).drop_duplicates(
        subset=["Codigo"],
        keep="last",
    )

    def _es_liquidado_row(row) -> bool:
        estatus = _cobranza_clean_text(row.get("Estatus_Seguimiento", "")).upper()
        comentario = _cobranza_clean_text(row.get("Comentario", ""))
        return estatus == "LIQUIDADO" or _cobranza_es_pago_completo(comentario)

    liquidados = com_mes[com_mes.apply(_es_liquidado_row, axis=1)]
    return set(liquidados["Codigo"].astype(str).tolist())


def _cobranza_texto_seguimiento_para_calendario(row) -> str:
    """Genera una nota de seguimiento para mostrarse en la columna del día comprometido."""
    estatus = _cobranza_clean_text(getattr(row, "Estatus_Seguimiento", "")).upper()
    fecha_txt = _cobranza_clean_text(getattr(row, "Fecha_Proximo_Pago", ""))
    folio = _cobranza_clean_text(getattr(row, "Folio", ""))

    if estatus not in {"PROMESA_PAGO", "PENDIENTE"} or not fecha_txt:
        return ""

    fecha_dt = pd.to_datetime(fecha_txt, errors="coerce")
    if pd.isna(fecha_dt):
        return ""

    estatus_legible = "Seg. promesa" if estatus == "PROMESA_PAGO" else "Seg. pendiente"
    prefijo_folio = f"{folio}: " if folio else ""
    return f"{prefijo_folio}{estatus_legible} {fecha_dt.strftime('%d/%m')}"

def render_cobranza_tab_gerente():
    st.subheader("📒 Cobranza")

    ws_base, ws_venc, ws_com = get_cobranza_worksheets_safe()
    if not ws_base or not ws_venc or not ws_com:
        st.info("La pestaña permanece visible, pero sin conexión activa a Google Sheets.")
        return

    base_headers = ["Mes", "Codigo", "Razon_Social", "Saldo", "No_Vencido", "Vencido", "Tipo_Pago", "Ultima_Actualizacion"]
    venc_headers = ["Mes", "Codigo", "Folio", "Fecha_Factura", "Fecha_Vencimiento", "Saldo_Vence", "Condicion", "Moneda", "Ultima_Actualizacion"]
    com_headers = [
        "Mes", "Codigo", "Folio", "Dia", "Comentario", "Actualizado_por", "Timestamp",
        "Fecha_Proximo_Pago", "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre", "Mes_Operativo"
    ]

    cache_key = "ger_cob_data_cache"

    def _load_cobranza_data(force_refresh: bool = False):
        if force_refresh or cache_key not in st.session_state:
            st.session_state[cache_key] = {
                "base_df": pd.DataFrame(cobranza_load_records_with_rows(ws_base, use_cache=not force_refresh)),
                "venc_df": pd.DataFrame(cobranza_load_records_with_rows(ws_venc, use_cache=not force_refresh)),
                "com_df": pd.DataFrame(cobranza_load_records_with_rows(ws_com, use_cache=not force_refresh)),
            }
        cache = st.session_state.get(cache_key, {})
        return (
            cache.get("base_df", pd.DataFrame()).copy(),
            cache.get("venc_df", pd.DataFrame()).copy(),
            cache.get("com_df", pd.DataFrame()).copy(),
        )

    try:
        migracion_comentarios = cobranza_migrar_comentarios_con_folio(ws_com)
        cobranza_ensure_headers(ws_base, base_headers)
        cobranza_ensure_headers(ws_venc, venc_headers)
        cobranza_ensure_headers(ws_com, com_headers)
        backfill_count = cobranza_backfill_mes_operativo(ws_com)
        if migracion_comentarios:
            st.info("ℹ️ Se migró la hoja de comentarios para incluir la columna Folio sin perder datos existentes.")
        if backfill_count:
            st.info(f"ℹ️ Se actualizaron {backfill_count} comentario(s) históricos con Mes_Operativo.")
    except gspread.exceptions.APIError as e:
        if _is_transient_gspread_error(e):
            st.warning(
                "⚠️ Google Sheets está con límite temporal de lecturas (quota/rate limit). "
                "Se continuará con los encabezados actuales y puedes reintentar en unos segundos."
            )
            st.caption(f"Detalle técnico: {e}")
        else:
            st.error("❌ No se pudieron validar encabezados de hojas de Cobranza.")
            st.caption(f"Detalle técnico: {e}")
            return

    now_dt = datetime.now()
    mes_sel = now_dt.strftime("%Y-%m")

    with st.expander("Cargar archivos del mes", expanded=False):
        st.caption("El mes se asigna automáticamente con base en la Fecha_Vencimiento del archivo ANTIGÜEDAD_SALDOS.xlsx.")

        with st.form("ger_cob_carga_form", clear_on_submit=False):
            reporte = st.file_uploader("REPORTE.xlsx", type=["xlsx"], key="ger_cob_reporte")
            antig = st.file_uploader("ANTIGÜEDAD_SALDOS.xlsx", type=["xlsx"], key="ger_cob_ant")
            procesar_carga = st.form_submit_button("Procesar")

    if procesar_carga:
        with st.spinner("Procesando archivos, por favor espera..."):
            try:
                if reporte is None or antig is None:
                    raise Exception("Carga ambos archivos para procesar.")
                df_base = parse_reporte_cobranza_excel(reporte, mes_sel)
                df_venc = parse_antiguedad_cobranza_excel(antig, mes_sel)

                mes_por_codigo = {}
                if not df_venc.empty:
                    tmp = df_venc[["Codigo", "Mes", "Fecha_Vencimiento"]].copy()
                    tmp = tmp[tmp["Mes"].astype(str).str.match(r"^\d{4}-\d{2}$", na=False)]
                    tmp = tmp.sort_values("Fecha_Vencimiento")
                    mes_por_codigo = tmp.groupby("Codigo")["Mes"].agg(lambda x: x.iloc[-1]).to_dict()

                codigos_base = df_base["Codigo"].astype(str)
                df_base["Mes"] = codigos_base.map(mes_por_codigo).fillna(mes_sel)

                base_codes = set(df_base["Codigo"].astype(str))
                venc_codes = set(df_venc["Codigo"].astype(str))
                no_encontrados = base_codes - venc_codes
                codigos_con_venc_sin_mes = (base_codes & venc_codes) - set(mes_por_codigo.keys())

                ts = now_cdmx().strftime("%Y-%m-%d %H:%M:%S")
                df_base["Tipo_Pago"] = np.where(df_base["Codigo"].astype(str).isin(no_encontrados), "CONTADO", "CREDITO")
                df_base["Ultima_Actualizacion"] = ts
                df_venc["Ultima_Actualizacion"] = ts

                cobranza_upsert_rows_by_key(ws_base, df_base[base_headers], ["Mes", "Codigo"], ["Razon_Social", "Saldo", "No_Vencido", "Vencido", "Tipo_Pago", "Ultima_Actualizacion"])
                if not df_venc.empty:
                    cobranza_upsert_rows_by_key(ws_venc, df_venc[venc_headers], ["Codigo", "Folio", "Fecha_Vencimiento"], ["Mes", "Fecha_Factura", "Saldo_Vence", "Condicion", "Moneda", "Ultima_Actualizacion"])

                st.session_state["ger_cob_stats"] = {
                    "clientes": int(len(df_base)),
                    "folios": int(len(df_venc)),
                    "contado": int(len(no_encontrados)),
                }
                st.session_state["ger_cob_missing"] = df_base[df_base["Codigo"].astype(str).isin(no_encontrados)][["Codigo", "Razon_Social"]].copy()
                st.session_state["ger_cob_force_refresh"] = True
                if codigos_con_venc_sin_mes:
                    st.warning(
                        f"{len(codigos_con_venc_sin_mes)} cliente(s) con vencimientos pero sin mes derivado de Fecha_Vencimiento; "
                        f"se asignó mes de carga {mes_sel}."
                    )
                st.success("✅ Proceso de cobranza completado.")
            except Exception as e:
                st.error(f"❌ Error al procesar: {e}")

    stats = st.session_state.get("ger_cob_stats")
    if stats:
        a, b, c = st.columns(3)
        a.metric("Clientes base", stats["clientes"])
        b.metric("Folios vencimientos", stats["folios"])
        c.metric("Clientes CONTADO", stats["contado"])

    missing = st.session_state.get("ger_cob_missing", pd.DataFrame(columns=["Codigo", "Razon_Social"]))
    if isinstance(missing, pd.DataFrame) and not missing.empty:
        st.warning("Clientes en REPORTE no encontrados en ANTIGÜEDAD (marcados como CONTADO).")
        st.dataframe(missing, use_container_width=True, hide_index=True)

    force_refresh = bool(st.session_state.pop("ger_cob_force_refresh", False))
    base_df, venc_df, com_df = _load_cobranza_data(force_refresh=force_refresh)
    st.markdown("### Comentarios")
    meses_disponibles = _cobranza_meses_disponibles(base_df)
    mes_actual = now_cdmx().strftime("%Y-%m")

    filtro_mes_activo = st.checkbox(
        "Filtrar por año y mes",
        value=st.session_state.get("ger_cob_filtro_mes_activo", False),
        key="ger_cob_filtro_mes_activo",
    )

    anios_disponibles = sorted({m.split("-")[0] for m in meses_disponibles if re.match(r"^\d{4}-\d{2}$", m)})
    mes_com = ""
    if filtro_mes_activo:
        if not anios_disponibles:
            st.info("No hay años disponibles para filtrar.")
        else:
            anio_default = st.session_state.get("ger_cob_filtro_anio", mes_actual.split("-")[0])
            if anio_default not in anios_disponibles:
                anio_default = anios_disponibles[-1]

            with st.container():
                st.caption("Selecciona un año y un mes, luego presiona **Aplicar filtro**.")
                col_anio, col_mes, col_btn = st.columns([1.2, 1.2, 0.8])
                with col_anio:
                    anio_sel = st.selectbox(
                        "Año",
                        options=anios_disponibles,
                        index=anios_disponibles.index(anio_default),
                        key="ger_cob_filtro_anio",
                    )
                meses_anio = sorted([m for m in meses_disponibles if m.startswith(f"{anio_sel}-")])
                meses_anio_num = [m.split("-")[1] for m in meses_anio]
                if not meses_anio_num:
                    meses_anio_num = [mes_actual.split("-")[1]]
                mes_default_num = st.session_state.get("ger_cob_filtro_mes_num", mes_actual.split("-")[1])
                if mes_default_num not in meses_anio_num:
                    mes_default_num = meses_anio_num[-1]
                with col_mes:
                    mes_num_sel = st.selectbox(
                        "Mes",
                        options=meses_anio_num,
                        format_func=lambda m: f"{m} - {MESES_ES[int(m)] if m.isdigit() and 1 <= int(m) <= 12 else m}",
                        index=meses_anio_num.index(mes_default_num),
                        key="ger_cob_filtro_mes_num",
                    )
                with col_btn:
                    st.write("")
                    st.write("")
                    aplicar_filtro = st.button("Aplicar filtro", key="ger_cob_aplicar_filtro_mes")

            if aplicar_filtro or ("ger_cob_mes_com_aplicado" not in st.session_state):
                st.session_state["ger_cob_mes_com_aplicado"] = f"{anio_sel}-{mes_num_sel}"

            mes_com = st.session_state.get("ger_cob_mes_com_aplicado", f"{anio_sel}-{mes_num_sel}")
            st.caption(f"Filtro activo: **{mes_com}**")

    if not mes_com:
        mes_com = "TODOS"

    if base_df.empty:
        clientes_mes = pd.DataFrame(columns=["Codigo", "Razon_Social"])
    else:
        clientes_mes = base_df.copy()
        if mes_com != "TODOS":
            clientes_mes = clientes_mes[clientes_mes.get("Mes", "").astype(str) == mes_com]
        if "Tipo_Pago" in clientes_mes.columns:
            tipo_pago = clientes_mes["Tipo_Pago"].astype(str).str.strip().str.upper()
            clientes_mes = clientes_mes[tipo_pago != "CONTADO"]

    if clientes_mes.empty:
        st.info("No hay clientes cargados para el filtro seleccionado (excluyendo CONTADO).")
    else:
        clientes_mes = clientes_mes[["Codigo", "Razon_Social"]].drop_duplicates().sort_values(["Razon_Social", "Codigo"])
        opciones = [f"{r.Codigo} - {r.Razon_Social}" for r in clientes_mes.itertuples(index=False)]
        cliente_sel = st.selectbox("Cliente", opciones, key="ger_cob_cliente")

        codigo = cliente_sel.split(" - ")[0].strip()
        venc_cliente = pd.DataFrame()
        if not venc_df.empty:
            venc_cliente = venc_df[
                (venc_df.get("Codigo", "").astype(str) == codigo)
            ].copy()
            if mes_com != "TODOS":
                venc_cliente = venc_cliente[venc_cliente.get("Mes", "").astype(str) == mes_com].copy()

        dias_venc = []
        if not venc_cliente.empty and "Fecha_Vencimiento" in venc_cliente.columns:
            fechas_venc = pd.to_datetime(venc_cliente["Fecha_Vencimiento"], errors="coerce")
            dias_venc = sorted({int(f.day) for f in fechas_venc.dropna()})

        if dias_venc:
            dias_txt = ", ".join(str(d) for d in dias_venc)
            total_folios = int(venc_cliente[["Folio", "Fecha_Vencimiento"]].drop_duplicates().shape[0])
            st.info(
                f"🗓️ **Vencimientos del cliente ({mes_com if mes_com != 'TODOS' else 'todos los meses'}):** el día **{dias_txt}** · "
                f"folios activos: **{total_folios}**."
            )
            with st.expander("Ver detalle de folios y vencimientos", expanded=False):
                detalle_cols = [c for c in ["Folio", "Fecha_Factura", "Fecha_Vencimiento", "Saldo_Vence", "Moneda"] if c in venc_cliente.columns]
                if detalle_cols:
                    detalle = venc_cliente[detalle_cols].drop_duplicates().sort_values(
                        by=[c for c in ["Fecha_Vencimiento", "Folio"] if c in detalle_cols]
                    )
                    st.dataframe(detalle, use_container_width=True, hide_index=True)
        else:
            st.caption("ℹ️ Este cliente no tiene vencimientos detectados para el filtro actual. Puedes capturar comentario manualmente.")

        acciones_cobranza = {
            "": "",
            "COBRO": "Se le cobró",
            "PAGO_PARCIAL": "Cliente pagó parcialmente",
            "LIQUIDADO": "Cliente liquidó factura",
        }
        respuestas_cliente = {
            "": "",
            "REVISA_CONTAB": "Revisará con contabilidad",
            "REVISA_FACTURA": "Revisará factura",
            "ENVIA_COMPROB": "Enviará comprobante",
            "SIN_RESPUESTA": "No respondió",
            "PAGO_PARCIAL": "Pagó parcialmente",
            "PAGO_COMPLETO": "Pagó completo",
        }

        acciones_por_texto = {v: k for k, v in acciones_cobranza.items()}
        respuestas_por_texto = {v: k for k, v in respuestas_cliente.items() if v}

        def _parse_cobranza_comentario_guardado(comentario_txt: str):
            txt = str(comentario_txt or "").strip()
            if not txt:
                return "", "", ""

            bloque_principal, comentario_extra = txt, ""
            if "|" in txt:
                bloque_principal, comentario_extra = txt.split("|", 1)
                comentario_extra = comentario_extra.strip()

            partes = [p.strip() for p in re.split(r"\s+[–-]\s+", bloque_principal) if p.strip()]
            if partes and re.match(r"^\d{2}/\d{2}/\d{4}$", partes[0]):
                partes = partes[1:]

            accion = ""
            respuesta = ""
            restantes = []
            for parte in partes:
                if not parte:
                    continue
                if not accion and parte in acciones_por_texto:
                    accion = acciones_por_texto[parte]
                    continue
                if not respuesta and parte in respuestas_por_texto:
                    respuesta = respuestas_por_texto[parte]
                    continue
                restantes.append(parte)

            if restantes:
                comentario_extra = " – ".join([t for t in [" – ".join(restantes), comentario_extra] if t]).strip()

            return accion, respuesta, comentario_extra

        folios_cliente = []
        if not venc_cliente.empty and "Folio" in venc_cliente.columns:
            folios_cliente = sorted({
                _cobranza_clean_text(f)
                for f in venc_cliente["Folio"].tolist()
                if _cobranza_clean_text(f)
            })

        folios_disponibles = folios_cliente if folios_cliente else ["SIN_FOLIO"]

        lote_activo = st.checkbox(
            "Comentar / dar seguimiento a varios folios",
            key="ger_cob_lote_activo",
            help="Activa esta opción para seleccionar varios folios y aplicar el mismo comentario en lote.",
        )

        if lote_activo:
            folios_sel_pre = st.session_state.get("ger_cob_folios", folios_disponibles[:1])
            if not isinstance(folios_sel_pre, list):
                folios_sel_pre = [folios_sel_pre]
            folios_sel_pre = [f for f in folios_sel_pre if f in folios_disponibles]
            if not folios_sel_pre and folios_disponibles:
                folios_sel_pre = folios_disponibles[:1]

            with st.expander("Selección de folios para comentario en lote", expanded=True):
                st.caption("Selecciona folios sin buscador para evitar que aparezca el recuadro de resultados.")
                with st.form("ger_cob_filtros_form", clear_on_submit=False):
                    col_todos, col_limpiar = st.columns(2)
                    with col_todos:
                        marcar_todos = st.form_submit_button("Seleccionar todos")
                    with col_limpiar:
                        limpiar_todos = st.form_submit_button("Quitar todos")

                    if marcar_todos:
                        for folio_opt in folios_disponibles:
                            st.session_state[f"ger_cob_folio_chk_{folio_opt}"] = True
                    if limpiar_todos:
                        for folio_opt in folios_disponibles:
                            st.session_state[f"ger_cob_folio_chk_{folio_opt}"] = False

                    for folio_opt in folios_disponibles:
                        key_chk = f"ger_cob_folio_chk_{folio_opt}"
                        if key_chk not in st.session_state:
                            st.session_state[key_chk] = folio_opt in folios_sel_pre
                        st.checkbox(folio_opt, key=key_chk)

                    aplicar_filtros = st.form_submit_button("Aplicar selección de folios")

            if aplicar_filtros:
                folios_guardados = [
                    folio_opt for folio_opt in folios_disponibles
                    if st.session_state.get(f"ger_cob_folio_chk_{folio_opt}", False)
                ]
                st.session_state["ger_cob_folios"] = folios_guardados
                st.success("✅ Selección de folios aplicada.")

            folios_sel = st.session_state.get("ger_cob_folios", folios_sel_pre)
            if not isinstance(folios_sel, list):
                folios_sel = [folios_sel]
            folios_sel = [f for f in folios_sel if f in folios_disponibles]
            folio_prefill = folios_sel[0] if len(folios_sel) == 1 else ""
        else:
            folio_sel = st.selectbox(
                "Folio",
                options=folios_disponibles,
                key="ger_cob_folio_single",
                help="Folios activos del cliente según el filtro actual.",
            )
            folios_sel = [folio_sel] if folio_sel else []
            folio_prefill = folio_sel if folio_sel else ""

        folios_sel_set = set(folios_sel)

        dia_actual = datetime.now().day
        dias_opciones = [dia_actual]
        if not com_df.empty:
            com_mes_cliente = com_df[
                (com_df.get("Codigo", "").astype(str) == str(codigo))
                & (com_df.get("Folio", "").astype(str).isin(folios_sel) if folios_sel else False)
                & ((com_df.get("Mes", "").astype(str) == str(mes_com)) if mes_com != "TODOS" else True)
            ].copy()
            if not com_mes_cliente.empty:
                dias_historicos = pd.to_numeric(com_mes_cliente.get("Dia", ""), errors="coerce")
                dias_historicos = [
                    int(d)
                    for d in dias_historicos.dropna().tolist()
                    if 1 <= int(d) <= 31 and int(d) <= dia_actual
                ]
                dias_opciones.extend(dias_historicos)

        dias_opciones = sorted(set(dias_opciones))
        dia_default = st.session_state.get("ger_cob_dia", dia_actual)
        if dia_default not in dias_opciones:
            dia_default = dia_actual

        dia_sel = st.selectbox(
            "Día",
            options=dias_opciones,
            index=dias_opciones.index(dia_default),
            key="ger_cob_dia",
        )

        dia_sel_int = int(dia_sel)
        comentario_existente = ""
        fecha_pago_existente_txt = ""
        recordatorio_existente = ""
        estatus_existente = ""
        if not com_df.empty:
            com_mes = com_df.get("Mes", "").astype(str)
            com_codigo = com_df.get("Codigo", "").astype(str)
            com_folio = com_df.get("Folio", "").astype(str)
            com_dia = pd.to_numeric(com_df.get("Dia", ""), errors="coerce")
            existentes = com_df[
                (com_codigo == str(codigo))
                & (com_folio.isin(folios_sel) if folios_sel else False)
                & ((com_mes == str(mes_com)) if mes_com != "TODOS" else True)
                & (com_dia == dia_sel_int)
            ].copy()
            if not existentes.empty:
                if len(folios_sel) == 1 and folio_prefill:
                    existentes = existentes[existentes["Folio"].astype(str) == folio_prefill]
                if not existentes.empty:
                    existentes = existentes.sort_values(by=[c for c in ["Timestamp", "__row"] if c in existentes.columns])
                    ultimo = existentes.iloc[-1]
                    comentario_existente = str(ultimo.get("Comentario", "") or "").strip()
                    recordatorio_existente = str(ultimo.get("Recordatorio_Activo", "") or "").strip().upper()
                    estatus_existente = str(ultimo.get("Estatus_Seguimiento", "") or "").strip().upper()
                    fecha_raw = str(ultimo.get("Fecha_Proximo_Pago", "") or "").strip()
                    try:
                        fecha_tmp = pd.to_datetime(fecha_raw, errors="coerce")
                        fecha_pago_existente_txt = "" if pd.isna(fecha_tmp) else fecha_tmp.strftime("%Y-%m-%d")
                    except Exception:
                        fecha_pago_existente_txt = ""

        prefill_ctx = (str(mes_com), str(codigo), tuple(sorted(folios_sel_set)), dia_sel_int)
        if st.session_state.get("ger_cob_prefill_ctx") != prefill_ctx:
            accion_pref, respuesta_pref, comentario_pref = _parse_cobranza_comentario_guardado(comentario_existente)
            seguimiento_activo_pref = bool(fecha_pago_existente_txt or recordatorio_existente or estatus_existente)
            for k in [
                "ger_cob_accion", "ger_cob_respuesta", "ger_cob_comentario",
                "ger_cob_fecha_picker", "ger_cob_seguimiento_activo",
                "ger_cob_recordatorio", "ger_cob_estatus"
            ]:
                st.session_state.pop(k, None)
            st.session_state["ger_cob_accion"] = accion_pref if accion_pref in acciones_cobranza else ""
            st.session_state["ger_cob_respuesta"] = respuesta_pref if respuesta_pref in respuestas_cliente else ""
            st.session_state["ger_cob_comentario"] = comentario_pref
            st.session_state["ger_cob_fecha_picker"] = pd.to_datetime(fecha_pago_existente_txt).date() if fecha_pago_existente_txt else date.today()
            st.session_state["ger_cob_seguimiento_activo"] = seguimiento_activo_pref
            st.session_state["ger_cob_recordatorio"] = recordatorio_existente if recordatorio_existente in {"SI", "NO"} else ""
            st.session_state["ger_cob_estatus"] = estatus_existente if estatus_existente in {"PENDIENTE", "PROMESA_PAGO", "LIQUIDADO"} else ""
            st.session_state["ger_cob_prefill_ctx"] = prefill_ctx

        st.session_state["ger_cob_seguimiento_activo"] = True
        aplicar_seg = False
        with st.expander("🔔 Seguimiento de próximo pago", expanded=False):
            with st.form("ger_cob_seguimiento_form", clear_on_submit=False):
                st.date_input(
                    "Fecha de próximo pago",
                    key="ger_cob_fecha_picker",
                    format="DD/MM/YYYY",
                )
                st.selectbox(
                    "Recordatorio activo",
                    options=["", "SI", "NO"],
                    key="ger_cob_recordatorio",
                )
                st.selectbox(
                    "Estatus de seguimiento",
                    options=["", "PENDIENTE", "PROMESA_PAGO", "LIQUIDADO"],
                    key="ger_cob_estatus",
                    help="PROMESA_PAGO agrupa promesas de pago; LIQUIDADO equivale a pagado completo y deja de mostrarse en seguimiento.",
                )
                aplicar_seg = st.form_submit_button("Aplicar seguimiento")

        fecha_pago_dt = st.session_state.get("ger_cob_fecha_picker")
        recordatorio_activo = st.session_state.get("ger_cob_recordatorio", "")
        estatus_seguimiento = st.session_state.get("ger_cob_estatus", "")

        if aplicar_seg:
            if not folios_sel:
                st.warning("⚠️ Selecciona al menos un folio para aplicar seguimiento.")
            elif not any([fecha_pago_dt, str(recordatorio_activo).strip(), str(estatus_seguimiento).strip()]):
                st.warning("⚠️ Captura al menos fecha, recordatorio o estatus para aplicar seguimiento.")
            else:
                dia_guardado = int(dia_sel)
                estatus_form = str(estatus_seguimiento or "").strip().upper()
                fecha_proximo_pago = ""
                if fecha_pago_dt and estatus_form in {"PENDIENTE", "PROMESA_PAGO"}:
                    fecha_proximo_pago = pd.to_datetime(fecha_pago_dt).strftime("%Y-%m-%d")

                fecha_cierre = now_cdmx().strftime("%Y-%m-%d") if estatus_form == "LIQUIDADO" else ""
                mes_operativo = _cobranza_mes_operativo(
                    mes_com if mes_com != "TODOS" else mes_actual,
                    estatus_form,
                    fecha_proximo_pago,
                )
                timestamp_actual = now_cdmx().strftime("%Y-%m-%d %H:%M:%S")
                usuario_actualizado = _safe_str(usuario_actual)

                seg_df = pd.DataFrame([
                    {
                        "Mes": mes_com if mes_com != "TODOS" else mes_actual,
                        "Codigo": codigo,
                        "Folio": folio,
                        "Dia": str(dia_guardado),
                        "Comentario": "",
                        "Actualizado_por": usuario_actualizado,
                        "Timestamp": timestamp_actual,
                        "Fecha_Proximo_Pago": fecha_proximo_pago,
                        "Recordatorio_Activo": str(recordatorio_activo or "").strip().upper(),
                        "Estatus_Seguimiento": estatus_form,
                        "Fecha_Cierre": fecha_cierre,
                        "Mes_Operativo": mes_operativo,
                    }
                    for folio in folios_sel
                ])
                cobranza_upsert_rows_by_key(
                    ws_com,
                    seg_df[com_headers],
                    ["Mes", "Codigo", "Folio", "Dia"],
                    [
                        "Actualizado_por", "Timestamp", "Fecha_Proximo_Pago",
                        "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre", "Mes_Operativo"
                    ],
                )
                st.session_state["ger_cob_force_refresh"] = True
                st.success("✅ Seguimiento aplicado correctamente.")
                st.rerun()

        with st.form("ger_cob_form", clear_on_submit=False):
            accion_code = st.selectbox(
                "Acción de cobranza",
                options=list(acciones_cobranza.keys()),
                format_func=lambda c: acciones_cobranza[c],
                key="ger_cob_accion",
            )
            respuesta_code = st.selectbox(
                "Respuesta / estado del cliente",
                options=list(respuestas_cliente.keys()),
                format_func=lambda c: respuestas_cliente[c],
                key="ger_cob_respuesta",
            )
            comentario = st.text_area("Comentario adicional (opcional)", key="ger_cob_comentario")
            guardar_comentario = st.form_submit_button("Guardar comentario")

        if guardar_comentario:
            fecha_txt = now_cdmx().strftime("%d/%m")
            if not folios_sel:
                st.warning("⚠️ Selecciona al menos un folio para guardar comentario.")
            elif not accion_code and not respuesta_code and not comentario.strip():
                st.warning("⚠️ Captura al menos una acción, una respuesta o un comentario antes de guardar.")
            else:
                comentario_partes = [fecha_txt]
                accion_label = acciones_cobranza.get(accion_code, accion_code)
                if accion_label:
                    comentario_partes.append(accion_label)
                if respuesta_code:
                    comentario_partes.append(respuestas_cliente.get(respuesta_code, respuesta_code))
                comentario_compuesto = " – ".join(comentario_partes)
                if comentario.strip():
                    comentario_compuesto = f"{comentario_compuesto} | {comentario.strip()}"

                dia_guardado = int(dia_sel)
                fecha_proximo_pago = ""
                if estatus_seguimiento in {"PENDIENTE", "PROMESA_PAGO"} and fecha_pago_dt:
                    fecha_proximo_pago = pd.to_datetime(fecha_pago_dt).strftime("%Y-%m-%d")

                recordatorio_guardado = str(recordatorio_activo or "").strip().upper()
                estatus_form = str(estatus_seguimiento or "").strip().upper()

                texto_pago = f"{comentario_compuesto} {respuestas_cliente.get(respuesta_code, '')}".strip()
                es_pagado = estatus_form == "LIQUIDADO" or _cobranza_es_pago_completo(texto_pago)
                estatus_guardado = "LIQUIDADO" if es_pagado else estatus_form
                fecha_cierre = now_cdmx().strftime("%Y-%m-%d") if es_pagado else ""

                com_df = pd.DataFrame([
                    {
                        "Mes": mes_com if mes_com != "TODOS" else mes_actual,
                        "Codigo": codigo,
                        "Folio": folio,
                        "Dia": str(dia_guardado),
                        "Comentario": comentario_compuesto,
                        "Actualizado_por": _safe_str(usuario_actual),
                        "Timestamp": now_cdmx().strftime("%Y-%m-%d %H:%M:%S"),
                        "Fecha_Proximo_Pago": fecha_proximo_pago,
                        "Recordatorio_Activo": recordatorio_guardado,
                        "Estatus_Seguimiento": estatus_guardado,
                        "Fecha_Cierre": fecha_cierre,
                        "Mes_Operativo": _cobranza_mes_operativo(
                            mes_com if mes_com != "TODOS" else mes_actual,
                            estatus_guardado,
                            fecha_proximo_pago,
                        ),
                    }
                    for folio in folios_sel
                ])
                cobranza_upsert_rows_by_key(
                    ws_com,
                    com_df[com_headers],
                    ["Mes", "Codigo", "Folio", "Dia"],
                    [
                        "Comentario", "Actualizado_por", "Timestamp", "Fecha_Proximo_Pago",
                        "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre", "Mes_Operativo"
                    ],
                )
                st.session_state["ger_cob_force_refresh"] = True
                st.success(f"✅ Comentario guardado en {len(folios_sel)} folio(s).")
                st.rerun()

    st.info("📌 El seguimiento de pagos ahora se gestiona en la pestaña **📊 Seguimiento Cobranza**.")

    def _generar_excel_cobranza_mes(mes_objetivo: str, actualizar_drive: bool = False, mostrar_toast_drive: bool = True):
        base_df, venc_df, com_df = _load_cobranza_data(force_refresh=actualizar_drive)
        base_all = base_df.copy() if not base_df.empty else pd.DataFrame()
        base_df = base_df[base_df.get("Mes", "").astype(str) == mes_objetivo] if not base_df.empty else pd.DataFrame()
        if not base_df.empty and "Tipo_Pago" in base_df.columns:
            tipo_pago = base_df["Tipo_Pago"].astype(str).str.strip().str.upper()
            base_df = base_df[tipo_pago != "CONTADO"]

        com_mes = pd.DataFrame()
        if not com_df.empty:
            com_mes = com_df.copy()
            com_mes["Mes_Operativo"] = com_mes.get("Mes_Operativo", "").astype(str)
            mask_mes_op_vacio = com_mes["Mes_Operativo"].str.strip() == ""
            if mask_mes_op_vacio.any():
                com_mes.loc[mask_mes_op_vacio, "Mes_Operativo"] = com_mes.loc[mask_mes_op_vacio].apply(
                    lambda row: _cobranza_mes_operativo(
                        row.get("Mes", ""),
                        row.get("Estatus_Seguimiento", ""),
                        row.get("Fecha_Proximo_Pago", ""),
                    ),
                    axis=1,
                )
            com_mes = com_mes[com_mes.get("Mes_Operativo", "").astype(str) == mes_objetivo].copy()

        if base_df.empty and com_mes.empty:
            st.error("No hay registros en cobranza_base para ese mes ni seguimientos del mes operativo seleccionado.")
            return None

        out = base_df[["Codigo", "Razon_Social"]].drop_duplicates().copy() if not base_df.empty else pd.DataFrame(columns=["Codigo", "Razon_Social"])

        if not com_mes.empty:
            codigos_seguimiento = {
                _cobranza_clean_text(c)
                for c in com_mes.get("Codigo", "").astype(str).tolist()
                if _cobranza_clean_text(c)
            }
            codigos_out = set(out.get("Codigo", pd.Series(dtype="string")).astype(str).tolist())
            faltantes = sorted(codigos_seguimiento - codigos_out)
            if faltantes:
                base_lookup = base_all.copy() if not base_all.empty else pd.DataFrame(columns=["Codigo", "Razon_Social"])
                if not base_lookup.empty:
                    base_lookup["Codigo"] = base_lookup.get("Codigo", "").astype(str)
                    base_lookup = base_lookup.sort_values(by=[c for c in ["Ultima_Actualizacion", "Mes"] if c in base_lookup.columns])
                    base_lookup = base_lookup.drop_duplicates(subset=["Codigo"], keep="last")
                map_razon = dict(zip(base_lookup.get("Codigo", []), base_lookup.get("Razon_Social", []))) if not base_lookup.empty else {}
                extras = pd.DataFrame([
                    {"Codigo": cod, "Razon_Social": _cobranza_clean_text(map_razon.get(cod, "")) or "SIN RAZON SOCIAL"}
                    for cod in faltantes
                ])
                out = pd.concat([out, extras], ignore_index=True)

        base_saldos = base_df[["Codigo", "Saldo"]].copy() if "Saldo" in base_df.columns else pd.DataFrame(columns=["Codigo", "Saldo"])
        if not base_saldos.empty:
            base_saldos["Saldo"] = pd.to_numeric(base_saldos["Saldo"], errors="coerce").fillna(0.0)
            base_saldos = base_saldos.groupby("Codigo", as_index=False)["Saldo"].sum()
            out = out.merge(base_saldos, on="Codigo", how="left")
        else:
            out["Saldo"] = np.nan

        # Si el cliente sólo aparece por seguimiento del mes operativo (sin base del mes),
        # heredamos el saldo más reciente disponible para no marcarlo como PAGADO por default.
        base_saldos_hist = pd.DataFrame(columns=["Codigo", "Saldo"])
        if not base_all.empty and "Saldo" in base_all.columns:
            base_saldos_hist = base_all[["Codigo", "Saldo"]].copy()
            base_saldos_hist["Codigo"] = base_saldos_hist.get("Codigo", "").astype(str)
            base_saldos_hist["Saldo"] = pd.to_numeric(base_saldos_hist["Saldo"], errors="coerce")
            base_saldos_hist = base_saldos_hist.sort_values(by=[c for c in ["Ultima_Actualizacion", "Mes"] if c in base_saldos_hist.columns])
            base_saldos_hist = base_saldos_hist.drop_duplicates(subset=["Codigo"], keep="last")
            base_saldos_hist = base_saldos_hist.rename(columns={"Saldo": "Saldo_hist"})
            out = out.merge(base_saldos_hist[["Codigo", "Saldo_hist"]], on="Codigo", how="left")
            out["Saldo"] = out["Saldo"].fillna(out["Saldo_hist"])
            out = out.drop(columns=["Saldo_hist"], errors="ignore")
        out["Saldo"] = pd.to_numeric(out.get("Saldo", 0.0), errors="coerce").fillna(0.0)
        if not venc_df.empty:
            venc_mes = venc_df[venc_df.get("Mes", "").astype(str) == mes_objetivo].copy()
            venc_mes = venc_mes[venc_mes.get("Codigo", "").astype(str).isin(out["Codigo"].astype(str))]
        else:
            venc_mes = pd.DataFrame()

        extra_cols = ["Folio", "Saldo_Vence", "Fecha_Vencimiento", "Condicion", "Moneda", "Estatus_Cobranza"]
        for c in extra_cols:
            out[c] = ""

        if not venc_mes.empty:
            venc_mes["Saldo_Vence"] = pd.to_numeric(venc_mes.get("Saldo_Vence", ""), errors="coerce")
            venc_ag = venc_mes.groupby("Codigo", as_index=False).agg({
                "Folio": lambda s: " | ".join(sorted({str(x).strip() for x in s if str(x).strip()})),
                "Saldo_Vence": "sum",
                "Fecha_Vencimiento": lambda s: " | ".join(sorted({str(x).strip() for x in s if str(x).strip()})),
                "Condicion": lambda s: " | ".join(sorted({str(x).strip() for x in s if str(x).strip()})),
                "Moneda": lambda s: " | ".join(sorted({str(x).strip() for x in s if str(x).strip()})),
            })
            out = out.merge(venc_ag, on="Codigo", how="left", suffixes=("", "_agg"))
            out["Folio"] = out["Folio_agg"].fillna("")
            out["Saldo_Vence"] = out["Saldo_Vence_agg"].fillna(0.0)
            out["Fecha_Vencimiento"] = out["Fecha_Vencimiento_agg"].fillna("")
            out["Condicion"] = out["Condicion_agg"].fillna("")
            out["Moneda"] = out["Moneda_agg"].fillna("")
            out = out.drop(columns=[c for c in ["Folio_agg", "Saldo_Vence_agg", "Fecha_Vencimiento_agg", "Condicion_agg", "Moneda_agg"] if c in out.columns])

        # Fallback histórico para seguimientos que pasan a un nuevo mes operativo y no traen
        # vencimientos en ese mes: mantiene folio/saldo/fecha desde el último registro conocido.
        if not venc_df.empty:
            venc_hist = venc_df.copy()
            venc_hist["Codigo"] = venc_hist.get("Codigo", "").astype(str)
            venc_hist["Fecha_Vencimiento"] = pd.to_datetime(venc_hist.get("Fecha_Vencimiento", ""), errors="coerce")
            venc_hist["Saldo_Vence"] = pd.to_numeric(venc_hist.get("Saldo_Vence", ""), errors="coerce").fillna(0.0)
            venc_hist = venc_hist[venc_hist["Codigo"].isin(out["Codigo"].astype(str))]
            venc_hist = venc_hist.sort_values(["Codigo", "Fecha_Vencimiento"])
            venc_hist = venc_hist.drop_duplicates(subset=["Codigo"], keep="last")
            venc_hist["Fecha_Vencimiento"] = venc_hist["Fecha_Vencimiento"].dt.strftime("%Y-%m-%d")
            venc_hist = venc_hist.rename(columns={
                "Folio": "Folio_hist",
                "Saldo_Vence": "Saldo_Vence_hist",
                "Fecha_Vencimiento": "Fecha_Vencimiento_hist",
                "Condicion": "Condicion_hist",
                "Moneda": "Moneda_hist",
            })
            cols_hist = ["Codigo", "Folio_hist", "Saldo_Vence_hist", "Fecha_Vencimiento_hist", "Condicion_hist", "Moneda_hist"]
            out = out.merge(venc_hist[cols_hist], on="Codigo", how="left")

            mask_folio_vacio = out["Folio"].astype(str).str.strip() == ""
            out.loc[mask_folio_vacio, "Folio"] = out.loc[mask_folio_vacio, "Folio_hist"].fillna("")
            out.loc[mask_folio_vacio, "Fecha_Vencimiento"] = out.loc[mask_folio_vacio, "Fecha_Vencimiento_hist"].fillna("")
            out.loc[mask_folio_vacio, "Condicion"] = out.loc[mask_folio_vacio, "Condicion_hist"].fillna("")
            out.loc[mask_folio_vacio, "Moneda"] = out.loc[mask_folio_vacio, "Moneda_hist"].fillna("")

            saldo_actual = pd.to_numeric(out["Saldo_Vence"], errors="coerce").fillna(0.0)
            saldo_hist = pd.to_numeric(out.get("Saldo_Vence_hist", 0.0), errors="coerce").fillna(0.0)
            out["Saldo_Vence"] = np.where(mask_folio_vacio, saldo_hist, saldo_actual)
            out = out.drop(columns=[c for c in ["Folio_hist", "Saldo_Vence_hist", "Fecha_Vencimiento_hist", "Condicion_hist", "Moneda_hist"] if c in out.columns], errors="ignore")

        out["Saldo_Vence"] = pd.to_numeric(out.get("Saldo_Vence", 0.0), errors="coerce").fillna(0.0)
        out["Estatus_Cobranza"] = np.where(
            (out["Saldo"] <= 0.0) & (out["Saldo_Vence"] <= 0.0),
            "PAGADO",
            "CON SALDO",
        )

        if not com_mes.empty:
            codigos_con_seguimiento_activo = {
                _cobranza_clean_text(cod)
                for cod, est in zip(
                    com_mes.get("Codigo", "").astype(str),
                    com_mes.get("Estatus_Seguimiento", "").astype(str),
                )
                if _cobranza_clean_text(cod) and _cobranza_clean_text(est).upper() in {"PROMESA_PAGO", "PENDIENTE"}
            }
            if codigos_con_seguimiento_activo:
                mask_seg_activo = out["Codigo"].astype(str).isin(codigos_con_seguimiento_activo)
                out.loc[mask_seg_activo, "Estatus_Cobranza"] = "CON SALDO"

        codigos_liquidados = _cobranza_codigos_liquidados_mes(com_df, mes_objetivo)
        if codigos_liquidados:
            mask_liquidados = out["Codigo"].astype(str).isin(codigos_liquidados)
            out.loc[mask_liquidados, "Estatus_Cobranza"] = "PAGADO"

        for d in range(1, 32):
            out[str(d)] = ""

        if not venc_mes.empty and "Fecha_Vencimiento" in venc_mes.columns:
            fechas = pd.to_datetime(venc_mes["Fecha_Vencimiento"], errors="coerce", dayfirst=False)
            saldos_vence = pd.to_numeric(venc_mes.get("Saldo_Vence", 0), errors="coerce").fillna(0.0)
            for codigo, fecha, saldo_vence in zip(venc_mes.get("Codigo", "").astype(str), fechas, saldos_vence):
                if pd.isna(fecha):
                    continue
                dia_col = str(int(fecha.day))
                nota = (
                    f"{fecha.strftime('%d/%m')}: Pagada"
                    if saldo_vence <= 0
                    else f"{fecha.strftime('%d/%m')}: Debe ${saldo_vence:,.2f}"
                )
                if dia_col in out.columns:
                    mask = out["Codigo"].astype(str) == str(codigo)
                    previo = out.loc[mask, dia_col].astype(str).fillna("")
                    out.loc[mask, dia_col] = np.where(
                        previo.str.strip() == "",
                        nota,
                        np.where(nota.strip() == "", previo, previo + "\n" + nota),
                    )

        if not com_mes.empty:
            for r in com_mes.itertuples(index=False):
                dia = _cobranza_clean_text(getattr(r, "Dia", "1")) or "1"
                cod = _cobranza_clean_text(getattr(r, "Codigo", ""))
                folio = _cobranza_clean_text(getattr(r, "Folio", ""))
                txt = _cobranza_clean_text(getattr(r, "Comentario", ""))
                txt = _cobranza_etiqueta_pago_completo(txt)
                if not cod or dia not in out.columns:
                    continue
                if folio:
                    txt = f"[{folio}] {txt}" if txt else f"[{folio}]"
                mask = out["Codigo"].astype(str) == cod
                previo = out.loc[mask, dia].astype(str).fillna("")
                out.loc[mask, dia] = np.where(
                    previo.str.strip() == "",
                    txt,
                    np.where(txt.strip() == "", previo, previo + "\n" + txt),
                )

                txt_seguimiento = _cobranza_texto_seguimiento_para_calendario(r)
                if txt_seguimiento:
                    fecha_seg = pd.to_datetime(getattr(r, "Fecha_Proximo_Pago", ""), errors="coerce")
                    if not pd.isna(fecha_seg):
                        dia_seg = str(int(fecha_seg.day))
                        if dia_seg in out.columns:
                            mask = out["Codigo"].astype(str) == cod
                            previo = out.loc[mask, dia_seg].astype(str).fillna("")
                            out.loc[mask, dia_seg] = np.where(
                                previo.str.strip() == "",
                                txt_seguimiento,
                                np.where(txt_seguimiento.strip() == "", previo, previo + "\n" + txt_seguimiento),
                            )

        cols_orden = ["Codigo", "Razon_Social", "Folio", "Saldo_Vence", "Fecha_Vencimiento", "Condicion", "Moneda", "Estatus_Cobranza"] + [str(d) for d in range(1, 32)]
        out = out[cols_orden]

        bio = BytesIO()
        with pd.ExcelWriter(
            bio,
            engine="xlsxwriter",
            engine_kwargs={"options": {"strings_to_urls": False}},
        ) as writer:
            out.to_excel(writer, sheet_name="Cobranza", index=False, startrow=1)
            ws = writer.sheets["Cobranza"]
            ws.write(0, 0, f"Fecha De Generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   Periodo: {mes_objetivo}")
            ws.freeze_panes(2, 2)

            wb = writer.book
            fmt_header = wb.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#D9E1F2", "border": 1})
            fmt_texto = wb.add_format({"text_wrap": True, "valign": "top", "border": 1})

            ws.set_row(1, 22)
            for c_idx, col in enumerate(out.columns):
                ws.write(1, c_idx, col, fmt_header)

            anchos = {
                "Codigo": 12,
                "Razon_Social": 34,
                "Folio": 22,
                "Saldo_Vence": 14,
                "Fecha_Vencimiento": 18,
                "Condicion": 18,
                "Moneda": 10,
                "Estatus_Cobranza": 15,
            }
            for c_idx, col in enumerate(out.columns):
                if col in anchos:
                    ws.set_column(c_idx, c_idx, anchos[col])
                elif str(col).isdigit():
                    ws.set_column(c_idx, c_idx, 26)
                else:
                    ws.set_column(c_idx, c_idx, 14)

            ws.autofilter(1, 0, len(out) + 1, len(out.columns) - 1)

            for row_idx, row in out.iterrows():
                max_lineas = 1
                for dia in range(1, 32):
                    col = str(dia)
                    if col not in out.columns:
                        continue
                    val = str(row.get(col, "") or "")
                    if val:
                        lineas = val.count("\n") + 1
                        estimado = max(1, int(len(val) / 34) + 1)
                        max_lineas = max(max_lineas, max(lineas, estimado))
                ws.set_row(row_idx + 2, min(110, max(20, max_lineas * 14)))

                for c_idx, col in enumerate(out.columns):
                    if str(col).isdigit():
                        continue
                    valor_col = row.get(col, "")
                    if col in {"Saldo_Vence"}:
                        try:
                            valor_col = float(valor_col)
                        except Exception:
                            pass
                    ws.write(row_idx + 2, c_idx, valor_col, fmt_texto)

                for dia in range(1, 32):
                    col = str(dia)
                    if col not in out.columns:
                        continue
                    c_idx = out.columns.get_loc(col)
                    valor = str(row.get(col, "") or "")
                    ws.write(row_idx + 2, c_idx, valor, fmt_texto)
        bio.seek(0)
        excel_bytes = bio.getvalue()

        drive_result = None
        if actualizar_drive:
            try:
                spreadsheet_id = get_cobranza_spreadsheet_id()
                nombre_hoja, creada = _cobranza_guardar_en_drive_por_mes(
                    spreadsheet_id,
                    mes_objetivo,
                    out,
                )
                drive_result = {"ok": True, "hoja": nombre_hoja, "creada": bool(creada)}
                if mostrar_toast_drive:
                    if creada:
                        st.success(f"✅ Excel actualizado. Se generó una hoja nueva en Drive: {nombre_hoja}")
                    else:
                        st.success(f"✅ Excel actualizado. Solo se actualizó la hoja existente en Drive: {nombre_hoja}")
            except Exception as e:
                drive_result = {"ok": False, "error": str(e)}
                if mostrar_toast_drive:
                    st.warning(f"⚠️ Se generó el Excel local, pero no se pudo guardar en Drive: {e}")

        return excel_bytes, drive_result

    st.markdown("### Actualizar Excel")
    st.caption("Actualiza en Drive el Excel de todos los meses que ya tengan comentarios. Si la hoja mensual no existe, se crea automáticamente.")
    if st.button("Actualizar Excel de todos los comentarios", key="ger_cob_excel_actualizar"):
        meses_comentarios = _cobranza_meses_con_comentarios(com_df)
        if not meses_comentarios:
            st.info("No hay comentarios con Mes_Operativo válido para actualizar en Drive.")
        else:
            st.session_state["ger_cob_excel_descargas"] = st.session_state.get("ger_cob_excel_descargas", {})
            hojas_creadas = []
            hojas_actualizadas = []
            hojas_error = []

            for mes_actualizar in meses_comentarios:
                excel_bytes, drive_result = _generar_excel_cobranza_mes(
                    mes_actualizar,
                    actualizar_drive=True,
                    mostrar_toast_drive=False,
                )
                if excel_bytes:
                    st.session_state["ger_cob_excel_descargas"][mes_actualizar] = excel_bytes
                if not drive_result:
                    hojas_error.append(f"{mes_actualizar} (sin respuesta de Drive)")
                    continue
                if drive_result.get("ok"):
                    if drive_result.get("creada"):
                        hojas_creadas.append(drive_result.get("hoja", f"Cobranza_{mes_actualizar}"))
                    else:
                        hojas_actualizadas.append(drive_result.get("hoja", f"Cobranza_{mes_actualizar}"))
                else:
                    hojas_error.append(f"{mes_actualizar}: {drive_result.get('error', 'Error desconocido')}")

            if hojas_creadas:
                st.success(f"✅ Se crearon hojas nuevas: {', '.join(hojas_creadas)}")
            if hojas_actualizadas:
                st.success(f"✅ Se actualizaron hojas existentes: {', '.join(hojas_actualizadas)}")
            if hojas_error:
                st.warning("⚠️ Hubo meses que no se pudieron actualizar: " + " | ".join(hojas_error))

    st.markdown("### Descargar")
    with st.expander("📥 Descargar hojas de meses disponibles", expanded=False):
        st.caption("Solo se muestran meses que ya tienen hoja creada en Drive.")
        meses_creados_drive = _cobranza_meses_hojas_creadas(get_cobranza_spreadsheet())
        if not meses_creados_drive:
            st.info("Aún no hay hojas mensuales creadas en Drive para descargar.")
            return
        mes_descarga = st.selectbox(
            "Mes disponible para descargar (YYYY-MM)",
            options=meses_creados_drive,
            index=meses_creados_drive.index(mes_actual) if mes_actual in meses_creados_drive else len(meses_creados_drive) - 1,
            key="ger_cob_mes_descarga",
        )
        if st.button("Preparar Excel para descarga", key="ger_cob_preparar_descarga"):
            excel_bytes, _ = _generar_excel_cobranza_mes(mes_descarga, actualizar_drive=False)
            if excel_bytes:
                st.session_state["ger_cob_excel_descargas"] = st.session_state.get("ger_cob_excel_descargas", {})
                st.session_state["ger_cob_excel_descargas"][mes_descarga] = excel_bytes

        excel_descargas = st.session_state.get("ger_cob_excel_descargas", {})
        excel_mes = excel_descargas.get(mes_descarga)
        if excel_mes:
            st.download_button(
                "Descargar Excel de cobranza",
                data=excel_mes,
                file_name=f"cobranza_{mes_descarga}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"ger_cob_download_{mes_descarga}",
            )
        else:
            st.info("Prepara primero el archivo del mes seleccionado para habilitar su descarga.")



def render_seguimiento_cobranza_tab_gerente(usuario_actual: str | None):
    st.subheader("📊 Seguimiento Cobranza")

    ws_base, _, ws_com = get_cobranza_worksheets_safe()
    if not ws_base or not ws_com:
        st.info("La pestaña permanece visible, pero sin conexión activa a Google Sheets.")
        return

    base_headers = ["Mes", "Codigo", "Razon_Social", "Saldo", "No_Vencido", "Vencido", "Tipo_Pago", "Ultima_Actualizacion"]
    com_headers = [
        "Mes", "Codigo", "Folio", "Dia", "Comentario", "Actualizado_por", "Timestamp",
        "Fecha_Proximo_Pago", "Recordatorio_Activo", "Estatus_Seguimiento", "Fecha_Cierre", "Mes_Operativo"
    ]

    try:
        cobranza_ensure_headers(ws_base, base_headers)
        cobranza_ensure_headers(ws_com, com_headers)
        cobranza_backfill_mes_operativo(ws_com)
    except gspread.exceptions.APIError as e:
        if _is_transient_gspread_error(e):
            st.warning("⚠️ Google Sheets está con límite temporal de lecturas. Reintenta en unos segundos.")
            st.caption(f"Detalle técnico: {e}")
        else:
            st.error("❌ No se pudieron validar encabezados de seguimiento de cobranza.")
            st.caption(f"Detalle técnico: {e}")
        return

    base_df = pd.DataFrame(cobranza_load_records_with_rows(ws_base))
    com_df = pd.DataFrame(cobranza_load_records_with_rows(ws_com))
    if com_df.empty:
        st.info("Aún no hay seguimientos capturados.")
        return

    cliente_nom = base_df[["Codigo", "Razon_Social"]].drop_duplicates() if not base_df.empty else pd.DataFrame(columns=["Codigo", "Razon_Social"])

    seg_tmp = com_df.copy()
    seg_tmp["Mes_Operativo"] = seg_tmp.get("Mes_Operativo", "").astype(str)
    mask_mes_op_vacio = seg_tmp["Mes_Operativo"].str.strip() == ""
    if mask_mes_op_vacio.any():
        seg_tmp.loc[mask_mes_op_vacio, "Mes_Operativo"] = seg_tmp.loc[mask_mes_op_vacio].apply(
            lambda row: _cobranza_mes_operativo(
                row.get("Mes", ""),
                row.get("Estatus_Seguimiento", ""),
                row.get("Fecha_Proximo_Pago", ""),
            ),
            axis=1,
        )

    meses_com = sorted({str(m).strip() for m in seg_tmp.get("Mes_Operativo", pd.Series(dtype="string")).tolist() if str(m).strip()}, reverse=True)
    opciones_mes = ["Todos"] + meses_com
    mes_sel = st.selectbox("Mes de seguimiento", options=opciones_mes, key="ger_seg_mes_sel")

    seg = seg_tmp.copy()
    if mes_sel != "Todos":
        seg = seg[seg.get("Mes_Operativo", "").astype(str) == mes_sel].copy()

    fecha_series_raw = seg["Fecha_Proximo_Pago"] if "Fecha_Proximo_Pago" in seg.columns else pd.Series("", index=seg.index, dtype="string")
    fecha_series = pd.to_datetime(fecha_series_raw, errors="coerce")
    seg["Fecha_Proximo_Pago"] = fecha_series

    rec_series_raw = seg["Recordatorio_Activo"] if "Recordatorio_Activo" in seg.columns else pd.Series("", index=seg.index, dtype="string")
    est_series_raw = seg["Estatus_Seguimiento"] if "Estatus_Seguimiento" in seg.columns else pd.Series("", index=seg.index, dtype="string")
    com_series_raw = seg["Comentario"] if "Comentario" in seg.columns else pd.Series("", index=seg.index, dtype="string")

    rec_series = rec_series_raw.astype(str).str.upper().str.strip()
    est_series = est_series_raw.astype(str).str.upper().str.strip()
    com_series = com_series_raw.astype(str)

    mask_seg = (
        (rec_series == "SI")
        & (est_series == "PROMESA_PAGO")
        & (~com_series.apply(_cobranza_es_pago_completo))
        & (fecha_series.notna())
    )
    seg = seg[mask_seg].copy()

    if not seg.empty:
        seg["_ts"] = pd.to_datetime(seg.get("Timestamp", ""), errors="coerce")
        seg["_dia_num"] = pd.to_numeric(seg.get("Dia", ""), errors="coerce")
        seg = seg.sort_values(["Codigo", "Folio", "_ts", "_dia_num"], ascending=[True, True, True, True])
        seg = seg.drop_duplicates(subset=["Codigo", "Folio"], keep="last").copy()
        seg = seg.drop(columns=[c for c in ["_ts", "_dia_num"] if c in seg.columns], errors="ignore")

    if seg.empty:
        st.caption("Sin promesas de pago con fecha pendiente.")
        return

    hoy = pd.Timestamp(date.today())
    seg["Dias_Restantes"] = (seg["Fecha_Proximo_Pago"].dt.normalize() - hoy).dt.days
    seg["Estado_Fecha"] = np.where(
        seg["Dias_Restantes"] < 0,
        "VENCIDO",
        np.where(seg["Dias_Restantes"] == 0, "HOY", "PROXIMO"),
    )

    vencidos = int((seg["Dias_Restantes"] < 0).sum())
    hoy_count = int((seg["Dias_Restantes"] == 0).sum())
    manana_count = int((seg["Dias_Restantes"] == 1).sum())

    m1, m2, m3 = st.columns(3)
    m1.metric("🔴 Vencidos", vencidos)
    m2.metric("🟠 Vencen hoy", hoy_count)
    m3.metric("🟡 Vencen mañana", manana_count)

    filtro_seg = st.selectbox(
        "Filtro de seguimiento",
        options=["Todos", "Vencidos", "Vence hoy", "Próximos 7 días"],
        key="ger_seg_filtro_seg",
    )
    seg_filtrado = seg.copy()
    if filtro_seg == "Vencidos":
        seg_filtrado = seg_filtrado[seg_filtrado["Dias_Restantes"] < 0]
    elif filtro_seg == "Vence hoy":
        seg_filtrado = seg_filtrado[seg_filtrado["Dias_Restantes"] == 0]
    elif filtro_seg == "Próximos 7 días":
        seg_filtrado = seg_filtrado[(seg_filtrado["Dias_Restantes"] >= 0) & (seg_filtrado["Dias_Restantes"] <= 7)]

    seg_view = seg_filtrado.merge(cliente_nom, on="Codigo", how="left")
    seg_view = seg_view.sort_values(["Fecha_Proximo_Pago", "Codigo", "Folio"]).copy()
    seg_view["Fecha_Proximo_Pago"] = seg_view["Fecha_Proximo_Pago"].dt.strftime("%Y-%m-%d")
    cols_seg = [
        "Codigo", "Razon_Social", "Folio", "Fecha_Proximo_Pago", "Dias_Restantes",
        "Estado_Fecha", "Comentario", "Actualizado_por", "Timestamp"
    ]
    seg_view = seg_view[[c for c in cols_seg if c in seg_view.columns]]

    def _seg_estado_color(v):
        txt = str(v).upper().strip()
        if txt == "VENCIDO":
            return "color: #ff4d6d; font-weight: 700"
        if txt == "HOY":
            return "color: #ffb86b; font-weight: 700"
        return "color: #f4d35e; font-weight: 600"

    def _seg_dias_color(v):
        n = pd.to_numeric(v, errors="coerce")
        if pd.isna(n):
            return ""
        if n < 0:
            return "color: #ff4d6d; font-weight: 700"
        if n == 0:
            return "color: #ffb86b; font-weight: 700"
        if n == 1:
            return "color: #f4d35e; font-weight: 700"
        return "color: #86efac; font-weight: 600"

    seg_styled = seg_view.style.applymap(_seg_estado_color, subset=["Estado_Fecha"])
    seg_styled = seg_styled.applymap(_seg_dias_color, subset=["Dias_Restantes"])
    st.dataframe(seg_styled, use_container_width=True, hide_index=True)

    st.markdown("#### 🧭 Gestión de seguimiento")
    if seg_filtrado.empty:
        st.info("No hay promesas activas para gestionar con el filtro seleccionado.")
        return

    seg_gestion = seg_filtrado.merge(cliente_nom, on="Codigo", how="left")
    seg_gestion = seg_gestion.sort_values(["Fecha_Proximo_Pago", "Codigo", "Folio"]).copy()

    # Nota: columnas con prefijo "__" pueden no exponerse como atributo en itertuples.
    # Por eso normalizamos a una columna interna de selección segura.
    row_src = seg_gestion.get("__row", seg_gestion.get("__row_number__", ""))
    seg_gestion["_row_id"] = pd.to_numeric(row_src, errors="coerce").fillna(0).astype(int)
    seg_gestion = seg_gestion[seg_gestion["_row_id"] > 0].copy()

    if seg_gestion.empty:
        st.info("No se encontraron filas editables en Google Sheets para esta vista.")
        return

    seg_gestion["Codigo"] = seg_gestion.get("Codigo", "").astype(str)
    seg_gestion["Razon_Social"] = seg_gestion.get("Razon_Social", "").astype(str)

    st.caption("Selecciona uno o varios folios por cliente para aplicar cambios masivos.")
    row_sel_multi: list[int] = []
    for (codigo_cli, razon_cli), grp in seg_gestion.groupby(["Codigo", "Razon_Social"], sort=True):
        grp_sorted = grp.sort_values(["Fecha_Proximo_Pago", "Folio"]).copy()
        opciones_cli = []
        etiquetas_cli = {}
        fechas_vencimiento_cli = []
        for _, row in grp_sorted.iterrows():
            row_id = int(row.get("_row_id", 0) or 0)
            if row_id <= 0:
                continue
            fecha_dt = pd.to_datetime(row.get("Fecha_Proximo_Pago", ""), errors="coerce")
            fecha_txt = "" if pd.isna(fecha_dt) else fecha_dt.strftime("%Y-%m-%d")
            if fecha_txt:
                fechas_vencimiento_cli.append(fecha_txt)
            folio_txt = _cobranza_clean_text(row.get("Folio", ""))
            estatus_txt = _cobranza_clean_text(row.get("Estatus_Seguimiento", "")).upper() or "PROMESA_PAGO"
            opciones_cli.append(row_id)
            etiquetas_cli[row_id] = f"Folio {folio_txt} · Estatus {estatus_txt} · Próximo pago {fecha_txt}"

        if not opciones_cli:
            continue

        fechas_unicas = sorted(set(fechas_vencimiento_cli))
        fechas_label = ", ".join(fechas_unicas) if fechas_unicas else "Sin fecha"
        exp_title = (
            f"{_cobranza_clean_text(codigo_cli)} · {_cobranza_clean_text(razon_cli)} "
            f"({len(opciones_cli)} folios) · Vence: {fechas_label}"
        )
        with st.expander(exp_title, expanded=False):
            sel_cli = st.multiselect(
                "Folios en seguimiento",
                options=opciones_cli,
                format_func=lambda rid, map_et=etiquetas_cli: map_et.get(rid, str(rid)),
                key=f"ger_seg_rows_cli_{_cobranza_clean_text(codigo_cli)}",
            )
            row_sel_multi.extend(int(rid) for rid in sel_cli)

    row_sel_multi = sorted(set(row_sel_multi))
    if not row_sel_multi:
        st.info("Selecciona al menos un folio para habilitar la edición de estatus, fecha y comentarios.")
        return

    seleccion_df = seg_gestion[seg_gestion["_row_id"].isin(row_sel_multi)].copy()
    estatus_default = "PROMESA_PAGO"
    if len(row_sel_multi) == 1:
        estatus_default = _cobranza_clean_text(seleccion_df.iloc[0].get("Estatus_Seguimiento", "")).upper() or "PROMESA_PAGO"
    if estatus_default not in {"PENDIENTE", "PROMESA_PAGO"}:
        estatus_default = "PROMESA_PAGO"

    fecha_actual_dt = pd.to_datetime(seleccion_df.get("Fecha_Proximo_Pago", pd.Series(dtype="string")), errors="coerce").dropna()
    fecha_default = fecha_actual_dt.min().date() if not fecha_actual_dt.empty else date.today()

    with st.form("ger_seg_gestion_form", clear_on_submit=False):
        nuevo_estatus = st.selectbox(
            "Nuevo estatus",
            options=["PENDIENTE", "PROMESA_PAGO"],
            index=["PENDIENTE", "PROMESA_PAGO"].index(estatus_default) if estatus_default in {"PENDIENTE", "PROMESA_PAGO"} else 1,
        )
        nueva_fecha_pago = st.date_input(
            "Nueva fecha de pago",
            value=fecha_default,
            format="DD/MM/YYYY",
            disabled=nuevo_estatus != "PROMESA_PAGO",
        )
        comentario_gestion = st.text_area("Nota de seguimiento (opcional)")
        col_a, col_b = st.columns(2)
        with col_a:
            aplicar_gestion = st.form_submit_button("Guardar cambios")
        with col_b:
            liquidar_directo = st.form_submit_button("✅ Liquidar folio(s) seleccionados")

    if not (aplicar_gestion or liquidar_directo):
        return


    estatus_final = "LIQUIDADO" if liquidar_directo else nuevo_estatus
    fecha_final = ""
    if estatus_final == "PROMESA_PAGO":
        fecha_final = pd.to_datetime(nueva_fecha_pago).strftime("%Y-%m-%d")

    idx = {h: i for i, h in enumerate(com_headers)}

    for row_number in row_sel_multi:
        row_values = _retry_gspread_api_call(lambda rn=row_number: ws_com.row_values(rn), retries=4, base_delay=1.0)
        if len(row_values) < len(com_headers):
            row_values.extend([""] * (len(com_headers) - len(row_values)))

        comentario_actual = _cobranza_clean_text(row_values[idx["Comentario"]])
        nota = comentario_gestion.strip()
        if estatus_final == "LIQUIDADO":
            nota = "Cliente liquidado." if not nota else f"{nota} | Cliente liquidado."
        if nota:
            row_values[idx["Comentario"]] = f"{comentario_actual} | {nota}".strip(" |")

        row_values[idx["Estatus_Seguimiento"]] = estatus_final
        row_values[idx["Recordatorio_Activo"]] = "SI" if estatus_final == "PROMESA_PAGO" else "NO"
        row_values[idx["Fecha_Proximo_Pago"]] = fecha_final
        row_values[idx["Fecha_Cierre"]] = now_cdmx().strftime("%Y-%m-%d") if estatus_final == "LIQUIDADO" else ""
        row_values[idx["Mes_Operativo"]] = _cobranza_mes_operativo(
            row_values[idx["Mes"]],
            row_values[idx["Estatus_Seguimiento"]],
            row_values[idx["Fecha_Proximo_Pago"]],
        )
        row_values[idx["Actualizado_por"]] = _safe_str(usuario_actual)
        row_values[idx["Timestamp"]] = now_cdmx().strftime("%Y-%m-%d %H:%M:%S")

        cobranza_update_row_values(ws_com, row_number, row_values)

    st.success(f"✅ Seguimiento actualizado en {len(row_sel_multi)} folio(s).")
    st.rerun()


def render_macheo_tool_tab_gerente():
    st.subheader("🧩 Macheo Tool")
    st.caption(
        "Herramienta independiente para cruzar REPORTE.xlsx y ANTIGÜEDAD_SALDOS.xlsx "
        "sin comentarios, sin días y sin usar otras hojas."
    )

    mes_ref = now_cdmx().strftime("%Y-%m")
    with st.form("ger_macheo_form", clear_on_submit=False):
        reporte = st.file_uploader("REPORTE.xlsx", type=["xlsx"], key="ger_macheo_reporte")
        antig = st.file_uploader("ANTIGÜEDAD_SALDOS.xlsx", type=["xlsx"], key="ger_macheo_ant")
        procesar = st.form_submit_button("Procesar macheo")

    if procesar:
        try:
            if reporte is None or antig is None:
                raise Exception("Carga ambos archivos para procesar el macheo.")

            df_base = parse_reporte_cobranza_excel(reporte, mes_ref)
            df_venc = parse_antiguedad_cobranza_excel(antig, mes_ref)

            if df_venc.empty:
                raise Exception("No se encontraron folios válidos en ANTIGÜEDAD_SALDOS.xlsx.")

            base_lookup = (
                df_base[["Codigo", "Razon_Social", "Saldo"]]
                .copy()
                .drop_duplicates(subset=["Codigo"], keep="last")
            )

            out = df_venc.merge(base_lookup, on="Codigo", how="left")
            out["Razon_Social"] = out["Razon_Social"].fillna("SIN RAZON SOCIAL")
            out["Saldo"] = pd.to_numeric(out["Saldo"], errors="coerce").fillna(0.0)
            out["Saldo_Vence"] = pd.to_numeric(out["Saldo_Vence"], errors="coerce").fillna(0.0)
            out["Estatus_Cobranza"] = np.where(
                (out["Saldo"] <= 0.0) & (out["Saldo_Vence"] <= 0.0),
                "PAGADO",
                "CON SALDO",
            )

            cols_salida = [
                "Codigo",
                "Razon_Social",
                "Folio",
                "Saldo_Vence",
                "Fecha_Vencimiento",
                "Condicion",
                "Moneda",
                "Estatus_Cobranza",
            ]
            out = out[cols_salida].copy()
            out = out.sort_values(["Codigo", "Fecha_Vencimiento", "Folio"]).reset_index(drop=True)
            st.session_state["ger_macheo_resultado"] = out

            meses_detectados = sorted({m for m in df_venc.get("Mes", "").astype(str) if re.match(r"^\d{4}-\d{2}$", m)})
            if meses_detectados:
                st.success(f"✅ Macheo generado. Mes(es) detectado(s) por Fecha_Vencimiento: {', '.join(meses_detectados)}")
            else:
                st.success("✅ Macheo generado correctamente.")
        except Exception as e:
            st.error(f"❌ No se pudo procesar el macheo: {e}")

    resultado = st.session_state.get("ger_macheo_resultado")
    if isinstance(resultado, pd.DataFrame) and not resultado.empty:
        st.markdown("### Previsualización")
        st.dataframe(resultado, use_container_width=True, hide_index=True)

        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            resultado.to_excel(writer, sheet_name="Macheo", index=False)
            ws = writer.sheets["Macheo"]
            for idx, col in enumerate(resultado.columns):
                width = max(len(str(col)), 14)
                ws.set_column(idx, idx, min(max(width, 14), 28))
            fmt_money = writer.book.add_format({"num_format": "$#,##0.00"})
            if "Saldo_Vence" in resultado.columns:
                col_idx = resultado.columns.get_loc("Saldo_Vence")
                ws.set_column(col_idx, col_idx, 16, fmt_money)

        st.download_button(
            "Descargar Excel de macheo",
            data=bio.getvalue(),
            file_name="macheo_tool.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="ger_macheo_download",
        )

# --- INTERFAZ ---
USUARIOS_VALIDOS = ["AlejandroVTD", "CeciliaATD", "SChava", "BreydaFTD", "SaraiFTD"]

PERMISOS_USUARIO = {
    "AlejandroVTD": {"organizador": True, "modificar": False, "cobranza": False},
    "CeciliaATD": {"organizador": False, "modificar": True, "cobranza": False},
    "SChava": {"organizador": True, "modificar": True, "cobranza": True},
    "BreydaFTD": {"organizador": False, "modificar": False, "cobranza": True},
    "SaraiFTD": {"organizador": False, "modificar": False, "cobranza": True},
}

COBRANZA_ONLY_USERS = {"BreydaFTD", "SaraiFTD"}


def _query_param_value(nombre_param: str) -> str:
    valor = st.query_params.get(nombre_param, "")
    if isinstance(valor, list):
        return str(valor[0]).strip() if valor else ""
    return str(valor).strip()


def ensure_user_logged_in():
    usuario_session = st.session_state.get("usuario", "").strip()

    if not usuario_session:
        usuario_qp = _query_param_value("usuario")
        if usuario_qp in USUARIOS_VALIDOS:
            st.session_state.usuario = usuario_qp
            usuario_session = usuario_qp

    with st.sidebar:
        st.markdown("### 👤 Acceso")
        if usuario_session:
            st.success(f"Sesión activa: **{usuario_session}**")
            if st.button("🚪 Cerrar sesión", key="cerrar_sesion_usuario"):
                st.session_state.pop("usuario", None)
                st.query_params.clear()
                st.rerun()
            return usuario_session

        st.caption("Iniciar sesión es opcional. Sin usuario solo podrás usar Buscar Pedido y Descargar Datos.")
        usuario_input = st.text_input(
            "Usuario",
            key="login_usuario_input",
            placeholder="Ingresa tu usuario",
        ).strip()
        if st.button("🔐 Iniciar sesión", key="login_usuario_btn"):
            if usuario_input in USUARIOS_VALIDOS:
                st.session_state.usuario = usuario_input
                st.query_params["usuario"] = usuario_input
                st.rerun()
            else:
                st.error("❌ Usuario no autorizado.")
    return None


def usuario_puede(usuario: str | None, permiso: str) -> bool:
    if not usuario:
        return False
    return PERMISOS_USUARIO.get(usuario, {}).get(permiso, False)


usuario_actual = ensure_user_logged_in()

if usuario_actual in COBRANZA_ONLY_USERS:
    tab_specs = [
        ("cobranza", "📒 Cobranza"),
        ("seguimiento_cobranza", "📊 Seguimiento Cobranza"),
        ("macheo_tool", "🧩 Macheo Tool"),
        ("buscar", "🔍 Buscar Pedido"),
    ]
else:
    tab_specs = [
        ("buscar", "🔍 Buscar Pedido"),
        ("descargar", "⬇️ Descargar Datos"),
    ]

    if usuario_puede(usuario_actual, "cobranza"):
        tab_specs.append(("cobranza", "📒 Cobranza"))
        tab_specs.append(("seguimiento_cobranza", "📊 Seguimiento Cobranza"))
        tab_specs.append(("macheo_tool", "🧩 Macheo Tool"))

    if usuario_puede(usuario_actual, "organizador"):
        tab_specs.insert(0, ("organizador", "🗂️ Organizador"))

    if usuario_puede(usuario_actual, "modificar"):
        tab_specs.append(("modificar", "✏️ Modificar Pedido"))

tabs = st.tabs([titulo for _, titulo in tab_specs])
tab_map = {clave: tab for (clave, _), tab in zip(tab_specs, tabs)}

with tab_map["buscar"]:
    modo_busqueda = st.radio("Selecciona el modo de búsqueda:", ["🔢 Por número de guía", "🧑 Por cliente/factura"], key="modo_busqueda_radio")

    orden_seleccionado = "Más recientes primero"
    recientes_primero = True
    filtrar_por_rango = False
    rango_fechas_input = ()
    fecha_inicio_dt = None
    fecha_fin_dt = None
    fecha_inicio_date = None
    fecha_fin_date = None

    if modo_busqueda == "🔢 Por número de guía":
        keyword = st.text_input("📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:")
        buscar_btn = st.button("🔎 Buscar")

        orden_seleccionado = st.selectbox(
            "Orden de los resultados",
            ["Más recientes primero", "Más antiguos primero"],
            index=0,
            key="orden_resultados_guia",
        )
        recientes_primero = orden_seleccionado == "Más recientes primero"

        filtrar_por_rango = st.checkbox("Filtrar por rango de fechas", value=False, key="filtrar_rango_guia")
        hoy = date.today()
        inicio_default = hoy - timedelta(days=30)
        rango_fechas_input = st.date_input(
            "Rango de fechas (opcional)",
            value=(inicio_default, hoy),
            format="YYYY-MM-DD",
            disabled=not filtrar_por_rango,
            help="Selecciona una fecha inicial y final para limitar los resultados mostrados.",
            key="rango_fechas_guia",
        )

        if filtrar_por_rango:
            if isinstance(rango_fechas_input, (list, tuple)):
                if len(rango_fechas_input) == 2:
                    fecha_inicio_date, fecha_fin_date = rango_fechas_input
                elif len(rango_fechas_input) == 1:
                    fecha_inicio_date = fecha_fin_date = rango_fechas_input[0]
            else:
                fecha_inicio_date = fecha_fin_date = rango_fechas_input

            if fecha_inicio_date and fecha_fin_date:
                if fecha_inicio_date > fecha_fin_date:
                    fecha_inicio_date, fecha_fin_date = fecha_fin_date, fecha_inicio_date
                fecha_inicio_dt = datetime.combine(fecha_inicio_date, datetime.min.time())
                fecha_fin_dt = datetime.combine(fecha_fin_date, datetime.max.time())

    elif modo_busqueda == "🧑 Por cliente/factura":
        keyword = st.text_input(
            "🧑 Ingresa el nombre del cliente o folio de factura a buscar:",
            help="Puedes escribir el nombre del cliente o el folio de factura; la búsqueda ignora mayúsculas, acentos y espacios en el folio.",
        )
        buscar_btn = st.button("🔍 Buscar Pedido del Cliente")


    filtro_fechas_activo = bool(filtrar_por_rango and fecha_inicio_dt and fecha_fin_dt)

    # --- EJECUCIÓN DE LA BÚSQUEDA ---
    if buscar_btn:
        if modo_busqueda == "🔢 Por número de guía":
            st.info("🔄 Buscando, por favor espera... puede tardar unos segundos...")

        resultados = []

        # ====== Siempre cargamos pedidos (datos_pedidos) porque la búsqueda por guía los necesita ======
        df_pedidos = cargar_pedidos()
        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos = df_pedidos.sort_values(by='Hora_Registro', ascending=not recientes_primero)
            if filtro_fechas_activo:
                mask_validas = df_pedidos['Hora_Registro'].notna()
                df_pedidos = df_pedidos[mask_validas & df_pedidos['Hora_Registro'].between(fecha_inicio_dt, fecha_fin_dt)]
            df_pedidos = df_pedidos.reset_index(drop=True)

        # ====== BÚSQUEDA POR CLIENTE: también carga y filtra casos_especiales ======
        if modo_busqueda == "🧑 Por cliente/factura":
            if not keyword.strip():
                st.warning("⚠️ Ingresa un nombre de cliente.")
                st.stop()

            keyword_cliente_normalizado = normalizar(keyword.strip())
            keyword_folio_normalizado = normalizar_folio(keyword.strip())

            # 2.1) Buscar en datos_pedidos (S3 + todos los archivos del pedido)
            for _, row in df_pedidos.iterrows():
                nombre = str(row.get("Cliente", "")).strip()
                folio = str(row.get("Folio_Factura", "")).strip()

                nombre_normalizado = normalizar(nombre) if nombre else ""
                folio_normalizado = normalizar_folio(folio)

                coincide_cliente = bool(nombre) and keyword_cliente_normalizado in nombre_normalizado
                coincide_folio = bool(folio_normalizado) and keyword_folio_normalizado == folio_normalizado

                if not coincide_cliente and not coincide_folio:
                    continue

                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                todos_los_archivos = obtener_todos_los_archivos(prefix) if prefix else []

                comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                otros = [
                    f for f in todos_los_archivos
                    if f not in comprobantes and f not in facturas
                ]

                resultados.append({
                    "__source": "pedidos",
                    "ID_Pedido": pedido_id,
                    "Cliente": row.get("Cliente", ""),
                    "Estado": row.get("Estado", ""),
                    "Vendedor": row.get("Vendedor_Registro", ""),
                    "Folio": row.get("Folio_Factura", ""),
                    "Hora_Registro": row.get("Hora_Registro", ""),
                    "Comentario": str(row.get("Comentario", "")).strip(),
                    "Comentarios": str(row.get("Comentarios", "")).strip(),
                    "Direccion_Guia_Retorno": str(row.get("Direccion_Guia_Retorno", "")).strip(),
                    "Nota_Venta": str(row.get("Nota_Venta", "")).strip(),
                    "Tiene_Nota_Venta": str(row.get("Tiene_Nota_Venta", "")).strip(),
                    "Motivo_NotaVenta": str(row.get("Motivo_NotaVenta", "")).strip(),
                    # 🛠 Modificación de surtido
                    "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                    "Fecha_Modificacion_Surtido": obtener_fecha_modificacion(row),
                    "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                    # Archivos registrados en la hoja
                    "Adjuntos_Guia_urls": partir_urls(row.get("Adjuntos_Guia", "")),
                    "Adjuntos_urls": partir_urls(row.get("Adjuntos", "")),
                    # ♻️ Refacturación
                    "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                    "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                    "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                    # Archivos S3
                    "Coincidentes": [],  # En modo cliente no destacamos PDFs guía específicos
                    "Comprobantes": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in comprobantes],
                    "Facturas": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in facturas],
                    "Otros": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in otros],
                })

            # 2.2) Buscar en casos_especiales (mostrar campos de la hoja + links de Adjuntos y Hoja_Ruta_Mensajero)
                df_casos = cargar_casos_especiales()
                # Ordenar por Hora_Registro si existe
                if "Hora_Registro" in df_casos.columns:
                    df_casos["Hora_Registro"] = pd.to_datetime(df_casos["Hora_Registro"], errors="coerce")
                    df_casos = df_casos.sort_values(by="Hora_Registro", ascending=not recientes_primero)
                    if filtro_fechas_activo:
                        mask_validas_casos = df_casos["Hora_Registro"].notna()
                        df_casos = df_casos[mask_validas_casos & df_casos["Hora_Registro"].between(fecha_inicio_dt, fecha_fin_dt)]
                    df_casos = df_casos.reset_index(drop=True)

                for _, row in df_casos.iterrows():
                    nombre = str(row.get("Cliente", "")).strip()
                    folio = str(row.get("Folio_Factura", "")).strip()

                    nombre_normalizado = normalizar(nombre) if nombre else ""
                    folio_normalizado = normalizar_folio(folio)

                    coincide_cliente = bool(nombre) and keyword_cliente_normalizado in nombre_normalizado
                    coincide_folio = bool(folio_normalizado) and keyword_folio_normalizado == folio_normalizado

                    if not coincide_cliente and not coincide_folio:
                        continue
                    resultados.append(preparar_resultado_caso(row))


        # ====== BÚSQUEDA POR NÚMERO DE GUÍA (tu flujo original sobre datos_pedidos + S3) ======
        elif modo_busqueda == "🔢 Por número de guía":
            clave = keyword.strip()
            if not clave:
                st.warning("⚠️ Ingresa una palabra clave o número de guía.")
                st.stop()

            for _, row in df_pedidos.iterrows():
                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_validos = obtener_archivos_pdf_validos(prefix)
                archivos_coincidentes = []

                for archivo in archivos_validos:
                    key = archivo["Key"]
                    texto = extraer_texto_pdf(key)

                    clave_sin_espacios = clave.replace(" ", "")
                    texto_limpio = texto.replace(" ", "").replace("\n", "")

                    coincide = (
                        clave in texto
                        or clave_sin_espacios in texto_limpio
                        or re.search(re.escape(clave), texto_limpio)
                        or re.search(re.escape(clave_sin_espacios), texto_limpio)
                    )

                    if coincide:
                        waybill_match = re.search(r"WAYBILL[\s:]*([0-9 ]{8,})", texto, re.IGNORECASE)
                        if waybill_match:
                            st.code(f"📦 WAYBILL detectado: {waybill_match.group(1)}")

                        archivos_coincidentes.append((key, get_s3_file_download_url(s3_client, key)))
                        todos_los_archivos = obtener_todos_los_archivos(prefix)
                        comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                        facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                        otros = [f for f in todos_los_archivos if f not in comprobantes and f not in facturas and f["Key"] != archivos_coincidentes[0][0]]

                        resultados.append({
                            "__source": "pedidos",
                            "ID_Pedido": pedido_id,
                            "Cliente": row.get("Cliente", ""),
                            "Estado": row.get("Estado", ""),
                            "Vendedor": row.get("Vendedor_Registro", ""),
                            "Folio": row.get("Folio_Factura", ""),
                            "Hora_Registro": row.get("Hora_Registro", ""),
                            "Comentario": str(row.get("Comentario", "")).strip(),
                            "Comentarios": str(row.get("Comentarios", "")).strip(),
                            "Direccion_Guia_Retorno": str(row.get("Direccion_Guia_Retorno", "")).strip(),
                            "Nota_Venta": str(row.get("Nota_Venta", "")).strip(),
                            "Tiene_Nota_Venta": str(row.get("Tiene_Nota_Venta", "")).strip(),
                            "Motivo_NotaVenta": str(row.get("Motivo_NotaVenta", "")).strip(),
                            # 🛠 Modificación de surtido
                            "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                            "Fecha_Modificacion_Surtido": obtener_fecha_modificacion(row),
                            "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                            # Archivos registrados en la hoja
                            "Adjuntos_Guia_urls": partir_urls(row.get("Adjuntos_Guia", "")),
                            "Adjuntos_urls": partir_urls(row.get("Adjuntos", "")),
                            # ♻️ Refacturación
                            "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                            "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                            "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                            # Archivos S3
                            "Coincidentes": archivos_coincidentes,
                            "Comprobantes": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in comprobantes],
                            "Facturas": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in facturas],
                            "Otros": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in otros],
                        })
                        break  # detener búsqueda tras encontrar coincidencia dentro del pedido

                if archivos_coincidentes:
                    break  # detener búsqueda global: por guía solo debe existir una coincidencia

        # ====== RENDER DE RESULTADOS ======
        st.markdown("---")
        if resultados:
            mensaje_exito = f"✅ Se encontraron coincidencias en {len(resultados)} registro(s)."
            if filtro_fechas_activo:
                mensaje_exito += " (Filtro temporal aplicado)"
            st.success(mensaje_exito)

            detalles_filtros = [f"Orden: {orden_seleccionado}"]
            if filtro_fechas_activo and fecha_inicio_date and fecha_fin_date:
                detalles_filtros.append(
                    f"Rango: {fecha_inicio_date.strftime('%Y-%m-%d')} → {fecha_fin_date.strftime('%Y-%m-%d')}"
                )
            if detalles_filtros:
                st.caption(" | ".join(detalles_filtros))

            # Ordena por Hora_Registro según la selección cuando exista
            def _parse_dt(v):
                try:
                    return pd.to_datetime(v)
                except Exception:
                    return pd.NaT
            resultados = sorted(
                resultados,
                key=lambda r: _parse_dt(r.get("Hora_Registro")),
                reverse=recientes_primero,
            )

            for res in resultados:
                if res.get("__source") == "casos":
                    render_caso_especial(res)
                else:
                    # ---------- Render de PEDIDOS (flujo actual) ----------
                    st.markdown(f"### 🤝 {res['Cliente'] or 'Cliente N/D'}")
                    st.markdown(
                        f"📄 **Folio:** `{res['Folio'] or 'N/D'}`  |  🔍 **Estado:** `{res['Estado'] or 'N/D'}`  |  🧑‍💼 **Vendedor:** `{res['Vendedor'] or 'N/D'}`  |  🕒 **Hora:** `{res['Hora_Registro'] or 'N/D'}`"
                    )

                    comentario_txt = str(res.get("Comentario", "") or res.get("Comentarios", "")).strip()
                    if comentario_txt:
                        st.markdown("#### 📝 Comentarios del pedido")
                        st.info(comentario_txt)

                    direccion_retorno = str(res.get("Direccion_Guia_Retorno", "")).strip()
                    if direccion_retorno:
                        st.markdown("#### 📍 Dirección para guía de retorno")
                        st.info(direccion_retorno)

                    nota_venta_valor = str(res.get("Nota_Venta", "")).strip()
                    tiene_nota_venta = str(res.get("Tiene_Nota_Venta", "")).strip()
                    motivo_nota_venta = str(res.get("Motivo_NotaVenta", "")).strip()
                    if nota_venta_valor or tiene_nota_venta or motivo_nota_venta:
                        st.markdown("#### 🧾 Nota de venta")
                        estado_texto = tiene_nota_venta or ("Sí" if nota_venta_valor else "No")
                        st.markdown(f"- **¿Tiene nota de venta?:** {estado_texto}")
                        if nota_venta_valor:
                            st.markdown(f"- **Detalle:** {nota_venta_valor}")
                        if motivo_nota_venta:
                            st.markdown(f"- **Motivo:** {motivo_nota_venta}")

                    mod_txt = res.get("Modificacion_Surtido", "") or ""
                    mod_fecha = res.get("Fecha_Modificacion_Surtido", "") or ""
                    mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
                    if mod_txt or mod_urls:
                        st.markdown("#### 🛠 Modificación de surtido")
                        if mod_fecha:
                            st.caption(f"📅 Fecha de modificación: {mod_fecha}")
                        if mod_txt:
                            st.info(mod_txt)
                        if mod_urls:
                            st.markdown("**Archivos de modificación:**")
                            for u in mod_urls:
                                nombre = extract_s3_key(u).split("/")[-1]
                                tmp = get_s3_file_download_url(s3_client, u)
                                st.markdown(
                                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                                    unsafe_allow_html=True,
                                )

                    # ♻️ Refacturación (si hay)
                    ref_t = res.get("Refacturacion_Tipo","")
                    ref_st = res.get("Refacturacion_Subtipo","")
                    ref_f = res.get("Folio_Factura_Refacturada","")
                    if any([ref_t, ref_st, ref_f]):
                        with st.expander("♻️ Refacturación", expanded=False):
                            st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
                            st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
                            st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

                    with st.expander("📁 Archivos del Pedido", expanded=True):
                        guia_hoja = res.get("Adjuntos_Guia_urls") or []
                        if guia_hoja:
                            st.markdown("#### 🧾 Guías registradas en la hoja:")
                            for idx, raw_url in enumerate(guia_hoja, start=1):
                                nombre, enlace = resolver_nombre_y_enlace(raw_url, f"Guía hoja #{idx}")
                                if not enlace:
                                    continue
                                st.markdown(
                                    f'- <a href="{enlace}" target="_blank">🧾 {nombre} (hoja)</a>',
                                    unsafe_allow_html=True,
                                )

                        if res.get("Coincidentes"):
                            st.markdown("#### 🔍 Guías detectadas en S3:")
                            for key, url in res["Coincidentes"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">🔍 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        if res.get("Comprobantes"):
                            st.markdown("#### 🧾 Comprobantes:")
                            for key, url in res["Comprobantes"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">📄 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        if res.get("Facturas"):
                            st.markdown("#### 📁 Facturas:")
                            for key, url in res["Facturas"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">📄 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        adjuntos_hoja = res.get("Adjuntos_urls") or []
                        otros_s3 = res.get("Otros") or []
                        otros_items = []
                        claves_vistas = set()

                        def _normalizar_clave(valor):
                            if not valor:
                                return None
                            valor_str = str(valor).strip()
                            if not valor_str:
                                return None
                            return valor_str.lower()

                        def _registrar_clave(valor):
                            clave_norm = _normalizar_clave(valor)
                            if clave_norm:
                                claves_vistas.add(clave_norm)

                        def _esta_registrada(valor):
                            clave_norm = _normalizar_clave(valor)
                            if not clave_norm:
                                return False
                            return clave_norm in claves_vistas

                        for raw_url in guia_hoja:
                            clave = extract_s3_key(raw_url) or raw_url
                            _registrar_clave(clave)
                            _registrar_clave(raw_url)

                        for key, url in res.get("Coincidentes") or []:
                            clave = extract_s3_key(key) or key
                            _registrar_clave(clave)
                            if url:
                                _registrar_clave(extract_s3_key(url) or url)

                        for key, url in res.get("Comprobantes") or []:
                            clave = extract_s3_key(key) or key
                            _registrar_clave(clave)
                            if url:
                                _registrar_clave(extract_s3_key(url) or url)

                        for key, url in res.get("Facturas") or []:
                            clave = extract_s3_key(key) or key
                            _registrar_clave(clave)
                            if url:
                                _registrar_clave(extract_s3_key(url) or url)

                        for key, url in otros_s3:
                            clave = extract_s3_key(key) or key or url
                            if _esta_registrada(clave) or _esta_registrada(url):
                                continue
                            nombre = key.split("/")[-1] if key else "Archivo"
                            otros_items.append((nombre, url))
                            _registrar_clave(clave)
                            if url:
                                _registrar_clave(url)

                        for idx, raw_url in enumerate(adjuntos_hoja, start=1):
                            nombre, enlace = resolver_nombre_y_enlace(raw_url, f"Adjunto hoja #{idx}")
                            if not enlace:
                                continue
                            clave = extract_s3_key(raw_url) or enlace
                            if _esta_registrada(clave):
                                continue
                            otros_items.append((nombre or f"Adjunto hoja #{idx}", enlace))
                            _registrar_clave(clave)

                        if otros_items:
                            st.markdown("#### 📂 Otros Archivos:")
                            for nombre, enlace in otros_items:
                                st.markdown(
                                    f'- <a href="{enlace}" target="_blank">📌 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )

        else:
            mensaje = (
                "⚠️ No se encontraron coincidencias en ningún archivo PDF."
                if modo_busqueda == "🔢 Por número de guía"
                else "⚠️ No se encontraron pedidos o casos para el cliente ingresado."
            )
            if filtro_fechas_activo:
                mensaje += " Revisa el rango de fechas seleccionado."
            st.warning(mensaje)

if "descargar" in tab_map:
    with tab_map["descargar"]:
        st.header("⬇️ Descargar Datos")

        if st.button(
            "🔄 Refrescar datos",
            help="Recarga los datos desde Google Sheets para ver la información más reciente.",
        ):
            st.cache_data.clear()
            st.rerun()

        df_todos = cargar_todos_los_pedidos()
        df_casos = cargar_casos_especiales()

        sub_tabs = st.tabs([
            "⚙️ Pedidos en Flujo",
            "📦 Pedidos Históricos",
            "🧾 Casos especiales",
            "🟢 Solo pedidos completados",
        ])

        with sub_tabs[0]:
            flujo_data = construir_descarga_flujo_por_categoria()

            st.markdown("#### 🚚 Foráneos")
            render_descarga_tabla(
                df_base=flujo_data["foraneos"],
                key_prefix="descarga_flujo_foraneos",
                permitir_filtros=False,
                ordenar_por_id=False,
                mostrar_descarga=False,
            )

            st.markdown("#### 📍 Locales")
            render_descarga_tabla(
                df_base=flujo_data["locales"],
                key_prefix="descarga_flujo_locales",
                permitir_filtros=False,
                ordenar_por_id=False,
                mostrar_descarga=False,
            )

            st.markdown("#### 🧾 Casos especiales")
            render_descarga_tabla(
                df_base=flujo_data["casos"],
                key_prefix="descarga_flujo_casos",
                permitir_filtros=False,
                ordenar_por_id=False,
                mostrar_descarga=False,
            )

            excel_flujo_buffer = construir_excel_flujo_unificado(flujo_data)
            fecha_hoy = datetime.now().strftime("%d-%m-%Y")
            st.download_button(
                label="⬇️ Descargar Excel unificado",
                data=excel_flujo_buffer.getvalue(),
                file_name=f"pedidos_en_flujo_{fecha_hoy}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="descarga_flujo_unificado",
            )

        with sub_tabs[1]:
            render_descarga_tabla(
                df_base=df_todos,
                key_prefix="descarga_historicos",
                permitir_filtros=True,
                ordenar_por_id=True,
            )

        with sub_tabs[2]:
            render_descarga_tabla(
                df_base=df_casos,
                key_prefix="descarga_casos",
                permitir_filtros=True,
                ordenar_por_id=True,
            )

        with sub_tabs[3]:
            df_solo_completados = construir_descarga_solo_completados()
            render_descarga_tabla(
                df_base=df_solo_completados,
                key_prefix="descarga_solo_completados",
                permitir_filtros=False,
                ordenar_por_id=True,
            )

if "modificar" in tab_map:
    with tab_map["modificar"]:
        st.header("✏️ Modificar Pedido Existente")

        if st.button(
            "🔄 Actualizar pedidos",
            key="refresh_modificar_pedido",
            help="Recarga la información más reciente de la hoja para mostrar nuevos pedidos y cambios.",
        ):
            st.cache_data.clear()
            st.rerun()

        df_pedidos = cargar_pedidos_modificables()
        df_casos = cargar_casos_especiales()
        # Fuente exclusiva para la sección de casos especiales (sin mezclar otras hojas).
        df_casos_garantias = df_casos.copy()

        # En modificación solo se incluyen casos especiales pendientes de limpieza.
        if "Completados_Limpiado" not in df_casos.columns:
            df_casos["Completados_Limpiado"] = ""
        df_casos = df_casos[
            df_casos["Completados_Limpiado"].astype(str).str.strip() == ""
        ]

        for d in (df_pedidos, df_casos):
            d["Hora_Registro"] = pd.to_datetime(d["Hora_Registro"], errors="coerce")

        df_pedidos["__source"] = "pedidos"
        df_casos["__source"] = "casos"
        df = pd.concat([df_pedidos, df_casos], ignore_index=True, sort=False)
        df = df[df["ID_Pedido"].notna()]
        df = df.sort_values(by="Hora_Registro", ascending=False)

        pedido_sel = None
        source_sel = None

        # Mostrar todos los pedidos de la hoja casos_especiales en esta sección.
        df_garantias = df_casos_garantias.copy()

        mostrar_garantias = st.checkbox(
            "🔘 Mostrar sección de casos especiales",
            help="Activa esta opción para consultar únicamente la información de la hoja casos_especiales.",
        )

        if mostrar_garantias:
            st.markdown("### 🛡️ Casos especiales registrados")
            termino_busqueda_garantia = st.text_input(
                "Buscar por cliente o folio",
                key="busqueda_casos_especiales",
                placeholder="Cliente o folio",
            )

            termino_normalizado = normalizar(termino_busqueda_garantia or "")
            termino_folio = (
                normalizar_folio(termino_busqueda_garantia)
                if termino_busqueda_garantia
                else ""
            )

            if termino_normalizado:

                def coincide_garantia(row):
                    cliente = normalizar(str(row.get("Cliente", "")))
                    folio = normalizar_folio(
                        row.get("Folio_Factura") or row.get("Folio") or ""
                    )
                    return termino_normalizado in cliente or (
                        termino_folio and termino_folio in folio
                    )

                df_garantias_filtrado = df_garantias[
                    df_garantias.apply(coincide_garantia, axis=1)
                ]
            else:
                df_garantias_filtrado = df_garantias

            if df_garantias_filtrado.empty:
                st.info(
                    "No se encontraron casos especiales con el criterio de búsqueda proporcionado."
                )
                st.stop()
            else:

                def formatear_fecha(valor, formato):
                    if pd.isna(valor):
                        return ""
                    if isinstance(valor, pd.Timestamp):
                        return valor.strftime(formato)
                    try:
                        fecha = pd.to_datetime(valor)
                        if pd.isna(fecha):
                            return ""
                        return fecha.strftime(formato)
                    except Exception:
                        return str(valor)

                columnas_tabla = {
                    "ID_Pedido": "Pedido",
                    "Hora_Registro": "Hora Registro",
                    "Vendedor_Registro": "Vendedor Registro",
                    "Cliente": "Cliente",
                    "Folio_Factura": "Folio / Factura",
                    "Numero_Serie": "Número Serie",
                    "Fecha_Compra": "Fecha Compra",
                    "Tipo_Envio": "Tipo Envío",
                    "Estado": "Estado",
                    "Estado_Caso": "Estado Caso",
                    "Seguimiento": "Seguimiento",
                }

                tabla_garantias = df_garantias_filtrado[list(columnas_tabla.keys())].copy()
                tabla_garantias["Hora_Registro"] = tabla_garantias["Hora_Registro"].apply(
                    lambda v: formatear_fecha(v, "%d/%m/%Y %H:%M")
                )
                tabla_garantias["Fecha_Compra"] = tabla_garantias["Fecha_Compra"].apply(
                    lambda v: formatear_fecha(v, "%d/%m/%Y") if str(v).strip() else ""
                )

                tabla_garantias = tabla_garantias.rename(columns=columnas_tabla)
                st.dataframe(tabla_garantias, use_container_width=True)

                opciones_select = [None] + df_garantias_filtrado.index.tolist()

                def format_garantia(idx):
                    if idx is None:
                        return "Selecciona un caso especial"
                    row = df_garantias_filtrado.loc[idx]
                    hora = formatear_fecha(row.get("Hora_Registro"), "%d/%m/%Y %H:%M")
                    estado = row.get("Estado_Caso") or row.get("Estado") or ""
                    return (
                        f"📦 {row.get('ID_Pedido', '')} | 🧾 {row.get('Folio_Factura', '')} | "
                        f"👤 {row.get('Cliente', '')} | 🚚 {row.get('Tipo_Envio', '')} | "
                        f"🔍 {estado} | 🕒 {hora}"
                    )

                idx_garantia = st.selectbox(
                    "Selecciona un caso especial para ver detalles o modificarlo:",
                    opciones_select,
                    format_func=format_garantia,
                    key="select_caso_especial",
                )

                if idx_garantia is not None and idx_garantia in df_garantias_filtrado.index:
                    row_garantia = df_garantias_filtrado.loc[idx_garantia]
                    pedido_sel = row_garantia.get("ID_Pedido")
                    source_sel = "casos"
                    st.markdown("#### 📘 Detalles del caso especial seleccionado")

                    def limpiar(valor):
                        if valor is None:
                            return ""
                        if isinstance(valor, str):
                            return "" if not valor.strip() or valor.strip().lower() == "nan" else valor.strip()
                        try:
                            if pd.isna(valor):
                                return ""
                        except Exception:
                            pass
                        return valor

                    def formatear_monto(valor):
                        try:
                            if valor is None or str(valor).strip() == "":
                                return ""
                            valor_float = float(valor)
                            valor_formateado = f"{valor_float:,.2f}"
                            valor_formateado = (
                                valor_formateado.replace(",", "_")
                                .replace(".", ",")
                                .replace("_", ".")
                            )
                            return f"${valor_formateado}"
                        except Exception:
                            valor_limpio = limpiar(valor)
                            return f"${valor_limpio}" if valor_limpio else ""

                    col_izq, col_der = st.columns(2)

                    detalles_izq = [
                        ("📦 Pedido", row_garantia.get("ID_Pedido", "")),
                        ("👤 Cliente", row_garantia.get("Cliente", "")),
                        ("🧾 Folio / Factura", row_garantia.get("Folio_Factura", "")),
                        ("🚚 Tipo de envío", row_garantia.get("Tipo_Envio", "")),
                        ("📊 Estado", row_garantia.get("Estado", "")),
                        ("🧮 Estado del caso", row_garantia.get("Estado_Caso", "")),
                        ("🕵️ Seguimiento", row_garantia.get("Seguimiento", "")),
                        ("🧑‍💼 Vendedor", row_garantia.get("Vendedor_Registro", "")),
                        (
                            "🕒 Hora de registro",
                            formatear_fecha(row_garantia.get("Hora_Registro"), "%d/%m/%Y %H:%M"),
                        ),
                    ]

                    detalles_der = [
                        ("🔢 Número de serie", row_garantia.get("Numero_Serie", "")),
                        (
                            "🗓️ Fecha de compra",
                            formatear_fecha(row_garantia.get("Fecha_Compra"), "%d/%m/%Y"),
                        ),
                        ("🎯 Resultado esperado", row_garantia.get("Resultado_Esperado", "")),
                        ("📦 Material devuelto", row_garantia.get("Material_Devuelto", "")),
                        ("💵 Monto devuelto", formatear_monto(row_garantia.get("Monto_Devuelto", ""))),
                        ("📝 Motivo detallado", row_garantia.get("Motivo_Detallado", "")),
                        ("🏢 Área responsable", row_garantia.get("Area_Responsable", "")),
                        ("👥 Responsable", row_garantia.get("Nombre_Responsable", "")),
                        ("🧾 Nota de venta", row_garantia.get("Nota_Venta", "")),
                        ("❓ ¿Tiene nota de venta?", row_garantia.get("Tiene_Nota_Venta", "")),
                        ("🧾 Motivo nota de venta", row_garantia.get("Motivo_NotaVenta", "")),
                        ("📍 Dirección guía retorno", row_garantia.get("Direccion_Guia_Retorno", "")),
                    ]

                    etiquetas_resaltadas = {"🕵️ Seguimiento", "📝 Motivo detallado"}

                    for columna, items in ((col_izq, detalles_izq), (col_der, detalles_der)):
                        for etiqueta, valor in items:
                            valor_limpio = limpiar(valor)
                            if not valor_limpio:
                                continue
                            if etiqueta in etiquetas_resaltadas:
                                columna.info(f"{etiqueta}: {valor_limpio}")
                            else:
                                columna.markdown(f"**{etiqueta}:** {valor_limpio}")

                    comentarios = str(row_garantia.get("Comentario", "")).strip()
                    comentarios_adicionales = str(row_garantia.get("Comentarios", "")).strip()
                    if comentarios or comentarios_adicionales:
                        st.markdown("#### 💬 Comentarios")
                        if comentarios:
                            st.info(comentarios)
                        if comentarios_adicionales:
                            st.info(comentarios_adicionales)

                    secciones_adjuntos = []

                    def agregar_adjuntos(titulo, valores):
                        urls = partir_urls(valores)
                        urls_limpias = []
                        for u in urls:
                            url_limpio = limpiar(u)
                            if url_limpio:
                                urls_limpias.append(url_limpio)
                        if urls_limpias:
                            secciones_adjuntos.append((titulo, urls_limpias))

                    agregar_adjuntos("📎 Archivos adjuntos", row_garantia.get("Adjuntos", ""))
                    agregar_adjuntos("🧾 Guías asociadas", row_garantia.get("Adjuntos_Guia", ""))
                    agregar_adjuntos("📬 Hoja de ruta", row_garantia.get("Hoja_Ruta_Mensajero", ""))
                    agregar_adjuntos("🛠 Archivos de surtido", row_garantia.get("Adjuntos_Surtido", ""))
                    agregar_adjuntos("💳 Notas de crédito", row_garantia.get("Nota_Credito_URL", ""))
                    agregar_adjuntos("📄 Documentos adicionales", row_garantia.get("Documento_Adicional_URL", ""))

                    if secciones_adjuntos:
                        st.markdown("#### 🗂️ Archivos del caso especial")
                        for titulo, urls in secciones_adjuntos:
                            st.markdown(f"**{titulo}:**")
                            for idx, raw_url in enumerate(urls, start=1):
                                nombre, enlace = resolver_nombre_y_enlace(raw_url, f"{titulo} #{idx}")
                                if not enlace:
                                    continue
                                st.markdown(
                                    f'- <a href="{enlace}" target="_blank">{nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                else:
                    pedido_sel = None
                    source_sel = None
                    st.info("Selecciona un caso especial para ver detalles o modificarlo.")
                    st.stop()


        if "pedido_modificado" in st.session_state:
            pedido_sel = st.session_state["pedido_modificado"]
            source_sel = st.session_state.get(
                "pedido_modificado_source", source_sel or "pedidos"
            )
            del st.session_state["pedido_modificado"]  # ✅ limpia la variable tras usarla
            if "pedido_modificado_source" in st.session_state:
                del st.session_state["pedido_modificado_source"]


        if pedido_sel is None:
            st.markdown("### 📋 Lista completa de pedidos y casos disponibles")

            if df.empty:
                st.warning("⚠️ No hay pedidos disponibles para modificar.")
                st.stop()

            def _fmt_hora_mod(valor):
                if pd.isna(valor):
                    return "Sin fecha"
                if isinstance(valor, pd.Timestamp):
                    return valor.strftime('%d/%m %H:%M')
                try:
                    return pd.to_datetime(valor).strftime('%d/%m %H:%M')
                except Exception:
                    return str(valor)

            df_lista = df.copy()
            df_lista["display"] = df_lista.apply(
                lambda row: (
                    f"🧾 {row.get('Folio_Factura', row.get('Folio',''))} – {row.get('Tipo_Envio','')} "
                    f"– 👤 {row.get('Cliente','')} – 🔍 {row.get('Estado', row.get('Estado_Caso',''))} "
                    f"– 🧑‍💼 {row.get('Vendedor_Registro','')} – 🕒 {_fmt_hora_mod(row.get('Hora_Registro'))}"
                ),
                axis=1,
            )

            idx_seleccion = st.selectbox(
                "⬇️ Selecciona el pedido a modificar:",
                df_lista.index.tolist(),
                format_func=lambda i: df_lista.loc[i, "display"],
            )
            pedido_sel = df_lista.loc[idx_seleccion, "ID_Pedido"]
            source_sel = df_lista.loc[idx_seleccion, "__source"]


        # --- Cargar datos del pedido seleccionado ---
        st.markdown("---")

        if pedido_sel is None:
            st.warning("⚠️ No se ha seleccionado ningún pedido válido.")
            st.stop()

        row_df = df_pedidos if source_sel == "pedidos" else df_casos_garantias
        row_sel = row_df[row_df["ID_Pedido"].astype(str) == str(pedido_sel)]
        if row_sel.empty:
            st.session_state.pop("pedido_modificado", None)
            st.session_state.pop("pedido_modificado_source", None)
            st.warning(
                "⚠️ El pedido seleccionado ya no está disponible en la lista actual. Selecciona otro pedido."
            )
            st.rerun()
        row = row_sel.iloc[0]
        gspread_row_idx = int(row.get("__sheet_row", row_sel.index[0] + 2))
        if "mensaje_exito" in st.session_state:
            st.success(st.session_state["mensaje_exito"])
            del st.session_state["mensaje_exito"]  # ✅ eliminar para que no se repita
        if "mensaje_error" in st.session_state:
            st.error(st.session_state["mensaje_error"])
            del st.session_state["mensaje_error"]


        # Definir la hoja de Google Sheets para modificación
        if source_sel == "pedidos":
            hoja_nombre = str(row.get("__hoja_origen", "")).strip() or "datos_pedidos"
        else:
            hoja_nombre = "casos_especiales"
        hoja = get_main_worksheet(hoja_nombre)

        def actualizar_celdas_y_confirmar(cambios, mensaje_exito):
            """Actualiza celdas en lote y valida lectura de los nuevos valores."""
            try:
                headers = hoja.row_values(1)
                mapa_columnas_hoja = {
                    str(nombre).strip(): idx + 1
                    for idx, nombre in enumerate(headers)
                    if str(nombre).strip()
                }

                updates = []
                celdas = []
                for nombre_col, valor in cambios:
                    if nombre_col not in mapa_columnas_hoja:
                        raise ValueError(
                            f"No existe la columna '{nombre_col}' en la hoja {hoja_nombre}."
                        )
                    col_idx = mapa_columnas_hoja[nombre_col]
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(gspread_row_idx, col_idx),
                        "values": [[valor]],
                    })
                    celdas.append(gspread.Cell(row=gspread_row_idx, col=col_idx, value=valor))

                if hasattr(hoja, "batch_update"):
                    hoja.batch_update(updates, value_input_option="USER_ENTERED")
                else:
                    hoja.update_cells(celdas, value_input_option="USER_ENTERED")

                for nombre_col, valor_esperado in cambios:
                    col_idx = mapa_columnas_hoja[nombre_col]
                    valor_real = hoja.cell(gspread_row_idx, col_idx).value
                    esperado = "" if valor_esperado is None else str(valor_esperado).strip()
                    real = "" if valor_real is None else str(valor_real).strip()
                    if esperado != real:
                        raise ValueError(
                            f"La columna '{nombre_col}' no se confirmó. Esperado: '{esperado}' | Guardado: '{real}'."
                        )

                st.session_state["pedido_modificado"] = pedido_sel
                st.session_state["pedido_modificado_source"] = source_sel
                st.session_state["mensaje_exito"] = mensaje_exito
                return True
            except Exception as e:
                st.session_state["mensaje_error"] = f"❌ No se pudo guardar en Excel: {e}"
                return False

        st.markdown(
            f"📦 **Cliente:** {row['Cliente']} &nbsp;&nbsp;&nbsp;&nbsp; 🧾 **Folio Factura:** {row.get('Folio_Factura', 'N/A')}"
        )

        with st.expander("📎 Adjuntar Archivos — Gestionar guías y documentos", expanded=False):
            col_guias = "Adjuntos_Guia" if source_sel == "pedidos" else "Hoja_Ruta_Mensajero"
            existentes_guias = partir_urls(row.get(col_guias, ""))
            existentes_otros = partir_urls(row.get("Adjuntos", ""))

            if existentes_guias or existentes_otros:
                with st.expander("📥 Archivos existentes", expanded=False):
                    if existentes_guias:
                        st.markdown("**Guías:**")
                        for u in existentes_guias:
                            tmp = get_s3_file_download_url(s3_client, u)
                            nombre = extract_s3_key(u).split("/")[-1]
                            st.markdown(f'- <a href="{tmp}" target="_blank">{nombre}</a>', unsafe_allow_html=True)
                    if existentes_otros:
                        st.markdown("**Otros:**")
                        for u in existentes_otros:
                            tmp = get_s3_file_download_url(s3_client, u)
                            nombre = extract_s3_key(u).split("/")[-1]
                            st.markdown(f'- <a href="{tmp}" target="_blank">{nombre}</a>', unsafe_allow_html=True)

            uploaded_guias = st.file_uploader("📄 Guías", accept_multiple_files=True)
            uploaded_otros = st.file_uploader("📁 Otros", accept_multiple_files=True)

            if st.button("⬆️ Subir archivos"):
                nuevas_guias_urls, nuevas_otros_urls = [], []
                for file in uploaded_guias or []:
                    key = f"adjuntos_pedidos/{pedido_sel}/{file.name}"
                    success, url_subida = upload_file_to_s3(s3_client, S3_BUCKET, file, key)
                    if success:
                        nuevas_guias_urls.append(url_subida)
                for file in uploaded_otros or []:
                    key = f"adjuntos_pedidos/{pedido_sel}/{file.name}"
                    success, url_subida = upload_file_to_s3(s3_client, S3_BUCKET, file, key)
                    if success:
                        nuevas_otros_urls.append(url_subida)

                cambios_archivos = []
                if nuevas_guias_urls:
                    existente = row.get(col_guias, "")
                    nuevo_valor = combinar_urls_existentes(existente, nuevas_guias_urls)
                    cambios_archivos.append((col_guias, nuevo_valor))
                if nuevas_otros_urls:
                    existente = row.get("Adjuntos", "")
                    nuevo_valor = combinar_urls_existentes(existente, nuevas_otros_urls)
                    cambios_archivos.append(("Adjuntos", nuevo_valor))

                if cambios_archivos:
                    if actualizar_celdas_y_confirmar(cambios_archivos, "📎 Archivos subidos correctamente."):
                        st.rerun()
                else:
                    st.warning("⚠️ No se cargaron archivos nuevos para actualizar en Excel.")


        # --- CAMPOS MODIFICABLES ---
        if source_sel == "casos":
            tipo_envio_val = str(row.get("Tipo_Envio", "") or "")
            tipo_caso_val = str(row.get("Tipo_Caso", "") or "")
            es_garantia = any("garant" in valor.lower() for valor in (tipo_envio_val, tipo_caso_val))

            if es_garantia:
                opciones_seguimiento = [
                    "llegó el material",
                    "en prueba",
                    "aprobada",
                    "rechazada",
                ]
                seguimiento_actual = str(row.get("Seguimiento", "") or "").strip()
                try:
                    index_preseleccion = next(
                        i for i, opcion in enumerate(opciones_seguimiento) if opcion.lower() == seguimiento_actual.lower()
                    )
                except StopIteration:
                    index_preseleccion = 0

                seguimiento_sel = st.selectbox(
                    "Seguimiento de garantía",
                    opciones_seguimiento,
                    index=index_preseleccion,
                )

                if st.button("Guardar seguimiento"):
                    if actualizar_celdas_y_confirmar(
                        [("Seguimiento", seguimiento_sel)],
                        "🔄 Seguimiento de garantía guardado correctamente.",
                    ):
                        st.rerun()

        comentario_usuario = st.text_area("📝 Comentario desde almacén", key="comentario_almacen")
        if st.button("Guardar comentario"):
            comentario_limpio = comentario_usuario.strip()
            if not comentario_limpio:
                st.warning("⚠️ Debes ingresar un comentario antes de guardarlo.")
            else:
                existente = str(row.get("Comentario", "") or "")
                nuevo_comentario = f"[ALMACÉN 🏷️] {comentario_limpio}"
                if existente.strip():
                    valor_final = f"{existente.rstrip()}\n{nuevo_comentario}"
                else:
                    valor_final = nuevo_comentario
                if actualizar_celdas_y_confirmar(
                    [("Comentario", valor_final)],
                    "📝 Comentario guardado correctamente.",
                ):
                    st.session_state["comentario_almacen"] = ""
                    st.rerun()

        vendedores = [
            "ALEJANDRO RODRIGUEZ",
            "ANA KAREN ORTEGA MAHUAD",
            "DANIELA LOPEZ RAMIREZ",
            "EDGAR ORLANDO GOMEZ VILLAGRAN",
            "GLORIA MICHELLE GARCIA TORRES",
            "GRISELDA CAROLINA SANCHEZ GARCIA",
            "HECTOR DEL ANGEL AREVALO ALCALA",
            "JOSELIN TRUJILLO PATRACA",
            "NORA ALEJANDRA MARTINEZ MORENO",
            "PAULINA TREJO"
        ]
        vendedor_actual = row.get("Vendedor_Registro", "").strip()

        with st.expander("🧑‍💼 Cambio de Vendedor — Reasignar responsable", expanded=False):
            st.markdown(f"**Actual:** {vendedor_actual}")

            opcion_manual = "✍️ Escribir vendedor manualmente"
            vendedores_opciones = [v for v in vendedores if v != vendedor_actual] or [vendedor_actual]
            vendedores_opciones = vendedores_opciones + [opcion_manual]
            nuevo_vendedor = st.selectbox("➡️ Cambiar a:", vendedores_opciones)

            vendedor_manual = ""
            if nuevo_vendedor == opcion_manual:
                vendedor_manual = st.text_input(
                    "📝 Nombre del vendedor",
                    placeholder="Ej. JUAN PÉREZ",
                    key="vendedor_manual_modificar",
                )

            vendedor_destino = (
                vendedor_manual.strip().upper()
                if nuevo_vendedor == opcion_manual
                else nuevo_vendedor
            )

            if st.button("🧑‍💼 Guardar cambio de vendedor"):
                if not vendedor_destino:
                    st.warning("⚠️ Escribe un nombre de vendedor válido para continuar.")
                    st.stop()
                if actualizar_celdas_y_confirmar(
                    [("Vendedor_Registro", vendedor_destino)],
                    "🎈 Vendedor actualizado correctamente.",
                ):
                    st.rerun()


        if source_sel == "pedidos":
            tipo_envio_actual = row["Tipo_Envio"].strip()
            with st.expander("🚚 Cambio de Tipo de Envío — Ajustar logística", expanded=False):
                st.markdown(f"**Actual:** {tipo_envio_actual}")

                opcion_contraria = "📍 Pedido Local" if "Foráneo" in tipo_envio_actual else "🚚 Pedido Foráneo"
                tipo_envio = st.selectbox("➡️ Cambiar a:", [opcion_contraria])

                if tipo_envio == "📍 Pedido Local":
                    nuevo_turno = st.selectbox("⏰ Turno", ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"])
                    fecha_entrega_actual_raw = str(row.get("Fecha_Entrega", "") or "").strip()
                    fecha_entrega_actual_dt = pd.to_datetime(fecha_entrega_actual_raw, errors="coerce")
                    fecha_entrega_actual_mostrar = (
                        fecha_entrega_actual_dt.strftime("%d/%m/%Y")
                        if pd.notna(fecha_entrega_actual_dt)
                        else "Sin fecha"
                    )
                    st.markdown(f"**📅 Fecha de entrega actual:** {fecha_entrega_actual_mostrar}")

                    fecha_entrega_nueva = st.date_input(
                        "📅 Fecha de entrega",
                        value=(
                            fecha_entrega_actual_dt.date()
                            if pd.notna(fecha_entrega_actual_dt)
                            else date.today()
                        ),
                        min_value=min(
                            fecha_entrega_actual_dt.date(),
                            date.today(),
                        )
                        if pd.notna(fecha_entrega_actual_dt)
                        else date.today(),
                        max_value=date.today() + timedelta(days=365),
                        format="DD/MM/YYYY",
                    )
                    fecha_entrega_nueva_str = fecha_entrega_nueva.strftime("%Y-%m-%d")
                else:
                    nuevo_turno = ""
                    fecha_entrega_nueva_str = str(row.get("Fecha_Entrega", "") or "").strip()

                if st.button("📦 Guardar cambio de tipo de envío"):
                    if actualizar_celdas_y_confirmar(
                        [
                            ("Tipo_Envio", tipo_envio),
                            ("Turno", nuevo_turno),
                            ("Fecha_Entrega", fecha_entrega_nueva_str),
                        ],
                        "📦 Tipo de envío, turno y fecha de entrega actualizados correctamente.",
                    ):
                        st.rerun()


        # --- NUEVO: CAMBIO DE ESTADO A CANCELADO ---
        estado_actual = row.get("Estado", "").strip()
        with st.expander("🟣 Cancelar Pedido — Marcar como no procesable", expanded=False):
            st.markdown(f"**Estado Actual:** {estado_actual}")

            # Solo mostrar la opción de cancelar si el pedido no está ya cancelado
            if "Cancelado" not in estado_actual:
                if st.button("🟣 Cambiar Estado a CANCELADO"):
                    try:
                        # Actualizar el estado en la hoja de cálculo
                        nuevo_estado = "🟣 Cancelado"
                        if actualizar_celdas_y_confirmar(
                            [("Estado", nuevo_estado)],
                            "🟣 Pedido marcado como CANCELADO correctamente.",
                        ):
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al cancelar el pedido: {str(e)}")
            else:
                st.info("ℹ️ Este pedido ya está marcado como CANCELADO.")


        if source_sel == "casos":
            completado = row.get("Completados_Limpiado", "")
            with st.expander("👁 Visibilidad en Pantalla de Producción — Mostrar u ocultar", expanded=False):
                opciones_visibilidad = {"Sí": "", "No": "sí"}
                valor_actual = completado.strip().lower()
                valor_preseleccionado = "No" if valor_actual == "sí" else "Sí"
                seleccion = st.selectbox("¿Mostrar este pedido en el Panel?", list(opciones_visibilidad.keys()), index=list(opciones_visibilidad.keys()).index(valor_preseleccionado))
                nuevo_valor_completado = opciones_visibilidad[seleccion]


                if st.button("👁 Guardar visibilidad en Panel"):
                    if actualizar_celdas_y_confirmar(
                        [("Completados_Limpiado", nuevo_valor_completado)],
                        "👁 Visibilidad en pantalla de producción actualizada.",
                    ):
                        st.rerun()

    # ===== ORGANIZADOR ALEJANDRO =====

if "organizador" in tab_map:
    with tab_map["organizador"]:
        st.header("🗂️ Organizador")

        if st.button("🔄 Refrescar Organizador", key="refresh_alejandro"):
            st.rerun()

        # --- Subpestañas internas del organizador ---
        sub = st.tabs(["📌 Hoy", "🗓️ Agenda", "✅ Pendientes", "💼 Cotizaciones", "📋 Checklist"])

        errores_alejandro = []

        try:
            df_citas = cargar_alejandro_hoja("CITAS")
        except Exception as e:
            errores_alejandro.append(f"CITAS: {e}")
            df_citas = pd.DataFrame(columns=ALE_COLUMNAS.get("CITAS", []))

        try:
            df_tareas = cargar_alejandro_hoja("TAREAS")
        except Exception as e:
            errores_alejandro.append(f"TAREAS: {e}")
            df_tareas = pd.DataFrame(columns=ALE_COLUMNAS.get("TAREAS", []))

        try:
            df_cot = cargar_alejandro_hoja("COTIZACIONES")
        except Exception as e:
            errores_alejandro.append(f"COTIZACIONES: {e}")
            df_cot = pd.DataFrame(columns=ALE_COLUMNAS.get("COTIZACIONES", []))

        try:
            df_checklist_daily = cargar_alejandro_hoja("CHECKLIST_DAILY")
        except Exception as e:
            errores_alejandro.append(f"CHECKLIST_DAILY: {e}")
            df_checklist_daily = pd.DataFrame(columns=ALE_COLUMNAS.get("CHECKLIST_DAILY", []))

        try:
            df_checklist_template = cargar_alejandro_hoja("CHECKLIST_TEMPLATE")
        except Exception as e:
            errores_alejandro.append(f"CHECKLIST_TEMPLATE: {e}")
            df_checklist_template = pd.DataFrame(columns=ALE_COLUMNAS.get("CHECKLIST_TEMPLATE", []))

        try:
            df_config = cargar_alejandro_hoja("CONFIG")
        except Exception as e:
            errores_alejandro.append(f"CONFIG: {e}")
            df_config = pd.DataFrame(columns=ALE_COLUMNAS.get("CONFIG", []))

        if errores_alejandro:
            st.warning("⚠️ Hay errores leyendo alejandro_data. Revisa los logs o ejecuta diagnóstico en modo mantenimiento.")

        with sub[0]:
            st.subheader("📌 Hoy")

            hoy = date.today()
            # chk_hoy base (por si no se sincroniza en este rerun)
            chk_hoy = df_checklist_daily.copy()
            if "Fecha" in chk_hoy.columns:
                chk_hoy["_f"] = _to_date(chk_hoy["Fecha"])
                chk_hoy = chk_hoy[chk_hoy["_f"] == hoy].copy()
            else:
                chk_hoy = chk_hoy.iloc[0:0]

            key_sync = f"chk_sync_{hoy.isoformat()}"
            if key_sync not in st.session_state:
                st.session_state[key_sync] = False

            sync_clicked = st.button("🔄 Sincronizar checklist de hoy", key=f"btn_sync_chk_hoy_{hoy.isoformat()}")

            # Sincroniza checklist recurrente del día (solo 1 vez por sesión/día o por botón)
            if sync_clicked or not st.session_state[key_sync]:
                try:
                    inserted = ensure_daily_checklist_items(hoy, df_checklist_template, df_checklist_daily)
                    st.session_state[key_sync] = True
                    if inserted > 0:
                        st.success(f"🧾 Se generaron {inserted} ítem(s) de checklist para hoy.")
                    else:
                        st.info("🧾 Checklist de hoy ya estaba sincronizado.")
                    df_checklist_daily = cargar_alejandro_hoja("CHECKLIST_DAILY")

                    # rebuild chk_hoy para dashboard tras recarga
                    chk_hoy = df_checklist_daily.copy()
                    if "Fecha" in chk_hoy.columns:
                        chk_hoy["_f"] = _to_date(chk_hoy["Fecha"])
                        chk_hoy = chk_hoy[chk_hoy["_f"] == hoy].copy()
                    else:
                        chk_hoy = chk_hoy.iloc[0:0]
                except Exception as e:
                    st.warning(f"⚠️ No se pudo sincronizar checklist diario: {e}")

            # --- CITAS HOY + SEGUIMIENTOS PENDIENTES ---
            citas = df_citas.copy()
            if "Fecha_Inicio" in citas.columns:
                citas["_fi"] = _to_dt(citas["Fecha_Inicio"])
                estatus_ci = citas.get("Estatus", "").astype(str).str.lower().str.strip()
                citas_pendientes = citas[estatus_ci != "realizada"].copy()

                citas_hoy = citas_pendientes[citas_pendientes["_fi"].dt.date == hoy].copy()
                citas_hoy = citas_hoy.sort_values("_fi", ascending=True)
                citas_otras = citas_pendientes[citas_pendientes["_fi"].dt.date != hoy].copy()
                citas_otras = citas_otras.sort_values("_fi", ascending=True)

                tipo_seg = citas.get("Tipo", "").astype(str).str.lower().str.contains("seguimiento", na=False)
                estatus_pend = ~estatus_ci.isin(["realizada", "cancelada"])
                seguimientos_pend = citas[tipo_seg & estatus_pend].copy()
                seguimientos_pend = seguimientos_pend.sort_values("_fi", ascending=True)
            else:
                citas_hoy = citas.iloc[0:0]
                citas_otras = citas.iloc[0:0]
                seguimientos_pend = citas.iloc[0:0]

            # --- TAREAS (HOY / VENCIDAS) ---
            tareas = df_tareas.copy()
            if "Fecha_Limite" in tareas.columns:
                tareas["_fl"] = _to_dt(tareas["Fecha_Limite"])
                tareas_hoy = tareas[tareas["_fl"].dt.date == hoy].copy()
                tareas_vencidas = tareas[(tareas["_fl"].dt.date < hoy) & (tareas["Estatus"].astype(str).str.lower() != "completada")].copy()
                tareas_hoy = tareas_hoy.sort_values("_fl", ascending=True)
                tareas_vencidas = tareas_vencidas.sort_values("_fl", ascending=True)

                tareas_hoy_total = len(tareas_hoy)
                tareas_hoy_done = (tareas_hoy["Estatus"].astype(str).str.lower() == "completada").sum()
                tareas_hoy_pct = round((tareas_hoy_done / tareas_hoy_total) * 100, 1) if tareas_hoy_total else 0
            else:
                tareas_hoy = tareas.iloc[0:0]
                tareas_vencidas = tareas.iloc[0:0]
                tareas_hoy_total, tareas_hoy_done, tareas_hoy_pct = 0, 0, 0

            # --- COTIZACIONES (PENDIENTES / VENCIDAS DE SEGUIMIENTO) ---
            cot = df_cot.copy()
            if "Fecha_Proximo_Seguimiento" in cot.columns:
                cot["_fps"] = _to_dt(cot["Fecha_Proximo_Seguimiento"])
                est = cot.get("Estatus", "").astype(str).str.lower()
                no_cerradas = ~est.str.contains("ganada|perdida", na=False)
                cot_pend = cot[no_cerradas].copy()
                cot_venc = cot_pend[cot_pend["_fps"].notna() & (cot_pend["_fps"].dt.date < hoy)].copy()
                cot_pend = cot_pend.sort_values("_fps", ascending=True)
                cot_venc = cot_venc.sort_values("_fps", ascending=True)
            else:
                cot_pend = cot.iloc[0:0]
                cot_venc = cot.iloc[0:0]

            # --- RECORDATORIOS DE CITA ACTIVOS ---
            recordatorios_activos = pd.DataFrame()
            if not df_citas.empty and "Fecha_Inicio" in df_citas.columns:
                recordatorios_activos = df_citas.copy()
                recordatorios_activos["_fi"] = _to_dt(recordatorios_activos["Fecha_Inicio"])
                mins = pd.to_numeric(recordatorios_activos.get("Reminder_Minutes_Before", 0), errors="coerce").fillna(0)
                recordatorios_activos["_inicio_recordatorio"] = recordatorios_activos["_fi"] - pd.to_timedelta(mins, unit="m")
                now_ts = pd.Timestamp(datetime.now())
                estatus_ci = recordatorios_activos.get("Estatus", "").astype(str).str.lower()
                status_rem = recordatorios_activos.get("Reminder_Status", "").astype(str).str.lower()
                activos = (
                    (mins > 0)
                    & recordatorios_activos["_fi"].notna()
                    & (recordatorios_activos["_inicio_recordatorio"] <= now_ts)
                    & (recordatorios_activos["_fi"] >= now_ts)
                    & (~estatus_ci.isin(["realizada", "cancelada"]))
                    & (~status_rem.isin(["enviado", "atendido"]))
                )
                recordatorios_activos = recordatorios_activos[activos].sort_values("_fi", ascending=True)

            # Persistencia para ocultar notificaciones de cita hasta que el usuario decida volver a mostrarlas.
            dismissed_key = "organizador_notificaciones_citas_ocultas"
            if dismissed_key not in st.session_state:
                st.session_state[dismissed_key] = set()
            if "Cita_ID" in recordatorios_activos.columns:
                ids_activos = set(recordatorios_activos["Cita_ID"].astype(str))
                # Limpia IDs que ya no están activos para evitar crecer indefinidamente en sesión.
                st.session_state[dismissed_key] = {
                    cid for cid in st.session_state[dismissed_key] if cid in ids_activos
                }

                recordatorios_visibles = recordatorios_activos[
                    ~recordatorios_activos["Cita_ID"].astype(str).isin(st.session_state[dismissed_key])
                ].copy()
            else:
                recordatorios_visibles = recordatorios_activos.copy()

            # --- CHECKLIST (% cumplimiento del día) ---
            chk = df_checklist_daily.copy()
            if "Fecha" in chk.columns:
                chk["_f"] = _to_date(chk["Fecha"])
                chk_hoy = chk[chk["_f"] == hoy].copy()
                if not chk_hoy.empty and "Completado" in chk_hoy.columns:
                    total = len(chk_hoy)
                    done = (chk_hoy["Completado"].astype(str).str.lower().isin(["1","true","sí","si","x","ok","✅"])).sum()
                    pct = round((done / total) * 100, 1) if total else 0
                else:
                    total, done, pct = 0, 0, 0
            else:
                chk_hoy = chk.iloc[0:0]
                total, done, pct = 0, 0, 0

            # ===== RESUMEN (KPIs) =====
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            k1.metric("📅 Citas hoy", len(citas_hoy))
            k2.metric("✅ Pendientes hoy", len(tareas_hoy))
            k3.metric("⏰ Pendientes vencidos", len(tareas_vencidas))
            k4.metric("🔁 Seguimientos pendientes", len(seguimientos_pend))
            k5.metric("📈 Cumplimiento pendientes de hoy", f"{tareas_hoy_pct}%")
            k6.metric("💰 Cotizaciones pendientes", len(cot_pend))

            k7 = st.columns(1)[0]
            k7.metric("🧾 Checklist hoy", f"{pct}%")

            if not recordatorios_visibles.empty:
                primera_cita = recordatorios_visibles.iloc[0]
                hora_cita = primera_cita.get("Fecha_Inicio", "")
                cliente_cita = primera_cita.get("Cliente_Persona", "Sin cliente")
                tipo_cita = primera_cita.get("Tipo", "Cita")
                minutos_previos = primera_cita.get("Reminder_Minutes_Before", "")

                st.warning(
                    f"🔔 **Tienes {len(recordatorios_visibles)} recordatorio(s) de cita activo(s)**. "
                    f"Próxima: {hora_cita} · {cliente_cita} · {tipo_cita} "
                    f"(aviso {minutos_previos} min antes)."
                )
                col_notif_1, col_notif_2 = st.columns([1, 1])
                with col_notif_1:
                    if st.button("✅ Quitar estas notificaciones", key="btn_ocultar_notif_citas"):
                        if "Cita_ID" in recordatorios_visibles.columns:
                            st.session_state[dismissed_key].update(
                                recordatorios_visibles["Cita_ID"].astype(str).tolist()
                            )
                        st.rerun()
                with col_notif_2:
                    st.caption("La alerta se mantiene visible hasta que la quites manualmente.")

            st.markdown("---")

            # ===== DETALLES =====
            st.markdown("### 📅 Citas de hoy")
            def render_citas_lista(df_lista: pd.DataFrame, key_prefix: str, empty_text: str):
                if df_lista.empty:
                    st.info(empty_text)
                    return

                cols = [c for c in ["Fecha_Inicio", "Cliente_Persona", "Empresa_Clinica", "Tipo", "Prioridad", "Estatus", "Notas"] if c in df_lista.columns]
                st.dataframe(df_lista[cols], use_container_width=True)

                for i, row in df_lista.iterrows():
                    cita_id = str(row.get("Cita_ID", "")).strip() or f"{key_prefix}_{i}"
                    cliente = row.get("Cliente_Persona", "Sin cliente")
                    fecha_ini = row.get("Fecha_Inicio", "")
                    estatus_actual = str(row.get("Estatus", "Programada") or "Programada")
                    notas_actuales = str(row.get("Notas", "") or "")
                    exp_title = f"📝 {fecha_ini} · {cliente} · {estatus_actual}"

                    with st.expander(exp_title, expanded=False):
                        with st.form(f"form_cita_hoy_{key_prefix}_{cita_id}"):
                            nuevo_estatus = st.selectbox(
                                "Estatus",
                                ["Programada", "Realizada", "Reprogramada", "Cancelada"],
                                index=["Programada", "Realizada", "Reprogramada", "Cancelada"].index(estatus_actual) if estatus_actual in ["Programada", "Realizada", "Reprogramada", "Cancelada"] else 0,
                                key=f"estatus_{key_prefix}_{cita_id}",
                            )
                            atendida = st.checkbox(
                                "Marcar como atendida (cambia estatus a Realizada y se quita de estas listas)",
                                value=False,
                                key=f"atendida_{key_prefix}_{cita_id}",
                            )
                            comentario = st.text_area(
                                "Comentarios / notas de la cita",
                                value=notas_actuales,
                                height=90,
                                key=f"nota_{key_prefix}_{cita_id}",
                            )
                            guardar = st.form_submit_button("💾 Guardar cambios")

                        if guardar:
                            try:
                                updates = {
                                    "Estatus": "Realizada" if atendida else nuevo_estatus,
                                    "Notas": comentario.strip(),
                                    "Last_Updated_At": now_iso(),
                                    "Last_Updated_By": "ALEJANDRO",
                                }
                                safe_update_by_id("CITAS", "Cita_ID", row.get("Cita_ID", ""), updates)
                                st.success(f"✅ Cita actualizada: {row.get('Cita_ID', '')}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ No se pudo actualizar la cita: {e}")

            render_citas_lista(citas_hoy, "hoy", "Sin citas para hoy (pendientes por atender).")

            st.markdown("### 📚 Citas pasadas y futuras (pendientes por atender)")
            render_citas_lista(citas_otras, "otras", "No hay citas pasadas/futuras pendientes por atender.")

            if not seguimientos_pend.empty:
                st.markdown("### 🔁 Seguimientos pendientes")
                cols = [c for c in ["Fecha_Inicio","Cliente_Persona","Empresa_Clinica","Tipo","Prioridad","Estatus","Notas"] if c in seguimientos_pend.columns]
                st.dataframe(seguimientos_pend[cols], use_container_width=True)

            st.markdown("### ✅ Pendientes de hoy")
            if tareas_hoy.empty:
                st.info("Sin pendientes para hoy.")
            else:
                cols = [c for c in ["Fecha_Limite","Titulo","Prioridad","Estatus","Cliente_Relacionado","Cotizacion_Folio_Relacionado"] if c in tareas_hoy.columns]
                st.dataframe(tareas_hoy[cols], use_container_width=True)

            st.markdown("### ⏰ Pendientes vencidos")
            if tareas_vencidas.empty:
                st.info("No hay pendientes vencidos 🎉")
            else:
                cols = [c for c in ["Fecha_Limite","Titulo","Prioridad","Estatus","Cliente_Relacionado"] if c in tareas_vencidas.columns]
                st.dataframe(tareas_vencidas[cols], use_container_width=True)

                st.caption("Acciones rápidas: actualiza aquí mismo el estatus sin cambiar de sección.")
                tareas_vencidas_accion = tareas_vencidas.copy()
                tareas_vencidas_accion["_id"] = tareas_vencidas_accion.get("Tarea_ID", "").astype(str)
                tareas_vencidas_accion["_titulo"] = tareas_vencidas_accion.get("Titulo", "").astype(str)
                tareas_vencidas_accion["_estatus"] = tareas_vencidas_accion.get("Estatus", "").astype(str)

                opciones_vencidas = [o for o in tareas_vencidas_accion["_id"].tolist() if str(o).strip()]
                if opciones_vencidas:
                    def format_vencida(tid):
                        r = tareas_vencidas_accion[tareas_vencidas_accion["_id"] == tid].iloc[0]
                        return f"{tid} | {r.get('_estatus', '')} | {r.get('Fecha_Limite', '')} | {r.get('_titulo', '')}"

                    with st.form("form_accion_rapida_vencidos"):
                        tarea_sel_vencida = st.selectbox(
                            "Selecciona pendiente vencido:",
                            opciones_vencidas,
                            format_func=format_vencida,
                            key="hoy_tarea_vencida_sel",
                        )

                        col_v_a, col_v_b = st.columns(2)
                        with col_v_a:
                            enviar_completar_vencida = st.form_submit_button(
                                "✅ Marcar como COMPLETADA",
                                use_container_width=True,
                            )
                        with col_v_b:
                            enviar_reabrir_vencida = st.form_submit_button(
                                "↩️ Reabrir (PENDIENTE)",
                                use_container_width=True,
                            )

                    if enviar_completar_vencida:
                        try:
                            ok = safe_update_by_id(
                                "TAREAS",
                                id_col="Tarea_ID",
                                id_value=tarea_sel_vencida,
                                updates={
                                    "Estatus": "Completada",
                                    "Fecha_Completado": now_iso(),
                                    "Last_Updated_At": now_iso(),
                                    "Last_Updated_By": "ALEJANDRO",
                                }
                            )
                            if ok:
                                st.success("🎈 Pendiente vencido marcado como completado.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error actualizando pendiente vencido: {e}")

                    if enviar_reabrir_vencida:
                        try:
                            ok = safe_update_by_id(
                                "TAREAS",
                                id_col="Tarea_ID",
                                id_value=tarea_sel_vencida,
                                updates={
                                    "Estatus": "Pendiente",
                                    "Fecha_Completado": "",
                                    "Last_Updated_At": now_iso(),
                                    "Last_Updated_By": "ALEJANDRO",
                                }
                            )
                            if ok:
                                st.success("🎈 Pendiente vencido reabierto (Pendiente).")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error reabriendo pendiente vencido: {e}")

            st.markdown("### 💰 Cotizaciones vencidas de seguimiento")
            if cot_venc.empty:
                st.info("No hay cotizaciones vencidas de seguimiento 🎉")
            else:
                cols = [c for c in ["Folio","Fecha_Cotizacion","Cliente","Monto","Estatus","Fecha_Proximo_Seguimiento","Notas"] if c in cot_venc.columns]
                st.dataframe(cot_venc[cols], use_container_width=True)

                st.caption("Acciones rápidas: actualiza aquí mismo el estatus de la cotización.")
                cot_venc_accion = cot_venc.copy()
                cot_venc_accion["_id"] = cot_venc_accion.get("Cotizacion_ID", "").astype(str)
                cot_venc_accion["_folio"] = cot_venc_accion.get("Folio", "").astype(str)
                cot_venc_accion["_cliente"] = cot_venc_accion.get("Cliente", "").astype(str)
                cot_venc_accion["_estatus"] = cot_venc_accion.get("Estatus", "").astype(str)
                cot_venc_accion["_prox"] = cot_venc_accion.get("Fecha_Proximo_Seguimiento", "").astype(str)
                cot_venc_accion["_monto"] = cot_venc_accion.get("Monto", "").astype(str)

                opciones_cot_venc = [o for o in cot_venc_accion["_id"].tolist() if str(o).strip()]
                if opciones_cot_venc:
                    def format_cot_vencida(cid):
                        r = cot_venc_accion[cot_venc_accion["_id"] == cid].iloc[0]
                        fol = r.get("_folio", "")
                        cli = r.get("_cliente", "")
                        est = r.get("_estatus", "")
                        prox = r.get("_prox", "")
                        mon = r.get("_monto", "")
                        return f"{fol} | {cli} | {est} | seg: {prox} | ${mon}"

                    with st.form("form_accion_rapida_cot_vencidas"):
                        cot_sel_vencida = st.selectbox(
                            "Selecciona cotización vencida:",
                            opciones_cot_venc,
                            format_func=format_cot_vencida,
                            key="hoy_cot_vencida_sel",
                        )
                        estado_cierre_hoy = st.radio(
                            "Nuevo estatus de cotización:",
                            ["Cerrada – Ganada", "Cerrada – Perdida", "En seguimiento"],
                            horizontal=True,
                            key="hoy_estado_cierre_cot",
                        )

                        enviar_cierre_hoy = st.form_submit_button(
                            "🏁 Actualizar estatus de cotización",
                            use_container_width=True,
                        )

                    if enviar_cierre_hoy:
                        try:
                            updates_cot_hoy = {
                                "Estatus": estado_cierre_hoy,
                                "Last_Updated_At": now_iso(),
                                "Last_Updated_By": "ALEJANDRO",
                            }
                            if estado_cierre_hoy == "En seguimiento":
                                updates_cot_hoy["Resultado_Cierre"] = ""
                                updates_cot_hoy["Ultimo_Seguimiento_Fecha"] = date.today().strftime("%Y-%m-%d")
                            else:
                                updates_cot_hoy["Resultado_Cierre"] = "Ganada" if "Ganada" in estado_cierre_hoy else "Perdida"

                            safe_update_by_id(
                                "COTIZACIONES",
                                id_col="Cotizacion_ID",
                                id_value=cot_sel_vencida,
                                updates=updates_cot_hoy,
                            )
                            st.success(f"✅ Estatus actualizado a: {estado_cierre_hoy}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al actualizar cotización: {e}")

            st.markdown("### ⏱️ Recordatorios de citas activos")
            if recordatorios_visibles.empty:
                st.info("No hay recordatorios activos por atender en este momento.")
            else:
                cols = [c for c in ["Cita_ID","Fecha_Inicio","Cliente_Persona","Tipo","Reminder_Minutes_Before","Reminder_Status","Estatus"] if c in recordatorios_activos.columns]
                st.dataframe(recordatorios_visibles[cols], use_container_width=True)

            st.markdown("### 🧾 Checklist de hoy")
            if chk_hoy.empty:
                st.info("No hay checklist cargado para hoy (aún).")
            else:
                cols = [c for c in ["Item","Completado","Completado_At","Notas"] if c in chk_hoy.columns]
                st.dataframe(chk_hoy[cols], use_container_width=True)

            st.markdown("### 🔔 Alertas y recordatorios")
            for level, msg in build_hoy_alerts(hoy, df_citas, df_tareas, df_cot, chk_hoy, df_config):
                if level == "error":
                    st.error(f"🚨 {msg}")
                elif level == "warning":
                    st.warning(f"⚠️ {msg}")
                else:
                    st.info(f"ℹ️ {msg}")

        with sub[1]:
            st.subheader("📅 Agenda")

            with st.form("form_nueva_cita", clear_on_submit=True):
                st.markdown("### ➕ Nueva cita")

                col1, col2 = st.columns(2)
                with col1:
                    fecha = st.date_input("Fecha", value=date.today(), format="DD/MM/YYYY")
                with col2:
                    hora = st.time_input("Hora", value=datetime.now().time().replace(second=0, microsecond=0))

                col3, col4 = st.columns(2)
                with col3:
                    duracion_min = st.number_input("Duración (min)", min_value=15, max_value=480, value=60, step=15)
                with col4:
                    prioridad = st.selectbox("Prioridad", ["Alta", "Media", "Baja"], index=1)

                cliente_persona = st.text_input("Cliente / persona")
                empresa = st.text_input("Empresa o clínica (opcional)")

                tipo = st.selectbox("Tipo", ["Visita", "Llamada", "Junta", "Seguimiento"], index=2)
                estatus = st.selectbox("Estatus", ["Programada", "Realizada", "Reprogramada", "Cancelada"], index=0)
                notas = st.text_area("Notas (opcional)", height=90)

                reminder = st.number_input("Recordatorio (min antes)", min_value=0, max_value=240, value=30, step=5)

                submitted_cita = st.form_submit_button("✅ Crear cita")

            if submitted_cita:
                if not cliente_persona.strip():
                    st.error("❌ Cliente/persona es obligatorio.")
                else:
                    try:
                        start_dt = datetime.combine(fecha, hora)
                        end_dt = start_dt + timedelta(minutes=int(duracion_min))

                        cita_id = new_id("CITA")
                        payload = {
                            "Cita_ID": cita_id,
                            "Created_At": now_iso(),
                            "Created_By": "ALEJANDRO",
                            "Fecha_Inicio": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "Fecha_Fin": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "Cliente_Persona": cliente_persona.strip(),
                            "Empresa_Clinica": empresa.strip(),
                            "Tipo": tipo,
                            "Prioridad": prioridad,
                            "Estatus": estatus,
                            "Notas": notas.strip(),
                            "Lugar": "",
                            "Telefono": "",
                            "Correo": "",
                            "Reminder_Minutes_Before": str(int(reminder)),
                            "Reminder_Status": "Pendiente" if int(reminder) > 0 else "N/A",
                            "Last_Updated_At": now_iso(),
                            "Last_Updated_By": "ALEJANDRO",
                            "Is_Deleted": "0",
                        }
                        safe_append("CITAS", payload)
                        st.success(f"🎈 Cita creada: {cita_id}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error creando cita: {e}")

            with st.expander("✏️ Editar cita existente", expanded=False):
                if df_citas.empty:
                    st.info("No hay citas para editar.")
                else:
                    citas_edit = df_citas.copy()
                    citas_edit["_fi"] = _to_dt(citas_edit.get("Fecha_Inicio", ""))
                    citas_edit = citas_edit.sort_values("_fi", ascending=False, na_position="last")
                    citas_edit["_label"] = (
                        citas_edit.get("Fecha_Inicio", "").astype(str)
                        + " · "
                        + citas_edit.get("Cliente_Persona", "").astype(str)
                        + " · "
                        + citas_edit.get("Tipo", "").astype(str)
                        + " · "
                        + citas_edit.get("Estatus", "").astype(str)
                    )
                    options = citas_edit.index.tolist()
                    selected_idx = st.selectbox(
                        "Selecciona una cita",
                        options=options,
                        format_func=lambda idx: citas_edit.loc[idx, "_label"],
                        key="organizador_cita_edit_selector",
                    )
                    cita_sel = citas_edit.loc[selected_idx]
                    start_dt = pd.to_datetime(cita_sel.get("Fecha_Inicio", ""), errors="coerce")
                    end_dt = pd.to_datetime(cita_sel.get("Fecha_Fin", ""), errors="coerce")
                    if pd.isna(start_dt):
                        start_dt = datetime.now().replace(second=0, microsecond=0)
                    if pd.isna(end_dt) or end_dt <= start_dt:
                        end_dt = start_dt + timedelta(minutes=60)
                    duracion_default = int(max(15, min(480, (end_dt - start_dt).total_seconds() // 60)))

                    with st.form("form_editar_cita"):
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            fecha_edit = st.date_input("Fecha", value=start_dt.date(), format="DD/MM/YYYY")
                        with col2:
                            hora_edit = st.time_input("Hora", value=start_dt.time())
                        with col3:
                            duracion_edit = st.number_input("Duración (min)", min_value=15, max_value=480, value=duracion_default, step=15)
                        with col4:
                            prioridad_edit = st.selectbox("Prioridad", ["Alta", "Media", "Baja"], index=["Alta", "Media", "Baja"].index(str(cita_sel.get("Prioridad", "Media"))) if str(cita_sel.get("Prioridad", "Media")) in ["Alta", "Media", "Baja"] else 1)

                        cliente_edit = st.text_input("Cliente / persona", value=str(cita_sel.get("Cliente_Persona", "")))
                        empresa_edit = st.text_input("Empresa o clínica", value=str(cita_sel.get("Empresa_Clinica", "")))

                        col5, col6 = st.columns(2)
                        with col5:
                            tipo_edit = st.selectbox("Tipo", ["Visita", "Llamada", "Junta", "Seguimiento"], index=["Visita", "Llamada", "Junta", "Seguimiento"].index(str(cita_sel.get("Tipo", "Seguimiento"))) if str(cita_sel.get("Tipo", "Seguimiento")) in ["Visita", "Llamada", "Junta", "Seguimiento"] else 3)
                        with col6:
                            estatus_edit = st.selectbox("Estatus", ["Programada", "Realizada", "Reprogramada", "Cancelada"], index=["Programada", "Realizada", "Reprogramada", "Cancelada"].index(str(cita_sel.get("Estatus", "Programada"))) if str(cita_sel.get("Estatus", "Programada")) in ["Programada", "Realizada", "Reprogramada", "Cancelada"] else 0)

                        notas_edit = st.text_area("Notas", value=str(cita_sel.get("Notas", "")), height=90)
                        col7, col8, col9 = st.columns(3)
                        with col7:
                            lugar_edit = st.text_input("Lugar", value=str(cita_sel.get("Lugar", "")))
                        with col8:
                            telefono_edit = st.text_input("Teléfono", value=str(cita_sel.get("Telefono", "")))
                        with col9:
                            correo_edit = st.text_input("Correo", value=str(cita_sel.get("Correo", "")))

                        reminder_actual = pd.to_numeric(pd.Series([cita_sel.get("Reminder_Minutes_Before", 30)]), errors="coerce").fillna(30).iloc[0]
                        reminder_edit = st.number_input("Recordatorio (min antes)", min_value=0, max_value=240, value=int(reminder_actual), step=5)

                        guardar_edit = st.form_submit_button("💾 Guardar edición completa")

                    if guardar_edit:
                        if not cliente_edit.strip():
                            st.error("❌ Cliente/persona es obligatorio.")
                        else:
                            try:
                                start_edit = datetime.combine(fecha_edit, hora_edit)
                                end_edit = start_edit + timedelta(minutes=int(duracion_edit))
                                updates_edit = {
                                    "Fecha_Inicio": start_edit.strftime("%Y-%m-%d %H:%M:%S"),
                                    "Fecha_Fin": end_edit.strftime("%Y-%m-%d %H:%M:%S"),
                                    "Cliente_Persona": cliente_edit.strip(),
                                    "Empresa_Clinica": empresa_edit.strip(),
                                    "Tipo": tipo_edit,
                                    "Prioridad": prioridad_edit,
                                    "Estatus": estatus_edit,
                                    "Notas": notas_edit.strip(),
                                    "Lugar": lugar_edit.strip(),
                                    "Telefono": telefono_edit.strip(),
                                    "Correo": correo_edit.strip(),
                                    "Reminder_Minutes_Before": str(int(reminder_edit)),
                                    "Reminder_Status": "Pendiente" if int(reminder_edit) > 0 else "N/A",
                                    "Last_Updated_At": now_iso(),
                                    "Last_Updated_By": "ALEJANDRO",
                                }
                                safe_update_by_id("CITAS", "Cita_ID", cita_sel.get("Cita_ID", ""), updates_edit)
                                st.success(f"✅ Cita actualizada: {cita_sel.get('Cita_ID', '')}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ No se pudo editar la cita: {e}")

            st.markdown("### 📋 Agenda")
            agenda_view = df_citas.copy()
            if "Fecha_Inicio" in agenda_view.columns:
                agenda_view["_fi"] = _to_dt(agenda_view["Fecha_Inicio"])
                agenda_hoy = agenda_view[agenda_view["_fi"].dt.date == date.today()].copy()
                fin_semana = date.today() + timedelta(days=7)
                agenda_semana = agenda_view[
                    agenda_view["_fi"].dt.date.between(date.today(), fin_semana)
                ].copy()
                agenda_hoy = agenda_hoy.sort_values("_fi", ascending=True)
                agenda_semana = agenda_semana.sort_values("_fi", ascending=True)
            else:
                agenda_hoy = agenda_view.iloc[0:0]
                agenda_semana = agenda_view.iloc[0:0]

            tab_agenda_hoy, tab_agenda_semana, tab_agenda_todo = st.tabs(["📌 Hoy", "🗓️ Semana", "📚 Todo"])
            with tab_agenda_hoy:
                if agenda_hoy.empty:
                    st.info("Sin citas para hoy.")
                else:
                    cols = [c for c in ["Fecha_Inicio","Cliente_Persona","Empresa_Clinica","Tipo","Prioridad","Estatus","Reminder_Minutes_Before","Reminder_Status"] if c in agenda_hoy.columns]
                    st.dataframe(agenda_hoy[cols], use_container_width=True)
            with tab_agenda_semana:
                if agenda_semana.empty:
                    st.info("Sin citas para los próximos 7 días.")
                else:
                    cols = [c for c in ["Fecha_Inicio","Cliente_Persona","Empresa_Clinica","Tipo","Prioridad","Estatus","Reminder_Minutes_Before","Reminder_Status"] if c in agenda_semana.columns]
                    st.dataframe(agenda_semana[cols], use_container_width=True)
            with tab_agenda_todo:
                st.dataframe(df_citas, use_container_width=True)

        with sub[2]:
            st.subheader("✅ Pendientes")

            # ===== Alta rápida =====
            with st.form("form_nueva_tarea", clear_on_submit=True):
                st.markdown("### ➕ Nuevo pendiente")
                titulo = st.text_input("Título", placeholder="Ej. Llamar a cliente X")
                descripcion = st.text_area("Descripción", placeholder="Detalles…", height=90)

                col1, col2, col3 = st.columns(3)
                with col1:
                    fecha_limite = st.date_input("Fecha límite", value=date.today(), format="DD/MM/YYYY")
                with col2:
                    prioridad = st.selectbox("Prioridad", ["Alta", "Media", "Baja"], index=1)
                with col3:
                    estatus = st.selectbox("Estatus", ["Pendiente", "Completada"], index=0)

                col4, col5 = st.columns(2)
                with col4:
                    cliente_rel = st.text_input("Cliente relacionado (opcional)")
                with col5:
                    folio_cot = st.text_input("Folio cotización (opcional)")

                submitted = st.form_submit_button("✅ Crear pendiente")

            if submitted:
                if not titulo.strip():
                    st.error("❌ El título es obligatorio.")
                else:
                    try:
                        tarea_id = new_id("TAREA")
                        payload = {
                            "Tarea_ID": tarea_id,
                            "Created_At": now_iso(),
                            "Created_By": "ALEJANDRO",
                            "Titulo": titulo.strip(),
                            "Descripcion": descripcion.strip(),
                            "Fecha_Limite": fecha_limite.strftime("%Y-%m-%d"),
                            "Prioridad": prioridad,
                            "Estatus": estatus,
                            "Cliente_Relacionado": cliente_rel.strip(),
                            "Cotizacion_Folio_Relacionado": folio_cot.strip(),
                            "Tipo": "Pendiente",
                            "Fecha_Completado": now_iso() if estatus.lower() == "completada" else "",
                            "Notas_Resultado": "",
                            "Last_Updated_At": now_iso(),
                            "Last_Updated_By": "ALEJANDRO",
                            "Is_Deleted": "0",
                        }
                        safe_append("TAREAS", payload)
                        st.success(f"🎈 Pendiente creado: {tarea_id}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error creando pendiente: {e}")

            st.markdown("---")
            st.markdown("### ✅ Completar / Reabrir pendiente")

            # Normaliza columnas básicas por si vienen vacías
            df_tareas["_id"] = df_tareas.get("Tarea_ID", "").astype(str)
            df_tareas["_titulo"] = df_tareas.get("Titulo", "").astype(str)
            df_tareas["_estatus"] = df_tareas.get("Estatus", "").astype(str)
            df_tareas["_fecha_limite"] = df_tareas.get("Fecha_Limite", "").astype(str)

            # Opcional: filtrar no borradas
            if "Is_Deleted" in df_tareas.columns:
                df_tareas = df_tareas[df_tareas["Is_Deleted"].astype(str).fillna("") != "1"]

            # Lista ordenada: pendientes primero, luego por fecha límite
            try:
                df_tareas["_fecha_dt"] = pd.to_datetime(df_tareas["_fecha_limite"], errors="coerce")
            except Exception:
                df_tareas["_fecha_dt"] = pd.NaT

            df_tareas_view = df_tareas.copy()
            df_tareas_view["_pend"] = df_tareas_view["_estatus"].str.lower().ne("completada")
            df_tareas_view = df_tareas_view.sort_values(by=["_pend", "_fecha_dt"], ascending=[False, True])

            opciones = [o for o in df_tareas_view["_id"].tolist() if str(o).strip()]

            def format_tarea(tid):
                r = df_tareas_view[df_tareas_view["_id"] == tid].iloc[0]
                est = r.get("_estatus", "")
                fec = r.get("_fecha_limite", "")
                tit = r.get("_titulo", "")
                return f"{tid} | {est} | {fec} | {tit}"

            if not opciones:
                st.info("No hay pendientes disponibles para actualizar.")
            else:
                with st.form("form_actualizar_pendiente"):
                    tarea_sel = st.selectbox(
                        "Selecciona un pendiente:",
                        opciones,
                        format_func=format_tarea,
                        key="tarea_sel_update",
                    )

                    colA, colB = st.columns(2)
                    with colA:
                        enviar_completar = st.form_submit_button(
                            "✅ Marcar como COMPLETADA",
                            use_container_width=True,
                        )
                    with colB:
                        enviar_reabrir = st.form_submit_button(
                            "↩️ Reabrir (PENDIENTE)",
                            use_container_width=True,
                        )

                if enviar_completar:
                    try:
                        ok = safe_update_by_id(
                            "TAREAS",
                            id_col="Tarea_ID",
                            id_value=tarea_sel,
                            updates={
                                "Estatus": "Completada",
                                "Fecha_Completado": now_iso(),
                                "Last_Updated_At": now_iso(),
                                "Last_Updated_By": "ALEJANDRO",
                            }
                        )
                        if ok:
                            st.success("🎈 Pendiente marcado como completado.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error actualizando pendiente: {e}")

                if enviar_reabrir:
                    try:
                        ok = safe_update_by_id(
                            "TAREAS",
                            id_col="Tarea_ID",
                            id_value=tarea_sel,
                            updates={
                                "Estatus": "Pendiente",
                                "Fecha_Completado": "",
                                "Last_Updated_At": now_iso(),
                                "Last_Updated_By": "ALEJANDRO",
                            }
                        )
                        if ok:
                            st.success("🎈 Pendiente reabierto (Pendiente).")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error reabriendo pendiente: {e}")

            st.markdown("### 📋 Lista")
            tareas_lista = df_tareas.copy()
            if "Fecha_Limite" in tareas_lista.columns:
                tareas_lista["_fl"] = _to_dt(tareas_lista["Fecha_Limite"])
                hoy_t = date.today()
                estado_t = tareas_lista.get("Estatus", "").astype(str).str.lower()
                tareas_hoy_tab = tareas_lista[tareas_lista["_fl"].dt.date == hoy_t].copy()
                tareas_vencidas_tab = tareas_lista[(tareas_lista["_fl"].dt.date < hoy_t) & (estado_t != "completada")].copy()
                tareas_proximas_tab = tareas_lista[(tareas_lista["_fl"].dt.date > hoy_t) & (estado_t != "completada")].copy()
                tareas_hoy_tab = tareas_hoy_tab.sort_values("_fl", ascending=True)
                tareas_vencidas_tab = tareas_vencidas_tab.sort_values("_fl", ascending=True)
                tareas_proximas_tab = tareas_proximas_tab.sort_values("_fl", ascending=True)
            else:
                tareas_hoy_tab = tareas_lista.iloc[0:0]
                tareas_vencidas_tab = tareas_lista.iloc[0:0]
                tareas_proximas_tab = tareas_lista.iloc[0:0]

            tab_t_hoy, tab_t_venc, tab_t_prox, tab_t_todo = st.tabs(["📌 Hoy", "⚠️ Vencidas", "⏭️ Próximas", "📚 Todo"])
            with tab_t_hoy:
                st.dataframe(tareas_hoy_tab, use_container_width=True)
            with tab_t_venc:
                st.dataframe(tareas_vencidas_tab, use_container_width=True)
            with tab_t_prox:
                st.dataframe(tareas_proximas_tab, use_container_width=True)
            with tab_t_todo:
                st.dataframe(df_tareas, use_container_width=True)

        with sub[3]:
            st.subheader("💰 Cotizaciones")

            with st.form("form_nueva_cot", clear_on_submit=True):
                st.markdown("### ➕ Nueva cotización")
                folio = st.text_input("Folio")
                fecha_cot = st.date_input("Fecha", value=date.today(), format="DD/MM/YYYY")
                cliente = st.text_input("Cliente")
                monto = st.number_input("Monto", min_value=0.0, value=0.0, step=100.0)
                vendedor = st.text_input("Vendedor (opcional)", placeholder="Alejandro")

                estatus = st.selectbox(
                    "Estatus",
                    ["Enviada", "En seguimiento", "Cerrada – Ganada", "Cerrada – Perdida"],
                    index=0
                )

                prox_seg = st.date_input("Próximo seguimiento", value=date.today(), format="DD/MM/YYYY")
                notas = st.text_area("Notas (opcional)", height=90)

                submitted_cot = st.form_submit_button("✅ Crear cotización")

            if submitted_cot:
                if not folio.strip() or not cliente.strip():
                    st.error("❌ Folio y Cliente son obligatorios.")
                else:
                    try:
                        cot_id = new_id("COT")
                        payload = {
                            "Cotizacion_ID": cot_id,
                            "Folio": folio.strip(),
                            "Created_At": now_iso(),
                            "Created_By": "ALEJANDRO",
                            "Fecha_Cotizacion": fecha_cot.strftime("%Y-%m-%d"),
                            "Cliente": cliente.strip(),
                            "Monto": float(monto),
                            "Vendedor": vendedor.strip(),
                            "Estatus": estatus,
                            "Fecha_Proximo_Seguimiento": prox_seg.strftime("%Y-%m-%d"),
                            "Ultimo_Seguimiento_Fecha": "",
                            "Dias_Sin_Seguimiento": "",
                            "Notas": notas.strip(),
                            "Resultado_Cierre": "",
                            "Convertida_A_Tarea_ID": "",
                            "Convertida_A_Cita_ID": "",
                            "Last_Updated_At": now_iso(),
                            "Last_Updated_By": "ALEJANDRO",
                            "Is_Deleted": "0",
                        }
                        safe_append("COTIZACIONES", payload)
                        st.success(f"🎈 Cotización creada: {cot_id}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error creando cotización: {e}")

            st.markdown("---")

            # Normalizar DF por si viene vacío o con tipos raros
            df_cot["_id"] = df_cot.get("Cotizacion_ID", "").astype(str)
            df_cot["_folio"] = df_cot.get("Folio", "").astype(str)
            df_cot["_cliente"] = df_cot.get("Cliente", "").astype(str)
            df_cot["_monto"] = df_cot.get("Monto", "").astype(str)
            df_cot["_estatus"] = df_cot.get("Estatus", "").astype(str)
            df_cot["_prox"] = df_cot.get("Fecha_Proximo_Seguimiento", "").astype(str)
            df_cot["_notas"] = df_cot.get("Notas", "").astype(str)

            # Opcional: filtrar no borradas
            if "Is_Deleted" in df_cot.columns:
                df_cot = df_cot[df_cot["Is_Deleted"].astype(str).fillna("") != "1"]

            # Ordenar: vencidas primero (prox_seg < hoy), luego por fecha de seguimiento
            try:
                df_cot["_prox_dt"] = pd.to_datetime(df_cot["_prox"], errors="coerce")
            except Exception:
                df_cot["_prox_dt"] = pd.NaT

            hoy_dt = pd.to_datetime(date.today())
            df_cot_view = df_cot.copy()
            df_cot_view["Convertida_A_Tarea_ID"] = df_cot.get("Convertida_A_Tarea_ID", "")
            df_cot_view["Convertida_A_Cita_ID"] = df_cot.get("Convertida_A_Cita_ID", "")
            df_cot_view["_vencida"] = df_cot_view["_prox_dt"].notna() & (df_cot_view["_prox_dt"] < hoy_dt)
            df_cot_view = df_cot_view.sort_values(by=["_vencida", "_prox_dt"], ascending=[False, True])

            opciones_cot = df_cot_view["_id"].tolist()

            def format_cot(cid):
                r = df_cot_view[df_cot_view["_id"] == cid].iloc[0]
                fol = r.get("_folio", "")
                cli = r.get("_cliente", "")
                est = r.get("_estatus", "")
                prox = r.get("_prox", "")
                mon = r.get("_monto", "")
                return f"{fol} | {cli} | {est} | seg: {prox} | ${mon}"

            def _normaliza_estatus_cotizacion(valor: str) -> str:
                txt = unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode("ascii")
                txt = re.sub(r"[^a-zA-Z0-9]+", " ", txt.lower()).strip()
                return txt

            with st.expander("🏁 Cerrar cotizaciones", expanded=True):
                estatus_normalizados = df_cot_view["_estatus"].apply(_normaliza_estatus_cotizacion)
                estatus_cerrados = {
                    "cerrada ganada",
                    "cerrada perdida",
                    "cerrada ganado",
                    "cerrada perdido",
                }
                df_cot_cerrables = df_cot_view[~estatus_normalizados.isin(estatus_cerrados)].copy()
                opciones_cierre = df_cot_cerrables["_id"].tolist()

                if not opciones_cierre:
                    st.info("No hay cotizaciones disponibles para cerrar.")
                else:
                    with st.form("form_cerrar_cotizacion"):
                        cot_sel_cierre = st.selectbox(
                            "Selecciona una cotización para cerrar:",
                            opciones_cierre,
                            format_func=format_cot,
                            key="cot_sel_cierre"
                        )
                        estado_cierre = st.radio(
                            "Nuevo estatus de cotización:",
                            ["Cerrada – Ganada", "Cerrada – Perdida", "En seguimiento"],
                            horizontal=True,
                            key="estado_cierre_cot"
                        )

                        enviar_cierre = st.form_submit_button(
                            "🏁 Actualizar estatus de cotización",
                            use_container_width=True,
                        )

                    if enviar_cierre:
                        try:
                            updates_cot = {
                                "Estatus": estado_cierre,
                                "Last_Updated_At": now_iso(),
                                "Last_Updated_By": "ALEJANDRO",
                            }
                            if estado_cierre == "En seguimiento":
                                updates_cot["Resultado_Cierre"] = ""
                                updates_cot["Ultimo_Seguimiento_Fecha"] = date.today().strftime("%Y-%m-%d")
                            else:
                                updates_cot["Resultado_Cierre"] = "Ganada" if "Ganada" in estado_cierre else "Perdida"

                            safe_update_by_id(
                                "COTIZACIONES",
                                id_col="Cotizacion_ID",
                                id_value=cot_sel_cierre,
                                updates=updates_cot,
                            )
                            st.success(f"✅ Estatus actualizado a: {estado_cierre}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al actualizar cotización: {e}")

            with st.expander("🔁 Convertir cotización a: Pendiente o Cita", expanded=False):
                if not opciones_cot:
                    st.info("No hay cotizaciones disponibles para convertir.")
                else:
                    cot_sel = st.selectbox(
                        "Selecciona una cotización:",
                        opciones_cot,
                        format_func=format_cot,
                        key="cot_sel_convert"
                    )

                    if cot_sel:
                        row_cot = df_cot_view[df_cot_view["_id"] == cot_sel].iloc[0]
                        folio = str(row_cot.get("_folio", "")).strip()
                        cliente = str(row_cot.get("_cliente", "")).strip()
                        estatus = str(row_cot.get("_estatus", "")).strip()
                        prox = str(row_cot.get("_prox", "")).strip()
                        notas = str(row_cot.get("_notas", "")).strip()

                        # --- Anti-duplicados: si ya se convirtió, deshabilitar botones ---
                        tarea_link = str(row_cot.get("Convertida_A_Tarea_ID", "") or "").strip()
                        cita_link = str(row_cot.get("Convertida_A_Cita_ID", "") or "").strip()

                        ya_tarea = bool(tarea_link)
                        ya_cita = bool(cita_link)

                        if ya_tarea:
                            st.info(f"🧩 Esta cotización ya fue convertida a **Pendiente**: {tarea_link}")
                        if ya_cita:
                            st.info(f"📅 Esta cotización ya fue convertida a **Cita**: {cita_link}")

                        st.markdown("#### ➜ Tipo de conversión")
                        tipo_conv = st.radio(
                            "¿Qué quieres crear?",
                            ["🧩 Pendiente", "📅 Cita"],
                            horizontal=True,
                            key="tipo_conversion_cot"
                        )

                        if tipo_conv == "🧩 Pendiente":
                            colA, colB = st.columns([2, 1])

                            with colA:
                                titulo_sugerido = st.text_input(
                                    "Título del pendiente (editable):",
                                    value=f"Seguimiento cotización {folio} - {cliente}",
                                    key="titulo_tarea_desde_cot"
                                )
                                desc_sugerida = st.text_area(
                                    "Descripción (opcional):",
                                    value=(f"Estatus cotización: {estatus}\n"
                                           f"Próx. seguimiento: {prox}\n"
                                           f"Notas: {notas}").strip(),
                                    height=90,
                                    key="desc_tarea_desde_cot"
                                )

                            with colB:
                                # Fecha límite por defecto = fecha de próximo seguimiento, si es válida; si no, hoy
                                try:
                                    prox_dt = pd.to_datetime(prox, errors="coerce")
                                    fecha_limite_default = prox_dt.date() if pd.notna(prox_dt) else date.today()
                                except Exception:
                                    fecha_limite_default = date.today()

                                fecha_limite = st.date_input(
                                    "Fecha límite",
                                    value=fecha_limite_default,
                                    format="DD/MM/YYYY",
                                    key="fecha_limite_tarea_desde_cot"
                                )
                                prioridad = st.selectbox(
                                    "Prioridad",
                                    ["Alta", "Media", "Baja"],
                                    index=1,
                                    key="prioridad_tarea_desde_cot"
                                )
                            if st.button(
                                "🧩 Convertir a PENDIENTE",
                                key="btn_convertir_a_tarea",
                                use_container_width=True,
                                disabled=ya_tarea,
                            ):
                                if not titulo_sugerido.strip():
                                    st.error("❌ El título del pendiente no puede ir vacío.")
                                else:
                                    try:
                                        tarea_id = new_id("TAREA")
                                        payload = {
                                            "Tarea_ID": tarea_id,
                                            "Created_At": now_iso(),
                                            "Created_By": "ALEJANDRO",
                                            "Titulo": titulo_sugerido.strip(),
                                            "Descripcion": desc_sugerida.strip(),
                                            "Fecha_Limite": fecha_limite.strftime("%Y-%m-%d"),
                                            "Prioridad": prioridad,
                                            "Estatus": "Pendiente",
                                            "Cliente_Relacionado": cliente,
                                            "Cotizacion_Folio_Relacionado": folio,
                                            "Tipo": "Seguimiento Cotización",
                                            "Fecha_Completado": "",
                                            "Notas_Resultado": "",
                                            "Last_Updated_At": now_iso(),
                                            "Last_Updated_By": "ALEJANDRO",
                                            "Is_Deleted": "0",
                                        }
                                        safe_append("TAREAS", payload)

                                        # Guardar vínculo en cotización (si existe columna)
                                        try:
                                            safe_update_by_id(
                                                "COTIZACIONES",
                                                id_col="Cotizacion_ID",
                                                id_value=cot_sel,
                                                updates={
                                                    "Convertida_A_Tarea_ID": tarea_id,
                                                    "Last_Updated_At": now_iso(),
                                                    "Last_Updated_By": "ALEJANDRO",
                                                }
                                            )
                                        except Exception:
                                            pass

                                        st.success(f"🎈 Pendiente creado desde cotización: {tarea_id}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error al convertir a pendiente: {e}")

                        else:
                            st.markdown("#### 📅 Configurar cita")
                            colx, coly = st.columns(2)
                            with colx:
                                fecha_cita = st.date_input("Fecha cita", value=date.today(), format="DD/MM/YYYY", key="fecha_cita_desde_cot")
                            with coly:
                                hora_cita = st.time_input(
                                    "Hora cita",
                                    value=datetime.now().time().replace(second=0, microsecond=0),
                                    key="hora_cita_desde_cot"
                                )

                            colx2, coly2 = st.columns(2)
                            with colx2:
                                duracion_min = st.number_input(
                                    "Duración (min)",
                                    min_value=15,
                                    max_value=480,
                                    value=30,
                                    step=15,
                                    key="dur_cita_desde_cot"
                                )
                            with coly2:
                                prioridad_cita = st.selectbox(
                                    "Prioridad",
                                    ["Alta", "Media", "Baja"],
                                    index=1,
                                    key="prioridad_cita_desde_cot"
                                )

                            tipo_cita = st.selectbox(
                                "Tipo de cita",
                                ["Seguimiento", "Llamada", "Visita", "Presentación", "Otro"],
                                index=0,
                                key="tipo_cita_desde_cot"
                            )

                            estatus_cita = st.selectbox(
                                "Estatus inicial",
                                ["Pendiente", "Programada", "Confirmada"],
                                index=0,
                                key="estatus_cita_desde_cot"
                            )

                            reminder_cita = st.number_input(
                                "Recordatorio (min antes)",
                                min_value=0,
                                max_value=1440,
                                value=30,
                                step=5,
                                key="reminder_cita_desde_cot"
                            )

                            notas_cita = st.text_area(
                                "Notas de cita (opcional)",
                                value=(f"Seguimiento de cotización {folio}\n"
                                       f"Cliente: {cliente}\n"
                                       f"Estatus actual: {estatus}\n"
                                       f"Notas cotización: {notas}").strip(),
                                height=90,
                                key="notas_cita_desde_cot"
                            )

                            if st.button(
                                "📅 Convertir a CITA",
                                key="btn_convertir_a_cita",
                                use_container_width=True,
                                disabled=ya_cita,
                            ):
                                try:
                                    start_dt = datetime.combine(fecha_cita, hora_cita)
                                    end_dt = start_dt + timedelta(minutes=int(duracion_min))

                                    cita_id = new_id("CITA")
                                    payload_cita = {
                                        "Cita_ID": cita_id,
                                        "Created_At": now_iso(),
                                        "Created_By": "ALEJANDRO",
                                        "Fecha_Inicio": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                        "Fecha_Fin": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                        "Cliente_Persona": cliente,
                                        "Empresa_Clinica": "",
                                        "Tipo": tipo_cita,
                                        "Prioridad": prioridad_cita,
                                        "Estatus": estatus_cita,
                                        "Notas": notas_cita.strip(),
                                        "Lugar": "",
                                        "Telefono": "",
                                        "Correo": "",
                                        "Reminder_Minutes_Before": str(int(reminder_cita)),
                                        "Reminder_Status": "Pendiente" if int(reminder_cita) > 0 else "N/A",
                                        "Last_Updated_At": now_iso(),
                                        "Last_Updated_By": "ALEJANDRO",
                                        "Is_Deleted": "0",
                                    }
                                    safe_append("CITAS", payload_cita)

                                    # Guardar vínculo en cotización (si existe columna)
                                    try:
                                        safe_update_by_id(
                                            "COTIZACIONES",
                                            id_col="Cotizacion_ID",
                                            id_value=cot_sel,
                                            updates={
                                                "Convertida_A_Cita_ID": cita_id,
                                                "Last_Updated_At": now_iso(),
                                                "Last_Updated_By": "ALEJANDRO",
                                            }
                                        )
                                    except Exception:
                                        pass

                                    st.success(f"🎈 Cita creada desde cotización: {cita_id}")
                                    st.rerun()

                                except Exception as e:
                                    st.error(f"❌ Error al convertir a cita: {e}")

            st.markdown("### 📋 Lista")
            cot_dash = df_cot.copy()
            if "Fecha_Proximo_Seguimiento" in cot_dash.columns:
                cot_dash["_fps"] = _to_dt(cot_dash["Fecha_Proximo_Seguimiento"])
            else:
                cot_dash["_fps"] = pd.NaT
            estatus_cot = cot_dash.get("Estatus", "").astype(str).str.lower()
            cot_pend_tab = cot_dash[~estatus_cot.str.contains("ganada|perdida", na=False)].copy()
            cot_seg_tab = cot_dash[estatus_cot.str.contains("en seguimiento", na=False)].copy()
            cot_venc_tab = cot_pend_tab[cot_pend_tab["_fps"].notna() & (cot_pend_tab["_fps"].dt.date < date.today())].copy()
            pipeline_monto = pd.to_numeric(cot_pend_tab.get("Monto", 0), errors="coerce").fillna(0).sum()

            d1, d2, d3 = st.columns(3)
            d1.metric("Pendientes", len(cot_pend_tab))
            d2.metric("Vencidas de seguimiento", len(cot_venc_tab))
            d3.metric("Pipeline monto", f"${pipeline_monto:,.2f}")

            tab_c_pend, tab_c_venc, tab_c_seg, tab_c_todo = st.tabs(["📌 Pendientes", "⚠️ Vencidas", "🔁 En seguimiento", "📚 Todo"])
            with tab_c_pend:
                st.dataframe(cot_pend_tab, use_container_width=True)
            with tab_c_venc:
                st.dataframe(cot_venc_tab, use_container_width=True)
            with tab_c_seg:
                st.dataframe(cot_seg_tab, use_container_width=True)
            with tab_c_todo:
                st.dataframe(df_cot, use_container_width=True)

        with sub[4]:
            st.subheader("🧾 Checklist")
            hoy = date.today()

            default_orden = 1
            if not df_checklist_template.empty and "Orden" in df_checklist_template.columns:
                ordenes_actuales = pd.to_numeric(df_checklist_template["Orden"], errors="coerce").dropna()
                if not ordenes_actuales.empty:
                    default_orden = int(min(999, ordenes_actuales.max() + 1))

            with st.expander("➕ Agregar ítem a plantilla recurrente", expanded=False):
                with st.form("form_add_check_template", clear_on_submit=True):
                    item_txt = st.text_input("Ítem")
                    orden = st.number_input("Orden", min_value=1, max_value=999, value=default_orden, step=1)
                    activo = st.checkbox("Activo", value=True)
                    submitted_item = st.form_submit_button("Guardar ítem")

                if submitted_item:
                    if not item_txt.strip():
                        st.error("❌ El ítem no puede ir vacío.")
                    else:
                        payload = {
                            "Item_ID": new_id("CHK"),
                            "Orden": str(int(orden)),
                            "Item": item_txt.strip(),
                            "Activo": "1" if activo else "0",
                        }
                        safe_append("CHECKLIST_TEMPLATE", payload)
                        # Fuerza re-sincronización del checklist diario tras cambios en plantilla.
                        st.session_state[f"chk_sync_{hoy.isoformat()}"] = False
                        st.success("✅ Ítem agregado a la plantilla.")
                        st.rerun()

            st.markdown("### Plantilla recurrente")
            st.dataframe(df_checklist_template, use_container_width=True)

            st.markdown("### Checklist de hoy")
            chk_hoy_edit = df_checklist_daily.copy()
            if "Fecha" in chk_hoy_edit.columns:
                chk_hoy_edit["_f"] = _to_date(chk_hoy_edit["Fecha"])
                chk_hoy_edit = chk_hoy_edit[chk_hoy_edit["_f"] == hoy].copy()

            done_chk = 0
            if not chk_hoy_edit.empty and "Completado" in chk_hoy_edit.columns:
                for _, row in chk_hoy_edit.iterrows():
                    item_id = _safe_str(row.get("Item_ID", ""))
                    item = _safe_str(row.get("Item", "(sin item)"))
                    comp = _to_bool(row.get("Completado", "0"))
                    item_key = f"{item_id or 'sinid'}_{item}".lower().replace(" ", "_")
                    chk_key = f"chk_done_{item_key}"
                    if _to_bool(st.session_state.get(chk_key, comp)):
                        done_chk += 1
            total_chk = len(chk_hoy_edit)
            pct_chk = (done_chk / total_chk * 100) if total_chk else 0
            st.metric("✅ Cumplimiento hoy", f"{pct_chk:.0f}%")

            if chk_hoy_edit.empty:
                st.info("No hay checklist para hoy. Entra a 'Hoy' para sincronizar con plantilla.")
            else:
                today_iso = hoy.strftime("%Y-%m-%d")
                lk_key = f"chk_row_lookup_{today_iso}"
                if lk_key not in st.session_state:
                    st.session_state[lk_key] = get_checklist_daily_row_lookup(today_iso)
                row_lookup = st.session_state[lk_key]

                hdr_key = "chk_headers_CHECKLIST_DAILY"
                if hdr_key not in st.session_state:
                    sheet_daily = get_alejandro_worksheet("CHECKLIST_DAILY")
                    st.session_state[hdr_key] = [h.strip() for h in sheet_daily.row_values(1)]
                checklist_headers = st.session_state[hdr_key]
                if "Orden" in df_checklist_template.columns and "Item_ID" in chk_hoy_edit.columns:
                    order_map = {
                        _safe_str(r.get("Item_ID", "")): pd.to_numeric(r.get("Orden", None), errors="coerce")
                        for _, r in df_checklist_template.iterrows()
                    }
                    chk_hoy_edit["_orden"] = chk_hoy_edit["Item_ID"].map(order_map)
                    chk_hoy_edit = chk_hoy_edit.sort_values("_orden", ascending=True, na_position="last")

                for _, row in chk_hoy_edit.iterrows():
                    item_id = _safe_str(row.get("Item_ID", ""))
                    item = _safe_str(row.get("Item", "(sin item)"))
                    comp = _to_bool(row.get("Completado", "0"))
                    notas_actuales = _safe_str(row.get("Notas", ""))

                    item_key = f"{item_id or 'sinid'}_{item}".lower().replace(" ", "_")
                    c1, c2, c3 = st.columns([4, 1.5, 2])
                    with c1:
                        nuevo_comp = st.checkbox(item, value=comp, key=f"chk_done_{item_key}")
                    with c2:
                        nuevo_notas = st.text_input("Notas", value=notas_actuales, key=f"chk_note_{item_key}")
                    with c3:
                        guardar_chk = st.button("💾 Guardar", key=f"chk_save_{item_key}")

                    if guardar_chk:
                        try:
                            row_number = None
                            if item_id:
                                row_number = row_lookup.get((hoy.strftime("%Y-%m-%d"), item_id, ""))
                            if row_number is None:
                                row_number = row_lookup.get((hoy.strftime("%Y-%m-%d"), "", item.lower()))

                            update_checklist_daily_item(
                                fecha_iso=today_iso,
                                item_id=item_id,
                                item=item,
                                completado=nuevo_comp,
                                notas=nuevo_notas,
                                row_number=row_number,
                                headers=checklist_headers,
                            )
                            # refrescar lookup cache tras cambios
                            st.session_state[lk_key] = get_checklist_daily_row_lookup(today_iso)
                            st.success(f"✅ Actualizado: {item}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ No se pudo actualizar '{item}': {e}")

            st.markdown("---")
            st.markdown("### 🗑️ Eliminar ítem")
            st.caption("Este borrado elimina el ítem en todas las fechas.")
            if not df_checklist_template.empty:
                template_opts = []
                for _, r in df_checklist_template.iterrows():
                    item_id_opt = _safe_str(r.get("Item_ID", ""))
                    item_txt_opt = _safe_str(r.get("Item", "(sin nombre)"))
                    label = f"{item_txt_opt} · {item_id_opt or 'SIN_ID'}"
                    template_opts.append((label, item_id_opt, item_txt_opt))

                with st.form("form_eliminar_items_checklist"):
                    selected_template_labels = st.multiselect(
                        "Selecciona uno o varios ítems a eliminar por completo",
                        options=[o[0] for o in template_opts],
                        key="chk_template_delete_select_multi",
                    )

                    enviar_eliminacion = st.form_submit_button(
                        "🗑️ Eliminar seleccionados de plantilla y daily (todas las fechas)",
                        type="secondary",
                    )

                if enviar_eliminacion:
                    try:
                        if not selected_template_labels:
                            raise Exception("Selecciona al menos un ítem para eliminar.")

                        selected_rows = [o for o in template_opts if o[0] in selected_template_labels]
                        if not selected_rows:
                            raise Exception("No se pudieron identificar los ítems seleccionados.")

                        ids_to_delete = {_safe_str(item_id) for _, item_id, _ in selected_rows if _safe_str(item_id)}
                        names_to_delete = {_safe_str(item_txt).lower() for _, item_id, item_txt in selected_rows if not _safe_str(item_id)}

                        deleted_template = safe_delete_rows_by_filter(
                            "CHECKLIST_TEMPLATE",
                            lambda rec: (
                                (_safe_str(rec.get("Item_ID", "")) in ids_to_delete)
                                or ((not _safe_str(rec.get("Item_ID", ""))) and (_safe_str(rec.get("Item", "")).lower() in names_to_delete))
                            )
                        )
                        deleted_daily = safe_delete_rows_by_filter(
                            "CHECKLIST_DAILY",
                            lambda rec: (
                                (_safe_str(rec.get("Item_ID", "")) in ids_to_delete)
                                or ((not _safe_str(rec.get("Item_ID", ""))) and (_safe_str(rec.get("Item", "")).lower() in names_to_delete))
                            )
                        )

                        st.success(
                            f"✅ Ítems eliminados por completo ({len(selected_rows)} seleccionados). "
                            f"Plantilla: {deleted_template} fila(s), daily (todas las fechas): {deleted_daily} fila(s)."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ No se pudieron eliminar los ítems: {e}")
            else:
                st.info("No hay ítems en plantilla para eliminar.")


if "cobranza" in tab_map:
    with tab_map["cobranza"]:
        render_cobranza_tab_gerente()

if "seguimiento_cobranza" in tab_map:
    with tab_map["seguimiento_cobranza"]:
        render_seguimiento_cobranza_tab_gerente(usuario_actual)

if "macheo_tool" in tab_map:
    with tab_map["macheo_tool"]:
        render_macheo_tool_tab_gerente()
