import streamlit as st
import pandas as pd
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

# --- CONFIGURACIÓN DE STREAMLIT ---
st.set_page_config(page_title="🔍 Buscador de Guías y Descargas", layout="wide")
st.title("🔍 Buscador de Pedidos por Guía o Cliente")

# ===== SPREADSHEETS =====
SPREADSHEET_ID_MAIN = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SPREADSHEET_ID_ALEJANDRO = "1lWZEL228boUMH_tAdQ3_ZGkYHZZuEkfv"
_ALE_ID_CACHE = {}
_ALE_BOOTSTRAP_CACHE = {}

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
    return gspread_client.open_by_key(
        SPREADSHEET_ID_MAIN
    ).worksheet("datos_pedidos")


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


def cargar_hoja_pedidos(nombre_hoja):
    """Carga una hoja de pedidos por nombre y garantiza columnas mínimas."""
    sheet = gspread_client.open_by_key(SPREADSHEET_ID_MAIN).worksheet(nombre_hoja)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    for c in PEDIDOS_COLUMNAS_MINIMAS:
        if c not in df.columns:
            df[c] = ""
    # Metadata interna para saber desde qué worksheet proviene cada pedido.
    # Se usa al momento de guardar cambios para escribir en la hoja correcta.
    df["__hoja_origen"] = nombre_hoja
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


def cargar_alejandro_hoja(nombre_hoja: str) -> pd.DataFrame:
    """Carga una hoja de alejandro_data y garantiza columnas mínimas."""
    sheet = get_alejandro_worksheet(nombre_hoja)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    cols_min = ALE_COLUMNAS.get(nombre_hoja, [])
    for c in cols_min:
        if c not in df.columns:
            df[c] = ""

    return df


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def new_id(prefix: str) -> str:
    # Ej: CITA-20260224-AB12CD34
    return f"{prefix}-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"


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
    return True


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
def cargar_casos_especiales():
    """
    Lee la hoja 'casos_especiales' y regresa un DataFrame.
    Si faltan columnas del ejemplo, las crea vacías para evitar KeyError.
    """
    sheet = gspread_client.open_by_key(SPREADSHEET_ID_MAIN).worksheet("casos_especiales")
    data = sheet.get_all_records()
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
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
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
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
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
    data = sheet.get_all_records()

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


def update_checklist_daily_item(fecha_iso: str, item_id: str, item: str, completado: bool, notas: str, row_number: int = None, headers: list = None):
    """Actualiza una fila en CHECKLIST_DAILY por (Fecha + Item_ID/Item)."""
    sheet = get_alejandro_worksheet("CHECKLIST_DAILY")
    ensure_headers(sheet, "CHECKLIST_DAILY")
    if headers is None:
        headers = [h.strip() for h in sheet.row_values(1)]
    if row_number is None:
        data = sheet.get_all_records()
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
        "Notas": _safe_str(notas),
    }
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
            alerts.append(("error", f"Hay {len(vencidas)} tarea(s) vencida(s)."))

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


# --- INTERFAZ ---
tabs = st.tabs([
    "🗂️ Organizador (Alejandro)",
    "🔍 Buscar Pedido",
    "⬇️ Descargar Datos",
    "✏️ Modificar Pedido",
])
with tabs[1]:
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

with tabs[2]:
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

CONTRASENA_ADMIN = "Ceci"  # puedes cambiar esta contraseña si lo deseas

# --- PESTAÑA DE MODIFICACIÓN DE PEDIDOS CON CONTRASEÑA ---
with tabs[3]:
    st.header("✏️ Modificar Pedido Existente")

    if st.button(
        "🔄 Actualizar pedidos",
        key="refresh_modificar_pedido",
        help="Recarga la información más reciente de la hoja para mostrar nuevos pedidos y cambios.",
    ):
        st.cache_data.clear()
        st.rerun()

    if "acceso_modificacion" not in st.session_state:
        st.session_state.acceso_modificacion = False

    if not st.session_state.acceso_modificacion:
        contrasena_ingresada = st.text_input("🔑 Ingresa la contraseña para modificar pedidos:", type="password")
        if st.button("🔓 Verificar Contraseña"):
            if contrasena_ingresada == CONTRASENA_ADMIN:
                st.session_state.acceso_modificacion = True
                st.success("✅ Acceso concedido.")
                st.rerun()
            else:
                st.error("❌ Contraseña incorrecta.")

    if st.session_state.acceso_modificacion:
        df_pedidos = cargar_pedidos()
        df_casos = cargar_casos_especiales()

        def es_devol_o_garant(row):
            for col in ("Tipo_Envio", "Tipo_Caso"):
                valor = str(row.get(col, ""))
                if valor and ("devolu" in normalizar(valor) or "garant" in normalizar(valor)):
                    return True
            return False

        df_casos = df_casos[df_casos.apply(es_devol_o_garant, axis=1)]

        for d in (df_pedidos, df_casos):
            d["Hora_Registro"] = pd.to_datetime(d["Hora_Registro"], errors="coerce")

        df_pedidos["__source"] = "pedidos"
        df_casos["__source"] = "casos"
        df = pd.concat([df_pedidos, df_casos], ignore_index=True, sort=False)
        df = df[df["ID_Pedido"].notna()]
        df = df.sort_values(by="Hora_Registro", ascending=False)

        pedido_sel = None
        source_sel = None

        def es_garantia(row):
            """Determina si un caso corresponde a una garantía."""
            for col in ("Tipo_Envio", "Tipo_Caso"):
                valor = normalizar(str(row.get(col, "")))
                if valor and "garant" in valor:
                    return True
            return False

        df_garantias = df_casos[df_casos.apply(es_garantia, axis=1)].copy()

        mostrar_garantias = st.checkbox(
            "🔘 Mostrar sección de garantías",
            help="Activa esta opción para consultar únicamente la información de garantías.",
        )

        if mostrar_garantias:
            st.markdown("### 🛡️ Garantías registradas")
            termino_busqueda_garantia = st.text_input(
                "Buscar por cliente o folio",
                key="busqueda_garantias",
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
                    "No se encontraron garantías con el criterio de búsqueda proporcionado."
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
                        return "Selecciona una garantía"
                    row = df_garantias_filtrado.loc[idx]
                    hora = formatear_fecha(row.get("Hora_Registro"), "%d/%m/%Y %H:%M")
                    estado = row.get("Estado_Caso") or row.get("Estado") or ""
                    return (
                        f"📦 {row.get('ID_Pedido', '')} | 🧾 {row.get('Folio_Factura', '')} | "
                        f"👤 {row.get('Cliente', '')} | 🚚 {row.get('Tipo_Envio', '')} | "
                        f"🔍 {estado} | 🕒 {hora}"
                    )

                idx_garantia = st.selectbox(
                    "Selecciona una garantía para ver detalles o modificarla:",
                    opciones_select,
                    format_func=format_garantia,
                    key="select_garantia",
                )

                if idx_garantia is not None and idx_garantia in df_garantias_filtrado.index:
                    row_garantia = df_garantias_filtrado.loc[idx_garantia]
                    pedido_sel = row_garantia.get("ID_Pedido")
                    source_sel = "casos"
                    st.markdown("#### 📘 Detalles de la garantía seleccionada")

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
                        st.markdown("#### 🗂️ Archivos de la garantía")
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
                    st.info("Selecciona una garantía para ver detalles o modificarla.")
                    st.stop()


        if "pedido_modificado" in st.session_state:
            pedido_sel = st.session_state["pedido_modificado"]
            source_sel = st.session_state.get(
                "pedido_modificado_source", source_sel or "pedidos"
            )
            del st.session_state["pedido_modificado"]  # ✅ limpia la variable tras usarla
            if "pedido_modificado_source" in st.session_state:
                del st.session_state["pedido_modificado_source"]


        usar_busqueda = False
        if pedido_sel is None:
            usar_busqueda = st.checkbox(
                "🔍 Buscar por nombre de cliente (activar para ocultar los últimos 10 pedidos)"
            )

        if pedido_sel is None:
            if usar_busqueda:
                st.markdown("### 🔍 Buscar Pedido por Cliente")
                cliente_buscado = st.text_input("👤 Escribe el nombre del cliente:")
                cliente_normalizado = normalizar(cliente_buscado)
                coincidencias = []

                if cliente_buscado:
                    for _, row_ in df.iterrows():
                        cliente_row = row_.get("Cliente", "").strip()
                        if not cliente_row:
                            continue
                        cliente_row_normalizado = normalizar(cliente_row)
                        if cliente_normalizado in cliente_row_normalizado:
                            coincidencias.append(row_)

                if not coincidencias:
                    st.warning("⚠️ No se encontraron pedidos para ese cliente.")
                    st.stop()
                else:
                    st.success(
                        f"✅ Se encontraron {len(coincidencias)} coincidencia(s) para este cliente."
                    )

                    if len(coincidencias) == 1:
                        pedido_sel = coincidencias[0]["ID_Pedido"]
                        source_sel = coincidencias[0]["__source"]
                        row = coincidencias[0]
                        st.markdown(
                            f"🧾 {row.get('Folio_Factura', row.get('Folio',''))} – 🚚 {row.get('Tipo_Envio','')} – 👤 {row['Cliente']} – 🔍 {row.get('Estado', row.get('Estado_Caso',''))} – 🧑‍💼 {row.get('Vendedor_Registro','')} – 🕒 {row['Hora_Registro'].strftime('%d/%m %H:%M')}"
                        )

                    else:
                        opciones = []
                        for r in coincidencias:
                            folio = r.get('Folio_Factura', r.get('Folio',''))
                            tipo_envio = r.get('Tipo_Envio','')
                            display = (
                                f"{folio} – 🚚 {tipo_envio} – 👤 {r['Cliente']} – 🔍 {r.get('Estado', r.get('Estado_Caso',''))} "
                                f"– 🧑‍💼 {r.get('Vendedor_Registro','')} – 🕒 {r['Hora_Registro'].strftime('%d/%m %H:%M')}"
                            )
                            opciones.append(display)
                        seleccion = st.selectbox(
                            "👥 Se encontraron múltiples pedidos, selecciona uno:", opciones
                        )
                        idx = opciones.index(seleccion)
                        pedido_sel = coincidencias[idx]["ID_Pedido"]
                        source_sel = coincidencias[idx]["__source"]

            else:
                ultimos_10 = df.head(10)
                st.markdown("### 🕒 Últimos 10 Pedidos Registrados")
                ultimos_10["display"] = ultimos_10.apply(
                    lambda row: (
                        f"{row.get('Folio_Factura', row.get('Folio',''))} – {row.get('Tipo_Envio','')} – 👤 {row['Cliente']} "
                        f"– 🔍 {row.get('Estado', row.get('Estado_Caso',''))} – 🧑‍💼 {row.get('Vendedor_Registro','')} "
                        f"– 🕒 {row['Hora_Registro'].strftime('%d/%m %H:%M')}"
                    ),
                    axis=1
                )
                idx_seleccion = st.selectbox(
                    "⬇️ Selecciona uno de los pedidos recientes:",
                    ultimos_10.index,
                    format_func=lambda i: ultimos_10.loc[i, "display"]
                )
                pedido_sel = ultimos_10.loc[idx_seleccion, "ID_Pedido"]
                source_sel = ultimos_10.loc[idx_seleccion, "__source"]


        # --- Cargar datos del pedido seleccionado ---
        st.markdown("---")

        if pedido_sel is None:
            st.warning("⚠️ No se ha seleccionado ningún pedido válido.")
            st.stop()

        row_df = df_pedidos if source_sel == "pedidos" else df_casos
        row = row_df[row_df["ID_Pedido"].astype(str) == str(pedido_sel)].iloc[0]
        gspread_row_idx = row_df[row_df["ID_Pedido"].astype(str) == str(pedido_sel)].index[0] + 2  # índice real en hoja
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
        hoja = gspread_client.open_by_key(SPREADSHEET_ID_MAIN).worksheet(hoja_nombre)

        def actualizar_celdas_y_confirmar(cambios, mensaje_exito):
            """Actualiza celdas en lote y valida lectura de los nuevos valores."""
            try:
                updates = []
                for nombre_col, valor in cambios:
                    if nombre_col not in row_df.columns:
                        raise ValueError(f"No existe la columna '{nombre_col}' en la hoja {hoja_nombre}.")
                    col_idx = row_df.columns.get_loc(nombre_col) + 1
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(gspread_row_idx, col_idx),
                        "values": [[valor]],
                    })

                hoja.batch_update(updates, value_input_option="USER_ENTERED")

                for nombre_col, valor_esperado in cambios:
                    col_idx = row_df.columns.get_loc(nombre_col) + 1
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

        st.markdown("### 📎 Adjuntar Archivos")
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

        st.markdown("### 🧑‍💼 Cambio de Vendedor")
        st.markdown(f"**Actual:** {vendedor_actual}")

        vendedores_opciones = [v for v in vendedores if v != vendedor_actual] or [vendedor_actual]
        nuevo_vendedor = st.selectbox("➡️ Cambiar a:", vendedores_opciones)

        if st.button("🧑‍💼 Guardar cambio de vendedor"):
            if actualizar_celdas_y_confirmar(
                [("Vendedor_Registro", nuevo_vendedor)],
                "🎈 Vendedor actualizado correctamente.",
            ):
                st.rerun()


        if source_sel == "pedidos":
            tipo_envio_actual = row["Tipo_Envio"].strip()
            st.markdown("### 🚚 Cambio de Tipo de Envío")
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
                    min_value=date.today(),
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
        st.markdown("### 🟣 Cancelar Pedido")
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


        completado = row.get("Completados_Limpiado", "")
        st.markdown("### 👁 Visibilidad en Pantalla de Producción")
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

# ===== ORGANIZADOR ALEJANDRO (CON CONTRASEÑA) =====
CONTRASENA_ALEJANDRO = "ale1"

with tabs[0]:
    st.header("🗂️ Organizador (Alejandro)")

    if "acceso_alejandro" not in st.session_state:
        st.session_state.acceso_alejandro = False

    if not st.session_state.acceso_alejandro:
        pw = st.text_input("🔑 Ingresa la contraseña:", type="password", key="pw_alejandro")
        if st.button("🔓 Entrar", key="btn_pw_alejandro"):
            if pw == CONTRASENA_ALEJANDRO:
                st.session_state.acceso_alejandro = True
                st.success("✅ Acceso concedido.")
                st.rerun()
            else:
                st.error("❌ Contraseña incorrecta.")
        st.stop()

    if st.button("🔄 Refrescar Organizador", key="refresh_alejandro"):
        st.rerun()

    # --- Subpestañas internas del organizador ---
    sub = st.tabs(["Hoy", "Agenda", "Tareas", "Cotizaciones", "Checklist", "Config"])

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
        st.warning("⚠️ Hay errores leyendo alejandro_data. Ve a Config > Diagnóstico para detalle.")

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

        # --- CITAS HOY ---
        citas = df_citas.copy()
        if "Fecha_Inicio" in citas.columns:
            citas["_fi"] = _to_dt(citas["Fecha_Inicio"])
            citas = citas[citas["_fi"].dt.date == hoy]
            citas = citas.sort_values("_fi", ascending=True)
        else:
            citas = citas.iloc[0:0]

        # --- TAREAS (HOY / VENCIDAS) ---
        tareas = df_tareas.copy()
        if "Fecha_Limite" in tareas.columns:
            tareas["_fl"] = _to_dt(tareas["Fecha_Limite"])
            tareas_hoy = tareas[tareas["_fl"].dt.date == hoy].copy()
            tareas_vencidas = tareas[(tareas["_fl"].dt.date < hoy) & (tareas["Estatus"].astype(str).str.lower() != "completada")].copy()
            tareas_hoy = tareas_hoy.sort_values("_fl", ascending=True)
            tareas_vencidas = tareas_vencidas.sort_values("_fl", ascending=True)
        else:
            tareas_hoy = tareas.iloc[0:0]
            tareas_vencidas = tareas.iloc[0:0]

        # --- COTIZACIONES (PENDIENTES / VENCIDAS DE SEGUIMIENTO) ---
        cot = df_cot.copy()
        if "Fecha_Proximo_Seguimiento" in cot.columns:
            cot["_fps"] = _to_dt(cot["Fecha_Proximo_Seguimiento"])
            # pendientes = no cerradas
            est = cot.get("Estatus", "").astype(str).str.lower()
            no_cerradas = ~est.str.contains("ganada|perdida", na=False)
            cot_pend = cot[no_cerradas].copy()
            cot_venc = cot_pend[cot_pend["_fps"].notna() & (cot_pend["_fps"].dt.date < hoy)].copy()
            cot_pend = cot_pend.sort_values("_fps", ascending=True)
            cot_venc = cot_venc.sort_values("_fps", ascending=True)
        else:
            cot_pend = cot.iloc[0:0]
            cot_venc = cot.iloc[0:0]

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
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("📅 Citas hoy", len(citas))
        k2.metric("✅ Tareas hoy", len(tareas_hoy))
        k3.metric("⏰ Tareas vencidas", len(tareas_vencidas))
        k4.metric("💰 Cotizaciones pendientes", len(cot_pend))
        k5.metric("🧾 Checklist hoy", f"{pct}%")

        st.markdown("---")

        # ===== DETALLES =====
        st.markdown("### 📅 Citas de hoy")
        if citas.empty:
            st.info("Sin citas para hoy.")
        else:
            cols = [c for c in ["Fecha_Inicio","Cliente_Persona","Empresa_Clinica","Tipo","Prioridad","Estatus","Notas"] if c in citas.columns]
            st.dataframe(citas[cols], use_container_width=True)

        st.markdown("### ✅ Tareas de hoy")
        if tareas_hoy.empty:
            st.info("Sin tareas para hoy.")
        else:
            cols = [c for c in ["Fecha_Limite","Titulo","Prioridad","Estatus","Cliente_Relacionado","Cotizacion_Folio_Relacionado"] if c in tareas_hoy.columns]
            st.dataframe(tareas_hoy[cols], use_container_width=True)

        st.markdown("### ⏰ Tareas vencidas")
        if tareas_vencidas.empty:
            st.info("No hay tareas vencidas 🎉")
        else:
            cols = [c for c in ["Fecha_Limite","Titulo","Prioridad","Estatus","Cliente_Relacionado"] if c in tareas_vencidas.columns]
            st.dataframe(tareas_vencidas[cols], use_container_width=True)

        st.markdown("### 💰 Cotizaciones vencidas de seguimiento")
        if cot_venc.empty:
            st.info("No hay cotizaciones vencidas de seguimiento 🎉")
        else:
            cols = [c for c in ["Folio","Fecha_Cotizacion","Cliente","Monto","Estatus","Fecha_Proximo_Seguimiento","Notas"] if c in cot_venc.columns]
            st.dataframe(cot_venc[cols], use_container_width=True)

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

        st.markdown("### 📋 Agenda")
        st.dataframe(df_citas, use_container_width=True)

    with sub[2]:
        st.subheader("✅ Tareas")

        # ===== Alta rápida =====
        with st.form("form_nueva_tarea", clear_on_submit=True):
            st.markdown("### ➕ Nueva tarea")
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

            submitted = st.form_submit_button("✅ Crear tarea")

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
                        "Tipo": "Tarea",
                        "Fecha_Completado": now_iso() if estatus.lower() == "completada" else "",
                        "Notas_Resultado": "",
                        "Last_Updated_At": now_iso(),
                        "Last_Updated_By": "ALEJANDRO",
                        "Is_Deleted": "0",
                    }
                    safe_append("TAREAS", payload)
                    st.success(f"🎈 Tarea creada: {tarea_id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error creando tarea: {e}")

        st.markdown("---")
        st.markdown("### ✅ Completar / Reabrir tarea")

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
            st.info("No hay tareas disponibles para actualizar.")
            tarea_sel = None
        else:
            tarea_sel = st.selectbox("Selecciona una tarea:", opciones, format_func=format_tarea, key="tarea_sel_update")

        if tarea_sel:
            colA, colB = st.columns(2)

            with colA:
                if st.button("✅ Marcar como COMPLETADA", use_container_width=True):
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
                            st.success("🎈 Tarea marcada como completada.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error actualizando tarea: {e}")

            with colB:
                if st.button("↩️ Reabrir (PENDIENTE)", use_container_width=True):
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
                            st.success("🎈 Tarea reabierta (Pendiente).")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error reabriendo tarea: {e}")

        st.markdown("### 📋 Lista")
        st.dataframe(df_tareas, use_container_width=True)

    with sub[3]:
        st.subheader("💰 Cotizaciones")

        with st.form("form_nueva_cot", clear_on_submit=True):
            st.markdown("### ➕ Nueva cotización")
            folio = st.text_input("Folio", placeholder="Ej. COT-12345")
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
        st.markdown("### 🔁 Convertir cotización a tarea")

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
                st.info(f"🧩 Esta cotización ya fue convertida a **Tarea**: {tarea_link}")
            if ya_cita:
                st.info(f"📅 Esta cotización ya fue convertida a **Cita**: {cita_link}")

            st.markdown("#### ➜ Tipo de conversión")
            tipo_conv = st.radio(
                "¿Qué quieres crear?",
                ["🧩 Tarea", "📅 Cita"],
                horizontal=True,
                key="tipo_conversion_cot"
            )

            if tipo_conv == "🧩 Tarea":
                colA, colB = st.columns([2, 1])

                with colA:
                    titulo_sugerido = st.text_input(
                        "Título de la tarea (editable):",
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
                    "🧩 Convertir a TAREA",
                    key="btn_convertir_a_tarea",
                    use_container_width=True,
                    disabled=ya_tarea,
                ):
                    if not titulo_sugerido.strip():
                        st.error("❌ El título de la tarea no puede ir vacío.")
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

                            st.success(f"🎈 Tarea creada desde cotización: {tarea_id}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al convertir a tarea: {e}")

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
                    ["Visita", "Llamada", "Junta", "Seguimiento"],
                    index=3,
                    key="tipo_cita_desde_cot"
                )
                estatus_cita = st.selectbox(
                    "Estatus",
                    ["Programada", "Realizada", "Reprogramada", "Cancelada"],
                    index=0,
                    key="estatus_cita_desde_cot"
                )
                reminder_cita = st.number_input(
                    "Recordatorio (min antes)",
                    min_value=0,
                    max_value=240,
                    value=30,
                    step=5,
                    key="reminder_cita_desde_cot"
                )

                # Usamos título/desc sugeridos para notas
                notas_cita = st.text_area(
                    "Notas de la cita",
                    value=(f"Seguimiento cotización {folio}\n"
                           f"Estatus: {estatus}\n"
                           f"Próx. seguimiento: {prox}\n"
                           f"Notas cotización: {notas}").strip(),
                    height=100,
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
        st.dataframe(df_cot, use_container_width=True)

    with sub[4]:
        st.subheader("🧾 Checklist")
        hoy = date.today()

        with st.expander("➕ Agregar ítem a plantilla recurrente", expanded=False):
            with st.form("form_add_check_template", clear_on_submit=True):
                item_txt = st.text_input("Ítem")
                orden = st.number_input("Orden", min_value=1, max_value=999, value=10, step=1)
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
                    st.success("✅ Ítem agregado a la plantilla.")
                    st.rerun()

        st.markdown("### Plantilla recurrente")
        st.dataframe(df_checklist_template, use_container_width=True)

        st.markdown("### Checklist de hoy")
        chk_hoy_edit = df_checklist_daily.copy()
        if "Fecha" in chk_hoy_edit.columns:
            chk_hoy_edit["_f"] = _to_date(chk_hoy_edit["Fecha"])
            chk_hoy_edit = chk_hoy_edit[chk_hoy_edit["_f"] == hoy].copy()

        done_chk = chk_hoy_edit["Completado"].apply(_to_bool).sum() if (not chk_hoy_edit.empty and "Completado" in chk_hoy_edit.columns) else 0
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

                c1, c2, c3 = st.columns([4, 1.5, 2])
                with c1:
                    nuevo_comp = st.checkbox(item, value=comp, key=f"chk_done_{item_id}_{item}")
                with c2:
                    nuevo_notas = st.text_input("Notas", value=notas_actuales, key=f"chk_note_{item_id}_{item}")
                with c3:
                    if st.button("💾 Guardar", key=f"chk_save_{item_id}_{item}"):
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

    with sub[5]:
        st.subheader("⚙️ Config")
        st.dataframe(df_config, use_container_width=True)

        st.markdown("---")
        st.markdown("### 🧪 Diagnóstico alejandro_data")

        if errores_alejandro:
            st.error("Errores detectados al leer hojas:")
            for err in errores_alejandro:
                st.code(err)

        if st.button("Ejecutar diagnóstico", key="diag_alejandro_doc"):
            diag = debug_alejandro_documento()
            st.json(diag)
            if diag.get("quota_fallback"):
                st.success(
                    "Drive está en quota/permisos restringidos y el fallback a spreadsheet principal está ACTIVADO por configuración."
                )
            elif diag.get("bootstrap_created"):
                st.success(
                    "No se pudo convertir el Excel por permisos (403). Se creó un Google Sheet bootstrap con tu estructura para esta sesión. "
                    "Copia resolved_spreadsheet_id a secrets para usarlo de forma permanente."
                )
            elif diag.get("auto_converted"):
                st.success(
                    "Se detectó Excel y se convirtió automáticamente a Google Sheet para esta sesión. "
                    "Copia el resolved_spreadsheet_id y guárdalo en secrets para hacerlo permanente."
                )

            err = str(diag.get("error", ""))
            mime = str(diag.get("drive_mimeType", ""))
            if ("not supported for this document" in err.lower()) or (
                mime and mime != "application/vnd.google-apps.spreadsheet"
            ):
                st.error(
                    "El archivo configurado no es Google Sheet nativo. En Drive: Abrir con > Hojas de cálculo de Google "
                    "(convierte), y después usa el ID del documento convertido en secrets."
                )
            elif diag.get("open_ok") and diag.get("missing_expected_sheets"):
                st.warning("Faltan hojas esperadas: " + ", ".join(diag.get("missing_expected_sheets", [])))
