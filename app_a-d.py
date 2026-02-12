
import time
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.exceptions import RequestException
import boto3
import re
import gspread.utils
import json # Import json for parsing credentials
import os
import uuid
from pytz import timezone
from urllib.parse import urlparse, unquote
import streamlit.components.v1 as components
from typing import Any, Optional, Sequence
import unicodedata

_MX_TZ = timezone("America/Mexico_City")

_RECOVERABLE_AUTH_PATTERNS = (
    "401",
    "UNAUTHENTICATED",
    "ACCESS_TOKEN_EXPIRED",
    "RESOURCE_EXHAUSTED",
    "RATE_LIMIT",
    "429",
)


def _is_recoverable_auth_error(exc: Exception) -> bool:
    err_text = str(exc)
    return any(code in err_text for code in _RECOVERABLE_AUTH_PATTERNS)

def mx_now():
    return datetime.now(_MX_TZ)           # objeto datetime tz-aware

def mx_now_str():
    return mx_now().strftime("%Y-%m-%d %H:%M:%S")

def mx_today():
    return mx_now().date()


def _ensure_visual_state_defaults():
    """Ensure session_state has all UI control keys with safe defaults."""

    state = st.session_state

    # Índices de pestañas y banderas de scroll
    state.setdefault("active_main_tab_index", 0)
    state.setdefault("active_subtab_local_index", 0)
    state.setdefault("active_date_tab_m_index", 0)
    state.setdefault("active_date_tab_t_index", 0)
    state.setdefault("active_date_tab_s_index", 0)
    state.setdefault("active_date_tab_m_label", "")
    state.setdefault("active_date_tab_t_label", "")
    state.setdefault("active_date_tab_s_label", "")
    state.setdefault("scroll_to_pedido_id", None)

    # Diccionarios que controlan expanders o secciones dinámicas
    state.setdefault("expanded_pedidos", {})
    state.setdefault("expanded_attachments", {})
    state.setdefault("expanded_subir_guia", {})
    state.setdefault("expanded_devoluciones", {})
    state.setdefault("expanded_garantias", {})

    # Otros mapas auxiliares usados por callbacks
    state.setdefault("guia_upload_success", {})
    state.setdefault("last_pedidos_count", 0)
    state.setdefault("last_casos_count", 0)
    state.setdefault("prev_pedidos_count", 0)
    state.setdefault("prev_casos_count", 0)
    state.setdefault("need_compare", False)
    state.setdefault("bulk_complete_mode", False)
    state.setdefault("bulk_selected_pedidos", set())


def _get_bulk_selected_ids() -> set[str]:
    selected = st.session_state.get("bulk_selected_pedidos", set())
    if isinstance(selected, set):
        return selected
    if isinstance(selected, (list, tuple)):
        return set(str(x).strip() for x in selected if str(x).strip())
    return set()


def _set_bulk_mode(enabled: bool) -> None:
    st.session_state["bulk_complete_mode"] = bool(enabled)
    if enabled:
        return

    st.session_state["bulk_selected_pedidos"] = set()
    for key in list(st.session_state.keys()):
        if key.startswith("bulk_chk_"):
            del st.session_state[key]


def _cleanup_bulk_selection(visible_ids: set[str]) -> None:
    selected = _get_bulk_selected_ids()
    if not visible_ids:
        selected = set()
    else:
        selected = selected.intersection(visible_ids)

    st.session_state["bulk_selected_pedidos"] = selected

    for key in list(st.session_state.keys()):
        if not key.startswith("bulk_chk_"):
            continue
        pedido_id = key.replace("bulk_chk_", "", 1)
        if pedido_id not in visible_ids:
            del st.session_state[key]


def _render_bulk_selector(row: Any) -> None:
    if not st.session_state.get("bulk_complete_mode", False):
        return

    if str(row.get("Estado", "")).strip() != "🔵 En Proceso":
        return

    pedido_id = str(row.get("ID_Pedido", "")).strip()
    if not pedido_id:
        return

    selected = _get_bulk_selected_ids()
    checkbox_key = f"bulk_chk_{pedido_id}"

    if checkbox_key not in st.session_state:
        st.session_state[checkbox_key] = pedido_id in selected

    col_chk, col_txt = st.columns([0.07, 0.93])
    checked = col_chk.checkbox(
        "Seleccionar pedido",
        key=checkbox_key,
        label_visibility="collapsed",
    )
    col_txt.caption("Seleccionar este pedido para **Completar Pedidos**")

    if checked:
        selected.add(pedido_id)
    else:
        selected.discard(pedido_id)

    st.session_state["bulk_selected_pedidos"] = selected


_TAB_LABELS_BY_TIPO = {
    "📍 Pedido Local": "📍 Pedidos Locales",
    "📍 Pedidos Locales": "📍 Pedidos Locales",
    "🚚 Pedido Foráneo": "🚚 Pedidos Foráneos",
    "🚚 Pedidos Foráneos": "🚚 Pedidos Foráneos",
    "🏙️ Pedido CDMX": "🏙️ Pedidos CDMX",
    "🏙️ Pedidos CDMX": "🏙️ Pedidos CDMX",
    "📋 Solicitudes de Guía": "📋 Solicitudes de Guía",
    "📋 Solicitud de Guía": "📋 Solicitudes de Guía",
    "📋 Solicitudes de Guia": "📋 Solicitudes de Guía",
    "🎓 Cursos y Eventos": "🎓 Cursos y Eventos",
    "🎓 Curso y Evento": "🎓 Cursos y Eventos",
    "🔁 Devolución": "🔁 Devoluciones",
    "🔁 Devoluciones": "🔁 Devoluciones",
    "🛠 Garantía": "🛠 Garantías",
    "🛠 Garantías": "🛠 Garantías",
}

_LOCAL_TURNO_TO_SUBTAB = {
    "☀️ Local Mañana": "🌅 Mañana",
    "🌙 Local Tarde": "🌇 Tarde",
    "🌵 Saltillo": "⛰️ Saltillo",
    "📦 Pasa a Bodega": "📦 En Bodega",
}

_LOCAL_SUBTAB_OPTIONS = ["🌅 Mañana", "🌇 Tarde", "⛰️ Saltillo", "📦 En Bodega"]
_LOCAL_NO_ENTREGADOS_TAB_LABEL = "🚫 No entregados"


def _clamp_tab_index(index: Any, options: Sequence[Any]) -> int:
    """Return a safe tab index within the bounds of ``options``."""

    if not options:
        return 0

    try:
        parsed_index = int(index)
    except (TypeError, ValueError):
        return 0

    if parsed_index < 0:
        return 0

    max_index = len(options) - 1
    if parsed_index > max_index:
        return max_index

    return parsed_index


def _clear_offscreen_pedido_flags(visible_ids: set[str]) -> None:
    """Remove confirmation flags for pedidos that are no longer visible."""

    if not visible_ids:
        visible_ids = set()

    keys_to_delete = []
    for key in st.session_state.keys():
        if not key.startswith("confirmar_completar_"):
            continue

        pedido_id = key.replace("confirmar_completar_", "", 1)
        if pedido_id not in visible_ids:
            keys_to_delete.append(key)

    for key in keys_to_delete:
        del st.session_state[key]


def _clear_offscreen_guide_flags(visible_ids: set[str]) -> None:
    prompts = st.session_state.get("confirm_complete_after_guide", {})

    if not visible_ids:
        return

    if not isinstance(prompts, dict):
        return

    keys_to_delete = []
    for pedido_id in list(prompts.keys()):
        if pedido_id not in visible_ids:
            keys_to_delete.append(pedido_id)

    for pedido_id in keys_to_delete:
        try:
            prompts.pop(pedido_id, None)
        except Exception:
            continue


def _get_first_query_value(params: Any, key: str) -> Optional[str]:
    """Return the first value for ``key`` from Streamlit query params."""

    if params is None:
        return None

    value = params.get(key)

    if isinstance(value, list):
        if not value:
            return None
        return value[0]

    return value


def _resolve_tab_index_from_query(
    params: Any,
    query_key: str,
    options: Sequence[Any],
    fallback_index: Any = 0,
) -> int:
    """Resolve a persisted tab index using the query string when available."""

    query_value = _get_first_query_value(params, query_key)

    if query_value is not None:
        return _clamp_tab_index(query_value, options)

    return _clamp_tab_index(fallback_index, options)


def _is_empty_text(value: Any) -> bool:
    return str(value).strip() == "" or str(value).strip().lower() == "nan"


def pedido_sin_guia(row: Any) -> bool:
    if not pedido_requiere_guia(row):
        return False

    estado_no_completado = str(row.get("Estado", "")).strip() != "🟢 Completado"
    completados_vacio = _is_empty_text(row.get("Completados_Limpiado", ""))

    if not (estado_no_completado and completados_vacio):
        return False

    if "Hoja_Ruta_Mensajero" in row:
        return _is_empty_text(row.get("Hoja_Ruta_Mensajero", ""))

    if "Adjuntos_Guia" in row:
        return _is_empty_text(row.get("Adjuntos_Guia", ""))

    return False


def _emit_recent_tab_group_script(active_index: int, query_param: str) -> None:
    """Attach JS to focus the most recently rendered tab group and persist selection."""

    components.html(
        f"""
        <script>
        (function() {{
            const tabGroups = window.parent.document.querySelectorAll('.stTabs');
            const targetGroup = tabGroups[tabGroups.length - 1];
            if (!targetGroup) {{
                return;
            }}
            const tabs = targetGroup.querySelectorAll('[data-baseweb="tab"]');
            const activeIndex = {int(active_index)};
            if (tabs[activeIndex]) {{
                tabs[activeIndex].click();
            }}
            const paramKey = {json.dumps(query_param)};
            tabs.forEach((tab, idx) => {{
                tab.addEventListener('click', () => {{
                    const params = new URLSearchParams(window.parent.location.search);
                    params.set(paramKey, idx);
                    const base = window.parent.location.origin + window.parent.location.pathname;
                    const query = params.toString();
                    const newUrl = query ? `${{base}}?${{query}}` : base;
                    window.parent.history.replaceState(null, '', newUrl);
                }});
            }});
        }})();
        </script>
        """,
        height=0,
    )

_UNKNOWN_TAB_LABEL = "Sin pestaña identificada"


_EMPTY_TEXT_MARKERS = {"", "nan", "none", "null", "n/a"}

GUIDE_REQUIRED_ERROR_MSG = (
    "❌ No puedes completar este pedido hasta subir la guía solicitada."
)


def _pending_modificaciones(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows with Modificacion_Surtido pending confirmation."""

    if df is None:
        return pd.DataFrame()

    if df.empty or "Modificacion_Surtido" not in df.columns:
        return df.iloc[0:0]

    mod_text = df["Modificacion_Surtido"].astype(str).str.strip()
    estado_series = df.get("Estado", pd.Series("", index=df.index)).astype(str)
    refact_tipo_series = df.get("Refacturacion_Tipo", pd.Series("", index=df.index)).astype(str)

    mask_non_empty = mod_text != ""
    mask_not_confirmed = ~mod_text.str.endswith("[✔CONFIRMADO]")
    mask_estado_activo = ~estado_series.isin(["🟢 Completado", "✅ Viajó"])
    mask_valid_refact_tipo = refact_tipo_series.str.strip() != "Datos Fiscales"

    mask = mask_non_empty & mask_not_confirmed & mask_estado_activo & mask_valid_refact_tipo
    return df[mask]


def normalize_sheet_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()

    if text.lower() in _EMPTY_TEXT_MARKERS:
        return ""

    return text


def _normalize_tab_field(value: Optional[Any]) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def derive_tab_label(tipo_envio: Optional[str], turno: Optional[str]) -> str:
    """Return the UI tab/subtab label for a given shipment type and shift."""

    tipo_envio = _normalize_tab_field(tipo_envio)
    turno = _normalize_tab_field(turno)

    base_label = _TAB_LABELS_BY_TIPO.get(tipo_envio)

    if tipo_envio == "📍 Pedido Local":
        subtab = _LOCAL_TURNO_TO_SUBTAB.get(turno)
        if base_label and subtab:
            return f"{base_label} • {subtab}"

    if base_label:
        return base_label

    if tipo_envio:
        return tipo_envio

    if turno:
        return turno

    return _UNKNOWN_TAB_LABEL


def collect_tab_locations(
    df: pd.DataFrame,
    tipo_col: str = "Tipo_Envio",
    turno_col: str = "Turno",
) -> list[str]:
    if df is None or df.empty:
        return []

    tipo_values = df.get(tipo_col)
    turno_values = df.get(turno_col)

    total_rows = len(df)
    resolved = []

    for idx in range(total_rows):
        tipo_val = tipo_values.iat[idx] if tipo_values is not None else None
        turno_val = turno_values.iat[idx] if turno_values is not None else None
        label = derive_tab_label(tipo_val, turno_val)
        resolved.append(label if label else _UNKNOWN_TAB_LABEL)

    return sorted(set(resolved))



def _normalize_text_for_matching(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(
        ch for ch in normalized if not unicodedata.combining(ch)
    )
    return without_accents.lower()


_GUIDE_REQUEST_PHRASES = (
    "solicito la guia",
    "solicitamos la guia",
    "solicitar la guia",
    "solicitud de guia",
    "favor de enviar la guia",
    "favor de proporcionar la guia",
    "favor de adjuntar guia",
    "favor de compartir la guia",
    "apoyar con la guia",
    "apoyarnos con la guia",
    "mandar la guia",
    "enviar la guia",
    "requiero la guia",
    "requiere la guia",
    "necesito la guia",
    "necesitamos la guia",
    "compartir guia",
    "proporcionar guia",
)

_GUIDE_REQUEST_KEYWORDS = (
    "solicito",
    "solicitamos",
    "solicitar",
    "solicitud",
    "favor de",
    "apoyar",
    "apoyarnos",
    "apoyeme",
    "apoyenme",
    "mandar",
    "manda",
    "manden",
    "enviar",
    "envia",
    "enviame",
    "envien",
    "requiere",
    "requiero",
    "requerimos",
    "necesita",
    "necesito",
    "necesitamos",
    "compartir",
    "proporcionar",
)

_GUIDE_TERMS = (
    "guia",
    "guia de envio",
    "guia de envío",
    "guia de entrega",
    "hoja de ruta",
    "hoja ruta",
    "hoja_ruta",
)

_ADDRESS_TERMS = (
    "calle",
    "col ",
    "col.",
    "colonia",
    "c.p",
    "cp.",
    "cp ",
    "cp:",
    "direccion",
    "dir.",
    "dir:",
    "av.",
    "avenida",
    "numero",
    "número",
    "no.",
    "num.",
    "entre",
    "esq",
    "esquina",
    "municipio",
    "delegacion",
    "delegación",
    "estado",
    "ciudad",
    "manzana",
    "lote",
    "fraccionamiento",
    "edificio",
)


def comentario_requiere_guia(comentario: Any) -> bool:
    text = str(comentario or "").strip()
    if not text:
        return False

    normalized = _normalize_text_for_matching(text)

    if not normalized:
        return False

    has_address_indicator = any(term in normalized for term in _ADDRESS_TERMS)
    if not has_address_indicator:
        return False

    has_specific_phrase = any(phrase in normalized for phrase in _GUIDE_REQUEST_PHRASES)

    if not has_specific_phrase:
        has_request_keyword = any(keyword in normalized for keyword in _GUIDE_REQUEST_KEYWORDS)
        has_guide_term = any(term in normalized for term in _GUIDE_TERMS)
        has_specific_phrase = has_request_keyword and has_guide_term

    if not has_specific_phrase:
        return False

    if (
        "sin guia" in normalized
        or "sin hoja de ruta" in normalized
        or "no requiere guia" in normalized
        or "no requerimos guia" in normalized
        or "no necesitamos guia" in normalized
    ):
        return False

    return True


def es_pedido_local_no_entregado(row: Any) -> bool:
    """Determina si el pedido local aún no ha sido entregado."""

    tipo = str(row.get("Tipo_Envio", "")).strip()
    estado = str(row.get("Estado", "")).strip()
    estado_entrega = str(row.get("Estado_Entrega", "")).strip()
    return (
        tipo == "📍 Pedido Local"
        and estado == "🟢 Completado"
        and estado_entrega == "⏳ No Entregado"
    )



st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

# 🔁 Restaurar pestañas activas si venimos de una acción que modificó datos
restoring_tabs = st.session_state.pop("restore_tabs_after_print", False)

params = st.query_params

if "preserve_main_tab" in st.session_state:
    restoring_tabs = True
    st.session_state["active_main_tab_index"] = st.session_state.pop("preserve_main_tab", 0)
    st.session_state["active_subtab_local_index"] = _clamp_tab_index(
        st.session_state.pop("preserve_local_tab", 0),
        _LOCAL_SUBTAB_OPTIONS,
    )
    st.session_state["active_date_tab_m_index"] = st.session_state.pop("preserve_date_tab_m", 0)
    st.session_state["active_date_tab_t_index"] = st.session_state.pop("preserve_date_tab_t", 0)
else:
    # 🧭 Leer pestaña activa desde parámetros de URL
    if "tab" in params:
        try:
            tab_val = params["tab"]
            if isinstance(tab_val, list):
                tab_val = tab_val[0]
            st.session_state["active_main_tab_index"] = int(tab_val)
        except (ValueError, TypeError):
            st.session_state["active_main_tab_index"] = 0

st.session_state["active_subtab_local_index"] = _resolve_tab_index_from_query(
    params,
    "local_tab",
    _LOCAL_SUBTAB_OPTIONS,
    st.session_state.get("active_subtab_local_index", 0),
)

if restoring_tabs and "active_main_tab_index" in st.session_state:
    st.query_params["tab"] = str(st.session_state["active_main_tab_index"])
else:
    st.query_params["tab"] = str(st.session_state.get("active_main_tab_index", 0))

st.query_params["local_tab"] = str(
    st.session_state.get("active_subtab_local_index", 0)
)

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
    st.session_state["refresh_data_caches_pending"] = True


_ensure_visual_state_defaults()


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
    required_aws_keys = {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_region",
        "s3_bucket_name",
    }
    missing_aws_keys = sorted(required_aws_keys.difference(AWS_CREDENTIALS.keys()))
    if missing_aws_keys:
        st.error("❌ Las credenciales de AWS S3 están incompletas. Faltan las claves: " + ", ".join(missing_aws_keys))
        st.info("Asegúrate de definir todas las claves requeridas dentro de la sección [aws] en tus secretos.")
        st.stop()

    AWS_ACCESS_KEY_ID = AWS_CREDENTIALS["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = AWS_CREDENTIALS["aws_secret_access_key"]
    AWS_REGION = AWS_CREDENTIALS["aws_region"]
    S3_BUCKET_NAME = AWS_CREDENTIALS["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Problema al acceder a una clave de AWS S3 en Streamlit secrets. Falta la clave: {e}")
    st.info("Asegúrate de que todas las claves (aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name) estén presentes en la sección [aws].")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

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
    if _is_recoverable_auth_error(exc):
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

    gsheets_secrets = st.secrets["gsheets"]

    if "google_credentials" not in gsheets_secrets:
        st.error("❌ Las credenciales de Google Sheets están incompletas. Falta la clave 'google_credentials' en la sección [gsheets].")
        st.info("Incluye el JSON completo de la cuenta de servicio en la clave google_credentials dentro de la sección [gsheets] de tu archivo .streamlit/secrets.toml o en los secretos de Streamlit Cloud.")
        st.stop()

    try:
        GSHEETS_CREDENTIALS = json.loads(gsheets_secrets["google_credentials"])
    except json.JSONDecodeError as decode_err:
        st.error("❌ No se pudieron leer las credenciales de Google Sheets. El valor de 'google_credentials' no es un JSON válido.")
        st.info(f"Detalle del error: {decode_err}. Revisa que el JSON esté completo y que los saltos de línea de la llave privada estén escapados (\\n).")
        st.stop()

    required_google_keys = {"client_email", "private_key", "token_uri"}
    missing_google_keys = sorted(required_google_keys.difference(GSHEETS_CREDENTIALS))
    if missing_google_keys:
        st.error("❌ Las credenciales de Google Sheets están incompletas. Faltan las claves: " + ", ".join(missing_google_keys))
        st.info("Descarga nuevamente el archivo JSON de la cuenta de servicio y copia todo su contenido en la clave google_credentials.")
        st.stop()

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
@st.cache_data(ttl=300, hash_funcs={gspread.client.Client: lambda _: None})
def get_raw_sheet_data(
    sheet_id: str,
    worksheet_name: str,
    client: Optional[gspread.client.Client] = None,
) -> list[list[str]]:
    gspread_client = client or g_spread_client
    if gspread_client is None:
        raise ValueError("No se proporcionó un cliente de gspread para obtener los datos.")
    max_attempts = 3
    base_delay = 1
    for attempt in range(max_attempts):
        wait_seconds = base_delay * (2 ** attempt)
        try:
            sheet = gspread_client.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            return worksheet.get_all_values()
        except gspread.exceptions.APIError as api_error:
            # ℹ️ Solo limpiamos la caché de esta función para no reiniciar otros estados de la app.
            get_raw_sheet_data.clear()
            if _is_recoverable_auth_error(api_error) and attempt < max_attempts - 1:
                st.warning(
                    f"🔁 Error de autenticación con Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error de la API de Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            if _is_recoverable_auth_error(api_error):
                handle_auth_error(api_error)
            else:
                st.error(f"❌ Error de la API de Google Sheets: {api_error}")
            raise
        except RequestException as net_err:
            get_raw_sheet_data.clear()
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error de red al conectar con Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            handle_auth_error(net_err)
            raise
        except Exception as e:
            get_raw_sheet_data.clear()  # 🔁 Limpiar solo la caché de esta función en caso de error de token/API
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error inesperado al conectar con Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            st.error(
                "❌ No se pudo conectar con Google Sheets después de varios intentos. "
                "Verifica tu conectividad o tus credenciales de servicio."
            )
            raise


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
        'Estado', 'Estado_Pago', 'Fecha_Completado', 'Hora_Proceso', 'Turno',
        'Estado_Entrega', 'Direccion_Guia_Retorno'
    ]



    for col in expected_columns:
        if col not in df.columns:
            df[col] = ''

    df['Comentario'] = df['Comentario'].apply(normalize_sheet_text)
    if 'Direccion_Guia_Retorno' in df.columns:
        df['Direccion_Guia_Retorno'] = df['Direccion_Guia_Retorno'].apply(
            normalize_sheet_text
        )
    else:
        df['Direccion_Guia_Retorno'] = ''
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
    if 'Estado_Entrega' in df.columns:
        df['Estado_Entrega'] = df['Estado_Entrega'].astype(str).str.strip()

    comentario_requiere = df['Comentario'].apply(comentario_requiere_guia)
    direccion_requiere = df['Direccion_Guia_Retorno'].apply(lambda val: bool(val))
    df['requiere_guia'] = comentario_requiere | direccion_requiere

    return df, headers


def _filter_relevant_pedidos(df: pd.DataFrame, headers: list[str], worksheet_name: str) -> pd.DataFrame:
    """Modo de carga liviana: mantiene filas pendientes (Completados_Limpiado vacío)."""
    if worksheet_name != GOOGLE_SHEET_WORKSHEET_NAME or df.empty:
        return df

    # Importante: filtrar solo si la columna viene del origen real de Sheets.
    # Evita falsos positivos cuando la columna fue agregada localmente en runtime.
    if "Completados_Limpiado" not in headers:
        return df

    serie = df.get("Completados_Limpiado", pd.Series([""] * len(df), index=df.index))
    mask = serie.apply(_is_empty_text)
    return df[mask].copy()


@st.cache_data(ttl=300, hash_funcs={gspread.client.Client: lambda _: None})
def get_filtered_sheet_dataframe(
    sheet_id: str,
    worksheet_name: str,
    client: Optional[gspread.client.Client] = None,
    *,
    light_mode: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    raw = get_raw_sheet_data(sheet_id=sheet_id, worksheet_name=worksheet_name, client=client)
    df, headers = process_sheet_data(raw)
    if light_mode:
        df = _filter_relevant_pedidos(df, headers, worksheet_name)
    return df, headers


def _record_local_sheet_update(worksheet_name: str, row_index: int, values: dict[str, Any]) -> None:
    local_updates = st.session_state.setdefault("local_sheet_updates", {})
    worksheet_updates = local_updates.setdefault(worksheet_name, {})
    row_updates = worksheet_updates.setdefault(int(row_index), {})
    row_updates.update(values)


def _apply_local_sheet_updates(df: pd.DataFrame, worksheet_name: str) -> pd.DataFrame:
    if df.empty:
        return df

    worksheet_updates = st.session_state.get("local_sheet_updates", {}).get(worksheet_name, {})
    if not worksheet_updates:
        return df

    rows = pd.to_numeric(df.get("_gsheet_row_index", pd.Series(dtype=float)), errors="coerce")
    for row_index, updates in worksheet_updates.items():
        mask = rows == int(row_index)
        if not mask.any():
            continue
        for col, value in updates.items():
            if col not in df.columns:
                df[col] = ""
            df.loc[mask, col] = value
    return df


def _get_worksheet_name_safe(worksheet: Any) -> str:
    return str(getattr(worksheet, "title", "") or "")


def _updates_list_to_column_values(
    headers: list[str],
    updates_list: list[dict[str, Any]],
    *,
    target_row_index: int,
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for item in updates_list:
        row, col = gspread.utils.a1_to_rowcol(item["range"])
        if int(row) != int(target_row_index):
            continue
        header_idx = int(col) - 1
        if 0 <= header_idx < len(headers):
            values[headers[header_idx]] = item["values"][0][0]
    return values


def update_gsheet_cell(worksheet, headers, row_index, col_name, value):
    """
    Actualiza una celda específica en Google Sheets.
    row_index es el índice de fila de gspread (base 1).
    col_name es el nombre de la columna.
    headers es la lista de encabezados obtenida previamente.
    """
    if col_name not in headers:
        st.error(f"❌ Error: La columna '{col_name}' no se encontró en Google Sheets para la actualización. Verifica los encabezados.")
        return False

    col_index = headers.index(col_name) + 1  # Convertir a índice base 1 de gspread

    max_attempts = 3
    base_delay = 1

    for attempt in range(max_attempts):
        wait_seconds = base_delay * (2 ** attempt)
        try:
            worksheet.update_cell(row_index, col_index, value)
            worksheet_name = _get_worksheet_name_safe(worksheet)
            if worksheet_name:
                _record_local_sheet_update(worksheet_name, int(row_index), {col_name: value})
            # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
            return True
        except gspread.exceptions.APIError as api_error:
            is_recoverable = _is_recoverable_auth_error(api_error)
            if attempt < max_attempts - 1:
                if is_recoverable:
                    st.warning(
                        f"🔁 Error de autenticación/cuota al actualizar Google Sheets "
                        f"(intento {attempt + 1}/{max_attempts}). Reintentando en {wait_seconds}s..."
                    )
                else:
                    st.warning(
                        f"⚠️ Error de la API de Google Sheets al actualizar celdas "
                        f"(intento {attempt + 1}/{max_attempts}). Reintentando en {wait_seconds}s..."
                    )
                time.sleep(wait_seconds)
                continue

            if is_recoverable:
                st.error(
                    "❌ No se pudo completar la actualización en Google Sheets por un error de autenticación/cuota "
                    "después de varios intentos."
                )
                handle_auth_error(api_error)
            else:
                st.error(f"❌ Error definitivo de la API de Google Sheets: {api_error}")
            break
        except RequestException as net_err:
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error de red al actualizar Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            st.error(
                "❌ No se pudo conectar con Google Sheets para actualizar los datos después de varios intentos. "
                "Verifica tu conexión o credenciales."
            )
            handle_auth_error(net_err)
            break
        except Exception as exc:
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error inesperado al actualizar Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            st.error(f"❌ Error inesperado al actualizar Google Sheets: {exc}")
            break

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


def batch_update_gsheet_cells(worksheet, updates_list, *, headers: Optional[list[str]] = None):
    """
    Realiza múltiples actualizaciones de celdas en una sola solicitud por lotes a Google Sheets
    utilizando worksheet.update_cells().
    updates_list: Lista de diccionarios, cada uno con las claves 'range' y 'values'.
                  Ej: [{'range': 'A1', 'values': [['nuevo_valor']]}, ...]
    """
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

    if not cell_list:
        return False

    max_attempts = 3
    base_delay = 1

    for attempt in range(max_attempts):
        wait_seconds = base_delay * (2 ** attempt)
        try:
            worksheet.update_cells(cell_list)  # Este es el método correcto para batch update en el worksheet
            if headers:
                worksheet_name = _get_worksheet_name_safe(worksheet)
                if worksheet_name:
                    updates_by_row: dict[int, dict[str, Any]] = {}
                    for item in updates_list:
                        row_idx, _ = gspread.utils.a1_to_rowcol(item["range"])
                        row_values = updates_by_row.setdefault(int(row_idx), {})
                        row_values.update(
                            _updates_list_to_column_values(headers, [item], target_row_index=int(row_idx))
                        )
                    for row_idx, values in updates_by_row.items():
                        if values:
                            _record_local_sheet_update(worksheet_name, row_idx, values)
            # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
            return True
        except gspread.exceptions.APIError as api_error:
            is_recoverable = _is_recoverable_auth_error(api_error)
            if attempt < max_attempts - 1:
                if is_recoverable:
                    st.warning(
                        f"🔁 Error de autenticación/cuota al actualizar Google Sheets "
                        f"(intento {attempt + 1}/{max_attempts}). Reintentando en {wait_seconds}s..."
                    )
                else:
                    st.warning(
                        f"⚠️ Error de la API de Google Sheets al actualizar celdas "
                        f"(intento {attempt + 1}/{max_attempts}). Reintentando en {wait_seconds}s..."
                    )
                time.sleep(wait_seconds)
                continue

            if is_recoverable:
                st.error(
                    "❌ No se pudo completar la actualización en Google Sheets por un error de autenticación/cuota "
                    "después de varios intentos."
                )
                handle_auth_error(api_error)
            else:
                st.error(f"❌ Error definitivo de la API de Google Sheets: {api_error}")
            break
        except RequestException as net_err:
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error de red al actualizar Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            st.error(
                "❌ No se pudo conectar con Google Sheets para actualizar los datos después de varios intentos. "
                "Verifica tu conexión o credenciales."
            )
            handle_auth_error(net_err)
            break
        except Exception as exc:
            if attempt < max_attempts - 1:
                st.warning(
                    f"⚠️ Error inesperado al actualizar Google Sheets (intento {attempt + 1}/{max_attempts}). "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            st.error(f"❌ Error inesperado al actualizar Google Sheets: {exc}")
            break

    return False


def mirror_guide_value(
    worksheet,
    headers,
    gsheet_row_index,
    df,
    df_idx,
    row,
    source_column,
    value,
):
    """Replica el valor de guía en la columna complementaria cuando está vacía."""

    if not value:
        return headers

    secondary_col = (
        "Hoja_Ruta_Mensajero" if source_column == "Adjuntos_Guia" else "Adjuntos_Guia"
    )

    existing_secondary = str(row.get(secondary_col, "")).strip()
    if existing_secondary:
        return headers

    if secondary_col not in headers:
        headers = ensure_columns(worksheet, headers, [secondary_col])

    if update_gsheet_cell(worksheet, headers, gsheet_row_index, secondary_col, value):
        df.at[df_idx, secondary_col] = value
        row[secondary_col] = value
        st.toast(
            "🔁 También se actualizó la columna complementaria de guías para mantener la compatibilidad.",
            icon="ℹ️",
        )

    return headers

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
INLINE_EXT = (".pdf", ".jpg", ".jpeg", ".png", ".webp")

def upload_file_to_s3(s3_client_param, bucket_name, file_obj, s3_key):
    try:
        # La visibilidad pública se controla mediante la bucket policy.
        # Sólo añadimos el ContentType si está disponible para evitar
        # errores "AccessControlListNotSupported".
        lower_key = s3_key.lower() if isinstance(s3_key, str) else ""
        is_inline = lower_key.endswith(INLINE_EXT)
        put_kwargs = {
            "Bucket": bucket_name,
            "Key": s3_key,
            "Body": file_obj.getvalue(),
            "ContentDisposition": "inline" if is_inline else "attachment",  # FORCE INLINE VIEW (PDF / IMAGES)
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
        params = {"Bucket": S3_BUCKET_NAME, "Key": clean_key}
        if isinstance(clean_key, str):
            lower_key = clean_key.lower()
            if lower_key.endswith(INLINE_EXT):
                filename = (clean_key.split("/")[-1] or "archivo").replace('"', "")
                params["ResponseContentDisposition"] = f'inline; filename="{filename}"'  # FORCE INLINE VIEW (PDF / IMAGES)
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


def resolve_storage_url(s3_client_param, value):
    """Return a usable URL for a stored value.

    If ``value`` already looks like an HTTP(S) URL, it is returned as-is.
    Otherwise it is treated as an S3 key and a presigned download link is
    generated via :func:`get_s3_file_download_url`.
    """
    if not value:
        return ""
    val = str(value).strip()
    scheme = urlparse(val).scheme
    if scheme in ("http", "https"):
        parsed = urlparse(val)
        s3_domains = (
            ".amazonaws.com",
            ".s3.amazonaws.com",
        )
        host = (parsed.netloc or "").lower()

        # If the sheet stored a direct S3 URL, regenerate a pre-signed URL
        # with inline headers so PDFs/images open in a browser tab.
        if any(domain in host for domain in s3_domains):
            return get_s3_file_download_url(s3_client_param, val)

        return val
    return get_s3_file_download_url(s3_client_param, val)


def _normalize_urls(value):
    """Return a list of URL-like strings parsed from ``value``.

    The helper accepts raw strings, JSON-encoded arrays/dicts, or iterables and
    returns a list without empty entries or duplicates.
    """

    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        candidates = list(value)
    else:
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return []
        except Exception:
            pass

        raw = str(value).strip()
        if not raw or raw.lower() in {"nan", "none", "n/a"}:
            return []

        candidates = []
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None

        if isinstance(parsed, (list, tuple)):
            candidates.extend(parsed)
        elif isinstance(parsed, dict):
            for key in ("url", "URL", "link", "href"):
                if parsed.get(key):
                    candidates.append(parsed[key])
        else:
            candidates.extend(p for p in re.split(r"[,\n;]+", raw) if p)

    normalized = []
    seen = set()
    for candidate in candidates:
        if isinstance(candidate, dict):
            for key in ("url", "URL", "link", "href"):
                val = candidate.get(key)
                if val and isinstance(val, str):
                    trimmed = val.strip()
                    if trimmed and trimmed not in seen:
                        seen.add(trimmed)
                        normalized.append(trimmed)
        else:
            if candidate is None:
                continue
            candidate_str = str(candidate).strip()
            if candidate_str and candidate_str not in seen:
                seen.add(candidate_str)
                normalized.append(candidate_str)

    return normalized


def _merge_uploaded_urls(existing_value, new_urls: Sequence[str]) -> str:
    """Merge existing stored URLs with new ones, preserving order and removing blanks."""

    existing_urls = _normalize_urls(existing_value)
    combined = list(existing_urls)
    for url in new_urls or []:
        if not url:
            continue
        combined.append(str(url).strip())

    seen = set()
    unique_urls = []
    for url in combined:
        if not url or url in seen:
            continue
        seen.add(url)
        unique_urls.append(url)

    return ", ".join(unique_urls)


def _is_row_empty(row: Any) -> bool:
    """Return True if ``row`` should be treated as empty."""

    if row is None:
        return True

    # Pandas objects (Series/DataFrame) expose ``empty`` to signal no values.
    empty_attr = getattr(row, "empty", None)
    if isinstance(empty_attr, bool):
        return empty_attr
    if callable(empty_attr):  # defensive: some objects expose empty() method
        try:
            return bool(empty_attr())
        except Exception:
            pass

    if isinstance(row, (list, tuple, set, dict)):
        return len(row) == 0

    return False


def pedido_requiere_guia(row: Any) -> bool:
    if _is_row_empty(row):
        return False

    if bool(row.get("requiere_guia")):
        return True

    comentario_val = row.get("Comentario")
    if comentario_requiere_guia(comentario_val):
        return True

    # 🔎 Detección nueva: dirección dentro del comentario
    comentario_norm = normalize_sheet_text(row.get("Comentario", ""))
    direccion_keywords = ["calle", "col.", "colonia", "av ", "avenida", "blvd", "cp", "c.p", "numero", "número"]
    if any(k in comentario_norm for k in direccion_keywords):
        return True

    # Detectar código postal de 5 dígitos dentro del comentario
    if re.search(r"\b\d{5}\b", comentario_norm):
        return True

    direccion_val = normalize_sheet_text(row.get("Direccion_Guia_Retorno", ""))
    return bool(direccion_val)


def pedido_tiene_guia_adjunta(row: Any) -> bool:
    if _is_row_empty(row):
        return False

    adjuntos = _normalize_urls(row.get("Adjuntos_Guia", ""))
    return len(adjuntos) > 0


def es_tab_solicitudes_guia(origen_tab: Any) -> bool:
    """Devuelve ``True`` cuando el contexto corresponde a Solicitudes de Guía."""

    normalized = str(origen_tab or "").strip().lower()
    return normalized in {
        "solicitudes",
        "solicitudes_guia",
        "solicitudes de guía",
        "solicitudes de guia",
        "📋 solicitudes de guía",
    }


def _mark_skip_demorado_check_once() -> None:
    """Evita el auto-cambio a Demorado en el rerun inmediato tras una acción manual."""

    st.session_state["skip_demorado_check_once"] = True


def _preserve_and_mark_skip_demorado() -> None:
    preserve_tab_state()
    _mark_skip_demorado_check_once()


def completar_pedido(
    df: pd.DataFrame,
    idx: int,
    row: Any,
    worksheet: Any,
    headers: list,
    gsheet_row_index: int,
    origen_tab: str,
    success_message: Optional[str] = None,
    trigger_rerun: bool = True,
) -> bool:
    """Marca un pedido como completado y preserva el estado visual - OPTIMIZADO."""

    estado_actual = str(row.get("Estado", "") or "").strip()
    if estado_actual != "🔵 En Proceso":
        st.warning(
            "⚠️ Para completar el pedido primero debe estar en **🔵 En Proceso**. "
            "Cambia el estado a en proceso antes de marcarlo como completado."
        )
        return False

    try:
        gsheet_row_index = int(gsheet_row_index)
    except (TypeError, ValueError):
        st.error(
            f"❌ No se puede completar el pedido '{row.get('ID_Pedido', '?')}' porque su índice de fila es inválido: {gsheet_row_index}."
        )
        return False

    if gsheet_row_index <= 0:
        st.error(
            f"❌ No se puede completar el pedido '{row.get('ID_Pedido', '?')}' porque su fila en Google Sheets no es válida."
        )
        return False

    try:
        estado_col_idx = headers.index("Estado") + 1
        fecha_completado_col_idx = headers.index("Fecha_Completado") + 1
    except ValueError as err:
        st.error(f"❌ No se puede completar el pedido porque falta la columna requerida: {err}")
        return False

    now = mx_now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 🚀 OPTIMIZACIÓN 1: Preservar scroll position ANTES de actualizar
    st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
    
    # 🚀 OPTIMIZACIÓN 2: Preservar pestañas activas ANTES de la actualización
    preserve_tab_state()

    updates = [
        {
            "range": gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
            "values": [["🟢 Completado"]],
        },
        {
            "range": gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
            "values": [[now_str]],
        },
    ]

    if not batch_update_gsheet_cells(worksheet, updates, headers=headers):
        st.error("❌ No se pudo completar el pedido.")
        return False

    # 🚀 OPTIMIZACIÓN 3: Actualizar el DataFrame localmente sin recargar desde GSheets
    df.loc[idx, "Estado"] = "🟢 Completado"
    df.loc[idx, "Fecha_Completado"] = now
    if isinstance(row, pd.Series):
        row["Estado"] = "🟢 Completado"
        row["Fecha_Completado"] = now

    st.session_state["expanded_pedidos"][row["ID_Pedido"]] = True
    st.session_state["expanded_attachments"][row["ID_Pedido"]] = True

    # 🚀 OPTIMIZACIÓN 4: Usar toast en lugar de success para feedback más rápido
    st.toast(success_message or f"✅ Pedido {row.get('ID_Pedido', '?')} completado", icon="✅")

    st.session_state["pedido_editado"] = row.get("ID_Pedido")
    st.session_state["fecha_seleccionada"] = row.get("Fecha_Entrega", "")
    st.session_state["subtab_local"] = origen_tab

    # 🚀 OPTIMIZACIÓN 5: NO limpiar toda la caché - esto es la causa principal de lentitud
    # st.cache_data.clear()  # ❌ REMOVIDO

    # 🚀 OPTIMIZACIÓN 6: Marcar contexto con scroll habilitado
    marcar_contexto_pedido(row.get("ID_Pedido"), origen_tab, scroll=False)
    try:
        df["Fecha_Completado"] = pd.to_datetime(df["Fecha_Completado"], errors="coerce")
    except:
        pass

    preserve_tab_state()
    st.session_state["reload_after_action"] = True
    if trigger_rerun:
        st.rerun()

    return True


# --- Helper Functions (existing in app.py) ---

def ordenar_pedidos_custom(df_pedidos_filtrados):
    """
    Ordena el DataFrame con:
    1. Modificación de Surtido
    2. Demorados
    3. Pendientes
    4. En Proceso
    En cada grupo se muestran primero los más antiguos y al final los más recientes.
    """
    if df_pedidos_filtrados.empty:
        return df_pedidos_filtrados

    df_pedidos_filtrados = df_pedidos_filtrados.copy()

    # Asegurar datetime para ordenar por antigüedad
    if 'Hora_Registro' in df_pedidos_filtrados.columns:
        df_pedidos_filtrados['Hora_Registro_dt'] = pd.to_datetime(
            df_pedidos_filtrados['Hora_Registro'], errors='coerce'
        )
    else:
        df_pedidos_filtrados['Hora_Registro_dt'] = pd.NaT

    if 'Fecha_Registro' in df_pedidos_filtrados.columns:
        df_pedidos_filtrados['Fecha_Registro_dt'] = pd.to_datetime(
            df_pedidos_filtrados['Fecha_Registro'], errors='coerce'
        )
    else:
        df_pedidos_filtrados['Fecha_Registro_dt'] = pd.NaT

    df_pedidos_filtrados['Fecha_Orden_dt'] = df_pedidos_filtrados['Hora_Registro_dt'].combine_first(
        df_pedidos_filtrados['Fecha_Registro_dt']
    )

    def get_sort_key(row):
        mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
        refact_tipo = str(row.get("Refacturacion_Tipo", "")).strip()
        estado = str(row.get("Estado", "")).strip()
        tiene_modificacion_sin_confirmar = (
            mod_texto and
            not mod_texto.endswith("[✔CONFIRMADO]") and
            refact_tipo != "Datos Fiscales"
        )

        fecha_orden = row.get('Fecha_Orden_dt')
        if pd.isna(fecha_orden):
            fecha_orden = pd.Timestamp.max

        if tiene_modificacion_sin_confirmar:
            return (0, fecha_orden)

        if estado == "🔴 Demorado":
            return (1, fecha_orden)

        if estado == "🟡 Pendiente":
            return (2, fecha_orden)

        if estado == "🔵 En Proceso":
            return (3, fecha_orden)

        return (4, fecha_orden)


    df_pedidos_filtrados['custom_sort_key'] = df_pedidos_filtrados.apply(get_sort_key, axis=1)

    df_sorted = df_pedidos_filtrados.sort_values(by='custom_sort_key', ascending=True)

    return df_sorted.drop(
        columns=['custom_sort_key', 'Hora_Registro_dt', 'Fecha_Registro_dt', 'Fecha_Orden_dt'],
        errors='ignore',
    )


def _render_paginated_iterrows(df_source: pd.DataFrame, view_key: str):
    """Limita el render por vista para evitar costos altos con miles de filas."""
    if df_source.empty:
        return []

    default_page_size = int(st.session_state.get("pedidos_page_size", 100))
    st.session_state["pedidos_page_size"] = default_page_size

    limit_key = f"visible_limit_{view_key}"
    if limit_key not in st.session_state:
        st.session_state[limit_key] = default_page_size

    total_rows = len(df_source)
    visible_limit = max(default_page_size, int(st.session_state.get(limit_key, default_page_size)))
    visible_limit = min(visible_limit, total_rows)

    if total_rows > default_page_size:
        st.caption(
            f"Mostrando {visible_limit} de {total_rows} pedidos en esta vista. "
            f"(límite base: {default_page_size})"
        )

    visible_df = df_source.head(visible_limit)
    remaining = total_rows - visible_limit
    if remaining > 0 and st.button(
        f"Cargar más ({remaining} restantes)",
        key=f"btn_load_more_{view_key}",
    ):
        st.session_state[limit_key] = min(total_rows, visible_limit + default_page_size)
        st.rerun()

    return enumerate(visible_df.iterrows(), start=1)

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
        if batch_update_gsheet_cells(worksheet, updates_to_perform, headers=headers):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            return df_to_check, changes_made
        else:
            st.error("❌ Falló la actualización por lotes a 'Demorado'.")
            return df_to_check, False

    return df_to_check, False


def marcar_contexto_pedido(row_id, origen_tab=None, *, scroll=True):
    """Prepara el estado de sesión para mantener el contexto de un pedido tras un rerun.

    Parameters
    ----------
    row_id : Any
        Identificador del pedido o caso cuyo contexto debe preservarse.
    origen_tab : str | None, optional
        Nombre de la pestaña secundaria para mantener la misma sección activa.
    scroll : bool, default True
        Si es ``True``, fija ``scroll_to_pedido_id`` para reposicionar la vista en la
        siguiente ejecución. Útil para callbacks que requieren reenfocar el pedido.
    """

    if origen_tab is not None:
        st.session_state["subtab_local"] = origen_tab

    expanded_pedidos = st.session_state.setdefault("expanded_pedidos", {})
    expanded_pedidos[row_id] = True

    expanded_subir_guia = st.session_state.setdefault("expanded_subir_guia", {})
    expanded_subir_guia[row_id] = True

    expanded_attachments = st.session_state.get("expanded_attachments")
    if isinstance(expanded_attachments, dict):
        expanded_attachments[row_id] = True

    if scroll:
        st.session_state["scroll_to_pedido_id"] = row_id
    preserve_tab_state()
    st.session_state["restore_tabs_after_print"] = True


def fijar_y_preservar(row, origen_tab):
    """Preserva pestañas y expansores antes de un posible rerun."""

    st.session_state["pedido_editado"] = row["ID_Pedido"]
    st.session_state["fecha_seleccionada"] = row.get("Fecha_Entrega", "")
    st.session_state["subtab_local"] = origen_tab

    # 🔄 Sincronizar la pestaña principal desde la URL antes de preservar
    tab_val = st.query_params.get("tab")
    if tab_val is not None:
        if isinstance(tab_val, list):
            tab_val = tab_val[0]
        try:
            st.session_state["active_main_tab_index"] = int(tab_val)
        except (ValueError, TypeError):
            st.session_state["active_main_tab_index"] = 0

    marcar_contexto_pedido(row["ID_Pedido"], origen_tab)


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
    st.query_params["local_tab"] = str(
        _clamp_tab_index(
            st.session_state.get("active_subtab_local_index", 0),
            _LOCAL_SUBTAB_OPTIONS,
        )
    )


def ensure_expanders_open(row_id: Any, *dict_names: str) -> None:
    """Marca los expanders indicados como abiertos en ``st.session_state``.

    Parameters
    ----------
    row_id : Any
        Identificador asociado al bloque que debe permanecer expandido.
    *dict_names : str
        Uno o más nombres de diccionarios en ``st.session_state`` que controlan
        el estado de expansión.
    """

    for dict_name in dict_names:
        if not dict_name:
            continue
        current_state = st.session_state.setdefault(dict_name, {})
        if isinstance(current_state, dict):
            current_state[row_id] = True
        else:
            st.session_state[dict_name] = {row_id: True}


def handle_generic_upload_change(
    row_id: Any,
    expander_dict_names: Sequence[str] | str | None,
    *,
    scroll_to_row: bool = True,
):
    """Mantiene expander y pestañas al seleccionar archivos.

    Parameters
    ----------
    row_id : Any
        Identificador de la fila asociada al pedido/caso.
    expander_dict_names : Sequence[str] | str | None
        Uno o varios nombres de diccionarios en ``st.session_state`` que
        controlan la expansión del elemento (por ejemplo ``"expanded_pedidos"``).
    scroll_to_row : bool, default True
        Si es ``True``, fija ``scroll_to_pedido_id`` para reenfocar la vista en la fila.
        Útil cuando se desea llevar al usuario al pedido tras un callback.
    """

    if isinstance(expander_dict_names, str) or expander_dict_names is None:
        names_to_update = [expander_dict_names] if expander_dict_names else []
    else:
        names_to_update = [name for name in expander_dict_names if name]

    ensure_expanders_open(row_id, *names_to_update)
    if scroll_to_row:
        st.session_state["scroll_to_pedido_id"] = row_id
    preserve_tab_state()
    # El script se vuelve a ejecutar automáticamente después de este callback,
    # así que evitamos una llamada explícita a st.rerun().


def render_guia_upload_feedback(
    placeholder,
    row_id,
    origen_tab,
    s3_client_param,
    *,
    ack_key: Optional[str] = None,
):
    """Muestra (y actualiza) el aviso de guías subidas sin forzar un rerun."""

    guia_success_map = st.session_state.setdefault("guia_upload_success", {})
    success_info = guia_success_map.get(row_id)

    placeholder.empty()

    if not success_info:
        return

    count = int(success_info.get("count") or 0)
    destino_col = success_info.get("column")
    destino_label = (
        "Hoja de Ruta del Mensajero"
        if destino_col == "Hoja_Ruta_Mensajero"
        else "Adjuntos de Guía"
    )
    plural = "archivo" if count == 1 else "archivos"
    files_info = success_info.get("files") or []
    timestamp = success_info.get("timestamp")

    with placeholder.container():
        st.success(
            f"📦 Se subieron correctamente {count} {plural} en {destino_label}."
        )
        if files_info:
            st.markdown("**Archivos guardados recientemente:**")
            for file_entry in files_info:
                raw_source = file_entry.get("key") or file_entry.get("url")
                display_name = file_entry.get("name") or os.path.basename(
                    str(raw_source or "").strip()
                )
                download_url = resolve_storage_url(s3_client_param, raw_source)
                if download_url:
                    st.markdown(
                        f"- <a href=\"{download_url}\" target=\"_blank\">{display_name}</a>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(f"- {display_name}")

        caption_parts = [
            "Las guías quedan guardadas inmediatamente; el botón 'Aceptar' solo cierra este aviso."
        ]
        if timestamp:
            caption_parts.append(f"Registro: {timestamp} (hora CDMX).")
        st.caption(" ".join(caption_parts))

        acknowledge_pressed = st.button(
            "Aceptar",
            key=ack_key or f"ack_guia_{row_id}",
        )
        if acknowledge_pressed:
            marcar_contexto_pedido(row_id, origen_tab, scroll=False)
            guia_success_map.pop(row_id, None)
            placeholder.empty()


def mostrar_confirmacion_completado_guia(
    row,
    df,
    idx,
    worksheet,
    headers,
    gsheet_row_index,
    origen_tab,
    *,
    success_message: Optional[str] = None,
):
    """Muestra confirmación antes de completar pedido tras subir guía."""

    if not es_tab_solicitudes_guia(origen_tab):
        return

    prompts = st.session_state.setdefault("confirm_complete_after_guide", {})
    if not prompts.get(row["ID_Pedido"]):
        return

    st.warning("¿Deseas marcar como completado este pedido al subir esta guía?")
    col_yes, col_cancel = st.columns(2)

    if col_yes.button(
        "Sí, completar pedido",
        key=f"confirm_complete_yes_{row['ID_Pedido']}",
        on_click=preserve_tab_state,
    ):
        completar_pedido(
            df,
            idx,
            row,
            worksheet,
            headers,
            gsheet_row_index,
            origen_tab,
            success_message or "✅ Pedido marcado como **🟢 Completado**.",
        )
        prompts.pop(row["ID_Pedido"], None)

    if col_cancel.button(
        "Cancelar",
        key=f"confirm_complete_cancel_{row['ID_Pedido']}",
        on_click=preserve_tab_state,
    ):
        prompts.pop(row["ID_Pedido"], None)
        st.info("Puedes completar el pedido manualmente desde el botón 🟢 Completar.")

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
    """Procesa el pedido: actualiza estado a 'En Proceso' sin alterar UI."""

    if col_print_btn.button(
        "⚙️ Procesar",
        key=f"procesar_{row['ID_Pedido']}_{origen_tab}",
        on_click=_mark_skip_demorado_check_once,
    ):
        # Solo para marcar que ya se presionó (si se usa para estilos/toasts)
        st.session_state.setdefault("printed_items", {})
        st.session_state["printed_items"][row["ID_Pedido"]] = True

        if row.get("Estado") in ["🟡 Pendiente", "🔴 Demorado"]:
            zona_mexico = timezone("America/Mexico_City")
            now = datetime.now(zona_mexico)
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")

            try:
                estado_col_idx = headers.index("Estado") + 1
                hora_proc_col_idx = headers.index("Hora_Proceso") + 1
            except ValueError:
                st.error(
                    "❌ No se encontraron las columnas 'Estado' y/o 'Hora_Proceso' en Google Sheets."
                )
            else:
                updates = [
                    {
                        "range": gspread.utils.rowcol_to_a1(
                            gsheet_row_index, estado_col_idx
                        ),
                        "values": [["🔵 En Proceso"]],
                    },
                    {
                        "range": gspread.utils.rowcol_to_a1(
                            gsheet_row_index, hora_proc_col_idx
                        ),
                        "values": [[now_str]],
                    },
                ]
                if batch_update_gsheet_cells(worksheet, updates, headers=headers):
                    df.at[idx, "Estado"] = "🔵 En Proceso"
                    df.at[idx, "Hora_Proceso"] = now_str
                    row["Estado"] = "🔵 En Proceso"
                    row["Hora_Proceso"] = now_str
                    st.toast("✅ Pedido marcado como 🔵 En Proceso", icon="✅")

                    # Mantener vista/pestaña sin forzar salto de scroll
                    marcar_contexto_pedido(row["ID_Pedido"], origen_tab, scroll=False)

                    preserve_tab_state()
                    st.session_state["reload_after_action"] = True
                    st.rerun()
                else:
                    st.error("❌ Falló la actualización del estado a 'En Proceso'.")
        else:
            st.toast("ℹ️ Este pedido ya no está en Pendiente/Demorado.", icon="ℹ️")

def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers, s3_client_param):
    """
    Displays a single order with its details, actions, and attachments.
    Includes logic for updating status, surtidor, notes, and handling attachments.
    """

    surtido_files_in_s3 = []  # ✅ Garantiza que la variable exista siempre
    pedido_folder_prefix = None  # ✅ Garantiza que esté definido aunque no se haya expandido adjuntos

    gsheet_row_index = row.get('_gsheet_row_index')
    if gsheet_row_index is None or str(gsheet_row_index).strip() == "":
        st.error(
            f"❌ No se puede operar el pedido '{row['ID_Pedido']}' porque no se encontró su fila en Google Sheets."
        )
        return
    try:
        gsheet_row_index = int(gsheet_row_index)
    except (TypeError, ValueError):
        st.error(
            f"❌ No se puede operar el pedido '{row['ID_Pedido']}' porque su índice de fila es inválido: {gsheet_row_index}."
        )
        return
    folio = row.get("Folio_Factura", "").strip() or row['ID_Pedido']
    guia_marker = "📋 " if pedido_sin_guia(row) else ""
    st.markdown(f'<a name="pedido_{row["ID_Pedido"]}"></a>', unsafe_allow_html=True)
    _render_bulk_selector(row)
    with st.expander(
        f"{guia_marker}{row['Estado']} - {folio} - {row['Cliente']}",
        expanded=st.session_state["expanded_pedidos"].get(row['ID_Pedido'], False),
    ):
        st.markdown("---")
        mod_texto = str(row.get("Modificacion_Surtido", "")).strip()
        hay_modificacion = mod_texto != ""

        es_local_no_entregado = es_pedido_local_no_entregado(row)
        tipo_envio_actual = row.get("Tipo_Envio")

        # --- Cambiar Fecha y Turno ---
        puede_cambiar_fecha = (
            tipo_envio_actual in ["📍 Pedido Local", "🚚 Pedido Foráneo"]
            and (
                row['Estado'] not in ["🟢 Completado", "✅ Viajó"]
                or es_local_no_entregado
            )
        )

        if puede_cambiar_fecha:
            # Muestra los controles solo cuando el usuario lo solicite, excepto
            # para pedidos locales completados sin entrega, donde deben estar
            # siempre visibles.
            if es_local_no_entregado:
                mostrar_cambio = True
            else:
                mostrar_cambio = st.checkbox(
                    "📅 Cambiar Fecha y Turno",
                    key=f"chk_fecha_{row['ID_Pedido']}_{idx}_{origen_tab}",
                )

            if mostrar_cambio:
                if es_local_no_entregado:
                    (
                        col_current_info_date,
                        col_current_info_turno,
                        col_estado_entrega,
                        _,
                    ) = st.columns([1, 1, 1.3, 1.7])
                else:
                    col_current_info_date, col_current_info_turno, _ = st.columns(
                        [1, 1, 2]
                    )
                    col_estado_entrega = None

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
                if tipo_envio_actual == "📍 Pedido Local":
                    col_current_info_turno.info(f"**Turno actual:** {current_turno}")
                else:
                    col_current_info_turno.empty()

                if es_local_no_entregado and col_estado_entrega is not None:
                    estado_entrega_valor = str(row.get("Estado_Entrega", "")).strip()
                    if estado_entrega_valor:
                        col_estado_entrega.info(
                            f"**Estado de entrega:** {estado_entrega_valor}"
                        )

                today = datetime.now().date()
                default_fecha = (
                    fecha_actual_dt.date()
                    if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today
                    else today
                )

                widget_suffix = f"{row['ID_Pedido']}_{idx}_{origen_tab}"

                fecha_key = f"new_fecha_{widget_suffix}"
                turno_key = f"new_turno_{widget_suffix}"

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

                if tipo_envio_actual == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
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
                    key=f"btn_apply_{widget_suffix}",
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

                    if tipo_envio_actual == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
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
                        if batch_update_gsheet_cells(worksheet, cambios, headers=headers):
                            if "Fecha_Entrega" in headers:
                                df.at[idx, "Fecha_Entrega"] = nueva_fecha_str
                            if (
                                "Turno" in headers
                                and tipo_envio_actual == "📍 Pedido Local"
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

        if es_local_no_entregado:
            estado_entrega_valor = str(row.get("Estado_Entrega", "")).strip()
            if estado_entrega_valor:
                st.markdown("##### 🚚 Estado de Entrega")
                st.info(
                    "Este pedido se reabrió porque está marcado como "
                    f"**{estado_entrega_valor}** en la bitácora de entrega."
                )

        direccion_retorno = str(row.get("Direccion_Guia_Retorno", "")).strip()

        if (
            (row.get("Tipo_Envio") == "🚚 Pedido Foráneo" or origen_tab == "Foráneo")
            and direccion_retorno
        ):
            st.markdown("📍 Dirección para guía de retorno:")
            st.info(direccion_retorno)


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



        if not es_local_no_entregado:
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
        else:
            col_print_btn.write("")



        # This block displays attachments inside an expander
        with st.expander(
            "📎 Archivos (Adjuntos y Guía)",
            expanded=True,
        ):
            st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")

            contenido_attachments = False
            sheet_attachments = _normalize_urls(row.get("Adjuntos", ""))
            sheet_attachment_keys = {
                extract_s3_key(att) for att in sheet_attachments if att
            }

            if sheet_attachments:
                contenido_attachments = True
                st.markdown("**Adjuntos registrados en la hoja:**")
                for attachment in sheet_attachments:
                    attachment_url = resolve_storage_url(s3_client_param, attachment)
                    parsed = urlparse(attachment)
                    display_name = os.path.basename(parsed.path) or attachment
                    if not display_name and attachment_url:
                        display_name = os.path.basename(urlparse(attachment_url).path)
                    if not display_name:
                        display_name = attachment
                    st.markdown(
                        f'- 📄 **{display_name}** (<a href="{attachment_url}" target="_blank">🔗 Ver/Descargar</a>)',
                        unsafe_allow_html=True,
                    )

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
                    filtered_files_to_display = [
                        f for f in filtered_files_to_display
                        if extract_s3_key(f['key']) not in sheet_attachment_keys
                    ]
                    if filtered_files_to_display:
                        contenido_attachments = True
                        st.markdown("**Adjuntos en carpeta S3:**")
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
                        if not contenido_attachments:
                            st.info("No hay adjuntos para mostrar (excluyendo comprobantes y surtidos).")
                else:
                    if not contenido_attachments:
                        st.info("No se encontraron archivos en la carpeta del pedido en S3.")
            elif not contenido_attachments:
                st.error(
                    f"❌ No se encontró la carpeta (prefijo S3) del pedido '{row['ID_Pedido']}'."
                )


        # Complete Button with streamlined confirmation
        if not es_local_no_entregado:
            requires = pedido_requiere_guia(row)
            has_file = pedido_tiene_guia_adjunta(row)
            is_tab_guias = es_tab_solicitudes_guia(origen_tab)

            if is_tab_guias:
                if col_complete_btn.button(
                    "🟢 Completar",
                    key=f"complete_button_{row['ID_Pedido']}_{origen_tab}",
                    disabled=disabled_if_completed,
                    on_click=_mark_skip_demorado_check_once,
                ):
                    if not has_file:
                        st.error(
                            "⚠️ Debes subir la guía antes de completar este pedido."
                        )
                    else:
                        completar_pedido(
                            df,
                            idx,
                            row,
                            worksheet,
                            headers,
                            gsheet_row_index,
                            origen_tab,
                        )
            elif not requires:
                if col_complete_btn.button(
                    "🟢 Completar",
                    key=f"complete_button_{row['ID_Pedido']}_{origen_tab}",
                    disabled=disabled_if_completed,
                    on_click=_mark_skip_demorado_check_once,
                ):
                    completar_pedido(
                        df,
                        idx,
                        row,
                        worksheet,
                        headers,
                        gsheet_row_index,
                        origen_tab,
                    )
            elif has_file:
                if col_complete_btn.button(
                    "🟢 Completar",
                    key=f"complete_button_{row['ID_Pedido']}_{origen_tab}",
                    disabled=disabled_if_completed,
                    on_click=_mark_skip_demorado_check_once,
                ):
                    completar_pedido(
                        df,
                        idx,
                        row,
                        worksheet,
                        headers,
                        gsheet_row_index,
                        origen_tab,
                    )
            else:
                flag_key = f"confirmar_completar_{row['ID_Pedido']}"
                if col_complete_btn.button(
                    "🟢 Completar",
                    key=f"complete_button_{row['ID_Pedido']}_{origen_tab}",
                    disabled=disabled_if_completed,
                    on_click=_mark_skip_demorado_check_once,
                ):
                    st.session_state[flag_key] = True

                if st.session_state.get(flag_key):
                    st.warning(
                        "⚠️ Este pedido requiere guía pero no se ha subido ninguna. ¿Quieres completarlo de todos modos?"
                    )

                    col1, col2 = st.columns(2)

                    if col1.button(
                        "📤 Subir guía primero",
                        key=f"btn_cancel_{row['ID_Pedido']}",
                    ):
                        st.session_state[flag_key] = False

                    if col2.button(
                        "🟢 Completar sin guía",
                        key=f"btn_force_complete_{row['ID_Pedido']}",
                        on_click=_mark_skip_demorado_check_once,
                    ):
                        completar_pedido(
                            df,
                            idx,
                            row,
                            worksheet,
                            headers,
                            gsheet_row_index,
                            origen_tab,
                        )
                        st.session_state[flag_key] = False
        else:
            col_complete_btn.write("")

                
        # ✅ BOTÓN PROCESAR MODIFICACIÓN - Solo para pedidos con estado 🛠 Modificación
        if row['Estado'] == "🛠 Modificación":
            col_process_mod = st.columns(1)[0]  # Crear columna para el botón
            if col_process_mod.button("🔧 Procesar Modificación", key=f"process_mod_{row['ID_Pedido']}_{origen_tab}"):
                try:
                    # 🚀 OPTIMIZACIÓN 1: Preservar scroll position ANTES de todo
                    st.session_state["scroll_to_pedido_id"] = row['ID_Pedido']
                    
                    # 🚀 OPTIMIZACIÓN 2: Usar función helper para preservar pestañas
                    preserve_tab_state()
                    
                    # ✅ Expandir el pedido
                    st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                    
                    # 🔄 Actualizar solo el estado a "En Proceso"
                    estado_col_idx = headers.index("Estado") + 1
                    updates = [
                        {'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx), 'values': [["🔵 En Proceso"]]}
                    ]
                    
                    if batch_update_gsheet_cells(worksheet, updates, headers=headers):
                        # 🚀 OPTIMIZACIÓN 3: Actualizar DataFrame localmente
                        df.at[idx, "Estado"] = "🔵 En Proceso"
                        row["Estado"] = "🔵 En Proceso"
                        
                        # 🚀 OPTIMIZACIÓN 4: Usar toast para feedback rápido
                        st.toast("🔧 Modificación procesada - Estado actualizado a 'En Proceso'", icon="✅")

                        # 🚀 OPTIMIZACIÓN 6: Marcar contexto sin salto de scroll
                        marcar_contexto_pedido(row["ID_Pedido"], origen_tab, scroll=False)

                        preserve_tab_state()
                        st.session_state["reload_after_action"] = True
                        st.rerun()
                    else:
                        st.error("❌ Falló la actualización del estado a 'En Proceso'.")
                        
                except Exception as e:
                    st.error(f"❌ Error al procesar la modificación: {e}")

        # --- Adjuntar archivos de guía ---
        if row['Estado'] not in ["🟢 Completado", "✅ Viajó"]:
            with st.expander(
                "📦 Subir Archivos de Guía",
                expanded=st.session_state["expanded_subir_guia"].get(row['ID_Pedido'], False),
            ):
                success_placeholder = st.empty()
                render_guia_upload_feedback(
                    success_placeholder,
                    row["ID_Pedido"],
                    origen_tab,
                    s3_client_param,
                )
                mostrar_confirmacion_completado_guia(
                    row,
                    df,
                    idx,
                    worksheet,
                    headers,
                    gsheet_row_index,
                    origen_tab,
                )

                upload_key = f"file_guia_{row['ID_Pedido']}"
                form_key = f"form_guia_{row['ID_Pedido']}"
                with st.form(key=form_key):
                    archivos_guia = st.file_uploader(
                        "📎 Subir guía(s) del pedido",
                        type=["pdf", "jpg", "jpeg", "png"],
                        accept_multiple_files=True,
                        key=upload_key,
                    )

                    submitted_upload = st.form_submit_button(
                        "📤 Subir Guía",
                        on_click=preserve_tab_state,
                    )

                if submitted_upload:
                    # 🚀 OPTIMIZACIÓN 1: Preservar posición de scroll INMEDIATAMENTE
                    st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
                    
                    handle_generic_upload_change(
                        row["ID_Pedido"],
                        ("expanded_pedidos", "expanded_subir_guia"),
                        scroll_to_row=True,  # 🚀 OPTIMIZACIÓN 2: Asegurar scroll
                    )

                    if archivos_guia:
                        # 🚀 OPTIMIZACIÓN 3: Mostrar progreso durante la subida
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        uploaded_keys = []
                        total_files = len(archivos_guia)
                        
                        for idx_file, archivo in enumerate(archivos_guia):
                            # 🚀 Actualizar progreso
                            progress_bar.progress((idx_file + 1) / total_files)
                            status_text.text(f"Subiendo archivo {idx_file + 1} de {total_files}...")
                            
                            ext = os.path.splitext(archivo.name)[1]
                            s3_key = f"{row['ID_Pedido']}/guia_{uuid.uuid4().hex[:6]}{ext}"
                            success, uploaded_key = upload_file_to_s3(
                                s3_client_param, S3_BUCKET_NAME, archivo, s3_key
                            )
                            if success and uploaded_key:
                                uploaded_keys.append(uploaded_key)
                        
                        # 🚀 Limpiar indicadores de progreso
                        progress_bar.empty()
                        status_text.empty()

                        if uploaded_keys:
                            uploaded_entries = [
                                {"key": key, "name": os.path.basename(key)}
                                for key in uploaded_keys
                            ]
                            tipo_envio_str = str(row.get("Tipo_Envio", "")).lower()
                            use_hoja_ruta = ("devol" in tipo_envio_str) or ("garant" in tipo_envio_str)
                            target_col_for_guide = (
                                "Hoja_Ruta_Mensajero" if use_hoja_ruta else "Adjuntos_Guia"
                            )
                            if target_col_for_guide not in headers:
                                headers = ensure_columns(worksheet, headers, [target_col_for_guide])
                            nueva_lista = _merge_uploaded_urls(
                                row.get(target_col_for_guide, ""),
                                uploaded_keys,
                            )
                            success = update_gsheet_cell(
                                worksheet, headers, gsheet_row_index, target_col_for_guide, nueva_lista
                            )
                            if success:
                                # 🚀 OPTIMIZACIÓN 4: Actualizar DataFrame localmente
                                if target_col_for_guide == "Hoja_Ruta_Mensajero":
                                    df.at[idx, "Hoja_Ruta_Mensajero"] = nueva_lista
                                    row["Hoja_Ruta_Mensajero"] = nueva_lista
                                else:
                                    df.at[idx, "Adjuntos_Guia"] = nueva_lista
                                    row["Adjuntos_Guia"] = nueva_lista

                                headers = mirror_guide_value(
                                    worksheet,
                                    headers,
                                    gsheet_row_index,
                                    df,
                                    idx,
                                    row,
                                    target_col_for_guide,
                                    nueva_lista,
                                )
                                # 🚀 OPTIMIZACIÓN 5: Feedback rápido con toast
                                st.toast(
                                    f"📤 {len(uploaded_keys)} guía(s) subida(s) con éxito.",
                                    icon="📦",
                                )
                                if es_tab_solicitudes_guia(origen_tab):
                                    prompts = st.session_state.setdefault(
                                        "confirm_complete_after_guide", {}
                                    )
                                    prompts[row["ID_Pedido"]] = True
                                ensure_expanders_open(
                                    row["ID_Pedido"],
                                    "expanded_pedidos",
                                    "expanded_subir_guia",
                                )
                                # 🚀 OPTIMIZACIÓN 6: Guardar info de éxito
                                guia_success_map = st.session_state.setdefault(
                                    "guia_upload_success", {}
                                )
                                guia_success_map[row["ID_Pedido"]] = {
                                    "count": len(uploaded_keys),
                                    "column": target_col_for_guide,
                                    "files": uploaded_entries,
                                    "timestamp": mx_now_str(),
                                }
                                # 🚀 OPTIMIZACIÓN 7: NO limpiar cache - Causa de lentitud
                                # st.cache_data.clear()  # ❌ REMOVIDO
                                # st.cache_resource.clear()  # ❌ REMOVIDO
                                
                                st.session_state["pedido_editado"] = row["ID_Pedido"]
                                st.session_state["fecha_seleccionada"] = row.get(
                                    "Fecha_Entrega", ""
                                )
                                st.session_state["subtab_local"] = origen_tab
                                tab_val = st.query_params.get("tab")
                                if tab_val is not None:
                                    if isinstance(tab_val, list):
                                        tab_val = tab_val[0]
                                    try:
                                        st.session_state["active_main_tab_index"] = int(tab_val)
                                    except (ValueError, TypeError):
                                        st.session_state["active_main_tab_index"] = 0
                                # 🚀 OPTIMIZACIÓN 8: Marcar contexto con scroll
                                marcar_contexto_pedido(row["ID_Pedido"], origen_tab, scroll=True)
                                preserve_tab_state()
                                render_guia_upload_feedback(
                                    success_placeholder,
                                    row["ID_Pedido"],
                                    origen_tab,
                                    s3_client_param,
                                )
                                mostrar_confirmacion_completado_guia(
                                    row,
                                    df,
                                    idx,
                                    worksheet,
                                    headers,
                                    gsheet_row_index,
                                    origen_tab,
                                )
                            else:
                                st.error(
                                    "❌ No se pudo actualizar el Google Sheet con los archivos de guía."
                                )
                                st.warning(
                                    "⚠️ Los archivos sí se subieron a S3, pero no quedaron registrados en el sheet. "
                                    "Copia estos enlaces para registrarlos manualmente si es necesario:"
                                )
                                for uploaded_key in uploaded_keys:
                                    st.code(uploaded_key)
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
                    if st.button(
                        "✅ Confirmar Cambios de Surtido",
                        key=f"confirm_mod_{row['ID_Pedido']}_{idx}_{origen_tab}",
                    ):
                        st.session_state["expanded_pedidos"][row['ID_Pedido']] = True
                        st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
                        nuevo_texto = mod_texto + " [✔CONFIRMADO]"
                        success = update_gsheet_cell(
                            worksheet, headers, gsheet_row_index, "Modificacion_Surtido", nuevo_texto
                        )
                        if success and "Estado" in headers:
                            success = update_gsheet_cell(
                                worksheet,
                                headers,
                                gsheet_row_index,
                                "Estado",
                                "🔵 En Proceso",
                            )
                        if success:
                            row["Estado"] = "🔵 En Proceso"
                            st.success("✅ Cambios de surtido confirmados y pedido en '🔵 En Proceso'.")
                            st.cache_data.clear()
                            marcar_contexto_pedido(row["ID_Pedido"], origen_tab)
                            st.rerun()
                        else:
                            st.error("❌ No se pudo confirmar la modificación.")
                
                # Mostrar info adicional si es refacturación por material
                if refact_tipo == "Material":
                    st.markdown("#### 🔁 Refacturación por Material")
                    st.info(f"📌 Tipo: **{refact_tipo}**  \n🔧 Subtipo: **{refact_subtipo}**")

            # ℹ️ Si es refacturación por Datos Fiscales
            elif refact_tipo == "Datos Fiscales":
                st.info(
                    "ℹ️ Esta modificación fue marcada como **Datos Fiscales**. "
                    "Se muestra como referencia, pero puedes confirmarla para pasar a **🔵 En Proceso**."
                )
                if mod_texto:
                    if mod_texto.endswith("[✔CONFIRMADO]"):
                        st.info(f"✉️ Modificación (Datos Fiscales):\n{mod_texto}")
                    else:
                        st.warning(f"✉️ Modificación (Datos Fiscales):\n{mod_texto}")
                        if st.button(
                            "✅ Confirmar Cambios de Surtido",
                            key=f"confirm_mod_df_{row['ID_Pedido']}_{idx}_{origen_tab}",
                        ):
                            st.session_state["expanded_pedidos"][row["ID_Pedido"]] = True
                            st.session_state["scroll_to_pedido_id"] = row["ID_Pedido"]
                            nuevo_texto = mod_texto + " [✔CONFIRMADO]"
                            success = update_gsheet_cell(
                                worksheet,
                                headers,
                                gsheet_row_index,
                                "Modificacion_Surtido",
                                nuevo_texto,
                            )
                            if success and "Estado" in headers:
                                success = update_gsheet_cell(
                                    worksheet,
                                    headers,
                                    gsheet_row_index,
                                    "Estado",
                                    "🔵 En Proceso",
                                )
                            if success:
                                row["Estado"] = "🔵 En Proceso"
                                st.success(
                                    "✅ Cambios de surtido confirmados y pedido en '🔵 En Proceso'."
                                )
                                st.cache_data.clear()
                                marcar_contexto_pedido(row["ID_Pedido"], origen_tab)
                                st.rerun()
                            else:
                                st.error("❌ No se pudo confirmar la modificación.")

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

            adjuntos_surtido_urls = _normalize_urls(row.get("Adjuntos_Surtido", ""))
            hay_adjuntos_texto = bool(mod_surtido_archivos_mencionados_raw)
            hay_adjuntos_s3 = bool(surtido_files_in_s3)
            hay_adjuntos_campo = bool(adjuntos_surtido_urls)

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

            for raw_url in adjuntos_surtido_urls:
                resolved_url = resolve_storage_url(s3_client_param, raw_url)
                if not resolved_url:
                    continue

                original_path = urlparse(raw_url).path
                resolved_path = urlparse(resolved_url).path
                base_name = os.path.basename(original_path) or os.path.basename(resolved_path) or raw_url
                base_name = unquote(base_name)

                all_surtido_related_files.append({
                    'title': base_name,
                    'url': resolved_url,
                })

            if all_surtido_related_files:
                st.markdown("Adjuntos de Modificación (Surtido/Relacionados):")
                archivos_ya_mostrados_para_mod = set()

                for file_info in all_surtido_related_files:
                    file_name_to_display = file_info.get('title')
                    if not file_name_to_display:
                        continue

                    if file_name_to_display in archivos_ya_mostrados_para_mod:
                        continue

                    object_key_to_download = file_info.get('key', '')
                    final_url = file_info.get('url')

                    try:
                        if not final_url:
                            if object_key_to_download and not object_key_to_download.startswith(S3_ATTACHMENT_PREFIX) and pedido_folder_prefix:
                                object_key_to_download = f"{pedido_folder_prefix}{file_name_to_display}"

                            if object_key_to_download:
                                if not pedido_folder_prefix and not object_key_to_download.startswith(S3_ATTACHMENT_PREFIX) and not object_key_to_download.startswith(S3_BUCKET_NAME):
                                    st.warning(f"⚠️ No se pudo determinar la ruta S3 para: {file_name_to_display}")
                                    continue

                                final_url = get_s3_file_download_url(s3_client_param, object_key_to_download)

                        if final_url and final_url != "#":
                            st.markdown(
                                f'- 📄 <a href="{final_url}" target="_blank">{file_name_to_display}</a>',
                                unsafe_allow_html=True,
                            )
                        else:
                            st.warning(f"⚠️ No se pudo generar el enlace para: {file_name_to_display}")
                    except Exception as e:
                        st.warning(f"⚠️ Error al procesar adjunto de modificación '{file_name_to_display}': {e}")

                    archivos_ya_mostrados_para_mod.add(file_name_to_display)
            else:
                if not (hay_adjuntos_texto or hay_adjuntos_s3 or hay_adjuntos_campo):
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

    _clear_offscreen_pedido_flags(st.session_state.get("pedidos_en_pantalla", set()))
    _clear_offscreen_guide_flags(st.session_state.get("pedidos_en_pantalla", set()))

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
    _render_bulk_selector(row)

    # Expander simple con info básica (sin acciones extra)
    guia_marker = "📋 " if pedido_sin_guia(row) else ""
    with st.expander(
        f"{guia_marker}{row['Estado']} - {folio} - {row.get('Cliente','')}",
        expanded=True,
    ):
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

        success_placeholder = st.empty()
        render_guia_upload_feedback(
            success_placeholder,
            row["ID_Pedido"],
            origen_tab,
            s3_client_param,
            ack_key=f"ack_guia_only_{row['ID_Pedido']}",
        )
        mostrar_confirmacion_completado_guia(
            row,
            df,
            idx,
            worksheet,
            headers,
            gsheet_row_index,
            origen_tab,
        )

        # Uploader siempre visible (sin expander)
        upload_key = f"file_guia_only_{row['ID_Pedido']}"
        form_key = f"form_guia_only_{row['ID_Pedido']}"
        with st.form(key=form_key):
            archivos_guia = st.file_uploader(
                "📎 Subir guía(s) del pedido",
                type=["pdf", "jpg", "jpeg", "png"],
                accept_multiple_files=True,
                key=upload_key,
            )

            submitted_upload = st.form_submit_button(
                "📤 Subir Guía",
                on_click=preserve_tab_state,
            )

        if submitted_upload:
            handle_generic_upload_change(
                row["ID_Pedido"], ("expanded_pedidos",)
            )
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
                    uploaded_entries = [
                        {"key": key, "name": os.path.basename(key)}
                        for key in uploaded_keys
                    ]
                    nueva_lista = _merge_uploaded_urls(
                        row.get("Adjuntos_Guia", ""),
                        uploaded_keys,
                    )
                    success = update_gsheet_cell(
                        worksheet, headers, gsheet_row_index, "Adjuntos_Guia", nueva_lista
                    )
                    if success:
                        df.at[idx, "Adjuntos_Guia"] = nueva_lista
                        row["Adjuntos_Guia"] = nueva_lista
                        headers = mirror_guide_value(
                            worksheet,
                            headers,
                            gsheet_row_index,
                            df,
                            idx,
                            row,
                            "Adjuntos_Guia",
                            nueva_lista,
                        )
                        st.toast(
                            f"📤 {len(uploaded_keys)} guía(s) subida(s) con éxito.",
                            icon="📦",
                        )
                        if es_tab_solicitudes_guia(origen_tab):
                            prompts = st.session_state.setdefault(
                                "confirm_complete_after_guide", {}
                            )
                            prompts[row["ID_Pedido"]] = True
                        ensure_expanders_open(
                            row["ID_Pedido"],
                            "expanded_pedidos",
                        )
                        guia_success_map = st.session_state.setdefault(
                            "guia_upload_success", {}
                        )
                        guia_success_map[row["ID_Pedido"]] = {
                            "count": len(uploaded_keys),
                            "column": "Adjuntos_Guia",
                            "files": uploaded_entries,
                            "timestamp": mx_now_str(),
                        }
                        st.cache_data.clear()
                        marcar_contexto_pedido(row["ID_Pedido"], origen_tab)
                        preserve_tab_state()
                        render_guia_upload_feedback(
                            success_placeholder,
                            row["ID_Pedido"],
                            origen_tab,
                            s3_client_param,
                            ack_key=f"ack_guia_only_{row['ID_Pedido']}",
                        )
                        mostrar_confirmacion_completado_guia(
                            row,
                            df,
                            idx,
                            worksheet,
                            headers,
                            gsheet_row_index,
                            origen_tab,
                        )
                    else:
                        st.error(
                            "❌ No se pudo actualizar el Google Sheet con los archivos de guía."
                        )
                        st.warning(
                            "⚠️ Los archivos sí se subieron a S3, pero no quedaron registrados en el sheet. "
                            "Copia estos enlaces para registrarlos manualmente si es necesario:"
                        )
                        for uploaded_key in uploaded_keys:
                            st.code(uploaded_key)
                else:
                    st.warning("⚠️ No se subió ningún archivo válido.")

        requires = pedido_requiere_guia(row)
        has_file = pedido_tiene_guia_adjunta(row)
        is_tab_guias = es_tab_solicitudes_guia(origen_tab)

        if st.button(
            "🟢 Completar",
            key=f"btn_completar_only_{row['ID_Pedido']}",
            on_click=_preserve_and_mark_skip_demorado,
        ):
            if is_tab_guias and not has_file:
                st.error("⚠️ Debes subir la guía antes de completar este pedido.")
            elif is_tab_guias:
                completar_pedido(
                    df,
                    idx,
                    row,
                    worksheet,
                    headers,
                    gsheet_row_index,
                    origen_tab,
                    "✅ Pedido marcado como **🟢 Completado**.",
                )
            elif not requires:
                completar_pedido(
                    df,
                    idx,
                    row,
                    worksheet,
                    headers,
                    gsheet_row_index,
                    origen_tab,
                    "✅ Pedido marcado como **🟢 Completado**.",
                )
            elif has_file:
                completar_pedido(
                    df,
                    idx,
                    row,
                    worksheet,
                    headers,
                    gsheet_row_index,
                    origen_tab,
                    "✅ Pedido marcado como **🟢 Completado**.",
                )
            else:
                flag_key = f"confirmar_completar_{row['ID_Pedido']}"
                st.session_state[flag_key] = True

        flag_key = f"confirmar_completar_{row['ID_Pedido']}"
        if st.session_state.get(flag_key):
            st.warning(
                "⚠️ Este pedido requiere guía pero no se ha subido ninguna. ¿Quieres completarlo de todos modos?"
            )

            col1, col2 = st.columns(2)

            if col1.button(
                "📤 Subir guía primero",
                key=f"btn_cancel_{row['ID_Pedido']}",
                on_click=preserve_tab_state,
            ):
                st.session_state[flag_key] = False

            if col2.button(
                "🟢 Completar sin guía",
                key=f"btn_force_complete_{row['ID_Pedido']}",
                on_click=_preserve_and_mark_skip_demorado,
            ):
                completar_pedido(
                    df,
                    idx,
                    row,
                    worksheet,
                    headers,
                    gsheet_row_index,
                    origen_tab,
                    "✅ Pedido marcado como **🟢 Completado**.",
                )
                st.session_state[flag_key] = False

    _clear_offscreen_pedido_flags(st.session_state.get("pedidos_en_pantalla", set()))
    _clear_offscreen_guide_flags(st.session_state.get("pedidos_en_pantalla", set()))

# --- Main Application Logic ---

def _load_pedidos():
    df, headers = get_filtered_sheet_dataframe(
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name=GOOGLE_SHEET_WORKSHEET_NAME,
        client=g_spread_client,
        light_mode=True,
    )
    df = _apply_local_sheet_updates(df, GOOGLE_SHEET_WORKSHEET_NAME)
    # Re-filtrar después de aplicar updates locales para reflejar de inmediato
    # cuando un pedido se marca como limpiado/completado en la sesión actual.
    df = _filter_relevant_pedidos(df, headers, GOOGLE_SHEET_WORKSHEET_NAME)
    return df, headers

def _load_casos():
    df, headers = get_filtered_sheet_dataframe(
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name="casos_especiales",
        client=g_spread_client,
        light_mode=False,
    )
    return _apply_local_sheet_updates(df, "casos_especiales"), headers


# 🔁 Rerun ligero después de acciones (Procesar/Completar)
if st.session_state.pop("reload_after_action", False):
    # Mantenemos rerun ligero; los cambios ya se reflejan por actualización local en sesión.
    pass

if st.session_state.pop("refresh_data_caches_pending", False):
    get_raw_sheet_data.clear()
    get_filtered_sheet_dataframe.clear()

if st.session_state.get("need_compare"):
    prev_pedidos = st.session_state.get("prev_pedidos_count", 0)
    prev_casos = st.session_state.get("prev_casos_count", 0)
    for attempt in range(3):
        get_raw_sheet_data.clear()
        get_filtered_sheet_dataframe.clear()
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
    "Turno", "Fecha_Entrega", "Modificacion_Surtido", "Estado_Entrega"
]
headers_main = ensure_columns(worksheet_main, headers_main, required_cols_main)

# Y asegura que el DataFrame también tenga esas columnas en esta ejecución
for col in required_cols_main:
    if col not in df_main.columns:
        df_main[col] = ""

skip_demorado_check_once = st.session_state.pop("skip_demorado_check_once", False)

if not df_main.empty:
    if not skip_demorado_check_once:
        df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
        if changes_made_by_demorado_check:
            st.cache_data.clear()

            set_active_main_tab(st.session_state.get("active_main_tab_index", 0))
            st.session_state["active_subtab_local_index"] = st.session_state.get("active_subtab_local_index", 0)
            st.session_state["active_date_tab_m_index"] = st.session_state.get("active_date_tab_m_index", 0)
            st.session_state["active_date_tab_t_index"] = st.session_state.get("active_date_tab_t_index", 0)

            st.rerun()

    # --- 🔔 Alerta de Modificación de Surtido ---
    mod_surtido_main_df = _pending_modificaciones(df_main)
    mod_surtido_casos_df = _pending_modificaciones(df_casos)

    mod_surtido_count = len(mod_surtido_main_df) + len(mod_surtido_casos_df)

    if mod_surtido_count > 0:
        ubicaciones = collect_tab_locations(mod_surtido_main_df) + collect_tab_locations(mod_surtido_casos_df)
        ubicaciones_unicas = sorted(set(ubicaciones))
        ubicaciones_str = ", ".join(ubicaciones_unicas) if ubicaciones_unicas else _UNKNOWN_TAB_LABEL

        st.warning(
            f"⚠️ Hay {mod_surtido_count} pedido(s) con **Modificación de Surtido** ➤ {ubicaciones_str}"
        )

    estados_visibles = ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado", "🛠 Modificación", "✏️ Modificación"]
    mask_estados_activos = df_main["Estado"].isin(estados_visibles)
    estado_entrega_series = df_main.get("Estado_Entrega")
    if estado_entrega_series is not None:
        estado_entrega_normalizado = estado_entrega_series.astype(str).str.strip()
    else:
        estado_entrega_normalizado = pd.Series([""] * len(df_main))
    mask_local_no_entregado = (
        (df_main["Estado"] == "🟢 Completado")
        & (df_main["Tipo_Envio"] == "📍 Pedido Local")
        & (estado_entrega_normalizado == "⏳ No Entregado")
    )
    df_pendientes_proceso_demorado = df_main[mask_estados_activos | mask_local_no_entregado].copy()

    st.session_state["pedidos_en_pantalla"] = set(
        df_pendientes_proceso_demorado.get("ID_Pedido", pd.Series(dtype=str))
        .astype(str)
        .str.strip()
    )

    # Limpieza preventiva de banderas de confirmación ligadas a pedidos que ya no están visibles
    _clear_offscreen_pedido_flags(st.session_state.get("pedidos_en_pantalla", set()))
    _clear_offscreen_guide_flags(st.session_state.get("pedidos_en_pantalla", set()))
    _cleanup_bulk_selection(st.session_state.get("pedidos_en_pantalla", set()))

    df_demorados_activos = df_pendientes_proceso_demorado[
        df_pendientes_proceso_demorado["Estado"] == "🔴 Demorado"
    ].copy()

    if not df_demorados_activos.empty:
        ubicaciones_demorados = collect_tab_locations(df_demorados_activos)
        ubicaciones_text = ", ".join(ubicaciones_demorados)
        total_demorados = len(df_demorados_activos)
        st.warning(
            f"⏱️ Hay {total_demorados} pedido{'s' if total_demorados != 1 else ''} en estado 🔴 Demorado ubicados en: {ubicaciones_text}"
        )

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
        "Direccion_Guia_Retorno", "Direccion_Envio",
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
    fill_empty_cols = [
        "Numero_Serie",
        "Fecha_Compra",
        "Completados_Limpiado",
        "Direccion_Guia_Retorno",
        "Direccion_Envio",
    ]
    for c in fill_empty_cols:
        if c not in df_casos.columns:
            df_casos[c] = ""
        else:
            df_casos[c] = df_casos[c].fillna("")

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
            '✏️ Modificación': (df["Estado"] == '✏️ Modificación').sum(),
            '🟣 Cancelado': len(cancelados_visible),
            '🟢 Completado': len(completados_visible),
        }

    counts_main = _count_states(df_main)
    counts_casos = _count_states(df_casos)
    estado_counts = {k: counts_main.get(k, 0) + counts_casos.get(k, 0)
                     for k in ['🟡 Pendiente', '🔵 En Proceso', '🔴 Demorado', '🛠 Modificación', '✏️ Modificación', '🟣 Cancelado', '🟢 Completado']}

    total_pedidos_estados = sum(estado_counts.values())
    estados_fijos = ['🟡 Pendiente', '🔵 En Proceso', '🟢 Completado']
    estados_condicionales = ['🔴 Demorado', '🛠 Modificación', '✏️ Modificación', '🟣 Cancelado']
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

    # 🔔 Aviso de devoluciones/garantías con seguimiento pendiente
    tipo_casos_col = "Tipo_Caso" if "Tipo_Caso" in df_casos.columns else (
        "Tipo_Envio" if "Tipo_Envio" in df_casos.columns else None
    )

    devoluciones_activas = pd.DataFrame(columns=df_casos.columns)
    garantias_activas = pd.DataFrame(columns=df_casos.columns)

    if tipo_casos_col and "Estado" in df_casos.columns:
        estados_activos = ["🟡 Pendiente"]
        estados_series = df_casos["Estado"].astype(str).str.strip()
        tipo_series = df_casos[tipo_casos_col].astype(str)
        base_mask = estados_series.isin(estados_activos)
        devoluciones_activas = df_casos[
            base_mask & tipo_series.str.contains("Devoluci", case=False, na=False)
        ]
        garantias_activas = df_casos[
            base_mask & tipo_series.str.contains("Garant", case=False, na=False)
        ]

    devoluciones_count = len(devoluciones_activas)
    garantias_count = len(garantias_activas)

    if devoluciones_count or garantias_count:
        partes_mensaje = []
        if devoluciones_count:
            partes_mensaje.append(
                f"{devoluciones_count} devolución{'es' if devoluciones_count != 1 else ''}"
            )
        if garantias_count:
            partes_mensaje.append(
                f"{garantias_count} garantía{'s' if garantias_count != 1 else ''}"
            )
        lista_casos = " y ".join(partes_mensaje)
        st.warning(
            f"⚠️ Hay {lista_casos} en estado pendiente en Casos Especiales."
        )

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

    col_bulk_btn, col_bulk_info = st.columns([1.2, 2.8])
    bulk_mode_active = st.session_state.get("bulk_complete_mode", False)
    selected_bulk_ids = _get_bulk_selected_ids()
    selected_bulk_count = len(selected_bulk_ids)
    has_multiple_selected = selected_bulk_count > 1

    if col_bulk_btn.button(
        "🟢 Completar Pedidos" if bulk_mode_active else "✅ Completar Varios",
        key="btn_toggle_bulk_complete_mode",
        disabled=(bulk_mode_active and not has_multiple_selected),
    ):
        if not bulk_mode_active:
            _set_bulk_mode(True)
        else:
            pedidos_lookup = (
                df_pendientes_proceso_demorado
                .set_index("ID_Pedido", drop=False)
                if "ID_Pedido" in df_pendientes_proceso_demorado.columns
                else pd.DataFrame()
            )

            pedidos_a_completar = []
            for pedido_id in sorted(selected_bulk_ids):
                if pedido_id not in pedidos_lookup.index:
                    continue
                row_to_complete = pedidos_lookup.loc[pedido_id]
                if isinstance(row_to_complete, pd.DataFrame):
                    row_to_complete = row_to_complete.iloc[0]
                if str(row_to_complete.get("Estado", "")).strip() != "🔵 En Proceso":
                    continue
                pedidos_a_completar.append(row_to_complete)

            if len(pedidos_a_completar) <= 1:
                st.warning("⚠️ Selecciona más de un pedido en proceso para completar varios.")
            else:
                completados_ok = 0
                preserve_tab_state()
                _mark_skip_demorado_check_once()
                for pedido_row in pedidos_a_completar:
                    pedido_id = str(pedido_row.get("ID_Pedido", "")).strip()
                    row_idx_list = df_main.index[
                        df_main.get("ID_Pedido", pd.Series(dtype=str)).astype(str).str.strip() == pedido_id
                    ].tolist()
                    if not row_idx_list:
                        continue
                    df_idx = row_idx_list[0]
                    gsheet_row_index = pedido_row.get("_gsheet_row_index")
                    if gsheet_row_index is None:
                        continue

                    if completar_pedido(
                        df_main,
                        df_idx,
                        pedido_row,
                        worksheet_main,
                        headers_main,
                        gsheet_row_index,
                        "bulk_multi",
                        success_message=f"✅ Pedido {pedido_id} completado",
                        trigger_rerun=False,
                    ):
                        completados_ok += 1

                _set_bulk_mode(False)
                if completados_ok > 0:
                    st.success(f"✅ Se completaron {completados_ok} pedidos.")
                    st.session_state["reload_after_action"] = True
                    st.rerun()
                else:
                    st.warning("⚠️ No se completó ningún pedido seleccionado.")

    if bulk_mode_active:
        if has_multiple_selected:
            col_bulk_info.success(
                f"Modo múltiple activo: {selected_bulk_count} pedidos seleccionados en proceso."
            )
        else:
            col_bulk_info.info(
                "Selecciona más de un pedido en proceso para habilitar **Completar Pedidos**."
            )
    else:
        col_bulk_info.caption(
            "Activa **Completar Varios** para mostrar checks fuera del expander en pedidos en proceso."
        )

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
            const query = params.toString();
            const base = window.parent.location.origin + window.parent.location.pathname;
            const newUrl = query ? `${{base}}?${{query}}` : base;
            window.parent.history.replaceState(null, '', newUrl);
        }});
    }});
    </script>
    """, height=0)

    with main_tabs[0]: # 📍 Pedidos Locales
        st.markdown("### 📋 Pedidos Locales")
        subtab_options_local = _LOCAL_SUBTAB_OPTIONS

        subtabs_local = st.tabs(subtab_options_local)

        local_tabs_script = f"""
        <script>
        (function() {{
            const expectedLabels = {json.dumps(subtab_options_local)};
            const tabGroups = window.parent.document.querySelectorAll('.stTabs');
            let targetGroup = null;
            tabGroups.forEach(group => {{
                if (targetGroup) {{
                    return;
                }}
                const tabs = group.querySelectorAll('[data-baseweb="tab"]');
                if (tabs.length !== expectedLabels.length) {{
                    return;
                }}
                const labels = Array.from(tabs, tab => tab.textContent.trim());
                const matches = expectedLabels.every(label => labels.includes(label));
                if (matches) {{
                    targetGroup = group;
                }}
            }});
            if (!targetGroup) {{
                return;
            }}
            const localTabs = targetGroup.querySelectorAll('[data-baseweb="tab"]');
            const activeIndex = {st.session_state.get("active_subtab_local_index", 0)};
            if (localTabs[activeIndex]) {{
                localTabs[activeIndex].click();
            }}
            localTabs.forEach((tab, idx) => {{
                tab.addEventListener('click', () => {{
                    const params = new URLSearchParams(window.parent.location.search);
                    params.set('local_tab', idx);
                    const base = window.parent.location.origin + window.parent.location.pathname;
                    const query = params.toString();
                    const newUrl = query ? `${{base}}?${{query}}` : base;
                    window.parent.history.replaceState(null, '', newUrl);
                }});
            }});
        }})();
        </script>
        """
        components.html(local_tabs_script, height=0)

        with subtabs_local[0]: # 🌅 Mañana
            pedidos_m_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "☀️ Local Mañana")
            ].copy()
            if not pedidos_m_display.empty:
                pedidos_m_display['Fecha_Entrega_dt'] = pd.to_datetime(
                    pedidos_m_display['Fecha_Entrega'],
                    errors='coerce'
                )
                estado_entrega_m = (
                    pedidos_m_display.get("Estado_Entrega", pd.Series(dtype=str))
                    .astype(str)
                    .str.strip()
                )
                mask_no_entregado_m = (
                    (pedidos_m_display["Estado"] == "🟢 Completado")
                    & (estado_entrega_m == "⏳ No Entregado")
                )
                pedidos_m_no_entregado = pedidos_m_display[mask_no_entregado_m].copy()
                pedidos_m_activos = pedidos_m_display[~mask_no_entregado_m].copy()
                fechas_unicas_dt = sorted(
                    pedidos_m_activos["Fecha_Entrega_dt"].dropna().unique()
                )

                if fechas_unicas_dt or not pedidos_m_no_entregado.empty:
                    date_tab_labels = [
                        f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}"
                        for fecha in fechas_unicas_dt
                    ]
                    if not pedidos_m_no_entregado.empty:
                        date_tab_labels = (
                            [_LOCAL_NO_ENTREGADOS_TAB_LABEL] + date_tab_labels
                        )

                    saved_label = st.session_state.get("active_date_tab_m_label", "")
                    fallback_index = (
                        date_tab_labels.index(saved_label)
                        if saved_label in date_tab_labels
                        else _clamp_tab_index(
                            st.session_state.get("active_date_tab_m_index", 0),
                            date_tab_labels,
                        )
                    )
                    active_date_tab_m_index = _resolve_tab_index_from_query(
                        st.query_params,
                        "local_m_date_tab",
                        date_tab_labels,
                        fallback_index,
                    )
                    st.session_state["active_date_tab_m_index"] = active_date_tab_m_index
                    st.session_state["active_date_tab_m_label"] = date_tab_labels[
                        active_date_tab_m_index
                    ]
                    st.query_params["local_m_date_tab"] = str(active_date_tab_m_index)

                    date_tabs_m = st.tabs(date_tab_labels)
                    _emit_recent_tab_group_script(
                        active_date_tab_m_index,
                        "local_m_date_tab",
                    )

                    for i, tab_label in enumerate(date_tab_labels):
                        with date_tabs_m[i]:
                            if tab_label == _LOCAL_NO_ENTREGADOS_TAB_LABEL:
                                st.markdown("#### 🚫 Pedidos Locales - Mañana - No entregados")
                                if pedidos_m_no_entregado.empty:
                                    st.info("No hay pedidos locales no entregados.")
                                else:
                                    fechas_ne_dt = sorted(
                                        pedidos_m_no_entregado["Fecha_Entrega_dt"]
                                        .dropna()
                                        .unique()
                                    )
                                    for fecha_dt in fechas_ne_dt:
                                        fecha_label = f"📅 {pd.to_datetime(fecha_dt).strftime('%d/%m/%Y')}"
                                        st.markdown(f"##### {fecha_label}")
                                        pedidos_fecha = pedidos_m_no_entregado[
                                            pedidos_m_no_entregado["Fecha_Entrega_dt"] == fecha_dt
                                        ].copy()
                                        pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                                        for orden, (idx, row) in enumerate(
                                            pedidos_fecha.iterrows(), start=1
                                        ):
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
                                    pedidos_sin_fecha = pedidos_m_no_entregado[
                                        pedidos_m_no_entregado["Fecha_Entrega_dt"].isna()
                                    ].copy()
                                    if not pedidos_sin_fecha.empty:
                                        st.markdown("##### 📅 Sin fecha de entrega")
                                        pedidos_sin_fecha = ordenar_pedidos_custom(
                                            pedidos_sin_fecha
                                        )
                                        for orden, (idx, row) in enumerate(
                                            pedidos_sin_fecha.iterrows(), start=1
                                        ):
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
                            else:
                                current_selected_date_dt = pd.to_datetime(
                                    tab_label.replace("📅 ", ""),
                                    format='%d/%m/%Y',
                                )
                                pedidos_fecha = pedidos_m_activos[
                                    pedidos_m_activos["Fecha_Entrega_dt"] == current_selected_date_dt
                                ].copy()
                                pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                                st.markdown(
                                    f"#### 🌅 Pedidos Locales - Mañana - {tab_label}"
                                )
                                for orden, (idx, row) in enumerate(
                                    pedidos_fecha.iterrows(), start=1
                                ):
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
                    st.session_state["active_date_tab_m_index"] = 0
                    st.session_state["active_date_tab_m_label"] = ""
                    st.query_params["local_m_date_tab"] = "0"
                    st.info("No hay pedidos para el turno mañana.")
            else: # Added: Message if no orders for morning shift
                st.session_state["active_date_tab_m_index"] = 0
                st.session_state["active_date_tab_m_label"] = ""
                st.query_params["local_m_date_tab"] = "0"
                st.info("No hay pedidos para el turno mañana.")
                                
        with subtabs_local[1]:  # 🌇 Tarde
            pedidos_t_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌙 Local Tarde")
            ].copy()
            if not pedidos_t_display.empty:
                pedidos_t_display['Fecha_Entrega_dt'] = pd.to_datetime(
                    pedidos_t_display['Fecha_Entrega'],
                    errors='coerce'
                )
                estado_entrega_t = (
                    pedidos_t_display.get("Estado_Entrega", pd.Series(dtype=str))
                    .astype(str)
                    .str.strip()
                )
                mask_no_entregado_t = (
                    (pedidos_t_display["Estado"] == "🟢 Completado")
                    & (estado_entrega_t == "⏳ No Entregado")
                )
                pedidos_t_no_entregado = pedidos_t_display[mask_no_entregado_t].copy()
                pedidos_t_activos = pedidos_t_display[~mask_no_entregado_t].copy()
                fechas_unicas_dt = sorted(
                    pedidos_t_activos["Fecha_Entrega_dt"].dropna().unique()
                )

                if fechas_unicas_dt or not pedidos_t_no_entregado.empty:
                    date_tab_labels = [
                        f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}"
                        for fecha in fechas_unicas_dt
                    ]
                    if not pedidos_t_no_entregado.empty:
                        date_tab_labels = (
                            [_LOCAL_NO_ENTREGADOS_TAB_LABEL] + date_tab_labels
                        )

                    saved_label_t = st.session_state.get("active_date_tab_t_label", "")
                    fallback_index_t = (
                        date_tab_labels.index(saved_label_t)
                        if saved_label_t in date_tab_labels
                        else _clamp_tab_index(
                            st.session_state.get("active_date_tab_t_index", 0),
                            date_tab_labels,
                        )
                    )
                    active_date_tab_t_index = _resolve_tab_index_from_query(
                        st.query_params,
                        "local_t_date_tab",
                        date_tab_labels,
                        fallback_index_t,
                    )
                    st.session_state["active_date_tab_t_index"] = active_date_tab_t_index
                    st.session_state["active_date_tab_t_label"] = date_tab_labels[
                        active_date_tab_t_index
                    ]
                    st.query_params["local_t_date_tab"] = str(active_date_tab_t_index)

                    date_tabs_t = st.tabs(date_tab_labels)
                    _emit_recent_tab_group_script(
                        active_date_tab_t_index,
                        "local_t_date_tab",
                    )
                    for i, tab_label in enumerate(date_tab_labels):
                        with date_tabs_t[i]:
                            if tab_label == _LOCAL_NO_ENTREGADOS_TAB_LABEL:
                                st.markdown("#### 🚫 Pedidos Locales - Tarde - No entregados")
                                if pedidos_t_no_entregado.empty:
                                    st.info("No hay pedidos locales no entregados.")
                                else:
                                    fechas_ne_dt = sorted(
                                        pedidos_t_no_entregado["Fecha_Entrega_dt"]
                                        .dropna()
                                        .unique()
                                    )
                                    for fecha_dt in fechas_ne_dt:
                                        fecha_label = f"📅 {pd.to_datetime(fecha_dt).strftime('%d/%m/%Y')}"
                                        st.markdown(f"##### {fecha_label}")
                                        pedidos_fecha = pedidos_t_no_entregado[
                                            pedidos_t_no_entregado["Fecha_Entrega_dt"] == fecha_dt
                                        ].copy()
                                        pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                                        for orden, (idx, row) in enumerate(
                                            pedidos_fecha.iterrows(), start=1
                                        ):
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
                                    pedidos_sin_fecha = pedidos_t_no_entregado[
                                        pedidos_t_no_entregado["Fecha_Entrega_dt"].isna()
                                    ].copy()
                                    if not pedidos_sin_fecha.empty:
                                        st.markdown("##### 📅 Sin fecha de entrega")
                                        pedidos_sin_fecha = ordenar_pedidos_custom(
                                            pedidos_sin_fecha
                                        )
                                        for orden, (idx, row) in enumerate(
                                            pedidos_sin_fecha.iterrows(), start=1
                                        ):
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
                                current_selected_date_dt_str = tab_label.replace(
                                    "📅 ", ""
                                )
                                current_selected_date_dt = pd.to_datetime(
                                    current_selected_date_dt_str, format='%d/%m/%Y'
                                )

                                pedidos_fecha = pedidos_t_activos[
                                    pedidos_t_activos["Fecha_Entrega_dt"] == current_selected_date_dt
                                ].copy()
                                pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                                st.markdown(
                                    f"#### 🌇 Pedidos Locales - Tarde - {tab_label}"
                                )
                                for orden, (idx, row) in enumerate(
                                    pedidos_fecha.iterrows(), start=1
                                ):
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
                    st.session_state["active_date_tab_t_index"] = 0
                    st.session_state["active_date_tab_t_label"] = ""
                    st.query_params["local_t_date_tab"] = "0"
                    st.info("No hay pedidos para el turno tarde.")
            else:
                st.session_state["active_date_tab_t_index"] = 0
                st.session_state["active_date_tab_t_label"] = ""
                st.query_params["local_t_date_tab"] = "0"
                st.info("No hay pedidos para el turno tarde.")

        with subtabs_local[2]: # ⛰️ Saltillo
            pedidos_s_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌵 Saltillo")
            ].copy()
            if not pedidos_s_display.empty:
                pedidos_s_display["Fecha_Entrega_dt"] = pd.to_datetime(
                    pedidos_s_display["Fecha_Entrega"],
                    errors="coerce",
                )
                estado_entrega_s = (
                    pedidos_s_display.get("Estado_Entrega", pd.Series(dtype=str))
                    .astype(str)
                    .str.strip()
                )
                mask_no_entregado_s = (
                    (pedidos_s_display["Estado"] == "🟢 Completado")
                    & (estado_entrega_s == "⏳ No Entregado")
                )
                pedidos_s_no_entregado = pedidos_s_display[mask_no_entregado_s].copy()
                pedidos_s_activos = pedidos_s_display[~mask_no_entregado_s].copy()
                fechas_unicas_s = sorted(
                    pedidos_s_activos["Fecha_Entrega_dt"].dropna().unique()
                )

                if fechas_unicas_s or not pedidos_s_no_entregado.empty:
                    date_tab_labels_s = [
                        f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}"
                        for fecha in fechas_unicas_s
                    ]
                    if not pedidos_s_no_entregado.empty:
                        date_tab_labels_s = (
                            [_LOCAL_NO_ENTREGADOS_TAB_LABEL] + date_tab_labels_s
                        )

                    saved_label_s = st.session_state.get("active_date_tab_s_label", "")
                    fallback_index_s = (
                        date_tab_labels_s.index(saved_label_s)
                        if saved_label_s in date_tab_labels_s
                        else _clamp_tab_index(
                            st.session_state.get("active_date_tab_s_index", 0),
                            date_tab_labels_s,
                        )
                    )
                    active_date_tab_s_index = _resolve_tab_index_from_query(
                        st.query_params,
                        "local_s_date_tab",
                        date_tab_labels_s,
                        fallback_index_s,
                    )
                    st.session_state["active_date_tab_s_index"] = active_date_tab_s_index
                    st.session_state["active_date_tab_s_label"] = date_tab_labels_s[
                        active_date_tab_s_index
                    ]
                    st.query_params["local_s_date_tab"] = str(active_date_tab_s_index)

                    date_tabs_s = st.tabs(date_tab_labels_s)
                    _emit_recent_tab_group_script(
                        active_date_tab_s_index,
                        "local_s_date_tab",
                    )
                    for i, tab_label in enumerate(date_tab_labels_s):
                        with date_tabs_s[i]:
                            if tab_label == _LOCAL_NO_ENTREGADOS_TAB_LABEL:
                                st.markdown(
                                    "#### 🚫 Pedidos Locales - Saltillo - No entregados"
                                )
                                if pedidos_s_no_entregado.empty:
                                    st.info("No hay pedidos locales no entregados.")
                                else:
                                    fechas_ne_dt = sorted(
                                        pedidos_s_no_entregado["Fecha_Entrega_dt"]
                                        .dropna()
                                        .unique()
                                    )
                                    for fecha_dt in fechas_ne_dt:
                                        fecha_label = (
                                            "📅 "
                                            f"{pd.to_datetime(fecha_dt).strftime('%d/%m/%Y')}"
                                        )
                                        st.markdown(f"##### {fecha_label}")
                                        pedidos_fecha = pedidos_s_no_entregado[
                                            pedidos_s_no_entregado["Fecha_Entrega_dt"]
                                            == fecha_dt
                                        ].copy()
                                        pedidos_fecha = ordenar_pedidos_custom(
                                            pedidos_fecha
                                        )
                                        for orden, (idx, row) in enumerate(
                                            pedidos_fecha.iterrows(), start=1
                                        ):
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
                                    pedidos_sin_fecha = pedidos_s_no_entregado[
                                        pedidos_s_no_entregado["Fecha_Entrega_dt"].isna()
                                    ].copy()
                                    if not pedidos_sin_fecha.empty:
                                        st.markdown("##### 📅 Sin fecha de entrega")
                                        pedidos_sin_fecha = ordenar_pedidos_custom(
                                            pedidos_sin_fecha
                                        )
                                        for orden, (idx, row) in enumerate(
                                            pedidos_sin_fecha.iterrows(), start=1
                                        ):
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
                                current_selected_date_dt = pd.to_datetime(
                                    tab_label.replace("📅 ", ""),
                                    format="%d/%m/%Y",
                                )
                                pedidos_fecha = pedidos_s_activos[
                                    pedidos_s_activos["Fecha_Entrega_dt"]
                                    == current_selected_date_dt
                                ].copy()
                                pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                                st.markdown(
                                    f"#### ⛰️ Pedidos Locales - Saltillo - {tab_label}"
                                )
                                for orden, (idx, row) in enumerate(
                                    pedidos_fecha.iterrows(), start=1
                                ):
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
                    st.session_state["active_date_tab_s_index"] = 0
                    st.session_state["active_date_tab_s_label"] = ""
                    st.query_params["local_s_date_tab"] = "0"
                    st.info("No hay pedidos para Saltillo.")
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
                for orden, (idx, row) in _render_paginated_iterrows(pedidos_b_display, "local_bodega"):
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
            for orden, (idx, row) in _render_paginated_iterrows(pedidos_foraneos_display, "foraneos"):
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
            for orden, (idx, row) in _render_paginated_iterrows(pedidos_cdmx_display, "cdmx"):
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
            for orden, (idx, row) in _render_paginated_iterrows(solicitudes_display, "solicitudes_guia"):
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
            for orden, (idx, row) in _render_paginated_iterrows(pedidos_cursos_display, "cursos_eventos"):
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

        direccion_retorno = str(row.get("Direccion_Guia_Retorno", "")).strip()
        st.markdown("📍 Dirección para guía de retorno:")
        st.info(direccion_retorno or "Sin dirección registrada.")

        direccion_envio = str(row.get("Direccion_Envio", "")).strip()
        st.markdown("🏠 Dirección de envío:")
        st.info(direccion_envio or "Sin dirección registrada.")

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
                    url = resolve_storage_url(s3_client, u)
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
                    url = resolve_storage_url(s3_client, u)
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
                    url = resolve_storage_url(s3_client, g)
                    if not url:
                        st.error(f"❌ No se pudo generar la URL para la guía {nombre}.")
                        continue
                    st.markdown(
                        f'- <a href="{url}" target="_blank">{nombre}</a>',
                        unsafe_allow_html=True,
                    )
            if not contenido:
                st.info("Sin archivos registrados en la hoja.")

    # 4) Recorrer cada devolución
    for idx, row in devoluciones_display.iterrows():
        idp         = str(row.get("ID_Pedido", "")).strip()
        folio       = str(row.get("Folio_Factura", "")).strip()
        cliente     = str(row.get("Cliente", "")).strip()
        estado      = str(row.get("Estado", "Pendiente")).strip()
        vendedor    = str(row.get("Vendedor_Registro", "")).strip()
        estado_rec  = str(row.get("Estado_Recepcion", "N/A")).strip()
        area_resp   = str(row.get("Area_Responsable", "")).strip()
        row_key_base = (idp or f"{folio}_{cliente}").replace(" ", "_") or "sin_id"
        row_key     = f"{row_key_base}_{idx}"

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
                            if batch_update_gsheet_cells(worksheet_casos, updates, headers=headers_casos):
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
                                    if ok and "Estado" in headers_casos:
                                        ok = update_gsheet_cell(
                                            worksheet_casos,
                                            headers_casos,
                                            gsheet_row_idx,
                                            "Estado",
                                            "🔵 En Proceso",
                                        )

                                    if ok:
                                        row["Estado"] = "🔵 En Proceso"
                                        st.success("✅ Cambios de surtido confirmados y pedido en '🔵 En Proceso'.")
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
                    items.append((file_name, resolve_storage_url(s3_client, u)))

                if nota_credito_url and nota_credito_url.lower() not in ("nan", "none", "n/a"):
                    items.append(("Nota de Crédito", resolve_storage_url(s3_client, nota_credito_url)))
                if documento_adic_url and documento_adic_url.lower() not in ("nan", "none", "n/a"):
                    items.append(("Documento Adicional", resolve_storage_url(s3_client, documento_adic_url)))

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
            success_placeholder = st.empty()
            render_guia_upload_feedback(
                success_placeholder,
                row_key,
                "🔁 Devoluciones",
                s3_client,
            )
            form_key = f"form_guia_{row_key}"
            with st.form(key=form_key):
                guia_files = st.file_uploader(
                    "📋 Subir Guía de Retorno (opcional)",
                    key=f"guia_{row_key}",
                    help="Opcional: sube la guía de mensajería para el retorno del producto (PDF/JPG/PNG)",
                    accept_multiple_files=True,
                )

                submitted_upload = st.form_submit_button(
                    "📤 Subir Guía",
                    on_click=preserve_tab_state,
                )


            if submitted_upload:
                handle_generic_upload_change(row_key, ("expanded_devoluciones",))
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
                                    uploaded_entries = [
                                        {"key": key, "name": os.path.basename(key)}
                                        for key in guia_keys
                                    ]
                                    devoluciones_display.at[
                                        row.name, "Hoja_Ruta_Mensajero"
                                    ] = guia_final
                                    row["Hoja_Ruta_Mensajero"] = guia_final
                                    st.toast(f"📤 {len(guia_keys)} guía(s) subida(s) con éxito.", icon="📦")
                                    ensure_expanders_open(
                                        row_key,
                                        "expanded_devoluciones",
                                    )
                                    set_active_main_tab(5)
                                    guia_success_map = st.session_state.setdefault(
                                        "guia_upload_success", {}
                                    )
                                    guia_success_map[row_key] = {
                                        "count": len(guia_keys),
                                        "column": "Hoja_Ruta_Mensajero",
                                        "files": uploaded_entries,
                                        "timestamp": mx_now_str(),
                                    }
                                    st.cache_data.clear()
                                    st.cache_resource.clear()
                                    marcar_contexto_pedido(row_key, "🔁 Devoluciones")
                                    render_guia_upload_feedback(
                                        success_placeholder,
                                        row_key,
                                        "🔁 Devoluciones",
                                        s3_client,
                                    )
                                else:
                                    st.error("❌ No se pudo actualizar la guía en Google Sheets.")
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")
                except Exception as e:
                    st.error(f"❌ Error al subir la guía: {e}")

            flag_key = f"confirm_complete_id_{row['ID_Pedido']}"
            if st.button(
                "🟢 Completar",
                key=f"btn_completar_{row_key}",
                on_click=preserve_tab_state,
            ):
                ensure_expanders_open(row_key, "expanded_devoluciones")
                st.session_state[flag_key] = row["ID_Pedido"]

            if st.session_state.get(flag_key) == row["ID_Pedido"]:
                st.warning("¿Estás seguro de completar este pedido?")
                confirm_col, cancel_col = st.columns(2)
                with confirm_col:
                    if st.button(
                        "Confirmar",
                        key=f"confirm_completar_{row_key}",
                        on_click=preserve_tab_state,
                    ):
                        ensure_expanders_open(row_key, "expanded_devoluciones")
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
                        ensure_expanders_open(row_key, "expanded_devoluciones")
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

        raw_suffix = row.get("_gsheet_row_index")
        if pd.notna(raw_suffix) and str(raw_suffix).strip():
            unique_suffix = f"{row_key}_{str(raw_suffix).strip()}"
        else:
            unique_suffix = f"{row_key}_{row.name}"
        unique_suffix = re.sub(r"[^0-9A-Za-z_-]", "_", str(unique_suffix))

        # Título del expander
        expander_title = f"🛠 {folio or 's/folio'} – {cliente or 's/cliente'} | Estado: {estado} | Estado_Recepcion: {estado_rec}"
        with st.expander(expander_title, expanded=st.session_state["expanded_garantias"].get(row_key, False)):
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

            st.markdown("**📍 Dirección para guía de retorno:**")
            st.info(str(row.get("Direccion_Guia_Retorno", "")).strip() or "Sin dirección registrada.")

            st.markdown("**🏠 Dirección de envío:**")
            st.info(str(row.get("Direccion_Envio", "")).strip() or "Sin dirección registrada.")

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
            if st.button("✅ Aplicar cambios de envío/fecha (Garantía)", key=f"btn_aplicar_envio_fecha_g_{unique_suffix}", on_click=preserve_tab_state):
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
                            if batch_update_gsheet_cells(worksheet_casos, updates, headers=headers_casos):
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
            if colA.button("⚙️ Procesar", key=f"procesar_g_{unique_suffix}"):
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
                if colB.button("🔧 Procesar Modificación", key=f"proc_mod_g_{unique_suffix}"):
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
                        if st.button("✅ Confirmar Cambios de Surtido (Garantía)", key=f"confirm_mod_g_{unique_suffix}"):
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
                                    if ok and "Estado" in headers_casos:
                                        ok = update_gsheet_cell(
                                            worksheet_casos,
                                            headers_casos,
                                            gsheet_row_idx,
                                            "Estado",
                                            "🔵 En Proceso",
                                        )

                                    if ok:
                                        row["Estado"] = "🔵 En Proceso"
                                        st.success("✅ Cambios de surtido confirmados y pedido en '🔵 En Proceso'.")
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
                        items.append((file_name, resolve_storage_url(s3_client, u)))
                if principal_url and principal_url.lower() not in ("nan", "none", "n/a"):
                    label_p = "Dictamen de Garantía" if dictamen_url else "Nota de Crédito"
                    items.append((label_p, resolve_storage_url(s3_client, principal_url)))
                if doc_adic_url and doc_adic_url.lower() not in ("nan", "none", "n/a"):
                    items.append(("Documento Adicional", resolve_storage_url(s3_client, doc_adic_url)))

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
            success_placeholder = st.empty()
            render_guia_upload_feedback(
                success_placeholder,
                row_key,
                "🛠 Garantías",
                s3_client,
            )
            form_key = f"form_guia_g_{unique_suffix}"
            with st.form(key=form_key):
                guia_files = st.file_uploader(
                    "📋 Subir Guía de Envío/Retorno (Garantía) (opcional)",
                    key=f"guia_g_{unique_suffix}",
                    help="Opcional: sube la guía de mensajería para envío de reposición o retorno (PDF/JPG/PNG)",
                    accept_multiple_files=True,
                )

                submitted_upload = st.form_submit_button(
                    "📤 Subir Guía",
                    on_click=preserve_tab_state,
                )


            if submitted_upload:
                handle_generic_upload_change(row_key, ("expanded_garantias",))
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
                                    uploaded_entries = [
                                        {"key": key, "name": os.path.basename(key)}
                                        for key in guia_keys
                                    ]
                                    garantias_display.at[
                                        row.name, "Hoja_Ruta_Mensajero"
                                    ] = guia_final
                                    row["Hoja_Ruta_Mensajero"] = guia_final
                                    st.toast(f"📤 {len(guia_keys)} guía(s) subida(s) con éxito.", icon="📦")
                                    ensure_expanders_open(
                                        row_key,
                                        "expanded_garantias",
                                    )
                                    set_active_main_tab(6)
                                    guia_success_map = st.session_state.setdefault(
                                        "guia_upload_success", {}
                                    )
                                    guia_success_map[row_key] = {
                                        "count": len(guia_keys),
                                        "column": "Hoja_Ruta_Mensajero",
                                        "files": uploaded_entries,
                                        "timestamp": mx_now_str(),
                                    }
                                    st.cache_data.clear()
                                    st.cache_resource.clear()
                                    marcar_contexto_pedido(row_key, "🛠 Garantías")
                                    render_guia_upload_feedback(
                                        success_placeholder,
                                        row_key,
                                        "🛠 Garantías",
                                        s3_client,
                                    )
                                else:
                                    st.error("❌ No se pudo actualizar la guía en Google Sheets.")
                        else:
                            st.warning("⚠️ No se subió ningún archivo válido.")
                except Exception as e:
                    st.error(f"❌ Error al subir la guía: {e}")

            flag_key = f"confirm_complete_{unique_suffix}"
            if st.button(
                "🟢 Completar Garantía",
                key=f"btn_completar_g_{unique_suffix}",
                on_click=preserve_tab_state,
            ):
                ensure_expanders_open(row_key, "expanded_garantias")
                st.session_state[flag_key] = row["ID_Pedido"]

            if st.session_state.get(flag_key) == row["ID_Pedido"]:
                st.warning("¿Estás seguro de completar este pedido?")
                confirm_col, cancel_col = st.columns(2)
                with confirm_col:
                    if st.button(
                        "Confirmar",
                        key=f"confirm_completar_g_{unique_suffix}",
                        on_click=preserve_tab_state,
                    ):
                        ensure_expanders_open(row_key, "expanded_garantias")
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
                        key=f"cancel_completar_g_{unique_suffix}",
                        on_click=preserve_tab_state,
                    ):
                        ensure_expanders_open(row_key, "expanded_garantias")
                        if flag_key in st.session_state:
                            del st.session_state[flag_key]
                            

with main_tabs[7]:  # ✅ Historial Completados/Cancelados
    df_completados_historial = df_main[
        (df_main["Estado"].isin(["🟢 Completado", "🟣 Cancelado"])) &
        (df_main.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
    ].copy()

    df_completados_historial['_gsheet_row_index'] = df_completados_historial['_gsheet_row_index'].astype(int)

    tipo_casos_col = None
    if 'Tipo_Caso' in df_casos.columns:
        tipo_casos_col = 'Tipo_Caso'
    elif 'Tipo_Envio' in df_casos.columns:
        tipo_casos_col = 'Tipo_Envio'
    df_casos_completados = df_casos[
        (df_casos["Estado"].isin(["🟢 Completado", "🟣 Cancelado"])) &
        (df_casos.get("Completados_Limpiado", "").astype(str).str.lower() != "sí")
    ].copy()
    if not df_casos_completados.empty:
        df_casos_completados['_gsheet_row_index'] = df_casos_completados['_gsheet_row_index'].astype(int)

    col_titulo, col_btn = st.columns([0.75, 0.25])
    with col_titulo:
        st.markdown("### Historial de Pedidos Completados/Cancelados")
    with col_btn:
        if not df_completados_historial.empty and st.button("🧹 Limpiar Todos los Completados/Cancelados"):
            updates = []
            col_idx = headers_main.index("Completados_Limpiado") + 1
            for _, row in df_completados_historial.iterrows():
                g_row = row.get("_gsheet_row_index")
                if g_row:
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(g_row, col_idx),
                        'values': [["sí"]]
                    })
            if updates and batch_update_gsheet_cells(worksheet_main, updates, headers=headers_main):
                st.success(f"✅ {len(updates)} pedidos marcados como limpiados.")
                get_raw_sheet_data.clear()
                get_filtered_sheet_dataframe.clear()
                set_active_main_tab(7)
                st.rerun()

    df_completados_historial["Fecha_Completado"] = pd.to_datetime(
        df_completados_historial["Fecha_Completado"],
        errors="coerce",
    )
    df_completados_historial = df_completados_historial.sort_values(
        by="Fecha_Completado",
        ascending=False,
    )

    displayed_historial_ids = set()

    # 🧹 Limpieza específica por grupo de completados/cancelados locales
    df_completados_historial["Fecha_dt"] = pd.to_datetime(
        df_completados_historial["Fecha_Entrega"], errors="coerce"
    )
    df_completados_historial["Grupo_Clave"] = df_completados_historial.apply(
        lambda row: (
            f"{row['Turno']} – {row['Fecha_dt'].strftime('%d/%m')}"
            if row["Tipo_Envio"] == "📍 Pedido Local"
            else None
        ),
        axis=1,
    )

    grupos_locales = (
        df_completados_historial[df_completados_historial["Grupo_Clave"].notna()][
            "Grupo_Clave"
        ]
        .unique()
        .tolist()
    )

    if grupos_locales:
        st.markdown("### 🧹 Limpieza Específica de Completados/Cancelados Locales")
        for grupo in grupos_locales:
            turno, fecha_str = grupo.split(" – ")
            fecha_dt = (
                pd.to_datetime(fecha_str, format="%d/%m", errors="coerce")
                .replace(year=datetime.now().year)
            )

            # Verificar si hay incompletos en ese grupo
            hay_incompletos = df_main[
                (df_main["Turno"] == turno)
                & (
                    pd.to_datetime(df_main["Fecha_Entrega"], errors="coerce").dt.date
                    == fecha_dt.date()
                )
                & (
                    df_main["Estado"].isin(
                        ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"]
                    )
                )
            ]

            if hay_incompletos.empty:
                label_btn = f"🧹 Limpiar {turno.strip()} - {fecha_str}"
                if st.button(label_btn):
                    pedidos_a_limpiar = df_completados_historial[
                        df_completados_historial["Grupo_Clave"] == grupo
                    ]
                    col_idx = headers_main.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            "range": gspread.utils.rowcol_to_a1(
                                int(row["_gsheet_row_index"]), col_idx
                            ),
                            "values": [["sí"]],
                        }
                        for _, row in pedidos_a_limpiar.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_main, updates, headers=headers_main):
                        st.success(
                            f"✅ {len(updates)} pedidos completados/cancelados en {grupo} marcados como limpiados."
                        )
                        get_raw_sheet_data.clear()
                        get_filtered_sheet_dataframe.clear()
                        set_active_main_tab(7)
                        st.rerun()

                pedidos_grupo = df_completados_historial[
                    df_completados_historial["Grupo_Clave"] == grupo
                ]
                for orden, (idx, row) in enumerate(
                    pedidos_grupo.iterrows(),
                    start=1,
                ):
                    mostrar_pedido(
                        df_main,
                        idx,
                        row,
                        orden,
                        "Historial",
                        "✅ Historial Completados/Cancelados",
                        worksheet_main,
                        headers_main,
                        s3_client,
                    )
                    displayed_historial_ids.add(row["ID_Pedido"])

    # Mostrar pedidos completados individuales
    if not df_completados_historial.empty:
        # 🧹 Botón de limpieza específico para foráneos
        completados_foraneos = df_completados_historial[
            df_completados_historial["Tipo_Envio"] == "🚚 Pedido Foráneo"
        ]

        if not completados_foraneos.empty:
            st.markdown("### 🧹 Limpieza de Completados/Cancelados Foráneos")
            if st.button("🧹 Limpiar Foráneos Completados/Cancelados"):
                col_idx = headers_main.index("Completados_Limpiado") + 1
                updates = [
                    {
                        "range": gspread.utils.rowcol_to_a1(
                            int(row["_gsheet_row_index"]), col_idx
                        ),
                        "values": [["sí"]],
                    }
                    for _, row in completados_foraneos.iterrows()
                ]
                if updates and batch_update_gsheet_cells(worksheet_main, updates, headers=headers_main):
                    st.success(
                        f"✅ {len(updates)} pedidos foráneos completados/cancelados fueron marcados como limpiados."
                    )
                    get_raw_sheet_data.clear()
                    get_filtered_sheet_dataframe.clear()
                    set_active_main_tab(7)
                    st.rerun()

            for orden, (idx, row) in enumerate(
                completados_foraneos.iterrows(),
                start=1,
            ):
                mostrar_pedido(
                    df_main,
                    idx,
                    row,
                    orden,
                    "Historial",
                    "✅ Historial Completados/Cancelados",
                    worksheet_main,
                    headers_main,
                    s3_client,
                )
                displayed_historial_ids.add(row["ID_Pedido"])

        pedidos_restantes = df_completados_historial[
            ~df_completados_historial["ID_Pedido"].isin(displayed_historial_ids)
        ]
        for orden, (idx, row) in enumerate(
            pedidos_restantes.iterrows(),
            start=1,
        ):
            mostrar_pedido(
                df_main,
                idx,
                row,
                orden,
                "Historial",
                "✅ Historial Completados/Cancelados",
                worksheet_main,
                headers_main,
                s3_client,
            )
    else:
        st.info("No hay pedidos completados/cancelados recientes o ya fueron limpiados.")

    # === Casos Especiales Completados/Cancelados ===
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
                direccion_retorno = str(row.get("Direccion_Guia_Retorno", "")).strip()
                st.markdown("📍 Dirección para guía de retorno:")
                st.info(direccion_retorno or "Sin dirección registrada.")
                direccion_envio = str(row.get("Direccion_Envio", "")).strip()
                st.markdown("🏠 Dirección de envío:")
                st.info(direccion_envio or "Sin dirección registrada.")
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
                            url = resolve_storage_url(s3_client, u)
                            st.markdown(f"- [{nombre}]({url})")
                    if guia:
                        contenido = True
                        st.markdown("**Guía:**")
                        guia_url = resolve_storage_url(s3_client, guia)
                        if urlparse(guia_url).scheme in ("http", "https"):
                            st.markdown(f"[Abrir guía]({guia_url})")
                        else:
                            st.markdown(guia_url)
                    if not contenido:
                        st.info("Sin archivos registrados en la hoja.")

            # Devoluciones completadas/canceladas
            comp_dev = df_casos_completados[df_casos_completados[tipo_casos_col].astype(str).str.contains("Devoluci", case=False, na=False)]
            if not comp_dev.empty:
                st.markdown("### 🔁 Devoluciones Completadas/Canceladas")
                if st.button("🧹 Limpiar Devoluciones Completadas/Canceladas"):
                    col_idx = headers_casos.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            'range': gspread.utils.rowcol_to_a1(int(row['_gsheet_row_index']), col_idx),
                            'values': [["sí"]]
                        }
                        for _, row in comp_dev.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_casos, updates, headers=headers_casos):
                        st.success(f"✅ {len(updates)} devoluciones marcadas como limpiadas.")
                        get_raw_sheet_data.clear()
                        get_filtered_sheet_dataframe.clear()
                        set_active_main_tab(7)
                        st.rerun()
                comp_dev = comp_dev.sort_values(by="Fecha_Completado", ascending=False)
                for _, row in comp_dev.iterrows():
                    with st.expander(f"🔁 {row.get('Folio_Factura', 'N/A')} – {row.get('Cliente', 'N/A')}"):
                        render_caso_especial_devolucion(row)

            # Garantías completadas/canceladas
            comp_gar = df_casos_completados[df_casos_completados[tipo_casos_col].astype(str).str.contains("Garant", case=False, na=False)]
            if not comp_gar.empty:
                st.markdown("### 🛠 Garantías Completadas/Canceladas")
                if st.button("🧹 Limpiar Garantías Completadas/Canceladas"):
                    col_idx = headers_casos.index("Completados_Limpiado") + 1
                    updates = [
                        {
                            'range': gspread.utils.rowcol_to_a1(int(row['_gsheet_row_index']), col_idx),
                            'values': [["sí"]]
                        }
                        for _, row in comp_gar.iterrows()
                    ]
                    if updates and batch_update_gsheet_cells(worksheet_casos, updates, headers=headers_casos):
                        st.success(f"✅ {len(updates)} garantías marcadas como limpiadas.")
                        get_raw_sheet_data.clear()
                        get_filtered_sheet_dataframe.clear()
                        set_active_main_tab(7)
                        st.rerun()
                comp_gar = comp_gar.sort_values(by="Fecha_Completado", ascending=False)
                for _, row in comp_gar.iterrows():
                    with st.expander(f"🛠 {row.get('Folio_Factura', 'N/A')} – {row.get('Cliente', 'N/A')}"):
                        render_caso_especial_garantia_hist(row)
        else:
            st.info("No hay casos especiales completados/cancelados o ya fueron limpiados.")
