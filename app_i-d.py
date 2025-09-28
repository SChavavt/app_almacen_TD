import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
import time
import unicodedata
from itertools import count
from zoneinfo import ZoneInfo
from streamlit_autorefresh import st_autorefresh
from textwrap import dedent

TZ = ZoneInfo("America/Mexico_City")

st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")

# --- Controles: recarga manual + autorefresco ---
col_title, col_actions = st.columns([0.7, 0.3])
with col_title:
    st.markdown(
        """
        <h2 style="color: white; font-size: 1.8rem; margin-bottom: 0rem;">
            <span style="font-size: 2.2rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
        </h2>
    """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        /* 🔢 Ajuste compacto para métricas */
        div[data-testid="metric-container"] { padding: 0.1rem 0.5rem; }
        div[data-testid="metric-container"] > div { font-size: 1.1rem !important; }
        div[data-testid="metric-container"] > label { font-size: 0.85rem !important; }
        </style>
    """,
        unsafe_allow_html=True,
    )

with col_actions:
    if st.button("🔄 Recargar pedidos ahora", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    st.checkbox(
        "⚡ Autorefrescar", key="auto_reload", help="Rerun automático sin limpiar caché"
    )
    st.selectbox("Intervalo (seg)", [60, 45], index=0, key="auto_reload_interval")

# ⏱️ Autorefresco (no limpia caché)
if st.session_state.get("auto_reload"):
    interval = int(st.session_state.get("auto_reload_interval", 60))
    # Utilizar st_autorefresh evita recargar la página y conserva la sesión
    st_autorefresh(interval=interval * 1000, key="auto_refresh_counter")

st.markdown("---")

# CSS tabla compacta
st.markdown(
    """
    <style>
    .dataframe td { white-space: unset !important; word-break: break-word; }
    .dataframe {
        table-layout: fixed; width: 100%;
    }
    .dataframe td {
        white-space: normal !important;
        overflow-wrap: break-word;
        font-size: 0.75rem;
        padding: 0.1rem 0.2rem;
        height: 1rem;
        line-height: 1.2rem;
        vertical-align: top;
    }
    .dataframe th {
        font-size: 0.75rem;
        padding: 0.1rem 0.2rem;
        text-align: left;
    }
    </style>
""",
    unsafe_allow_html=True,
)

# Helpers UI automáticos
def sanitize_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except Exception:
        pass
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.lower() in {"nan", "none", "null"}:
            return ""
        return cleaned
    return str(value)


def parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value
    try:
        dt = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if isinstance(dt, pd.Series) and not dt.empty:
        dt = dt.iloc[0]
    if pd.isna(dt):
        return None
    return dt


def format_date(value) -> str:
    dt = parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%d/%m")


def format_time(value) -> str:
    dt = parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%H:%M")


def compute_sort_key(row) -> pd.Timestamp:
    candidates = [
        parse_datetime(row.get("Hora_Registro")),
        parse_datetime(row.get("Fecha_Entrega")),
        parse_datetime(row.get("Fecha_Completado")),
        parse_datetime(row.get("Fecha_Pago_Comprobante")),
        parse_datetime(row.get("Hora_Proceso")),
    ]
    for dt in candidates:
        if dt is not None:
            return dt
    idx = row.get("gsheet_row_index")
    try:
        if idx is not None and not pd.isna(idx):
            base = pd.Timestamp("1970-01-01")
            return base + pd.to_timedelta(int(float(idx)), unit="s")
    except Exception:
        pass
    return pd.Timestamp.max


def assign_numbers(entries, counter):
    for entry in entries:
        entry["numero"] = next(counter)
        entry.pop("sort_key", None)


def format_cliente_line(row) -> str:
    folio = sanitize_text(row.get("Folio_Factura", ""))
    cliente = sanitize_text(row.get("Cliente", ""))
    if folio and cliente:
        return f"📄 <b>{folio}</b> – {cliente}"
    if folio:
        return f"📄 <b>{folio}</b>"
    if cliente:
        return cliente
    return "—"


def unique_preserve(values):
    seen = set()
    ordered = []
    for value in values:
        cleaned = sanitize_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def build_base_entry(row, categoria: str):
    entry = {
        "categoria": categoria,
        "estado": sanitize_text(row.get("Estado", "")),
        "cliente": format_cliente_line(row),
        "fecha": format_date(row.get("Fecha_Entrega")),
        "hora": format_time(row.get("Hora_Registro")),
        "id_pedido": sanitize_text(row.get("ID_Pedido", "")),
        "vendedor": sanitize_text(row.get("Vendedor_Registro", "")),
        "turno": sanitize_text(row.get("Turno", "")),
        "tipo_envio": sanitize_text(row.get("Tipo_Envio", "")),
        "tipo_envio_original": sanitize_text(row.get("Tipo_Envio_Original", "")),
        "tipo": sanitize_text(row.get("Tipo", "")),
        "badges": [],
        "details": [],
        "sort_key": compute_sort_key(row),
    }
    return entry


def build_entries_local(df_local: pd.DataFrame):
    entries = []
    for _, row in df_local.iterrows():
        entry = build_base_entry(row, "📍 Local")
        badges = unique_preserve([entry["turno"], entry["tipo_envio"]])
        details = []
        if entry["id_pedido"]:
            details.append(f"🆔 {entry['id_pedido']}")
        if entry["vendedor"]:
            details.append(f"👤 {entry['vendedor']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_casos(df_casos: pd.DataFrame):
    entries = []
    for _, row in df_casos.iterrows():
        entry = build_base_entry(row, "🧰 Casos")
        badges = unique_preserve([entry["tipo"], entry["turno"], entry["tipo_envio_original"]])
        details = []
        if entry["id_pedido"]:
            details.append(f"🆔 {entry['id_pedido']}")
        if entry["tipo_envio"] and entry["tipo_envio"] not in badges:
            details.append(f"🚚 {entry['tipo_envio']}")
        if entry["vendedor"]:
            details.append(f"👤 {entry['vendedor']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_foraneo(df_for: pd.DataFrame):
    entries = []
    for _, row in df_for.iterrows():
        entry = build_base_entry(row, "🌍 Foráneo")
        badges = unique_preserve([entry["tipo_envio"], entry["turno"]])
        details = []
        if entry["id_pedido"]:
            details.append(f"🆔 {entry['id_pedido']}")
        if entry["tipo_envio_original"] and entry["tipo_envio_original"] not in badges:
            details.append(f"📦 {entry['tipo_envio_original']}")
        if entry["vendedor"]:
            details.append(f"👤 {entry['vendedor']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_cdmx(df_cdmx: pd.DataFrame):
    entries = []
    for _, row in df_cdmx.iterrows():
        entry = build_base_entry(row, "🏙️ CDMX")
        badges = unique_preserve(["🏙️ Pedido CDMX", entry["tipo_envio"]])
        details = []
        if entry["id_pedido"]:
            details.append(f"🆔 {entry['id_pedido']}")
        if entry["vendedor"]:
            details.append(f"👤 {entry['vendedor']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_guias(df_guias: pd.DataFrame):
    entries = []
    for _, row in df_guias.iterrows():
        entry = build_base_entry(row, "📋 Guía")
        badges = unique_preserve(["📋 Solicitud de Guía", entry["tipo_envio"]])
        details = []
        if entry["id_pedido"]:
            details.append(f"🆔 {entry['id_pedido']}")
        if entry["vendedor"]:
            details.append(f"👤 {entry['vendedor']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def render_auto_cards(entries, layout: str = "small"):
    if not entries:
        st.info("No hay pedidos para mostrar.")
        return

    panel_class = "auto-panel-small" if layout == "small" else "auto-panel-large"
    card_class = "auto-card-small" if layout == "small" else "auto-card-large"

    cards_html = []
    for entry in entries:
        badges_html = ""
        badges = entry.get("badges", [])
        if badges:
            badges_html = "<div class='auto-card-badges'>" + "".join(
                f"<span class='auto-card-badge'>{badge}</span>" for badge in badges
            ) + "</div>"

        meta_parts = []
        if entry.get("fecha"):
            meta_parts.append(f"📅 {entry['fecha']}")
        if entry.get("hora"):
            meta_parts.append(f"⏰ {entry['hora']}")
        meta_html = (
            "<div class='auto-card-meta'>" + " · ".join(meta_parts) + "</div>"
            if meta_parts
            else ""
        )

        detail_parts = []
        for part in entry.get("details", []):
            cleaned = sanitize_text(part)
            if cleaned:
                detail_parts.append(cleaned)
        detail_html = (
            "<div class='auto-card-details'>" + " · ".join(detail_parts) + "</div>"
            if detail_parts
            else ""
        )

        cards_html.append(
            dedent(
                f"""
                <div class='{card_class}'>
                    <div class='auto-card-header'>
                        <div>
                            <span class='card-number'>#{entry.get('numero', '?')}</span>
                            <span class='card-category'>{entry.get('categoria', '')}</span>
                        </div>
                        <div class='auto-card-status'>{entry.get('estado', '')}</div>
                    </div>
                    <div class='auto-card-client'>{entry.get('cliente', '—')}</div>
                    {badges_html}
                    {meta_html}
                    {detail_html}
                </div>
                """
            ).strip()
        )

    st.markdown(
        f"<div class='{panel_class}'>" + "".join(cards_html) + "</div>",
        unsafe_allow_html=True,
    )


def get_local_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_local = df_all[
        df_all["Tipo_Envio"].isin(["📍 Pedido Local", "🎓 Cursos y Eventos"])
    ].copy()
    if df_local.empty:
        return df_local
    if "Completados_Limpiado" not in df_local.columns:
        df_local["Completados_Limpiado"] = ""
    df_local = df_local[
        ~(
            df_local["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_local["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ]
    if "Turno" not in df_local.columns:
        df_local["Turno"] = ""
    df_local["Turno"] = df_local["Turno"].fillna("").astype(str)
    df_local.loc[df_local["Turno"].str.lower() == "nan", "Turno"] = ""
    mask_curso_evento = df_local["Tipo_Envio"] == "🎓 Cursos y Eventos"
    mask_turno_vacio = df_local["Turno"].str.strip() == ""
    df_local.loc[mask_curso_evento & mask_turno_vacio, "Turno"] = "🎓 Cursos y Eventos"
    return df_local


def get_foraneo_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_for = df_all[df_all["Tipo_Envio"] == "🚚 Pedido Foráneo"].copy()
    if df_for.empty:
        return df_for
    if "Completados_Limpiado" not in df_for.columns:
        df_for["Completados_Limpiado"] = ""
    df_for = df_for[
        ~(
            df_for["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_for["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ]
    return df_for


def get_cdmx_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_cdmx = df_all[df_all["Tipo_Envio"] == "🏙️ Pedido CDMX"].copy()
    if df_cdmx.empty:
        return df_cdmx
    if "Completados_Limpiado" not in df_cdmx.columns:
        df_cdmx["Completados_Limpiado"] = ""
    df_cdmx = df_cdmx[
        ~(
            df_cdmx["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_cdmx["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()
    return df_cdmx


def get_guias_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_guias = df_all[df_all["Tipo_Envio"] == "📋 Solicitudes de Guía"].copy()
    if df_guias.empty:
        return df_guias
    if "Completados_Limpiado" not in df_guias.columns:
        df_guias["Completados_Limpiado"] = ""
    df_guias = df_guias[
        ~(
            df_guias["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_guias["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()
    return df_guias


def _etiqueta_tipo_caso(valor: str) -> str:
    s = sanitize_text(valor).lower()
    if "garant" in s:
        return "🛠 Garantía"
    if "devolu" in s:
        return "🔁 Devolución"
    return "—"


def get_casos_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    df_casos = load_casos_from_gsheets()
    if df_casos.empty:
        df_casos = pd.DataFrame()
    else:
        raw_headers = df_casos.columns.tolist()
        fixed = []
        seen_empty = 0
        for h in raw_headers:
            h_norm = unicodedata.normalize("NFKD", h or "").encode("ascii", "ignore").decode("ascii")
            h_norm = h_norm.strip().replace(" ", "_")
            if not h_norm:
                seen_empty += 1
                h_norm = f"_col_vacia_{seen_empty}"
            base = h_norm
            k = 2
            while h_norm in fixed:
                h_norm = f"{base}_{k}"
                k += 1
            fixed.append(h_norm)
        df_casos.columns = fixed

        dt_cols = [
            "Hora_Registro",
            "Fecha_Entrega",
            "Fecha_Completado",
            "Fecha_Pago_Comprobante",
            "Hora_Proceso",
            "Fecha_Recepcion_Devolucion",
        ]
        for c in dt_cols:
            if c in df_casos.columns:
                df_casos[c] = pd.to_datetime(df_casos[c], errors="coerce")

        for base in [
            "ID_Pedido",
            "Cliente",
            "Vendedor_Registro",
            "Folio_Factura",
            "Estado",
            "Tipo_Envio",
            "Tipo_Envio_Original",
            "Turno",
        ]:
            if base not in df_casos.columns:
                df_casos[base] = ""
            else:
                df_casos[base] = df_casos[base].astype(str).fillna("").str.strip()

        if "Tipo_Caso" not in df_casos.columns and "Tipo_Envio" in df_casos.columns:
            df_casos["Tipo_Caso"] = df_casos["Tipo_Envio"]
        if "Tipo_Envio_Original" not in df_casos.columns and "Tipo_Envio" in df_casos.columns:
            df_casos["Tipo_Envio_Original"] = df_casos["Tipo_Envio"]

    df_garantias_pedidos = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        df_garantias_pedidos = df_all[
            df_all["Tipo_Envio"].astype(str).str.contains("Garant", case=False, na=False)
        ].copy()
        if not df_garantias_pedidos.empty:
            df_garantias_pedidos["Tipo_Caso"] = df_garantias_pedidos["Tipo_Envio"]
            df_garantias_pedidos["Tipo_Envio_Original"] = df_garantias_pedidos["Tipo_Envio"]

    casos = pd.concat([df_casos, df_garantias_pedidos], ignore_index=True)
    if casos.empty:
        return casos

    mask = casos["Tipo_Caso"].astype(str).str.contains("Devoluci|Garant", case=False, na=False)
    casos = casos[mask].copy()
    if casos.empty:
        return casos

    if "Completados_Limpiado" not in casos.columns:
        casos["Completados_Limpiado"] = ""
    if "Estado" in casos.columns:
        casos = casos[
            ~(
                casos["Estado"].astype(str).str.strip().isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
                & (casos["Completados_Limpiado"].astype(str).str.lower() == "sí")
            )
        ]

    for base in [
        "Fecha_Entrega",
        "Cliente",
        "Vendedor_Registro",
        "Estado",
        "Folio_Factura",
        "Turno",
        "Tipo_Envio_Original",
        "Tipo_Envio",
    ]:
        if base not in casos.columns:
            casos[base] = ""

    casos["Tipo"] = casos["Tipo_Caso"].apply(_etiqueta_tipo_caso)
    return casos
# Estilos para paneles automáticos
st.markdown(
    """
    <style>
    .auto-panel-small {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
        gap: 0.35rem;
        margin-top: 0.25rem;
        align-items: stretch;
    }
    .auto-panel-large {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
        gap: 0.6rem;
        margin-top: 0.5rem;
        align-items: stretch;
    }
    .auto-card-small,
    .auto-card-large {
        background: rgba(28, 28, 30, 0.9);
        border-radius: 0.75rem;
        padding: 0.55rem 0.75rem;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.25);
        color: #f7f7f7;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 9rem;
    }
    .auto-card-small {
        font-size: 0.78rem;
        line-height: 1.15rem;
    }
    .auto-card-large {
        font-size: 0.95rem;
        line-height: 1.3rem;
        padding: 0.75rem 1rem;
        min-height: 11rem;
    }
    .auto-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.5rem;
        font-weight: 600;
    }
    .auto-card-header .card-number {
        background: rgba(255, 255, 255, 0.15);
        border-radius: 0.6rem;
        padding: 0.15rem 0.45rem;
        font-size: 0.75rem;
        letter-spacing: 0.03em;
    }
    .auto-card-header .card-category {
        margin-left: 0.4rem;
    }
    .auto-card-status {
        font-weight: 700;
    }
    .auto-card-client {
        margin-top: 0.35rem;
        font-weight: 500;
    }
    .auto-card-badges {
        margin-top: 0.3rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.25rem;
    }
    .auto-card-badge {
        background: rgba(255, 255, 255, 0.12);
        padding: 0.1rem 0.35rem;
        border-radius: 0.5rem;
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 0.02em;
    }
    .auto-card-meta {
        margin-top: 0.25rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        opacity: 0.85;
    }
    .auto-card-details {
        margin-top: 0.3rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        opacity: 0.85;
    }
    @media (max-width: 1100px) {
        .auto-card-large {
            font-size: 0.88rem;
            padding: 0.65rem 0.8rem;
        }
        .auto-panel-large {
            gap: 0.45rem;
        }
    }
    </style>
""",
    unsafe_allow_html=True,
)

# --- IDs de Sheets ---
GOOGLE_SHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SHEET_PEDIDOS = "datos_pedidos"
SHEET_CASOS = "casos_especiales"


# --- Auth helpers ---
def construir_gspread_client(creds_dict):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
        creds_dict["private_key"] = (
            creds_dict["private_key"].replace("\\n", "\n").strip()
        )
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


@st.cache_resource
def get_gspread_client(_credentials_json_dict, max_attempts: int = 3):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    _credentials_json_dict["private_key"] = (
        _credentials_json_dict["private_key"].replace("\\n", "\n").strip()
    )

    for attempt in range(1, max_attempts + 1):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            _credentials_json_dict, scope
        )
        client = gspread.authorize(creds)
        try:
            client.open_by_key(GOOGLE_SHEET_ID)
            return client
        except gspread.exceptions.APIError as e:
            if "expired" in str(e).lower() or "RESOURCE_EXHAUSTED" in str(e):
                st.cache_resource.clear()
            wait_time = 2 ** (attempt - 1)
            if attempt >= max_attempts:
                st.error(
                    f"❌ Error al autenticar con Google Sheets después de {max_attempts} intentos: {e}"
                )
                st.stop()
            st.warning(
                f"🔁 Error de autenticación. Reintentando en {wait_time} s..."
            )
            time.sleep(wait_time)


# --- AWS S3 ---
try:
    if "aws" not in st.secrets:
        st.error(
            "❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets."
        )
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
    try:
        return boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
    except Exception as e:
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        st.stop()


# --- Clientes iniciales ---
try:
    if "gsheets" not in st.secrets:
        st.error(
            "❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets."
        )
        st.stop()
    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace(
        "\\n", "\n"
    )

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()
    spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
    worksheet_main = spreadsheet.worksheet(SHEET_PEDIDOS)
    worksheet_casos = spreadsheet.worksheet(SHEET_CASOS)

except gspread.exceptions.APIError as e:
    if "ACCESS_TOKEN_EXPIRED" in str(e) or "UNAUTHENTICATED" in str(e):
        st.cache_resource.clear()
        st.warning("🔄 La sesión con Google Sheets expiró. Reconectando...")
        time.sleep(1)
        g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        s3_client = get_s3_client()
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet_main = spreadsheet.worksheet(SHEET_PEDIDOS)
        worksheet_casos = spreadsheet.worksheet(SHEET_CASOS)
    else:
        st.error(f"❌ Error al autenticar clientes: {e}")
        st.stop()
except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()


# --- Carga de datos ---
@st.cache_data(ttl=60)
def load_data_from_gsheets():
    data = worksheet_main.get_all_values()
    if not data:
        return pd.DataFrame()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    df["gsheet_row_index"] = df.index + 2

    # Tipos
    if "ID_Pedido" in df.columns:
        df["ID_Pedido"] = df["ID_Pedido"].astype(str)

    dt_cols = [
        "Hora_Registro",
        "Fecha_Entrega",
        "Fecha_Completado",
        "Fecha_Pago_Comprobante",
        "Hora_Proceso",
    ]
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "Turno" in df.columns:
        df["Turno"] = df["Turno"].astype(str).replace({"nan": "", "": None}).fillna("")
    else:
        df["Turno"] = ""

    return df


@st.cache_data(ttl=60)
def load_casos_from_gsheets():
    """Lee 'casos_especiales' y normaliza headers/fechas."""
    data = worksheet_casos.get_all_values()
    if not data:
        return pd.DataFrame()
    raw_headers = data[0]
    fixed = []
    seen_empty = 0
    for h in raw_headers:
        h = unicodedata.normalize("NFKD", h or "").encode("ascii", "ignore").decode("ascii")
        h = h.strip().replace(" ", "_")
        if not h:
            seen_empty += 1
            h = f"_col_vacia_{seen_empty}"
        base = h
        k = 2
        while h in fixed:
            h = f"{base}_{k}"
            k += 1
        fixed.append(h)
    df = pd.DataFrame(data[1:], columns=fixed)
    df["gsheet_row_index"] = df.index + 2

    # Fechas típicas
    dt_cols = [
        "Hora_Registro",
        "Fecha_Entrega",
        "Fecha_Completado",
        "Fecha_Pago_Comprobante",
        "Hora_Proceso",
        "Fecha_Recepcion_Devolucion",
    ]
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Normaliza campos base
    for base in [
        "ID_Pedido",
        "Cliente",
        "Vendedor_Registro",
        "Folio_Factura",
        "Estado",
        "Tipo_Envio",
        "Tipo_Envio_Original",
        "Turno",
    ]:
        if base in df.columns:
            df[base] = df[base].astype(str).fillna("").str.strip()
    if "Turno" not in df.columns:
        df["Turno"] = ""
    return df


# --- S3 helper (solo lectura presignada aquí) ---
def get_s3_file_url(s3_object_key):
    if not s3_object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_object_key},
            ExpiresIn=3600,
        )
    except Exception:
        return None


def display_attachments(adjuntos_str):
    """Renderiza enlaces; acepta lista separada por comas con keys o URLs completas."""
    if pd.isna(adjuntos_str) or not str(adjuntos_str).strip():
        return "N/A"
    parts = [p.strip() for p in str(adjuntos_str).split(",") if p.strip()]
    links = []
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({p})")
        else:
            url = get_s3_file_url(p)
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({url})" if url else f"❌ {p}")
    return " | ".join(links) if links else "N/A"


# --- Render tabla compacta ---
def display_dataframe_with_formatting(df_to_display):
    columnas_deseadas = [
        "Fecha_Entrega",
        "Tipo_Envio",
        "Cliente",
        "Vendedor_Registro",
        "Estado",
    ]
    cols_exist = [c for c in columnas_deseadas if c in df_to_display.columns]
    if not cols_exist:
        st.info("No hay columnas relevantes para mostrar.")
        return

    df_vista = df_to_display.copy()

    # Cliente = Folio + Cliente
    if "Folio_Factura" in df_vista.columns and "Cliente" in df_vista.columns:
        df_vista["Cliente"] = df_vista.apply(
            lambda row: f"📄 <b>{row['Folio_Factura']}</b> 🤝 {row['Cliente']}", axis=1
        )

    # Renombrar columnas
    ren = {
        "Fecha_Entrega": "Fecha Entrega",
        "Vendedor_Registro": "Vendedor",
        "Tipo_Envio": "Tipo Envío",
    }
    for k, v in ren.items():
        if k in df_vista.columns:
            df_vista.rename(columns={k: v}, inplace=True)

    if "Fecha Entrega" in df_vista.columns:
        df_vista["Fecha Entrega"] = df_vista["Fecha Entrega"].apply(
            lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
        )

    mostrar_cols = [
        c
        for c in ["Fecha Entrega", "Tipo Envío", "Cliente", "Vendedor", "Estado"]
        if c in df_vista.columns
    ]
    df_vista = df_vista[mostrar_cols]

    st.markdown(df_vista.to_html(escape=False, index=False), unsafe_allow_html=True)


# --- Helpers de métrica + agrupación ---
def status_counts_block(df_src):
    comps = df_src.copy()
    if "Completados_Limpiado" not in comps.columns:
        comps["Completados_Limpiado"] = ""
    completados_visibles = comps[
        (comps["Estado"] == "🟢 Completado")
        & (comps["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cancelados_visibles = comps[
        (comps["Estado"] == "🟣 Cancelado")
        & (comps["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    counts = {
        "🟡 Pendiente": (comps["Estado"] == "🟡 Pendiente").sum(),
        "🔵 En Proceso": (comps["Estado"] == "🔵 En Proceso").sum(),
        "🔴 Demorado": (comps["Estado"] == "🔴 Demorado").sum(),
        "🛠 Modificación": (comps["Estado"] == "🛠 Modificación").sum(),
        "🟣 Cancelado": len(cancelados_visibles),
        "🟢 Completado": len(completados_visibles),
    }
    total = sum(counts.values())
    estados_fijos = ["🟡 Pendiente", "🔵 En Proceso", "🟢 Completado"]
    estados_cond = ["🔴 Demorado", "🛠 Modificación", "🟣 Cancelado"]
    items = [("📦 Total Pedidos", total)]
    for e in estados_fijos:
        items.append((e, counts[e]))
    for e in estados_cond:
        if counts[e] > 0:
            items.append((e, counts[e]))

    cols = st.columns(len(items))
    for c, (label, val) in zip(cols, items):
        c.metric(label, int(val))


def group_key_local(row, local_flag_col="Turno"):
    """Devuelve el turno o etiquetas para cursos/eventos y locales sin turno."""
    turno = str(row.get(local_flag_col, "") or "")
    if turno:
        return turno
    tipo_envio = str(row.get("Tipo_Envio", "") or "")
    if tipo_envio == "🎓 Cursos y Eventos":
        return "🎓 Cursos y Eventos"
    return "📍 Local (sin turno)"


def show_grouped_panel(df_source, mode: str = "local", group_turno: bool = True):
    """Muestra paneles agrupados por turno (local) o fecha.

    Cuando ``group_turno`` es ``False`` en modo "local", agrupa únicamente
    por ``Fecha_Entrega``.
    """
    if df_source.empty:
        st.info("No hay registros para mostrar.")
        return
    work = df_source.copy()
    work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
    if mode == "foraneo" or (mode == "local" and not group_turno):
        work["Grupo_Clave"] = work["Fecha_Entrega_Str"]
    else:
        work["Grupo_Clave"] = work.apply(
            lambda r: f"{group_key_local(r)} – {r['Fecha_Entrega_Str']}", axis=1
        )
    grupos = []
    grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"])
    for (clave, f), df_g in sorted(
        grouped, key=lambda x: x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max
    ):
        if not df_g.empty:
            grupos.append((f"{clave} ({len(df_g)})", df_g))

    if not grupos:
        st.info("No hay grupos para mostrar.")
        return

    num_cols_per_row = 3
    for i in range(0, len(grupos), num_cols_per_row):
        fila = grupos[i : i + num_cols_per_row]
        cols = st.columns(len(fila))
        for j, (titulo, df_g) in enumerate(fila):
            with cols[j]:
                st.markdown(f"#### {titulo}")
                if "Hora_Registro" in df_g.columns:
                    df_g = df_g.sort_values(
                        by="Hora_Registro", ascending=False
                    ).reset_index(drop=True)
                display_dataframe_with_formatting(df_g)


# =========================
# Helpers para Casos Especiales
# =========================
def status_counts_block_casos(df: pd.DataFrame):
    estados = df.get("Estado", pd.Series(dtype=str)).astype(str)
    if "Completados_Limpiado" not in df.columns:
        df["Completados_Limpiado"] = ""
    total = len(df)
    pend = estados.str.contains("Pendiente", case=False, na=False).sum()
    proc = estados.str.contains("En Proceso", case=False, na=False).sum()
    completados_visibles = df[
        (df["Estado"].astype(str).str.strip() == "🟢 Completado")
        & (df["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cancelados_visibles = df[
        (df["Estado"].astype(str).str.strip() == "🟣 Cancelado")
        & (df["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cols = st.columns(5)
    cols[0].metric("Total Pedidos", int(total))
    cols[1].metric("🟡 Pendiente", int(pend))
    cols[2].metric("🔵 En Proceso", int(proc))
    cols[3].metric("🟢 Completado", int(len(completados_visibles)))
    cols[4].metric("🟣 Cancelado", int(len(cancelados_visibles)))


if "show_grouped_panel_casos" not in globals():

    def show_grouped_panel_casos(df: pd.DataFrame):
        """Agrupa por Turno (Local) o Foráneo genérico y fecha; muestra tablas."""
        df_local = df.copy()

        # Asegura columnas base
        for base in [
            "Fecha_Entrega",
            "Cliente",
            "Vendedor_Registro",
            "Estado",
            "Folio_Factura",
            "Turno",
            "Tipo_Envio_Original",
        ]:
            if base not in df_local.columns:
                df_local[base] = ""

        # Fecha string para el título
        df_local["Fecha_Entrega_Str"] = (
            df_local["Fecha_Entrega"].dt.strftime("%d/%m").fillna("Sin Fecha")
            if "Fecha_Entrega" in df_local.columns
            else "Sin Fecha"
        )

        # Determinar etiqueta de grupo
        if "Turno" not in df_local.columns:
            df_local["Turno"] = ""

        # Si no hay turno pero viene marcado como Local → etiqueta genérica
        if "Tipo_Envio_Original" in df_local.columns:
            mask_local_sin_turno = (df_local["Turno"].astype(str).str.strip() == "") & (
                df_local["Tipo_Envio_Original"]
                .astype(str)
                .str.contains("Local", case=False, na=False)
            )
            df_local.loc[mask_local_sin_turno, "Turno"] = "📍 Local (sin turno)"

        # Cuando no sea local, foráneo genérico
        es_local = (
            df_local["Turno"]
            .astype(str)
            .str.contains("Local|Saltillo|Bodega|Mañana|Tarde", case=False, na=False)
        )
        df_local.loc[~es_local, "Turno"] = "🌍 Foráneo"

        # Clave de grupo
        df_local["Grupo_Clave"] = df_local.apply(
            lambda r: f"{r['Turno']} – {r['Fecha_Entrega_Str']}", axis=1
        )

        # Orden por fecha real (NaT al final)
        if "Fecha_Entrega" in df_local.columns:
            df_local["_fecha_sort"] = df_local["Fecha_Entrega"].fillna(pd.Timestamp.max)
        else:
            df_local["_fecha_sort"] = pd.Timestamp.max

        grupos = []
        for (clave, _), sub in sorted(
            df_local.groupby(["Grupo_Clave", "Fecha_Entrega"], dropna=False),
            key=lambda x: x[0][1],
        ):
            if "Hora_Registro" in sub.columns:
                sub = sub.sort_values(by="Hora_Registro", ascending=False)
            grupos.append(
                (
                    f"{clave} ({len(sub)})",
                    sub.drop(columns=["_fecha_sort"], errors="ignore"),
                )
            )

        if not grupos:
            st.info("Sin grupos para mostrar.")
            return

        num_cols_per_row = 3
        for i in range(0, len(grupos), num_cols_per_row):
            fila = grupos[i : i + num_cols_per_row]
            cols = st.columns(len(fila))
            for j, (titulo, df_grupo) in enumerate(fila):
                with cols[j]:
                    st.markdown(f"### {titulo}")
                    # Vista enriquecida con tipo de caso, envío y turno
                    base_cols = [
                        "Tipo",
                        "Tipo_Envio_Original",
                        "Turno",
                        "Fecha_Entrega",
                        "Cliente",
                        "Vendedor_Registro",
                        "Estado",
                        "Folio_Factura",
                    ]
                    for c in base_cols:
                        if c not in df_grupo.columns:
                            df_grupo[c] = ""
                    vista = df_grupo[base_cols].copy()
                    vista.rename(
                        columns={
                            "Tipo_Envio_Original": "Tipo Envío",
                            "Fecha_Entrega": "Fecha Entrega",
                            "Vendedor_Registro": "Vendedor",
                        },
                        inplace=True,
                    )
                    vista["Fecha Entrega"] = vista["Fecha Entrega"].apply(
                        lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
                    )
                    vista["Cliente"] = vista.apply(
                        lambda r: f"📄 <b>{r['Folio_Factura']}</b> 🤝 {r['Cliente']}",
                        axis=1,
                    )
                    st.markdown(
                        vista[
                            [
                                "Tipo",
                                "Tipo Envío",
                                "Turno",
                                "Fecha Entrega",
                                "Cliente",
                                "Vendedor",
                                "Estado",
                            ]
                        ].to_html(escape=False, index=False),
                        unsafe_allow_html=True,
                    )


# ===========================
#        MAIN RENDER
# ===========================
df_all = load_data_from_gsheets()
st.caption(f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')}")

# Tabs principales
tab_labels = [
    "📍 Local + 🧰 Casos (Automática)",
    "🌍 Foráneo + 🏙️ CDMX (Automática)",
    "📍 Local",
    "🌍 Foráneo",
    "🏙️ CDMX y Guías",
    "🧰 Casos Especiales",
]
tabs = st.tabs(tab_labels)

# Contador compartido para numeración en vistas automáticas
auto_card_counter = count(1)

# ---------------------------
# TAB 0: Local + Casos (Automática)
# ---------------------------
with tabs[0]:
    st_autorefresh(interval=60000, key="auto_refresh_local_casos")
    st.caption("♻️ Vista consolidada (Local + Casos) actualizada automáticamente cada 60 s.")
    df_local_auto = get_local_orders(df_all)
    df_casos_auto = get_casos_orders(df_all)
    combined_entries = []
    if not df_local_auto.empty:
        combined_entries.extend(build_entries_local(df_local_auto))
    if not df_casos_auto.empty:
        combined_entries.extend(build_entries_casos(df_casos_auto))
    combined_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    assign_numbers(combined_entries, auto_card_counter)
    render_auto_cards(combined_entries, layout="small")

# ---------------------------
# TAB 1: Foráneo + CDMX (Automática)
# ---------------------------
with tabs[1]:
    st_autorefresh(interval=60000, key="auto_refresh_foraneo_cdmx")
    st.caption("♻️ Panel ampliado (Foráneo + CDMX) con actualización automática cada 60 s.")
    df_for_auto = get_foraneo_orders(df_all)
    df_cdmx_auto = get_cdmx_orders(df_all)
    df_guias_auto = get_guias_orders(df_all)
    combined_entries = []
    if not df_for_auto.empty:
        combined_entries.extend(build_entries_foraneo(df_for_auto))
    if not df_cdmx_auto.empty:
        combined_entries.extend(build_entries_cdmx(df_cdmx_auto))
    if not df_guias_auto.empty:
        combined_entries.extend(build_entries_guias(df_guias_auto))
    combined_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    assign_numbers(combined_entries, auto_card_counter)
    render_auto_cards(combined_entries, layout="large")

# ---------------------------
# TAB 2: Local
# ---------------------------
with tabs[2]:
    if df_all.empty:
        st.info("Sin datos en 'datos_pedidos'.")
    else:
        df_local = get_local_orders(df_all)
        if df_local.empty:
            st.info("Sin pedidos locales.")
        else:
            turnos = df_local["Turno"].dropna().unique()
            if len(turnos) == 0:
                st.info("Sin pedidos locales.")
            else:
                sub_tabs = st.tabs([t if t else "Sin Turno" for t in turnos])
                for idx, turno in enumerate(turnos):
                    df_turno = df_local[df_local["Turno"] == turno]
                    with sub_tabs[idx]:
                        label = turno if turno else "Sin Turno"
                        st.markdown(f"#### 📊 Resumen ({label})")
                        status_counts_block(df_turno)
                        st.markdown("### 📚 Grupos")
                        show_grouped_panel(df_turno, mode="local", group_turno=False)

# ---------------------------
# TAB 3: Foráneo
# ---------------------------
with tabs[3]:
    if df_all.empty:
        st.info("Sin datos en 'datos_pedidos'.")
    else:
        df_for = get_foraneo_orders(df_all)
        if df_for.empty:
            st.info("Sin pedidos foráneos.")
        else:
            st.markdown("#### 📊 Resumen (Foráneo)")
            status_counts_block(df_for)
            st.markdown("### 📚 Grupos")
            show_grouped_panel(df_for, mode="foraneo")

# ---------------------------
# TAB 4: CDMX y Guías
# ---------------------------
with tabs[4]:
    if df_all.empty:
        st.info("Sin datos en 'datos_pedidos'.")
    else:
        df_cdmx_filtrado = get_cdmx_orders(df_all)
        df_guias_filtrado = get_guias_orders(df_all)

        df_cdmx_guias = pd.concat(
            [df_cdmx_filtrado, df_guias_filtrado], ignore_index=True
        )
        if df_cdmx_guias.empty:
            st.info("No hay pedidos CDMX ni solicitudes de guía visibles para resumir.")
        else:
            st.markdown("##### Resumen CDMX + Guías")
            status_counts_block(df_cdmx_guias)

        st.subheader("🏙️ Pedidos CDMX")
        if df_cdmx_filtrado.empty:
            st.info("No hay pedidos CDMX.")
        else:
            st.markdown("##### Grupos CDMX (por fecha)")
            work = df_cdmx_filtrado.copy()
            work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
            work["Grupo_Clave"] = work.apply(
                lambda r: f"CDMX – {r['Fecha_Entrega_Str']}", axis=1
            )
            grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"])
            grupos = []
            for (clave, f), df_g in sorted(
                grouped,
                key=lambda x: x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
            ):
                if not df_g.empty:
                    grupos.append((f"🏙️ {clave} ({len(df_g)})", df_g))
            if not grupos:
                st.info("No hay grupos para mostrar.")
            else:
                for titulo, df_g in grupos:
                    st.markdown(f"#### {titulo}")
                    df_g = df_g.sort_values(
                        by="Hora_Registro", ascending=False
                    ).reset_index(drop=True)
                    display_dataframe_with_formatting(df_g)

        st.markdown("---")

        st.subheader("📋 Solicitudes de Guía")
        if df_guias_filtrado.empty:
            st.info("No hay solicitudes de guía.")
        else:
            st.markdown("##### Grupos Guías (por fecha)")
            work = df_guias_filtrado.copy()
            work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
            work["Grupo_Clave"] = work.apply(
                lambda r: f"Guías – {r['Fecha_Entrega_Str']}", axis=1
            )
            grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"])
            grupos = []
            for (clave, f), df_g in sorted(
                grouped,
                key=lambda x: x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
            ):
                if not df_g.empty:
                    grupos.append((f"📋 {clave} ({len(df_g)})", df_g))
            if not grupos:
                st.info("No hay grupos para mostrar.")
            else:
                for titulo, df_g in grupos:
                    st.markdown(f"#### {titulo}")
                    df_g = df_g.sort_values(
                        by="Hora_Registro", ascending=False
                    ).reset_index(drop=True)
                    display_dataframe_with_formatting(df_g)

# ---------------------------
# TAB 5: Casos Especiales (Devoluciones + Garantías)
# ---------------------------
with tabs[5]:
    casos = get_casos_orders(df_all)
    if casos.empty:
        st.info("Sin datos de devoluciones o garantías.")
    else:
        st.markdown("#### 📊 Resumen Casos Especiales")
        status_counts_block_casos(casos)
        st.markdown("### 📚 Grupos (Local por Turno / Foráneo genérico)")
        show_grouped_panel_casos(casos)
