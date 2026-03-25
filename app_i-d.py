import streamlit as st
from openai import OpenAI
import base64
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
import time
import unicodedata
import streamlit.components.v1 as components
from itertools import count
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from streamlit_autorefresh import st_autorefresh
from textwrap import dedent
from difflib import SequenceMatcher
from urllib.parse import urlsplit, urlunsplit, quote

TZ = ZoneInfo("America/Mexico_City")

TD_ASSISTANT_SYSTEM_PROMPT = dedent(
    """
    Eres el asistente interno de TD para apoyar a vendedores y personal comercial.
    TD es una empresa mexicana que vende material dental especializado en ortodoncia a nivel nacional.
    Tu función es resolver dudas operativas internas de forma clara, breve, útil y profesional.
    Cuando haya términos clínicos del giro dental (por ejemplo: arco), interprétalos en contexto de ortodoncia dental.
    Ayudas especialmente con:
    - claves de materiales
    - zonas remotas
    - cobertura
    - envíos
    - pedidos locales y foráneos
    - procesos internos
    - criterios operativos para vendedores

    Reglas:
    - Responde como  interno de TD.
    - Usa respuestas cortas, claras y prácticas.
    - Si no tienes certeza de un dato, no lo inventes.
    - Si falta información, pide solo el dato necesario.
    - Si algo depende de una validación interna no confirmada, dilo claramente.
    - No respondas como vendedor a cliente final.
    - No menciones OpenAI, IA, modelo, sistema ni detalles técnicos.
    - Mantén tono profesional, útil y natural.
    - Prioriza claridad operativa sobre texto largo.
    - Aclara siempre el significado operativo de estados/fuentes:
      * data_pedidos = flujo actual; si dice "Completado" significa listo para recolección de paquetería, NO necesariamente enviado/entregado.
      * datos_pedidos = históricos que ya salieron de almacén.
      * casos_especiales = devoluciones/garantías/casos especiales; confirma salida solo con Completados_Limpiado = "sí".
    - Si preguntan "¿sí llegó/está en sistema?" responde primero confirmando existencia del pedido (sí/no), y si sí, agrega fecha/hora de registro, vendedor y estado.
    - Si consultan por nombre de cliente (sin folio/ID), busca y responde priorizando data_pedidos; si no aparece, usa datos_pedidos.
    - Si el mensaje menciona devolución o garantía, prioriza casos_especiales.
    - ID_Pedido es un identificador interno: nunca lo expongas ni lo uses en la respuesta al usuario.
    - Para dudas de productos, usa la hoja "Productos" como fuente principal; prioriza devolver Código + Descripción exacta.
    - Si una descripción coincide con varios productos, muestra opciones cortas con sus códigos y pide precisión.
    - Si no encuentras una coincidencia exacta en productos, no cierres con un simple "no hay": explica que no se encontró exacto y ofrece las coincidencias más cercanas disponibles en la base.
    - Si preguntan por el último pedido subido del vendedor logueado, prioriza data_pedidos y usa históricos solo como respaldo si no hay pedidos actuales.
    - Antes de afirmar que no existe un folio/material, valida las coincidencias exactas incluidas en el contexto; si hay una coincidencia exacta, debes reconocerla.
    - Para pedidos, la gente consulta sobre todo por folio o por nombre de cliente; no bases la respuesta en ID_Pedido.
    - Si hay coincidencias parciales por nombre de cliente pero más de un pedido posible, dilo claramente y ofrece opciones cortas en vez de negar existencia.
    - Si el folio o nombre parece venir con un pequeño error de captura (una letra/número faltante o cambiado), revisa coincidencias aproximadas antes de decir que no existe.
    """
).strip()

TD_ASSISTANT_MODEL = "gpt-4.1-mini"

VENDEDOR_CREDENTIALS = {
    "DIANASOFIA47": "DIANA SOFIA",
    "ALEJANDRO38": "ALEJANDRO RODRIGUEZ",
    "ANA45": "ANA KAREN ORTEGA MAHUAD",
    "CURSOS92": "CURSOS Y EVENTOS",
    "CASSANDRA93": "CASSANDRA MIROSLAVA",
    "CECILIA94": "CECILIA SEPULVEDA",
    "DANIELA73": "DANIELA LOPEZ RAMIREZ",
    "GRISELDA82": "GRISELDA CAROLINA SANCHEZ GARCIA",
    "GLORIA53": "GLORIA MICHELLE GARCIA TORRES",
    "JUAN24": "JUAN CASTILLEJO",
    "JOSE31": "JOSE CORTES",
    "KAREN58": "KAREN JAQUELINE",
    "PAULINA57": "PAULINA TREJO",
    "RUBEN67": "RUBEN",
    "ROBERTO51": "DISTRIBUCION Y UNIVERSIDADES",
}

st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")

# --- Ajustes UI compactos ---
st.markdown(
    """
    <style>
    section.main > div { padding-top: 0.5rem; }
    .header-compact h2 { margin: 0; font-size: 1.5rem; line-height: 1.6rem; }
    .header-meta { font-size: 0.8rem; color: #c9c9c9; }
    div[data-testid="stHorizontalBlock"] { gap: 0.4rem; }
    div[data-testid="stVerticalBlock"] > div:has(iframe) { margin-bottom: 0.2rem; }
    div[data-testid="stRadio"] > label { margin-bottom: 0; }
    div[data-testid="element-container"] { margin-bottom: 0.25rem; }
    div[data-testid="stRadio"] div[role="radiogroup"] {
        gap: 0.15rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.25);
        padding-bottom: 0.15rem;
    }
    div[data-testid="stRadio"] div[role="radiogroup"] > label {
        background: transparent;
        border: none;
        border-radius: 0.45rem 0.45rem 0 0;
        padding: 0.28rem 0.62rem;
        font-size: 0.82rem;
        color: rgba(255,255,255,.75);
    }
    div[data-testid="stRadio"] div[role="radiogroup"] > label[data-checked="true"] {
        background: rgba(255,255,255,.14);
        color: #fff;
        box-shadow: inset 0 -2px 0 rgba(255,255,255,.45);
    }
    div[data-testid="stRadio"] div[role="radiogroup"] > label svg {
        display: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Encabezado ---
current_time = datetime.now(TZ).strftime("%d/%m %H:%M:%S")
col_title, col_update, col_actions = st.columns([0.6, 0.2, 0.2])
with col_title:
    st.markdown(
        """
        <div class="header-compact">
            <h2 style="color: white;">
                <span style="font-size: 1.8rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
            </h2>
        </div>
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
with col_update:
    st.markdown(f'<div class="header-meta">🕒 Última actualización: {current_time}</div>', unsafe_allow_html=True)
with col_actions:
    if st.button("🔄 Refrescar ahora", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

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


def get_numeric_column(df: pd.DataFrame, column_name: str, default: float = 0.0) -> pd.Series:
    if not isinstance(df, pd.DataFrame):
        return pd.Series(dtype="float64")
    if column_name not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[column_name], errors="coerce").fillna(default)


def filter_df_by_vendedor(
    df: pd.DataFrame, vendedor: str, candidate_cols: list[str] | None = None
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty or vendedor == "(Todos)":
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    cols = candidate_cols or ["Vendedor_Registro", "Vendedor", "id_vendedor"]
    vend_norm = _normalize_vendedor_name(vendedor)
    mask = pd.Series(False, index=df.index)
    for col in cols:
        if col in df.columns:
            mask = mask | (df[col].map(_normalize_vendedor_name) == vend_norm)
    return df[mask].copy()


def ensure_columns(df: pd.DataFrame, columns: list[str], default_value="") -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        base = pd.DataFrame()
    else:
        base = df.copy()
    for col in columns:
        if col not in base.columns:
            base[col] = default_value
    return base


def init_td_assistant_state() -> None:
    if "td_assistant_messages" not in st.session_state:
        st.session_state.td_assistant_messages = []
    if "td_assistant_enable_image" not in st.session_state:
        st.session_state.td_assistant_enable_image = False


def init_login_state() -> None:
    if "auth_user" not in st.session_state:
        st.session_state.auth_user = ""
    if "auth_vendor" not in st.session_state:
        st.session_state.auth_vendor = ""


def get_query_param_value(param_name: str) -> str:
    value = st.query_params.get(param_name, "")
    if isinstance(value, list):
        return sanitize_text(value[0]) if value else ""
    return sanitize_text(value)


def clear_query_param(param_name: str) -> None:
    try:
        st.query_params.pop(param_name, None)
    except Exception:
        pass


def get_logged_vendor() -> str:
    return sanitize_text(st.session_state.get("auth_vendor", ""))


def get_logged_user() -> str:
    return sanitize_text(st.session_state.get("auth_user", ""))


def get_user_tone_instruction() -> str:
    logged_user = get_logged_user().upper()
    if logged_user == "GRISELDA82":
        return (
            "Cuando el usuario logueado sea GRISELDA82, puedes dirigirte a ella como Caro y usar "
            "un tono cercano, relajado y ligeramente juguetón, sin perder claridad operativa."
        )
    return ""


def get_openai_api_key() -> str:
    try:
        return sanitize_text(st.secrets["OPENAI_API_KEY"])
    except Exception:
        return ""


def _looks_like_latest_query(user_message: str) -> bool:
    normalized = unicodedata.normalize("NFKD", sanitize_text(user_message).lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    latest_patterns = [
        "ultimo pedido",
        "ultima orden",
        "pedido mas reciente",
        "pedido reciente",
        "mas reciente",
        "ultimo que subi",
        "ultimo que subio",
        "reciente",
    ]
    return any(pattern in normalized for pattern in latest_patterns)


def _select_relevant_rows_for_assistant(
    df: pd.DataFrame,
    user_message: str,
    candidate_columns: list[str],
    max_rows: int,
    match_columns: Optional[list[str]] = None,
    fallback_to_head: bool = False,
    sort_by_recent: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df

    work = df.copy()
    for col in candidate_columns:
        if col in work.columns:
            work[col] = work[col].apply(sanitize_text)

    def normalize_for_match(text: object) -> str:
        raw = sanitize_text(text).lower()
        normalized = unicodedata.normalize("NFKD", raw)
        return "".join(ch for ch in normalized if not unicodedata.combining(ch))

    stopwords = {
        "que", "cual", "cuál", "para", "por", "con", "sin", "del", "de", "los", "las",
        "una", "uno", "unos", "unas", "este", "esta", "eso", "esa", "material", "producto",
        "clave", "codigo", "código", "sera", "será", "me", "dice", "dime", "oye", "tal",
    }
    tokens: list[str] = []
    for raw_token in sanitize_text(user_message).replace("#", " ").split():
        token = normalize_for_match(raw_token)
        if not token:
            continue
        looks_like_code = any(ch.isdigit() for ch in token) and len(token) >= 2
        if looks_like_code:
            tokens.append(token)
            continue
        if len(token) >= 3 and token not in stopwords:
            tokens.append(token)
    if sort_by_recent and "Hora_Registro" in work.columns:
        try:
            work = work.assign(Hora_Registro=pd.to_datetime(work["Hora_Registro"], errors="coerce")).sort_values(
                "Hora_Registro", ascending=False, na_position="last"
            )
        except Exception:
            pass

    if not tokens:
        return work.head(max_rows) if fallback_to_head else work.iloc[0:0][candidate_columns]

    columns_for_match = [
        c
        for c in (
            match_columns
            or ["ID_Pedido", "Folio_Factura", "Cliente", "Vendedor", "Vendedor_Registro", "Estado"]
        )
        if c in work.columns
    ]
    if not columns_for_match:
        return work.head(max_rows) if fallback_to_head else work.iloc[0:0][candidate_columns]

    mask = pd.Series(False, index=work.index)
    normalized_cache: dict[str, pd.Series] = {}

    for col in columns_for_match:
        try:
            normalized_cache[col] = work[col].apply(normalize_for_match)
        except Exception:
            continue

    for token in tokens[:8]:
        for col, normalized_series in normalized_cache.items():
            try:
                mask = mask | normalized_series.str.contains(token, na=False)
            except Exception:
                continue

    if mask.any():
        return work.loc[mask, candidate_columns].head(max_rows)
    return work[candidate_columns].head(max_rows) if fallback_to_head else work.iloc[0:0][candidate_columns]


def _normalize_lookup_text(value: object) -> str:
    raw = sanitize_text(value).strip().lower()
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return "".join(ch for ch in normalized if ch.isalnum())


def _extract_lookup_tokens(user_message: str, min_length: int = 4, max_tokens: int = 10) -> list[str]:
    raw_tokens = re.findall(r"\b[a-zA-Z0-9\-_]+\b", sanitize_text(user_message))
    tokens: list[str] = []
    for raw_token in raw_tokens:
        cleaned = _normalize_lookup_text(raw_token)
        if len(cleaned) < min_length:
            continue
        if not any(ch.isdigit() for ch in cleaned):
            continue
        if cleaned not in tokens:
            tokens.append(cleaned)
        if len(tokens) >= max_tokens:
            break
    return tokens


def _extract_name_tokens(user_message: str, min_length: int = 3, max_tokens: int = 6) -> list[str]:
    stopwords = {
        "que", "cual", "cuál", "para", "por", "con", "sin", "del", "de", "los", "las",
        "una", "uno", "unos", "unas", "este", "esta", "eso", "esa", "material", "producto",
        "clave", "codigo", "código", "sera", "será", "me", "dice", "dime", "oye", "tal",
        "folio", "pedido", "cliente", "busca", "buscar", "encuentra", "enviar", "envio",
        "cuando", "quien", "quién", "donde", "dónde", "dato", "datos", "registrado",
    }
    raw_tokens = re.findall(r"\b[^\W\d_]+\b", sanitize_text(user_message), flags=re.UNICODE)
    tokens: list[str] = []
    for raw_token in raw_tokens:
        cleaned = _normalize_lookup_text(raw_token)
        if len(cleaned) < min_length or cleaned in stopwords:
            continue
        if cleaned not in tokens:
            tokens.append(cleaned)
        if len(tokens) >= max_tokens:
            break
    return tokens


def _build_exact_match_summary(
    df: pd.DataFrame,
    source_name: str,
    lookup_tokens: list[str],
    candidate_columns: list[str],
    match_columns: list[str],
    max_rows: int = 10,
) -> list[dict[str, object]]:
    if df is None or df.empty or not lookup_tokens:
        return []

    available_columns = [col for col in candidate_columns if col in df.columns]
    if not available_columns:
        available_columns = [col for col in match_columns if col in df.columns]
    if not available_columns:
        return []

    work = df.copy()
    available_match_columns = [col for col in match_columns if col in work.columns]
    if not available_match_columns:
        return []

    normalized_cache = {
        col: work[col].apply(_normalize_lookup_text)
        for col in available_match_columns
    }

    matched_indexes: list[int] = []
    matched_tokens_by_index: dict[int, set[str]] = {}
    for idx in work.index:
        row_tokens = {
            normalized_cache[col].get(idx, "")
            for col in available_match_columns
        }
        row_tokens.discard("")
        exact_hits = [token for token in lookup_tokens if token in row_tokens]
        if exact_hits:
            matched_indexes.append(idx)
            matched_tokens_by_index[idx] = set(exact_hits)

    if not matched_indexes:
        return []

    results: list[dict[str, object]] = []
    for idx in matched_indexes[:max_rows]:
        row = {
            key: _serialize_context_value(value)
            for key, value in work.loc[idx, available_columns].to_dict().items()
        }
        row["_source"] = source_name
        row["_matched_tokens"] = sorted(matched_tokens_by_index.get(idx, set()))
        results.append(row)
    return results


def _build_client_match_summary(
    df: pd.DataFrame,
    source_name: str,
    client_tokens: list[str],
    candidate_columns: list[str],
    client_column: str = "Cliente",
    max_rows: int = 10,
) -> list[dict[str, object]]:
    if df is None or df.empty or not client_tokens or client_column not in df.columns:
        return []

    available_columns = [col for col in candidate_columns if col in df.columns]
    if not available_columns:
        available_columns = [client_column]

    work = df.copy()
    client_series = work[client_column].apply(_normalize_lookup_text)
    strong_mask = pd.Series(True, index=work.index)
    broad_mask = pd.Series(False, index=work.index)

    for token in client_tokens:
        contains_token = client_series.str.contains(re.escape(token), na=False)
        strong_mask = strong_mask & contains_token
        broad_mask = broad_mask | contains_token

    selected = work.loc[strong_mask].copy()
    match_level = "all_tokens"
    if selected.empty:
        selected = work.loc[broad_mask].copy()
        match_level = "partial_tokens"
    if selected.empty:
        return []

    if client_column in selected.columns:
        selected["__client_match_score"] = selected[client_column].apply(
            lambda value: sum(1 for token in client_tokens if token in _normalize_lookup_text(value))
        )
        selected = selected.sort_values(
            by=["__client_match_score", client_column],
            ascending=[False, True],
            na_position="last",
        )

    results: list[dict[str, object]] = []
    for _, row_data in selected.head(max_rows).iterrows():
        row = {
            key: _serialize_context_value(value)
            for key, value in row_data[available_columns].to_dict().items()
        }
        row["_source"] = source_name
        row["_client_match_level"] = match_level
        row["_matched_name_tokens"] = [
            token for token in client_tokens if token in _normalize_lookup_text(row_data.get(client_column, ""))
        ]
        results.append(row)
    return results


def _build_approx_folio_match_summary(
    df: pd.DataFrame,
    source_name: str,
    lookup_tokens: list[str],
    candidate_columns: list[str],
    folio_column: str = "Folio_Factura",
    max_rows: int = 10,
    min_ratio: float = 0.78,
) -> list[dict[str, object]]:
    if df is None or df.empty or not lookup_tokens or folio_column not in df.columns:
        return []

    available_columns = [col for col in candidate_columns if col in df.columns]
    if not available_columns:
        available_columns = [folio_column]

    normalized_folios = df[folio_column].apply(_normalize_lookup_text)
    candidates: list[tuple[float, int, str]] = []
    for idx, normalized_folio in normalized_folios.items():
        if not normalized_folio:
            continue
        best_ratio = 0.0
        best_token = ""
        for token in lookup_tokens:
            if not token:
                continue
            length_gap = abs(len(token) - len(normalized_folio))
            if length_gap > 2:
                continue
            ratio = SequenceMatcher(None, token, normalized_folio).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_token = token
        if best_ratio >= min_ratio:
            candidates.append((best_ratio, idx, best_token))

    if not candidates:
        return []

    candidates.sort(key=lambda item: (-item[0], str(df.at[item[1], folio_column])))
    results: list[dict[str, object]] = []
    for ratio, idx, token in candidates[:max_rows]:
        row = {
            key: _serialize_context_value(value)
            for key, value in df.loc[idx, available_columns].to_dict().items()
        }
        row["_source"] = source_name
        row["_approx_match_token"] = token
        row["_approx_match_ratio"] = round(ratio, 3)
        results.append(row)
    return results


def _build_approx_client_match_summary(
    df: pd.DataFrame,
    source_name: str,
    client_tokens: list[str],
    candidate_columns: list[str],
    client_column: str = "Cliente",
    max_rows: int = 10,
    min_ratio: float = 0.72,
) -> list[dict[str, object]]:
    if df is None or df.empty or not client_tokens or client_column not in df.columns:
        return []

    available_columns = [col for col in candidate_columns if col in df.columns]
    if not available_columns:
        available_columns = [client_column]

    query_text = "".join(client_tokens)
    if not query_text:
        return []

    candidates: list[tuple[float, int]] = []
    for idx, client_value in df[client_column].items():
        normalized_client = _normalize_lookup_text(client_value)
        if not normalized_client:
            continue
        ratio = SequenceMatcher(None, query_text, normalized_client).ratio()
        token_hits = sum(1 for token in client_tokens if token in normalized_client)
        adjusted_ratio = ratio + min(token_hits * 0.06, 0.18)
        if adjusted_ratio >= min_ratio:
            candidates.append((adjusted_ratio, idx))

    if not candidates:
        return []

    candidates.sort(key=lambda item: (-item[0], str(df.at[item[1], client_column])))
    results: list[dict[str, object]] = []
    for ratio, idx in candidates[:max_rows]:
        row_data = df.loc[idx]
        row = {
            key: _serialize_context_value(value)
            for key, value in row_data[available_columns].to_dict().items()
        }
        row["_source"] = source_name
        row["_approx_client_ratio"] = round(ratio, 3)
        row["_matched_name_tokens"] = [
            token for token in client_tokens if token in _normalize_lookup_text(row_data.get(client_column, ""))
        ]
        results.append(row)
    return results


@st.cache_data(ttl=120)
def load_historicos_from_gsheets() -> pd.DataFrame:
    """Intenta leer 'datos_pedidos' (históricos). Si no existe, usa pedidos_confirmados."""

    try:
        ws_hist = spreadsheet.worksheet(SHEET_PEDIDOS_HISTORICOS)
        data = _fetch_with_retry(ws_hist, "_cache_datos_pedidos_historicos")
        if not data:
            return pd.DataFrame()
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
    except Exception:
        df = get_cached_confirmados_df(SHEET_CONFIRMADOS)

    for col in ["ID_Pedido", "Folio_Factura", "Cliente", "Vendedor", "Vendedor_Registro", "Estado", "Tipo_Envio"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].apply(sanitize_text)

    if "Hora_Registro" in df.columns:
        df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")

    return df


@st.cache_data(ttl=300)
def load_remote_postal_codes() -> set[str]:
    """Lee hoja Zonas_Remotas y devuelve CPs normalizados en formato texto."""
    try:
        ws_remote = spreadsheet.worksheet(SHEET_ZONAS_REMOTAS)
        data = _fetch_with_retry(ws_remote, "_cache_zonas_remotas")
    except Exception:
        return set()

    if not data:
        return set()

    codes: set[str] = set()
    for row in data[1:]:
        if not row:
            continue
        raw = sanitize_text(row[0])
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            continue
        codes.add(digits)
        codes.add(digits.lstrip("0") or "0")
    return codes


@st.cache_data(ttl=300)
def load_productos_from_gsheets() -> pd.DataFrame:
    try:
        ws_products = spreadsheet.worksheet(SHEET_PRODUCTOS)
        data = _fetch_with_retry(ws_products, "_cache_productos")
    except Exception:
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    for col in df.columns:
        df[col] = df[col].apply(sanitize_text)
    return df


def _filter_rows_by_vendor(df: pd.DataFrame, vendor_name: str) -> pd.DataFrame:
    if df is None or df.empty or not vendor_name:
        return pd.DataFrame() if df is None else df.copy()

    vend_norm = _normalize_vendedor_name(vendor_name)
    vendor_columns = [col for col in ["Vendedor", "Vendedor_Registro"] if col in df.columns]
    if not vendor_columns:
        return df.iloc[0:0].copy()

    mask = pd.Series(False, index=df.index)
    for col in vendor_columns:
        mask = mask | (df[col].map(_normalize_vendedor_name) == vend_norm)
    return df.loc[mask].copy()


def _serialize_context_value(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat(sep=" ")
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            pass
    return value


def _records_to_json_ready(df: pd.DataFrame) -> list[dict[str, object]]:
    records = df.to_dict(orient="records")
    return [
        {key: _serialize_context_value(val) for key, val in record.items()}
        for record in records
    ]


def build_logged_vendor_context(
    df_actual: pd.DataFrame,
    df_historicos: pd.DataFrame,
    df_casos: pd.DataFrame,
    user_message: str,
) -> str:
    logged_vendor = get_logged_vendor()
    if not logged_vendor:
        return ""

    latest_query = _looks_like_latest_query(user_message)
    sources = [
        ("data_pedidos", _filter_rows_by_vendor(df_actual, logged_vendor), ["Folio_Factura", "Cliente", "Vendedor", "Estado", "Tipo_Envio", "Hora_Registro", "Fecha_Entrega"]),
        ("datos_pedidos_historicos", _filter_rows_by_vendor(df_historicos, logged_vendor), ["Folio_Factura", "Cliente", "Vendedor", "Vendedor_Registro", "Estado", "Tipo_Envio", "Hora_Registro", "Fecha_Completado"]),
        ("casos_especiales", _filter_rows_by_vendor(df_casos, logged_vendor), ["Folio_Factura", "Cliente", "Vendedor_Registro", "Estado", "Completados_Limpiado", "Tipo_Envio", "Hora_Registro", "Fecha_Recepcion_Devolucion"]),
    ]

    snippets: list[str] = []
    for source_name, df_source, columns in sources:
        if df_source.empty:
            continue
        available_columns = [col for col in columns if col in df_source.columns]
        if not available_columns:
            continue
        try:
            df_source = df_source.assign(Hora_Registro=pd.to_datetime(df_source.get("Hora_Registro"), errors="coerce"))
            df_source = df_source.sort_values("Hora_Registro", ascending=False, na_position="last")
        except Exception:
            pass
        top_records = _records_to_json_ready(df_source[available_columns].head(3))
        snippets.append(f"Fuente {source_name}: {json.dumps(top_records, ensure_ascii=False)}")

    if not snippets:
        return ""

    latest_instruction = (
        "Si el usuario pregunta por el último pedido subido, usa primero el registro más reciente de data_pedidos para el vendedor logueado; "
        "solo usa históricos si no hay registros actuales. "
        if latest_query
        else ""
    )

    return dedent(
        f"""
        Contexto priorizado para el vendedor logueado:
        - Vendedor logueado: {logged_vendor}.
        - {latest_instruction}No asumas pedidos de otros vendedores si la pregunta está en primera persona o se refiere al contexto del vendedor logueado.
        - Registros recientes por vendedor:
        {chr(10).join(snippets)}
        """
    ).strip()


def build_remote_cp_context(user_message: str, remote_postal_codes: set[str]) -> str:
    found = re.findall(r"\b\d{4,5}\b", sanitize_text(user_message))
    cleaned_candidates = []
    for cp in found:
        normalized = cp.strip()
        if normalized and normalized not in cleaned_candidates:
            cleaned_candidates.append(normalized)

    rows: list[dict[str, str]] = []
    for cp in cleaned_candidates[:8]:
        variants = {cp, cp.lstrip("0") or "0"}
        is_remote = any(v in remote_postal_codes for v in variants)
        rows.append(
            {
                "codigo_postal": cp,
                "zona_remota": "sí" if is_remote else "no",
            }
        )

    return dedent(
        f"""
        Validación de Zonas Remotas:
        - Fuente: hoja '{SHEET_ZONAS_REMOTAS}'.
        - Total de códigos cargados: {len(remote_postal_codes)}.
        - Si el usuario pregunta por zona remota, responde directo con "sí/no" por cada CP detectado.
        - Si no detectas CP en el mensaje, pide el código postal exacto (4 o 5 dígitos).
        - Resultado para esta consulta: {json.dumps(rows, ensure_ascii=False)}
        """
    ).strip()


def build_td_orders_data_context(
    df_actual: pd.DataFrame,
    df_historicos: pd.DataFrame,
    df_casos: pd.DataFrame,
    remote_postal_codes: set[str],
    user_message: str,
    max_rows_per_source: int = 15,
) -> str:
    lookup_tokens = _extract_lookup_tokens(user_message)
    client_tokens = _extract_name_tokens(user_message)
    sources = [
        {
            "name": "data_pedidos",
            "description": "Pedidos en flujo actual (normalmente no han viajado)",
            "df": df_actual,
            "columns": ["Folio_Factura", "Cliente", "Vendedor", "Estado", "Tipo_Envio", "Turno", "Fecha_Entrega", "Hora_Registro"],
            "match_columns": ["Folio_Factura"],
        },
        {
            "name": "datos_pedidos_historicos",
            "description": "Pedidos históricos que ya viajaron (o fallback a pedidos_confirmados)",
            "df": df_historicos,
            "columns": ["Folio_Factura", "Cliente", "Vendedor", "Vendedor_Registro", "Estado", "Tipo_Envio", "Hora_Registro", "Fecha_Entrega", "Fecha_Completado"],
            "match_columns": ["Folio_Factura"],
        },
        {
            "name": "casos_especiales",
            "description": "Devoluciones, garantías y casos especiales",
            "df": df_casos,
            "columns": ["Folio_Factura", "Cliente", "Vendedor_Registro", "Estado", "Completados_Limpiado", "Tipo_Envio", "Tipo_Envio_Original", "Hora_Registro", "Fecha_Recepcion_Devolucion"],
            "match_columns": ["Folio_Factura"],
        },
    ]

    chunks: list[str] = []
    exact_matches: list[dict[str, object]] = []
    approx_folio_matches: list[dict[str, object]] = []
    client_matches: list[dict[str, object]] = []
    approx_client_matches: list[dict[str, object]] = []
    for source in sources:
        df = source["df"]
        if df is None or df.empty:
            chunks.append(
                f"Fuente: {source['name']} | {source['description']} | registros cargados: 0"
            )
            continue

        available_columns = [c for c in source["columns"] if c in df.columns]
        if not available_columns:
            available_columns = list(df.columns[:8])

        relevant = _select_relevant_rows_for_assistant(
            df=df,
            user_message=user_message,
            candidate_columns=available_columns,
            max_rows=max_rows_per_source,
            fallback_to_head=_looks_like_latest_query(user_message),
            sort_by_recent=_looks_like_latest_query(user_message),
        )
        records = _records_to_json_ready(relevant)
        exact_matches.extend(
            _build_exact_match_summary(
                df=df,
                source_name=source["name"],
                lookup_tokens=lookup_tokens,
                candidate_columns=available_columns,
                match_columns=source["match_columns"],
                max_rows=max_rows_per_source,
            )
        )
        approx_folio_matches.extend(
            _build_approx_folio_match_summary(
                df=df,
                source_name=source["name"],
                lookup_tokens=lookup_tokens,
                candidate_columns=available_columns,
                max_rows=max_rows_per_source,
            )
        )
        client_matches.extend(
            _build_client_match_summary(
                df=df,
                source_name=source["name"],
                client_tokens=client_tokens,
                candidate_columns=available_columns,
                max_rows=max_rows_per_source,
            )
        )
        approx_client_matches.extend(
            _build_approx_client_match_summary(
                df=df,
                source_name=source["name"],
                client_tokens=client_tokens,
                candidate_columns=available_columns,
                max_rows=max_rows_per_source,
            )
        )
        chunks.append(
            dedent(
                f"""
                Fuente: {source['name']}
                Descripción: {source['description']}
                Registros cargados: {len(df)}
                Registros relevantes para esta consulta: {len(records)} (máximo {max_rows_per_source})
                Columnas usadas: {', '.join(available_columns)}
                Datos: {json.dumps(records, ensure_ascii=False)}
                """
            ).strip()
        )

    remote_cp_context = build_remote_cp_context(
        user_message=user_message,
        remote_postal_codes=remote_postal_codes,
    )
    exact_match_context = dedent(
        f"""
        Resumen de verificación exacta de pedidos:
        - Tokens de búsqueda detectados: {json.dumps(lookup_tokens, ensure_ascii=False)}
        - Coincidencias exactas por Folio_Factura: {json.dumps(exact_matches, ensure_ascii=False)}
        - Coincidencias aproximadas por Folio_Factura (posible error de captura): {json.dumps(approx_folio_matches, ensure_ascii=False)}
        - Regla crítica: si aquí aparece al menos una coincidencia exacta, debes afirmar que SÍ existe el pedido y responder con esa fila.
        - Si no hay coincidencia exacta pero sí aproximada, di que probablemente se refiere a ese folio y pide confirmación breve.
        - Solo puedes decir que "no encontré" un pedido cuando la lista de coincidencias exactas y aproximadas esté vacía y tampoco exista evidencia suficiente en los registros relevantes o en coincidencias de cliente.
        """
    ).strip()
    client_match_context = dedent(
        f"""
        Resumen de coincidencias por nombre de cliente:
        - Tokens de nombre detectados: {json.dumps(client_tokens, ensure_ascii=False)}
        - Coincidencias por cliente: {json.dumps(client_matches, ensure_ascii=False)}
        - Coincidencias aproximadas por cliente (nombre incompleto o con error): {json.dumps(approx_client_matches, ensure_ascii=False)}
        - Si hay varias coincidencias por cliente, no niegues la existencia: responde que encontraste varias opciones y enumera las más útiles (folio, estado, fecha, vendedor).
        - Si solo hay coincidencias parciales o aproximadas por nombre, aclara que puede haber un nombre incompleto/error de captura y pide confirmar el cliente o el folio si hace falta.
        """
    ).strip()

    return dedent(
        f"""
        Contexto de datos internos TD para consulta operativa:
        - Usa estas fuentes para verificar si un pedido existe en sistema, su estado, vendedor y tipo.
        - Si está en data_pedidos: sigue en flujo actual. OJO: estado "Completado" aquí = pedido listo para recolección, no confirma entrega/envío final.
        - Si está en datos_pedidos_historicos: se considera histórico y ya salió de almacén.
        - Si está en casos_especiales: tratarlo como devolución/garantía/caso especial; confirmar salida con Completados_Limpiado = "sí".
        - Si piden búsqueda por nombre de cliente, prioriza data_pedidos y si no hay match continúa en datos_pedidos_historicos.
        - Si no aparece en ninguna fuente, dilo claramente y pide Folio/Cliente + fecha.
        - Nunca uses registros no relacionados como si fueran respuesta válida; si no hay match exacto, dilo.
        - Si la consulta pide el "último" o "más reciente", ordena por Hora_Registro descendente y aclara de qué fuente salió el dato.
        - Para dudas de zona remota por CP, prioriza "Validación de Zonas Remotas".

        {exact_match_context}

        {client_match_context}

        {remote_cp_context}

        {chr(10).join(chunks)}
        """
    ).strip()


def build_td_products_context(
    df_productos: pd.DataFrame,
    user_message: str,
    max_rows: int = 20,
) -> str:
    if df_productos is None or df_productos.empty:
        return "Catálogo de productos (hoja Productos): sin datos cargados."

    lookup_tokens = _extract_lookup_tokens(user_message)

    preferred_columns = [
        "Código",
        "Codigo",
        "Descripción",
        "Descripcion",
        "Descripción inglés",
        "Descripción Adicional",
        "Marca",
        "Línea",
        "Sublínea",
        "Subsublínea",
        "Tipo",
        "Subtipo",
        "Medida",
        "Precio de venta",
        "Costo",
        "Moneda del producto",
        "ClaveProdServ",
        "Tags e-commerce",
        "Descontinuado",
    ]
    available_columns = [c for c in preferred_columns if c in df_productos.columns]
    if not available_columns:
        available_columns = list(df_productos.columns[:12])

    code_col = "Código" if "Código" in df_productos.columns else ("Codigo" if "Codigo" in df_productos.columns else "")
    explicit_code_tokens = []
    for raw_token in sanitize_text(user_message).replace("#", " ").split():
        token = sanitize_text(raw_token).upper()
        clean_token = "".join(ch for ch in token if ch.isalnum() or ch in {"-", "_"})
        if len(clean_token) >= 4 and any(ch.isdigit() for ch in clean_token):
            explicit_code_tokens.append(clean_token)

    if code_col and explicit_code_tokens:
        code_series = df_productos[code_col].astype(str).str.upper()
        code_mask = pd.Series(False, index=df_productos.index)
        for token in explicit_code_tokens[:5]:
            code_mask = code_mask | code_series.str.contains(re.escape(token), na=False)
        if code_mask.any():
            exact_by_code = df_productos.loc[code_mask, available_columns].head(max_rows)
            records = exact_by_code.to_dict(orient="records")
            return dedent(
                f"""
                Catálogo de productos (hoja Productos):
                - Registros cargados: {len(df_productos)}
                - Registros relevantes para esta consulta: {len(records)} (máximo {max_rows})
                - Columnas usadas: {', '.join(available_columns)}
                - Regla de respuesta: se detectó una posible clave de producto en la consulta; prioriza responder con Código + Descripción exacta.
                Datos: {json.dumps(records, ensure_ascii=False)}
                """
            ).strip()

    exact_product_matches = _build_exact_match_summary(
        df=df_productos,
        source_name="Productos",
        lookup_tokens=lookup_tokens,
        candidate_columns=available_columns,
        match_columns=[col for col in ["Código", "Codigo", "ClaveProdServ"] if col in df_productos.columns],
        max_rows=max_rows,
    )

    relevant = _select_relevant_rows_for_assistant(
        df=df_productos,
        user_message=user_message,
        candidate_columns=available_columns,
        max_rows=max_rows,
        match_columns=[
            "Código",
            "Codigo",
            "Descripción",
            "Descripcion",
            "Descripción inglés",
            "Descripción Adicional",
            "Marca",
            "Línea",
            "Sublínea",
            "Subsublínea",
            "Tags e-commerce",
            "ClaveProdServ",
        ],
    )
    if relevant.empty:
        normalized_query = sanitize_text(user_message).strip().lower()
        similarity_col = "Descripción" if "Descripción" in df_productos.columns else ("Descripcion" if "Descripcion" in df_productos.columns else "")
        if similarity_col:
            work = df_productos.copy()
            work["__similaridad"] = work[similarity_col].map(
                lambda value: SequenceMatcher(None, sanitize_text(value).lower(), normalized_query).ratio()
            )
            relevant = work.sort_values("__similaridad", ascending=False)[available_columns].head(min(max_rows, 8))
    records = _records_to_json_ready(relevant)

    return dedent(
        f"""
        Catálogo de productos (hoja Productos):
        - Registros cargados: {len(df_productos)}
        - Registros relevantes para esta consulta: {len(records)} (máximo {max_rows})
        - Coincidencias exactas por código detectado: {json.dumps(exact_product_matches, ensure_ascii=False)}
        - Columnas usadas: {', '.join(available_columns)}
        - Regla de respuesta: cuando te pidan identificar un producto, prioriza mostrar Código + Descripción y, si existe, Marca/Línea/Precio de venta.
        - Si el producto exacto no aparece, dilo explícitamente y sugiere solo coincidencias plausibles de la hoja Productos; no respondas solo "no hay".
        - Si hay más de una coincidencia plausible, presenta opciones breves y pide confirmación.
        - Si existen coincidencias exactas por código, no digas que el material no existe.
        Datos: {json.dumps(records, ensure_ascii=False)}
        """
    ).strip()


def build_td_assistant_context(
    df_actual: pd.DataFrame,
    df_historicos: pd.DataFrame,
    df_casos: pd.DataFrame,
    df_productos: pd.DataFrame,
    remote_postal_codes: set[str],
    user_message: str,
    max_messages: int = 12,
) -> list[dict[str, str]]:
    history = st.session_state.get("td_assistant_messages", [])
    recent_history = history[-max_messages:]
    data_context = build_td_orders_data_context(
        df_actual=df_actual,
        df_historicos=df_historicos,
        df_casos=df_casos,
        remote_postal_codes=remote_postal_codes,
        user_message=user_message,
    )
    products_context = build_td_products_context(
        df_productos=df_productos,
        user_message=user_message,
    )
    logged_vendor_context = build_logged_vendor_context(
        df_actual=df_actual,
        df_historicos=df_historicos,
        df_casos=df_casos,
        user_message=user_message,
    )

    context = [{"role": "system", "content": TD_ASSISTANT_SYSTEM_PROMPT}]
    logged_vendor = get_logged_vendor()
    logged_user = get_logged_user()
    if logged_vendor:
        context.append(
            {
                "role": "system",
                "content": (
                    "Vendedor logueado en esta sesión: "
                    f"{logged_vendor}"
                    + (f" (usuario: {logged_user})" if logged_user else "")
                    + ". Usa este dato para personalizar la respuesta y para priorizar búsquedas del vendedor."
                ),
            }
        )
    tone_instruction = get_user_tone_instruction()
    if tone_instruction:
        context.append({"role": "system", "content": tone_instruction})
    if logged_vendor_context:
        context.append({"role": "system", "content": logged_vendor_context})
    context.append(
        {
            "role": "system",
            "content": f"Contexto interno para esta consulta\n{data_context}\n\n{products_context}",
        }
    )

    for item in recent_history:
        role = sanitize_text(item.get("role", ""))
        content = sanitize_text(item.get("content", ""))
        if role in {"user", "assistant"} and content:
            context.append({"role": role, "content": content})
    return context


def fetch_td_assistant_reply(
    user_message: str,
    df_actual: pd.DataFrame,
    df_historicos: pd.DataFrame,
    df_casos: pd.DataFrame,
    df_productos: pd.DataFrame,
    remote_postal_codes: set[str],
    image_bytes: Optional[bytes] = None,
    image_mime_type: Optional[str] = None,
) -> str:
    api_key = get_openai_api_key()
    if not api_key:
        raise ValueError("missing_api_key")

    client = OpenAI(api_key=api_key)
    context = build_td_assistant_context(
        df_actual=df_actual,
        df_historicos=df_historicos,
        df_casos=df_casos,
        df_productos=df_productos,
        remote_postal_codes=remote_postal_codes,
        user_message=user_message,
    )
    if image_bytes and image_mime_type:
        encoded_image = base64.b64encode(image_bytes).decode("utf-8")
        context.append(
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_message},
                    {
                        "type": "input_image",
                        "image_url": f"data:{image_mime_type};base64,{encoded_image}",
                    },
                ],
            }
        )
    else:
        context.append({"role": "user", "content": user_message})

    response = client.responses.create(
        model=TD_ASSISTANT_MODEL,
        input=context,
    )

    answer = sanitize_text(getattr(response, "output_text", ""))
    if answer:
        return answer
    return "No pude responder en este momento. Intenta de nuevo."

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


def build_auto_number_key(entry) -> str:
    base_id = sanitize_text(entry.get("id_pedido", ""))
    fallback = f"{sanitize_text(entry.get('cliente', ''))}|{sanitize_text(entry.get('hora', ''))}"
    base = base_id or fallback
    categoria = sanitize_text(entry.get("categoria", ""))
    if not base:
        return ""
    if categoria:
        return f"{categoria}|{base}"
    return base


def build_surtidor_key(entry) -> str:
    key = build_auto_number_key(entry)
    if key:
        return key
    fallback = f"{sanitize_text(entry.get('cliente', ''))}|{sanitize_text(entry.get('hora', ''))}"
    return fallback


def apply_surtidor_assignments(entries, assignments: dict) -> None:
    for entry in entries:
        key = build_surtidor_key(entry)
        if key:
            entry["surtidor"] = assignments.get(key, "")


def assign_shared_numbers(entries_local, entries_foraneo):
    combined = list(entries_local) + list(entries_foraneo)
    combined.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    counter = count(1)
    def next_number() -> int:
        return next(counter)

    number_map = {}
    for entry in combined:
        key = build_auto_number_key(entry)
        if not key:
            number = next_number()
            key = f"__auto__{number}"
            number_map[key] = number
        elif key not in number_map:
            number_map[key] = next_number()
        entry["numero"] = number_map[key]
        # NO borrar sort_key


_AUTO_LIST_COUNTER = count(1)


def _parse_foraneo_number(raw) -> Optional[int]:
    text = sanitize_text(raw)
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        value = int(digits)
    except Exception:
        return None
    return value if value > 0 else None


def _flow_match_key(value) -> str:
    return sanitize_text(value).lower()


def _flow_row_key_from_row(row: pd.Series) -> str:
    for field in ("gsheet_row_index", "_gsheet_row_index", "__sheet_row"):
        raw = row.get(field)
        try:
            if raw is not None and not pd.isna(raw):
                return f"row:{int(float(raw))}"
        except Exception:
            continue
    return ""


def _flow_row_key_from_entry(entry: dict) -> str:
    raw = entry.get("gsheet_row_index")
    try:
        if raw is not None and not pd.isna(raw):
            return f"row:{int(float(raw))}"
    except Exception:
        pass
    return ""


def _is_cancelado_estado(value: object) -> bool:
    estado = sanitize_text(value).lower()
    return "cancelado" in estado


def _build_flow_number_maps(df_all: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    if df_all.empty:
        return {}, {}

    work = df_all.copy()
    if "Tipo_Envio" not in work.columns:
        work["Tipo_Envio"] = ""
    if "Tipo_Envio_Original" not in work.columns:
        work["Tipo_Envio_Original"] = ""
    if "ID_Pedido" not in work.columns:
        work["ID_Pedido"] = ""
    if "Folio_Factura" not in work.columns:
        work["Folio_Factura"] = ""

    tipo_norm = work["Tipo_Envio"].astype(str).apply(_normalize_envio_original)
    tipo_original_norm = work["Tipo_Envio_Original"].astype(str).apply(_normalize_envio_original)
    mask_foraneo = tipo_norm.str.contains("foraneo", na=False) | tipo_original_norm.str.contains("foraneo", na=False)

    df_foraneo = work[mask_foraneo].reset_index(drop=True)
    df_local = work[~mask_foraneo].reset_index(drop=True)

    def build_map(df_src: pd.DataFrame, formatter) -> dict[str, str]:
        out: dict[str, str] = {}
        for idx, row in df_src.iterrows():
            numero = formatter(idx)
            row_key = _flow_row_key_from_row(row)
            for raw_key in (row_key, row.get("ID_Pedido", ""), row.get("Folio_Factura", "")):
                key = raw_key if isinstance(raw_key, str) and raw_key.startswith("row:") else _flow_match_key(raw_key)
                if key and key not in out:
                    out[key] = numero
        return out

    local_map = build_map(df_local, lambda idx: str(idx + 1))
    foraneo_map = build_map(df_foraneo, lambda idx: f"{idx + 1:02d}")
    return local_map, foraneo_map


def assign_flow_numbers(entries_local, entries_foraneo, df_all: pd.DataFrame) -> None:
    local_map, _ = _build_flow_number_maps(df_all)
    foraneo_map: dict[str, str] = {}

    # Integrar Numero_Foraneo manual de devoluciones/casos foráneos,
    # sin alterar la numeración base de pedidos foráneos.
    def _is_limpiado_entry(entry: dict) -> bool:
        raw = sanitize_text(entry.get("completados_limpiado", "")).lower().strip()
        normalized = unicodedata.normalize("NFKD", raw)
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        return normalized == "si"

    active_foraneo_entries = [
        e for e in sorted(entries_foraneo, key=lambda e: e.get("sort_key", pd.Timestamp.max))
        if not _is_cancelado_estado(e.get("estado", "")) and not _is_limpiado_entry(e)
    ]

    manual_numbers: set[int] = set()
    for entry in active_foraneo_entries:
        if not sanitize_text(entry.get("tipo", "")):
            continue
        parsed = _parse_foraneo_number(entry.get("numero_foraneo", ""))
        if parsed is not None:
            manual_numbers.add(parsed)

    used_numbers: set[int] = set(manual_numbers)
    # Mantener continuidad del flujo: pedidos foráneos normales deben
    # ocupar el menor número libre, respetando los manuales reservados
    # por devoluciones/casos (Numero_Foraneo).
    #
    # Si un caso manual toma 23, los pedidos previos siguen 01-22 y el
    # siguiente libre será 24.
    next_foraneo = 1

    # 1) Casos/devoluciones foráneos con Numero_Foraneo manual.
    for entry in active_foraneo_entries:
        if not sanitize_text(entry.get("tipo", "")):
            continue

        keys = [
            _flow_row_key_from_entry(entry),
            _flow_match_key(entry.get("id_pedido", "")),
            _flow_match_key(entry.get("folio", "")),
        ]
        if not any(keys):
            continue

        parsed = _parse_foraneo_number(entry.get("numero_foraneo", ""))
        if parsed is None:
            continue

        numero_fmt = f"{parsed:02d}"
        for key in keys:
            if key and key not in foraneo_map:
                foraneo_map[key] = numero_fmt

    # 2) Pedidos foráneos normales: secuencia continua sin repetir manuales.
    for entry in active_foraneo_entries:
        if sanitize_text(entry.get("tipo", "")):
            continue

        keys = [
            _flow_row_key_from_entry(entry),
            _flow_match_key(entry.get("id_pedido", "")),
            _flow_match_key(entry.get("folio", "")),
        ]
        if not any(keys):
            continue
        row_key = keys[0] if keys else ""
        if row_key and row_key in foraneo_map:
            continue

        while next_foraneo in used_numbers:
            next_foraneo += 1
        numero = next_foraneo
        next_foraneo += 1

        used_numbers.add(numero)
        numero_fmt = f"{numero:02d}"
        for key in keys:
            if key and key not in foraneo_map:
                foraneo_map[key] = numero_fmt

    def assign(
        entries,
        primary_map: dict[str, str],
        fallback_map: dict[str, str],
        suppress_cancelled_number: bool = False,
    ) -> None:
        for entry in entries:
            if suppress_cancelled_number and _is_cancelado_estado(entry.get("estado", "")):
                entry["numero"] = ""
                continue

            if suppress_cancelled_number and _is_limpiado_entry(entry):
                entry["numero"] = ""
                continue

            if suppress_cancelled_number and sanitize_text(entry.get("tipo", "")):
                if _parse_foraneo_number(entry.get("numero_foraneo", "")) is None:
                    entry["numero"] = ""
                    continue

            keys = [
                _flow_row_key_from_entry(entry),
                _flow_match_key(entry.get("id_pedido", "")),
                _flow_match_key(entry.get("folio", "")),
            ]
            number = None
            for key in keys:
                if key and key in primary_map:
                    number = primary_map[key]
                    break

            if number is None and not suppress_cancelled_number:
                for key in keys:
                    if key and key in fallback_map:
                        number = fallback_map[key]
                        break

            if suppress_cancelled_number and sanitize_text(entry.get("tipo", "")) and not number:
                entry["numero"] = ""
            else:
                entry["numero"] = number or "?"

    assign(entries_local, local_map, foraneo_map)
    assign(entries_foraneo, foraneo_map, local_map, suppress_cancelled_number=True)


def assign_display_numbers(auto_local_entries, auto_foraneo_entries, today_date) -> None:
    _ = today_date
    for entry in auto_local_entries + auto_foraneo_entries:
        entry.pop("display_num", None)
        numero_raw = sanitize_text(entry.get("numero", ""))
        try:
            entry["display_num"] = int(numero_raw)
        except Exception:
            continue

_TURNOS_CANONICAL = {
    "☀ local manana": "☀️ Local Mañana",
    "local manana": "☀️ Local Mañana",
    "🌙 local tarde": "🌙 Local Tarde",
    "local tarde": "🌙 Local Tarde",
    "🌵 saltillo": "🌵 Saltillo",
    "saltillo": "🌵 Saltillo",
    "📦 pasa a bodega": "📦 Pasa a Bodega",
    "pasa a bodega": "📦 Pasa a Bodega",
}


def normalize_turno_label(value: str) -> str:
    base = sanitize_text(value)
    if not base:
        return ""

    without_variation = base.replace("\ufe0f", "")
    normalized = unicodedata.normalize("NFKD", without_variation)
    ascii_clean = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    key = " ".join(ascii_clean.lower().split())

    return _TURNOS_CANONICAL.get(key, base.strip())


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
        "completados_limpiado": sanitize_text(row.get("Completados_Limpiado", "")),
        "cliente": format_cliente_line(row),
        "cliente_nombre": sanitize_text(row.get("Cliente", "")),
        "folio": sanitize_text(row.get("Folio_Factura", "")),
        "fecha": format_date(row.get("Fecha_Entrega")),
        "hora": format_time(row.get("Hora_Registro")),
        "fecha_entrega_dt": parse_datetime(row.get("Fecha_Entrega")),
        "id_pedido": sanitize_text(row.get("ID_Pedido", "")),
        "vendedor": sanitize_text(row.get("Vendedor_Registro", "")),
        "turno": sanitize_text(row.get("Turno", "")),
        "tipo_envio": sanitize_text(row.get("Tipo_Envio", "")),
        "tipo_envio_original": sanitize_text(row.get("Tipo_Envio_Original", "")),
        "tipo": sanitize_text(row.get("Tipo", "")),
        "numero_foraneo": sanitize_text(row.get("Numero_Foraneo", "")),
        "gsheet_row_index": row.get("gsheet_row_index", row.get("_gsheet_row_index", row.get("__sheet_row"))),
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
        estado_entrega = sanitize_text(row.get("Estado_Entrega", ""))
        if estado_entrega == "⏳ No Entregado":
            details.append("⏳ Entrega: No Entregado")
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
        if entry["tipo_envio"] and entry["tipo_envio"] not in badges:
            details.append(f"🚚 {entry['tipo_envio']}")
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
        tipo_caso = sanitize_text(entry.get("tipo", ""))
        if tipo_caso and tipo_caso != "—":
            details.append(tipo_caso)
        elif entry["tipo_envio_original"] and entry["tipo_envio_original"] not in badges:
            details.append(f"📦 {entry['tipo_envio_original']}")
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

        meta_html = (
            f"<div class='auto-card-meta'>📅 Fecha Entrega: {entry['fecha']}</div>"
            if entry.get("fecha")
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

def render_auto_list(
    entries,
    title: str,
    subtitle: str = "",
    max_rows: int = 60,
    start_number: int = 1,
    panel_height: int = 720,
    scroll_max_height: int = 640,
    show_header: bool = True,
):
    if not entries:
        st.info("No hay pedidos para mostrar.")
        return start_number

    indexed_entries = list(enumerate(entries, start_number))
    visible = indexed_entries[:max_rows]

    rows_html = []
    for fallback_number, e in visible:
        is_cancelado = _is_cancelado_estado(e.get("estado", ""))
        has_explicit_number = bool(sanitize_text(e.get("numero", "")))
        display_number = None
        if not is_cancelado and has_explicit_number:
            display_number = e.get("display_num", fallback_number)
        number_label = f"#{display_number}" if display_number is not None else "—"
        chips = []

        # Chips principales (máx 3)
        for b in (e.get("badges", []) or [])[:3]:
            bb = sanitize_text(b)
            if bb:
                chips.append(f"<span class='chip'>{bb}</span>")

        # Detalles (máx 1)
        details = e.get("details", []) or []
        if details:
            d0 = sanitize_text(details[0])
            if d0:
                chips.append(f"<span class='chip'>{d0}</span>")

        # ⚠️ Marca “Sin fecha”
        dt_ent = e.get("fecha_entrega_dt")
        try:
            is_missing = (dt_ent is None) or pd.isna(dt_ent)
        except Exception:
            is_missing = (dt_ent is None)
        if is_missing:
            chips.insert(0, "<span class='chip'>⚠️ Sin Fecha_Entrega</span>")

        # 📅 Fecha de entrega visible (si existe)
        fecha_txt = sanitize_text(e.get("fecha", ""))
        if fecha_txt:
            chips.insert(0, f"<span class='chip'>📅 {fecha_txt}</span>")


        chips_html = (
            f"<div class='board-meta'>{''.join(chips)}"
            f"<span class='board-status'>{sanitize_text(e.get('estado',''))}</span></div>"
        )

        surtidor = sanitize_text(e.get("surtidor", ""))
        surtidor_html = (
            f"<span class='surtidor-tag'>{surtidor}</span>" if surtidor else ""
        )
        rows_html.append(
            f"""
            <tr class='board-row'>
              <td class='board-n'>{number_label}</td>
              <td class='board-main'>
                <div class='board-client'>{e.get('cliente','—')}{surtidor_html}</div>
                {chips_html}
              </td>
            </tr>
            """
        )

    sub = f"<div class='board-sub'>{subtitle}</div>" if subtitle else ""

    list_id = f"board-{next(_AUTO_LIST_COUNTER)}"
    scroll_class = "board-scroll"

    header_html = (
        f"""
    <div class=\"board-title\">
        <div>{title}{sub}</div>
        <div class=\"board-sub\">Mostrando {len(visible)}/{len(entries)}</div>
    </div>
    """
        if show_header
        else ""
    )

    row_height_px = 44
    title_height_px = 58 if show_header else 10
    min_content = 140 if show_header else 100
    safety_padding_px = 24
    content_height = max(
        min_content, (len(visible) * row_height_px) + title_height_px + safety_padding_px
    )
    component_height = content_height

    html = f"""
    <style>
    .board-col{{flex:1;background:rgba(18,18,20,0.92);border-radius:0.9rem;padding:0.55rem 0.7rem;box-shadow:0 2px 12px rgba(0,0,0,0.25);height:100%;font-family:"Source Sans Pro", sans-serif;}}
    .board-title{{display:flex;justify-content:space-between;align-items:center;gap:0.6rem;margin-bottom:0.45rem;font-weight:600;font-size:1.03rem;color:#fff;letter-spacing:0.01em;}}
    .board-sub{{font-size:0.73rem;opacity:0.8;font-weight:500;}}
    .board-table{{width:100%;border-collapse:collapse;table-layout:fixed;}}
    .board-row{{border-top:1px solid rgba(255,255,255,0.08);}}
    .board-row:first-child{{border-top:none;}}
    .board-n{{width:2.3rem;font-size:0.95rem;font-weight:600;padding:0.15rem 0.12rem;opacity:0.95;vertical-align:top;white-space:nowrap;color:#fff;}}
    .board-main{{padding:0.18rem 0.15rem;vertical-align:top;}}
    .board-client{{font-size:0.84rem;font-weight:500;line-height:1.05rem;color:#fff;word-break:break-word;display:flex;align-items:center;gap:0.3rem;flex-wrap:wrap;}}
    .surtidor-tag{{margin-left:0.2rem;padding:0.08rem 0.36rem;border-radius:0.7rem;background:rgba(114,190,255,0.18);color:#a9dcff;font-weight:600;font-size:0.68rem;white-space:nowrap;}}
    .board-meta{{margin-top:0.08rem;display:flex;flex-wrap:wrap;gap:0.2rem;font-size:0.66rem;opacity:0.85;font-weight:500;align-items:center;color:#fff;line-height:1rem;}}
    .chip{{padding:0.04rem 0.34rem;border-radius:0.55rem;background:rgba(255,255,255,0.10);white-space:nowrap;}}
    .board-status{{margin-left:auto;font-size:0.7rem;font-weight:600;white-space:nowrap;opacity:0.95;}}
    #{list_id} .board-scroll{{max-height:none;overflow:visible;position:relative;}}
    </style>
    <div class="board-col" id="{list_id}">
    {header_html}
    <div class="{scroll_class}">
        <table class="board-table">
            {''.join(rows_html)}
        </table>
    </div>
    </div>
    """


    # ✅ Forzar render HTML real (no texto)
    components.html(html, height=component_height, scrolling=False)
    return start_number + len(visible)


def _is_done_estado(estado: str) -> bool:
    s = sanitize_text(estado)
    return s in {"🟢 Completado", "🟣 Cancelado", "✅ Viajó"}


def _is_visible_auto_entry(entry: dict) -> bool:
    if not _is_done_estado(entry.get("estado", "")):
        return True
    return sanitize_text(entry.get("completados_limpiado", "")) == ""


def _is_surtidor_visible_estado(estado: str) -> bool:
    cleaned = sanitize_text(estado).lower()
    if any(term in cleaned for term in ("pendiente", "demorado", "modificacion")):
        return False
    return "en proceso" in cleaned or "completado" in cleaned


def last_3_days_previous_range(today_date):
    start = today_date - timedelta(days=3)
    end = today_date - timedelta(days=1)
    return start, end


def filter_entries_by_entrega(entries, start_date, end_date):
    """Incluye entries cuya Fecha_Entrega_dt esté entre start_date y end_date (incluye límites)."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if start_date <= d <= end_date:
            out.append(e)
    return out


def filter_entries_before_date(entries, reference_date):
    """Incluye entries con Fecha_Entrega anterior a reference_date."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d < reference_date:
            out.append(e)
    return out


def filter_entries_on_or_after(entries, reference_date):
    """Incluye entries con Fecha_Entrega en reference_date o posterior."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d >= reference_date:
            out.append(e)
    return out


def filter_entries_on_date(entries, reference_date):
    """Incluye entries con Fecha_Entrega exactamente en reference_date."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d == reference_date:
            out.append(e)
    return out


def filter_entries_no_entrega_date(entries):
    """Entries sin Fecha_Entrega (para que no se pierdan)."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            out.append(e)
            continue
        try:
            if pd.isna(dt):
                out.append(e)
        except Exception:
            pass
    return out


def sort_entries_by_delivery(entries):
    """Ordena por Fecha_Entrega (más próxima primero), luego por sort_key."""
    def _key(e):
        dt = e.get("fecha_entrega_dt")
        try:
            if dt is None or pd.isna(dt):
                dt = pd.Timestamp.max
        except Exception:
            if dt is None:
                dt = pd.Timestamp.max
        if not isinstance(dt, pd.Timestamp):
            try:
                dt = pd.to_datetime(dt)
            except Exception:
                dt = pd.Timestamp.max
        return (dt, e.get("sort_key", pd.Timestamp.max))

    return sorted(entries, key=_key)


def sort_entries_by_flow_number_desc(entries):
    """Ordena por número de flujo descendente (más reciente arriba)."""

    def _num(entry):
        raw = sanitize_text(entry.get("numero", ""))
        try:
            return int(raw)
        except Exception:
            return -1

    return sorted(entries, key=lambda e: (_num(e), e.get("sort_key", pd.Timestamp.min)), reverse=True)


def _normalize_match_value(value: str) -> str:
    cleaned = sanitize_text(value)
    return cleaned.lower()


def drop_local_duplicates_for_cases(
    df_local: pd.DataFrame, df_casos: pd.DataFrame
) -> pd.DataFrame:
    if df_local.empty or df_casos.empty:
        return df_local

    case_ids = set()
    case_folios = set()
    if "ID_Pedido" in df_casos.columns:
        case_ids = {
            _normalize_match_value(v)
            for v in df_casos["ID_Pedido"].astype(str)
            if _normalize_match_value(v)
        }
    if "Folio_Factura" in df_casos.columns:
        case_folios = {
            _normalize_match_value(v)
            for v in df_casos["Folio_Factura"].astype(str)
            if _normalize_match_value(v)
        }

    if not case_ids and not case_folios:
        return df_local

    local_ids = pd.Series("", index=df_local.index, dtype=str)
    local_folios = pd.Series("", index=df_local.index, dtype=str)
    if "ID_Pedido" in df_local.columns:
        local_ids = df_local["ID_Pedido"].astype(str).apply(_normalize_match_value)
    if "Folio_Factura" in df_local.columns:
        local_folios = df_local["Folio_Factura"].astype(str).apply(_normalize_match_value)

    mask_case_id = (
        local_ids.isin(case_ids)
        if case_ids
        else pd.Series(False, index=df_local.index)
    )
    mask_case_folio = (
        local_folios.isin(case_folios)
        if case_folios
        else pd.Series(False, index=df_local.index)
    )
    mask_duplicate = mask_case_id | mask_case_folio

    return df_local.loc[~mask_duplicate].copy()


def get_local_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    base_local = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        base_local = df_all[
            df_all["Tipo_Envio"].isin(["📍 Pedido Local", "🎓 Cursos y Eventos"])
        ].copy()

    extra_local = pd.DataFrame()
    if not df_all.empty and "Turno" in df_all.columns:
        turnos_locales = {"🌵 Saltillo", "📦 Pasa a Bodega"}
        turno_normalizado = df_all["Turno"].fillna("").astype(str).str.strip().apply(
            normalize_turno_label
        )
        mask_turno_local = turno_normalizado.isin(turnos_locales)
        extra_local = df_all[mask_turno_local].copy()

    casos_local, _ = get_case_envio_assignments(df_all)
    frames = [df for df in [base_local, extra_local, casos_local] if not df.empty]
    if not frames:
        return pd.DataFrame()

    df_local = pd.concat(frames, ignore_index=True, sort=False)
    df_local = df_local.drop_duplicates()

    if "Completados_Limpiado" not in df_local.columns:
        df_local["Completados_Limpiado"] = ""

    if "Estado_Entrega" in df_local.columns:
        estado_entrega_col = df_local["Estado_Entrega"].astype(str).str.strip()
        mask_no_entregado = estado_entrega_col == "⏳ No Entregado"
    else:
        mask_no_entregado = pd.Series(False, index=df_local.index, dtype=bool)

    filtro_completados = df_local["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
    filtro_limpiado = df_local["Completados_Limpiado"].astype(str).str.lower() == "sí"

    df_local = df_local[~(filtro_completados & filtro_limpiado & ~mask_no_entregado)].copy()

    if "Turno" not in df_local.columns:
        df_local["Turno"] = ""

    df_local["Turno"] = df_local["Turno"].fillna("").astype(str).str.strip()
    df_local.loc[df_local["Turno"].str.lower() == "nan", "Turno"] = ""
    df_local["Turno"] = df_local["Turno"].apply(normalize_turno_label)

    mask_curso_evento = df_local["Tipo_Envio"] == "🎓 Cursos y Eventos"
    mask_turno_vacio = df_local["Turno"].str.strip() == ""
    df_local.loc[mask_curso_evento & mask_turno_vacio, "Turno"] = "🎓 Cursos y Eventos"

    return df_local


def get_foraneo_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    base_foraneo = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        base_foraneo = df_all[df_all["Tipo_Envio"] == "🚚 Pedido Foráneo"].copy()

    _, casos_foraneo = get_case_envio_assignments(df_all)
    frames = [df for df in [base_foraneo, casos_foraneo] if not df.empty]
    if not frames:
        return pd.DataFrame()

    df_for = pd.concat(frames, ignore_index=True, sort=False)

    if "Completados_Limpiado" not in df_for.columns:
        df_for["Completados_Limpiado"] = ""

    df_for = df_for[
        ~(
            df_for["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_for["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()

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

        if "Turno" in df_casos.columns:
            df_casos["Turno"] = df_casos["Turno"].apply(normalize_turno_label)

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


def _normalize_envio_original(value: str) -> str:
    """Remove emojis/accents and return a lowercased representation."""
    cleaned = sanitize_text(value)
    if not cleaned:
        return ""
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    filtered = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in ascii_only)
    return " ".join(filtered.lower().split())


def get_case_envio_assignments(
    df_all: pd.DataFrame, df_casos: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return local/foráneo assignments detected from special cases."""

    if df_casos is None:
        df_casos = get_casos_orders(df_all)

    if df_casos.empty:
        return pd.DataFrame(), pd.DataFrame()

    working = df_casos.copy()
    if "Turno" not in working.columns:
        working["Turno"] = ""

    normalized = working["Tipo_Envio_Original"].apply(_normalize_envio_original)
    turno_clean = working["Turno"].astype(str).fillna("").str.strip()

    mask_local = normalized.str.contains("local", na=False) & (turno_clean != "")
    mask_foraneo = normalized.str.contains("foraneo", na=False)

    df_local = working[mask_local].copy()
    if not df_local.empty:
        df_local["Tipo_Envio"] = "📍 Pedido Local"

    df_foraneo = working[mask_foraneo].copy()
    if not df_foraneo.empty:
        df_foraneo["Tipo_Envio"] = "🚚 Pedido Foráneo"

    return df_local, df_foraneo


def _pedido_selector_envio_emoji(tipo_envio: str) -> str:
    normalized = _normalize_envio_original(tipo_envio)
    if "foraneo" in normalized:
        return "🚚"
    if "local" in normalized:
        return "📍"
    return ""

st.markdown(
    """
    <style>
    .board-wrap{display:flex;gap:0.8rem;width:100%;align-items:flex-start;}
    .board-col{flex:1;background:rgba(18,18,20,0.92);border-radius:0.9rem;padding:0.8rem 0.9rem;box-shadow:0 2px 14px rgba(0,0,0,0.25);min-height:70vh;}
    .board-title{display:flex;justify-content:space-between;align-items:center;gap:0.6rem;margin-bottom:0.6rem;font-weight:800;font-size:1.35rem;color:#fff;}
    .board-sub{font-size:0.9rem;opacity:0.8;font-weight:600;}
    .board-table{width:100%;border-collapse:collapse;table-layout:fixed;}
    .board-row{border-top:1px solid rgba(255,255,255,0.08);}
    .board-row:first-child{border-top:none;}
    .board-n{width:3.2rem;font-size:1.35rem;font-weight:900;padding:0.25rem 0.2rem;opacity:0.95;vertical-align:top;white-space:nowrap;}
    .board-main{padding:0.25rem 0.2rem;vertical-align:top;}
    .board-client{font-size:1.05rem;font-weight:800;line-height:1.25rem;color:#fff;word-break:break-word;}
    .board-meta{margin-top:0.18rem;display:flex;flex-wrap:wrap;gap:0.35rem;font-size:0.85rem;opacity:0.85;font-weight:650;align-items:center;}
    .chip{padding:0.1rem 0.45rem;border-radius:0.7rem;background:rgba(255,255,255,0.10);white-space:nowrap;}
    .board-status{margin-left:auto;font-size:0.95rem;font-weight:900;white-space:nowrap;opacity:0.95;}
    @media (min-width: 1200px){
      .board-client{font-size:1.15rem;}
      .board-n{font-size:1.5rem;}
      .board-title{font-size:1.5rem;}
    }
    </style>
    """,
    unsafe_allow_html=True,
)

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
SHEET_PEDIDOS = "data_pedidos"
SHEET_CASOS = "casos_especiales"
SHEET_CONFIRMADOS = "pedidos_confirmados"
SHEET_PEDIDOS_HISTORICOS = "datos_pedidos"
SHEET_ZONAS_REMOTAS = "Zonas_Remotas"
SHEET_PRODUCTOS = "Productos"


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
            if "expired" in str(e).lower() or "UNAUTHENTICATED" in str(e):
                get_gspread_client.clear()
            wait_time = min(30, 2 ** (attempt - 1))
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


@st.cache_resource
def get_main_sheet_handles(_credentials_json_dict):
    client = get_gspread_client(_credentials_json_dict=_credentials_json_dict)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return {
        "client": client,
        "spreadsheet": spreadsheet,
        "worksheet_main": spreadsheet.worksheet(SHEET_PEDIDOS),
        "worksheet_casos": spreadsheet.worksheet(SHEET_CASOS),
    }


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

    handles = get_main_sheet_handles(_credentials_json_dict=GSHEETS_CREDENTIALS)
    g_spread_client = handles["client"]
    s3_client = get_s3_client()
    spreadsheet = handles["spreadsheet"]
    worksheet_main = handles["worksheet_main"]
    worksheet_casos = handles["worksheet_casos"]

except gspread.exceptions.APIError as e:
    auth_error_text = str(e)
    if any(token in auth_error_text for token in ["ACCESS_TOKEN_EXPIRED", "UNAUTHENTICATED", "RESOURCE_EXHAUSTED", "429"]):
        st.warning("🔄 Ajustando conexión con Google Sheets...")
        time.sleep(1)
        get_main_sheet_handles.clear()
        get_gspread_client.clear()
        handles = get_main_sheet_handles(_credentials_json_dict=GSHEETS_CREDENTIALS)
        g_spread_client = handles["client"]
        s3_client = get_s3_client()
        spreadsheet = handles["spreadsheet"]
        worksheet_main = handles["worksheet_main"]
        worksheet_casos = handles["worksheet_casos"]
    else:
        st.error(f"❌ Error al autenticar clientes: {e}")
        st.stop()
except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()


def refresh_main_sheet_handles() -> bool:
    """Fuerza reconexión de handles principales a Google Sheets en caliente."""
    global g_spread_client, spreadsheet, worksheet_main, worksheet_casos
    try:
        get_main_sheet_handles.clear()
        get_gspread_client.clear()
        handles = get_main_sheet_handles(_credentials_json_dict=GSHEETS_CREDENTIALS)
        g_spread_client = handles["client"]
        spreadsheet = handles["spreadsheet"]
        worksheet_main = handles["worksheet_main"]
        worksheet_casos = handles["worksheet_casos"]
        return True
    except Exception:
        return False


# --- Carga de datos ---
def _fetch_with_retry(worksheet, cache_key: str, max_attempts: int = 4):
    """Lee datos de una worksheet con reintentos y respaldo local.

    Cuando Google Sheets responde con un 429 (límite de cuota) se realizan
    reintentos exponenciales. Si todos los intentos fallan pero se cuenta con
    datos almacenados en la sesión, se devuelven como último recurso para evitar
    detener la aplicación.
    """

    def _is_rate_limit_error(error: Exception) -> bool:
        status_code = getattr(getattr(error, "response", None), "status_code", None)
        if status_code == 429:
            return True
        text = str(error).lower()
        return "rate_limit" in text or "quota" in text or "429" in text or "resource_exhausted" in text

    last_success = st.session_state.get(cache_key)
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            data = worksheet.get_all_values()
            st.session_state[cache_key] = data
            return data
        except gspread.exceptions.APIError as e:
            last_error = e
            if not _is_rate_limit_error(e):
                raise

            wait_time = min(30, 2 ** attempt)
            st.warning(
                f"⚠️ Límite de lectura de Google Sheets alcanzado. "
                f"Reintentando en {wait_time} s (intento {attempt}/{max_attempts})."
            )
            time.sleep(wait_time)

    if last_success is not None:
        st.info(
            "ℹ️ Usando datos en caché debido al límite de cuota de Google Sheets."
        )
        return last_success

    if last_error is not None:
        raise last_error
    raise RuntimeError("No se pudieron obtener datos de Google Sheets")


def _open_worksheet_with_retry(
    client,
    sheet_id: str,
    sheet_name: str,
    max_attempts: int = 2,
    cooldown_seconds: int = 120,
):
    """Abre una worksheet con reintentos y *cooldown* para fallas transitorias."""

    cooldown_key = f"_gsheets_open_cooldown_until_{sheet_name}"
    notice_key = f"_gsheets_open_notice_at_{sheet_name}"
    now_ts = time.time()
    blocked_until = float(st.session_state.get(cooldown_key, 0))

    if blocked_until > now_ts:
        last_notice = float(st.session_state.get(notice_key, 0))
        if now_ts - last_notice >= 30:
            remaining = int(blocked_until - now_ts)
            st.warning(
                f"⚠️ Google Sheets sigue inestable para '{sheet_name}'. "
                f"Usando caché local; próximo intento en ~{remaining}s."
            )
            st.session_state[notice_key] = now_ts
        raise RuntimeError(f"Cooldown activo para la hoja '{sheet_name}'")

    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            spreadsheet = client.open_by_key(sheet_id)
            st.session_state[cooldown_key] = 0.0
            return spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.APIError as e:
            last_error = e
            wait_time = min(5, attempt)
            if attempt == 1:
                st.warning(
                    f"⚠️ Error temporal al abrir la hoja '{sheet_name}'. "
                    "Reintentando automáticamente..."
                )
            time.sleep(wait_time)

    st.session_state[cooldown_key] = time.time() + cooldown_seconds
    st.session_state[notice_key] = time.time()
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"No se pudo abrir la hoja '{sheet_name}' en Google Sheets")


def _warn_and_get_dataframe_fallback(cache_key: str, label: str) -> pd.DataFrame:
    fallback_df = st.session_state.get(cache_key)
    warning_key = f"_warn_once_{cache_key}"
    now_ts = time.time()
    last_warn = float(st.session_state.get(warning_key, 0))
    if now_ts - last_warn >= 30:
        st.warning(
            f"⚠️ No se pudo actualizar {label} desde Google Sheets en este momento; se muestran los últimos datos disponibles si existen."
        )
        st.session_state[warning_key] = now_ts
    if isinstance(fallback_df, pd.DataFrame):
        return fallback_df.copy()
    return pd.DataFrame()


@st.cache_data(ttl=60)
def load_data_from_gsheets():
    try:
        data = _fetch_with_retry(worksheet_main, "_cache_datos_pedidos")
    except gspread.exceptions.APIError:
        if refresh_main_sheet_handles():
            try:
                data = _fetch_with_retry(worksheet_main, "_cache_datos_pedidos")
            except Exception:
                return _warn_and_get_dataframe_fallback("_cache_datos_pedidos_df", "los pedidos")
        else:
            return _warn_and_get_dataframe_fallback("_cache_datos_pedidos_df", "los pedidos")
    except RuntimeError:
        if refresh_main_sheet_handles():
            try:
                data = _fetch_with_retry(worksheet_main, "_cache_datos_pedidos")
            except Exception:
                return _warn_and_get_dataframe_fallback("_cache_datos_pedidos_df", "los pedidos")
        else:
            return _warn_and_get_dataframe_fallback("_cache_datos_pedidos_df", "los pedidos")
    if not data:
        df = pd.DataFrame()
        st.session_state["_cache_datos_pedidos_df"] = df.copy()
        return df
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
        df["Turno"] = df["Turno"].apply(normalize_turno_label)
    else:
        df["Turno"] = ""

    st.session_state["_cache_datos_pedidos_df"] = df.copy()
    return df


@st.cache_data(ttl=60)
def load_casos_from_gsheets():
    """Lee 'casos_especiales' y normaliza headers/fechas."""
    try:
        data = _fetch_with_retry(worksheet_casos, "_cache_casos_especiales")
    except gspread.exceptions.APIError:
        if refresh_main_sheet_handles():
            try:
                data = _fetch_with_retry(worksheet_casos, "_cache_casos_especiales")
            except Exception:
                return _warn_and_get_dataframe_fallback("_cache_casos_especiales_df", "los casos especiales")
        else:
            return _warn_and_get_dataframe_fallback("_cache_casos_especiales_df", "los casos especiales")
    except RuntimeError:
        if refresh_main_sheet_handles():
            try:
                data = _fetch_with_retry(worksheet_casos, "_cache_casos_especiales")
            except Exception:
                return _warn_and_get_dataframe_fallback("_cache_casos_especiales_df", "los casos especiales")
        else:
            return _warn_and_get_dataframe_fallback("_cache_casos_especiales_df", "los casos especiales")
    if not data:
        df = pd.DataFrame()
        st.session_state["_cache_casos_especiales_df"] = df.copy()
        return df
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
        "Numero_Foraneo",
    ]:
        if base in df.columns:
            df[base] = df[base].astype(str).fillna("").str.strip()
    if "Turno" in df.columns:
        df["Turno"] = df["Turno"].apply(normalize_turno_label)
    else:
        df["Turno"] = ""
    st.session_state["_cache_casos_especiales_df"] = df.copy()
    return df


@st.cache_data(ttl=600)
def load_confirmados_from_gsheets(credentials_dict: dict, sheet_id: str, sheet_name: str):
    cache_df_key = f"_cache_{sheet_name}_df"
    try:
        client = get_gspread_client(_credentials_json_dict=credentials_dict)
        ws = _open_worksheet_with_retry(client, sheet_id, sheet_name)
        data = _fetch_with_retry(ws, f"_cache_{sheet_name}")
    except gspread.exceptions.APIError:
        return _warn_and_get_dataframe_fallback(cache_df_key, "los pedidos confirmados")
    except RuntimeError:
        return _warn_and_get_dataframe_fallback(cache_df_key, "los pedidos confirmados")

    if not data:
        df = pd.DataFrame()
        st.session_state[cache_df_key] = df.copy()
        return df

    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)

    for col in [
        "Cliente",
        "Vendedor_Registro",
        "Hora_Registro",
        "Estado_Pago",
        "Tipo_Envio",
        "Comprobante_Confirmado",
    ]:
        if col not in df.columns:
            df[col] = ""

    if "Monto_Comprobante" in df.columns:
        df["Monto_Comprobante"] = pd.to_numeric(
            df["Monto_Comprobante"], errors="coerce"
        ).fillna(0.0)
    else:
        df["Monto_Comprobante"] = 0.0

    # Fecha real (cuando se registró el pedido)
    if "Hora_Registro" in df.columns:
        df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")
        fecha_ventas = _resolve_sales_datetime(df)
        df["AñoMes"] = fecha_ventas.dt.to_period("M").astype(str)
        df["FechaDia"] = fecha_ventas.dt.date.astype(str)
    else:
        df["Hora_Registro"] = pd.NaT
        df["AñoMes"] = ""
        df["FechaDia"] = ""

    st.session_state[cache_df_key] = df.copy()
    return df


def get_cached_confirmados_df(sheet_name: str = SHEET_CONFIRMADOS) -> pd.DataFrame:
    cache_df_key = f"_cache_{sheet_name}_df"
    cached_df = st.session_state.get(cache_df_key)
    if isinstance(cached_df, pd.DataFrame):
        return cached_df.copy()
    return pd.DataFrame()


def refresh_confirmados_cache(
    credentials_dict: dict,
    sheet_id: str,
    sheet_name: str = SHEET_CONFIRMADOS,
) -> pd.DataFrame:
    load_confirmados_from_gsheets.clear()
    return load_confirmados_from_gsheets(credentials_dict, sheet_id, sheet_name)


def _clean_cliente_name(x: str) -> str:
    x = sanitize_text(str(x)).upper()
    x = unicodedata.normalize("NFKD", x)
    x = "".join(ch for ch in x if not unicodedata.combining(ch))
    x = x.replace(" ", " ")
    x = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in x)
    x = " ".join(x.split())
    return x


def _normalize_vendedor_name(value) -> str:
    return sanitize_text(value).casefold()


def _resolve_sales_datetime(df: pd.DataFrame) -> pd.Series:
    """Fecha base para métricas de ventas: prioriza Fecha_Pago_Comprobante."""

    def _parse_pago(series: pd.Series) -> pd.Series:
        raw = series.fillna("").astype(str).str.strip()
        # Casos como "2026-02-16 y 2026-02-17" o con ruido: tomar la primera fecha válida.
        first_date = raw.str.extract(r"(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)", expand=False)
        parsed_first = pd.to_datetime(first_date, errors="coerce")
        parsed_raw = pd.to_datetime(raw, errors="coerce")
        return parsed_first.fillna(parsed_raw)

    if "Fecha_Pago_Comprobante" in df.columns:
        fecha_pago = _parse_pago(df["Fecha_Pago_Comprobante"])
        if "Hora_Registro" in df.columns:
            hora_registro = pd.to_datetime(df["Hora_Registro"], errors="coerce")
            return fecha_pago.fillna(hora_registro)
        return fecha_pago

    if "Hora_Registro" in df.columns:
        return pd.to_datetime(df["Hora_Registro"], errors="coerce")

    return pd.Series(pd.NaT, index=df.index)


@st.cache_data(ttl=600)
def build_cliente_risk_table(df_conf: pd.DataFrame):
    """
    Replica el notebook:
    - calcula Dias_Entre_Compras por cliente (diff)
    - filtra intervalos > 7
    - Promedio_Ciclo = mean(Dias_Entre_Compras)
    - Dias_Desde_Ultima = hoy - ultima_compra
    - Ratio y Estado (Activo/Alerta/Riesgo)
    - Proxima_Estimada = ultima_compra + Promedio_Ciclo
    - añade Ticket_Promedio, Ventas_Total, Num_Pedidos, Ultimo_Vendedor
    """
    if df_conf.empty:
        return pd.DataFrame(), pd.Timestamp.now()

    df = df_conf.copy()

    df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")
    df = df[pd.notna(df["Hora_Registro"])].copy()

    if df.empty:
        return pd.DataFrame(), pd.Timestamp.now()

    if "Monto_Comprobante" not in df.columns:
        df["Monto_Comprobante"] = 0.0
    df["Monto_Comprobante"] = pd.to_numeric(df["Monto_Comprobante"], errors="coerce").fillna(0.0)

    if "Cliente" not in df.columns:
        df["Cliente"] = ""
    df["Cliente_Limpio"] = df["Cliente"].astype(str).map(_clean_cliente_name)

    hoy = df["Hora_Registro"].max()

    df = df.sort_values("Hora_Registro")
    df["Dias_Entre_Compras"] = df.groupby("Cliente_Limpio")["Hora_Registro"].diff().dt.days

    df_valid = df[(df["Dias_Entre_Compras"].notna()) & (df["Dias_Entre_Compras"] > 7)].copy()

    promedio_ciclo = df_valid.groupby("Cliente_Limpio")["Dias_Entre_Compras"].mean()
    ciclo_min = df_valid.groupby("Cliente_Limpio")["Dias_Entre_Compras"].min()
    ciclo_max = df_valid.groupby("Cliente_Limpio")["Dias_Entre_Compras"].max()
    ultima_compra = df.groupby("Cliente_Limpio")["Hora_Registro"].max()
    dias_desde_ultima = (hoy - ultima_compra).dt.days

    tabla = pd.DataFrame(
        {
            "Promedio_Ciclo": promedio_ciclo,
            "Ciclo_Min_Dias": ciclo_min,
            "Ciclo_Max_Dias": ciclo_max,
            "Ultima_Compra": ultima_compra,
            "Dias_Desde_Ultima": dias_desde_ultima,
        }
    )

    tabla["Promedio_Ciclo"] = pd.to_numeric(tabla["Promedio_Ciclo"], errors="coerce")
    tabla["Ciclo_Min_Dias"] = pd.to_numeric(tabla["Ciclo_Min_Dias"], errors="coerce")
    tabla["Ciclo_Max_Dias"] = pd.to_numeric(tabla["Ciclo_Max_Dias"], errors="coerce")
    tabla["Proxima_Estimada"] = tabla["Ultima_Compra"] + pd.to_timedelta(
        tabla["Promedio_Ciclo"], unit="D"
    )

    def clasificar(row):
        ciclo = row["Promedio_Ciclo"]
        if pd.isna(ciclo) or ciclo <= 0:
            return "Nuevo/SinHistorial"
        r = row["Dias_Desde_Ultima"] / ciclo
        if r <= 1:
            return "Activo"
        elif r <= 1.5:
            return "Alerta"
        return "Riesgo"

    tabla["Estado"] = tabla.apply(clasificar, axis=1)
    tabla["Ratio"] = (tabla["Dias_Desde_Ultima"] / tabla["Promedio_Ciclo"]).where(
        tabla["Promedio_Ciclo"] > 0
    )

    if "Vendedor_Registro" not in df.columns:
        df["Vendedor_Registro"] = ""
    ultimo_vendedor = df.groupby("Cliente_Limpio")["Vendedor_Registro"].last()
    tabla["Vendedor"] = ultimo_vendedor

    # Evita que montos vacíos/corruptos convertidos a 0 distorsionen el ticket promedio.
    # Para ticket solo consideramos comprobantes con monto positivo.
    ticket_prom = (
        df[df["Monto_Comprobante"] > 0]
        .groupby("Cliente_Limpio")["Monto_Comprobante"]
        .mean()
    )
    ventas_total = df.groupby("Cliente_Limpio")["Monto_Comprobante"].sum()
    num_pedidos = df.groupby("Cliente_Limpio")["Monto_Comprobante"].size()

    tabla["Ticket_Promedio"] = ticket_prom
    tabla["Ticket_Promedio"] = pd.to_numeric(tabla["Ticket_Promedio"], errors="coerce").fillna(0.0)
    tabla["Ventas_Total"] = ventas_total
    tabla["Num_Pedidos"] = num_pedidos

    tabla = tabla.reset_index().rename(columns={"Cliente_Limpio": "Cliente"})
    return tabla, hoy


@st.cache_data(ttl=600)
def build_resumen_vendedor(tabla_clientes: pd.DataFrame):
    if tabla_clientes.empty:
        return pd.DataFrame()

    pivot = tabla_clientes.groupby(["Vendedor", "Estado"]).size().unstack(fill_value=0)

    for col in ["Activo", "Alerta", "Riesgo", "Nuevo/SinHistorial"]:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["Total"] = (
        pivot["Activo"]
        + pivot["Alerta"]
        + pivot["Riesgo"]
        + pivot["Nuevo/SinHistorial"]
    )
    pivot["Total_Evaluado"] = pivot["Activo"] + pivot["Alerta"] + pivot["Riesgo"]
    pivot["%Riesgo"] = (
        (pivot["Riesgo"] / pivot["Total_Evaluado"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    ventas_vend = tabla_clientes.groupby("Vendedor")["Ventas_Total"].sum()
    pedidos_vend = tabla_clientes.groupby("Vendedor")["Num_Pedidos"].sum()

    pivot["Ventas"] = ventas_vend
    pivot["Pedidos"] = pedidos_vend
    pivot["Ticket_Prom"] = (
        (pivot["Ventas"] / pivot["Pedidos"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    pivot = pivot.reset_index().sort_values("%Riesgo", ascending=False)
    return pivot


@st.cache_data(ttl=600)
def compute_proyeccion_30(tabla_clientes: pd.DataFrame, hoy: pd.Timestamp):
    if tabla_clientes.empty:
        return 0.0, 0, pd.DataFrame()

    prox = tabla_clientes[
        (pd.to_datetime(tabla_clientes["Proxima_Estimada"], errors="coerce") <= hoy + timedelta(days=30))
        & (~tabla_clientes["Estado"].isin(["Riesgo", "Nuevo/SinHistorial"]))
    ].copy()

    prox["Ticket_Promedio"] = pd.to_numeric(prox["Ticket_Promedio"], errors="coerce").fillna(0.0)
    prox = prox[prox["Ticket_Promedio"] > 0].copy()

    total = float(prox["Ticket_Promedio"].sum())
    n = int(len(prox))
    return total, n, prox


@st.cache_data(ttl=600)
def compute_dashboard_base(df_conf: pd.DataFrame):
    if df_conf.empty:
        return {
            "df": df_conf,
            "ventas_mes": pd.Series(dtype=float),
            "ventas_vendedor": pd.Series(dtype=float),
            "pedidos_vendedor": pd.Series(dtype=int),
        }

    ventas_mes = df_conf.groupby("AñoMes")["Monto_Comprobante"].sum().sort_index()
    ventas_vendedor = (
        df_conf.groupby("Vendedor_Registro")["Monto_Comprobante"]
        .sum()
        .sort_values(ascending=False)
    )
    pedidos_vendedor = df_conf["Vendedor_Registro"].value_counts()

    return {
        "df": df_conf,
        "ventas_mes": ventas_mes,
        "ventas_vendedor": ventas_vendedor,
        "pedidos_vendedor": pedidos_vendedor,
    }


@st.cache_data(ttl=120)
def build_ultimos_pedidos_data(df_pedidos: pd.DataFrame, vendedor: str) -> pd.DataFrame:
    work = df_pedidos.copy() if not df_pedidos.empty else pd.DataFrame()
    if not work.empty:
        work["_origen_pedido"] = "pedidos"
    casos = load_casos_from_gsheets()

    if not casos.empty:
        if "Completados_Limpiado" not in casos.columns:
            casos["Completados_Limpiado"] = ""
        casos = casos[
            casos["Completados_Limpiado"].map(sanitize_text) == ""
        ].copy()

        if not casos.empty:
            casos["_origen_pedido"] = "casos_especiales"
            for base_col in [
                "Hora_Registro",
                "Cliente",
                "Vendedor_Registro",
                "Folio_Factura",
                "Tipo_Envio",
                "Fecha_Entrega",
                "Estado",
            ]:
                if base_col not in casos.columns:
                    casos[base_col] = ""
            work = pd.concat([work, casos], ignore_index=True, sort=False)

    if work.empty:
        return pd.DataFrame()

    if "Hora_Registro" not in work.columns:
        work["Hora_Registro"] = pd.NaT
    if "Vendedor_Registro" not in work.columns:
        work["Vendedor_Registro"] = ""

    if vendedor != "(Todos)":
        vend_norm = _normalize_vendedor_name(vendedor)
        work = work[
            work["Vendedor_Registro"].map(_normalize_vendedor_name) == vend_norm
        ]

    return work.sort_values("Hora_Registro", ascending=False).copy()


@st.cache_data(ttl=120)
def build_ultimos_pedidos(df_pedidos: pd.DataFrame, vendedor: str):
    work = build_ultimos_pedidos_data(df_pedidos, vendedor)

    if work.empty:
        return pd.DataFrame()

    columnas = [
        "Hora_Registro",
        "Cliente",
        "Vendedor_Registro",
        "Folio_Factura",
        "Tipo_Envio",
        "Fecha_Entrega",
        "Estado",
    ]
    cols_exist = [c for c in columnas if c in work.columns]
    if not cols_exist:
        return pd.DataFrame()

    vista = work[cols_exist].copy()
    if "Hora_Registro" in vista.columns:
        vista["Hora_Registro"] = pd.to_datetime(
            vista["Hora_Registro"], errors="coerce"
        ).dt.strftime("%d/%m/%Y %H:%M")
    if "Fecha_Entrega" in vista.columns:
        vista["Fecha_Entrega"] = pd.to_datetime(
            vista["Fecha_Entrega"], errors="coerce"
        ).dt.strftime("%d/%m/%Y")
    return vista


@st.cache_data(ttl=120)
def build_temporal_sales_dataset(df_pedidos: pd.DataFrame, vendedor: str) -> pd.DataFrame:
    if df_pedidos.empty:
        return pd.DataFrame(columns=["Fecha", "Monto", "Pedidos", "Vendedor"])

    work = df_pedidos.copy()
    work = filter_df_by_vendedor(work, vendedor)

    work["Fecha"] = _resolve_sales_datetime(work)
    work["Monto"] = get_numeric_column(work, "Monto_Comprobante", default=0.0)
    work = work.dropna(subset=["Fecha"]).copy()
    if work.empty:
        return pd.DataFrame(columns=["Fecha", "Monto", "Pedidos", "Vendedor"])

    work["Fecha"] = work["Fecha"].dt.normalize()
    work["Pedidos"] = 1
    work["Vendedor"] = work.get("Vendedor_Registro", "").map(sanitize_text)
    work.loc[work["Vendedor"] == "", "Vendedor"] = "Sin vendedor"
    return work[["Fecha", "Monto", "Pedidos", "Vendedor"]]


def aggregate_temporal_view(
    base_df: pd.DataFrame,
    granularidad: str,
    fecha_inicio: pd.Timestamp,
    fecha_fin: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if base_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    freq_map = {"Día": "D", "Semana": "W-MON", "Mes": "MS"}
    freq = freq_map.get(granularidad, "D")
    actual_range = base_df[(base_df["Fecha"] >= fecha_inicio) & (base_df["Fecha"] <= fecha_fin)].copy()
    if actual_range.empty:
        return pd.DataFrame(), pd.DataFrame()

    actual = (
        actual_range.groupby(pd.Grouper(key="Fecha", freq=freq))[["Monto", "Pedidos"]]
        .sum()
        .reset_index()
        .sort_values("Fecha")
    )

    period_days = max(1, int((fecha_fin - fecha_inicio).days) + 1)
    prev_fin = fecha_inicio - pd.Timedelta(days=1)
    prev_inicio = prev_fin - pd.Timedelta(days=period_days - 1)
    prev_range = base_df[(base_df["Fecha"] >= prev_inicio) & (base_df["Fecha"] <= prev_fin)].copy()
    if prev_range.empty:
        return actual, pd.DataFrame()

    prev = (
        prev_range.groupby(pd.Grouper(key="Fecha", freq=freq))[["Monto", "Pedidos"]]
        .sum()
        .reset_index()
        .sort_values("Fecha")
    )
    min_len = min(len(actual), len(prev))
    if min_len:
        prev = prev.tail(min_len).copy()
        prev["Fecha"] = actual.tail(min_len)["Fecha"].values
    return actual, prev


def _format_detail_value(value) -> str:
    formatted = sanitize_text(value)
    return formatted if formatted else "—"


def _render_detail_row(label: str, value):
    st.markdown(f"**{label}:** {_format_detail_value(value)}")


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

    def _encode_url(url: str) -> str:
        parsed = urlsplit(url)
        encoded_path = quote(parsed.path, safe="/%._-~")
        encoded_query = quote(parsed.query, safe="=&%._-~")
        encoded_fragment = quote(parsed.fragment, safe="%._-~")
        return urlunsplit((parsed.scheme, parsed.netloc, encoded_path, encoded_query, encoded_fragment))

    parts = [p.strip() for p in str(adjuntos_str).split(",") if p.strip()]
    links = []
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            safe_url = _encode_url(p)
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({safe_url})")
        else:
            url = get_s3_file_url(p)
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({_encode_url(url)})" if url else f"❌ {p}")
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

    if "Estado_Entrega" in df_to_display.columns:
        estado_entrega_series = df_to_display["Estado_Entrega"].astype(str).str.strip()
        mask_no_entregado = estado_entrega_series == "⏳ No Entregado"
        if mask_no_entregado.any():
            df_vista["Estado Entrega"] = estado_entrega_series.where(
                mask_no_entregado, ""
            )

    columnas_base = ["Fecha Entrega", "Tipo Envío", "Cliente", "Vendedor", "Estado"]
    if "Estado Entrega" in df_vista.columns:
        if "Estado" in columnas_base:
            idx_estado = columnas_base.index("Estado")
        else:
            idx_estado = len(columnas_base)
        columnas_base.insert(idx_estado, "Estado Entrega")

    mostrar_cols = [c for c in columnas_base if c in df_vista.columns]
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
        "🟣 Cancelado": len(cancelados_visibles),
        "🟢 Completado": len(completados_visibles),
    }
    total = sum(counts.values())
    estados_fijos = ["🟡 Pendiente", "🔵 En Proceso", "🟢 Completado"]
    estados_cond = ["🔴 Demorado", "🟣 Cancelado"]
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
    turno = normalize_turno_label(row.get(local_flag_col, ""))
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
    work["Fecha_Entrega_Str"] = work["Fecha_Entrega_Str"].fillna("Sin fecha")
    mask_fecha_vacia = work["Fecha_Entrega_Str"].astype(str).str.strip() == ""
    work.loc[mask_fecha_vacia, "Fecha_Entrega_Str"] = "Sin fecha"
    if mode == "foraneo" or (mode == "local" and not group_turno):
        work["Grupo_Clave"] = work["Fecha_Entrega_Str"]
    else:
        work["Grupo_Clave"] = work.apply(
            lambda r: f"{group_key_local(r)} – {r['Fecha_Entrega_Str']}", axis=1
        )
    grupos = []
    grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"], dropna=False)
    for (clave, f), df_g in sorted(
        grouped,
        key=lambda x: (
            pd.isna(x[0][1]),
            x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
        ),
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

# Tabs principales
tab_labels = [
    "📈 Dashboard",
    "🧠 Asistente TD",
    "⚙️ Auto Local",
    "🚚 Auto Foráneo",
    "🧑‍🔧 Surtidores",
]

# ---------------------------
# Persistencia de tab activa (para autorefresh)
# ---------------------------
init_login_state()

if not get_logged_user():
    usuario_qp = get_query_param_value("usuario").upper()
    if usuario_qp in VENDEDOR_CREDENTIALS:
        st.session_state.auth_user = usuario_qp
        st.session_state.auth_vendor = VENDEDOR_CREDENTIALS[usuario_qp]

tab_qp = get_query_param_value("tab")
if "active_main_tab" not in st.session_state:
    st.session_state.active_main_tab = 0
elif st.session_state.active_main_tab >= len(tab_labels):
    st.session_state.active_main_tab = 0
radio_tab_state = st.session_state.get("_radio_main_tab")
if isinstance(radio_tab_state, int) and 0 <= radio_tab_state < len(tab_labels):
    st.session_state.active_main_tab = radio_tab_state
elif tab_qp.isdigit():
    tab_index = int(tab_qp)
    if 0 <= tab_index < len(tab_labels):
        st.session_state.active_main_tab = tab_index

selected_tab = st.radio(
    "Vista",
    options=list(range(len(tab_labels))),
    format_func=lambda i: tab_labels[i],
    index=st.session_state.active_main_tab,
    horizontal=True,
    label_visibility="collapsed",
    key="_radio_main_tab",
)
st.session_state.active_main_tab = selected_tab

# helper para "simular" tabs
tabs = [None] * len(tab_labels)

logged_vendor = get_logged_vendor()
logged_user = get_logged_user()

st.query_params["tab"] = str(selected_tab)
if logged_user:
    st.query_params["usuario"] = logged_user
else:
    clear_query_param("usuario")

with st.sidebar:
    st.markdown("### 👤 Acceso")
    if logged_vendor:
        session_label = f"Sesión activa: **{logged_vendor}**"
        if logged_user:
            session_label += f" ({logged_user})"
        st.success(session_label)
        if st.button("🚪 Cerrar sesión", key="logout_vendor_sidebar"):
            st.session_state.auth_user = ""
            st.session_state.auth_vendor = ""
            st.session_state.dashboard_vendedor_sel = "(Todos)"
            st.session_state.td_assistant_messages = []
            clear_query_param("usuario")
            st.rerun()
    else:
        st.caption("Iniciar sesión es opcional. Si no inicias sesión, la app funciona normal sin vendedor preseleccionado.")
        user_input = st.text_input(
            "Usuario",
            key="vendor_login_sidebar_input",
            placeholder="Ingresa tu usuario",
        ).strip().upper()
        if st.button("🔐 Iniciar sesión", key="vendor_login_sidebar_btn"):
            vendor_name = VENDEDOR_CREDENTIALS.get(user_input, "")
            if vendor_name:
                st.session_state.auth_user = user_input
                st.session_state.auth_vendor = vendor_name
                st.session_state.dashboard_vendedor_sel = vendor_name
                st.query_params["usuario"] = user_input
                st.rerun()
            st.error("Usuario no válido. Verifica la clave e intenta de nuevo.")

if not logged_vendor:
    st.warning(
        "⚠️ Aún no has iniciado sesión. Para guardar tu usuario en el enlace y evitar volver a loguearte, inicia sesión desde la barra lateral."
    )


# Entradas compartidas para numeración única entre Auto Local y Auto Foráneo
auto_local_entries = []
auto_foraneo_entries = []
if selected_tab in (2, 3, 4):
    df_local_auto = get_local_orders(df_all)
    casos_local_auto, _ = get_case_envio_assignments(df_all)
    df_local_auto = drop_local_duplicates_for_cases(df_local_auto, casos_local_auto)
    if not df_local_auto.empty:
        auto_local_entries.extend(build_entries_local(df_local_auto))
    if not casos_local_auto.empty:
        auto_local_entries.extend(build_entries_casos(casos_local_auto))

    df_for_auto = get_foraneo_orders(df_all)
    if not df_for_auto.empty:
        auto_foraneo_entries.extend(build_entries_foraneo(df_for_auto))

    auto_local_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    auto_foraneo_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))

    assign_flow_numbers(auto_local_entries, auto_foraneo_entries, df_all)

    if "surtidor_assignments" not in st.session_state:
        st.session_state.surtidor_assignments = {}
    apply_surtidor_assignments(auto_local_entries, st.session_state.surtidor_assignments)
    apply_surtidor_assignments(auto_foraneo_entries, st.session_state.surtidor_assignments)
    assign_display_numbers(auto_local_entries, auto_foraneo_entries, datetime.now(TZ).date())

# ---------------------------
# TAB 1: Asistente interno TD
# ---------------------------
if selected_tab == 1:
    init_td_assistant_state()

    st.markdown(
        """
        <style>
        .td-assistant-shell {
            background: linear-gradient(180deg, #0f2027 0%, #12232d 45%, #0e1d23 100%);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 14px;
            padding: 0.8rem 0.9rem 0.2rem;
            margin-bottom: 0.75rem;
        }
        .td-assistant-shell h3 {
            margin-bottom: 0.1rem;
        }
        .td-assistant-shell p {
            margin-top: 0;
            color: #b9d8cf;
            font-size: 0.9rem;
        }
        div[data-testid="stChatMessage"] {
            border-radius: 16px;
            padding: 0.4rem 0.75rem;
            margin-bottom: 0.45rem;
            border: 1px solid transparent;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.25);
        }
        div[data-testid="stChatMessage"]:has(div[data-testid="chatAvatarIcon-user"]) {
            background: linear-gradient(130deg, #0f766e 0%, #14b8a6 100%);
            border-color: rgba(15, 118, 110, 0.55);
        }
        div[data-testid="stChatMessage"]:has(div[data-testid="chatAvatarIcon-assistant"]) {
            background: linear-gradient(145deg, #1e293b 0%, #111827 100%);
            border-color: rgba(148, 163, 184, 0.28);
        }
        div[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p {
            margin-bottom: 0;
            line-height: 1.4;
            font-size: 0.95rem;
        }
        div[data-testid="stChatInput"] {
            background: rgba(15, 23, 42, 0.68);
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 999px;
            padding: 0.25rem 0.65rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="td-assistant-shell">
            <h3>🧠 Asistente TD</h3>
            <p>Tu asistente inteligente para resolver dudas de pedidos, estatus, incidencias y claves de productos (hoja Productos).</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if get_logged_vendor():
        st.caption(f"Atendiendo como vendedor: {get_logged_vendor()}.")

    # Fuentes para el asistente interno
    df_casos_assistant = load_casos_from_gsheets()
    df_hist = load_historicos_from_gsheets()
    df_productos_assistant = load_productos_from_gsheets()
    remote_postal_codes = load_remote_postal_codes()

    if st.button("🧹 Limpiar conversación", use_container_width=False):
        st.session_state.td_assistant_messages = []
        st.rerun()

    api_key = get_openai_api_key()
    uploaded_image = None
    if not api_key:
        st.warning("Falta configurar OPENAI_API_KEY en st.secrets para usar el asistente.")
    else:
        st.checkbox(
            "📎 Habilitar espacio para adjuntar imagen",
            key="td_assistant_enable_image",
            help="Actívalo solo cuando necesites enviar una imagen para que no estorbe en el chat.",
        )

        if st.session_state.td_assistant_enable_image:
            uploaded_image = st.file_uploader(
                "Adjunta imagen para analizar en tu consulta (opcional)",
                type=["png", "jpg", "jpeg", "webp"],
                key="td_assistant_image_upload",
                help="Puedes subir una captura, comprobante o foto para que el asistente la considere en su respuesta.",
            )
            if uploaded_image is not None:
                st.caption(f"Vista previa mínima: {uploaded_image.name}")
                st.image(uploaded_image, width=120)
        else:
            st.session_state.pop("td_assistant_image_upload", None)

    for message in st.session_state.td_assistant_messages:
        role = message.get("role", "assistant")
        content = sanitize_text(message.get("content", ""))
        if role not in {"user", "assistant"} or not content:
            continue
        with st.chat_message(role):
            st.markdown(content)

    if api_key:
        user_prompt = st.chat_input("Escribe tu duda operativa...")
        if user_prompt:
            user_prompt = sanitize_text(user_prompt)
            if user_prompt:
                image_bytes = uploaded_image.getvalue() if uploaded_image is not None else None
                image_name = uploaded_image.name if uploaded_image is not None else ""
                image_type = uploaded_image.type if uploaded_image is not None else ""
                st.session_state.td_assistant_messages.append(
                    {
                        "role": "user",
                        "content": (
                            user_prompt
                            + (f"\n\n📎 Imagen adjunta: {image_name}" if image_bytes else "")
                        ),
                    }
                )
                with st.chat_message("user"):
                    st.markdown(user_prompt)
                    if uploaded_image is not None:
                        st.caption(f"📎 Imagen enviada: {image_name}")
                        st.image(uploaded_image, width=120)

                with st.chat_message("assistant"):
                    with st.spinner("Pensando..."):
                        try:
                            assistant_reply = fetch_td_assistant_reply(
                                user_prompt,
                                df_all,
                                df_hist,
                                df_casos_assistant,
                                df_productos_assistant,
                                remote_postal_codes,
                                image_bytes=image_bytes,
                                image_mime_type=image_type,
                            )
                        except ValueError:
                            assistant_reply = (
                                "Falta configurar OPENAI_API_KEY en st.secrets para usar el asistente."
                            )
                        except Exception:
                            assistant_reply = "No pude responder en este momento. Intenta de nuevo."
                    st.markdown(assistant_reply)

                st.session_state.td_assistant_messages.append(
                    {"role": "assistant", "content": assistant_reply}
                )
                st.rerun()

# ---------------------------
# TAB 1: Auto Local (Casos asignados) — 2 columnas
# ---------------------------
if selected_tab == 2:
    st_autorefresh(interval=60000, key="auto_refresh_local_casos")

    combined_entries = [
        e for e in auto_local_entries if _is_visible_auto_entry(e)
    ]

    turno_priority = [
        "☀️ Local Mañana",
        "🌙 Local Tarde",
        "🌵 Saltillo",
        "📦 Pasa a Bodega",
        "📍 Local (sin turno)",
    ]
    grouped: dict[str, list] = {label: [] for label in turno_priority}
    for entry in combined_entries:
        turno = normalize_turno_label(entry.get("turno", ""))
        if not turno:
            turno = "📍 Local (sin turno)"
        if turno not in grouped:
            grouped[turno] = []
        grouped[turno].append(entry)

    ordered_labels = [
        label for label in turno_priority if label in grouped and grouped[label]
    ]
    extra_labels = sorted(
        [label for label in grouped.keys() if label not in turno_priority and grouped[label]]
    )
    ordered_labels.extend(extra_labels)

    if not ordered_labels:
        st.info("No hay pedidos locales activos por turno.")
    else:
        col_left, col_right = st.columns(2, gap="large")
        columns = [col_left, col_right]
        next_number = 1

        for idx, label in enumerate(ordered_labels):
            target_col = columns[idx % 2]
            entries = sort_entries_by_flow_number_desc(grouped[label])
            with target_col:
                next_number = render_auto_list(
                    entries,
                    title=f"📍 LOCALES • {label}",
                    subtitle="Pedidos activos por turno",
                    max_rows=140,
                    start_number=next_number,
                    panel_height=220,
                    scroll_max_height=300,
                )

# ---------------------------
# TAB 2: Auto Foráneo (Casos asignados) — 2 columnas
# ---------------------------
if selected_tab == 3:
    st_autorefresh(interval=60000, key="auto_refresh_foraneo_cdmx")

    hoy = datetime.now(TZ).date()


    # 1) Entradas (foráneo + casos asignados a foráneo)
    combined_entries = list(auto_foraneo_entries)

    visible_entries = [e for e in combined_entries if _is_visible_auto_entry(e)]

    # Devoluciones/casos foráneos con Numero_Foraneo manual deben aparecer
    # junto a los de HOY/FUTUROS, ordenados por número de flujo.
    asignados = [
        e for e in visible_entries if _parse_foraneo_number(e.get("numero_foraneo", "")) is not None
    ]
    asignados = sort_entries_by_flow_number_desc(asignados)

    restantes = [
        e for e in visible_entries if _parse_foraneo_number(e.get("numero_foraneo", "")) is None
    ]

    ant = filter_entries_before_date(restantes, hoy)
    ant = sort_entries_by_flow_number_desc(ant)

    sin_fecha = filter_entries_no_entrega_date(restantes)
    sin_fecha = sort_entries_by_flow_number_desc(sin_fecha)

    # En HOY incluimos también devoluciones/casos con número manual,
    # sin depender de fecha de registro para conservar su secuencia.
    hoy_entries = filter_entries_on_or_after(restantes, hoy) + asignados
    hoy_entries = sort_entries_by_flow_number_desc(hoy_entries)

    anteriores = sort_entries_by_flow_number_desc(ant + sin_fecha)

    # Distribución inteligente: usar el espacio libre de "Anteriores"
    # para continuar la lista de "Hoy" y evitar columnas desbalanceadas.
    ant_count = len(anteriores)
    hoy_count = len(hoy_entries)
    objetivo_derecha = int(np.ceil((ant_count + hoy_count) / 2.0))
    hoy_primarios = hoy_entries[:objetivo_derecha]
    hoy_continuacion = hoy_entries[objetivo_derecha:]

    # 2) Layout: izquierda/derecha
    col_left, col_right = st.columns(2, gap="large")

    # --- IZQUIERDA: ANTERIORES + CONTINUACIÓN DE HOY ---
    with col_left:
        next_number = render_auto_list(
            anteriores,
            title="🚚 FORÁNEOS • ANTERIORES",
            subtitle=f"Fechas previas + pedidos sin Fecha_Entrega",
            max_rows=140,
            panel_height=220,
        )

        if hoy_continuacion:
            render_auto_list(
                hoy_continuacion,
                title=f"🚚 FORÁNEOS • HOY ({hoy.strftime('%d/%m')})",
                subtitle="Todos los de hoy y fechas futuras",
                max_rows=140,
                start_number=next_number,
                panel_height=160,
            )

    # --- DERECHA: HOY + FUTUROS + SIN Fecha_Entrega ---
    with col_right:
        render_auto_list(
            hoy_primarios,
            title=f"🚚 FORÁNEOS • HOY ({hoy.strftime('%d/%m')})",
            subtitle="Todos los de hoy y fechas futuras",
            max_rows=140,
            start_number=next_number,
        )

# ---------------------------
# TAB 3: Surtidores (Asignación)
# ---------------------------
if selected_tab == 4:

    st.markdown("### 🧑‍🔧 Asignación de surtidores")
    st.caption("Selecciona pedidos visibles y escribe tu nombre o inicial para asignarlos.")

    surtidor_nombre = st.text_input("Nombre o inicial del surtidor")

    seen_local = set()
    local_hoy = []
    for entry in auto_local_entries:
        if not _is_visible_auto_entry(entry):
            continue
        if entry.get("display_num") is None:
            continue
        key = build_surtidor_key(entry)
        if not key or key in seen_local:
            continue
        seen_local.add(key)
        local_hoy.append(entry)
    foraneo_hoy = []
    seen_foraneo = set()
    for entry in auto_foraneo_entries:
        if not _is_visible_auto_entry(entry):
            continue
        if entry.get("display_num") is None:
            continue
        key = build_surtidor_key(entry)
        if not key or key in seen_foraneo:
            continue
        seen_foraneo.add(key)
        foraneo_hoy.append(entry)

    def _entry_label(entry) -> str:
        numero = entry.get("display_num", entry.get("numero", "—"))
        cliente = sanitize_text(entry.get("cliente_nombre", ""))
        estado = sanitize_text(entry.get("estado", ""))
        parts = [f"#{numero}", cliente, estado]
        return " · ".join([p for p in parts if p])

    local_options = {build_surtidor_key(e): _entry_label(e) for e in local_hoy}
    foraneo_options = {build_surtidor_key(e): _entry_label(e) for e in foraneo_hoy}
    local_order = {build_surtidor_key(e): e.get("display_num", float("inf")) for e in local_hoy}
    foraneo_order = {
        build_surtidor_key(e): e.get("display_num", float("inf")) for e in foraneo_hoy
    }
    local_sorted_keys = sorted(local_options.keys(), key=lambda k: local_order.get(k, float("inf")))
    foraneo_sorted_keys = sorted(
        foraneo_options.keys(), key=lambda k: foraneo_order.get(k, float("inf"))
    )

    col_local, col_foraneo = st.columns(2, gap="large")
    with col_local:
        st.markdown("#### 📍 Auto Local")
        selected_local = st.multiselect(
            "Pedidos locales",
            options=local_sorted_keys,
            format_func=lambda k: local_options.get(k, k),
        )
    with col_foraneo:
        st.markdown("#### 🚚 Auto Foráneo")
        selected_foraneo = st.multiselect(
            "Pedidos foráneos",
            options=foraneo_sorted_keys,
            format_func=lambda k: foraneo_options.get(k, k),
        )

    if st.button("✅ Asignar surtidor", use_container_width=True):
        nombre = sanitize_text(surtidor_nombre)
        if not nombre:
            st.warning("Escribe un nombre o inicial para asignar.")
        else:
            selected_keys = selected_local + selected_foraneo
            if not selected_keys:
                st.warning("Selecciona al menos un pedido.")
            else:
                for key in selected_keys:
                    st.session_state.surtidor_assignments[key] = nombre
                st.success("Asignación guardada.")
                st.rerun()

    st.markdown("---")
    st.markdown("#### 📋 Asignaciones actuales")
    assignments = st.session_state.get("surtidor_assignments", {})
    if not assignments:
        st.info("Sin asignaciones registradas.")
    else:
        entry_lookup = {}
        envio_lookup = {}
        for entry in auto_local_entries:
            key = build_surtidor_key(entry)
            if key:
                entry_lookup[key] = entry
                envio_lookup[key] = "📍"
        for entry in auto_foraneo_entries:
            key = build_surtidor_key(entry)
            if key:
                entry_lookup[key] = entry
                envio_lookup[key] = "🚚"

        def _assignment_label(key: str) -> str:
            entry = entry_lookup.get(key)
            if not entry:
                return key
            numero = entry.get("display_num", entry.get("numero", "—"))
            cliente = sanitize_text(entry.get("cliente_nombre", ""))
            estado = sanitize_text(entry.get("estado", ""))
            envio = envio_lookup.get(key, "")
            numero_label = f"{envio} #{numero}" if envio else f"#{numero}"
            parts = [numero_label, cliente, estado]
            return " · ".join([p for p in parts if p])

        rows = [
            {"Pedido": _assignment_label(key), "Surtidor": value}
            for key, value in assignments.items()
            if value
        ]
        if rows:
            df_assign = pd.DataFrame(rows)
            st.dataframe(df_assign, use_container_width=True, height=300)
        else:
            st.info("Sin asignaciones registradas.")


if selected_tab == 0:
    st_autorefresh(interval=60000, key="auto_refresh_dashboard")

    st.caption("📦 pedidos_confirmados se consulta solo bajo demanda; en autorefresh se usa caché local.")

    df_conf = get_cached_confirmados_df(SHEET_CONFIRMADOS)
    hoy = pd.Timestamp.now()
    confirmados_cache_missing = df_conf.empty
    if confirmados_cache_missing:
        tabla_clientes = pd.DataFrame(
            columns=[
                "Cliente",
                "Vendedor",
                "Estado",
                "Ticket_Promedio",
                "Ventas_Total",
                "Num_Pedidos",
                "Proxima_Estimada",
            ]
        )
    else:
        tabla_clientes, hoy = build_cliente_risk_table(df_conf)
        if tabla_clientes.empty:
            st.warning(
                "No se pudo construir tabla de clientes con los confirmados actuales; "
                "se mantiene visible el dashboard de flujo y puedes reintentar la actualización manual."
            )
            tabla_clientes = pd.DataFrame(
                columns=[
                    "Cliente",
                    "Vendedor",
                    "Estado",
                    "Ticket_Promedio",
                    "Ventas_Total",
                    "Num_Pedidos",
                    "Proxima_Estimada",
                ]
            )

    if not tabla_clientes.empty and "Vendedor" in tabla_clientes.columns:
        vendedores_raw = tabla_clientes["Vendedor"].dropna().astype(str).unique().tolist()
    else:
        vendedores_raw = []
        for col in ["Vendedor_Registro", "Vendedor"]:
            if col in df_all.columns:
                vendedores_raw.extend(df_all[col].dropna().astype(str).unique().tolist())
        if not vendedores_raw:
            logged_vendor = get_logged_vendor()
            if logged_vendor:
                vendedores_raw = [logged_vendor]

    _vendedores = sorted({sanitize_text(v) for v in vendedores_raw if sanitize_text(v)})

    total_ventas = float(get_numeric_column(df_conf, "Monto_Comprobante", default=0.0).sum())
    total_pedidos = int(len(df_conf))
    ticket_prom = float(total_ventas / total_pedidos) if total_pedidos else 0.0

    evaluados = int(tabla_clientes["Estado"].isin(["Activo", "Alerta", "Riesgo"]).sum())
    nuevos = int((tabla_clientes["Estado"] == "Nuevo/SinHistorial").sum())
    total_clientes_actuales = evaluados + nuevos
    activos = int((tabla_clientes["Estado"] == "Activo").sum())
    riesgo = int((tabla_clientes["Estado"] == "Riesgo").sum())
    pct_activo = (activos / evaluados) if evaluados else 0.0
    pct_riesgo = (riesgo / evaluados) if evaluados else 0.0

    colf1, colf2 = st.columns([0.6, 0.4])
    with colf1:
        vendedor_options = ["(Todos)"] + _vendedores
        vendedor_state_key = "dashboard_vendedor_sel"
        logged_vendor = get_logged_vendor()
        default_vendor = "(Todos)"
        if logged_vendor:
            logged_vendor_norm = _normalize_vendedor_name(logged_vendor)
            for opt in vendedor_options:
                if _normalize_vendedor_name(opt) == logged_vendor_norm:
                    default_vendor = opt
                    break
        if vendedor_state_key not in st.session_state:
            st.session_state[vendedor_state_key] = default_vendor
        if st.session_state[vendedor_state_key] not in vendedor_options:
            st.session_state[vendedor_state_key] = default_vendor
        vendedor_sel = st.selectbox(
            "Filtrar por vendedor (opcional)",
            options=vendedor_options,
            key=vendedor_state_key,
        )
    with colf2:
        estado_sel = st.multiselect(
            "Estado cliente",
            options=["Activo", "Alerta", "Riesgo", "Nuevo/SinHistorial"],
            default=["Activo", "Alerta", "Riesgo", "Nuevo/SinHistorial"],
        )

    if vendedor_sel == "(Todos)":
        st.markdown("## 📈 Dashboard Inteligente (Riesgo + Proyección)")
        st.caption("Basado en Hora_Registro y patrón real por cliente.")

        row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
        row1_col1.metric("💰 Ventas históricas", f"${total_ventas:,.0f}")
        row1_col2.metric("📦 Pedidos históricos", f"{total_pedidos:,}")
        row1_col3.metric("% cartera activa", f"{pct_activo * 100:.1f}%")
        row1_col4.metric("% cartera en riesgo", f"{pct_riesgo * 100:.1f}%")

        row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
        row2_col1.metric("👥 Clientes con historial", f"{evaluados:,}")
        row2_col2.metric("🆕 Nuevos/Sin historial", f"{nuevos:,}")
        row2_col3.metric("👥 Clientes totales actuales", f"{total_clientes_actuales:,}")
        row2_col4.metric("🎟️ Ticket prom (global)", f"${ticket_prom:,.0f}")

        st.markdown("### 🔀 Vista temporal de ventas")
        temporal_enabled = st.toggle(
            "Activar análisis por periodo (día / semana / mes)",
            key="dashboard_temporal_toggle",
            value=False,
        )
        if temporal_enabled:
            temporal_base = build_temporal_sales_dataset(df_conf, vendedor_sel)
            if temporal_base.empty:
                st.info("No hay fechas válidas para construir la vista temporal.")
            else:
                ctrl1, ctrl2, ctrl3 = st.columns([0.24, 0.46, 0.3])
                with ctrl1:
                    gran_sel = st.selectbox("Granularidad", ["Día", "Semana", "Mes"], key="dashboard_temporal_gran")
                with ctrl2:
                    fecha_min = temporal_base["Fecha"].min().date()
                    fecha_max = temporal_base["Fecha"].max().date()
                    rango = st.date_input(
                        "Periodo a analizar",
                        value=(max(fecha_min, fecha_max - timedelta(days=60)), fecha_max),
                        min_value=fecha_min,
                        max_value=fecha_max,
                        key="dashboard_temporal_rango",
                    )
                with ctrl3:
                    metrica_sel = st.radio(
                        "Métrica",
                        options=["Ventas", "Pedidos"],
                        horizontal=True,
                        key="dashboard_temporal_metrica",
                    )

                fechas_validas = False
                fecha_inicio = None
                fecha_fin = None
                if isinstance(rango, tuple):
                    rango = tuple(r for r in rango if r is not None)
                if isinstance(rango, tuple) and len(rango) == 2:
                    fecha_inicio = pd.Timestamp(rango[0])
                    fecha_fin = pd.Timestamp(rango[1])
                    if fecha_fin >= fecha_inicio:
                        fechas_validas = True
                    else:
                        st.warning("La fecha final no puede ser menor que la fecha inicial.")
                else:
                    st.warning("Selecciona fecha inicial y final para analizar el periodo.")

                if fechas_validas:
                    actual_df, prev_df = aggregate_temporal_view(temporal_base, gran_sel, fecha_inicio, fecha_fin)
                else:
                    actual_df, prev_df = pd.DataFrame(), pd.DataFrame()

                if actual_df.empty:
                    st.warning("No hay datos en el periodo seleccionado.")
                else:
                    metric_col = "Monto" if metrica_sel == "Ventas" else "Pedidos"
                    metric_label = "$" if metrica_sel == "Ventas" else ""
                    total_actual = float(actual_df[metric_col].sum())
                    total_prev = float(prev_df[metric_col].sum()) if not prev_df.empty else 0.0
                    delta = ((total_actual / total_prev) - 1) if total_prev > 0 else np.nan

                    periodo_label = {
                        "Día": "día",
                        "Semana": "semana",
                        "Mes": "mes",
                    }.get(gran_sel, "periodo")
                    punto_idx = actual_df[metric_col].idxmax()
                    mejor_fecha = actual_df.loc[punto_idx, "Fecha"]

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric(
                        f"{metrica_sel} del periodo",
                        f"{metric_label}{total_actual:,.0f}",
                        f"{delta * 100:.1f}%" if pd.notna(delta) else "Sin comparativo",
                    )
                    m2.metric("Puntos analizados", f"{len(actual_df):,}")
                    m3.metric("Rango seleccionado", f"{(fecha_fin - fecha_inicio).days + 1} días")
                    m4.metric(
                        f"Mejor {periodo_label}",
                        f"{mejor_fecha.strftime('%d/%m/%Y')} ({metric_label}{actual_df.loc[punto_idx, metric_col]:,.0f})",
                    )

                    resumen = actual_df[["Fecha", "Monto", "Pedidos"]].copy()
                    resumen["Ticket_Promedio"] = np.where(
                        resumen["Pedidos"] > 0,
                        resumen["Monto"] / resumen["Pedidos"],
                        0.0,
                    )
                    resumen["Periodo"] = resumen["Fecha"].dt.strftime(
                        "%d/%m/%Y" if gran_sel == "Día" else ("Semana %V - %Y" if gran_sel == "Semana" else "%m/%Y")
                    )
                    st.caption("Detalle del comportamiento de ventas en el rango seleccionado")
                    st.dataframe(
                        resumen[["Periodo", "Monto", "Pedidos", "Ticket_Promedio"]].rename(
                            columns={
                                "Monto": "Ventas",
                                "Pedidos": "Pedidos",
                                "Ticket_Promedio": "Ticket promedio",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                        height=220,
                    )

                    venta_por_vendedor = (
                        temporal_base[(temporal_base["Fecha"] >= fecha_inicio) & (temporal_base["Fecha"] <= fecha_fin)]
                        .groupby("Vendedor", as_index=False)[["Monto", "Pedidos"]]
                        .sum()
                        .sort_values("Monto", ascending=False)
                    )
                    if not venta_por_vendedor.empty:
                        venta_por_vendedor["Ticket_Promedio"] = np.where(
                            venta_por_vendedor["Pedidos"] > 0,
                            venta_por_vendedor["Monto"] / venta_por_vendedor["Pedidos"],
                            0.0,
                        )
                        st.caption("Ranking de vendedores por ventas en el periodo")
                        st.dataframe(
                            venta_por_vendedor.rename(
                                columns={
                                    "Vendedor": "Vendedor",
                                    "Monto": "Ventas",
                                    "Pedidos": "Pedidos",
                                    "Ticket_Promedio": "Ticket promedio",
                                }
                            ),
                            use_container_width=True,
                            hide_index=True,
                            height=220,
                        )

                    serie = actual_df[["Fecha", metric_col]].copy()
                    if not prev_df.empty:
                        serie = serie.merge(
                            prev_df[["Fecha", metric_col]].rename(columns={metric_col: f"{metric_col}_Prev"}),
                            on="Fecha",
                            how="left",
                        )
                    st.caption("Comparativa del periodo actual vs periodo anterior equivalente")
                    st.line_chart(serie.set_index("Fecha"), height=220)

                    bars = actual_df[["Fecha", metric_col]].copy()
                    bars["Etiqueta"] = bars["Fecha"].dt.strftime("%d/%m")
                    st.bar_chart(bars.set_index("Etiqueta")[[metric_col]], height=220)

                    st.caption("Ranking histórico por periodo (top 5)")
                    rank_cols = st.columns(3)
                    rank_specs = [
                        ("Día", "D", "%d/%m/%Y"),
                        ("Semana", "W-MON", "Semana %V - %Y"),
                        ("Mes", "MS", "%m/%Y"),
                    ]
                    for rank_col, (rank_label, rank_freq, rank_fmt) in zip(rank_cols, rank_specs):
                        rank_df = (
                            temporal_base.groupby(pd.Grouper(key="Fecha", freq=rank_freq))[["Monto", "Pedidos"]]
                            .sum()
                            .reset_index()
                            .sort_values("Monto", ascending=False)
                            .head(5)
                        )
                        if rank_df.empty:
                            rank_col.info(f"Sin datos para ranking de {rank_label.lower()}.")
                            continue
                        rank_df["Periodo"] = rank_df["Fecha"].dt.strftime(rank_fmt)
                        rank_col.markdown(f"**Top {rank_label.lower()}s**")
                        rank_col.dataframe(
                            rank_df[["Periodo", "Monto", "Pedidos"]].rename(
                                columns={"Monto": "Ventas", "Pedidos": "Pedidos"}
                            ),
                            use_container_width=True,
                            hide_index=True,
                            height=210,
                        )

        st.markdown("---")

    tc = tabla_clientes.copy()
    if vendedor_sel != "(Todos)":
        tc = tc[
            tc["Vendedor"].map(_normalize_vendedor_name)
            == _normalize_vendedor_name(vendedor_sel)
        ]
    if estado_sel:
        tc = tc[tc["Estado"].isin(estado_sel)]

    st.markdown("#### 📌 Últimos pedidos según filtro")
    update_col, button_col = st.columns([0.75, 0.25])
    with update_col:
        st.caption(
            f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')} · Auto-actualización cada 60 s"
        )
    with button_col:
        if st.button("🔄 Actualizar lista", key="manual_refresh_ultimos_pedidos", use_container_width=True):
            load_data_from_gsheets.clear()
            st.rerun()

    ultimos_filtrados = build_ultimos_pedidos(df_all, vendedor_sel)
    ultimos_base = build_ultimos_pedidos_data(df_all, vendedor_sel)
    if ultimos_filtrados.empty:
        st.info("No hay pedidos recientes para el filtro seleccionado.")
    else:
        st.caption(
            "Mostrando pedidos y casos especiales en flujo"
            if vendedor_sel == "(Todos)"
            else f"Mostrando pedidos de {vendedor_sel} en flujo"
        )
        st.dataframe(ultimos_filtrados, use_container_width=True, height=260, hide_index=True)

        st.markdown("##### 🔎 Ver detalle de un pedido")
        selector_df = ultimos_base.copy()
        if "Folio_Factura" not in selector_df.columns:
            selector_df["Folio_Factura"] = ""
        if "Cliente" not in selector_df.columns:
            selector_df["Cliente"] = ""
        if "Estado" not in selector_df.columns:
            selector_df["Estado"] = ""
        if "Hora_Registro" not in selector_df.columns:
            selector_df["Hora_Registro"] = pd.NaT

        selector_df["_label_hora"] = pd.to_datetime(
            selector_df["Hora_Registro"], errors="coerce"
        ).dt.strftime("%d/%m/%Y %H:%M").fillna("sin fecha")
        selector_df["_label_folio"] = selector_df["Folio_Factura"].map(sanitize_text)
        selector_df.loc[selector_df["_label_folio"] == "", "_label_folio"] = "Sin folio"
        guia_adjuntos = selector_df.get(
            "Adjuntos_Guia", pd.Series("", index=selector_df.index)
        ).map(sanitize_text)
        guia_hoja_ruta = selector_df.get(
            "Hoja_Ruta_Mensajero", pd.Series("", index=selector_df.index)
        ).map(sanitize_text)
        origen_pedido = selector_df.get(
            "_origen_pedido", pd.Series("", index=selector_df.index)
        ).map(sanitize_text)
        selector_df["_guia_contenido"] = guia_adjuntos
        selector_df.loc[
            origen_pedido == "casos_especiales", "_guia_contenido"
        ] = guia_hoja_ruta

        selector_df["_label_guia"] = selector_df["_guia_contenido"].map(
            lambda x: "📋 " if x else ""
        )
        selector_df["_label_cliente"] = selector_df["Cliente"].map(sanitize_text)
        selector_df.loc[selector_df["_label_cliente"] == "", "_label_cliente"] = "Sin cliente"
        selector_df["_label_estado"] = selector_df["Estado"].map(sanitize_text)
        if "Tipo_Envio" in selector_df.columns:
            selector_df["_label_envio"] = selector_df["Tipo_Envio"].map(_pedido_selector_envio_emoji)
        else:
            selector_df["_label_envio"] = ""
        selector_df["_pedido_label"] = selector_df.apply(
            lambda r: (
                f"{r['_label_guia']}{r['_label_folio']} · {r['_label_cliente']} · {r['_label_estado']} · {r['_label_hora']}"
                f" {r['_label_envio']}" if r["_label_envio"] else f"{r['_label_guia']}{r['_label_folio']} · {r['_label_cliente']} · {r['_label_estado']} · {r['_label_hora']}"
            ),
            axis=1,
        )

        pedido_idx = st.selectbox(
            "🧭 Selecciona un pedido para ver más información",
            options=selector_df.index.tolist(),
            format_func=lambda idx: selector_df.loc[idx, "_pedido_label"],
            key="dashboard_detalle_pedido_idx",
        )
        pedido_sel = selector_df.loc[pedido_idx]

        with st.container(border=True):
            st.markdown("**🧾 Info general**")
            c1, c2 = st.columns(2)
            with c1:
                _render_detail_row("👤 id_vendedor", pedido_sel.get("id_vendedor", ""))
                turno_val = sanitize_text(pedido_sel.get("Turno", ""))
                if turno_val:
                    _render_detail_row("🕒 Turno", turno_val)
                _render_detail_row("💬 Comentario", pedido_sel.get("Comentario", ""))
            with c2:
                _render_detail_row("💳 Estado_Pago", pedido_sel.get("Estado_Pago", ""))
                _render_detail_row("📎 Adjuntos", display_attachments(pedido_sel.get("Adjuntos", "")))

            st.markdown("---")
            st.markdown("**📦 Sección de guías**")
            g1, g2 = st.columns(2)
            with g1:
                _render_detail_row(
                    "📬 ",
                    pedido_sel.get("Direccion_Guia_Retorno", ""),
                )
            with g2:
                _render_detail_row(
                    "🧷 Adjuntos_Guia",
                    display_attachments(pedido_sel.get("_guia_contenido", "")),
                )

            mod_cols = ["id_vendedor_Mod", "Modificacion_Surtido", "Adjuntos_Surtido"]
            has_mod_data = any(sanitize_text(pedido_sel.get(col, "")) for col in mod_cols)
            if has_mod_data:
                st.markdown("---")
                st.markdown("**🛠️ Sección de modificación**")
                m1, m2, m3 = st.columns(3)
                with m1:
                    _render_detail_row("👷 id_vendedor_Mod", pedido_sel.get("id_vendedor_Mod", ""))
                with m2:
                    _render_detail_row("🛠️ Modificacion_Surtido", pedido_sel.get("Modificacion_Surtido", ""))
                with m3:
                    _render_detail_row(
                        "📎 Adjuntos_Surtido",
                        display_attachments(pedido_sel.get("Adjuntos_Surtido", "")),
                    )

        with st.expander("🧾 Revisado de pedidos que viajaron", expanded=False):
            h_update_col, h_button_col = st.columns([0.75, 0.25])
            with h_update_col:
                st.caption(
                    f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')} · "
                    "Auto-actualización cada 60 s"
                )
            with h_button_col:
                if st.button(
                    "🔄 Actualizar datos_pedidos",
                    key="manual_refresh_historial_pedidos",
                    use_container_width=True,
                ):
                    load_historicos_from_gsheets.clear()
                    st.rerun()

            historial_df = load_historicos_from_gsheets()
            if historial_df.empty:
                st.info("No hay datos disponibles en `datos_pedidos` para mostrar.")
            else:
                historial_work = historial_df.copy()
                for col in [
                    "Hora_Registro",
                    "Fecha_Entrega",
                    "Cliente",
                    "Vendedor",
                    "Vendedor_Registro",
                    "Folio_Factura",
                    "Tipo_Envio",
                    "Estado",
                ]:
                    if col not in historial_work.columns:
                        historial_work[col] = ""

                historial_work["Hora_Registro"] = pd.to_datetime(
                    historial_work["Hora_Registro"], errors="coerce"
                )
                historial_work["Fecha_Entrega"] = pd.to_datetime(
                    historial_work["Fecha_Entrega"], errors="coerce"
                )

                if vendedor_sel != "(Todos)":
                    vend_norm = _normalize_vendedor_name(vendedor_sel)
                    mask_vendedor = (
                        historial_work["Vendedor_Registro"].map(_normalize_vendedor_name) == vend_norm
                    ) | (
                        historial_work["Vendedor"].map(_normalize_vendedor_name) == vend_norm
                    )
                    historial_work = historial_work[mask_vendedor].copy()

                buscador_hist = st.text_input(
                    "🔎 Buscar en historial por cliente o folio",
                    key="dashboard_historial_busqueda",
                    placeholder="Ej. F199985 o nombre del cliente",
                ).strip()
                if buscador_hist:
                    q_norm = sanitize_text(buscador_hist).casefold()
                    mask_busqueda = (
                        historial_work["Cliente"].map(lambda x: sanitize_text(x).casefold()).str.contains(q_norm, na=False)
                        | historial_work["Folio_Factura"].map(lambda x: sanitize_text(x).casefold()).str.contains(q_norm, na=False)
                    )
                    historial_work = historial_work[mask_busqueda].copy()

                historial_work["_fecha_revision"] = historial_work["Hora_Registro"].dt.date
                sin_hora = historial_work["_fecha_revision"].isna()
                historial_work.loc[sin_hora, "_fecha_revision"] = historial_work.loc[
                    sin_hora, "Fecha_Entrega"
                ].dt.date
                historial_work = historial_work.sort_values("Hora_Registro", ascending=False)

                historial_cols = [
                    "Hora_Registro",
                    "Cliente",
                    "Vendedor_Registro",
                    "Folio_Factura",
                    "Tipo_Envio",
                    "Fecha_Entrega",
                    "Estado",
                ]
                base_historial = historial_work[historial_cols].copy()
                base_historial["Hora_Registro"] = pd.to_datetime(
                    base_historial["Hora_Registro"], errors="coerce"
                ).dt.strftime("%d/%m/%Y %H:%M")
                base_historial["Fecha_Entrega"] = pd.to_datetime(
                    base_historial["Fecha_Entrega"], errors="coerce"
                ).dt.strftime("%d/%m/%Y")
                st.markdown("**Últimos 5 registros históricos**")
                st.dataframe(base_historial.head(5), use_container_width=True, hide_index=True, height=220)

                filtro_col1, filtro_col2 = st.columns(2)
                with filtro_col1:
                    fechas_disponibles = sorted(
                        [f for f in historial_work["_fecha_revision"].dropna().unique().tolist()],
                        reverse=True,
                    )
                    usar_filtro_fecha = st.toggle(
                        "Filtrar por fecha exacta",
                        value=True,
                        key="dashboard_historial_usar_fecha",
                    )
                    fecha_sel = None
                    if fechas_disponibles:
                        fecha_sel = st.date_input(
                            "Fecha",
                            value=fechas_disponibles[0],
                            min_value=fechas_disponibles[-1],
                            max_value=fechas_disponibles[0],
                            key="dashboard_historial_fecha_picker",
                        )
                    else:
                        st.caption("Sin fechas disponibles para filtrar.")
                with filtro_col2:
                    tipo_sel = st.selectbox(
                        "Filtrar por tipo de envío",
                        options=["(Todos)", "📍 Pedido Local", "🚚 Pedido Foráneo"],
                        key="dashboard_historial_tipo_envio",
                    )

                historial_filtrado = historial_work.copy()
                if usar_filtro_fecha and fecha_sel is not None:
                    historial_filtrado = historial_filtrado[
                        historial_filtrado["_fecha_revision"] == fecha_sel
                    ]
                if tipo_sel != "(Todos)":
                    tipo_norm_sel = _normalize_envio_original(tipo_sel)
                    historial_filtrado = historial_filtrado[
                        historial_filtrado["Tipo_Envio"].astype(str).apply(_normalize_envio_original).str.contains(
                            tipo_norm_sel, na=False
                        )
                    ]

                vista_filtrada = historial_filtrado[historial_cols].copy()
                vista_filtrada["Hora_Registro"] = pd.to_datetime(
                    vista_filtrada["Hora_Registro"], errors="coerce"
                ).dt.strftime("%d/%m/%Y %H:%M")
                vista_filtrada["Fecha_Entrega"] = pd.to_datetime(
                    vista_filtrada["Fecha_Entrega"], errors="coerce"
                ).dt.strftime("%d/%m/%Y")
                st.caption(f"Coincidencias encontradas: {len(vista_filtrada)}")
                if vista_filtrada.empty:
                    st.info("No se encontraron coincidencias para el filtro seleccionado.")
                else:
                    st.caption(
                        "Mostrando pedidos históricos enviados"
                        if vendedor_sel == "(Todos)"
                        else f"Mostrando pedidos históricos de {vendedor_sel}"
                    )
                    st.dataframe(vista_filtrada, use_container_width=True, hide_index=True, height=260)

                    st.markdown("##### 🔎 Ver detalle de un pedido histórico")
                    selector_hist = historial_filtrado.copy()
                    for c in [
                        "Folio_Factura",
                        "Cliente",
                        "Estado",
                        "Hora_Registro",
                        "Tipo_Envio",
                        "Adjuntos_Guia",
                        "Hoja_Ruta_Mensajero",
                    ]:
                        if c not in selector_hist.columns:
                            selector_hist[c] = ""

                    selector_hist["_label_hora"] = pd.to_datetime(
                        selector_hist["Hora_Registro"], errors="coerce"
                    ).dt.strftime("%d/%m/%Y %H:%M").fillna("sin fecha")
                    selector_hist["_label_folio"] = selector_hist["Folio_Factura"].map(sanitize_text)
                    selector_hist.loc[selector_hist["_label_folio"] == "", "_label_folio"] = "Sin folio"
                    selector_hist["_label_cliente"] = selector_hist["Cliente"].map(sanitize_text)
                    selector_hist.loc[selector_hist["_label_cliente"] == "", "_label_cliente"] = "Sin cliente"
                    selector_hist["_label_estado"] = selector_hist["Estado"].map(sanitize_text)
                    selector_hist["_label_envio"] = selector_hist["Tipo_Envio"].map(_pedido_selector_envio_emoji)

                    guia_adjuntos_hist = selector_hist.get(
                        "Adjuntos_Guia", pd.Series("", index=selector_hist.index)
                    ).map(sanitize_text)
                    guia_hoja_ruta_hist = selector_hist.get(
                        "Hoja_Ruta_Mensajero", pd.Series("", index=selector_hist.index)
                    ).map(sanitize_text)
                    selector_hist["_guia_contenido"] = guia_adjuntos_hist
                    selector_hist.loc[
                        selector_hist["_guia_contenido"] == "", "_guia_contenido"
                    ] = guia_hoja_ruta_hist
                    selector_hist["_label_guia"] = selector_hist["_guia_contenido"].map(
                        lambda x: "📋 " if x else ""
                    )
                    selector_hist["_pedido_label"] = selector_hist.apply(
                        lambda r: (
                            f"{r['_label_guia']}{r['_label_folio']} · {r['_label_cliente']} · {r['_label_estado']} · {r['_label_hora']}"
                            f" {r['_label_envio']}" if r["_label_envio"] else f"{r['_label_guia']}{r['_label_folio']} · {r['_label_cliente']} · {r['_label_estado']} · {r['_label_hora']}"
                        ),
                        axis=1,
                    )

                    idx_hist = st.selectbox(
                        "🧭 Selecciona un pedido histórico para ver más información",
                        options=selector_hist.index.tolist(),
                        format_func=lambda idx: selector_hist.loc[idx, "_pedido_label"],
                        key="dashboard_detalle_pedido_hist_idx",
                    )
                    pedido_hist_sel = selector_hist.loc[idx_hist]

                    with st.container(border=True):
                        st.markdown("**🧾 Info general**")
                        hc1, hc2 = st.columns(2)
                        with hc1:
                            _render_detail_row("👤 id_vendedor", pedido_hist_sel.get("id_vendedor", ""))
                            turno_hist = sanitize_text(pedido_hist_sel.get("Turno", ""))
                            if turno_hist:
                                _render_detail_row("🕒 Turno", turno_hist)
                            _render_detail_row("💬 Comentario", pedido_hist_sel.get("Comentario", ""))
                        with hc2:
                            _render_detail_row("💳 Estado_Pago", pedido_hist_sel.get("Estado_Pago", ""))
                            _render_detail_row("📎 Adjuntos", display_attachments(pedido_hist_sel.get("Adjuntos", "")))

                        st.markdown("---")
                        st.markdown("**📦 Sección de guías**")
                        hg1, hg2 = st.columns(2)
                        with hg1:
                            _render_detail_row("📬 ", pedido_hist_sel.get("Direccion_Guia_Retorno", ""))
                        with hg2:
                            _render_detail_row(
                                "🧷 Adjuntos_Guia",
                                display_attachments(pedido_hist_sel.get("_guia_contenido", "")),
                            )

    st.markdown("---")

    tc["Ticket_Promedio"] = pd.to_numeric(tc["Ticket_Promedio"], errors="coerce").fillna(0.0)
    tc_top = tc[tc["Ticket_Promedio"] > 0].copy()

    df_metricas_v = filter_df_by_vendedor(df_conf, vendedor_sel)

    ventas_v = float(
        get_numeric_column(df_metricas_v, "Monto_Comprobante", default=0.0).sum()
    )
    pedidos_v = int(len(df_metricas_v))
    ticket_v = float(ventas_v / pedidos_v) if pedidos_v else 0.0

    resumen_v = build_resumen_vendedor(tc)
    if vendedor_sel != "(Todos)":
        vm1, vm2, vm3 = st.columns(3)
        vm1.metric("💰 Ventas vendedor", f"${ventas_v:,.0f}")
        vm2.metric("📦 Pedidos vendedor", f"{pedidos_v:,}")
        vm3.metric("🎟️ Ticket prom vendedor", f"${ticket_v:,.0f}")

    resumen_v = build_resumen_vendedor(tc)
    if vendedor_sel != "(Todos)" and not resumen_v.empty:
        fila_v = resumen_v.iloc[0]
        sm1, sm2, sm3, sm4, sm5 = st.columns(5)
        sm1.metric("👥 Clientes con historial", f"{int(fila_v['Total_Evaluado']):,}")
        sm2.metric("✅ Activo", f"{int(fila_v['Activo']):,}")
        sm3.metric("⚠️ Alerta", f"{int(fila_v['Alerta']):,}")
        sm4.metric("🚨 Riesgo", f"{int(fila_v['Riesgo']):,}")
        sm5.metric("🆕 Nuevo/SinHistorial", f"{int(fila_v['Nuevo/SinHistorial']):,}")

    if confirmados_cache_missing:
        st.info(
            "No hay caché de pedidos_confirmados todavía. "
            "Si necesitas esas métricas, usa este botón para cargar confirmados manualmente."
        )

    if st.button("🔄 Actualizar pedidos confirmados", key="refresh_confirmados_dashboard", use_container_width=True):
        with st.spinner("Actualizando pedidos_confirmados desde Google Sheets..."):
            refresh_confirmados_cache(GSHEETS_CREDENTIALS, GOOGLE_SHEET_ID, SHEET_CONFIRMADOS)
        st.success("✅ pedidos_confirmados actualizado.")
        st.rerun()

    st.markdown("### 📊 Vista rápida y accionable")
    view_col1, view_col2 = st.columns([0.48, 0.52])

    with view_col1:
        estado_order = ["Activo", "Alerta", "Riesgo", "Nuevo/SinHistorial"]
        estado_colors = {
            "Activo": "#22c55e",
            "Alerta": "#f59e0b",
            "Riesgo": "#ef4444",
            "Nuevo/SinHistorial": "#60a5fa",
        }
        estado_counts = (
            tc["Estado"].value_counts().reindex(estado_order, fill_value=0).reset_index()
        )
        estado_counts.columns = ["Estado", "Clientes"]
        st.caption("Distribución de clientes por estado")
        st.vega_lite_chart(
            estado_counts,
            {
                "mark": {"type": "arc", "innerRadius": 55},
                "encoding": {
                    "theta": {"field": "Clientes", "type": "quantitative"},
                    "color": {
                        "field": "Estado",
                        "type": "nominal",
                        "scale": {
                            "domain": list(estado_colors.keys()),
                            "range": list(estado_colors.values()),
                        },
                    },
                    "tooltip": [
                        {"field": "Estado", "type": "nominal"},
                        {"field": "Clientes", "type": "quantitative"},
                    ],
                },
            },
            use_container_width=True,
        )

    with view_col2:
        if vendedor_sel != "(Todos)":
            st.caption("Ventas del mes actual")

            monthly_df = df_metricas_v.copy()
            monthly_df["Fecha"] = _resolve_sales_datetime(monthly_df)
            monthly_df["Monto"] = get_numeric_column(
                monthly_df, "Monto_Comprobante", default=0.0
            )
            monthly_df = monthly_df.dropna(subset=["Fecha"])

            if monthly_df.empty:
                st.info("No hay ventas con fecha válida para el vendedor seleccionado.")
            else:
                ahora = pd.Timestamp.now()
                inicio_mes = ahora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                inicio_mes_anterior = (inicio_mes - pd.offsets.MonthBegin(1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

                ventas_mes_actual = float(
                    monthly_df.loc[monthly_df["Fecha"] >= inicio_mes, "Monto"].sum()
                )
                ventas_mes_anterior = float(
                    monthly_df.loc[
                        (monthly_df["Fecha"] >= inicio_mes_anterior)
                        & (monthly_df["Fecha"] < inicio_mes),
                        "Monto",
                    ].sum()
                )
                ventas_hoy = float(
                    monthly_df.loc[monthly_df["Fecha"].dt.date == ahora.date(), "Monto"].sum()
                )

                met1, met2 = st.columns(2)
                met1.metric("💰 Ventas mes", f"${ventas_mes_actual:,.0f}")
                met2.metric("📅 Ventas de hoy", f"${ventas_hoy:,.0f}")

                if ventas_mes_anterior > 0:
                    ratio_avance = ventas_mes_actual / ventas_mes_anterior
                    avance_pct = max(0.0, min(ratio_avance * 100, 100.0))
                    faltante_pct = max(0.0, 100.0 - (ratio_avance * 100))
                    monto_faltante = max(0.0, ventas_mes_anterior - ventas_mes_actual)

                    st.caption("Progreso para alcanzar ventas del mes anterior")
                    st.progress(int(avance_pct))
                    if ratio_avance < 1:
                        st.markdown(
                            f"Te falta **{faltante_pct:.1f}%** (≈ **${monto_faltante:,.0f}**) para igualar el mes anterior."
                        )
                    else:
                        excedente = ventas_mes_actual - ventas_mes_anterior
                        st.success(
                            f"¡Meta superada! Ya vas **{ratio_avance * 100:.1f}%** del mes anterior (+${excedente:,.0f})."
                        )
                else:
                    st.info("No hay referencia de ventas del mes anterior para calcular avance.")

                mes_actual_df = monthly_df[monthly_df["Fecha"] >= inicio_mes].copy()
                if mes_actual_df.empty:
                    st.info("Aún no hay ventas registradas en el mes actual.")
                else:
                    mes_actual_df["DiaMes"] = mes_actual_df["Fecha"].dt.day
                    mes_actual_df["SemanaMes"] = ((mes_actual_df["DiaMes"] - 1) // 7) + 1
                    mes_actual_df["DiaSemanaNum"] = mes_actual_df["Fecha"].dt.dayofweek
                    day_labels = {
                        0: "Lun",
                        1: "Mar",
                        2: "Mié",
                        3: "Jue",
                        4: "Vie",
                        5: "Sáb",
                        6: "Dom",
                    }
                    mes_actual_df["DiaSemana"] = mes_actual_df["DiaSemanaNum"].map(day_labels)
                    heat = (
                        mes_actual_df.groupby(["SemanaMes", "DiaSemanaNum", "DiaSemana"], as_index=False)["Monto"]
                        .sum()
                    )

                    st.caption(
                        "Mapa de calor semanal del mes: más oscuro = día con mayor venta para este vendedor."
                    )
                    st.vega_lite_chart(
                        heat,
                        {
                            "mark": {"type": "rect", "cornerRadius": 4},
                            "encoding": {
                                "x": {
                                    "field": "DiaSemana",
                                    "type": "ordinal",
                                    "sort": ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
                                    "title": "Día de la semana",
                                },
                                "y": {
                                    "field": "SemanaMes",
                                    "type": "ordinal",
                                    "title": "Semana del mes",
                                },
                                "color": {
                                    "field": "Monto",
                                    "type": "quantitative",
                                    "title": "Venta",
                                    "scale": {"scheme": "blues"},
                                },
                                "tooltip": [
                                    {"field": "SemanaMes", "title": "Semana"},
                                    {"field": "DiaSemana", "title": "Día"},
                                    {"field": "Monto", "type": "quantitative", "title": "Ventas"},
                                ],
                            },
                            "height": 230,
                        },
                        use_container_width=True,
                    )
        else:
            st.caption("Riesgo por vendedor (% de cartera en riesgo)")
            if resumen_v.empty:
                st.info("Sin datos de vendedores para el filtro actual.")
            else:
                riesgo_v = resumen_v[["Vendedor", "%Riesgo", "Ventas"]].copy()
                riesgo_v["%Riesgo"] = pd.to_numeric(riesgo_v["%Riesgo"], errors="coerce").fillna(0)
                riesgo_v = riesgo_v.sort_values("%Riesgo", ascending=False).head(8)
                st.bar_chart(riesgo_v.set_index("Vendedor")[["%Riesgo"]], height=230)

    trend_col1, trend_col2 = st.columns([0.7, 0.3])
    with trend_col1:
        st.caption("Tendencia semanal de ventas (basada en confirmados)")
        trend_df = df_metricas_v.copy()
        trend_df["Fecha"] = _resolve_sales_datetime(trend_df)
        trend_df["Monto"] = get_numeric_column(
            trend_df, "Monto_Comprobante", default=0.0
        )
        trend_df = trend_df.dropna(subset=["Fecha"])
        if trend_df.empty:
            st.info("No hay fechas válidas para graficar tendencia.")
        else:
            trend_df["Semana"] = trend_df["Fecha"].dt.to_period("W").dt.start_time
            semana_actual = pd.Timestamp.now().to_period("W").start_time
            trend_df = trend_df[trend_df["Semana"] < semana_actual]
            if trend_df.empty:
                st.info("No hay semanas cerradas para graficar tendencia.")
            else:
                weekly = trend_df.groupby("Semana", as_index=False)["Monto"].sum().sort_values("Semana")
                weekly = weekly.tail(10)
                st.line_chart(weekly.set_index("Semana")["Monto"], height=220)
    with trend_col2:
        st.info(
            "\n".join(
                [
                    "**Qué hacer hoy**",
                    f"• Prioriza {int((tc['Estado'] == 'Riesgo').sum()):,} clientes en Riesgo.",
                    f"• Da seguimiento a {int((tc['Estado'] == 'Alerta').sum()):,} clientes en Alerta.",
                    f"• Ticket promedio visible: ${tc['Ticket_Promedio'].mean():,.0f}.",
                ]
            )
        )

    proy_total, proy_n, prox_df = compute_proyeccion_30(tc, hoy)

    if vendedor_sel == "(Todos)":
        with st.expander("🧑‍💼 Salud de cartera por vendedor", expanded=False):
            if resumen_v.empty:
                st.info("No hay datos para el filtro seleccionado.")
            else:
                st.dataframe(
                    resumen_v[
                        [
                            "Vendedor",
                            "Ventas",
                            "Pedidos",
                            "Ticket_Prom",
                            "Activo",
                            "Alerta",
                            "Riesgo",
                            "Nuevo/SinHistorial",
                            "%Riesgo",
                            "Total_Evaluado",
                            "Total",
                        ]
                    ].sort_values("%Riesgo", ascending=False),
                    use_container_width=True,
                    height=380,
                    hide_index=True,
                )

    with st.expander("🏥 Clientes (Top)", expanded=False):
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.caption("Top clientes por dinero total")
            top_money = tc_top.sort_values("Ventas_Total", ascending=False).head(15)[
                [
                    "Cliente",
                    "Ventas_Total",
                    "Estado",
                    "Vendedor",
                ]
            ]
            st.dataframe(top_money, use_container_width=True, height=420, hide_index=True)

        with col_b:
            st.caption("Clientes más recurrentes (más pedidos)")
            top_freq = tc_top.sort_values("Num_Pedidos", ascending=False).head(15)[
                [
                    "Cliente",
                    "Num_Pedidos",
                    "Estado",
                    "Vendedor",
                ]
            ]
            st.dataframe(top_freq, use_container_width=True, height=420, hide_index=True)

        with col_c:
            st.caption("Ticket promedio más alto (perfil proyecto)")
            top_ticket = tc_top.sort_values("Ticket_Promedio", ascending=False).head(15)[
                [
                    "Cliente",
                    "Ticket_Promedio",
                    "Estado",
                    "Vendedor",
                ]
            ]
            st.dataframe(top_ticket, use_container_width=True, height=420, hide_index=True)

    st.markdown("---")

    priority = {"Riesgo": 0, "Alerta": 1, "Activo": 2, "Nuevo/SinHistorial": 3}
    tc_priority = tc.copy()
    tc_priority["prio"] = tc_priority["Estado"].map(priority).fillna(99)
    tc_priority = tc_priority.sort_values(["prio", "Ventas_Total"], ascending=[True, False])

    with st.expander("🎯 Recomendaciones de acción (Top 5)", expanded=False):
        acciones = tc_priority[tc_priority["Estado"].isin(["Riesgo", "Alerta"])].head(5).copy()
        if acciones.empty:
            st.success("No hay clientes en riesgo/alerta con el filtro actual.")
        else:
            acciones["Acción sugerida"] = np.where(
                acciones["Estado"].eq("Riesgo"),
                "Llamada hoy + propuesta de recompra",
                "Seguimiento comercial en 24h",
            )
            st.dataframe(
                acciones[
                    [
                        "Cliente",
                        "Estado",
                        "Dias_Desde_Ultima",
                        "Ticket_Promedio",
                        "Vendedor",
                        "Acción sugerida",
                    ]
                ],
                use_container_width=True,
                height=220,
                hide_index=True,
            )

    with st.expander("🚨 Clientes en Alerta / Riesgo (prioridad)", expanded=False):
        tc_r = tc_priority[tc_priority["Estado"].isin(["Alerta", "Riesgo"])].copy().head(50)
        vis_cols = [
            "Cliente",
            "Estado",
            "Dias_Desde_Ultima",
            "Promedio_Ciclo",
            "Ciclo_Min_Dias",
            "Ciclo_Max_Dias",
            "Ratio",
            "Proxima_Estimada",
            "Ticket_Promedio",
            "Ventas_Total",
            "Num_Pedidos",
            "Vendedor",
        ]
        tc_r_display = ensure_columns(tc_r, vis_cols)
        styled_tc_r = tc_r_display[vis_cols].style.apply(
            lambda row: [
                (
                    "background-color: rgba(239, 68, 68, 0.30); color: #fff;"
                    if row["Estado"] == "Riesgo"
                    else "background-color: rgba(245, 158, 11, 0.30); color: #fff;"
                )
                if col == "Estado"
                else ""
                for col in vis_cols
            ],
            axis=1,
        )
        st.dataframe(styled_tc_r, use_container_width=True, height=520, hide_index=True)

    st.markdown("---")

    with st.expander("🔮 Próximas compras estimadas (30 días)", expanded=False):
        st.caption("Se excluyen clientes en Riesgo.")
        st.write(
            f"Clientes esperados: **{proy_n:,}** · Proyección total: **${proy_total:,.0f}**"
        )

        prox_cols = [
            "Cliente",
            "Vendedor",
            "Proxima_Estimada",
            "Ticket_Promedio",
            "Estado",
            "Promedio_Ciclo",
            "Ciclo_Min_Dias",
            "Ciclo_Max_Dias",
            "Dias_Desde_Ultima",
        ]
        prox_display = ensure_columns(prox_df, prox_cols)
        st.dataframe(
            prox_display.sort_values("Proxima_Estimada", ascending=True)[prox_cols],
            use_container_width=True,
            height=420,
            hide_index=True,
        )
