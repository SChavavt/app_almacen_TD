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
from zoneinfo import ZoneInfo
from streamlit_autorefresh import st_autorefresh

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
def get_gspread_client(_credentials_json_dict):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    _credentials_json_dict["private_key"] = (
        _credentials_json_dict["private_key"].replace("\\n", "\n").strip()
    )
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        _credentials_json_dict, scope
    )
    client = gspread.authorize(creds)
    try:
        _ = client.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        if "expired" in str(e).lower() or "RESOURCE_EXHAUSTED" in str(e):
            st.cache_resource.clear()
            st.warning(
                "🔁 Token expirado o cuota alcanzada. Reintentando autenticación..."
            )
            creds = ServiceAccountCredentials.from_json_keyfile_dict(
                _credentials_json_dict, scope
            )
            client = gspread.authorize(creds)
        # segundo intento limpio
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            _credentials_json_dict, scope
        )
        client = gspread.authorize(creds)
    return client


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
    columnas_deseadas = ["Fecha_Entrega", "Cliente", "Vendedor_Registro", "Estado"]
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
    ren = {"Fecha_Entrega": "Fecha Entrega", "Vendedor_Registro": "Vendedor"}
    for k, v in ren.items():
        if k in df_vista.columns:
            df_vista.rename(columns={k: v}, inplace=True)

    if "Fecha Entrega" in df_vista.columns:
        df_vista["Fecha Entrega"] = df_vista["Fecha Entrega"].apply(
            lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
        )

    mostrar_cols = [
        c
        for c in ["Fecha Entrega", "Cliente", "Vendedor", "Estado"]
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
    counts = {
        "🟡 Pendiente": (comps["Estado"] == "🟡 Pendiente").sum(),
        "🔵 En Proceso": (comps["Estado"] == "🔵 En Proceso").sum(),
        "🔴 Demorado": (comps["Estado"] == "🔴 Demorado").sum(),
        "🛠 Modificación": (comps["Estado"] == "🛠 Modificación").sum(),
        "🟣 Cancelado": (comps["Estado"] == "🟣 Cancelado").sum(),
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


def group_key_local_foraneo(row, local_flag_col="Turno"):
    """Devuelve Turno si hay (Local), sino etiqueta genérica Foráneo."""
    turno = str(row.get(local_flag_col, "") or "")
    return turno if turno else "🌍 Foráneo"


def show_grouped_panel(df_source):
    if df_source.empty:
        st.info("No hay registros para mostrar.")
        return
    work = df_source.copy()
    work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
    work["Grupo_Clave"] = work.apply(
        lambda r: f"{group_key_local_foraneo(r)} – {r['Fecha_Entrega_Str']}", axis=1
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


# ===========================
#        MAIN RENDER
# ===========================
df_all = load_data_from_gsheets()
st.caption(f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')}")

# Tabs principales
tabs = st.tabs(["📦 Pedidos (Local/Foráneo)", "🏙️ CDMX y Guías", "🧰 Casos Especiales"])

# ---------------------------
# TAB 0: Local / Foráneo
# ---------------------------
with tabs[0]:
    if df_all.empty:
        st.info("Sin datos en 'datos_pedidos'.")
    else:
        # Filtra solo Local y Foráneo (excluye CDMX y Solicitudes de Guía)
        df0 = df_all[
            df_all["Tipo_Envio"].isin(["📍 Pedido Local", "🚚 Pedido Foráneo"])
        ].copy()

        # Excluye Completados limpiados
        if "Completados_Limpiado" not in df0.columns:
            df0["Completados_Limpiado"] = ""
        df0 = df0[
            (df0["Estado"] != "🟢 Completado")
            | (
                (df0["Estado"] == "🟢 Completado")
                & (df0["Completados_Limpiado"].astype(str).str.lower() != "sí")
            )
        ]

        st.markdown("#### 📊 Resumen (Local/Foráneo)")
        status_counts_block(df0)

        st.markdown("### 📚 Grupos")
        show_grouped_panel(df0)

# ---------------------------
# TAB 1: CDMX y Guías
# ---------------------------
with tabs[1]:
    if df_all.empty:
        st.info("Sin datos en 'datos_pedidos'.")
    else:
        # ----- 1) CDMX -----
        st.subheader("🏙️ Pedidos CDMX")
        df_cdmx = df_all[df_all["Tipo_Envio"] == "🏙️ Pedido CDMX"].copy()
        if df_cdmx.empty:
            st.info("No hay pedidos CDMX.")
        else:
            if "Completados_Limpiado" not in df_cdmx.columns:
                df_cdmx["Completados_Limpiado"] = ""
            df_cdmx = df_cdmx[
                (df_cdmx["Estado"] != "🟢 Completado")
                | (
                    (df_cdmx["Estado"] == "🟢 Completado")
                    & (df_cdmx["Completados_Limpiado"].astype(str).str.lower() != "sí")
                )
            ]
            st.markdown("##### Resumen CDMX")
            status_counts_block(df_cdmx)
            st.markdown("##### Grupos CDMX (por fecha)")
            # Para CDMX vamos a agrupar solo por fecha (clave "CDMX – dd/mm")
            work = df_cdmx.copy()
            work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
            work["Grupo_Clave"] = work.apply(
                lambda r: f"🏙️ CDMX – {r['Fecha_Entrega_Str']}", axis=1
            )
            grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"])
            grupos = []
            for (clave, f), df_g in sorted(
                grouped,
                key=lambda x: x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
            ):
                if not df_g.empty:
                    grupos.append((f"{clave} ({len(df_g)})", df_g))
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

        # ----- 2) Solicitudes de Guía -----
        st.subheader("📋 Solicitudes de Guía")
        df_guias = df_all[df_all["Tipo_Envio"] == "📋 Solicitudes de Guía"].copy()
        if df_guias.empty:
            st.info("No hay solicitudes de guía.")
        else:
            if "Completados_Limpiado" not in df_guias.columns:
                df_guias["Completados_Limpiado"] = ""
            df_guias = df_guias[
                (df_guias["Estado"] != "🟢 Completado")
                | (
                    (df_guias["Estado"] == "🟢 Completado")
                    & (df_guias["Completados_Limpiado"].astype(str).str.lower() != "sí")
                )
            ]
            st.markdown("##### Resumen Guías")
            status_counts_block(df_guias)
            st.markdown("##### Grupos Guías (por fecha)")
            work = df_guias.copy()
            work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
            work["Grupo_Clave"] = work.apply(
                lambda r: f"📋 Guías – {r['Fecha_Entrega_Str']}", axis=1
            )
            grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"])
            grupos = []
            for (clave, f), df_g in sorted(
                grouped,
                key=lambda x: x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
            ):
                if not df_g.empty:
                    grupos.append((f"{clave} ({len(df_g)})", df_g))
            if not grupos:
                st.info("No hay grupos para mostrar.")
            else:
                for titulo, df_g in grupos:
                    st.markdown(f"#### {titulo}")
                    df_g = df_g.sort_values(
                        by="Hora_Registro", ascending=False
                    ).reset_index(drop=True)
                    display_dataframe_with_formatting(df_g)
# =========================
# Helpers para Casos Especiales
# =========================
if "load_casos_from_gsheets" not in globals():

    @st.cache_data(ttl=60)
    def load_casos_from_gsheets() -> pd.DataFrame:
        ws = spreadsheet.worksheet("casos_especiales")
        vals = ws.get_all_values()
        if not vals:
            return pd.DataFrame()
        headers = vals[0]
        df = pd.DataFrame(vals[1:], columns=headers)
        df["gsheet_row_index"] = df.index + 2

        # Parse fechas/horas
        for c in [
            "Hora_Registro",
            "Fecha_Entrega",
            "Fecha_Completado",
            "Hora_Proceso",
            "Fecha_Recepcion_Devolucion",
        ]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")

        # Normalizaciones mínimas
        for c in [
            "Cliente",
            "Vendedor_Registro",
            "Estado",
            "Folio_Factura",
            "Turno",
            "Tipo_Envio_Original",
            "Tipo_Envio",
            "Tipo_Caso",
        ]:
            if c not in df.columns:
                df[c] = ""
            else:
                df[c] = df[c].astype(str)

        return df


def status_counts_block_casos(df: pd.DataFrame):
    estados = df.get("Estado", pd.Series(dtype=str)).astype(str)
    total = len(df)
    pend = estados.str.contains("Pendiente", case=False, na=False).sum()
    proc = estados.str.contains("En Proceso", case=False, na=False).sum()
    comp = estados.str.contains("Completado", case=False, na=False).sum()
    cols = st.columns(4)
    cols[0].metric("Total Pedidos", int(total))
    cols[1].metric("🟡 Pendiente", int(pend))
    cols[2].metric("🔵 En Proceso", int(proc))
    cols[3].metric("🟢 Completado", int(comp))


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
            df_local["Fecha_Entrega"].dt.strftime("%d/%m")
            if "Fecha_Entrega" in df_local.columns
            else ""
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
            df_local.groupby(["Grupo_Clave", "Fecha_Entrega"]), key=lambda x: x[0][1]
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


# =========================
# TAB 2: Casos Especiales (Devoluciones + Garantías)
# =========================
with tabs[2]:
    df_casos = load_casos_from_gsheets()
    # Normaliza columnas para detección de tipo
    if (
        not df_casos.empty
        and "Tipo_Caso" not in df_casos.columns
        and "Tipo_Envio" in df_casos.columns
    ):
        df_casos["Tipo_Caso"] = df_casos["Tipo_Envio"]
    # Asegura tipo de envío original para poder mostrarlo en la vista
    if (
        not df_casos.empty
        and "Tipo_Envio_Original" not in df_casos.columns
        and "Tipo_Envio" in df_casos.columns
    ):
        df_casos["Tipo_Envio_Original"] = df_casos["Tipo_Envio"]

    # También incluir pedidos con Tipo_Envio de garantía desde 'datos_pedidos'
    df_garantias_pedidos = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        df_garantias_pedidos = df_all[
            df_all["Tipo_Envio"]
            .astype(str)
            .str.contains("Garant", case=False, na=False)
        ].copy()
        if not df_garantias_pedidos.empty:
            df_garantias_pedidos["Tipo_Caso"] = df_garantias_pedidos["Tipo_Envio"]
            df_garantias_pedidos["Tipo_Envio_Original"] = df_garantias_pedidos[
                "Tipo_Envio"
            ]

    casos = pd.concat([df_casos, df_garantias_pedidos], ignore_index=True)
    if casos.empty:
        st.info("Sin datos de devoluciones o garantías.")
    else:
        # Filtra devoluciones o garantías
        mask = (
            casos["Tipo_Caso"]
            .astype(str)
            .str.contains("Devoluci|Garant", case=False, na=False)
        )
        casos = casos[mask].copy()

        if casos.empty:
            st.info("No hay devoluciones/garantías para mostrar.")
        else:
            # Excluir completados limpiados, mostrar el resto
            if "Completados_Limpiado" not in casos.columns:
                casos["Completados_Limpiado"] = ""
            if "Estado" in casos.columns:
                casos = casos[
                    (casos["Estado"].astype(str).str.strip() != "🟢 Completado")
                    | (
                        (casos["Estado"].astype(str).str.strip() == "🟢 Completado")
                        & (
                            casos["Completados_Limpiado"].astype(str).str.lower()
                            != "sí"
                        )
                    )
                ]

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
                if base not in casos.columns:
                    casos[base] = ""

            # 🏷️ Etiqueta visible del tipo (Devolución/Garantía)
            def _etiqueta_tipo(v):
                s = str(v).lower()
                if "garant" in s:
                    return "🛠 Garantía"
                if "devolu" in s:
                    return "🔁 Devolución"
                return "—"

            casos["Tipo"] = casos["Tipo_Caso"].apply(_etiqueta_tipo)

            # --- Resumen
            st.markdown("#### 📊 Resumen Casos Especiales")
            status_counts_block_casos(casos)

            # --- Grupos
            st.markdown("### 📚 Grupos (Local por Turno / Foráneo genérico)")
            show_grouped_panel_casos(casos)
