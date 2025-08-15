import streamlit as st
import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# =========================
# Configuración base
# =========================
TZ = ZoneInfo("America/Mexico_City")
st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")

# --- Controles: recarga manual + autorefresco ---
col_title, col_actions = st.columns([0.7, 0.3])
with col_title:
    st.markdown("""
        <h2 style="color: white; font-size: 1.8rem; margin-bottom: 0rem;">
            <span style="font-size: 2.2rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
        </h2>
    """, unsafe_allow_html=True)
    st.markdown("""
        <style>
        /* 🔢 Ajuste compacto para métricas */
        div[data-testid="metric-container"] { padding: 0.1rem 0.5rem; }
        div[data-testid="metric-container"] > div { font-size: 1.1rem !important; }
        div[data-testid="metric-container"] > label { font-size: 0.85rem !important; }
        </style>
    """, unsafe_allow_html=True)

with col_actions:
    if st.button("🔄 Recargar pedidos ahora", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.checkbox("⚡ Autorefrescar", key="auto_reload", help="Rerun automático sin limpiar caché")
    st.selectbox("Intervalo (seg)", [60, 45], index=0, key="auto_reload_interval")

if st.session_state.get("auto_reload"):
    interval = int(st.session_state.get("auto_reload_interval", 60))
    st.markdown(f'<meta http-equiv="refresh" content="{interval}">', unsafe_allow_html=True)

st.markdown("---")

# Mantén CSS de tabla compacta
st.markdown("""
    <style>
    .dataframe td { white-space: unset !important; word-break: break-word; }
    .dataframe {
        table-layout: fixed;
        width: 100%;
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
""", unsafe_allow_html=True)

# =========================
# Conexiones
# =========================
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
SHEET_MAIN = 'datos_pedidos'
SHEET_CASOS = 'casos_especiales'

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    _credentials_json_dict = dict(_credentials_json_dict)
    _credentials_json_dict["private_key"] = _credentials_json_dict["private_key"].replace("\\n", "\n").strip()
    creds = ServiceAccountCredentials.from_json_keyfile_dict(_credentials_json_dict, scope)
    client = gspread.authorize(creds)
    try:
        _ = client.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as e:
        if "expired" in str(e).lower() or "RESOURCE_EXHAUSTED" in str(e):
            st.cache_resource.clear()
            st.warning("🔁 Token expirado o cuota alcanzada. Reintentando autenticación...")
            creds = ServiceAccountCredentials.from_json_keyfile_dict(_credentials_json_dict, scope)
            client = gspread.authorize(creds)
        else:
            raise
    return client

try:
    if "aws" not in st.secrets:
        st.error("❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets.")
        st.stop()
    AWS_CREDENTIALS = st.secrets["aws"]
    AWS_ACCESS_KEY_ID = AWS_CREDENTIALS["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = AWS_CREDENTIALS["aws_secret_access_key"]
    AWS_REGION = AWS_CREDENTIALS["aws_region"]
    S3_BUCKET_NAME = AWS_CREDENTIALS["s3_bucket_name"]
except Exception as e:
    st.error(f"❌ Error al cargar credenciales de AWS S3: {e}")
    st.stop()

@st.cache_resource
def get_s3_client():
    try:
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        st.stop()

try:
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets.")
        st.stop()
    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")
    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()
    spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
    worksheet_main = spreadsheet.worksheet(SHEET_MAIN)
    # Hoja de casos puede no existir; la abrimos cuando la usemos
except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()

# =========================
# Carga de datos
# =========================
@st.cache_data(ttl=60)
def load_main_data():
    data = worksheet_main.get_all_values()
    if not data:
        return pd.DataFrame()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    df['gsheet_row_index'] = df.index + 2

    # Tipos
    if 'ID_Pedido' in df.columns:
        # No forzamos a int para evitar NaN → mantenemos string
        df['ID_Pedido'] = df['ID_Pedido'].astype(str)

    # Fechas/Horas
    for col in ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado', 'Fecha_Pago_Comprobante', 'Hora_Proceso']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'Turno' in df.columns:
        df['Turno'] = df['Turno'].astype(str).replace({'nan': '', '': None}).fillna('')
    else:
        df['Turno'] = ''

    # Normaliza Estado/Tipo_Envio
    for c in ['Estado', 'Tipo_Envio']:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna('').str.strip()

    # Para agrupación visual
    if 'Fecha_Entrega' in df.columns:
        df['Fecha_Entrega_Str'] = df['Fecha_Entrega'].dt.strftime("%d/%m")
    else:
        df['Fecha_Entrega_Str'] = ''

    # Campo auxiliar de adjuntos (string crudo)
    if 'Adjuntos' not in df.columns:
        df['Adjuntos'] = ''

    # Completados_Limpiado
    if 'Completados_Limpiado' not in df.columns:
        df['Completados_Limpiado'] = ''

    return df

@st.cache_data(ttl=60)
def load_casos_data():
    try:
        ws_casos = spreadsheet.worksheet(SHEET_CASOS)
    except gspread.exceptions.WorksheetNotFound:
        return pd.DataFrame(), []
    data = ws_casos.get_all_values()
    if not data:
        return pd.DataFrame(), []
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    df['gsheet_row_index'] = df.index + 2

    # Normaliza campos que solemos usar
    for c in ['Estado', 'Tipo_Envio', 'Tipo_Caso', 'Cliente', 'Vendedor_Registro', 'Folio_Factura']:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna('').str.strip()

    # Fechas relevantes
    for col in ['Fecha_Registro', 'Fecha_Entrega', 'Fecha_Completado', 'Hora_Proceso']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Para agrupación
    if 'Fecha_Entrega' in df.columns:
        df['Fecha_Entrega_Str'] = df['Fecha_Entrega'].dt.strftime("%d/%m")
    else:
        df['Fecha_Entrega_Str'] = ''

    return df, headers

# =========================
# Utilidades de presentación
# =========================
def get_s3_file_url(s3_object_key):
    """Si es un key S3, genera URL presignada; si ya es URL http(s), la devuelve tal cual."""
    if not s3_object_key:
        return None
    try:
        key = str(s3_object_key).strip()
        if key.startswith("http://") or key.startswith("https://"):
            return key
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
            ExpiresIn=3600
        )
    except Exception:
        return None

def display_attachments(adjuntos_str):
    if pd.isna(adjuntos_str) or not str(adjuntos_str).strip():
        return "N/A"
    try:
        parts = [p.strip() for p in str(adjuntos_str).split(',') if p.strip()]
        links = []
        for p in parts:
            url = get_s3_file_url(p)
            file_name = p.split('/')[-1]
            links.append(f"[{file_name}]({url})" if url else f"❌ {p}")
        return " | ".join(links)
    except Exception as e:
        return f"Error adjuntos: {e}"

def display_dataframe_with_formatting(df_to_display):
    """Vista compacta con columnas clave si existen."""
    columnas_deseadas = ["Fecha_Entrega", "Cliente", "Vendedor_Registro", "Estado"]
    cols_exist = [c for c in columnas_deseadas if c in df_to_display.columns]
    if not cols_exist:
        st.info("No hay columnas relevantes para mostrar.")
        return

    df_vista = df_to_display.copy()

    # Columna Cliente enriquecida con Folio si existe
    if "Folio_Factura" in df_vista.columns and "Cliente" in df_vista.columns:
        df_vista["Cliente"] = df_vista.apply(
            lambda row: f"📄 <b>{row['Folio_Factura']}</b> 🤝 {row['Cliente']}", axis=1
        )

    # Renombres suaves
    ren = {}
    if "Fecha_Entrega" in df_vista.columns:
        ren["Fecha_Entrega"] = "Fecha Entrega"
    if "Vendedor_Registro" in df_vista.columns:
        ren["Vendedor_Registro"] = "Vendedor"
    df_vista = df_vista.rename(columns=ren)

    if "Fecha Entrega" in df_vista.columns:
        df_vista["Fecha Entrega"] = df_vista["Fecha Entrega"].apply(
            lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
        )

    # Subset final en orden razonable si existen
    ordered = [c for c in ["Fecha Entrega", "Cliente", "Vendedor", "Estado"] if c in df_vista.columns]
    if not ordered:
        ordered = cols_exist
    st.markdown(df_vista[ordered].to_html(escape=False, index=False), unsafe_allow_html=True)

# =========================
# LÓGICA PRINCIPAL
# =========================
df_main = load_main_data()
st.caption(f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')}")

if df_main.empty:
    st.info("No hay datos en la hoja principal.")
    st.stop()

# Filtrado: oculta Completados limpiados
df_vista = df_main.copy()
df_vista = df_vista[
    (df_vista['Estado'] != '🟢 Completado') |
    ((df_vista['Estado'] == '🟢 Completado') &
     (df_vista['Completados_Limpiado'].astype(str).str.lower() != "sí"))
].copy()

# Solo estados de interés
df_vista = df_vista[df_vista['Estado'].isin(
    ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado", "🛠 Modificación", "🟣 Cancelado", "🟢 Completado"]
)]

# Resumen de estados (corrigiendo Completados limpiados)
completados_visibles = df_main[
    (df_main['Estado'] == '🟢 Completado') &
    (df_main.get('Completados_Limpiado', '').astype(str).str.lower() != 'sí')
]
estado_counts = {
    '🟡 Pendiente': (df_main['Estado'] == '🟡 Pendiente').sum(),
    '🔵 En Proceso': (df_main['Estado'] == '🔵 En Proceso').sum(),
    '🔴 Demorado': (df_main['Estado'] == '🔴 Demorado').sum(),
    '🛠 Modificación': (df_main['Estado'] == '🛠 Modificación').sum(),
    '🟣 Cancelado': (df_main['Estado'] == '🟣 Cancelado').sum(),
    '🟢 Completado': len(completados_visibles),
}

# Totales/métricas
st.markdown("#### 📊 Resumen General de Pedidos")
total_pedidos_estados = sum(estado_counts.values())
estados_fijos = ['🟡 Pendiente', '🔵 En Proceso', '🟢 Completado']
estados_condicionales = ['🔴 Demorado', '🛠 Modificación', '🟣 Cancelado']

estados_a_mostrar = [("📦 Total Pedidos", total_pedidos_estados)]
for e in estados_fijos:
    estados_a_mostrar.append((e, estado_counts[e]))
for e in estados_condicionales:
    if estado_counts.get(e, 0) > 0:
        estados_a_mostrar.append((e, estado_counts[e]))

cols = st.columns(len(estados_a_mostrar))
for col, (label, qty) in zip(cols, estados_a_mostrar):
    col.metric(label, int(qty))

st.markdown("---")

# =========================
# TABS PRINCIPALES
# =========================
tabs = st.tabs([
    "📍 Pedidos Locales",
    "🚚 Pedidos Foráneos",
    "🏙️ Pedidos CDMX",
    "📋 Solicitudes de Guía",
    "🔁 Devoluciones",
    "🛠 Garantías",
])

# ---- 📍 Pedidos Locales
with tabs[0]:
    st.markdown("### 📍 Pedidos Locales")
    df_loc = df_vista[(df_vista.get("Tipo_Envio", "") == "📍 Pedido Local")].copy()
    if df_loc.empty:
        st.info("No hay pedidos locales.")
    else:
        # Agrupar por Turno + Fecha
        df_loc['Grupo_Clave'] = df_loc.apply(
            lambda r: f"{r.get('Turno','').strip() or 'Sin turno'} – {r.get('Fecha_Entrega_Str','')}", axis=1
        )
        grouped = df_loc.groupby(['Grupo_Clave', 'Fecha_Entrega'])
        grupos = []
        for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
            if not df_g.empty:
                grupos.append((f"{clave} ({len(df_g)})", df_g.sort_values(by='Hora_Registro', ascending=False)))
        if grupos:
            for i in range(0, len(grupos), 3):
                row = grupos[i:i+3]
                cols = st.columns(len(row))
                for j, (titulo, df_g) in enumerate(row):
                    with cols[j]:
                        st.markdown(f"#### {titulo}")
                        display_dataframe_with_formatting(df_g)
        else:
            st.info("No hay grupos para mostrar.")

# ---- 🚚 Pedidos Foráneos
with tabs[1]:
    st.markdown("### 🚚 Pedidos Foráneos")
    df_for = df_vista[(df_vista.get("Tipo_Envio", "") == "🚚 Pedido Foráneo")].copy()
    if df_for.empty:
        st.info("No hay pedidos foráneos.")
    else:
        df_for['Grupo_Clave'] = df_for.apply(
            lambda r: f"🌍 Foráneo – {r.get('Fecha_Entrega_Str','')}", axis=1
        )
        grouped = df_for.groupby(['Grupo_Clave', 'Fecha_Entrega'])
        grupos = []
        for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
            if not df_g.empty:
                grupos.append((f"{clave} ({len(df_g)})", df_g.sort_values(by='Hora_Registro', ascending=False)))
        if grupos:
            for i in range(0, len(grupos), 3):
                row = grupos[i:i+3]
                cols = st.columns(len(row))
                for j, (titulo, df_g) in enumerate(row):
                    with cols[j]:
                        st.markdown(f"#### {titulo}")
                        display_dataframe_with_formatting(df_g)
        else:
            st.info("No hay grupos para mostrar.")

# ---- 🏙️ Pedidos CDMX
with tabs[2]:
    st.markdown("### 🏙️ Pedidos CDMX")
    df_cdmx = df_vista[(df_vista.get("Tipo_Envio", "") == "🏙️ Pedido CDMX")].copy()
    if df_cdmx.empty:
        st.info("No hay pedidos CDMX.")
    else:
        df_cdmx['Grupo_Clave'] = df_cdmx.apply(
            lambda r: f"🏙️ CDMX – {r.get('Fecha_Entrega_Str','')}", axis=1
        )
        grouped = df_cdmx.groupby(['Grupo_Clave', 'Fecha_Entrega'])
        grupos = []
        for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
            if not df_g.empty:
                grupos.append((f"{clave} ({len(df_g)})", df_g.sort_values(by='Hora_Registro', ascending=False)))
        if grupos:
            for i in range(0, len(grupos), 3):
                row = grupos[i:i+3]
                cols = st.columns(len(row))
                for j, (titulo, df_g) in enumerate(row):
                    with cols[j]:
                        st.markdown(f"#### {titulo}")
                        display_dataframe_with_formatting(df_g)
        else:
            st.info("No hay grupos para mostrar.")

# ---- 📋 Solicitudes de Guía
with tabs[3]:
    st.markdown("### 📋 Solicitudes de Guía")
    df_sg = df_vista[(df_vista.get("Tipo_Envio", "") == "📋 Solicitudes de Guía")].copy()
    if df_sg.empty:
        st.info("No hay solicitudes de guía.")
    else:
        df_sg['Grupo_Clave'] = df_sg.apply(
            lambda r: f"📋 Guía – {r.get('Fecha_Entrega_Str','')}", axis=1
        )
        grouped = df_sg.groupby(['Grupo_Clave', 'Fecha_Entrega'])
        grupos = []
        for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
            if not df_g.empty:
                grupos.append((f"{clave} ({len(df_g)})", df_g.sort_values(by='Hora_Registro', ascending=False)))
        if grupos:
            for i in range(0, len(grupos), 3):
                row = grupos[i:i+3]
                cols = st.columns(len(row))
                for j, (titulo, df_g) in enumerate(row):
                    with cols[j]:
                        st.markdown(f"#### {titulo}")
                        display_dataframe_with_formatting(df_g)
        else:
            st.info("No hay grupos para mostrar.")

# ---- 🔁 Devoluciones (desde casos_especiales)
with tabs[4]:
    st.markdown("### 🔁 Devoluciones (casos_especiales)")
    df_casos, headers_casos = load_casos_data()
    if df_casos.empty:
        st.info("No hay hoja 'casos_especiales' o está vacía.")
    else:
        tipo_col = "Tipo_Caso" if "Tipo_Caso" in df_casos.columns else ("Tipo_Envio" if "Tipo_Envio" in df_casos.columns else None)
        if not tipo_col:
            st.warning("La hoja 'casos_especiales' no tiene columna 'Tipo_Caso' ni 'Tipo_Envio'.")
        else:
            df_dev = df_casos[
                df_casos[tipo_col].astype(str).str.contains("Devoluci", case=False, na=False)
            ].copy()

            # opcional: excluir completados
            if "Estado" in df_dev.columns:
                df_dev = df_dev[df_dev["Estado"].astype(str).str.strip() != "🟢 Completado"]

            if df_dev.empty:
                st.info("No hay devoluciones pendientes.")
            else:
                # agrupamos por fecha
                if "Fecha_Entrega" in df_dev.columns:
                    df_dev['Fecha_Entrega_Str'] = df_dev['Fecha_Entrega'].dt.strftime("%d/%m")
                else:
                    df_dev['Fecha_Entrega_Str'] = ''
                df_dev['Grupo_Clave'] = df_dev.apply(
                    lambda r: f"{r.get('Fecha_Entrega_Str','') or 'Sin fecha'}", axis=1
                )
                grouped = df_dev.groupby(['Grupo_Clave', 'Fecha_Entrega'])
                grupos = []
                for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
                    if not df_g.empty:
                        grupos.append((f"📦 {clave} ({len(df_g)})", df_g.sort_values(by='Fecha_Registro', ascending=False, na_position='last') if 'Fecha_Registro' in df_g.columns else df_g))
                if grupos:
                    for i in range(0, len(grupos), 3):
                        row = grupos[i:i+3]
                        cols = st.columns(len(row))
                        for j, (titulo, df_g) in enumerate(row):
                            with cols[j]:
                                st.markdown(f"#### {titulo}")
                                # Asegura columnas esperadas para el renderer
                                for col in ["Cliente", "Vendedor_Registro", "Estado", "Fecha_Entrega", "Folio_Factura"]:
                                    if col not in df_g.columns:
                                        df_g[col] = ""
                                display_dataframe_with_formatting(df_g[["Fecha_Entrega","Cliente","Vendedor_Registro","Estado","Folio_Factura"] if "Folio_Factura" in df_g.columns else ["Fecha_Entrega","Cliente","Vendedor_Registro","Estado"]])
                else:
                    st.info("No hay grupos para mostrar.")

# ---- 🛠 Garantías (desde casos_especiales)
with tabs[5]:
    st.markdown("### 🛠 Garantías (casos_especiales)")
    df_casos, headers_casos = load_casos_data()
    if df_casos.empty:
        st.info("No hay hoja 'casos_especiales' o está vacía.")
    else:
        tipo_col = "Tipo_Caso" if "Tipo_Caso" in df_casos.columns else ("Tipo_Envio" if "Tipo_Envio" in df_casos.columns else None)
        if not tipo_col:
            st.warning("La hoja 'casos_especiales' no tiene columna 'Tipo_Caso' ni 'Tipo_Envio'.")
        else:
            df_gar = df_casos[
                df_casos[tipo_col].astype(str).str.contains("Garant", case=False, na=False)
            ].copy()

            # opcional: excluir completados
            if "Estado" in df_gar.columns:
                df_gar = df_gar[df_gar["Estado"].astype(str).str.strip() != "🟢 Completado"]

            if df_gar.empty:
                st.info("No hay garantías pendientes.")
            else:
                if "Fecha_Entrega" in df_gar.columns:
                    df_gar['Fecha_Entrega_Str'] = df_gar['Fecha_Entrega'].dt.strftime("%d/%m")
                else:
                    df_gar['Fecha_Entrega_Str'] = ''
                df_gar['Grupo_Clave'] = df_gar.apply(
                    lambda r: f"{r.get('Fecha_Entrega_Str','') or 'Sin fecha'}", axis=1
                )
                grouped = df_gar.groupby(['Grupo_Clave', 'Fecha_Entrega'])
                grupos = []
                for (clave, _), df_g in sorted(grouped, key=lambda x: (x[0][1] if x[0][1] is not None else pd.Timestamp.max)):
                    if not df_g.empty:
                        grupos.append((f"📦 {clave} ({len(df_g)})", df_g.sort_values(by='Fecha_Registro', ascending=False, na_position='last') if 'Fecha_Registro' in df_g.columns else df_g))
                if grupos:
                    for i in range(0, len(grupos), 3):
                        row = grupos[i:i+3]
                        cols = st.columns(len(row))
                        for j, (titulo, df_g) in enumerate(row):
                            with cols[j]:
                                st.markdown(f"#### {titulo}")
                                for col in ["Cliente", "Vendedor_Registro", "Estado", "Fecha_Entrega", "Folio_Factura"]:
                                    if col not in df_g.columns:
                                        df_g[col] = ""
                                display_dataframe_with_formatting(df_g[["Fecha_Entrega","Cliente","Vendedor_Registro","Estado","Folio_Factura"] if "Folio_Factura" in df_g.columns else ["Fecha_Entrega","Cliente","Vendedor_Registro","Estado"]])
                else:
                    st.info("No hay grupos para mostrar.")
