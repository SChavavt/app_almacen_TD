import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # <- nuevo

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
    # Recarga manual: limpia cachés y rerun (pega a la API solo cuando TTL haya expirado)
    if st.button("🔄 Recargar pedidos ahora", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    # Toggle de autorefresco + selector de intervalo
    st.checkbox("⚡ Autorefrescar", key="auto_reload", help="Rerun automático sin limpiar caché")
    st.selectbox("Intervalo (seg)", [60, 45], index=0, key="auto_reload_interval")

# ⏱️ Disparador de autorefresco (no limpia caché → evita 429)
if st.session_state.get("auto_reload"):
    interval = int(st.session_state.get("auto_reload_interval", 60))
    # meta refresh: rerun de la página cada N segundos
    st.markdown(f'<meta http-equiv="refresh" content="{interval}">', unsafe_allow_html=True)

st.markdown("---")

# Mantén tu CSS de tabla
st.markdown("""
    <style>
    .dataframe td { white-space: unset !important; word-break: break-word; }
    </style>
""", unsafe_allow_html=True)


GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

def construir_gspread_client(creds_dict):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
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

        creds = ServiceAccountCredentials.from_json_keyfile_dict(_credentials_json_dict, scope)
        client = gspread.authorize(creds)
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
    st.error(f"❌ Error al cargar las credenciales de AWS S3: {e}")
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

    try:
        g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        s3_client = get_s3_client()
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet_main = spreadsheet.worksheet(GOOGLE_SHEET_WORKSHEET_NAME)

    except gspread.exceptions.APIError as e:
        if "ACCESS_TOKEN_EXPIRED" in str(e) or "UNAUTHENTICATED" in str(e):
            st.cache_resource.clear()
            st.warning("🔄 La sesión con Google Sheets expiró. Reconectando...")
            time.sleep(1)
            g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
            s3_client = get_s3_client()
            spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
            worksheet_main = spreadsheet.worksheet(GOOGLE_SHEET_WORKSHEET_NAME)
        else:
            st.error(f"❌ Error al autenticar clientes: {e}")
            st.stop()

except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()
@st.cache_data(ttl=60)
def load_data_from_gsheets():
    try:
        data = worksheet_main.get_all_values()
        if not data:
            return pd.DataFrame()

        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        df['gsheet_row_index'] = df.index + 2

        numerical_cols = ['ID_Pedido']
        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        date_time_cols = ['Hora_Registro', 'Fecha_Entrega', 'Fecha_Completado', 'Fecha_Pago_Comprobante', 'Hora_Proceso']
        for col in date_time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'Turno' in df.columns:
            df['Turno'] = df['Turno'].astype(str).replace({'nan': '', '': None}).fillna('')
        else:
            df['Turno'] = ''

        return df
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Google Sheets: {e}")
        st.stop()


def get_s3_file_url(s3_object_key):
    if not s3_object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_object_key},
            ExpiresIn=3600
        )
    except Exception as e:
        st.error(f"❌ Error URL S3: {e}")
        return None

def display_attachments(adjuntos_str, s3_client_instance):
    if pd.isna(adjuntos_str) or not adjuntos_str.strip():
        return "N/A"
    try:
        file_keys = [fk.strip() for fk in adjuntos_str.split(',') if fk.strip()]
        links = []
        for fk in file_keys:
            url = get_s3_file_url(fk)
            if url:
                file_name = fk.split('/')[-1]
                links.append(f"[{file_name}]({url})")
            else:
                links.append(f"❌ {fk} (Error URL)")
        return " | ".join(links)
    except Exception as e:
        return f"Error adjuntos: {e}"

def display_dataframe_with_formatting(df_to_display, num_columnas_actuales=1):
    columnas_deseadas = ["Fecha_Entrega", "Cliente", "Vendedor_Registro", "Estado"]
    columnas_existentes = [col for col in columnas_deseadas if col in df_to_display.columns]
    if not columnas_existentes:
        st.info("No hay columnas relevantes para mostrar.")
        return

    df_vista = df_to_display[columnas_existentes].copy()

    if "Folio_Factura" in df_to_display.columns and "Cliente" in df_to_display.columns:
        df_vista["Cliente"] = df_to_display.apply(
            lambda row: f"📄 <b>{row['Folio_Factura']}</b> 🤝 {row['Cliente']}", axis=1
        )


    df_vista = df_vista.rename(columns={
        "Fecha_Entrega": "Fecha Entrega",
        "Vendedor_Registro": "Vendedor"
    })

    if "Fecha Entrega" in df_vista.columns:
        df_vista["Fecha Entrega"] = df_vista["Fecha Entrega"].apply(
            lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
        )

    # 🔁 Ajuste inteligente: considera columnas (grupos simultáneos) y filas
    # Ajuste inteligente: considera columnas (grupos simultáneos) y filas
    # (row_height variable removed as it was unused)

    st.markdown("""
        <style>
        .dataframe {
            table-layout: fixed;
            width: 100%;
        }
        .dataframe td {
            white-space: normal !important;
            overflow-wrap: break-word;
            font-size: 0.75rem;  /* ✅ Aumentado ligeramente */
            padding: 0.1rem 0.2rem;
            height: 1rem;
            line-height: 1.2rem;
            vertical-align: top;
        }
        .dataframe th {
            font-size: 0.75rem;  /* ✅ También aumentamos encabezados */
            padding: 0.1rem 0.2rem;
            text-align: left;
        }
        </style>
    """, unsafe_allow_html=True)


    st.markdown(df_vista.to_html(escape=False, index=False), unsafe_allow_html=True)


# --- Lógica principal ---

df_all_data = load_data_from_gsheets()

if 'ID_Pedido' in df_all_data.columns:
    df_all_data['ID_Pedido'] = df_all_data['ID_Pedido'].astype(str)

if 'Adjuntos' in df_all_data.columns:
    df_all_data['Adjuntos_Enlaces'] = df_all_data['Adjuntos'].apply(
        lambda x: display_attachments(x, s3_client)
    )
if not df_all_data.empty:
    df_display_data = df_all_data.copy()
    st.caption(f"🕒 Última actualización: {datetime.now(TZ).strftime('%d/%m %H:%M:%S')}")

    # Mostrar solo completados que NO estén marcados como limpiados
    if 'Completados_Limpiado' not in df_display_data.columns:
        df_display_data['Completados_Limpiado'] = ''

    df_display_data = df_display_data[
        (df_display_data['Estado'] != '🟢 Completado') |
        ((df_display_data['Estado'] == '🟢 Completado') &
         (df_display_data['Completados_Limpiado'].astype(str).str.lower() != "sí"))
    ].copy()

    # ⬅️ Incluye también 🟣 Cancelado en la lista de estados visibles en la tabla
    df_display_data = df_display_data[df_display_data['Estado'].isin(
        ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado", "🛠 Modificación", "🟣 Cancelado", "🟢 Completado"]
    )]

    # --- Contador de estados (corrigiendo Completados limpiados) ---
    completados_visibles = df_all_data[
        (df_all_data['Estado'] == '🟢 Completado') &
        (df_all_data.get('Completados_Limpiado', '').astype(str).str.lower() != 'sí')
    ]

    # ⬅️ Agrega conteo de 🟣 Cancelado
    estado_counts = {
        '🟡 Pendiente': (df_all_data['Estado'] == '🟡 Pendiente').sum(),
        '🔵 En Proceso': (df_all_data['Estado'] == '🔵 En Proceso').sum(),
        '🔴 Demorado': (df_all_data['Estado'] == '🔴 Demorado').sum(),
        '🛠 Modificación': (df_all_data['Estado'] == '🛠 Modificación').sum(),
        '🟣 Cancelado': (df_all_data['Estado'] == '🟣 Cancelado').sum(),
        '🟢 Completado': len(completados_visibles),
    }

    # 🔄 NUEVA agrupación por tipo de envío (turno o foráneo) y fecha de entrega
    df_display_data['Fecha_Entrega_Str'] = df_display_data['Fecha_Entrega'].dt.strftime("%d/%m")
    df_display_data['Grupo_Clave'] = df_display_data.apply(
        lambda row: f"{row['Turno'] if row['Turno'] else '🌍 Foráneo'} – {row['Fecha_Entrega_Str']}", axis=1
    )

    grupos_a_mostrar = []
    grouped = df_display_data.groupby(['Grupo_Clave', 'Fecha_Entrega'])
    for (clave, _), df_grupo in sorted(grouped, key=lambda x: x[0][1]):
        if not df_grupo.empty:
            grupos_a_mostrar.append((f"{clave} ({len(df_grupo)})", df_grupo))

    # --- Mostrar resumen de estados ---
    st.markdown("#### 📊 Resumen General de Pedidos")

    # Total
    total_pedidos_estados = sum(estado_counts.values())

    # Siempre visibles
    estados_fijos = ['🟡 Pendiente', '🔵 En Proceso', '🟢 Completado']

    # Dinámicos (solo si > 0)
    estados_condicionales = ['🔴 Demorado', '🛠 Modificación', '🟣 Cancelado']

    # Construcción de métricas (sin plurales, nombres tal cual)
    estados_a_mostrar = []
    estados_a_mostrar.append(("📦 Total Pedidos", total_pedidos_estados))

    for estado in estados_fijos:
        estados_a_mostrar.append((estado, estado_counts[estado]))

    for estado in estados_condicionales:
        cantidad = estado_counts.get(estado, 0)
        if cantidad > 0:
            estados_a_mostrar.append((estado, cantidad))

    # Render de métricas
    cols = st.columns(len(estados_a_mostrar))
    for col, (nombre_estado, cantidad) in zip(cols, estados_a_mostrar):
        col.metric(nombre_estado, int(cantidad))


    # 🔽 Mostrar los grupos
    if grupos_a_mostrar:
        num_cols_per_row = 3
        for i in range(0, len(grupos_a_mostrar), num_cols_per_row):
            row = grupos_a_mostrar[i:i+num_cols_per_row]
            cols = st.columns(len(row))
            for j, (titulo, df_grupo) in enumerate(row):
                with cols[j]:
                    st.markdown(f"#### {titulo}")
                    if 'Hora_Registro' in df_grupo.columns:
                        df_grupo = df_grupo.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)

                    display_dataframe_with_formatting(df_grupo, num_columnas_actuales=len(row))
