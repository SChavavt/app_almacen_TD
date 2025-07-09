import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import os
import gspread.utils
from streamlit_autorefresh import st_autorefresh

# --- Configuración de la página ---
st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")

# 🔄 Refrescar cada 5 segundos automáticamente
st_autorefresh(interval=5 * 1000, key="datarefresh_integrated")

# Título con emoji colorido
st.markdown(
    """
    <h1 style="color: white; font-size: 2.5rem; margin-bottom: 2rem;">
        <span style="font-size: 3rem;">🏷️</span> Flujo de Pedidos en Tiempo Real (Integrado)
    </h1>
    """,
    unsafe_allow_html=True,
)

# Inyectar CSS para el word-wrap en las celdas del dataframe
st.markdown(
    """
    <style>
    .dataframe td {
        white-space: unset !important;
        word-break: break-word;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Google Sheets Configuration ---
# GOOGLE_SHEET_ID y GOOGLE_SHEET_WORKSHEET_NAME pueden venir de st.secrets si se prefiere
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

# --- AWS S3 Configuration ---
try:
    # Asegúrate de que la sección 'aws' esté en .streamlit/secrets.toml
    if "aws" not in st.secrets:
        st.error("❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [aws].")
        st.info("Falta la clave: 'st.secrets has no key \"aws\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? Más información: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
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

# --- Cached Clients for Google Sheets and AWS S3 ---

@st.cache_resource
def get_gspread_client(_credentials_json_dict):
    """
    Autentica con Google Sheets usando las credenciales de la cuenta de servicio
    y retorna un cliente de gspread.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        creds_dict = dict(_credentials_json_dict)

        # Es CRÍTICO para el error 'Incorrect padding' asegurarse de que la clave privada
        # tenga saltos de línea reales y no espacios en blanco circundantes.
        if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()

        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.info("ℹ️ Verifica que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tus credenciales de servicio en `secrets.toml` sean válidas.")
        st.stop()

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
        st.info("ℹ️ Revisa tus credenciales de AWS en `st.secrets['aws']` y la configuración de la región.")
        st.stop()

# Initialize clients globally
try:
    # Obtener credenciales de Google Sheets de st.secrets
    if "gsheets" not in st.secrets:
        st.error("❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [gsheets].")
        st.info("Falta la clave: 'st.secrets has no key \"gsheets\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? Más información: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
        st.stop()

    # Cargar las credenciales de gsheets como JSON
    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    # Asegurarse de que los saltos de línea en la clave privada sean correctos
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()

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
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud. También, revisa tus credenciales de AWS S3 y Google Sheets en `.streamlit/secrets.toml` o en la interfaz de Streamlit Cloud.")
    st.stop()


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_data(ttl=60)
def get_raw_sheet_data(sheet_id: str, worksheet_name: str, credentials: dict) -> list[list[str]]:
    """
    Lee todos los valores desde una hoja de Google Sheets.
    Se cachea porque solo recibe tipos hasheables (str, dict).
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Asegurarse de que los saltos de línea en la clave privada sean correctos
    credentials["private_key"] = credentials["private_key"].replace("\\n", "\n")
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
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
        'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Notas', 'Modificacion_Surtido',
        'Adjuntos', 'Adjuntos_Surtido', 'Estado', 'Estado_Pago', 'Fecha_Completado',
        'Hora_Proceso', 'Turno', 'Surtidor'
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


# Cargar y procesar datos de Google Sheets
raw_data_main = get_raw_sheet_data(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME, GSHEETS_CREDENTIALS)
df_pedidos, headers_main = process_sheet_data(raw_data_main)

# --- Funciones de S3 (desde app_a-d.py) ---
def find_pedido_subfolder_prefix(s3_client_param, parent_prefix, folder_name):
    if not s3_client_param:
        return None
    possible_prefixes = [
        f"{parent_prefix}{folder_name}/",
        f"{parent_prefix}{folder_name}",
        f"adjuntos_pedidos/{folder_name}/",
        f"adjuntos_pedidos/{folder_name}",
        f"{folder_name}/",
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
            continue
    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=100
        )
        if 'Contents' in response:
            for obj in response['Contents']:
                if folder_name in obj['Key']:
                    if '/' in obj['Key']:
                        prefix_parts = obj['Key'].split('/')[:-1]
                        return '/'.join(prefix_parts) + '/'
    except Exception:
        pass
    return None

def get_files_in_s3_prefix(s3_client_param, prefix):
    if not s3_client_param or not prefix:
        return []
    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=100
        )
        files = []
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'):
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
    if not s3_client_param or not object_key:
        return "#"
    try:
        url = s3_client_param.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=7200
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para '{object_key}': {e}")
        return "#"

# --- Helper Functions from app_almacen.py (adapted for GSheets data) ---
def formatear_fecha_consistente(fecha_str):
    """Convierte cualquier formato de fecha al formato dd/mm/yyyy"""
    if pd.isna(fecha_str) or str(fecha_str).strip() == "Sin fecha" or str(fecha_str).strip() == "":
        return "Sin fecha"
    try:
        # Intentar parsear como datetime si es posible, luego formatear
        if isinstance(fecha_str, datetime):
            return fecha_str.strftime('%d/%m/%Y')
        # Intentar como fecha de Google Sheets (YYYY-MM-DD)
        dt_obj = datetime.strptime(str(fecha_str).split(" ")[0], '%Y-%m-%d')
        return dt_obj.strftime('%d/%m/%Y')
    except ValueError:
        try:
            # Intentar otro formato común dd/mm/yyyy
            dt_obj = datetime.strptime(str(fecha_str).split(" ")[0], '%d/%m/%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            return str(fecha_str) # Retorna el original si no se puede parsear

def get_attachments_for_pedido(id_pedido_str, s3_client_param):
    """
    Busca adjuntos en S3 para un ID de pedido dado.
    """
    if not id_pedido_str:
        return []

    # Se usará la lógica de find_pedido_subfolder_prefix para determinar si existe una carpeta.
    # En app_a.py esta función no está directamente llamada, pero es la lógica detrás.
    # Aquí simulamos su uso para obtener el prefijo correcto.
    pedido_prefix = find_pedido_subfolder_prefix(s3_client_param, S3_ATTACHMENT_PREFIX, id_pedido_str)
    
    if pedido_prefix:
        return get_files_in_s3_prefix(s3_client_param, pedido_prefix)
    return []

# --- Funciones de actualización a Google Sheets (ajustadas para _gsheet_row_index) ---
def update_gsheet_cell(worksheet, headers, row_index, col_name, value):
    """
    Actualiza una celda específica en Google Sheets.
    `row_index` es el índice de fila de gspread (base 1).
    `col_name` es el nombre de la columna.
    `headers` es la lista de encabezados obtenida previamente.
    """
    try:
        if col_name not in headers:
            st.error(f"❌ Error: La columna '{col_name}' no se encontró en Google Sheets para la actualización. Verifica los encabezados.")
            return False
        col_index = headers.index(col_name) + 1 # Convertir a índice base 1 de gspread
        worksheet.update_cell(row_index, col_index, value)
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la celda ({row_index}, {col_name}) en Google Sheets: {e}")
        return False

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
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False

# --- Filtros y visualización (similar a app_a-d.py) ---
st.markdown("### Filtros")
col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])

estado_filtro = col1.selectbox(
    "Filtrar por Estado",
    options=["Todos", "🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado", "🟢 Completado", "⚫ Cancelado"],
    key="estado_filtro"
)
tipo_envio_filtro = col2.selectbox(
    "Filtrar por Tipo de Envío",
    options=["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "📦 Paquetería"],
    key="tipo_envio_filtro"
)

# Filtro por rango de fechas (Fecha_Entrega)
col_date_start, col_date_end = st.columns(2)
start_date = col_date_start.date_input("Fecha de Entrega (Desde)", value=None, key="start_date_filter")
end_date = col_date_end.date_input("Fecha de Entrega (Hasta)", value=None, key="end_date_filter")

# Asegurar que las fechas sean objetos datetime.date para comparación
if start_date:
    start_date = datetime.combine(start_date, datetime.min.time()).date()
if end_date:
    end_date = datetime.combine(end_date, datetime.max.time()).date() # Incluir todo el día final

df_filtrado = df_pedidos.copy()

if estado_filtro != "Todos":
    df_filtrado = df_filtrado[df_filtrado["Estado"] == estado_filtro]

if tipo_envio_filtro != "Todos":
    df_filtrado = df_filtrado[df_filtrado["Tipo_Envio"] == tipo_envio_filtro]

if start_date:
    df_filtrado['Fecha_Entrega_dt'] = pd.to_datetime(df_filtrado['Fecha_Entrega'], errors='coerce').dt.date
    df_filtrado = df_filtrado[df_filtrado['Fecha_Entrega_dt'] >= start_date]

if end_date:
    df_filtrado['Fecha_Entrega_dt'] = pd.to_datetime(df_filtrado['Fecha_Entrega'], errors='coerce').dt.date
    df_filtrado = df_filtrado[df_filtrado['Fecha_Entrega_dt'] <= end_date]

# Eliminar columna temporal de fecha
if 'Fecha_Entrega_dt' in df_filtrado.columns:
    df_filtrado = df_filtrado.drop(columns=['Fecha_Entrega_dt'])

st.markdown("---")

# --- Display Data ---
if not df_filtrado.empty:
    # Ordenar por Hora_Registro más reciente primero
    df_filtrado['Hora_Registro_dt'] = pd.to_datetime(df_filtrado['Hora_Registro'], errors='coerce')
    df_filtrado = df_filtrado.sort_values(by="Hora_Registro_dt", ascending=False).reset_index(drop=True)
    df_filtrado = df_filtrado.drop(columns=['Hora_Registro_dt'])

    st.subheader("📊 Pedidos Filtrados")
    st.dataframe(
        df_filtrado[[
            'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
            'Tipo_Envio', 'Fecha_Entrega', 'Fecha_Completado', 'Notas', 'Modificacion_Surtido',
            'Adjuntos', 'Adjuntos_Surtido', 'Turno'
        ]],
        use_container_width=True,
        hide_index=True
    )

    st.markdown("---")
    st.subheader("🔍 Detalles del Pedido y Acciones")

    # Selección de pedido para ver detalles
    pedido_ids = df_filtrado['ID_Pedido'].tolist()
    selected_pedido_id = st.selectbox("Seleccionar Pedido para Detalles/Acciones", [""] + pedido_ids, key="select_pedido_detail")

    if selected_pedido_id:
        selected_pedido = df_filtrado[df_filtrado['ID_Pedido'] == selected_pedido_id].iloc[0]
        gsheet_row_index = selected_pedido.get('_gsheet_row_index')

        if gsheet_row_index is None:
            st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{selected_pedido_id}'.")
        else:
            with st.expander(f"Detalles del Pedido: {selected_pedido_id} - {selected_pedido['Cliente']}"):
                col_info_1, col_info_2 = st.columns(2)
                col_info_1.write(f"**ID Pedido:** {selected_pedido['ID_Pedido']}")
                col_info_1.write(f"**Cliente:** {selected_pedido['Cliente']}")
                col_info_1.write(f"**Estado:** {selected_pedido['Estado']}")
                col_info_1.write(f"**Tipo Envío:** {selected_pedido['Tipo_Envio']}")
                col_info_2.write(f"**Vendedor Registro:** {selected_pedido['Vendedor_Registro']}")
                col_info_2.write(f"**Fecha Entrega:** {formatear_fecha_consistente(selected_pedido['Fecha_Entrega'])}")
                col_info_2.write(f"**Fecha Completado:** {formatear_fecha_consistente(selected_pedido['Fecha_Completado'])}")
                col_info_2.write(f"**Turno:** {selected_pedido['Turno']}")

                st.markdown("---")
                st.write("**Notas:**")
                st.info(selected_pedido['Notas'] if selected_pedido['Notas'] else "Sin notas adicionales.")
                
                if selected_pedido.get("Modificacion_Surtido"):
                    st.warning(f"**Modificación Surtido:** {selected_pedido['Modificacion_Surtido']}")

                st.markdown("---")
                st.subheader("Adjuntos del Pedido")
                adjuntos = get_attachments_for_pedido(selected_pedido_id, s3_client)
                if adjuntos:
                    for i, file_info in enumerate(adjuntos):
                        file_url = get_s3_file_download_url(s3_client, file_info['key'])
                        st.markdown(f"- [{file_info['title']}]({file_url})")
                else:
                    st.info("No hay adjuntos para este pedido.")

                st.markdown("---")
                st.subheader("Acciones del Pedido")

                # Actualizar estado a "En Proceso"
                if selected_pedido['Estado'] == "🟡 Pendiente":
                    if st.button("🔵 Marcar como 'En Proceso'", key=f"btn_in_process_{selected_pedido_id}"):
                        if update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Estado", "🔵 En Proceso"):
                            update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Hora_Proceso", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            st.success(f"Pedido {selected_pedido_id} marcado como 'En Proceso'.")
                            st.rerun()
                        else:
                            st.error("❌ Falló la actualización a 'En Proceso'.")

                # Actualizar estado a "Completado"
                if selected_pedido['Estado'] in ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"]:
                    if st.button("🟢 Marcar como 'Completado'", key=f"btn_completed_{selected_pedido_id}"):
                        if update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Estado", "🟢 Completado"):
                            update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Fecha_Completado", datetime.now().strftime('%Y-%m-%d'))
                            st.success(f"Pedido {selected_pedido_id} marcado como 'Completado'.")
                            st.rerun()
                        else:
                            st.error("❌ Falló la actualización a 'Completado'.")
                
                # Actualizar estado a "Cancelado"
                if selected_pedido['Estado'] != "⚫ Cancelado":
                    if st.button("⚫ Marcar como 'Cancelado'", key=f"btn_canceled_{selected_pedido_id}"):
                        if st.warning("¿Estás seguro de que quieres cancelar este pedido?"):
                            if st.button("Confirmar Cancelación", key=f"confirm_cancel_{selected_pedido_id}"):
                                if update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Estado", "⚫ Cancelado"):
                                    st.success(f"Pedido {selected_pedido_id} marcado como 'Cancelado'.")
                                    st.rerun()
                                else:
                                    st.error("❌ Falló la actualización a 'Cancelado'.")

                # Campo para actualizar el surtidor
                surtidor_current = selected_pedido.get("Surtidor", "")
                new_surtidor = st.text_input(
                    "Actualizar Surtidor", 
                    value=surtidor_current, 
                    key=f"surtidor_input_{selected_pedido_id}"
                )
                if st.button("Guardar Surtidor", key=f"save_surtidor_{selected_pedido_id}"):
                    if new_surtidor != surtidor_current:
                        if update_gsheet_cell(worksheet_main, headers_main, gsheet_row_index, "Surtidor", new_surtidor):
                            st.success(f"Surtidor actualizado a '{new_surtidor}' para el pedido {selected_pedido_id}.")
                            st.rerun()
                        else:
                            st.error("❌ Falló la actualización del surtidor.")
                    else:
                        st.info("No hay cambios en el surtidor para guardar.")

else:
    st.info("No hay pedidos para mostrar según los criterios de filtro.")

if __name__ == '__main__':
    # Esto asegura que el código principal de Streamlit se ejecute.
    # El resto del script define funciones y lógica que Streamlit usa directamente.
    pass
