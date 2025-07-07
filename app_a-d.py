import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils

st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")


st.title("📬 Bandeja de Pedidos TD")

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
if "expanded_attachments" not in st.session_state:
    st.session_state["expanded_attachments"] = {}


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

        # Ensure private_key has actual newlines and no surrounding whitespace.
        # This is CRITICAL for 'Incorrect padding' error.
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
        st.info("Falta la clave: 'st.secrets has no key \"gsheets\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud? More info: https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management'")
        st.stop()

    import json

    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
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
        # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
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
            # st.cache_data.clear() # Limpiar solo si hay un cambio que justifique una recarga completa
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False

# --- AWS S3 Helper Functions (Copied from app_admin.py directly) ---

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
            MaxKeys=100 # Revisar si esto es suficiente o si se necesita paginación
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

# --- Helper Functions (existentes en app.py) ---

def ordenar_pedidos_custom(df_pedidos_filtrados):
    """
    Ordena el DataFrame filtrado con 'Demorado' al principio,
    luego el orden original para 'Pendiente'/'En proceso', y 'Completado' al final.
    """
    if df_pedidos_filtrados.empty:
        return df_pedidos_filtrados

    # Asegurarse de que 'Hora_Registro' sea datetime para el ordenamiento
    df_pedidos_filtrados['Hora_Registro_dt'] = pd.to_datetime(df_pedidos_filtrados['Hora_Registro'], errors='coerce')

    def get_sort_key(row):
        if row["Estado"] == "🔴 Demorado":
            return 0, row['Hora_Registro_dt'] # Mayor prioridad, luego por hora
        elif row["Estado"] in ["🟡 Pendiente", "🔵 En Proceso"]:
            return 1, row['Hora_Registro_dt'] # Prioridad media, mantiene el orden de llegada
        elif row["Estado"] == "🟢 Completado":
            # Para completados, ordenar por Fecha_Completado descendente
            # Si no hay Fecha_Completado, usar Hora_Registro_dt
            fecha_orden = row['Fecha_Completado'] if pd.notna(row['Fecha_Completado']) else row['Hora_Registro_dt']
            return 2, fecha_orden # Menor prioridad, al final, ordenado por fecha de completado

        return 3, row['Hora_Registro_dt'] # Para cualquier otro estado desconocido, al final

    df_pedidos_filtrados['custom_sort_key'] = df_pedidos_filtrados.apply(get_sort_key, axis=1)

    # Ordenar primero por la clave personalizada (ascendente), luego por el segundo elemento de la tupla (fecha)
    # Si la clave es 2 (Completado/Cancelado), el segundo elemento se ordena descendente.
    df_sorted = df_pedidos_filtrados.sort_values(
        by=['custom_sort_key'],
        ascending=[True]
    )

    df_sorted = df_sorted.drop(columns=['custom_sort_key', 'Hora_Registro_dt'])
    return df_sorted

def check_and_update_demorados(df_to_check, worksheet, headers):
    """
    Checks for orders in 'En Proceso' status that have exceeded 1 hour and
    updates their status to 'Demorado' in the DataFrame and Google Sheets.
    Utiliza actualización por lotes para mayor eficiencia.
    """
    updates_to_perform = []
    current_time = datetime.now()
    one_hour_ago = current_time - timedelta(hours=1)

    try:
        estado_col_index = headers.index('Estado') + 1
        headers.index('Hora_Proceso') + 1 # Obtener índice de columna Hora_Proceso
    except ValueError:
        st.error("❌ Error interno: Columna 'Estado' o 'Hora_Proceso' no encontrada en los encabezados de Google Sheets.")
        return df_to_check, False

    changes_made = False # Flag para indicar si hubo cambios en los estados

    for idx, row in df_to_check.iterrows():
        if row['Estado'] == "🔵 En Proceso" and pd.notna(row['Hora_Proceso']):
            hora_proceso_dt = pd.to_datetime(row['Hora_Proceso'], errors='coerce')

            if pd.notna(hora_proceso_dt) and hora_proceso_dt < one_hour_ago:
                gsheet_row_index = row.get('_gsheet_row_index')

                if gsheet_row_index is not None:
                    # Preparar actualización a "🔴 Demorado"
                    updates_to_perform.append({
                        'range': f"{gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_index)}",
                        'values': [["🔴 Demorado"]]
                    })
                    # Actualizar DataFrame en memoria
                    df_to_check.loc[idx, "Estado"] = "🔴 Demorado"
                    changes_made = True
                else:
                    st.warning(f"⚠️ ID_Pedido '{row['ID_Pedido']}' no tiene '_gsheet_row_index'. No se pudo actualizar el estado a 'Demorado'.")

    if updates_to_perform:
        if batch_update_gsheet_cells(worksheet, updates_to_perform):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            # st.cache_data.clear() # Limpiar la caché de datos para que se recargue si es necesario
            return df_to_check, changes_made
        else:
            st.error("Falló la actualización por lotes de estados 'Demorado'.")
            return df_to_check, False
    
    return df_to_check, False # No hubo actualizaciones

def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers, s3_client_param):
    gsheet_row_index = row.get('_gsheet_row_index')
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{row['ID_Pedido']}'. No se puede actualizar este pedido.")
        return

    safe_fecha = str(row.get("Fecha_Entrega", "")).replace("-", "_").replace("/", "_")
    widget_key_base = f"{row['ID_Pedido']}_{origen_tab}_{safe_fecha}_{orden}"

    with st.container():
        st.markdown("---")
        tiene_modificacion = row.get("Modificacion_Surtido") and pd.notna(row["Modificacion_Surtido"]) and str(row["Modificacion_Surtido"]).strip() != ''
        if tiene_modificacion:
            st.warning(f"⚠ ¡MODIFICACIÓN DE SURTIDO DETECTADA! Pedido #{orden}")

        if row['Estado'] != "🟢 Completado" and row.get("Tipo_Envio") in ["📍 Pedido Local", "🚚 Pedido Foráneo"]:
            st.markdown("##### 📅 Cambiar Fecha y Turno")
            col_current_info_date, col_current_info_turno, col_inputs = st.columns([1, 1, 2])

            fecha_actual_str = row.get("Fecha_Entrega", "")
            fecha_actual_dt = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            fecha_mostrar = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else "Sin fecha"
            col_current_info_date.info(f"**Fecha de envío actual:** {fecha_mostrar}")

            current_turno = row.get("Turno", "")
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                col_current_info_turno.info(f"**Turno actual:** {current_turno}")
            else:
                col_current_info_turno.empty()

            today = datetime.now().date()
            date_input_value = today
            if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today:
                date_input_value = fecha_actual_dt.date()

            new_fecha_entrega_dt = col_inputs.date_input(
                "Nueva fecha de envío:",
                value=date_input_value,
                key=f"new_date_{widget_key_base}",
                disabled=(row['Estado'] == "🟢 Completado")
            )

            new_turno = current_turno

            if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                turno_options = ["", "☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"]
                try:
                    default_index_turno = turno_options.index(current_turno)
                except ValueError:
                    default_index_turno = 0

                new_turno = col_inputs.selectbox(
                    "Clasificar Turno como:",
                    options=turno_options,
                    index=default_index_turno,
                    key=f"new_turno_{widget_key_base}",
                    disabled=(row['Estado'] == "🟢 Completado")
                )

            if st.button("✅ Aplicar Cambios de Fecha/Turno", key=f"apply_changes_{widget_key_base}", disabled=(row['Estado'] == "🟢 Completado")):
                changes_made = False
                updates_to_gsheet = []

                new_fecha_entrega_str = new_fecha_entrega_dt.strftime('%Y-%m-%d')
                if new_fecha_entrega_str != fecha_actual_str:
                    try:
                        col_idx_fecha = headers.index("Fecha_Entrega") + 1
                        updates_to_gsheet.append({
                            'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx_fecha),
                            'values': [[new_fecha_entrega_str]]
                        })
                        df.loc[idx, "Fecha_Entrega"] = new_fecha_entrega_str
                        changes_made = True
                    except ValueError:
                        st.error("Error interno: Columna 'Fecha_Entrega' no encontrada.")

                if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"] and new_turno != current_turno:
                    try:
                        col_idx_turno = headers.index("Turno") + 1
                        updates_to_gsheet.append({
                            'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx_turno),
                            'values': [[new_turno]]
                        })
                        df.loc[idx, "Turno"] = new_turno
                        changes_made = True
                    except ValueError:
                        st.error("Error interno: Columna 'Turno' no encontrada.")

                if updates_to_gsheet:
                    if batch_update_gsheet_cells(worksheet, updates_to_gsheet):
                        st.success(f"✅ Cambios aplicados para el pedido {row['ID_Pedido']}!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Falló la aplicación de cambios de fecha/turno.")
                elif changes_made:
                    st.error("No se pudieron preparar las actualizaciones para Google Sheets.")
                else:
                    st.info("No se realizaron cambios en la fecha o turno.")

        col_order_num, col_client, col_time, col_status, col_surtidor, col_print_btn, col_complete_btn = st.columns([0.5, 2, 1.5, 1, 1.2, 1, 1])

        col_order_num.write(f"**{orden}**")
        col_client.write(f"**{row['Cliente']}**")

        hora_registro_dt = pd.to_datetime(row['Hora_Registro'], errors='coerce')
        if pd.notna(hora_registro_dt):
            col_time.write(f"🕒 {hora_registro_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            col_time.write("")

        col_status.write(f"{row['Estado']}")

        surtidor_current = row.get("Surtidor", "")

        def update_surtidor_callback(current_idx, current_gsheet_row_index, current_surtidor_key, df_param):
            new_surtidor_val = st.session_state[current_surtidor_key]
            if new_surtidor_val != surtidor_current:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Surtidor", new_surtidor_val):
                    df_param.loc[current_idx, "Surtidor"] = new_surtidor_val
                    st.toast("Surtidor actualizado", icon="✅")
                else:
                    st.error("Falló la actualización del surtidor.")

        surtidor_key = f"surtidor_{widget_key_base}"
        col_surtidor.text_input(
            "Surtidor",
            value=surtidor_current,
            label_visibility="collapsed",
            placeholder="Surtidor",
            key=surtidor_key,
            disabled=(row['Estado'] == "🟢 Completado"),
            on_change=update_surtidor_callback,
            args=(idx, gsheet_row_index, surtidor_key, df)
        )

        if col_print_btn.button("🖨 Imprimir", key=f"print_button_{widget_key_base}", disabled=(row['Estado'] == "🟢 Completado")):
            st.write("Aquí iría la lógica para imprimir el pedido.")

        if col_complete_btn.button("🟢 Completar", key=f"done_button_{widget_key_base}", disabled=(row['Estado'] == "🟢 Completado")):
            st.write("Aquí iría la lógica para completar el pedido.")

        current_notas = row.get("Notas", "")

        def update_notas_callback(current_idx, current_gsheet_row_index, current_notas_key, df_param):
            new_notas_val = st.session_state[current_notas_key]
            if new_notas_val != current_notas:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Notas", new_notas_val):
                    df_param.loc[current_idx, "Notas"] = new_notas_val
                    st.toast("Notas actualizadas", icon="✅")
                else:
                    st.error("Falló la actualización de las notas.")

        notas_key = f"notas_edit_{widget_key_base}"
        st.text_area(
            "📝 Notas (editable)",
            value=current_notas,
            key=notas_key,
            height=70,
            disabled=(row['Estado'] == "🟢 Completado"),
            on_change=update_notas_callback,
            args=(idx, gsheet_row_index, notas_key, df)
        )


# --- Main Application Logic ---

raw_data = get_raw_sheet_data(
    sheet_id=GOOGLE_SHEET_ID,
    worksheet_name=GOOGLE_SHEET_WORKSHEET_NAME,
    credentials=GSHEETS_CREDENTIALS
)
df_main, headers_main = process_sheet_data(raw_data)


if not df_main.empty:
    df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
    if changes_made_by_demorado_check:
        st.cache_data.clear() # Limpiar caché para forzar recarga si hay cambios de estado
        st.rerun() # Se mantiene para asegurar que los pedidos demorados se muevan de inmediato

    df_pendientes_proceso_demorado = df_main[df_main["Estado"].isin(["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"])].copy()
    df_completados_historial = df_main[df_main["Estado"] == "🟢 Completado"].copy()

    st.markdown("### 📊 Resumen de Estados")

    estado_counts = df_main['Estado'].astype(str).value_counts().reindex([
        '🟡 Pendiente', '🔵 En Proceso', '🔴 Demorado', '🟢 Completado'
    ], fill_value=0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🟡 Pendientes", estado_counts.get('🟡 Pendiente', 0))
    col2.metric("🔵 En Proceso", estado_counts.get('🔵 En Proceso', 0))
    col3.metric("🔴 Demorados", estado_counts.get('🔴 Demorado', 0))
    col4.metric("🟢 Completados", estado_counts.get('🟢 Completado', 0))

    # --- Implementación de Pestañas con st.tabs ---
    tab_options = ["📍 Pedidos Locales", "🚚 Pedidos Foráneos", "🛠 Garantías", "🔁 Devoluciones", "📬 Solicitud de Guía", "✅ Historial Completados"]

    main_tabs = st.tabs(tab_options)

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
                        date_label = f"📅 {fecha_dt.strftime('%d/%m/%Y')}"
                        with date_tabs_m[i]:
                            pedidos_fecha = pedidos_m_display[pedidos_m_display["Fecha_Entrega_dt"] == fecha_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌅 Pedidos Locales - Mañana - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Mañana", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)


                            current_selected_date_dt_str = date_label.replace("📅 ", "")

                            current_selected_date_dt = pd.to_datetime(current_selected_date_dt_str, format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_m_display[pedidos_m_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌅 Pedidos Locales - Mañana - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Mañana", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
                else:
                    st.info("No hay pedidos para el turno mañana.")
            else:
                st.info("No hay pedidos para el turno mañana.")

        with subtabs_local[1]: # 🌇 Tarde
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
                    for i, date_label in enumerate(date_tabs_t):
                        with date_tabs_t[i]:
                            current_selected_date_dt_str = date_label.replace("📅 ", "")
                            current_selected_date_dt = pd.to_datetime(current_selected_date_dt_str, format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_t_display[pedidos_t_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌇 Pedidos Locales - Tarde - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Tarde", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
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
                    mostrar_pedido(df_main, idx, row, orden, "Saltillo", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
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
                    mostrar_pedido(df_main, idx, row, orden, "Pasa a Bodega", "📍 Pedidos Locales", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos para pasar a bodega.")

    with main_tabs[1]: # 🚚 Pedidos Foráneos
        pedidos_foraneos_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🚚 Pedido Foráneo")
        ].copy()
        if not pedidos_foraneos_display.empty:
            pedidos_foraneos_display = ordenar_pedidos_custom(pedidos_foraneos_display)
            for orden, (idx, row) in enumerate(pedidos_foraneos_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Foráneo", "🚚 Pedidos Foráneos", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos foráneos.")

    with main_tabs[2]: # 🛠 Garantías
        garantias_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🛠 Garantía")].copy()
        if not garantias_display.empty:
            garantias_display = ordenar_pedidos_custom(garantias_display)
            for orden, (idx, row) in enumerate(garantias_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Garantía", "🛠 Garantías", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay garantías.")

    with main_tabs[3]: # 🔁 Devoluciones
        devoluciones_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🔁 Devolución")].copy()
        if not devoluciones_display.empty:
            devoluciones_display = ordenar_pedidos_custom(devoluciones_display)
            for orden, (idx, row) in enumerate(devoluciones_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Devolución", "🔁 Devoluciones", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay devoluciones.")

    with main_tabs[4]: # 📬 Solicitud de Guía
        solicitudes_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "📬 Solicitud de guía")].copy()
        if not solicitudes_display.empty:
            solicitudes_display = ordenar_pedidos_custom(solicitudes_display)
            for orden, (idx, row) in enumerate(solicitudes_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Solicitud de Guía", "📬 Solicitud de Guía", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay solicitudes de guía.")

    with main_tabs[5]: # ✅ Historial Completados
        st.markdown("### Historial de Pedidos Completados")
        if not df_completados_historial.empty:
            # Ordenar por Fecha_Completado en orden descendente para mostrar los más recientes
            df_completados_historial['Fecha_Completado_dt'] = pd.to_datetime(df_completados_historial['Fecha_Completado'], errors='coerce')
            df_completados_historial = df_completados_historial.sort_values(by="Fecha_Completado_dt", ascending=False).reset_index(drop=True)

            st.dataframe(
                df_completados_historial[[
                    'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
                    'Tipo_Envio', 'Fecha_Entrega', 'Fecha_Completado', 'Notas', 'Modificacion_Surtido',
                    'Adjuntos', 'Adjuntos_Surtido', 'Turno'
                ]].head(50), # Mostrar los 50 más recientes
                use_container_width=True, hide_index=True
            )
            st.info("Mostrando los 50 pedidos completados más recientes. Puedes ajustar este límite si es necesario.")
        else:
            st.info("No hay pedidos completados en el historial.")

else:
    st.info("No se encontraron datos de pedidos en la hoja de Google Sheets. Asegúrate de que los datos se están subiendo correctamente desde la aplicación de Vendedores.")
