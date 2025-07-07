# app_a.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import re
import gspread.utils
# from streamlit_autorefresh import st_autorefresh # Línea comentada/eliminada


st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

st.title("📬 Bandeja de Pedidos TD")

# 🔄 La recarga automática cada 5 segundos ha sido eliminada por tu solicitud.
# st_autorefresh(interval=5 * 1000, key="datarefresh_app_a") # Línea comentada/eliminada

# --- Google Sheets Configuration ---
try:
    # Lee las credenciales de gsheets desde st.secrets
    GSHEETS_CREDENTIALS = st.secrets["gsheets"]
    GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
    GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [gsheets]. Falta la clave: {e}")
    st.stop()


# --- AWS S3 Configuration ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws"]["aws_region"]
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [aws]. Falta la clave: {e}")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- Initialize Session State for tab persistence ---
# Con st.tabs, Streamlit maneja la persistencia por sí mismo en gran medida,
# pero aún podemos usar session_state para controlar el índice inicial o recalcularlo si es necesario.
if "active_main_tab_index" not in st.session_state:
    st.session_state["active_main_tab_index"] = 0 # Default to the first tab

if "active_subtab_local_index" not in st.session_state:
    st.session_state["active_subtab_local_index"] = 0

if "active_date_tab_m_index" not in st.session_state:
    st.session_state["active_date_tab_m_index"] = 0 # Será dinámico

if "active_date_tab_t_index" not in st.session_state:
    st.session_state["active_date_tab_t_index"] = 0 # Será dinámico

if "expanded_attachments" not in st.session_state:
    st.session_state["expanded_attachments"] = {}


# --- Cached Clients for Google Sheets and AWS S3 ---

# Eliminamos la función load_credentials_from_file ya que ahora leeremos de st.secrets

@st.cache_resource
def get_gspread_client(credentials_json_dict):
    """
    Autentica con Google Sheets usando las credenciales de la cuenta de servicio
    y retorna un cliente de gspread, compatible con oauth2client.
    Este método es idéntico al usado en app_admin.py.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        # Pasa el diccionario de credenciales directamente
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.info("ℹ️ Verifica que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tus credenciales de servicio sean válidas.")
        st.stop()

@st.cache_resource
def get_s3_client():
    """
    Inicializa y retorna un cliente de S3, usando credenciales globales.
    Este método es idéntico al usado en app_admin.py.
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
    # Usa las credenciales directamente de st.secrets para Google Sheets
    g_spread_client = get_gspread_client(GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()
except Exception as e:
    st.error(f"❌ Error general al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas. También, revisa tus credenciales de AWS S3 y Google Sheets en `.streamlit/secrets.toml`.")
    st.stop()


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_resource(ttl=60) # Carga cada 60 segundos o cuando se invalide la caché
def load_data_from_gsheets(sheet_id, worksheet_name):
    """
    Carga todos los datos de una hoja de cálculo de Google Sheets en un DataFrame de Pandas
    y añade el índice de fila de la hoja de cálculo.
    Retorna el DataFrame, el objeto worksheet y los encabezados.
    """
    try:
        spreadsheet = g_spread_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Obtener todos los valores incluyendo los encabezados para poder calcular el índice de fila
        all_data = worksheet.get_all_values()
        if not all_data:
            return pd.DataFrame(), worksheet, [] # Devolver también los encabezados vacíos

        headers = all_data[0]
        data_rows = all_data[1:]

        df = pd.DataFrame(data_rows, columns=headers)

        # Añadir el índice de fila de Google Sheet (basado en 1)
        # Asumiendo que el encabezado está en la fila 1, la primera fila de datos es la fila 2.
        df['_gsheet_row_index'] = df.index + 2

        # Define las columnas esperadas y asegúrate de que existan
        expected_columns = [
            'ID_Pedido', 'Folio_Factura', 'Hora_Registro', 'Vendedor_Registro', 'Cliente',
            'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Notas', 'Modificacion_Surtido',
            'Adjuntos', 'Adjuntos_Surtido', 'Estado', 'Estado_Pago', 'Fecha_Completado',
            'Hora_Proceso', 'Turno', 'Surtidor'
        ]

        for col in expected_columns:
            if col not in df.columns:
                df[col] = '' # Inicializa columnas faltantes como cadena vacía

        # Asegura que las columnas de fecha/hora se manejen correctamente
        df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(
            lambda x: str(x) if pd.notna(x) and str(x).strip() != '' else ''
        )

        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce') # Ensure Hora_Proceso is datetime

        # IMPORTANT: Strip whitespace from key columns to ensure correct filtering and finding
        df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
        df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
        df['Turno'] = df['Turno'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()

        return df, worksheet, headers # Devolver también los encabezados

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Error: La hoja de cálculo con ID '{sheet_id}' no se encontró. Verifica el ID.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error: La pestaña '{worksheet_name}' no se encontró en la hoja de cálculo. Verifica el nombre de la pestaña.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar los datos desde Google Sheets: {e}")
        st.stop()

# --- Data Saving/Updating to Google Sheets ---

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
        # st.cache_resource.clear() # ELIMINADO para evitar recargas constantes
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
            # st.cache_resource.clear() # ELIMINADO para evitar recargas constantes
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

def check_and_update_demorados(df_to_check, worksheet, headers): # Añadir 'headers'
    """
    Checks for orders in 'En Proceso' status that have exceeded 1 hour and
    updates their status to 'Demorado' in the DataFrame and Google Sheets.
    Utiliza actualización por lotes para mayor eficiencia.
    """
    updates_to_perform = []
    updated_indices_df = []
    current_time = datetime.now()
    one_hour_ago = current_time - timedelta(hours=1)

    try:
        estado_col_index = headers.index('Estado') + 1
    except ValueError:
        st.error("❌ Error interno: Columna 'Estado' no encontrada en los encabezados de Google Sheets.")
        return df_to_check, False
    
    for idx, row in df_to_check.iterrows():
        if row['Estado'] == "🔵 En Proceso" and pd.notna(row['Hora_Proceso']):
            hora_proceso_dt = pd.to_datetime(row['Hora_Proceso'], errors='coerce')

            if pd.notna(hora_proceso_dt) and hora_proceso_dt < one_hour_ago:
                gsheet_row_index = row.get('_gsheet_row_index') # Usar el índice pre-calculado

                if gsheet_row_index is not None:
                    updates_to_perform.append({
                        'range': f"{gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_index)}",
                        'values': [["🔴 Demorado"]]
                    })
                    df_to_check.loc[idx, "Estado"] = "🔴 Demorado"
                    updated_indices_df.append(idx)
                else:
                    st.warning(f"⚠️ ID_Pedido '{row['ID_Pedido']}' no tiene '_gsheet_row_index' o no se encontró en Google Sheets. No se pudo actualizar el estado a 'Demorado'.")

    if updates_to_perform:
        if batch_update_gsheet_cells(worksheet, updates_to_perform):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            # st.cache_resource.clear() # ELIMINADO para evitar recargas constantes, se mantiene df_to_check en memoria
            return df_to_check, True
        else:
            st.error("Falló la actualización por lotes de estados 'Demorado'.")
            return df_to_check, False

    return df_to_check, False

def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers): # Añadir 'headers'
    """
    Muestra los detalles de un pedido y permite acciones.
    """
    # Initialize pedido_folder_prefix to None to prevent UnboundLocalError
    pedido_folder_prefix = None

    gsheet_row_index = row.get('_gsheet_row_index') # Obtener el índice de fila de GSheet del DataFrame
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{row['ID_Pedido']}'. No se puede actualizar este pedido.")
        return

    with st.container():
        st.markdown("---")
        tiene_modificacion = row.get("Modificacion_Surtido") and pd.notna(row["Modificacion_Surtido"]) and str(row["Modificacion_Surtido"]).strip() != ''
        if tiene_modificacion:
            st.warning(f"⚠ ¡MODIFICACIÓN DE SURTIDO DETECTADA! Pedido #{orden}")

        # --- Sección "Cambiar Fecha y Turno" ---
        # Se muestra si el estado no es Completado Y (es Pedido Local O es Pedido Foráneo)
        if row['Estado'] != "🟢 Completado" and \
           (row.get("Tipo_Envio") == "📍 Pedido Local" or row.get("Tipo_Envio") == "🚚 Pedido Foráneo"):
            st.markdown("##### 📅 Cambiar Fecha y Turno")
            col_current_info_date, col_current_info_turno, col_inputs = st.columns([1, 1, 2])

            fecha_actual_str = row.get("Fecha_Entrega", "")
            fecha_actual_dt = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            fecha_mostrar = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else "Sin fecha"
            col_current_info_date.info(f"**Fecha de envío actual:** {fecha_mostrar}")

            # Mostrar el turno actual solo si es un Pedido Local
            current_turno = row.get("Turno", "") # Obtener el turno actual para uso posterior
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                col_current_info_turno.info(f"**Turno actual:** {current_turno}")
            else: # Para foráneos, esta columna no es relevante para el "turno"
                col_current_info_turno.empty() # O podrías poner un mensaje como "No aplica"


            today = datetime.now().date()
            date_input_value = today
            if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today:
                date_input_value = fecha_actual_dt.date()

            new_fecha_entrega_dt = col_inputs.date_input(
                "Nueva fecha de envío:",
                value=date_input_value,
                key=f"new_date_{row['ID_Pedido']}_{origen_tab}",
                disabled=(row['Estado'] == "🟢 Completado")
            )

            # Inicializar new_turno con el valor actual por defecto
            new_turno = current_turno

            # Mostrar el selector de turno solo para Pedidos Locales (Mañana/Tarde)
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
                    key=f"new_turno_{row['ID_Pedido']}_{origen_tab}",
                    disabled=(row['Estado'] == "🟢 Completado")
                )
            # Para Saltillo, Pasa a Bodega, y Foráneos, el new_turno ya se inicializó con el current_turno
            # y no se mostrará un selectbox para modificarlo.

            if st.button("✅ Aplicar Cambios de Fecha/Turno", key=f"apply_changes_{row['ID_Pedido']}_{origen_tab}", disabled=(row['Estado'] == "🟢 Completado")):
                changes_made = False

                new_fecha_entrega_str = new_fecha_entrega_dt.strftime('%Y-%m-%d')
                if new_fecha_entrega_str != fecha_actual_str:
                    if update_gsheet_cell(worksheet, headers, gsheet_row_index, "Fecha_Entrega", new_fecha_entrega_str):
                        df.loc[idx, "Fecha_Entrega"] = new_fecha_entrega_str # Actualizar DataFrame en memoria
                        changes_made = True
                    else:
                        st.error("Falló la actualización de la fecha de entrega.")

                # Solo intentar actualizar el turno si el selector de turno fue visible y su valor ha cambiado
                # (es decir, solo para Pedidos Locales en "Mañana" o "Tarde")
                if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"] and new_turno != current_turno:
                    if update_gsheet_cell(worksheet, headers, gsheet_row_index, "Turno", new_turno):
                        df.loc[idx, "Turno"] = new_turno # Actualizar DataFrame en memoria
                        changes_made = True
                    else:
                        st.error("Falló la actualización del turno.")
                elif row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab not in ["Mañana", "Tarde"] and new_turno != current_turno:
                    # En Saltillo o Pasa a Bodega, si por alguna razón el turno cambió (aunque no debería con esta UI)
                    # Este caso es para asegurar que si hay una diferencia en new_turno vs current_turno, se maneje.
                    # Sin embargo, con la UI actual, new_turno === current_turno para estos casos.
                    pass # No se realiza ninguna actualización de turno si no hay un selectbox para ello.

                if changes_made:
                    st.success(f"✅ Cambios aplicados para el pedido {row['ID_Pedido']}!")
                    # st.rerun() # ELIMINADO: Ya no es necesario, los cambios se reflejan en memoria
                else:
                    st.info("No se realizaron cambios en la fecha o turno.")

        st.markdown("---")

        # --- Layout Principal del Pedido (como en la imagen original) ---
        disabled_if_completed = (row['Estado'] == "🟢 Completado")

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
        # Usamos on_change para manejar la actualización del surtidor
        def update_surtidor_callback(current_idx, current_gsheet_row_index, current_surtidor_key, df_param): # Añadir df_param
            new_surtidor_val = st.session_state[current_surtidor_key]
            if new_surtidor_val != surtidor_current:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Surtidor", new_surtidor_val):
                    df_param.loc[current_idx, "Surtidor"] = new_surtidor_val # Actualizar DataFrame en memoria
                    # st.toast("Surtidor actualizado", icon="✅") # ELIMINADO para evitar mensajes redundantes en cada keystroke
                else:
                    st.error("Falló la actualización del surtidor.")

        surtidor_key = f"surtidor_{row['ID_Pedido']}_{origen_tab}"
        col_surtidor.text_input(
            "Surtidor",
            value=surtidor_current,
            label_visibility="collapsed",
            placeholder="Surtidor",
            key=surtidor_key,
            disabled=disabled_if_completed,
            on_change=update_surtidor_callback,
            args=(idx, gsheet_row_index, surtidor_key, df) # Pasar df_main para actualizar en memoria
        )


        # Imprimir/Ver Adjuntos and change to "En Proceso"
        if col_print_btn.button("🖨 Imprimir", key=f"print_button_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            if row['Estado'] != "🔵 En Proceso":
                
                updates_for_print_button = []
                estado_col_idx = headers.index('Estado') + 1
                hora_proceso_col_idx = headers.index('Hora_Proceso') + 1
                fecha_completado_col_idx = headers.index('Fecha_Completado') + 1

                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                    'values': [["🔵 En Proceso"]]
                })
                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proceso_col_idx),
                    'values': [[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                })
                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                    'values': [[""]]
                })

                if batch_update_gsheet_cells(worksheet, updates_for_print_button):
                    df.loc[idx, "Estado"] = "🔵 En Proceso" # Actualizar DataFrame en memoria
                    df.loc[idx, "Hora_Proceso"] = datetime.now() # Actualizar DataFrame en memoria
                    df.loc[idx, "Fecha_Completado"] = pd.NaT # Actualizar DataFrame en memoria
                    st.toast(f"✅ Pedido {orden} marcado como 'En Proceso' y adjuntos desplegados.", icon="✅")
                    # st.cache_resource.clear() # ELIMINADO para evitar recargas constantes
                else:
                    st.error("Falló la actualización del estado a 'En Proceso' al imprimir.")

            st.session_state["expanded_attachments"][row['ID_Pedido']] = not st.session_state["expanded_attachments"].get(row['ID_Pedido'], False)
            # st.rerun() # ELIMINADO: Ya no es necesario, los cambios se reflejan en memoria


        # Completar
        if col_complete_btn.button("🟢 Completar", key=f"done_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            surtidor_final = row.get("Surtidor", "")
            if surtidor_final:
                
                updates_for_complete_button = []
                estado_col_idx = headers.index('Estado') + 1
                fecha_completado_col_idx = headers.index('Fecha_Completado') + 1
                hora_proceso_col_idx = headers.index('Hora_Proceso') + 1

                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                    'values': [["🟢 Completado"]]
                })
                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                    'values': [[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                })
                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proceso_col_idx),
                    'values': [[""]]
                })

                if batch_update_gsheet_cells(worksheet, updates_for_complete_button):
                    df.loc[idx, "Estado"] = "🟢 Completado" # Actualizar DataFrame en memoria
                    df.loc[idx, "Fecha_Completado"] = datetime.now() # Actualizar DataFrame en memoria
                    df.loc[idx, "Hora_Proceso"] = pd.NaT # Actualizar DataFrame en memoria
                    st.toast(f"✅ Pedido {orden} marcado como completado", icon="✅")
                    # st.cache_resource.clear() # ELIMINADO para evitar recargas constantes
                    if row['ID_Pedido'] in st.session_state["expanded_attachments"]:
                        del st.session_state["expanded_attachments"][row['ID_Pedido']]
                    # st.rerun() # ELIMINADO: Ya no es necesario, los cambios se reflejan en memoria
                else:
                    st.error("Falló la actualización del estado a 'Completado'.")
            else:
                st.warning("⚠ Por favor, ingrese el Surtidor antes de completar el pedido.")

        # --- Adjuntos desplegados (if expanded) ---
        if st.session_state["expanded_attachments"].get(row['ID_Pedido'], False):
            st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")

            pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                files_in_folder = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)

                if files_in_folder:
                    filtered_files_to_display = [
                        f for f in files_in_folder
                        if "comprobante" not in f['title'].lower() and "surtido" not in f['title'].lower()
                    ]

                    if filtered_files_to_display:
                        for file_info in filtered_files_to_display:
                            file_url = get_s3_file_download_url(s3_client, file_info['key'])
                            display_name = file_info['title']
                            if row['ID_Pedido'] in display_name:
                                display_name = display_name.replace(row['ID_Pedido'], "").replace("__", "_").replace("_-", "_").replace("-_", "_").strip('_').strip('-')

                            st.markdown(f"- 📄 **{display_name}** ([🔗 Ver/Descargar]({file_url}))")
                    else:
                        st.info("No hay adjuntos para mostrar (excluyendo comprobantes y surtidos).")
                else:
                    st.info("No se encontraron archivos en la carpeta del pedido en S3.")
            else:
                st.error(f"❌ No se encontró la carpeta (prefijo S3) del pedido '{row['ID_Pedido']}'.")


        # --- Campo de Notas editable y Comentario ---
        st.markdown("---")
        info_text_comment = row.get("Comentario")
        if pd.notna(info_text_comment) and str(info_text_comment).strip() != '':
            st.info(f"💬 Comentario: {info_text_comment}")

        current_notas = row.get("Notas", "")
        # Usamos on_change para manejar la actualización de las notas
        def update_notas_callback(current_idx, current_gsheet_row_index, current_notas_key, df_param): # Añadir df_param
            new_notas_val = st.session_state[current_notas_key]
            if new_notas_val != current_notas:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Notas", new_notas_val):
                    df_param.loc[current_idx, "Notas"] = new_notas_val # Actualizar DataFrame en memoria
                    # st.toast("Notas actualizadas", icon="✅") # ELIMINADO
                else:
                    st.error("Falló la actualización de las notas.")

        notas_key = f"notas_edit_{row['ID_Pedido']}_{origen_tab}"
        st.text_area(
            "📝 Notas (editable)",
            value=current_notas,
            key=notas_key,
            height=70,
            disabled=disabled_if_completed,
            on_change=update_notas_callback,
            args=(idx, gsheet_row_index, notas_key, df) # Pasar df_main para actualizar en memoria
        )

        if tiene_modificacion:
            st.warning(f"🟡 Modificación de Surtido:\n{row['Modificacion_Surtido']}")

            mod_surtido_archivos_mencionados_raw = []
            for linea in str(row['Modificacion_Surtido']).split('\n'):
                match = re.search(r'\(Adjunto: (.+?)\)', linea)
                if match:
                    mod_surtido_archivos_mencionados_raw.extend([f.strip() for f in match.group(1).split(',')])

            surtido_files_in_s3 = []
            if pedido_folder_prefix is None:
                pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                all_files_in_folder = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)
                surtido_files_in_s3 = [
                    f for f in all_files_in_folder
                    if "surtido" in f['title'].lower()
                ]

            all_surtido_related_files = []
            for f_name in mod_surtido_archivos_mencionados_raw:
                all_surtido_related_files.append({
                    'title': f_name,
                    'key': f"{pedido_folder_prefix}{f_name}"
                })

            for s_file in surtido_files_in_s3:
                if not any(s_file['title'] == existing_f['title'] for existing_f in all_surtido_related_files):
                    all_surtido_related_files.append(s_file)


            if all_surtido_related_files:
                st.markdown("Adjuntos de Modificación (Surtido/Relacionados):")
                archivos_ya_mostrados_para_mod = set()

                for file_info in all_surtido_related_files:
                    file_name_to_display = file_info['title']
                    object_key_to_download = file_info['key']

                    if file_name_to_display in archivos_ya_mostrados_para_mod:
                        continue

                    try:
                        if not object_key_to_download.startswith(S3_ATTACHMENT_PREFIX):
                            object_key_to_download = f"{pedido_folder_prefix}{file_name_to_display}"

                        presigned_url = get_s3_file_download_url(s3_client, object_key_to_download)
                        if presigned_url:
                            st.markdown(f"- 📄 [{file_name_to_display}]({presigned_url})")
                        else:
                            st.warning(f"⚠️ No se pudo generar el enlace para: {file_name_to_display}")
                    except Exception as e:
                        st.warning(f"⚠️ Error al procesar adjunto de modificación '{file_name_to_display}': {e}")

                    archivos_ya_mostrados_para_mod.add(file_name_to_display)
            else:
                st.info("No hay adjuntos específicos para esta modificación de surtido mencionados en el texto.")

# --- Main Application Logic ---

df_main, worksheet_main, headers_main = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

if not df_main.empty:
    df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
    if changes_made_by_demorado_check:
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

    # --- Implementación de Pestañas con st.tabs (revertido) ---
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
                    
                    for i, date_label in enumerate(date_tab_labels):
                        with date_tabs_m[i]:
                            current_selected_date_dt_str = date_label.replace("📅 ", "") 
                            current_selected_date_dt = pd.to_datetime(current_selected_date_dt_str, format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_m_display[pedidos_m_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌅 Pedidos Locales - Mañana - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Mañana", "📍 Pedidos Locales", worksheet_main, headers_main)
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
                    for i, date_label in enumerate(date_tab_labels):
                        with date_tabs_t[i]:
                            current_selected_date_dt_str = date_label.replace("📅 ", "")
                            current_selected_date_dt = pd.to_datetime(current_selected_date_dt_str, format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_t_display[pedidos_t_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌇 Pedidos Locales - Tarde - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Tarde", "📍 Pedidos Locales", worksheet_main, headers_main)
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
                    mostrar_pedido(df_main, idx, row, orden, "Saltillo", "📍 Pedidos Locales", worksheet_main, headers_main)
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
                    mostrar_pedido(df_main, idx, row, orden, "Pasa a Bodega", "📍 Pedidos Locales", worksheet_main, headers_main)
            else:
                st.info("No hay pedidos para pasar a bodega.")

    with main_tabs[1]: # 🚚 Pedidos Foráneos
        pedidos_foraneos_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🚚 Pedido Foráneo")
        ].copy()
        if not pedidos_foraneos_display.empty:
            pedidos_foraneos_display = ordenar_pedidos_custom(pedidos_foraneos_display)
            for orden, (idx, row) in enumerate(pedidos_foraneos_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Foráneo", "🚚 Pedidos Foráneos", worksheet_main, headers_main)
        else:
            st.info("No hay pedidos foráneos.")

    with main_tabs[2]: # 🛠 Garantías
        garantias_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🛠 Garantía")].copy()
        if not garantias_display.empty:
            garantias_display = ordenar_pedidos_custom(garantias_display)
            for orden, (idx, row) in enumerate(garantias_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Garantía", "🛠 Garantías", worksheet_main, headers_main)
        else:
            st.info("No hay garantías.")

    with main_tabs[3]: # 🔁 Devoluciones
        devoluciones_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🔁 Devolución")].copy()
        if not devoluciones_display.empty:
            devoluciones_display = ordenar_pedidos_custom(devoluciones_display)
            for orden, (idx, row) in enumerate(devoluciones_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Devolución", "🔁 Devoluciones", worksheet_main, headers_main)
        else:
            st.info("No hay devoluciones.")

    with main_tabs[4]: # 📬 Solicitud de Guía
        solicitudes_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "📬 Solicitud de guía")].copy()
        if not solicitudes_display.empty:
            solicitudes_display = ordenar_pedidos_custom(solicitudes_display)
            for orden, (idx, row) in enumerate(solicitudes_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Solicitud de Guía", "📬 Solicitud de Guía", worksheet_main, headers_main)
        else:
            st.info("No hay solicitudes de guía.")

    with main_tabs[5]: # ✅ Historial Completados
        st.markdown("### Historial de Pedidos Completados")
        if not df_completados_historial.empty:
            st.dataframe(
                df_completados_historial[[
                    'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
                    'Tipo_Envio', 'Fecha_Entrega', 'Fecha_Completado', 'Notas', 'Modificacion_Surtido',
                    'Adjuntos', 'Adjuntos_Surtido', 'Turno'
                ]].head(50),
                use_container_width=True, hide_index=True
            )
            st.info("Mostrando los 50 pedidos completados más recientes. Puedes ajustar este límite si es necesario.")
        else:
            st.info("No hay pedidos completados en el historial.")

else:
    st.info("No se encontraron datos de pedidos en la hoja de Google Sheets. Asegúrate de que los datos se están subiendo correctamente desde la aplicación de Vendedores.")