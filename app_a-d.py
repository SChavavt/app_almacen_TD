import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import re
import gspread.utils

st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

st.title("📬 Bandeja de Pedidos TD")

# Botón de refrescar
if st.button("🔄 Recargar Pedidos", help="Haz clic para recargar todos los pedidos desde Google Sheets."):
    st.cache_data.clear()  # Limpia la caché de datos para forzar la recarga
    st.rerun()  # Vuelve a ejecutar la aplicación para recargar los datos

# --- Google Sheets Constants (pueden venir de st.secrets si se prefiere) ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos'

# --- AWS S3 Configuration ---
try:
    if "aws" not in st.secrets:
        st.error("❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente con la sección [aws].")
        st.info("Falta la clave: 'st.secrets has no key \"aws\". Did you forget to add it to secrets.toml, mount it to secret directory, or the app settings on Streamlit Cloud?")
        st.stop() # Detiene la ejecución si las credenciales no están presentes

    aws_access_key_id = st.secrets["aws"]["aws_access_key_id"]
    aws_secret_access_key = st.secrets["aws"]["aws_secret_access_key"]
    aws_region_name = st.secrets["aws"]["aws_region_name"]
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
    S3_ATTACHMENT_PREFIX = st.secrets["aws"]["s3_attachment_prefix"]

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name
    )
except KeyError as e:
    st.error(f"❌ Error al cargar credenciales de AWS S3: {e}. Asegúrate de que 'aws_access_key_id', 'aws_secret_access_key', 'aws_region_name' y 's3_bucket_name' estén en tu secrets.toml.")
    st.stop()
except Exception as e:
    st.error(f"❌ Ocurrió un error inesperado al inicializar S3: {e}")
    st.stop()


# --- Google Sheets Authentication and Functions ---
@st.cache_resource(ttl=3600)
def get_gsheet_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        return client
    except KeyError:
        st.error("❌ Credenciales de Google Sheets (gcp_service_account) no encontradas en Streamlit secrets.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.stop()

@st.cache_data(ttl=60) # Cache for 60 seconds
def get_raw_sheet_data(sheet_id, worksheet_name):
    try:
        client = get_gsheet_client()
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
        if not data:
            return pd.DataFrame(), [], worksheet
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        
        # Añadir índice de fila de Google Sheets (1-basado)
        df['_gsheet_row_index'] = df.index + 2 # +2 porque la fila 0 es encabezado y pandas es 0-indexed

        return df, headers, worksheet
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Hoja de cálculo con ID '{sheet_id}' no encontrada.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Hoja de trabajo '{worksheet_name}' no encontrada en la hoja de cálculo.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al leer datos de Google Sheets: {e}")
        st.stop()

def update_gsheet_cell(worksheet, headers, row_index, column_name, new_value):
    try:
        col_idx = headers.index(column_name) + 1 # gspread es 1-indexed
        worksheet.update_cell(row_index, col_idx, new_value)
        return True
    except Exception as e:
        st.error(f"Error al actualizar celda en Google Sheets ({column_name}): {e}")
        return False

def batch_update_gsheet_cells(worksheet, updates_list):
    try:
        worksheet.batch_update(updates_list)
        return True
    except Exception as e:
        st.error(f"Error al realizar actualización por lotes en Google Sheets: {e}")
        return False

# --- AWS S3 Functions ---
def get_files_in_s3_prefix(s3_client, prefix):
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        if 'Contents' in response:
            for item in response['Contents']:
                # Excluir la "carpeta" misma si el prefijo termina en /
                if item['Key'] == prefix and prefix.endswith('/'):
                    continue
                file_name = item['Key'].split('/')[-1]
                if file_name: # Asegurarse de que no sea una entrada de carpeta vacía
                    files.append({
                        'title': file_name,
                        'key': item['Key']
                    })
        return files
    except Exception as e:
        st.error(f"Error al listar archivos en S3 para el prefijo {prefix}: {e}")
        return []

def get_s3_file_download_url(s3_client, object_key, expiration=3600):
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': S3_BUCKET_NAME,
                                                            'Key': object_key},
                                                    ExpiresIn=expiration)
        return response
    except Exception as e:
        st.error(f"Error al generar URL pre-firmada para {object_key}: {e}")
        return "#" # Retorna un hash para un enlace no funcional

def find_pedido_subfolder_prefix(s3_client_param, base_prefix, pedido_id):
    """
    Busca la subcarpeta de un pedido dado el ID, manejando variaciones como
    "ID_PEDIDO/" o "ID-PEDIDO/".
    """
    if not base_prefix.endswith('/'):
        base_prefix += '/'

    # Intenta con el formato original primero
    potential_prefix = f"{base_prefix}{pedido_id}/"
    if get_files_in_s3_prefix(s3_client_param, potential_prefix):
        return potential_prefix
    
    # Intenta reemplazar guiones bajos con guiones medios (si aplica)
    if '_' in pedido_id:
        alt_pedido_id = pedido_id.replace('_', '-')
        potential_prefix_alt = f"{base_prefix}{alt_pedido_id}/"
        if get_files_in_s3_prefix(s3_client_param, potential_prefix_alt):
            return potential_prefix_alt

    # Intenta reemplazar guiones medios con guiones bajos (si aplica)
    if '-' in pedido_id:
        alt_pedido_id = pedido_id.replace('-', '_')
        potential_prefix_alt = f"{base_prefix}{alt_pedido_id}/"
        if get_files_in_s3_prefix(s3_client_param, potential_prefix_alt):
            return potential_prefix_alt
            
    return None # No se encontró una carpeta para el pedido

# --- Data Processing ---
@st.cache_data(ttl=60)
def process_sheet_data(df_raw):
    df = df_raw.copy()
    
    # Convertir 'Fecha_Entrega' a datetime
    df['Fecha_Entrega'] = pd.to_datetime(df['Fecha_Entrega'], errors='coerce')
    
    # Crear una columna temporal para la hora de registro para el ordenamiento
    df['Hora_Registro_dt'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
    
    # Filtrar pedidos sin estado o con estado "Cancelado"
    df = df[df['Estado'].fillna('') != "🔴 Cancelado"]
    df = df[df['Estado'].fillna('') != ""] # Excluir filas con estado vacío/nulo

    # Ordenar por fecha de entrega (ascendente) y luego hora de registro (ascendente)
    df = df.sort_values(by=['Fecha_Entrega', 'Hora_Registro_dt'], ascending=[True, True])

    # Clasificar pedidos
    df_pendientes = df[df['Estado'] == "🟡 Pendiente"].copy()
    df_en_proceso = df[df['Estado'] == "🔵 En Proceso"].copy()
    df_listos_recolectar = df[df['Estado'] == "📦 Listo para Recolectar"].copy()
    df_demorados = df[df['Estado'] == "🟠 Demorado"].copy()
    df_solicitud_guia = df[df['Estado'] == "📬 Solicitud de Guía"].copy()
    df_completados_historial = df[df['Estado'] == "🟢 Completado"].copy()

    # Separar "En Proceso" por turno para "Pedido Local"
    df_en_proceso_local = df_en_proceso[df_en_proceso['Tipo_Envio'] == "📍 Pedido Local"].copy()
    df_en_proceso_foraneo = df_en_proceso[df_en_proceso['Tipo_Envio'] == "🚚 Pedido Foráneo"].copy()
    df_en_proceso_guia = df_en_proceso[df_en_proceso['Tipo_Envio'] == "📦 Con Guía"].copy()

    df_en_proceso_local_manana = df_en_proceso_local[df_en_proceso_local['Turno'] == "☀️ Local Mañana"].copy()
    df_en_proceso_local_tarde = df_en_proceso_local[df_en_proceso_local['Turno'] == "🌙 Local Tarde"].copy()
    df_en_proceso_local_sin_turno = df_en_proceso_local[df_en_proceso_local['Turno'] == ""].copy()


    return {
        "df_pendientes": df_pendientes,
        "df_en_proceso_local_manana": df_en_proceso_local_manana,
        "df_en_proceso_local_tarde": df_en_proceso_local_tarde,
        "df_en_proceso_local_sin_turno": df_en_proceso_local_sin_turno,
        "df_en_proceso_foraneo": df_en_proceso_foraneo,
        "df_en_proceso_guia": df_en_proceso_guia,
        "df_listos_recolectar": df_listos_recolectar,
        "df_demorados": df_demorados,
        "df_solicitud_guia": df_solicitud_guia,
        "df_completados_historial": df_completados_historial,
    }

# --- Check for Demorados ---
def check_and_update_demorados(df_to_check, worksheet, headers, s3_client_param):
    current_time = datetime.now()
    demorados_updates = []
    
    # Asegúrate de que 'Hora_Proceso' sea datetime
    df_to_check['Hora_Proceso_dt'] = pd.to_datetime(df_to_check['Hora_Proceso'], errors='coerce')

    for idx, row in df_to_check.iterrows():
        # Solo procesar si el estado es "En Proceso" y tiene una Hora_Proceso válida
        if row['Estado'] == "🔵 En Proceso" and pd.notna(row['Hora_Proceso_dt']):
            time_in_process = current_time - row['Hora_Proceso_dt']
            
            # Si lleva más de 2 horas en proceso, marcar como demorado
            if time_in_process > timedelta(hours=2):
                if row['Estado'] != "🟠 Demorado": # Evitar actualizar si ya está demorado
                    gsheet_row_index = row.get('_gsheet_row_index')
                    if gsheet_row_index:
                        estado_col_idx = headers.index('Estado') + 1
                        demorados_updates.append({
                            'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                            'values': [["🟠 Demorado"]]
                        })
                        # Actualizar el DataFrame localmente para la sesión actual
                        df_to_check.loc[idx, 'Estado'] = "🟠 Demorado"
                        st.warning(f"⏰ Pedido {row['ID_Pedido']} marcado como '🟠 Demorado'.")
    
    if demorados_updates:
        if batch_update_gsheet_cells(worksheet, demorados_updates):
            st.success("✅ Estados de pedidos demorados actualizados en Google Sheets.")
            st.cache_data.clear() # Limpiar cache para que la UI se actualice
            st.rerun() # Rerun para reflejar los cambios
        else:
            st.error("❌ Falló la actualización de pedidos demorados en Google Sheets.")
    return df_to_check

# --- Display Order Function ---
def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers, s3_client_param):
    gsheet_row_index = row.get('_gsheet_row_index')
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{row['ID_Pedido']}'.")
        return

    with st.container():
        st.markdown("---")
        tiene_modificacion = row.get("Modificacion_Surtido") and pd.notna(row["Modificacion_Surtido"]) and str(row["Modificacion_Surtido"]).strip() != ''
        if tiene_modificacion:
            st.warning(f"⚠ ¡MODIFICACIÓN DE SURTIDO DETECTADA! Pedido #{orden}")

        # --- Cambiar Fecha y Turno ---
        if row['Estado'] != "🟢 Completado" and row.get("Tipo_Envio") in ["📍 Pedido Local", "🚚 Pedido Foráneo"]:
            st.markdown("##### 📅 Cambiar Fecha y Turno")
            col_current_info_date, col_current_info_turno, col_inputs = st.columns([1, 1, 2])

            fecha_actual_str = row.get("Fecha_Entrega", "")
            fecha_actual_dt = pd.to_datetime(fecha_actual_str, errors='coerce') if fecha_actual_str else None
            fecha_mostrar = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else "Sin fecha"
            col_current_info_date.info(f"**Fecha actual:** {fecha_mostrar}")

            current_turno = row.get("Turno", "")
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                col_current_info_turno.info(f"**Turno actual:** {current_turno}")
            else:
                col_current_info_turno.empty()


            today = datetime.now().date()
            default_fecha = fecha_actual_dt.date() if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today else today

            fecha_key = f"new_fecha_{row['ID_Pedido']}"
            turno_key = f"new_turno_{row['ID_Pedido']}"

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

            if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                turno_options = ["", "☀️ Local Mañana", "🌙 Local Tarde"]
                if st.session_state[turno_key] not in turno_options:
                    st.session_state[turno_key] = turno_options[0]

                st.selectbox(
                    "Clasificar turno como:",
                    options=turno_options,
                    key=turno_key,
                )

            if st.button("✅ Aplicar Cambios de Fecha/Turno", key=f"btn_apply_{row['ID_Pedido']}"):
                cambios = []
                nueva_fecha_str = st.session_state[fecha_key].strftime('%Y-%m-%d')

                if nueva_fecha_str != fecha_actual_str:
                    col_idx = headers.index("Fecha_Entrega") + 1
                    cambios.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx), 'values': [[nueva_fecha_str]]})
                    df.loc[idx, "Fecha_Entrega"] = nueva_fecha_str

                if row.get("Tipo_Envio") == "📍 Pedido Local" and origen_tab in ["Mañana", "Tarde"]:
                    nuevo_turno = st.session_state[turno_key]
                    if nuevo_turno != current_turno:
                        col_idx = headers.index("Turno") + 1
                        cambios.append({'range': gspread.utils.rowcol_to_a1(gsheet_row_index, col_idx), 'values': [[nuevo_turno]]})
                        df.loc[idx, "Turno"] = nuevo_turno

                if cambios:
                    if batch_update_gsheet_cells(worksheet, cambios):
                        st.success(f"✅ Pedido {row['ID_Pedido']} actualizado.")
                        st.session_state["pedido_editado"] = row['ID_Pedido']
                        st.session_state["fecha_seleccionada"] = nueva_fecha_str
                        st.session_state["subtab_local"] = origen_tab
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Falló la actualización en Google Sheets.")
                else:
                    st.info("No hubo cambios para aplicar.")
        
        st.markdown("---")

        # --- Layout Principal del Pedido ---
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
        def update_surtidor_callback(current_idx, current_gsheet_row_index, current_surtidor_key, df_param):
            new_surtidor_val = st.session_state[current_surtidor_key]
            if new_surtidor_val != surtidor_current:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Surtidor", new_surtidor_val):
                    df_param.loc[current_idx, "Surtidor"] = new_surtidor_val
                    st.toast("Surtidor actualizado", icon="✅")
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
            args=(idx, gsheet_row_index, surtidor_key, df)
        )


        # Imprimir/Ver Adjuntos and change to "En Proceso"
        
        if col_print_btn.button("🖨 Imprimir", key=f"print_{row['ID_Pedido']}_{origen_tab}"):
            st.session_state["expanded_attachments"][row['ID_Pedido']] = not st.session_state["expanded_attachments"].get(row['ID_Pedido'], False)
            
            # --- LÓGICA AGREGADA PARA CAMBIAR EL ESTADO A "EN PROCESO" ---
            if row['Estado'] == "🟡 Pendiente": # Solo cambia si actualmente es "Pendiente"
                try:
                    updates = []
                    estado_col_idx = headers.index('Estado') + 1
                    hora_proceso_col_idx = headers.index('Hora_Proceso') + 1

                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                        'values': [["🔵 En Proceso"]]
                    })
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proceso_col_idx),
                        'values': [[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                    })

                    if batch_update_gsheet_cells(worksheet, updates):
                        df.loc[idx, "Estado"] = "🔵 En Proceso"
                        df.loc[idx, "Hora_Proceso"] = datetime.now() # Actualizar DataFrame también
                        
                        st.toast(f"✅ Pedido {row['ID_Pedido']} marcado como 'En Proceso' en la hoja de cálculo.", icon="✅")
                        # NO st.cache_data.clear() ni st.rerun() aquí
                    else:
                        st.error("❌ Falló la actualización del estado a 'En Proceso' en Google Sheets.")
                except Exception as e:
                    st.error(f"Error al cambiar estado a 'En Proceso': {e}")
            # --- FIN LÓGICA AGREGADA ---
            st.toast("Acción 'Imprimir' completada.", icon="🖨")


        if st.session_state["expanded_attachments"].get(row["ID_Pedido"], False):
            st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")
            pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client_param, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                files_in_folder = get_files_in_s3_prefix(s3_client_param, pedido_folder_prefix)
                if files_in_folder:
                    filtered_files_to_display = [
                        f for f in files_in_folder
                        if "comprobante" not in f['title'].lower() and "surtido" not in f['title'].lower()
                    ]
                    if filtered_files_to_display:
                        for file_info in filtered_files_to_display:
                            file_url = get_s3_file_download_url(s3_client_param, file_info['key'])
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


        # Botón Completar
        if col_complete_btn.button("🟢 Completar", key=f"complete_button_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            surtidor_val = st.session_state.get(surtidor_key, "").strip()
            if not surtidor_val:
                st.warning("⚠️ Debes ingresar el nombre del surtidor antes de completar el pedido.")
            else:
                try:
                    updates = []
                    estado_col_idx = headers.index('Estado') + 1
                    fecha_completado_col_idx = headers.index('Fecha_Completado') + 1

                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                        'values': [["🟢 Completado"]]
                    })
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                        'values': [[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                    })

                    if batch_update_gsheet_cells(worksheet, updates):
                        df.loc[idx, "Estado"] = "🟢 Completado"
                        df.loc[idx, "Fecha_Completado"] = datetime.now()
                        st.success(f"✅ Pedido {row['ID_Pedido']} completado exitosamente.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ No se pudo completar el pedido.")
                except Exception as e:
                    st.error(f"Error al completar el pedido: {e}")

        # --- Campo de Notas editable y Comentario ---
        st.markdown("---")
        info_text_comment = row.get("Comentario")
        if pd.notna(info_text_comment) and str(info_text_comment).strip() != '':
            st.info(f"💬 Comentario: {info_text_comment}")

        current_notas = row.get("Notas", "")
        def update_notas_callback(current_idx, current_gsheet_row_index, current_notas_key, df_param):
            new_notas_val = st.session_state[current_notas_key]
            if new_notas_val != current_notas:
                if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Notas", new_notas_val):
                    df_param.loc[current_idx, "Notas"] = new_notas_val
                    st.toast("Notas actualizadas", icon="✅")
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
            args=(idx, gsheet_row_index, notas_key, df)
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
                pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client_param, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                all_files_in_folder = get_files_in_s3_prefix(s3_client_param, pedido_folder_prefix)
                surtido_files_in_s3 = [
                    f for f in all_files_in_folder
                    if "surtido" in f['title'].lower()
                ]

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


            if all_surtido_related_files:
                st.markdown("Adjuntos de Modificación (Surtido/Relacionados):")
                archivos_ya_mostrados_para_mod = set()

                for file_info in all_surtido_related_files:
                    file_name_to_display = file_info['title']
                    object_key_to_download = file_info['key']

                    if file_name_to_display in archivos_ya_mostrados_para_mod:
                        continue

                    try:
                        if not object_key_to_download.startswith(S3_ATTACHMENT_PREFIX) and pedido_folder_prefix:
                            object_key_to_download = f"{pedido_folder_prefix}{file_name_to_display}"
                        
                        if not pedido_folder_prefix and not object_key_to_download.startswith(S3_BUCKET_NAME):
                             st.warning(f"⚠️ No se pudo determinar la ruta S3 para: {file_name_to_display}")
                             continue

                        presigned_url = get_s3_file_download_url(s3_client_param, object_key_to_download)
                        if presigned_url and presigned_url != "#":
                            st.markdown(f"- 📄 [{file_name_to_display}]({presigned_url})")
                        else:
                            st.warning(f"⚠️ No se pudo generar el enlace para: {file_name_to_display}")
                    except Exception as e:
                        st.warning(f"⚠️ Error al procesar adjunto de modificación '{file_name_to_display}': {e}")

                    archivos_ya_mostrados_para_mod.add(file_name_to_display)
            else:
                st.info("No hay adjuntos específicos para esta modificación de surtido mencionados en el texto.")


# --- Main Application Logic ---
df_main, headers_main, worksheet_main = get_raw_sheet_data(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

if df_main.empty:
    st.info("No hay pedidos en la hoja de cálculo.")
else:
    processed_data = process_sheet_data(df_main)
    
    df_pendientes = processed_data["df_pendientes"]
    df_en_proceso_local_manana = processed_data["df_en_proceso_local_manana"]
    df_en_proceso_local_tarde = processed_data["df_en_proceso_local_tarde"]
    df_en_proceso_local_sin_turno = processed_data["df_en_proceso_local_sin_turno"]
    df_en_proceso_foraneo = processed_data["df_en_proceso_foraneo"]
    df_en_proceso_guia = processed_data["df_en_proceso_guia"]
    df_listos_recolectar = processed_data["df_listos_recolectar"]
    df_demorados = processed_data["df_demorados"]
    df_solicitud_guia = processed_data["df_solicitud_guia"]
    df_completados_historial = processed_data["df_completados_historial"]

    # Inicializar st.session_state para expanded_attachments
    if "expanded_attachments" not in st.session_state:
        st.session_state["expanded_attachments"] = {}

    main_tabs = st.tabs([
        "🟡 Pendientes", "🔵 En Proceso (Local)", "🔵 En Proceso (Foráneo)",
        "📦 Listos para Recolectar", "🟠 Demorados", "📬 Solicitud de Guía",
        "✅ Historial Completados"
    ])

    with main_tabs[0]: # 🟡 Pendientes
        st.markdown("### Pedidos Pendientes")
        if not df_pendientes.empty:
            for i, row in df_pendientes.iterrows():
                mostrar_pedido(df_pendientes, i, row, i+1, "Pendientes", "Pendientes", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos pendientes.")

    with main_tabs[1]: # 🔵 En Proceso (Local)
        st.markdown("### Pedidos En Proceso (Locales)")
        local_subtabs = st.tabs(["☀️ Mañana", "🌙 Tarde", "Sin Turno / Otros"])
        
        # Procesar df_en_proceso_local_sin_turno con la función de demorados
        df_en_proceso_local_sin_turno = check_and_update_demorados(df_en_proceso_local_sin_turno, worksheet_main, headers_main, s3_client)

        with local_subtabs[0]: # Mañana
            if not df_en_proceso_local_manana.empty:
                for i, row in df_en_proceso_local_manana.iterrows():
                    mostrar_pedido(df_en_proceso_local_manana, i, row, i+1, "Mañana", "En Proceso (Local)", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos locales en proceso para la mañana.")
        
        with local_subtabs[1]: # Tarde
            if not df_en_proceso_local_tarde.empty:
                for i, row in df_en_proceso_local_tarde.iterrows():
                    mostrar_pedido(df_en_proceso_local_tarde, i, row, i+1, "Tarde", "En Proceso (Local)", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos locales en proceso para la tarde.")

        with local_subtabs[2]: # Sin Turno / Otros
            if not df_en_proceso_local_sin_turno.empty:
                for i, row in df_en_proceso_local_sin_turno.iterrows():
                    mostrar_pedido(df_en_proceso_local_sin_turno, i, row, i+1, "Sin Turno", "En Proceso (Local)", worksheet_main, headers_main, s3_client)
            else:
                st.info("No hay pedidos locales en proceso sin turno clasificado.")

    with main_tabs[2]: # 🔵 En Proceso (Foráneo)
        st.markdown("### Pedidos En Proceso (Foráneos)")
        df_en_proceso_foraneo = check_and_update_demorados(df_en_proceso_foraneo, worksheet_main, headers_main, s3_client)

        if not df_en_proceso_foraneo.empty:
            for i, row in df_en_proceso_foraneo.iterrows():
                mostrar_pedido(df_en_proceso_foraneo, i, row, i+1, "Foráneos", "En Proceso (Foráneo)", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos foráneos en proceso.")

    with main_tabs[3]: # 📦 Listos para Recolectar
        st.markdown("### Pedidos Listos para Recolectar")
        if not df_listos_recolectar.empty:
            for i, row in df_listos_recolectar.iterrows():
                mostrar_pedido(df_listos_recolectar, i, row, i+1, "Listos Recolectar", "Listos para Recolectar", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos listos para recolectar.")

    with main_tabs[4]: # 🟠 Demorados
        st.markdown("### Pedidos Demorados")
        # Asegurarse de que los demorados sean solo aquellos con estado "Demorado"
        df_demorados_filtrado = df_demorados[df_demorados['Estado'] == "🟠 Demorado"].copy()
        if not df_demorados_filtrado.empty:
            for i, row in df_demorados_filtrado.iterrows():
                mostrar_pedido(df_demorados_filtrado, i, row, i+1, "Demorados", "Demorados", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay pedidos demorados.")

    with main_tabs[5]: # 📬 Solicitud de Guía
        st.markdown("### Pedidos con Solicitud de Guía")
        if not df_solicitud_guia.empty:
            for i, row in df_solicitud_guia.iterrows():
                mostrar_pedido(df_solicitud_guia, i, row, i+1, "Solicitud de Guía", "Solicitud de Guía", worksheet_main, headers_main, s3_client)
        else:
            st.info("No hay solicitudes de guía.")

    with main_tabs[6]: # ✅ Historial Completados
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
            st.info("Mostrando los 50 pedidos completados más recientes.")
        else:
            st.info("No hay pedidos completados.")
