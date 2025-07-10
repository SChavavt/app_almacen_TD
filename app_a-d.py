import streamlit as st
import os
from datetime import datetime, timedelta
import json
import uuid
import pandas as pd
from io import BytesIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# NEW: Import boto3 for AWS S3
import boto3

# --- STREAMLIT CONFIGURATION ---
st.set_page_config(page_title="App Vendedores TD", layout="wide")


# --- GOOGLE SHEETS CONFIGURATION ---
# Eliminamos la línea SERVICE_ACCOUNT_FILE ya que leeremos de secrets
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

# NEW: Function to get gspread client from Streamlit secrets
def get_google_sheets_client():
    """
    Función para obtener el cliente de gspread usando credenciales de Streamlit secrets.
    """
    try:
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except KeyError:
        st.error("❌ Error: Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que estén configuradas correctamente como 'google_credentials'.")
        st.stop()
    except json.JSONDecodeError:
        st.error("❌ Error: Las credenciales de Google Sheets en Streamlit secrets no son un JSON válido.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar credenciales de Google Sheets: {e}")
        st.stop()


# --- AWS S3 CONFIGURATION (NEW) ---
# Load AWS credentials from Streamlit secrets
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: AWS S3 credentials not found in Streamlit secrets. Make sure your .streamlit/secrets.toml file is correctly configured. Missing key: {e}")
    st.stop()


st.title("🛒 App de Vendedores TD")
st.write("¡Bienvenido! Aquí puedes registrar y gestionar tus pedidos.")

# --- AUTHENTICATION AND CLIENT FUNCTIONS ---

# Removed the old load_credentials_from_file and get_gspread_client functions
# as they are replaced by get_google_sheets_client()

# NEW: Function to upload files to AWS S3
@st.cache_resource
def get_s3_client():
    """Initializes and returns an S3 client."""
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
        st.stop()

def upload_file_to_s3(s3_client, bucket_name, file_obj, s3_key):
    """
    Sube un archivo a un bucket de S3.

    Args:
        s3_client: El cliente S3 inicializado.
        bucket_name: El nombre del bucket S3.
        file_obj: El objeto de archivo cargado por st.file_uploader.
        s3_key: La ruta completa y nombre del archivo en S3 (ej. 'pedido_id/filename.pdf').

    Returns:
        tuple: (True, URL del archivo) si tiene éxito, (False, None) en caso de error.
    """
    try:
        # Asegúrate de que el puntero del archivo esté al principio
        file_obj.seek(0)
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, file_url
    except Exception as e:
        st.error(f"❌ Error al subir el archivo '{s3_key}' a S3: {e}")
        return False, None

# --- Initialize Gspread Client and S3 Client ---
# NEW: Initialize gspread client using the new function
g_spread_client = get_google_sheets_client()
s3_client = get_s3_client() # Initialize S3 client

# Removed the old try-except block for client initialization

# --- Tab Definition ---
tab1, tab2, tab3, tab4 = st.tabs(["🛒 Registrar Nuevo Pedido", "✏️ Modificar Pedido Existente", "🧾 Pedidos Pendientes de Comprobante", "⬇️ Descargar Datos"])

# --- List of Vendors (reusable and explicitly alphabetically sorted) ---
VENDEDORES_LIST = sorted([
    "ANA KAREN ORTEGA MAHUAD",
    "DANIELA LOPEZ RAMIREZ",
    "EDGAR ORLANDO GOMEZ VILLAGRAN",
    "GLORIA MICHELLE GARCIA TORRES",
    "GRISELDA CAROLINA SANCHEZ GARCIA",
    "HECTOR DEL ANGEL AREVALO ALCALA",
    "JOSELIN TRUJILLO PATRACA",
    "NORA ALEJANDRA MARTINEZ MORENO",
    "PAULINA TREJO"
])

# Initialize session state for vendor
if 'last_selected_vendedor' not in st.session_state:
    st.session_state.last_selected_vendedor = VENDEDORES_LIST[0] if VENDEDORES_LIST else ""

# --- TAB 1: REGISTER NEW ORDER ---
with tab1:
    st.header("📝 Nuevo Pedido")

    tipo_envio = st.selectbox(
        "📦 Tipo de Envío",
        ["📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
        index=0,
        key="tipo_envio_selector_global"
    )

    subtipo_local = ""
    if tipo_envio == "📍 Pedido Local":
        st.markdown("---")
        st.subheader("⏰ Detalle de Pedido Local")
        subtipo_local = st.selectbox(
            "Turno/Locales",
            ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"],
            index=0,
            help="Selecciona el turno o tipo de entrega para pedidos locales."
        )

    with st.form(key="new_pedido_form", clear_on_submit=True):
        st.markdown("---")
        st.subheader("Información Básica del Cliente y Pedido")

        try:
            initial_vendedor_index = VENDEDORES_LIST.index(st.session_state.last_selected_vendedor)
        except ValueError:
            initial_vendedor_index = 0

        vendedor = st.selectbox(
            "👤 Vendedor",
            options=VENDEDORES_LIST,
            index=initial_vendedor_index,
            help="Selecciona el nombre del vendedor que registra el pedido."
        )

        if vendedor != st.session_state.last_selected_vendedor:
            st.session_state.last_selected_vendedor = vendedor

        registro_cliente = st.text_input("🤝 Cliente", help="Nombre o ID del cliente que realiza el pedido.")

        folio_factura = st.text_input("📄 Folio de Factura", help="Número de folio de la factura para identificar al cliente.")

        fecha_entrega = st.date_input("🗓 Fecha de Entrega Requerida", datetime.now().date(), help="Fecha en la que el cliente espera recibir el pedido.")

        comentario = st.text_area("💬 Comentario / Descripción Detallada", help="Cualquier nota adicional o descripción detallada del pedido.")

        st.markdown("---")
        st.subheader("Adjuntos del Pedido (Otros Archivos)")
        uploaded_files = st.file_uploader("📎 Archivos del Pedido", type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"], accept_multiple_files=True, help="Puedes subir documentos, imágenes o cualquier archivo relevante al pedido (ej. lista de productos, especificaciones).")
        st.info("💡 Asegúrate de que los nombres de archivo sean únicos si vas a adjuntar múltiples veces el mismo archivo para diferentes pedidos.")

        submit_button = st.form_submit_button("✅ Registrar Pedido")

    st.markdown("---")
    st.subheader("Estado de Pago")
    estado_pago = st.selectbox(
        "💰 Estado de Pago",
        ["🔴 No Pagado", "✅ Pagado"],
        index=0,
        key="estado_pago_selector_final"
    )

    comprobante_pago_file = None
    if estado_pago == "✅ Pagado":
        comprobante_pago_file = st.file_uploader(
            "💲 Subir Comprobante de Pago (Obligatorio si es Pagado)",
            type=["pdf", "jpg", "jpeg", "png"],
            help="Sube una imagen o PDF del comprobante de pago.",
            key="comprobante_uploader_final"
        )
        st.info("⚠️ Si el estado es 'Pagado' debes subir un comprobante.")

    if submit_button:
        if not vendedor:
            st.warning("⚠️ Por favor, selecciona el Vendedor.")
            st.stop()
        if not registro_cliente:
            st.warning("⚠️ Por favor, ingresa el nombre del Cliente.")
            st.stop()

        if estado_pago == "✅ Pagado" and comprobante_pago_file is None:
            st.warning("⚠️ Marcaste el pedido como 'Pagado', pero no subiste un comprobante. Por favor, sube uno o cambia el estado a 'No Pagado'.")
            st.stop()

        try:
            spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
            worksheet = spreadsheet.worksheet('datos_pedidos')
            headers = worksheet.row_values(1)
            if not headers:
                st.error("❌ Error: La primera fila del Google Sheet está vacía. Se necesitan encabezados de columna.")
                st.stop()

            now = datetime.now()
            id_pedido = f"PED-{now.strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:4].upper()}"
            # MODIFICATION 1: Add date to Hora_Registro
            hora_registro = now.strftime('%Y-%m-%d %H:%M:%S')

            # NEW: S3 upload logic
            adjuntos_urls = []
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    file_extension = os.path.splitext(uploaded_file.name)[1]
                    # Create a unique key for S3, e.g., 'PED-YYYYMMDDHHMMSS-ABCD/original_filename_UUID.ext'
                    s3_key = f"{id_pedido}/{uploaded_file.name.replace(' ', '_').replace(file_extension, '')}_{uuid.uuid4().hex[:4]}{file_extension}"

                    success, file_url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, uploaded_file, s3_key)
                    if success:
                        adjuntos_urls.append(file_url)
                    else:
                        st.error(f"❌ Falló la subida de '{uploaded_file.name}'. El pedido no se registrará.")
                        st.stop()

            comprobante_pago_url = ""
            if comprobante_pago_file:
                file_extension_cp = os.path.splitext(comprobante_pago_file.name)[1]
                s3_key_cp = f"{id_pedido}/comprobante_{id_pedido}_{now.strftime('%Y%m%d%H%M%S')}{file_extension_cp}"

                success_cp, file_url_cp = upload_file_to_s3(s3_client, S3_BUCKET_NAME, comprobante_pago_file, s3_key_cp)
                if success_cp:
                    comprobante_pago_url = file_url_cp
                    adjuntos_urls.append(comprobante_pago_url)
                else:
                    st.error("❌ Falló la subida del comprobante de pago. El pedido no se registrará.")
                    st.stop()

            adjuntos_str = ", ".join(adjuntos_urls)

            values_to_append = []
            for header in headers:
                if header == "ID_Pedido":
                    values_to_append.append(id_pedido)
                elif header == "Hora_Registro":
                    values_to_append.append(hora_registro)
                elif header == "Vendedor" or header == "Vendedor_Registro":
                    values_to_append.append(vendedor)
                elif header == "Cliente" or header == "RegistroCliente":
                    values_to_append.append(registro_cliente)
                elif header == "Folio_Factura":
                    values_to_append.append(folio_factura)
                elif header == "Tipo_Envio":
                    values_to_append.append(tipo_envio)
                elif header == "Turno":
                    values_to_append.append(subtipo_local)
                elif header == "Fecha_Entrega":
                    values_to_append.append(fecha_entrega.strftime('%Y-%m-%d'))
                elif header == "Comentario":
                    values_to_append.append(comentario)
                elif header == "Modificacion_Surtido":
                    values_to_append.append("")
                elif header == "Adjuntos": # This column will now store S3 URLs
                    values_to_append.append(adjuntos_str)
                elif header == "Adjuntos_Surtido": # This column will also store S3 URLs
                    values_to_append.append("")
                elif header == "Estado":
                    values_to_append.append("🟡 Pendiente")
                elif header == "Surtidor":
                    values_to_append.append("")
                elif header == "Estado_Pago":
                    values_to_append.append(estado_pago)
                elif header == "Fecha_Completado":
                    values_to_append.append("")
                elif header == "Hora_Proceso":
                    values_to_append.append("")
                elif header == "Fecha_Completado_dt":
                    values_to_append.append("")
                elif header == "Notas":
                    values_to_append.append("")
                else:
                    values_to_append.append("")

            try:
                worksheet.append_row(values_to_append)
                st.success(f"🎉 Pedido `{id_pedido}` registrado con éxito!")
                if adjuntos_urls:
                    st.info(f"📎 Archivos subidos a S3: {', '.join([os.path.basename(url) for url in adjuntos_urls])}")
                st.balloons()

            except Exception as append_error:
                st.error(f"❌ Error al escribir en el Google Sheet: {append_error}. Puede que los adjuntos se hayan subido, pero el pedido no se registró.")
                st.info("ℹ️ Verifica los permisos de escritura de la cuenta de servicio en el Google Sheet.")
                st.stop()

        except Exception as e:
            st.error(f"❌ Ocurrió un error inesperado al registrar el pedido: {e}")
            st.info("ℹ️ Revisa tu conexión a internet, los permisos de la cuenta de servicio o la configuración del Google Sheet.")


# --- TAB 2: MODIFY EXISTING ORDER ---
with tab2:
    st.header("✏️ Modificar Pedido Existente")

    message_placeholder_tab2 = st.empty()

    df_pedidos = pd.DataFrame()
    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        headers = worksheet.row_values(1)
        if headers:
            df_pedidos = pd.DataFrame(worksheet.get_all_records())
            if 'Folio_Factura' in df_pedidos.columns:
                df_pedidos['Folio_Factura'] = df_pedidos['Folio_Factura'].astype(str).replace('nan', '')
            if 'Vendedor_Registro' in df_pedidos.columns:
                df_pedidos['Vendedor_Registro'] = df_pedidos['Vendedor_Registro'].apply(
                    lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
        else:
            message_placeholder_tab2.warning("No se pudieron cargar los encabezados del Google Sheet. Asegúrate de que la primera fila no esté vacía.")

    except Exception as e:
        message_placeholder_tab2.error(f"❌ Error al cargar pedidos para modificación: {e}")
        message_placeholder_tab2.info("Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados.")


    selected_order_id = None
    selected_row_data = None
    current_modificacion_surtido_value = ""
    current_notas_value = ""
    current_estado_pago_value = "🔴 No Pagado"
    current_adjuntos_list = []
    current_adjuntos_surtido_list = []

    if df_pedidos.empty:
        message_placeholder_tab2.warning("No hay pedidos registrados para modificar.")
    else:
        df_pedidos['Filtro_Envio_Combinado'] = df_pedidos.apply(
            lambda row: row['Turno'] if row['Tipo_Envio'] == "📍 Pedido Local" and pd.notna(row['Turno']) and row['Turno'] else row['Tipo_Envio'],
            axis=1
        )

        all_filter_options = ["Todos"] + df_pedidos['Filtro_Envio_Combinado'].unique().tolist()

        unique_filter_options = []
        for option in all_filter_options:
            if option not in unique_filter_options:
                unique_filter_options.append(option)

        col1, col2 = st.columns(2)

        filtered_orders = df_pedidos.copy()

        with col1:
            if 'Vendedor_Registro' in filtered_orders.columns:
                unique_vendedores_mod = ["Todos"] + sorted(filtered_orders['Vendedor_Registro'].unique().tolist())
                selected_vendedor_mod = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_mod,
                    key="vendedor_filter_mod"
                )
                if selected_vendedor_mod != "Todos":
                    filtered_orders = filtered_orders[filtered_orders['Vendedor_Registro'] == selected_vendedor_mod]
            else:
                st.warning("La columna 'Vendedor_Registro' no se encontró para aplicar el filtro de vendedor.")

        with col2:
            tipo_envio_filter = st.selectbox(
                "Filtrar por Tipo de Envío:",
                options=unique_filter_options,
                key="tipo_envio_filter_mod"
            )

        if tipo_envio_filter != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Filtro_Envio_Combinado'] == tipo_envio_filter]


        if filtered_orders.empty:
            message_placeholder_tab2.warning("No hay pedidos que coincidan con los filtros seleccionados.")

        else:
            # Limpiar columnas usadas para evitar mostrar 'nan' o valores vacíos
            for col in ['Folio_Factura', 'Cliente', 'Estado', 'Tipo_Envio', 'ID_Pedido']:
                filtered_orders[col] = filtered_orders[col].astype(str).fillna('').replace(['nan', 'None'], '')

            # Generar display_label robusto y legible
            filtered_orders['display_label'] = filtered_orders.apply(lambda row:
                f"📄 {row['Folio_Factura'] if row['Folio_Factura'] else row['ID_Pedido']} - "
                f"{row['Cliente'] if row['Cliente'] else 'Cliente no definido'} - "
                f"{row['Estado'] if row['Estado'] else 'Sin estado'} - "
                f"{row['Tipo_Envio'] if row['Tipo_Envio'] else 'Sin tipo'}", axis=1
            )

            # Ordenar por folio y ID_Pedido como antes
            filtered_orders = filtered_orders.sort_values(
                by=['Folio_Factura', 'ID_Pedido'],
                key=lambda x: x.astype(str).str.lower(),
                na_position='last'
            )

            # Mostrar el selector
            selected_order_display = st.selectbox(
                "📝 Seleccionar Pedido para Modificar",
                filtered_orders['display_label'].tolist(),
                key="select_order_to_modify"
            )

            if selected_order_display:
                selected_order_id = filtered_orders[filtered_orders['display_label'] == selected_order_display]['ID_Pedido'].iloc[0]
                selected_row_data = filtered_orders[filtered_orders['ID_Pedido'] == selected_order_id].iloc[0]

                st.subheader(f"Detalles del Pedido: Folio `{selected_row_data.get('Folio_Factura', 'N/A')}` (ID `{selected_order_id}`)")
                st.write(f"**Vendedor:** {selected_row_data.get('Vendedor', selected_row_data.get('Vendedor_Registro', 'No especificado'))}")
                st.write(f"**Cliente:** {selected_row_data.get('Cliente', 'N/A')}")
                st.write(f"**Folio de Factura:** {selected_row_data.get('Folio_Factura', 'N/A')}")
                st.write(f"**Estado Actual:** {selected_row_data.get('Estado', 'N/A')}")
                st.write(f"**Tipo de Envío:** {selected_row_data.get('Tipo_Envio', 'N/A')}")
                if selected_row_data.get('Tipo_Envio') == "📍 Pedido Local":
                    st.write(f"**Turno Local:** {selected_row_data.get('Turno', 'N/A')}")
                st.write(f"**Fecha de Entrega:** {selected_row_data.get('Fecha_Entrega', 'N/A')}")
                st.write(f"**Comentario Original:** {selected_row_data.get('Comentario', 'N/A')}")
                st.write(f"**Estado de Pago:** {selected_row_data.get('Estado_Pago', '🔴 No Pagado')}")

                current_modificacion_surtido_value = selected_row_data.get('Modificacion_Surtido', '')
                current_notas_value = selected_row_data.get('Notas', '')
                current_estado_pago_value = selected_row_data.get('Estado_Pago', '🔴 No Pagado')

                current_adjuntos_str = selected_row_data.get('Adjuntos', '')
                current_adjuntos_list = [f.strip() for f in current_adjuntos_str.split(',') if f.strip()]

                current_adjuntos_surtido_str = selected_row_data.get('Adjuntos_Surtido', '')
                current_adjuntos_surtido_list = [f.strip() for f in current_adjuntos_surtido_str.split(',') if f.strip()]

                if current_adjuntos_list:
                    st.write("**Adjuntos Originales:**")
                    for adj in current_adjuntos_list:
                        # Displaying URLs for existing attachments
                        st.markdown(f"- [{os.path.basename(adj)}]({adj})")
                else:
                    st.write("**Adjuntos Originales:** Ninguno")

                if current_adjuntos_surtido_list:
                    st.write("**Adjuntos de Modificación/Surtido:**")
                    for adj_surtido in current_adjuntos_surtido_list:
                        # Displaying URLs for existing attachments
                        st.markdown(f"- [{os.path.basename(adj_surtido)}]({adj_surtido})")
                else:
                    st.write("**Adjuntos de Modificación/Surtido:** Ninguno")


                st.markdown("---")
                st.subheader("Modificar Campos y Adjuntos (Surtido)")

                with st.form(key="modify_pedido_form_inner", clear_on_submit=True):
                    new_modificacion_surtido_input = st.text_area(
                        "✍️ Notas de Modificación/Surtido",
                        value=current_modificacion_surtido_value,
                        height=100,
                        key="new_modificacion_surtido_input"
                    )
                    # MODIFICATION 2: Rename "Notas Adicionales" to "Notas de Almacén"
                    new_notas_input = st.text_area(
                        "✍️ Notas de Almacén",
                        value=current_notas_value,
                        height=100,
                        key="new_notas_input"
                    )

                    uploaded_files_surtido = st.file_uploader(
                        "📎 Subir Archivos para Modificación/Surtido",
                        type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
                        accept_multiple_files=True,
                        key="uploaded_files_surtido"
                    )

                    modify_button = st.form_submit_button("💾 Guardar Cambios")

                    if modify_button:
                        message_placeholder_tab2.empty()
                        try:
                            headers = worksheet.row_values(1)

                            if 'Modificacion_Surtido' not in headers:
                                message_placeholder_tab2.error("Error: La columna 'Modificacion_Surtido' no se encuentra en el Google Sheet. Por favor, verifica el nombre EXACTO.")
                                st.stop()
                            if 'Notas' not in headers:
                                message_placeholder_tab2.error("Error: La columna 'Notas' no se encuentra en el Google Sheet. Por favor, verifica el nombre EXACTO.")
                                st.stop()
                            if 'Estado_Pago' not in headers:
                                message_placeholder_tab2.error("Error: La columna 'Estado_Pago' no se encuentra en el Google Sheet. Por favor, verifica el nombre EXACTO.")
                                st.stop()
                            if 'Adjuntos' not in headers:
                                message_placeholder_tab2.error("Error: La columna 'Adjuntos' no se encuentra en el Google Sheet. Por favor, verifica el nombre EXACTO.")
                                st.stop()
                            if 'Adjuntos_Surtido' not in headers:
                                message_placeholder_tab2.error("Error: La columna 'Adjuntos_Surtido' no se encuentra en el Google Sheet. Por favor, agrégala o verifica el nombre EXACTO.")
                                st.stop()


                            df_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_order_id].index[0]
                            gsheet_row_index = df_row_index + 2

                            modificacion_surtido_col_idx = headers.index('Modificacion_Surtido') + 1
                            notas_col_idx = headers.index('Notas') + 1
                            estado_pago_col_idx = headers.index('Estado_Pago') + 1
                            adjuntos_col_idx = headers.index('Adjuntos') + 1
                            adjuntos_surtido_col_idx = headers.index('Adjuntos_Surtido') + 1

                            changes_made = False

                            if new_modificacion_surtido_input != current_modificacion_surtido_value:
                                worksheet.update_cell(gsheet_row_index, modificacion_surtido_col_idx, new_modificacion_surtido_input)
                                changes_made = True

                            if new_notas_input != current_notas_value:
                                worksheet.update_cell(gsheet_row_index, notas_col_idx, new_notas_input)
                                changes_made = True

                            # NEW: Handle S3 upload for 'Adjuntos_Surtido'
                            new_adjuntos_surtido_urls = []
                            if uploaded_files_surtido:
                                for uploaded_file in uploaded_files_surtido:
                                    file_extension = os.path.splitext(uploaded_file.name)[1]
                                    s3_key = f"{selected_order_id}/surtido_{uploaded_file.name.replace(' ', '_').replace(file_extension, '')}_{uuid.uuid4().hex[:4]}{file_extension}"

                                    success, file_url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, uploaded_file, s3_key)
                                    if success:
                                        new_adjuntos_surtido_urls.append(file_url)
                                        changes_made = True
                                    else:
                                        message_placeholder_tab2.warning(f"⚠️ Falló la subida de '{uploaded_file.name}' para surtido. Continuará con otros cambios.")

                            if new_adjuntos_surtido_urls:
                                updated_adjuntos_surtido_list = current_adjuntos_surtido_list + new_adjuntos_surtido_urls
                                updated_adjuntos_surtido_str = ", ".join(updated_adjuntos_surtido_list)
                                worksheet.update_cell(gsheet_row_index, adjuntos_surtido_col_idx, updated_adjuntos_surtido_str)
                                changes_made = True
                                message_placeholder_tab2.info(f"📎 Nuevos archivos para Surtido subidos a S3: {', '.join([os.path.basename(url) for url in new_adjuntos_surtido_urls])}")

                            if changes_made:
                                message_placeholder_tab2.success(f"✅ Pedido `{selected_order_id}` actualizado con éxito.")
                                st.session_state.show_success_message = True
                                st.session_state.last_updated_order_id = selected_order_id
                            else:
                                message_placeholder_tab2.info("ℹ️ No se detectaron cambios para guardar.")
                                st.session_state.show_success_message = False

                            st.rerun()

                        except Exception as e:
                            message_placeholder_tab2.error(f"❌ Error al guardar los cambios en el Google Sheet: {e}")
                            message_placeholder_tab2.info("ℹ️ Verifica que la cuenta de servicio tenga permisos de escritura en la hoja y que las columnas sean correctas. Asegúrate de que todas las columnas usadas existen en la primera fila de tu Google Sheet.")

    if 'show_success_message' in st.session_state and st.session_state.show_success_message:
        message_placeholder_tab2.success(f"✅ Pedido `{st.session_state.last_updated_order_id}` actualizado con éxito.")
        del st.session_state.show_success_message
        del st.session_state.last_updated_order_id


# --- TAB 3: PENDING PROOF OF PAYMENT ---
with tab3:
    st.header("🧾 Pedidos Pendientes de Comprobante")

    df_pedidos_comprobante = pd.DataFrame()
    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        headers = worksheet.row_values(1)
        if headers:
            df_pedidos_comprobante = pd.DataFrame(worksheet.get_all_records())
            if 'Folio_Factura' in df_pedidos_comprobante.columns:
                df_pedidos_comprobante['Folio_Factura'] = df_pedidos_comprobante['Folio_Factura'].astype(str).replace('nan', '')
            if 'Vendedor_Registro' in df_pedidos_comprobante.columns:
                df_pedidos_comprobante['Vendedor_Registro'] = df_pedidos_comprobante['Vendedor_Registro'].apply(
                    lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
        else:
            st.warning("No se pudieron cargar los encabezados del Google Sheet. Asegúrate de que la primera fila no esté vacía.")

    except Exception as e:
        st.error(f"❌ Error al cargar pedidos para comprobante: {e}")

    if df_pedidos_comprobante.empty:
        st.info("No hay pedidos registrados.")
    else:
        filtered_pedidos_comprobante = df_pedidos_comprobante.copy()

        col3_tab3, col4_tab3 = st.columns(2)
        with col3_tab3:
            if 'Vendedor_Registro' in filtered_pedidos_comprobante.columns:
                unique_vendedores_comp = ["Todos"] + sorted(filtered_pedidos_comprobante['Vendedor_Registro'].unique().tolist())
                selected_vendedor_comp = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_comp,
                    key="comprobante_vendedor_filter"
                )
                if selected_vendedor_comp != "Todos":
                    filtered_pedidos_comprobante = filtered_pedidos_comprobante[filtered_pedidos_comprobante['Vendedor_Registro'] == selected_vendedor_comp]
            else:
                st.warning("La columna 'Vendedor_Registro' no se encontró para aplicar el filtro de vendedor.")

        with col4_tab3:
            if 'Tipo_Envio' in filtered_pedidos_comprobante.columns:
                unique_tipos_envio_comp = ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"]
                selected_tipo_envio_comp = st.selectbox(
                    "Filtrar por Tipo de Envío:",
                    options=unique_tipos_envio_comp,
                    key="comprobante_tipo_envio_filter"
                )
                if selected_tipo_envio_comp != "Todos":
                    filtered_pedidos_comprobante = filtered_pedidos_comprobante[filtered_pedidos_comprobante['Tipo_Envio'] == selected_tipo_envio_comp]
            else:
                st.warning("La columna 'Tipo_Envio' no se encontró para aplicar el filtro de tipo de envío.")


        if 'Estado_Pago' in filtered_pedidos_comprobante.columns and 'Adjuntos' in filtered_pedidos_comprobante.columns:
            # Modified condition for pending comprobante: check for '🔴 No Pagado' and if 'comprobante' substring is NOT in any Adjuntos URL
            pedidos_sin_comprobante = filtered_pedidos_comprobante[
                (filtered_pedidos_comprobante['Estado_Pago'] == '🔴 No Pagado') &
                (~filtered_pedidos_comprobante['Adjuntos'].astype(str).str.contains('comprobante', na=False, case=False))
            ].copy()
        else:
            st.warning("Las columnas 'Estado_Pago' o 'Adjuntos' no se encontraron en el Google Sheet. No se puede filtrar por comprobantes.")
            pedidos_sin_comprobante = pd.DataFrame()

        if pedidos_sin_comprobante.empty:
            st.success("¡🎉 Todos los pedidos pagados tienen comprobante o están en un estado diferente!")
        else:
            st.warning(f"¡Hay {len(pedidos_sin_comprobante)} pedidos pendientes de comprobante!")

            desired_columns = [
                'ID_Pedido', 'Cliente', 'Folio_Factura', 'Vendedor_Registro', 'Tipo_Envio', 'Turno',
                'Fecha_Entrega', 'Estado', 'Estado_Pago', 'Comentario',
                'Notas', 'Modificacion_Surtido', 'Adjuntos', 'Adjuntos_Surtido'
            ]

            existing_columns_to_display = [col for col in desired_columns if col in pedidos_sin_comprobante.columns]

            if existing_columns_to_display:
                st.dataframe(pedidos_sin_comprobante[existing_columns_to_display].sort_values(by='Fecha_Entrega'), use_container_width=True, hide_index=True)
            else:
                st.warning("No hay columnas relevantes para mostrar en la tabla de pedidos pendientes.")


            st.markdown("---")
            st.subheader("Subir Comprobante para un Pedido")

            pedidos_sin_comprobante['display_label'] = pedidos_sin_comprobante.apply(lambda row:
                f"📄 {row.get('Folio_Factura', 'N/A') if row.get('Folio_Factura', 'N/A') != '' else row.get('ID_Pedido', 'N/A')} - "
                f"{row.get('Cliente', 'N/A')} - {row.get('Estado', 'N/A')}", axis=1
            )
            pedidos_sin_comprobante = pedidos_sin_comprobante.sort_values(
                by=['Folio_Factura', 'ID_Pedido'],
                key=lambda x: x.astype(str).str.lower(),
                na_position='last'
            )


            selected_pending_order_display = st.selectbox(
                "📝 Seleccionar Pedido para Subir Comprobante",
                pedidos_sin_comprobante['display_label'].tolist(),
                key="select_pending_order_comprobante"
            )

            if selected_pending_order_display:
                selected_pending_order_id = pedidos_sin_comprobante[pedidos_sin_comprobante['display_label'] == selected_pending_order_display]['ID_Pedido'].iloc[0]
                selected_pending_row_data = pedidos_sin_comprobante[pedidos_sin_comprobante['ID_Pedido'] == selected_pending_order_id].iloc[0]

                st.info(f"Subiendo comprobante para el pedido: Folio `{selected_pending_row_data.get('Folio_Factura', 'N/A')}` (ID `{selected_pending_order_id}`) del cliente `{selected_pending_row_data.get('Cliente', 'N/A')}`")

                with st.form(key=f"upload_comprobante_form_{selected_pending_order_id}"):
                    comprobante_file_for_pending = st.file_uploader(
                        "💲 Comprobante de Pago",
                        type=["pdf", "jpg", "jpeg", "png"],
                        key=f"comprobante_uploader_pending_{selected_pending_order_id}"
                    )
                    submit_comprobante_button = st.form_submit_button("✅ Subir Comprobante y Actualizar Estado")

                    if submit_comprobante_button:
                        if comprobante_file_for_pending:
                            try:
                                headers = worksheet.row_values(1)
                                df_row_index = df_pedidos_comprobante[df_pedidos_comprobante['ID_Pedido'] == selected_pending_order_id].index[0]
                                gsheet_row_index = df_row_index + 2

                                file_extension_cp = os.path.splitext(comprobante_file_for_pending.name)[1]
                                # Create a unique S3 key for the comprobante
                                s3_key_cp = f"{selected_pending_order_id}/comprobante_{selected_pending_order_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{file_extension_cp}"

                                success_cp, file_url_cp = upload_file_to_s3(s3_client, S3_BUCKET_NAME, comprobante_file_for_pending, s3_key_cp)

                                if success_cp:
                                    adjuntos_col_idx = headers.index('Adjuntos') + 1
                                    current_adjuntos_str = worksheet.cell(gsheet_row_index, adjuntos_col_idx).value
                                    current_adjuntos_list = [f.strip() for f in current_adjuntos_str.split(',') if f.strip()]

                                    if file_url_cp not in current_adjuntos_list: # Store the URL
                                        current_adjuntos_list.append(file_url_cp)
                                    updated_adjuntos_str = ", ".join(current_adjuntos_list)
                                    worksheet.update_cell(gsheet_row_index, adjuntos_col_idx, updated_adjuntos_str)

                                    estado_pago_col_idx = headers.index('Estado_Pago') + 1
                                    worksheet.update_cell(gsheet_row_index, estado_pago_col_idx, "✅ Pagado")

                                    st.success(f"🎉 Comprobante para el pedido `{selected_pending_order_id}` subido a S3 y estado actualizado a 'Pagado' con éxito!")
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error("❌ Falló la subida del comprobante de pago.")

                            except Exception as e:
                                st.error(f"❌ Error al procesar el comprobante para el pedido: {e}")
                                st.info("ℹ️ Revisa tu conexión a internet o los permisos de la cuenta de servicio.")
                        else:
                            st.warning("⚠️ Por favor, sube un archivo de comprobante antes de guardar.")


# --- TAB 4: DOWNLOAD DATA ---
with tab4:
    st.header("⬇️ Descargar Datos de Pedidos")

    df_all_pedidos = pd.DataFrame()
    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        headers = worksheet.row_values(1)
        if headers:
            df_all_pedidos = pd.DataFrame(worksheet.get_all_records())

            if 'Fecha_Entrega' in df_all_pedidos.columns:
                df_all_pedidos['Fecha_Entrega'] = pd.to_datetime(df_all_pedidos['Fecha_Entrega'], errors='coerce')

            if 'Vendedor_Registro' in df_all_pedidos.columns:
                df_all_pedidos['Vendedor_Registro'] = df_all_pedidos['Vendedor_Registro'].apply(
                    lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
            else:
                st.warning("La columna 'Vendedor_Registro' no se encontró en el Google Sheet para el filtrado. Asegúrate de que exista y esté correctamente nombrada.")

            if 'Folio_Factura' in df_all_pedidos.columns:
                df_all_pedidos['Folio_Factura'] = df_all_pedidos['Folio_Factura'].astype(str).replace('nan', '')
            else:
                 st.warning("La columna 'Folio_Factura' no se encontró en el Google Sheet. No se podrá mostrar en la vista previa.")

        else:
            st.warning("No se pudieron cargar los encabezados del Google Sheet. Asegúrate de que la primera fila no esté vacía.")

    except Exception as e:
        st.error(f"❌ Error al cargar datos para descarga: {e}")
        st.info("Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados y que la API de Google Sheets está habilitada.")

    if df_all_pedidos.empty:
        st.info("No hay datos de pedidos para descargar.")
    else:
        st.markdown("---")
        st.subheader("Opciones de Filtro")

        time_filter = st.radio(
            "Selecciona un rango de tiempo:",
            ("Todos los datos", "Últimas 24 horas", "Últimos 7 días", "Últimos 30 días"),
            key="download_time_filter"
        )

        filtered_df_download = df_all_pedidos.copy()

        if time_filter != "Todos los datos" and 'Fecha_Entrega' in filtered_df_download.columns:
            current_time = datetime.now()
            # MODIFICATION 3: Convert Fecha_Entrega to date only for comparison
            filtered_df_download['Fecha_Solo_Fecha'] = filtered_df_download['Fecha_Entrega'].dt.date

            if time_filter == "Últimas 24 horas":
                start_datetime = current_time - timedelta(hours=24)
                filtered_df_download = filtered_df_download[filtered_df_download['Fecha_Entrega'] >= start_datetime]
            else:
                if time_filter == "Últimos 7 días":
                    start_date = current_time.date() - timedelta(days=7)
                elif time_filter == "Últimos 30 días":
                    start_date = current_time.date() - timedelta(days=30)

                filtered_df_download = filtered_df_download[filtered_df_download['Fecha_Solo_Fecha'] >= start_date]

            filtered_df_download = filtered_df_download.drop(columns=['Fecha_Solo_Fecha'])


        if 'Vendedor_Registro' in df_all_pedidos.columns:
            unique_vendedores_en_df = set(filtered_df_download['Vendedor_Registro'].unique())

            options_for_selectbox = ["Todos"]
            for vendedor_nombre in VENDEDORES_LIST:
                if vendedor_nombre in unique_vendedores_en_df:
                    options_for_selectbox.append(vendedor_nombre)

            if 'Otro/Desconocido' in unique_vendedores_en_df and 'Otro/Desconocido' not in options_for_selectbox:
                options_for_selectbox.append('Otro/Desconocido')

            if 'N/A' in unique_vendedores_en_df and 'N/A' not in options_for_selectbox:
                options_for_selectbox.append('N/A')

            selected_vendedor = st.selectbox(
                "Filtrar por Vendedor:",
                options=options_for_selectbox,
                key="download_vendedor_filter_tab4_final"
            )

            if selected_vendedor != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Vendedor_Registro'] == selected_vendedor]
        else:
            st.warning("La columna 'Vendedor_Registro' no está disponible en los datos cargados para aplicar este filtro. Por favor, asegúrate de que el nombre de la columna en tu Google Sheet sea 'Vendedor_Registro'.")

        if 'Tipo_Envio' in filtered_df_download.columns:
            unique_tipos_envio_download = ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"]
            selected_tipo_envio_download = st.selectbox(
                "Filtrar por Tipo de Envío:",
                options=unique_tipos_envio_download,
                key="download_tipo_envio_filter"
            )
            if selected_tipo_envio_download != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Tipo_Envio'] == selected_tipo_envio_download]
        else:
            st.warning("La columna 'Tipo_Envio' no se encontró para aplicar el filtro de tipo de envío.")


        if 'Estado' in filtered_df_download.columns:
            unique_estados = ["Todos"] + list(filtered_df_download['Estado'].dropna().unique())
            selected_estado = st.selectbox("Filtrar por Estado:", unique_estados, key="download_estado_filter_tab4")
            if selected_estado != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Estado'] == selected_estado]

        st.markdown("---")
        st.subheader("Vista Previa de Datos a Descargar")

        # MODIFICATION 3: Format 'Fecha_Entrega' for display
        display_df = filtered_df_download[['Folio_Factura', 'ID_Pedido', 'Cliente', 'Estado', 'Vendedor_Registro', 'Tipo_Envio', 'Fecha_Entrega']].copy()
        if 'Fecha_Entrega' in display_df.columns:
            display_df['Fecha_Entrega'] = display_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d')

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        if not filtered_df_download.empty:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # MODIFICATION 3: Ensure Fecha_Entrega is formatted as date string in Excel
                excel_df = filtered_df_download.copy()
                if 'Fecha_Entrega' in excel_df.columns:
                    excel_df['Fecha_Entrega'] = excel_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d')
                excel_df.to_excel(writer, index=False, sheet_name='Pedidos_Filtrados')
            processed_data = output.getvalue()

            st.download_button(
                label="📥 Descargar Excel Filtrado",
                data=processed_data,
                file_name=f"pedidos_filtrados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Haz clic para descargar los datos de la tabla mostrada arriba en formato Excel."
            )
        else:
            st.info("No hay datos que coincidan con los filtros seleccionados para descargar.")
