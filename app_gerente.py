import streamlit as st
import pandas as pd
import boto3
import gspread
import pdfplumber
import json
import re
import unicodedata
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURACIÓN DE STREAMLIT ---
st.set_page_config(page_title="🔍 Buscador de Guías y Descargas", layout="wide")
st.title("🔍 Buscador de Pedidos por Guía o Cliente")

# --- CREDENCIALES DESDE SECRETS ---
try:
    credentials_dict = json.loads(st.secrets["gsheets"]["google_credentials"])
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gspread_client = gspread.authorize(creds)
except Exception as e:
    st.error(f"❌ Error al autenticar con Google Sheets: {e}")
    st.stop()

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"]["aws_region"]
    )
    S3_BUCKET = st.secrets["aws"]["s3_bucket_name"]
except Exception as e:
    st.error(f"❌ Error al autenticar con AWS S3: {e}")
    st.stop()

# --- FUNCIONES ---
@st.cache_data(ttl=300)
def cargar_pedidos():
    sheet = gspread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
    data = sheet.get_all_records()
    return pd.DataFrame(data)

def obtener_prefijo_s3(pedido_id):
    posibles_prefijos = [
        f"{pedido_id}/", f"adjuntos_pedidos/{pedido_id}/",
        f"adjuntos_pedidos/{pedido_id}", f"{pedido_id}"
    ]
    for prefix in posibles_prefijos:
        try:
            respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
            if "Contents" in respuesta:
                return prefix if prefix.endswith("/") else prefix + "/"
        except Exception:
            continue
    return None

def obtener_archivos_pdf_validos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        archivos = respuesta.get("Contents", [])
        return [f for f in archivos if f["Key"].lower().endswith(".pdf") and any(x in f["Key"].lower() for x in ["guia", "guía", "descarga"])]
    except Exception as e:
        st.error(f"❌ Error al listar archivos en S3 para prefijo {prefix}: {e}")
        return []

def obtener_todos_los_archivos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        return respuesta.get("Contents", [])
    except Exception:
        return []

def extraer_texto_pdf(s3_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        with pdfplumber.open(BytesIO(response["Body"].read())) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        return f"[ERROR AL LEER PDF]: {e}"

def generar_url_s3(s3_key):
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET, 'Key': s3_key},
        ExpiresIn=3600
    )

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower()

# --- INTERFAZ ---
tabs = st.tabs(["🔍 Buscar Pedido", "✏️ Modificar Pedido"])
with tabs[0]:
    modo_busqueda = st.radio("Selecciona el modo de búsqueda:", ["🔢 Por número de guía", "🧑 Por cliente"], key="modo_busqueda_radio")

    if modo_busqueda == "🔢 Por número de guía":
        keyword = st.text_input("📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:")
        buscar_btn = st.button("🔎 Buscar")

    elif modo_busqueda == "🧑 Por cliente":
        keyword = st.text_input("🧑 Ingresa el nombre del cliente a buscar (sin importar mayúsculas ni acentos):")
        buscar_btn = st.button("🔍 Buscar Pedido del Cliente")

        cliente_normalizado = normalizar(keyword.strip()) if keyword else ""



# --- EJECUCIÓN DE LA BÚSQUEDA ---
    if buscar_btn:
        if modo_busqueda == "🔢 Por número de guía":
            st.info("🔄 Buscando, por favor espera... puede tardar unos segundos...")
        df_pedidos = cargar_pedidos()
        resultados = []

        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos = df_pedidos.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)

        for _, row in df_pedidos.iterrows():
            pedido_id = str(row.get("ID_Pedido", "")).strip()
            if not pedido_id:
                continue

            if modo_busqueda == "🧑 Por cliente":
                cliente_row = row.get("Cliente", "").strip()
                if not cliente_row:
                    continue
                cliente_row_normalizado = normalizar(cliente_row)
                if cliente_normalizado not in cliente_row_normalizado:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_coincidentes = []  # no se buscan coincidencias
                todos_los_archivos = obtener_todos_los_archivos(prefix)

            elif modo_busqueda == "🔢 Por número de guía":
                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_validos = obtener_archivos_pdf_validos(prefix)
                archivos_coincidentes = []

                for archivo in archivos_validos:
                    key = archivo["Key"]
                    texto = extraer_texto_pdf(key)

                    clave = keyword.strip()
                    clave_sin_espacios = clave.replace(" ", "")
                    texto_limpio = texto.replace(" ", "").replace("\n", "")

                    coincide = (
                        clave in texto
                        or clave_sin_espacios in texto_limpio
                        or re.search(re.escape(clave), texto_limpio)
                        or re.search(re.escape(clave_sin_espacios), texto_limpio)
                    )

                    if coincide:
                        waybill_match = re.search(r"WAYBILL[\s:]*([0-9 ]{8,})", texto, re.IGNORECASE)
                        if waybill_match:
                            st.code(f"📦 WAYBILL detectado: {waybill_match.group(1)}")

                        archivos_coincidentes.append((key, generar_url_s3(key)))
                        todos_los_archivos = obtener_todos_los_archivos(prefix)
                        break  # detener búsqueda tras encontrar coincidencia
                else:
                    continue  # ningún PDF coincidió

            else:
                continue  # modo no reconocido

            # Una vez tenemos los archivos del pedido
            comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
            facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
            otros = [
                f for f in todos_los_archivos
                if f not in comprobantes and f not in facturas and
                (modo_busqueda == "🧑 Por cliente" or f["Key"] != archivos_coincidentes[0][0])
            ]

            comprobantes_links = [(f["Key"], generar_url_s3(f["Key"])) for f in comprobantes]
            facturas_links = [(f["Key"], generar_url_s3(f["Key"])) for f in facturas]
            otros_links = [(f["Key"], generar_url_s3(f["Key"])) for f in otros]

            resultados.append({
                "ID_Pedido": pedido_id,
                "Cliente": row.get("Cliente", ""),
                "Estado": row.get("Estado", ""),
                "Vendedor": row.get("Vendedor_Registro", ""),
                "Folio": row.get("Folio_Factura", ""),
                "Hora_Registro": row.get("Hora_Registro", ""),  # 🆕 Agregamos este campo
                "Coincidentes": archivos_coincidentes,
                "Comprobantes": comprobantes_links,
                "Facturas": facturas_links,
                "Otros": otros_links
            })


            if modo_busqueda == "🔢 Por número de guía":
                break  # Solo detener si es búsqueda por guía

        st.markdown("---")
        if resultados:
            st.success(f"✅ Se encontraron coincidencias en {len(resultados)} pedido(s).")

            for res in resultados:
                st.markdown(f"### 🤝 {res['Cliente']}")
                st.markdown(f"📄 **Folio:** `{res['Folio']}`  |  🔍 **Estado:** `{res['Estado']}`  |  🧑‍💼 **Vendedor:** `{res['Vendedor']}`  |  🕒 **Hora:** `{res['Hora_Registro']}`")

                with st.expander("📁 Archivos del Pedido", expanded=True):
                    if res["Coincidentes"]:
                        st.markdown("#### 🔍 Guías:")
                        for key, url in res["Coincidentes"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [🔍 {nombre}]({url})")

                    if res["Comprobantes"]:
                        st.markdown("#### 🧾 Comprobantes:")
                        for key, url in res["Comprobantes"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📄 {nombre}]({url})")

                    if res["Facturas"]:
                        st.markdown("#### 📁 Facturas:")
                        for key, url in res["Facturas"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📄 {nombre}]({url})")

                    if res["Otros"]:
                        st.markdown("#### 📂 Otros Archivos:")
                        for key, url in res["Otros"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📌 {nombre}]({url})")

        else:
            mensaje = (
                "⚠️ No se encontraron coincidencias en ningún archivo PDF."
                if modo_busqueda == "🔢 Por número de guía"
                else "⚠️ No se encontraron pedidos para el cliente ingresado."
            )
            st.warning(mensaje)


CONTRASENA_ADMIN = "Ceci"  # puedes cambiar esta contraseña si lo deseas

# --- PESTAÑA DE MODIFICACIÓN DE PEDIDOS CON CONTRASEÑA ---
with tabs[1]:
    st.header("✏️ Modificar Pedido Existente")

    if "acceso_modificacion" not in st.session_state:
        st.session_state.acceso_modificacion = False

    if not st.session_state.acceso_modificacion:
        contrasena_ingresada = st.text_input("🔑 Ingresa la contraseña para modificar pedidos:", type="password")
        if st.button("🔓 Verificar Contraseña"):
            if contrasena_ingresada == CONTRASENA_ADMIN:
                st.session_state.acceso_modificacion = True
                st.success("✅ Acceso concedido.")
                st.rerun()
            else:
                st.error("❌ Contraseña incorrecta.")
        st.stop()

    df = cargar_pedidos()
    df = df[df["ID_Pedido"].notna()]
    df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors='coerce')
    df = df.sort_values(by="Hora_Registro", ascending=False)
    df = df.sort_values(by="Hora_Registro", ascending=False)
    pedido_sel = None  # ✅ evitar NameError si no se selecciona nada aún


    usar_busqueda = st.checkbox("🔍 Buscar por nombre de cliente (activar para ocultar los últimos 10 pedidos)")

    if usar_busqueda:
        st.markdown("### 🔍 Buscar Pedido por Cliente")
        cliente_buscado = st.text_input("👤 Escribe el nombre del cliente:")
        cliente_normalizado = normalizar(cliente_buscado)
        coincidencias = []

        if cliente_buscado:
            for _, row_ in df.iterrows():
                cliente_row = row_.get("Cliente", "").strip()
                if not cliente_row:
                    continue
                cliente_row_normalizado = normalizar(cliente_row)
                if cliente_normalizado in cliente_row_normalizado:
                    coincidencias.append(row_)

            if not coincidencias:
                st.warning("⚠️ No se encontraron pedidos para ese cliente.")
                st.stop()
            elif len(coincidencias) == 1:
                pedido_sel = coincidencias[0]["ID_Pedido"]
            else:
                opciones = [
                    f"{r['ID_Pedido']} – {r['Cliente']} – {r['Estado']} – {r['Vendedor_Registro']} – {r['Hora_Registro'].strftime('%d/%m %H:%M')}"
                    for r in coincidencias
                ]
                seleccion = st.selectbox("👥 Se encontraron múltiples pedidos, selecciona uno:", opciones)
                pedido_sel = seleccion.split(" – ")[0]
    else:
        ultimos_10 = df.head(10)
        st.markdown("### 🕒 Últimos 10 Pedidos Registrados")
        ultimos_10["display"] = ultimos_10.apply(
            lambda row: f"👤 {row['Cliente']} – 🔍 {row['Estado']} – 🧑‍💼 {row['Vendedor_Registro']} – 🕒 {row['Hora_Registro'].strftime('%d/%m %H:%M')}",
            axis=1
        )
        pedido_rapido_label = st.selectbox(
            "⬇️ Selecciona uno de los pedidos recientes:",
            ultimos_10["display"].tolist()
        )
        pedido_sel = ultimos_10[ultimos_10["display"] == pedido_rapido_label]["ID_Pedido"].values[0]

    # --- Cargar datos del pedido seleccionado ---
    st.markdown("---")

    if pedido_sel is None:
        st.warning("⚠️ No se ha seleccionado ningún pedido válido.")
        st.stop()

    st.markdown(f"📦 **Pedido seleccionado:** `{pedido_sel}`")

    row = df[df["ID_Pedido"] == pedido_sel].iloc[0]
    gspread_row_idx = df[df["ID_Pedido"] == pedido_sel].index[0] + 2  # índice real en hoja


    # --- CAMPOS MODIFICABLES ---
    vendedores = [
        "ALEJANDRO RODRIGUEZ",
        "ANA KAREN ORTEGA MAHUAD",
        "DANIELA LOPEZ RAMIREZ",
        "EDGAR ORLANDO GOMEZ VILLAGRAN",
        "GLORIA MICHELLE GARCIA TORRES", 
        "GRISELDA CAROLINA SANCHEZ GARCIA",
        "HECTOR DEL ANGEL AREVALO ALCALA",
        "JOSELIN TRUJILLO PATRACA",
        "NORA ALEJANDRA MARTINEZ MORENO",
        "PAULINA TREJO"
    ]
    vendedor_actual = row.get("Vendedor_Registro", "")
    indice_vendedor = vendedores.index(vendedor_actual) if vendedor_actual in vendedores else 0

    nuevo_vendedor = st.selectbox("🧑‍💼 Vendedor", vendedores, index=indice_vendedor)


    tipo_envio_actual = row["Tipo_Envio"]
    tipo_envio = st.selectbox("🚚 Tipo de Envío", ["📍 Pedido Local", "🚚 Pedido Foráneo"], index=0 if "Local" in tipo_envio_actual else 1)

    turno_actual = row.get("Turno", "")
    if tipo_envio == "📍 Pedido Local":
        nuevo_turno = st.selectbox("⏰ Turno", ["☀ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"], index=0 if turno_actual not in ["🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"] else
            ["☀ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"].index(turno_actual))
    else:
        nuevo_turno = ""

    completado = row.get("Completados_Limpiado", "")
    mostrar_en_app_i = st.checkbox("👁 Mostrar en app_i", value=(completado.strip().lower() == "sí"))

    if st.button("✅ Aplicar Cambios"):
        hoja = gspread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Vendedor_Registro")+1, nuevo_vendedor)
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Tipo_Envio")+1, tipo_envio)
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Turno")+1, nuevo_turno)
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Completados_Limpiado")+1, "sí" if mostrar_en_app_i else "")
        st.success("✅ Cambios aplicados correctamente.")
