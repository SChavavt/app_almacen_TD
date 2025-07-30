import streamlit as st
import pandas as pd
import boto3
import gspread
import pdfplumber
import json
import re
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
        except:
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
    except Exception as e:
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

# --- INTERFAZ ---
tabs = st.tabs(["🔍 Buscar Pedido", "✏️ Modificar Pedido"])
with tabs[0]:
    modo_busqueda = st.radio("Selecciona el modo de búsqueda:", ["🔢 Por número de guía", "🧑 Por cliente"], key="modo_busqueda_radio")
    st.title("🔍 Buscador de Pedidos por Guía o Cliente")

    if modo_busqueda == "🔢 Por número de guía":
        keyword = st.text_input("📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:")
        buscar_btn = st.button("🔎 Buscar")

    elif modo_busqueda == "🧑 Por cliente":
        df_pedidos = cargar_pedidos()
        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos = df_pedidos.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)

        clientes_unicos = df_pedidos['Cliente'].dropna().unique().tolist()
        clientes_unicos.sort()
        cliente_seleccionado = st.selectbox("🧑 Selecciona el cliente:", clientes_unicos)
        buscar_btn = st.button("🔎 Buscar Pedido del Cliente")
        keyword = None  # para no interferir con el otro modo



# --- EJECUCIÓN DE LA BÚSQUEDA ---
if buscar_btn:
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

        if modo_busqueda == "🔢 Por número de guía":
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
                    break
            else:
                continue  # no hubo match, pasa al siguiente pedido

        elif modo_busqueda == "🧑 Por cliente":
            if row.get("Cliente", "").strip() != cliente_seleccionado.strip():
                continue
            prefix = obtener_prefijo_s3(pedido_id)
            if not prefix:
                continue
            archivos_coincidentes = []
            todos_los_archivos = obtener_todos_los_archivos(prefix)
            # No hay que buscar coincidencia, solo mostrar adjuntos
            break

        # Una vez tenemos los archivos del pedido
        comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
        facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
        otros = [f for f in todos_los_archivos if f not in comprobantes and f not in facturas and (modo_busqueda == "🧑 Por cliente" or f["Key"] != archivos_coincidentes[0][0])]

        comprobantes_links = [(f["Key"], generar_url_s3(f["Key"])) for f in comprobantes]
        facturas_links = [(f["Key"], generar_url_s3(f["Key"])) for f in facturas]
        otros_links = [(f["Key"], generar_url_s3(f["Key"])) for f in otros]

        resultados.append({
            "ID_Pedido": pedido_id,
            "Cliente": row.get("Cliente", ""),
            "Estado": row.get("Estado", ""),
            "Vendedor": row.get("Vendedor_Registro", ""),
            "Folio": row.get("Folio_Factura", ""),
            "Coincidentes": archivos_coincidentes,
            "Comprobantes": comprobantes_links,
            "Facturas": facturas_links,
            "Otros": otros_links
        })
        break  # detener búsqueda tras encontrar uno



    st.markdown("---")
    if resultados:
        st.success(f"✅ Se encontraron coincidencias en {len(resultados)} pedido(s).")

        for res in resultados:
            st.markdown(f"### 📦 Pedido **{res['ID_Pedido']}** – 🤝 {res['Cliente']}")
            st.markdown(f"📄 **Folio:** `{res['Folio']}`  |  🔍 **Estado:** `{res['Estado']}`  |  🧑‍💼 **Vendedor:** `{res['Vendedor']}`")

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
        st.warning("⚠️ No se encontraron coincidencias en ningún archivo PDF.")


# --- PESTAÑA DE MODIFICACIÓN DE PEDIDOS ---
with tabs[1]:
    st.header("✏️ Modificar Pedido Existente")

    df = cargar_pedidos()
    df = df[df["ID_Pedido"].notna()]
    df = df.sort_values(by="Hora_Registro", ascending=False)

    pedidos_opciones = df["ID_Pedido"].astype(str).tolist()
    pedido_sel = st.selectbox("📦 Selecciona el pedido a modificar:", pedidos_opciones)

    row = df[df["ID_Pedido"] == pedido_sel].iloc[0]
    gspread_row_idx = df[df["ID_Pedido"] == pedido_sel].index[0] + 2  # índice real en la hoja

    # --- CAMPOS MODIFICABLES ---
    nuevo_vendedor = st.selectbox("🧑‍💼 Vendedor", [
        "ANA KAREN ORTEGA MAHUAD", "NORA ALEJANDRA MARTINEZ MORENO", "BRENDA VANESSA VILLALOBOS GONZALEZ",
        "LUIS MANUEL CORDOVA MARQUEZ", "JOSE ANGEL RANGEL DE LEON", "XIMENA GARZA", "DANIELA CASTILLO"
    ], index=0 if row["Vendedor_Registro"] == "" else
       ["ANA KAREN ORTEGA MAHUAD", "NORA ALEJANDRA MARTINEZ MORENO", "BRENDA VANESSA VILLALOBOS GONZALEZ",
        "LUIS MANUEL CORDOVA MARQUEZ", "JOSE ANGEL RANGEL DE LEON", "XIMENA GARZA", "DANIELA CASTILLO"].index(row["Vendedor_Registro"]))

    tipo_envio_actual = row["Tipo"]
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
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Tipo")+1, tipo_envio)
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Turno")+1, nuevo_turno)
        hoja.update_cell(gspread_row_idx, df.columns.get_loc("Completados_Limpiado")+1, "sí" if mostrar_en_app_i else "")
        st.success("✅ Cambios aplicados correctamente.")
