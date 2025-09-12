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
from urllib.parse import urlparse, unquote
from datetime import datetime, timedelta

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
    AWS_REGION = st.secrets["aws"]["aws_region"]
except Exception as e:
    st.error(f"❌ Error al autenticar con AWS S3: {e}")
    st.stop()

def get_worksheet():
    """Obtiene la hoja de cálculo principal de pedidos."""
    return gspread_client.open_by_key(
        "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
    ).worksheet("datos_pedidos")

# --- FUNCIONES ---
@st.cache_data(ttl=300)
def cargar_pedidos():
    sheet = gspread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    # columnas mínimas que usaremos (incluye modif. y refacturación)
    needed = [
        "ID_Pedido","Hora_Registro","Cliente","Estado","Vendedor_Registro","Folio_Factura",
        "Modificacion_Surtido","Adjuntos_Surtido","Adjuntos_Guia","Adjuntos",
        "Refacturacion_Tipo","Refacturacion_Subtipo","Folio_Factura_Refacturada"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""
    return df
@st.cache_data(ttl=300)
def cargar_casos_especiales():
    """
    Lee la hoja 'casos_especiales' y regresa un DataFrame.
    Si faltan columnas del ejemplo, las crea vacías para evitar KeyError.
    """
    sheet = gspread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("casos_especiales")
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    columnas_ejemplo = [
        "ID_Pedido","Hora_Registro","Vendedor_Registro","Cliente","Folio_Factura","Folio_Factura_Error","Tipo_Envio",
        "Fecha_Entrega","Comentario","Adjuntos","Estado","Resultado_Esperado","Material_Devuelto",
        "Monto_Devuelto","Motivo_Detallado","Area_Responsable","Nombre_Responsable","Fecha_Completado",
        "Completados_Limpiado","Estado_Caso","Hoja_Ruta_Mensajero","Numero_Cliente_RFC","Tipo_Envio_Original",
        "Tipo_Caso","Fecha_Recepcion_Devolucion","Estado_Recepcion","Nota_Credito_URL","Documento_Adicional_URL",
        "Seguimiento",
        "Comentarios_Admin_Devolucion","Modificacion_Surtido","Adjuntos_Surtido","Refacturacion_Tipo",
        "Refacturacion_Subtipo","Folio_Factura_Refacturada","Turno","Hora_Proceso",
        # Campos específicos de garantías
        "Numero_Serie","Fecha_Compra"
    ]
    for c in columnas_ejemplo:
        if c not in df.columns:
            df[c] = ""
    return df


@st.cache_data(ttl=300)
def cargar_todos_los_pedidos():
    """Carga todos los pedidos desde la hoja de cálculo principal."""
    sheet = get_worksheet()
    data = sheet.get_all_records()
    return pd.DataFrame(data)


def partir_urls(value):
    """
    Devuelve una lista de URLs limpia a partir de un string que puede venir
    como JSON, CSV, separado por ; o saltos de línea.
    """
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "n/a"):
        return []
    urls = []
    # Intento JSON
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, str) and it.strip():
                    urls.append(it.strip())
                elif isinstance(it, dict):
                    for k in ("url", "URL", "href", "link"):
                        if k in it and str(it[k]).strip():
                            urls.append(str(it[k]).strip())
        elif isinstance(obj, dict):
            for k in ("url", "URL", "href", "link"):
                if k in obj and str(obj[k]).strip():
                    urls.append(str(obj[k]).strip())
    except Exception:
        # Split por coma, punto y coma o salto de línea
        for p in re.split(r"[,\n;]+", s):
            p = p.strip()
            if p:
                urls.append(p)

    # Quitar duplicados preservando orden
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


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


# --- AWS S3 Helper Functions ---
def upload_file_to_s3(s3_client_param, bucket_name, file_obj, s3_key):
    try:
        put_kwargs = {
            "Bucket": bucket_name,
            "Key": s3_key,
            "Body": file_obj.getvalue(),
        }
        if hasattr(file_obj, "type") and file_obj.type:
            put_kwargs["ContentType"] = file_obj.type
        s3_client_param.put_object(**put_kwargs)
        permanent_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, permanent_url
    except Exception as e:
        st.error(f"❌ Error al subir archivo a S3: {e}")
        return False, None


def extract_s3_key(url_or_key: str) -> str:
    if not isinstance(url_or_key, str):
        return url_or_key
    parsed = urlparse(url_or_key)
    if parsed.scheme and parsed.netloc:
        return unquote(parsed.path.lstrip("/"))
    return url_or_key


def get_s3_file_download_url(s3_client_param, object_key_or_url, expires_in=604800):
    if not s3_client_param or not S3_BUCKET:
        st.error("❌ Configuración de S3 incompleta. Verifica el cliente y el nombre del bucket.")
        return "#"
    try:
        clean_key = extract_s3_key(object_key_or_url)
        return s3_client_param.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": clean_key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        st.error(f"❌ Error al generar URL prefirmada: {e}")
        return "#"

def combinar_urls_existentes(existente, nuevas):
    """Combina listas de URLs respetando el formato previo (JSON o separado por comas/semicolons)."""
    existentes = partir_urls(existente)
    total = existentes + [u for u in nuevas if u not in existentes]
    existente = str(existente).strip()
    if existente.startswith('[') or existente.startswith('{'):
        return json.dumps(total, ensure_ascii=False)
    if ';' in existente:
        return '; '.join(total)
    return ', '.join(total)

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower()


def preparar_resultado_caso(row):
    """Convierte una fila de la hoja `casos_especiales` en un diccionario uniforme."""
    return {
        "__source": "casos",
        "ID_Pedido": str(row.get("ID_Pedido", "")).strip(),
        "Cliente": row.get("Cliente", ""),
        "Vendedor": row.get("Vendedor_Registro", ""),
        "Folio": row.get("Folio_Factura", ""),
        "Folio_Factura_Error": row.get("Folio_Factura_Error", ""),
        "Hora_Registro": row.get("Hora_Registro", ""),
        "Tipo_Envio": row.get("Tipo_Envio", ""),
        "Estado": row.get("Estado", ""),
        "Estado_Caso": row.get("Estado_Caso", ""),
        "Resultado_Esperado": row.get("Resultado_Esperado", ""),
        "Material_Devuelto": row.get("Material_Devuelto", ""),
        "Monto_Devuelto": row.get("Monto_Devuelto", ""),
        "Motivo_Detallado": row.get("Motivo_Detallado", ""),
        "Area_Responsable": row.get("Area_Responsable", ""),
        "Nombre_Responsable": row.get("Nombre_Responsable", ""),
        "Numero_Cliente_RFC": row.get("Numero_Cliente_RFC", ""),
        "Tipo_Envio_Original": row.get("Tipo_Envio_Original", ""),
        "Fecha_Entrega": row.get("Fecha_Entrega", ""),
        "Fecha_Recepcion_Devolucion": row.get("Fecha_Recepcion_Devolucion", ""),
        "Estado_Recepcion": row.get("Estado_Recepcion", ""),
        "Nota_Credito_URL": row.get("Nota_Credito_URL", ""),
        "Documento_Adicional_URL": row.get("Documento_Adicional_URL", ""),
        "Seguimiento": row.get("Seguimiento", ""),
        "Comentarios_Admin_Devolucion": row.get("Comentarios_Admin_Devolucion", ""),
        "Turno": row.get("Turno", ""),
        "Hora_Proceso": row.get("Hora_Proceso", ""),
        "Numero_Serie": row.get("Numero_Serie", ""),
        "Fecha_Compra": row.get("Fecha_Compra", ""),
        # 🛠 Modificación de surtido
        "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
        "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
        # ♻️ Refacturación
        "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo", "")).strip(),
        "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo", "")).strip(),
        "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada", "")).strip(),
        # Archivos del caso
        "Adjuntos_urls": partir_urls(row.get("Adjuntos", "")),
        "Guia_url": str(row.get("Hoja_Ruta_Mensajero", "")).strip(),
    }


def render_caso_especial(res):
    """Renderiza en pantalla la información de un caso especial."""
    titulo = f"🧾 Caso Especial – {res.get('Tipo_Envio','') or 'N/A'}"
    st.markdown(f"### {titulo}")

    tipo_envio_val = str(res.get('Tipo_Envio',''))
    is_devolucion = (tipo_envio_val.strip() == "🔁 Devolución")
    is_garantia = "garant" in tipo_envio_val.lower()
    if is_devolucion:
        folio_nuevo = res.get("Folio","") or "N/A"
        folio_error = res.get("Folio_Factura_Error","") or "N/A"
        st.markdown(
            f"📄 **Folio Nuevo:** `{folio_nuevo}`  |  📄 **Folio Error:** `{folio_error}`  |  "
            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
        )
    else:
        st.markdown(
            f"📄 **Folio:** `{res.get('Folio','') or 'N/A'}`  |  "
            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
        )

    st.markdown(
        f"**👤 Cliente:** {res.get('Cliente','N/A')}  |  **RFC:** {res.get('Numero_Cliente_RFC','') or 'N/A'}"
    )
    st.markdown(
        f"**Estado:** {res.get('Estado','') or 'N/A'}  |  **Estado del Caso:** {res.get('Estado_Caso','') or 'N/A'}  |  **Turno:** {res.get('Turno','') or 'N/A'}"
    )
    if is_garantia:
        st.markdown(
            f"**🔢 Número de Serie:** {res.get('Numero_Serie','') or 'N/A'}  |  **📅 Fecha de Compra:** {res.get('Fecha_Compra','') or 'N/A'}"
        )

    ref_t = res.get("Refacturacion_Tipo","")
    ref_st = res.get("Refacturacion_Subtipo","")
    ref_f = res.get("Folio_Factura_Refacturada","")
    if any([ref_t, ref_st, ref_f]):
        st.markdown("**♻️ Refacturación:**")
        st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
        st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
        st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

    if str(res.get("Resultado_Esperado","")).strip():
        st.markdown(f"**🎯 Resultado Esperado:** {res.get('Resultado_Esperado','')}")
    if str(res.get("Motivo_Detallado","")).strip():
        st.markdown("**📝 Motivo / Descripción:**")
        st.info(str(res.get("Motivo_Detallado","")).strip())
    if str(res.get("Material_Devuelto","")).strip():
        st.markdown("**📦 Piezas / Material:**")
        st.info(str(res.get("Material_Devuelto","")).strip())
    if str(res.get("Monto_Devuelto","")).strip():
        st.markdown(f"**💵 Monto (dev./estimado):** {res.get('Monto_Devuelto','')}")

    st.markdown(
        f"**🏢 Área Responsable:** {res.get('Area_Responsable','') or 'N/A'}  |  **👥 Responsable del Error:** {res.get('Nombre_Responsable','') or 'N/A'}"
    )
    st.markdown(
        f"**📅 Fecha Entrega/Cierre (si aplica):** {res.get('Fecha_Entrega','') or 'N/A'}  |  "
        f"**📅 Recepción:** {res.get('Fecha_Recepcion_Devolucion','') or 'N/A'}  |  "
        f"**📦 Recepción:** {res.get('Estado_Recepcion','') or 'N/A'}"
    )
    st.markdown(
        f"**🧾 Nota de Crédito:** {res.get('Nota_Credito_URL','') or 'N/A'}  |  "
        f"**📂 Documento Adicional:** {res.get('Documento_Adicional_URL','') or 'N/A'}"
    )
    if str(res.get("Comentarios_Admin_Devolucion","")).strip():
        st.markdown("**🗒️ Comentario Administrativo:**")
        st.info(str(res.get("Comentarios_Admin_Devolucion","")).strip())

    seguimiento_txt = str(res.get("Seguimiento",""))
    if (is_devolucion or is_garantia) and seguimiento_txt.strip():
        st.markdown("**📌 Seguimiento:**")
        st.info(seguimiento_txt.strip())

    mod_txt = res.get("Modificacion_Surtido", "") or ""
    mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
    if mod_txt or mod_urls:
        st.markdown("#### 🛠 Modificación de surtido")
        if mod_txt:
            st.info(mod_txt)
        if mod_urls:
            st.markdown("**Archivos de modificación:**")
            for u in mod_urls:
                nombre = extract_s3_key(u).split("/")[-1]
                tmp = get_s3_file_download_url(s3_client, u)
                st.markdown(
                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                    unsafe_allow_html=True,
                )

    with st.expander("📎 Archivos (Adjuntos y Guía)", expanded=False):
        adj = res.get("Adjuntos_urls", []) or []
        guia = res.get("Guia_url", "")
        if adj:
            st.markdown("**Adjuntos:**")
            for u in adj:
                nombre = extract_s3_key(u).split("/")[-1]
                tmp = get_s3_file_download_url(s3_client, u)
                st.markdown(
                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                    unsafe_allow_html=True,
                )
        if guia and guia.lower() not in ("nan","none","n/a"):
            st.markdown("**Guía:**")
            tmp = get_s3_file_download_url(s3_client, guia)
            st.markdown(
                f'- <a href="{tmp}" target="_blank">Abrir guía</a>',
                unsafe_allow_html=True,
            )
        if not adj and not guia:
            st.info("Sin archivos registrados en la hoja.")

    st.markdown("---")

# --- INTERFAZ ---
tabs = st.tabs([
    "🔍 Buscar Pedido",
    "⬇️ Descargar Datos",
    "✏️ Modificar Pedido",
])
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

        resultados = []

        # ====== Siempre cargamos pedidos (datos_pedidos) porque la búsqueda por guía los necesita ======
        df_pedidos = cargar_pedidos()
        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos["ID_Pedido"] = pd.to_numeric(df_pedidos["ID_Pedido"], errors="coerce")
            df_pedidos = df_pedidos.sort_values(by="ID_Pedido", ascending=True).reset_index(drop=True)

        # ====== BÚSQUEDA POR CLIENTE: también carga y filtra casos_especiales ======
        if modo_busqueda == "🧑 Por cliente":
            if not keyword.strip():
                st.warning("⚠️ Ingresa un nombre de cliente.")
                st.stop()

            cliente_normalizado = normalizar(keyword.strip())

            # 2.1) Buscar en datos_pedidos (S3 + todos los archivos del pedido)
            for _, row in df_pedidos.iterrows():
                nombre = str(row.get("Cliente", "")).strip()
                if not nombre:
                    continue
                if cliente_normalizado not in normalizar(nombre):
                    continue

                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                todos_los_archivos = obtener_todos_los_archivos(prefix) if prefix else []

                comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                otros = [
                    f for f in todos_los_archivos
                    if f not in comprobantes and f not in facturas
                ]

                resultados.append({
                    "__source": "pedidos",
                    "ID_Pedido": pedido_id,
                    "Cliente": row.get("Cliente", ""),
                    "Estado": row.get("Estado", ""),
                    "Vendedor": row.get("Vendedor_Registro", ""),
                    "Folio": row.get("Folio_Factura", ""),
                    "Hora_Registro": row.get("Hora_Registro", ""),
                    # 🛠 Modificación de surtido
                    "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                    "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                    # ♻️ Refacturación
                    "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                    "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                    "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                    # Archivos S3
                    "Coincidentes": [],  # En modo cliente no destacamos PDFs guía específicos
                    "Comprobantes": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in comprobantes],
                    "Facturas": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in facturas],
                    "Otros": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in otros],
                })

            # 2.2) Buscar en casos_especiales (mostrar campos de la hoja + links de Adjuntos y Hoja_Ruta_Mensajero)
            df_casos = cargar_casos_especiales()
            # Ordenar por Hora_Registro si existe
            if "Hora_Registro" in df_casos.columns:
                df_casos["Hora_Registro"] = pd.to_datetime(df_casos["Hora_Registro"], errors="coerce")

            for _, row in df_casos.iterrows():
                nombre = str(row.get("Cliente", "")).strip()
                if not nombre:
                    continue
                if cliente_normalizado not in normalizar(nombre):
                    continue
                resultados.append(preparar_resultado_caso(row))


        # ====== BÚSQUEDA POR NÚMERO DE GUÍA (tu flujo original sobre datos_pedidos + S3) ======
        elif modo_busqueda == "🔢 Por número de guía":
            clave = keyword.strip()
            if not clave:
                st.warning("⚠️ Ingresa una palabra clave o número de guía.")
                st.stop()

            for _, row in df_pedidos.iterrows():
                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_validos = obtener_archivos_pdf_validos(prefix)
                archivos_coincidentes = []

                for archivo in archivos_validos:
                    key = archivo["Key"]
                    texto = extraer_texto_pdf(key)

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

                        archivos_coincidentes.append((key, get_s3_file_download_url(s3_client, key)))
                        todos_los_archivos = obtener_todos_los_archivos(prefix)
                        comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                        facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                        otros = [f for f in todos_los_archivos if f not in comprobantes and f not in facturas and f["Key"] != archivos_coincidentes[0][0]]

                        resultados.append({
                            "__source": "pedidos",
                            "ID_Pedido": pedido_id,
                            "Cliente": row.get("Cliente", ""),
                            "Estado": row.get("Estado", ""),
                            "Vendedor": row.get("Vendedor_Registro", ""),
                            "Folio": row.get("Folio_Factura", ""),
                            "Hora_Registro": row.get("Hora_Registro", ""),
                            # 🛠 Modificación de surtido
                            "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                            "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                            # ♻️ Refacturación
                            "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                            "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                            "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                            # Archivos S3
                            "Coincidentes": archivos_coincidentes,
                            "Comprobantes": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in comprobantes],
                            "Facturas": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in facturas],
                            "Otros": [(f["Key"], get_s3_file_download_url(s3_client, f["Key"])) for f in otros],
                        })
                        break  # detener búsqueda tras encontrar coincidencia
                else:
                    continue  # ningún PDF coincidió

                break  # Solo un pedido en búsqueda por guía

        # ====== RENDER DE RESULTADOS ======
        st.markdown("---")
        if resultados:
            st.success(f"✅ Se encontraron coincidencias en {len(resultados)} registro(s).")

            # Ordena por Hora_Registro descendente cuando exista
            def _parse_dt(v):
                try:
                    return pd.to_datetime(v)
                except Exception:
                    return pd.NaT
            resultados = sorted(resultados, key=lambda r: _parse_dt(r.get("Hora_Registro")), reverse=True)

            for res in resultados:
                if res.get("__source") == "casos":
                    render_caso_especial(res)
                else:
                    # ---------- Render de PEDIDOS (flujo actual) ----------
                    st.markdown(f"### 🤝 {res['Cliente'] or 'Cliente N/D'}")
                    st.markdown(
                        f"📄 **Folio:** `{res['Folio'] or 'N/D'}`  |  🔍 **Estado:** `{res['Estado'] or 'N/D'}`  |  🧑‍💼 **Vendedor:** `{res['Vendedor'] or 'N/D'}`  |  🕒 **Hora:** `{res['Hora_Registro'] or 'N/D'}`"
                    )

                    mod_txt = res.get("Modificacion_Surtido", "") or ""
                    mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
                    if mod_txt or mod_urls:
                        st.markdown("#### 🛠 Modificación de surtido")
                        if mod_txt:
                            st.info(mod_txt)
                        if mod_urls:
                            st.markdown("**Archivos de modificación:**")
                            for u in mod_urls:
                                nombre = extract_s3_key(u).split("/")[-1]
                                tmp = get_s3_file_download_url(s3_client, u)
                                st.markdown(
                                    f'- <a href="{tmp}" target="_blank">{nombre}</a>',
                                    unsafe_allow_html=True,
                                )

                    # ♻️ Refacturación (si hay)
                    ref_t = res.get("Refacturacion_Tipo","")
                    ref_st = res.get("Refacturacion_Subtipo","")
                    ref_f = res.get("Folio_Factura_Refacturada","")
                    if any([ref_t, ref_st, ref_f]):
                        with st.expander("♻️ Refacturación", expanded=False):
                            st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
                            st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
                            st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

                    with st.expander("📁 Archivos del Pedido", expanded=True):
                        if res.get("Coincidentes"):
                            st.markdown("#### 🔍 Guías:")
                            for key, url in res["Coincidentes"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">🔍 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        if res.get("Comprobantes"):
                            st.markdown("#### 🧾 Comprobantes:")
                            for key, url in res["Comprobantes"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">📄 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        if res.get("Facturas"):
                            st.markdown("#### 📁 Facturas:")
                            for key, url in res["Facturas"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">📄 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )
                        if res.get("Otros"):
                            st.markdown("#### 📂 Otros Archivos:")
                            for key, url in res["Otros"]:
                                nombre = key.split("/")[-1]
                                st.markdown(
                                    f'- <a href="{url}" target="_blank">📌 {nombre}</a>',
                                    unsafe_allow_html=True,
                                )

        else:
            mensaje = (
                "⚠️ No se encontraron coincidencias en ningún archivo PDF."
                if modo_busqueda == "🔢 Por número de guía"
                else "⚠️ No se encontraron pedidos o casos para el cliente ingresado."
            )
            st.warning(mensaje)


with tabs[1]:
    st.header("⬇️ Descargar Datos")

    df_todos = cargar_todos_los_pedidos()
    df_casos = cargar_casos_especiales()

    mostrar_casos = st.checkbox("Mostrar solo casos especiales")
    df = df_casos if mostrar_casos else df_todos

    if df.empty:
        st.info("No hay datos disponibles para descargar.")
    else:
        df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")
        df["ID_Pedido"] = pd.to_numeric(df["ID_Pedido"], errors="coerce")
        df = df.sort_values(by="ID_Pedido", ascending=True)

        rango_tiempo = st.selectbox(
            "Rango de tiempo",
            ["12 horas", "24 horas", "7 días", "Todos"],
        )
        estados_sel = st.multiselect("Estado", sorted(df["Estado"].dropna().unique()))
        tipos_sel = st.multiselect("Tipo de envío", sorted(df["Tipo_Envio"].dropna().unique()))

        filtrado = df
        delta = None
        if rango_tiempo == "12 horas":
            delta = timedelta(hours=12)
        elif rango_tiempo == "24 horas":
            delta = timedelta(hours=24)
        elif rango_tiempo == "7 días":
            delta = timedelta(days=7)

        if delta is not None:
            filtrado = filtrado[filtrado["Hora_Registro"] >= datetime.now() - delta]
        if estados_sel:
            filtrado = filtrado[filtrado["Estado"].isin(estados_sel)]
        if tipos_sel:
            filtrado = filtrado[filtrado["Tipo_Envio"].isin(tipos_sel)]

        filtrado = filtrado.drop(columns=["ID_Pedido"], errors="ignore")
        filtrado = filtrado.reset_index(drop=True)
        filtrado.index = filtrado.index + 1

        st.markdown(f"{len(filtrado)} registros encontrados")
        # Show all rows that match the selected filters without truncating
        st.dataframe(filtrado)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            filtrado.to_excel(writer, index=False, sheet_name="Pedidos")
        buffer.seek(0)

        st.download_button(
            label="⬇️ Descargar Excel",
            data=buffer.getvalue(),
            file_name="pedidos_filtrados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


CONTRASENA_ADMIN = "Ceci"  # puedes cambiar esta contraseña si lo deseas

# --- PESTAÑA DE MODIFICACIÓN DE PEDIDOS CON CONTRASEÑA ---
with tabs[2]:
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

    df_pedidos = cargar_pedidos()
    df_casos = cargar_casos_especiales()

    def es_devol_o_garant(row):
        for col in ("Tipo_Envio", "Tipo_Caso"):
            valor = str(row.get(col, ""))
            if valor and ("devolu" in normalizar(valor) or "garant" in normalizar(valor)):
                return True
        return False

    df_casos = df_casos[df_casos.apply(es_devol_o_garant, axis=1)]

    for d in (df_pedidos, df_casos):
        d["Hora_Registro"] = pd.to_datetime(d["Hora_Registro"], errors="coerce")

    df_pedidos["__source"] = "pedidos"
    df_casos["__source"] = "casos"
    df = pd.concat([df_pedidos, df_casos], ignore_index=True, sort=False)
    df = df[df["ID_Pedido"].notna()]
    df["ID_Pedido"] = pd.to_numeric(df["ID_Pedido"], errors="coerce")
    df = df.sort_values(by="ID_Pedido", ascending=True)

    if "pedido_modificado" in st.session_state:
        pedido_sel = st.session_state["pedido_modificado"]
        source_sel = st.session_state.get("pedido_modificado_source", "pedidos")
        del st.session_state["pedido_modificado"]  # ✅ limpia la variable tras usarla
        if "pedido_modificado_source" in st.session_state:
            del st.session_state["pedido_modificado_source"]
    else:
       pedido_sel = None  # ✅ evitar NameError si no se selecciona nada aún
       source_sel = None


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
        else:
            st.success(f"✅ Se encontraron {len(coincidencias)} coincidencia(s) para este cliente.")

            if len(coincidencias) == 1:
                pedido_sel = coincidencias[0]["ID_Pedido"]
                source_sel = coincidencias[0]["__source"]
                row = coincidencias[0]
                st.markdown(
                    f"🧾 {row.get('Folio_Factura', row.get('Folio',''))} – 🚚 {row.get('Tipo_Envio','')} – 👤 {row['Cliente']} – 🔍 {row.get('Estado', row.get('Estado_Caso',''))} – 🧑‍💼 {row.get('Vendedor_Registro','')} – 🕒 {row['Hora_Registro'].strftime('%d/%m %H:%M')}"
                )

            else:
                opciones = []
                for r in coincidencias:
                    folio = r.get('Folio_Factura', r.get('Folio',''))
                    tipo_envio = r.get('Tipo_Envio','')
                    display = (
                        f"{folio} – 🚚 {tipo_envio} – 👤 {r['Cliente']} – 🔍 {r.get('Estado', r.get('Estado_Caso',''))} "
                        f"– 🧑‍💼 {r.get('Vendedor_Registro','')} – 🕒 {r['Hora_Registro'].strftime('%d/%m %H:%M')}"
                    )
                    opciones.append(display)
                seleccion = st.selectbox("👥 Se encontraron múltiples pedidos, selecciona uno:", opciones)
                idx = opciones.index(seleccion)
                pedido_sel = coincidencias[idx]["ID_Pedido"]
                source_sel = coincidencias[idx]["__source"]

    else:
        ultimos_10 = df.head(10)
        st.markdown("### 🕒 Últimos 10 Pedidos Registrados")
        ultimos_10["display"] = ultimos_10.apply(
            lambda row: (
                f"{row.get('Folio_Factura', row.get('Folio',''))} – {row.get('Tipo_Envio','')} – 👤 {row['Cliente']} "
                f"– 🔍 {row.get('Estado', row.get('Estado_Caso',''))} – 🧑‍💼 {row.get('Vendedor_Registro','')} "
                f"– 🕒 {row['Hora_Registro'].strftime('%d/%m %H:%M')}"
            ),
            axis=1
        )
        idx_seleccion = st.selectbox(
            "⬇️ Selecciona uno de los pedidos recientes:",
            ultimos_10.index,
            format_func=lambda i: ultimos_10.loc[i, "display"]
        )
        pedido_sel = ultimos_10.loc[idx_seleccion, "ID_Pedido"]
        source_sel = ultimos_10.loc[idx_seleccion, "__source"]


    # --- Cargar datos del pedido seleccionado ---
    st.markdown("---")

    if pedido_sel is None:
        st.warning("⚠️ No se ha seleccionado ningún pedido válido.")
        st.stop()

    row_df = df_pedidos if source_sel == "pedidos" else df_casos
    row = row_df[row_df["ID_Pedido"].astype(str) == str(pedido_sel)].iloc[0]
    gspread_row_idx = row_df[row_df["ID_Pedido"].astype(str) == str(pedido_sel)].index[0] + 2  # índice real en hoja
    if "mensaje_exito" in st.session_state:
        st.success(st.session_state["mensaje_exito"])
        del st.session_state["mensaje_exito"]  # ✅ eliminar para que no se repita


    # Definir la hoja de Google Sheets para modificación
    hoja_nombre = "datos_pedidos" if source_sel == "pedidos" else "casos_especiales"
    hoja = gspread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet(hoja_nombre)

    st.markdown(
        f"📦 **Cliente:** {row['Cliente']} &nbsp;&nbsp;&nbsp;&nbsp; 🧾 **Folio Factura:** {row.get('Folio_Factura', 'N/A')}"
    )

    st.markdown("### 📎 Adjuntar Archivos")
    col_guias = "Adjuntos_Guia" if source_sel == "pedidos" else "Hoja_Ruta_Mensajero"
    existentes_guias = partir_urls(row.get(col_guias, ""))
    existentes_otros = partir_urls(row.get("Adjuntos", ""))

    if existentes_guias or existentes_otros:
        with st.expander("📥 Archivos existentes", expanded=False):
            if existentes_guias:
                st.markdown("**Guías:**")
                for u in existentes_guias:
                    tmp = get_s3_file_download_url(s3_client, u)
                    nombre = extract_s3_key(u).split("/")[-1]
                    st.markdown(f'- <a href="{tmp}" target="_blank">{nombre}</a>', unsafe_allow_html=True)
            if existentes_otros:
                st.markdown("**Otros:**")
                for u in existentes_otros:
                    tmp = get_s3_file_download_url(s3_client, u)
                    nombre = extract_s3_key(u).split("/")[-1]
                    st.markdown(f'- <a href="{tmp}" target="_blank">{nombre}</a>', unsafe_allow_html=True)

    uploaded_guias = st.file_uploader("📄 Guías", accept_multiple_files=True)
    uploaded_otros = st.file_uploader("📁 Otros", accept_multiple_files=True)

    if st.button("⬆️ Subir archivos"):
        nuevas_guias_urls, nuevas_otros_urls = [], []
        for file in uploaded_guias or []:
            key = f"adjuntos_pedidos/{pedido_sel}/{file.name}"
            success, url_subida = upload_file_to_s3(s3_client, S3_BUCKET, file, key)
            if success:
                nuevas_guias_urls.append(url_subida)
        for file in uploaded_otros or []:
            key = f"adjuntos_pedidos/{pedido_sel}/{file.name}"
            success, url_subida = upload_file_to_s3(s3_client, S3_BUCKET, file, key)
            if success:
                nuevas_otros_urls.append(url_subida)

        if nuevas_guias_urls:
            existente = row.get(col_guias, "")
            nuevo_valor = combinar_urls_existentes(existente, nuevas_guias_urls)
            hoja.update_cell(gspread_row_idx, row_df.columns.get_loc(col_guias)+1, nuevo_valor)
        if nuevas_otros_urls:
            existente = row.get("Adjuntos", "")
            nuevo_valor = combinar_urls_existentes(existente, nuevas_otros_urls)
            hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Adjuntos")+1, nuevo_valor)

        st.session_state["pedido_modificado"] = pedido_sel
        st.session_state["pedido_modificado_source"] = source_sel
        st.session_state["mensaje_exito"] = "📎 Archivos subidos correctamente."
        st.rerun()


    # --- CAMPOS MODIFICABLES ---
    if source_sel == "casos":
        comentario_usuario = st.text_area("📝 Comentario desde almacén")
        if st.button("Guardar comentario"):
            existente = row.get("Comentario", "")
            nuevo_coment = f"[Almacen] {comentario_usuario.strip()}"
            valor_final = f"{existente} | {nuevo_coment}" if existente else nuevo_coment
            hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Comentario") + 1, valor_final)
            st.session_state["pedido_modificado"] = pedido_sel
            st.session_state["pedido_modificado_source"] = source_sel
            st.session_state["mensaje_exito"] = "📝 Comentario guardado correctamente."
            st.rerun()

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
    vendedor_actual = row.get("Vendedor_Registro", "").strip()

    st.markdown("### 🧑‍💼 Cambio de Vendedor")
    st.markdown(f"**Actual:** {vendedor_actual}")

    vendedores_opciones = [v for v in vendedores if v != vendedor_actual] or [vendedor_actual]
    nuevo_vendedor = st.selectbox("➡️ Cambiar a:", vendedores_opciones)

    if st.button("🧑‍💼 Guardar cambio de vendedor"):
        hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Vendedor_Registro")+1, nuevo_vendedor)
        st.session_state["pedido_modificado"] = pedido_sel
        st.session_state["pedido_modificado_source"] = source_sel
        st.session_state["mensaje_exito"] = "🎈 Vendedor actualizado correctamente."
        st.rerun()


    if source_sel == "pedidos":
        tipo_envio_actual = row["Tipo_Envio"].strip()
        st.markdown("### 🚚 Cambio de Tipo de Envío")
        st.markdown(f"**Actual:** {tipo_envio_actual}")

        opcion_contraria = "📍 Pedido Local" if "Foráneo" in tipo_envio_actual else "🚚 Pedido Foráneo"
        tipo_envio = st.selectbox("➡️ Cambiar a:", [opcion_contraria])

        if tipo_envio == "📍 Pedido Local":
            nuevo_turno = st.selectbox("⏰ Turno", ["☀ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"])
        else:
            nuevo_turno = ""

        if st.button("📦 Guardar cambio de tipo de envío"):
            hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Tipo_Envio")+1, tipo_envio)
            hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Turno")+1, nuevo_turno)
            st.session_state["pedido_modificado"] = pedido_sel
            st.session_state["pedido_modificado_source"] = source_sel
            st.session_state["mensaje_exito"] = "📦 Tipo de envío y turno actualizados correctamente."
            st.rerun()


    # --- NUEVO: CAMBIO DE ESTADO A CANCELADO ---
    estado_actual = row.get("Estado", "").strip()
    st.markdown("### 🟣 Cancelar Pedido")
    st.markdown(f"**Estado Actual:** {estado_actual}")
    
    # Solo mostrar la opción de cancelar si el pedido no está ya cancelado
    if "Cancelado" not in estado_actual:
        if st.button("🟣 Cambiar Estado a CANCELADO"):
            try:
                # Actualizar el estado en la hoja de cálculo
                nuevo_estado = "🟣 Cancelado"
                hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Estado")+1, nuevo_estado)
                # Usar el mismo sistema que las otras secciones
                st.session_state["pedido_modificado"] = pedido_sel
                st.session_state["pedido_modificado_source"] = source_sel
                st.session_state["mensaje_exito"] = "🟣 Pedido marcado como CANCELADO correctamente."
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error al cancelar el pedido: {str(e)}")
    else:
        st.info("ℹ️ Este pedido ya está marcado como CANCELADO.")


    completado = row.get("Completados_Limpiado", "")
    st.markdown("### 👁 Visibilidad en Pantalla de Producción")
    opciones_visibilidad = {"Sí": "", "No": "sí"}
    valor_actual = completado.strip().lower()
    valor_preseleccionado = "No" if valor_actual == "sí" else "Sí"
    seleccion = st.selectbox("¿Mostrar este pedido en el Panel?", list(opciones_visibilidad.keys()), index=list(opciones_visibilidad.keys()).index(valor_preseleccionado))
    nuevo_valor_completado = opciones_visibilidad[seleccion]


    if st.button("👁 Guardar visibilidad en Panel"):
        hoja.update_cell(gspread_row_idx, row_df.columns.get_loc("Completados_Limpiado")+1, nuevo_valor_completado)
        st.session_state["pedido_modificado"] = pedido_sel
        st.session_state["pedido_modificado_source"] = source_sel
        st.session_state["mensaje_exito"] = "👁 Visibilidad en pantalla de producción actualizada."
        st.rerun()


