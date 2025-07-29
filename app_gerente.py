import streamlit as st
import pandas as pd
import pdfplumber
import boto3
import gspread
import json
import re
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="🔍 Buscador de Palabras Clave en PDFs", layout="wide")
st.title("🔍 Buscador de Archivos PDF en Pedidos S3")
st.markdown("Busca palabras clave, números de guía o cualquier texto en los PDFs adjuntos de todos los pedidos.")

# --- INPUT ---
palabra_clave = st.text_input("📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:")
palabra_clave = palabra_clave.strip()  # ✅ quita espacios iniciales y finales
buscar_btn = st.button("🔎 Buscar en todos los pedidos")

# --- CREDENCIALES DESDE SECRETS ---
AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
AWS_REGION = st.secrets["aws"]["aws_region"]
S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]

GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace("\\n", "\n")
GOOGLE_SHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SHEET_NAME = "datos_pedidos"

# --- FUNCIONES DE AUTENTICACIÓN ---
@st.cache_resource
def get_clients():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(GSHEETS_CREDENTIALS, scope)
    gspread_client = gspread.authorize(creds)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    return gspread_client, s3_client

# --- EXTRACCIÓN DE TEXTO DE PDF ---
def contiene_palabra(pdf_bytes, keyword):
    try:
        keyword_clean = re.sub(r"[\s\n\r\-\_]+", "", keyword.lower())
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages):
                texto = page.extract_text() or ""
                texto_limpio = re.sub(r"[\s\n\r\-]+", "", texto.lower())

                # 👇 DEBUG TEMPORAL
                st.write(f"🧪 Página {i+1}")
                st.code(texto[:1000])  # Muestra los primeros 1000 caracteres extraídos

                if keyword_clean in texto_limpio:
                    st.success("🎯 Coincidencia con texto limpio")
                    return True
                keyword_raw = keyword.lower().strip()
                if keyword_raw in texto.lower():
                    st.success("🎯 Coincidencia con texto exacto")
                    return True
    except Exception as e:
        st.error(f"❌ Error en contiene_palabra: {e}")
    return False


# --- BÚSQUEDA EN PDF DE S3 ---
def buscar_pdf_en_s3(s3, bucket, key, keyword):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        pdf_bytes = obj["Body"].read()
        return contiene_palabra(pdf_bytes, keyword)
    except Exception:
        return False

# --- PROCESO PRINCIPAL ---
if buscar_btn and palabra_clave.strip():
    gspread_client, s3 = get_clients()
    st.info("🔄 Buscando, por favor espera... puede tardar unos segundos.")

    hoja = gspread_client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
    data = hoja.get_all_records()
    df = pd.DataFrame(data)
    df["ID_Pedido"] = df["ID_Pedido"].astype(str)

    resultados = []

    for _, row in df.iterrows():
        id_pedido = row["ID_Pedido"]
        cliente = row.get("Cliente", "")
        estado = row.get("Estado", "")
        vendedor = row.get("Vendedor_Registro", "")
        folio = row.get("Folio_Factura", "")
        archivos_encontrados = []

        # 1. Buscar en S3 por prefijos conocidos
        for carpeta in ["adjuntos_pedidos", "adjuntos_guias", "adjuntos_facturas"]:
            prefix = f"{carpeta}/{id_pedido}/"
            try:
                response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
                for obj in response.get("Contents", []):
                    key = obj["Key"]
                    if key.lower().endswith(".pdf") and buscar_pdf_en_s3(s3, S3_BUCKET_NAME, key, palabra_clave):
                        archivos_encontrados.append({
                            "archivo": key.split("/")[-1],
                            "url": f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"
                        })
            except Exception:
                continue

        # 2. Buscar también en campos de URLs externas (Adjuntos_Surtido / Adjuntos_Guia)
        for col in ["Adjuntos_Surtido", "Adjuntos_Guia"]:
            urls_str = row.get(col, "")
            urls = [x.strip() for x in urls_str.split(",") if x.strip()]
            for url in urls:
                try:
                    if S3_BUCKET_NAME in url:
                        key = url.split(f"{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/")[-1]
                        if key.lower().endswith(".pdf") and buscar_pdf_en_s3(s3, S3_BUCKET_NAME, key, palabra_clave):
                            archivos_encontrados.append({"archivo": key.split("/")[-1], "url": url})
                except Exception:
                    continue

        # Si se encontraron coincidencias
        if archivos_encontrados:
            resultados.append({
                "ID": id_pedido,
                "Cliente": cliente,
                "Estado": estado,
                "Vendedor": vendedor,
                "Folio": folio,
                "Archivos": archivos_encontrados
            })

    # Mostrar resultados
    if resultados:
        st.success(f"✅ Se encontró la palabra en {len(resultados)} pedido(s):")
        for r in resultados:
            st.markdown(f"---\n### 📦 Pedido {r['ID']} – {r['Cliente']} ({r['Folio']})")
            st.markdown(f"Estado: {r['Estado']}  |  Vendedor: {r['Vendedor']}")
            for archivo in r["Archivos"]:
                st.markdown(f"- 📄 [{archivo['archivo']}]({archivo['url']})")
    else:
        st.warning("🔍 No se encontró la palabra en ningún PDF.")
