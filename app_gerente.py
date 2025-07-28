import streamlit as st
import boto3
import json
import gspread
import pdfplumber
from io import BytesIO
from datetime import datetime
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="Buscador de Archivos PDF", layout="wide")
st.title("🔍 Buscador de Archivos PDF")

# --- Credenciales AWS
AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
AWS_REGION = st.secrets["aws"]["aws_region"]
S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
S3_ATTACHMENT_PREFIX = "adjuntos_pedidos/"

# --- Cliente AWS S3
@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

s3_client = get_s3_client()

# --- Función: Obtener archivos PDF en S3 por pedido
def listar_pdfs_en_pedido(pedido_id):
    prefix = f"{S3_ATTACHMENT_PREFIX}{pedido_id}/"
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        if "Contents" not in response:
            return []
        return [obj["Key"] for obj in response["Contents"] if obj["Key"].lower().endswith(".pdf")]
    except Exception as e:
        st.error(f"Error al listar archivos en S3: {e}")
        return []

# --- Función: Extraer texto de un PDF desde S3
def extraer_texto_pdf_s3(s3_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        pdf_bytes = response["Body"].read()
        texto_completo = ""
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                texto_completo += page.extract_text() or ""
        return texto_completo
    except Exception as e:
        return f"❌ Error al leer PDF: {e}"

# --- Función: Obtener pedidos desde Google Sheets
@st.cache_data(ttl=300)
def obtener_pedidos_desde_gsheet():
    credentials_json_str = st.secrets["gsheets"]["google_credentials"]
    creds_dict = json.loads(credentials_json_str)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    hoja = client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
    datos = hoja.get_all_records()
    df = pd.DataFrame(datos)
    df = df[df["ID_Pedido"].astype(str).str.strip().ne("")]
    return df


# =========================
# 🌎 BÚSQUEDA GLOBAL
# =========================
st.markdown("## 🌎 Buscar palabra clave en TODOS los pedidos")

palabra_global = st.text_input("🔤 Palabra a buscar en todos los PDFs del sistema:", "")

if palabra_global.strip():
    df_pedidos = obtener_pedidos_desde_gsheet()
    resultados = []

    for _, row in df_pedidos.iterrows():
        pedido_id = row["ID_Pedido"]
        pdfs = listar_pdfs_en_pedido(pedido_id)
        for s3_key in pdfs:
            texto = extraer_texto_pdf_s3(s3_key)
            if palabra_global.lower() in texto.lower():
                resultados.append({
                    "ID_Pedido": pedido_id,
                    "Cliente": row.get("Cliente", ""),
                    "Vendedor": row.get("Vendedor_Registro", ""),
                    "Estado": row.get("Estado", ""),
                    "Folio": row.get("Folio_Factura", ""),
                    "Archivo": s3_key.split("/")[-1],
                    "URL": f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
                })
                break  # si ya la encontramos en un PDF de ese pedido, pasamos al siguiente

    if resultados:
        st.success(f"✅ Se encontró la palabra en {len(resultados)} pedido(s).")
        for res in resultados:
            with st.expander(f"📄 {res['Archivo']} — Pedido: {res['ID_Pedido']}"):
                st.write(f"**Cliente:** {res['Cliente']}")
                st.write(f"**Vendedor:** {res['Vendedor']}")
                st.write(f"**Estado:** {res['Estado']}")
                st.write(f"**Folio:** {res['Folio']}")
                st.markdown(f"[🔗 Ver Archivo PDF]({res['URL']})")
    else:
        st.warning("🔍 No se encontró la palabra en ningún PDF.")
