# Cómo hace `app_a-d.py` para abrir archivos en otra pestaña (sin descarga automática)

La app combina **2 piezas** para lograrlo:

1. En S3, al subir archivos, define `ContentDisposition="inline"` para extensiones visualizables (`.pdf`, `.jpg`, `.jpeg`, `.png`, `.webp`).
2. Al generar la URL firmada, vuelve a forzar `ResponseContentDisposition="inline"` (y `ResponseContentType` correcto).
3. En la UI, pinta enlaces HTML con `target="_blank"` para abrir en pestaña nueva.

Si quieres replicarlo en otra app, copia estas funciones/lógica de `app_a-d.py`:

- `INLINE_EXT`
- `upload_file_to_s3(...)`
- `get_s3_file_download_url(...)`
- `resolve_storage_url(...)`

Y renderiza los links como:

```python
st.markdown(
    f'<a href="{url}" target="_blank">Ver archivo</a>',
    unsafe_allow_html=True,
)
```
