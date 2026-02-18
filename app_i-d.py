import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import gspread.utils
import time
import unicodedata
import streamlit.components.v1 as components
from itertools import count
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from streamlit_autorefresh import st_autorefresh
from textwrap import dedent

TZ = ZoneInfo("America/Mexico_City")

st.set_page_config(page_title="Panel de Almacén Integrado", layout="wide")

# --- Ajustes UI compactos ---
st.markdown(
    """
    <style>
    section.main > div { padding-top: 0.5rem; }
    .header-compact h2 { margin: 0; font-size: 1.5rem; line-height: 1.6rem; }
    .header-meta { font-size: 0.8rem; color: #c9c9c9; }
    div[data-testid="stHorizontalBlock"] { gap: 0.4rem; }
    div[data-testid="stRadio"] > label { margin-bottom: 0; }
    div[data-testid="stRadio"] div[role="radiogroup"] { gap: 0.25rem; }
    div[data-testid="stRadio"] label { padding: 0.1rem 0.4rem; font-size: 0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Encabezado ---
current_time = datetime.now(TZ).strftime("%d/%m %H:%M:%S")
col_title, col_update, col_actions = st.columns([0.6, 0.2, 0.2])
with col_title:
    st.markdown(
        """
        <div class="header-compact">
            <h2 style="color: white;">
                <span style="font-size: 1.8rem;">🏷️</span> Flujo de Pedidos en Tiempo Real
            </h2>
        </div>
    """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        /* 🔢 Ajuste compacto para métricas */
        div[data-testid="metric-container"] { padding: 0.1rem 0.5rem; }
        div[data-testid="metric-container"] > div { font-size: 1.1rem !important; }
        div[data-testid="metric-container"] > label { font-size: 0.85rem !important; }
        </style>
    """,
        unsafe_allow_html=True,
    )
with col_update:
    st.markdown(f'<div class="header-meta">🕒 Última actualización: {current_time}</div>', unsafe_allow_html=True)
with col_actions:
    if st.button("🔄 Refrescar ahora", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

# CSS tabla compacta
st.markdown(
    """
    <style>
    .dataframe td { white-space: unset !important; word-break: break-word; }
    .dataframe {
        table-layout: fixed; width: 100%;
    }
    .dataframe td {
        white-space: normal !important;
        overflow-wrap: break-word;
        font-size: 0.75rem;
        padding: 0.1rem 0.2rem;
        height: 1rem;
        line-height: 1.2rem;
        vertical-align: top;
    }
    .dataframe th {
        font-size: 0.75rem;
        padding: 0.1rem 0.2rem;
        text-align: left;
    }
    </style>
""",
    unsafe_allow_html=True,
)

# Helpers UI automáticos
def sanitize_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except Exception:
        pass
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.lower() in {"nan", "none", "null"}:
            return ""
        return cleaned
    return str(value)


def parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value
    try:
        dt = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if isinstance(dt, pd.Series) and not dt.empty:
        dt = dt.iloc[0]
    if pd.isna(dt):
        return None
    return dt


def format_date(value) -> str:
    dt = parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%d/%m")


def format_time(value) -> str:
    dt = parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%H:%M")


def compute_sort_key(row) -> pd.Timestamp:
    candidates = [
        parse_datetime(row.get("Hora_Registro")),
        parse_datetime(row.get("Fecha_Entrega")),
        parse_datetime(row.get("Fecha_Completado")),
        parse_datetime(row.get("Fecha_Pago_Comprobante")),
        parse_datetime(row.get("Hora_Proceso")),
    ]
    for dt in candidates:
        if dt is not None:
            return dt
    idx = row.get("gsheet_row_index")
    try:
        if idx is not None and not pd.isna(idx):
            base = pd.Timestamp("1970-01-01")
            return base + pd.to_timedelta(int(float(idx)), unit="s")
    except Exception:
        pass
    return pd.Timestamp.max


def build_auto_number_key(entry) -> str:
    base_id = sanitize_text(entry.get("id_pedido", ""))
    fallback = f"{sanitize_text(entry.get('cliente', ''))}|{sanitize_text(entry.get('hora', ''))}"
    base = base_id or fallback
    categoria = sanitize_text(entry.get("categoria", ""))
    if not base:
        return ""
    if categoria:
        return f"{categoria}|{base}"
    return base


def build_surtidor_key(entry) -> str:
    key = build_auto_number_key(entry)
    if key:
        return key
    fallback = f"{sanitize_text(entry.get('cliente', ''))}|{sanitize_text(entry.get('hora', ''))}"
    return fallback


def apply_surtidor_assignments(entries, assignments: dict) -> None:
    for entry in entries:
        key = build_surtidor_key(entry)
        if key:
            entry["surtidor"] = assignments.get(key, "")


def assign_shared_numbers(entries_local, entries_foraneo):
    combined = list(entries_local) + list(entries_foraneo)
    combined.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    counter = count(1)
    def next_number() -> int:
        return next(counter)

    number_map = {}
    for entry in combined:
        key = build_auto_number_key(entry)
        if not key:
            number = next_number()
            key = f"__auto__{number}"
            number_map[key] = number
        elif key not in number_map:
            number_map[key] = next_number()
        entry["numero"] = number_map[key]
        # NO borrar sort_key


_AUTO_LIST_COUNTER = count(1)


def assign_display_numbers(auto_local_entries, auto_foraneo_entries, today_date) -> None:
    for entry in auto_local_entries + auto_foraneo_entries:
        entry.pop("display_num", None)

    combined_local = [e for e in auto_local_entries if _is_visible_auto_entry(e)]
    turno_priority = [
        "☀️ Local Mañana",
        "🌙 Local Tarde",
        "🌵 Saltillo",
        "📦 Pasa a Bodega",
        "📍 Local (sin turno)",
    ]
    grouped_local: dict[str, list] = {label: [] for label in turno_priority}
    for entry in combined_local:
        turno = normalize_turno_label(entry.get("turno", ""))
        if not turno:
            turno = "📍 Local (sin turno)"
        if turno not in grouped_local:
            grouped_local[turno] = []
        grouped_local[turno].append(entry)

    ordered_labels = [
        label for label in turno_priority if label in grouped_local and grouped_local[label]
    ]
    extra_labels = sorted(
        [
            label
            for label in grouped_local.keys()
            if label not in turno_priority and grouped_local[label]
        ]
    )
    ordered_labels.extend(extra_labels)

    next_number = 1
    for label in ordered_labels:
        entries = sort_entries_by_delivery(grouped_local[label])
        visible_entries = entries[:140]
        for offset, entry in enumerate(visible_entries, start=next_number):
            entry["display_num"] = offset
        next_number += len(visible_entries)

    combined_foraneo = list(auto_foraneo_entries)
    ant = filter_entries_before_date(combined_foraneo, today_date)
    ant = [e for e in ant if _is_visible_auto_entry(e)]
    ant = sort_entries_by_delivery(ant)
    visible_ant = ant[:140]
    for offset, entry in enumerate(visible_ant, start=1):
        entry["display_num"] = offset

    next_number = 1 + len(visible_ant)
    hoy_entries = filter_entries_on_or_after(combined_foraneo, today_date)
    sin_fecha = filter_entries_no_entrega_date(combined_foraneo)

    seen = set()
    merged = []
    for entry in (hoy_entries + sin_fecha):
        key = sanitize_text(entry.get("id_pedido", "")) or (
            sanitize_text(entry.get("cliente", "")) + "|" + sanitize_text(entry.get("hora", ""))
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(entry)

    merged = [e for e in merged if _is_visible_auto_entry(e)]
    merged = sort_entries_by_delivery(merged)
    visible_merged = merged[:140]
    for offset, entry in enumerate(visible_merged, start=next_number):
        entry["display_num"] = offset

_TURNOS_CANONICAL = {
    "☀ local manana": "☀️ Local Mañana",
    "local manana": "☀️ Local Mañana",
    "🌙 local tarde": "🌙 Local Tarde",
    "local tarde": "🌙 Local Tarde",
    "🌵 saltillo": "🌵 Saltillo",
    "saltillo": "🌵 Saltillo",
    "📦 pasa a bodega": "📦 Pasa a Bodega",
    "pasa a bodega": "📦 Pasa a Bodega",
}


def normalize_turno_label(value: str) -> str:
    base = sanitize_text(value)
    if not base:
        return ""

    without_variation = base.replace("\ufe0f", "")
    normalized = unicodedata.normalize("NFKD", without_variation)
    ascii_clean = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    key = " ".join(ascii_clean.lower().split())

    return _TURNOS_CANONICAL.get(key, base.strip())


def format_cliente_line(row) -> str:
    folio = sanitize_text(row.get("Folio_Factura", ""))
    cliente = sanitize_text(row.get("Cliente", ""))
    if folio and cliente:
        return f"📄 <b>{folio}</b> – {cliente}"
    if folio:
        return f"📄 <b>{folio}</b>"
    if cliente:
        return cliente
    return "—"


def unique_preserve(values):
    seen = set()
    ordered = []
    for value in values:
        cleaned = sanitize_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def build_base_entry(row, categoria: str):
    entry = {
        "categoria": categoria,
        "estado": sanitize_text(row.get("Estado", "")),
        "completados_limpiado": sanitize_text(row.get("Completados_Limpiado", "")),
        "cliente": format_cliente_line(row),
        "cliente_nombre": sanitize_text(row.get("Cliente", "")),
        "folio": sanitize_text(row.get("Folio_Factura", "")),
        "fecha": format_date(row.get("Fecha_Entrega")),
        "hora": format_time(row.get("Hora_Registro")),
        "fecha_entrega_dt": parse_datetime(row.get("Fecha_Entrega")),
        "id_pedido": sanitize_text(row.get("ID_Pedido", "")),
        "vendedor": sanitize_text(row.get("Vendedor_Registro", "")),
        "turno": sanitize_text(row.get("Turno", "")),
        "tipo_envio": sanitize_text(row.get("Tipo_Envio", "")),
        "tipo_envio_original": sanitize_text(row.get("Tipo_Envio_Original", "")),
        "tipo": sanitize_text(row.get("Tipo", "")),
        "badges": [],
        "details": [],
        "sort_key": compute_sort_key(row),
    }
    return entry


def build_entries_local(df_local: pd.DataFrame):
    entries = []
    for _, row in df_local.iterrows():
        entry = build_base_entry(row, "📍 Local")
        badges = unique_preserve([entry["turno"], entry["tipo_envio"]])
        details = []
        estado_entrega = sanitize_text(row.get("Estado_Entrega", ""))
        if estado_entrega == "⏳ No Entregado":
            details.append("⏳ Entrega: No Entregado")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_casos(df_casos: pd.DataFrame):
    entries = []
    for _, row in df_casos.iterrows():
        entry = build_base_entry(row, "🧰 Casos")
        badges = unique_preserve([entry["tipo"], entry["turno"], entry["tipo_envio_original"]])
        details = []
        if entry["tipo_envio"] and entry["tipo_envio"] not in badges:
            details.append(f"🚚 {entry['tipo_envio']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_foraneo(df_for: pd.DataFrame):
    entries = []
    for _, row in df_for.iterrows():
        entry = build_base_entry(row, "🌍 Foráneo")
        badges = unique_preserve([entry["tipo_envio"], entry["turno"]])
        details = []
        tipo_caso = sanitize_text(entry.get("tipo", ""))
        if tipo_caso and tipo_caso != "—":
            details.append(tipo_caso)
        elif entry["tipo_envio_original"] and entry["tipo_envio_original"] not in badges:
            details.append(f"📦 {entry['tipo_envio_original']}")
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_cdmx(df_cdmx: pd.DataFrame):
    entries = []
    for _, row in df_cdmx.iterrows():
        entry = build_base_entry(row, "🏙️ CDMX")
        badges = unique_preserve(["🏙️ Pedido CDMX", entry["tipo_envio"]])
        details = []
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def build_entries_guias(df_guias: pd.DataFrame):
    entries = []
    for _, row in df_guias.iterrows():
        entry = build_base_entry(row, "📋 Guía")
        badges = unique_preserve(["📋 Solicitud de Guía", entry["tipo_envio"]])
        details = []
        entry["badges"] = badges
        entry["details"] = unique_preserve(details)
        entries.append(entry)
    return entries


def render_auto_cards(entries, layout: str = "small"):
    if not entries:
        st.info("No hay pedidos para mostrar.")
        return

    panel_class = "auto-panel-small" if layout == "small" else "auto-panel-large"
    card_class = "auto-card-small" if layout == "small" else "auto-card-large"

    cards_html = []
    for entry in entries:
        badges_html = ""
        badges = entry.get("badges", [])
        if badges:
            badges_html = "<div class='auto-card-badges'>" + "".join(
                f"<span class='auto-card-badge'>{badge}</span>" for badge in badges
            ) + "</div>"

        meta_html = (
            f"<div class='auto-card-meta'>📅 Fecha Entrega: {entry['fecha']}</div>"
            if entry.get("fecha")
            else ""
        )

        detail_parts = []
        for part in entry.get("details", []):
            cleaned = sanitize_text(part)
            if cleaned:
                detail_parts.append(cleaned)
        detail_html = (
            "<div class='auto-card-details'>" + " · ".join(detail_parts) + "</div>"
            if detail_parts
            else ""
        )

        cards_html.append(
            dedent(
                f"""
                <div class='{card_class}'>
                    <div class='auto-card-header'>
                        <div>
                            <span class='card-number'>#{entry.get('numero', '?')}</span>
                            <span class='card-category'>{entry.get('categoria', '')}</span>
                        </div>
                        <div class='auto-card-status'>{entry.get('estado', '')}</div>
                    </div>
                    <div class='auto-card-client'>{entry.get('cliente', '—')}</div>
                    {badges_html}
                    {meta_html}
                    {detail_html}
                </div>
                """
            ).strip()
        )

    st.markdown(
        f"<div class='{panel_class}'>" + "".join(cards_html) + "</div>",
        unsafe_allow_html=True,
    )

def render_auto_list(
    entries,
    title: str,
    subtitle: str = "",
    max_rows: int = 60,
    start_number: int = 1,
    scroll_threshold: int = 10,
    panel_height: int = 720,
    scroll_max_height: int = 640,
):
    if not entries:
        st.info("No hay pedidos para mostrar.")
        return start_number

    indexed_entries = list(enumerate(entries, start_number))
    visible = indexed_entries[:max_rows]

    rows_html = []
    for display_number, e in visible:
        chips = []

        # Chips principales (máx 3)
        for b in (e.get("badges", []) or [])[:3]:
            bb = sanitize_text(b)
            if bb:
                chips.append(f"<span class='chip'>{bb}</span>")

        # Detalles (máx 1)
        details = e.get("details", []) or []
        if details:
            d0 = sanitize_text(details[0])
            if d0:
                chips.append(f"<span class='chip'>{d0}</span>")

        # ⚠️ Marca “Sin fecha”
        dt_ent = e.get("fecha_entrega_dt")
        try:
            is_missing = (dt_ent is None) or pd.isna(dt_ent)
        except Exception:
            is_missing = (dt_ent is None)
        if is_missing:
            chips.insert(0, "<span class='chip'>⚠️ Sin Fecha_Entrega</span>")

        # 📅 Fecha de entrega visible (si existe)
        fecha_txt = sanitize_text(e.get("fecha", ""))
        if fecha_txt:
            chips.insert(0, f"<span class='chip'>📅 {fecha_txt}</span>")


        chips_html = (
            f"<div class='board-meta'>{''.join(chips)}"
            f"<span class='board-status'>{sanitize_text(e.get('estado',''))}</span></div>"
        )

        surtidor = sanitize_text(e.get("surtidor", ""))
        surtidor_html = (
            f"<span class='surtidor-tag'>{surtidor}</span>" if surtidor else ""
        )
        rows_html.append(
            f"""
            <tr class='board-row'>
              <td class='board-n'>#{display_number}</td>
              <td class='board-main'>
                <div class='board-client'>{e.get('cliente','—')}{surtidor_html}</div>
                {chips_html}
              </td>
            </tr>
            """
        )

    sub = f"<div class='board-sub'>{subtitle}</div>" if subtitle else ""

    list_id = f"board-{next(_AUTO_LIST_COUNTER)}"
    enable_auto_scroll = len(visible) > scroll_threshold
    scroll_duration = max(14, len(visible) * 1.2)
    scroll_class = "board-scroll auto-scroll" if enable_auto_scroll else "board-scroll"

    html = f"""
    <style>
    .board-col{{flex:1;background:rgba(18,18,20,0.92);border-radius:0.9rem;padding:0.55rem 0.7rem;box-shadow:0 2px 12px rgba(0,0,0,0.25);height:100%;}}
    .board-title{{display:flex;justify-content:space-between;align-items:center;gap:0.6rem;margin-bottom:0.45rem;font-weight:800;font-size:1.15rem;color:#fff;}}
    .board-sub{{font-size:0.8rem;opacity:0.8;font-weight:600;}}
    .board-table{{width:100%;border-collapse:collapse;table-layout:fixed;}}
    .board-row{{border-top:1px solid rgba(255,255,255,0.08);}}
    .board-row:first-child{{border-top:none;}}
    .board-n{{width:2.6rem;font-size:1.1rem;font-weight:900;padding:0.18rem 0.15rem;opacity:0.95;vertical-align:top;white-space:nowrap;color:#fff;}}
    .board-main{{padding:0.18rem 0.15rem;vertical-align:top;}}
    .board-client{{font-size:0.95rem;font-weight:800;line-height:1.15rem;color:#fff;word-break:break-word;display:flex;align-items:center;gap:0.4rem;flex-wrap:wrap;}}
    .surtidor-tag{{margin-left:0.25rem;padding:0.1rem 0.45rem;border-radius:0.7rem;background:rgba(114,190,255,0.18);color:#a9dcff;font-weight:800;font-size:0.75rem;white-space:nowrap;}}
    .board-meta{{margin-top:0.12rem;display:flex;flex-wrap:wrap;gap:0.25rem;font-size:0.72rem;opacity:0.85;font-weight:650;align-items:center;color:#fff;}}
    .chip{{padding:0.05rem 0.4rem;border-radius:0.6rem;background:rgba(255,255,255,0.10);white-space:nowrap;}}
    .board-status{{margin-left:auto;font-size:0.82rem;font-weight:900;white-space:nowrap;opacity:0.95;}}
    #{list_id} .board-scroll{{max-height:{scroll_max_height}px;overflow:hidden;position:relative;}}
    #{list_id} .board-scroll.auto-scroll .board-table{{animation: board-scroll-{list_id} var(--scroll-duration, 18s) linear infinite;}}
    @keyframes board-scroll-{list_id} {{
        0% {{ transform: translateY(0); }}
        10% {{ transform: translateY(0); }}
        45% {{ transform: translateY(calc(var(--scroll-distance, 0px) * -1)); }}
        55% {{ transform: translateY(calc(var(--scroll-distance, 0px) * -1)); }}
        90% {{ transform: translateY(0); }}
        100% {{ transform: translateY(0); }}
    }}
    </style>
    <div class="board-col" id="{list_id}">
    <div class="board-title">
        <div>{title}{sub}</div>
        <div class="board-sub">Mostrando {len(visible)}/{len(entries)}</div>
    </div>
    <div class="{scroll_class}" data-auto-scroll="{str(enable_auto_scroll).lower()}">
        <table class="board-table">
            {''.join(rows_html)}
        </table>
    </div>
    </div>
    <script>
    (() => {{
        const root = document.getElementById("{list_id}");
        if (!root) return;
        const wrapper = root.querySelector(".board-scroll");
        const table = root.querySelector(".board-table");
        if (!wrapper || !table) return;
        if (wrapper.dataset.autoScroll !== "true") return;
        const distance = table.scrollHeight - wrapper.clientHeight;
        if (distance > 0) {{
            wrapper.style.setProperty("--scroll-distance", `${{distance}}px`);
            wrapper.style.setProperty("--scroll-duration", "{scroll_duration}s");
            wrapper.classList.add("auto-scroll");
        }}
    }})();
    </script>
    """


    # ✅ Forzar render HTML real (no texto)
    components.html(html, height=panel_height, scrolling=False)
    return start_number + len(visible)


def _is_done_estado(estado: str) -> bool:
    s = sanitize_text(estado)
    return s in {"🟢 Completado", "🟣 Cancelado", "✅ Viajó"}


def _is_visible_auto_entry(entry: dict) -> bool:
    if not _is_done_estado(entry.get("estado", "")):
        return True
    return sanitize_text(entry.get("completados_limpiado", "")) == ""


def _is_surtidor_visible_estado(estado: str) -> bool:
    cleaned = sanitize_text(estado).lower()
    if any(term in cleaned for term in ("pendiente", "demorado", "modificacion")):
        return False
    return "en proceso" in cleaned or "completado" in cleaned


def last_3_days_previous_range(today_date):
    start = today_date - timedelta(days=3)
    end = today_date - timedelta(days=1)
    return start, end


def filter_entries_by_entrega(entries, start_date, end_date):
    """Incluye entries cuya Fecha_Entrega_dt esté entre start_date y end_date (incluye límites)."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if start_date <= d <= end_date:
            out.append(e)
    return out


def filter_entries_before_date(entries, reference_date):
    """Incluye entries con Fecha_Entrega anterior a reference_date."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d < reference_date:
            out.append(e)
    return out


def filter_entries_on_or_after(entries, reference_date):
    """Incluye entries con Fecha_Entrega en reference_date o posterior."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d >= reference_date:
            out.append(e)
    return out


def filter_entries_on_date(entries, reference_date):
    """Incluye entries con Fecha_Entrega exactamente en reference_date."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            continue
        try:
            if pd.isna(dt):
                continue
        except Exception:
            continue

        d = pd.to_datetime(dt).date()
        if d == reference_date:
            out.append(e)
    return out


def filter_entries_no_entrega_date(entries):
    """Entries sin Fecha_Entrega (para que no se pierdan)."""
    out = []
    for e in entries:
        dt = e.get("fecha_entrega_dt")
        if dt is None:
            out.append(e)
            continue
        try:
            if pd.isna(dt):
                out.append(e)
        except Exception:
            pass
    return out


def sort_entries_by_delivery(entries):
    """Ordena por Fecha_Entrega (más próxima primero), luego por sort_key."""
    def _key(e):
        dt = e.get("fecha_entrega_dt")
        try:
            if dt is None or pd.isna(dt):
                dt = pd.Timestamp.max
        except Exception:
            if dt is None:
                dt = pd.Timestamp.max
        if not isinstance(dt, pd.Timestamp):
            try:
                dt = pd.to_datetime(dt)
            except Exception:
                dt = pd.Timestamp.max
        return (dt, e.get("sort_key", pd.Timestamp.max))

    return sorted(entries, key=_key)


def _normalize_match_value(value: str) -> str:
    cleaned = sanitize_text(value)
    return cleaned.lower()


def drop_local_duplicates_for_cases(
    df_local: pd.DataFrame, df_casos: pd.DataFrame
) -> pd.DataFrame:
    if df_local.empty or df_casos.empty:
        return df_local

    case_ids = set()
    case_folios = set()
    if "ID_Pedido" in df_casos.columns:
        case_ids = {
            _normalize_match_value(v)
            for v in df_casos["ID_Pedido"].astype(str)
            if _normalize_match_value(v)
        }
    if "Folio_Factura" in df_casos.columns:
        case_folios = {
            _normalize_match_value(v)
            for v in df_casos["Folio_Factura"].astype(str)
            if _normalize_match_value(v)
        }

    if not case_ids and not case_folios:
        return df_local

    local_ids = pd.Series("", index=df_local.index, dtype=str)
    local_folios = pd.Series("", index=df_local.index, dtype=str)
    if "ID_Pedido" in df_local.columns:
        local_ids = df_local["ID_Pedido"].astype(str).apply(_normalize_match_value)
    if "Folio_Factura" in df_local.columns:
        local_folios = df_local["Folio_Factura"].astype(str).apply(_normalize_match_value)

    mask_case_id = (
        local_ids.isin(case_ids)
        if case_ids
        else pd.Series(False, index=df_local.index)
    )
    mask_case_folio = (
        local_folios.isin(case_folios)
        if case_folios
        else pd.Series(False, index=df_local.index)
    )
    mask_duplicate = mask_case_id | mask_case_folio

    return df_local.loc[~mask_duplicate].copy()


def get_local_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    base_local = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        base_local = df_all[
            df_all["Tipo_Envio"].isin(["📍 Pedido Local", "🎓 Cursos y Eventos"])
        ].copy()

    extra_local = pd.DataFrame()
    if not df_all.empty and "Turno" in df_all.columns:
        turnos_locales = {"🌵 Saltillo", "📦 Pasa a Bodega"}
        turno_normalizado = df_all["Turno"].fillna("").astype(str).str.strip().apply(
            normalize_turno_label
        )
        mask_turno_local = turno_normalizado.isin(turnos_locales)
        extra_local = df_all[mask_turno_local].copy()

    casos_local, _ = get_case_envio_assignments(df_all)
    frames = [df for df in [base_local, extra_local, casos_local] if not df.empty]
    if not frames:
        return pd.DataFrame()

    df_local = pd.concat(frames, ignore_index=True, sort=False)
    df_local = df_local.drop_duplicates()

    if "Completados_Limpiado" not in df_local.columns:
        df_local["Completados_Limpiado"] = ""

    if "Estado_Entrega" in df_local.columns:
        estado_entrega_col = df_local["Estado_Entrega"].astype(str).str.strip()
        mask_no_entregado = estado_entrega_col == "⏳ No Entregado"
    else:
        mask_no_entregado = pd.Series(False, index=df_local.index, dtype=bool)

    filtro_completados = df_local["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
    filtro_limpiado = df_local["Completados_Limpiado"].astype(str).str.lower() == "sí"

    df_local = df_local[~(filtro_completados & filtro_limpiado & ~mask_no_entregado)].copy()

    if "Turno" not in df_local.columns:
        df_local["Turno"] = ""

    df_local["Turno"] = df_local["Turno"].fillna("").astype(str).str.strip()
    df_local.loc[df_local["Turno"].str.lower() == "nan", "Turno"] = ""
    df_local["Turno"] = df_local["Turno"].apply(normalize_turno_label)

    mask_curso_evento = df_local["Tipo_Envio"] == "🎓 Cursos y Eventos"
    mask_turno_vacio = df_local["Turno"].str.strip() == ""
    df_local.loc[mask_curso_evento & mask_turno_vacio, "Turno"] = "🎓 Cursos y Eventos"

    return df_local


def get_foraneo_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    base_foraneo = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        base_foraneo = df_all[df_all["Tipo_Envio"] == "🚚 Pedido Foráneo"].copy()

    _, casos_foraneo = get_case_envio_assignments(df_all)
    frames = [df for df in [base_foraneo, casos_foraneo] if not df.empty]
    if not frames:
        return pd.DataFrame()

    df_for = pd.concat(frames, ignore_index=True, sort=False)

    if "Completados_Limpiado" not in df_for.columns:
        df_for["Completados_Limpiado"] = ""

    df_for = df_for[
        ~(
            df_for["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_for["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()

    return df_for


def get_cdmx_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_cdmx = df_all[df_all["Tipo_Envio"] == "🏙️ Pedido CDMX"].copy()
    if df_cdmx.empty:
        return df_cdmx
    if "Completados_Limpiado" not in df_cdmx.columns:
        df_cdmx["Completados_Limpiado"] = ""
    df_cdmx = df_cdmx[
        ~(
            df_cdmx["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_cdmx["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()
    return df_cdmx


def get_guias_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "Tipo_Envio" not in df_all.columns:
        return pd.DataFrame()
    df_guias = df_all[df_all["Tipo_Envio"] == "📋 Solicitudes de Guía"].copy()
    if df_guias.empty:
        return df_guias
    if "Completados_Limpiado" not in df_guias.columns:
        df_guias["Completados_Limpiado"] = ""
    df_guias = df_guias[
        ~(
            df_guias["Estado"].isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
            & (df_guias["Completados_Limpiado"].astype(str).str.lower() == "sí")
        )
    ].copy()
    return df_guias


def _etiqueta_tipo_caso(valor: str) -> str:
    s = sanitize_text(valor).lower()
    if "garant" in s:
        return "🛠 Garantía"
    if "devolu" in s:
        return "🔁 Devolución"
    return "—"


def get_casos_orders(df_all: pd.DataFrame) -> pd.DataFrame:
    df_casos = load_casos_from_gsheets()
    if df_casos.empty:
        df_casos = pd.DataFrame()
    else:
        raw_headers = df_casos.columns.tolist()
        fixed = []
        seen_empty = 0
        for h in raw_headers:
            h_norm = unicodedata.normalize("NFKD", h or "").encode("ascii", "ignore").decode("ascii")
            h_norm = h_norm.strip().replace(" ", "_")
            if not h_norm:
                seen_empty += 1
                h_norm = f"_col_vacia_{seen_empty}"
            base = h_norm
            k = 2
            while h_norm in fixed:
                h_norm = f"{base}_{k}"
                k += 1
            fixed.append(h_norm)
        df_casos.columns = fixed

        dt_cols = [
            "Hora_Registro",
            "Fecha_Entrega",
            "Fecha_Completado",
            "Fecha_Pago_Comprobante",
            "Hora_Proceso",
            "Fecha_Recepcion_Devolucion",
        ]
        for c in dt_cols:
            if c in df_casos.columns:
                df_casos[c] = pd.to_datetime(df_casos[c], errors="coerce")

        for base in [
            "ID_Pedido",
            "Cliente",
            "Vendedor_Registro",
            "Folio_Factura",
            "Estado",
            "Tipo_Envio",
            "Tipo_Envio_Original",
            "Turno",
        ]:
            if base not in df_casos.columns:
                df_casos[base] = ""
            else:
                df_casos[base] = df_casos[base].astype(str).fillna("").str.strip()

        if "Turno" in df_casos.columns:
            df_casos["Turno"] = df_casos["Turno"].apply(normalize_turno_label)

        if "Tipo_Caso" not in df_casos.columns and "Tipo_Envio" in df_casos.columns:
            df_casos["Tipo_Caso"] = df_casos["Tipo_Envio"]
        if "Tipo_Envio_Original" not in df_casos.columns and "Tipo_Envio" in df_casos.columns:
            df_casos["Tipo_Envio_Original"] = df_casos["Tipo_Envio"]

    df_garantias_pedidos = pd.DataFrame()
    if not df_all.empty and "Tipo_Envio" in df_all.columns:
        df_garantias_pedidos = df_all[
            df_all["Tipo_Envio"].astype(str).str.contains("Garant", case=False, na=False)
        ].copy()
        if not df_garantias_pedidos.empty:
            df_garantias_pedidos["Tipo_Caso"] = df_garantias_pedidos["Tipo_Envio"]
            df_garantias_pedidos["Tipo_Envio_Original"] = df_garantias_pedidos["Tipo_Envio"]

    casos = pd.concat([df_casos, df_garantias_pedidos], ignore_index=True)
    if casos.empty:
        return casos

    mask = casos["Tipo_Caso"].astype(str).str.contains("Devoluci|Garant", case=False, na=False)
    casos = casos[mask].copy()
    if casos.empty:
        return casos

    if "Completados_Limpiado" not in casos.columns:
        casos["Completados_Limpiado"] = ""
    if "Estado" in casos.columns:
        casos = casos[
            ~(
                casos["Estado"].astype(str).str.strip().isin(["🟢 Completado", "🟣 Cancelado", "✅ Viajó"])
                & (casos["Completados_Limpiado"].astype(str).str.lower() == "sí")
            )
        ]

    for base in [
        "Fecha_Entrega",
        "Cliente",
        "Vendedor_Registro",
        "Estado",
        "Folio_Factura",
        "Turno",
        "Tipo_Envio_Original",
        "Tipo_Envio",
    ]:
        if base not in casos.columns:
            casos[base] = ""

    casos["Tipo"] = casos["Tipo_Caso"].apply(_etiqueta_tipo_caso)
    return casos


def _normalize_envio_original(value: str) -> str:
    """Remove emojis/accents and return a lowercased representation."""
    cleaned = sanitize_text(value)
    if not cleaned:
        return ""
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    filtered = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in ascii_only)
    return " ".join(filtered.lower().split())


def get_case_envio_assignments(
    df_all: pd.DataFrame, df_casos: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return local/foráneo assignments detected from special cases."""

    if df_casos is None:
        df_casos = get_casos_orders(df_all)

    if df_casos.empty:
        return pd.DataFrame(), pd.DataFrame()

    working = df_casos.copy()
    if "Turno" not in working.columns:
        working["Turno"] = ""

    normalized = working["Tipo_Envio_Original"].apply(_normalize_envio_original)
    turno_clean = working["Turno"].astype(str).fillna("").str.strip()

    mask_local = normalized.str.contains("local", na=False) & (turno_clean != "")
    mask_foraneo = normalized.str.contains("foraneo", na=False)

    df_local = working[mask_local].copy()
    if not df_local.empty:
        df_local["Tipo_Envio"] = "📍 Pedido Local"

    df_foraneo = working[mask_foraneo].copy()
    if not df_foraneo.empty:
        df_foraneo["Tipo_Envio"] = "🚚 Pedido Foráneo"

    return df_local, df_foraneo

st.markdown(
    """
    <style>
    .board-wrap{display:flex;gap:0.8rem;width:100%;align-items:flex-start;}
    .board-col{flex:1;background:rgba(18,18,20,0.92);border-radius:0.9rem;padding:0.8rem 0.9rem;box-shadow:0 2px 14px rgba(0,0,0,0.25);min-height:70vh;}
    .board-title{display:flex;justify-content:space-between;align-items:center;gap:0.6rem;margin-bottom:0.6rem;font-weight:800;font-size:1.35rem;color:#fff;}
    .board-sub{font-size:0.9rem;opacity:0.8;font-weight:600;}
    .board-table{width:100%;border-collapse:collapse;table-layout:fixed;}
    .board-row{border-top:1px solid rgba(255,255,255,0.08);}
    .board-row:first-child{border-top:none;}
    .board-n{width:3.2rem;font-size:1.35rem;font-weight:900;padding:0.25rem 0.2rem;opacity:0.95;vertical-align:top;white-space:nowrap;}
    .board-main{padding:0.25rem 0.2rem;vertical-align:top;}
    .board-client{font-size:1.05rem;font-weight:800;line-height:1.25rem;color:#fff;word-break:break-word;}
    .board-meta{margin-top:0.18rem;display:flex;flex-wrap:wrap;gap:0.35rem;font-size:0.85rem;opacity:0.85;font-weight:650;align-items:center;}
    .chip{padding:0.1rem 0.45rem;border-radius:0.7rem;background:rgba(255,255,255,0.10);white-space:nowrap;}
    .board-status{margin-left:auto;font-size:0.95rem;font-weight:900;white-space:nowrap;opacity:0.95;}
    @media (min-width: 1200px){
      .board-client{font-size:1.15rem;}
      .board-n{font-size:1.5rem;}
      .board-title{font-size:1.5rem;}
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Estilos para paneles automáticos
st.markdown(
    """
    <style>
    .auto-panel-small {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
        gap: 0.35rem;
        margin-top: 0.25rem;
        align-items: stretch;
    }
    .auto-panel-large {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
        gap: 0.6rem;
        margin-top: 0.5rem;
        align-items: stretch;
    }
    .auto-card-small,
    .auto-card-large {
        background: rgba(28, 28, 30, 0.9);
        border-radius: 0.75rem;
        padding: 0.55rem 0.75rem;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.25);
        color: #f7f7f7;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 9rem;
    }
    .auto-card-small {
        font-size: 0.78rem;
        line-height: 1.15rem;
    }
    .auto-card-large {
        font-size: 0.95rem;
        line-height: 1.3rem;
        padding: 0.75rem 1rem;
        min-height: 11rem;
    }
    .auto-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.5rem;
        font-weight: 600;
    }
    .auto-card-header .card-number {
        background: rgba(255, 255, 255, 0.15);
        border-radius: 0.6rem;
        padding: 0.15rem 0.45rem;
        font-size: 0.75rem;
        letter-spacing: 0.03em;
    }
    .auto-card-header .card-category {
        margin-left: 0.4rem;
    }
    .auto-card-status {
        font-weight: 700;
    }
    .auto-card-client {
        margin-top: 0.35rem;
        font-weight: 500;
    }
    .auto-card-badges {
        margin-top: 0.3rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.25rem;
    }
    .auto-card-badge {
        background: rgba(255, 255, 255, 0.12);
        padding: 0.1rem 0.35rem;
        border-radius: 0.5rem;
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 0.02em;
    }
    .auto-card-meta {
        margin-top: 0.25rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        opacity: 0.85;
    }
    .auto-card-details {
        margin-top: 0.3rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        opacity: 0.85;
    }
    @media (max-width: 1100px) {
        .auto-card-large {
            font-size: 0.88rem;
            padding: 0.65rem 0.8rem;
        }
        .auto-panel-large {
            gap: 0.45rem;
        }
    }
    </style>
""",
    unsafe_allow_html=True,
)

# --- IDs de Sheets ---
GOOGLE_SHEET_ID = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"
SHEET_PEDIDOS = "datos_pedidos"
SHEET_CASOS = "casos_especiales"
SHEET_CONFIRMADOS = "pedidos_confirmados"


# --- Auth helpers ---
def construir_gspread_client(creds_dict):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
        creds_dict["private_key"] = (
            creds_dict["private_key"].replace("\\n", "\n").strip()
        )
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


@st.cache_resource
def get_gspread_client(_credentials_json_dict, max_attempts: int = 3):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    _credentials_json_dict["private_key"] = (
        _credentials_json_dict["private_key"].replace("\\n", "\n").strip()
    )

    for attempt in range(1, max_attempts + 1):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            _credentials_json_dict, scope
        )
        client = gspread.authorize(creds)
        try:
            client.open_by_key(GOOGLE_SHEET_ID)
            return client
        except gspread.exceptions.APIError as e:
            if "expired" in str(e).lower() or "RESOURCE_EXHAUSTED" in str(e):
                st.cache_resource.clear()
            wait_time = 2 ** (attempt - 1)
            if attempt >= max_attempts:
                st.error(
                    f"❌ Error al autenticar con Google Sheets después de {max_attempts} intentos: {e}"
                )
                st.stop()
            st.warning(
                f"🔁 Error de autenticación. Reintentando en {wait_time} s..."
            )
            time.sleep(wait_time)


# --- AWS S3 ---
try:
    if "aws" not in st.secrets:
        st.error(
            "❌ Las credenciales de AWS S3 no se encontraron en Streamlit secrets."
        )
        st.stop()
    AWS_CREDENTIALS = st.secrets["aws"]
    AWS_ACCESS_KEY_ID = AWS_CREDENTIALS["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = AWS_CREDENTIALS["aws_secret_access_key"]
    AWS_REGION = AWS_CREDENTIALS["aws_region"]
    S3_BUCKET_NAME = AWS_CREDENTIALS["s3_bucket_name"]
except Exception as e:
    st.error(f"❌ Error al cargar las credenciales de AWS S3: {e}")
    st.stop()


@st.cache_resource
def get_s3_client():
    try:
        return boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
    except Exception as e:
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        st.stop()


# --- Clientes iniciales ---
try:
    if "gsheets" not in st.secrets:
        st.error(
            "❌ Las credenciales de Google Sheets no se encontraron en Streamlit secrets."
        )
        st.stop()
    GSHEETS_CREDENTIALS = json.loads(st.secrets["gsheets"]["google_credentials"])
    GSHEETS_CREDENTIALS["private_key"] = GSHEETS_CREDENTIALS["private_key"].replace(
        "\\n", "\n"
    )

    g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
    s3_client = get_s3_client()
    spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
    worksheet_main = spreadsheet.worksheet(SHEET_PEDIDOS)
    worksheet_casos = spreadsheet.worksheet(SHEET_CASOS)

except gspread.exceptions.APIError as e:
    if "ACCESS_TOKEN_EXPIRED" in str(e) or "UNAUTHENTICATED" in str(e):
        st.cache_resource.clear()
        st.warning("🔄 La sesión con Google Sheets expiró. Reconectando...")
        time.sleep(1)
        g_spread_client = get_gspread_client(_credentials_json_dict=GSHEETS_CREDENTIALS)
        s3_client = get_s3_client()
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet_main = spreadsheet.worksheet(SHEET_PEDIDOS)
        worksheet_casos = spreadsheet.worksheet(SHEET_CASOS)
    else:
        st.error(f"❌ Error al autenticar clientes: {e}")
        st.stop()
except Exception as e:
    st.error(f"❌ Error al autenticar clientes: {e}")
    st.stop()


# --- Carga de datos ---
def _fetch_with_retry(worksheet, cache_key: str, max_attempts: int = 4):
    """Lee datos de una worksheet con reintentos y respaldo local.

    Cuando Google Sheets responde con un 429 (límite de cuota) se realizan
    reintentos exponenciales. Si todos los intentos fallan pero se cuenta con
    datos almacenados en la sesión, se devuelven como último recurso para evitar
    detener la aplicación.
    """

    def _is_rate_limit_error(error: Exception) -> bool:
        text = str(error).lower()
        return "rate_limit" in text or "quota" in text or "429" in text

    last_success = st.session_state.get(cache_key)
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            data = worksheet.get_all_values()
            st.session_state[cache_key] = data
            return data
        except gspread.exceptions.APIError as e:
            last_error = e
            if not _is_rate_limit_error(e):
                raise

            wait_time = min(30, 2 ** attempt)
            st.warning(
                f"⚠️ Límite de lectura de Google Sheets alcanzado. "
                f"Reintentando en {wait_time} s (intento {attempt}/{max_attempts})."
            )
            time.sleep(wait_time)

    if last_success is not None:
        st.info(
            "ℹ️ Usando datos en caché debido al límite de cuota de Google Sheets."
        )
        return last_success

    if last_error is not None:
        raise last_error
    raise RuntimeError("No se pudieron obtener datos de Google Sheets")


@st.cache_data(ttl=60)
def load_data_from_gsheets():
    data = _fetch_with_retry(worksheet_main, "_cache_datos_pedidos")
    if not data:
        return pd.DataFrame()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    df["gsheet_row_index"] = df.index + 2

    # Tipos
    if "ID_Pedido" in df.columns:
        df["ID_Pedido"] = df["ID_Pedido"].astype(str)

    dt_cols = [
        "Hora_Registro",
        "Fecha_Entrega",
        "Fecha_Completado",
        "Fecha_Pago_Comprobante",
        "Hora_Proceso",
    ]
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "Turno" in df.columns:
        df["Turno"] = df["Turno"].apply(normalize_turno_label)
    else:
        df["Turno"] = ""

    return df


@st.cache_data(ttl=60)
def load_casos_from_gsheets():
    """Lee 'casos_especiales' y normaliza headers/fechas."""
    data = _fetch_with_retry(worksheet_casos, "_cache_casos_especiales")
    if not data:
        return pd.DataFrame()
    raw_headers = data[0]
    fixed = []
    seen_empty = 0
    for h in raw_headers:
        h = unicodedata.normalize("NFKD", h or "").encode("ascii", "ignore").decode("ascii")
        h = h.strip().replace(" ", "_")
        if not h:
            seen_empty += 1
            h = f"_col_vacia_{seen_empty}"
        base = h
        k = 2
        while h in fixed:
            h = f"{base}_{k}"
            k += 1
        fixed.append(h)
    df = pd.DataFrame(data[1:], columns=fixed)
    df["gsheet_row_index"] = df.index + 2

    # Fechas típicas
    dt_cols = [
        "Hora_Registro",
        "Fecha_Entrega",
        "Fecha_Completado",
        "Fecha_Pago_Comprobante",
        "Hora_Proceso",
        "Fecha_Recepcion_Devolucion",
    ]
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Normaliza campos base
    for base in [
        "ID_Pedido",
        "Cliente",
        "Vendedor_Registro",
        "Folio_Factura",
        "Estado",
        "Tipo_Envio",
        "Tipo_Envio_Original",
        "Turno",
    ]:
        if base in df.columns:
            df[base] = df[base].astype(str).fillna("").str.strip()
    if "Turno" in df.columns:
        df["Turno"] = df["Turno"].apply(normalize_turno_label)
    else:
        df["Turno"] = ""
    return df


@st.cache_data(ttl=600)
def load_confirmados_from_gsheets(credentials_dict: dict, sheet_id: str, sheet_name: str):
    client = get_gspread_client(_credentials_json_dict=credentials_dict)
    spreadsheet = client.open_by_key(sheet_id)
    ws = spreadsheet.worksheet(sheet_name)
    data = _fetch_with_retry(ws, f"_cache_{sheet_name}")
    if not data:
        return pd.DataFrame()

    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)

    for col in [
        "Cliente",
        "Vendedor_Registro",
        "Estado_Pago",
        "Tipo_Envio",
        "Comprobante_Confirmado",
    ]:
        if col not in df.columns:
            df[col] = ""

    if "Monto_Comprobante" in df.columns:
        df["Monto_Comprobante"] = pd.to_numeric(
            df["Monto_Comprobante"], errors="coerce"
        ).fillna(0.0)
    else:
        df["Monto_Comprobante"] = 0.0

    # Fecha real (cuando se registró el pedido)
    if "Hora_Registro" in df.columns:
        df["Hora_Registro"] = pd.to_datetime(df["Hora_Registro"], errors="coerce")
        df["AñoMes"] = df["Hora_Registro"].dt.to_period("M").astype(str)
        df["FechaDia"] = df["Hora_Registro"].dt.date.astype(str)
    else:
        df["Hora_Registro"] = pd.NaT
        df["AñoMes"] = ""
        df["FechaDia"] = ""

    return df


@st.cache_data(ttl=600)
def compute_dashboard_base(df_conf: pd.DataFrame):
    if df_conf.empty:
        return {
            "df": df_conf,
            "ventas_mes": pd.Series(dtype=float),
            "ventas_vendedor": pd.Series(dtype=float),
            "pedidos_vendedor": pd.Series(dtype=int),
        }

    ventas_mes = df_conf.groupby("AñoMes")["Monto_Comprobante"].sum().sort_index()
    ventas_vendedor = (
        df_conf.groupby("Vendedor_Registro")["Monto_Comprobante"]
        .sum()
        .sort_values(ascending=False)
    )
    pedidos_vendedor = df_conf["Vendedor_Registro"].value_counts()

    return {
        "df": df_conf,
        "ventas_mes": ventas_mes,
        "ventas_vendedor": ventas_vendedor,
        "pedidos_vendedor": pedidos_vendedor,
    }


# --- S3 helper (solo lectura presignada aquí) ---
def get_s3_file_url(s3_object_key):
    if not s3_object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_object_key},
            ExpiresIn=3600,
        )
    except Exception:
        return None


def display_attachments(adjuntos_str):
    """Renderiza enlaces; acepta lista separada por comas con keys o URLs completas."""
    if pd.isna(adjuntos_str) or not str(adjuntos_str).strip():
        return "N/A"
    parts = [p.strip() for p in str(adjuntos_str).split(",") if p.strip()]
    links = []
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({p})")
        else:
            url = get_s3_file_url(p)
            name = p.split("/")[-1] or "archivo"
            links.append(f"[{name}]({url})" if url else f"❌ {p}")
    return " | ".join(links) if links else "N/A"


# --- Render tabla compacta ---
def display_dataframe_with_formatting(df_to_display):
    columnas_deseadas = [
        "Fecha_Entrega",
        "Tipo_Envio",
        "Cliente",
        "Vendedor_Registro",
        "Estado",
    ]
    cols_exist = [c for c in columnas_deseadas if c in df_to_display.columns]
    if not cols_exist:
        st.info("No hay columnas relevantes para mostrar.")
        return

    df_vista = df_to_display.copy()

    # Cliente = Folio + Cliente
    if "Folio_Factura" in df_vista.columns and "Cliente" in df_vista.columns:
        df_vista["Cliente"] = df_vista.apply(
            lambda row: f"📄 <b>{row['Folio_Factura']}</b> 🤝 {row['Cliente']}", axis=1
        )

    # Renombrar columnas
    ren = {
        "Fecha_Entrega": "Fecha Entrega",
        "Vendedor_Registro": "Vendedor",
        "Tipo_Envio": "Tipo Envío",
    }
    for k, v in ren.items():
        if k in df_vista.columns:
            df_vista.rename(columns={k: v}, inplace=True)

    if "Fecha Entrega" in df_vista.columns:
        df_vista["Fecha Entrega"] = df_vista["Fecha Entrega"].apply(
            lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
        )

    if "Estado_Entrega" in df_to_display.columns:
        estado_entrega_series = df_to_display["Estado_Entrega"].astype(str).str.strip()
        mask_no_entregado = estado_entrega_series == "⏳ No Entregado"
        if mask_no_entregado.any():
            df_vista["Estado Entrega"] = estado_entrega_series.where(
                mask_no_entregado, ""
            )

    columnas_base = ["Fecha Entrega", "Tipo Envío", "Cliente", "Vendedor", "Estado"]
    if "Estado Entrega" in df_vista.columns:
        if "Estado" in columnas_base:
            idx_estado = columnas_base.index("Estado")
        else:
            idx_estado = len(columnas_base)
        columnas_base.insert(idx_estado, "Estado Entrega")

    mostrar_cols = [c for c in columnas_base if c in df_vista.columns]
    df_vista = df_vista[mostrar_cols]

    st.markdown(df_vista.to_html(escape=False, index=False), unsafe_allow_html=True)


# --- Helpers de métrica + agrupación ---
def status_counts_block(df_src):
    comps = df_src.copy()
    if "Completados_Limpiado" not in comps.columns:
        comps["Completados_Limpiado"] = ""
    completados_visibles = comps[
        (comps["Estado"] == "🟢 Completado")
        & (comps["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cancelados_visibles = comps[
        (comps["Estado"] == "🟣 Cancelado")
        & (comps["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    counts = {
        "🟡 Pendiente": (comps["Estado"] == "🟡 Pendiente").sum(),
        "🔵 En Proceso": (comps["Estado"] == "🔵 En Proceso").sum(),
        "🔴 Demorado": (comps["Estado"] == "🔴 Demorado").sum(),
        "🟣 Cancelado": len(cancelados_visibles),
        "🟢 Completado": len(completados_visibles),
    }
    total = sum(counts.values())
    estados_fijos = ["🟡 Pendiente", "🔵 En Proceso", "🟢 Completado"]
    estados_cond = ["🔴 Demorado", "🟣 Cancelado"]
    items = [("📦 Total Pedidos", total)]
    for e in estados_fijos:
        items.append((e, counts[e]))
    for e in estados_cond:
        if counts[e] > 0:
            items.append((e, counts[e]))

    cols = st.columns(len(items))
    for c, (label, val) in zip(cols, items):
        c.metric(label, int(val))


def group_key_local(row, local_flag_col="Turno"):
    """Devuelve el turno o etiquetas para cursos/eventos y locales sin turno."""
    turno = normalize_turno_label(row.get(local_flag_col, ""))
    if turno:
        return turno
    tipo_envio = str(row.get("Tipo_Envio", "") or "")
    if tipo_envio == "🎓 Cursos y Eventos":
        return "🎓 Cursos y Eventos"
    return "📍 Local (sin turno)"


def show_grouped_panel(df_source, mode: str = "local", group_turno: bool = True):
    """Muestra paneles agrupados por turno (local) o fecha.

    Cuando ``group_turno`` es ``False`` en modo "local", agrupa únicamente
    por ``Fecha_Entrega``.
    """
    if df_source.empty:
        st.info("No hay registros para mostrar.")
        return
    work = df_source.copy()
    work["Fecha_Entrega_Str"] = work["Fecha_Entrega"].dt.strftime("%d/%m")
    work["Fecha_Entrega_Str"] = work["Fecha_Entrega_Str"].fillna("Sin fecha")
    mask_fecha_vacia = work["Fecha_Entrega_Str"].astype(str).str.strip() == ""
    work.loc[mask_fecha_vacia, "Fecha_Entrega_Str"] = "Sin fecha"
    if mode == "foraneo" or (mode == "local" and not group_turno):
        work["Grupo_Clave"] = work["Fecha_Entrega_Str"]
    else:
        work["Grupo_Clave"] = work.apply(
            lambda r: f"{group_key_local(r)} – {r['Fecha_Entrega_Str']}", axis=1
        )
    grupos = []
    grouped = work.groupby(["Grupo_Clave", "Fecha_Entrega"], dropna=False)
    for (clave, f), df_g in sorted(
        grouped,
        key=lambda x: (
            pd.isna(x[0][1]),
            x[0][1] if pd.notna(x[0][1]) else pd.Timestamp.max,
        ),
    ):
        if not df_g.empty:
            grupos.append((f"{clave} ({len(df_g)})", df_g))

    if not grupos:
        st.info("No hay grupos para mostrar.")
        return

    num_cols_per_row = 3
    for i in range(0, len(grupos), num_cols_per_row):
        fila = grupos[i : i + num_cols_per_row]
        cols = st.columns(len(fila))
        for j, (titulo, df_g) in enumerate(fila):
            with cols[j]:
                st.markdown(f"#### {titulo}")
                if "Hora_Registro" in df_g.columns:
                    df_g = df_g.sort_values(
                        by="Hora_Registro", ascending=False
                    ).reset_index(drop=True)
                display_dataframe_with_formatting(df_g)


# =========================
# Helpers para Casos Especiales
# =========================
def status_counts_block_casos(df: pd.DataFrame):
    estados = df.get("Estado", pd.Series(dtype=str)).astype(str)
    if "Completados_Limpiado" not in df.columns:
        df["Completados_Limpiado"] = ""
    total = len(df)
    pend = estados.str.contains("Pendiente", case=False, na=False).sum()
    proc = estados.str.contains("En Proceso", case=False, na=False).sum()
    completados_visibles = df[
        (df["Estado"].astype(str).str.strip() == "🟢 Completado")
        & (df["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cancelados_visibles = df[
        (df["Estado"].astype(str).str.strip() == "🟣 Cancelado")
        & (df["Completados_Limpiado"].astype(str).str.lower() != "sí")
    ]
    cols = st.columns(5)
    cols[0].metric("Total Pedidos", int(total))
    cols[1].metric("🟡 Pendiente", int(pend))
    cols[2].metric("🔵 En Proceso", int(proc))
    cols[3].metric("🟢 Completado", int(len(completados_visibles)))
    cols[4].metric("🟣 Cancelado", int(len(cancelados_visibles)))


if "show_grouped_panel_casos" not in globals():

    def show_grouped_panel_casos(df: pd.DataFrame):
        """Agrupa por Turno (Local) o Foráneo genérico y fecha; muestra tablas."""
        df_local = df.copy()

        # Asegura columnas base
        for base in [
            "Fecha_Entrega",
            "Cliente",
            "Vendedor_Registro",
            "Estado",
            "Folio_Factura",
            "Turno",
            "Tipo_Envio_Original",
        ]:
            if base not in df_local.columns:
                df_local[base] = ""

        # Fecha string para el título
        df_local["Fecha_Entrega_Str"] = (
            df_local["Fecha_Entrega"].dt.strftime("%d/%m").fillna("Sin Fecha")
            if "Fecha_Entrega" in df_local.columns
            else "Sin Fecha"
        )

        # Determinar etiqueta de grupo
        if "Turno" not in df_local.columns:
            df_local["Turno"] = ""

        # Si no hay turno pero viene marcado como Local → etiqueta genérica
        if "Tipo_Envio_Original" in df_local.columns:
            mask_local_sin_turno = (df_local["Turno"].astype(str).str.strip() == "") & (
                df_local["Tipo_Envio_Original"]
                .astype(str)
                .str.contains("Local", case=False, na=False)
            )
            df_local.loc[mask_local_sin_turno, "Turno"] = "📍 Local (sin turno)"

        # Cuando no sea local, foráneo genérico
        es_local = (
            df_local["Turno"]
            .astype(str)
            .str.contains("Local|Saltillo|Bodega|Mañana|Tarde", case=False, na=False)
        )
        df_local.loc[~es_local, "Turno"] = "🌍 Foráneo"

        # Clave de grupo
        df_local["Grupo_Clave"] = df_local.apply(
            lambda r: f"{r['Turno']} – {r['Fecha_Entrega_Str']}", axis=1
        )

        # Orden por fecha real (NaT al final)
        if "Fecha_Entrega" in df_local.columns:
            df_local["_fecha_sort"] = df_local["Fecha_Entrega"].fillna(pd.Timestamp.max)
        else:
            df_local["_fecha_sort"] = pd.Timestamp.max

        grupos = []
        for (clave, _), sub in sorted(
            df_local.groupby(["Grupo_Clave", "Fecha_Entrega"], dropna=False),
            key=lambda x: x[0][1],
        ):
            if "Hora_Registro" in sub.columns:
                sub = sub.sort_values(by="Hora_Registro", ascending=False)
            grupos.append(
                (
                    f"{clave} ({len(sub)})",
                    sub.drop(columns=["_fecha_sort"], errors="ignore"),
                )
            )

        if not grupos:
            st.info("Sin grupos para mostrar.")
            return

        num_cols_per_row = 3
        for i in range(0, len(grupos), num_cols_per_row):
            fila = grupos[i : i + num_cols_per_row]
            cols = st.columns(len(fila))
            for j, (titulo, df_grupo) in enumerate(fila):
                with cols[j]:
                    st.markdown(f"### {titulo}")
                    # Vista enriquecida con tipo de caso, envío y turno
                    base_cols = [
                        "Tipo",
                        "Tipo_Envio_Original",
                        "Turno",
                        "Fecha_Entrega",
                        "Cliente",
                        "Vendedor_Registro",
                        "Estado",
                        "Folio_Factura",
                    ]
                    for c in base_cols:
                        if c not in df_grupo.columns:
                            df_grupo[c] = ""
                    vista = df_grupo[base_cols].copy()
                    vista.rename(
                        columns={
                            "Tipo_Envio_Original": "Tipo Envío",
                            "Fecha_Entrega": "Fecha Entrega",
                            "Vendedor_Registro": "Vendedor",
                        },
                        inplace=True,
                    )
                    vista["Fecha Entrega"] = vista["Fecha Entrega"].apply(
                        lambda x: x.strftime("%d/%m") if pd.notna(x) else ""
                    )
                    vista["Cliente"] = vista.apply(
                        lambda r: f"📄 <b>{r['Folio_Factura']}</b> 🤝 {r['Cliente']}",
                        axis=1,
                    )
                    st.markdown(
                        vista[
                            [
                                "Tipo",
                                "Tipo Envío",
                                "Turno",
                                "Fecha Entrega",
                                "Cliente",
                                "Vendedor",
                                "Estado",
                            ]
                        ].to_html(escape=False, index=False),
                        unsafe_allow_html=True,
                    )


# ===========================
#        MAIN RENDER
# ===========================
df_all = load_data_from_gsheets()

# Tabs principales
tab_labels = [
    "⚙️ Auto Local",
    "🚚 Auto Foráneo",
    "🧑‍🔧 Surtidores",
    "📈 Dashboard",
]

# ---------------------------
# Persistencia de tab activa (para autorefresh)
# ---------------------------
if "active_main_tab" not in st.session_state:
    st.session_state.active_main_tab = 0
elif st.session_state.active_main_tab >= len(tab_labels):
    st.session_state.active_main_tab = 0

def _set_active_main_tab(i: int):
    st.session_state.active_main_tab = i

selected_tab = st.radio(
    "Vista",
    options=list(range(len(tab_labels))),
    format_func=lambda i: tab_labels[i],
    index=st.session_state.active_main_tab,
    horizontal=True,
    label_visibility="collapsed",
    on_change=lambda: _set_active_main_tab(st.session_state["_radio_main_tab"]),
    key="_radio_main_tab",
)

# helper para "simular" tabs
tabs = [None] * len(tab_labels)


# Entradas compartidas para numeración única entre Auto Local y Auto Foráneo
auto_local_entries = []
auto_foraneo_entries = []
if selected_tab in (0, 1, 2):
    df_local_auto = get_local_orders(df_all)
    casos_local_auto, _ = get_case_envio_assignments(df_all)
    df_local_auto = drop_local_duplicates_for_cases(df_local_auto, casos_local_auto)
    if not df_local_auto.empty:
        auto_local_entries.extend(build_entries_local(df_local_auto))
    if not casos_local_auto.empty:
        auto_local_entries.extend(build_entries_casos(casos_local_auto))

    df_for_auto = get_foraneo_orders(df_all)
    if not df_for_auto.empty:
        auto_foraneo_entries.extend(build_entries_foraneo(df_for_auto))

    auto_local_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))
    auto_foraneo_entries.sort(key=lambda e: e.get("sort_key", pd.Timestamp.max))

    assign_shared_numbers(auto_local_entries, auto_foraneo_entries)

    if "surtidor_assignments" not in st.session_state:
        st.session_state.surtidor_assignments = {}
    apply_surtidor_assignments(auto_local_entries, st.session_state.surtidor_assignments)
    apply_surtidor_assignments(auto_foraneo_entries, st.session_state.surtidor_assignments)
    assign_display_numbers(auto_local_entries, auto_foraneo_entries, datetime.now(TZ).date())

# ---------------------------
# TAB 0: Auto Local (Casos asignados) — 2 columnas
# ---------------------------
if selected_tab == 0:
    st_autorefresh(interval=60000, key="auto_refresh_local_casos")

    combined_entries = [
        e for e in auto_local_entries if _is_visible_auto_entry(e)
    ]

    turno_priority = [
        "☀️ Local Mañana",
        "🌙 Local Tarde",
        "🌵 Saltillo",
        "📦 Pasa a Bodega",
        "📍 Local (sin turno)",
    ]
    grouped: dict[str, list] = {label: [] for label in turno_priority}
    for entry in combined_entries:
        turno = normalize_turno_label(entry.get("turno", ""))
        if not turno:
            turno = "📍 Local (sin turno)"
        if turno not in grouped:
            grouped[turno] = []
        grouped[turno].append(entry)

    ordered_labels = [
        label for label in turno_priority if label in grouped and grouped[label]
    ]
    extra_labels = sorted(
        [label for label in grouped.keys() if label not in turno_priority and grouped[label]]
    )
    ordered_labels.extend(extra_labels)

    if not ordered_labels:
        st.info("No hay pedidos locales activos por turno.")
    else:
        col_left, col_right = st.columns(2, gap="large")
        columns = [col_left, col_right]
        next_number = 1

        for idx, label in enumerate(ordered_labels):
            target_col = columns[idx % 2]
            entries = sort_entries_by_delivery(grouped[label])
            with target_col:
                next_number = render_auto_list(
                    entries,
                    title=f"📍 LOCALES • {label}",
                    subtitle="Pedidos activos por turno",
                    max_rows=140,
                    start_number=next_number,
                    scroll_threshold=8,
                    panel_height=380,
                    scroll_max_height=300,
                )

# ---------------------------
# TAB 1: Auto Foráneo (Casos asignados) — 2 columnas
# ---------------------------
if selected_tab == 1:
    st_autorefresh(interval=60000, key="auto_refresh_foraneo_cdmx")

    hoy = datetime.now(TZ).date()


    # 1) Entradas (foráneo + casos asignados a foráneo)
    combined_entries = list(auto_foraneo_entries)

    # 2) Layout: izquierda/derecha
    col_left, col_right = st.columns(2, gap="large")

    # --- IZQUIERDA: ANTERIORES (todos los previos) ---
    with col_left:
        ant = filter_entries_before_date(combined_entries, hoy)
        ant = [e for e in ant if _is_visible_auto_entry(e)]
        ant = sort_entries_by_delivery(ant)

        next_number = render_auto_list(
            ant,
            title="🚚 FORÁNEOS • ANTERIORES",
            subtitle="Fechas previas (sin completados)",
            max_rows=140,
        )

    # --- DERECHA: HOY + FUTUROS + SIN Fecha_Entrega ---
    with col_right:
        hoy_entries = filter_entries_on_or_after(combined_entries, hoy)
        sin_fecha = filter_entries_no_entrega_date(combined_entries)

        seen = set()
        merged = []
        for e in (hoy_entries + sin_fecha):
            key = sanitize_text(e.get("id_pedido", "")) or (
                sanitize_text(e.get("cliente", "")) + "|" + sanitize_text(e.get("hora", ""))
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(e)

        merged = [e for e in merged if _is_visible_auto_entry(e)]
        merged = sort_entries_by_delivery(merged)
        render_auto_list(
            merged,
            title=f"🚚 FORÁNEOS • HOY ({hoy.strftime('%d/%m')})",
            subtitle="Todos los de hoy y fechas futuras + pedidos sin Fecha_Entrega",
            max_rows=140,
            start_number=next_number,
        )

# ---------------------------
# TAB 2: Surtidores (Asignación)
# ---------------------------
if selected_tab == 2:

    st.markdown("### 🧑‍🔧 Asignación de surtidores")
    st.caption("Selecciona pedidos visibles y escribe tu nombre o inicial para asignarlos.")

    surtidor_nombre = st.text_input("Nombre o inicial del surtidor")

    seen_local = set()
    local_hoy = []
    for entry in auto_local_entries:
        if not _is_visible_auto_entry(entry):
            continue
        if entry.get("display_num") is None:
            continue
        key = build_surtidor_key(entry)
        if not key or key in seen_local:
            continue
        seen_local.add(key)
        local_hoy.append(entry)
    foraneo_hoy = []
    seen_foraneo = set()
    for entry in auto_foraneo_entries:
        if not _is_visible_auto_entry(entry):
            continue
        if entry.get("display_num") is None:
            continue
        key = build_surtidor_key(entry)
        if not key or key in seen_foraneo:
            continue
        seen_foraneo.add(key)
        foraneo_hoy.append(entry)

    def _entry_label(entry) -> str:
        numero = entry.get("display_num", entry.get("numero", "—"))
        cliente = sanitize_text(entry.get("cliente_nombre", ""))
        estado = sanitize_text(entry.get("estado", ""))
        parts = [f"#{numero}", cliente, estado]
        return " · ".join([p for p in parts if p])

    local_options = {build_surtidor_key(e): _entry_label(e) for e in local_hoy}
    foraneo_options = {build_surtidor_key(e): _entry_label(e) for e in foraneo_hoy}
    local_order = {build_surtidor_key(e): e.get("display_num", float("inf")) for e in local_hoy}
    foraneo_order = {
        build_surtidor_key(e): e.get("display_num", float("inf")) for e in foraneo_hoy
    }
    local_sorted_keys = sorted(local_options.keys(), key=lambda k: local_order.get(k, float("inf")))
    foraneo_sorted_keys = sorted(
        foraneo_options.keys(), key=lambda k: foraneo_order.get(k, float("inf"))
    )

    col_local, col_foraneo = st.columns(2, gap="large")
    with col_local:
        st.markdown("#### 📍 Auto Local")
        selected_local = st.multiselect(
            "Pedidos locales",
            options=local_sorted_keys,
            format_func=lambda k: local_options.get(k, k),
        )
    with col_foraneo:
        st.markdown("#### 🚚 Auto Foráneo")
        selected_foraneo = st.multiselect(
            "Pedidos foráneos",
            options=foraneo_sorted_keys,
            format_func=lambda k: foraneo_options.get(k, k),
        )

    if st.button("✅ Asignar surtidor", use_container_width=True):
        nombre = sanitize_text(surtidor_nombre)
        if not nombre:
            st.warning("Escribe un nombre o inicial para asignar.")
        else:
            selected_keys = selected_local + selected_foraneo
            if not selected_keys:
                st.warning("Selecciona al menos un pedido.")
            else:
                for key in selected_keys:
                    st.session_state.surtidor_assignments[key] = nombre
                st.success("Asignación guardada.")
                st.rerun()

    st.markdown("---")
    st.markdown("#### 📋 Asignaciones actuales")
    assignments = st.session_state.get("surtidor_assignments", {})
    if not assignments:
        st.info("Sin asignaciones registradas.")
    else:
        entry_lookup = {}
        envio_lookup = {}
        for entry in auto_local_entries:
            key = build_surtidor_key(entry)
            if key:
                entry_lookup[key] = entry
                envio_lookup[key] = "📍"
        for entry in auto_foraneo_entries:
            key = build_surtidor_key(entry)
            if key:
                entry_lookup[key] = entry
                envio_lookup[key] = "🚚"

        def _assignment_label(key: str) -> str:
            entry = entry_lookup.get(key)
            if not entry:
                return key
            numero = entry.get("display_num", entry.get("numero", "—"))
            cliente = sanitize_text(entry.get("cliente_nombre", ""))
            estado = sanitize_text(entry.get("estado", ""))
            envio = envio_lookup.get(key, "")
            numero_label = f"{envio} #{numero}" if envio else f"#{numero}"
            parts = [numero_label, cliente, estado]
            return " · ".join([p for p in parts if p])

        rows = [
            {"Pedido": _assignment_label(key), "Surtidor": value}
            for key, value in assignments.items()
            if value
        ]
        if rows:
            df_assign = pd.DataFrame(rows)
            st.dataframe(df_assign, use_container_width=True, height=300)
        else:
            st.info("Sin asignaciones registradas.")


if selected_tab == 3:
    st.markdown("## 📈 Dashboard Inteligente")
    st.caption("Cálculos base cacheados (10 min) + filtros dinámicos en tiempo real.")

    df_conf = load_confirmados_from_gsheets(GSHEETS_CREDENTIALS, GOOGLE_SHEET_ID, SHEET_CONFIRMADOS)
    base = compute_dashboard_base(df_conf)

    df = base["df"]
    if df.empty:
        st.info("No hay datos en pedidos_confirmados.")
        st.stop()

    total_pedidos = len(df)
    total_ventas = float(df["Monto_Comprobante"].sum())
    ticket_prom = float(df["Monto_Comprobante"].mean()) if total_pedidos else 0.0
    mask_credito = df["Estado_Pago"].astype(str).str.contains("CREDITO", case=False, na=False)
    pedidos_sin_monto_real = int(((df["Monto_Comprobante"] <= 0) & ~mask_credito).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Pedidos confirmados", f"{total_pedidos:,}")
    c2.metric("💰 Ventas confirmadas", f"${total_ventas:,.0f}")
    c3.metric("🎟️ Ticket promedio", f"${ticket_prom:,.0f}")
    c4.metric("🧾 Sin monto real", f"{pedidos_sin_monto_real:,}")

    st.markdown("---")

    min_dt = df["Hora_Registro"].min()
    max_dt = df["Hora_Registro"].max()

    default_start = min_dt.date() if pd.notna(min_dt) else datetime.now(TZ).date()
    default_end = max_dt.date() if pd.notna(max_dt) else datetime.now(TZ).date()
    if default_start > default_end:
        default_start, default_end = default_end, default_start

    colf0, colf1, colf2, colf3 = st.columns([0.35, 0.25, 0.2, 0.2])
    with colf0:
        rango = st.date_input(
            "Rango (Hora_Registro)",
            value=(default_start, default_end),
        )
    with colf1:
        vendedor_sel = st.multiselect(
            "Filtrar vendedor",
            options=sorted(df["Vendedor_Registro"].dropna().astype(str).unique().tolist()),
        )
    with colf2:
        envio_sel = st.multiselect(
            "Tipo envío",
            options=sorted(df["Tipo_Envio"].dropna().astype(str).unique().tolist()),
        )
    with colf3:
        estado_pago_sel = st.multiselect(
            "Estado pago",
            options=sorted(df["Estado_Pago"].dropna().astype(str).unique().tolist()),
        )

    df_f = df.copy()
    rango_default = (default_start, default_end)
    if isinstance(rango, tuple) and len(rango) == 2:
        d1, d2 = rango
        df_f = df_f[pd.notna(df_f["Hora_Registro"])].copy()
        df_f = df_f[
            (df_f["Hora_Registro"].dt.date >= d1)
            & (df_f["Hora_Registro"].dt.date <= d2)
        ]
    if vendedor_sel:
        df_f = df_f[df_f["Vendedor_Registro"].isin(vendedor_sel)]
    if envio_sel:
        df_f = df_f[df_f["Tipo_Envio"].isin(envio_sel)]
    if estado_pago_sel:
        df_f = df_f[df_f["Estado_Pago"].isin(estado_pago_sel)]

    has_filters = bool(vendedor_sel or envio_sel or estado_pago_sel or (rango != rango_default))

    with st.expander("📆 Tendencia mensual (ventas)", expanded=True):
        if has_filters:
            ventas_mes = df_f.groupby("AñoMes")["Monto_Comprobante"].sum().sort_index()
        else:
            ventas_mes = base["ventas_mes"]
        st.dataframe(ventas_mes.reset_index(name="Ventas"), use_container_width=True)

        ventas_dia = (
            df_f.groupby(df_f["Hora_Registro"].dt.date)["Monto_Comprobante"]
            .sum()
            .sort_index()
        )
        st.dataframe(ventas_dia.reset_index(name="Ventas"), use_container_width=True)

    with st.expander("🧑‍💼 Ranking vendedores (dinero y pedidos)", expanded=True):
        if has_filters:
            money = (
                df_f.groupby("Vendedor_Registro")["Monto_Comprobante"]
                .sum()
                .sort_values(ascending=False)
            )
            cnt = df_f["Vendedor_Registro"].value_counts()
        else:
            money = base["ventas_vendedor"]
            cnt = base["pedidos_vendedor"]
        out = pd.DataFrame({"Pedidos": cnt, "Ventas": money}).fillna(0)
        out["Ticket_Prom"] = (out["Ventas"] / out["Pedidos"]).where(out["Pedidos"] > 0)
        st.dataframe(
            out.sort_values("Ventas", ascending=False),
            use_container_width=True,
            height=420,
        )

    with st.expander("🏥 Clientes: ranking + limpieza de nombre", expanded=True):
        def _clean_name(x: str) -> str:
            x = sanitize_text(x).upper()
            x = unicodedata.normalize("NFKD", x)
            x = "".join(ch for ch in x if not unicodedata.combining(ch))
            x = x.replace(" ", " ")
            x = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in x)
            x = " ".join(x.split())
            return x

        df_c = df_f.copy()
        df_c["Cliente_Limpio"] = df_c["Cliente"].astype(str).map(_clean_name)

        top_clients = (
            df_c.groupby("Cliente_Limpio")["Monto_Comprobante"]
            .sum()
            .sort_values(ascending=False)
            .head(30)
        )
        st.dataframe(top_clients.reset_index(name="Ventas"), use_container_width=True)
