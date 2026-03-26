# Script Control:
# - Role: Field applications UI tab (calendar planning + chemical outputs linked to stock movements).
from __future__ import annotations

import calendar
import os
import threading
from datetime import date, datetime
import tkinter as tk
from tkinter import Frame, messagebox
from tkinter import ttk
from typing import Any

from app.core.DTE_Recibidos import inventory as INV


def _clean_path(path_value: str) -> str:
    return (path_value or "").strip().strip('"').strip("'")


def _fmt_qty(value: float) -> str:
    try:
        return f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(value)


def _parse_date_iso(raw: str) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    value10 = value[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value10, fmt).date().isoformat()
        except Exception:
            pass
    return None


def create_applications_tab(parent):
    frame = Frame(parent, bg="#FFFEFF")

    container = ttk.Frame(frame, padding=8)
    container.pack(fill="both", expand=True)
    container.columnconfigure(0, weight=1)
    container.columnconfigure(1, weight=2)
    container.rowconfigure(0, weight=1)
    container.rowconfigure(1, weight=0)

    left = ttk.LabelFrame(container, text="Nueva aplicacion de campo", padding=10)
    left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
    left.columnconfigure(0, weight=1)

    right = ttk.LabelFrame(container, text="Calendario y gestion", padding=8)
    right.grid(row=0, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(3, weight=1)
    right.rowconfigure(5, weight=1)

    ui_status_var = tk.StringVar(value="")
    ttk.Label(container, textvariable=ui_status_var).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))

    products_by_display: dict[str, dict] = {}
    products_by_code: dict[str, dict] = {}
    draft_products: list[dict[str, Any]] = []
    calendar_cells: list[dict[str, Any]] = []
    action_buttons: list[ttk.Button] = []

    today = date.today()
    current_year = {"value": today.year}
    current_month = {"value": today.month}

    selected_date_var = tk.StringVar(value=today.isoformat())
    form_date_var = tk.StringVar(value=today.isoformat())

    def _db_path() -> str:
        db = _clean_path(os.getenv("DB_PATH_DTE_RECIBIDOS", ""))
        if not db:
            raise ValueError("No se encontro DB_PATH_DTE_RECIBIDOS.")
        return db

    def _set_status(msg: str) -> None:
        ui_status_var.set((msg or "").strip())

    def _set_buttons_state(state: str) -> None:
        for b in action_buttons:
            try:
                b.configure(state=state)
            except Exception:
                pass

    def run_async(start_msg: str, worker_fn, on_done=None):
        _set_status(start_msg)
        _set_buttons_state("disabled")

        def _worker():
            try:
                result = worker_fn()

                def _finish_ok():
                    if on_done:
                        on_done(result)
                    _set_buttons_state("normal")

                frame.after(0, _finish_ok)
            except Exception as e:
                def _finish_err():
                    _set_status(f"[ERROR] {e}")
                    _set_buttons_state("normal")

                frame.after(0, _finish_err)

        threading.Thread(target=_worker, daemon=True).start()

    # --- Left panel: form ---
    ttk.Label(left, text="Titulo:").grid(row=0, column=0, sticky="w")
    title_var = tk.StringVar(value="")
    title_entry = ttk.Entry(left, textvariable=title_var, width=40)
    title_entry.grid(row=1, column=0, sticky="ew", pady=(2, 6))

    ttk.Label(left, text="Fecha programada (YYYY-MM-DD):").grid(row=2, column=0, sticky="w")
    date_entry = ttk.Entry(left, textvariable=form_date_var, width=20)
    date_entry.grid(row=3, column=0, sticky="w", pady=(2, 6))

    ttk.Label(left, text="Estado inicial:").grid(row=4, column=0, sticky="w")
    status_var = tk.StringVar(value="PROGRAMADA")
    combo_status = ttk.Combobox(
        left,
        textvariable=status_var,
        values=("PROGRAMADA", "CANCELADA"),
        state="readonly",
        width=18,
    )
    combo_status.grid(row=5, column=0, sticky="w", pady=(2, 6))

    ttk.Label(left, text="Descripcion:").grid(row=6, column=0, sticky="w")
    txt_description = tk.Text(left, height=4, width=42)
    txt_description.grid(row=7, column=0, sticky="ew", pady=(2, 8))

    sep = ttk.Separator(left, orient="horizontal")
    sep.grid(row=8, column=0, sticky="ew", pady=6)

    ttk.Label(left, text="Producto quimico (codigo):").grid(row=9, column=0, sticky="w")
    selected_product_var = tk.StringVar(value="")
    combo_product = ttk.Combobox(left, textvariable=selected_product_var, width=42)
    combo_product.grid(row=10, column=0, sticky="ew", pady=(2, 6))

    product_row = ttk.Frame(left)
    product_row.grid(row=11, column=0, sticky="ew")
    product_row.columnconfigure(3, weight=1)

    ttk.Label(product_row, text="Cantidad:").grid(row=0, column=0, sticky="w")
    qty_var = tk.StringVar(value="")
    qty_entry = ttk.Entry(product_row, textvariable=qty_var, width=10)
    qty_entry.grid(row=0, column=1, sticky="w", padx=(4, 8))

    ttk.Label(product_row, text="Obs:").grid(row=0, column=2, sticky="w")
    product_obs_var = tk.StringVar(value="")
    obs_entry = ttk.Entry(product_row, textvariable=product_obs_var, width=18)
    obs_entry.grid(row=0, column=3, sticky="ew", padx=(4, 6))

    btn_add_product = ttk.Button(product_row, text="Cargar producto")
    btn_add_product.grid(row=0, column=4, sticky="e")

    draft_columns = ("codigo", "descripcion", "cantidad", "unidad")
    draft_tree = ttk.Treeview(left, columns=draft_columns, show="headings", height=6)
    draft_tree.grid(row=12, column=0, sticky="ew", pady=(6, 4))
    draft_tree.heading("codigo", text="Codigo")
    draft_tree.heading("descripcion", text="Descripcion")
    draft_tree.heading("cantidad", text="Cantidad")
    draft_tree.heading("unidad", text="Unidad")
    draft_tree.column("codigo", width=95, minwidth=70, anchor="w")
    draft_tree.column("descripcion", width=170, minwidth=120, anchor="w")
    draft_tree.column("cantidad", width=70, minwidth=65, anchor="e")
    draft_tree.column("unidad", width=55, minwidth=50, anchor="center")

    btn_remove_product = ttk.Button(left, text="Quitar seleccionado")
    btn_remove_product.grid(row=13, column=0, sticky="w")

    action_hint = ttk.Label(left, text="Paso 1: Cargar productos. Paso 2: Aceptar aplicacion.")
    action_hint.grid(row=14, column=0, sticky="w", pady=(8, 2))

    btn_save_application = ttk.Button(left, text="Aceptar aplicacion")
    btn_save_application.grid(row=15, column=0, sticky="ew")

    # --- Right panel: calendar + management ---
    top = ttk.Frame(right)
    top.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    top.columnconfigure(2, weight=1)

    btn_prev_month = ttk.Button(top, text="<")
    lbl_month = ttk.Label(top, text="", font=("Segoe UI", 10, "bold"))
    btn_next_month = ttk.Button(top, text=">")
    btn_today = ttk.Button(top, text="Hoy")

    btn_prev_month.grid(row=0, column=0, padx=(0, 4))
    lbl_month.grid(row=0, column=1, padx=(0, 8))
    btn_next_month.grid(row=0, column=2, sticky="w")
    btn_today.grid(row=0, column=3, padx=(6, 10))

    ttk.Label(top, text="Filtro estado:").grid(row=0, column=4, padx=(0, 4))
    filter_status_var = tk.StringVar(value="TODOS")
    combo_filter_status = ttk.Combobox(
        top,
        textvariable=filter_status_var,
        values=("TODOS", "PROGRAMADA", "EJECUTADA", "CANCELADA"),
        state="readonly",
        width=12,
    )
    combo_filter_status.grid(row=0, column=5, padx=(0, 6))

    btn_refresh = ttk.Button(top, text="Refrescar")
    btn_refresh.grid(row=0, column=6)

    day_headers = ("LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM")
    cal_grid = tk.Frame(right, bg="#E4E4E4", bd=1, relief="solid")
    cal_grid.grid(row=1, column=0, sticky="ew")
    for col in range(7):
        cal_grid.columnconfigure(col, weight=1)
    for row in range(7):
        cal_grid.rowconfigure(row, weight=1)

    for idx, day_name in enumerate(day_headers):
        lbl_day = tk.Label(
            cal_grid,
            text=day_name,
            bg="#F0F0F0",
            fg="#2A2A2A",
            font=("Segoe UI", 9, "bold"),
            bd=1,
            relief="solid",
            padx=2,
            pady=2,
        )
        lbl_day.grid(row=0, column=idx, sticky="nsew")

    for r in range(1, 7):
        for c in range(7):
            cell = tk.Label(
                cal_grid,
                text="",
                bg="#FFFFFF",
                fg="#202020",
                font=("Segoe UI", 9),
                bd=1,
                relief="solid",
                padx=2,
                pady=4,
                cursor="hand2",
            )
            cell.grid(row=r, column=c, sticky="nsew")
            calendar_cells.append({"widget": cell, "date": None})

    lbl_selected = ttk.Label(right, textvariable=selected_date_var)
    lbl_selected.grid(row=2, column=0, sticky="w", pady=(6, 4))

    apps_columns = ("id", "fecha", "estado", "titulo", "descripcion", "productos", "salidas")
    apps_tree = ttk.Treeview(right, columns=apps_columns, show="headings", height=7, selectmode="browse")
    apps_scroll = ttk.Scrollbar(right, orient="vertical", command=apps_tree.yview)
    apps_tree.configure(yscrollcommand=apps_scroll.set)
    apps_tree.grid(row=3, column=0, sticky="nsew")
    apps_scroll.grid(row=3, column=1, sticky="ns")

    apps_tree.heading("id", text="ID")
    apps_tree.heading("fecha", text="Fecha")
    apps_tree.heading("estado", text="Estado")
    apps_tree.heading("titulo", text="Titulo")
    apps_tree.heading("descripcion", text="Descripcion")
    apps_tree.heading("productos", text="Prod")
    apps_tree.heading("salidas", text="Salidas")
    apps_tree.column("id", width=46, minwidth=38, anchor="center")
    apps_tree.column("fecha", width=86, minwidth=80, anchor="center")
    apps_tree.column("estado", width=92, minwidth=80, anchor="center")
    apps_tree.column("titulo", width=140, minwidth=120, anchor="w")
    apps_tree.column("descripcion", width=210, minwidth=140, anchor="w")
    apps_tree.column("productos", width=50, minwidth=45, anchor="center")
    apps_tree.column("salidas", width=70, minwidth=60, anchor="center")

    actions = ttk.Frame(right)
    actions.grid(row=4, column=0, sticky="ew", pady=(6, 4))
    btn_execute = ttk.Button(actions, text="Marcar ejecutada + salidas")
    btn_cancel = ttk.Button(actions, text="Marcar cancelada")
    btn_execute.grid(row=0, column=0, padx=(0, 6))
    btn_cancel.grid(row=0, column=1)

    detail_columns = ("codigo", "descripcion", "cantidad", "unidad", "salida")
    detail_tree = ttk.Treeview(right, columns=detail_columns, show="headings", height=5, selectmode="none")
    detail_tree.grid(row=5, column=0, sticky="nsew")
    detail_tree.heading("codigo", text="Codigo")
    detail_tree.heading("descripcion", text="Descripcion")
    detail_tree.heading("cantidad", text="Cantidad")
    detail_tree.heading("unidad", text="Unidad")
    detail_tree.heading("salida", text="Movimiento")
    detail_tree.column("codigo", width=90, minwidth=70, anchor="w")
    detail_tree.column("descripcion", width=220, minwidth=130, anchor="w")
    detail_tree.column("cantidad", width=80, minwidth=70, anchor="e")
    detail_tree.column("unidad", width=60, minwidth=52, anchor="center")
    detail_tree.column("salida", width=110, minwidth=90, anchor="center")

    action_buttons.extend(
        [
            btn_add_product,
            btn_remove_product,
            btn_save_application,
            btn_prev_month,
            btn_next_month,
            btn_today,
            btn_refresh,
            btn_execute,
            btn_cancel,
        ]
    )

    month_names = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }

    def _extract_code(raw: str) -> str:
        value = (raw or "").strip().upper()
        if "|" in value:
            return value.split("|", 1)[0].strip()
        return value

    def _active_filter_status() -> str:
        raw = (filter_status_var.get() or "").strip().upper()
        return "" if raw in ("", "TODOS") else raw

    def _render_draft_products() -> None:
        for iid in draft_tree.get_children():
            draft_tree.delete(iid)
        for p in draft_products:
            draft_tree.insert(
                "",
                "end",
                values=(
                    p["codigo"],
                    p.get("descripcion", ""),
                    _fmt_qty(float(p["cantidad"])),
                    p.get("unidad", ""),
                ),
            )

    def _render_detail_products(rows: list[dict[str, Any]]) -> None:
        for iid in detail_tree.get_children():
            detail_tree.delete(iid)
        for r in rows:
            movement_label = f"SI #{r['movimiento_id']}" if r.get("movimiento_id") else "PENDIENTE"
            detail_tree.insert(
                "",
                "end",
                values=(
                    r.get("codigo", ""),
                    r.get("descripcion_estandar", ""),
                    _fmt_qty(float(r.get("cantidad", 0) or 0)),
                    r.get("unidad", ""),
                    movement_label,
                ),
            )

    def _calendar_bg_for_day(info: dict[str, Any]) -> str:
        programadas = int(info.get("programadas", 0) or 0)
        ejecutadas = int(info.get("ejecutadas", 0) or 0)
        canceladas = int(info.get("canceladas", 0) or 0)

        # Prioridad visual:
        # 1) Si hay al menos una programada, se muestra amarillo.
        # 2) Si no hay programadas y hay ejecutadas, se muestra verde.
        # 3) Solo canceladas no se pintan (queda blanco).
        if programadas > 0:
            return "#FCE49A"
        if ejecutadas > 0:
            return "#A6E8AF"
        if canceladas > 0:
            return "#FFFFFF"
        return "#FFFFFF"

    def _paint_selected_date() -> None:
        selected_iso = _parse_date_iso(selected_date_var.get())
        for cell_data in calendar_cells:
            widget = cell_data["widget"]
            iso = cell_data.get("date")
            base_bg = cell_data.get("base_bg", "#FFFFFF")
            widget.configure(bg=base_bg, fg="#202020", bd=1, relief="solid")
            if iso and iso == selected_iso:
                widget.configure(bd=2, relief="solid", fg="#0B4A33")

    def _select_calendar_date(iso: str) -> None:
        if not iso:
            return
        selected_date_var.set(iso)
        form_date_var.set(iso)
        _paint_selected_date()
        refresh_applications_list()

    def _open_real_outputs_dialog(
        app_id: int,
        app_title: str,
        fecha_default: str,
        planned_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        dialog = tk.Toplevel(frame)
        dialog.title(f"Salida real aplicacion #{app_id}")
        dialog.geometry("760x520")
        dialog.resizable(False, False)
        dialog.configure(bg="#FFFEFF")
        dialog.transient(frame.winfo_toplevel())
        dialog.grab_set()

        result: dict[str, Any] = {"ok": False, "data": None}

        header = ttk.Frame(dialog, padding=8)
        header.pack(fill="x")
        ttk.Label(header, text=f"Aplicacion #{app_id}: {app_title}", font=("Segoe UI", 10, "bold")).pack(anchor="w")

        date_row = ttk.Frame(dialog, padding=(8, 0, 8, 8))
        date_row.pack(fill="x")
        ttk.Label(date_row, text="Fecha ejecucion (YYYY-MM-DD):").pack(side="left")
        exec_date_var = tk.StringVar(value=fecha_default)
        ttk.Entry(date_row, textvariable=exec_date_var, width=14).pack(side="left", padx=(6, 0))

        body = ttk.Frame(dialog, padding=(8, 0, 8, 8))
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        ttk.Label(body, text="Productos reales utilizados:").grid(row=0, column=0, sticky="w", pady=(0, 4))

        cols = ("codigo", "descripcion", "cantidad", "unidad", "observacion")
        tree = ttk.Treeview(body, columns=cols, show="headings", height=10, selectmode="browse")
        tree.grid(row=1, column=0, sticky="nsew")
        tree.heading("codigo", text="Codigo")
        tree.heading("descripcion", text="Descripcion")
        tree.heading("cantidad", text="Cantidad")
        tree.heading("unidad", text="Unidad")
        tree.heading("observacion", text="Observacion")
        tree.column("codigo", width=95, minwidth=70, anchor="w")
        tree.column("descripcion", width=220, minwidth=140, anchor="w")
        tree.column("cantidad", width=85, minwidth=70, anchor="e")
        tree.column("unidad", width=60, minwidth=55, anchor="center")
        tree.column("observacion", width=190, minwidth=120, anchor="w")

        data_rows: list[dict[str, Any]] = []
        for p in planned_rows:
            data_rows.append(
                {
                    "codigo": (p.get("codigo") or "").strip().upper(),
                    "descripcion": (p.get("descripcion_estandar") or "").strip(),
                    "cantidad": float(p.get("cantidad", 0) or 0),
                    "unidad": (p.get("unidad") or "").strip().upper(),
                    "observacion": (p.get("observacion") or "").strip(),
                }
            )

        def _render_rows() -> None:
            for iid in tree.get_children():
                tree.delete(iid)
            for r in data_rows:
                tree.insert(
                    "",
                    "end",
                    values=(
                        r["codigo"],
                        r.get("descripcion", ""),
                        _fmt_qty(float(r.get("cantidad", 0) or 0)),
                        r.get("unidad", ""),
                        r.get("observacion", ""),
                    ),
                )

        editor = ttk.Frame(dialog, padding=8)
        editor.pack(fill="x")
        editor.columnconfigure(5, weight=1)

        ttk.Label(editor, text="Producto:").grid(row=0, column=0, sticky="w")
        selected_real_product_var = tk.StringVar(value="")
        combo_real_product = ttk.Combobox(editor, textvariable=selected_real_product_var, width=34)
        combo_real_product.grid(row=0, column=1, padx=(4, 8), sticky="w")
        combo_real_product["values"] = list(combo_product["values"] or [])
        if combo_real_product["values"]:
            selected_real_product_var.set(str(combo_real_product["values"][0]))

        ttk.Label(editor, text="Cant:").grid(row=0, column=2, sticky="w")
        real_qty_var = tk.StringVar(value="")
        ttk.Entry(editor, textvariable=real_qty_var, width=10).grid(row=0, column=3, padx=(4, 8), sticky="w")

        ttk.Label(editor, text="Obs:").grid(row=0, column=4, sticky="w")
        real_obs_var = tk.StringVar(value="")
        ttk.Entry(editor, textvariable=real_obs_var, width=20).grid(row=0, column=5, padx=(4, 8), sticky="ew")

        def _add_or_merge_real_product() -> None:
            code = _extract_code(selected_real_product_var.get())
            if not code:
                messagebox.showwarning("Aplicaciones", "Selecciona un codigo de producto.", parent=dialog)
                return
            try:
                qty = float(str(real_qty_var.get()).replace(",", "."))
            except Exception:
                messagebox.showwarning("Aplicaciones", "Cantidad invalida.", parent=dialog)
                return
            if qty <= 0:
                messagebox.showwarning("Aplicaciones", "La cantidad debe ser mayor a cero.", parent=dialog)
                return

            product = products_by_code.get(code) or {}
            desc = (product.get("descripcion_estandar") or "").strip()
            um = (product.get("unidad_base") or "").strip().upper()
            obs = (real_obs_var.get() or "").strip()

            existing = next((r for r in data_rows if r.get("codigo") == code), None)
            if existing is None:
                data_rows.append(
                    {
                        "codigo": code,
                        "descripcion": desc,
                        "cantidad": qty,
                        "unidad": um,
                        "observacion": obs,
                    }
                )
            else:
                existing["cantidad"] = float(existing.get("cantidad", 0) or 0) + qty
                if obs:
                    prev = (existing.get("observacion") or "").strip()
                    existing["observacion"] = f"{prev} | {obs}" if prev else obs

            real_qty_var.set("")
            real_obs_var.set("")
            _render_rows()

        def _remove_selected_real_product() -> None:
            selected = tree.selection()
            if not selected:
                return
            code = str(tree.item(selected[0]).get("values", [""])[0]).strip().upper()
            if not code:
                return
            data_rows[:] = [r for r in data_rows if str(r.get("codigo", "")).upper() != code]
            _render_rows()

        buttons = ttk.Frame(dialog, padding=(8, 0, 8, 8))
        buttons.pack(fill="x")
        ttk.Button(buttons, text="Cargar producto real", command=_add_or_merge_real_product).pack(side="left")
        ttk.Button(buttons, text="Quitar seleccionado", command=_remove_selected_real_product).pack(side="left", padx=(6, 0))

        footer = ttk.Frame(dialog, padding=8)
        footer.pack(fill="x")

        def _cancel() -> None:
            dialog.destroy()

        def _accept() -> None:
            fecha_exec = _parse_date_iso(exec_date_var.get() or "")
            if not fecha_exec:
                messagebox.showwarning("Aplicaciones", "Fecha de ejecucion invalida.", parent=dialog)
                return
            if not data_rows:
                messagebox.showwarning(
                    "Aplicaciones",
                    "Debes registrar al menos un producto real utilizado.",
                    parent=dialog,
                )
                return

            payload_products = [
                {
                    "codigo": str(r.get("codigo", "")).strip().upper(),
                    "cantidad": float(r.get("cantidad", 0) or 0),
                    "unidad": str(r.get("unidad", "")).strip().upper(),
                    "observacion": str(r.get("observacion", "")).strip(),
                }
                for r in data_rows
                if str(r.get("codigo", "")).strip()
            ]
            if not payload_products:
                messagebox.showwarning(
                    "Aplicaciones",
                    "No hay productos validos para registrar salida real.",
                    parent=dialog,
                )
                return

            result["ok"] = True
            result["data"] = {
                "fecha_ejecucion": fecha_exec,
                "productos_reales": payload_products,
            }
            dialog.destroy()

        ttk.Button(footer, text="Cancelar", command=_cancel).pack(side="right")
        ttk.Button(footer, text="Aceptar salida real", command=_accept).pack(side="right", padx=(0, 6))

        _render_rows()
        dialog.bind("<Escape>", lambda _e: _cancel())
        dialog.wait_window()
        return result["data"] if result.get("ok") else None

    def refresh_products_combo() -> None:
        try:
            db = _db_path()
            products = INV.list_catalog_products(db)
        except Exception as e:
            _set_status(f"[WARN] No se pudo cargar catalogo: {e}")
            return

        products_by_display.clear()
        products_by_code.clear()
        values: list[str] = []
        for p in products:
            code = (p.get("codigo") or "").strip().upper()
            if not code:
                continue
            desc = (p.get("descripcion_estandar") or "").strip()
            unidad = (p.get("unidad_base") or "").strip().upper()
            label = f"{code} | {desc} [{unidad}]".strip()
            products_by_display[label] = p
            products_by_code[code] = p
            values.append(label)

        combo_product["values"] = values
        if not selected_product_var.get() and values:
            selected_product_var.set(values[0])

    def add_product_to_draft() -> None:
        code = _extract_code(selected_product_var.get())
        if not code:
            messagebox.showwarning("Aplicaciones", "Selecciona un codigo de producto.")
            return

        try:
            qty = float(str(qty_var.get()).replace(",", "."))
        except Exception:
            messagebox.showwarning("Aplicaciones", "Cantidad invalida.")
            return
        if qty <= 0:
            messagebox.showwarning("Aplicaciones", "La cantidad debe ser mayor a cero.")
            return

        product = products_by_code.get(code) or {}
        desc = (product.get("descripcion_estandar") or "").strip()
        unit = (product.get("unidad_base") or "").strip().upper()
        obs = (product_obs_var.get() or "").strip()

        existing = next((p for p in draft_products if p["codigo"] == code), None)
        if existing is None:
            draft_products.append(
                {
                    "codigo": code,
                    "descripcion": desc,
                    "cantidad": qty,
                    "unidad": unit,
                    "observacion": obs,
                }
            )
        else:
            existing["cantidad"] = float(existing["cantidad"]) + qty
            if obs:
                prev = (existing.get("observacion") or "").strip()
                existing["observacion"] = f"{prev} | {obs}" if prev else obs

        qty_var.set("")
        product_obs_var.set("")
        _render_draft_products()

    def remove_draft_product() -> None:
        selected = draft_tree.selection()
        if not selected:
            return
        code = str(draft_tree.item(selected[0]).get("values", [""])[0]).strip().upper()
        if not code:
            return
        draft_products[:] = [p for p in draft_products if p.get("codigo", "").upper() != code]
        _render_draft_products()

    def _clear_form(keep_date: bool = True) -> None:
        title_var.set("")
        if not keep_date:
            form_date_var.set(date.today().isoformat())
        txt_description.delete("1.0", tk.END)
        status_var.set("PROGRAMADA")
        draft_products.clear()
        _render_draft_products()

    def refresh_applications_list() -> None:
        for iid in apps_tree.get_children():
            apps_tree.delete(iid)

        selected_iso = _parse_date_iso(selected_date_var.get())
        if not selected_iso:
            _set_status("[WARN] Fecha seleccionada invalida.")
            return

        try:
            rows = INV.list_field_applications(
                db_path=_db_path(),
                date_from=selected_iso,
                date_to=selected_iso,
                status=_active_filter_status(),
                search_text="",
                limit=1000,
            )
        except Exception as e:
            _set_status(f"[WARN] No se pudieron cargar aplicaciones: {e}")
            return

        for r in rows:
            desc = (r.get("descripcion") or "").strip().replace("\n", " ")
            if len(desc) > 56:
                desc = f"{desc[:53]}..."
            total = int(r.get("productos_total", 0) or 0)
            linked = int(r.get("productos_con_salida", 0) or 0)
            apps_tree.insert(
                "",
                "end",
                values=(
                    r.get("id", ""),
                    r.get("fecha_programada", ""),
                    r.get("estado", ""),
                    r.get("titulo", ""),
                    desc,
                    str(total),
                    f"{linked}/{total}",
                ),
            )

        _render_detail_products([])
        _set_status(f"[INFO] Aplicaciones del {selected_iso}: {len(rows)}")

    def refresh_calendar() -> None:
        year = current_year["value"]
        month = current_month["value"]
        lbl_month.configure(text=f"{month_names.get(month, str(month))} {year}")

        try:
            summary = INV.get_field_calendar_summary(
                db_path=_db_path(),
                year=year,
                month=month,
                status=_active_filter_status(),
            )
        except Exception as e:
            _set_status(f"[WARN] No se pudo cargar calendario: {e}")
            summary = {}

        cal = calendar.Calendar(firstweekday=0)
        weeks = cal.monthdayscalendar(year, month)
        while len(weeks) < 6:
            weeks.append([0, 0, 0, 0, 0, 0, 0])

        flat_days: list[int] = []
        for week in weeks:
            flat_days.extend(week)

        for idx, day_num in enumerate(flat_days):
            if idx >= len(calendar_cells):
                break
            cell_data = calendar_cells[idx]
            widget = cell_data["widget"]

            if day_num <= 0:
                cell_data["date"] = None
                cell_data["base_bg"] = "#F5F5F5"
                widget.unbind("<Button-1>")
                widget.configure(
                    text="",
                    bg="#F5F5F5",
                    fg="#B0B0B0",
                    cursor="arrow",
                    bd=1,
                    relief="solid",
                )
                continue

            iso = f"{year:04d}-{month:02d}-{day_num:02d}"
            info = summary.get(iso, {})
            total = int(info.get("total", 0) or 0)
            done = int(info.get("ejecutadas", 0) or 0)
            pending = int(info.get("programadas", 0) or 0)
            cell_bg = _calendar_bg_for_day(info)
            text = f"{day_num}"
            if total > 0:
                text = f"{day_num}\nE:{done} P:{pending}"

            cell_data["date"] = iso
            cell_data["base_bg"] = cell_bg
            widget.configure(
                text=text,
                bg=cell_bg,
                fg="#202020",
                cursor="hand2",
                bd=1,
                relief="solid",
            )

            def _click(_event, iso_value=iso):
                _select_calendar_date(iso_value)

            widget.bind("<Button-1>", _click)

        _paint_selected_date()

    def _change_month(delta: int) -> None:
        y = current_year["value"]
        m = current_month["value"] + delta
        if m <= 0:
            m = 12
            y -= 1
        elif m > 12:
            m = 1
            y += 1
        current_year["value"] = y
        current_month["value"] = m
        selected_date_var.set(f"{y:04d}-{m:02d}-01")
        form_date_var.set(selected_date_var.get())
        refresh_calendar()
        refresh_applications_list()

    def go_today() -> None:
        now = date.today()
        current_year["value"] = now.year
        current_month["value"] = now.month
        selected_date_var.set(now.isoformat())
        form_date_var.set(now.isoformat())
        refresh_calendar()
        refresh_applications_list()

    def _selected_application_id() -> int:
        selected = apps_tree.selection()
        if not selected:
            return 0
        values = apps_tree.item(selected[0]).get("values", [])
        try:
            return int(values[0])
        except Exception:
            return 0

    def on_application_selected(_event=None) -> None:
        app_id = _selected_application_id()
        if app_id <= 0:
            _render_detail_products([])
            return
        try:
            rows = INV.list_field_application_products(_db_path(), app_id)
        except Exception as e:
            _set_status(f"[WARN] No se pudo cargar detalle de aplicacion: {e}")
            rows = []
        _render_detail_products(rows)

    def save_application() -> None:
        db = ""
        try:
            db = _db_path()
        except Exception as e:
            messagebox.showerror("Aplicaciones", str(e))
            return

        titulo = (title_var.get() or "").strip()
        if not titulo:
            messagebox.showwarning("Aplicaciones", "Ingresa un titulo para la aplicacion.")
            return

        fecha_prog = _parse_date_iso(form_date_var.get())
        if not fecha_prog:
            messagebox.showwarning("Aplicaciones", "Fecha invalida. Usa YYYY-MM-DD.")
            return

        estado = (status_var.get() or "PROGRAMADA").strip().upper()
        if estado not in ("PROGRAMADA", "CANCELADA"):
            messagebox.showwarning("Aplicaciones", "Estado invalido.")
            return

        descripcion = txt_description.get("1.0", tk.END).strip()

        payload_products = [
            {
                "codigo": p["codigo"],
                "cantidad": float(p["cantidad"]),
                "unidad": p.get("unidad", ""),
                "observacion": p.get("observacion", ""),
            }
            for p in draft_products
        ]
        fecha_ejec = None

        def _worker():
            return INV.create_field_application(
                db_path=db,
                titulo=titulo,
                fecha_programada=fecha_prog,
                descripcion=descripcion,
                estado=estado,
                productos=payload_products,
                registrar_salidas=False,
                fecha_ejecucion=fecha_ejec,
                allow_negative=False,
            )

        def _done(res):
            if not res.get("ok"):
                err = res.get("error", "No se pudo guardar la aplicacion.")
                messagebox.showerror("Aplicaciones", err)
                _set_status(f"[ERROR] {err}")
                return

            _clear_form(keep_date=True)
            current_year["value"] = int(fecha_prog[:4])
            current_month["value"] = int(fecha_prog[5:7])
            selected_date_var.set(fecha_prog)
            form_date_var.set(fecha_prog)
            refresh_calendar()
            refresh_applications_list()
            _set_status(
                f"[OK] Aplicacion #{res.get('application_id')} guardada. "
                f"Productos planificados={res.get('products_count', 0)}"
            )

        run_async("[SAVE] Guardando aplicacion...", _worker, _done)

    def execute_selected_application() -> None:
        app_id = _selected_application_id()
        if app_id <= 0:
            messagebox.showwarning("Aplicaciones", "Selecciona una aplicacion.")
            return

        app_selected = apps_tree.selection()
        if not app_selected:
            messagebox.showwarning("Aplicaciones", "Selecciona una aplicacion.")
            return
        values = apps_tree.item(app_selected[0]).get("values", [])
        app_title = str(values[3]).strip() if len(values) > 3 else ""

        fecha_exec = _parse_date_iso(selected_date_var.get()) or date.today().isoformat()
        planned_rows = INV.list_field_application_products(_db_path(), app_id)
        real_data = _open_real_outputs_dialog(
            app_id=app_id,
            app_title=app_title,
            fecha_default=fecha_exec,
            planned_rows=planned_rows,
        )
        if not real_data:
            return

        fecha_real = real_data["fecha_ejecucion"]
        productos_reales = real_data["productos_reales"]

        def _worker():
            rep = INV.replace_field_application_products(
                db_path=_db_path(),
                application_id=app_id,
                productos=productos_reales,
                require_no_movements=True,
            )
            if not rep.get("ok"):
                return rep
            return INV.execute_field_application(
                db_path=_db_path(),
                application_id=app_id,
                fecha_ejecucion=fecha_real,
                registrar_salidas=True,
                allow_negative=False,
            )

        def _done(res):
            if not res.get("ok"):
                err = res.get("error", "No se pudo ejecutar la aplicacion.")
                messagebox.showerror("Aplicaciones", err)
                _set_status(f"[ERROR] {err}")
                return
            refresh_calendar()
            refresh_applications_list()
            _set_status(
                f"[OK] Aplicacion #{app_id} ejecutada. "
                f"salidas_creadas={res.get('movements_created', 0)}"
            )

        run_async("[RUN] Registrando salida real y ejecutando aplicacion...", _worker, _done)

    def cancel_selected_application() -> None:
        app_id = _selected_application_id()
        if app_id <= 0:
            messagebox.showwarning("Aplicaciones", "Selecciona una aplicacion.")
            return

        ok = messagebox.askyesno(
            "Aplicaciones",
            f"La aplicacion #{app_id} quedara en estado CANCELADA.\n\nDeseas continuar?",
        )
        if not ok:
            return

        def _worker():
            return INV.update_field_application_status(
                db_path=_db_path(),
                application_id=app_id,
                estado="CANCELADA",
            )

        def _done(res):
            if not res.get("ok"):
                err = res.get("error", "No se pudo cambiar el estado.")
                messagebox.showerror("Aplicaciones", err)
                _set_status(f"[ERROR] {err}")
                return
            refresh_calendar()
            refresh_applications_list()
            _set_status(f"[OK] Aplicacion #{app_id} marcada como CANCELADA.")

        run_async("[STATE] Actualizando estado...", _worker, _done)

    def refresh_all() -> None:
        refresh_products_combo()
        refresh_calendar()
        refresh_applications_list()

    # --- Bindings ---
    btn_add_product.configure(command=add_product_to_draft)
    btn_remove_product.configure(command=remove_draft_product)
    btn_save_application.configure(command=save_application)
    btn_prev_month.configure(command=lambda: _change_month(-1))
    btn_next_month.configure(command=lambda: _change_month(1))
    btn_today.configure(command=go_today)
    btn_refresh.configure(command=refresh_all)
    btn_execute.configure(command=execute_selected_application)
    btn_cancel.configure(command=cancel_selected_application)

    combo_filter_status.bind("<<ComboboxSelected>>", lambda _e: (refresh_calendar(), refresh_applications_list()))
    apps_tree.bind("<<TreeviewSelect>>", on_application_selected)

    # Entradas rapidas
    title_entry.bind("<Return>", lambda _e: date_entry.focus_set())
    date_entry.bind("<Return>", lambda _e: qty_entry.focus_set())
    qty_entry.bind("<Return>", lambda _e: add_product_to_draft())

    frame.run_applications_refresh = refresh_all

    # Initial load
    try:
        INV.ensure_inventory_schema(_db_path())
        _set_status("[INFO] Modulo de aplicaciones listo.")
    except Exception as e:
        _set_status(f"[WARN] {e}")
    refresh_all()

    return frame
