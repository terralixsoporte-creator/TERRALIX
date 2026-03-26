# Script Control:
# - Role: Inventory UI tab (catalog import, DTE sync, stock view, usage registry).
from __future__ import annotations

import os
import threading
from datetime import date, datetime
from pathlib import Path
import tkinter as tk
from tkinter import Frame, filedialog, messagebox
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


def _column_label(col_name: str) -> str:
    return {
        "codigo": "Codigo",
        "descripcion_estandar": "Descripcion",
        "unidad_base": "Unidad",
        "stock_actual": "Stock actual",
        "ultima_fecha": "Ultima modificacion",
        "ultima_modificacion_tipo": "Tipo",
    }.get(col_name, col_name)


def create_inventory_tab(parent):
    frame = Frame(parent, bg="#FFFEFF", width=917, height=500)
    frame.pack_propagate(False)

    container = ttk.Frame(frame, padding=10)
    container.pack(fill="both", expand=True)

    container.columnconfigure(0, weight=0)
    container.columnconfigure(1, weight=1)
    container.rowconfigure(1, weight=1)

    top = ttk.Frame(container)
    top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
    top.columnconfigure(3, weight=1)

    btn_refresh = ttk.Button(top, text="Refrescar")
    btn_export = ttk.Button(top, text="Exportar Stock")

    btn_refresh.grid(row=0, column=0, padx=(0, 6), sticky="w")
    btn_export.grid(row=0, column=1, padx=(0, 10), sticky="w")

    search_var = tk.StringVar(value="")
    ttk.Label(top, text="Buscar:").grid(row=0, column=2, padx=(0, 6), sticky="w")
    search_entry = ttk.Entry(top, textvariable=search_var)
    search_entry.grid(row=0, column=3, sticky="ew")

    left = ttk.LabelFrame(container, text="Registrar uso", padding=10)
    left.grid(row=1, column=0, sticky="nsw", padx=(0, 10))

    right = ttk.LabelFrame(container, text="Stock actual", padding=8)
    right.grid(row=1, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(0, weight=1)
    right.rowconfigure(1, weight=0)

    ttk.Label(left, text="Producto (codigo):").grid(row=0, column=0, sticky="w")
    selected_code_var = tk.StringVar(value="")
    combo_code = ttk.Combobox(left, textvariable=selected_code_var, width=40)
    combo_code.grid(row=1, column=0, sticky="ew", pady=(2, 8))

    ttk.Label(left, text="Cantidad usada:").grid(row=2, column=0, sticky="w")
    qty_var = tk.StringVar(value="")
    qty_entry = ttk.Entry(left, textvariable=qty_var, width=14)
    qty_entry.grid(row=3, column=0, sticky="w", pady=(2, 8))

    ttk.Label(left, text="Fecha (YYYY-MM-DD):").grid(row=4, column=0, sticky="w")
    date_var = tk.StringVar(value=date.today().isoformat())
    date_entry = ttk.Entry(left, textvariable=date_var, width=14)
    date_entry.grid(row=5, column=0, sticky="w", pady=(2, 8))

    ttk.Label(left, text="Observacion:").grid(row=6, column=0, sticky="w")
    obs_var = tk.StringVar(value="")
    obs_entry = ttk.Entry(left, textvariable=obs_var, width=42)
    obs_entry.grid(row=7, column=0, sticky="ew", pady=(2, 8))

    unit_info_var = tk.StringVar(value="Unidad: -")
    stock_info_var = tk.StringVar(value="Stock actual: 0")
    ttk.Label(left, textvariable=unit_info_var).grid(row=8, column=0, sticky="w")
    ttk.Label(left, textvariable=stock_info_var).grid(row=9, column=0, sticky="w", pady=(0, 8))

    btn_register = ttk.Button(left, text="Registrar uso")
    btn_register.grid(row=10, column=0, sticky="ew")
    left.columnconfigure(0, weight=1)

    columns = (
        "codigo",
        "descripcion_estandar",
        "unidad_base",
        "stock_actual",
        "ultima_fecha",
        "ultima_modificacion_tipo",
    )
    tree = ttk.Treeview(right, columns=columns, show="headings", height=16)
    yscroll = ttk.Scrollbar(right, orient="vertical", command=tree.yview)
    xscroll = ttk.Scrollbar(right, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
    tree.grid(row=0, column=0, sticky="nsew")
    yscroll.grid(row=0, column=1, sticky="ns")
    xscroll.grid(row=1, column=0, sticky="ew")

    tree.heading("codigo", text="Codigo")
    tree.heading("descripcion_estandar", text="Descripcion")
    tree.heading("unidad_base", text="Unidad")
    tree.heading("stock_actual", text="Stock actual")
    tree.heading("ultima_fecha", text="Ultima modificacion")
    tree.heading("ultima_modificacion_tipo", text="Tipo")

    tree.column("codigo", width=190, minwidth=130, anchor="w")
    tree.column("descripcion_estandar", width=400, minwidth=200, anchor="w")
    tree.column("unidad_base", width=90, minwidth=60, anchor="center")
    tree.column("stock_actual", width=150, minwidth=90, anchor="e")
    tree.column("ultima_fecha", width=160, minwidth=120, anchor="center")
    tree.column("ultima_modificacion_tipo", width=140, minwidth=100, anchor="center")
    tree.tag_configure("neg", foreground="#A30000")

    products_by_display: dict[str, dict] = {}
    products_by_code: dict[str, dict] = {}
    active_editor: dict[str, Any] = {"widget": None}

    def _db_path() -> str:
        db = _clean_path(os.getenv("DB_PATH_DTE_RECIBIDOS", ""))
        if not db:
            raise ValueError("No se encontro DB_PATH_DTE_RECIBIDOS.")
        return db

    def log(msg: str) -> None:
        # Terminal integrada removida por diseño.
        _ = msg

    def run_async(start_msg: str, worker_fn, on_done=None):
        log(start_msg)
        for b in (btn_refresh, btn_export, btn_register):
            b.configure(state="disabled")

        def _worker():
            try:
                result = worker_fn()

                def _finish_ok():
                    if on_done:
                        on_done(result)
                    for b in (btn_refresh, btn_export, btn_register):
                        b.configure(state="normal")

                frame.after(0, _finish_ok)
            except Exception as e:
                def _finish_err():
                    log(f"[ERROR] {e}")
                    for b in (btn_refresh, btn_export, btn_register):
                        b.configure(state="normal")

                frame.after(0, _finish_err)

        threading.Thread(target=_worker, daemon=True).start()

    def _restore_selected_code(code: str) -> None:
        code_norm = (code or "").strip().upper()
        if not code_norm:
            return
        values = list(combo_code["values"] or [])
        prefix = f"{code_norm} |"
        for v in values:
            if str(v).upper().startswith(prefix):
                selected_code_var.set(v)
                break
        _refresh_selected_info()

    def refresh_stock_table() -> None:
        try:
            db = _db_path()
            data = INV.get_stock_summary(db, search_text=search_var.get(), limit=5000)
        except Exception as e:
            log(f"[WARN] No se pudo cargar stock: {e}")
            return

        for iid in tree.get_children():
            tree.delete(iid)

        for r in data:
            stock = float(r["stock_actual"])
            tag = ("neg",) if stock < 0 else ()
            tree.insert(
                "",
                "end",
                values=(
                    r["codigo"],
                    r["descripcion_estandar"],
                    (r.get("unidad_base") or ""),
                    _fmt_qty(stock),
                    r["ultima_fecha"],
                    (r.get("ultima_modificacion_tipo") or "-"),
                ),
                tags=tag,
            )

        log(f"[INFO] Stock cargado: {len(data)} codigo(s).")

    def refresh_products_combo() -> None:
        nonlocal products_by_display, products_by_code
        try:
            db = _db_path()
            products = INV.list_catalog_products(db)
        except Exception as e:
            log(f"[WARN] No se pudo cargar catalogo de inventario: {e}")
            return

        products_by_display = {}
        products_by_code = {}
        values: list[str] = []
        for p in products:
            code = (p.get("codigo") or "").strip().upper()
            if not code:
                continue
            desc = (p.get("descripcion_estandar") or "").strip()
            um = (p.get("unidad_base") or "").strip().upper()
            label = f"{code} | {desc} [{um}]".strip()
            values.append(label)
            products_by_display[label] = p
            products_by_code[code] = p

        combo_code["values"] = values
        if not selected_code_var.get() and values:
            selected_code_var.set(values[0])
        _refresh_selected_info()

    def _extract_code(raw: str) -> str:
        value = (raw or "").strip().upper()
        if "|" in value:
            return value.split("|", 1)[0].strip()
        return value

    def _refresh_selected_info(*_args) -> None:
        code = _extract_code(selected_code_var.get())
        p = products_by_code.get(code)
        um = (p.get("unidad_base") if p else "") or "-"
        unit_info_var.set(f"Unidad: {um}")

        try:
            db = _db_path()
            stock = INV.get_stock_for_code(db, code) if code else 0.0
            stock_info_var.set(f"Stock actual: {_fmt_qty(stock)}")
        except Exception:
            stock_info_var.set("Stock actual: -")

    def sync_from_dte() -> None:
        try:
            db = _db_path()
        except Exception as e:
            messagebox.showerror("Inventario", str(e))
            return

        def _worker():
            return INV.sync_entries_from_detalle(db, only_categoria=None)

        def _done(res):
            if not res.get("ok"):
                log(f"[ERROR] {res.get('error', 'No se pudo sincronizar inventario desde DTE')}")
                return
            stats = res.get("catalog_stats", {}) or {}
            purge = res.get("purge_ignored_codes", {}) or {}
            removed_non_fact = int(res.get("removed_non_factura_movements", 0) or 0)
            log(
                "[OK] Sync DTE inventario: "
                f"filas={res.get('rows_scanned', 0)} | "
                f"mov_ins={res.get('movements_inserted', 0)} | "
                f"mov_upd={res.get('movements_updated', 0)} | "
                f"cat_ins={res.get('catalog_inserted', 0)} | "
                f"cat_upd={res.get('catalog_updated', 0)} | "
                f"stats_upd={stats.get('catalog_updated_stats', 0)} | "
                f"variaciones_si={stats.get('codes_variaciones_si', 0)} | "
                f"purge_no_factura_mov={removed_non_fact} | "
                f"purge_cod_ignorado(cat={purge.get('catalog_deleted', 0)}, mov={purge.get('movements_deleted', 0)}) | "
                "filtro_doc=Factura_* | "
                "filtro_categoria=ninguno"
            )
            refresh_products_combo()
            refresh_stock_table()

        run_async("[REFRESH] Sincronizando inventario desde detalle DTE (solo facturas)...", _worker, _done)

    def _begin_cell_edit(event) -> None:
        # Solo edición de celdas de datos (no encabezados/espacios)
        row_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not row_id or not col_id or col_id == "#0":
            return

        try:
            col_index = int(col_id.replace("#", "")) - 1
        except Exception:
            return
        if col_index < 0 or col_index >= len(columns):
            return

        col_name = columns[col_index]
        if col_name == "codigo":
            messagebox.showinfo("Inventario", "La columna 'codigo' no es editable.")
            return

        editable_cols = {
            "descripcion_estandar",
            "unidad_base",
            "stock_actual",
            "ultima_fecha",
            "ultima_modificacion_tipo",
        }
        if col_name not in editable_cols:
            messagebox.showinfo("Inventario", "Esta columna no es editable.")
            return

        bbox = tree.bbox(row_id, col_id)
        if not bbox:
            return
        x, y, w, h = bbox

        # Cierra editor anterior si existe
        old_editor = active_editor.get("widget")
        try:
            if old_editor is not None and old_editor.winfo_exists():
                old_editor.destroy()
        except Exception:
            pass

        old_value = str(tree.set(row_id, col_name) or "").strip()
        codigo = str(tree.set(row_id, "codigo") or "").strip().upper()
        if not codigo:
            return

        editor = ttk.Entry(tree)
        editor.place(x=x, y=y, width=w, height=h)
        editor.insert(0, old_value)
        editor.focus_set()
        editor.selection_range(0, tk.END)
        active_editor["widget"] = editor

        finished = {"done": False}

        def _close_editor(save: bool) -> None:
            if finished["done"]:
                return
            finished["done"] = True

            try:
                new_value = str(editor.get() or "").strip()
            except Exception:
                new_value = ""
            try:
                editor.destroy()
            except Exception:
                pass
            active_editor["widget"] = None

            if not save:
                return

            if new_value == old_value:
                return

            label = _column_label(col_name)
            confirmed = messagebox.askyesno(
                "Confirmar edicion",
                f"Codigo: {codigo}\n"
                f"Campo: {label}\n"
                f"Anterior: {old_value or '(vacio)'}\n"
                f"Nuevo: {new_value or '(vacio)'}\n\n"
                "¿Confirmas guardar este cambio?",
            )
            if not confirmed:
                return

            def _worker():
                return INV.update_stock_cell(
                    db_path=_db_path(),
                    codigo=codigo,
                    column_name=col_name,
                    new_value=new_value,
                    observacion="EDICION_DOBLE_CLICK",
                )

            def _done(res):
                if not res.get("ok"):
                    err = res.get("error", "No se pudo editar la celda.")
                    log(f"[ERROR] {err}")
                    messagebox.showerror("Inventario", err)
                    return

                action = res.get("action", "")
                if action == "catalog_update":
                    log(
                        f"[OK] Editado {codigo}::{col_name} | "
                        f"'{res.get('before', '')}' -> '{res.get('after', '')}'"
                    )
                elif action == "stock_adjust":
                    if res.get("no_change"):
                        log(f"[INFO] {codigo}::stock_actual sin cambios (mismo valor).")
                    else:
                        log(
                            f"[OK] Ajuste stock {codigo}: "
                            f"{_fmt_qty(res.get('stock_before', 0.0))} -> {_fmt_qty(res.get('stock_after', 0.0))} "
                            f"(delta={_fmt_qty(res.get('delta', 0.0))})"
                        )
                elif action == "last_date_update":
                    if res.get("no_change"):
                        log(f"[INFO] {codigo}::ultima_fecha sin cambios.")
                    else:
                        log(
                            f"[OK] Editado {codigo}::ultima_fecha | "
                            f"{res.get('before', '')} -> {res.get('after', '')} "
                            "(override visual)"
                        )
                elif action == "last_type_update":
                    if res.get("no_change"):
                        log(f"[INFO] {codigo}::ultima_modificacion_tipo sin cambios.")
                    else:
                        log(
                            f"[OK] Editado {codigo}::ultima_modificacion_tipo | "
                            f"{res.get('before', '')} -> {res.get('after', '')} "
                            "(override visual)"
                        )
                else:
                    log(f"[OK] Cambio aplicado en {codigo}::{col_name}.")

                refresh_products_combo()
                refresh_stock_table()
                _restore_selected_code(codigo)

            run_async(
                f"[EDIT] Guardando {codigo}::{col_name}...",
                _worker,
                _done,
            )

        editor.bind("<Return>", lambda _e: _close_editor(True))
        editor.bind("<Escape>", lambda _e: _close_editor(False))
        editor.bind("<FocusOut>", lambda _e: _close_editor(True))

    def register_usage() -> None:
        try:
            db = _db_path()
        except Exception as e:
            messagebox.showerror("Inventario", str(e))
            return

        code = _extract_code(selected_code_var.get())
        if not code:
            messagebox.showwarning("Inventario", "Selecciona un codigo de producto.")
            return

        try:
            qty = float(str(qty_var.get()).replace(",", "."))
        except Exception:
            messagebox.showwarning("Inventario", "Cantidad invalida.")
            return

        if qty <= 0:
            messagebox.showwarning("Inventario", "La cantidad debe ser mayor a cero.")
            return

        fecha_raw = date_var.get().strip()
        try:
            datetime.strptime(fecha_raw[:10], "%Y-%m-%d")
        except Exception:
            messagebox.showwarning("Inventario", "La fecha debe estar en formato YYYY-MM-DD.")
            return

        unidad = "-"
        p = products_by_code.get(code)
        if p:
            unidad = (p.get("unidad_base") or "").strip().upper() or "-"

        stock_before = INV.get_stock_for_code(db, code)
        stock_after = stock_before - qty
        allow_negative = False
        if stock_after < 0:
            allow_negative = messagebox.askyesno(
                "Stock negativo",
                f"El uso deja stock negativo para {code}.\n"
                f"Actual: {_fmt_qty(stock_before)} {unidad}\n"
                f"Post-uso: {_fmt_qty(stock_after)} {unidad}\n\n"
                "Deseas guardar igualmente?",
            )
            if not allow_negative:
                return

        def _worker():
            return INV.register_usage(
                db_path=db,
                codigo=code,
                cantidad=qty,
                fecha_uso=fecha_raw,
                observacion=obs_var.get(),
                unidad=unidad if unidad != "-" else "",
                allow_negative=allow_negative,
            )

        def _done(res):
            if not res.get("ok"):
                log(f"[ERROR] {res.get('error', 'No se pudo registrar uso')}")
                return
            log(
                f"[OK] Uso registrado: {code} | cantidad={_fmt_qty(qty)} "
                f"| stock { _fmt_qty(res.get('stock_before', 0.0)) } -> { _fmt_qty(res.get('stock_after', 0.0)) }"
            )
            qty_var.set("")
            obs_var.set("")
            _refresh_selected_info()
            refresh_stock_table()

        run_async("[MOV] Registrando uso manual...", _worker, _done)

    def export_stock_excel() -> None:
        try:
            db = _db_path()
        except Exception as e:
            messagebox.showerror("Inventario", str(e))
            return

        default_dir = Path(db).resolve().parent
        default_name = "inventario_actual.xlsx"
        output_path = filedialog.asksaveasfilename(
            title="Exportar stock teorico",
            initialdir=str(default_dir),
            initialfile=default_name,
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
        )
        if not output_path:
            return

        def _worker():
            return INV.export_stock_to_excel(db, output_path)

        def _done(res):
            if not res.get("ok"):
                log(f"[ERROR] No se pudo exportar stock.")
                return
            log(f"[OK] Stock exportado: {res.get('path')} ({res.get('n_rows', 0)} filas)")

        run_async("[EXPORT] Exportando stock a Excel...", _worker, _done)

    btn_refresh.configure(command=sync_from_dte)
    btn_export.configure(command=export_stock_excel)
    btn_register.configure(command=register_usage)
    combo_code.bind("<<ComboboxSelected>>", _refresh_selected_info)
    combo_code.bind("<KeyRelease>", _refresh_selected_info)
    search_entry.bind("<Return>", lambda _e: refresh_stock_table())
    tree.bind("<Double-1>", _begin_cell_edit)

    frame.run_inventory_refresh = sync_from_dte

    # Primer carga
    try:
        db = _db_path()
        INV.ensure_inventory_schema(db)
        log(f"[INFO] DB inventario lista: {db}")
    except Exception as e:
        log(f"[WARN] {e}")

    refresh_products_combo()
    refresh_stock_table()

    return frame
