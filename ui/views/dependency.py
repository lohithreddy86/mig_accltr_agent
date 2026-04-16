"""views/dependency.py — Dependency Gate resolution UI."""
from __future__ import annotations
import json, sys
from pathlib import Path
from nicegui import ui, app, events

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import get_store


def _upload_info(e):
    """Extract (name, bytes) from UploadEventArguments — handles NiceGUI version diffs."""
    name = getattr(e, 'name', None) or getattr(getattr(e, 'file', None), 'name', 'unknown')
    if hasattr(e, 'content'):
        data = e.content.read()
    else:
        data = getattr(getattr(e, 'file', None), '_data', b'')
    return name, data

ACTION_LABELS = {
    "upload":       "📁 Upload Source File — I have the file with this UDF",
    "inline_body":  "💻 Paste UDF Body — I'll provide the function code inline",
    "stub_todo":    "📝 Treat as TODO — UDF unknown, emit TODO markers",
    "skip_callers": "⏭️ Skip All Callers — skip all procs that call this",
}

def build_page():
    storage = app.storage.user

    # ── Run ID input in white card ────────────────────────────────────
    with ui.element("div").classes("wc w-full"):
        run_id_input = ui.input(label="Run ID", value=storage.get("active_run_id", ""),
                                 placeholder="migration_20250323_143000"
                                 ).props('outlined').classes('w-full')
        with run_id_input.add_slot('prepend'):
            ui.icon('search').style('color:var(--tl)')

    content = ui.column().classes('w-full')

    # Per-dep resolution data (action + payload)
    resolutions: dict[str, dict] = {}
    uploaded_dep_files: dict[str, list[tuple[str, bytes]]] = {}

    def refresh():
        content.clear()
        resolutions.clear()
        uploaded_dep_files.clear()
        rid = (run_id_input.value or "").strip()

        if not rid:
            with content:
                ui.html('<div class="ib">ℹ️ Enter a Run ID to view dependency gate status.</div>').classes('w-full')
            return

        store = get_store(rid)

        try:
            dep_data = store.read("analysis", "dependency_resolution.json")
        except Exception:
            dep_data = None

        if not dep_data:
            with content:
                ui.html('<div class="ib">⚠️ No dependency resolution file. Run Analysis first.</div>').classes('w-full')
            return

        deps = dep_data.get("deps", [])

        if dep_data.get("all_resolved"):
            with content:
                ui.html('<div class="log-status log-ok">✅ All dependencies resolved. Pipeline can proceed.</div>').classes('w-full')
                for dep in deps:
                    icons = {"upload":"📁","stub_todo":"📝","inline_body":"💻","skip_callers":"⏭️"}
                    icon = icons.get(dep.get("resolution", ""), "•")
                    ui.html(f'<div style="background:#F8FAFC;border-radius:6px;padding:6px 12px;margin-bottom:4px">'
                            f'{icon} <code style="color:#6C5CE7">{dep["dep_name"]}</code> → '
                            f'<span style="color:#7D8590">{dep.get("resolution","?")}</span></div>').classes('w-full')
            return

        unresolved = [d for d in deps if not d.get("resolution")]

        with content:
            ui.html(f'<div style="color:#7D8590;font-size:13px;margin-bottom:8px">'
                    f'<b>{len(unresolved)}</b> unresolved dependency(ies)</div>').classes('w-full')
            ui.html('<div class="pg-hr"></div>').classes('w-full')

            for dep in deps:
                dep_name = dep["dep_name"]
                callers = dep.get("called_by", [])
                already = dep.get("resolution", "")

                # Header
                resolved_badge = (f'&nbsp;&nbsp;<span style="color:#4ECDC4;font-size:12px">✅ {already}</span>'
                                  if already else "")
                ui.html(
                    f'<div style="background:#F8FAFC;border:1px solid #B8A5FF44;'
                    f'border-left:4px solid #B8A5FF;border-radius:0 10px 10px 0;'
                    f'padding:12px 18px;margin-bottom:4px">'
                    f'<span style="color:#B8A5FF;font-size:16px;font-weight:800;'
                    f'font-family:monospace">{dep_name}()</span>{resolved_badge}'
                    f'<br><span style="color:#7D8590;font-size:12px">'
                    f'Called by: {", ".join(callers[:5])}{"..." if len(callers)>5 else ""}</span>'
                    f'</div>'
                ).classes('w-full')

                if already:
                    ui.html('<div class="pg-hr"></div>').classes('w-full')
                    continue

                # Resolution radio
                action = ui.radio(
                    {k: v for k, v in ACTION_LABELS.items()},
                    value="stub_todo"
                ).classes('w-full')

                # Conditional fields container
                fields_area = ui.column().classes('w-full')
                uploaded_dep_files[dep_name] = []

                def update_fields(dep_n=dep_name, act=action, area=fields_area, callers_list=callers):
                    area.clear()
                    a = act.value
                    with area:
                        if a == "upload":
                            def on_dep_upload(e: events.UploadEventArguments, dn=dep_n):
                                name, data = _upload_info(e)
                                uploaded_dep_files[dn].append((name, data))
                                ui.notify(f"Uploaded: {name}", type="positive")
                            ui.upload(on_upload=on_dep_upload, auto_upload=True,
                                      label=f"Upload file with {dep_n}"
                            ).props('accept=".sql,.proc,.bteq,.txt"').classes('w-full')

                        elif a == "inline_body":
                            body_input = ui.textarea(
                                label=f"Paste full body of {dep_n}",
                                placeholder=f"CREATE OR REPLACE FUNCTION {dep_n}(...)..."
                            ).classes('w-full').style('min-height:120px')
                            resolutions[dep_n] = {"action": a, "_body_ref": body_input}

                        elif a == "stub_todo":
                            ui.html(f'<div style="background:#F1F5F9;border-radius:6px;padding:8px 12px;'
                                    f'color:#7D8590;font-size:12px">Every call to <code>{dep_n}</code> '
                                    f'→ <code style="color:#B8A5FF"># TODO: UNMAPPED</code></div>').classes('w-full')

                        elif a == "skip_callers":
                            ui.html(f'<div style="background:#FBF5F5;border-radius:6px;padding:8px 12px;'
                                    f'color:#9B8080;font-size:12px">Excluded: '
                                    f'<code>{"</code>, <code>".join(callers_list)}</code></div>').classes('w-full')

                    resolutions[dep_n] = {"action": a}

                action.on('change', lambda e, dn=dep_name, act=action, area=fields_area, cl=callers: update_fields(dn, act, area, cl))
                update_fields(dep_name, action, fields_area, callers)

                ui.html('<div class="pg-hr"></div>').classes('w-full')

            # Submit all
            async def submit_all():
                final = {}
                for dep in deps:
                    if dep.get("resolution"):
                        continue
                    dn = dep["dep_name"]
                    res = resolutions.get(dn, {"action": "stub_todo"})
                    a = res.get("action", "stub_todo")
                    entry = {"action": a}

                    if a == "upload":
                        if not uploaded_dep_files.get(dn):
                            ui.notify(f"Upload file for {dn}", type="negative")
                            return
                        # Save files
                        jobs_dir = Path(store.base_dir).parent
                        upload_dir = jobs_dir / "uploaded_deps"
                        upload_dir.mkdir(parents=True, exist_ok=True)
                        for fname, fbytes in uploaded_dep_files[dn]:
                            (upload_dir / fname).write_bytes(fbytes)
                            entry["path"] = str(upload_dir / fname)

                    elif a == "inline_body":
                        body_ref = res.get("_body_ref")
                        body = body_ref.value if body_ref else ""
                        if not (body or "").strip():
                            ui.notify(f"Paste body for {dn}", type="negative")
                            return
                        entry["body"] = body

                    final[dn] = entry

                try:
                    from sql_migration.core.dependency_gate import DependencyGate, DependencyGateResult, ExternalDepInfo
                    gate = DependencyGate(store)
                    result = DependencyGateResult(
                        has_unresolved=True,
                        deps=[ExternalDepInfo(dep_name=d["dep_name"], called_by=d.get("called_by", []),
                                               call_count=d.get("call_count", 0),
                                               sample_call_sites=d.get("sample_call_sites", []))
                              for d in deps],
                    )
                    resolved = gate.apply_resolutions(result, final)
                    ui.notify(f"✅ All {len(deps)} dependencies resolved!", type="positive")
                    refresh()
                except Exception as e:
                    ui.notify(f"Failed: {e}", type="negative")

            ui.button("📤 Submit All Resolutions", on_click=submit_all).props(
                'color=deep-purple-6 no-caps unelevated').classes('w-full').style(
                'padding:12px;border-radius:9px;font-size:14px')

    run_id_input.on('change', lambda: refresh())
    refresh()