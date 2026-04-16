"""views/frozen_feedback.py — Human feedback UI for FROZEN procs."""
from __future__ import annotations
import sys
from pathlib import Path
from nicegui import ui, app, events

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import (
    get_checkpoint, get_converted_code, get_feedback_bundle,
    get_store, get_validation_result, status_badge,
)


def _upload_info(e):
    """Extract (name, bytes) from UploadEventArguments — handles NiceGUI version diffs."""
    name = getattr(e, 'name', None) or getattr(getattr(e, 'file', None), 'name', 'unknown')
    if hasattr(e, 'content'):
        data = e.content.read()
    else:
        data = getattr(getattr(e, 'file', None), '_data', b'')
    return name, data

ACTION_DEFS = [
    ("mark_skip",                "⏭️ Mark as Skip",
     "Skip this proc entirely — not included in migration output."),
    ("override_pass",            "✅ Override Pass",
     "Accept current code as-is. Mark PARTIAL and continue."),
    ("replan_with_notes",        "🔄 Replan with Notes",
     "Add notes for Planning Agent to change strategy and retry."),
    ("manual_rewrite_provided",  "✍️ Manual Rewrite",
     "Provide corrected code. Validation runs on your code."),
    ("upload_source_and_reanalyze", "📁 Upload Source & Re-analyze",
     "Upload missing files. Pipeline re-runs Analysis for all affected procs."),
]


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

    def refresh():
        content.clear()
        rid = (run_id_input.value or "").strip()
        if not rid:
            with content:
                ui.html('<div class="ib">ℹ️ Enter a Run ID to view frozen procs.</div>').classes('w-full')
            return

        store = get_store(rid)
        ck = get_checkpoint(store)
        if not ck:
            with content:
                ui.html(f'<div class="ib">⚠️ No checkpoint for run `{rid}`.</div>').classes('w-full')
            return

        procs_data = ck.get("procs", {})
        frozen_procs = [n for n, s in procs_data.items() if s.get("status") == "FROZEN"]
        resolved_procs = store.list_resolved_procs()

        if not frozen_procs:
            with content:
                ui.html('<div class="log-status log-ok">✅ No frozen procs. All progressing normally.</div>').classes('w-full')
                if resolved_procs:
                    ui.html(f'<div style="color:#7D8590;font-size:12px">Previously resolved: {", ".join(resolved_procs)}</div>').classes('w-full')
            return

        with content:
            ui.html(f'<div style="color:#7D8590;font-size:13px;margin-bottom:8px">'
                    f'<b>{len(frozen_procs)}</b> proc(s) awaiting review</div>').classes('w-full')
            ui.html('<div class="pg-hr"></div>').classes('w-full')

            for proc_name in frozen_procs:
                _build_frozen_proc(store, proc_name, procs_data.get(proc_name, {}),
                                    proc_name in resolved_procs)
                ui.html('<div class="pg-hr"></div>').classes('w-full')

    def _build_frozen_proc(store, proc_name, state, already_resolved):
        # Header
        resolved_badge = ('&nbsp;&nbsp;<span style="color:#4ECDC4;font-size:12px">✅ Resolved</span>'
                          if already_resolved else "")
        ui.html(
            f'<div style="background:#FBF5F5;border:1px solid #9B6B6B55;'
            f'border-left:4px solid #9B6B6B;border-radius:0 10px 10px 0;'
            f'padding:12px 18px;margin-bottom:8px">'
            f'<span style="color:#9B8080;font-size:16px;font-weight:800">🔴 {proc_name}</span>'
            f'{resolved_badge}</div>'
        ).classes('w-full')

        if already_resolved:
            try:
                res = store.read("feedback", f"feedback_resolved_{proc_name}.json")
                ui.html(f'<div style="background:#ECFDF5;border:1px solid rgba(78,205,196,0.4);'
                        f'border-radius:8px;padding:10px 14px">'
                        f'<span style="color:#7ECFC7;font-weight:700">Resolution: {res.get("action","")}</span>'
                        f'<br><span style="color:#7D8590;font-size:12px">{res.get("notes","")}</span></div>').classes('w-full')
            except Exception:
                pass
            return

        bundle = get_feedback_bundle(store, proc_name)

        # Tabs
        with ui.tabs().classes('w-full') as tabs:
            tab_why = ui.tab("❓ Why It Failed")
            tab_code = ui.tab("📄 Code")
            items = (bundle or {}).get("manual_review_items", [])
            tab_unmapped = ui.tab(f"⚠️ UNMAPPED ({len(items)})")
            tab_resolve = ui.tab("✅ Resolve")

        with ui.tab_panels(tabs, value=tab_why).classes('w-full'):
            # Tab 1: Why it failed
            with ui.tab_panel(tab_why):
                _build_why_tab(store, proc_name, state, bundle)

            # Tab 2: Converted code
            with ui.tab_panel(tab_code):
                _build_code_tab(store, proc_name, bundle)

            # Tab 3: UNMAPPED constructs
            with ui.tab_panel(tab_unmapped):
                _build_unmapped_tab(items, proc_name)

            # Tab 4: Resolution
            with ui.tab_panel(tab_resolve):
                _build_resolve_tab(store, proc_name)

    def _build_why_tab(store, proc_name, state, bundle):
        # Manifest context
        manifest = (bundle or {}).get("manifest_entry", {})
        if manifest:
            ui.html(f'<div style="display:flex;gap:16px;margin-bottom:8px">'
                    f'<div style="text-align:center"><div class="metric-lbl">Lines</div>'
                    f'<div class="metric-val" style="font-size:20px">{manifest.get("line_count","—")}</div></div>'
                    f'<div style="text-align:center"><div class="metric-lbl">Cursor</div>'
                    f'<div class="metric-val" style="font-size:20px">{"Yes" if manifest.get("has_cursor") else "No"}</div></div>'
                    f'<div style="text-align:center"><div class="metric-lbl">Dynamic SQL</div>'
                    f'<div class="metric-val" style="font-size:20px">{"Yes" if manifest.get("has_dynamic_sql") else "No"}</div></div>'
                    f'</div>').classes('w-full')

        # Complexity
        cx = (bundle or {}).get("complexity_entry", {})
        if cx:
            score = cx.get("score", "—")
            sc = {"LOW":"#4ECDC4","MEDIUM":"#B8A5FF","HIGH":"#9B6B6B"}.get(score,"#7D8590")
            ui.html(f'<div style="background:#F1F5F9;border-radius:6px;padding:8px 12px;margin:8px 0">'
                    f'Complexity: <span style="color:{sc};font-weight:700">{score}</span>'
                    f'<span style="color:#7D8590;font-size:12px;margin-left:8px">{cx.get("rationale","")}</span></div>').classes('w-full')

        # Error history
        errors = state.get("error_history", [])
        if errors:
            ui.html(f'<div style="font-weight:700;font-size:13px;color:#1B2838;margin:8px 0">'
                    f'Error History ({len(errors)} entries)</div>').classes('w-full')
            for e in errors:
                ts = (e.get("timestamp","") if isinstance(e,dict) else "")[:19].replace("T"," ")
                msg = e.get("message","") if isinstance(e,dict) else str(e)
                etype = e.get("error_type","") if isinstance(e,dict) else ""
                ui.html(f'<div style="background:#F1F5F9;border-radius:6px;padding:8px 12px;margin-bottom:4px">'
                        f'<span style="color:#9B6B6B;font-size:11px;font-family:monospace">[{etype}]</span>'
                        f'<br><span style="color:#1B2838;font-size:12px">{msg[:300]}</span>'
                        f'<br><span style="color:#484F58;font-size:10px">{ts}</span></div>').classes('w-full')

        # Validation result
        val = get_validation_result(store, proc_name)
        if val:
            outcome = val.get("outcome","—")
            oc = {"PASS":"#4ECDC4","PARTIAL":"#7ECFC7","FAIL":"#9B6B6B"}.get(outcome,"#7D8590")
            ui.html(f'<div style="background:#F1F5F9;border-radius:6px;padding:10px 14px;margin-top:8px">'
                    f'Validation: <span style="color:{oc};font-weight:700">{outcome}</span>'
                    f' · Level: {val.get("validation_level","—")}'
                    f'{"<br>Reason: " + val.get("fail_reason","") if val.get("fail_reason") else ""}'
                    f'</div>').classes('w-full')

    def _build_code_tab(store, proc_name, bundle):
        chunks = (bundle or {}).get("chunk_codes", {})
        if chunks:
            for cid, code in chunks.items():
                lang = "python" if "def " in code or "spark" in code.lower() else "sql"
                ui.html(f'<div style="font-weight:700;font-size:12px;margin-bottom:4px">Chunk: {cid}</div>').classes('w-full')
                ui.html(f'<pre style="background:#0d1117;color:#c9d1d9;border-radius:6px;'
                        f'padding:10px;font-size:11px;max-height:400px;overflow:auto">{code}</pre>').classes('w-full')
        else:
            code, ext = get_converted_code(store, proc_name)
            if code:
                ui.html(f'<pre style="background:#0d1117;color:#c9d1d9;border-radius:6px;'
                        f'padding:10px;font-size:11px;max-height:400px;overflow:auto">{code}</pre>').classes('w-full')
            else:
                ui.html('<div class="ib">No converted code — conversion failed before producing output.</div>').classes('w-full')

    def _build_unmapped_tab(items, proc_name):
        if not items:
            ui.html('<div class="ib">No UNMAPPED constructs. Failure not due to missing mappings.</div>').classes('w-full')
            return

        for item in items:
            fn = item.get("original_fn", "Unknown")
            line = item.get("line", "?")
            comment = item.get("comment", "")
            ui.html(f'<div style="background:#F5F3FF;border:1px solid rgba(220,180,180,0.3);'
                    f'border-radius:8px;padding:10px 14px;margin-bottom:8px">'
                    f'<span style="color:#B8A5FF;font-weight:700;font-family:monospace">{fn}</span>'
                    f'&nbsp;&nbsp;<span style="color:#484F58;font-size:11px">Line {line}</span>'
                    f'<br><code style="color:#B8A5FF;font-size:11px">{comment}</code></div>').classes('w-full')

    def _build_resolve_tab(store, proc_name):
        ui.html('<div style="font-weight:700;font-size:13px;color:#1B2838;margin-bottom:8px">'
                'Choose a resolution action:</div>').classes('w-full')

        action_radio = ui.radio(
            {a[0]: f"{a[1]} — {a[2]}" for a in ACTION_DEFS},
            value="mark_skip",
        ).classes('w-full')

        fields = ui.column().classes('w-full')
        uploaded_paths: list[str] = []

        notes_ref = [None]
        code_ref = [None]
        upload_notes_ref = [None]

        def update_fields():
            fields.clear()
            a = action_radio.value
            with fields:
                if a == "replan_with_notes":
                    notes_ref[0] = ui.textarea(label="Notes for Planning Agent",
                                                placeholder="E.g.: Switch to PYSPARK_PIPELINE..."
                    ).classes('w-full').style('min-height:120px')
                elif a == "manual_rewrite_provided":
                    code_ref[0] = ui.textarea(label="Corrected Code",
                                               placeholder="-- Your corrected code here"
                    ).classes('w-full').style('min-height:200px')
                elif a == "upload_source_and_reanalyze":
                    def on_src_upload(e: events.UploadEventArguments):
                        name, data = _upload_info(e)
                        upload_dir = Path(store.base_dir) / "uploaded_sources"
                        upload_dir.mkdir(parents=True, exist_ok=True)
                        out = upload_dir / name
                        out.write_bytes(data)
                        uploaded_paths.append(str(out))
                        ui.notify(f"Saved: {name}", type="positive")

                    ui.upload(on_upload=on_src_upload, multiple=True, auto_upload=True,
                              label="Upload source files"
                    ).props('accept=".sql,.proc,.bteq,.txt"').classes('w-full')
                    upload_notes_ref[0] = ui.textarea(label="Optional notes",
                                                       placeholder="E.g.: calc_risk_score is in risk_utils.sql"
                    ).classes('w-full').style('min-height:80px')

        action_radio.on('change', lambda: update_fields())
        update_fields()

        async def submit_resolution():
            a = action_radio.value
            notes = ""
            corrected = ""

            if a == "replan_with_notes":
                notes = notes_ref[0].value if notes_ref[0] else ""
                if not notes.strip():
                    ui.notify("Provide notes for the Planning Agent.", type="negative")
                    return
            elif a == "manual_rewrite_provided":
                corrected = code_ref[0].value if code_ref[0] else ""
                if not corrected.strip():
                    ui.notify("Provide the corrected code.", type="negative")
                    return
            elif a == "upload_source_and_reanalyze":
                if not uploaded_paths:
                    ui.notify("Upload at least one source file.", type="negative")
                    return
                notes = upload_notes_ref[0].value if upload_notes_ref[0] else ""

            try:
                from sql_migration.core.human_feedback import HumanFeedbackHandler
                handler = HumanFeedbackHandler(store)
                handler.submit_resolution(
                    proc_name=proc_name, action=a, notes=notes,
                    corrected_code=corrected,
                    uploaded_file_paths=uploaded_paths if uploaded_paths else None,
                )
                label = next(ad[1] for ad in ACTION_DEFS if ad[0] == a)
                ui.notify(f"✅ Resolution submitted: {label}", type="positive")
            except Exception as e:
                ui.notify(f"Failed: {e}", type="negative")

        ui.button(f"📤 Submit Resolution for `{proc_name}`", on_click=submit_resolution).props(
            'color=deep-purple-6 no-caps unelevated').classes('w-full').style(
            'padding:12px;border-radius:9px;font-size:14px;margin-top:8px')

    run_id_input.on('change', lambda: refresh())
    refresh()