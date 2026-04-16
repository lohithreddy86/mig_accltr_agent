"""views/artifacts.py — Browse all pipeline artifacts."""
from __future__ import annotations
import json, sys
from pathlib import Path
from nicegui import ui, app

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import get_store

AGENTS_ORDER = ["analysis","planning","orchestrator","conversion","validation","feedback","samples"]
AGENT_LABELS = {
    "analysis":"00 · Analysis","planning":"01 · Planning","orchestrator":"02 · Orchestrator",
    "conversion":"03 · Conversion","validation":"04 · Validation",
    "feedback":"05 · Feedback","samples":"06 · Sample Data",
}
AGENT_COLORS = {
    "analysis":"#6C5CE7","planning":"#6C5CE7","orchestrator":"#4ECDC4",
    "conversion":"#B8A5FF","validation":"#6C5CE7","feedback":"#9B6B6B","samples":"#7ECFC7",
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

    def refresh():
        content.clear()
        rid = (run_id_input.value or "").strip()
        if not rid:
            with content:
                ui.html('<div class="ib">ℹ️ Enter a Run ID to browse artifacts.</div>').classes('w-full')
            return

        store = get_store(rid)
        summary = store.artifact_summary()
        if not summary:
            with content:
                ui.html('<div class="ib">⚠️ No artifacts found for this run yet.</div>').classes('w-full')
            return

        by_agent: dict[str, list] = {}
        for art in summary:
            by_agent.setdefault(art["agent"], []).append(art)

        with content:
            ui.html(f'<div style="color:#7D8590;font-size:13px;margin-bottom:8px">'
                    f'<b>{len(summary)}</b> files across <b>{len(by_agent)}</b> agents</div>').classes('w-full')
            ui.html('<div class="pg-hr"></div>').classes('w-full')

            for agent_id in AGENTS_ORDER:
                if agent_id not in by_agent:
                    continue
                files = by_agent[agent_id]
                color = AGENT_COLORS.get(agent_id, "#7D8590")
                label = AGENT_LABELS.get(agent_id, agent_id)

                with ui.expansion(f"{label}  ({len(files)} files)",
                                   value=(agent_id == "orchestrator")).classes('w-full'):
                    for art in sorted(files, key=lambda x: x["filename"]):
                        fname = art["filename"]
                        size_kb = round(art["size_bytes"] / 1024, 1)
                        ts = art.get("written_at", "")[:16].replace("T", " ")
                        fpath = art["path"]

                        # Wrapper column: row on top, preview below
                        with ui.column().classes('w-full gap-0'):
                            # File info row
                            with ui.row(wrap=False).classes('w-full items-center').style('padding:2px 0'):
                                ui.html(f'<code style="color:{color};font-size:12px">{fname}</code>'
                                        f'<span style="color:#484F58;font-size:11px;margin-left:8px">{ts}</span>'
                                        ).style('flex:1')
                                ui.html(f'<span style="color:#7D8590;font-size:11px">{size_kb} KB</span>')
                                # Placeholder button — handler set below after preview_area exists
                                view_btn = ui.button("👁").props('flat dense size=sm')

                            # Preview area — BELOW the row (inside wrapper column, after row)
                            preview_area = ui.column().classes('w-full')
                            preview_area.set_visibility(False)

                            def toggle_view(pa=preview_area, fp=fpath, fn=fname):
                                if pa.visible:
                                    pa.set_visibility(False)
                                    return
                                pa.clear()
                                pa.set_visibility(True)
                                with pa:
                                    try:
                                        p = Path(fp)
                                        if fn.endswith(".json"):
                                            data = json.loads(p.read_text(encoding="utf-8"))
                                            if isinstance(data, dict) and "_meta" in data and "data" in data:
                                                data = data["data"]
                                            ui.html(f'<pre style="background:#0d1117;color:#c9d1d9;'
                                                    f'border-radius:6px;padding:10px;font-size:11px;'
                                                    f'max-height:300px;overflow:auto">'
                                                    f'{json.dumps(data, indent=2)[:5000]}</pre>').classes('w-full')
                                        elif fn.endswith((".py", ".sql")):
                                            code = p.read_text(encoding="utf-8", errors="replace")[:5000]
                                            ui.html(f'<pre style="background:#0d1117;color:#c9d1d9;'
                                                    f'border-radius:6px;padding:10px;font-size:11px;'
                                                    f'max-height:300px;overflow:auto">{code}</pre>').classes('w-full')
                                        else:
                                            text = p.read_text(encoding="utf-8", errors="replace")[:2000]
                                            ui.html(f'<pre style="background:#F1F5F9;border-radius:6px;'
                                                    f'padding:10px;font-size:11px;max-height:200px;'
                                                    f'overflow:auto">{text}</pre>').classes('w-full')
                                    except Exception as e:
                                        ui.html(f'<div style="color:#9B6B6B;font-size:12px">Error: {e}</div>')

                            view_btn.on_click(toggle_view)

    run_id_input.on('change', lambda: refresh())
    refresh()