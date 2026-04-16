"""
app.py — Migration Accelerator v10 UI (NiceGUI)
=================================================
Main entry point. Run with:  python ui/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from nicegui import ui, app

# ── Make project root + src importable ────────────────────────────────────────
_UI_DIR  = Path(__file__).resolve().parent
_PROJECT = _UI_DIR.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))
if str(_PROJECT / "src") not in sys.path:
    sys.path.insert(0, str(_PROJECT / "src"))

from ui.helpers import (
    recover_pipeline_state, get_pipeline_log_data, clear_pipeline_state,
)


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS
# ══════════════════════════════════════════════════════════════════════════════
ui.add_css("""
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&display=swap');
:root{--bg:#1B2838;--sb:#131A28;--c:#FFF;--inp:#F8FAFC;--p:#6C5CE7;
--pg:linear-gradient(135deg,#7C6CF0,#5545D0);--td:#1B2838;--tl:#5A6577;
--th:#9BA5B5;--tw:#E8ECF2;--tm:rgba(255,255,255,.4);--b:#E2E8F0;--bd:rgba(255,255,255,.08)}

body,.q-page,.q-layout,.q-page-container{font-family:'DM Sans',system-ui,sans-serif!important;background:var(--bg)!important}
.q-page-container{background-image:radial-gradient(ellipse 70% 50% at 50% -5%,rgba(108,92,231,.09) 0%,transparent 60%),radial-gradient(ellipse 50% 35% at 85% 5%,rgba(80,120,200,.06) 0%,transparent 50%)!important}

/* Header hidden */
.q-header{display:none!important}
.q-toolbar{display:none!important}

/* Drawer */
.q-drawer{background:var(--sb)!important;border-right:1px solid var(--bd)!important}
.q-drawer .q-mini-drawer-hide{transition:opacity .2s}

/* Main content */
.nicegui-content{padding:0!important}

/* ── Sidebar ─────────────────────────────────────────── */
.sb-brand{display:flex;align-items:center;gap:8px;padding:4px 6px 6px}
.sb-av{width:32px;height:32px;border-radius:8px;background:var(--pg);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:500;color:#fff;flex-shrink:0}
.sb-t{font-size:13px;font-weight:600;color:var(--tw);line-height:1.2}
.sb-s{font-size:7px;color:rgba(255,255,255,.2);letter-spacing:.5px;text-transform:uppercase}
.sb-sep{height:1px;background:var(--bd);margin:4px 0 6px}
.sb-n{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;font-size:13px;color:rgba(255,255,255,.45);cursor:pointer;transition:all .15s;margin:0 0 1px;user-select:none;white-space:nowrap;overflow:hidden;width:100%}
.sb-n:hover{background:rgba(255,255,255,.04);color:rgba(255,255,255,.7)}
.sb-n.on{background:var(--pg);color:#fff;font-weight:500;box-shadow:0 2px 8px rgba(108,92,231,.3);border-radius:8px}
.sb-n .material-icons{width:22px;text-align:center;flex-shrink:0;font-size:18px}
.sb-ft{font-size:8px;color:rgba(255,255,255,.12);line-height:1.3;padding:0 4px}

/* Kill NiceGUI gaps inside drawer */
.q-drawer .nicegui-content{gap:0!important}
.q-drawer .q-list,.q-drawer .nicegui-column{gap:0!important}
.q-drawer div.nicegui-element{margin:0!important;padding:0!important}

/* ── Top bar ─────────────────────────────────────────── */
.top-bar{display:flex;align-items:center;justify-content:flex-end;gap:10px;padding:0 0 4px}
.u-n{font-size:12px;color:var(--tm)}
.u-av{width:32px;height:32px;border-radius:50%;background:var(--pg);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:500;color:#fff;box-shadow:0 2px 8px rgba(108,92,231,.3)}

/* ── Page titles ─────────────────────────────────────── */
.pg-t{font-size:20px;font-weight:600;color:var(--tw);margin:0 0 2px}
.pg-d{font-size:13px;color:var(--tm);margin:0 0 10px}
.pg-hr{height:1px;background:var(--bd);margin:0 0 12px}

/* ── Section header ──────────────────────────────────── */
.sh{font-size:11px;font-weight:600;color:#7D8590;letter-spacing:1px;text-transform:uppercase;margin:0 0 8px;display:flex;align-items:center;gap:6px}
.cd{width:7px;height:7px;border-radius:3px;background:var(--pg);flex-shrink:0}
.section-hdr{font-size:11px;font-weight:700;color:#5A6577;letter-spacing:1.5px;text-transform:uppercase;margin:16px 0 6px 0}

/* ── White card ──────────────────────────────────────── */
.wc{background:var(--c)!important;border-radius:12px!important;padding:26px 30px!important;box-shadow:0 1px 12px rgba(0,0,0,.08)!important;border:none!important}
.wc .q-field__control{background:var(--inp)!important;border:1px solid var(--b)!important;border-radius:7px!important;min-height:38px!important}
.wc .q-field__control:focus-within{border-color:var(--p)!important;box-shadow:0 0 0 2px rgba(108,92,231,.1)!important}
.wc .q-field__native,.wc .q-field__input{color:var(--td)!important;font-family:'DM Sans',system-ui!important;font-size:13px!important}
.wc .q-field__label{color:var(--tl)!important;font-size:12px!important}
.wc .q-field__bottom{padding:2px 12px 0!important}
.wc .q-field__messages{color:var(--th)!important;font-size:10px!important}
.wc .q-field{margin-bottom:8px}
.wc .q-select .q-field__native{color:var(--td)!important}
.wc .q-field__marginal{color:var(--tl)!important}
/* Outlined variant (search/Run ID inputs) */
.wc .q-field--outlined .q-field__control{background:var(--inp)!important;border:1.5px solid var(--b)!important;border-radius:8px!important}
.wc .q-field--outlined .q-field__control:focus-within{border-color:var(--p)!important}
.wc .q-field--outlined .q-field__control:before{border:none!important}
.wc .q-field--outlined .q-field__control:after{border:none!important}

/* ── Upload zone — override Quasar QUploader ─────── */
.uz{border:1.5px dashed #CBD5E1;border-radius:8px;padding:18px 14px;text-align:center;cursor:pointer;transition:all .15s;background:#FAFBFC;margin-top:3px}
.uz:hover{border-color:var(--p);background:#F8F7FF}
.uz-i{font-size:18px;color:#CBD5E1;margin-bottom:4px}
.uz-t{font-size:11px;color:#6B7A8D;line-height:1.4}
.uz-t strong{color:#475569}
.uz-b{display:inline-block;padding:4px 14px;border-radius:5px;border:1px solid var(--b);font-size:10px;color:#475469;background:#fff;margin-top:6px;cursor:pointer;font-family:inherit;transition:all .15s}
.uz-b:hover{border-color:var(--p);color:var(--p)}
.uz-h{font-size:9px;color:var(--th);margin-top:4px}
.uz-l{font-size:11px;color:var(--tl);font-weight:500;margin:14px 0 4px}

/* Override Quasar QUploader to match design */
.q-uploader{border:1.5px dashed #CBD5E1!important;border-radius:10px!important;background:#FAFBFC!important;box-shadow:none!important;max-height:none!important}
.q-uploader:hover{border-color:var(--p)!important;background:#F8F7FF!important}
.q-uploader__header{background:#FAFBFC!important;color:var(--tl)!important;border-bottom:1px solid #E2E8F0!important;padding:8px 12px!important;min-height:auto!important}
.q-uploader__title,.q-uploader__subtitle{color:var(--tl)!important;font-size:12px!important;font-family:'DM Sans',system-ui!important}
.q-uploader__subtitle{font-size:10px!important;color:var(--th)!important}
.q-uploader__list{background:#FAFBFC!important;padding:6px!important}
.q-uploader .q-btn{color:var(--p)!important}
.q-uploader__dnd{background:rgba(108,92,231,.04)!important;outline:2px dashed var(--p)!important;outline-offset:-4px}

/* ── Right-side panel cards ──────────────────────────── */
.pc{background:var(--c);border-radius:12px;padding:18px 22px;box-shadow:0 1px 12px rgba(0,0,0,.08);margin-bottom:12px}
.pc-t{font-size:14px;font-weight:600;color:var(--td);margin-bottom:8px}
.pr{display:flex;align-items:center;justify-content:space-between;padding:9px 0;border-bottom:1px solid #F1F5F9}
.pr:last-of-type{border-bottom:none}
.pll{display:flex;align-items:center;gap:9px;flex-shrink:0}
.pi{width:26px;height:26px;border-radius:7px;background:linear-gradient(135deg,#F0EDFF,#E8E4FF);display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--p);flex-shrink:0}
.plb{font-size:12px;color:var(--tl);white-space:nowrap}
.prr{display:flex;align-items:center;gap:5px;margin-left:auto;padding-left:10px}
.pv{font-size:12px;color:var(--td);font-weight:500}
.pvd{font-size:12px;color:#B0B8C5}
.pm{font-family:'SF Mono','Fira Code',monospace;font-size:10px;color:var(--p);background:#F5F3FF;padding:2px 7px;border-radius:3px}
.pch{color:#CBD5E1;font-size:14px}
.tg{width:34px;height:18px;border-radius:9px;background:#E2E8F0;position:relative;cursor:pointer;transition:background .2s;flex-shrink:0}
.tg.on{background:var(--pg);box-shadow:0 1px 5px rgba(108,92,231,.3)}
.tg-d{width:14px;height:14px;border-radius:50%;background:#fff;position:absolute;top:2px;left:2px;transition:left .2s;box-shadow:0 1px 2px rgba(0,0,0,.15)}
.tg.on .tg-d{left:18px}
.pc-f{padding-top:8px;text-align:center}
.pc-f a{font-size:11px;color:var(--p);text-decoration:none;font-weight:500}
.pc-f a:hover{opacity:.7}

/* ── Submit button ───────────────────────────────────── */
.btn-go{width:100%;padding:12px 20px;border-radius:9px;background:var(--pg);color:#fff;border:none;font-size:14px;font-weight:500;cursor:pointer;font-family:'DM Sans',system-ui;box-shadow:0 4px 16px rgba(108,92,231,.35);transition:all .2s;text-align:center}
.btn-go:hover{box-shadow:0 6px 24px rgba(108,92,231,.5);transform:translateY(-1px)}

/* ── Metrics ─────────────────────────────────────────── */
.metric-lbl{font-size:11px;color:#5A6577;text-transform:uppercase;letter-spacing:1px}
.metric-val{font-size:28px;font-weight:800;line-height:1.1}
.mono-sm{font-family:monospace;font-size:12px;color:#5A6577}

/* ── Flow diagram ────────────────────────────────────── */
.fw{background:var(--bg);border-radius:8px;padding:12px;border:1px solid rgba(255,255,255,.05)}
.fd{font-size:11px;color:rgba(255,255,255,.3);margin-top:8px;padding:6px 10px;border-left:2px solid var(--p);background:rgba(108,92,231,.04);border-radius:0 5px 5px 0;line-height:1.4}

/* ── Expander ────────────────────────────────────────── */
.q-expansion-item{background:var(--c)!important;border-radius:12px!important;box-shadow:0 1px 8px rgba(0,0,0,.06)!important;border:none!important;overflow:hidden}
.q-expansion-item .q-item{min-height:40px!important}
.q-expansion-item .q-item__label{color:var(--td)!important;font-weight:500!important;font-size:13px!important}
.q-expansion-item .q-item__section--side .q-icon{color:var(--tl)!important}
.q-expansion-item__content{background:var(--c)!important;padding:4px 12px 12px!important}
.q-expansion-item .q-tab{color:var(--tl)!important;font-size:12px!important;text-transform:none!important;min-height:32px!important}
.q-expansion-item .q-tab--active{color:var(--p)!important}
.q-expansion-item .q-tab-panel{padding:0!important}

/* ── Info banner ─────────────────────────────────────── */
.ib{background:rgba(108,92,231,.05);border:1px solid rgba(108,92,231,.12);border-radius:8px;padding:10px 16px;font-size:11px;color:var(--tm);display:flex;align-items:center;gap:8px;margin:12px 0}

/* ── Log viewer ──────────────────────────────────────── */
.log-box{background:#0d1117;border:1px solid rgba(255,255,255,.08);border-radius:8px;padding:12px;max-height:400px;overflow-y:auto;font-family:'SF Mono','Fira Code',monospace;font-size:11px;color:#c9d1d9;white-space:pre-wrap;word-break:break-all;line-height:1.5}
.log-status{padding:8px 14px;border-radius:8px;font-size:12px;display:flex;align-items:center;gap:8px;margin-bottom:8px}
.log-running{background:rgba(108,92,231,.1);border:1px solid rgba(108,92,231,.2);color:#B8A5FF}
.log-ok{background:rgba(78,205,196,.1);border:1px solid rgba(78,205,196,.2);color:#4ECDC4}
.log-err{background:rgba(155,107,107,.1);border:1px solid rgba(155,107,107,.2);color:#D4A5A5}
.log-warn{background:rgba(255,193,7,.08);border:1px solid rgba(255,193,7,.15);color:#FFD54F}
""", shared=True)


# ══════════════════════════════════════════════════════════════════════════════
# SHARED LAYOUT — sidebar + top bar + log viewer
# ══════════════════════════════════════════════════════════════════════════════

NAV_ITEMS = [
    ("rocket_launch", "Submit job",      "submit",    "/"),
    ("bar_chart",     "Pipeline monitor", "monitor",   "/monitor"),
    ("cable",         "Dependencies",     "deps",      "/deps"),
    ("ac_unit",       "Frozen procs",     "frozen",    "/frozen"),
    ("folder",        "Artifacts",        "artifacts", "/artifacts"),
]


def page_shell(active: str = "submit"):
    """Creates sidebar with mini-mode toggle. Returns drawer ref."""
    ui.header().style('display:none')

    drawer = ui.left_drawer(value=True, top_corner=True, bottom_corner=True).props(
        'width=185 mini-width=60 behavior=desktop bordered=false'
    ).style('background:var(--sb);border-right:1px solid var(--bd);padding:6px 6px')

    def toggle_mini():
        if 'mini' in drawer._props:
            drawer.props(remove='mini')
        else:
            drawer.props('mini')

    with drawer:
        # Hamburger + brand on same line
        with ui.row(wrap=False).classes('items-center w-full').style('gap:8px;padding:2px 4px 0'):
            ui.button(on_click=toggle_mini, icon='menu').props(
                'flat color=white dense').style('opacity:.5')
            ui.html('<div class="sb-av" style="width:30px;height:30px;font-size:10px">MA</div>')
            ui.html('<div class="q-mini-drawer-hide" style="line-height:1.1">'
                    '<div class="sb-t" style="font-size:13px">Migration</div>'
                    '<div class="sb-s">Accelerator v10</div></div>')

        ui.html('<div class="sb-sep"></div>')

        # Nav — all in one HTML block (sanitize=False to allow onclick)
        nav_html = ""
        for ico, lbl, key, href in NAV_ITEMS:
            c = "on" if key == active else ""
            nav_html += (f'<div class="sb-n {c}" onclick="window.location=\'{href}\'">'
                         f'<i class="material-icons">{ico}</i>'
                         f'<span class="q-mini-drawer-hide">{lbl}</span></div>')
        ui.html(nav_html, sanitize=False).classes('w-full')

        ui.space()
        ui.html('<div class="sb-sep"></div>'
                '<div class="sb-ft q-mini-drawer-hide">'
                'Lakehouse Platform<br>HDFC Bank — Data Engineering</div>')

    return drawer


def page_header(title: str, desc: str = ""):
    """Page title (left) + SK avatar (right) on the SAME row. Ensures vertical alignment with sidebar brand."""
    with ui.row(wrap=False).classes('w-full items-start').style('margin-bottom:4px'):
        with ui.column().classes('flex-1 gap-0'):
            ui.html(f'<div class="pg-t">{title}</div>').classes('w-full')
            if desc:
                ui.html(f'<div class="pg-d">{desc}</div>').classes('w-full')
        ui.html('<div style="display:flex;align-items:center;gap:8px;padding-top:2px">'
                '<span class="u-n">Suryansh K.</span>'
                '<div class="u-av">SK</div></div>')
    ui.html('<div class="pg-hr"></div>').classes('w-full')


def build_log_viewer(on_monitor: bool = False):
    """
    Pipeline log viewer — shown at bottom of every page except monitor.
    Uses ui.timer to auto-refresh while pipeline is running.
    """
    if on_monitor:
        return

    storage = app.storage.user

    # Container for log output
    log_container = ui.column().classes('w-full')

    def refresh_log():
        log_data = get_pipeline_log_data(storage)
        log_container.clear()

        if not log_data:
            return

        with log_container:
            ui.html('<div class="pg-hr"></div>').classes('w-full')
            ui.html(f'<div class="pg-t" style="font-size:16px">'
                    f'📜 Pipeline Output — <code>{log_data["run_id"]}</code></div>').classes('w-full')

            status = log_data["status"]
            if status == "running":
                ui.html(f'<div class="log-status log-running">'
                        f'⚙️ Pipeline running — PID: <b>{log_data["pid"]}</b></div>').classes('w-full')
            elif status == "failed":
                ui.html(f'<div class="log-status log-err">'
                        f'❌ Pipeline failed — PID: {log_data["pid"]}</div>').classes('w-full')
            elif status == "complete":
                ui.html(f'<div class="log-status log-ok">'
                        f'✅ Pipeline completed. Switch to Pipeline Monitor for details.</div>').classes('w-full')
            else:
                ui.html(f'<div class="log-status log-warn">'
                        f'Pipeline ended — PID: {log_data["pid"]}</div>').classes('w-full')

            ui.html(f'<div class="log-box">{log_data["log_tail"]}</div>').classes('w-full')

            if not log_data["is_running"]:
                def do_clear():
                    clear_pipeline_state(storage)
                    log_container.clear()

                ui.button("🗑️ Clear log", on_click=do_clear).props(
                    'flat color=red dense').style('margin-top:6px')

    # Initial render
    refresh_log()

    # Auto-refresh while running
    def poll_log():
        log_data = get_pipeline_log_data(storage)
        if log_data and log_data["is_running"]:
            refresh_log()

    ui.timer(4.0, poll_log)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

@ui.page("/")
def page_submit():
    page_shell("submit")
    recover_pipeline_state(app.storage.user)

    with ui.column().classes('w-full').style('padding:6px 24px 24px'):
        page_header("Submit migration job", "Upload your source files and configure the conversion pipeline.")
        from ui.views.submit_job import build_page
        build_page()
        build_log_viewer()


@ui.page("/monitor")
def page_monitor():
    page_shell("monitor")
    recover_pipeline_state(app.storage.user)

    with ui.column().classes('w-full').style('padding:6px 24px 24px'):
        page_header("Pipeline monitor", "Live conversion status and proc-level drill-down.")
        from ui.views.pipeline_monitor import build_page
        build_page()


@ui.page("/deps")
def page_deps():
    page_shell("deps")
    recover_pipeline_state(app.storage.user)

    with ui.column().classes('w-full').style('padding:6px 24px 24px'):
        page_header("Dependency resolution", "Resolve missing external dependencies before conversion starts.")
        from ui.views.dependency import build_page
        build_page()
        build_log_viewer()


@ui.page("/frozen")
def page_frozen():
    page_shell("frozen")
    recover_pipeline_state(app.storage.user)

    with ui.column().classes('w-full').style('padding:6px 24px 24px'):
        page_header("Frozen procs — Human review", "Procs that exhausted all automatic retries need your input.")
        from ui.views.frozen_feedback import build_page
        build_page()
        build_log_viewer()


@ui.page("/artifacts")
def page_artifacts():
    page_shell("artifacts")
    recover_pipeline_state(app.storage.user)

    with ui.column().classes('w-full').style('padding:6px 24px 24px'):
        page_header("Artifact browser", "Inspect every artifact produced by the pipeline.")
        from ui.views.artifacts import build_page
        build_page()
        build_log_viewer()


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
ui.run(
    port=8082,
    title="Migration Accelerator",
    favicon="⚙",
    dark=True,
    storage_secret="lh-genie-v10-migration-accelerator",
    show=True,
    reload=False,
)
