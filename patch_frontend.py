#!/usr/bin/env python3
"""
patch_frontend_v2.py — Idempotent patcher for ProfitPilot index.html

Applies six surgical edits to convert the frontend to 100% rule-based mode:
  1. Disable Vercel live API (VERCEL_URL = '')
  2. Remove Gemini fetch from switchTab('ai')
  3. Neutralize fetchBriefing() into a no-op shim that calls renderAI()
  4. Remove delayed Gemini enhancement from init()
  5. Fix misleading "24 NSE stocks" empty-state copy
  6. Drop the legacy Gemini fallback path in renderAI()

Safe to run multiple times. Writes index.html.bak on first change.

Usage:
    python patch_frontend_v2.py index.html
"""
from __future__ import annotations
import re
import sys
from pathlib import Path


def patch(path: Path) -> int:
    if not path.exists():
        print(f"✗ {path} not found", file=sys.stderr)
        return 1

    original = path.read_text(encoding="utf-8")
    s = original
    changes = 0

    # ─── Edit 1: Disable Vercel ─────────────────────────────────────────────
    vercel_re = re.compile(r"const VERCEL_URL = '[^']*';")
    if "const VERCEL_URL = '';" in s:
        print("• [1/6] VERCEL_URL already disabled")
    elif vercel_re.search(s):
        s = vercel_re.sub(
            "// Rule-based engine only — no LLM, no live Vercel API.\n"
            "// predictions.json is the single source of truth, refreshed daily by GitHub Actions.\n"
            "const VERCEL_URL = '';",
            s, count=1,
        )
        changes += 1
        print("✓ [1/6] VERCEL_URL disabled")
    else:
        print("⚠ [1/6] VERCEL_URL declaration not found")

    # ─── Edit 2: switchTab ai branch ────────────────────────────────────────
    tab_variants = [
        "  if (tab==='ai' && !llm) fetchBriefing();",
        "  if (tab==='ai') { if (data && data.daily_brief) { renderAI(); } else if (!llm) { fetchBriefing(); } }",
        "  // Load AI briefing when user navigates to AI tab\n  if (tab==='ai' && !llm) fetchBriefing();",
        "  // Load AI briefing when user navigates to AI tab\n  if (tab==='ai') { if (data && data.daily_brief) { renderAI(); } else if (!llm) { fetchBriefing(); } }",
    ]
    new_tab = ("  // AI Brief tab — render rule-based daily_brief only. "
               "Never calls any LLM.\n  if (tab === 'ai') renderAI();")
    if new_tab in s:
        print("• [2/6] switchTab already patched")
    else:
        hit = False
        for v in tab_variants:
            if v in s:
                s = s.replace(v, new_tab, 1)
                changes += 1
                hit = True
                print("✓ [2/6] switchTab('ai') routed to renderAI()")
                break
        if not hit:
            print("⚠ [2/6] switchTab ai branch not found")

    # ─── Edit 3: Replace fetchBriefing function with no-op shim ─────────────
    shim = ("// ── AI Brief is 100% rule-based. This shim exists only for any legacy\n"
            "//    call sites — it renders the deterministic brief and never hits an LLM.\n"
            "async function fetchBriefing() {\n"
            "  renderAI();\n"
            "}\n")
    if "This shim exists only" in s:
        print("• [3/6] fetchBriefing already neutralized")
    else:
        pat = re.compile(r"async function fetchBriefing\(\)\s*\{.*?\n\}\n", re.DOTALL)
        m = pat.search(s)
        if m:
            s = s[:m.start()] + shim + s[m.end():]
            changes += 1
            print("✓ [3/6] fetchBriefing() replaced with no-op shim")
        else:
            print("⚠ [3/6] fetchBriefing() not found")

    # ─── Edit 4: Remove delayed Gemini enhancement in init() ────────────────
    d_variants = [
        "  // Only fetch optional Gemini enhancement if we have no local brief\n"
        "  if (!data.daily_brief) {\n    setTimeout(() => fetchBriefing(), 1500);\n  }\n",
        "  if (!data.daily_brief) {\n    setTimeout(() => fetchBriefing(), 1500);\n  }\n",
        "  setTimeout(() => fetchBriefing(), 1500);\n",
    ]
    hit = False
    for v in d_variants:
        if v in s:
            s = s.replace(v, "", 1)
            changes += 1
            hit = True
            print("✓ [4/6] delayed fetchBriefing() removed from init()")
            break
    if not hit:
        print("• [4/6] no delayed fetchBriefing() in init() (already clean)")

    # ─── Edit 5: Empty-state copy ───────────────────────────────────────────
    old_e = ('    el.innerHTML = `<div class="empty-state">\n'
             '      <div class="empty-icon">${catIcon}</div>\n'
             '      <div class="empty-title" style="color:var(--wht)">No ${cat}-term picks right now</div>\n'
             '      <div class="empty-text" style="max-width:340px;margin:0 auto">\n'
             '        Our engine analyzed <strong style="color:${catColor}">${analyzed} NSE stocks</strong>\n'
             '        and found no high-confidence ${catLabel} setups meeting our criteria today.<br><br>')
    new_e = ('    const universeLine = analyzed > 0\n'
             '      ? `Our engine analyzed <strong style="color:${catColor}">${analyzed} NSE stocks</strong> '
             'and found no high-confidence ${catLabel} setups meeting our criteria today.`\n'
             '      : `The rules engine has not published picks for this horizon in the latest run.`;\n'
             '    el.innerHTML = `<div class="empty-state">\n'
             '      <div class="empty-icon">${catIcon}</div>\n'
             '      <div class="empty-title" style="color:var(--wht)">No ${cat}-term picks right now</div>\n'
             '      <div class="empty-text" style="max-width:340px;margin:0 auto">\n'
             '        ${universeLine}<br><br>')
    if "const universeLine" in s:
        print("• [5/6] empty-state copy already patched")
    elif old_e in s:
        s = s.replace(old_e, new_e, 1)
        changes += 1
        print("✓ [5/6] empty-state copy fixed (no more hardcoded '24 NSE stocks')")
    else:
        print("⚠ [5/6] empty-state block not found")

    # ─── Edit 6: renderAI — drop legacy Gemini fallback ─────────────────────
    # Case A: newer version with "if (!brief && !llmData)"
    old_f_new = ("  if (!brief && !llmData) {\n"
                 "    el.innerHTML = `<div class=\"db-wrap\"><div class=\"db-card\">\n"
                 "      <div class=\"db-headline\">End-of-day brief not ready</div>\n"
                 "      <div style=\"font-family:var(--sans);font-size:12px;color:var(--tx2);"
                 "line-height:1.7;padding:8px 0\">\n"
                 "        Run the daily workflow (GitHub → Actions → Daily Stock Analysis) to generate the brief.\n"
                 "      </div>\n"
                 "    </div></div>`;\n"
                 "    return;\n"
                 "  }\n\n"
                 "  if (!brief) return renderAI_legacy(llmData);")
    new_f = ("  if (!brief) {\n"
             "    el.innerHTML = `<div class=\"db-wrap\"><div class=\"db-card\">\n"
             "      <div class=\"db-headline\">End-of-day brief not ready</div>\n"
             "      <div style=\"font-family:var(--sans);font-size:12px;color:var(--tx2);"
             "line-height:1.7;padding:8px 0\">\n"
             "        The daily rules engine has not produced a brief yet. This usually means\n"
             "        the 4:15 PM IST workflow hasn't run today, or <code>predictions.json</code>\n"
             "        is stale.<br><br>\n"
             "        <span style=\"color:var(--txd)\">Trigger it manually: GitHub → Actions → "
             "Daily Stock Analysis → Run workflow.</span>\n"
             "      </div>\n"
             "    </div></div>`;\n"
             "    return;\n"
             "  }")

    if "rules engine has not produced a brief yet" in s:
        print("• [6/6] renderAI empty branch already patched")
    elif old_f_new in s:
        s = s.replace(old_f_new, new_f, 1)
        # Also strip the llmData variable if present
        s = s.replace("  const llmData = llm || (data && data.llm_analysis);\n", "")
        changes += 1
        print("✓ [6/6] renderAI() legacy Gemini branch dropped")
    else:
        # Case B: very old version where renderAI only handled Gemini
        # Replace the whole function to read from data.daily_brief.
        ai_pat = re.compile(r"function\s+renderAI\s*\(\s*\)\s*\{", re.MULTILINE)
        m = ai_pat.search(s)
        if m:
            start = m.start()
            i = m.end()
            depth = 1
            while i < len(s) and depth > 0:
                if s[i] == "{":
                    depth += 1
                elif s[i] == "}":
                    depth -= 1
                i += 1
            if depth == 0:
                new_fn = _render_ai_function()
                s = s[:start] + new_fn + s[i:]
                changes += 1
                print("✓ [6/6] renderAI() fully rewritten for rule-based daily_brief")
            else:
                print("⚠ [6/6] could not balance renderAI() braces")
        else:
            print("⚠ [6/6] renderAI() not found")

    if changes == 0:
        print("\nNo changes needed — file already patched.")
        return 0

    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        backup.write_text(original, encoding="utf-8")
        print(f"\n✓ backup → {backup}")
    path.write_text(s, encoding="utf-8")
    print(f"✓ wrote {path} ({changes} change{'s' if changes != 1 else ''})")
    return 0


def _render_ai_function() -> str:
    """Minimal rule-based renderAI used only when the original is the very
    old Gemini-only version and needs a full rewrite."""
    return r"""function renderAI() {
  const el = document.getElementById('aiBriefingContent');
  if (!el) return;
  const brief = (data && data.daily_brief) || null;

  if (!brief) {
    el.innerHTML = `<div class="db-wrap"><div class="db-card">
      <div class="db-headline">End-of-day brief not ready</div>
      <div style="font-family:var(--sans);font-size:12px;color:var(--tx2);line-height:1.7;padding:8px 0">
        The daily rules engine has not produced a brief yet. This usually means
        the 4:15 PM IST workflow hasn't run today, or <code>predictions.json</code>
        is stale.<br><br>
        <span style="color:var(--txd)">Trigger it manually: GitHub → Actions → Daily Stock Analysis → Run workflow.</span>
      </div>
    </div></div>`;
    return;
  }

  const regime = brief.regime || {};
  const pulse = brief.market_pulse || {};
  const regLabel = regime.label || 'UNKNOWN';

  const narr = (brief.narrative || [])
    .map(b => `<div style="font-family:var(--sans);font-size:12px;color:var(--tx2);line-height:1.75;padding-left:14px;position:relative;margin-bottom:5px"><span style="position:absolute;left:0;color:var(--grn)">▸</span>${b}</div>`)
    .join('');

  const conv = (brief.conviction_board || []).map(c => `
    <div style="background:var(--bg);border:1px solid var(--bdr);padding:12px 14px;margin-bottom:8px">
      <div style="display:flex;justify-content:space-between;margin-bottom:4px">
        <strong style="color:var(--wht)">${c.symbol || '—'}</strong>
        <span style="color:var(--grn);font-size:11px">${c.score || 0}/100 · ${c.verdict || c.action || ''}</span>
      </div>
      <div style="font-size:11px;color:var(--tx2)">${c.sector || ''} · ${(c.reason || '').slice(0, 120)}</div>
      ${c.narrative ? `<div style="font-family:var(--serif);font-size:12px;color:var(--tx1);line-height:1.7;font-style:italic;margin-top:8px;padding-top:8px;border-top:1px solid var(--bdr)">${c.narrative}</div>` : ''}
    </div>`).join('');

  el.innerHTML = `<div class="db-wrap"><div class="db-card">
    <div class="db-headline">${brief.headline || 'End-of-day evaluation'}</div>
    <div class="db-chips">
      <span class="db-chip">REGIME ${regLabel}</span>
      <span class="db-chip">Breadth ${regime.breadth_pct || 0}%</span>
      <span class="db-chip">Analyzed ${pulse.total_analyzed || 0}</span>
    </div>
    ${brief.key_insight ? `<div class="db-insight">${brief.key_insight}</div>` : ''}
    ${narr}
  </div>
  ${conv ? `<div class="db-card"><div class="db-stitle">🏆 Conviction Board</div>${conv}</div>` : ''}
  </div>`;
}
"""


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 1
    return patch(Path(sys.argv[1]))


if __name__ == "__main__":
    sys.exit(main())
