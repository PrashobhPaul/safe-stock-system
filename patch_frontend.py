#!/usr/bin/env python3
"""
patch_frontend.py — surgical idempotent patch for index.html
=============================================================
Three targeted edits to light up the rebuilt backend:

  1. Extend .sig-pill CSS with new verdict styles
     (ACCUMULATE ON DIP / HOLD / BLACKOUT / BOOK PARTIAL / EXIT / DO NOT TRADE)

  2. Rewrite sigCls() to route any verdict string to the correct pill class,
     and make makeCard() prefer p.verdict over p.signal when present.

  3. Rewrite renderAI() to read data.daily_brief and render a deterministic
     end-of-day report — never blank, no Gemini dependency. The old fetchBriefing()
     Gemini path remains available as an optional "Enhance with Gemini" button.

All edits are idempotent — running twice is safe. A `.bak` is written on
first change.

Usage:
    python patch_frontend.py index.html
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

MARKER = "/* ── SS_VERDICT_PATCH_v1 ── */"


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 1 — Extended pill CSS
# ─────────────────────────────────────────────────────────────────────────────

NEW_PILL_CSS = """
    """ + MARKER + """
    .sig-accum  { background: rgba(255,179,0,.10); border: 1px solid rgba(255,179,0,.45); color: #ffcc66; }
    .sig-hold   { background: rgba(160,160,160,.08); border: 1px solid rgba(200,200,200,.35); color: #c8c8c8; }
    .sig-black  { background: rgba(171,71,188,.10); border: 1px solid rgba(171,71,188,.5); color: #d59bff; }
    .sig-trim   { background: rgba(255,120,40,.10); border: 1px solid rgba(255,120,40,.5); color: #ffa566; }
    .sig-exit   { background: rgba(255,68,68,.10); border: 1px solid rgba(255,68,68,.5); color: #ff9090; }
    .sig-avoid  { background: rgba(255,68,68,.06); border: 1px solid rgba(255,68,68,.35); color: #ff7878; }
    .sig-dnt    { background: rgba(120,120,120,.08); border: 1px dashed rgba(160,160,160,.45); color: #a8a8a8; }
    /* ── Daily brief panel ── */
    .db-wrap      { padding: 14px 16px; }
    .db-card      { background: var(--bgc); border: 1px solid var(--bdr); padding: 16px 16px 14px; margin-bottom: 14px; position: relative; }
    .db-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: linear-gradient(90deg, var(--grn), var(--blu), var(--pur)); }
    .db-headline  { font-family: var(--serif); font-size: 18px; font-weight: 600; color: var(--wht); line-height: 1.35; margin-bottom: 8px; }
    .db-regime    { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
    .db-chip      { font-family: var(--mono); font-size: 10px; letter-spacing: .08em; padding: 4px 9px; border: 1px solid var(--bdrb); color: var(--tx2); text-transform: uppercase; }
    .db-chip.bull { color: var(--grn); border-color: var(--grnd); }
    .db-chip.bear { color: #ff9090; border-color: rgba(255,68,68,.4); }
    .db-chip.side { color: var(--amb); border-color: rgba(255,179,0,.4); }
    .db-insight   { background: var(--bg); border-left: 3px solid var(--grn); padding: 12px 14px; font-family: var(--sans); font-size: 13px; color: var(--tx1); line-height: 1.75; margin-bottom: 14px; }
    .db-pulse     { display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; margin-bottom: 14px; }
    .db-pulse-box { background: var(--bg); border: 1px solid var(--bdr); padding: 10px 12px; text-align: center; }
    .db-pulse-n   { font-family: var(--mono); font-size: 20px; font-weight: 700; color: var(--wht); line-height: 1.1; }
    .db-pulse-n.g { color: var(--grn); }
    .db-pulse-n.r { color: #ff9090; }
    .db-pulse-n.a { color: var(--amb); }
    .db-pulse-l   { font-size: 9px; letter-spacing: .1em; text-transform: uppercase; color: var(--txd); margin-top: 4px; font-weight: 600; }
    .db-section   { margin-bottom: 16px; }
    .db-stitle    { font-size: 10px; letter-spacing: .12em; text-transform: uppercase; color: var(--txd); margin-bottom: 8px; font-weight: 700; padding-bottom: 4px; border-bottom: 1px solid var(--bdr); }
    .db-row       { display: grid; grid-template-columns: auto 1fr auto; gap: 10px; align-items: center; padding: 8px 10px; background: var(--bg); border: 1px solid var(--bdr); margin-bottom: 6px; }
    .db-row-sym   { font-family: var(--mono); font-size: 12px; font-weight: 700; color: var(--wht); min-width: 70px; }
    .db-row-body  { font-family: var(--sans); font-size: 11px; color: var(--tx2); line-height: 1.45; }
    .db-row-meta  { font-family: var(--mono); font-size: 10px; color: var(--txd); text-align: right; white-space: nowrap; }
    .db-row-meta.g { color: var(--grn); }
    .db-row-meta.r { color: #ff9090; }
    .db-plan-pill { display: inline-block; font-family: var(--mono); font-size: 9px; font-weight: 700; padding: 2px 7px; letter-spacing: .08em; margin-right: 6px; }
    .db-plan-pill.p1 { background: rgba(255,68,68,.15); color: #ff9090; border: 1px solid rgba(255,68,68,.4); }
    .db-plan-pill.p2 { background: rgba(255,120,40,.12); color: #ffa566; border: 1px solid rgba(255,120,40,.4); }
    .db-plan-pill.p3 { background: rgba(171,71,188,.12); color: #d59bff; border: 1px solid rgba(171,71,188,.4); }
    .db-plan-pill.p4 { background: rgba(0,230,118,.12); color: var(--grn); border: 1px solid var(--grnd); }
    .db-narr      { font-family: var(--sans); font-size: 12px; color: var(--tx2); line-height: 1.75; padding-left: 14px; position: relative; margin-bottom: 6px; }
    .db-narr::before { content: '▸'; position: absolute; left: 0; color: var(--grn); }
    .db-sec-bar   { display: grid; grid-template-columns: 120px 1fr auto; gap: 10px; align-items: center; padding: 6px 0; font-family: var(--mono); font-size: 11px; }
    .db-sec-bar-name { color: var(--tx2); }
    .db-sec-bar-t { height: 5px; background: var(--bg); border: 1px solid var(--bdr); position: relative; }
    .db-sec-bar-f { height: 100%; background: linear-gradient(90deg, var(--grnd), var(--grn)); }
    .db-sec-bar-v { color: var(--txd); font-size: 10px; min-width: 30px; text-align: right; }
    .db-sec-bar.dn .db-sec-bar-f { background: linear-gradient(90deg, rgba(255,68,68,.4), #ff9090); }
    .db-empty-sec { font-size: 11px; color: var(--txd); font-style: italic; padding: 6px 0; }
    .db-footer    { font-family: var(--sans); font-size: 10px; color: var(--txd); margin-top: 8px; padding-top: 10px; border-top: 1px solid var(--bdr); line-height: 1.5; }
    .db-enhance-btn { background: var(--bgc); border: 1px solid var(--bdrb); color: var(--grn); font-family: var(--mono); font-size: 10px; letter-spacing: .08em; padding: 7px 14px; cursor: pointer; text-transform: uppercase; margin-top: 10px; }
    .db-enhance-btn:hover { background: rgba(0,230,118,.06); }
"""


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 2 — sigCls() rewrite (handles all verdict strings)
# ─────────────────────────────────────────────────────────────────────────────

NEW_SIG_CLS = """const sigCls= s => {
  if (!s) return 'sig-hold';
  const v = String(s).toUpperCase();
  if (v === 'STRONG BUY') return 'sig-sb';
  if (v === 'BUY') return 'sig-b';
  if (v === 'ACCUMULATE ON DIP' || v === 'ACCUMULATE') return 'sig-accum';
  if (v === 'HOLD' || v === 'WATCH') return 'sig-hold';
  if (v === 'EARNINGS BLACKOUT' || v === 'BLACKOUT') return 'sig-black';
  if (v === 'BOOK PARTIAL' || v === 'TRIM') return 'sig-trim';
  if (v === 'EXIT') return 'sig-exit';
  if (v === 'DO NOT TRADE' || v === 'DNT') return 'sig-dnt';
  if (v === 'AVOID') return 'sig-avoid';
  return 'sig-hold';
};"""

OLD_SIG_CLS = "const sigCls= s => s==='STRONG BUY'?'sig-sb':'sig-b';"


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 3 — makeCard() verdict preference
# ─────────────────────────────────────────────────────────────────────────────
# We replace the single line that renders the pill so it prefers p.verdict.

OLD_PILL_RENDER = '<div class="${sigCls(p.signal)} sig-pill"><span class="sdot"></span>${p.signal}</div>'
NEW_PILL_RENDER = '<div class="${sigCls(p.verdict||p.signal)} sig-pill"><span class="sdot"></span>${p.verdict||p.signal}</div>'


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 3b — renderAll() should always call renderAI() so the deterministic
#            brief shows on initial render even when no llm_analysis exists.
# ─────────────────────────────────────────────────────────────────────────────

OLD_RENDER_ALL_LINE = "if (data.llm_analysis) renderAI();"
NEW_RENDER_ALL_LINE = "if (data.daily_brief || data.llm_analysis) renderAI();"


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 3c — switchTab('ai') should NOT fire a Gemini fetch when we already
#            have a deterministic brief in hand. Enhance-with-Gemini becomes
#            an explicit user action via the db-enhance-btn button.
# ─────────────────────────────────────────────────────────────────────────────

OLD_SWITCHTAB_AI = "if (tab==='ai' && !llm) fetchBriefing();"
NEW_SWITCHTAB_AI = "if (tab==='ai') { if (data && data.daily_brief) { renderAI(); } else if (!llm) { fetchBriefing(); } }"


# ─────────────────────────────────────────────────────────────────────────────
# PATCH 4 — renderAI() full rewrite using data.daily_brief
# ─────────────────────────────────────────────────────────────────────────────

NEW_RENDER_AI = r"""function renderAI() {
  const el = document.getElementById('aiBriefingContent');
  if (!el) return;

  // Primary data source: deterministic rule-based EOD brief in predictions.json
  const brief = (data && data.daily_brief) || null;
  const llmData = llm || (data && data.llm_analysis);

  // Last-resort empty state if even the brief is missing
  if (!brief && !llmData) {
    el.innerHTML = `<div class="db-wrap"><div class="db-card">
      <div class="db-headline">End-of-day report not ready</div>
      <div style="font-family:var(--sans);font-size:12px;color:var(--tx2);line-height:1.7;padding:8px 0">
        The daily analysis has not produced a brief yet. This usually means the
        4:15 PM IST workflow hasn't run, or <code>predictions.json</code> is stale.<br><br>
        <span style="color:var(--txd)">Check GitHub → Actions → Daily Portfolio Analysis.</span>
      </div>
    </div></div>`;
    return;
  }

  if (!brief) {
    // No rule-based brief but we have a legacy llm payload — render it the old way
    return renderAI_legacy(llmData);
  }

  // Regime chip class
  const regLabel = (brief.regime && brief.regime.label) || 'UNKNOWN';
  const regClass = regLabel === 'BULL' ? 'bull' : regLabel === 'BEAR' ? 'bear'
                   : regLabel === 'SIDEWAYS' ? 'side' : '';

  const pulse = brief.market_pulse || {};

  // Conviction board rows
  const convRows = (brief.conviction_board || []).map(c => `
    <div class="db-row">
      <div class="db-row-sym">${c.symbol || '—'}</div>
      <div class="db-row-body">
        <strong style="color:var(--wht)">${c.verdict || c.action || ''}</strong> ·
        ${c.sector || ''} · ${(c.reason || '').slice(0, 110)}
      </div>
      <div class="db-row-meta g">${c.score || 0}/100</div>
    </div>`).join('');

  // Risk watchlist
  const riskRows = (brief.risk_watchlist || []).map(r => `
    <div class="db-row">
      <div class="db-row-sym">${r.symbol || r.name || '—'}</div>
      <div class="db-row-body">
        <strong style="color:#ff9090">${r.verdict || r.action || ''}</strong> ·
        ${(r.flags || []).join(' · ')}
      </div>
      <div class="db-row-meta r">${r.action || ''}</div>
    </div>`).join('');

  // Breakout watch
  const breakRows = (brief.breakout_watch || []).map(b => `
    <div class="db-row">
      <div class="db-row-sym">${b.symbol || '—'}</div>
      <div class="db-row-body">
        ${b.pct_from_52h}% from 52-week high${b.in_base ? ' · in tight base' : ''}
      </div>
      <div class="db-row-meta g">${b.score || 0}/100</div>
    </div>`).join('');

  // Action plan
  const planRows = (brief.action_plan || []).map(p => `
    <div class="db-row">
      <div class="db-row-sym">${p.symbol || '—'}</div>
      <div class="db-row-body">
        <span class="db-plan-pill p${p.priority || 4}">${p.type || ''}</span>
        ${p.instruction || ''} <span style="color:var(--txd)">· ${(p.reason || '').slice(0, 90)}</span>
      </div>
      <div class="db-row-meta">${p.confidence || ''}</div>
    </div>`).join('');

  // Sector heatmap (top 8)
  const sectors = (brief.sector_heatmap || []).slice(0, 8);
  const maxScore = Math.max(...sectors.map(s => s.avg_score || 0), 80);
  const sectorBars = sectors.map(s => `
    <div class="db-sec-bar ${s.trend === 'down' ? 'dn' : ''}">
      <div class="db-sec-bar-name">${s.sector}</div>
      <div class="db-sec-bar-t"><div class="db-sec-bar-f" style="width:${Math.max(5, (s.avg_score / maxScore * 100))}%"></div></div>
      <div class="db-sec-bar-v">${s.avg_score}</div>
    </div>`).join('') || '<div class="db-empty-sec">No sector data</div>';

  // Narrative bullets
  const narr = (brief.narrative || []).map(b => `<div class="db-narr">${b}</div>`).join('');

  // Movers
  const moversUp = (brief.top_movers_up || []).slice(0, 5).map(m => `
    <div class="db-row">
      <div class="db-row-sym">${m.symbol}</div>
      <div class="db-row-body">${m.name || ''} <span style="color:var(--txd)">· ${m.action || ''}</span></div>
      <div class="db-row-meta g">+${m.change_pct}%</div>
    </div>`).join('');
  const moversDn = (brief.top_movers_down || []).slice(0, 5).map(m => `
    <div class="db-row">
      <div class="db-row-sym">${m.symbol}</div>
      <div class="db-row-body">${m.name || ''} <span style="color:var(--txd)">· ${m.action || ''}</span></div>
      <div class="db-row-meta r">${m.change_pct}%</div>
    </div>`).join('');

  // Enhance button (only if Vercel+Gemini is configured)
  const enhanceBtn = (typeof VERCEL_URL !== 'undefined' && VERCEL_URL)
    ? `<button class="db-enhance-btn" onclick="fetchBriefing()">↻ Enhance with Gemini (optional)</button>`
    : '';

  el.innerHTML = `<div class="db-wrap">
    <!-- Headline card -->
    <div class="db-card">
      <div class="db-headline">${brief.headline || 'End-of-day evaluation'}</div>
      <div class="db-regime">
        <span class="db-chip ${regClass}">${regLabel}</span>
        <span class="db-chip">Breadth ${(brief.regime && brief.regime.breadth_pct) || 0}%</span>
        <span class="db-chip">Momentum ${sign((brief.regime && brief.regime.momentum_pct) || 0)}${(brief.regime && brief.regime.momentum_pct) || 0}%</span>
        <span class="db-chip">ATR ${(brief.regime && brief.regime.volatility) || 0}%</span>
      </div>

      <!-- Market pulse -->
      <div class="db-pulse">
        <div class="db-pulse-box"><div class="db-pulse-n g">${pulse.actionable_buys || 0}</div><div class="db-pulse-l">Actionable Buys</div></div>
        <div class="db-pulse-box"><div class="db-pulse-n">${pulse.holds || 0}</div><div class="db-pulse-l">Holds</div></div>
        <div class="db-pulse-box"><div class="db-pulse-n r">${pulse.risk_alerts || 0}</div><div class="db-pulse-l">Risk Alerts</div></div>
        <div class="db-pulse-box"><div class="db-pulse-n a">${pulse.earnings_blackouts || 0}</div><div class="db-pulse-l">Blackouts</div></div>
        <div class="db-pulse-box"><div class="db-pulse-n">${pulse.total_analyzed || 0}</div><div class="db-pulse-l">Analyzed</div></div>
        <div class="db-pulse-box"><div class="db-pulse-n">${pulse.strong_buys || 0}</div><div class="db-pulse-l">Strong Buys</div></div>
      </div>

      <!-- Key insight -->
      ${brief.key_insight ? `<div class="db-insight">${brief.key_insight}</div>` : ''}

      <!-- Narrative bullets -->
      ${narr ? `<div class="db-section"><div class="db-stitle">▸ Session Read</div>${narr}</div>` : ''}
    </div>

    <!-- Action plan -->
    ${planRows ? `<div class="db-card">
      <div class="db-stitle">📋 Tomorrow's Action Plan (by urgency)</div>
      ${planRows}
    </div>` : ''}

    <!-- Conviction board -->
    ${convRows ? `<div class="db-card">
      <div class="db-stitle">🏆 Conviction Board</div>
      ${convRows}
    </div>` : ''}

    <!-- Breakout watch -->
    ${breakRows ? `<div class="db-card">
      <div class="db-stitle">📈 Breakout Watch — within 3.5% of 52W high</div>
      ${breakRows}
    </div>` : ''}

    <!-- Risk watchlist -->
    ${riskRows ? `<div class="db-card">
      <div class="db-stitle">⚠ Risk Watchlist</div>
      ${riskRows}
    </div>` : ''}

    <!-- Sector heatmap -->
    <div class="db-card">
      <div class="db-stitle">🌐 Sector Heatmap</div>
      ${sectorBars}
    </div>

    <!-- Top movers -->
    ${(moversUp || moversDn) ? `<div class="db-card">
      <div class="db-stitle">📊 Top Movers</div>
      ${moversUp ? `<div style="margin-bottom:10px"><div style="font-size:10px;color:var(--grn);letter-spacing:.1em;margin-bottom:6px">GAINERS</div>${moversUp}</div>` : ''}
      ${moversDn ? `<div><div style="font-size:10px;color:#ff9090;letter-spacing:.1em;margin-bottom:6px">DECLINERS</div>${moversDn}</div>` : ''}
    </div>` : ''}

    <div class="db-footer">
      End-of-day evaluation · rules engine v3 · no LLM · generated ${fmtIST(brief.generated_at || '')}
      ${enhanceBtn}
    </div>
  </div>`;
}

// ── LEGACY: original Gemini-based render, kept as fallback ──
function renderAI_legacy(llmData) {
  const el = document.getElementById('aiBriefingContent');
  if (!llmData) return;
  if (llmData.provider === 'placeholder' || llmData.error) {
    el.innerHTML = `<div class="ai-card">
      <div class="ai-card-title">📊 AI Briefing</div>
      <div style="font-family:var(--sans);font-size:13px;color:var(--tx2);padding:12px 0;line-height:1.8">
        ${llmData.market_summary || 'AI briefing unavailable.'}
      </div>
    </div>`;
    return;
  }
  const sent = (llmData.overall_sentiment || 'NEUTRAL').replace(' ', '_');
  const themes = (llmData.sector_themes || []).map(t => `<span class="ai-tag sec">${t}</span>`).join('');
  const risks = (llmData.risks_to_watch || []).map(r => `<span class="ai-tag risk">⚠ ${r}</span>`).join('');
  const narrs = Object.entries(llmData.stock_narratives || {}).slice(0, 6).map(([sym, txt]) => `
    <div class="ai-narr"><div class="ai-narr-sym">${sym}</div><div class="ai-narr-text">${txt}</div></div>`).join('');
  el.innerHTML = `<div class="ai-card">
    <div class="ai-card-title">📊 Gemini AI Briefing (legacy)</div>
    <div class="ai-meta">
      <span class="ai-model-tag">🤖 ${llmData.provider || 'gemini'}</span>
      <span class="sent-tag sent-${sent}">${sent.replace('_', ' ')}</span>
    </div>
    ${llmData.market_summary ? `<div class="ai-summary">${llmData.market_summary}</div>` : ''}
    ${themes ? `<div class="ai-tags-section"><div class="ai-tag-title">▸ Sector Themes</div>${themes}</div>` : ''}
    ${risks ? `<div class="ai-tags-section"><div class="ai-tag-title">▸ Risks</div>${risks}</div>` : ''}
    ${narrs ? `<div class="ai-tags-section"><div class="ai-tag-title">▸ Stocks</div><div class="ai-narr-grid">${narrs}</div></div>` : ''}
  </div>`;
}"""


# ─────────────────────────────────────────────────────────────────────────────
# Patch logic
# ─────────────────────────────────────────────────────────────────────────────

def patch(path: Path) -> int:
    if not path.exists():
        print(f"✗ {path} not found", file=sys.stderr)
        return 1

    html = path.read_text(encoding="utf-8")
    orig = html
    changes = 0

    # ── PATCH 1: Inject verdict pill CSS + daily brief CSS ──
    if MARKER in html:
        print("• CSS already patched — skipping (idempotent)")
    else:
        # Insert immediately after the last .sig-b rule
        anchor = "    .sig-b  { background: rgba(0,184,90,.08);  border: 1px solid var(--grnd); color: var(--grnd); }"
        if anchor in html:
            html = html.replace(anchor, anchor + NEW_PILL_CSS, 1)
            changes += 1
            print("✓ injected verdict pill CSS + daily brief CSS")
        else:
            print("⚠ could not find .sig-b anchor — skipping CSS injection")

    # ── PATCH 2: sigCls() rewrite ──
    if "sig-accum" in html and "const sigCls" in html and OLD_SIG_CLS not in html:
        print("• sigCls already patched — skipping")
    elif OLD_SIG_CLS in html:
        html = html.replace(OLD_SIG_CLS, NEW_SIG_CLS, 1)
        changes += 1
        print("✓ rewrote sigCls() to handle all verdict labels")
    else:
        print("⚠ sigCls() signature not found — manual edit needed")

    # ── PATCH 3: makeCard() pill render ──
    if NEW_PILL_RENDER in html:
        print("• makeCard pill render already patched — skipping")
    elif OLD_PILL_RENDER in html:
        html = html.replace(OLD_PILL_RENDER, NEW_PILL_RENDER, 1)
        changes += 1
        print("✓ makeCard now prefers p.verdict over p.signal")
    else:
        print("⚠ makeCard pill line not found — manual edit needed")

    # ── PATCH 3b: renderAll() — always call renderAI when brief is present ──
    if NEW_RENDER_ALL_LINE in html:
        print("• renderAll call already patched — skipping")
    elif OLD_RENDER_ALL_LINE in html:
        html = html.replace(OLD_RENDER_ALL_LINE, NEW_RENDER_ALL_LINE, 1)
        changes += 1
        print("✓ renderAll now calls renderAI when daily_brief is present")
    else:
        print("⚠ renderAll conditional not found — manual edit needed")

    # ── PATCH 3c: switchTab('ai') — prefer deterministic brief over Gemini ──
    if NEW_SWITCHTAB_AI in html:
        print("• switchTab ai branch already patched — skipping")
    elif OLD_SWITCHTAB_AI in html:
        html = html.replace(OLD_SWITCHTAB_AI, NEW_SWITCHTAB_AI, 1)
        changes += 1
        print("✓ switchTab('ai') now renders local brief instead of Gemini fetch")
    else:
        print("⚠ switchTab ai branch not found — manual edit needed")

    # ── PATCH 4: renderAI() replacement ──
    # Detect existing function boundaries by their opening and closing braces.
    ai_pat = re.compile(r"function\s+renderAI\s*\(\s*\)\s*\{", re.MULTILINE)
    m = ai_pat.search(html)
    if m and "renderAI_legacy" in html:
        print("• renderAI already patched (has renderAI_legacy) — skipping")
    elif m:
        # Walk braces to find the matching close
        start = m.start()
        i = m.end()
        depth = 1
        while i < len(html) and depth > 0:
            ch = html[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            i += 1
        if depth == 0:
            # Replace entire function body with new one
            html = html[:start] + NEW_RENDER_AI + html[i:]
            changes += 1
            print("✓ replaced renderAI() with deterministic daily-brief renderer")
        else:
            print("⚠ could not balance renderAI braces — skipping")
    else:
        print("⚠ renderAI() not found — skipping")

    if not changes:
        print("\nNo changes applied.")
        return 0

    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        backup.write_text(orig, encoding="utf-8")
        print(f"✓ backup saved to {backup}")
    path.write_text(html, encoding="utf-8")
    print(f"✓ wrote {path} ({changes} change{'s' if changes != 1 else ''})")
    return 0


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 1
    return patch(Path(sys.argv[1]))


if __name__ == "__main__":
    sys.exit(main())
