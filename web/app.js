/* digestr playground — vanilla JS front end over the WebAssembly engine.
 *
 * The engine exposes a global `digestr` object (see cmd/digestr-wasm). It holds
 * one live Digester per session id; every mutating call (newSession, addEvent,
 * advanceClock, flush, reset) returns the whole fresh state, so the page
 * re-renders from one value. There is no network and no framework.
 *
 * digestr owns no timer — the caller supplies `now`. Here the clock is driven by
 * the draggable handle on the timeline and by the scenario replays; instants
 * cross the boundary as seconds from the session base. The clock is monotonic,
 * mirroring digestr's own forward clamp: dragging `now` left never rewinds it.
 */

(function () {
  "use strict";

  const NS = "http://www.w3.org/2000/svg";
  const SID = "main"; // the single live session this page drives

  // Categorical palette for recipients — legible on light and dark backgrounds.
  const PALETTE = ["#4c6ef5", "#f2a63b", "#37b679", "#e06c9f", "#8a63d2", "#2bb3c0", "#d1495b", "#6d8b3a"];

  // Notification bodies cycled per recipient so chips read like real updates.
  const BODIES = [
    "liked your post", "left a comment", "started following you", "mentioned you",
    "shared your post", "replied to your comment", "sent you a message", "reacted with 🎉",
  ];
  const KINDS = ["social", "social", "social", "mention", "share", "reply", "message", "reaction"];

  let C = null; // the digestr engine (window.digestr)

  const state = {
    strategy: "fixed",
    windowSec: 30,
    maxSize: 0,
    lookbackSec: 120,
    dedup: false,
    roster: ["amy", "ben", "carol"],
    fireCount: {}, // per-recipient counter, to vary the body
    last: null, // last engine state
    range: { lo: 0, hi: 45 },
    replayGen: 0,
    dragging: false,
  };

  const colorMap = new Map();
  function colorOf(key) {
    if (!colorMap.has(key)) colorMap.set(key, PALETTE[colorMap.size % PALETTE.length]);
    return colorMap.get(key);
  }

  const seenChips = new Set();
  const seenBatches = new Set();

  // ---- tiny helpers --------------------------------------------------------

  function $(id) { return document.getElementById(id); }
  function el(tag, attrs) {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  }
  function svgText(x, y, cls, text, extra) {
    const a = Object.assign({ x, y }, extra || {});
    if (cls) a.class = cls;
    const t = el("text", a);
    t.textContent = text;
    return t;
  }
  function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }
  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
  function fmtT(sec) {
    if (sec == null || !isFinite(sec)) return "—";
    const n = Math.round(sec * 10) / 10;
    return "T+" + (Number.isInteger(n) ? n : n.toFixed(1)) + "s";
  }

  function showErr(id, resp) {
    const b = $(id);
    if (!b) return false;
    if (resp && resp.error) { b.textContent = resp.error; b.hidden = false; return true; }
    b.hidden = true;
    return false;
  }

  // =========================================================================
  //  ENGINE CALLS
  // =========================================================================

  function cfg() {
    return {
      sessionId: SID,
      strategy: state.strategy,
      windowSec: Number(state.windowSec) || 0,
      maxSize: Number(state.maxSize) || 0,
      lookbackSec: Number(state.lookbackSec) || 0,
      dedup: !!state.dedup,
    };
  }

  // newSession rebuilds the engine's digester from the current config, resetting
  // the whole board. A build failure (a bad window, a zero look-back under
  // backoff) is a top-level {error}; the old board is left untouched.
  function newSession() {
    const resp = C.newSession(cfg());
    if (resp && resp.error && !Array.isArray(resp.recipients)) {
      showErr("config-err", resp);
      return;
    }
    seenChips.clear();
    seenBatches.clear();
    render(resp);
  }

  function fire(recipient, opts) {
    opts = opts || {};
    if (!recipient) return;
    const n = state.fireCount[recipient] || 0;
    state.fireCount[recipient] = n + 1;
    const body = opts.body != null ? opts.body : BODIES[n % BODIES.length];
    const kind = opts.kind != null ? opts.kind : KINDS[n % KINDS.length];
    const req = { sessionId: SID, recipient, kind, body, id: body };
    if (opts.atSec != null) req.atSec = opts.atSec;
    render(C.addEvent(req));
  }

  let rafPending = false, rafTarget = 0;
  function advanceClockThrottled(sec) {
    rafTarget = sec;
    if (rafPending) return;
    rafPending = true;
    requestAnimationFrame(() => {
      rafPending = false;
      render(C.advanceClock({ sessionId: SID, atSec: rafTarget }));
    });
  }
  function advanceClock(sec) { render(C.advanceClock({ sessionId: SID, atSec: sec })); }
  function flushDue() { render(C.flush({ sessionId: SID })); }
  function resetBoard() {
    seenChips.clear();
    seenBatches.clear();
    render(C.reset({ sessionId: SID }));
  }

  // =========================================================================
  //  RENDER
  // =========================================================================

  function render(resp) {
    if (resp && resp.error && !Array.isArray(resp.recipients)) {
      showErr("board-err", resp);
      return;
    }
    showErr("config-err", null);
    state.last = resp;
    // An embedded {error} rides beside a valid board (a cap hit, a missing name).
    showErr("board-err", resp && resp.error ? resp : null);
    if (!state.dragging) computeRange();
    renderTimeline();
    renderCounters();
    renderLanes();
    renderBatches();
  }

  // computeRange sizes the visible seconds window to cover the clock, every open
  // window's deadline, and every emitted batch, plus a pad so the clock can be
  // dragged past the last deadline. Not called mid-drag, so the axis never
  // shifts under the cursor.
  function computeRange() {
    const s = state.last;
    let hi = state.windowSec || 30;
    if (s) {
      hi = Math.max(hi, s.clockSec || 0);
      for (const r of s.recipients || []) {
        if (r.hasWindow) hi = Math.max(hi, r.dueSec);
      }
      for (const b of s.batches || []) hi = Math.max(hi, b.flushedSec, b.dueSec);
    }
    // The backoff horizon (lastSend + lookback) can sit far in the future; it is
    // a secondary marker, so it never stretches the axis — off-range markers are
    // simply not drawn.
    hi += Math.max((state.windowSec || 30) * 0.5, 8);
    state.range = { lo: 0, hi: Math.max(hi, 12) };
  }

  // ---- timeline ------------------------------------------------------------

  const VBW = 960, PLOT_L = 92, PLOT_R = 936;
  const ROW_TOP = 40, ROW_BOT = 168, AXIS_Y = 176;

  function xOf(sec) {
    const R = state.range;
    return PLOT_L + ((sec - R.lo) / (R.hi - R.lo)) * (PLOT_R - PLOT_L);
  }
  function inRange(sec) { return sec >= state.range.lo && sec <= state.range.hi; }
  function clientXToSec(clientX) {
    const svg = $("timeline-svg");
    const rect = svg.getBoundingClientRect();
    const vx = ((clientX - rect.left) / rect.width) * VBW;
    let sec = state.range.lo + ((vx - PLOT_L) / (PLOT_R - PLOT_L)) * (state.range.hi - state.range.lo);
    sec = Math.round(sec); // snap to whole seconds
    return Math.max(0, Math.min(state.range.hi, sec));
  }

  function renderTimeline() {
    const svg = $("timeline-svg");
    clear(svg);
    const s = state.last;
    const recips = (s && s.recipients) || [];

    // x-axis ticks.
    const R = state.range;
    svg.appendChild(el("line", { x1: PLOT_L, y1: AXIS_Y, x2: PLOT_R, y2: AXIS_Y, class: "tl-axis" }));
    const nTicks = 6;
    for (let i = 0; i <= nTicks; i++) {
      const sec = R.lo + ((R.hi - R.lo) / nTicks) * i;
      const x = xOf(sec);
      svg.appendChild(el("line", { x1: x, y1: ROW_TOP - 6, x2: x, y2: AXIS_Y, class: "tl-grid" }));
      svg.appendChild(svgText(x, AXIS_Y + 15, "tl-tick", fmtT(sec), { "text-anchor": "middle" }));
    }

    // recipient rows.
    const rows = recips.length || 1;
    const avail = ROW_BOT - ROW_TOP;
    const rowH = Math.min(34, avail / rows);
    if (recips.length === 0) {
      svg.appendChild(svgText((PLOT_L + PLOT_R) / 2, (ROW_TOP + ROW_BOT) / 2, "tl-empty", "Fire an event to open a window", { "text-anchor": "middle" }));
    }
    recips.forEach((r, i) => {
      const yMid = ROW_TOP + rowH * i + rowH / 2;
      const col = colorOf(r.key);

      // lane baseline + label.
      svg.appendChild(el("line", { x1: PLOT_L, y1: yMid, x2: PLOT_R, y2: yMid, class: "tl-lane" }));
      svg.appendChild(el("circle", { cx: PLOT_L - 78, cy: yMid, r: 4, fill: col }));
      svg.appendChild(svgText(PLOT_L - 68, yMid + 4, "tl-rowlabel", r.key, { "text-anchor": "start" }));

      // backoff horizon marker (lastSend + lookback) and last-send dot.
      if (r.lastSendSec != null && inRange(r.lastSendSec)) {
        svg.appendChild(el("circle", { cx: xOf(r.lastSendSec), cy: yMid, r: 3, class: "tl-send" }));
      }
      if (r.horizonSec != null && inRange(r.horizonSec)) {
        const hx = xOf(r.horizonSec);
        svg.appendChild(el("line", { x1: hx, y1: yMid - 9, x2: hx, y2: yMid + 9, class: "tl-horizon" }));
      }

      // emitted batch markers on this row.
      for (const b of (s.batches || [])) {
        if (b.key !== r.key) continue;
        const bx = xOf(b.flushedSec);
        const cls = b.reason === "SizeReached" ? "size" : b.reason === "Passthrough" ? "pass" : "time";
        svg.appendChild(el("path", { d: `M${bx},${yMid - 8} L${bx + 6},${yMid} L${bx},${yMid + 8} L${bx - 6},${yMid} Z`, class: "tl-batch " + cls }));
      }

      // open window bar with deadline.
      if (r.hasWindow) {
        const x0 = xOf(r.openedSec), x1 = xOf(r.dueSec);
        const barY = yMid - rowH * 0.32, barH = rowH * 0.64;
        svg.appendChild(el("rect", {
          x: Math.min(x0, x1), y: barY, width: Math.max(3, Math.abs(x1 - x0)), height: barH, rx: 4,
          class: "tl-window" + (r.overdue ? " overdue" : ""), style: `fill:${col}`,
        }));
        // deadline flag + a count label just past it (kept off the translucent
        // bar so it reads on both themes).
        svg.appendChild(el("line", { x1: x1, y1: barY - 3, x2: x1, y2: barY + barH + 3, class: "tl-due" + (r.overdue ? " overdue" : "") }));
        svg.appendChild(svgText(x1 + 6, yMid + 4, "tl-count" + (r.overdue ? " overdue" : ""), String(r.size) + (r.overdue ? " · due" : ""), { "text-anchor": "start" }));
      }
    });

    // now handle (draggable), drawn last so it sits on top.
    const clockSec = s ? s.clockSec : 0;
    const nx = xOf(clockSec);
    svg.appendChild(el("line", { x1: nx, y1: ROW_TOP - 20, x2: nx, y2: AXIS_Y, class: "tl-now" }));
    svg.appendChild(el("circle", { cx: nx, cy: ROW_TOP - 20, r: 7, class: "tl-now-grip" }));
    drawFlag(svg, nx, ROW_TOP - 26, "now " + fmtT(clockSec));
  }

  function drawFlag(svg, x, y, label) {
    const w = 14 + label.length * 6.0;
    let fx = x - w / 2;
    fx = Math.max(PLOT_L, Math.min(PLOT_R - w, fx));
    svg.appendChild(el("rect", { x: fx, y: y - 12, width: w, height: 17, rx: 4, class: "tl-now-pill" }));
    svg.appendChild(svgText(fx + w / 2, y, "tl-now-text", label, { "text-anchor": "middle" }));
  }

  function wireDrag() {
    const svg = $("timeline-svg");
    const move = (e) => { if (!state.dragging) return; advanceClockThrottled(clientXToSec(e.clientX)); };
    svg.addEventListener("pointerdown", (e) => {
      cancelReplay();
      state.dragging = true;
      try { svg.setPointerCapture(e.pointerId); } catch (_) {}
      advanceClock(clientXToSec(e.clientX));
      e.preventDefault();
    });
    svg.addEventListener("pointermove", move);
    const end = (e) => { state.dragging = false; try { svg.releasePointerCapture(e.pointerId); } catch (_) {} computeRange(); render(state.last); };
    svg.addEventListener("pointerup", end);
    svg.addEventListener("pointercancel", () => { state.dragging = false; });
  }

  // ---- counters ------------------------------------------------------------

  function renderCounters() {
    const host = $("counters");
    clear(host);
    const s = state.last;
    if (!s) return;
    const sends = (s.digestsOut || 0) + (s.passthroughs || 0);
    const tiles = [
      { k: "events in", v: s.eventsIn || 0, cls: "in" },
      { k: "digests out", v: s.digestsOut || 0, cls: "out" },
    ];
    if (s.strategy === "backoff") tiles.push({ k: "sent immediately", v: s.passthroughs || 0, cls: "pass" });
    tiles.push({ k: "batching ratio", v: `${s.eventsIn || 0} → ${sends}`, cls: "ratio", small: true });
    for (const t of tiles) {
      const c = document.createElement("div");
      c.className = "counter " + t.cls;
      c.innerHTML = `<div class="c-val${t.small ? " small" : ""}">${escapeHtml(String(t.v))}</div><div class="c-key">${escapeHtml(t.k)}</div>`;
      host.appendChild(c);
    }
  }

  // ---- in lanes ------------------------------------------------------------

  function chipEl(ev) {
    const chip = document.createElement("span");
    let cls = "chip";
    if (!seenChips.has(ev.seq)) { cls += " enter"; seenChips.add(ev.seq); }
    chip.className = cls;
    chip.style.borderColor = colorOf(ev.recipient);
    chip.innerHTML = `<span class="chip-kind">${escapeHtml(ev.kind || "event")}</span>${escapeHtml(ev.body || "")}`;
    return chip;
  }

  function renderLanes() {
    const host = $("in-lanes");
    clear(host);
    const s = state.last;
    const open = ((s && s.recipients) || []).filter((r) => r.hasWindow);
    if (open.length === 0) {
      host.innerHTML = `<p class="empty">No open windows. Fire an event to open one.</p>`;
      return;
    }
    for (const r of open) {
      const col = colorOf(r.key);
      const lane = document.createElement("div");
      lane.className = "lane" + (r.overdue ? " overdue" : "");
      lane.style.setProperty("--lane", col);

      const head = document.createElement("div");
      head.className = "lane-head";
      let sizeBadge;
      if (state.maxSize > 0) {
        sizeBadge = `<span class="lane-size">${r.size} / ${state.maxSize}</span>`;
      } else {
        sizeBadge = `<span class="lane-size">${r.size}</span>`;
      }
      head.innerHTML = `<span class="dot" style="background:${col}"></span><span class="lane-name">${escapeHtml(r.key)}</span>${sizeBadge}`;
      lane.appendChild(head);

      if (state.maxSize > 0) {
        const track = document.createElement("div");
        track.className = "lane-track";
        const fill = document.createElement("div");
        fill.className = "lane-fill";
        fill.style.width = Math.min(100, (r.size / state.maxSize) * 100) + "%";
        fill.style.background = col;
        track.appendChild(fill);
        lane.appendChild(track);
      }

      const chips = document.createElement("div");
      chips.className = "chips";
      for (const ev of r.events) chips.appendChild(chipEl(ev));
      lane.appendChild(chips);

      const foot = document.createElement("div");
      foot.className = "lane-foot";
      let ftxt = `due ${fmtT(r.dueSec)}`;
      if (r.overdue) ftxt += ` <span class="overdue-chip">overdue · awaiting Due</span>`;
      foot.innerHTML = ftxt;
      lane.appendChild(foot);

      host.appendChild(lane);
    }
  }

  // ---- out batches ---------------------------------------------------------

  const REASON_META = {
    Elapsed: { label: "time · elapsed", cls: "time" },
    SizeReached: { label: "size · reached", cls: "size" },
    Passthrough: { label: "sent immediately", cls: "pass" },
  };

  function renderBatches() {
    const host = $("out-batches");
    clear(host);
    const s = state.last;
    const batches = (s && s.batches) || [];
    if (batches.length === 0) {
      host.innerHTML = `<p class="empty">No batches yet. Flush a window by size, by the clock, or by backoff.</p>`;
      return;
    }
    // newest first.
    for (let i = batches.length - 1; i >= 0; i--) {
      const b = batches[i];
      const meta = REASON_META[b.reason] || { label: b.reason, cls: "time" };
      const col = colorOf(b.key);
      const card = document.createElement("div");
      let cls = "batch " + meta.cls;
      if (!seenBatches.has(b.seq)) { cls += " enter"; seenBatches.add(b.seq); }
      card.className = cls;
      card.style.setProperty("--lane", col);

      const head = document.createElement("div");
      head.className = "batch-head";
      head.innerHTML =
        `<span class="reason ${meta.cls}">${escapeHtml(meta.label)}</span>` +
        `<span class="batch-key"><span class="dot" style="background:${col}"></span>${escapeHtml(b.key)}</span>` +
        `<span class="batch-meta">${escapeHtml(String(b.events.length))} ${b.events.length === 1 ? "event" : "events"} · ${fmtT(b.flushedSec)}</span>`;
      card.appendChild(head);

      const chips = document.createElement("div");
      chips.className = "chips";
      for (const ev of b.events) {
        const chip = document.createElement("span");
        chip.className = "chip static";
        chip.style.borderColor = col;
        chip.innerHTML = `<span class="chip-kind">${escapeHtml(ev.kind || "event")}</span>${escapeHtml(ev.body || "")}`;
        chips.appendChild(chip);
      }
      card.appendChild(chips);
      host.appendChild(card);
    }
  }

  // =========================================================================
  //  FIRE CONTROLS + CONFIG
  // =========================================================================

  function buildFireButtons() {
    const host = $("fire-buttons");
    clear(host);
    for (const r of state.roster) {
      const b = document.createElement("button");
      b.className = "fire-btn";
      b.type = "button";
      b.style.setProperty("--c", colorOf(r));
      b.innerHTML = `<span class="dot" style="background:${colorOf(r)}"></span>${escapeHtml(r)}`;
      b.addEventListener("click", () => { cancelReplay(); fire(r); });
      host.appendChild(b);
    }
  }

  function addRosterRecipient(name) {
    name = String(name || "").trim();
    if (!name || state.roster.indexOf(name) >= 0) return name;
    if (state.roster.length >= 8) state.roster.shift();
    state.roster.push(name);
    buildFireButtons();
    return name;
  }

  function setStrategy(strat) {
    state.strategy = strat;
    $("strat-fixed").classList.toggle("active", strat === "fixed");
    $("strat-backoff").classList.toggle("active", strat === "backoff");
    $("cfg-lookback-wrap").hidden = strat !== "backoff";
  }

  function wireConfig() {
    $("strat-fixed").addEventListener("click", () => { cancelReplay(); setStrategy("fixed"); newSession(); });
    $("strat-backoff").addEventListener("click", () => { cancelReplay(); setStrategy("backoff"); newSession(); });
    $("cfg-window").addEventListener("change", (e) => { cancelReplay(); state.windowSec = Number(e.target.value); newSession(); });
    $("cfg-maxsize").addEventListener("change", (e) => { cancelReplay(); state.maxSize = Number(e.target.value); newSession(); });
    $("cfg-lookback").addEventListener("change", (e) => { cancelReplay(); state.lookbackSec = Number(e.target.value); newSession(); });
    $("cfg-dedup").addEventListener("change", (e) => { cancelReplay(); state.dedup = e.target.checked; newSession(); });
    $("btn-reset").addEventListener("click", () => { cancelReplay(); resetBoard(); });
    $("btn-flush").addEventListener("click", () => { cancelReplay(); flushDue(); });

    $("fire-custom-btn").addEventListener("click", fireCustom);
    $("fire-custom").addEventListener("keydown", (e) => { if (e.key === "Enter") fireCustom(); });
  }

  function fireCustom() {
    const inp = $("fire-custom");
    const name = addRosterRecipient(inp.value);
    if (!name) return;
    inp.value = "";
    cancelReplay();
    fire(name);
  }

  function syncConfigControls() {
    setStrategy(state.strategy);
    $("cfg-window").value = String(state.windowSec);
    $("cfg-maxsize").value = String(state.maxSize);
    $("cfg-lookback").value = String(state.lookbackSec);
    $("cfg-dedup").checked = !!state.dedup;
  }

  // =========================================================================
  //  SCENARIOS
  // =========================================================================

  function buildScenarioButtons() {
    const host = $("scenario-buttons");
    clear(host);
    const list = C.scenarios || [];
    for (const sc of list) {
      const b = document.createElement("button");
      b.className = "btn";
      b.type = "button";
      b.textContent = sc.title;
      b.dataset.name = sc.name;
      b.title = sc.blurb || "";
      b.addEventListener("click", () => loadScenario(sc.name, b));
      host.appendChild(b);
    }
  }

  function cancelReplay() { state.replayGen++; }

  async function loadScenario(name, btn) {
    const sc = C.loadScenario(name);
    if (!sc || sc.error) return;
    cancelReplay();
    const gen = state.replayGen;

    document.querySelectorAll("#scenario-buttons .btn").forEach((b) => b.classList.toggle("active", b === btn || b.dataset.name === name));
    const note = $("scenario-note");
    note.innerHTML = `<b>${escapeHtml(sc.title)}.</b> ${escapeHtml(sc.note)}`;
    note.hidden = false;

    // apply config and reset the board.
    const c = sc.config || {};
    state.strategy = c.strategy || "fixed";
    state.windowSec = c.windowSec != null ? c.windowSec : 30;
    state.maxSize = c.maxSize != null ? c.maxSize : 0;
    state.lookbackSec = c.lookbackSec != null ? c.lookbackSec : 120;
    state.dedup = !!c.dedup;
    state.fireCount = {};
    syncConfigControls();

    // seed the roster with the recipients the scenario uses, in order.
    for (const step of sc.steps || []) {
      if (step.type === "event" && step.recipient && state.roster.indexOf(step.recipient) < 0) {
        if (state.roster.length >= 8) state.roster.shift();
        state.roster.push(step.recipient);
      }
    }
    buildFireButtons();
    newSession();

    // replay the steps with a cadence so windows visibly fill and flush.
    const steps = sc.steps || [];
    for (const step of steps) {
      if (gen !== state.replayGen) return; // cancelled by another action
      await sleep(560);
      if (gen !== state.replayGen) return;
      if (step.type === "event") {
        fire(step.recipient, { kind: step.kind, body: step.body, atSec: step.atSec });
      } else {
        advanceClock(step.atSec);
      }
    }
  }

  // =========================================================================
  //  WIRING + BOOT
  // =========================================================================

  let started = false;
  function start() {
    if (started) return;
    started = true;
    C = window.digestr;

    buildFireButtons();
    buildScenarioButtons();
    syncConfigControls();
    wireConfig();
    wireDrag();

    $("boot").classList.add("hidden");

    // open on the flagship story.
    const first = document.querySelector('#scenario-buttons .btn[data-name="chatty-backoff"]') ||
      document.querySelector("#scenario-buttons .btn");
    if (first) loadScenario(first.dataset.name, first);
    else newSession();
  }

  function boot() {
    if (!window.Go) { fail("wasm_exec.js failed to load."); return; }
    const go = new Go();
    const url = "digestr.wasm";
    window.__digestrOnReady = () => start();
    const inst = (obj) => { go.run(obj.instance); if (window.__digestrReady) start(); };
    if (WebAssembly.instantiateStreaming) {
      WebAssembly.instantiateStreaming(fetch(url), go.importObject).then(inst).catch(() => fallback(go, url, inst));
    } else {
      fallback(go, url, inst);
    }
  }
  function fallback(go, url, inst) {
    fetch(url).then((r) => r.arrayBuffer()).then((buf) => WebAssembly.instantiate(buf, go.importObject)).then(inst)
      .catch((e) => fail("Could not load the wasm module: " + e));
  }
  function fail(msg) { const b = $("boot"); b.textContent = msg; b.classList.add("error"); }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
