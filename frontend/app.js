// Reletix LLM Benchmark — frontend logic

const $  = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));

const api = {
  meta:        ()      => fetch("/api/meta").then(r => r.json()),
  envKey:      (p)     => fetch(`/api/env_key?provider=${p}`).then(r => r.json()),
  models:      (p,b)   => fetch(`/api/models?provider=${p}${b?`&base_url=${encodeURIComponent(b)}`:""}`).then(r => r.json()),
  prompts:     ()      => fetch("/api/prompts").then(r => r.json()),
  newPrompt:   (b)     => fetch("/api/prompts", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(b)}).then(r => r.json()),
  delPrompt:   (id)    => fetch(`/api/prompts/${id}`, {method:"DELETE"}).then(r => r.json()),
  datasets:    ()      => fetch("/api/datasets").then(r => r.json()),
  uploadDS:    (file, tpl)  => { const fd = new FormData(); fd.append("file", file); if (tpl) fd.append("image_url_template", tpl); return fetch("/api/datasets", {method:"POST", body: fd}).then(r => r.json()); },
  deleteDS:    (id)    => fetch(`/api/datasets/${id}`, {method:"DELETE"}).then(r => r.json()),
  patchDS:     (id, b) => fetch(`/api/datasets/${id}`, {method:"PATCH", headers:{"Content-Type":"application/json"}, body:JSON.stringify(b)}).then(r => r.json()),
  dsRows:      (id, offset=0, limit=20, q="", onlyCorrected=false) =>
                  fetch(`/api/datasets/${id}/rows?offset=${offset}&limit=${limit}` +
                        `${q?`&q=${encodeURIComponent(q)}`:""}` +
                        `${onlyCorrected?`&only_corrected=true`:""}`).then(r => r.json()),
  previewDS:   (id)    => fetch(`/api/datasets/${id}/preview`).then(r => r.json()),
  runs:        (pid)   => fetch(`/api/runs${pid?`?prompt_id=${pid}`:""}`).then(r => r.json()),
  run:         (id)    => fetch(`/api/runs/${id}`).then(r => r.json()),
  startRun:    (b)     => fetch("/api/runs", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(b)}).then(r => r.json()),
  pauseRun:    (id)    => fetch(`/api/runs/${id}/pause`,  {method:"POST"}).then(r => r.json()),
  resumeRun:   (id)    => fetch(`/api/runs/${id}/resume`, {method:"POST"}).then(r => r.json()),
  cancelRun:   (id)    => fetch(`/api/runs/${id}/cancel`, {method:"POST"}).then(r => r.json()),
  deleteRun:   (id)    => fetch(`/api/runs/${id}`,        {method:"DELETE"}).then(async r => {
    if (r.status === 404 || r.status === 405) {
      return {ok: false, status: r.status, _needs_restart: true};
    }
    return r.json();
  }),
  leaderboard: (pid)   => fetch(`/api/leaderboard/${pid}`).then(r => r.json()),
  filteredRuns:(q)     => fetch("/api/runs?" + new URLSearchParams(q || {}).toString()).then(r => r.json()),
  compare:     (ids)   => fetch(`/api/compare?ids=${ids.join(",")}`).then(r => r.json()),
  corrections: (dsId)  => fetch(`/api/corrections?dataset_id=${dsId}`).then(r => r.json()),
  saveCorrection: (b)  => fetch("/api/corrections", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(b)}).then(r => r.json()),
  delCorrection:  (id) => fetch(`/api/corrections/${id}`, {method:"DELETE"}).then(r => r.json()),
  dsVersions:  (id)    => fetch(`/api/datasets/${id}/versions`).then(r => r.json()),
  imageHistory:(dsId, imageId) => fetch(`/api/datasets/${dsId}/image/${encodeURIComponent(imageId)}/history`).then(r => r.json()),
  restoreDs:   (id, body) => fetch(`/api/datasets/${id}/restore`, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)}).then(r => r.json()),
  retryRow:    (runId, rowIdx, apiKey, baseUrl) => fetch(`/api/runs/${runId}/rows/${rowIdx}/retry`, {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({api_key: apiKey||null, base_url: baseUrl||null})
  }).then(r => r.json()),
  machineId:   ()      => fetch("/api/machine_id").then(r => r.json()),
  exportBundle:(params) => `/api/export?${new URLSearchParams(params).toString()}`,
  importPreview: (file) => { const fd = new FormData(); fd.append("file", file); return fetch("/api/import/preview", {method:"POST", body:fd}).then(r => r.json()); },
  importApply:  (token) => fetch("/api/import/apply", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({import_token: token})}).then(r => r.json()),
};

const LB = { selected: new Set(), filters: {provider:"", model_id:"", prompt_id:"", dataset_id:"", status:""}, bestOnly: false, runs: [] };

const ACTIVE = { runId: null, expanded: true, expandedRows: new Set(), rawExpanded: new Set() };
// Independent state for the read-only Runs-tab review (so it doesn't fight
// the live poll's ACTIVE state).
const REVIEW = { runId: null, expandedRows: new Set(), rawExpanded: new Set(), model_id: "" };

function onRawToggle(e, idx) {
  // Use ACTIVE if a live poll is on, REVIEW otherwise
  const state = (ACTIVE.runId && document.getElementById("active-run") && !document.getElementById("active-run").classList.contains("hidden"))
    ? ACTIVE : REVIEW;
  if (e.target.open) state.rawExpanded.add(idx);
  else state.rawExpanded.delete(idx);
}
function toggleReviewRow(idx) {
  if (REVIEW.expandedRows.has(idx)) REVIEW.expandedRows.delete(idx);
  else REVIEW.expandedRows.add(idx);
  renderReviewRows();
}

// ----- Image lightbox (click any thumbnail to expand) -----
function ensureLightbox() {
  let lb = document.getElementById("lightbox");
  if (lb) return lb;
  lb = document.createElement("div");
  lb.id = "lightbox";
  lb.className = "lightbox hidden";
  lb.innerHTML = `
    <div class="lightbox-backdrop"></div>
    <div class="lightbox-content">
      <button class="lightbox-close" aria-label="close">×</button>
      <img class="lightbox-img" alt="" />
      <a class="lightbox-caption" target="_blank" rel="noopener"></a>
    </div>`;
  document.body.appendChild(lb);
  lb.querySelector(".lightbox-backdrop").onclick = closeLightbox;
  lb.querySelector(".lightbox-close").onclick = closeLightbox;
  return lb;
}

function openLightbox(src, caption) {
  if (!src) return;
  const lb = ensureLightbox();
  lb.querySelector(".lightbox-img").src = src;
  const cap = lb.querySelector(".lightbox-caption");
  cap.textContent = caption || src;
  cap.href = src;
  lb.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function closeLightbox() {
  const lb = document.getElementById("lightbox");
  if (lb) lb.classList.add("hidden");
  document.body.style.overflow = "";
}

// Single delegated listener — works for every thumbnail re-rendered by polling
document.addEventListener("click", (e) => {
  const t = e.target;
  if (t && t.classList && (t.classList.contains("thumb") || t.classList.contains("ds-thumb")) && t.dataset.src) {
    e.stopPropagation();
    openLightbox(t.dataset.src, t.dataset.caption || "");
    return;
  }
  // Row-card head click → toggle the right state (live ACTIVE or review REVIEW)
  const head = t && t.closest && t.closest(".row-card-head");
  if (head && head.dataset && head.dataset.rowIdx) {
    const idx = parseInt(head.dataset.rowIdx, 10);
    // Decide which state we're in: if active-run panel is visible AND has the row, use ACTIVE
    const inActive = ACTIVE.runId &&
      document.getElementById("active-run") &&
      !document.getElementById("active-run").classList.contains("hidden") &&
      head.closest("#active-run");
    if (inActive) toggleRow(idx);
    else toggleReviewRow(idx);
  }
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeLightbox();
});

let META = null;
let CACHE = { prompts: [], datasets: [] };

// ----- theme toggle -----
function applyTheme(t) {
  document.documentElement.setAttribute("data-theme", t);
  const btn = document.getElementById("theme-toggle");
  if (btn) btn.textContent = (t === "dark") ? "☀" : "☾";
  try { localStorage.setItem("reletix:theme", t); } catch (e) {}
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute("data-theme") || "light";
  applyTheme(cur === "dark" ? "light" : "dark");
}
(function initTheme() {
  const t = (() => { try { return localStorage.getItem("reletix:theme"); } catch { return null; } })() || "light";
  applyTheme(t);
  document.addEventListener("click", (e) => {
    if (e.target && e.target.id === "theme-toggle") toggleTheme();
  });
})();

// ----- tabs -----
// Only wire switchTab on buttons that actually have a data-tab target;
// Export/Import buttons share the .tab class but have their own onclick handlers.
$$(".tab[data-tab]").forEach(t => t.onclick = () => switchTab(t.dataset.tab));
function switchTab(name) {
  $$(".tab").forEach(t => t.classList.toggle("active", t.dataset.tab === name));
  $$(".view").forEach(v => v.classList.toggle("hidden", v.id !== `view-${name}`));
  if (name === "leaderboard") refreshLeaderboard();
  if (name === "datasets")    refreshDatasets();
  if (name === "prompts")     refreshPrompts();
  if (name === "runs")        refreshRuns();
  if (name === "benchmark")   refreshBenchmarkForm();
}

// ----- toast -----
function toast(msg, kind="ok") {
  const t = $("#toast");
  t.textContent = msg;
  t.className = kind === "error" ? "toast-error" : "";
  t.classList.remove("hidden");
  const delay = kind === "error" ? 5000 : 2600;
  setTimeout(() => t.classList.add("hidden"), delay);
}

// ----- bootstrap -----
async function bootstrap() {
  META = await api.meta();
  CACHE.prompts  = await api.prompts();
  CACHE.datasets = await api.datasets();
  if (CACHE.prompts.length === 0) {
    await seedDefaultPrompt();
    CACHE.prompts = await api.prompts();
  }
  // Pre-fill upload URL template from last successful upload
  const lastTpl = localStorage.getItem("reletix:last_url_tpl");
  if (lastTpl && $("#upload-url-tpl") && !$("#upload-url-tpl").value) {
    $("#upload-url-tpl").value = lastTpl;
  }
  // Resume tracking of any in-flight run
  const runs = await api.runs();
  const live = runs.find(r => r.status === "running" || r.status === "paused");
  if (live) {
    ACTIVE.runId = live.id;
    switchTab("benchmark");
    pollRun(live.id);
  } else {
    refreshLeaderboard();
  }
}

async function seedDefaultPrompt() {
  await api.newPrompt({
    name: "Default · Full schema (food + macros + ingredients + health)",
    description: "Baseline prompt asking the model to identify the dish, estimate macros, list ingredients with quantities, and assign an A–E health grade.",
    system_prompt:
      "You are a precise food vision and nutrition expert. Given a food image, " +
      "identify the dish, estimate per-serving macros, list every visible ingredient " +
      "with an estimated quantity in grams, and assign a health grade from A (very " +
      "healthy) to E (very unhealthy). Be conservative and consistent. " +
      "Output ONLY valid JSON matching the requested schema. No prose.",
  });
}

// ----- LEADERBOARD (filterable, multi-select, compare) -----
async function refreshLeaderboard() {
  CACHE.prompts  = await api.prompts();
  CACHE.datasets = await api.datasets();
  // Pull all runs ungrouped to populate dropdowns + table
  const all = await api.runs();

  // Wire filter dropdown options
  const providers = [...new Set(all.map(r => r.provider))].sort();
  const models = [...new Set(all.map(r => r.model_id))].sort();
  function setOptions(id, items, current) {
    const sel = $(id);
    if (!sel) return;
    sel.innerHTML = `<option value="">all</option>` +
      items.map(v => `<option value="${escape(v)}" ${v == current ? "selected" : ""}>${escape(v)}</option>`).join("");
  }
  setOptions("#lb-provider", providers, LB.filters.provider);
  setOptions("#lb-model", models, LB.filters.model_id);
  $("#lb-prompt").innerHTML = `<option value="">all</option>` +
    CACHE.prompts.map(p => `<option value="${p.id}" ${p.id == LB.filters.prompt_id ? "selected" : ""}>${escape(p.name)}</option>`).join("");
  $("#lb-dataset").innerHTML = `<option value="">all</option>` +
    CACHE.datasets.map(d => `<option value="${d.id}" ${d.id == LB.filters.dataset_id ? "selected" : ""}>${escape(d.name)}</option>`).join("");
  $("#lb-status").value = LB.filters.status || "";
  $("#lb-best-only").checked = LB.bestOnly;

  ["#lb-provider", "#lb-model", "#lb-prompt", "#lb-dataset", "#lb-status"].forEach(id => {
    $(id).onchange = () => {
      LB.filters = {
        provider:   $("#lb-provider").value,
        model_id:   $("#lb-model").value,
        prompt_id:  $("#lb-prompt").value,
        dataset_id: $("#lb-dataset").value,
        status:     $("#lb-status").value || "",
      };
      reloadLeaderboardTable();
    };
  });
  $("#lb-best-only").onchange = (e) => { LB.bestOnly = e.target.checked; reloadLeaderboardTable(); };

  reloadLeaderboardTable();
}

async function reloadLeaderboardTable() {
  const q = {};
  for (const k of ["provider","model_id","prompt_id","dataset_id","status"]) {
    if (LB.filters[k]) q[k] = LB.filters[k];
  }
  let runs = await api.filteredRuns(q);
  // Default sort by composite score desc, then accuracy desc
  runs.sort((a,b) => (b.composite_score||0) - (a.composite_score||0) || (b.accuracy||0) - (a.accuracy||0));

  if (LB.bestOnly) {
    const seen = {};
    runs = runs.filter(r => {
      const k = r.provider + "|" + r.model_id;
      if (seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }
  LB.runs = runs;

  if (!runs.length) {
    $("#leaderboard-table").innerHTML = `<p class="muted">No runs match these filters. <a href="#" onclick="switchTab('benchmark')">Run a benchmark →</a></p>`;
    renderActionBar();
    return;
  }

  $("#leaderboard-table").innerHTML = `
    <table class="lb-table">
      <thead><tr>
        <th style="width:32px"><input type="checkbox" id="lb-checkall" onchange="toggleAllSelected(event)" /></th>
        <th>#</th><th>Status</th><th>Provider</th><th>Model</th>
        <th>Prompt</th><th>Dataset</th>
        <th>Score</th><th>Accuracy</th><th>Latency</th>
        <th>Tokens (in/out)</th><th>Cost</th><th>Rows</th>
        <th></th>
      </tr></thead>
      <tbody>
      ${runs.map((r,i) => `<tr ${LB.selected.has(r.id)?'class="lb-selected"':''} class="lb-row" data-run-id="${r.id}">
        <td onclick="event.stopPropagation()"><input type="checkbox" data-run-id="${r.id}" ${LB.selected.has(r.id)?'checked':''} onchange="toggleSelected(event)" /></td>
        <td class="num lb-id">#${r.id}</td>
        <td><span class="pill ${r.status==='completed'?'ok':r.status==='failed'?'err':r.status==='cancelled'?'err':'run'}">${r.status}</span></td>
        <td><span class="pill">${escape(r.provider)}</span></td>
        <td><strong>${escape(r.model_id)}</strong></td>
        <td class="small">${escape(r.prompt_name||"")}</td>
        <td class="small">${escape(r.dataset_name||"")}${r.dataset_version?`<span class="muted small"> · v${r.dataset_version}</span>`:""}</td>
        <td>${renderBullet(r.composite_score||0)}</td>
        <td class="num">${((r.accuracy||0)*100).toFixed(1)}%</td>
        <td class="num">${((r.avg_latency_ms||0)/1000).toFixed(2)}s</td>
        <td class="num">${fmtNum(r.total_input_tokens||0)} / ${fmtNum(r.total_output_tokens||0)}</td>
        <td class="num">$${(r.total_cost_usd||0).toFixed(4)}</td>
        <td class="num">${r.n_done}/${r.n_rows}</td>
        <td onclick="event.stopPropagation()"><button class="link-btn" title="Delete this run" onclick="deleteRunSingle(${r.id})">delete</button></td>
      </tr>`).join("")}
      </tbody>
    </table>
  `;
  renderActionBar();
}

// Bullet chart (Stephen Few): qualitative ranges + central bar + target tick.
// Used for composite score across leaderboard + compare cards.
function renderBullet(value, target = 80, max = 100) {
  const v = Math.max(0, Math.min(max, value));
  const t = Math.max(0, Math.min(max, target));
  return `
    <div class="bullet" title="composite ${v.toFixed(1)} · target ${t}">
      <div class="bullet-track">
        <div class="bullet-q1" style="width:${33.33}%"></div>
        <div class="bullet-q2" style="width:${33.33}%"></div>
        <div class="bullet-q3" style="width:${33.34}%"></div>
        <div class="bullet-bar" style="width:${v}%"></div>
        <div class="bullet-target" style="left:${t}%"></div>
      </div>
      <div class="bullet-val num">${v.toFixed(1)}</div>
    </div>`;
}

// 7-dot per-nutrient sparkline (Tufte). Colors track the score.
function renderNutrientSpark(perNut) {
  if (!perNut) return "";
  const order = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"];
  return `<div class="spark" title="cal · prot · carbs · fat · fib · sug · Na">${
    order.map(k => {
      const v = perNut[k];
      if (v == null) return `<span class="spark-dot spark-empty" title="${k}: —"></span>`;
      const cls = v>=0.85 ? "spark-ok" : v>=0.5 ? "spark-mid" : "spark-low";
      return `<span class="spark-dot ${cls}" title="${k}: ${(v*100).toFixed(0)}%"></span>`;
    }).join("")
  }</div>`;
}

function toggleSelected(e) {
  const id = parseInt(e.target.dataset.runId);
  if (e.target.checked) LB.selected.add(id);
  else LB.selected.delete(id);
  // Don't re-render whole table, just the row class + action bar
  const tr = e.target.closest("tr");
  if (tr) tr.classList.toggle("lb-selected", e.target.checked);
  renderActionBar();
}

// Click anywhere on a leaderboard row (except checkbox / delete link) → detail
document.addEventListener("click", (e) => {
  const t = e.target;
  if (t && t.closest && t.closest(".lb-row")) {
    const tr = t.closest(".lb-row");
    const runId = parseInt(tr.dataset.runId);
    if (runId) openRunDetailDrawer(runId);
  }
});
function toggleAllSelected(e) {
  if (e.target.checked) LB.runs.forEach(r => LB.selected.add(r.id));
  else LB.selected.clear();
  reloadLeaderboardTable();
}

function renderActionBar() {
  const bar = $("#lb-action-bar");
  if (!bar) return;
  if (LB.selected.size < 1) {
    bar.classList.add("hidden"); bar.innerHTML = ""; return;
  }
  bar.classList.remove("hidden");
  const ids = [...LB.selected].sort((a,b) => a-b);
  bar.innerHTML = `
    <div class="card lb-bar-inner">
      <div><strong>${LB.selected.size}</strong> selected · ${ids.map(i => `<span class="pill">#${i}</span>`).join(" ")}</div>
      <div style="display:flex;gap:8px">
        <button class="btn-ghost" onclick="LB.selected.clear(); reloadLeaderboardTable();">Clear</button>
        <button class="btn-ghost btn-danger-text" onclick="deleteSelectedRuns()">Delete ${LB.selected.size}</button>
        <button class="btn" ${LB.selected.size < 2 ? "disabled" : ""} onclick="openCompareView()">Compare ${LB.selected.size} →</button>
      </div>
    </div>`;
}

async function deleteSelectedRuns() {
  const ids = [...LB.selected].sort((a,b) => a-b);
  if (!ids.length) return;
  const msg = ids.length === 1
    ? `Delete run #${ids[0]}? This removes the run and all its row results. The dataset and prompt are kept.`
    : `Delete ${ids.length} runs? This removes them and all their row results. Datasets and prompts are kept.\n\nIDs: ${ids.join(", ")}`;
  if (!confirm(msg)) return;

  let ok = 0, needsRestart = false, fail = 0;
  for (const id of ids) {
    const r = await api.deleteRun(id);
    if (r && r._needs_restart) { needsRestart = true; break; }
    if (r && r.deleted) { ok++; LB.selected.delete(id); }
    else fail++;
  }
  if (needsRestart) {
    toast("Server restart required to enable run deletion.");
    alert(
      "The server is running an older build that doesn't yet expose DELETE /api/runs/{id}.\n\n" +
      "The endpoint is in the new code, but the server hasn't been restarted (you asked us not to interrupt your live runs).\n\n" +
      "Once your runs finish, restart with:\n  ./run.sh\nand try Delete again."
    );
    return;
  }
  if (ok) toast(`Deleted ${ok} run${ok===1?"":"s"}.${fail?` (${fail} failed)`:""}`);
  reloadLeaderboardTable();
}

async function deleteRunSingle(id) {
  if (!confirm(`Delete run #${id}? This removes the run and all its row results.`)) return;
  const r = await api.deleteRun(id);
  if (r && r._needs_restart) {
    alert(
      "Server restart required.\n\n" +
      "The DELETE /api/runs/{id} endpoint is in the new code, but the running server hasn't been restarted yet.\n" +
      "Restart with ./run.sh once your in-flight runs are done."
    );
    return;
  }
  if (r && r.deleted) {
    toast(`Deleted run #${id}.`);
    LB.selected.delete(id);
    reloadLeaderboardTable();
  } else {
    toast(`Delete failed: ${(r && r.reason) || "unknown"}`);
  }
}

// ----- COMPARE VIEW -----
async function openCompareView() {
  const ids = [...LB.selected].sort((a,b) => a-b);
  if (ids.length < 2) return toast("Pick at least 2 runs.");
  const box = $("#compare-view");
  box.innerHTML = `<div class="card"><p class="muted">Loading comparison for runs ${ids.join(", ")}…</p></div>`;
  let data;
  try { data = await api.compare(ids); } catch (e) { box.innerHTML = `<div class="error-box">${escape(e.message)}</div>`; return; }
  box.innerHTML = renderCompare(data);
  box.scrollIntoView({behavior:"smooth", block:"start"});
}

function renderCompare(d) {
  const runs = d.runs;
  const fmtPct = v => v == null ? "—" : `${(v*100).toFixed(1)}%`;

  // Run summary cards
  const cards = runs.map(r => {
    const ag = r.aggregates;
    return `<div class="cmp-card">
      <div class="cmp-card-head">
        <div><span class="pill">${escape(r.provider)}</span> <strong>${escape(r.model_id)}</strong></div>
        <div class="muted small">Run #${r.id} · ${escape(r.status)}</div>
      </div>
      <div class="cmp-stats">
        <div><div class="muted small">Composite</div><div class="num" style="font-size:18px;font-weight:600">${(r.composite_score||0).toFixed(1)}</div></div>
        <div><div class="muted small">Accuracy</div><div class="num">${fmtPct(r.accuracy)}</div></div>
        <div><div class="muted small">Macros</div><div class="num">${fmtPct(ag.macros_avg)}</div></div>
        <div><div class="muted small">Ing F1</div><div class="num">${fmtPct(ag.ingredient_f1)}</div></div>
        <div><div class="muted small">Weight</div><div class="num">${fmtPct(ag.weight_acc)}</div></div>
        <div><div class="muted small">Health</div><div class="num">${fmtPct(ag.health_acc)}</div></div>
      </div>
      <div class="cmp-meta">
        <div><span class="muted small">Prompt</span><div>${escape(r.prompt_name)} <span class="muted small">id ${r.prompt_id}</span></div></div>
        <div><span class="muted small">Dataset</span><div>${escape(r.dataset_name)}</div></div>
        <div><span class="muted small">Cost · Tokens · Latency</span>
          <div class="num small">$${(r.total_cost_usd||0).toFixed(4)} · ${fmtNum(r.total_input_tokens)}/${fmtNum(r.total_output_tokens)} · ${((r.avg_latency_ms||0)/1000).toFixed(2)}s</div>
        </div>
      </div>
      <details style="margin-top:10px">
        <summary class="muted small" style="cursor:pointer">View prompt</summary>
        <pre class="raw-output" style="max-height:200px">${escape(r.prompt_text||"")}</pre>
      </details>
    </div>`;
  }).join("");

  // Per-nutrient heatmap (Stephen Few): backgrounds carry the magnitude,
  // numbers carry the precision.
  const nuts = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"];
  const heatBg = v => {
    if (v == null) return "transparent";
    if (v >= 0.85) return `rgba(91,122,79,${0.10 + 0.50*(v-0.85)/0.15})`;       // green
    if (v >= 0.5)  return `rgba(204,120,92,${0.10 + 0.30*(v-0.5)/0.35})`;       // copper
    return `rgba(160,74,60,${0.12 + 0.30*(0.5-v)/0.5})`;                        // red
  };
  const nutTable = `
    <table class="cmp-table heatmap-table">
      <thead><tr><th>Nutrient</th>${runs.map(r => `<th class="num">#${r.id} · ${escape(r.model_id)}</th>`).join("")}</tr></thead>
      <tbody>
      ${nuts.map(n => `<tr>
        <td>${n}</td>
        ${runs.map(r => {
          const v = r.aggregates.per_nutrient[n];
          return `<td class="num heat-cell" style="background:${heatBg(v)}">${fmtPct(v)}</td>`;
        }).join("")}
      </tr>`).join("")}
      </tbody>
    </table>`;

  // Slope graph (Tufte): each metric becomes a slope line across the runs.
  // Reads at-a-glance which dimensions improved / regressed across versions.
  const slopeMetrics = [
    {key: "accuracy",      label: "Accuracy",     get: r => r.accuracy},
    {key: "macros_avg",    label: "Macros",       get: r => r.aggregates.macros_avg},
    {key: "ingredient_f1", label: "Ing F1",       get: r => r.aggregates.ingredient_f1},
    {key: "weight_acc",    label: "Weights",      get: r => r.aggregates.weight_acc},
    {key: "health_acc",    label: "Health",       get: r => r.aggregates.health_acc},
  ];
  const slopeWidth = 80; // px per run column
  const slopePad = 100;  // left label, right label
  const slopeH = 220;
  const slopeW = slopePad*2 + slopeWidth*(runs.length-1);
  const slopeChart = `
    <div class="slope-wrap">
      <svg width="${slopeW}" height="${slopeH}" class="slope-svg" viewBox="0 0 ${slopeW} ${slopeH}">
        <!-- background grid -->
        ${[0.25, 0.5, 0.75, 1.0].map(y => {
          const yy = 30 + (1-y)*(slopeH-50);
          return `<line x1="${slopePad}" x2="${slopeW-slopePad}" y1="${yy}" y2="${yy}" stroke="var(--hairline)" stroke-dasharray="2,3"/>
                  <text x="${slopePad-8}" y="${yy+3}" text-anchor="end" font-size="10" fill="var(--muted)" font-family="var(--mono)">${(y*100).toFixed(0)}%</text>`;
        }).join("")}
        <!-- run column labels -->
        ${runs.map((r,i) => {
          const x = slopePad + i*slopeWidth;
          return `<text x="${x}" y="20" text-anchor="middle" font-size="10" fill="var(--muted)" font-family="var(--mono)">#${r.id}</text>
                  <text x="${x}" y="${slopeH-6}" text-anchor="middle" font-size="9" fill="var(--ink-2)" font-family="var(--sans)">${escape((r.model_id||"").slice(0,16))}</text>`;
        }).join("")}
        <!-- one polyline per metric -->
        ${slopeMetrics.map((m, mi) => {
          const colors = ["var(--copper)","var(--ink)","var(--copper-deep)","var(--green)","#7a6e8c"];
          const c = colors[mi % colors.length];
          const pts = runs.map((r, i) => {
            const v = m.get(r) || 0;
            return [slopePad + i*slopeWidth, 30 + (1-v)*(slopeH-50)];
          });
          const poly = pts.map(p => p.join(",")).join(" ");
          // Right label
          const last = pts[pts.length-1];
          return `
            <polyline points="${poly}" fill="none" stroke="${c}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            ${pts.map(p => `<circle cx="${p[0]}" cy="${p[1]}" r="3" fill="${c}"/>`).join("")}
            <text x="${last[0]+8}" y="${last[1]+3}" font-size="10" fill="${c}" font-family="var(--sans)" font-weight="600">${escape(m.label)}</text>
          `;
        }).join("")}
      </svg>
    </div>`;

  // Biggest swings table
  const swings = d.biggest_swings || [];
  const swingsTable = swings.length ? `
    <h4 style="margin:14px 0 8px">Biggest score swings — top ${swings.length} of ${d.n_shared} shared rows</h4>
    <table class="cmp-table cmp-rows">
      <thead><tr>
        <th></th><th>Row</th><th>Truth food</th><th>Spread</th>
        ${runs.map(r => `<th>#${r.id} score · pred</th>`).join("")}
      </tr></thead>
      <tbody>
      ${swings.map(s => {
        const imgSrc = s.image_ref && s.image_ref.startsWith("http") ? s.image_ref : (s.image_ref ? `/images/${s.image_ref.split('/').pop()}` : null);
        return `<tr>
          <td>${imgSrc ? `<img class="thumb" data-src="${escape(imgSrc)}" data-caption="${escape(s.truth_food||"")}" src="${imgSrc}" loading="lazy" referrerpolicy="no-referrer" />` : ""}</td>
          <td class="num">${s.row_idx}</td>
          <td class="small">${escape(s.truth_food||"—")}</td>
          <td class="num"><span class="pill">${(s.score_spread*100).toFixed(0)}%</span></td>
          ${runs.map(r => {
            const bd = s.by_run[String(r.id)] || {};
            const ov = (bd.scores||{}).overall || 0;
            const c = ov>=0.85?"var(--green)":ov>=0.5?"var(--copper)":"var(--red)";
            const pred = bd.error ? `<span style="color:var(--red)">ERROR</span>` : escape(bd.pred_food||"—");
            return `<td class="small"><span class="num" style="color:${c};font-weight:600">${(ov*100).toFixed(0)}%</span> · ${pred}</td>`;
          }).join("")}
        </tr>`;
      }).join("")}
      </tbody>
    </table>` : `<p class="muted">No shared rows between selected runs.</p>`;

  const ids = runs.map(r => r.id).join(",");
  return `
    <div class="card cmp-wrap">
      <div class="row-between" style="align-items:center;margin-bottom:14px">
        <h3 style="margin:0">Comparing runs ${runs.map(r => `<span class="pill">#${r.id}</span>`).join(" ")}</h3>
        <div style="display:flex;gap:8px">
          <button class="btn-ghost" onclick="copyCompareMd('${ids}')">Copy as markdown</button>
          <a class="btn-ghost" href="/api/compare.md?ids=${ids}" target="_blank" rel="noopener">Open MD ↗</a>
          <button class="btn-ghost" onclick="closeCompareView()">Close</button>
        </div>
      </div>
      <div class="cmp-grid">${cards}</div>

      <h4 style="margin:18px 0 8px" class="cmp-h">Metric trajectory across selected runs</h4>
      <div class="muted small" style="margin-bottom:6px">A slope graph (Tufte) — each line is one metric, each column one run. Up = better.</div>
      ${slopeChart}

      <h4 style="margin:18px 0 8px" class="cmp-h">Per-nutrient accuracy heatmap</h4>
      ${nutTable}
      ${swingsTable}
    </div>`;
}

function closeCompareView() { $("#compare-view").innerHTML = ""; }

async function copyCompareMd(idsCsv) {
  const r = await fetch(`/api/compare.md?ids=${idsCsv}`);
  const txt = await r.text();
  try {
    await navigator.clipboard.writeText(txt);
    toast("Comparison copied to clipboard.");
  } catch {
    // Fallback: open in a new window
    const w = window.open("", "_blank");
    w.document.write(`<pre>${txt.replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}</pre>`);
  }
}

// ----- DATASETS -----
const DS_VIEW = {}; // dataset_id -> {open: bool, offset: int, q: str, only_corrected: bool}

const URL_PRESETS = [
  {
    name: "WILLMA S3",
    template: "https://willma-prod-assets.s3.eu-central-1.amazonaws.com/meals_images/{image_id}",
  },
];

async function refreshDatasets() {
  CACHE.datasets = await api.datasets();
  const list = $("#datasets-list");
  if (!CACHE.datasets.length) {
    list.innerHTML = `<p class="muted">No datasets yet. Upload one above.</p>`;
    return;
  }
  // Pull versions metadata for each dataset in parallel
  const versionsByDs = {};
  await Promise.all(CACHE.datasets.map(async d => {
    try { versionsByDs[d.id] = await api.dsVersions(d.id); }
    catch { versionsByDs[d.id] = {current_version: 0, history: []}; }
  }));

  list.innerHTML = CACHE.datasets.map(d => {
    const cols = Object.entries(JSON.parse(d.columns_detected||"{}"));
    const hasTpl = !!d.image_url_template;
    const presetButtons = URL_PRESETS.map(p =>
      `<button class="btn-ghost" onclick="applyTpl(${d.id}, '${escape(p.template)}')">Apply ${escape(p.name)} preset</button>`
    ).join(" ");

    const v   = versionsByDs[d.id] || {current_version: 0, history: []};
    const cur = v.current_version || 0;
    const nChanges = (v.history || []).length;
    const uniqueImgs = new Set((v.history || []).map(h => h.image_id)).size;
    const versionStrip = `
      <div class="ds-version-strip">
        <span class="version-pill version-pill-large">dataset @ v${cur}</span>
        ${cur > 0
          ? `<span class="muted small">
               ${uniqueImgs} corrected image${uniqueImgs===1?"":"s"} ·
               ${nChanges} total change${nChanges===1?"":"s"}
             </span>
             <button class="btn-ghost" onclick="openVersionsTimeline(${d.id})">View timeline</button>
             <button class="btn-ghost" onclick="toggleDsView(${d.id}, {only_corrected:true})">See ${uniqueImgs} corrected row${uniqueImgs===1?"":"s"}</button>
             <button class="btn-ghost" onclick="rescoreAllForDataset(${d.id})" title="Re-score every completed run against the latest truth">↻ Re-score all runs</button>`
          : `<span class="muted small">no corrections yet — promote a model output or edit truth on any row to start versioning</span>`
        }
      </div>`;

    return `
      <div class="card ds-card" id="ds-card-${d.id}">
        <div class="row-between" style="margin-bottom:0;align-items:center">
          <div>
            <strong>${escape(d.name)}</strong>
            <span class="muted small" style="margin-left:10px">#${d.id} · ${d.n_rows.toLocaleString()} rows · ${new Date(d.created_at*1000).toLocaleString()}</span>
            <div class="small" style="margin-top:4px">${cols.map(([k,v])=>`<span class="pill">${k}→${escape(v)}</span>`).join(" ")}</div>
            <div class="small ds-tpl-row" id="ds-tpl-${d.id}">
              <span class="muted small">image url:</span>
              ${hasTpl
                ? `<span class="pill ok">configured</span><code class="tpl-code">${escape(d.image_url_template)}</code>
                   <button class="link-btn" onclick="openTplEditor(${d.id})">edit</button>`
                : `<span class="pill err">missing — images won't load</span>
                   ${presetButtons}
                   <button class="link-btn" onclick="openTplEditor(${d.id})">custom URL…</button>`}
            </div>
          </div>
          <div style="display:flex;gap:6px">
            <button class="btn-ghost" onclick="toggleDsView(${d.id})" id="ds-toggle-${d.id}">View rows</button>
            <button class="btn-ghost btn-danger-text" onclick="deleteDataset(${d.id}, '${escape(d.name).replace(/'/g,"\\'")}')">Delete</button>
          </div>
        </div>
        ${versionStrip}
        <div id="ds-view-${d.id}"></div>
      </div>
    `;
  }).join("");
}

async function applyTpl(id, template) {
  await api.patchDS(id, {image_url_template: template});
  localStorage.setItem("reletix:last_url_tpl", template);
  toast("Template applied. Reloading…");
  await refreshDatasets();
  if (DS_VIEW[id] && DS_VIEW[id].open) await renderDsView(id);
}

function openTplEditor(id) {
  const ds = CACHE.datasets.find(d => d.id === id);
  const cur = (ds && ds.image_url_template) || "";
  const row = $(`#ds-tpl-${id}`);
  row.innerHTML = `
    <span class="muted small">image url:</span>
    <input type="text" id="tpl-input-${id}" value="${escape(cur)}"
           placeholder="https://example.com/path/{image_id}"
           style="flex:1;min-width:280px;padding:6px 10px;font-size:12px;font-family:var(--mono)" />
    <button class="btn-ghost" onclick="saveTpl(${id})">Save</button>
    ${URL_PRESETS.map(p =>
      `<button class="link-btn" onclick="document.getElementById('tpl-input-${id}').value='${escape(p.template)}'">${escape(p.name)}</button>`
    ).join(" ")}
    <button class="link-btn" onclick="refreshDatasets()">cancel</button>
  `;
  setTimeout(() => $(`#tpl-input-${id}`).focus(), 50);
  $(`#tpl-input-${id}`).onkeydown = (e) => { if (e.key === "Enter") saveTpl(id); };
}

async function saveTpl(id) {
  const v = $(`#tpl-input-${id}`).value.trim();
  await api.patchDS(id, {image_url_template: v || null});
  if (v) localStorage.setItem("reletix:last_url_tpl", v);
  toast(v ? "Template saved." : "Template cleared.");
  await refreshDatasets();
  if (DS_VIEW[id] && DS_VIEW[id].open) await renderDsView(id);
}

// Legacy alias so any old buttons still resolve
const editDsTpl = openTplEditor;

async function deleteDataset(id, name) {
  if (!confirm(`Delete dataset "${name}"?\n\nThis also deletes ALL runs against this dataset.`)) return;
  const r = await api.deleteDS(id);
  if (r.deleted) {
    toast(`Deleted dataset (and ${r.runs_removed||0} runs).`);
    refreshDatasets();
  } else {
    toast("Delete failed: " + (r.reason||"unknown"));
  }
}

async function toggleDsView(id, opts) {
  const state = DS_VIEW[id] = DS_VIEW[id] || {open: false, offset: 0, q: "", only_corrected: false};
  if (opts && opts.only_corrected != null) {
    state.only_corrected = !!opts.only_corrected;
    state.offset = 0;
    state.open = true;   // force open when toggling filters
  } else {
    state.open = !state.open;
  }
  $(`#ds-toggle-${id}`).textContent = state.open ? "Hide rows" : "View rows";
  if (state.open) await renderDsView(id);
  else $(`#ds-view-${id}`).innerHTML = "";
}

async function renderDsView(id) {
  const state = DS_VIEW[id];
  const box = $(`#ds-view-${id}`);
  box.innerHTML = `<div class="muted small" style="padding:14px 0">Loading rows…</div>`;
  const data = await api.dsRows(id, state.offset, 20, state.q, state.only_corrected);
  const rows = data.rows || [];
  const start = data.offset + 1;
  const end = data.offset + rows.length;
  const total = data.total;
  const cur = data.current_version || 0;

  box.innerHTML = `
    <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--line)">
      <div class="row-between" style="margin-bottom:10px;align-items:center;flex-wrap:wrap">
        <div class="muted small">
          ${state.only_corrected
            ? `<span class="pill run">corrected only</span> ${total} row${total===1?"":"s"}`
            : `${start}–${end} of ${total.toLocaleString()}${state.q?` matching "${escape(state.q)}"`:""}`}
          · <span class="pill ${cur>0?"run":""}">dataset @ v${cur}</span>
          ${cur>0 ? `<button class="link-btn" onclick="openVersionsTimeline(${id})" title="View change history + restore">timeline</button>` : ""}
        </div>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          <label class="small muted" style="display:flex;align-items:center;gap:4px">
            <input type="checkbox" id="ds-corr-${id}" ${state.only_corrected?"checked":""} onchange="toggleDsCorrected(${id}, this.checked)" />
            corrected only
          </label>
          <input type="text" placeholder="search food name…" value="${escape(state.q)}" id="ds-q-${id}" style="width:180px;padding:6px 10px;font-size:13px" />
          <button class="btn-ghost" onclick="dsSearch(${id})">Search</button>
          <button class="btn-ghost" onclick="dsPage(${id}, -20)" ${state.offset<=0?"disabled":""}>← prev</button>
          <button class="btn-ghost" onclick="dsPage(${id}, 20)" ${end>=total?"disabled":""}>next →</button>
        </div>
      </div>
      <div class="ds-grid">
        ${rows.length === 0
          ? `<div class="muted small">${state.only_corrected ? "No corrections yet for this dataset." : "No rows."}</div>`
          : rows.map(r => renderDsRow(r, id)).join("")}
      </div>
    </div>`;
  const inp = $(`#ds-q-${id}`);
  if (inp) inp.onkeydown = (e) => { if (e.key === "Enter") dsSearch(id); };
}

function toggleDsCorrected(id, checked) {
  const state = DS_VIEW[id];
  if (!state) return;
  state.only_corrected = !!checked;
  state.offset = 0;
  renderDsView(id);
}

// ============================================================
// Version timeline + per-image history modals
// ============================================================
async function openVersionsTimeline(datasetId) {
  const data = await api.dsVersions(datasetId);
  const ds = (CACHE.datasets || []).find(d => d.id === datasetId);
  const items = data.history || [];
  // Group by version
  const byVersion = {};
  for (const h of items) {
    (byVersion[h.version_after] = byVersion[h.version_after] || []).push(h);
  }
  const versions = Object.keys(byVersion).map(v => parseInt(v)).sort((a,b)=>b-a);
  const cur = data.current_version || 0;

  showCustomModal({
    title: `Version history · ${escape(ds?.name||`dataset #${datasetId}`)}`,
    subtitle: `${versions.length} version${versions.length===1?"":"s"} · current = v${cur}`,
    bodyHtml: `
      <div class="version-timeline">
        <div class="vt-row vt-row-base">
          <div class="vt-v">v0</div>
          <div class="vt-meta">
            <strong>Original benchmark</strong>
            <div class="muted small">Truth as parsed from the source Excel.</div>
          </div>
          <div class="vt-actions">
            ${cur > 0 ? `<button class="btn-ghost btn-danger-text" onclick="confirmRestore(${datasetId}, 0)">Restore</button>` : `<span class="muted small">current</span>`}
          </div>
        </div>
        ${versions.map(v => {
          const entries = byVersion[v];
          const e = entries[0];
          const actionLabel = entries.length > 1 ? `${entries.length} changes` : (e.action || "change");
          const isCurrent = v === cur;
          return `
            <div class="vt-row${isCurrent?" vt-row-current":""}">
              <div class="vt-v">v${v}</div>
              <div class="vt-meta">
                <strong>${escape(actionLabel)}</strong>
                <div class="muted small">
                  ${entries.map(en => `<button class="link-btn" onclick="openImageHistory(${datasetId}, '${escape(en.image_id)}')">${escape(en.image_id.slice(0,18))}…</button>`).join(" · ")}
                </div>
                ${e.note ? `<div class="muted small" style="margin-top:2px;font-style:italic">"${escape(e.note)}"</div>` : ""}
                <div class="muted small">${new Date((e.created_at||0)*1000).toLocaleString()}</div>
              </div>
              <div class="vt-actions">
                ${isCurrent ? `<span class="muted small">current</span>` : `<button class="btn-ghost" onclick="confirmRestore(${datasetId}, ${v})">Restore</button>`}
              </div>
            </div>`;
        }).join("")}
      </div>
    `,
    footerHtml: `
      <button class="btn-ghost" onclick="closeCustomModal()">Close</button>
      <button class="btn" onclick="rescoreAllForDataset(${datasetId}); closeCustomModal();">↻ Re-score all runs at current</button>
    `,
  });
}

async function confirmRestore(datasetId, targetVersion) {
  if (!confirm(`Restore dataset to version ${targetVersion}?\n\nThis rolls the active corrections set forward/backward to match v${targetVersion} and creates a NEW version (so you can always restore back).`)) return;
  const r = await api.restoreDs(datasetId, {version: targetVersion, note: `restore to v${targetVersion}`});
  toast(`Restored to v${targetVersion} → new v${r.new_version} (${r.n_changed} change${r.n_changed===1?"":"s"})`);
  closeCustomModal();
  refreshDatasets();
  if (DS_VIEW[datasetId] && DS_VIEW[datasetId].open) renderDsView(datasetId);
}

async function openImageHistory(datasetId, imageId) {
  const data = await api.imageHistory(datasetId, imageId);
  const items = data.history || [];
  showCustomModal({
    title: `Change history`,
    subtitle: `image ${escape(imageId.slice(0,32))}…  ·  ${items.length} change${items.length===1?"":"s"}`,
    bodyHtml: items.length === 0 ? `<p class="muted">No corrections yet for this image.</p>` : `
      <div class="image-history">
        ${items.slice().reverse().map(h => {
          let prevT, newT;
          try { prevT = h.prev_truth_json ? JSON.parse(h.prev_truth_json) : null; } catch { prevT = null; }
          try { newT  = h.truth_json      ? JSON.parse(h.truth_json) : null;      } catch { newT  = null; }
          return `
            <div class="ih-entry">
              <div class="ih-head">
                <span class="vt-v">v${h.version_after}</span>
                <span class="pill ${h.action==='delete'?'err':h.action==='restore'?'warn':'run'}">${h.action}</span>
                <span class="muted small">${new Date((h.created_at||0)*1000).toLocaleString()}</span>
                ${h.note ? `<span class="muted small">· ${escape(h.note)}</span>` : ""}
              </div>
              <div class="ih-diff">
                ${renderTruthDiff(prevT, newT)}
              </div>
            </div>`;
        }).join("")}
      </div>
    `,
    footerHtml: `<button class="btn-ghost" onclick="closeCustomModal()">Close</button>`,
  });
}

function renderTruthDiff(prev, next) {
  const fields = ["food", "description", "health_score"];
  const macros = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"];
  let out = "";
  for (const k of fields) {
    const a = prev ? prev[k] : null;
    const b = next ? next[k] : null;
    if (a === b) continue;
    out += `<div class="ih-field"><span class="muted small">${k}</span><div><span class="ih-old">${escape(a||"—")}</span> <span class="muted">→</span> <span class="ih-new">${escape(b||"—")}</span></div></div>`;
  }
  // Macros table
  const pn = (prev && prev.nutrition) || {};
  const nn = (next && next.nutrition) || {};
  const changedMacros = macros.filter(m => (pn[m] ?? null) !== (nn[m] ?? null));
  if (changedMacros.length) {
    out += `<table class="ih-macros"><thead><tr><th></th>${changedMacros.map(m=>`<th>${m}</th>`).join("")}</tr></thead>
      <tbody>
        <tr><td class="muted small">before</td>${changedMacros.map(m=>`<td class="num ih-old">${pn[m]??"—"}</td>`).join("")}</tr>
        <tr><td class="muted small">after</td>${changedMacros.map(m=>`<td class="num ih-new">${nn[m]??"—"}</td>`).join("")}</tr>
      </tbody></table>`;
  }
  // Ingredients counts
  const pi = (prev && prev.ingredients) || [];
  const ni = (next && next.ingredients) || [];
  if (pi.length !== ni.length) {
    out += `<div class="ih-field"><span class="muted small">ingredients</span><div><span class="ih-old">${pi.length}</span> <span class="muted">→</span> <span class="ih-new">${ni.length}</span></div></div>`;
  }
  return out || `<div class="muted small">(no field-level diff available)</div>`;
}

// Generic modal helper (reuses .modal styles)
function showCustomModal({title, subtitle, bodyHtml, footerHtml}) {
  let m = document.getElementById("custom-modal");
  if (!m) {
    m = document.createElement("div");
    m.id = "custom-modal";
    m.className = "modal hidden";
    m.innerHTML = `
      <div class="modal-backdrop" data-close="1"></div>
      <div class="modal-panel">
        <div class="modal-head">
          <div>
            <h3 id="cm-title" style="margin:0">—</h3>
            <div class="muted small" id="cm-subtitle"></div>
          </div>
          <button class="btn-ghost" onclick="closeCustomModal()">✕</button>
        </div>
        <div class="modal-body" id="cm-body"></div>
        <div class="modal-foot" id="cm-foot"></div>
      </div>`;
    document.body.appendChild(m);
    m.querySelector(".modal-backdrop").onclick = closeCustomModal;
  }
  m.querySelector("#cm-title").textContent = title;
  m.querySelector("#cm-subtitle").innerHTML = subtitle || "";
  m.querySelector("#cm-body").innerHTML = bodyHtml || "";
  m.querySelector("#cm-foot").innerHTML = footerHtml || "";
  m.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}
function closeCustomModal() {
  const m = document.getElementById("custom-modal");
  if (m) m.classList.add("hidden");
  document.body.style.overflow = "";
}

function renderDsRow(r, dsId) {
  const src = r.image_url || (r.image_path ? `/images/${r.image_path.split('/').pop()}` : null);
  const nut = r.nutrition_truth || {};
  const cells = [
    ["kcal", nut.calories],
    ["P",    nut.protein_g],
    ["C",    nut.carbs_g],
    ["F",    nut.fat_g],
    ["fib",  nut.fiber_g],
    ["sug",  nut.sugar_g],
    ["Na",   nut.sodium_mg],
  ];
  const corr = r._correction || {};
  const versionPill = corr.is_corrected
    ? `<button class="version-pill" onclick="event.stopPropagation(); openImageHistory(${dsId}, '${escape(r.image_id||"")}')" title="${corr.change_count||1} change${corr.change_count===1?"":"s"} · last at v${corr.version_when_changed}">
         ✎ v${corr.version_when_changed||"?"}${corr.change_count>1?` ×${corr.change_count}`:""}
       </button>`
    : "";
  return `
    <div class="ds-row${corr.is_corrected?" ds-row-corrected":""}">
      ${src
        ? `<img class="ds-thumb" src="${src}" data-src="${escape(src)}" data-caption="${escape(r.food||"")}" loading="lazy" referrerpolicy="no-referrer" title="click to expand" onerror="this.outerHTML='<div class=\\'ds-thumb ds-thumb-err\\' title=\\'failed to load\\'>×</div>'" />`
        : `<div class="ds-thumb ds-thumb-empty" title="no image url — set the URL template on this dataset">∅</div>`}
      <div class="ds-row-body">
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
          <strong>${escape(r.food||"—")}</strong>
          ${versionPill}
        </div>
        <div class="muted small">row ${r.row_idx}${r.image_id?` · id ${escape(r.image_id.slice(0,18))}…`:""}</div>
        <div class="ds-nut">
          ${cells.map(([k,v]) => `<span class="ds-nut-cell"><span class="muted small">${k}</span><span class="num">${v??"—"}</span></span>`).join("")}
        </div>
      </div>
    </div>`;
}

function dsSearch(id) {
  const state = DS_VIEW[id];
  state.q = $(`#ds-q-${id}`).value.trim();
  state.offset = 0;
  renderDsView(id);
}
function dsPage(id, delta) {
  const state = DS_VIEW[id];
  state.offset = Math.max(0, state.offset + delta);
  renderDsView(id);
}

const upload = $("#upload-zone");
const fileInput = $("#file-input");
upload.onclick = () => fileInput.click();
$("#browse-link").onclick = (e) => { e.preventDefault(); fileInput.click(); };
upload.ondragover = (e) => { e.preventDefault(); upload.classList.add("drag"); };
upload.ondragleave = () => upload.classList.remove("drag");
upload.ondrop = (e) => { e.preventDefault(); upload.classList.remove("drag"); if (e.dataTransfer.files[0]) doUpload(e.dataTransfer.files[0]); };
fileInput.onchange = (e) => { if (e.target.files[0]) doUpload(e.target.files[0]); };

async function doUpload(file) {
  $("#upload-status").innerHTML = `<div class="muted small">Uploading & parsing ${escape(file.name)}…</div>`;
  try {
    const tpl = ($("#upload-url-tpl") && $("#upload-url-tpl").value.trim()) || null;
    if (tpl) localStorage.setItem("reletix:last_url_tpl", tpl);
    const ds = await api.uploadDS(file, tpl);
    if (ds.error || ds.detail) throw new Error(ds.error || ds.detail);
    const cols = ds.columns_detected ? Object.entries(JSON.parse(ds.columns_detected||"{}")) : [];
    $("#upload-status").innerHTML =
      `<div class="card" style="margin-top:14px">
         <strong>${escape(ds.name)}</strong> — ${ds.n_rows} rows<br>
         ${cols.length ? `<span class="muted small">Detected: ${cols.map(([k,v])=>`<span class="pill">${k}→${escape(v)}</span>`).join(" ")}</span>` : `<span class="pill err">No food/nutrition columns detected</span>`}
         ${ds.preview ? renderPreview(ds.preview) : ""}
       </div>`;
    refreshDatasets();
    toast("Dataset uploaded.");
  } catch (e) {
    $("#upload-status").innerHTML = `<div class="error-box">${escape(e.message)}</div>`;
  }
}

function renderPreview(rows) {
  return `<div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
    ${rows.map(r => {
      const src = r.image_url ? r.image_url
                : r.image_path ? `/images/${r.image_path.split('/').pop()}`
                : null;
      return `
      <div style="flex:1;min-width:200px;border:1px solid var(--line);border-radius:8px;padding:10px;background:var(--paper)">
        ${src ? `<img class="thumb" src="${src}" loading="lazy" />` : '<div class="thumb" style="background:var(--paper-2)"></div>'}
        <div class="small"><strong>${escape(r.food||"—")}</strong></div>
        <div class="muted small">${r.image_id?`id: ${escape(r.image_id.slice(0,16))}…`:""}</div>
        <div class="small num">${Object.entries(r.nutrition_truth||{}).map(([k,v])=>`${k}:${v}`).join(" · ")}</div>
      </div>`;
    }).join("")}
  </div>`;
}

// ----- PROMPTS -----
async function refreshPrompts() {
  CACHE.prompts = await api.prompts();
  $("#prompts-list").innerHTML = !CACHE.prompts.length ?
    `<p class="muted">No competitions saved.</p>` :
    CACHE.prompts.map(p => `
      <div class="card">
        <div class="row-between">
          <div>
            <strong>${escape(p.name)}</strong>
            ${p.description ? `<div class="muted small">${escape(p.description)}</div>` : ""}
          </div>
          <button class="btn-ghost" onclick="deletePrompt(${p.id})">Delete</button>
        </div>
        <pre style="margin:10px 0 0;font-family:var(--mono);font-size:12px;white-space:pre-wrap;color:var(--ink-2);background:var(--paper);padding:10px;border-radius:6px;border:1px solid var(--line)">${escape(p.system_prompt)}</pre>
      </div>`).join("");
}

async function createPrompt() {
  const name = $("#p-name").value.trim();
  const text = $("#p-text").value.trim();
  if (!name || !text) return toast("Name and prompt required");
  await api.newPrompt({name, system_prompt: text, description: $("#p-desc").value.trim() || null});
  $("#p-name").value = $("#p-text").value = $("#p-desc").value = "";
  toast("Competition saved.");
  refreshPrompts();
}

async function deletePrompt(id) {
  if (!confirm("Delete this competition? Its run history is kept.")) return;
  await api.delPrompt(id);
  refreshPrompts();
}

// ----- RUNS -----
async function refreshRuns() {
  const runs = await api.runs();
  $("#runs-list").innerHTML = !runs.length ?
    `<p class="muted">No runs yet.</p>` :
    `<table>
       <thead><tr><th>#</th><th>Status</th><th>Competition</th><th>Provider/Model</th><th>Progress</th><th>Score</th><th>Accuracy</th><th>Cost</th><th>Started</th></tr></thead>
       <tbody>${runs.map(r => `<tr onclick="showRun(${r.id})" style="cursor:pointer">
         <td class="num">${r.id}</td>
         <td><span class="pill ${r.status==='completed'?'ok':r.status==='failed'?'err':'run'}">${r.status}</span></td>
         <td>${escape(r.prompt_name)}</td>
         <td><span class="pill">${r.provider}</span> ${escape(r.model_id)}</td>
         <td class="num">${r.n_done}/${r.n_rows}</td>
         <td class="num">${(r.composite_score||0).toFixed(1)}</td>
         <td class="num">${((r.accuracy||0)*100).toFixed(1)}%</td>
         <td class="num">$${(r.total_cost_usd||0).toFixed(4)}</td>
         <td class="muted small">${new Date(r.started_at*1000).toLocaleString()}</td>
       </tr>`).join("")}</tbody></table>`;
}

async function showRun(id) {
  const r = await api.run(id);
  REVIEW.runId = r.id;
  REVIEW.datasetId = r.dataset_id;          // <- needed by renderRowCard's promote panel
  REVIEW.model_id = r.model_id || "";
  REVIEW.expandedRows = new Set();
  REVIEW.rawExpanded = new Set();
  $("#runs-list").innerHTML = renderReview(r);
}

// Drawer-style detail view that opens from anywhere (leaderboard, runs).
// Shares the REVIEW state with showRun().
function openRunDetailDrawer(id) {
  let drawer = document.getElementById("run-drawer");
  if (!drawer) {
    drawer = document.createElement("div");
    drawer.id = "run-drawer";
    drawer.className = "run-drawer hidden";
    drawer.innerHTML = `
      <div class="run-drawer-backdrop"></div>
      <div class="run-drawer-panel">
        <div class="run-drawer-head">
          <h3 id="run-drawer-title" style="margin:0">Loading…</h3>
          <button class="btn-ghost" onclick="closeRunDrawer()">✕ Close</button>
        </div>
        <div class="run-drawer-body" id="run-drawer-body"></div>
      </div>`;
    document.body.appendChild(drawer);
    drawer.querySelector(".run-drawer-backdrop").onclick = closeRunDrawer;
  }
  drawer.classList.remove("hidden");
  document.body.style.overflow = "hidden";
  loadRunDrawer(id);
}

async function loadRunDrawer(id) {
  REVIEW.runId = id;
  REVIEW.expandedRows = new Set();
  REVIEW.rawExpanded = new Set();
  const body = document.getElementById("run-drawer-body");
  body.innerHTML = `<p class="muted">Loading run #${id}…</p>`;
  let r;
  try { r = await api.run(id); } catch (e) {
    body.innerHTML = `<div class="error-box">${escape(String(e))}</div>`; return;
  }
  REVIEW.model_id = r.model_id || "";
  REVIEW.datasetId = r.dataset_id;          // <- needed by promote panel
  document.getElementById("run-drawer-title").textContent =
    `Run #${r.id} · ${r.model_id} · ${r.status}`;
  body.innerHTML = renderReviewBody(r);
}

function closeRunDrawer() {
  const drawer = document.getElementById("run-drawer");
  if (drawer) drawer.classList.add("hidden");
  document.body.style.overflow = "";
  REVIEW.runId = null;
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeRunDrawer();
});

function renderReview(r) {
  const head = `
    <div class="card">
      <div class="row-between" style="align-items:center;margin-bottom:6px">
        <div>
          <h3 style="margin:0">Run #${r.id} · ${escape(r.model_id)} <span class="pill ${r.status==='completed'?'ok':r.status==='failed'?'err':r.status==='cancelled'?'err':'run'}">${r.status}</span></h3>
          <div class="muted small">${escape(r.prompt_name||"")} · ${escape(r.provider)} · ${r.n_done}/${r.n_rows} rows · ${(r.avg_latency_ms/1000||0).toFixed(2)}s/row · ${fmtNum(r.total_input_tokens)} in / ${fmtNum(r.total_output_tokens)} out</div>
        </div>
        <button class="btn-ghost" onclick="refreshRuns()">← back to all runs</button>
      </div>
      ${r.error ? `<div class="error-box">${escape(r.error)}</div>` : ""}
      ${renderReviewStats(r)}
    </div>
  `;
  return head + `<div id="review-rows">${renderReviewRowList(r)}</div>`;
}

// Used by the drawer (no "back to all runs" button)
function renderReviewBody(r) {
  return `
    <div class="card" style="margin-bottom:12px">
      ${r.error ? `<div class="error-box">${escape(r.error)}</div>` : ""}
      <div class="row-between" style="align-items:center;margin-bottom:8px">
        <div class="muted small">
          ${escape(r.prompt_name||"")} · ${escape(r.provider)} · ${r.n_done}/${r.n_rows} rows · ${(r.avg_latency_ms/1000||0).toFixed(2)}s/row · ${fmtNum(r.total_input_tokens)} in / ${fmtNum(r.total_output_tokens)} out
        </div>
        <button class="btn-ghost" onclick="rescoreRun(${r.id})" title="Recompute scores against the latest truth + corrections (no model calls)">↻ Re-score</button>
      </div>
      ${renderReviewStats(r)}
    </div>
    <div id="review-rows">${renderReviewRowList(r)}</div>
  `;
}

function renderReviewStats(r) {
  const rows = r.rows || [];
  const fmtPct = v => v == null ? "—" : `${(v*100).toFixed(1)}%`;
  const accAvg = avgFromRows(rows, "macros_avg");
  const f1Avg  = avgFromRows(rows, "ingredient_f1");
  const wAvg   = avgFromRows(rows, "weight_acc");
  const hAvg   = avgFromRows(rows, "health.score");
  return `
    <div class="stat-row">
      ${_statBox("Composite", `${(r.composite_score||0).toFixed(1)}`, _color((r.composite_score||0)/100))}
      ${_statBox("Accuracy",  fmtPct(r.accuracy),  _color(r.accuracy||0))}
      ${_statBox("Macros",    fmtPct(accAvg),       _color(accAvg))}
      ${_statBox("Ing F1",    fmtPct(f1Avg),        _color(f1Avg))}
      ${_statBox("Weights",   fmtPct(wAvg),         _color(wAvg))}
      ${_statBox("Health",    fmtPct(hAvg),         _color(hAvg))}
      ${_statBox("Cost",      `$${(r.total_cost_usd||0).toFixed(4)}`, "var(--ink)")}
    </div>`;
}

function renderReviewRowList(r) {
  const rows = r.rows || [];
  if (rows.length === 0) return `<p class="muted">No rows.</p>`;
  return rows.map(rr => renderRowCard(rr, {
    state: REVIEW,
    modelId: r.model_id,
    toggleFn: toggleReviewRow,
    runId: r.id,
    datasetId: r.dataset_id,
  })).join("");
}

function avgFromRows(rows, key) {
  const vals = [];
  for (const rr of rows) {
    if (rr.error) continue;
    const sc = JSON.parse(rr.scores || "{}");
    let v;
    if (key.includes(".")) {
      const parts = key.split(".");
      v = sc[parts[0]] && sc[parts[0]][parts[1]];
    } else {
      v = sc[key];
    }
    if (typeof v === "number") vals.push(v);
  }
  return vals.length ? vals.reduce((a,b)=>a+b,0)/vals.length : 0;
}

async function renderReviewRows() {
  if (!REVIEW.runId) return;
  const r = await api.run(REVIEW.runId);
  // Keep REVIEW state in sync — earlier code paths may have skipped this.
  REVIEW.datasetId = r.dataset_id;
  REVIEW.model_id  = r.model_id || "";
  $("#review-rows").innerHTML = (r.rows||[]).map(rr => renderRowCard(rr, {
    state:   REVIEW,
    modelId: r.model_id,
    toggleFn:toggleReviewRow,
    runId:   r.id,
    datasetId: r.dataset_id,
  })).join("");
}

// ----- BENCHMARK -----
async function refreshBenchmarkForm() {
  CACHE.prompts  = await api.prompts();
  CACHE.datasets = await api.datasets();
  $("#r-prompt").innerHTML  = CACHE.prompts.map(p => `<option value="${p.id}">${escape(p.name)}</option>`).join("");
  $("#r-dataset").innerHTML = CACHE.datasets.map(d => `<option value="${d.id}">${escape(d.name)} (${d.n_rows})</option>`).join("");
  onProviderChange();
  $("#r-provider").onchange = onProviderChange;
  $("#refresh-models").onclick = (e) => { e.preventDefault(); fetchModels(); };
}

const PROVIDER_INFO = {
  openai:    {name: "OpenAI",    placeholder: "sk-...",                 envvar: "OPENAI_API_KEY",    keys_url: "https://platform.openai.com/api-keys"},
  anthropic: {name: "Anthropic", placeholder: "sk-ant-api03-...",       envvar: "ANTHROPIC_API_KEY", keys_url: "https://console.anthropic.com/settings/keys"},
  gemini:    {name: "Gemini",    placeholder: "AIza...",                envvar: "GEMINI_API_KEY",    keys_url: "https://aistudio.google.com/apikey"},
};

async function onProviderChange() {
  const prov = $("#r-provider").value;
  const local = (prov === "ollama" || prov === "lmstudio");
  $("#apikey-block").classList.toggle("hidden", local);
  $("#baseurl-block").classList.toggle("hidden", !local);
  // Always reset base_url on provider switch so a leftover localhost
  // URL doesn't get sent to a cloud SDK (causes "404 page not found").
  $("#r-baseurl").value = "";
  if (prov === "ollama")   $("#r-baseurl").value = "http://localhost:11434";
  if (prov === "lmstudio") $("#r-baseurl").value = "http://localhost:1234/v1";

  // Reset model selection when switching providers
  $("#r-model").value = "";
  $("#r-model-select").innerHTML = `<option value="">— loading —</option>`;

  if (!local) {
    const info = PROVIDER_INFO[prov] || {};
    $("#r-apikey").placeholder = info.placeholder || "paste your key here";
    const env = await api.envKey(prov);
    $("#apikey-hint").innerHTML = env.present
      ? `<span class="pill ok">found in .env</span>`
      : "";
    $("#apikey-help").innerHTML = env.present
      ? `Leave blank to use the <code>${info.envvar}</code> from .env, or paste a different key to override for this run only.`
      : `Paste your <strong>${info.name}</strong> key (<code>${info.envvar}</code>). It's used only for this run and never stored.${info.keys_url?` <a href="${info.keys_url}" target="_blank" rel="noopener">Get a key →</a>`:""}`;
  }
  fetchModels();
}

async function fetchModels() {
  const prov = $("#r-provider").value;
  const base = (prov === "ollama" || prov === "lmstudio") ? $("#r-baseurl").value : null;
  const sel = $("#r-model-select");
  const cnt = $("#models-count");
  sel.innerHTML = `<option value="">— loading —</option>`;
  cnt.textContent = "";
  let r;
  try {
    r = await api.models(prov, base);
  } catch (e) {
    sel.innerHTML = `<option value="">— error —</option>`;
    cnt.textContent = `(error: ${e.message})`;
    return;
  }
  const details = r.details || (r.models||[]).map(id => ({id, label: id, group:"current", input:0, output:0}));
  if (!details.length) {
    sel.innerHTML = `<option value="">— none found${r.error?`: ${escape(r.error)}`:""} —</option>`;
    cnt.textContent = `(0 models${r.error?` · ${escape(r.error)}`:""})`;
    return;
  }
  const groups = {current: [], legacy: []};
  details.forEach(m => (groups[m.group] || (groups.current)).push(m));

  const renderOpt = m => {
    const price = (m.input || m.output)
      ? ` — $${m.input}/$${m.output} per M`
      : "";
    const note = m.notes ? `  · ${m.notes}` : "";
    return `<option value="${escape(m.id)}" title="${escape(m.label)}${escape(note)}">${escape(m.label)}${price}</option>`;
  };

  let html = `<option value="">— pick a vision-capable model —</option>`;
  if (groups.current.length) {
    html += `<optgroup label="${prov === 'ollama' || prov === 'lmstudio' ? 'Available locally' : 'Current'}">${groups.current.map(renderOpt).join("")}</optgroup>`;
  }
  if (groups.legacy.length) {
    html += `<optgroup label="Legacy / older">${groups.legacy.map(renderOpt).join("")}</optgroup>`;
  }
  sel.innerHTML = html;
  cnt.textContent = `(${details.length} model${details.length===1?"":"s"})`;

  sel.onchange = () => {
    if (sel.value) $("#r-model").value = sel.value;
  };
  // Default to first current model
  const first = (groups.current[0] || groups.legacy[0]);
  if (!$("#r-model").value && first) {
    sel.value = first.id;
    $("#r-model").value = first.id;
  } else if ($("#r-model").value && details.find(m => m.id === $("#r-model").value)) {
    sel.value = $("#r-model").value;
  }
}

async function refreshLiveRunsList() {
  const box = $("#live-runs");
  if (!box) return;
  let runs;
  try { runs = await api.runs(); } catch { return; }
  const live = runs.filter(r => r.status === "running" || r.status === "paused" || r.status === "pending");
  if (!live.length) { box.classList.add("hidden"); box.innerHTML = ""; return; }
  box.classList.remove("hidden");
  box.innerHTML = `
    <div class="card" style="padding:12px 16px">
      <div class="muted small" style="margin-bottom:8px">Live runs (${live.length})</div>
      <div class="live-run-chips">
        ${live.map(r => {
          const cls = r.id === ACTIVE.runId ? "live-chip active" : "live-chip";
          const pct = r.n_rows ? Math.round(100*r.n_done/r.n_rows) : 0;
          return `<button class="${cls}" onclick="focusRun(${r.id})" title="${escape(r.prompt_name||"")}">
            <span class="pill ${r.status==='paused'?'run':'run'}">${r.status}</span>
            <span class="lr-id">#${r.id}</span>
            <span class="lr-model">${escape(r.model_id)}</span>
            <span class="lr-prog">${r.n_done}/${r.n_rows}<span class="muted small"> · ${pct}%</span></span>
          </button>`;
        }).join("")}
      </div>
    </div>`;
}

function focusRun(id) {
  ACTIVE.runId = id;
  pollRun(id);
}

async function startRun() {
  const provider = $("#r-provider").value;
  const isLocal = (provider === "ollama" || provider === "lmstudio");
  const body = {
    prompt_id:  parseInt($("#r-prompt").value),
    dataset_id: parseInt($("#r-dataset").value),
    provider:   provider,
    model_id:   $("#r-model").value.trim(),
    api_key:    $("#r-apikey").value.trim() || null,
    // base_url ONLY for local providers — cloud SDKs use their own defaults
    base_url:   isLocal ? ($("#r-baseurl").value.trim() || null) : null,
    max_rows:   $("#r-maxrows").value ? parseInt($("#r-maxrows").value) : null,
  };
  body.random_sample = $("#r-randsample").checked;
  const pin  = parseFloat($("#r-pricein").value);
  const pout = parseFloat($("#r-priceout").value);
  if (!isNaN(pin) || !isNaN(pout)) {
    body.pricing_override = {input: isNaN(pin)?0:pin, output: isNaN(pout)?0:pout};
  }
  if (!body.prompt_id || !body.dataset_id || !body.model_id) {
    return toast("Fill all required fields.");
  }
  const r = await api.startRun(body);
  if (!r.run_id) return toast("Failed to start.");
  toast(`Run #${r.run_id} started.`);
  ACTIVE.runId = r.run_id;
  ACTIVE.expanded = true;
  ACTIVE.expandedRows = new Set();
  ACTIVE.rawExpanded = new Set();
  pollRun(r.run_id);
}

async function controlRun(action, id) {
  if (action === "cancel" && !confirm("Cancel this run? It will stop after the current row.")) return;
  if (action === "pause")  await api.pauseRun(id);
  if (action === "resume") await api.resumeRun(id);
  if (action === "cancel") await api.cancelRun(id);
  toast(action[0].toUpperCase()+action.slice(1)+"d run #"+id);
  refreshLiveRunsList();
}

function statusPill(s) {
  const map = {completed:"ok", failed:"err", cancelled:"err", running:"run", paused:"run", pending:"run"};
  return `<span class="pill ${map[s]||"run"}">${s}</span>`;
}

function captureRowScroll() {
  const out = new Map();
  document.querySelectorAll(".raw-output[data-row-idx]").forEach(el => {
    if (el.scrollTop > 0) out.set(el.dataset.rowIdx, el.scrollTop);
  });
  return out;
}

function restoreRowScroll(state) {
  state.forEach((top, idx) => {
    const el = document.querySelector(`.raw-output[data-row-idx="${idx}"]`);
    if (el) el.scrollTop = top;
  });
}

let _lastRenderSig = "";
function _renderSig(r) {
  const rows = (r.rows||[]).map(rr =>
    `${rr.row_idx}:${rr.input_tokens||0}:${rr.output_tokens||0}:${rr.error?1:0}`
  ).join("|");
  return `${r.id}|${r.status}|${r.n_done}|${r.error||""}|${rows}`;
}

// Single-poll guarantee: only the most recently started loop is allowed to render.
let _pollToken = 0;
let _stopActivePoll = null;

function stopActivePoll() {
  if (_stopActivePoll) { try { _stopActivePoll(); } catch {} _stopActivePoll = null; }
}

async function pollRun(id) {
  stopActivePoll();
  const box = $("#active-run");
  // Restyle the active-run shell with the brand-tinted "card-live" treatment
  box.classList.remove("hidden");
  box.classList.add("card-live");
  const myToken = ++_pollToken;
  let active = true;
  _stopActivePoll = () => { active = false; };
  _lastRenderSig = "";

  const tick = async () => {
    if (!active || myToken !== _pollToken) return;
    let r;
    try { r = await api.run(id); }
    catch { if (active && myToken === _pollToken) setTimeout(tick, 2000); return; }
    if (!active || myToken !== _pollToken) return;  // raced with another poll start

    const sig = _renderSig(r);
    if (sig !== _lastRenderSig) {
      const scroll = captureRowScroll();
      box.innerHTML = renderActiveRun(r);
      restoreRowScroll(scroll);
      bindActiveRun(r);
      _lastRenderSig = sig;
    }
    const live = (r.status === "running" || r.status === "paused" || r.status === "pending");
    if (live && active && myToken === _pollToken) setTimeout(tick, 1500);
    else if (!live) refreshLiveRunsList();  // run terminated; refresh the list above
  };
  tick();
  refreshLiveRunsList();  // also refresh the multi-run header
}

function renderActiveRun(r) {
  ACTIVE.runId = r.id;
  ACTIVE.datasetId = r.dataset_id;
  ACTIVE.model_id = r.model_id || "";
  const live = (r.status === "running" || r.status === "paused" || r.status === "pending");
  const pct = r.n_rows ? Math.min(100, r.n_done/r.n_rows*100) : 0;
  const elapsed = r.finished_at ? (r.finished_at - r.started_at) : (Date.now()/1000 - r.started_at);
  const eta = (r.status === "running" && r.n_done > 0)
    ? ((elapsed / r.n_done) * (r.n_rows - r.n_done))
    : null;

  const ctrl = `
    <div style="display:flex;gap:6px">
      ${r.status === "running"  ? `<button class="btn-ghost" onclick="controlRun('pause', ${r.id})">⏸ Pause</button>` : ""}
      ${r.status === "paused"   ? `<button class="btn-ghost" onclick="controlRun('resume', ${r.id})">▶ Resume</button>` : ""}
      ${live                    ? `<button class="btn-ghost btn-danger-text" onclick="controlRun('cancel', ${r.id})">✕ Cancel</button>` : ""}
      <button class="btn-ghost" onclick="toggleExpanded()">${ACTIVE.expanded ? "▾ Collapse" : "▸ Expand"} details</button>
    </div>`;
  const cardClass = live ? "card card-live" : "card";

  const summary = `
    <div class="row-between" style="align-items:center;margin:0">
      <div>
        <h3 style="margin:0">Run #${r.id} · ${escape(r.model_id)} ${statusPill(r.status)}</h3>
        <div class="muted small">${escape(r.prompt_name||"")} · ${escape(r.provider)}</div>
      </div>
      ${ctrl}
    </div>
    <div class="bar" style="margin-top:14px"><span style="width:${pct}%"></span></div>
    <div class="run-stats">
      <div><span class="muted small">Progress</span><div class="num">${r.n_done}/${r.n_rows}</div></div>
      <div><span class="muted small">Accuracy</span><div class="num">${((r.accuracy||0)*100).toFixed(1)}%</div></div>
      <div><span class="muted small">Composite</span><div class="num">${(r.composite_score||0).toFixed(1)}</div></div>
      <div><span class="muted small">Avg latency</span><div class="num">${(r.avg_latency_ms/1000||0).toFixed(2)}s</div></div>
      <div><span class="muted small">Tokens</span><div class="num">${fmtNum(r.total_input_tokens)} / ${fmtNum(r.total_output_tokens)}</div></div>
      <div><span class="muted small">Cost</span><div class="num">$${(r.total_cost_usd||0).toFixed(4)}</div></div>
      <div><span class="muted small">Elapsed</span><div class="num">${fmtSec(elapsed)}</div></div>
      <div><span class="muted small">${live?"ETA":"Done in"}</span><div class="num">${eta!==null?fmtSec(eta):(r.finished_at?fmtSec(r.finished_at-r.started_at):"—")}</div></div>
    </div>
    ${r.error ? `<div class="error-box">${escape(r.error)}</div>` : ""}
  `;

  const rows = (r.rows||[]).slice().reverse(); // most recent first
  const detail = ACTIVE.expanded ? `
    <h4 style="margin:18px 0 8px">Live results <span class="muted small">(newest first)</span></h4>
    <div class="row-list">
      ${rows.length === 0
        ? `<div class="muted small">No rows completed yet…</div>`
        : rows.map(rr => renderRowCard(rr, {
            state: ACTIVE,
            modelId: r.model_id,
            runId: r.id,
            datasetId: r.dataset_id,
          })).join("")}
    </div>` : "";

  return summary + detail;
}

function renderPromoteToTruth(rr, runId, datasetId) {
  // Image id from image_ref (S3 URLs end with the id). For local-only datasets
  // (no template) we can't safely identify the row across re-uploads.
  const ref = rr.image_ref || "";
  const imageId = ref.startsWith("http") ? ref.split("/").pop() : ref;
  if (!imageId || !datasetId) {
    return `<div class="promote-row promote-disabled">
      <div class="muted small">
        Promote-to-truth not available: ${!imageId ? "row has no image_id" : "datasetId unknown"}.
      </div>
    </div>`;
  }
  return `
    <div class="promote-row">
      <div class="muted small">
        Disagree with the benchmark? Promote the model's prediction as the new truth for this image —
        future runs against this dataset will score against the corrected truth (the original Excel is untouched).
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px">
        <button class="btn-ghost" onclick="promoteToTruth(${rr.id}, ${runId}, ${datasetId}, '${escape(imageId)}', 'pred')">
          ⤴ Save model output as truth
        </button>
        <button class="btn-ghost" onclick="promoteToTruth(${rr.id}, ${runId}, ${datasetId}, '${escape(imageId)}', 'edit')">
          ✎ Edit truth manually
        </button>
        <span class="muted small" id="promote-status-${rr.row_idx}"></span>
      </div>
    </div>`;
}

// =====================================================================
// Truth editor — modal form. Same look as the rest of the tool.
// =====================================================================
const TRUTH_EDIT = { datasetId: null, imageId: null, runId: null, rowIdx: null,
                     truth: null, pred: null, imageUrl: null };
const NUTRIENT_KEYS = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"];
const ING_NUTRIENT_KEYS = ["calories","protein","carbohydrates","fat","sodium","sugar","fiber"];
const HEALTH_GRADES = ["A","B","C","D","E"];

async function promoteToTruth(rowResultId, runId, datasetId, imageId, mode) {
  const run = await api.run(runId);
  const rr = (run.rows || []).find(x => x.id === rowResultId);
  if (!rr) return toast("Row not found.");
  const op    = JSON.parse(rr.output_parsed || "{}");
  const truth = JSON.parse(rr.truth || "{}");
  const ref   = rr.image_ref || "";
  const imageUrl = ref.startsWith("http") ? ref : (ref ? `/images/${ref.split("/").pop()}` : null);

  TRUTH_EDIT.datasetId = datasetId;
  TRUTH_EDIT.imageId   = imageId;
  TRUTH_EDIT.runId     = runId;
  TRUTH_EDIT.rowIdx    = rr.row_idx;
  TRUTH_EDIT.pred      = op;
  TRUTH_EDIT.truth     = JSON.parse(JSON.stringify(truth));
  TRUTH_EDIT.imageUrl  = imageUrl;

  if (mode === "pred") {
    // Fast-path: directly save the model's prediction as the truth, no editor.
    const payload = {
      food: op.food || null,
      description: op.description || null,
      nutrition: op.nutrition || {},
      ingredients: op.ingredients || [],
      health_score: op.health_score || null,
    };
    const status = document.getElementById(`promote-status-${rr.row_idx}`);
    if (status) status.textContent = "saving…";
    const res = await api.saveCorrection({
      dataset_id: datasetId, image_id: imageId, truth: payload,
      source_run_id: runId, source_row_idx: rr.row_idx,
      note: `Promoted from run #${runId} row ${rr.row_idx}`,
    });
    if (status) status.innerHTML = `<span class="pill ok">✓ saved (correction #${res.id})</span>`;
    toast("Truth correction saved.");
    offerRescoreAfterCorrection(datasetId, runId);
    return;
  }

  // Full editor
  openTruthEditor();
}

function openTruthEditor() {
  // Seed the form with whatever truth the row has stored
  const t = TRUTH_EDIT.truth || {};
  const ings = (t.ingredients || []).map(i => normaliseIngredientForForm(i));
  const formState = {
    food: t.food || "",
    description: t.description || "",
    health_score: t.health_score || "",
    nutrition: NUTRIENT_KEYS.reduce((o,k) => (o[k] = (t.nutrition||{})[k] ?? "", o), {}),
    ingredients: ings.length ? ings : [emptyIngredient()],
  };
  TRUTH_EDIT._form = formState;
  document.getElementById("te-subtitle").textContent =
    `image ${TRUTH_EDIT.imageId.slice(0, 24)}…  · row ${TRUTH_EDIT.rowIdx}  · run #${TRUTH_EDIT.runId}`;
  renderTruthEditorBody();
  const m = document.getElementById("truth-editor");
  m.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function closeTruthEditor() {
  const m = document.getElementById("truth-editor");
  if (m) m.classList.add("hidden");
  document.body.style.overflow = "";
}

document.addEventListener("click", (e) => {
  if (e.target && e.target.matches && e.target.matches(".modal-backdrop[data-close]")) {
    closeTruthEditor();
  }
});

function emptyIngredient() {
  return {name:"", quantity:"", unit:"g",
          calories:"", protein:"", carbohydrates:"", fat:"",
          sodium:"", sugar:"", fiber:""};
}

function normaliseIngredientForForm(i) {
  return {
    name: i.name || "",
    quantity: i.quantity ?? "",
    unit: i.unit || "g",
    // Truth ingredients use canonical (protein_g) — coerce to the WILLMA names the form uses
    calories: i.calories ?? "",
    protein: i.protein ?? i.protein_g ?? "",
    carbohydrates: i.carbohydrates ?? i.carbs_g ?? "",
    fat: i.fat ?? i.fat_g ?? "",
    sodium: i.sodium ?? i.sodium_mg ?? "",
    sugar: i.sugar ?? i.sugar_g ?? "",
    fiber: i.fiber ?? i.fiber_g ?? "",
  };
}

function renderTruthEditorBody() {
  const f = TRUTH_EDIT._form;
  const body = document.getElementById("te-body");
  const imgHtml = TRUTH_EDIT.imageUrl
    ? `<img class="te-image" src="${escape(TRUTH_EDIT.imageUrl)}" referrerpolicy="no-referrer" />`
    : `<div class="te-image te-image-empty">no image</div>`;

  body.innerHTML = `
    <div class="te-grid">
      ${imgHtml}
      <div class="te-form-col">
        <label class="label">Food name <span class="lbl-truth">truth</span></label>
        <input id="te-food" type="text" value="${escape(f.food)}" />

        <label class="label">Description</label>
        <input id="te-desc" type="text" value="${escape(f.description)}" />

        <label class="label">Health grade</label>
        <div class="te-grade">
          ${HEALTH_GRADES.map(g => `
            <label class="te-grade-opt ${f.health_score === g ? "active" : ""}">
              <input type="radio" name="te-health" value="${g}" ${f.health_score === g ? "checked":""} />
              <span>${g}</span>
            </label>
          `).join("")}
          <label class="te-grade-opt ${!f.health_score ? "active" : ""}">
            <input type="radio" name="te-health" value="" ${!f.health_score ? "checked":""} />
            <span>—</span>
          </label>
        </div>
      </div>
    </div>

    <h4 class="rowcard-h" style="margin-top:18px">Nutrition (per serving)</h4>
    <div class="te-macros">
      ${NUTRIENT_KEYS.map(k => {
        const unit = k === "calories" ? "kcal" : k.endsWith("_mg") ? "mg" : "g";
        const lbl = k.replace("_g","").replace("_mg","");
        return `
          <div class="te-macro-cell">
            <label class="label">${lbl} <span class="muted">(${unit})</span></label>
            <input type="number" step="any" data-nut="${k}" value="${escape(f.nutrition[k])}" />
          </div>`;
      }).join("")}
    </div>

    <h4 class="rowcard-h" style="margin-top:18px;display:flex;justify-content:space-between;align-items:center">
      <span>Ingredients <span class="muted small">(${f.ingredients.length})</span></span>
      <button class="btn-ghost" type="button" onclick="teAddIngredient()">+ Add ingredient</button>
    </h4>
    <div id="te-ings">
      ${f.ingredients.map((ing, i) => renderIngredientForm(ing, i)).join("")}
    </div>
  `;

  // Wire up live updates
  body.querySelector("#te-food").oninput = e => f.food = e.target.value;
  body.querySelector("#te-desc").oninput = e => f.description = e.target.value;
  body.querySelectorAll('input[name="te-health"]').forEach(r => {
    r.onchange = e => {
      f.health_score = e.target.value || null;
      // Toggle "active" class
      body.querySelectorAll('.te-grade-opt').forEach(el => el.classList.remove("active"));
      r.closest(".te-grade-opt").classList.add("active");
    };
  });
  body.querySelectorAll('[data-nut]').forEach(inp => {
    inp.oninput = e => {
      const v = e.target.value;
      f.nutrition[inp.dataset.nut] = v === "" ? "" : parseFloat(v);
    };
  });
  // Ingredient bindings
  wireIngredientForms();
}

function renderIngredientForm(ing, i) {
  return `
    <div class="te-ing" data-i="${i}">
      <div class="te-ing-head">
        <input class="te-ing-name" type="text" placeholder="e.g. rice, white, cooked" value="${escape(ing.name)}" />
        <input class="te-ing-qty num" type="number" step="any" placeholder="qty" value="${escape(ing.quantity)}" />
        <select class="te-ing-unit">
          <option ${ing.unit==="g"?"selected":""}>g</option>
          <option ${ing.unit==="ml"?"selected":""}>ml</option>
        </select>
        <button class="btn-ghost" type="button" onclick="teToggleIngDetails(${i})" title="Per-ingredient nutrition">⌄</button>
        <button class="btn-ghost btn-danger-text" type="button" onclick="teRemoveIngredient(${i})" title="Remove">×</button>
      </div>
      <div class="te-ing-detail hidden" id="te-ing-detail-${i}">
        ${ING_NUTRIENT_KEYS.map(k => {
          const lbl = ({calories:"kcal",protein:"protein g",carbohydrates:"carbs g",
                        fat:"fat g",sodium:"sodium mg",sugar:"sugar g",fiber:"fiber g"})[k];
          return `
            <label class="label">${lbl}</label>
            <input class="te-ing-${k} num" type="number" step="any" value="${escape(ing[k] ?? "")}" />
          `;
        }).join("")}
      </div>
    </div>`;
}

function wireIngredientForms() {
  const f = TRUTH_EDIT._form;
  document.querySelectorAll(".te-ing").forEach(el => {
    const i = parseInt(el.dataset.i);
    el.querySelector(".te-ing-name").oninput = e => f.ingredients[i].name = e.target.value;
    el.querySelector(".te-ing-qty" ).oninput = e => f.ingredients[i].quantity = e.target.value === "" ? "" : parseFloat(e.target.value);
    el.querySelector(".te-ing-unit").onchange= e => f.ingredients[i].unit = e.target.value;
    ING_NUTRIENT_KEYS.forEach(k => {
      const inp = el.querySelector(".te-ing-" + k);
      if (inp) inp.oninput = e => f.ingredients[i][k] = e.target.value === "" ? "" : parseFloat(e.target.value);
    });
  });
}

function teToggleIngDetails(i) {
  const el = document.getElementById(`te-ing-detail-${i}`);
  if (el) el.classList.toggle("hidden");
}

function teAddIngredient() {
  TRUTH_EDIT._form.ingredients.push(emptyIngredient());
  renderTruthEditorBody();
}

function teRemoveIngredient(i) {
  TRUTH_EDIT._form.ingredients.splice(i, 1);
  if (!TRUTH_EDIT._form.ingredients.length) {
    TRUTH_EDIT._form.ingredients.push(emptyIngredient());
  }
  renderTruthEditorBody();
}

function teFillFromPred() {
  const op = TRUTH_EDIT.pred || {};
  const f = {
    food: op.food || "",
    description: op.description || "",
    health_score: op.health_score || "",
    nutrition: NUTRIENT_KEYS.reduce((o,k) => (o[k] = (op.nutrition||{})[k] ?? "", o), {}),
    ingredients: (op.ingredients || []).map(i => normaliseIngredientForForm(i)),
  };
  if (!f.ingredients.length) f.ingredients = [emptyIngredient()];
  TRUTH_EDIT._form = f;
  renderTruthEditorBody();
  toast("Loaded model output into the form.");
}

async function teReset() {
  if (!confirm("Remove the saved correction for this image and use the original Excel value?\n\nFuture runs will score against the source benchmark again.")) return;
  // Find existing correction by listing
  const list = await api.corrections(TRUTH_EDIT.datasetId);
  const c = list.find(x => x.image_id === TRUTH_EDIT.imageId);
  if (!c) { toast("No correction to remove."); return; }
  await api.delCorrection(c.id);
  toast("Correction removed.");
  closeTruthEditor();
  offerRescoreAfterCorrection(TRUTH_EDIT.datasetId, TRUTH_EDIT.runId);
}

async function teSave() {
  const f = TRUTH_EDIT._form;
  // Build canonical payload
  const nutrition = {};
  for (const k of NUTRIENT_KEYS) {
    if (f.nutrition[k] !== "" && f.nutrition[k] != null && !isNaN(f.nutrition[k])) {
      nutrition[k] = parseFloat(f.nutrition[k]);
    }
  }
  const ingredients = f.ingredients
    .filter(i => i.name && i.name.trim())
    .map(i => {
      const out = {name: i.name.trim(), unit: i.unit || "g"};
      if (i.quantity !== "" && i.quantity != null) out.quantity = parseFloat(i.quantity);
      for (const k of ING_NUTRIENT_KEYS) {
        if (i[k] !== "" && i[k] != null && !isNaN(i[k])) out[k] = parseFloat(i[k]);
      }
      return out;
    });
  const payload = {
    food: f.food.trim() || null,
    description: f.description.trim() || null,
    nutrition: nutrition,
    ingredients: ingredients,
    health_score: f.health_score || null,
  };
  try {
    const res = await api.saveCorrection({
      dataset_id: TRUTH_EDIT.datasetId,
      image_id:   TRUTH_EDIT.imageId,
      truth:      payload,
      source_run_id:  TRUTH_EDIT.runId,
      source_row_idx: TRUTH_EDIT.rowIdx,
      note: "Edited via truth editor",
    });
    toast(`Correction saved (#${res.id}).`);
    closeTruthEditor();
    // Update the inline status pill
    const status = document.getElementById(`promote-status-${TRUTH_EDIT.rowIdx}`);
    if (status) status.innerHTML = `<span class="pill ok">✓ saved (correction #${res.id})</span>`;
    offerRescoreAfterCorrection(TRUTH_EDIT.datasetId, TRUTH_EDIT.runId);
  } catch (e) {
    toast("Save failed: " + e.message);
  }
}

// =====================================================================
// Re-score plumbing (no API calls, just recompute scores from stored
// predictions against the corrected truth).
// =====================================================================
async function rescoreRun(runId) {
  toast(`Re-scoring run #${runId}…`);
  const r = await fetch(`/api/runs/${runId}/rescore`, {method:"POST"}).then(r=>r.json());
  toast(
    `Run #${runId} re-scored · ${r.rows_changed} row${r.rows_changed===1?"":"s"} changed · ` +
    `${(r.accuracy_before*100).toFixed(1)}% → ${(r.accuracy_after*100).toFixed(1)}%`
  );
  // Refresh the affected views
  if (LB && typeof reloadLeaderboardTable === "function") reloadLeaderboardTable();
  if (REVIEW.runId === runId) loadRunDrawer(runId);
  return r;
}

async function rescoreAllForDataset(datasetId) {
  toast("Re-scoring all runs against this dataset…");
  const r = await fetch(`/api/datasets/${datasetId}/rescore_all`, {method:"POST"}).then(r=>r.json());
  toast(`Re-scored ${r.n} run${r.n===1?"":"s"}.`);
  if (typeof reloadLeaderboardTable === "function") reloadLeaderboardTable();
  return r;
}

async function retryRowEntry(runId, rowIdx) {
  const btnId = `retry-btn-${rowIdx}`;
  const statusId = `retry-status-${rowIdx}`;
  const btn = document.getElementById(btnId);
  const statusEl = document.getElementById(statusId);
  if (btn) { btn.disabled = true; btn.textContent = "↺ Retrying…"; }
  if (statusEl) statusEl.textContent = "";

  try {
    const result = await api.retryRow(runId, rowIdx, null, null);
    if (result && result.detail) throw new Error(result.detail);

    toast(`Row ${rowIdx} retried successfully`);

    // Re-render just this row card in place
    const cardHead = document.querySelector(`.row-card-head[data-row-idx="${rowIdx}"]`);
    if (cardHead) {
      const card = cardHead.closest(".row-card");
      if (card) {
        // Determine which state/opts context we're in
        const isActiveRun = !!card.closest("#active-run");
        const state    = isActiveRun ? ACTIVE : REVIEW;
        const runIdCtx = isActiveRun ? ACTIVE.runId : REVIEW.runId;
        const dsId     = isActiveRun ? null : REVIEW.datasetId;
        const modelId  = isActiveRun ? ACTIVE.model_id : REVIEW.model_id;
        // Keep the row expanded
        state.expandedRows.add(rowIdx);
        const newHtml = renderRowCard(result, {
          state, modelId, runId: runIdCtx, datasetId: dsId,
          toggleFn: isActiveRun ? (idx => toggleRow(idx)) : (idx => toggleReviewRow(idx)),
        });
        card.outerHTML = newHtml;
      }
    }
    // Also refresh run-level stats in header if visible
    if (REVIEW.runId === runId && typeof renderRunDrawerHeader === "function") renderRunDrawerHeader();
    if (ACTIVE.runId === runId) pollRunOnce();
  } catch(e) {
    if (statusEl) statusEl.textContent = "Error: " + e.message;
    toast("Retry failed: " + e.message);
    if (btn) { btn.disabled = false; btn.textContent = "↺ Retry"; }
  }
}

function offerRescoreAfterCorrection(datasetId, runId) {
  // Quick prompt — minimal friction
  const yes = confirm(
    "Re-score the previous runs now so the leaderboard reflects the new truth?\n\n" +
    "(This recomputes scores against the saved model outputs — no model API calls, free + fast.)\n\n" +
    "Yes = re-score all runs against this dataset.\n" +
    "No  = leave previous runs as they were; only future runs use the new truth."
  );
  if (yes) rescoreAllForDataset(datasetId);
}

function _pct(v) { return (v*100).toFixed(1)+"%"; }
function _color(v) { return v>=0.85?"var(--green)":v>=0.5?"var(--copper)":"var(--red)"; }
function _statBox(label, value, color) {
  return `<div class="stat-box">
    <div class="muted small">${escape(label)}</div>
    <div class="stat-val" ${color?`style="color:${color}"`:""}>${value}</div>
  </div>`;
}

function renderRowCard(rr, opts) {
  // opts: {state, modelId, toggleFn, runId, datasetId}
  const state    = (opts && opts.state)    || ACTIVE;
  const modelId  = (opts && opts.modelId)  || rr.run_model_id || "";
  const toggleFn = (opts && opts.toggleFn) || (idx => toggleRow(idx));
  const runId    = (opts && opts.runId)    || ACTIVE.runId;
  const datasetId= (opts && opts.datasetId);

  const sc = JSON.parse(rr.scores||"{}");
  const op = JSON.parse(rr.output_parsed||"{}");
  const truth = JSON.parse(rr.truth||"{}");
  const overall = (sc.overall||0);
  const macros = (sc.macros_avg||0);
  const f1 = (sc.ingredient_f1||0);
  const weight = (sc.weight_acc||0);
  const health = sc.health || {};
  const ing = sc.ingredients || {};
  const ok = !rr.error;
  const expanded = state.expandedRows.has(rr.row_idx);
  const img = rr.image_ref || "";
  const imgSrc = img.startsWith("http") ? img : (img ? `/images/${img.split('/').pop()}` : null);
  const ts = rr.id ? new Date().toLocaleString() : "";

  // ---- top stat tiles ----
  const stats = [
    _statBox("Composite",     _pct(overall), _color(overall)),
    _statBox("Macros",        _pct(macros),  _color(macros)),
    truth.ingredients && truth.ingredients.length
      ? _statBox("Ingredient F1", _pct(f1), _color(f1)) : "",
    truth.ingredients && truth.ingredients.length
      ? _statBox("Weights",     _pct(weight), _color(weight)) : "",
    (health.score!=null)
      ? _statBox("Health score", _pct(health.score), _color(health.score)) : "",
    _statBox("Total cost",    "$"+(rr.cost_usd||0).toFixed(4), "var(--ink)"),
  ].filter(Boolean).join("");

  // ---- macros table (truth column tinted slate, pred column tinted copper) ----
  const macroRows = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"]
    .map(k => {
      const d = (sc.nutrition_detail||{})[k];
      if (!d) return "";
      const unit = k==="calories"?"kcal":k.endsWith("_mg")?"mg":"g";
      const lbl = k.replace("_g","").replace("_mg","").replace(/^./, c=>c.toUpperCase());
      const c = _color(d.score);
      const flag = d.score>=0.85?"✓":d.score>=0.5?"△":"✗";
      const tip = d.in_tol ? `within ±${d.allowed}` : (d.missing?"missing":`Δ=${d.delta} vs ±${d.allowed}`);
      return `<tr>
        <td>${lbl}</td>
        <td class="num col-truth">${d.truth==null?"—":d.truth+" "+unit}</td>
        <td class="num col-pred">${d.pred==null?"—":d.pred+" "+unit}</td>
        <td class="num" style="color:${c}" title="${escape(tip)}">${flag} ${_pct(d.score)}</td>
      </tr>`;
    }).join("");

  // ---- ingredient table (truth columns tinted slate, model columns tinted copper) ----
  let ingTable = "";
  if (truth.ingredients && truth.ingredients.length) {
    const matchRows = (ing.matches||[]).map(m => {
      const wc = m.weight_score==null ? "muted" : "";
      const wPct = m.weight_score==null ? "—" : _pct(m.weight_score);
      const wColor = m.weight_score==null ? "var(--muted)" : _color(m.weight_score);
      return `<tr>
        <td class="col-truth">${escape(m.truth_name||"")}</td>
        <td class="num col-truth">${m.truth_qty==null?"—":m.truth_qty+" "+escape(m.unit||"g")}</td>
        <td class="col-pred">${escape(m.pred_name||"")}</td>
        <td class="num col-pred">${m.pred_qty==null?"—":m.pred_qty+" "+escape(m.unit||"g")}</td>
        <td class="num" style="color:${_color(m.name_sim)}">${_pct(m.name_sim)}</td>
        <td class="num ${wc}" style="color:${wColor}">${wPct}</td>
      </tr>`;
    }).join("");
    const missingRows = (ing.unmatched_truth||[]).map(u => `<tr>
      <td class="col-truth" style="color:var(--red)">${escape(u.name||"")}</td>
      <td class="num col-truth" style="color:var(--red)">${u.qty==null?"—":u.qty+" "+escape(u.unit||"g")}</td>
      <td colspan="4" class="muted small" style="font-style:italic">✗ model didn't return this ingredient</td>
    </tr>`).join("");
    const extraRows = (ing.unmatched_pred||[]).map(u => `<tr>
      <td colspan="2" class="muted small" style="font-style:italic">✗ not in benchmark truth</td>
      <td class="col-pred" style="color:var(--copper-deep)">${escape(u.name||"")}</td>
      <td class="num col-pred" style="color:var(--copper-deep)">${u.qty==null?"—":u.qty+" "+escape(u.unit||"g")}</td>
      <td colspan="2"></td>
    </tr>`).join("");
    ingTable = `
      <h4 class="rowcard-h">Ingredients · precision ${_pct(ing.precision||0)} · recall ${_pct(ing.recall||0)}</h4>
      <table class="ing-table">
        <thead><tr>
          <th class="th-truth" colspan="2">▎WILLMA truth</th>
          <th class="th-pred" colspan="2">▎Model — ${escape(modelId||"prediction")}</th>
          <th>Name match</th><th>Qty acc</th>
        </tr><tr class="ing-table-sub">
          <th class="col-truth">Ingredient</th>
          <th class="col-truth">Qty</th>
          <th class="col-pred">Ingredient</th>
          <th class="col-pred">Qty</th>
          <th></th><th></th>
        </tr></thead>
        <tbody>${matchRows}${missingRows}${extraRows}</tbody>
      </table>`;
  }

  // ---- tokens & cost panel ----
  const cost = rr.cost_usd||0;
  // We don't have separate input/output cost — derive proportional split from totals
  // Best-effort: use pricing not directly accessible here; just show the totals we have.
  const tokenPanel = `
    <h4 class="rowcard-h">Tokens & cost</h4>
    <div class="kv-grid">
      <div>Input tokens</div><div class="num">${fmtNum(rr.input_tokens||0)}</div>
      <div>Output tokens</div><div class="num"><b>${fmtNum(rr.output_tokens||0)}</b></div>
      <div>Latency</div><div class="num">${(rr.latency_ms/1000).toFixed(2)} s</div>
      <div><b>Total cost</b></div><div class="num"><b>$${cost.toFixed(4)}</b></div>
    </div>`;

  const healthLine = (health.score!=null) ? `
    <div class="small" style="margin-top:8px">
      <span class="muted">Health grade —</span>
      <span class="lbl-truth">truth</span> <code>${escape(health.truth||"—")}</code>
      <span class="muted">→</span>
      <span class="lbl-pred">model</span> <code>${escape(health.pred||"—")}</code>
      ${health.delta!=null ? ` <span class="muted">(Δ ${health.delta} grade${health.delta===1?"":"s"})</span>` : ""}
    </div>` : "";

  const mealLine = `
    <div class="small" style="margin-top:6px">
      <div><span class="lbl-truth">truth</span> ${escape(truth.food||"—")}</div>
      <div><span class="lbl-pred">model</span> <span style="color:var(--ink)">${escape(op.food||"—")}</span></div>
    </div>`;

  return `
    <div class="row-card ${ok?"":"row-err"}">
      <div class="row-card-head" data-row-idx="${rr.row_idx}">
        ${imgSrc
          ? `<img class="thumb" src="${imgSrc}" data-src="${escape(imgSrc)}" data-caption="${escape(truth.food||op.food||"")}" loading="lazy" referrerpolicy="no-referrer" title="click to expand" />`
          : `<div class="thumb" style="background:var(--paper-2)"></div>`}
        <div style="flex:1;min-width:0">
          <div class="rc-names">
            <div class="rc-name"><span class="lbl-truth">truth</span> <strong>${escape(truth.food || "—")}</strong></div>
            <div class="rc-name"><span class="lbl-pred">model</span> <span class="${(sc.name_sim||0)>=0.85?"":"rc-name-pred"}">${escape(op.food || "—")}</span></div>
          </div>
          <div class="muted small">row ${rr.row_idx}${modelId?` · ${escape(modelId)}`:""} · ${(rr.latency_ms/1000).toFixed(2)} s · $${cost.toFixed(4)}${rr.error?` · <span style="color:var(--red)">error</span>`:""}</div>
        </div>
        <div style="text-align:right;min-width:90px">
          <div class="num" style="font-size:18px;font-weight:600;color:${_color(overall)}">${(overall*100).toFixed(1)}%</div>
          <div class="bar" style="width:80px"><span style="width:${overall*100}%;background:${_color(overall)}"></span></div>
        </div>
        <div class="caret">${expanded?"▾":"▸"}</div>
      </div>
      ${expanded ? `
        <div class="row-card-body">
          ${rr.error ? `<div class="error-box">${escape(rr.error)}</div>` : `
            <div class="stat-row">${stats}</div>
            <div class="rowcard-grid">
              <div>
                <h4 class="rowcard-h">Macros vs benchmark</h4>
                <table class="macro-table">
                  <thead><tr><th>Macro</th><th>Benchmark</th><th>Model</th><th>Accuracy</th></tr></thead>
                  <tbody>${macroRows}</tbody>
                </table>
                ${healthLine}
                ${mealLine}
              </div>
              <div>${tokenPanel}</div>
            </div>
            ${ingTable}
          `}
          <div class="retry-bar">
            <button class="btn-ghost btn-retry" id="retry-btn-${rr.row_idx}"
                    onclick="retryRowEntry(${runId}, ${rr.row_idx})">↺ Retry this row</button>
            <span class="muted small" id="retry-status-${rr.row_idx}"></span>
          </div>
          ${renderPromoteToTruth(rr, runId, datasetId)}
          <details style="margin-top:14px" ${state.rawExpanded.has(rr.row_idx) ? "open" : ""}
                   ontoggle="onRawToggle(event, ${rr.row_idx})">
            <summary class="muted small" style="cursor:pointer">Raw model response</summary>
            <pre class="raw-output" data-row-idx="${rr.row_idx}">${escape(rr.output_text||"")}</pre>
          </details>
        </div>
      ` : ""}
    </div>`;
}

function bindActiveRun(_r) { /* no-op for now */ }

function toggleExpanded() {
  ACTIVE.expanded = !ACTIVE.expanded;
  if (ACTIVE.runId) pollRunOnce();
}
function toggleRow(idx) {
  if (ACTIVE.expandedRows.has(idx)) ACTIVE.expandedRows.delete(idx);
  else ACTIVE.expandedRows.add(idx);
  pollRunOnce();
}
async function pollRunOnce() {
  if (!ACTIVE.runId) return;
  const r = await api.run(ACTIVE.runId);
  $("#active-run").innerHTML = renderActiveRun(r);
}

function fmtSec(s) {
  if (s == null || isNaN(s)) return "—";
  if (s < 60) return s.toFixed(1)+"s";
  const m = Math.floor(s/60), sec = Math.round(s%60);
  return `${m}m ${sec}s`;
}

// ----- helpers -----
function escape(s) {
  return String(s ?? "")
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;").replace(/'/g,"&#39;");
}
function fmtNum(n) { return (n||0).toLocaleString(); }

// ─── Export ──────────────────────────────────────────────────────────────────

async function openExportModal() {
  // Verify the server is reachable before triggering the download
  toast("Generating export bundle…");
  try {
    const probe = await fetch("/api/export?include_row_results=true&include_history=true");
    if (!probe.ok) {
      const txt = await probe.text().catch(() => probe.statusText);
      toast("Export failed: " + txt, "error");
      return;
    }
    const blob = await probe.blob();
    const cd   = probe.headers.get("Content-Disposition") || "";
    const match = cd.match(/filename="?([^"]+)"?/);
    const fname = match ? match[1] : "benchmark-export.zip";
    const blobUrl = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = fname;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(blobUrl), 10000);
    toast("Export downloaded ✓");
  } catch (e) {
    toast("Export failed — is the server running? (" + e.message + ")", "error");
  }
}

// ─── Import ──────────────────────────────────────────────────────────────────

const IMPORT_STATE = { token: null, file: null };

function openImportModal() {
  IMPORT_STATE.token = null;
  IMPORT_STATE.file  = null;
  _imStep("upload");
  $("#im-apply-btn").classList.add("hidden");
  $("#im-upload-status").textContent = "";
  $("#im-machine-label").textContent = "";
  api.machineId().then(r => {
    $("#im-machine-label").textContent = `This machine: ${r.machine_id.slice(0,8)}…`;
  }).catch(() => {});
  $("#import-modal").classList.remove("hidden");
}

function closeImportModal() {
  $("#import-modal").classList.add("hidden");
}

function _imStep(step) {
  ["upload","preview","result"].forEach(s => {
    $("#im-step-"+s).classList.toggle("hidden", s !== step);
  });
}

// Wire up file picker + drag-drop once DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  const dropZone = $("#im-drop-zone");
  const fileInput = $("#im-file-input");
  const browseLink = $("#im-browse");

  if (!dropZone) return;

  browseLink.addEventListener("click", e => { e.preventDefault(); fileInput.click(); });
  fileInput.addEventListener("change", () => {
    if (fileInput.files[0]) _imHandleFile(fileInput.files[0]);
  });
  dropZone.addEventListener("dragover", e => { e.preventDefault(); dropZone.classList.add("drag-over"); });
  dropZone.addEventListener("dragleave", () => dropZone.classList.remove("drag-over"));
  dropZone.addEventListener("drop", e => {
    e.preventDefault();
    dropZone.classList.remove("drag-over");
    const f = e.dataTransfer.files[0];
    if (f) _imHandleFile(f);
  });
});

async function _imHandleFile(file) {
  if (!file.name.endsWith(".zip")) {
    $("#im-upload-status").textContent = "Please select a .zip bundle file.";
    return;
  }
  IMPORT_STATE.file = file;
  $("#im-upload-status").textContent = `Analysing ${file.name}…`;
  try {
    const plan = await api.importPreview(file);
    if (plan.error) throw new Error(plan.error);
    IMPORT_STATE.token = plan.import_token;
    _imRenderPlan(plan);
    _imStep("preview");
    $("#im-apply-btn").classList.remove("hidden");
  } catch(e) {
    $("#im-upload-status").textContent = "Error: " + e.message;
  }
}

function _imRenderPlan(plan) {
  const sm = (plan.source_machine_id || "unknown").slice(0,8);
  const d  = new Date((plan.exported_at || 0) * 1000).toLocaleString();

  const rowOf = (icon, label, val) =>
    `<tr><td class="ip-icon">${icon}</td><td>${label}</td><td class="ip-val">${val}</td></tr>`;

  const pRows = (plan.prompts.details || []).map(p =>
    `<div class="ip-detail ${p.status==="new"?"ip-new":"ip-match"}">
      ${p.status==="new" ? "✓ new" : "◯ match"} — <strong>${escape(p.name)}</strong>
      ${p.local_id ? `<span class="muted small">→ local #${p.local_id}</span>` : ""}
    </div>`).join("");

  const dRows = (plan.datasets.details || []).map(d =>
    `<div class="ip-detail ${d.status==="new"?"ip-new":"ip-match"}">
      ${d.status==="new" ? "✓ new" : "◯ match"} — <strong>${escape(d.name)}</strong>
      ${d.local_name && d.local_name!==d.name ? `<span class="muted small">(local name: ${escape(d.local_name)})</span>` : ""}
      ${d.n_import_corrections ? `<span class="muted small">· ${d.n_import_corrections} correction(s) to merge</span>` : ""}
    </div>`).join("");

  const byStatus = Object.entries(plan.runs.by_status || {})
    .map(([s,n]) => `${n} ${s}`).join(", ") || "";

  const warnings = (plan.warnings || []).map(w =>
    `<div class="ip-warn">⚠ ${escape(w)}</div>`).join("");

  $("#im-plan").innerHTML = `
    <div class="ip-header">
      <div>From machine <code>${escape(sm)}…</code></div>
      <div class="muted small">Exported ${escape(d)}</div>
    </div>
    <table class="ip-table">
      <tbody>
        ${rowOf(plan.prompts.new?"✓":"◯",  "Prompts",     `<b>${plan.prompts.new}</b> new · ${plan.prompts.matched} already here`)}
        ${rowOf(plan.datasets.new?"✓":"◯", "Datasets",    `<b>${plan.datasets.new}</b> new · ${plan.datasets.matched} matched by file hash`)}
        ${rowOf(plan.runs.new?"✓":"◯",     "Runs",        `<b>${plan.runs.new}</b> new · ${plan.runs.already_imported} already imported · ${plan.runs.skipped_orphan||0} skipped${byStatus?" ("+byStatus+")":""}`)}
        ${rowOf(plan.corrections.new?"✓":"◯","Corrections",`<b>${plan.corrections.new}</b> new · ${plan.corrections.merged} merged into history${plan.corrections.conflicts?" · ⚠ "+plan.corrections.conflicts+" conflict(s)":""}`)}
      </tbody>
    </table>
    ${pRows ? `<div class="ip-details">${pRows}</div>` : ""}
    ${dRows ? `<div class="ip-details">${dRows}</div>` : ""}
    ${warnings}
    <div class="muted small" style="margin-top:12px">Click <strong>Apply import</strong> to merge this data into your local instance. This cannot be undone, but imported runs are tagged and re-importing the same bundle is safe (idempotent).</div>
  `;
}

async function applyImport() {
  if (!IMPORT_STATE.token) return;
  const btn = $("#im-apply-btn");
  btn.disabled = true;
  btn.textContent = "Applying…";
  try {
    const result = await api.importApply(IMPORT_STATE.token);
    if (result.error || result.detail) throw new Error(result.error || result.detail);
    _imStep("result");
    btn.classList.add("hidden");
    $("#im-result").innerHTML = `
      <div class="ip-success">Import applied successfully.</div>
      <table class="ip-table" style="margin-top:12px">
        <tbody>
          <tr><td>Prompts</td><td class="ip-val">${result.prompts.new} new · ${result.prompts.matched} matched</td></tr>
          <tr><td>Datasets</td><td class="ip-val">${result.datasets.new} new · ${result.datasets.matched} matched</td></tr>
          <tr><td>Runs</td><td class="ip-val">${result.runs.new} imported · ${result.runs.skipped} skipped</td></tr>
          <tr><td>Corrections</td><td class="ip-val">${result.corrections.new} new · ${result.corrections.merged} merged</td></tr>
        </tbody>
      </table>
    `;
    toast(`Import done — ${result.runs.new} run(s) added`);
    // Refresh leaderboard and runs list
    if (typeof reloadLeaderboardTable === "function") reloadLeaderboardTable();
    if (typeof refreshRuns === "function") refreshRuns();
  } catch(e) {
    btn.disabled = false;
    btn.textContent = "Apply import";
    toast("Import failed: " + e.message);
  }
}

bootstrap();
