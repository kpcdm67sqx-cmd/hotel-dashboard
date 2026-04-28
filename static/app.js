/* ── State ── */
let currentHotelId = null;
let chartOcc = null;
let chartRev = null;
let statusInterval = null;
let currentView = "daily";  // "daily" | "otb"
let lastOtbUpdate = null;   // track OTB import timestamp for auto-refresh

const MONTH_NAMES = ["", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                         "Jul", "Ago", "Set", "Out", "Nov", "Dez"];

/* ── Init ── */
document.addEventListener("DOMContentLoaded", async () => {
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById("date-picker").value = today;

  const thirtyAgo = new Date(Date.now() - 30 * 86400_000).toISOString().slice(0, 10);
  document.getElementById("range-start").value = thirtyAgo;
  document.getElementById("range-end").value = today;

  await wakeServer();

  loadSummary();
  populateOTBSelector();
  pollStatus();
  statusInterval = setInterval(pollStatus, 10_000);
});

/* ── Wake-up: poll /ping until server responds, show banner if slow ── */
async function wakeServer() {
  const banner  = document.getElementById("wake-banner");
  const wakeMsg = document.getElementById("wake-msg");
  let elapsed = 0;
  while (true) {
    try {
      const res = await Promise.race([
        fetch("/ping"),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 4000))
      ]);
      if (res.ok) break;
    } catch (_) {
      elapsed += 4;
      banner.classList.remove("hidden");
      wakeMsg.textContent = `A acordar o servidor… (${elapsed}s)`;
    }
  }
  banner.classList.add("hidden");
}

/* ── View switching ── */
function switchView(view) {
  currentView = view;
  document.getElementById("section-daily").classList.toggle("hidden", view !== "daily");
  document.getElementById("section-otb").classList.toggle("hidden", view !== "otb");
  document.getElementById("nav-daily").classList.toggle("active", view === "daily");
  document.getElementById("nav-otb").classList.toggle("active", view === "otb");

  if (view === "otb") {
    const sel = document.getElementById("otb-hotel-select");
    if (sel.value) loadOTB();
  }
}

/* ── Summary (all hotels) ── */
async function loadSummary() {
  const date = document.getElementById("date-picker").value;
  const rows = await fetchJSON(`/api/summary?date=${date}`);

  renderKPIs(rows);
  renderSummaryTable(rows);
}

function renderKPIs(rows) {
  const container = document.getElementById("kpi-cards");
  const filled = rows.filter(r => r.occupancy_pct != null);

  const avgOcc = filled.length
    ? (filled.reduce((s, r) => s + (r.occupancy_pct || 0), 0) / filled.length).toFixed(1) + "%"
    : "—";
  const totalRev = filled.reduce((s, r) => s + (r.room_revenue || 0), 0);
  const avgPrice = filled.filter(r => r.avg_room_price).length
    ? (filled.reduce((s, r) => s + (r.avg_room_price || 0), 0) /
       filled.filter(r => r.avg_room_price).length).toFixed(0) + " €"
    : "—";

  container.innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Hotéis com dados</div>
      <div class="kpi-value">${filled.length}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Ocupação Média</div>
      <div class="kpi-value">${avgOcc}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Revenue Total</div>
      <div class="kpi-value">${formatEur(totalRev)}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Preço Médio</div>
      <div class="kpi-value">${avgPrice}</div>
    </div>
  `;
}

function renderSummaryTable(rows) {
  const tbody = document.getElementById("summary-tbody");
  const noData = document.getElementById("no-data-msg");
  const table = document.getElementById("summary-table");

  const hasAnyData = rows.some(r => r.data_date != null);
  if (!rows.length) {
    tbody.innerHTML = "";
    table.classList.add("hidden");
    noData.classList.remove("hidden");
    return;
  }

  table.classList.remove("hidden");
  noData.classList.add("hidden");
  if (!hasAnyData) noData.classList.remove("hidden");

  tbody.innerHTML = rows.map(r => {
    const hasData = r.data_date != null;
    return `
    <tr class="clickable-row" onclick="showHotel(${r.hotel_id}, '${escHtml(r.hotel_name)}')">
      <td><strong>${escHtml(r.hotel_name)}</strong></td>
      <td class="num">${hasData ? (r.occupancy_rooms ?? "—") : '<span class="no-report">Sem relatório</span>'}</td>
      <td class="num ${occClass(r.occupancy_pct)}">${hasData ? fmtPct(r.occupancy_pct) : "—"}</td>
      <td class="num">${formatEur(r.total_revenue)}</td>
      <td class="num">${hasData ? formatEur(r.room_revenue) : "—"}</td>
      <td class="num">${formatEur(r.fb_revenue)}</td>
      <td class="num">${hasData ? formatEur(r.avg_room_price) : "—"}</td>
      <td class="num">${r.rooms_out_of_service != null ? r.rooms_out_of_service : "—"}</td>
      <td class="num">${formatEur(r.pending_balance)}</td>
    </tr>`;
  }).join("");
}

/* ── Hotel detail ── */
function showHotel(id, name) {
  currentHotelId = id;
  document.getElementById("view-all").classList.add("hidden");
  document.getElementById("view-hotel").classList.remove("hidden");
  document.getElementById("hotel-title").textContent = name;
  loadHotelCharts();
}

function showAllHotels() {
  document.getElementById("view-hotel").classList.add("hidden");
  document.getElementById("view-all").classList.remove("hidden");
  currentHotelId = null;
}

async function loadHotelCharts() {
  if (!currentHotelId) return;
  const start = document.getElementById("range-start").value;
  const end   = document.getElementById("range-end").value;
  const rows  = await fetchJSON(`/api/hotel/${currentHotelId}/metrics?start=${start}&end=${end}`);

  const labels   = rows.map(r => r.date);
  const occData  = rows.map(r => r.occupancy_pct ?? null);
  const revData  = rows.map(r => r.room_revenue  ?? null);

  renderChart("chart-occ", chartOcc, c => { chartOcc = c; }, labels, occData, "% Ocupação", "#2d6a9f", true);
  renderChart("chart-rev", chartRev, c => { chartRev = c; }, labels, revData, "Receita (€)", "#e8a020", false);

  renderHotelTable(rows);
}

function renderChart(canvasId, existing, setter, labels, data, label, color, fill) {
  if (existing) existing.destroy();
  const ctx = document.getElementById(canvasId).getContext("2d");
  setter(new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label,
        data,
        borderColor: color,
        backgroundColor: fill ? color + "22" : "transparent",
        fill,
        tension: 0.3,
        pointRadius: labels.length > 60 ? 0 : 3,
        spanGaps: true,
      }]
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxTicksLimit: 10, font: { size: 11 } } },
        y: { beginAtZero: false, ticks: { font: { size: 11 } } }
      }
    }
  }));
}

function renderHotelTable(rows) {
  const tbody = document.getElementById("hotel-tbody");
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.date}</td>
      <td class="num">${r.occupancy_rooms ?? "—"}</td>
      <td class="num ${occClass(r.occupancy_pct)}">${fmtPct(r.occupancy_pct)}</td>
      <td class="num">${formatEur(r.total_revenue)}</td>
      <td class="num">${formatEur(r.room_revenue)}</td>
      <td class="num">${formatEur(r.fb_revenue)}</td>
      <td class="num">${formatEur(r.avg_room_price)}</td>
      <td class="num">${r.rooms_out_of_service != null ? r.rooms_out_of_service : "—"}</td>
      <td class="num">${formatEur(r.pending_balance)}</td>
    </tr>
  `).join("");
}

/* ── OTB ── */
async function populateOTBSelector() {
  const hotels = await fetchJSON("/api/otb/summary");
  const sel = document.getElementById("otb-hotel-select");
  const prevValue = sel.value;

  // Clear all except placeholder
  while (sel.options.length > 1) sel.remove(1);

  hotels.forEach(h => {
    const opt = document.createElement("option");
    opt.value = h.hotel_id;
    opt.textContent = h.hotel_name;
    sel.appendChild(opt);
  });

  // Restore previous selection or auto-select first
  if (prevValue && [...sel.options].some(o => o.value === prevValue)) {
    sel.value = prevValue;
  } else if (hotels.length) {
    sel.value = hotels[0].hotel_id;
  }

  if (currentView === "otb" && sel.value) loadOTB();
}

// Column groups for OTB tables
// Each group: { label, cols: [{key, label, fmt}] }
const OTB_COL_GROUPS = [
  {
    label: "Ocupação",
    cols: [
      { key: "occ_pct_current",    label: "2026",  fmt: "pct" },
      { key: "occ_pct_comparison", label: "Comp.", fmt: "pct" },
      { key: "variance_nights",    label: "Var. Noites", fmt: "int_signed" },
      { key: "variance_pct",       label: "Var. %",      fmt: "pct_signed" },
    ]
  },
  {
    label: "Receita Total",
    cols: [
      { key: "total_revenue_current",    label: "2026",  fmt: "eur" },
      { key: "total_revenue_comparison", label: "Comp.", fmt: "eur" },
      { key: "total_revenue_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "total_revenue_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
  {
    label: "Receita Quartos",
    cols: [
      { key: "room_revenue_current",    label: "2026",  fmt: "eur" },
      { key: "room_revenue_comparison", label: "Comp.", fmt: "eur" },
      { key: "room_revenue_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "room_revenue_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
  {
    label: "Receita F&B",
    cols: [
      { key: "fb_revenue_current",    label: "2026",  fmt: "eur" },
      { key: "fb_revenue_comparison", label: "Comp.", fmt: "eur" },
      { key: "fb_revenue_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "fb_revenue_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
  {
    label: "Rec. Outros",
    cols: [
      { key: "other_revenue_current",    label: "2026",  fmt: "eur" },
      { key: "other_revenue_comparison", label: "Comp.", fmt: "eur" },
      { key: "other_revenue_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "other_revenue_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
  {
    label: "Rec. SPA",
    cols: [
      { key: "spa_revenue_current",    label: "2026",  fmt: "eur" },
      { key: "spa_revenue_comparison", label: "Comp.", fmt: "eur" },
      { key: "spa_revenue_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "spa_revenue_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
  {
    label: "ADR",
    cols: [
      { key: "adr_current",    label: "2026",  fmt: "eur" },
      { key: "adr_comparison", label: "Comp.", fmt: "eur" },
      { key: "adr_variance",   label: "Var.",  fmt: "eur_signed" },
      { key: "adr_var_pct",    label: "Var. %", fmt: "pct_signed" },
    ]
  },
];

async function loadOTB() {
  const sel = document.getElementById("otb-hotel-select");
  const hotelId = sel.value;
  const noData  = document.getElementById("otb-no-data");
  const content = document.getElementById("otb-content");
  const dateLabel = document.getElementById("otb-date-label");

  if (!hotelId) {
    content.classList.add("hidden");
    noData.classList.add("hidden");
    return;
  }

  const rows = await fetchJSON(`/api/otb/${hotelId}`);

  if (!rows.length) {
    content.classList.add("hidden");
    noData.classList.remove("hidden");
    dateLabel.textContent = "";
    return;
  }

  content.classList.remove("hidden");
  noData.classList.add("hidden");

  const otbDate = rows[0].otb_date;
  dateLabel.textContent = otbDate ? `OTB de ${formatDate(otbDate)}` : "";

  const byType = { sdly: [], closed_month: [], budget: [] };
  rows.forEach(r => { if (byType[r.analysis_type]) byType[r.analysis_type].push(r); });

  renderOTBTable("otb-sdly-head",   "otb-sdly-tbody",   byType.sdly);
  renderOTBTable("otb-closed-head", "otb-closed-tbody", byType.closed_month);
  renderOTBTable("otb-budget-head", "otb-budget-tbody", byType.budget);

  // Load insights asynchronously
  document.getElementById("otb-insights").classList.add("hidden");
  fetchJSON(`/api/otb/${hotelId}/insights`).then(renderOTBInsights);
}

function renderOTBTable(theadId, tbodyId, rows) {
  const thead = document.getElementById(theadId);
  const tbody = document.getElementById(tbodyId);
  const totalCols = 1 + OTB_COL_GROUPS.reduce((s, g) => s + g.cols.length, 0);

  if (!rows.length) {
    thead.innerHTML = "";
    tbody.innerHTML = `<tr><td colspan="${totalCols}" class="no-data" style="padding:16px">Sem dados</td></tr>`;
    return;
  }

  // Build two-row header: group labels (rowspan 1 / colspan N) + col labels
  const groupRow = document.createElement("tr");
  const colRow   = document.createElement("tr");

  // Month column spans both rows
  const thMonth = document.createElement("th");
  thMonth.rowSpan = 2;
  thMonth.textContent = "Mês";
  groupRow.appendChild(thMonth);

  OTB_COL_GROUPS.forEach((g, gi) => {
    const thGroup = document.createElement("th");
    thGroup.colSpan = g.cols.length;
    thGroup.className = "num otb-group-header" + (gi > 0 ? " group-sep" : "");
    thGroup.textContent = g.label;
    groupRow.appendChild(thGroup);

    g.cols.forEach((c, ci) => {
      const th = document.createElement("th");
      th.className = "num" + (ci === 0 && gi > 0 ? " group-sep" : "");
      th.textContent = c.label;
      colRow.appendChild(th);
    });
  });

  thead.innerHTML = "";
  thead.appendChild(groupRow);
  thead.appendChild(colRow);

  // Sort: months 1-12, then total (month=0)
  const sorted = [...rows].sort((a, b) => (a.month === 0 ? 13 : a.month) - (b.month === 0 ? 13 : b.month));

  tbody.innerHTML = sorted.map(r => {
    const isTotal = r.month === 0;
    const monthLabel = isTotal ? "<strong>Total</strong>" : (MONTH_NAMES[r.month] || r.month);

    const cells = OTB_COL_GROUPS.map((g, gi) =>
      g.cols.map((c, ci) => {
        const sep = ci === 0 && gi > 0 ? " group-sep" : "";
        return `<td class="num${sep}">${fmtOTBCell(r[c.key], c.fmt)}</td>`;
      }).join("")
    ).join("");

    return `<tr${isTotal ? ' class="total-row"' : ''}><td>${monthLabel}</td>${cells}</tr>`;
  }).join("");
}

function renderOTBInsights(data) {
  const card = document.getElementById("otb-insights");
  if (!data || (!data.changes?.length && !data.suggestions?.length)) {
    card.classList.add("hidden");
    return;
  }

  // Header subtitle
  const subtitle = data.previous_date
    ? `vs OTB de ${formatDate(data.previous_date)}`
    : "sem OTB anterior disponível";
  document.querySelector("#insights-changes .insights-title").textContent =
    `📊 Alterações ${subtitle}`;

  // Changes list
  const changesList = document.getElementById("insights-changes-list");
  if (data.changes?.length) {
    changesList.innerHTML = data.changes.map(c => {
      const sign     = c.nights_delta >= 0 ? "+" : "";
      const occSign  = c.occ_delta_pp >= 0 ? "+" : "";
      const cls      = c.nights_delta >= 0 ? "ins-pos" : "ins-neg";
      const revCls   = c.rev_delta >= 0 ? "var-pos" : "var-neg";
      const revTxt   = c.rev_delta !== 0
        ? ` · receita <span class="${revCls}">${c.rev_delta >= 0 ? "+" : ""}${formatEur(c.rev_delta)}</span>`
        : "";
      const adrSign  = c.adr_delta >= 0 ? "+" : "";
      const adrCls   = c.adr_delta >= 0 ? "var-pos" : "var-neg";
      const adrTxt   = c.adr_delta != null && c.adr_delta !== 0
        ? ` · ADR <span class="${adrCls}">${adrSign}${formatEur(c.adr_delta)}</span>`
        : "";
      const nightsCls = c.nights_delta >= 0 ? "var-pos" : "var-neg";
      const occCls    = c.occ_delta_pp >= 0 ? "var-pos" : "var-neg";
      return `<li class="${cls}">
        <strong>${c.label}</strong>:
        <span class="${nightsCls}">${sign}${c.nights_delta} noites</span>
        (<span class="${occCls}">${occSign}${c.occ_delta_pp}pp occ</span>)${revTxt}${adrTxt}
      </li>`;
    }).join("");
  } else {
    changesList.innerHTML = "<li class='ins-neutral'>Sem alterações significativas face à semana anterior.</li>";
  }

  // Suggestions list
  const suggList = document.getElementById("insights-suggestions-list");
  if (data.suggestions?.length) {
    const icons = { high: "🔴", medium: "🟡", warning: "⚠️", opportunity: "🟢" };
    suggList.innerHTML = data.suggestions.map(s =>
      `<li class="ins-${s.priority}">${icons[s.priority] || "•"} ${escHtml(s.text)}</li>`
    ).join("");
  } else {
    suggList.innerHTML = "<li class='ins-neutral'>Sem alertas para o período em análise.</li>";
  }

  card.classList.remove("hidden");
}

function fmtOTBCell(v, fmt) {
  if (v == null || (typeof v === "number" && isNaN(v))) return "—";
  const sign = v > 0 ? "+" : "";
  const cls  = fmt.includes("signed") ? (v >= 0 ? ' class="var-pos"' : ' class="var-neg"') : "";
  const wrap = cls ? `<span${cls}>` : "";
  const end  = cls ? "</span>" : "";

  let text;
  switch (fmt) {
    case "pct":        text = fmtPct(v); break;
    case "pct_signed": text = sign + fmtPct(v); break;
    case "eur":        text = formatEur(v); break;
    case "eur_signed": text = sign + formatEur(Math.abs(v)); break;
    case "int_signed": text = sign + Math.round(v).toLocaleString("pt-PT"); break;
    default:           text = String(v);
  }
  return wrap + text + end;
}

/* ── Import / status ── */
async function reimport() {
  const btn = document.getElementById("btn-reimport");
  btn.disabled = true;
  await fetchJSON("/api/reimport", "POST");
  pollStatus();
}

async function pollStatus() {
  const s = await fetchJSON("/api/status");
  const imp = s.import;

  const bar  = document.getElementById("import-bar");
  const inner = document.getElementById("import-bar-inner");
  const msg   = document.getElementById("import-msg");
  const btn   = document.getElementById("btn-reimport");
  const ts    = document.getElementById("last-update");

  if (imp.running) {
    bar.classList.remove("hidden");
    const pct = imp.total ? Math.round((imp.progress / imp.total) * 100) : 0;
    inner.style.width = pct + "%";
    msg.textContent = imp.message;
  } else {
    bar.classList.add("hidden");
    btn.disabled = false;
    if (imp.message && imp.message.startsWith("Concluído")) {
      loadSummary();
      populateOTBSelector().then(() => { if (currentView === "otb") loadOTB(); });
    }
  }

  // Auto-refresh OTB view when new data is detected by the watcher
  if (s.last_otb_update && s.last_otb_update !== lastOtbUpdate) {
    if (lastOtbUpdate !== null) {
      // Data changed since last poll — reload OTB selector and view
      populateOTBSelector().then(() => { if (currentView === "otb") loadOTB(); });
    }
    lastOtbUpdate = s.last_otb_update;
  }

  if (s.last_update) {
    const d = new Date(s.last_update.replace(" ", "T"));
    ts.textContent = "Atualizado: " + d.toLocaleString("pt-PT");
  }
}

/* ── Helpers ── */
function formatEur(v) {
  if (v == null || isNaN(v)) return "—";
  return Number(v).toLocaleString("pt-PT", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + " €";
}

function fmtPct(v) {
  if (v == null || isNaN(v)) return "—";
  return (Number(v) * (Math.abs(v) <= 1 ? 100 : 1)).toFixed(1) + "%";
}

function formatDate(iso) {
  if (!iso) return iso;
  const [y, m, d] = iso.split("-");
  return `${d}/${m}/${y}`;
}

function occClass(v) {
  if (v == null) return "";
  const pct = v <= 1 ? v * 100 : v;
  return pct >= 70 ? "badge-green" : pct < 40 ? "badge-red" : "";
}

function escHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

async function fetchJSON(url, method = "GET", retries = 3) {
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await Promise.race([
        fetch(url, { method }),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 12000))
      ]);
      if (!res.ok) return method === "GET" ? [] : {};
      return res.json();
    } catch (_) {
      if (i === retries) return method === "GET" ? [] : {};
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}
