/* ── State ── */
let currentHotelId = null;
let chartOcc = null;
let chartRev = null;
let statusInterval = null;
let currentView = "daily";  // "daily" | "otb"
let lastOtbUpdate = null;   // track OTB import timestamp for auto-refresh
let _insightsCache = {};    // { hotelId: {data, fetchedAt} } — avoids re-fetching on every poll
let _insightsFetching = {}; // { hotelId: true } — prevent parallel requests for same hotel
let _otbRevChart = null;    // Chart.js instance for OTB revenue trend

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
  document.getElementById("section-reviews").classList.toggle("hidden", view !== "reviews");
  document.getElementById("nav-daily").classList.toggle("active", view === "daily");
  document.getElementById("nav-otb").classList.toggle("active", view === "otb");
  document.getElementById("nav-reviews").classList.toggle("active", view === "reviews");

  if (view === "otb") {
    const sel = document.getElementById("otb-hotel-select");
    if (sel.value) loadOTB();
  }
  if (view === "reviews" && !_revTabsBuilt) initReviews();
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

async function loadOTB(forceInsights = false) {
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

  renderOTBRevChart(byType);

  // Load insights — keep old content visible while fetching (no flicker)
  _loadOTBInsights(hotelId, forceInsights);
}

async function _loadOTBInsights(hotelId, force) {
  // Skip if already fetching for this hotel
  if (_insightsFetching[hotelId]) return;

  const cached = _insightsCache[hotelId];
  const cacheAge = cached ? (Date.now() - cached.fetchedAt) / 1000 : Infinity;

  // Use cache if fresh (< 5 min) and not forced
  if (!force && cached && cacheAge < 300) {
    renderOTBInsights(cached.data);
    return;
  }

  _insightsFetching[hotelId] = true;
  try {
    const data = await fetchJSON(`/api/otb/${hotelId}/insights`);
    _insightsCache[hotelId] = { data, fetchedAt: Date.now() };
    renderOTBInsights(data);
  } finally {
    delete _insightsFetching[hotelId];
  }
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

function renderOTBRevChart(byType) {
  const card = document.getElementById("otb-rev-chart-card");
  const canvas = document.getElementById("otb-rev-chart");

  // Build month-indexed maps (months 1-12 only, skip total=0)
  const sdlyRows   = (byType.sdly         || []).filter(r => r.month >= 1 && r.month <= 12);
  const budgetRows = (byType.budget        || []).filter(r => r.month >= 1 && r.month <= 12);
  const closedRows = (byType.closed_month  || []).filter(r => r.month >= 1 && r.month <= 12);

  // Use sdly for current + SDLY, budget for budget comparison
  const byMonth = m => arr => (arr.find(r => r.month === m) || {});

  const labels   = MONTH_NAMES.slice(1); // Jan…Dez
  const current  = labels.map((_, i) => byMonth(i+1)(sdlyRows).total_revenue_current   ?? null);
  const prevYear = labels.map((_, i) => byMonth(i+1)(sdlyRows).total_revenue_comparison ?? null);
  const budget   = labels.map((_, i) => byMonth(i+1)(budgetRows).total_revenue_comparison ?? null);

  // All nulls → hide chart
  const hasData = current.some(v => v != null) || budget.some(v => v != null);
  if (!hasData) { card.classList.add("hidden"); return; }
  card.classList.remove("hidden");

  if (_otbRevChart) { _otbRevChart.destroy(); _otbRevChart = null; }

  _otbRevChart = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "OTB Atual",
          data: current,
          borderColor: "#003580",
          backgroundColor: "rgba(0,53,128,0.08)",
          borderWidth: 2.5,
          pointRadius: 4,
          pointHoverRadius: 6,
          tension: 0.3,
          fill: false,
        },
        {
          label: "Ano Anterior (fechado)",
          data: prevYear,
          borderColor: "#888",
          backgroundColor: "transparent",
          borderWidth: 2,
          borderDash: [6, 3],
          pointRadius: 3,
          pointHoverRadius: 5,
          tension: 0.3,
          fill: false,
        },
        {
          label: "Budget 2026",
          data: budget,
          borderColor: "#e6a817",
          backgroundColor: "transparent",
          borderWidth: 2,
          borderDash: [3, 3],
          pointRadius: 3,
          pointHoverRadius: 5,
          tension: 0.3,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top", labels: { boxWidth: 20, font: { size: 12 } } },
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              if (v == null) return null;
              return ` ${ctx.dataset.label}: ${v.toLocaleString("pt-PT", { style: "currency", currency: "EUR", maximumFractionDigits: 0 })}`;
            },
          },
        },
      },
      scales: {
        x: { grid: { color: "rgba(0,0,0,0.05)" } },
        y: {
          grid: { color: "rgba(0,0,0,0.05)" },
          ticks: {
            callback: v => v >= 1000 ? (v/1000).toFixed(0)+"k €" : v+"€",
          },
        },
      },
    },
  });
}

function renderOTBInsights(data) {
  const card = document.getElementById("otb-insights");
  if (!data || !data.changes?.length) {
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

/* ════════════════════════════════════════════════════════════
   REVIEWS MODULE
   ════════════════════════════════════════════════════════════ */

let _revTabsBuilt   = false;
let _revHotelId     = null;
let _revHotelName   = "";
let _revAllScores   = [];   // scores data for single hotel
let _revComplaints  = [];   // all complaints for single hotel
let revTrendChart   = null;
let revVolumeChart  = null;

const PLAT_LABEL = {
  booking: "Booking",
  google: "Google"
};
const PLAT_COLOR = {
  booking: "#003580",
  google: "#4285f4"
};
const PLAT_ORDER = ["booking", "google"];

async function syncReviews(platform) {
  const btns   = document.querySelectorAll(".rev-sync-bar .btn-primary");
  const status = document.getElementById("rev-google-status");
  btns.forEach(b => b.disabled = true);
  const label = platform === "google" ? "Google Reviews" : "TripAdvisor";
  status.textContent = `A sincronizar ${label}…`;
  const url = platform === "google" ? "/api/google-reviews/sync" : "/api/tripadvisor/sync";
  const res = await fetchJSON(url, "POST");
  if (res.error) {
    status.textContent = "Erro: " + res.error;
  } else {
    status.textContent = `${label} sincronizado — dados disponíveis em instantes.`;
    setTimeout(() => {
      if (_revHotelId) loadRevHotel(_revHotelId);
      else loadRevAllHotels();
      status.textContent = "";
    }, 5000);
  }
  btns.forEach(b => b.disabled = false);
}

async function initReviews() {
  _revTabsBuilt = true;
  const today = new Date();
  const ym = today.toISOString().slice(0, 7);
  document.getElementById("rev-period").value = ym;
  document.getElementById("rev-hotel-period").value = ym;

  const hotels = await fetchJSON("/api/hotels");
  const tabs = document.getElementById("rev-hotel-tabs");
  tabs.innerHTML = `<button class="rev-tab active" onclick="selectRevHotel(null, this)">Todos os Hotéis</button>`;
  hotels.forEach(h => {
    const btn = document.createElement("button");
    btn.className = "rev-tab";
    btn.textContent = h.name;
    btn.onclick = (e) => selectRevHotel(h.id, e.currentTarget, h.name);
    tabs.appendChild(btn);
  });

  loadRevAllHotels();
}

function selectRevHotel(hotelId, btn, hotelName) {
  document.querySelectorAll(".rev-tab").forEach(t => t.classList.remove("active"));
  btn.classList.add("active");
  _revHotelId = hotelId;
  _revHotelName = hotelName || "";

  const allView    = document.getElementById("rev-view-all");
  const hotelView  = document.getElementById("rev-view-hotel");

  if (!hotelId) {
    allView.classList.remove("hidden");
    hotelView.classList.add("hidden");
    loadRevAllHotels();
  } else {
    allView.classList.add("hidden");
    hotelView.classList.remove("hidden");
    document.getElementById("rev-hotel-title").textContent = hotelName;
    loadRevHotel(hotelId);
  }
}

function reloadRevHotel() {
  if (_revHotelId) loadRevHotel(_revHotelId);
}

async function loadRevAllHotels() {
  const data = await fetchJSON("/api/reviews/summary");
  renderRevHotelGrid(data);
}

function renderRevHotelGrid(data) {
  const grid = document.getElementById("rev-hotel-grid");
  if (!data.length) {
    grid.innerHTML = `<p class="no-data" style="padding:24px">
      Sem dados de Reviews.<br/>Coloque o ficheiro <strong>reviews.xlsx</strong>
      na pasta <strong>[Hotel]/Reviews/</strong> no OneDrive e clique em <strong>Reimportar tudo</strong>.
    </p>`;
    return;
  }

  // Group by hotel
  const hotelMap = {};
  data.forEach(r => {
    if (!hotelMap[r.hotel_id]) hotelMap[r.hotel_id] = { id: r.hotel_id, name: r.hotel_name, plats: {} };
    hotelMap[r.hotel_id].plats[r.platform] = r;
  });

  grid.innerHTML = Object.values(hotelMap).map(hotel => {
    const platsHtml = PLAT_ORDER.map(p => {
      const d = hotel.plats[p];
      const score = d?.score != null ? d.score.toFixed(1) : "—";
      return `<div class="rev-platform-score${d ? "" : " rev-platform-empty"}">
        <span class="rev-plat-name">${PLAT_LABEL[p] || p}</span>
        <span class="rev-plat-score">${score}</span>
      </div>`;
    }).join("");

    const alertLevel = _hotelAlertLevel(hotel.plats);
    return `<div class="rev-hotel-card" onclick="selectRevHotelById(${hotel.id}, '${escHtml(hotel.name)}')">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
        <div class="rev-hotel-card-name" style="flex:1">${escHtml(hotel.name)}</div>
        <span class="rev-alert-dot ${alertLevel}"></span>
      </div>
      <div class="rev-platform-scores">${platsHtml}</div>
    </div>`;
  }).join("");
}

function selectRevHotelById(hotelId, hotelName) {
  const btn = [...document.querySelectorAll(".rev-tab")].find(b => b.textContent === hotelName);
  if (btn) selectRevHotel(hotelId, btn, hotelName);
}

function _hotelAlertLevel(plats) {
  let worst = "ok";
  Object.values(plats).forEach(d => {
    if (d.score == null) return;
    if (d.score < 7.0) { worst = "critical"; return; }
    if (d.score < 8.0 && worst !== "critical") worst = "warning";
  });
  return worst;
}

async function loadRevHotel(hotelId) {
  const period = document.getElementById("rev-hotel-period").value;
  const periodDate = period ? period + "-01" : null;

  const [scores, complaints, keywords, compset] = await Promise.all([
    fetchJSON(`/api/reviews/${hotelId}/scores`),
    fetchJSON(`/api/reviews/${hotelId}/complaints${periodDate ? "?period=" + periodDate : ""}`),
    fetchJSON(`/api/reviews/${hotelId}/keywords${periodDate ? "?period=" + periodDate : ""}`),
    fetchJSON(`/api/reviews/${hotelId}/compset${periodDate ? "?period=" + periodDate : ""}`),
  ]);

  _revAllScores  = scores;
  _revComplaints = complaints;

  const latestPeriod = _latestPeriod(scores);

  renderRevScoreCards(scores, latestPeriod);
  renderRevTrendChart(scores);
  renderRevVolumeChart(scores);
  renderRevResponseBars(scores, latestPeriod);
  renderRevSentiment(complaints);
  renderRevComplaints(complaints, "");
  renderRevWordCloud(keywords);
  renderRevAlerts(scores, latestPeriod);
  renderRevCompset(compset);
}

function _latestPeriod(scores) {
  if (!scores.length) return null;
  return scores.reduce((max, r) => r.period > max ? r.period : max, scores[0].period);
}

function _prevPeriod(period) {
  if (!period) return null;
  const [y, m] = period.slice(0, 7).split("-").map(Number);
  const prev = new Date(y, m - 2, 1);
  return prev.toISOString().slice(0, 7) + "-01";
}

function _splyPeriod(period) {
  if (!period) return null;
  return (parseInt(period.slice(0, 4)) - 1) + period.slice(4);
}

function renderRevScoreCards(scores, latestPeriod) {
  const container = document.getElementById("rev-score-cards");
  if (!scores.length) { container.innerHTML = ""; return; }

  const byPlat = {};
  scores.forEach(r => {
    if (!byPlat[r.platform]) byPlat[r.platform] = [];
    byPlat[r.platform].push(r);
  });

  const prevPeriod = _prevPeriod(latestPeriod);
  const splyPeriod = _splyPeriod(latestPeriod);

  container.innerHTML = PLAT_ORDER.filter(p => byPlat[p]).map(p => {
    const hist = byPlat[p].sort((a, b) => a.period < b.period ? -1 : 1);
    const cur  = hist.find(r => r.period === latestPeriod);
    const prev = hist.find(r => r.period === prevPeriod);
    const sply = hist.find(r => r.period === splyPeriod);

    if (!cur) return "";

    const score = cur.score != null ? cur.score.toFixed(1) : "—";
    const reviews = cur.num_reviews ? `${cur.num_reviews} reviews` : "";

    const deltaMoM  = _delta(cur.score, prev?.score);
    const deltaSPLY = _delta(cur.score, sply?.score);

    const alertDot = cur.score < 7.0 ? "critical" : cur.score < 8.0 ? "warning" : "ok";

    return `<div class="rev-score-card">
      <span class="rev-alert-dot ${alertDot}"></span>
      <div class="rev-score-card-platform">${PLAT_LABEL[p] || p}</div>
      <div class="rev-score-card-value">${score}</div>
      <div class="rev-score-card-reviews">${reviews}</div>
      <div class="rev-deltas">
        ${_deltaChip(deltaMoM, "vs mês ant.")}
        ${_deltaChip(deltaSPLY, "vs ano ant.")}
      </div>
    </div>`;
  }).join("");
}

function _delta(cur, prev) {
  if (cur == null || prev == null) return null;
  return Math.round((cur - prev) * 100) / 100;
}

function _deltaChip(delta, label) {
  if (delta === null) return "";
  const sign  = delta > 0 ? "+" : "";
  const cls   = delta > 0 ? "pos" : delta < 0 ? "neg" : "neu";
  const arrow = delta > 0 ? "▲" : delta < 0 ? "▼" : "=";
  return `<div>
    <div class="rev-delta ${cls}">${arrow} ${sign}${delta.toFixed(2)}</div>
    <div class="rev-delta-label">${label}</div>
  </div>`;
}

function renderRevTrendChart(scores) {
  if (revTrendChart) { revTrendChart.destroy(); revTrendChart = null; }
  const ctx = document.getElementById("rev-chart-trend").getContext("2d");

  const periods = [...new Set(scores.map(r => r.period))].sort();
  const labels  = periods.map(p => { const [y, m] = p.slice(0, 7).split("-"); return MONTH_NAMES[+m] + "/" + y.slice(2); });

  const datasets = PLAT_ORDER.filter(p => scores.some(r => r.platform === p)).map(p => {
    const dataMap = {};
    scores.filter(r => r.platform === p).forEach(r => { dataMap[r.period] = r.score; });
    return {
      label: PLAT_LABEL[p] || p,
      data: periods.map(per => dataMap[per] ?? null),
      borderColor: PLAT_COLOR[p] || "#999",
      backgroundColor: "transparent",
      borderWidth: 2, pointRadius: 3, tension: 0.3, spanGaps: true,
    };
  });

  revTrendChart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      plugins: { legend: { position: "bottom", labels: { font: { size: 11 } } } },
      scales: {
        x: { ticks: { maxTicksLimit: 12, font: { size: 10 } } },
        y: { ticks: { font: { size: 10 } } },
      },
    },
  });
}

function renderRevVolumeChart(scores) {
  if (revVolumeChart) { revVolumeChart.destroy(); revVolumeChart = null; }
  const ctx = document.getElementById("rev-chart-volume").getContext("2d");

  const periods = [...new Set(scores.map(r => r.period))].sort();
  const labels  = periods.map(p => { const [y, m] = p.slice(0, 7).split("-"); return MONTH_NAMES[+m] + "/" + y.slice(2); });

  const volMap = {};
  scores.forEach(r => { volMap[r.period] = (volMap[r.period] || 0) + (r.num_reviews || 0); });

  revVolumeChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Total reviews",
        data: periods.map(p => volMap[p] || 0),
        backgroundColor: "#2d6a9f55",
        borderColor: "#2d6a9f",
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxTicksLimit: 12, font: { size: 10 } } },
        y: { beginAtZero: true, ticks: { font: { size: 10 } } },
      },
    },
  });
}

function renderRevResponseBars(scores, latestPeriod) {
  const container = document.getElementById("rev-response-bars");
  const cur = scores.filter(r => r.period === latestPeriod);
  if (!cur.length) { container.innerHTML = `<p class="no-data">Sem dados.</p>`; return; }

  container.innerHTML = PLAT_ORDER.filter(p => cur.some(r => r.platform === p)).map(p => {
    const d = cur.find(r => r.platform === p);
    const rate  = d?.response_rate  != null ? d.response_rate  : null;
    const hours = d?.avg_response_hours != null ? d.avg_response_hours : null;

    const ratePct  = rate  != null ? Math.min(rate, 100)  : 0;
    const rateCls  = rate == null ? "warn" : rate >= 80 ? "good" : rate >= 50 ? "warn" : "bad";
    const rateStr  = rate  != null ? rate.toFixed(0) + "%" : "—";
    const hoursStr = hours != null ? (hours < 24 ? hours.toFixed(0) + "h" : (hours / 24).toFixed(1) + "d") : "—";

    return `<div class="rev-response-row">
      <div class="rev-response-label">
        <span>${PLAT_LABEL[p] || p}</span>
        <span>${rateStr} · ${hoursStr}</span>
      </div>
      <div class="rev-bar-track"><div class="rev-bar-fill ${rateCls}" style="width:${ratePct}%"></div></div>
      <div class="rev-response-sub">Taxa de resposta · Tempo médio de resposta</div>
    </div>`;
  }).join("");
}

function renderRevSentiment(complaints) {
  const container = document.getElementById("rev-sentiment");
  if (!complaints.length) {
    container.innerHTML = `<p class="no-data">Sem dados de sentimento.</p>`;
    return;
  }

  const counts = { positivo: 0, neutro: 0, negativo: 0 };
  complaints.forEach(c => {
    const s = c.sentiment?.toLowerCase();
    if (s in counts) counts[s] += c.volume;
    else counts.neutro += c.volume;
  });

  const total = Object.values(counts).reduce((a, b) => a + b, 0) || 1;
  const posP = (counts.positivo / total * 100).toFixed(0);
  const neuP = (counts.neutro   / total * 100).toFixed(0);
  const negP = (counts.negativo / total * 100).toFixed(0);

  container.innerHTML = `
    <div class="rev-sentiment-bar" title="${posP}% pos · ${neuP}% neu · ${negP}% neg">
      <div class="rev-sent-pos" style="width:${posP}%"></div>
      <div class="rev-sent-neu" style="width:${neuP}%"></div>
      <div class="rev-sent-neg" style="width:${negP}%"></div>
    </div>
    <div class="rev-sentiment-legend">
      <span><span class="rev-sent-dot" style="background:var(--green)"></span>Positivo ${posP}% (${counts.positivo})</span>
      <span><span class="rev-sent-dot" style="background:#bdc3c7"></span>Neutro ${neuP}% (${counts.neutro})</span>
      <span><span class="rev-sent-dot" style="background:var(--red)"></span>Negativo ${negP}% (${counts.negativo})</span>
    </div>`;
}

function renderRevComplaints(complaints, deptFilter) {
  const tbody = document.getElementById("rev-complaints-tbody");
  let rows = complaints;
  if (deptFilter) rows = rows.filter(c => c.department === deptFilter);

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="no-data" style="padding:16px">Sem queixas para este filtro.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.slice(0, 50).map((c, i) => {
    const sent = (c.sentiment || "neutro").toLowerCase();
    return `<tr>
      <td class="num">${i + 1}</td>
      <td>${escHtml(c.department)}</td>
      <td>${escHtml(c.complaint)}</td>
      <td class="num">${c.volume}</td>
      <td><span class="rev-sent-badge ${sent}">${sent}</span></td>
    </tr>`;
  }).join("");
}

function filterRevComplaints() {
  const dept = document.getElementById("rev-dept-filter").value;
  renderRevComplaints(_revComplaints, dept);
}

function renderRevWordCloud(keywords) {
  const container = document.getElementById("rev-wordcloud");
  if (!keywords.length) {
    container.innerHTML = `<p class="no-data">Sem palavras-chave para este período.</p>`;
    return;
  }

  const maxFreq = Math.max(...keywords.map(k => k.frequency));
  container.innerHTML = keywords.slice(0, 60).map(k => {
    const size   = 0.75 + (k.frequency / maxFreq) * 1.8;
    const sent   = (k.sentiment || "neutro").toLowerCase();
    return `<span class="rev-word ${sent}" style="font-size:${size.toFixed(2)}em"
      title="${k.frequency} menções">${escHtml(k.keyword)}</span>`;
  }).join("");
}

function renderRevAlerts(scores, latestPeriod) {
  const container = document.getElementById("rev-alerts-list");
  const alerts = [];

  const prevPeriod = _prevPeriod(latestPeriod);
  const cur  = scores.filter(r => r.period === latestPeriod);
  const prev = scores.filter(r => r.period === prevPeriod);

  cur.forEach(d => {
    const prevD = prev.find(r => r.platform === d.platform);
    const plat  = PLAT_LABEL[d.platform] || d.platform;

    if (d.score != null) {
      if (d.score < 7.0) {
        alerts.push({ level: "critical", icon: "🔴", title: `${plat}: Score crítico`, text: `Score atual ${d.score.toFixed(1)} abaixo de 7.0.` });
      } else if (d.score < 8.0) {
        alerts.push({ level: "warning", icon: "🟡", title: `${plat}: Score baixo`, text: `Score atual ${d.score.toFixed(1)} — requer atenção.` });
      }
    }

    if (prevD?.score != null && d.score != null) {
      const drop = prevD.score - d.score;
      if (drop >= 0.3) {
        alerts.push({ level: "critical", icon: "🔴", title: `${plat}: Queda abrupta`, text: `Descida de ${drop.toFixed(2)} pontos face ao mês anterior.` });
      } else if (drop >= 0.1) {
        alerts.push({ level: "warning", icon: "🟡", title: `${plat}: Queda moderada`, text: `Descida de ${drop.toFixed(2)} pontos face ao mês anterior.` });
      } else if (drop <= -0.2) {
        alerts.push({ level: "info", icon: "🟢", title: `${plat}: Subida significativa`, text: `Subida de ${Math.abs(drop).toFixed(2)} pontos face ao mês anterior.` });
      }
    }

    if (d.response_rate != null && d.response_rate < 25) {
      alerts.push({ level: "critical", icon: "🔴", title: `${plat}: Taxa de resposta muito baixa`, text: `Apenas ${d.response_rate.toFixed(0)}% de respostas a reviews.` });
    } else if (d.response_rate != null && d.response_rate < 50) {
      alerts.push({ level: "warning", icon: "🟡", title: `${plat}: Taxa de resposta baixa`, text: `${d.response_rate.toFixed(0)}% de respostas — objetivo mínimo: 80%.` });
    }
  });

  if (!alerts.length) {
    container.innerHTML = `<div class="rev-alert info"><span class="rev-alert-icon">🟢</span><div class="rev-alert-text">Sem alertas ativos para este período.</div></div>`;
    return;
  }

  alerts.sort((a, b) => (a.level === "critical" ? 0 : a.level === "warning" ? 1 : 2) - (b.level === "critical" ? 0 : b.level === "warning" ? 1 : 2));
  container.innerHTML = alerts.map(a => `
    <div class="rev-alert ${a.level}">
      <span class="rev-alert-icon">${a.icon}</span>
      <div class="rev-alert-text"><strong>${escHtml(a.title)}</strong>${escHtml(a.text)}</div>
    </div>`).join("");
}

function renderRevCompset(compset) {
  const tbody  = document.getElementById("rev-compset-tbody");
  const empty  = document.getElementById("rev-compset-empty");
  if (!compset.length) {
    tbody.innerHTML = "";
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");

  const latestP = compset.reduce((max, r) => r.period > max ? r.period : max, compset[0].period);
  const rows = compset.filter(r => r.period === latestP).sort((a, b) => (a.our_rank || 99) - (b.our_rank || 99));

  tbody.innerHTML = rows.map(r => {
    const rank = r.our_rank;
    const cls  = rank === 1 ? "rank1" : rank === 2 ? "rank2" : rank === 3 ? "rank3" : "rankN";
    const isUs = r.competitor.toLowerCase().includes("nós") || r.competitor.toLowerCase().includes("nos ");
    return `<tr${isUs ? ' class="rev-our-hotel"' : ""}>
      <td class="num"><span class="rev-rank-badge ${cls}">${rank ?? "—"}</span></td>
      <td>${escHtml(r.competitor)}</td>
      <td>${PLAT_LABEL[r.platform] || r.platform}</td>
      <td class="num">${r.competitor_score != null ? r.competitor_score.toFixed(1) : "—"}</td>
      <td>${rank === 1 ? "🥇 Líder" : rank <= 3 ? "🏅 Top 3" : "—"}</td>
    </tr>`;
  }).join("");
}

/* ════════════════════════════════════════════════════════════
   END REVIEWS MODULE
   ════════════════════════════════════════════════════════════ */

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
