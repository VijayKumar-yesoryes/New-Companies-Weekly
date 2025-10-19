import fs from "node:fs/promises";
import path from "node:path";
import ExcelJS from "exceljs";
import nodemailer from "nodemailer";
import dotenv from "dotenv"

dotenv.config();
 
export async function runWeeklyJob() {
  const API_KEY = process.env.DGI_API_KEY;
  const RESOURCE_ID = process.env.DGI_RESOURCE_ID;
  if (!API_KEY || !RESOURCE_ID) throw new Error("Missing DGI_API_KEY/DGI_RESOURCE_ID");
 
  const PAGE_LIMIT = Number(process.env.PAGE_LIMIT || 1000);
  const MAX_PAGES  = Number(process.env.MAX_PAGES  || 200);
 
  const BREVO_HOST   = process.env.BREVO_HOST || "smtp-relay.brevo.com";
  const BREVO_PORT   = Number(process.env.BREVO_PORT || 2525);
//   const BREVO_SECURE = String(process.env.SMTP_SECURE || "true") === "true";
  const BREVO_USER   = process.env.BREVO_USER;
  const BREVO_PASS   = process.env.BREVO_PASS;
  const MAIL_FROM   = process.env.MAIL_FROM || BREVO_USER;
  const MAIL_TO     = process.env.MAIL_TO || "";
  const MAIL_SUBJECT= process.env.MAIL_SUBJECT || "Newly Registered Companies";
 
  const STATE_DIR = ".state";
  const OUT_DIR   = "out";
  const CHECKPOINT_FILE = path.join(STATE_DIR, "last_run.json");
 
  const BASE = `https://api.data.gov.in/resource/${RESOURCE_ID}`;
  const today = dateOnly(new Date());
 
  async function ensureDirs() {
    await fs.mkdir(STATE_DIR, { recursive: true });
    await fs.mkdir(OUT_DIR, { recursive: true });
  }
  async function loadCheckpoint() {
    try {
      const raw = await fs.readFile(CHECKPOINT_FILE, "utf8");
      const j = JSON.parse(raw);
      const d = j?.last_run_iso ? new Date(j.last_run_iso) : null;
      if (d && !isNaN(d)) return dateOnly(d);
    } catch {}
    // default: last 7 days
    return dateOnly(new Date(Date.now() - 7*24*3600*1000));
  }
  async function saveCheckpoint(d) {
    await fs.writeFile(CHECKPOINT_FILE, JSON.stringify({ last_run_iso: d.toISOString() }, null, 2));
  }
  function dateOnly(d) { return new Date(d.getFullYear(), d.getMonth(), d.getDate()); }
  function toISO(d) { return d.toISOString().slice(0,10); }
 
  function tryParseDateLoose(v) {
    if (!v) return null;
    const s = String(v).trim();
    const tries = [
      () => new Date(s),
      () => { const m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})/); if (!m) throw 0; return new Date(+m[3], +m[2]-1, +m[1]); },
      () => { const m = s.match(/^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})/); if (!m) throw 0; return new Date(+m[1], +m[2]-1, +m[3]); },
    ];
    for (const f of tries) {
      try { const d = f(); if (!isNaN(d)) return dateOnly(d); } catch {}
    }
    return null;
  }
  function detectDateField(sample) {
    for (const k of ["date_of_registration","CompanyRegistrationdate_date","date_of_incorporation","Date_of_Registration"])
      if (k in sample) return k;
    for (const k of Object.keys(sample)) if (tryParseDateLoose(sample[k])) return k;
    return null;
  }
  async function fetchPage(limit, offset, extraParams={}) {
    const url = new URL(BASE);
    url.searchParams.set("api-key", API_KEY);
    url.searchParams.set("format", "json");
    url.searchParams.set("limit", String(limit));
    url.searchParams.set("offset", String(offset));
    for (const [k,v] of Object.entries(extraParams)) url.searchParams.append(k, v);
    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    const j = await res.json();
    return Array.isArray(j.records) ? j.records : [];
  }
  async function buildExcel(records, excelPath, dateField, idField) {
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("New Companies");
    if (!records.length) {
      ws.addRow(["No new companies in the selected window"]);
      await wb.xlsx.writeFile(excelPath);
      return;
    }
    const headers = Object.keys(records[0]);
    ws.columns = headers.map(h => ({ header: h, key: h }));
    for (const r of records) ws.addRow(headers.map(h => r[h]));
    ws.getRow(1).font = { bold: true };
    ws.views = [{ state: "frozen", ySplit: 1 }];
    // summary
    const s = wb.addWorksheet("Summary");
    s.addRow(["Date", "Count"]).font = { bold: true };
    const byDay = {};
    for (const r of records) {
      const d = tryParseDateLoose(r[dateField]); if (!d) continue;
      const k = toISO(d); byDay[k] = (byDay[k] || 0) + 1;
    }
    Object.entries(byDay).sort().forEach(([k,c]) => s.addRow([k,c]));
    await wb.xlsx.writeFile(excelPath);
  }
  function toCSV(records) {
    if (!records.length) return "";
    const headers = Object.keys(records[0]);
    const esc = v => {
      if (v == null) return "";
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    };
    const lines = [headers.join(",")];
    for (const r of records) lines.push(headers.map(h => esc(r[h])).join(","));
    return lines.join("\n");
  }
 
  // ---- run ----
  await ensureDirs();
  const lastRun = await loadCheckpoint();
 
  // learn schema
  let first = await fetchPage(50, 0);
  if (!first.length) { await saveCheckpoint(today); return { count: 0, from: toISO(lastRun), to: toISO(today), files: {} }; }
  const dateField = detectDateField(first[0]);
  const idField = ["cin","CIN","company_name","CompanyName"].find(k => k in first[0]) || Object.keys(first[0])[0];
  const sortParam = dateField ? { [`sort[${dateField}]`]: "desc" } : {};
 
  // page & filter
  let offset = 0, pages = 0;
  const fresh = [];
  while (true) {
    if (++pages > MAX_PAGES) break;
    const recs = await fetchPage(PAGE_LIMIT, offset, sortParam);
    if (!recs.length) break;
 
    let oldest = today;
    for (const r of recs) {
      const d = tryParseDateLoose(r[dateField]);
      if (d) {
        if (d < oldest) oldest = d;
        if (d >= lastRun && d <= today) fresh.push(r);
      }
    }
    if (oldest < lastRun) break;
    if (recs.length < PAGE_LIMIT) break;
    offset += PAGE_LIMIT;
  }
 
  fresh.sort((a,b) => {
    const da = tryParseDateLoose(a[dateField])?.getTime() ?? 0;
    const db = tryParseDateLoose(b[dateField])?.getTime() ?? 0;
    if (da !== db) return da - db;
    return String(a[idField] ?? "").localeCompare(String(b[idField] ?? ""));
  });
 
  const stamp = toISO(today);
  const jsonPath = path.join(OUT_DIR, `new_companies_${stamp}.json`);
  const csvPath  = path.join(OUT_DIR, `new_companies_${stamp}.csv`);
  const xlsPath  = path.join(OUT_DIR, `new_companies_${stamp}.xlsx`);
  await fs.writeFile(jsonPath, JSON.stringify(fresh, null, 2), "utf8");
  await fs.writeFile(csvPath, toCSV(fresh), "utf8");
  await buildExcel(fresh, xlsPath, dateField, idField);
 
  if (BREVO_USER && BREVO_PASS && MAIL_TO) {
    const transporter = nodemailer.createTransport({
      host: BREVO_HOST, port: BREVO_PORT, secure: BREVO_PORT === 465,
      auth: { user: BREVO_USER, pass: BREVO_PASS },
      connectionTimeout: 15000,
      greetingTimeout: 10000
    });
    await transporter.verify()
    console.log();
    
    const html = `
      <div style="font-family:system-ui,Segoe UI,Roboto">
        <h2>Newly Registered Companies</h2>
        <p>Window: <b>${toISO(lastRun)}</b> → <b>${toISO(today)}</b><br/>
        Count: <b>${fresh.length}</b></p>
        <p>Files attached (JSON, CSV, XLSX).</p>
      </div>`;
    await transporter.sendMail({
      from: MAIL_FROM || BREVO_USER,
      to: MAIL_TO.split(",").map(s=>s.trim()).filter(Boolean),
      subject: MAIL_SUBJECT,
      html,
      attachments: [
        { filename: path.basename(xlsPath), path: xlsPath },
        { filename: path.basename(csvPath), path: csvPath },
        { filename: path.basename(jsonPath), path: jsonPath },
      ],
    });
  }
 
  await saveCheckpoint(today);
  return { count: fresh.length, from: toISO(lastRun), to: toISO(today), files: { json: jsonPath, csv: csvPath, xlsx: xlsPath } };
}


const result = await runWeeklyJob();
console.log(result);
