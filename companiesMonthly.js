import fs from "node:fs/promises";
import path from "node:path";
import dotenv from 'dotenv'

dotenv.config();

export async function getNewCompaniesThisMonth({
  apiKey,
  resourceId,
  pageLimit = 1000,
  maxPages = 200,
  dateFieldOverride = null,
} = {}) {
  if (!apiKey || !resourceId)
    throw new Error("apiKey and resourceId are required.");
 
  const BASE = `https://api.data.gov.in/resource/${resourceId}`;
 
  // Compute current month range
  const today = new Date();
  const startOfMonth = new Date(today.getFullYear(), today.getMonth() - 5, 1);
 
  // First small page to detect date field
  const first = await fetchPage(BASE, apiKey, 50, 0);
  if (!first.length) return [];
 
  const dateField = dateFieldOverride || detectDateField(first[0]);
  if (!dateField) throw new Error("Could not detect date field");
 
  const sortParam = { [`sort[${dateField}]`]: "desc" };
 
  let offset = 0;
  let pages = 0;
  const fresh = [];
 
  while (true) {
    pages++;
    if (pages > maxPages) break;
 
    const recs = await fetchPage(BASE, apiKey, pageLimit, offset, sortParam);
    if (!recs.length) break;
 
    let oldestInPage = today;
    for (const r of recs) {
      const d = tryParseDateLoose(r[dateField]);
      if (d && d >= startOfMonth && d <= today) fresh.push(r);
      if (d && d < oldestInPage) oldestInPage = d;
    }
 
    if (oldestInPage < startOfMonth) break;
    if (recs.length < pageLimit) break;
    offset += pageLimit;
  }
 
  return fresh;
}

async function fetchPage(base, apiKey, limit, offset, extraParams = {}) {
  const url = new URL(base);
  url.searchParams.set("api-key", apiKey);
  url.searchParams.set("format", "json");
  url.searchParams.set("limit", limit);
  url.searchParams.set("offset", offset);
  for (const [k, v] of Object.entries(extraParams)) url.searchParams.append(k, v);
 
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  const j = await res.json();
  return Array.isArray(j.records) ? j.records : [];
}
 
function detectDateField(sample) {
  const guesses = [
    "date_of_registration",
    "CompanyRegistrationdate_date",
    "date_of_incorporation",
    "Date_of_Registration",
  ];
  for (const g of guesses) if (g in sample) return g;
  for (const k of Object.keys(sample)) if (tryParseDateLoose(sample[k])) return k;
  return null;
}
 
function tryParseDateLoose(v) {
  if (!v) return null;
  const s = String(v).trim();
  const attempts = [
    () => new Date(s),
    () => {
      const m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})/);
      if (!m) throw 0;
      return new Date(+m[3], +m[2] - 1, +m[1]);
    },
    () => {
      const m = s.match(/^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})/);
      if (!m) throw 0;
      return new Date(+m[1], +m[2] - 1, +m[3]);
    },
  ];
  for (const fn of attempts) {
    try {
      const d = fn();
      if (!isNaN(d)) return d;
    } catch {}
  }
  return null;
}
 
/* ---------------- Example run ---------------- */
const res = await getNewCompaniesThisMonth({
  apiKey: process.env.DGI_API_KEY,
  resourceId: process.env.DGI_RESOURCE_ID,
});
console.log("New companies this month:", res.length);
fs.writeFile('new_companies.json', JSON.stringify(res, null, 2), (err) => {
  if (err) {
    console.error('Error writing file:', err);
  } else {
    console.log('Data written to new_companies.json');
  }
});

