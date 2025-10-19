import express from "express";
import { runWeeklyJob } from "./weekly_job.mjs";
import dotenv from "dotenv"

dotenv.config();
 
const app = express();
const PORT = process.env.PORT || 10000;
 
function isAuthorized(req) {
  const k = req.query.key || req.header("X-Cron-Key");
  console.log(k, process.env.CRON_SECRET)
  return k && process.env.CRON_SECRET && k === process.env.CRON_SECRET;
}
 
app.get("/healthz", (_req, res) => res.json({ ok: true }));
 
app.post("/run/daily", async (req, res) => {
  if (!isAuthorized(req)) return res.status(401).json({ error: "unauthorized" });
  try {
    const result = await runWeeklyJob();
    res.json({ ok: true, ...result });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});
 
app.get("/run/daily", async (req, res) => {
  if (!isAuthorized(req)) return res.status(401).json({ error: "unauthorized" });
  res.json({ ok: true, message: "Job started in background" });
 
  runWeeklyJob()
    .then(r => console.log("Weekly job done:", r.count))
    .catch(e => console.error("Weekly job failed:", e));
});

app.listen(PORT, () => console.log(`Server on :${PORT}`));