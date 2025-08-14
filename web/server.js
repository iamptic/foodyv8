// server.js (ESM) — fixes ENOENT by serving static under '/web' prefix
// Works for Root directory = '/' or 'web', and for builds in 'web/' or 'web/dist'.
import express from "express";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const exists = (p) => { try { return fs.existsSync(p); } catch { return false; } };

// Candidate bases that may contain the built app (index.html or sub-apps)
const baseCandidates = [
  path.join(__dirname, "web", "dist"),
  path.join(__dirname, "web"),
  path.join(__dirname, "dist"),
  __dirname,
  path.join(process.cwd(), "web", "dist"),
  path.join(process.cwd(), "web"),
  path.join(process.cwd(), "dist"),
  process.cwd(),
];

function hasEntry(base) {
  return [
    "index.html",
    path.join("buyer", "index.html"),
    path.join("merchant", "index.html"),
  ].some((rel) => exists(path.join(base, rel)));
}

const WEB_BASE = baseCandidates.find(hasEntry);
if (!WEB_BASE) {
  console.error("Foody web: build directory not found. Tried:", baseCandidates);
  process.exit(1);
}

const app = express();

// Health
app.get(["/health", "/health/"], (_req, res) => res.json({ ok: true, base: WEB_BASE }));

// Runtime config for frontend
app.get("/config.js", (_req, res) => {
  const cfg = { FOODY_API: process.env.FOODY_API || "https://foodyback-production.up.railway.app" };
  res.setHeader("Content-Type", "application/javascript; charset=utf-8");
  res.send(`window.__FOODY__=${JSON.stringify(cfg)};`);
});

// STATIC under '/web':
// This is the critical fix: requests like '/web/merchant/style.css' will map to '<WEB_BASE>/merchant/style.css'
app.use("/web", express.static(WEB_BASE, { index: false }));

// SPA fallback (prefer merchant/buyer entry points when present)
function sendIndexFor(req, res) {
  const merchantIndex = path.join(WEB_BASE, "merchant", "index.html");
  const buyerIndex = path.join(WEB_BASE, "buyer", "index.html");
  const rootIndex = path.join(WEB_BASE, "index.html");

  const wantMerchant = req.path.startsWith("/web/merchant");
  const wantBuyer = req.path === "/web" || req.path.startsWith("/web/buyer");

  let chosen = rootIndex;
  if (wantMerchant && exists(merchantIndex)) chosen = merchantIndex;
  else if (wantBuyer && exists(buyerIndex)) chosen = buyerIndex;
  else if (exists(rootIndex)) chosen = rootIndex;
  else if (exists(buyerIndex)) chosen = buyerIndex;
  else if (exists(merchantIndex)) chosen = merchantIndex;

  res.sendFile(chosen);
}

// Important: fallback routes must be AFTER the static middleware.
app.get("/web", sendIndexFor);
app.get("/web/*", sendIndexFor);

const PORT = Number(process.env.PORT) || 3000;
app.listen(PORT, () => {
  console.log(`Foody web running on :${PORT}, base ${WEB_BASE} (static mounted at '/web')`);
});
