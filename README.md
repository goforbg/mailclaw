<div align="center">

```
╔══════════════════════════════════════════════════════════════════════════╗
║ MAILCLAW  ·  Cold email ops for GTM teams, agencies & RevOps             ║
║ CSV → verify → AI enrich → Instantly  ·  analytics  ·  Telegram bot      ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### Cold email ops · Verify · Enrich · Instantly · Analytics · Telegram

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Proprietary-8B0000?style=for-the-badge)](LICENSE)
[![Instantly](https://img.shields.io/badge/Instantly-API%20V2-5B4FFF?style=for-the-badge)](https://developer.instantly.ai)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)

<br/>

[![Book implementation](https://img.shields.io/badge/Book%20implementation-InboxPirates-FF4F00?style=for-the-badge)](https://inboxpirates.com/cal)
[![Tuco demo](https://img.shields.io/static/v1?label=tuco.ai&message=demo&color=000000&style=for-the-badge)](https://tuco.ai/demo)

<sub><a href="https://inboxpiratesconsulting.com"><strong>InboxPirates Consulting</strong></a> × <a href="https://tuco.ai"><strong>Tuco.ai</strong></a></sub>

</div>

---


**Mailclaw** is an **operations stack for GTM and cold-email teams**: one CLI to take prospect lists from **raw CSV → verified → AI-enriched → live in Instantly**, then **measure what actually happened**—per campaign, per client, and portfolio-wide—without spreadsheet hell.

**Who it’s for**

- **Outbound & cold email agencies** running multiple Instantly workspaces: standardize how every client gets cleaned data, safe sends, and reporting that doesn’t double-count leads across campaigns.
- **GTM / growth / RevOps** at SaaS and services companies: ship lists faster, keep deliverability honest (verification + key rotation), and answer *“what did we book, reply, and lose to bounce this week?”* on demand.
- **Founders and reps** who want **Telegram** as a control room: natural-language analytics, CSV drops for verification, and campaign reports without logging into five tools.

**What you get**

- **Pipeline:** column mapping, Reoon verification with rotating keys, optional AI enrichment (Gemini / Claude / GPT), upload to **Instantly v2**.
- **Truthful analytics:** date-ranged reports, cross-campaign deduplication, meetings / opportunities / reply metrics, Excel exports, and a **multi-client** model (separate keys per client on Railway or laptop).
- **Always-on bot:** ask questions in plain English, get live numbers, attach exports—built for agencies that need speed and clarity across accounts.

Built by **[InboxPirates Consulting](https://inboxpiratesconsulting.com)** — the agency behind some of the highest-performing cold email systems in B2B SaaS.  
Powered by **[Tuco.ai](https://tuco.ai)** — automate your iMessage outreach (**[demo](https://tuco.ai/demo)**).

<div align="center">

**If Mailclaw stops you living inside a spreadsheet, leave a star — it helps.**

[![Stars](https://img.shields.io/github/stars/goforbg/mailclaw?label=Stars&logo=github&style=social)](https://github.com/goforbg/mailclaw)
[![Follow](https://img.shields.io/badge/Follow-%40goforbg-1DA1F2?style=social&logo=x)](https://x.com/goforbg)

</div>

---

## TL;DR

| You want… | You type… |
|:---|:---|
| **Numbers without opening Instantly** | `python mailclaw.py ask "how many meetings did we book this month?"` |
| **A client-specific answer** | `python mailclaw.py ask --profile acme "bounce rate last week"` or Telegram: `/ask acme what was our reply rate in March?` |
| **A full Excel war room** | `python mailclaw.py analytics` → pick dates → export |
| **Verify a list from your phone** | Send a `.csv` to the Telegram bot |
| **Credits that aren’t made up** | `python mailclaw.py balance` or `/balance` (live Reoon API) |

> **One sentence:** Mailclaw is the **CLI + Telegram layer** that sits on top of Instantly + Reoon + your AI keys so **GTM and agencies** stop duct-taping CSVs and screenshots.

### Spreadsheet trauma vs Mailclaw

| Without Mailclaw | With Mailclaw |
|:---|:---|
| Five tabs and a prayer | One **CLI** or **Telegram** answer tied to Instantly |
| “I’ll pull numbers tomorrow” | `python mailclaw.py ask "…"` from the terminal **or** your phone |
| Duplicate leads counted twice in reports | Cross-campaign dedupe in **analytics** |
| Another VA copying CSVs | **Drop a CSV in Telegram** → verified splits back |
| Guessing remaining Reoon credits | `balance` / `/balance` → **live API** when reachable |

---

### Table of contents

**Setup & deploy:** [What it does](#what-it-does) · [Quick start](#quick-start) · [Commands](#commands) · [Deploy to Railway](#deploy-to-railway-hosted-bot)  
**Examples (the good stuff):** [Examples galore](#examples-galore) · [Telegram Bot](#telegram-bot) · [Analytics](#analytics)  
**Trust & ops:** [Tech stack](#tech-stack) · [Operations & reliability](#operations--reliability) · [License](#license) · [By](#by)

---

### Work with us

| | |
|:---|:---|
| **Ship Mailclaw for your agency** | **[Book a call → inboxpirates.com/cal](https://inboxpirates.com/cal)** |
| **Site** | **[inboxpiratesconsulting.com](https://inboxpiratesconsulting.com)** |
| **Tuco (iMessage at scale)** | **[tuco.ai](https://tuco.ai)** · **[tuco.ai/demo](https://tuco.ai/demo)** |

---

## What it does

```text
   ┌─────┐   ┌─────────┐   ┌────────┐   ┌────────┐   ┌──────────────┐
   │ CSV │──▶│ Map cols│──▶│ Verify │──▶│ Enrich │──▶│ Instantly V2 │
   └─────┘   └─────────┘   │ Reoon  │   │  AI    │   └──────────────┘
                           └────────┘   └────────┘          │
                                                            ▼
                                                   ┌──────────────────┐
                                                   │ Telegram bot     │
                                                   │ analytics · CSV  │
                                                   └──────────────────┘
```

---

## Quick start

```bash
pip install -r requirements.txt
python mailclaw.py onboard       # first-time setup
python mailclaw.py run           # full pipeline
python mailclaw.py analytics     # campaign report
python mailclaw.py bot           # start Telegram bot
```

---

## Commands

| Command | What it does |
|---|---|
| `mailclaw.py run` | Full pipeline: map → verify → enrich → upload |
| `mailclaw.py verify` | Email verification only (Reoon) |
| `mailclaw.py enrich` | AI enrichment only |
| `mailclaw.py upload` | Upload to Instantly only |
| `mailclaw.py analytics` | Interactive analytics report |
| `mailclaw.py ask "…"` | Natural-language analytics (Instantly + AI); `--profile name` for one client |
| `mailclaw.py bot` | Start Telegram bot (24/7) |
| `mailclaw.py balance` | Check Reoon + AI credits |
| `mailclaw.py config` | Manage API keys |
| `mailclaw.py profiles` | Manage enrichment profiles |
| `mailclaw.py clients` | Manage Instantly clients |
| `mailclaw.py analytics-profiles` | Manage analytics profiles |

---

## Examples galore

**Natural-language analytics (CLI)** — uses live Instantly data + Gemini (same engine as the Telegram bot):

```bash
# Portfolio questions (default analytics profile)
python mailclaw.py ask "how many emails did we send yesterday?"
python mailclaw.py ask "what was our human reply rate last week?"
python mailclaw.py ask "how many meetings did we book this month?"

# Lock to one analytics profile (multi-client agencies)
python mailclaw.py ask --profile will "show me stats for this week"
python mailclaw.py ask --profile acme "what was our bounce rate in Q1?"

# Lists + exports (phrases like “export” / “csv” / “excel” trigger attachments when applicable)
python mailclaw.py ask "which leads booked a meeting this week — export csv"
python mailclaw.py ask "download full analytics excel for last month"
```

**Interactive report (CLI)** — full campaign breakdown + Excel when you want menus:

```bash
python mailclaw.py analytics
python mailclaw.py analytics --start 2026-03-01 --end 2026-03-31
```

**Ops & keys**

```bash
python mailclaw.py balance          # live Reoon + AI key status
python mailclaw.py config           # keys, limits, Telegram allowlist
python mailclaw.py analytics-profiles
```

<details>
<summary><strong>Even more Telegram examples</strong> (tap to expand)</summary>

**Slash commands**

```text
/analytics                    → lists analytics profiles
/analytics will               → full report for profile “will” (live data)
/analytics will 2026-03-01 2026-03-31   → same, custom window
/balance                      → Reoon credits (synced from API when reachable)
/help                         → help + /ask usage
```

**Plain English (same as `mailclaw ask`)**

```text
how many meetings did we book this week?
what was our reply rate in March?
will how many leads did we generate last month?     ← optional first word = profile
what meetings did we book this week — export csv
```

**Drop a file**

```text
Upload leads.csv  →  verification runs  →  you get safe / catchall / by-ESP CSVs back
```

*The bot has strong opinions about nonsense messages. `/help` is your friend.*

</details>

---

## Analytics

- Auto-selects active campaigns by date — no checkbox needed
- Daily / Weekly / Monthly / Custom date picker
- Cross-campaign deduplication (positive reply subsequences handled correctly)
- Corrected meetings booked count (removes cross-campaign inflated API numbers)
- Leads audit with first-reply timestamps and dupe flagging
- Excel export with colored sheets per campaign + unified

```
Time period:
  📅  All time
  📆  Daily
  📅  Weekly   →  Week 12 2026  (16 Mar – 22 Mar)
  🗓   Monthly  →  March 2026
  ✏️   Custom
```

---

## Deploy to Railway (hosted bot)

This repo includes a **`Dockerfile`** that runs **`python mailclaw.py bot`** (see `CMD` in the file). Railway sets **`PORT`** automatically; Mailclaw binds a small **HTTP health server** on that port for `GET /health`.

1. Push this repo to GitHub (private or public).
2. **[Create a new Railway project](https://railway.app/new)** → **Deploy from GitHub** → select this repo. Railway will detect the **Dockerfile** and use its **`CMD`** (no need to override the start command unless you know what you’re doing).
3. Add **all** secrets as **environment variables** (see below). The container filesystem is **ephemeral**; do not rely on `~/.mailclaw/config.json` surviving redeploys — use **env-only** config on Railway (`RAILWAY_ENVIRONMENT` is set automatically, or set `MAILCLAW_CONFIG_SOURCE=env`).
4. After deploy, open your service **public URL** (or generate a domain in Railway) and check: **`GET /health`** → `{"status":"ok","bot":"running"}` once the Telegram bot has finished starting.

[![Deploy on Railway](https://img.shields.io/static/v1?label=Deploy&message=Railway&color=0B0D0E&style=for-the-badge&logo=railway&logoColor=white)](https://railway.app/new)

*If the graphic above fails to load, use the link: **[railway.app/new](https://railway.app/new)** → Deploy from GitHub.*

### Config mode: env-only vs local files

| Mode | When | Behaviour |
|------|------|-----------|
| **Env-only** | `RAILWAY_ENVIRONMENT` is set (Railway does this), or `MAILCLAW_CONFIG_SOURCE=env`, or `MAILCLAW_USE_ENV=1` | No `config.json` or `email_history.json` on disk. All API keys and clients come from env vars. Verification **history** is kept **in memory** only (resets on restart). |
| **File** | Default on your laptop | Uses `~/.mailclaw/*.json` as before. |
| **Force files on Railway** | `MAILCLAW_CONFIG_SOURCE=file` | Use if you mount a persistent volume at `~/.mailclaw`. |

No Redis or MongoDB is required. If you later need durable verification history across restarts, add a small Redis (e.g. Upstash) — not bundled in this repo.

On Railway, **INFO** logs (config mode, `analytics_ask` client resolution, AI key scope) go to the deploy log automatically. Set `MAILCLAW_LOG_STDERR=0` to silence them.

### Multi-client agencies (simple prefix rule)

**Full reference:** [docs/CLIENT_ENV.md](docs/CLIENT_ENV.md)

- **Instantly — exactly one API key per client** (each client’s workspace):

  ```bash
  INSTANTLY_CLIENT_WILL=sk_xxx
  INSTANTLY_CLIENT_GD=sk_yyy
  ```

  The suffix (`WILL`, `GD`) becomes the internal client name (`will`, `gd`). Use that string in analytics profile JSON as `client_name`.

- **AI keys — global and/or per client**

  - **Global** (fallback for any client): `GEMINI_API_KEY`, `GEMINI_API_KEY_2`, `OPENAI_API_KEY`, …
  - **Per client** — same names with a **prefix** derived from the Instantly client name (uppercase, non-alphanumerics → `_`):

  ```bash
  # Client WILL: three Gemini keys in rotation, one OpenAI
  WILL_GEMINI_API_KEY=AIza_first
  WILL_GEMINI_API_KEY_2=AIza_second
  WILL_GEMINI_API_KEY_3=AIza_third
  WILL_OPENAI_API_KEY=sk_will

  # Client GD: its own keys (optional)
  GD_GEMINI_API_KEY=AIza_gd
  ```

  When you run `/analytics will` (or `analytics_ask` for that profile), Mailclaw uses **`WILL_*` keys** if set; otherwise **global** `GEMINI_*` / `OPENAI_*`.

- **Reoon** — usually **shared** across clients (global pool):

  ```bash
  REOON_KEY=...
  REOON_KEY_1=...
  REOON_KEY_1_NAME=backup
  ```

- **Analytics profiles on Railway** — JSON in env; `client_name` must match an `INSTANTLY_CLIENT_*` suffix:

  ```bash
  ANALYTICS_PROFILE_WILL='{"name":"will","client_name":"will","benchmarks":{...},"campaign_name_filter":""}'
  ```

### Railway copy-paste sample (two clients + bot)

Minimal variables so deploy succeeds (replace placeholders):

```bash
TELEGRAM_TOKEN=123456:ABC...
INSTANTLY_CLIENT_WILL=sk_replace
INSTANTLY_CLIENT_GD=sk_replace
GEMINI_API_KEY=AIza_replace
WILL_GEMINI_API_KEY=AIza_replace
WILL_GEMINI_API_KEY_2=AIza_replace
GD_GEMINI_API_KEY=AIza_replace
REOON_KEY=reoon_replace
ANALYTICS_PROFILE_WILL={"name":"will","client_name":"will","benchmarks":{},"campaign_name_filter":""}
```

Copy `.env.example` for more options.

---

## Telegram Bot

Same brain as **`mailclaw ask`** — see **[Examples galore](#examples-galore)** for copy-paste. Short version:

```
/analytics                          → list profiles
/analytics will                     → full report (live Instantly)
/analytics will 2026-03-01 2026-03-31
/balance                            → Reoon (live sync when API is up)
/help                               → help + /ask profile cheat sheet
/ask will how many meetings Q1?     → optional profile as first word after /ask
```

**Plain text** (no slash): same questions as the CLI — or start with a **profile name** + question.

**Documents:** send a **`.csv`** → Reoon verification → **safe / catchall / ESP** CSVs back.

<div align="center">

<sub><b>Share line for Twitter / LinkedIn:</b> “We wired Instantly + Reoon + Telegram into one CLI. CSV in → verified → enriched → live sends → honest analytics. Mailclaw.” · <code>@goforbg</code></sub>

</div>

---

## Tech stack

- Python 3.9+ — single file, no framework
- [Instantly V2 API](https://developer.instantly.ai) — campaigns, analytics, leads
- [Reoon](https://reoon.com) — email verification with key rotation
- [python-telegram-bot](https://python-telegram-bot.org) — Telegram integration
- Gemini / Claude / GPT-4o — AI enrichment and analytics Q&A
- openpyxl — Excel export

---

## Operations & reliability

- **Logging:** configurable log level; failures in analytics, Telegram sends, CSV export, and Reoon sync are **`log.exception` / `log.warning`** so hosted deploys (e.g. Railway) can trace issues in process logs.
- **Telegram `/balance`:** calls Reoon’s **live balance API** in a **background thread** so the bot event loop is not blocked; malformed API fields are handled safely with warnings in logs.
- **Telegram sends:** reply helpers catch **`TelegramError`** and log instead of crashing the bot.
- **CLI `mailclaw balance`:** same live Reoon sync as `/balance` (see above).

---

## License

**Copyright © 2026 Crewcharge Technologies Private Limited and Foxwell & Pierce Group Inc.** See **[`LICENSE`](LICENSE)** for the full legal text.

### Who can use Mailclaw (simple version)

| Tier | Who | Cost |
|:---|:---|:---|
| **Hobby / personal** | Non-commercial, personal learning & experiments | **Free** |
| **Small agency / lean team** | Agency or business with **under USD $5K MRR** (monthly recurring revenue), using Mailclaw for your own ops | **Free** |
| **Everyone else (commercial)** | Larger agencies, resale, embedding Mailclaw in a product for customers, or revenue **≥ USD $5K MRR** | **Commercial license** — book a call to align terms |

**Commercial license & questions:** **[Book a call → inboxpirates.com/cal](https://inboxpirates.com/cal)** · **[inboxpiratesconsulting.com](https://inboxpiratesconsulting.com)**

The full **`LICENSE`** file is binding; this table is a **summary** only. When in doubt, book a call.

---

**Proprietary software.** All use is governed by **[`LICENSE`](LICENSE)**. Commercial use beyond the free tiers above requires a **written commercial license** — start at **[inboxpirates.com/cal](https://inboxpirates.com/cal)**.

---

## By

<div align="center">

### Bharadwaj Giridhar · **`@goforbg`**

[![GitHub](https://img.shields.io/badge/GitHub-goforbg-181717?style=for-the-badge&logo=github)](https://github.com/goforbg)
[![X](https://img.shields.io/badge/X-@goforbg-000000?style=for-the-badge&logo=x)](https://x.com/goforbg)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-in%2Fgoforbg-0A66C2?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/goforbg/)
[![Instagram](https://img.shields.io/badge/Instagram-@goforbg-E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://www.instagram.com/goforbg/)

<br/>

**[InboxPirates Consulting](https://inboxpiratesconsulting.com)** — cold email that lands · **[Book a call](https://inboxpirates.com/cal)**  
**[Tuco.ai](https://tuco.ai)** — iMessage automation · **[Demo](https://tuco.ai/demo)**

</div>

---

<p align="center">
  <b>Mailclaw</b> · <a href="https://github.com/goforbg">@goforbg</a>
  · <a href="https://inboxpirates.com/cal">Agency cal</a>
  · <a href="https://tuco.ai/demo">Tuco demo</a>
</p>

<p align="center"><i>Questions? Open an issue or DM on social above.</i></p>
