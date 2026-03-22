```
 __  __       _  _  ___  _                
|  \/  | __ _(_)| |/ __|| | __ _ __      
| |\/| |/ _` | || | (__ | |/ _` \ \ /\ / /
| |  | | (_| | || |\__ \| | (_| |\ V  V / 
|_|  |_|\__,_|_||_||___/|_|\__,_| \_/\_/  
                                           
  cold email pipeline  •  analytics  •  v2.0
  by inboxpiratesconsulting.com × Tuco.ai
```

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
Powered by **[Tuco.ai](https://tuco.ai)** — automate your iMessage outreach.

---

```
 █████╗  ██████╗ ███████╗███╗   ██╗ ██████╗ ██╗███████╗███████╗
██╔══██╗██╔════╝ ██╔════╝████╗  ██║██╔═══██╗██║██╔════╝██╔════╝
███████║██║  ███╗█████╗  ██╔██╗ ██║██║   ██║██║███████╗█████╗  
██╔══██║██║   ██║██╔══╝  ██║╚██╗██║██║   ██║██║╚════██║██╔══╝  
██║  ██║╚██████╔╝███████╗██║ ╚████║╚██████╔╝██║███████║███████╗
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝╚══════╝╚══════╝
 ██╗   ██╗ ██████╗ ██╗   ██╗██████╗     █████╗  ██████╗ ███████╗███╗   ██╗ ██████╗██╗   ██╗
 ██║   ██║██╔═══██╗██║   ██║██╔══██╗   ██╔══██╗██╔════╝ ██╔════╝████╗  ██║██╔════╝╚██╗ ██╔╝
 ██║   ██║██║   ██║██║   ██║██████╔╝   ███████║██║  ███╗█████╗  ██╔██╗ ██║██║      ╚████╔╝ 
 ╚██╗ ██╔╝██║   ██║██║   ██║██╔══██╗   ██╔══██║██║   ██║██╔══╝  ██║╚██╗██║██║       ╚██╔╝  
  ╚████╔╝ ╚██████╔╝╚██████╔╝██║  ██║   ██║  ██║╚██████╔╝███████╗██║ ╚████║╚██████╗   ██║   
   ╚═══╝   ╚═════╝  ╚═════╝ ╚═╝  ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝   ╚═╝   
```

### **→ [BOOK A STRATEGY CALL — GET MAILCLAW BUILT FOR YOUR AGENCY](https://inboxpirates.com/demo)** ←

**Agency teams:** if you want this pipeline, analytics stack, and Telegram ops **implemented end-to-end for your shop**, book here:

# **[https://inboxpirates.com/demo](https://inboxpirates.com/demo)**

Implementation • infra • multi-client keys • reporting — **[inboxpiratesconsulting.com](https://inboxpiratesconsulting.com)**

---

```
████████╗██╗   ██╗ ██████╗ ██████╗      █████╗ ██╗
╚══██╔══╝██║   ██║██╔════╝██╔═══██╗    ██╔══██╗██║
   ██║   ██║   ██║██║     ██║   ██║    ███████║██║
   ██║   ██║   ██║██║     ██║   ██║    ██╔══██║██║
   ██║   ╚██████╔╝╚██████╗╚██████╔╝    ██║  ██║██║
   ╚═╝    ╚═════╝  ╚═════╝ ╚═════╝     ╚═╝  ╚═╝╚═╝

  Automate your iMessage outreach.
  tuco.ai  •  @tuco_ai
```

---

## What it does

```
CSV  →  column map  →  email verify (Reoon)  →  AI enrich  →  Instantly upload
                                                      ↓
                                             Telegram analytics bot
                                             campaign reports  •  dupe audit
                                             daily / weekly / monthly
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
| `mailclaw.py bot` | Start Telegram bot (24/7) |
| `mailclaw.py balance` | Check Reoon + AI credits |
| `mailclaw.py config` | Manage API keys |
| `mailclaw.py profiles` | Manage enrichment profiles |
| `mailclaw.py clients` | Manage Instantly clients |
| `mailclaw.py analytics-profiles` | Manage analytics profiles |

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

1. Push this repo to GitHub (private or public)
2. Railway → New Project → Deploy from GitHub (Dockerfile is auto-detected)
3. Add **all** secrets as environment variables (see below) — the container filesystem is **ephemeral**; do not rely on `~/.mailclaw/config.json` surviving redeploys.
4. Health check: `GET /health` → `{"status":"ok","bot":"running"}`

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app)

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

```
/analytics          → list profiles
/analytics will     → run report (live Instantly data)
/balance            → Reoon credits (live API sync; falls back if API unreachable)
/help               → commands + /ask profile help

Or just type any question:
  "how many meetings did we book this week?"
  "what was our reply rate in March?"

Drop a .csv → get back verified + filtered CSVs
```

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

**Proprietary — all rights reserved.** This repository is public **for reference only**.  
**No use, copying, modification, distribution, or commercial use is permitted** without a **written commercial license** from the copyright holder.

See [`LICENSE`](LICENSE) for full terms.

Copyright © 2026 Crewcharge Technologies Private Limited and Foxwell & Pierce Group Inc.

---

## By

```
 ██████╗ ██╗   ██╗██╗██████╗ ██╗      █████╗ ██████╗ ██╗    ██╗ █████╗      ██████╗ ██╗██████╗ ██╗██╗  ██╗ █████╗ ██████╗ 
 ██╔══██╗██║   ██║██║██╔══██╗██║     ██╔══██╗██╔══██╗██║    ██║██╔══██╗    ██╔════╝ ██║██╔══██╗██║██║  ██║██╔══██╗██╔══██╗
 ██████╔╝██║   ██║██║██████╔╝██║     ███████║██████╔╝██║ █╗ ██║███████║    ██║  ███╗██║██████╔╝██║███████║███████║██████╔╝
 ██╔══██╗██║   ██║██║██╔══██╗██║     ██╔══██║██╔══██╗██║███╗██║██╔══██║    ██║   ██║██║██╔══██╗██║██╔══██║██╔══██║██╔══██╗
 ██████╔╝╚██████╔╝██║██║  ██║███████╗██║  ██║██║  ██║╚███╔███╔╝██║  ██║    ╚██████╔╝██║██║  ██║██║██║  ██║██║  ██║██║  ██║
 ╚═════╝  ╚═════╝ ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚══╝╚══╝ ╚═╝  ╚═╝     ╚═════╝ ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝
```

**Bharadwaj Giridhar** — founder / builder — **`@goforbg` everywhere**

| Platform | **@goforbg** |
|:---:|:---|
| GitHub | **[github.com/goforbg](https://github.com/goforbg)** |
| X (Twitter) | **[x.com/goforbg](https://x.com/goforbg)** |
| LinkedIn | **[linkedin.com/in/goforbg](https://www.linkedin.com/in/goforbg/)** |
| Instagram | **[instagram.com/goforbg](https://www.instagram.com/goforbg/)** |

```
  ___       _            ____  _           _            
 |_ _|_ __ | |__   ___ |  _ \(_)_ __ __ _| |_ ___  ___ 
  | || '_ \| '_ \ / _ \| |_) | | '__/ _` | __/ _ \/ __|
  | || | | | |_) | (_) |  __/| | | | (_| | ||  __/\__ \
 |___|_| |_|_.__/ \___/|_|   |_|_|  \__,_|\__\___||___/
                                                        
  inboxpiratesconsulting.com  •  Cold email that actually lands.
  BOOK THE PIPELINE FOR YOUR AGENCY → inboxpirates.com/demo
```

```
 _____                          _ 
|_   _|   _  ___  ___     __ _(_)
  | || | | |/ __/ _ \   / _` | |
  | || |_| | (_| (_) | | (_| | |
  |_| \__,_|\___\___/   \__,_|_|
                                 
  tuco.ai  •  iMessage automation for sales teams.
  Reply from your own number. At scale.
```

---

**Mailclaw** • **[@goforbg](https://github.com/goforbg)** on GitHub · [X](https://x.com/goforbg) · [LinkedIn](https://www.linkedin.com/in/goforbg/) · [Instagram](https://www.instagram.com/goforbg/) — *Bharadwaj Giridhar*

**Agency?** **[Book a call — inboxpirates.com/demo](https://inboxpirates.com/demo)** · **[inboxpiratesconsulting.com](https://inboxpiratesconsulting.com)**

*Questions? Open an issue or DM on social above.*
