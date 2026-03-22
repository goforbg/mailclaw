```
 __  __       _  _  ___  _                
|  \/  | __ _(_)| |/ __|| | __ _ __      
| |\/| |/ _` | || | (__ | |/ _` \ \ /\ / /
| |  | | (_| | || |\__ \| | (_| |\ V  V / 
|_|  |_|\__,_|_||_||___/|_|\__,_| \_/\_/  
                                           
  cold email pipeline  •  analytics  •  v2.0
  by InboxPirates Consulting × Tuco.ai
```

**Mailclaw** is a battle-tested cold email operations CLI. CSV in → verified, enriched, uploaded to Instantly in minutes. Full analytics reporting with cross-campaign deduplication. Telegram bot included.

Built by [InboxPirates Consulting](https://inboxpirates.com) — the agency behind some of the highest-performing cold email systems in B2B SaaS. Powered by [Tuco.ai](https://tuco.ai) — automate your iMessage outreach.

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

## Deploy to Railway

1. Push this repo to GitHub (private or public)
2. Railway → New Project → Deploy from GitHub
3. Add environment variables from `.env.example`
4. Health check: `GET /health` → `{"status":"ok","bot":"running"}`

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app)

---

## Environment Variables

See `.env.example` for full reference.

```bash
TELEGRAM_TOKEN=...
TELEGRAM_ALLOWED_USERS=123456789,987654321
INSTANTLY_CLIENT_4DD=your_api_key
REOON_KEY=your_reoon_key
ANTHROPIC_API_KEY=sk-ant-...
ANALYTICS_PROFILE_WILL={"name":"will",...}
```

---

## Telegram Bot

```
/analytics          → list profiles
/analytics will     → run report (live Instantly data)
/balance            → Reoon credits
/help               → commands

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

## License

Copyright © 2026 Crewcharge Technologies Private Limited / Foxwell & Pierce Group Inc.  
All rights reserved. Not licensed for reuse without permission.

---

## By

```
  ___       _            ____  _           _            
 |_ _|_ __ | |__   ___ |  _ \(_)_ __ __ _| |_ ___  ___ 
  | || '_ \| '_ \ / _ \| |_) | | '__/ _` | __/ _ \/ __|
  | || | | | |_) | (_) |  __/| | | | (_| | ||  __/\__ \
 |___|_| |_|_.__/ \___/|_|   |_|_|  \__,_|\__\___||___/
                                                        
  inboxpirates.com  •  Cold email that actually lands.
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

*Built with obsession by [@bharadwaj_g](https://twitter.com/bharadwaj_g)*  
*Questions? DM on Telegram or open an issue.*
