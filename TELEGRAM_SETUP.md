# Laddu Telegram Bot — Setup Guide

## What Will Can Do
- `/analytics will` — get a full analytics report for the 4DD account
- `/analytics` — list all available profiles
- `/balance` — check Reoon verification credits
- Drop a `.csv` file — get email-verified CSVs back

---

## Step 1: Create a Telegram Bot (you do this once)

1. Open Telegram → search for **@BotFather**
2. Send: `/newbot`
3. Choose a name: `Laddu InboxPirates`
4. Choose a username: `laddu_inboxpirates_bot` (must end in `bot`)
5. BotFather gives you a token like: `7891234567:AAHxyz...`
6. Copy it.

---

## Step 2: Add the token to Laddu

```bash
python3 laddu.py config
```
→ Paste your Telegram token when prompted.

---

## Step 3: Get Will's Telegram user ID

1. Have Will message your bot (or any message to @userinfobot)
2. Or: run the bot, Will sends `/start`, check the debug log:
   ```
   grep "effective_user" ~/.laddu/laddu_debug.log
   ```
3. His ID is a number like `123456789`

---

## Step 4: Whitelist Will in config

Edit `~/.laddu/config.json`:
```json
{
  "telegram_allowed_users": [123456789, YOUR_OWN_ID]
}
```
Get your own ID the same way (message the bot yourself).

---

## Step 5: Run the bot (on your Mac or server)

```bash
python3 laddu.py bot
```

Keep this running. On Mac with screen:
```bash
screen -S laddu
python3 laddu.py bot
# Ctrl+A then D to detach
# screen -r laddu to reattach
```

On a Linux server (Contabo/Hetzner):
```bash
nohup python3 laddu.py bot > ~/laddu_bot.log 2>&1 &
```

---

## Step 6: Will's commands

```
/analytics                  → lists profiles (e.g. /analytics will)
/analytics will             → runs the "will" profile, all campaigns auto-selected
/balance                    → Reoon credit check
```

Analytics report takes ~15–30 seconds (fetches live data from Instantly).

---

## What the Analytics Report Looks Like

```
📊 WILL — Analytics Report
All time · 5 primary campaigns (1 subsequence excluded)

👥 Unique Leads:  7,937
📧 Emails Sent:  19,277

📩 Total Reply Rate:  2.9%  ✅
   👤 Human:  0.9%  ✅
   🤖 OOO:  161

🎯 Opportunities:  16  (0.20%)  ✅
   ❌ Not Interested:  55
   📅 Meetings Booked:  4
   🏆 Deals Closed:  0

📊 Per Campaign:
• NA_Leads_Apollo&Listkit…
  4,294 leads · 2.7% reply · 11 opps · 3 booked
• AI_50_1K_S25_GEMINI_AMERICAS…
  2,585 leads · 2.8% reply · 3 opps · 0 booked
• ANTHROPIC_AMERICAS_GMAIL
  279 leads · 3.6% reply · 2 opps · 1 booked
```

---

## Analytics Profile for Will

The "will" profile is at `~/.laddu/analytics/will.json`.
It stores benchmarks, client name, and any campaign name filters.
Campaigns are auto-selected each run based on activity — no stale IDs.

To create/edit a profile:
```bash
python3 laddu.py analytics-profiles
```

---

## Troubleshooting

**Bot not responding?**
- Check token is correct in config
- Make sure `python3 laddu.py bot` is running
- Check Will's user ID is in `telegram_allowed_users`

**Analytics returns error?**
- Run `python3 laddu.py --debug bot` and check `~/.laddu/laddu_debug.log`

**"No analytics profiles"?**
- Create one: `python3 laddu.py analytics-profiles`
