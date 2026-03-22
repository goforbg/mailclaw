# Per-client environment variables

Mailclaw supports **one Instantly workspace API key per client** and **optional AI key pools per client** using a simple prefix rule.

## Naming rule

1. **Instantly client name** comes from the env var:

   `INSTANTLY_CLIENT_<NAME>=<api_key>`

   Example: `INSTANTLY_CLIENT_WILL=sk_...` → internal client name `will` (lowercased).

2. **Prefix for that client’s AI keys** = `<NAME>` uppercased, non-alphanumeric characters → `_`:

   | Client (`INSTANTLY_CLIENT_*` suffix) | Prefix for AI vars |
   |-----------------------------------|--------------------|
   | `WILL` | `WILL_` |
   | `gd-team` | `GD_TEAM_` |

3. **Per-client AI keys** (all optional; if omitted, global keys are used):

   ```text
   <PREFIX>GEMINI_API_KEY
   <PREFIX>GEMINI_API_KEY_2
   <PREFIX>GEMINI_API_KEY_3
   <PREFIX>GOOGLE_API_KEY        # same as GEMINI for Gemini
   <PREFIX>OPENAI_API_KEY
   <PREFIX>OPENAI_API_KEY_2
   <PREFIX>ANTHROPIC_API_KEY
   ```

   Rotation: same as global — multiple `_2`, `_3` keys; failed requests bump to the next key.

## Global vs client

When you run analytics or `/ask` for a profile whose `client_name` is `will`:

- If `WILL_GEMINI_API_KEY` (etc.) is set → those keys are used for that client.
- If not set → `GEMINI_API_KEY` / `OPENAI_API_KEY` / … global vars apply.

## Instantly

- Exactly **one** key per client: **`INSTANTLY_CLIENT_<NAME>`** only.
- There is no separate “extra Instantly keys per client” — multi-workspace = multiple `INSTANTLY_CLIENT_*` entries.

## Enrichment CLI (`mailclaw run`)

In the profile JSON (`~/.mailclaw/profiles/…`), set:

```json
"ai_client": "will"
```

(use the same name as the Instantly client). Then enrichment uses `WILL_*` AI keys when set.

## Debugging

Set logging to see pool selection (Python logging level DEBUG), or run with `--debug` if your entrypoint enables file debug logs.

Look for log lines:

- `mailclaw config: client=… prefix=… AI keys …`
- `AI key: provider=… scope=client:…` or `scope=global`
- `analytics_ask: profile=… client_name=…`

## Railway sample (two clients)

See `.env.example` — minimal pattern:

- `INSTANTLY_CLIENT_WILL=…`
- `INSTANTLY_CLIENT_GD=…`
- `WILL_GEMINI_API_KEY=…` + `WILL_GEMINI_API_KEY_2=…` (three keys if you add `_3`)
- `GD_GEMINI_API_KEY=…`
- Global fallback: `GEMINI_API_KEY=…` (optional if every client has prefixed keys)
