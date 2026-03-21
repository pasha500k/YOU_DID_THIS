# bot_funtime

Standalone Node.js bot for Minecraft Java auction scanning.

What it does:

- connects to your server
- stays online and waits for manual console input
- starts a full auction scan only after you type `/ah` in the bot console
- saves the full market snapshot
- builds a local financial model
- optionally sends aggregated results to OpenAI for a narrative report
- exposes a local dashboard on port `3500` with status, chat, and scan info
- exposes a live first-person viewer page at `/live`
- auto-accepts the server resource pack for the bot session

## Output

Each scan creates a timestamped folder inside `out/`:

- `market_snapshot.json` - raw lots and page metadata
- `financial_model.json` - grouped prices, spreads, liquidity, opportunities
- `financial_model.md` - local text summary
- `ai_market_report.md` - OpenAI report when `OPENAI_API_KEY` is set

## Quick start

1. Copy [`.env.example`](.env.example) to `.env`
2. Fill in your Minecraft and OpenAI settings
3. Install dependencies:

```powershell
npm install
```

4. Start the bot:

```powershell
npm start
```

5. After the bot joins the server, use the console:

```text
/ah     start full auction scan
/spawn  send a normal server command without scanning
.help   show built-in help
.quit   disconnect and exit
```

6. Open the dashboard:

```text
http://127.0.0.1:3500
http://127.0.0.1:3500/live
```

## Notes

- The scanner is built for Minecraft Java GUI auctions opened via `/ah`
- By default the next-page button is assumed to be in slot `50`
- Non-market slots are skipped with `AH_IGNORE_SLOTS`
- Price and seller parsing supports both Russian and English lore patterns
- Some servers may need custom `.env` tuning if their GUI layout differs
- If the server uses cracked/offline auth with a chat password, the bot can send `/login {password}` automatically after spawn
- The bot can accept the server resource pack, but the embedded live viewer still renders prismarine-viewer textures rather than the server pack itself
- After a server switch like `/an304`, the bot now waits for the protocol to return to `play` before sending new commands
- If the server stays in `configuration` after a resource pack prompt, the bot now downloads the pack, sends `downloaded`, and then retries delayed `loaded` acknowledgements as a compatibility fallback
- After a server switch, the bot also refreshes client settings and brand in `play`, and the protocol watchdog timeout is relaxed to better tolerate slow backend keep-alives
- If a manual `/an...` switch returns the bot to the lobby with a backend-down message, the bot can retry that same switch automatically with a bounded delay

## Main config

- `MC_HOST`, `MC_PORT`, `MC_USERNAME`, `MC_AUTH` - server login settings
- `MC_BRAND` - client brand sent through the vanilla plugin channel, default `vanilla`
- `MC_CHECK_TIMEOUT_MS` - client-side keep-alive watchdog timeout, default `120000`
- `MC_CONNECT_RETRY_DELAY_MS` - delay before retrying a transient connect/login failure like `ECONNRESET`, default `5000`
- `MC_CONNECT_RETRY_MAX_ATTEMPTS` - max startup retry attempts, `0` means retry forever, default `0`
- `MC_LOGIN_COMMAND_TEMPLATE` - post-spawn login command for offline/cracked servers, default `/login {password}`
- `MC_READY_STATE_TIMEOUT_MS` - how long console input waits for the client to return to `play`, default `30000`
- `MC_SERVER_SWITCH_RETRY_ENABLED` - retry a manual `/an...` switch when the server says the backend shut down and moved the bot back to the lobby, default `true`
- `MC_SERVER_SWITCH_RETRY_DELAY_MS` - pause before retrying the same `/an...` switch, default `8000`
- `MC_SERVER_SWITCH_RETRY_MAX_ATTEMPTS` - maximum automatic retries for the same `/an...` command, default `10`
- `AH_NEXT_PAGE_SLOT` - slot index for the next-page button
- `AH_IGNORE_SLOTS` - slot indexes to skip
- `AH_TAX_PERCENT` - fee/tax used in the financial model
- `OPENAI_MODEL` - OpenAI model for the report, default `gpt-5.4`
- `DASHBOARD_PORT` - local dashboard port, default `3500`
- `LIVE_VIEW_PORT` - internal prismarine-viewer port, default `3501`
- `RESOURCE_PACK_AUTO_ACCEPT` - accept server resource packs automatically, default `true`
- `RESOURCE_PACK_DOWNLOAD_ENABLED` - download the server resource pack before sending follow-up status packets, default `true`
- `RESOURCE_PACK_DOWNLOAD_TIMEOUT_MS` - timeout for downloading the server resource pack before reporting a failed download, default `30000`
- `RESOURCE_PACK_LOADED_ACK_DELAY_MS` - delay before each delayed resource-pack loaded acknowledgement, default `2500`
- `RESOURCE_PACK_LOADED_ACK_REPEATS` - how many delayed loaded acknowledgements to send while stuck in `configuration`, default `2`
- `PACKET_TRACE_LIMIT` - how many recent packets to keep on the dashboard, default `160`

## Help

```powershell
node src/index.js --help
```
