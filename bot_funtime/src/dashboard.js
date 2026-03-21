const http = require("http");

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderRows(items, formatter) {
  if (!items || items.length === 0) {
    return "<li>No data yet.</li>";
  }

  return items.map(formatter).join("");
}

function renderPage(snapshot, liveViewConfig) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>bot_funtime dashboard</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4efe7;
        --panel: #fffaf3;
        --ink: #1e1d1a;
        --muted: #6e685f;
        --accent: #8b4f2a;
        --line: #d9cdbf;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        padding: 24px;
        background:
          radial-gradient(circle at top left, #fff6db 0, transparent 35%),
          linear-gradient(180deg, #f8f2e7 0%, var(--bg) 100%);
        color: var(--ink);
        font-family: Georgia, "Times New Roman", serif;
      }
      .wrap {
        max-width: 1120px;
        margin: 0 auto;
        display: grid;
        gap: 16px;
      }
      .hero, .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 20px;
        box-shadow: 0 14px 40px rgba(49, 34, 15, 0.08);
      }
      .hero h1, .panel h2 {
        margin: 0 0 10px;
        font-weight: 700;
      }
      .meta {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin-top: 16px;
      }
      .card {
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 12px;
        background: #fffdf9;
      }
      .label {
        display: block;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 4px;
      }
      .value {
        font-size: 18px;
        font-weight: 700;
      }
      .grid {
        display: grid;
        gap: 16px;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      }
      pre {
        white-space: pre-wrap;
        word-break: break-word;
      }
      ul {
        margin: 0;
        padding-left: 18px;
      }
      li {
        margin: 0 0 8px;
        line-height: 1.35;
      }
      code {
        background: #f3eadf;
        border-radius: 6px;
        padding: 2px 6px;
      }
      .note {
        color: var(--muted);
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <section class="hero">
        <h1>bot_funtime dashboard</h1>
        <p class="note">
          This page shows connection state, chat history, scan status, and resource-pack state.
        </p>
        <div class="meta">
          <div class="card">
            <span class="label">Connection</span>
            <span class="value">${escapeHtml(snapshot.connectionStatus)}</span>
          </div>
          <div class="card">
            <span class="label">Busy</span>
            <span class="value">${snapshot.busy ? "yes" : "no"}</span>
          </div>
          <div class="card">
            <span class="label">BotFilter</span>
            <span class="value">${snapshot.botFilterDetected ? "detected" : "not detected"}</span>
          </div>
          <div class="card">
            <span class="label">Started</span>
            <span class="value">${escapeHtml(snapshot.startedAt)}</span>
          </div>
        </div>
        <p class="note" style="margin-top: 12px;">
          Resource pack: ${escapeHtml(
            snapshot.resourcePack
              ? `${snapshot.resourcePack.accepted ? "accepted" : "seen"} ${snapshot.resourcePack.url || snapshot.resourcePack.token || ""}`.trim()
              : "none",
          )}
        </p>
        <p class="note" style="margin-top: 16px;">
          Local commands: <code>.help</code>, <code>.quit</code>. Any other input in the bot console is sent to the server.
        </p>
        <p class="note" style="margin-top: 8px;">
          Live view: <a href="/live">/live</a>
        </p>
      </section>
      <div class="grid">
        <section class="panel">
          <h2>Recent Chat</h2>
          <ul>${renderRows(snapshot.recentChat, (item) =>
            `<li><strong>${escapeHtml(item.direction)}</strong> [${escapeHtml(item.at)}] ${escapeHtml(item.message)}</li>`)}
          </ul>
        </section>
        <section class="panel">
          <h2>Recent Events</h2>
          <ul>${renderRows(snapshot.recentEvents, (item) =>
            `<li><strong>${escapeHtml(item.level)}</strong> [${escapeHtml(item.at)}] ${escapeHtml(item.message)}</li>`)}
          </ul>
        </section>
        <section class="panel">
          <h2>Last Scan</h2>
          <pre>${escapeHtml(JSON.stringify(snapshot.lastScanSummary, null, 2) || "null")}</pre>
        </section>
        <section class="panel">
          <h2>Last Error</h2>
          <pre>${escapeHtml(snapshot.lastError || "none")}</pre>
        </section>
        <section class="panel">
          <h2>Last Kick</h2>
          <pre>${escapeHtml(snapshot.lastKickReason || "none")}</pre>
        </section>
        <section class="panel">
          <h2>Recent Packets</h2>
          <pre>${escapeHtml(JSON.stringify(snapshot.recentPackets || [], null, 2))}</pre>
        </section>
      </div>
    </div>
    <script>
      setTimeout(() => window.location.reload(), 3000);
    </script>
  </body>
</html>`;
}

function renderLivePage(snapshot, liveViewConfig) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>bot_funtime live</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #13100c;
        --panel: #1f1a14;
        --ink: #f6eee2;
        --muted: #c1b4a4;
        --line: #3a3026;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background:
          radial-gradient(circle at top, rgba(215, 146, 80, 0.18), transparent 35%),
          linear-gradient(180deg, #1a140f 0%, var(--bg) 100%);
        color: var(--ink);
        font-family: Georgia, "Times New Roman", serif;
      }
      .wrap {
        min-height: 100vh;
        display: grid;
        grid-template-rows: auto 1fr;
        gap: 12px;
        padding: 16px;
      }
      .topbar {
        background: rgba(31, 26, 20, 0.88);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 14px 16px;
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: center;
        flex-wrap: wrap;
      }
      .title {
        font-size: 20px;
        font-weight: 700;
      }
      .meta {
        display: flex;
        gap: 14px;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: 14px;
      }
      .viewer-shell {
        background: rgba(31, 26, 20, 0.92);
        border: 1px solid var(--line);
        border-radius: 18px;
        overflow: hidden;
        min-height: calc(100vh - 100px);
      }
      iframe {
        width: 100%;
        height: calc(100vh - 120px);
        border: 0;
        display: block;
        background: #000;
      }
      a {
        color: #ffd7a1;
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <section class="topbar">
        <div>
          <div class="title">bot_funtime live view</div>
          <div class="meta">
            <span>connection: ${escapeHtml(snapshot.connectionStatus)}</span>
            <span>busy: ${snapshot.busy ? "yes" : "no"}</span>
            <span>botfilter: ${snapshot.botFilterDetected ? "detected" : "not detected"}</span>
            <span>resource pack: ${snapshot.resourcePack?.accepted ? "accepted" : snapshot.resourcePack ? "seen" : "none"}</span>
          </div>
        </div>
        <div class="meta">
          <a href="/">dashboard</a>
          <span>viewer port: ${liveViewConfig.port}</span>
        </div>
      </section>
      <section class="topbar" style="padding-top: 10px; padding-bottom: 10px;">
        <div class="meta">
          <span>
            Note: the server resource pack is accepted by the bot, but the embedded viewer still uses prismarine-viewer textures.
          </span>
        </div>
      </section>
      <section class="viewer-shell">
        <iframe
          src=""
          title="bot live view"
          id="live-frame"
          allowfullscreen
        ></iframe>
      </section>
    </div>
    <script>
      const host = window.location.hostname || "127.0.0.1";
      const port = ${JSON.stringify(liveViewConfig.port)};
      const prefix = ${JSON.stringify(liveViewConfig.prefix || "")};
      const path = prefix && prefix !== "/" ? prefix : "";
      document.getElementById("live-frame").src = "http://" + host + ":" + port + path + "/";
    </script>
  </body>
</html>`;
}

function createDashboardServer(runtimeState, log, config) {
  const port = config.dashboard.port;
  const host = config.dashboard.host;

  const server = http.createServer((request, response) => {
    const snapshot = runtimeState.snapshot();

    if (request.url === "/api/state") {
      response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
      response.end(JSON.stringify(snapshot, null, 2));
      return;
    }

    if (request.url === "/" || request.url === "/index.html") {
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      response.end(renderPage(snapshot, config.liveView));
      return;
    }

    if (request.url === "/live") {
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      response.end(renderLivePage(snapshot, config.liveView));
      return;
    }

    response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    response.end("Not found");
  });

  server.on("error", (error) => {
    if (error?.code === "EADDRINUSE") {
      throw new Error(
        `Dashboard port ${host}:${port} is already in use. Stop the previous bot process or change DASHBOARD_PORT in .env.`,
      );
    }

    throw error;
  });

  server.listen(port, host, () => {
    log.info(`Dashboard is available at http://${host}:${port}`);
    runtimeState.addEvent("INFO", `Dashboard started on http://${host}:${port}`);
  });

  return server;
}

module.exports = {
  createDashboardServer,
};
