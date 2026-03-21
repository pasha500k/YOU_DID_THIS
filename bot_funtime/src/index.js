const fs = require("fs");
const path = require("path");
const readline = require("readline");

const {
  connectAuctionBot,
  rememberServerSwitchCommand,
  scanAuctionHouse,
} = require("./auction-scanner");
const { loadConfig } = require("./config");
const { createDashboardServer } = require("./dashboard");
const {
  buildFinancialModel,
  renderFinancialModelMarkdown,
} = require("./financial-model");
const { createLiveView } = require("./live-view");
const { createLogger } = require("./logger");
const { generateOpenAiReport } = require("./openai-report");
const { createRuntimeState } = require("./runtime-state");

function printHelp() {
  console.log(`
bot_funtime

Usage:
  node src/index.js
  npm start

Required env:
  MC_HOST
  MC_USERNAME

Console commands:
  /ah     start a full market scan
  anything else is sent to the server chat as-is
  .help   show this help
  .quit   disconnect the bot and exit
`);
}

function ensureConfig(config) {
  const missing = [];
  if (!config.minecraft.host) {
    missing.push("MC_HOST");
  }
  if (!config.minecraft.username) {
    missing.push("MC_USERNAME");
  }

  if (missing.length > 0) {
    throw new Error(`Missing required env vars: ${missing.join(", ")}`);
  }
}

function makeRunDirectory(rootDir) {
  fs.mkdirSync(rootDir, { recursive: true });
  const runDir = path.join(
    rootDir,
    new Date().toISOString().replace(/[:.]/g, "-"),
  );
  fs.mkdirSync(runDir, { recursive: true });
  return runDir;
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeText(filePath, text) {
  fs.writeFileSync(filePath, `${text.trim()}\n`, "utf8");
}

function writeDiagnosticsSnapshot(config, runtimeState, log, suffix = "snapshot") {
  const diagnosticsDir = path.join(config.outputDir, "diagnostics");
  fs.mkdirSync(diagnosticsDir, { recursive: true });

  const filePath = path.join(
    diagnosticsDir,
    `${new Date().toISOString().replace(/[:.]/g, "-")}-${suffix}.json`,
  );

  writeJson(filePath, runtimeState.snapshot());
  log.info(`Diagnostics snapshot saved: ${filePath}`);
  return filePath;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryConnect(error) {
  const message = error instanceof Error ? error.message : String(error || "");
  return [
    "ECONNRESET",
    "ECONNREFUSED",
    "ETIMEDOUT",
    "socket hang up",
    "Bot disconnected before it finished logging in.",
    "Bot login timeout exceeded.",
    "read ECONNRESET",
  ].some((fragment) => message.includes(fragment));
}

async function connectAuctionBotWithRetry(config, log, runtimeState) {
  const delayMs = Math.max(1000, config.minecraft.connectRetryDelayMs || 5000);
  const maxAttempts = Math.max(0, config.minecraft.connectRetryMaxAttempts || 0);

  for (let attempt = 1; ; attempt += 1) {
    runtimeState.setConnectionStatus(attempt === 1 ? "connecting" : "reconnecting");
    runtimeState.addEvent("INFO", `Connection attempt ${attempt} started.`);

    try {
      const bot = await connectAuctionBot(config, log, runtimeState);
      runtimeState.setLastError(null);
      return bot;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      runtimeState.setLastError(message);
      runtimeState.addEvent("WARN", `Connection attempt ${attempt} failed: ${message}`);
      log.warn(`Connection attempt ${attempt} failed: ${message}`);

      if (!shouldRetryConnect(error)) {
        throw error;
      }

      if (maxAttempts > 0 && attempt >= maxAttempts) {
        throw new Error(
          `Could not connect after ${attempt} attempts. Last error: ${message}`,
        );
      }

      log.info(`Retrying connection in ${delayMs} ms.`);
      runtimeState.addEvent("INFO", `Retrying connection in ${delayMs} ms.`);
      await sleep(delayMs);
    }
  }
}

function waitForPlayState(bot, config, runtimeState, log, contextLabel) {
  if (bot?._client?.state === "play") {
    return Promise.resolve();
  }

  const timeoutMs = config.minecraft.readyStateTimeoutMs;
  const startedAt = Date.now();
  const waitMessage = `Waiting for play state before ${contextLabel}. Current state: ${bot?._client?.state || "unknown"}.`;
  runtimeState.addEvent("INFO", waitMessage);
  log.info(waitMessage);

  return new Promise((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      clearInterval(interval);
      clearTimeout(timer);
      bot.removeListener("end", onEnd);
      bot.removeListener("error", onError);
    };

    const finish = (handler) => (value) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      handler(value);
    };

    const onEnd = finish(() =>
      reject(new Error(`Bot disconnected while waiting for play state before ${contextLabel}.`)),
    );
    const onError = finish((error) =>
      reject(
        error instanceof Error
          ? error
          : new Error(`Bot error while waiting for play state before ${contextLabel}.`),
      ),
    );

    const interval = setInterval(() => {
      if (bot?._client?.state === "play") {
        const readyMessage = `Play state is ready after ${Date.now() - startedAt} ms for ${contextLabel}.`;
        runtimeState.addEvent("INFO", readyMessage);
        log.info(readyMessage);
        finish(resolve)();
      }
    }, 150);

    const timer = setTimeout(
      finish(() =>
        reject(
          new Error(
            `Timed out waiting for play state before ${contextLabel}. Last state: ${bot?._client?.state || "unknown"}.`,
          ),
        ),
      ),
      timeoutMs,
    );

    bot.once("end", onEnd);
    bot.once("error", onError);
  });
}

async function generateArtifacts(scanResult, config, log) {
  const runDir = makeRunDirectory(config.outputDir);
  log.info(`Run directory: ${runDir}`);
  writeJson(path.join(runDir, "market_snapshot.json"), scanResult);

  const financialModel = buildFinancialModel(scanResult, config);
  writeJson(path.join(runDir, "financial_model.json"), financialModel);
  writeText(
    path.join(runDir, "financial_model.md"),
    renderFinancialModelMarkdown(financialModel),
  );

  if (config.openai.enabled && config.openai.apiKey) {
    log.info(`Generating OpenAI report with model ${config.openai.model}.`);
    const aiReport = await generateOpenAiReport(financialModel, config);
    writeText(path.join(runDir, "ai_market_report.md"), aiReport);
  } else {
    log.info("OpenAI report skipped because OPENAI_API_KEY is missing or disabled.");
  }

  log.info("Auction scan and financial model are complete.");
}

async function main() {
  if (process.argv.includes("--help")) {
    printHelp();
    return;
  }

  const config = loadConfig();
  const log = createLogger(config.verbose);
  const runtimeState = createRuntimeState();
  ensureConfig(config);

  const dashboard = createDashboardServer(runtimeState, log, config);
  const bot = await connectAuctionBotWithRetry(config, log, runtimeState);
  const liveView = createLiveView(bot, config, log, runtimeState);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true,
  });
  log.bindReadline(rl);

  let isBusy = false;
  let isShuttingDown = false;

  const shutdown = async () => {
    if (isShuttingDown) {
      return;
    }

    isShuttingDown = true;
    log.unbindReadline();
    rl.close();
    runtimeState.setConnectionStatus("stopping");
    runtimeState.addEvent("INFO", "Shutdown requested.");
    dashboard.close();
    liveView.close();

    try {
      bot.quit("console exit");
    } catch {
      bot.end();
    }
  };

  const runAuctionScan = async (command) => {
    if (isBusy) {
      log.warn("Scan is already running. Wait until it finishes.");
      return;
    }

    isBusy = true;
    runtimeState.setBusy(true);
    try {
      await waitForPlayState(bot, config, runtimeState, log, `auction scan ${command}`);
      runtimeState.addEvent("INFO", `Starting auction scan from console command: ${command}`);
      log.info(`Starting auction scan from console command: ${command}`);
      const scanResult = await scanAuctionHouse(bot, config, log, {
        sendCommand: true,
        command,
      });
      await generateArtifacts(scanResult, config, log);
      runtimeState.setLastScanSummary({
        scannedAt: scanResult.scannedAt,
        pages: scanResult.pages.length,
        listings: scanResult.listings.length,
      });
      runtimeState.setLastError(null);
    } catch (error) {
      log.error(`Scan failed: ${error.message}`);
      runtimeState.setLastError(error.message);
      runtimeState.addEvent("ERROR", `Scan failed: ${error.message}`);
    } finally {
      isBusy = false;
      runtimeState.setBusy(false);
    }
  };

  bot.on("end", () => {
    if (!isShuttingDown) {
      log.unbindReadline();
      log.warn("Bot disconnected from the server. The process will stop.");
      runtimeState.setConnectionStatus("disconnected");
      runtimeState.addEvent("WARN", "Bot disconnected from the server.");
      writeDiagnosticsSnapshot(config, runtimeState, log, "disconnect");
      rl.close();
      process.exitCode = 1;
    }
  });

  bot.on("kicked", () => {
    if (!isShuttingDown) {
      writeDiagnosticsSnapshot(config, runtimeState, log, "kicked");
    }
  });

  process.on("SIGINT", async () => {
    log.info("Stopping bot.");
    await shutdown();
    process.exit(0);
  });

  printHelp();
  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async (line) => {
    const input = line.trim();
    if (!input) {
      rl.prompt();
      return;
    }

    if (input === ".help") {
      printHelp();
      rl.prompt();
      return;
    }

    if (input === ".quit" || input === ".exit") {
      await shutdown();
      process.exit(0);
      return;
    }

    if (input === config.auction.command) {
      await runAuctionScan(input);
      rl.prompt();
      return;
    }

    if (isBusy) {
      log.warn("Bot is busy scanning. Wait until the scan finishes.");
      runtimeState.addEvent("WARN", "Ignored console input while scan was running.");
      rl.prompt();
      return;
    }

    try {
      await waitForPlayState(bot, config, runtimeState, log, `console input ${input}`);
    } catch (error) {
      log.warn(error.message);
      runtimeState.setLastError(error.message);
      runtimeState.addEvent("WARN", error.message);
      rl.prompt();
      return;
    }

    bot.chat(input);
    rememberServerSwitchCommand(bot, input);
    runtimeState.addChat("bot", input);
    runtimeState.addEvent("INFO", `Sent console input to server: ${input}`);
    log.info(`Sent chat input: ${input}`);
    rl.prompt();
  });
}

main().catch((error) => {
  console.error(`[FATAL] ${error.message}`);
  process.exitCode = 1;
});
