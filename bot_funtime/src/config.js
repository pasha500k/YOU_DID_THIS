const path = require("path");
const dotenv = require("dotenv");

dotenv.config();

function readNumber(name, defaultValue) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return defaultValue;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

function readBoolean(name, defaultValue) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return defaultValue;
  }

  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

function readIntegerList(name, defaultValue = []) {
  const value = process.env[name];
  if (!value) {
    return defaultValue;
  }

  return value
    .split(",")
    .map((entry) => Number(entry.trim()))
    .filter((entry) => Number.isInteger(entry));
}

function loadConfig() {
  return {
    verbose: readBoolean("VERBOSE", true),
    outputDir: path.resolve(process.cwd(), process.env.OUTPUT_DIR || "out"),
    minecraft: {
      host: process.env.MC_HOST || "",
      port: readNumber("MC_PORT", 25565),
      version: process.env.MC_VERSION || undefined,
      username: process.env.MC_USERNAME || "",
      password: process.env.MC_PASSWORD || undefined,
      auth: process.env.MC_AUTH || "offline",
      brand: process.env.MC_BRAND || "vanilla",
      profilesFolder: path.resolve(
        process.cwd(),
        process.env.MC_PROFILES_FOLDER || ".profiles",
      ),
      loginTimeoutMs: readNumber("MC_LOGIN_TIMEOUT_MS", 45000),
      loginCommandTemplate:
        process.env.MC_LOGIN_COMMAND_TEMPLATE === undefined
          ? "/login {password}"
          : process.env.MC_LOGIN_COMMAND_TEMPLATE,
      postLoginDelayMs: readNumber("MC_POST_LOGIN_DELAY_MS", 4000),
      postLoginSettleMs: readNumber("MC_POST_LOGIN_SETTLE_MS", 2500),
      checkTimeoutMs: readNumber("MC_CHECK_TIMEOUT_MS", 120000),
      readyStateTimeoutMs: readNumber("MC_READY_STATE_TIMEOUT_MS", 30000),
      connectRetryDelayMs: readNumber("MC_CONNECT_RETRY_DELAY_MS", 5000),
      connectRetryMaxAttempts: readNumber("MC_CONNECT_RETRY_MAX_ATTEMPTS", 0),
      serverSwitchRetryEnabled: readBoolean("MC_SERVER_SWITCH_RETRY_ENABLED", true),
      serverSwitchRetryDelayMs: readNumber("MC_SERVER_SWITCH_RETRY_DELAY_MS", 8000),
      serverSwitchRetryMaxAttempts: readNumber("MC_SERVER_SWITCH_RETRY_MAX_ATTEMPTS", 10),
    },
    auction: {
      command: process.env.AH_COMMAND || "/ah",
      openTimeoutMs: readNumber("AH_OPEN_TIMEOUT_MS", 20000),
      openDelayMs: readNumber("AH_OPEN_DELAY_MS", 2500),
      pageSettleMs: readNumber("AH_PAGE_SETTLE_MS", 1800),
      navigationTimeoutMs: readNumber("AH_NAVIGATION_TIMEOUT_MS", 10000),
      maxPages: readNumber("AH_MAX_PAGES", 75),
      nextPageSlot: readNumber("AH_NEXT_PAGE_SLOT", 50),
      ignoreSlots: readIntegerList("AH_IGNORE_SLOTS", [
        45, 46, 47, 48, 49, 51, 52, 53,
      ]),
      taxPercent: readNumber("AH_TAX_PERCENT", 5),
      minSample: readNumber("AH_MIN_SAMPLE", 3),
      minRoiPercent: readNumber("AH_MIN_ROI_PERCENT", 8),
      minProfit: readNumber("AH_MIN_PROFIT", 10000),
      maxGroupsForAi: readNumber("AH_MAX_GROUPS_FOR_AI", 120),
      maxOpportunitiesForAi: readNumber("AH_MAX_OPPORTUNITIES_FOR_AI", 40),
    },
    openai: {
      enabled: readBoolean("OPENAI_ENABLED", true),
      apiKey: process.env.OPENAI_API_KEY || "",
      model: process.env.OPENAI_MODEL || "gpt-5.4",
    },
    dashboard: {
      host: process.env.DASHBOARD_HOST || "127.0.0.1",
      port: readNumber("DASHBOARD_PORT", 3500),
    },
    liveView: {
      host: process.env.LIVE_VIEW_HOST || "127.0.0.1",
      port: readNumber("LIVE_VIEW_PORT", 3501),
      prefix: process.env.LIVE_VIEW_PREFIX || "",
      firstPerson: readBoolean("LIVE_VIEW_FIRST_PERSON", true),
      viewDistance: readNumber("LIVE_VIEW_DISTANCE", 6),
    },
    resourcePack: {
      autoAccept: readBoolean("RESOURCE_PACK_AUTO_ACCEPT", true),
      downloadEnabled: readBoolean("RESOURCE_PACK_DOWNLOAD_ENABLED", true),
      downloadTimeoutMs: readNumber("RESOURCE_PACK_DOWNLOAD_TIMEOUT_MS", 30000),
      loadedAckDelayMs: readNumber("RESOURCE_PACK_LOADED_ACK_DELAY_MS", 2500),
      loadedAckRepeats: readNumber("RESOURCE_PACK_LOADED_ACK_REPEATS", 2),
    },
    diagnostics: {
      packetTraceLimit: readNumber("PACKET_TRACE_LIMIT", 160),
    },
  };
}

module.exports = {
  loadConfig,
};
