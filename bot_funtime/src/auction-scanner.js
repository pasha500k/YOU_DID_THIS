const mineflayer = require("mineflayer");
const http = require("http");
const https = require("https");
const util = require("util");

const { cleanText, extractItemMeta } = require("./mc-text");

const CONTROL_ITEM_PATTERN =
  /(next|previous|prev|page|refresh|sort|back|\u0441\u043b\u0435\u0434|\u043f\u0440\u0435\u0434|\u0441\u0442\u0440\u0430\u043d\u0438\u0446|\u043e\u0431\u043d\u043e\u0432|\u0441\u043e\u0440\u0442|\u043d\u0430\u0437\u0430\u0434)/iu;
const PRICE_PATTERNS = [
  /(?:\u0446\u0435\u043d\u0430|\u0441\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c|price|cost|buyout)[^0-9]{0,20}([\d\s,._]+)/iu,
  /([\d\s,._]+)\s*(?:\$|\u043c\u043e\u043d\u0435\u0442|\u043c\u043e\u043d\u0435\u0442\u044b|coins?)/iu,
];
const SELLER_PATTERNS = [
  /(?:\u043f\u0440\u043e\u0434\u0430\u0432\u0435\u0446|seller|\u0438\u0433\u0440\u043e\u043a|\u043d\u0438\u043a|nick)[^A-Za-z\u0410-\u042f\u0430-\u044f0-9_]{0,20}([A-Za-z0-9_]+)/iu,
];
const TIME_PATTERNS = [
  /(?:\u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c|remaining|expires|expiry)[^:]{0,10}:?\s*(.+)/iu,
];
const BOT_FILTER_PATTERN =
  /(botfilter|\u0432\u0432\u0435\u0434\u0438\u0442\u0435 \u043d\u043e\u043c\u0435\u0440 \u0441 \u043a\u0430\u0440\u0442\u0438\u043d\u043a\u0438)/iu;
const SERVER_SWITCH_COMMAND_PATTERN = /^\/an\d+\b/iu;
const SERVER_SWITCH_BOUNCE_PATTERN =
  /\u0441\u0435\u0440\u0432\u0435\u0440,\s*\u043d\u0430\s*\u043a\u043e\u0442\u043e\u0440\u043e\u043c\s*\u0432\u044b\s*\u0438\u0433\u0440\u0430\u043b\u0438,\s*\u0432\u044b\u043a\u043b\u044e\u0447\u0438\u043b\u0441\u044f.*\u043f\u0435\u0440\u0435\u043c\u0435\u0449\u0435\u043d\u044b\s*\u0432\s*\u043b\u043e\u0431\u0431\u0438/iu;
const CONFIGURATION_BLOCKED_PACKETS = new Set([
  "position",
  "position_look",
  "look",
  "flying",
  "vehicle_move",
  "steer_vehicle",
  "boat_paddle_state",
]);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clearServerSwitchRetry(bot) {
  if (bot?._serverSwitchRetryTimer) {
    clearTimeout(bot._serverSwitchRetryTimer);
    bot._serverSwitchRetryTimer = null;
  }
}

function rememberServerSwitchCommand(bot, command) {
  if (!bot || !SERVER_SWITCH_COMMAND_PATTERN.test(command || "")) {
    return false;
  }

  bot._serverSwitchCommand = command;
  bot._serverSwitchRetryAttempts = 0;
  clearServerSwitchRetry(bot);
  return true;
}

function scheduleServerSwitchRetry(bot, config, log, runtimeState) {
  if (
    !bot?._client ||
    !config.minecraft.serverSwitchRetryEnabled ||
    !bot._serverSwitchCommand
  ) {
    return;
  }

  if (bot._serverSwitchRetryTimer) {
    return;
  }

  const maxAttempts = Math.max(0, config.minecraft.serverSwitchRetryMaxAttempts || 0);
  const nextAttempt = (bot._serverSwitchRetryAttempts || 0) + 1;
  if (nextAttempt > maxAttempts) {
    const message =
      `Server-switch retry limit reached for ${bot._serverSwitchCommand}. ` +
      `No more automatic retries will be sent.`;
    runtimeState?.addEvent("WARN", message);
    log.warn(message);
    clearServerSwitchRetry(bot);
    return;
  }

  const delayMs = Math.max(1000, config.minecraft.serverSwitchRetryDelayMs || 8000);
  const command = bot._serverSwitchCommand;
  bot._serverSwitchRetryTimer = setTimeout(() => {
    bot._serverSwitchRetryTimer = null;

    if (!bot?._client) {
      return;
    }

    if (bot._client.state !== "play") {
      scheduleServerSwitchRetry(bot, config, log, runtimeState);
      return;
    }

    bot._serverSwitchRetryAttempts = nextAttempt;
    bot.chat(command);
    runtimeState?.addChat("bot", command);
    const message =
      `Retrying server switch (${nextAttempt}/${maxAttempts}): ${command}`;
    runtimeState?.addEvent("WARN", message);
    log.warn(message);
  }, delayMs);

  runtimeState?.addEvent(
    "INFO",
    `Scheduled server-switch retry ${nextAttempt}/${maxAttempts} in ${delayMs} ms for ${command}.`,
  );
}

function getBrandChannelName(bot) {
  if (bot.supportFeature("customChannelMCPrefixed")) {
    return "MC|Brand";
  }

  if (bot.supportFeature("customChannelIdentifier")) {
    return "minecraft:brand";
  }

  throw new Error("Unsupported brand channel name.");
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "unknown size";
  }

  if (bytes < 1024) {
    return `${bytes} B`;
  }

  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }

  return `${(bytes / (1024 * 1024)).toFixed(2)} MiB`;
}

function downloadResourcePack(url, timeoutMs, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    if (!url) {
      reject(new Error("Resource-pack URL is empty."));
      return;
    }

    if (redirectCount > 5) {
      reject(new Error("Too many redirects while downloading the resource pack."));
      return;
    }

    const client = url.startsWith("https:") ? https : http;
    const request = client.get(url, (response) => {
      const statusCode = response.statusCode || 0;

      if (
        [301, 302, 303, 307, 308].includes(statusCode) &&
        response.headers.location
      ) {
        const redirectUrl = new URL(response.headers.location, url).toString();
        response.resume();
        resolve(downloadResourcePack(redirectUrl, timeoutMs, redirectCount + 1));
        return;
      }

      if (statusCode < 200 || statusCode >= 300) {
        response.resume();
        reject(new Error(`Resource-pack download failed with HTTP ${statusCode}.`));
        return;
      }

      let bytes = 0;
      response.on("data", (chunk) => {
        bytes += chunk.length;
      });
      response.on("end", () => {
        resolve({
          bytes,
          contentLength: Number(response.headers["content-length"]) || null,
          contentType: response.headers["content-type"] || "",
        });
      });
      response.on("error", reject);
    });

    request.setTimeout(timeoutMs, () => {
      request.destroy(new Error("Resource-pack download timed out."));
    });
    request.on("error", reject);
  });
}

function summarizeValue(value, depth = 0) {
  if (value === null || value === undefined) {
    return value;
  }

  if (depth >= 2) {
    if (Array.isArray(value)) {
      return `[array:${value.length}]`;
    }
    if (Buffer.isBuffer(value)) {
      return `[buffer:${value.length}]`;
    }
    if (typeof value === "object") {
      return "[object]";
    }
    return value;
  }

  if (typeof value === "string") {
    return value.length > 180 ? `${value.slice(0, 180)}...` : value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (Buffer.isBuffer(value)) {
    return `[buffer:${value.length}]`;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 12).map((entry) => summarizeValue(entry, depth + 1));
  }

  if (typeof value === "object") {
    const output = {};
    for (const [key, entry] of Object.entries(value).slice(0, 16)) {
      output[key] = summarizeValue(entry, depth + 1);
    }
    return output;
  }

  return String(value);
}

function formatStructuredMessage(value) {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(formatStructuredMessage).join("");
  }

  if (typeof value === "object") {
    if (typeof value.text === "string") {
      const extra = Array.isArray(value.extra)
        ? value.extra.map(formatStructuredMessage).join("")
        : "";
      return `${value.text}${extra}`;
    }

    if (Array.isArray(value.extra)) {
      return value.extra.map(formatStructuredMessage).join("");
    }
  }

  return "";
}

function formatKickReason(reason) {
  const structured = cleanText(formatStructuredMessage(reason));
  if (structured) {
    return structured;
  }

  try {
    return JSON.stringify(reason);
  } catch {
    return util.inspect(reason, { depth: 6, colors: false, compact: true });
  }
}

function sendResourcePackStatusAck(bot, resourcePackId, result) {
  if (bot.supportFeature("resourcePackUsesUUID") && resourcePackId) {
    bot._client.write("resource_pack_receive", {
      uuid: resourcePackId,
      result,
    });
    return;
  }

  bot._client.write("resource_pack_receive", {
    result,
  });
}

function sendResourcePackAcceptedAck(bot, resourcePackId) {
  sendResourcePackStatusAck(bot, resourcePackId, 3);
}

function sendResourcePackDownloadedAck(bot, resourcePackId) {
  sendResourcePackStatusAck(bot, resourcePackId, 4);
}

function sendResourcePackLoadedAck(bot, resourcePackId) {
  sendResourcePackStatusAck(bot, resourcePackId, 0);
}

function scheduleResourcePackLoadedRetries(
  bot,
  resourcePackId,
  config,
  log,
  runtimeState,
) {
  const repeats = Math.max(0, config.resourcePack.loadedAckRepeats || 0);
  const delayMs = Math.max(250, config.resourcePack.loadedAckDelayMs || 2500);

  for (let attempt = 1; attempt <= repeats; attempt += 1) {
    setTimeout(() => {
      if (!bot?._client || bot._client.state !== "configuration") {
        return;
      }

      try {
        sendResourcePackLoadedAck(bot, resourcePackId);
        const message = `Sent delayed resource-pack loaded acknowledgement (${attempt}/${repeats}).`;
        runtimeState?.addEvent("INFO", message);
        log.info(message);
      } catch (error) {
        const message = `Failed repeated resource-pack acknowledgement: ${error.message}`;
        runtimeState?.addEvent("WARN", message);
        log.warn(message);
      }
    }, delayMs * attempt);
  }
}

function scheduleFinishConfigurationFallback(bot, config, log, runtimeState) {
  const baseDelay =
    Math.max(250, config.resourcePack.loadedAckDelayMs || 2500) *
      Math.max(1, config.resourcePack.loadedAckRepeats || 1) +
    1000;

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    setTimeout(() => {
      if (!bot?._client || bot._client.state !== "configuration") {
        return;
      }

      try {
        bot._client.write("finish_configuration", {});
        const message = `Sent finish_configuration fallback (${attempt}/2).`;
        runtimeState?.addEvent("INFO", message);
        log.info(message);
      } catch (error) {
        const message = `Failed finish_configuration fallback: ${error.message}`;
        runtimeState?.addEvent("WARN", message);
        log.warn(message);
      }
    }, baseDelay + (attempt - 1) * 3000);
  }
}

async function handleAcceptedResourcePack(
  bot,
  resourcePackId,
  url,
  config,
  log,
  runtimeState,
) {
  if (!config.resourcePack.downloadEnabled) {
    runtimeState?.addEvent(
      "INFO",
      "Resource-pack download is disabled by config. Skipping download step.",
    );
    log.info("Resource-pack download is disabled by config. Skipping download step.");
    scheduleResourcePackLoadedRetries(
      bot,
      resourcePackId,
      config,
      log,
      runtimeState,
    );
    scheduleFinishConfigurationFallback(bot, config, log, runtimeState);
    return;
  }

  if (!url) {
    scheduleResourcePackLoadedRetries(
      bot,
      resourcePackId,
      config,
      log,
      runtimeState,
    );
    scheduleFinishConfigurationFallback(bot, config, log, runtimeState);
    return;
  }

  const timeoutMs = Math.max(
    5000,
    config.resourcePack.downloadTimeoutMs || 30000,
  );

  try {
    runtimeState?.addEvent("INFO", `Downloading resource pack from ${url}`);
    const result = await downloadResourcePack(url, timeoutMs);
    const sizeHint = result.contentLength || result.bytes;

    runtimeState?.addEvent(
      "INFO",
      `Resource pack downloaded: ${formatBytes(sizeHint)} (${result.contentType || "unknown type"}).`,
    );
    log.info(
      `Resource pack downloaded: ${formatBytes(sizeHint)} (${result.contentType || "unknown type"}).`,
    );

    if (bot?._client?.state === "configuration") {
      sendResourcePackDownloadedAck(bot, resourcePackId);
      runtimeState?.addEvent("INFO", "Sent resource-pack downloaded acknowledgement.");
      log.info("Sent resource-pack downloaded acknowledgement.");
    }

    scheduleResourcePackLoadedRetries(
      bot,
      resourcePackId,
      config,
      log,
      runtimeState,
    );
    scheduleFinishConfigurationFallback(bot, config, log, runtimeState);
  } catch (error) {
    runtimeState?.addEvent("WARN", `Resource-pack download failed: ${error.message}`);
    runtimeState?.setResourcePack({
      at: new Date().toISOString(),
      url,
      token: String(resourcePackId ?? ""),
      accepted: false,
      error: error.message,
    });
    log.warn(`Resource-pack download failed: ${error.message}`);

    if (bot?._client) {
      try {
        sendResourcePackStatusAck(bot, resourcePackId, 2);
      } catch {
        // no-op
      }
    }
  }
}

function resendClientSettings(bot, log, runtimeState, context = "client refresh") {
  if (typeof bot?.setSettings !== "function") {
    return;
  }

  try {
    bot.setSettings({});
    const message = `Resent client settings during ${context}.`;
    runtimeState?.addEvent("INFO", message);
    log.info(message);
  } catch (error) {
    runtimeState?.addEvent(
      "WARN",
      `Failed to resend client settings: ${error.message}`,
    );
    log.warn(`Failed to resend client settings: ${error.message}`);
  }
}

function resendClientBrand(bot, brand, log, runtimeState) {
  if (!bot?._client || typeof bot._client.writeChannel !== "function") {
    return;
  }

  try {
    const brandChannel = getBrandChannelName(bot);
    bot._client.writeChannel(brandChannel, brand || "vanilla");
    runtimeState?.addEvent("INFO", `Resent client brand during play: ${brand || "vanilla"}.`);
    log.info(`Resent client brand during play: ${brand || "vanilla"}.`);
  } catch (error) {
    runtimeState?.addEvent(
      "WARN",
      `Failed to resend client brand: ${error.message}`,
    );
    log.warn(`Failed to resend client brand: ${error.message}`);
  }
}

function schedulePostSwitchClientRefresh(bot, config, log, runtimeState) {
  setTimeout(() => {
    if (!bot?._client || bot._client.state !== "play") {
      return;
    }

    resendClientSettings(bot, log, runtimeState, "play recovery");
    resendClientBrand(bot, config.minecraft.brand, log, runtimeState);
  }, 250);
}

function attachPacketTracing(bot, config, runtimeState) {
  const limit = config.diagnostics.packetTraceLimit;
  const client = bot._client;
  if (!client) {
    return;
  }

  const originalWrite = client.write.bind(client);

  client.on("packet", (data, metadata) => {
    if (metadata?.name === "start_configuration") {
      runtimeState?.addEvent("INFO", "Server switch entered configuration state.");
    }

    if (metadata?.name === "finish_configuration") {
      runtimeState?.addEvent("INFO", "Server switch finished configuration state.");
    }

    runtimeState?.addPacket(
      {
        direction: "in",
        name: metadata?.name || "unknown",
        state: metadata?.state || "unknown",
        data: summarizeValue(data),
      },
      limit,
    );
  });

  client.write = (name, params) => {
    if (
      client.state === "configuration" &&
      CONFIGURATION_BLOCKED_PACKETS.has(name)
    ) {
      runtimeState?.addPacket(
        {
          direction: "drop",
          name,
          state: client.state,
          data: summarizeValue(params),
        },
        limit,
      );
      runtimeState?.addEvent(
        "WARN",
        `Dropped play packet during configuration: ${name}`,
      );
      return;
    }

    runtimeState?.addPacket(
      {
        direction: "out",
        name,
        state: client.state || "unknown",
        data: summarizeValue(params),
      },
      limit,
    );
    return originalWrite(name, params);
  };
}

function parseDigits(rawValue) {
  if (!rawValue) {
    return null;
  }

  const digits = String(rawValue).replace(/[^\d]/g, "");
  if (!digits) {
    return null;
  }

  const parsed = Number(digits);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstMatch(lines, patterns) {
  for (const line of lines) {
    for (const pattern of patterns) {
      const match = line.match(pattern);
      if (match) {
        return match[1] ? match[1].trim() : line.trim();
      }
    }
  }

  return null;
}

function normalizeName(name, enchantments) {
  const base = cleanText(name).toLowerCase();
  const ench = enchantments.length > 0 ? ` | ${enchantments.sort().join(",")}` : "";
  return `${base}${ench}`.trim();
}

function parsePageNumber(title, fallbackValue) {
  const cleanTitle = cleanText(title);
  const patterns = [
    /(\d+)\s*\/\s*(\d+)/,
    /(?:page|\u0441\u0442\u0440\u0430\u043d\u0438\u0446[\u0430\u044b]?|\u0441\u0442\u0440)\s*(\d+)/iu,
  ];

  for (const pattern of patterns) {
    const match = cleanTitle.match(pattern);
    if (match) {
      const page = Number(match[1]);
      if (Number.isFinite(page)) {
        return page;
      }
    }
  }

  return fallbackValue;
}

function waitForWindowOpen(bot, timeoutMs) {
  return new Promise((resolve, reject) => {
    const onOpen = (window) => {
      clearTimeout(timer);
      resolve(window);
    };

    const timer = setTimeout(() => {
      bot.removeListener("windowOpen", onOpen);
      reject(new Error("Auction window did not open in time."));
    }, timeoutMs);

    bot.once("windowOpen", onOpen);
  });
}

function waitForSpawn(bot, timeoutMs) {
  return new Promise((resolve, reject) => {
    let finished = false;

    const cleanup = () => {
      bot.removeListener("spawn", onSpawn);
      bot.removeListener("error", onError);
      bot.removeListener("end", onEnd);
      bot.removeListener("kicked", onKicked);
      clearTimeout(timer);
    };

    const finish = (handler) => (value) => {
      if (finished) {
        return;
      }

      finished = true;
      cleanup();
      handler(value);
    };

    const onSpawn = finish(resolve);
    const onError = finish(reject);
    const onEnd = finish(() => {
      if (bot._botFilterDetected) {
        reject(
          new Error(
            "Server requested BotFilter verification. Manual approval or an admin-side exception is required before the bot can stay online.",
          ),
        );
        return;
      }

      reject(new Error("Bot disconnected before it finished logging in."));
    });
    const onKicked = finish((reason) =>
      reject(new Error(`Bot was kicked before spawn: ${reason}`)),
    );
    const timer = setTimeout(
      finish(() => reject(new Error("Bot login timeout exceeded."))),
      timeoutMs,
    );

    bot.once("spawn", onSpawn);
    bot.once("error", onError);
    bot.once("end", onEnd);
    bot.once("kicked", onKicked);
  });
}

function getTopInventorySlotLimit(window) {
  if (Number.isInteger(window.inventoryStart) && window.inventoryStart > 0) {
    return window.inventoryStart;
  }

  if (Number.isInteger(window.slots?.length) && window.slots.length > 36) {
    return window.slots.length - 36;
  }

  return window.slots?.length || 0;
}

function isLikelyControlItem(slot, listing, config) {
  if (config.auction.ignoreSlots.includes(slot)) {
    return true;
  }

  if (slot === config.auction.nextPageSlot) {
    return true;
  }

  if (!listing.price && CONTROL_ITEM_PATTERN.test(listing.displayName)) {
    return true;
  }

  return false;
}

function parseListing(item, slot, scanPageIndex, windowTitle) {
  if (!item) {
    return null;
  }

  const meta = extractItemMeta(item);
  const displayName = meta.cleanName || cleanText(item.name);
  const priceRaw = firstMatch(meta.lore, PRICE_PATTERNS);
  const seller = firstMatch(meta.lore, SELLER_PATTERNS);
  const timeLeft = firstMatch(meta.lore, TIME_PATTERNS);
  const price = parseDigits(priceRaw);
  const normalizedName = normalizeName(displayName, meta.enchantments);

  return {
    slot,
    scanPageIndex,
    pageNumber: parsePageNumber(windowTitle, scanPageIndex),
    itemName: item.name || "unknown",
    itemId: item.type ?? null,
    displayName,
    normalizedName,
    quantity: item.count || 1,
    price,
    priceRaw,
    seller,
    timeLeft,
    lore: meta.lore,
    enchantments: meta.enchantments,
  };
}

function fingerprintWindow(window, config, pageIndex) {
  const title = cleanText(window?.title);
  const topLimit = getTopInventorySlotLimit(window);
  const parts = [title, String(parsePageNumber(title, pageIndex))];

  for (let slot = 0; slot < topLimit; slot += 1) {
    if (config.auction.ignoreSlots.includes(slot)) {
      continue;
    }

    const item = window.slots[slot];
    if (!item) {
      parts.push(`${slot}:_`);
      continue;
    }

    const listing = parseListing(item, slot, pageIndex, title);
    parts.push(
      [
        slot,
        listing.displayName,
        listing.quantity,
        listing.price ?? "na",
        listing.seller ?? "na",
      ].join(":"),
    );
  }

  return parts.join("|");
}

async function waitForPageChange(bot, previousFingerprint, config) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= config.auction.navigationTimeoutMs) {
    const currentWindow = bot.currentWindow;
    if (currentWindow) {
      const currentFingerprint = fingerprintWindow(currentWindow, config, 0);
      if (currentFingerprint !== previousFingerprint) {
        await sleep(config.auction.pageSettleMs);
        return currentWindow;
      }
    }

    await sleep(150);
  }

  return null;
}

function clickWindow(bot, slot) {
  return new Promise((resolve, reject) => {
    bot.clickWindow(slot, 0, 0, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function parseAuctionPage(window, scanPageIndex, config) {
  const title = cleanText(window?.title);
  const topLimit = getTopInventorySlotLimit(window);
  const listings = [];

  for (let slot = 0; slot < topLimit; slot += 1) {
    const item = window.slots[slot];
    if (!item) {
      continue;
    }

    const listing = parseListing(item, slot, scanPageIndex, title);
    if (!listing || isLikelyControlItem(slot, listing, config)) {
      continue;
    }

    listings.push(listing);
  }

  return {
    scanPageIndex,
    pageNumber: parsePageNumber(title, scanPageIndex),
    title,
    listings,
    fingerprint: fingerprintWindow(window, config, scanPageIndex),
  };
}

function attachRuntimeLogging(bot, config, log, runtimeState) {
  bot.on("error", (error) => {
    log.error(`Bot error: ${error.message}`);
    runtimeState?.setLastError(error.message);
    runtimeState?.addEvent("ERROR", `Bot error: ${error.message}`);
  });

  bot.on("kicked", (reason) => {
    const formattedReason = formatKickReason(reason);
    log.warn(`Bot kicked: ${formattedReason}`);
    runtimeState?.setLastKickReason(formattedReason);
    runtimeState?.addEvent("WARN", `Bot kicked: ${formattedReason}`);
  });

  bot.on("messagestr", (message) => {
    const trimmed = cleanText(message);
    if (trimmed) {
      if (BOT_FILTER_PATTERN.test(trimmed)) {
        bot._botFilterDetected = true;
        runtimeState?.setBotFilterDetected(true);
      }
      if (SERVER_SWITCH_BOUNCE_PATTERN.test(trimmed)) {
        scheduleServerSwitchRetry(bot, config, log, runtimeState);
      }
      runtimeState?.addChat("server", trimmed);
    }
  });

  bot.on("resourcePack", (...args) => {
    let url = "";
    let token = "";

    if (typeof args[0] === "string" && typeof args[1] !== "string") {
      url = args[0];
      token = String(args[1] ?? "");
    } else {
      token = String(args[0] ?? "");
      url = typeof args[1] === "string" ? args[1] : "";
    }

    runtimeState?.setResourcePack({
      at: new Date().toISOString(),
      url,
      token,
      accepted: false,
    });
    runtimeState?.addEvent("INFO", `Server sent resource pack: ${url || token || "unknown"}`);
  });

  bot._client.on("start_configuration", () => {
    setTimeout(
      () => resendClientSettings(bot, log, runtimeState, "configuration"),
      50,
    );
  });
}

async function maybeSendServerLogin(bot, config, log, runtimeState) {
  const template = config.minecraft.loginCommandTemplate;
  if (
    config.minecraft.auth !== "offline" ||
    !config.minecraft.password ||
    template === undefined ||
    template === null ||
    template === ""
  ) {
    return;
  }

  await sleep(config.minecraft.postLoginDelayMs);
  const command = template.replaceAll("{password}", config.minecraft.password);
  bot.chat(command);
  runtimeState?.addChat("bot", command.replaceAll(config.minecraft.password, "******"));
  runtimeState?.addEvent("INFO", "Sent post-login server command.");
  log.info("Sent post-login server command.");
  await sleep(config.minecraft.postLoginSettleMs);
}

function attachPostSwitchLogin(bot, config, log, runtimeState) {
  let initialReady = false;
  let reconfigurePending = false;
  let loginInFlight = false;

  bot.once("spawn", () => {
    initialReady = true;
  });

  bot._client.on("start_configuration", () => {
    if (initialReady) {
      reconfigurePending = true;
    }
  });

  bot._client.on("finish_configuration", () => {
    if (!initialReady || !reconfigurePending || loginInFlight) {
      return;
    }

    reconfigurePending = false;
    loginInFlight = true;
    schedulePostSwitchClientRefresh(bot, config, log, runtimeState);

    setTimeout(async () => {
      try {
        await maybeSendServerLogin(bot, config, log, runtimeState);
      } catch (error) {
        runtimeState?.addEvent(
          "WARN",
          `Post-switch login command failed: ${error.message}`,
        );
        log.warn(`Post-switch login command failed: ${error.message}`);
      } finally {
        loginInFlight = false;
      }
    }, 500);
  });
}

async function resolveAuctionWindow(bot, config, options = {}) {
  const shouldSendCommand = options.sendCommand !== false;
  const command = options.command || config.auction.command;

  const windowPromise = waitForWindowOpen(bot, config.auction.openTimeoutMs);
  if (shouldSendCommand) {
    bot.chat(command);
  }

  const openedWindow = await windowPromise;
  await sleep(config.auction.openDelayMs);
  return bot.currentWindow || openedWindow;
}

async function scanAuctionHouse(bot, config, log, options = {}) {
  if (bot.currentWindow) {
    bot.closeWindow(bot.currentWindow);
    await sleep(300);
  }

  let window = await resolveAuctionWindow(bot, config, options);
  const pages = [];
  const listings = [];
  const seenFingerprints = new Set();

  for (
    let scanPageIndex = 1;
    scanPageIndex <= config.auction.maxPages;
    scanPageIndex += 1
  ) {
    const currentWindow = bot.currentWindow || window;
    if (!currentWindow) {
      throw new Error("Auction window closed during scan.");
    }

    const parsedPage = parseAuctionPage(currentWindow, scanPageIndex, config);
    if (seenFingerprints.has(parsedPage.fingerprint)) {
      log.warn("Repeated page fingerprint detected, stopping scan.");
      break;
    }

    seenFingerprints.add(parsedPage.fingerprint);
    pages.push({
      scanPageIndex: parsedPage.scanPageIndex,
      pageNumber: parsedPage.pageNumber,
      title: parsedPage.title,
      listingCount: parsedPage.listings.length,
    });
    listings.push(...parsedPage.listings);

    log.info(
      `Page ${parsedPage.scanPageIndex}: ${parsedPage.listings.length} lots, total ${listings.length}.`,
    );

    const nextControlItem = currentWindow.slots[config.auction.nextPageSlot];
    if (!nextControlItem) {
      log.info("Next page button is missing, scan finished.");
      break;
    }

    const previousFingerprint = parsedPage.fingerprint;
    await clickWindow(bot, config.auction.nextPageSlot);
    const nextWindow = await waitForPageChange(bot, previousFingerprint, config);

    if (!nextWindow) {
      log.info("Next page did not load, scan finished.");
      break;
    }

    window = nextWindow;
  }

  if (bot.currentWindow) {
    bot.closeWindow(bot.currentWindow);
  }

  return {
    scannedAt: new Date().toISOString(),
    pages,
    listings,
  };
}

async function connectAuctionBot(config, log, runtimeState) {
  let bot;

  try {
    bot = mineflayer.createBot({
      host: config.minecraft.host,
      port: config.minecraft.port,
      username: config.minecraft.username,
      password: config.minecraft.password,
      auth: config.minecraft.auth,
      version: config.minecraft.version,
      brand: config.minecraft.brand,
      profilesFolder: config.minecraft.profilesFolder,
      checkTimeoutInterval: config.minecraft.checkTimeoutMs,
      logErrors: false,
    });
  } catch (error) {
    const versionPart = config.minecraft.version
      ? ` with MC_VERSION=${config.minecraft.version}`
      : "";
    throw new Error(
      `Failed to initialize Mineflayer${versionPart}: ${error.message}`,
    );
  }

  attachRuntimeLogging(bot, config, log, runtimeState);
  attachPacketTracing(bot, config, runtimeState);
  attachPostSwitchLogin(bot, config, log, runtimeState);
  runtimeState?.setConnectionStatus("connecting");
  runtimeState?.addEvent("INFO", "Connecting to the Minecraft server.");

  if (config.resourcePack.autoAccept) {
    bot.on("resourcePack", (...args) => {
      let url = "";
      let token = "";
      let resourcePackId;

      if (typeof args[0] === "string" && typeof args[1] !== "string") {
        url = args[0];
        token = String(args[1] ?? "");
        resourcePackId = args[1];
      } else {
        token = String(args[0] ?? "");
        url = typeof args[1] === "string" ? args[1] : "";
        resourcePackId = args[0];
      }

      try {
        sendResourcePackAcceptedAck(bot, resourcePackId);
        handleAcceptedResourcePack(
          bot,
          resourcePackId,
          url,
          config,
          log,
          runtimeState,
        );
        runtimeState?.setResourcePack({
          at: new Date().toISOString(),
          url,
          token,
          accepted: true,
        });
        runtimeState?.addEvent("INFO", "Accepted server resource pack.");
        log.info("Accepted server resource pack.");
      } catch (error) {
        runtimeState?.setResourcePack({
          at: new Date().toISOString(),
          url,
          token,
          accepted: false,
          error: error.message,
        });
        runtimeState?.addEvent("WARN", `Failed to accept resource pack: ${error.message}`);
        log.warn(`Failed to accept resource pack: ${error.message}`);
      }
    });
  }

  try {
    await waitForSpawn(bot, config.minecraft.loginTimeoutMs);
    runtimeState?.setConnectionStatus("connected");
    runtimeState?.addEvent("INFO", "Spawn completed.");
    await maybeSendServerLogin(bot, config, log, runtimeState);
    log.info("Bot entered the server and is ready for console commands.");
    runtimeState?.addEvent("INFO", "Bot is ready for console commands.");
    return bot;
  } catch (error) {
    try {
      bot?.end?.("connect failed");
    } catch {
      // no-op
    }
    throw error;
  }
}

module.exports = {
  connectAuctionBot,
  rememberServerSwitchCommand,
  scanAuctionHouse,
};
