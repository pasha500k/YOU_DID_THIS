function createRuntimeState() {
  const state = {
    startedAt: new Date().toISOString(),
    connectionStatus: "starting",
    busy: false,
    lastError: null,
    lastKickReason: null,
    lastScanSummary: null,
    botFilterDetected: false,
    resourcePack: null,
    recentChat: [],
    recentEvents: [],
    recentPackets: [],
  };

  function pushBounded(target, entry, maxSize = 100) {
    target.push(entry);
    if (target.length > maxSize) {
      target.splice(0, target.length - maxSize);
    }
  }

  return {
    setConnectionStatus(status) {
      state.connectionStatus = status;
    },
    setBusy(value) {
      state.busy = Boolean(value);
    },
    setLastError(message) {
      state.lastError = message || null;
    },
    setLastKickReason(message) {
      state.lastKickReason = message || null;
    },
    setLastScanSummary(summary) {
      state.lastScanSummary = summary || null;
    },
    setBotFilterDetected(value) {
      state.botFilterDetected = Boolean(value);
    },
    setResourcePack(resourcePack) {
      state.resourcePack = resourcePack || null;
    },
    addPacket(packet, maxSize = 160) {
      pushBounded(state.recentPackets, {
        at: new Date().toISOString(),
        ...packet,
      }, maxSize);
    },
    addChat(direction, message) {
      pushBounded(state.recentChat, {
        at: new Date().toISOString(),
        direction,
        message,
      });
    },
    addEvent(level, message) {
      pushBounded(state.recentEvents, {
        at: new Date().toISOString(),
        level,
        message,
      });
    },
    snapshot() {
      return {
        ...state,
        recentChat: [...state.recentChat],
        recentEvents: [...state.recentEvents],
        recentPackets: [...state.recentPackets],
      };
    },
  };
}

module.exports = {
  createRuntimeState,
};
