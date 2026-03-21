const { mineflayer: createMineflayerViewer } = require("prismarine-viewer");

function createLiveView(bot, config, log, runtimeState) {
  const options = {
    port: config.liveView.port,
    firstPerson: config.liveView.firstPerson,
    viewDistance: config.liveView.viewDistance,
    prefix: config.liveView.prefix,
  };

  createMineflayerViewer(bot, options);
  runtimeState?.addEvent(
    "INFO",
    `Live viewer started on http://${config.liveView.host}:${config.liveView.port}${config.liveView.prefix || "/"}`,
  );
  log.info(
    `Live viewer is available at http://${config.liveView.host}:${config.liveView.port}${config.liveView.prefix || "/"}`,
  );

  return {
    close() {
      try {
        bot.viewer?.close?.();
      } catch {
        // no-op
      }
    },
  };
}

module.exports = {
  createLiveView,
};
