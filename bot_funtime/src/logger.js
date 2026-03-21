const readline = require("readline");

function createLogger(verbose = true) {
  let activeReadline = null;

  const format = (level, message) =>
    `[${new Date().toISOString()}] [${level}] ${message}`;

  function writeLine(text, fallbackMethod = "log") {
    if (activeReadline?.terminal && activeReadline.output?.writable) {
      try {
        readline.clearLine(activeReadline.output, 0);
        readline.cursorTo(activeReadline.output, 0);
        activeReadline.output.write(`${text}\n`);

        if (typeof activeReadline._refreshLine === "function") {
          activeReadline._refreshLine();
        } else {
          activeReadline.prompt(true);
        }
        return;
      } catch {
        // fall back to the regular console below
      }
    }

    console[fallbackMethod](text);
  }

  return {
    bindReadline(rl) {
      activeReadline = rl || null;
    },
    unbindReadline() {
      activeReadline = null;
    },
    info(message) {
      writeLine(format("INFO", message), "log");
    },
    warn(message) {
      writeLine(format("WARN", message), "warn");
    },
    error(message) {
      writeLine(format("ERROR", message), "error");
    },
    debug(message) {
      if (verbose) {
        writeLine(format("DEBUG", message), "log");
      }
    },
  };
}

module.exports = {
  createLogger,
};
