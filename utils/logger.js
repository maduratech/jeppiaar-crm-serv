// Structured logging utility
// Replaces console.log with structured logging

const LOG_LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
};

const currentLogLevel = process.env.LOG_LEVEL || "INFO";

function shouldLog(level) {
  const levelMap = {
    ERROR: LOG_LEVELS.ERROR,
    WARN: LOG_LEVELS.WARN,
    INFO: LOG_LEVELS.INFO,
    DEBUG: LOG_LEVELS.DEBUG,
  };
  return levelMap[level] <= levelMap[currentLogLevel];
}

function formatLog(level, message, metadata = {}) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    message,
    ...metadata,
  };
  return JSON.stringify(logEntry);
}

export const logger = {
  error: (message, metadata = {}) => {
    if (shouldLog("ERROR")) {
      console.error(formatLog("ERROR", message, metadata));
    }
  },

  warn: (message, metadata = {}) => {
    if (shouldLog("WARN")) {
      console.warn(formatLog("WARN", message, metadata));
    }
  },

  info: (message, metadata = {}) => {
    if (shouldLog("INFO")) {
      console.log(formatLog("INFO", message, metadata));
    }
  },

  debug: (message, metadata = {}) => {
    if (shouldLog("DEBUG")) {
      console.log(formatLog("DEBUG", message, metadata));
    }
  },
};
