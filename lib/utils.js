// Utility functions for Chronicle

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// Constants for easy configuration
const DEFAULT_DATA_RANGE_MINUTES = 86400;  // 60 days in minutes
const EARLY_CANDLE_CUSHION_MINUTES = 4320;  // 3 days in minutes
const LATE_CANDLE_CUSHION_MINUTES = 180;    // 3 hours in minutes
const DEFAULT_TIMEZONE = 'America/New_York';  // Eastern Time

// Helper function to parse timeframe strings into milliseconds
function parseTimeframeMs(timeframe) {
    const match = timeframe.match(/^(\d+)([mhd])$/);
    if (!match) {
        throw new Error(`Invalid timeframe format: ${timeframe}. Expected format like '1m', '5m', '1h', '4h', '1d'.`);
    }
    const value = parseInt(match[1]);
    const unit = match[2];
    let multiplier;
    if (unit === 'm') multiplier = 60 * 1000; // minutes to milliseconds
    else if (unit === 'h') multiplier = 60 * 60 * 1000; // hours to milliseconds
    else if (unit === 'd') multiplier = 24 * 60 * 60 * 1000; // days to milliseconds
    return value * multiplier;
}

// Ensure logs directory exists
function ensureLogDirectory(logDir) {
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir);
    }
}

// Export constants and functions
module.exports = {
    DEFAULT_DATA_RANGE_MINUTES,
    EARLY_CANDLE_CUSHION_MINUTES,
    LATE_CANDLE_CUSHION_MINUTES,
    DEFAULT_TIMEZONE,
    parseTimeframeMs,
    ensureLogDirectory
}; 