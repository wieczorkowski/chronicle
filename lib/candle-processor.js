// Candle processing functions for Chronicle

const { parseTimeframeMs } = require('./utils');

// Cache for session boundaries to avoid expensive timezone calculations
const sessionBoundaryCache = new Map();

// Helper function to get the 18:00 ET session start for a given UTC date
function getSessionStartUTC(utcTimestamp) {
    // Use day-level cache key to avoid recalculating for same day
    const dayKey = Math.floor(utcTimestamp / (24 * 60 * 60 * 1000));
    
    if (sessionBoundaryCache.has(dayKey)) {
        return sessionBoundaryCache.get(dayKey);
    }
    
    // Get the date in Eastern time
    const easternDate = new Date(utcTimestamp);
    const easternTimeString = easternDate.toLocaleDateString("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    });
    
    const [month, day, year] = easternTimeString.split('/').map(Number);
    
    // Create 18:00 ET for this Eastern date
    // Use a more direct approach with known Eastern offset
    const sessionDate = new Date(`${month}/${day}/${year} 18:00:00`);
    
    // Calculate Eastern timezone offset for this date (handles DST automatically)
    const tempUTC = new Date(year, month - 1, day, 12, 0, 0); // noon local time
    const tempETStr = tempUTC.toLocaleString("en-US", {timeZone: "America/New_York"});
    const tempETDate = new Date(tempETStr);
    const offsetMs = tempUTC.getTime() - tempETDate.getTime();
    
    const sessionStartUTC = sessionDate.getTime() + offsetMs;
    
    // Cache this result
    sessionBoundaryCache.set(dayKey, sessionStartUTC);
    
    return sessionStartUTC;
}

// Optimized session-based alignment with caching
function getSessionBasedAlignment(timestamp, intervalMs) {
    // Get the session start (18:00 ET) for this timestamp's date
    const sessionStartToday = getSessionStartUTC(timestamp);
    
    // Determine which session this timestamp belongs to
    let baseSessionStart;
    if (timestamp < sessionStartToday) {
        // Before today's 18:00 ET, use yesterday's session
        baseSessionStart = sessionStartToday - (24 * 60 * 60 * 1000);
    } else {
        // After today's 18:00 ET, use today's session
        baseSessionStart = sessionStartToday;
    }
    
    // Calculate how many intervals have passed since the base session start
    const intervalsSinceBase = Math.floor((timestamp - baseSessionStart) / intervalMs);
    const alignedTime = baseSessionStart + (intervalsSinceBase * intervalMs);
    
    return alignedTime;
}

// Determine if timeframe needs session-based alignment
function needsSessionAlignment(timeframe) {
    const intervalMs = parseTimeframeMs(timeframe);
    const oneHourMs = 60 * 60 * 1000;
    const oneDayMs = 24 * 60 * 60 * 1000;
    return intervalMs > oneHourMs && intervalMs <= oneDayMs;
}

// Aggregate candles from 1m data to the requested timeframe with hybrid "isClosed" logic
function aggregateCandles(instrument, timeframe, start, end, candles1m) {
    console.log(`Starting aggregation for ${instrument} to ${timeframe}`);
    const aggStartTime = Date.now();

    if (timeframe === '1m') {
        const result = candles1m.filter(c => c.timestamp >= start && c.timestamp <= end);
        const aggEndTime = Date.now();
        console.log(`Aggregated ${result.length} candles for ${instrument} to ${timeframe} in ${aggEndTime - aggStartTime} ms`);
        return result;
    }

    const intervalMs = parseTimeframeMs(timeframe);
    const useSessionAlignment = needsSessionAlignment(timeframe);
    const maxTs = Math.max(...candles1m.map(c => c.timestamp));  // Latest 1m candle timestamp
    const result = [];
    let currentCandle = null;
    let currentCandleTimestamps = [];  // Track 1m timestamps within the candle

    console.log(`Using ${useSessionAlignment ? 'session-based' : 'UTC-based'} alignment for ${timeframe}`);

    candles1m.sort((a, b) => a.timestamp - b.timestamp);
    for (const candle1m of candles1m) {
        let baseTime;
        if (useSessionAlignment) {
            baseTime = getSessionBasedAlignment(candle1m.timestamp, intervalMs);
        } else {
            baseTime = Math.floor(candle1m.timestamp / intervalMs) * intervalMs;
        }
        
        if (!currentCandle || baseTime !== currentCandle.timestamp) {
            if (currentCandle) {
                const lastPossibleTs = currentCandle.timestamp + intervalMs - 60000;  // Last 1m slot (e.g., 20:59 for 20:5520:59)
                const hasLastPossible = currentCandleTimestamps.includes(lastPossibleTs);
                const hasLaterData = maxTs >= currentCandle.timestamp + intervalMs;
                currentCandle.isClosed = hasLaterData || hasLastPossible;  // Closed if either condition is true
                result.push(currentCandle);
            }
            currentCandle = {
                timestamp: baseTime,
                open: candle1m.open,
                high: candle1m.high,
                low: candle1m.low,
                close: candle1m.close,
                volume: candle1m.volume,
                instrument,
                timeframe,
                source: 'A'
            };
            currentCandleTimestamps = [candle1m.timestamp];
        } else {
            currentCandle.high = Math.max(currentCandle.high, candle1m.high);
            currentCandle.low = Math.min(currentCandle.low, candle1m.low);
            currentCandle.close = candle1m.close;
            currentCandle.volume += candle1m.volume;
            currentCandleTimestamps.push(candle1m.timestamp);
        }
    }
    if (currentCandle) {
        const lastPossibleTs = currentCandle.timestamp + intervalMs - 60000;
        const hasLastPossible = currentCandleTimestamps.includes(lastPossibleTs);
        const hasLaterData = maxTs >= currentCandle.timestamp + intervalMs;
        currentCandle.isClosed = hasLaterData || hasLastPossible;  // Apply the same logic to the last candle
        result.push(currentCandle);
    }

    const aggEndTime = Date.now();
    console.log(`Aggregated ${result.length} candles for ${instrument} to ${timeframe} in ${aggEndTime - aggStartTime} ms`);
    return result.filter(c => c.timestamp >= start && c.timestamp <= end);
}

module.exports = {
    aggregateCandles,
    getSessionBasedAlignment,
    needsSessionAlignment
}; 