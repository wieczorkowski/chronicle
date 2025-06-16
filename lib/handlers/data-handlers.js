// Data handlers for Chronicle

const fs = require('fs');
const path = require('path');
const { DEFAULT_DATA_RANGE_MINUTES, DEFAULT_TIMEZONE, EARLY_CANDLE_CUSHION_MINUTES, LATE_CANDLE_CUSHION_MINUTES } = require('../utils');
const { fetchHistorical1mCandles, fetchLive1mCandles, startLiveTradesSubscription } = require('../data-fetcher');
const { getCached1mCandles, batchInsertCandles } = require('../db');
const { aggregateCandles } = require('../candle-processor');

// Handle 'get_data' action
async function handleGetData(db, ws, request, wss) {
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Received get_data request: ${JSON.stringify(request)}` }));
    let { subscriptions, start_time, end_time, live_data = 'none', sendto = 'console', use_cache = true, save_cache = true, timezone = DEFAULT_TIMEZONE } = request;

    // Convert live_data to number if it's not "none" or "all"
    if (typeof live_data === 'string' && live_data !== 'none' && live_data !== 'all') {
        live_data = Number(live_data);
        if (isNaN(live_data)) {
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid live_data value: must be "none", "all", or a number' }));
            return;
        }
    }

    // Validate subscriptions
    if (!Array.isArray(subscriptions) || !subscriptions.every(sub => sub.instrument && sub.timeframe)) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid subscriptions format' }));
        return;
    }

    // Set up active subscriptions
    ws.activeSubscriptions = {};
    subscriptions.forEach(sub => {
        const { instrument, timeframe } = sub;
        if (!ws.activeSubscriptions[instrument]) {
            ws.activeSubscriptions[instrument] = [];
        }
        if (!ws.activeSubscriptions[instrument].includes(timeframe)) {
            ws.activeSubscriptions[instrument].push(timeframe);
        }
    });

    // Set defaults for time range
    const now = Date.now();
    const startMs = start_time === undefined ? now - DEFAULT_DATA_RANGE_MINUTES * 60 * 1000 : new Date(start_time).getTime();
    const endMs = end_time === undefined || end_time === 'current' ? now : new Date(end_time).getTime();
    console.log(`  DEBUG - Start Time = ${start_time}, startMs = ${startMs} | End Time = ${end_time}, endMs = ${endMs} `);

    // Store parameters in ws for use in add_timeframe and remove_timeframe
    ws.startMs = startMs;
    ws.endMs = endMs;
    ws.sendto = sendto;

    // Load DATAFEED settings
    const settings = await require('../db').getSettings(db, 'DATAFEED');
    if (!settings) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'DATAFEED settings not configured' }));
        return;
    }

    // Initialize log file stream if needed and attach to ws
    if (sendto === 'log') {
        const logDir = './logs';
        const logFile = path.join(logDir, `candles-${new Date().toISOString().replace(/T/, '-').replace(/:/g, '-').slice(0, 19)}.log`);
        ws.logFileStream = fs.createWriteStream(logFile, { flags: 'a' });
    }

    // Output function with timezone adjustment, stored in ws
    ws.outputFunc = (candle) => {
        let outputCandle = { mtyp: 'data', ...candle };
        const date = new Date(candle.timestamp);
        const options = {
            timeZone: timezone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        };
        const formatter = new Intl.DateTimeFormat('en-US', options);
        const parts = formatter.formatToParts(date);
        const formattedDate = `${parts.find(p => p.type === 'year').value}-${parts.find(p => p.type === 'month').value}-${parts.find(p => p.type === 'day').value} ${parts.find(p => p.type === 'hour').value}:${parts.find(p => p.type === 'minute').value}:${parts.find(p => p.type === 'second').value}`;
        outputCandle.dateTime = formattedDate;
        const message = JSON.stringify(outputCandle);
        if (ws.sendto === 'console') {
            console.log(message);
        } else if (ws.sendto === 'log') {
            if (ws.logFileStream) {
                ws.logFileStream.write(message + '\n');
            }
        } else {
            ws.send(message);
        }
    };

    // Group subscriptions by symbol internally
    const symbolSubscriptions = ws.activeSubscriptions;

    // Track the next1mStart for each symbol to determine the earliest start time for live trades
    const next1mStarts = {};

    // Process each symbol
    for (const symbol in symbolSubscriptions) {
        const timeframes = symbolSubscriptions[symbol];
        let candles1m = [];

        if (use_cache) {
            candles1m = await getCached1mCandles(db, symbol, startMs, endMs);
        }

        if (candles1m.length === 0) {
            // No cached data, fetch the entire range
            try {
                candles1m = await fetchHistorical1mCandles(settings, symbol, startMs, endMs);
                if (save_cache && candles1m.length > 0) {
                    await batchInsertCandles(db, candles1m, symbol, '1m');
                }
            } catch (err) {
                console.error(`Failed to fetch historical data for ${symbol}:`, err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: `Failed to fetch historical data for ${symbol}` }));
                continue;
            }
        } else {
            // Cached data exists, fill in gaps
            const earliestCached = Math.min(...candles1m.map(c => c.timestamp));
            const latestCachedForSymbol = Math.max(...candles1m.map(c => c.timestamp));

            // Fill in earlier data if needed, but check cushion first
            if (startMs < earliestCached) {
                const cushionMs = EARLY_CANDLE_CUSHION_MINUTES * 60 * 1000;
                if (earliestCached - startMs <= cushionMs) {
                    console.log(`Bypassing early historical fetch for ${symbol}: earliest cached candle within cushion of ${EARLY_CANDLE_CUSHION_MINUTES} minutes.`);
                } else {
                    try {
                        const diagCacheBeginRange = earliestCached - 60000;
                        console.log(`Checking Historical Early Range - ${startMs} to ${diagCacheBeginRange}...`);
                        const earlierCandles = await fetchHistorical1mCandles(settings, symbol, startMs, earliestCached - 60000); // 1 minute before to avoid overlap
                        if (earlierCandles.length > 0) {
                            candles1m = [...earlierCandles, ...candles1m];
                            if (save_cache) {
                                await batchInsertCandles(db, earlierCandles, symbol, '1m');
                            }
                        }
                    } catch (err) {
                        console.error(`Failed to fetch earlier historical data for ${symbol}:`, err.message);
                        // Continue with existing data, no error sent to client
                    }
                }
            }

            // Fill in later data if needed, but check cushion first
            if (endMs > latestCachedForSymbol) {
                if (end_time === undefined || end_time === 'current') {
                    const cushionMs = LATE_CANDLE_CUSHION_MINUTES * 60 * 1000;
                    if (endMs - latestCachedForSymbol <= cushionMs) {
                        console.log(`Bypassing late historical fetch for ${symbol}: latest cached candle within cushion of ${LATE_CANDLE_CUSHION_MINUTES} minutes.`);
                    } else {
                        try {
                            const diagCacheEndRange = latestCachedForSymbol + 60000;
                            console.log(`Checking Historical Late Range - ${latestCachedForSymbol + 60000} to ${endMs}...`);
                            const laterCandles = await fetchHistorical1mCandles(settings, symbol, latestCachedForSymbol + 60000, endMs); // 1 minute after to avoid overlap
                            if (laterCandles.length > 0) {
                                candles1m = [...candles1m, ...laterCandles];
                                if (save_cache) {
                                    await batchInsertCandles(db, laterCandles, symbol, '1m');
                                }
                            }
                        } catch (err) {
                            console.error(`Failed to fetch later historical data for ${symbol}:`, err.message);
                            // Continue with existing data, no error sent to client
                        }
                    }
                } else {
                    // If end_time is specified, always try to fetch
                    try {
                        const diagCacheEndRange = latestCachedForSymbol + 60000;
                        console.log(`Checking Historical Late Range - ${latestCachedForSymbol + 60000} to ${endMs}...`);
                        const laterCandles = await fetchHistorical1mCandles(settings, symbol, latestCachedForSymbol + 60000, endMs); // 1 minute after to avoid overlap
                        if (laterCandles.length > 0) {
                            candles1m = [...candles1m, ...laterCandles];
                            if (save_cache) {
                                await batchInsertCandles(db, laterCandles, symbol, '1m');
                            }
                        }
                    } catch (err) {
                        console.error(`Failed to fetch later historical data for ${symbol}:`, err.message);
                        // Continue with existing data, no error sent to client
                    }
                }
            }

            // Sort the combined candles
            candles1m.sort((a, b) => a.timestamp - b.timestamp);
        }

        // Check if live data is needed for initial dataset
        const latestCachedForSymbol = candles1m.length > 0 ? Math.max(...candles1m.map(c => c.timestamp)) : startMs;
        if (end_time === 'current' || endMs > latestCachedForSymbol) {
            console.log(`Live data required for ${symbol} from ${new Date(latestCachedForSymbol + 60000).toISOString()} to ${end_time === 'current' ? 'current' : new Date(endMs).toISOString()}`);
            try {
                const liveCandles = await fetchLive1mCandles(settings, [symbol], latestCachedForSymbol + 60000, endMs);
                if (liveCandles.length > 0) {
                    candles1m = [...candles1m, ...liveCandles];
                    if (save_cache) {
                        await batchInsertCandles(db, liveCandles, symbol, '1m');
                        console.log(`Added ${liveCandles.length} live OHLCV-1m candles to CANDLES cache for ${symbol}`);
                    }
                }
            } catch (err) {
                console.error(`Failed to fetch live data for ${symbol}:`, err.message);
                // Continue with existing data, no error sent to client
            }
            // Sort again after adding live data
            candles1m.sort((a, b) => a.timestamp - b.timestamp);
        }

        // Serve each requested timeframe
        for (const timeframe of timeframes) {
            let candlesToServe;
            if (timeframe === '1m') {
                candlesToServe = candles1m.filter(c => c.timestamp >= startMs && c.timestamp <= endMs);
            } else {
                candlesToServe = aggregateCandles(symbol, timeframe, startMs, endMs, candles1m);
            }
            console.log(`Starting to send ${candlesToServe.length} ${timeframe} candles for ${symbol} to ${sendto}`);
            candlesToServe.forEach(ws.outputFunc);
        }

        // Initialize open candles for live data if needed
        if (live_data !== 'none') {
            if (!ws.open1mCandles) ws.open1mCandles = new Map();
            if (!ws.openCandles) ws.openCandles = new Map();

            const candles1mForSymbol = candles1m.filter(c => c.instrument === symbol);
            const last1mCandle = candles1mForSymbol[candles1mForSymbol.length - 1];
            const next1mStart = last1mCandle ? last1mCandle.timestamp + 60000 : Math.floor(now / 60000) * 60000;
            console.log(`DEBUG: next 1m candle time for ${symbol}: ${new Date(next1mStart).toISOString()}`);  // Log 1: Expected start time
            ws.open1mCandles.set(symbol, {
                timestamp: next1mStart,
                open: null,
                high: null,
                low: null,
                close: null,
                volume: 0,
                instrument: symbol,
                timeframe: '1m',
                source: 'T',
                isClosed: false,
                firstUpdate: true  // Flag to log when candle starts processing
            });

            const openCandlesForSymbol = new Map();
            for (const timeframe of timeframes) {
                const candlesToServe = aggregateCandles(symbol, timeframe, startMs, endMs, candles1mForSymbol);
                const lastCandle = candlesToServe[candlesToServe.length - 1];
                let openCandle;
                if (lastCandle && !lastCandle.isClosed) {
                    openCandle = { ...lastCandle, source: 'T' };
                } else {
                    const intervalMs = require('../utils').parseTimeframeMs(timeframe);
                    const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
                    let currentStart;
                    if (needsSessionAlignment(timeframe)) {
                        currentStart = getSessionBasedAlignment(next1mStart, intervalMs);
                    } else {
                        currentStart = Math.floor(next1mStart / intervalMs) * intervalMs;
                    }
                    openCandle = {
                        timestamp: currentStart,
                        open: null,
                        high: null,
                        low: null,
                        close: null,
                        volume: 0,
                        instrument: symbol,
                        timeframe,
                        source: 'T',
                        isClosed: false
                    };
                }
                openCandlesForSymbol.set(timeframe, openCandle);
            }
            ws.openCandles.set(symbol, openCandlesForSymbol);

            // Track the next1mStart for this symbol
            next1mStarts[symbol] = next1mStart;
        }
    }

    // Start live trades subscription if live_data is not "none"
    if (live_data !== 'none') {
        ws.isLiveActive = true;
        ws.tradeQueue = ws.tradeQueue || []; // Initialize trade queue if it doesn't exist
        const instruments = Object.keys(symbolSubscriptions);
        // Find the earliest next1mStart across all symbols
        const earliestNext1mStart = Math.min(...Object.values(next1mStarts));
        const startTs = earliestNext1mStart * 1e6; // Convert to nanoseconds for Databento API
        console.log(`Live trades subscription will start from: ${new Date(earliestNext1mStart).toLocaleString()}`);

        // Define processTradeForClient with diagnostics and live updates
        const processTradeForClient = (trade) => {
            if (!ws.isLiveActive) {
                return; // Skip processing if live data is not active
            }
            
            if (ws.isProcessingTimeframeChange) {
                ws.tradeQueue.push(trade);
                return;
            }
            console.log(`Trade for ${trade.instrument} at ${new Date(trade.timestamp).toISOString()} (Client ID: ${ws.clientID || 'unknown'})`);
            const instrument = trade.instrument;
            if (!ws.activeSubscriptions[instrument]) return;

            // Only process trades that are on or after the instrument's next1mStart
            const next1mStart = next1mStarts[instrument];
            if (trade.timestamp < next1mStart) {
                return; // Trade is before the start of the next candle, ignore
            }

            // Update 1m open candle (always tracked for caching)
            const open1mCandle = ws.open1mCandles.get(instrument);
            if (trade.timestamp >= open1mCandle.timestamp + 60000) {
                // Close current 1m candle
                open1mCandle.isClosed = true;
                // Send the closed 1m candle if subscribed
                if (ws.activeSubscriptions[instrument].includes('1m')) {
                    ws.outputFunc(open1mCandle); // Push final closed candle
                }
                // Cache it (only if it's not a "null" candle)
                batchInsertCandles(db, [open1mCandle], instrument, '1m').catch(err => console.error('Error caching 1m candle:', err));
                // Start new open 1m candle
                const newStart = Math.floor(trade.timestamp / 60000) * 60000;
                ws.open1mCandles.set(instrument, {
                    timestamp: newStart,
                    open: trade.price,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    volume: trade.size,
                    instrument,
                    timeframe: '1m',
                    source: 'T',
                    isClosed: false,
                    firstUpdate: true  // Reset flag for new candle
                });
                // Send the new 1m candle if subscribed
                if (ws.activeSubscriptions[instrument].includes('1m')) {
                    ws.outputFunc(ws.open1mCandles.get(instrument));
                }
            } else {
                // Update current open 1m candle
                if (open1mCandle.firstUpdate) {
                    console.log(`DEBUG: next 1m time reached for instrument ${instrument}, candle started`);  // Log 2: Candle processing begins
                    open1mCandle.firstUpdate = false;
                }
                if (open1mCandle.open === null) {
                    open1mCandle.open = trade.price;
                }
                open1mCandle.high = Math.max(open1mCandle.high || trade.price, trade.price);
                open1mCandle.low = Math.min(open1mCandle.low || trade.price, trade.price);
                open1mCandle.close = trade.price;
                open1mCandle.volume += trade.size;
                // Send the updated 1m candle if subscribed
                if (ws.activeSubscriptions[instrument].includes('1m')) {
                    ws.outputFunc(open1mCandle);
                }
            }

            // Update open candles for requested timeframes (excluding 1m)
            const openCandlesForSymbol = ws.openCandles.get(instrument);
            if (openCandlesForSymbol) {
                for (const [timeframe, openCandle] of openCandlesForSymbol) {
                    if (timeframe === '1m') continue; // Skip 1m to avoid duplication
                    const intervalMs = require('../utils').parseTimeframeMs(timeframe);
                    if (trade.timestamp >= openCandle.timestamp + intervalMs) {
                        // Close current open candle
                        openCandle.isClosed = true;
                        ws.outputFunc(openCandle); // Send final version to client
                        // Start new open candle
                        const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
                        let newStart;
                        if (needsSessionAlignment(timeframe)) {
                            newStart = getSessionBasedAlignment(trade.timestamp, intervalMs);
                        } else {
                            newStart = Math.floor(trade.timestamp / intervalMs) * intervalMs;
                        }
                        openCandlesForSymbol.set(timeframe, {
                            timestamp: newStart,
                            open: trade.price,
                            high: trade.price,
                            low: trade.price,
                            close: trade.price,
                            volume: trade.size,
                            instrument,
                            timeframe,
                            source: 'T',
                            isClosed: false
                        });
                        ws.outputFunc(openCandlesForSymbol.get(timeframe)); // Send new open candle
                        console.log(`Updated open ${timeframe} candle for ${instrument}: ${JSON.stringify(openCandlesForSymbol.get(timeframe))}`);
                    } else {
                        // Update current open candle
                        if (openCandle.open === null) {
                            openCandle.open = trade.price;
                        }
                        openCandle.high = Math.max(openCandle.high || trade.price, trade.price);
                        openCandle.low = Math.min(openCandle.low || trade.price, trade.price);
                        openCandle.close = trade.price;
                        openCandle.volume += trade.size;
                        ws.outputFunc(openCandle); // Send updated candle to client
                    }
                }
            }
        };

        // Create a callback to update the ws.liveClient reference if a connection retry occurs
        const updateLiveClientRef = (newClient) => {
            console.log(`Updating live client reference${ws.clientID ? ` for client ID: ${ws.clientID}` : ''}`);
            ws.liveClient = newClient;
        };

        // Start live trades subscription with the updateLiveClientRef callback
        console.log(`Live trades subscription started for ${live_data} seconds or until client disconnects`);
        ws.liveClient = await startLiveTradesSubscription(settings, instruments, startTs, processTradeForClient, 0, updateLiveClientRef);

        if (typeof live_data === 'number') {
            setTimeout(() => {
                if (ws.liveClient) {
                    ws.liveClient.destroy();
                    ws.liveClient = null;
                    ws.isLiveActive = false;
                    console.log(`Live trades subscription stopped after ${live_data} seconds`);
                    if (ws.sendto === 'log' && ws.logFileStream) {
                        ws.logFileStream.end();
                        ws.logFileStream = null;
                    }
                }
            }, live_data * 1000);
        }
    }
}

// Handle 'add_timeframe' action
async function handleAddTimeframe(db, ws, request) {
    if (!ws.isLiveActive) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Cannot add timeframe: no active live data subscription' }));
        return;
    }
    const { instrument, timeframe } = request;
    if (!instrument || !timeframe) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing instrument or timeframe' }));
        return;
    }
    if (!ws.activeSubscriptions[instrument]) {
        ws.send(JSON.stringify({ mtyp: 'error', message: `Instrument ${instrument} not subscribed` }));
        return;
    }
    if (ws.activeSubscriptions[instrument].includes(timeframe)) {
        ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Timeframe ${timeframe} for ${instrument} already being sent` }));
        return;
    }

    // Add to active subscriptions
    ws.activeSubscriptions[instrument].push(timeframe);

    // Use original startMs and current time as endMs for consistency with get_data
    const startMs = ws.startMs;
    const endMs = Date.now();

    // Load DATAFEED settings
    const settings = await require('../db').getSettings(db, 'DATAFEED');

    let candles1m = await getCached1mCandles(db, instrument, startMs, endMs);
    if (candles1m.length === 0) {
        try {
            candles1m = await fetchHistorical1mCandles(settings, instrument, startMs, endMs);
            await batchInsertCandles(db, candles1m, instrument, '1m');
        } catch (err) {
            console.error(`Failed to fetch historical data for ${instrument}:`, err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: `Failed to fetch historical data for ${instrument}` }));
            return;
        }
    } else {
        const earliestCached = Math.min(...candles1m.map(c => c.timestamp));
        const latestCached = Math.max(...candles1m.map(c => c.timestamp));
        if (startMs < earliestCached) {
            try {
                const earlierCandles = await fetchHistorical1mCandles(settings, instrument, startMs, earliestCached - 60000);
                if (earlierCandles.length > 0) {
                    candles1m = [...earlierCandles, ...candles1m];
                    await batchInsertCandles(db, earlierCandles, instrument, '1m');
                }
            } catch (err) {
                console.error(`Failed to fetch earlier historical data for ${instrument}:`, err.message);
            }
        }
        if (endMs > latestCached) {
            // Set flag before fetching live candles
            ws.isProcessingTimeframeChange = true;
            
            // Create a callback to update the ws.liveClient reference if a connection retry occurs
            const updateLiveClientRef = (newClient) => {
                console.log(`Updating live client reference for add_timeframe${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
                // Only update if this is a live data client situation
                if (ws.liveClient) {
                    ws.liveClient = newClient;
                }
            };
            
            try {
                const laterCandles = await fetchLive1mCandles(settings, [instrument], latestCached + 60000, endMs, 0, updateLiveClientRef);
                if (laterCandles.length > 0) {
                    candles1m = [...candles1m, ...laterCandles];
                    await batchInsertCandles(db, laterCandles, instrument, '1m');
                }
            } catch (err) {
                console.error(`Failed to fetch live data for ${instrument}:`, err.message);
            }
        }
        candles1m.sort((a, b) => a.timestamp - b.timestamp);
    }

    // Aggregate and send historical candles using the stored outputFunc
    const aggregatedCandles = aggregateCandles(instrument, timeframe, startMs, endMs, candles1m);
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Starting streaming ${timeframe} for ${instrument}` }));
    aggregatedCandles.forEach(ws.outputFunc);

    // Initialize live candle with inclusion of open 1m candle
    const openCandlesForSymbol = ws.openCandles.get(instrument) || new Map();
    const lastAggregatedCandle = aggregatedCandles[aggregatedCandles.length - 1];
    const intervalMs = require('../utils').parseTimeframeMs(timeframe);
    const currentTime = Date.now();
    let liveCandle;

    if (lastAggregatedCandle && !lastAggregatedCandle.isClosed && currentTime < lastAggregatedCandle.timestamp + intervalMs) {
        // Continue with the last open aggregated candle
        liveCandle = { ...lastAggregatedCandle, source: 'T' };
    } else {
        // Start a new live candle at the CURRENT appropriate interval
        const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
        let currentStart;
        if (needsSessionAlignment(timeframe)) {
            currentStart = getSessionBasedAlignment(currentTime, intervalMs);
        } else {
            currentStart = Math.floor(currentTime / intervalMs) * intervalMs;
        }
        liveCandle = {
            timestamp: currentStart,
            open: null,
            high: null,
            low: null,
            close: null,
            volume: 0,
            instrument,
            timeframe,
            source: 'T',
            isClosed: false
        };
    }

    // Include open 1m candle if it falls within this interval
    const open1mCandle = ws.open1mCandles.get(instrument);
    if (open1mCandle && open1mCandle.timestamp >= liveCandle.timestamp && open1mCandle.timestamp < liveCandle.timestamp + intervalMs) {
        if (liveCandle.open === null && open1mCandle.open !== null) {
            liveCandle.open = open1mCandle.open;
        }
        liveCandle.high = Math.max(liveCandle.high || open1mCandle.high || -Infinity, open1mCandle.high || -Infinity);
        liveCandle.low = Math.min(liveCandle.low || open1mCandle.low || Infinity, open1mCandle.low || Infinity);
        liveCandle.close = open1mCandle.close || liveCandle.close;
        liveCandle.volume += open1mCandle.volume || 0;
    }

    openCandlesForSymbol.set(timeframe, liveCandle);
    ws.openCandles.set(instrument, openCandlesForSymbol);

    // Reset the flag to false
    ws.isProcessingTimeframeChange = false;

    // Process any queued trades 
    while (ws.tradeQueue.length > 0) {
        const queuedTrade = ws.tradeQueue.shift();
        processTradeForClient(queuedTrade);
    }
}

// Handle 'remove_timeframe' action
function handleRemoveTimeframe(ws, request) {
    if (!ws.isLiveActive) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Cannot remove timeframe: no active live data subscription' }));
        return;
    }
    const { instrument, timeframe } = request;
    if (!instrument || !timeframe) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing instrument or timeframe' }));
        return;
    }
    if (!ws.activeSubscriptions[instrument]) {
        ws.send(JSON.stringify({ mtyp: 'error', message: `Instrument ${instrument} not subscribed` }));
        return;
    }
    const index = ws.activeSubscriptions[instrument].indexOf(timeframe);
    if (index === -1) {
        ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Timeframe ${timeframe} for ${instrument} not being sent` }));
        return;
    }

    // Remove from active subscriptions
    ws.activeSubscriptions[instrument].splice(index, 1);

    // Free live candle aggregation
    const openCandlesForSymbol = ws.openCandles.get(instrument);
    if (openCandlesForSymbol) {
        openCandlesForSymbol.delete(timeframe);
    }

    // Send control message (via WebSocket, consistent with existing behavior)
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Stopped streaming ${timeframe} for ${instrument}` }));
}

// Handle 'stop_data' action
function handleStopData(ws) {
    if (!ws.isLiveActive) {
        ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'No active data stream to stop' }));
        return;
    }

    console.log('Stopping live data subscription');
    ws.isLiveActive = false;
    
    if (ws.liveClient) {
        ws.liveClient.destroy();
        ws.liveClient = null;
    }
    
    if (ws.sendto === 'log' && ws.logFileStream) {
        ws.logFileStream.end();
        ws.logFileStream = null;
    }
    
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Live data subscription stopped' }));
}

module.exports = {
    handleGetData,
    handleAddTimeframe,
    handleRemoveTimeframe,
    handleStopData
}; 