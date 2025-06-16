// Replay handlers for Chronicle

const fs = require('fs');
const path = require('path');
const { DEFAULT_TIMEZONE, parseTimeframeMs } = require('../utils');
const { getCached1mCandles } = require('../db');
const { fetchHistorical1mCandles } = require('../data-fetcher');

// Handle 'get_replay' action
async function handleGetReplay(db, ws, request) {
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Received get_replay request: ${JSON.stringify(request)}` }));

    const { 
        subscriptions, 
        history_start, 
        live_start,
        live_end = 'none', 
        replay_interval = 1000, 
        sendto = 'console', 
        use_cache = true 
    } = request;

    // Validate subscriptions
    if (!Array.isArray(subscriptions) || !subscriptions.every(sub => sub.instrument && sub.timeframe)) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid subscriptions format' }));
        return;
    }

    // Set up active replay subscriptions
    ws.activeReplaySubscriptions = {};
    subscriptions.forEach(sub => {
        const { instrument, timeframe } = sub;
        if (!ws.activeReplaySubscriptions[instrument]) {
            ws.activeReplaySubscriptions[instrument] = [];
        }
        if (!ws.activeReplaySubscriptions[instrument].includes(timeframe)) {
            ws.activeReplaySubscriptions[instrument].push(timeframe);
        }
    });

    // Determine time ranges
    const now = Date.now();
    let historyStartMs;
    if (typeof history_start === 'number' && history_start < 0) {
        // Interpret as minutes from current time
        historyStartMs = now + history_start * 60 * 1000;
    } else {
        // Interpret as timestamp
        historyStartMs = new Date(history_start).getTime();
    }

    let liveStartMs;
    if (live_start === 'current') {
        liveStartMs = now;
    } else {
        liveStartMs = new Date(live_start).getTime();
    }

    let liveEndMs;
    if (live_end === 'none') {
        liveEndMs = liveStartMs; // No live replay
    } else if (live_end === 'all') {
        liveEndMs = now; // Replay until current time
    } else if (typeof live_end === 'number') {
        // Check if this is a timestamp (large number in milliseconds) or seconds to stream (small number)
        // Typical timestamp milliseconds will be > 1,000,000,000,000 (Year 2001+)
        // Seconds to stream would typically be < 100,000 (over a day)
        if (live_end > 100000000) {  // If it's a large number, treat as timestamp
            liveEndMs = live_end;
        } else {  // If it's a small number, treat as seconds to stream
            liveEndMs = liveStartMs + live_end * 1000;
        }
    } else {
        // Interpret as timestamp string
        liveEndMs = new Date(live_end).getTime();
    }

    console.log(`Replay time ranges: History start = ${new Date(historyStartMs).toISOString()}, Live start = ${new Date(liveStartMs).toISOString()}, Live end = ${new Date(liveEndMs).toISOString()}`);

    // Initialize log file stream if needed and attach to ws
    if (sendto === 'log') {
        const logDir = './logs';
        const logFile = path.join(logDir, `replay-${new Date().toISOString().replace(/T/, '-').replace(/:/g, '-').slice(0, 19)}.log`);
        ws.replayLogFileStream = fs.createWriteStream(logFile, { flags: 'a' });
    }

    // Output function with timezone adjustment, stored in ws
    ws.replayOutputFunc = (candle) => {
        let outputCandle = { mtyp: 'data', ...candle };
        const date = new Date(candle.timestamp);
        const options = {
            timeZone: DEFAULT_TIMEZONE,
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
        if (sendto === 'console') {
            console.log(message);
        } else if (sendto === 'log') {
            if (ws.replayLogFileStream) {
                ws.replayLogFileStream.write(message + '\n');
            }
        } else {
            ws.send(message);
        }
    };

    // Load DATAFEED settings
    const settings = await require('../db').getSettings(db, 'DATAFEED');
    if (!settings) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'DATAFEED settings not configured' }));
        return;
    }

    // Create data structures to manage replay
    ws.replayData = {};
    ws.replayInterval = null;
    ws.replayIsActive = true;
    ws.replayIsPaused = false;
    ws.replayIntervalMs = replay_interval;
    
    // Process each symbol
    for (const symbol in ws.activeReplaySubscriptions) {
        const timeframes = ws.activeReplaySubscriptions[symbol];
        let candles1m = [];

        // First try to get data from cache if use_cache is true
        if (use_cache) {
            candles1m = await getCached1mCandles(db, symbol, historyStartMs, liveEndMs);
            console.log(`Retrieved ${candles1m.length} candles from cache for ${symbol}`);
        }

        // If no cached data or insufficient cached data, fetch from Databento Historical API
        if (candles1m.length === 0) {
            try {
                candles1m = await fetchHistorical1mCandles(settings, symbol, historyStartMs, liveEndMs);
                console.log(`Fetched ${candles1m.length} historical candles for ${symbol} from Databento`);
            } catch (err) {
                console.error(`Failed to fetch historical data for ${symbol}:`, err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: `Failed to fetch historical data for ${symbol}` }));
                continue;
            }
        } else {
            // Cached data exists, check if we need to fill gaps with API calls
            const earliestCached = Math.min(...candles1m.map(c => c.timestamp));
            const latestCached = Math.max(...candles1m.map(c => c.timestamp));

            // Fill in earlier data if needed
            if (historyStartMs < earliestCached) {
                try {
                    const earlierCandles = await fetchHistorical1mCandles(settings, symbol, historyStartMs, earliestCached - 60000);
                    if (earlierCandles.length > 0) {
                        candles1m = [...earlierCandles, ...candles1m];
                    }
                } catch (err) {
                    console.error(`Failed to fetch earlier historical data for ${symbol}:`, err.message);
                }
            }

            // Fill in later data if needed
            if (liveEndMs > latestCached) {
                try {
                    const laterCandles = await fetchHistorical1mCandles(settings, symbol, latestCached + 60000, liveEndMs);
                    if (laterCandles.length > 0) {
                        candles1m = [...candles1m, ...laterCandles];
                    }
                } catch (err) {
                    console.error(`Failed to fetch later historical data for ${symbol}:`, err.message);
                }
            }

            // Sort the combined candles
            candles1m.sort((a, b) => a.timestamp - b.timestamp);
        }

        // Store the complete set of 1m candles for this symbol
        ws.replayData[symbol] = {
            candles1m,
            currentIndex: 0,
            openAggregateCandles: new Map()
        };

        // Initialize open aggregate candles for higher timeframes
        timeframes.forEach(timeframe => {
            if (timeframe !== '1m') {
                ws.replayData[symbol].openAggregateCandles.set(timeframe, null);
            }
        });

        // Send historical data for all requested timeframes up to liveStartMs
        for (const timeframe of timeframes) {
            const candles = candles1m.filter(c => c.timestamp < liveStartMs);
            if (timeframe === '1m') {
                // Send 1m candles directly
                console.log(`Sending ${candles.length} historical 1m candles for ${symbol}${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
                candles.forEach(candle => {
                    ws.replayOutputFunc({
                        ...candle,
                        source: candle.source || 'H',
                        isClosed: true
                    });
                });
            } else {
                // Aggregate and send higher timeframe candles
                const aggregatedCandles = aggregateReplayCandles(symbol, timeframe, historyStartMs, liveStartMs, candles);
                console.log(`Sending ${aggregatedCandles.length} aggregated ${timeframe} candles for ${symbol}${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
                aggregatedCandles.forEach(candle => {
                    ws.replayOutputFunc({
                        ...candle,
                        source: 'A',
                        isClosed: true
                    });
                });
            }
        }

        // For each symbol, set the current index to the first candle at or after liveStartMs
        const liveStartIndex = candles1m.findIndex(c => c.timestamp >= liveStartMs);
        if (liveStartIndex !== -1) {
            ws.replayData[symbol].currentIndex = liveStartIndex;
        } else {
            ws.replayData[symbol].currentIndex = candles1m.length; // Past the end if no live data
        }
    }

    // Set up the replay interval for the simulated live data
    if (liveStartMs < liveEndMs) {
        ws.replayCurrentTime = liveStartMs;
        ws.replayLiveEndMs = liveEndMs; // Store the end time in the WebSocket object
        console.log(`Starting live replay data stream at interval of ${replay_interval}ms${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
        ws.replayInterval = setInterval(() => {
            if (!ws.replayIsActive) {
                clearInterval(ws.replayInterval);
                return;
            }

            // Skip processing if replay is paused
            if (ws.replayIsPaused) {
                return;
            }

            // Send the next minute of data for all symbols
            let anyActive = false;
            let nextCandleTime = Number.MAX_SAFE_INTEGER;

            for (const symbol in ws.replayData) {
                const data = ws.replayData[symbol];
                const candles1m = data.candles1m;
                const currentIndex = data.currentIndex;

                if (currentIndex < candles1m.length) {
                    const currentCandle = candles1m[currentIndex];
                    
                    // Check if this candle's timestamp is for the current replay minute
                    if (currentCandle.timestamp <= ws.replayCurrentTime) {
                        // Send 1m candle if subscribed
                        if (ws.activeReplaySubscriptions[symbol].includes('1m')) {
                            ws.replayOutputFunc({
                                ...currentCandle,
                                source: 'T',
                                isClosed: true
                            });
                        }

                        // Update and send aggregate candles for higher timeframes
                        for (const timeframe of ws.activeReplaySubscriptions[symbol]) {
                            if (timeframe === '1m') continue;
                            
                            const intervalMs = parseTimeframeMs(timeframe);
                            const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
                            let baseTime;
                            if (needsSessionAlignment(timeframe)) {
                                baseTime = getSessionBasedAlignment(currentCandle.timestamp, intervalMs);
                            } else {
                                baseTime = Math.floor(currentCandle.timestamp / intervalMs) * intervalMs;
                            }
                            
                            let openCandle = ws.replayData[symbol].openAggregateCandles.get(timeframe);
                            
                            if (!openCandle || openCandle.timestamp !== baseTime) {
                                // Start a new aggregate candle
                                openCandle = {
                                    timestamp: baseTime,
                                    open: currentCandle.open,
                                    high: currentCandle.high,
                                    low: currentCandle.low,
                                    close: currentCandle.close,
                                    volume: currentCandle.volume,
                                    instrument: symbol,
                                    timeframe: timeframe,
                                    source: 'T',
                                    isClosed: false
                                };
                                ws.replayData[symbol].openAggregateCandles.set(timeframe, openCandle);
                            } else {
                                // Update existing aggregate candle
                                openCandle.high = Math.max(openCandle.high, currentCandle.high);
                                openCandle.low = Math.min(openCandle.low, currentCandle.low);
                                openCandle.close = currentCandle.close;
                                openCandle.volume += currentCandle.volume;
                            }
                            
                            // Determine if candle should be closed
                            const isLastCandleInInterval = currentCandle.timestamp === baseTime + intervalMs - 60000;
                            if (isLastCandleInInterval) {
                                openCandle.isClosed = true;
                            }
                            
                            // Send the current state of the aggregate candle
                            ws.replayOutputFunc({...openCandle});
                            
                            // Reset the aggregation if this was the last candle
                            if (openCandle.isClosed) {
                                ws.replayData[symbol].openAggregateCandles.set(timeframe, null);
                            }
                        }
                        
                        // Move to the next candle
                        ws.replayData[symbol].currentIndex++;
                        anyActive = true;
                    } else {
                        // This candle is in the future, track the earliest future candle
                        nextCandleTime = Math.min(nextCandleTime, currentCandle.timestamp);
                    }
                }
            }

            // If no active candles and we haven't reached the end yet, jump to the next candle time if there is one
            if (!anyActive && ws.replayCurrentTime < ws.replayLiveEndMs && nextCandleTime < Number.MAX_SAFE_INTEGER) {
                console.log(`Gap detected - jumping from ${new Date(ws.replayCurrentTime).toISOString()} to ${new Date(nextCandleTime).toISOString()}`);
                ws.replayCurrentTime = nextCandleTime;
            } else {
                // Normal case - advance the current replay time by one minute
                ws.replayCurrentTime += 60000;
            }
            
            // Check if we've reached the end of the replay - ONLY check end time
            if (ws.replayCurrentTime > ws.replayLiveEndMs) {
                clearInterval(ws.replayInterval);
                ws.replayIsActive = false;
                console.log(`Replay ended naturally by reaching end time (${new Date(ws.replayLiveEndMs).toISOString()})${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
                ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay completed' }));
                
                // Close log file stream if needed
                if (sendto === 'log' && ws.replayLogFileStream) {
                    ws.replayLogFileStream.end();
                    ws.replayLogFileStream = null;
                }
            }
        }, replay_interval);
    } else {
        // No live replay data, just finish
        ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay completed (no live data)' }));
        
        // Close log file stream if needed
        if (sendto === 'log' && ws.replayLogFileStream) {
            ws.replayLogFileStream.end();
            ws.replayLogFileStream = null;
        }
    }
}

// Handle 'stop_replay' action
function handleStopReplay(ws) {
    if (!ws.replayIsActive) {
        ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'No active replay to stop' }));
        return;
    }

    console.log(`Stopping replay${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
    ws.replayIsActive = false;
    
    if (ws.replayInterval) {
        clearInterval(ws.replayInterval);
        ws.replayInterval = null;
    }
    
    if (ws.replayLogFileStream) {
        ws.replayLogFileStream.end();
        ws.replayLogFileStream = null;
    }
    
    ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay stopped' }));
}

// Handle 'modify_replay' action
function handleModifyReplay(ws, request) {
    if (!ws.replayIsActive) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'No active replay to modify' }));
        return;
    }
    
    const { pause, replay_interval } = request;
    
    // Handle pause/resume if specified
    if (pause !== undefined) {
        if (pause === true && !ws.replayIsPaused) {
            console.log(`Pausing replay${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
            ws.replayIsPaused = true;
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay paused' }));
        } else if (pause === false && ws.replayIsPaused) {
            console.log(`Resuming replay${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
            ws.replayIsPaused = false;
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay resumed' }));
        } else if (pause === true && ws.replayIsPaused) {
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay already paused' }));
        } else if (pause === false && !ws.replayIsPaused) {
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay already running' }));
        }
    }
    
    // Handle replay interval change if specified
    if (replay_interval !== undefined && typeof replay_interval === 'number' && replay_interval > 0) {
        if (replay_interval !== ws.replayIntervalMs) {
            console.log(`Changing replay interval from ${ws.replayIntervalMs}ms to ${replay_interval}ms${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
            
            // Clear existing interval
            if (ws.replayInterval) {
                clearInterval(ws.replayInterval);
            }
            
            // Update the interval value
            ws.replayIntervalMs = replay_interval;
            
            // Restart the interval with the new value
            ws.replayInterval = setInterval(() => {
                if (!ws.replayIsActive) {
                    clearInterval(ws.replayInterval);
                    return;
                }

                // Skip processing if replay is paused
                if (ws.replayIsPaused) {
                    return;
                }

                // Send the next minute of data for all symbols
                let anyActive = false;
                let nextCandleTime = Number.MAX_SAFE_INTEGER;

                for (const symbol in ws.replayData) {
                    const data = ws.replayData[symbol];
                    const candles1m = data.candles1m;
                    const currentIndex = data.currentIndex;

                    if (currentIndex < candles1m.length) {
                        const currentCandle = candles1m[currentIndex];
                        
                        // Check if this candle's timestamp is for the current replay minute
                        if (currentCandle.timestamp <= ws.replayCurrentTime) {
                            // Send 1m candle if subscribed
                            if (ws.activeReplaySubscriptions[symbol].includes('1m')) {
                                ws.replayOutputFunc({
                                    ...currentCandle,
                                    source: 'T',
                                    isClosed: true
                                });
                            }

                            // Update and send aggregate candles for higher timeframes
                            for (const timeframe of ws.activeReplaySubscriptions[symbol]) {
                                if (timeframe === '1m') continue;
                                
                                const intervalMs = parseTimeframeMs(timeframe);
                                const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
                                let baseTime;
                                if (needsSessionAlignment(timeframe)) {
                                    baseTime = getSessionBasedAlignment(currentCandle.timestamp, intervalMs);
                                } else {
                                    baseTime = Math.floor(currentCandle.timestamp / intervalMs) * intervalMs;
                                }
                                
                                let openCandle = ws.replayData[symbol].openAggregateCandles.get(timeframe);
                                
                                if (!openCandle || openCandle.timestamp !== baseTime) {
                                    // Start a new aggregate candle
                                    openCandle = {
                                        timestamp: baseTime,
                                        open: currentCandle.open,
                                        high: currentCandle.high,
                                        low: currentCandle.low,
                                        close: currentCandle.close,
                                        volume: currentCandle.volume,
                                        instrument: symbol,
                                        timeframe: timeframe,
                                        source: 'T',
                                        isClosed: false
                                    };
                                    ws.replayData[symbol].openAggregateCandles.set(timeframe, openCandle);
                                } else {
                                    // Update existing aggregate candle
                                    openCandle.high = Math.max(openCandle.high, currentCandle.high);
                                    openCandle.low = Math.min(openCandle.low, currentCandle.low);
                                    openCandle.close = currentCandle.close;
                                    openCandle.volume += currentCandle.volume;
                                }
                                
                                // Determine if candle should be closed
                                const isLastCandleInInterval = currentCandle.timestamp === baseTime + intervalMs - 60000;
                                if (isLastCandleInInterval) {
                                    openCandle.isClosed = true;
                                }
                                
                                // Send the current state of the aggregate candle
                                ws.replayOutputFunc({...openCandle});
                                
                                // Reset the aggregation if this was the last candle
                                if (openCandle.isClosed) {
                                    ws.replayData[symbol].openAggregateCandles.set(timeframe, null);
                                }
                            }
                            
                            // Move to the next candle
                            ws.replayData[symbol].currentIndex++;
                            anyActive = true;
                        } else {
                            // This candle is in the future, track the earliest future candle
                            nextCandleTime = Math.min(nextCandleTime, currentCandle.timestamp);
                        }
                    }
                }

                // If no active candles and we haven't reached the end yet, jump to the next candle time if there is one
                if (!anyActive && ws.replayCurrentTime < ws.replayLiveEndMs && nextCandleTime < Number.MAX_SAFE_INTEGER) {
                    console.log(`Gap detected - jumping from ${new Date(ws.replayCurrentTime).toISOString()} to ${new Date(nextCandleTime).toISOString()}`);
                    ws.replayCurrentTime = nextCandleTime;
                } else {
                    // Normal case - advance the current replay time by one minute
                    ws.replayCurrentTime += 60000;
                }
                
                // Check if we've reached the end of the replay - ONLY check end time
                if (ws.replayCurrentTime > ws.replayLiveEndMs) {
                    clearInterval(ws.replayInterval);
                    ws.replayIsActive = false;
                    ws.send(JSON.stringify({ mtyp: 'ctrl', message: 'Replay completed' }));
                    
                    // Close log file stream if needed
                    if (ws.replayLogFileStream) {
                        ws.replayLogFileStream.end();
                        ws.replayLogFileStream = null;
                    }
                }
            }, replay_interval);
            
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Replay interval changed to ${replay_interval}ms` }));
        } else {
            ws.send(JSON.stringify({ mtyp: 'ctrl', message: `Replay interval already set to ${replay_interval}ms` }));
        }
    }
}

// Aggregate candles for replay from 1m data
function aggregateReplayCandles(instrument, timeframe, start, end, candles1m) {
    console.log(`Starting replay aggregation for ${instrument} to ${timeframe}`);
    
    if (timeframe === '1m') {
        return candles1m.filter(c => c.timestamp >= start && c.timestamp <= end);
    }

    const intervalMs = parseTimeframeMs(timeframe);
    const result = [];
    let currentCandle = null;

    candles1m.sort((a, b) => a.timestamp - b.timestamp);
    
    for (const candle1m of candles1m) {
        if (candle1m.timestamp < start || candle1m.timestamp > end) continue;
        
        const { getSessionBasedAlignment, needsSessionAlignment } = require('../candle-processor');
        let baseTime;
        if (needsSessionAlignment(timeframe)) {
            baseTime = getSessionBasedAlignment(candle1m.timestamp, intervalMs);
        } else {
            baseTime = Math.floor(candle1m.timestamp / intervalMs) * intervalMs;
        }
        
        if (!currentCandle || baseTime !== currentCandle.timestamp) {
            if (currentCandle) {
                currentCandle.isClosed = true;
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
                isClosed: false
            };
        } else {
            currentCandle.high = Math.max(currentCandle.high, candle1m.high);
            currentCandle.low = Math.min(currentCandle.low, candle1m.low);
            currentCandle.close = candle1m.close;
            currentCandle.volume += candle1m.volume;
        }
    }
    
    if (currentCandle) {
        currentCandle.isClosed = true;
        result.push(currentCandle);
    }

    console.log(`Aggregated ${result.length} replay candles for ${instrument} to ${timeframe}`);
    return result;
}

// Helper function to check if all data has been exhausted
function allDataExhausted(replayData) {
    // Check if we've processed all candles for all symbols
    for (const symbol in replayData) {
        const data = replayData[symbol];
        if (data.currentIndex < data.candles1m.length) {
            return false;  // Still have data to process
        }
    }
    return true;  // All data exhausted
}

module.exports = {
    handleGetReplay,
    handleStopReplay,
    handleModifyReplay
}; 