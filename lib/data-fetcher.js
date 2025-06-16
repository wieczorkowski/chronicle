// Data fetching functions for Chronicle

const axios = require('axios');
const net = require('net');
const crypto = require('crypto');

// Fetch historical 1m candles from Databento API with retry logic and empty response handling
async function fetchHistorical1mCandles(settings, instrument, startMs, endMs) {
    let params = new URLSearchParams();
    params.append('dataset', settings.dataset);
    params.append('symbols', instrument);
    params.append('schema', 'ohlcv-1m');
    const startTimeISO = new Date(startMs).toISOString().slice(0, 16);
    const endTimeISO = new Date(endMs).toISOString().slice(0, 16);
    params.append('start', startTimeISO);
    params.append('end', endTimeISO);
    params.append('encoding', 'json');

    console.log(`HTTP Historical API call for ${instrument} from ${startTimeISO} to ${endTimeISO}`);

    let attempt = 0;
    while (attempt < 2) {
        try {
            const requestStartTime = Date.now();
            const response = await axios.post(settings.hist_host, params, {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                auth: { username: settings.api_key, password: '' }
            });
            const requestEndTime = Date.now();

            // Handle empty or whitespace-only response
            if (!response.data || response.data.trim() === '') {
                console.log(`HTTP request for ${instrument} returned 0 candles in ${requestEndTime - requestStartTime} ms`);
                return []; // Return empty array gracefully
            }

            const candles1m = response.data.trim().split('\n').map(line => {
                const raw = JSON.parse(line);
                return {
                    timestamp: Number(raw.hd.ts_event) / 1e6,
                    open: Number(raw.open) / 1e9,
                    high: Number(raw.high) / 1e9,
                    low: Number(raw.low) / 1e9,
                    close: Number(raw.close) / 1e9,
                    volume: Number(raw.volume || 0),
                    instrument,
                    timeframe: '1m',
                    source: 'H',
                    isClosed: true  // Historical 1m candles are always closed
                };
            });
            console.log(`HTTP request for ${instrument} returned ${candles1m.length} OHLCV-1m candles in ${requestEndTime - requestStartTime} ms`);
            return candles1m;
        } catch (err) {
            if (err.response && err.response.status === 422 && attempt === 0) {
                const detail = err.response.data.detail;
                if (detail && detail.payload && detail.payload.available_end) {
                    const availableEnd = detail.payload.available_end;
                    params.set('end', availableEnd);
                    attempt++;
                } else {
                    throw err;
                }
            } else {
                throw err;
            }
        }
    }
    throw new Error('Failed to fetch historical data after retrying');
}

// Start live trades subscription from Databento Live RAW API with diagnostics
async function startLiveTradesSubscription(settings, instruments, startTs, onTrade, retryCount = 0, updateClientRef = null) {
    if (!settings.live_host || !settings.api_key || !settings.dataset) {
        throw new Error('Live API settings (live_host, api_key, dataset) not configured in DATAFEED');
    }

    if (retryCount > 3) {
        throw new Error('Exceeded maximum retry attempts for live trades subscription');
    }

    const [liveHost, livePort] = settings.live_host.split(':');
    const livePortNum = parseInt(livePort || 13000);

    const liveClient = new net.Socket();
    liveClient.setKeepAlive(true, 30000);
    
    // Update client reference if callback provided
    if (updateClientRef && typeof updateClientRef === 'function') {
        updateClientRef(liveClient);
    }
    
    let buffer = '';
    let versionReceived = false;
    let cramReceived = false;
    let subscribed = false;
    const instrumentIdToSymbol = new Map(); // Added to map instrument_id to symbol

    console.log(`Connecting to Databento Live RAW API at ${liveHost}:${livePortNum} for instruments: ${instruments.join(', ')}`);

    // Create a promise to handle the rtype 21 invalid start time errors
    return new Promise((resolve, reject) => {
        let isHandlingError = false;

        liveClient.connect({ host: liveHost, port: livePortNum }, () => {
            console.log(`Connected to Databento Live RAW API at ${liveHost}:${livePortNum}`);
        });

        liveClient.on('data', (data) => {
            if (isHandlingError) return; // Skip processing if we're already handling an error

            buffer += data.toString();
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines.filter(l => l.trim())) {
                if (line.startsWith('lsg_version=') && !versionReceived) {
                    console.log('Server version received:', line);
                    versionReceived = true;
                } else if (line.startsWith('cram=') && !cramReceived) {
                    const cramChallenge = line.split('cram=')[1].trim();
                    console.log('CRAM challenge:', cramChallenge);

                    const hash = crypto.createHash('sha256')
                        .update(`${cramChallenge}|${settings.api_key}`)
                        .digest('hex');
                    const resp = `${hash}-${settings.api_key.slice(-5)}`;
                    const authResponse = `auth=${resp}|dataset=${settings.dataset}|encoding=json|ts_out=1|heartbeat_interval_s=10\n`;
                    liveClient.write(Buffer.from(authResponse));
                    console.log('Sent auth_response:', authResponse.trim());
                    cramReceived = true;
                } else if (line.includes('success=1')) {
                    console.log('Authentication successful!');
                    const subMsg = `schema=trades|stype_in=raw_symbol|symbols=${instruments.join(',')}|start=${startTs}\n`;
                    liveClient.write(Buffer.from(subMsg));
                    console.log('Sent subscription (trades):', subMsg.trim());
                    liveClient.write(Buffer.from('start_session=1\n'));
                    console.log('Sent start_session');
                    subscribed = true;
                } else if (line.includes('success=0')) {
                    console.error('Authentication failed:', line);
                    liveClient.destroy();
                    reject(new Error('Authentication failed'));
                } else if (subscribed) {
                    try {
                        const msg = JSON.parse(line);
                        if (msg.hd && msg.hd.rtype === 21 && msg.err && msg.err.startsWith('Invalid start time.')) {
                            // Handle invalid start time error
                            console.log('Received invalid start time error:', msg.err);
                            // Extract the suggested start time from the error message
                            const match = msg.err.match(/Must be ([0-9-T:+]+) or later/);
                            if (match && match[1]) {
                                isHandlingError = true;
                                
                                // Clear inactivity timer
                                if (inactivityTimer) {
                                    clearTimeout(inactivityTimer);
                                    inactivityTimer = null;
                                }
                                
                                const suggestedStartTime = new Date(match[1]);
                                console.log(`Extracted suggested start time: ${suggestedStartTime.toISOString()}`);
                                
                                // Convert to milliseconds for the new startMs
                                const newStartMs = suggestedStartTime.getTime();
                                console.log(`Will retry connection with new start time: ${suggestedStartTime.toISOString()}`);
                                
                                // Close current connection
                                liveClient.destroy();
                                
                                // Retry with the new start time, passing the update callback
                                console.log(`Retrying connection (attempt ${retryCount + 1})`);
                                startLiveTradesSubscription(settings, instruments, newStartMs, onTrade, retryCount + 1, updateClientRef)
                                    .then(resolve)
                                    .catch(reject);
                            } else {
                                reject(new Error(`Could not extract suggested start time from error: ${msg.err}`));
                            }
                        } else if (msg.hd && msg.hd.rtype === 22) { // Instrument mapping message
                            const instrumentId = msg.hd.instrument_id;
                            const symbol = msg.stype_in_symbol;
                            instrumentIdToSymbol.set(instrumentId, symbol);
                            console.log(`Mapped instrument_id ${instrumentId} to symbol ${symbol}`);
                        } else if (msg.hd && msg.hd.rtype === 0 && msg.action === "T") { // Trade message
                            const instrumentId = msg.hd.instrument_id;
                            const symbol = instrumentIdToSymbol.get(instrumentId) || 'unknown';
                            const trade = {
                                timestamp: Number(msg.hd.ts_event) / 1e6,
                                price: Number(msg.price) / 1e9,
                                size: Number(msg.size),
                                side: msg.side,
                                instrument: symbol
                            };
                            onTrade(trade); // Call the provided callback
                        } else if (msg.hd && msg.hd.rtype === 23 && msg.msg === 'Heartbeat') {
                            console.log(`   ***   [${new Date().toLocaleString()}] - ${msg.msg} received, rtype ${msg.hd.rtype}   ***   `);
                        } else {
                            console.log(`[${new Date().toLocaleString()}] Control message (rtype: ${msg.hd ? msg.hd.rtype : 'unknown'}):`, msg);
                        }
                    } catch (err) {
                        console.error('Error parsing live data message:', err.message, 'line:', line);
                    }
                }
            }
        });

        liveClient.on('error', (err) => {
            if (!isHandlingError) {
                console.error('Live API connection error:', err.message);
                reject(err);
            }
        });

        liveClient.on('close', () => {
            if (!isHandlingError) {
                console.log('Live API connection closed');
                resolve(liveClient); // Resolve with the client for backward compatibility
            }
        });
    });
}

// Fetch live 1m candles from Databento Live RAW API with inactivity timeout
async function fetchLive1mCandles(settings, instruments, startMs, endMs, retryCount = 0, updateClientRef = null) {
    if (!settings.live_host || !settings.api_key || !settings.dataset) {
        throw new Error('Live API settings (live_host, api_key, dataset) not configured in DATAFEED');
    }

    if (retryCount > 3) {
        throw new Error('Exceeded maximum retry attempts for fetching live candles');
    }

    const [liveHost, livePort] = settings.live_host.split(':');
    const livePortNum = parseInt(livePort || 13000);

    return new Promise((resolve, reject) => {
        const liveClient = new net.Socket();
        liveClient.setKeepAlive(true, 30000);
        
        // Update client reference if callback provided
        if (updateClientRef && typeof updateClientRef === 'function') {
            updateClientRef(liveClient);
        }
        
        let buffer = '';
        let versionReceived = false;
        let cramReceived = false;
        let subscribed = false;
        const liveCandles = [];
        let inactivityTimer;
        let isHandlingError = false;

        console.log(`Connecting to Databento Live RAW API at ${liveHost}:${livePortNum} for instruments: ${instruments.join(', ')}`);

        liveClient.connect({ host: liveHost, port: livePortNum }, () => {
            console.log(`Connected to Databento Live RAW API at ${liveHost}:${livePortNum}`);
        });

        liveClient.on('data', (data) => {
            if (isHandlingError) return; // Skip processing if we're already handling an error

            buffer += data.toString();
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines.filter(l => l.trim())) {
                if (line.startsWith('lsg_version=') && !versionReceived) {
                    console.log('Server version received:', line);
                    versionReceived = true;
                } else if (line.startsWith('cram=') && !cramReceived) {
                    const cramChallenge = line.split('cram=')[1].trim();
                    console.log('CRAM challenge:', cramChallenge);

                    const hash = crypto.createHash('sha256')
                        .update(`${cramChallenge}|${settings.api_key}`)
                        .digest('hex');
                    const resp = `${hash}-${settings.api_key.slice(-5)}`;
                    const authResponse = `auth=${resp}|dataset=${settings.dataset}|encoding=json|ts_out=1|heartbeat_interval_s=10\n`;
                    liveClient.write(Buffer.from(authResponse));
                    console.log('Sent auth_response:', authResponse.trim());
                    cramReceived = true;
                } else if (line.includes('success=1')) {
                    console.log('Authentication successful!');
                    const startTs = startMs * 1e6; // Convert to nanoseconds
                    const subMsg = `schema=ohlcv-1m|stype_in=raw_symbol|symbols=${instruments.join(',')}|start=${startTs}\n`;
                    liveClient.write(Buffer.from(subMsg));
                    console.log('Sent subscription (OHLCV-1m):', subMsg.trim());
                    liveClient.write(Buffer.from('start_session=1\n'));
                    console.log('Sent start_session');
                    subscribed = true;
                    // Start inactivity timer
                    inactivityTimer = setTimeout(() => {
                        console.log(`Inactivity timeout: no candles received after 500ms.`);
                        liveClient.destroy();
                        resolve(liveCandles); // Resolve with whatever was collected, possibly empty
                    }, 500);
                } else if (line.includes('success=0')) {
                    console.error('Authentication failed:', line);
                    reject(new Error('Authentication failed'));
                } else if (subscribed) {
                    try {
                        const msg = JSON.parse(line);
                        if (msg.hd && msg.hd.rtype === 21 && msg.err && msg.err.startsWith('Invalid start time.')) {
                            // Handle invalid start time error
                            console.log('Received invalid start time error:', msg.err);
                            // Extract the suggested start time from the error message
                            const match = msg.err.match(/Must be ([0-9-T:+]+) or later/);
                            if (match && match[1]) {
                                isHandlingError = true;
                                
                                // Clear inactivity timer
                                if (inactivityTimer) {
                                    clearTimeout(inactivityTimer);
                                    inactivityTimer = null;
                                }
                                
                                const suggestedStartTime = new Date(match[1]);
                                console.log(`Extracted suggested start time: ${suggestedStartTime.toISOString()}`);
                                
                                // Convert to milliseconds for the new startMs
                                const newStartMs = suggestedStartTime.getTime();
                                console.log(`Will retry connection with new start time: ${suggestedStartTime.toISOString()}`);
                                
                                // Close current connection
                                liveClient.destroy();
                                
                                // Retry with the new start time, passing the update callback
                                console.log(`Retrying connection (attempt ${retryCount + 1})`);
                                fetchLive1mCandles(settings, instruments, newStartMs, endMs, retryCount + 1, updateClientRef)
                                    .then(resolve)
                                    .catch(reject);
                            } else {
                                reject(new Error(`Could not extract suggested start time from error: ${msg.err}`));
                            }
                        } else if (msg.hd && msg.hd.rtype === 33) { // OHLCV-1m candle
                            const timestamp = Number(msg.hd.ts_event) / 1e6;
                            if (timestamp < startMs || timestamp > endMs) return;

                            const candle = {
                                timestamp,
                                open: Number(msg.open) / 1e9,
                                high: Number(msg.high) / 1e9,
                                low: Number(msg.low) / 1e9,
                                close: Number(msg.close) / 1e9,
                                volume: Number(msg.volume || 0),
                                instrument: msg.symbol || instruments[0], // Use symbol from message or first instrument
                                timeframe: '1m',
                                source: 'L',
                                isClosed: true  // Live 1m candles are always closed
                            };
                            liveCandles.push(candle);

                            console.log('Received OHLCV-1m candle:', new Date(candle.timestamp).toLocaleString(),
                                'OHLC:', candle.open, candle.high, candle.low, candle.close, 'Volume:', candle.volume);

                            // Reset inactivity timer
                            if (inactivityTimer) clearTimeout(inactivityTimer);
                            inactivityTimer = setTimeout(() => {
                                console.log(`Inactivity timeout: no new candles for 500ms. Received ${liveCandles.length} candles.`);
                                liveClient.destroy();
                                resolve(liveCandles); // Resolve with collected candles
                            }, 500);
                        } else {
                            console.log(`[${new Date().toLocaleString()}] Control message (rtype: ${msg.hd ? msg.hd.rtype : 'unknown'}):`, msg);
                        }
                    } catch (err) {
                        console.error('Error parsing live data message:', err.message, 'line:', line);
                    }
                }
            }
        });

        liveClient.on('error', (err) => {
            if (!isHandlingError) {
                console.error('Live API connection error:', err.message);
                if (inactivityTimer) clearTimeout(inactivityTimer);
                reject(err);
            }
        });

        liveClient.on('close', () => {
            if (!isHandlingError) {
                if (inactivityTimer) clearTimeout(inactivityTimer);
                console.log(`Live API connection closed with ${liveCandles.length} candles received`);
                resolve(liveCandles); // Resolve with whatever was collected
            }
        });
    });
}

module.exports = {
    fetchHistorical1mCandles,
    startLiveTradesSubscription,
    fetchLive1mCandles
}; 