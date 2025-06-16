// Cache handlers for Chronicle

// Handle 'clear_cache' action
function handleClearCache(db, ws, request) {
    const { instrument = 'all', timeframe = 'all', start_time = 'all', end_time = 'all' } = request;
    let query = 'DELETE FROM CANDLES';
    const conditions = [];
    const params = [];

    if (instrument !== 'all') {
        conditions.push('instrument = ?');
        params.push(instrument);
    }
    if (timeframe !== 'all') {
        conditions.push('timeframe = ?');
        params.push(timeframe);
    }
    if (start_time !== 'all' || end_time !== 'all') {
        if (start_time !== 'all') {
            conditions.push('timestamp >= ?');
            params.push(start_time);
        }
        if (end_time !== 'all') {
            conditions.push('timestamp <= ?');
            params.push(end_time);
        }
    }

    if (conditions.length > 0) {
        query += ' WHERE ' + conditions.join(' AND ');
    }

    db.run(query, params, function (err) {
        if (err) {
            console.error('Error clearing cache:', err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error clearing cache' }));
        } else {
            console.log(`Cleared ${this.changes} candles from cache with query: ${query}`, params);
            ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'clear_cache_response', message: `Cleared ${this.changes} candles` }));
        }
    });
}

// Handle 'stop_data' action
function handleStopData(ws) {
    if (!ws.isLiveActive) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'No active data feed to stop' }));
        return;
    }
    
    console.log(`Stopping data feed for client${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
    
    // Stop the live client connection
    if (ws.liveClient) {
        ws.liveClient.destroy();
        ws.liveClient = null;
    }
    
    // Reset state
    ws.isLiveActive = false;
    
    // Close log file if open
    if (ws.sendto === 'log' && ws.logFileStream) {
        ws.logFileStream.end();
        ws.logFileStream = null;
    }
    
    // Send confirmation message
    ws.send(JSON.stringify({ 
        mtyp: 'ctrl', 
        action: 'stop_data_response', 
        message: 'Data feed stopped successfully' 
    }));
}

module.exports = {
    handleClearCache,
    handleStopData
}; 