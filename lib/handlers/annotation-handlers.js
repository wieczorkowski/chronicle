// Annotation handlers for Chronicle

const WebSocket = require('ws');

// Handle 'get_anno' action
function handleGetAnno(db, ws, request) {
    const { clientid, clienttype, instrument, timeframe } = request;
    
    // Build the WHERE clause based on provided parameters
    let whereClause = [];
    let params = [];
    
    // Add clientid to WHERE clause if provided and not 'all'
    if (clientid && clientid !== 'all') {
        whereClause.push('client_id = ?');
        params.push(clientid);
    }
    
    // Add instrument to WHERE clause if provided and not 'all'
    if (instrument && instrument !== 'all') {
        whereClause.push('instrument = ?');
        params.push(instrument);
    }
    
    // Add timeframe to WHERE clause if provided and not 'all'
    if (timeframe && timeframe !== 'all') {
        whereClause.push('timeframe = ?');
        params.push(timeframe);
    }
    
    // Build the query
    let query = 'SELECT client_id, instrument, timeframe, annotype, unique_id, object FROM ANNOTATIONS';
    if (whereClause.length > 0) {
        query += ' WHERE ' + whereClause.join(' AND ');
    }
    
    // Execute the query
    db.all(query, params, (err, rows) => {
        if (err) {
            console.error('Error retrieving annotations:', err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error retrieving annotations' }));
            return;
        }
        
        // Format the results
        const formattedRows = rows.map(row => {
            return {
                clientid: row.client_id,
                instrument: row.instrument,
                timeframe: row.timeframe,
                annotype: row.annotype,
                unique: row.unique_id,
                object: JSON.parse(row.object)
            };
        });
        
        console.log(`Retrieved ${rows.length} annotation(s) with query: ${query}`, params);
        
        // Send the response
        ws.send(JSON.stringify({
            mtyp: 'ctrl',
            action: 'get_anno_response',
            clienttype: clienttype,
            annos: formattedRows
        }));
    });
}

// Handle 'save_anno' action
function handleSaveAnno(db, ws, request, wss) {
    const { clientid, instrument, timeframe, annotype, unique, object } = request;
    
    if (!clientid || !instrument || !timeframe || !annotype || !unique || !object) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing required annotation fields' }));
        return;
    }
    
    // Convert object to JSON string
    const objectJson = JSON.stringify(object);
    
    // Execute INSERT OR REPLACE
    db.run(
        'INSERT OR REPLACE INTO ANNOTATIONS (client_id, instrument, timeframe, annotype, unique_id, object) VALUES (?, ?, ?, ?, ?, ?)',
        [clientid, instrument, timeframe, annotype, unique, objectJson],
        function(err) {
            if (err) {
                console.error('Error saving annotation:', err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error saving annotation' }));
            } else {
                console.log(`Saved annotation for client ${clientid}, unique ID: ${unique}`);
                ws.send(JSON.stringify({ 
                    mtyp: 'ctrl', 
                    action: 'save_anno_response', 
                    clientid: clientid,
                    unique: unique,
                    message: 'Annotation saved successfully' 
                }));
                
                // Check if this annotation is from a registered strategy
                db.get('SELECT client_id, subscribers FROM STRATEGIES WHERE client_id = ?', [clientid], (err, row) => {
                    if (err) {
                        console.error('Error checking if annotation is from a registered strategy:', err.message);
                        return;
                    }
                    
                    if (row) {
                        // This annotation is from a registered strategy - send to subscribed clients
                        let subscribersObj;
                        try {
                            subscribersObj = row.subscribers ? JSON.parse(row.subscribers) : { "subscribers": [] };
                        } catch (e) {
                            console.error('Error parsing subscribers JSON:', e);
                            subscribersObj = { "subscribers": [] };
                        }
                        
                        const subscribers = subscribersObj.subscribers || [];
                        
                        if (subscribers.length > 0) {
                            console.log(`Sending targeted messages for strategy annotation update for client ${clientid} to ${subscribers.length} subscribers`);
                            
                            // Create annotation data message
                            const annoData = {
                                clientid,
                                instrument,
                                timeframe,
                                annotype,
                                unique,
                                object
                            };
                            
                            // Send to each subscribed client
                            wss.clients.forEach(client => {
                                if (client.readyState === WebSocket.OPEN && client.clientID && subscribers.includes(client.clientID)) {
                                    client.send(JSON.stringify({
                                        mtyp: 'strategy',
                                        action: 'anno_saved',
                                        anno: annoData
                                    }));
                                }
                            });
                        }
                    }
                });
            }
        }
    );
}

// Handle 'delete_anno' action
function handleDeleteAnno(db, ws, request, wss) {
    const { clientid, unique } = request;
    
    if (!clientid || !unique) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing clientid or unique identifier' }));
        return;
    }
    
    // Build the WHERE clause based on clientid and unique values
    let whereClause = [];
    let params = [];
    
    // Add clientid to WHERE clause if not 'all'
    if (clientid !== 'all') {
        whereClause.push('client_id = ?');
        params.push(clientid);
    }
    
    // Add unique_id to WHERE clause if not 'all'
    if (unique !== 'all') {
        whereClause.push('unique_id = ?');
        params.push(unique);
    }
    
    // First check if this is a registered strategy if we have a specific clientid
    if (clientid && clientid !== 'all') {
        db.get('SELECT client_id, subscribers FROM STRATEGIES WHERE client_id = ?', [clientid], (err, stratRow) => {
            if (err) {
                console.error('Error checking if annotation is from a registered strategy:', err.message);
            }
            
            // Now execute the delete
            executeDelete(stratRow ? true : false, stratRow);
        });
    } else {
        // If clientid is 'all', we don't need to check strategies
        executeDelete(false, null);
    }
    
    // Function to execute the delete query and handle broadcasting if needed
    function executeDelete(isStrategy, strategyRow) {
        // Build the final query
        let query = 'DELETE FROM ANNOTATIONS';
        if (whereClause.length > 0) {
            query += ' WHERE ' + whereClause.join(' AND ');
        }
        
        // Execute the query
        db.run(query, params, function(err) {
            if (err) {
                console.error('Error deleting annotation:', err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error deleting annotation' }));
            } else {
                console.log(`Deleted ${this.changes} annotation(s) with query: ${query}`, params);
                ws.send(JSON.stringify({ 
                    mtyp: 'ctrl', 
                    action: 'delete_anno_response', 
                    deleted: this.changes,
                    clientid: clientid,
                    unique: unique,
                    message: `Deleted ${this.changes} annotation(s)` 
                }));
                
                // If this was a strategy annotation, send targeted messages to subscribers
                if (isStrategy && this.changes > 0 && strategyRow) {
                    let subscribersObj;
                    try {
                        subscribersObj = strategyRow.subscribers ? JSON.parse(strategyRow.subscribers) : { "subscribers": [] };
                    } catch (e) {
                        console.error('Error parsing subscribers JSON:', e);
                        subscribersObj = { "subscribers": [] };
                    }
                    
                    const subscribers = subscribersObj.subscribers || [];
                    
                    if (subscribers.length > 0) {
                        console.log(`Sending targeted messages for strategy annotation deletion for client ${clientid} to ${subscribers.length} subscribers`);
                        
                        // Send to each subscribed client
                        wss.clients.forEach(client => {
                            if (client.readyState === WebSocket.OPEN && client.clientID && subscribers.includes(client.clientID)) {
                                client.send(JSON.stringify({
                                    mtyp: 'strategy',
                                    action: 'anno_deleted',
                                    clientid: clientid,
                                    uniqueid: unique
                                }));
                            }
                        });
                    }
                }
            }
        });
    }
}

module.exports = {
    handleGetAnno,
    handleSaveAnno,
    handleDeleteAnno
}; 