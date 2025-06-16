// Strategy handlers for Chronicle

// Handle 'register_strat' action
function handleRegisterStrat(db, ws, request) {
    const { clientid, reinit = false, name, description, parameters } = request;
    
    if (!clientid) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing required clientid field' }));
        return;
    }
    
    // Convert parameters object to JSON string if provided
    const parametersJson = parameters ? JSON.stringify(parameters) : null;
    
    if (reinit) {
        // If reinit is true, first delete any annotations with this client_id
        db.run('DELETE FROM ANNOTATIONS WHERE client_id = ?', [clientid], function(err) {
            if (err) {
                console.error('Error deleting annotations during strategy reinit:', err.message);
            } else {
                console.log(`Deleted ${this.changes} annotation(s) for client ${clientid} during strategy reinit`);
            }
            
            // Then INSERT OR REPLACE the strategy
            db.run(
                'INSERT OR REPLACE INTO STRATEGIES (client_id, strategy_name, description, parameters, subscribers) VALUES (?, ?, ?, ?, ?)',
                [clientid, name, description, parametersJson, JSON.stringify({"subscribers":[]})],
                function(err) {
                    if (err) {
                        console.error('Error registering strategy:', err.message);
                        ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error registering strategy' }));
                    } else {
                        console.log(`Registered strategy for client ${clientid} with reinit=true`);
                        ws.send(JSON.stringify({ 
                            mtyp: 'ctrl', 
                            action: 'register_strat_response', 
                            clientid: clientid,
                            message: 'Strategy registered successfully with reinit' 
                        }));
                    }
                }
            );
        });
    } else {
        // If reinit is false, try to insert the strategy
        db.get('SELECT client_id FROM STRATEGIES WHERE client_id = ?', [clientid], (err, row) => {
            if (err) {
                console.error('Error checking existing strategy:', err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error checking existing strategy' }));
                return;
            }
            
            if (row) {
                // Strategy already exists with this client_id
                console.log(`Strategy already registered with client ${clientid}`);
                ws.send(JSON.stringify({ 
                    mtyp: 'ctrl', 
                    action: 'register_strat_response', 
                    clientid: clientid,
                    message: 'Strategy already registered with this client ID' 
                }));
            } else {
                // Insert new strategy
                db.run(
                    'INSERT INTO STRATEGIES (client_id, strategy_name, description, parameters, subscribers) VALUES (?, ?, ?, ?, ?)',
                    [clientid, name, description, parametersJson, JSON.stringify({"subscribers":[]})],
                    function(err) {
                        if (err) {
                            console.error('Error registering strategy:', err.message);
                            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error registering strategy' }));
                        } else {
                            console.log(`Registered new strategy for client ${clientid}`);
                            ws.send(JSON.stringify({ 
                                mtyp: 'ctrl', 
                                action: 'register_strat_response', 
                                clientid: clientid,
                                message: 'Strategy registered successfully' 
                            }));
                        }
                    }
                );
            }
        });
    }
}

// Handle 'unregister_strat' action
function handleUnregisterStrat(db, ws, request) {
    const { clientid } = request;
    
    if (!clientid) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing clientid' }));
        return;
    }
    
    // First remove the strategy from STRATEGIES table
    db.run('DELETE FROM STRATEGIES WHERE client_id = ?', [clientid], function(stratErr) {
        if (stratErr) {
            console.error('Error removing strategy:', stratErr.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error removing strategy' }));
            return;
        }
        
        const stratsRemoved = this.changes;
        
        // Then remove annotations with this client_id
        db.run('DELETE FROM ANNOTATIONS WHERE client_id = ?', [clientid], function(annoErr) {
            if (annoErr) {
                console.error('Error removing annotations during unregister:', annoErr.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error removing annotations' }));
                return;
            }
            
            const annosRemoved = this.changes;
            console.log(`Unregistered strategy for client ${clientid}: removed ${stratsRemoved} strategy and ${annosRemoved} annotations`);
            
            ws.send(JSON.stringify({ 
                mtyp: 'ctrl', 
                action: 'unregister_strat_response', 
                clientid: clientid,
                stratsRemoved: stratsRemoved,
                annosRemoved: annosRemoved,
                message: `Unregistered strategy for client ${clientid}: removed ${stratsRemoved} strategy and ${annosRemoved} annotations` 
            }));
        });
    });
}

// Handle 'get_strat' action
function handleGetStrat(db, ws, request) {
    const { clientid, name } = request;
    
    // Build the WHERE clause based on provided parameters
    let whereClause = [];
    let params = [];
    
    // Add clientid to WHERE clause if provided and not 'all'
    if (clientid && clientid !== 'all') {
        whereClause.push('client_id = ?');
        params.push(clientid);
    }
    
    // Add strategy_name to WHERE clause if provided and not 'all'
    if (name && name !== 'all') {
        whereClause.push('strategy_name = ?');
        params.push(name);
    }
    
    // Build the query
    let query = 'SELECT client_id, strategy_name, description, parameters, subscribers FROM STRATEGIES';
    if (whereClause.length > 0) {
        query += ' WHERE ' + whereClause.join(' AND ');
    }
    
    // Execute the query
    db.all(query, params, (err, rows) => {
        if (err) {
            console.error('Error retrieving strategies:', err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error retrieving strategies' }));
            return;
        }
        
        // Format the results (parse parameters JSON)
        const formattedRows = rows.map(row => {
            return {
                clientid: row.client_id,
                name: row.strategy_name,
                description: row.description,
                parameters: row.parameters ? JSON.parse(row.parameters) : null,
                subscribers: row.subscribers? JSON.parse(row.subscribers) : null
            };
        });
        
        console.log(`Retrieved ${rows.length} strategy/strategies with query: ${query}`, params);
        
        // Send the response
        ws.send(JSON.stringify({
            mtyp: 'ctrl',
            action: 'get_strat_response',
            strats: formattedRows
        }));
    });
}

// Handle 'sub_strat' action
function handleSubStrat(db, ws, request) {
    const { clientid, stratid } = request;
    
    if (!clientid || !stratid) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing clientid or stratid' }));
        return;
    }
    
    // Get the strategy record
    db.get('SELECT client_id, subscribers FROM STRATEGIES WHERE client_id = ?', [stratid], (err, row) => {
        if (err) {
            console.error('Error retrieving strategy for subscription:', err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error retrieving strategy' }));
            return;
        }
        
        if (!row) {
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Strategy not found' }));
            return;
        }
        
        // Parse the subscribers JSON
        let subscribersObj;
        try {
            subscribersObj = row.subscribers ? JSON.parse(row.subscribers) : { "subscribers": [] };
        } catch (e) {
            console.error('Error parsing subscribers JSON:', e);
            subscribersObj = { "subscribers": [] };
        }
        
        // Check if client is already subscribed
        if (subscribersObj.subscribers.includes(clientid)) {
            ws.send(JSON.stringify({ 
                mtyp: 'ctrl', 
                action: 'sub_strat_response', 
                clientid: clientid,
                stratid: stratid,
                message: 'Already subscribed to this strategy' 
            }));
            return;
        }
        
        // Add client to subscribers array
        subscribersObj.subscribers.push(clientid);
        
        // Update the strategy record
        db.run(
            'UPDATE STRATEGIES SET subscribers = ? WHERE client_id = ?',
            [JSON.stringify(subscribersObj), stratid],
            function(err) {
                if (err) {
                    console.error('Error updating strategy subscribers:', err.message);
                    ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error updating strategy subscribers' }));
                } else {
                    console.log(`Client ${clientid} subscribed to strategy ${stratid}`);
                    ws.send(JSON.stringify({ 
                        mtyp: 'ctrl', 
                        action: 'sub_strat_response', 
                        clientid: clientid,
                        stratid: stratid,
                        message: 'Successfully subscribed to strategy' 
                    }));
                }
            }
        );
    });
}

// Handle 'unsub_strat' action
function handleUnsubStrat(db, ws, request) {
    const { clientid, stratid } = request;
    
    if (!clientid || !stratid) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing clientid or stratid' }));
        return;
    }
    
    // Get the strategy record
    db.get('SELECT client_id, subscribers FROM STRATEGIES WHERE client_id = ?', [stratid], (err, row) => {
        if (err) {
            console.error('Error retrieving strategy for unsubscription:', err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error retrieving strategy' }));
            return;
        }
        
        if (!row) {
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Strategy not found' }));
            return;
        }
        
        // Parse the subscribers JSON
        let subscribersObj;
        try {
            subscribersObj = row.subscribers ? JSON.parse(row.subscribers) : { "subscribers": [] };
        } catch (e) {
            console.error('Error parsing subscribers JSON:', e);
            subscribersObj = { "subscribers": [] };
        }
        
        // Check if client is subscribed
        if (!subscribersObj.subscribers.includes(clientid)) {
            ws.send(JSON.stringify({ 
                mtyp: 'ctrl', 
                action: 'unsub_strat_response', 
                clientid: clientid,
                stratid: stratid,
                message: 'Not subscribed to this strategy' 
            }));
            return;
        }
        
        // Remove client from subscribers array
        subscribersObj.subscribers = subscribersObj.subscribers.filter(id => id !== clientid);
        
        // Update the strategy record
        db.run(
            'UPDATE STRATEGIES SET subscribers = ? WHERE client_id = ?',
            [JSON.stringify(subscribersObj), stratid],
            function(err) {
                if (err) {
                    console.error('Error updating strategy subscribers:', err.message);
                    ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error updating strategy subscribers' }));
                } else {
                    console.log(`Client ${clientid} unsubscribed from strategy ${stratid}`);
                    ws.send(JSON.stringify({ 
                        mtyp: 'ctrl', 
                        action: 'unsub_strat_response', 
                        clientid: clientid,
                        stratid: stratid,
                        message: 'Successfully unsubscribed from strategy' 
                    }));
                }
            }
        );
    });
}

module.exports = {
    handleRegisterStrat,
    handleUnregisterStrat,
    handleGetStrat,
    handleSubStrat,
    handleUnsubStrat
}; 