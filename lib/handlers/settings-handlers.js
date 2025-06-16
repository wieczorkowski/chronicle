// Settings handlers for Chronicle

// Handle 'set_client_id' action
function handleSetClientId(ws, request) {
    const { clientid } = request;
    if (!clientid || typeof clientid !== 'string') {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid or missing clientid' }));
        return;
    }
    ws.clientID = clientid;
    console.log(`Client ID set to ${ws.clientID}`);
    ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'set_client_id_response', message: 'Client ID set successfully' }));
}

// Handle 'get_settings' action
function handleGetSettings(db, ws, request) {
    const { settings } = request;
    if (!settings || typeof settings !== 'string') {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid or missing settings name' }));
        return;
    }
    db.get('SELECT setting_value FROM SYSTEM_SETTINGS WHERE setting_name = ?', [settings], (err, row) => {
        if (err) {
            console.error(`Error fetching settings ${settings}:`, err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error fetching settings' }));
        } else if (row) {
            ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'settings_response', settings, value: JSON.parse(row.setting_value) }));
        } else {
            ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'settings_response', settings, value: null, message: 'Settings not found' }));
        }
    });
}

// Handle 'save_settings' action
function handleSaveSettings(db, ws, request) {
    const { settings, new_values } = request;
    if (!settings || typeof settings !== 'string' || !new_values || typeof new_values !== 'object') {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid settings name or values' }));
        return;
    }
    const settingValue = JSON.stringify(new_values);
    db.run(
        'INSERT OR REPLACE INTO SYSTEM_SETTINGS (setting_name, setting_value) VALUES (?, ?)',
        [settings, settingValue],
        (err) => {
            if (err) {
                console.error(`Error saving settings ${settings}:`, err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error saving settings' }));
            } else {
                console.log(`Settings ${settings} saved:`, new_values);
                ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'save_settings_response', settings, message: 'Settings saved successfully' }));
            }
        }
    );
}

// Handle 'save_client_settings' action
function handleSaveClientSettings(db, ws, request) {
    const { client_id, new_values } = request;
    if (!client_id || !new_values || typeof new_values !== 'object') {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid or missing client_id or new_values' }));
        return;
    }
    const settingsValue = JSON.stringify(new_values);
    db.run(
        'INSERT OR REPLACE INTO CLIENT_SETTINGS (client_id, client_settings) VALUES (?, ?)',
        [client_id, settingsValue],
        (err) => {
            if (err) {
                console.error(`Error saving client settings for ${client_id}:`, err.message);
                ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error saving client settings' }));
            } else {
                console.log(`Client settings for ${client_id} saved successfully.`);
                ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'save_client_settings_response', message: 'Client settings saved successfully' }));
            }
        }
    );
}

// Handle 'get_client_settings' action
function handleGetClientSettings(db, ws, request) {
    const { client_id } = request;
    if (!client_id) {
        ws.send(JSON.stringify({ mtyp: 'error', message: 'Missing client_id' }));
        return;
    }
    db.get('SELECT client_settings FROM CLIENT_SETTINGS WHERE client_id = ?', [client_id], (err, row) => {
        if (err) {
            console.error(`Error fetching client settings for ${client_id}:`, err.message);
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Database error fetching client settings' }));
        } else if (row) {
            const settingsValue = JSON.parse(row.client_settings);
            ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'client_settings_response', settings: settingsValue }));
        } else {
            ws.send(JSON.stringify({ mtyp: 'ctrl', action: 'client_settings_response', settings: null, message: 'No settings found for client' }));
        }
    });
}

module.exports = {
    handleSetClientId,
    handleGetSettings,
    handleSaveSettings,
    handleSaveClientSettings,
    handleGetClientSettings
}; 