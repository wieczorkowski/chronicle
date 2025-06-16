// chronicle.js - WebSocket server back-end for trading/charting software

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// Import utility modules
const utils = require('./lib/utils');
const db = require('./lib/db');
const candleProcessor = require('./lib/candle-processor');
const dataFetcher = require('./lib/data-fetcher');

// Import handlers
const settingsHandlers = require('./lib/handlers/settings-handlers');
const cacheHandlers = require('./lib/handlers/cache-handlers');
const annotationHandlers = require('./lib/handlers/annotation-handlers');
const strategyHandlers = require('./lib/handlers/strategy-handlers');
const dataHandlers = require('./lib/handlers/data-handlers');
const replayHandlers = require('./lib/handlers/replay-handlers');

// Chronicle Back-end Version Level - for developer reference
const CHRONICLE_VERSION = '0.2.2' // Added Replay Functionality


// Configuration
const WS_PORT = 8080;
const DB_PATH = './chronicle_data.db';
const LOG_DIR = './logs';

// Ensure logs directory exists
utils.ensureLogDirectory(LOG_DIR);

// Initialize SQLite database
const database = db.initializeDatabase(DB_PATH);

// Set up database tables
db.setupTables(database);

// Initialize WebSocket server
const wss = new WebSocket.Server({ port: WS_PORT }, () => {
    console.log(`\n********\n*** CHRONICLE Data Manager Back-End WS Server, version ${CHRONICLE_VERSION} \n********\n`);
    console.log(`WebSocket server running on ws://localhost:${WS_PORT}`);
});

// Handle WebSocket connections
wss.on('connection', (ws) => {
    console.log('New client connected');
    ws.liveClient = null; // For live trades subscription
    ws.logFileStream = null; // Initialize logFileStream on ws
    ws.activeSubscriptions = {}; // Track active subscriptions: { instrument: [timeframe1, timeframe2, ...] }
    ws.isLiveActive = false; // Track if live data is active
    ws.tradeQueue = []; // Queue for trades during timeframe changes
    ws.isProcessingTimeframeChange = false; // Flag to serialize trade processing
    
    // Replay-specific properties
    ws.replayIsActive = false;  // Track if replay is active
    ws.replayLogFileStream = null;  // For replay log file
    ws.replayData = {};  // To store replay data
    ws.activeReplaySubscriptions = {};  // Track active replay subscriptions

    ws.on('message', async (msg) => {
        let request;
        try {
            request = JSON.parse(msg);
            console.log('Received WebSocket request:', JSON.stringify(request, null, 2));
        } catch (err) {
            ws.send(JSON.stringify({ mtyp: 'error', message: 'Invalid JSON message' }));
            return;
        }

        const { action } = request;

        // Route request to appropriate handler based on action
        try {
            switch (action) {
                case 'set_client_id':
                    settingsHandlers.handleSetClientId(ws, request);
                    break;
                    
                case 'get_settings':
                    settingsHandlers.handleGetSettings(database, ws, request);
                    break;
                    
                case 'save_settings':
                    settingsHandlers.handleSaveSettings(database, ws, request);
                    break;
                    
                case 'save_client_settings':
                    settingsHandlers.handleSaveClientSettings(database, ws, request);
                    break;
                    
                case 'get_client_settings':
                    settingsHandlers.handleGetClientSettings(database, ws, request);
                    break;
                    
                case 'clear_cache':
                    cacheHandlers.handleClearCache(database, ws, request);
                    break;
                    
                case 'stop_data':
                    dataHandlers.handleStopData(ws);
                    break;
                    
                case 'get_anno':
                    annotationHandlers.handleGetAnno(database, ws, request);
                    break;
                    
                case 'save_anno':
                    annotationHandlers.handleSaveAnno(database, ws, request, wss);
                    break;
                    
                case 'delete_anno':
                    annotationHandlers.handleDeleteAnno(database, ws, request, wss);
                    break;
                    
                case 'register_strat':
                    strategyHandlers.handleRegisterStrat(database, ws, request);
                    break;
                    
                case 'unregister_strat':
                    strategyHandlers.handleUnregisterStrat(database, ws, request);
                    break;
                    
                case 'get_strat':
                    strategyHandlers.handleGetStrat(database, ws, request);
                    break;
                    
                case 'sub_strat':
                    strategyHandlers.handleSubStrat(database, ws, request);
                    break;
                    
                case 'unsub_strat':
                    strategyHandlers.handleUnsubStrat(database, ws, request);
                    break;
                    
                case 'get_data':
                    await dataHandlers.handleGetData(database, ws, request, wss);
                    break;
                    
                case 'add_timeframe':
                    await dataHandlers.handleAddTimeframe(database, ws, request);
                    break;
                    
                case 'remove_timeframe':
                    dataHandlers.handleRemoveTimeframe(ws, request);
                    break;
                    
                case 'get_replay':
                    await replayHandlers.handleGetReplay(database, ws, request);
                    break;
                    
                case 'stop_replay':
                    replayHandlers.handleStopReplay(ws);
                    break;
                    
                case 'modify_replay':
                    replayHandlers.handleModifyReplay(ws, request);
                    break;
                    
                default:
                    console.log(`${action} is an unknown action.`);
                    ws.send(JSON.stringify({ mtyp: 'error', message: 'Unknown action' }));
            }
        } catch (err) {
            console.error(`Error handling ${action} action:`, err);
            ws.send(JSON.stringify({ mtyp: 'error', message: `Internal error processing ${action}: ${err.message}` }));
        }
    });

    ws.on('close', () => {
        console.log(`Client disconnected${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
        
        // Properly handle active connections on disconnect
        if (ws.isLiveActive) {
            console.log(`Cleaning up live connection for disconnected client${ws.clientID ? ` (Client ID: ${ws.clientID})` : ''}`);
            ws.isLiveActive = false;
        }
        
        if (ws.liveClient) {
            console.log('Destroying live client connection');
            ws.liveClient.destroy();
            ws.liveClient = null;
        }
        
        if (ws.logFileStream) {
            console.log('Closing log file stream');
            ws.logFileStream.end();
            ws.logFileStream = null;
        }
        
        // Clean up replay resources
        if (ws.replayIsActive) {
            console.log('Cleaning up replay resources');
            ws.replayIsActive = false;
            
            if (ws.replayInterval) {
                clearInterval(ws.replayInterval);
                ws.replayInterval = null;
            }
            
            if (ws.replayLogFileStream) {
                ws.replayLogFileStream.end();
                ws.replayLogFileStream = null;
            }
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket error:', err.message);
    });
});

// Handle server errors and shutdown
wss.on('error', (err) => console.error('WebSocket server error:', err.message));

process.on('SIGINT', () => {
    console.log('Shutting down server...');

    // Close all WebSocket client connections
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.close(1000, 'Server shutting down'); // Send close frame to clients
        }
    });

    // Stop live data streaming for all connected clients
    wss.clients.forEach(client => {
        if (client.liveClient) {
            client.liveClient.destroy(); // Destroy the live data feed for this client
            client.liveClient = null; // Clear reference
        }
        
        // Stop replay intervals
        if (client.replayInterval) {
            clearInterval(client.replayInterval);
            client.replayInterval = null;
        }
    });

    // Close the database
    database.close((err) => {
        if (err) console.error('Error closing database:', err);
        console.log('Database closed');

        // Close the WebSocket server
        wss.close(() => {
            console.log('WebSocket server closed');
            process.exit(0); // Exit cleanly
        });
    });
});