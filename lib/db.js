// Database operations for Chronicle

const sqlite3 = require('sqlite3').verbose();

// Initialize SQLite database with PRAGMA optimizations
function initializeDatabase(dbPath) {
    const db = new sqlite3.Database(dbPath, (err) => {
        if (err) {
            console.error('Failed to connect to SQLite database:', err.message);
            process.exit(1);
        }
        console.log('Connected to SQLite database at', dbPath);
    });

    // Apply PRAGMA settings for performance
    db.serialize(() => {
        db.run('PRAGMA journal_mode = WAL;', (err) => {
            if (err) console.error('Failed to set journal_mode to WAL:', err);
            else console.log('Set journal_mode to WAL');
        });
        db.run('PRAGMA synchronous = NORMAL;', (err) => {
            if (err) console.error('Failed to set synchronous to NORMAL:', err);
            else console.log('Set synchronous to NORMAL');
        });
        db.run('PRAGMA cache_size = -131072;', (err) => {  // 128MB cache
            if (err) console.error('Failed to set cache_size:', err);
            else console.log('Set cache_size to 128MB');
        });
    });

    return db;
}

// Set up database tables
function setupTables(db) {
    db.serialize(() => {
        db.run(`
            CREATE TABLE IF NOT EXISTS CANDLES (
                instrument TEXT,
                timeframe TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (instrument, timeframe, timestamp)
            )
        `, (err) => {
            if (err) console.error('Error creating CANDLES table:', err.message);
            else console.log('CANDLES table ready');
        });

        db.run(`
            CREATE TABLE IF NOT EXISTS SYSTEM_SETTINGS (
                setting_name TEXT PRIMARY KEY,
                setting_value TEXT
            )
        `, (err) => {
            if (err) console.error('Error creating SYSTEM_SETTINGS table:', err.message);
            else console.log('SYSTEM_SETTINGS table ready');
        });

        db.run(`
            CREATE TABLE IF NOT EXISTS CLIENT_SETTINGS (
                client_id TEXT PRIMARY KEY,
                client_settings TEXT
            )
        `, (err) => {
            if (err) console.error('Error creating CLIENT_SETTINGS table:', err.message);
            else console.log('CLIENT_SETTINGS table ready');
        });

        db.run(`
            CREATE TABLE IF NOT EXISTS ANNOTATIONS (
                client_id TEXT,
                instrument TEXT,
                timeframe TEXT,
                annotype TEXT,
                unique_id TEXT,
                object TEXT,
                PRIMARY KEY (client_id, unique_id)
            )
        `, (err) => {
            if (err) console.error('Error creating ANNOTATIONS table:', err.message);
            else console.log('ANNOTATIONS table ready');
        });

        db.run(`
            CREATE TABLE IF NOT EXISTS STRATEGIES (
                strategy_name TEXT,
                client_id TEXT,
                description TEXT,
                parameters TEXT,
                subscribers TEXT,
                PRIMARY KEY (client_id)
            )
        `, (err) => {
            if (err) console.error('Error creating STRATEGIES table:', err.message);
            else console.log('STRATEGIES table ready');
        });
    });
}

// Get settings from the database
function getSettings(db, settingName) {
    return new Promise((resolve, reject) => {
        db.get('SELECT setting_value FROM SYSTEM_SETTINGS WHERE setting_name = ?', [settingName], (err, row) => {
            if (err) reject(err);
            else resolve(row ? JSON.parse(row.setting_value) : null);
        });
    });
}

// Batch insert candles into the database, skipping "null" candles
async function batchInsertCandles(db, candles, instrument, timeframe) {
    console.log(`Starting cache write of ${candles.length} candles for ${instrument} ${timeframe}`);

    // Filter out "null" candles (volume == 0 or any OHLC is null)
    const validCandles = candles.filter(candle =>
        candle.volume > 0 &&
        candle.open !== null &&
        candle.high !== null &&
        candle.low !== null &&
        candle.close !== null
    );

    // Log skipped "null" candles
    const skippedCandles = candles.filter(candle =>
        candle.volume === 0 ||
        candle.open === null ||
        candle.high === null ||
        candle.low === null ||
        candle.close === null
    );
    skippedCandles.forEach(candle => {
        console.log(`Null candle encountered for ${candle.instrument} at ${new Date(candle.timestamp).toISOString()}: not saved to cache.`);
    });

    const startTime = Date.now();
    return new Promise((resolve, reject) => {
        if (validCandles.length === 0) {
            console.log(`No valid candles to insert for ${instrument} ${timeframe}`);
            resolve();
            return;
        }

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            const stmt = db.prepare(`
                INSERT OR REPLACE INTO CANDLES (
                    instrument, timeframe, timestamp, open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `);
            for (const candle of validCandles) {
                stmt.run([candle.instrument, candle.timeframe, candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume]);
            }
            stmt.finalize((err) => {
                if (err) {
                    db.run('ROLLBACK');
                    reject(err);
                } else {
                    db.run('COMMIT', (err) => {
                        if (err) reject(err);
                        else {
                            const endTime = Date.now();
                            console.log(`Completed cache write of ${validCandles.length} candles for ${instrument} ${timeframe} in ${endTime - startTime} ms`);
                            resolve();
                        }
                    });
                }
            });
        });
    });
}

// Get cached 1m candles for a symbol
async function getCached1mCandles(db, symbol, startMs, endMs) {
    const queryStartTime = Date.now();
    return new Promise((resolve) => {
        console.log(`Querying Cache - ${startMs} to ${endMs}`);
        db.all('SELECT * FROM CANDLES WHERE instrument = ? AND timeframe = "1m" AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC',
            [symbol, startMs, endMs], (err, rows) => {
                const queryEndTime = Date.now();
                if (err) {
                    console.error(`Cache fetch error for ${symbol} 1m:`, err);
                    resolve([]);
                } else {
                    console.log(`Retrieved ${rows.length} candles from cache for ${symbol} 1m in ${queryEndTime - queryStartTime} ms`);
                    resolve(rows.map(row => ({
                        timestamp: row.timestamp,
                        open: row.open,
                        high: row.high,
                        low: row.low,
                        close: row.close,
                        volume: row.volume,
                        instrument: row.instrument,
                        timeframe: row.timeframe,
                        source: 'C',
                        isClosed: true  // Cached 1m candles are always closed
                    })));
                }
            });
    });
}

module.exports = {
    initializeDatabase,
    setupTables,
    getSettings,
    batchInsertCandles,
    getCached1mCandles
}; 