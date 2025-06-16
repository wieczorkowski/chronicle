# Chronicle

Chronicle is a back-end data engine written in JavaScript using Node.js that connects to market data providers (currently Databento), retrieves financial market data, and serves that data to charting and trading clients in OHLCV format via WebSocket connections in JSON format, aggregated across multiple timeframes.

## Features

- **Real-time Market Data**: Connects to Databento for live market data feeds
- **Multi-timeframe Support**: Aggregates data across multiple timeframes (1m, 5m, 15m, etc.)
- **WebSocket API**: Real-time data streaming to connected clients
- **SQLite Database**: Persistent storage for market data, settings, and annotations
- **Replay Functionality**: Historical data playback capabilities
- **Strategy Management**: Support for trading strategy registration and monitoring
- **Annotation System**: Chart annotation and markup support
- **Data Caching**: Efficient data caching and retrieval system

## Architecture

```
chronicle/
├── chronicle.js              # Main WebSocket server
├── lib/                      # Core modules
│   ├── utils.js             # Utility functions
│   ├── db.js                # Database initialization and setup
│   ├── data-fetcher.js      # Market data fetching logic
│   ├── candle-processor.js  # OHLCV candle processing
│   └── handlers/            # Request handlers
│       ├── data-handlers.js      # Live data management
│       ├── replay-handlers.js    # Historical data replay
│       ├── settings-handlers.js  # User settings management
│       ├── cache-handlers.js     # Data caching operations
│       ├── annotation-handlers.js # Chart annotations
│       └── strategy-handlers.js  # Trading strategy management
├── logs/                    # Application logs
└── chronicle-admin.html     # Web-based administration interface
```

## Prerequisites

- Node.js (v16 or higher)
- npm (comes with Node.js)
- Databento API account and credentials (for live data)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/chronicle.git
   cd chronicle
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Set up environment variables:**
   Create a `.env` file in the root directory (optional, for Databento credentials):
   ```
   DATABENTO_API_KEY=your_databento_api_key_here
   DATABENTO_USERNAME=your_databento_username
   DATABENTO_PASSWORD=your_databento_password
   ```

## Quick Start

1. **Start the Chronicle server:**
   ```bash
   node chronicle.js
   ```
   
   Or use the included batch file (Windows):
   ```bash
   launch_chronicle.bat
   ```

2. **Verify the server is running:**
   - WebSocket server will be available at `ws://localhost:8080`
   - Console will display: "WebSocket server running on ws://localhost:8080"

3. **Access the admin interface:**
   - Open `chronicle-admin.html` in your web browser
   - This provides a web-based interface for testing and administration

## WebSocket API

Chronicle communicates with clients via WebSocket using JSON messages. Here are the main message types:

### Connection
```javascript
// Connect to WebSocket
const ws = new WebSocket('ws://localhost:8080');
```

### Request Format
```javascript
{
  "action": "action_name",
  "parameter1": "value1",
  "parameter2": "value2"
}
```

### Key Actions

#### Get Market Data
```javascript
{
  "action": "get_data",
  "instrument": "AAPL",
  "timeframe": "1m",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31"
}
```

#### Subscribe to Live Data
```javascript
{
  "action": "add_timeframe",
  "instrument": "AAPL",
  "timeframe": "1m"
}
```

#### Get Historical Replay
```javascript
{
  "action": "get_replay",
  "instrument": "AAPL",
  "timeframe": "1m",
  "start_date": "2024-01-01",
  "speed": 1.0
}
```

### Response Format
```javascript
{
  "mtyp": "response_type",
  "data": { /* response data */ },
  "message": "status message"
}
```

## Configuration

The server uses several configuration options that can be modified in `chronicle.js`:

- **WS_PORT**: WebSocket server port (default: 8080)
- **DB_PATH**: SQLite database file path (default: ./chronicle_data.db)
- **LOG_DIR**: Log files directory (default: ./logs)

## Database

Chronicle uses SQLite for data persistence. The database is automatically created on first run and includes tables for:

- Market data (OHLCV candles)
- User settings and preferences
- Chart annotations
- Trading strategies
- Cache management

## Development

### Project Structure

- **Main Server**: `chronicle.js` - WebSocket server and main application entry point
- **Core Libraries**: `lib/` directory contains modular components
- **Handlers**: `lib/handlers/` contains request-specific logic
- **Database**: SQLite database with automatic schema creation
- **Logging**: Structured logging to `logs/` directory

### Adding New Features

1. Create handler functions in appropriate `lib/handlers/` files
2. Add new actions to the switch statement in `chronicle.js`
3. Update this README with new API endpoints

## Troubleshooting

### Common Issues

1. **Port 8080 already in use:**
   - Change the `WS_PORT` constant in `chronicle.js`
   - Or kill the process using port 8080

2. **Database errors:**
   - Delete `chronicle_data.db` to reset the database
   - Check file permissions in the project directory

3. **Market data connection issues:**
   - Verify Databento credentials are correct
   - Check network connectivity
   - Review logs in the `logs/` directory

### Logs

Application logs are stored in the `logs/` directory and include:
- Connection events
- Data retrieval operations
- Error messages
- Client interactions

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the ISC License - see the package.json file for details.

## Version History

- **v0.2.2**: Added Replay Functionality
- **v0.2.1**: Enhanced WebSocket handling and stability improvements
- **v0.2.0**: Multi-timeframe support and strategy management
- **v0.1.0**: Initial release with basic WebSocket server and Databento integration

## Support

For questions, issues, or feature requests, please open an issue on GitHub. 