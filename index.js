const Client = require('ssh2-sftp-client');
const cron = require('node-cron');
const fs = require('fs');
const { Parser } = require('json2csv');
const sqlite3 = require('sqlite3').verbose();
require('dotenv').config();

const sftp = new Client();
const db = new sqlite3.Database('./data.db');

// Initialize database
db.serialize(() => {
  db.run("CREATE TABLE IF NOT EXISTS imported_data (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");
});

async function importJsonFromSftp() {
  try {
    await sftp.connect({
      host: process.env.SFTP_HOST,
      port: process.env.SFTP_PORT,
      username: process.env.SFTP_USERNAME,
      privateKey: fs.readFileSync(process.env.SSH_KEY_PATH)
    });

    console.log('Connected to SFTP server');

    const data = await sftp.get(process.env.REMOTE_JSON_PATH);
    const jsonData = data.toString();

    // Write to database
    db.run("INSERT INTO imported_data (data) VALUES (?)", [jsonData], function(err) {
      if (err) {
        return console.error('Error inserting into database:', err.message);
      }
      console.log(`JSON data inserted into database with ID ${this.lastID}`);
    });

    console.log('JSON file imported successfully');
  } catch (err) {
    console.error('Error importing JSON:', err);
  } finally {
    sftp.end();
  }
}

function exportAsJson() {
  db.get("SELECT data FROM imported_data ORDER BY timestamp DESC LIMIT 1", (err, row) => {
    if (err) {
      return console.error('Error retrieving data from database:', err.message);
    }
    if (row) {
      const exportPath = './export.json';
      fs.writeFileSync(exportPath, row.data);
      console.log(`Data exported as JSON to ${exportPath}`);
    } else {
      console.log('No data found in the database to export');
    }
  });
}

function exportAsCsv() {
  db.get("SELECT data FROM imported_data ORDER BY timestamp DESC LIMIT 1", (err, row) => {
    if (err) {
      return console.error('Error retrieving data from database:', err.message);
    }
    if (row) {
      const data = JSON.parse(row.data);
      const parser = new Parser();
      const csv = parser.parse(data);
      const exportPath = './export.csv';
      fs.writeFileSync(exportPath, csv);
      console.log(`Data exported as CSV to ${exportPath}`);
    } else {
      console.log('No data found in the database to export');
    }
  });
}

// Schedule import every 15 minutes
cron.schedule('*/15 * * * *', () => {
  console.log('Importing JSON from SFTP...');
  importJsonFromSftp();
});

// Initial import
importJsonFromSftp();

// Example usage of export functions
setTimeout(() => {
  exportAsJson();
  exportAsCsv();
}, 5000);

console.log('SFTP JSON Import and Export Tool is running...');

// Graceful shutdown
process.on('SIGINT', () => {
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Database connection closed');
    process.exit(0);
  });
});