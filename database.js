import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

export async function initDb() {
  const db = await open({
    filename: './database.sqlite',
    driver: sqlite3.Database
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS transactions (
      id TEXT PRIMARY KEY,
      clientId TEXT,
      amount REAL,
      city TEXT,
      timestamp INTEGER,
      fraudType TEXT,
      isFraud INTEGER
    )
  `);

  return db;
}
