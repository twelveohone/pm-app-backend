#!/usr/bin/env node
/**
 * Import enterprise hardware inventory from .xlsx into hardware_inventory_rows.
 *
 * This is NOT the same as pm_items (PM session checklist lines). The spreadsheet
 * is a flat asset register; the app stores checklist state per session/unit.
 *
 * Usage:
 *   node scripts/import-hardware-inventory.js "D:\path\file.xlsx" --batch tn-2026-04-29
 *   node scripts/import-hardware-inventory.js ".\file.xlsx" --batch tn-v1 --replace
 *   node scripts/import-hardware-inventory.js ".\file.xlsx" --dry-run
 *
 * Requires DATABASE_URL (or DB_* vars) in .env unless --dry-run.
 * After import, optional: --match-sites tries to set site_id where location = sites.station or site_name.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Pool } = require('pg');
const XLSX = require('xlsx');

const DB_SSL = String(process.env.DB_SSL || '').toLowerCase() === 'true';
const DATABASE_URL = process.env.DATABASE_URL || '';

const poolConfig = DATABASE_URL
  ? {
      connectionString: DATABASE_URL,
      ssl: DB_SSL ? { rejectUnauthorized: false } : undefined,
    }
  : {
      user: process.env.DB_USER || 'postgres',
      host: process.env.DB_HOST || 'localhost',
      database: process.env.DB_NAME || 'inventory_dev',
      password: String(process.env.DB_PASSWORD || 'change_me'),
      port: Number(process.env.DB_PORT || 5432),
      ssl: DB_SSL ? { rejectUnauthorized: false } : undefined,
    };

function normCell(v) {
  if (v == null) return '';
  return String(v)
    .replace(/^\uFEFF/, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normHeader(v) {
  return normCell(v).toLowerCase();
}

function findHeaderRow(matrix) {
  for (let i = 0; i < Math.min(matrix.length, 40); i++) {
    const row = matrix[i];
    if (!Array.isArray(row)) continue;
    const cells = row.map(normHeader);
    const joined = cells.join('\t');
    if (joined.includes('asset tag') && (joined.includes('model') || joined.includes('categories'))) {
      return i;
    }
  }
  return -1;
}

function colIndex(headers, ...candidates) {
  for (const c of candidates) {
    const want = c.toLowerCase();
    const j = headers.indexOf(want);
    if (j >= 0) return j;
  }
  return -1;
}

function parseArgs(argv) {
  const out = {
    file: null,
    batch: `import-${new Date().toISOString().slice(0, 10)}`,
    replace: false,
    dryRun: false,
    matchSites: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--batch') out.batch = String(argv[++i] || '').trim() || out.batch;
    else if (a === '--replace') out.replace = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--match-sites') out.matchSites = true;
    else if (!a.startsWith('--') && !out.file) out.file = a;
  }
  return out;
}

function rowIsEmpty(obj) {
  return !Object.values(obj).some((v) => v != null && String(v).trim() !== '');
}

function extractRecords(matrix, headerRowIdx) {
  const headerCells = matrix[headerRowIdx].map((h) => normHeader(h));
  const idx = {
    model_category: colIndex(headerCells, 'model categories', 'model category'),
    model: colIndex(headerCells, 'model'),
    account: colIndex(headerCells, 'account'),
    asset_tag: colIndex(headerCells, 'asset tag'),
    serial_number: colIndex(headerCells, 'serial number'),
    state_name: colIndex(headerCells, 'state'),
    location: colIndex(headerCells, 'location'),
    stockroom: colIndex(headerCells, 'stockroom'),
    company: colIndex(headerCells, 'company'),
    installed: colIndex(headerCells, 'installed'),
    purchase_order: colIndex(headerCells, 'purchase order'),
    warranty_expiration: colIndex(headerCells, 'warranty expiration'),
    owned: colIndex(headerCells, 'owned'),
    contract: colIndex(headerCells, 'contract'),
    sold_product: colIndex(headerCells, 'sold product'),
  };

  if (idx.asset_tag < 0 && idx.serial_number < 0) {
    throw new Error('Could not find Asset Tag / Serial Number columns in header row.');
  }

  const keys = Object.keys(idx).filter((k) => idx[k] >= 0);
  const records = [];

  for (let r = headerRowIdx + 1; r < matrix.length; r++) {
    const line = matrix[r];
    if (!Array.isArray(line)) continue;
    const o = {};
    for (const k of keys) {
      const c = line[idx[k]];
      o[k] = normCell(c) || null;
    }
    if (rowIsEmpty(o)) continue;
    if (!o.model_category && !o.model && !o.asset_tag && !o.serial_number) continue;
    records.push(o);
  }

  return records;
}

async function ensureTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS hardware_inventory_rows (
      id TEXT PRIMARY KEY,
      import_batch TEXT NOT NULL,
      source_file TEXT,
      model_category TEXT,
      model TEXT,
      account TEXT,
      asset_tag TEXT,
      serial_number TEXT,
      state_name TEXT,
      location TEXT,
      stockroom TEXT,
      company TEXT,
      installed TEXT,
      purchase_order TEXT,
      warranty_expiration TEXT,
      owned TEXT,
      contract TEXT,
      sold_product TEXT,
      site_id TEXT REFERENCES sites(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    ALTER TABLE hardware_inventory_rows
      ADD COLUMN IF NOT EXISTS pm_cleaned SMALLINT,
      ADD COLUMN IF NOT EXISTS pm_damaged SMALLINT,
      ADD COLUMN IF NOT EXISTS pm_notes TEXT,
      ADD COLUMN IF NOT EXISTS last_pm_sync_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS pm_session_id TEXT
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_hardware_inventory_batch ON hardware_inventory_rows (import_batch)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_hardware_inventory_location ON hardware_inventory_rows (location)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_hardware_inventory_serial ON hardware_inventory_rows (serial_number)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_hardware_inventory_asset_tag ON hardware_inventory_rows (asset_tag)`
  );
}

async function matchSites(pool, batch) {
  const r = await pool.query(
    `UPDATE hardware_inventory_rows r
     SET site_id = s.id
     FROM sites s
     WHERE r.import_batch = $1
       AND r.site_id IS NULL
       AND (
         LOWER(TRIM(REGEXP_REPLACE(COALESCE(r.location, ''), '\\s+', ' ', 'g'))) =
           LOWER(TRIM(REGEXP_REPLACE(COALESCE(s.station, ''), '\\s+', ' ', 'g')))
         OR LOWER(TRIM(REGEXP_REPLACE(COALESCE(r.location, ''), '\\s+', ' ', 'g'))) =
           LOWER(TRIM(REGEXP_REPLACE(COALESCE(s.site_name, ''), '\\s+', ' ', 'g')))
       )`,
    [batch]
  );
  return r.rowCount;
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.file) {
    console.error(
      'Usage: node scripts/import-hardware-inventory.js <file.xlsx> [--batch id] [--replace] [--match-sites] [--dry-run]'
    );
    process.exit(1);
  }
  const abs = path.isAbsolute(args.file) ? args.file : path.resolve(process.cwd(), args.file);
  if (!fs.existsSync(abs)) {
    console.error('File not found:', abs);
    process.exit(1);
  }

  const wb = XLSX.readFile(abs, { cellDates: true });
  const sheetName = wb.SheetNames[0];
  const sheet = wb.Sheets[sheetName];
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '', raw: false });
  const headerIdx = findHeaderRow(matrix);
  if (headerIdx < 0) {
    console.error('Could not locate header row (expected columns like Asset Tag, Model).');
    process.exit(1);
  }

  const records = extractRecords(matrix, headerIdx);
  console.log(`Sheet: ${sheetName}; data rows: ${records.length}`);

  if (args.dryRun) {
    console.log(JSON.stringify(records.slice(0, 3), null, 2));
    console.log('Dry run — no database writes.');
    process.exit(0);
  }

  if (!DATABASE_URL && !process.env.DB_HOST) {
    console.error('Set DATABASE_URL (or DB_HOST/DB_NAME/...) in .env to import.');
    process.exit(1);
  }

  const pool = new Pool(poolConfig);
  try {
    await ensureTable(pool);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      if (args.replace) {
        const del = await client.query(
          `DELETE FROM hardware_inventory_rows WHERE import_batch = $1`,
          [args.batch]
        );
        console.log(`Deleted ${del.rowCount} existing row(s) for batch ${args.batch}.`);
      }

      const sourceFile = path.basename(abs);
      let n = 0;
      for (const rec of records) {
        await client.query(
          `INSERT INTO hardware_inventory_rows (
            id, import_batch, source_file,
            model_category, model, account, asset_tag, serial_number,
            state_name, location, stockroom, company,
            installed, purchase_order, warranty_expiration, owned, contract, sold_product
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
          [
            crypto.randomUUID(),
            args.batch,
            sourceFile,
            rec.model_category,
            rec.model,
            rec.account,
            rec.asset_tag,
            rec.serial_number,
            rec.state_name,
            rec.location,
            rec.stockroom,
            rec.company,
            rec.installed,
            rec.purchase_order,
            rec.warranty_expiration,
            rec.owned,
            rec.contract,
            rec.sold_product,
          ]
        );
        n++;
      }
      await client.query('COMMIT');
      console.log(`Inserted ${n} row(s) into hardware_inventory_rows (batch: ${args.batch}).`);

      if (args.matchSites) {
        const updated = await matchSites(pool, args.batch);
        console.log(`Matched site_id on ${updated} row(s) (exact station or site_name = location).`);
      }
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
