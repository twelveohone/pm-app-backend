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
 * On Render, prefer uploading the same .xlsx via Admin → Inventory → Hardware inventory (.xlsx)
 * so the import uses the server DATABASE_URL without copying secrets locally.
 *
 * Requires DATABASE_URL (or DB_* vars) in .env unless --dry-run.
 * After import, optional: --match-sites tries to set site_id where location = sites.station or site_name.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { parseXlsxBuffer, importRecords } = require('../lib/hardwareInventoryImport');

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

  const buf = fs.readFileSync(abs);
  const { sheetName, records } = parseXlsxBuffer(buf, path.basename(abs));
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
    const sourceFile = path.basename(abs);
    const result = await importRecords(pool, {
      records,
      batch: args.batch,
      sourceFile,
      replace: args.replace,
      matchSites: args.matchSites,
    });
    if (args.replace) {
      console.log(`Deleted ${result.deleted} existing row(s) for batch ${args.batch}.`);
    }
    console.log(`Inserted ${result.inserted} row(s) into hardware_inventory_rows (batch: ${args.batch}).`);

    if (args.matchSites) {
      console.log(
        `Matched site_id on ${result.matchedSites} row(s) (exact station or site_name = location).`
      );
    }
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
