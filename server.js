const path = require('path');
const express = require('express');
const multer = require('multer');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const {
  parseXlsxBuffer,
  importRecords: importHardwareInventoryRows,
} = require('./lib/hardwareInventoryImport');

const hardwareInventoryUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 45 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const name = (file.originalname || '').toLowerCase();
    if (name.endsWith('.xlsx')) return cb(null, true);
    const okMime =
      file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      file.mimetype === 'application/octet-stream';
    if (okMime) return cb(null, true);
    cb(new Error('Upload an Excel .xlsx file'));
  },
});

const app = express();
const PORT = Number(process.env.PORT || 3000);
const JWT_SECRET = process.env.JWT_SECRET || '';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '12h';
const SALT_ROUNDS = Number(process.env.PASSWORD_SALT_ROUNDS || 10);
const REQUIRE_AUTH = String(process.env.REQUIRE_AUTH || 'false').toLowerCase() === 'true';
const ALLOW_SELF_SIGNUP = String(process.env.ALLOW_SELF_SIGNUP || 'true').toLowerCase() === 'true';

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

function attachUserFromToken(req) {
  const raw = req.headers.authorization || '';
  const [scheme, token] = raw.split(' ');
  if (scheme !== 'Bearer' || !token || !JWT_SECRET) return;
  try {
    req.user = jwt.verify(token, JWT_SECRET);
  } catch {
    /* invalid token */
  }
}

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
const pool = new Pool(poolConfig);

const PM_DEVICE_TYPES = new Set([
  'printer',
  'camera_tower',
  'scanner',
  'signature_pad',
  'ups',
  'handheld_scanner',
  'kiosk',
  'agent_facing_monitor',
  'passport_scanner',
  'card_scanner',
  'document_scanner',
  'customer_monitor',
  'vision_tester',
  'topaz',
  'testing_station',
  'payment_device',
]);

/** Default TN + MA configs (matches PM-App-Expanded config/states.ts). */
const SEED_PM_STATE_CONFIGS = [
  {
    code: 'TN',
    name: 'Tennessee',
    pod_devices: [
      'camera_tower',
      'signature_pad',
      'scanner',
      'handheld_scanner',
      'ups',
      'printer',
    ],
    kiosk_devices: ['kiosk', 'handheld_scanner', 'ups'],
    models: {
      printer: 'HP Color Laserjet Pro 4201DW',
      camera_tower: 'Canon T7 Camera Tower',
      scanner: 'Canon ImageFormula DR-C240',
      signature_pad: 'Verifone M400',
      ups: 'APC Model BR1000MB',
      handheld_scanner: 'Zebra Model DS8108',
      kiosk: '',
      agent_facing_monitor: '',
      passport_scanner: '',
      card_scanner: '',
      document_scanner: '',
      customer_monitor: '',
      vision_tester: '',
      topaz: '',
      testing_station: '',
      payment_device: '',
    },
  },
  {
    code: 'MA',
    name: 'Massachusetts',
    pod_devices: [
      'agent_facing_monitor',
      'passport_scanner',
      'card_scanner',
      'payment_device',
      'document_scanner',
      'customer_monitor',
      'camera_tower',
      'vision_tester',
      'signature_pad',
      'printer',
    ],
    kiosk_devices: ['testing_station'],
    models: {
      printer: 'Lexmark MS631 Printer',
      camera_tower: 'T7 Camera Tower',
      scanner: '',
      signature_pad: 'Topaz',
      ups: '',
      handheld_scanner: '',
      kiosk: '',
      agent_facing_monitor: 'Agent Facing Monitor',
      passport_scanner: 'B5000',
      card_scanner: 'M500',
      document_scanner: 'Ricoh 8170',
      customer_monitor: 'Customer Monitor',
      vision_tester: 'Vision Tester',
      topaz: '',
      testing_station: 'ATS',
      payment_device: 'Verifone M400',
    },
  },
];

function coerceDeviceList(value) {
  if (Array.isArray(value)) {
    return value.map((x) => String(x).trim()).filter((x) => PM_DEVICE_TYPES.has(x));
  }
  if (typeof value === 'string') {
    const out = [];
    const seen = new Set();
    for (const line of value.split(/\r?\n/)) {
      for (const part of line.split(',')) {
        const d = part.trim();
        if (!d || !PM_DEVICE_TYPES.has(d) || seen.has(d)) continue;
        seen.add(d);
        out.push(d);
      }
    }
    return out;
  }
  return [];
}

function coerceModels(raw) {
  const out = {};
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    for (const [k, v] of Object.entries(raw)) {
      if (PM_DEVICE_TYPES.has(k)) out[k] = String(v ?? '');
    }
  }
  return out;
}

async function ensurePmStateConfigs() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pm_state_configs (
      code TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      pod_devices JSONB NOT NULL DEFAULT '[]'::jsonb,
      kiosk_devices JSONB NOT NULL DEFAULT '[]'::jsonb,
      models JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  const cnt = await pool.query(`SELECT COUNT(*)::int AS c FROM pm_state_configs`);
  if (Number(cnt.rows[0].c) > 0) return;
  for (const row of SEED_PM_STATE_CONFIGS) {
    await pool.query(
      `INSERT INTO pm_state_configs (code, name, pod_devices, kiosk_devices, models, updated_at)
       VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, NOW())`,
      [
        row.code,
        row.name,
        JSON.stringify(row.pod_devices),
        JSON.stringify(row.kiosk_devices),
        JSON.stringify(row.models),
      ]
    );
  }
  console.log('Seeded pm_state_configs (TN, MA)');
}

async function ensureHardwareInventoryTable() {
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

/**
 * Push PM item fields into enterprise hardware_inventory_rows when serial or asset tag matches.
 * Matches against either serial_number or asset_tag on the inventory row (handles legacy mix-ups).
 * @param {import('pg').Pool | import('pg').PoolClient} q
 */
async function syncHardwareInventoryFromPmItem(q, item, sessionId) {
  const serial =
    item.serial != null && String(item.serial).trim() !== '' ? String(item.serial).trim() : null;
  const assetTag =
    item.asset_tag != null && String(item.asset_tag).trim() !== ''
      ? String(item.asset_tag).trim()
      : null;
  if (!serial && !assetTag) return;

  const cleaned = Number(item.cleaned) ? 1 : 0;
  const damaged = Number(item.damaged) ? 1 : 0;
  const notes = item.notes != null ? String(item.notes) : null;
  const sid = sessionId != null ? String(sessionId) : item.session_id != null ? String(item.session_id) : null;

  await q.query(
    `UPDATE hardware_inventory_rows
     SET serial_number = CASE
           WHEN $1::text IS NOT NULL AND BTRIM($1::text) <> '' THEN BTRIM($1::text)
           ELSE serial_number
         END,
         asset_tag = CASE
           WHEN $2::text IS NOT NULL AND BTRIM($2::text) <> '' THEN BTRIM($2::text)
           ELSE asset_tag
         END,
         pm_cleaned = $3::smallint,
         pm_damaged = $4::smallint,
         pm_notes = $5,
         last_pm_sync_at = NOW(),
         pm_session_id = $6
     WHERE (
       ($1::text IS NOT NULL AND BTRIM($1::text) <> '' AND (
         LOWER(BTRIM(COALESCE(serial_number, ''))) = LOWER(BTRIM($1::text))
         OR LOWER(BTRIM(COALESCE(asset_tag, ''))) = LOWER(BTRIM($1::text))
       ))
       OR
       ($2::text IS NOT NULL AND BTRIM($2::text) <> '' AND (
         LOWER(BTRIM(COALESCE(serial_number, ''))) = LOWER(BTRIM($2::text))
         OR LOWER(BTRIM(COALESCE(asset_tag, ''))) = LOWER(BTRIM($2::text))
       ))
     )`,
    [serial, assetTag, cleaned, damaged, notes, sid]
  );
}

async function ensureSitesTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sites (
      id TEXT PRIMARY KEY,
      station TEXT,
      site_name TEXT NOT NULL,
      address TEXT,
      city TEXT,
      zip TEXT,
      county TEXT,
      region TEXT,
      primary_phone TEXT,
      alt_phones TEXT,
      hours TEXT,
      state TEXT,
      workstations INTEGER NOT NULL DEFAULT 0,
      kiosks INTEGER NOT NULL DEFAULT 0,
      first_pod_system TEXT,
      first_kiosk_system TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
  `);
}

/**
 * PM session + line-item tables (mobile sync, import-session, admin inventory).
 * Render / fresh Postgres DBs only had `sites` via ensureSitesTable — without these,
 * admin inventory and PM APIs fail with "relation pm_items does not exist".
 */
async function ensurePmSessionsAndItemsTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pm_sessions (
      id TEXT PRIMARY KEY NOT NULL,
      site_id TEXT NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
      technician_name TEXT NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      pods_count INTEGER NOT NULL DEFAULT 0,
      kiosks_count INTEGER NOT NULL DEFAULT 0,
      first_pod_system TEXT,
      first_kiosk_system TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pm_items (
      id TEXT PRIMARY KEY NOT NULL,
      session_id TEXT NOT NULL REFERENCES pm_sessions(id) ON DELETE CASCADE,
      unit_type TEXT NOT NULL,
      unit_index INTEGER NOT NULL,
      device_type TEXT NOT NULL,
      cleaned SMALLINT NOT NULL DEFAULT 0,
      damaged SMALLINT NOT NULL DEFAULT 0,
      notes TEXT,
      serial TEXT,
      asset_tag TEXT,
      printer_master_pod SMALLINT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_pm_items_session_unit ON pm_items (session_id, unit_type, unit_index)`
  );
}

/**
 * Upsert a site row (used by PUT /sites and import-session).
 * @param {import('pg').PoolClient | import('pg').Pool} q
 */
async function upsertSiteRow(q, site) {
  const id = String(site?.id || '').trim();
  const site_name = String(site?.site_name || '').trim();
  if (!id || !site_name) return;

  const station = site.station != null && site.station !== '' ? String(site.station) : null;
  const address = site.address != null && site.address !== '' ? String(site.address) : null;
  const city = site.city != null && site.city !== '' ? String(site.city) : null;
  const zip = site.zip != null && site.zip !== '' ? String(site.zip) : null;
  const county = site.county != null && site.county !== '' ? String(site.county) : null;
  const region = site.region != null && site.region !== '' ? String(site.region) : null;
  const primary_phone = site.primary_phone != null && site.primary_phone !== '' ? String(site.primary_phone) : null;
  const alt_phones = site.alt_phones != null && site.alt_phones !== '' ? String(site.alt_phones) : null;
  const hours = site.hours != null && site.hours !== '' ? String(site.hours) : null;
  const state = site.state != null && site.state !== '' ? String(site.state) : null;
  const workstations = Math.max(0, Number(site.workstations ?? 0));
  const kiosks = Math.max(0, Number(site.kiosks ?? 0));
  const first_pod_system =
    site.first_pod_system != null && site.first_pod_system !== '' ? String(site.first_pod_system) : null;
  const first_kiosk_system =
    site.first_kiosk_system != null && site.first_kiosk_system !== '' ? String(site.first_kiosk_system) : null;

  const created_at = site.created_at ? new Date(site.created_at) : new Date();
  const updated_at = site.updated_at ? new Date(site.updated_at) : new Date();

  await q.query(
    `INSERT INTO sites (
      id, station, site_name, address, city, zip, county, region, primary_phone, alt_phones, hours,
      state, workstations, kiosks, first_pod_system, first_kiosk_system, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
    ON CONFLICT (id) DO UPDATE SET
      station = EXCLUDED.station,
      site_name = EXCLUDED.site_name,
      address = EXCLUDED.address,
      city = EXCLUDED.city,
      zip = EXCLUDED.zip,
      county = EXCLUDED.county,
      region = EXCLUDED.region,
      primary_phone = EXCLUDED.primary_phone,
      alt_phones = EXCLUDED.alt_phones,
      hours = EXCLUDED.hours,
      state = EXCLUDED.state,
      workstations = EXCLUDED.workstations,
      kiosks = EXCLUDED.kiosks,
      first_pod_system = EXCLUDED.first_pod_system,
      first_kiosk_system = EXCLUDED.first_kiosk_system,
      updated_at = EXCLUDED.updated_at`,
    [
      id,
      station,
      site_name,
      address,
      city,
      zip,
      county,
      region,
      primary_phone,
      alt_phones,
      hours,
      state,
      workstations,
      kiosks,
      first_pod_system,
      first_kiosk_system,
      created_at,
      updated_at,
    ]
  );
}

function authRequired(req, res, next) {
  attachUserFromToken(req);
  if (!REQUIRE_AUTH) return next();
  if (!req.user) {
    return res.status(401).json({ error: 'Missing or invalid authorization header' });
  }
  if (!JWT_SECRET) {
    return res.status(500).json({ error: 'JWT secret is not configured on server' });
  }
  return next();
}

function adminRequired(req, res, next) {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ error: 'Admin permission required' });
  }
  return next();
}

async function ensureAuthTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY,
      full_name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'tech',
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`);

  const adminEmail = (process.env.ADMIN_EMAIL || '').trim().toLowerCase();
  const adminPassword = process.env.ADMIN_PASSWORD || '';
  const adminName = (process.env.ADMIN_NAME || 'Admin User').trim();
  const resetOnBoot =
    String(process.env.ADMIN_RESET_PASSWORD_ON_BOOT || '').toLowerCase() === 'true';

  if (!adminEmail || !adminPassword) {
    console.log('ADMIN_EMAIL/ADMIN_PASSWORD not set; skipping admin bootstrap');
    return;
  }

  if (resetOnBoot) {
    const passwordHash = await bcrypt.hash(adminPassword, SALT_ROUNDS);
    const updated = await pool.query(
      `UPDATE users
       SET password_hash = $1, full_name = $2, role = 'admin', is_active = TRUE, updated_at = NOW()
       WHERE email = $3`,
      [passwordHash, adminName, adminEmail]
    );
    if (updated.rowCount === 0) {
      await pool.query(
        `INSERT INTO users (id, full_name, email, password_hash, role, is_active)
         VALUES (gen_random_uuid(), $1, $2, $3, 'admin', TRUE)`,
        [adminName, adminEmail, passwordHash]
      );
      console.log(`Seeded admin user (reset mode): ${adminEmail}`);
    } else {
      console.log(`Admin password updated from env (ADMIN_RESET_PASSWORD_ON_BOOT): ${adminEmail}`);
    }
    return;
  }

  const existing = await pool.query(`SELECT id FROM users WHERE email = $1 LIMIT 1`, [adminEmail]);
  if (existing.rows.length > 0) {
    console.log(`Admin user already exists, skipping seed: ${adminEmail}`);
    return;
  }

  const passwordHash = await bcrypt.hash(adminPassword, SALT_ROUNDS);
  await pool.query(
    `INSERT INTO users (id, full_name, email, password_hash, role, is_active)
     VALUES (gen_random_uuid(), $1, $2, $3, 'admin', TRUE)`,
    [adminName, adminEmail, passwordHash]
  );
  console.log(`Seeded admin user: ${adminEmail}`);
}

app.get('/', (req, res) => {
  res.send('Backend is running');
});

app.get('/test-db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Database connection error');
  }
});

app.post('/auth/login', async (req, res) => {
  const email = String(req.body?.email || '')
    .trim()
    .toLowerCase();
  const password = String(req.body?.password || '');

  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password are required' });
  }
  if (!JWT_SECRET) {
    return res.status(500).json({ error: 'JWT secret is not configured on server' });
  }

  try {
    const result = await pool.query(
      `SELECT id, full_name, email, password_hash, role, is_active
       FROM users
       WHERE email = $1
       LIMIT 1`,
      [email]
    );
    const user = result.rows[0];
    if (!user || !user.is_active) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign(
      { sub: user.id, email: user.email, role: user.role, full_name: user.full_name },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    return res.json({
      token,
      user: {
        id: user.id,
        full_name: user.full_name,
        email: user.email,
        role: user.role,
      },
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Login failed' });
  }
});

app.post('/auth/register', async (req, res) => {
  if (!ALLOW_SELF_SIGNUP) {
    return res.status(403).json({ error: 'Registration is disabled. Ask an admin for an account.' });
  }
  if (!JWT_SECRET) {
    return res.status(500).json({ error: 'JWT secret is not configured on server' });
  }

  const fullName = String(req.body?.full_name || '').trim();
  const email = String(req.body?.email || '')
    .trim()
    .toLowerCase();
  const password = String(req.body?.password || '');

  if (!fullName || !email || !password) {
    return res.status(400).json({ error: 'full_name, email, and password are required' });
  }
  if (password.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters' });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  try {
    const passwordHash = await bcrypt.hash(password, SALT_ROUNDS);
    const result = await pool.query(
      `INSERT INTO users (id, full_name, email, password_hash, role, is_active, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, $2, $3, 'tech', TRUE, NOW(), NOW())
       RETURNING id, full_name, email, role`,
      [fullName, email, passwordHash]
    );
    const user = result.rows[0];
    const token = jwt.sign(
      { sub: user.id, email: user.email, role: user.role, full_name: user.full_name },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    return res.status(201).json({
      token,
      user: {
        id: user.id,
        full_name: user.full_name,
        email: user.email,
        role: user.role,
      },
    });
  } catch (err) {
    if (String(err?.code) === '23505') {
      return res.status(409).json({ error: 'An account with this email already exists' });
    }
    console.error(err);
    return res.status(500).json({ error: 'Registration failed' });
  }
});

app.get('/auth/me', authRequired, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, full_name, email, role, is_active
       FROM users
       WHERE id = $1
       LIMIT 1`,
      [req.user.sub]
    );
    const user = result.rows[0];
    if (!user || !user.is_active) {
      return res.status(404).json({ error: 'User not found' });
    }
    return res.json(user);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to fetch user profile' });
  }
});

app.post('/auth/users', authRequired, adminRequired, async (req, res) => {
  const fullName = String(req.body?.full_name || '').trim();
  const email = String(req.body?.email || '')
    .trim()
    .toLowerCase();
  const password = String(req.body?.password || '');
  const role = String(req.body?.role || 'tech').trim().toLowerCase();

  if (!fullName || !email || !password) {
    return res.status(400).json({ error: 'full_name, email, and password are required' });
  }
  if (!['tech', 'manager', 'admin'].includes(role)) {
    return res.status(400).json({ error: 'role must be tech, manager, or admin' });
  }

  try {
    const passwordHash = await bcrypt.hash(password, SALT_ROUNDS);
    const result = await pool.query(
      `INSERT INTO users (id, full_name, email, password_hash, role, is_active, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, $2, $3, $4, TRUE, NOW(), NOW())
       RETURNING id, full_name, email, role, is_active, created_at`,
      [fullName, email, passwordHash, role]
    );
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    if (String(err?.code) === '23505') {
      return res.status(409).json({ error: 'Email already exists' });
    }
    console.error(err);
    return res.status(500).json({ error: 'Failed to create user' });
  }
});

app.get('/auth/users', authRequired, adminRequired, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, full_name, email, role, is_active, created_at
       FROM users
       ORDER BY created_at DESC`
    );
    return res.json(result.rows);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to list users' });
  }
});

app.patch('/auth/users/:id', authRequired, adminRequired, async (req, res) => {
  const id = String(req.params.id || '').trim();
  const roleIn = req.body?.role;
  const isActiveIn = req.body?.is_active;
  const password =
    req.body?.password !== undefined && req.body?.password !== null
      ? String(req.body.password)
      : undefined;

  if (!id) {
    return res.status(400).json({ error: 'User id required' });
  }
  if (roleIn === undefined && isActiveIn === undefined && password === undefined) {
    return res.status(400).json({ error: 'Nothing to update' });
  }
  if (isActiveIn === false && id === req.user.sub) {
    return res.status(400).json({ error: 'You cannot deactivate your own account' });
  }

  const sets = [];
  const values = [];
  let i = 1;

  if (roleIn !== undefined) {
    const role = String(roleIn).trim().toLowerCase();
    if (!['tech', 'manager', 'admin'].includes(role)) {
      return res.status(400).json({ error: 'role must be tech, manager, or admin' });
    }
    sets.push(`role = $${i++}`);
    values.push(role);
  }
  if (isActiveIn !== undefined) {
    sets.push(`is_active = $${i++}`);
    values.push(Boolean(isActiveIn));
  }
  if (password !== undefined) {
    if (password.length < 8) {
      return res.status(400).json({ error: 'Password must be at least 8 characters' });
    }
    sets.push(`password_hash = $${i++}`);
    values.push(await bcrypt.hash(password, SALT_ROUNDS));
  }

  sets.push('updated_at = NOW()');
  values.push(id);

  try {
    const sql = `UPDATE users SET ${sets.join(', ')} WHERE id = $${i} RETURNING id, full_name, email, role, is_active, created_at`;
    const result = await pool.query(sql, values);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    return res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to update user' });
  }
});

app.delete('/auth/users/:id', authRequired, adminRequired, async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) {
    return res.status(400).json({ error: 'User id required' });
  }
  if (id === req.user.sub) {
    return res.status(400).json({ error: 'You cannot delete your own account' });
  }
  try {
    const result = await pool.query(`DELETE FROM users WHERE id = $1 RETURNING id`, [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to delete user' });
  }
});

async function adminCountTable(tableKey) {
  const allowed = {
    users: 'users',
    sites: 'sites',
    pm_sessions: 'pm_sessions',
    pm_items: 'pm_items',
    hardware_inventory_rows: 'hardware_inventory_rows',
  };
  const table = allowed[tableKey];
  if (!table) return null;
  try {
    const r = await pool.query(`SELECT COUNT(*)::int AS c FROM ${table}`);
    return r.rows[0].c;
  } catch {
    return null;
  }
}

app.get('/auth/admin/stats', authRequired, adminRequired, async (req, res) => {
  try {
    const [users, sites, pm_sessions, pm_items, hardware_inventory] = await Promise.all([
      adminCountTable('users'),
      adminCountTable('sites'),
      adminCountTable('pm_sessions'),
      adminCountTable('pm_items'),
      adminCountTable('hardware_inventory_rows'),
    ]);
    return res.json({ users, sites, pm_sessions, pm_items, hardware_inventory });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to load stats' });
  }
});

app.get('/auth/admin/hardware-inventory-summary', authRequired, adminRequired, async (req, res) => {
  try {
    const total = await pool.query(`SELECT COUNT(*)::int AS c FROM hardware_inventory_rows`);
    const batches = await pool.query(
      `SELECT import_batch AS batch, COUNT(*)::int AS count
       FROM hardware_inventory_rows
       GROUP BY import_batch
       ORDER BY MAX(created_at) DESC
       LIMIT 50`
    );
    return res.json({ total: total.rows[0].c, batches: batches.rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to load hardware inventory summary' });
  }
});

const ADMIN_INVENTORY_ORDER_BY = `ORDER BY ps.started_at DESC NULLS LAST,
  COALESCE(s.site_name, '') ASC,
  CASE pi.unit_type WHEN 'pod' THEN 1 WHEN 'kiosk' THEN 2 ELSE 3 END,
  pi.unit_index ASC,
  CASE pi.device_type
    WHEN 'camera_tower' THEN 1
    WHEN 'signature_pad' THEN 2
    WHEN 'printer' THEN 3
    WHEN 'scanner' THEN 4
    WHEN 'ups' THEN 5
    WHEN 'handheld_scanner' THEN 6
    WHEN 'testing_station' THEN 7
    ELSE 99
  END, pi.device_type`;

async function queryAdminInventory(poolConn, stateCodeRaw) {
  const stateCode = String(stateCodeRaw || '')
    .trim()
    .toUpperCase();
  if (!stateCode || !/^[A-Z0-9]{2,10}$/.test(stateCode)) {
    const err = new Error('INVALID_STATE');
    err.code = 'INVALID_STATE';
    throw err;
  }
  const result = await poolConn.query(
    `SELECT
      pi.id AS item_id,
      pi.session_id,
      pi.unit_type,
      pi.unit_index,
      pi.device_type,
      pi.cleaned,
      pi.damaged,
      pi.notes,
      pi.serial,
      pi.asset_tag,
      pi.printer_master_pod,
      pi.updated_at AS item_updated_at,
      ps.technician_name,
      ps.started_at AS session_started_at,
      COALESCE(s.site_name, '(unknown site)') AS site_name,
      COALESCE(NULLIF(TRIM(s.state), ''), 'TN') AS resolved_site_state
    FROM pm_items pi
    INNER JOIN pm_sessions ps ON pi.session_id = ps.id
    LEFT JOIN sites s ON ps.site_id = s.id
    WHERE UPPER(COALESCE(NULLIF(TRIM(s.state), ''), 'TN')) = $1
    ${ADMIN_INVENTORY_ORDER_BY}
    LIMIT 100000`,
    [stateCode]
  );
  return { state: stateCode, rows: result.rows };
}

function csvEscapeCell(val) {
  if (val == null) return '';
  const s = String(val);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

app.get('/auth/admin/inventory', authRequired, adminRequired, async (req, res) => {
  try {
    const { state, rows } = await queryAdminInventory(pool, req.query.state);
    return res.json({ state, count: rows.length, rows });
  } catch (err) {
    if (err.code === 'INVALID_STATE') {
      return res.status(400).json({ error: 'state query param is required (2–10 letter or digit code)' });
    }
    console.error(err);
    return res.status(500).json({ error: 'Failed to load inventory' });
  }
});

app.get('/auth/admin/inventory-export', authRequired, adminRequired, async (req, res) => {
  try {
    const { state, rows } = await queryAdminInventory(pool, req.query.state);
    const headers = [
      'site_name',
      'resolved_site_state',
      'session_id',
      'session_started_at',
      'technician_name',
      'unit_type',
      'unit_index',
      'device_type',
      'cleaned',
      'damaged',
      'serial',
      'asset_tag',
      'printer_master_pod',
      'notes',
      'item_updated_at',
      'item_id',
    ];
    const lines = [headers.join(',')];
    for (const row of rows) {
      lines.push(headers.map((h) => csvEscapeCell(row[h])).join(','));
    }
    const body = lines.join('\r\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="inventory-${state}.csv"`);
    res.send(body);
  } catch (err) {
    if (err.code === 'INVALID_STATE') {
      return res.status(400).json({ error: 'state query param is required (2–10 letter or digit code)' });
    }
    console.error(err);
    return res.status(500).json({ error: 'Failed to export inventory' });
  }
});

app.post(
  '/auth/admin/hardware-inventory-import',
  authRequired,
  adminRequired,
  (req, res, next) => {
    hardwareInventoryUpload.single('file')(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message || 'Upload failed' });
      next();
    });
  },
  async (req, res) => {
    try {
      if (!req.file || !req.file.buffer) {
        return res.status(400).json({ error: 'Missing file (form field name: file)' });
      }
      const batch =
        String(req.body.batch || '').trim() ||
        `import-${new Date().toISOString().slice(0, 10)}`;
      const replace =
        req.body.replace === true ||
        req.body.replace === 'true' ||
        req.body.replace === 'on' ||
        req.body.replace === '1';
      const matchSites =
        req.body.matchSites !== false &&
        req.body.matchSites !== 'false' &&
        req.body.matchSites !== '0';

      const { sheetName, records, originalName } = parseXlsxBuffer(
        req.file.buffer,
        req.file.originalname || 'upload.xlsx'
      );
      const sourceFile = path.basename(originalName || req.file.originalname || 'upload.xlsx');

      const result = await importHardwareInventoryRows(pool, {
        records,
        batch,
        sourceFile,
        replace,
        matchSites,
      });

      console.log(
        `[hardware-inventory-import] inserted=${result.inserted} batch=${batch} sheet=${sheetName} file=${sourceFile}`
      );

      return res.json({
        ok: true,
        sheetName,
        batch,
        rowCount: records.length,
        inserted: result.inserted,
        deletedPreviousBatchRows: result.deleted,
        matchedSites: result.matchedSites,
      });
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: err.message || 'Import failed' });
    }
  }
);

app.get('/state-configs', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT code, name, pod_devices, kiosk_devices, models, updated_at
       FROM pm_state_configs
       ORDER BY name ASC`
    );
    return res.json(result.rows);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to load state configs' });
  }
});

app.get('/state-configs/device-types', (req, res) => {
  return res.json([...PM_DEVICE_TYPES].sort());
});

app.post('/state-configs', authRequired, adminRequired, async (req, res) => {
  const code = String(req.body?.code || '')
    .trim()
    .toUpperCase();
  const name = String(req.body?.name || '').trim();
  const pod_devices = coerceDeviceList(req.body?.pod_devices);
  const kiosk_devices = coerceDeviceList(req.body?.kiosk_devices);
  const models = coerceModels(req.body?.models);

  if (!code || !/^[A-Z0-9]{2,10}$/.test(code)) {
    return res.status(400).json({ error: 'code must be 2-10 letters or digits' });
  }
  if (!name) {
    return res.status(400).json({ error: 'name is required' });
  }
  if (pod_devices.length === 0) {
    return res.status(400).json({ error: 'pod_devices must include at least one valid device type' });
  }

  try {
    await pool.query(
      `INSERT INTO pm_state_configs (code, name, pod_devices, kiosk_devices, models, updated_at)
       VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, NOW())`,
      [code, name, JSON.stringify(pod_devices), JSON.stringify(kiosk_devices), JSON.stringify(models)]
    );
    const row = await pool.query(
      `SELECT code, name, pod_devices, kiosk_devices, models, updated_at
       FROM pm_state_configs WHERE code = $1`,
      [code]
    );
    return res.status(201).json(row.rows[0]);
  } catch (err) {
    if (String(err?.code) === '23505') {
      return res.status(409).json({ error: 'State code already exists' });
    }
    console.error(err);
    return res.status(500).json({ error: 'Failed to create state config' });
  }
});

app.put('/state-configs/:code', authRequired, adminRequired, async (req, res) => {
  const code = String(req.params.code || '')
    .trim()
    .toUpperCase();
  const name = String(req.body?.name || '').trim();
  const pod_devices = coerceDeviceList(req.body?.pod_devices);
  const kiosk_devices = coerceDeviceList(req.body?.kiosk_devices);
  const models = coerceModels(req.body?.models);

  if (!code) {
    return res.status(400).json({ error: 'code required' });
  }
  if (!name) {
    return res.status(400).json({ error: 'name is required' });
  }
  if (pod_devices.length === 0) {
    return res.status(400).json({ error: 'pod_devices must include at least one valid device type' });
  }

  try {
    const result = await pool.query(
      `UPDATE pm_state_configs
       SET name = $2,
           pod_devices = $3::jsonb,
           kiosk_devices = $4::jsonb,
           models = $5::jsonb,
           updated_at = NOW()
       WHERE code = $1
       RETURNING code, name, pod_devices, kiosk_devices, models, updated_at`,
      [code, name, JSON.stringify(pod_devices), JSON.stringify(kiosk_devices), JSON.stringify(models)]
    );
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'State not found' });
    }
    return res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to update state config' });
  }
});

app.delete('/state-configs/:code', authRequired, adminRequired, async (req, res) => {
  const code = String(req.params.code || '')
    .trim()
    .toUpperCase();
  if (!code) {
    return res.status(400).json({ error: 'code required' });
  }
  try {
    const n = await pool.query(`SELECT COUNT(*)::int AS c FROM pm_state_configs`);
    if (Number(n.rows[0].c) <= 1) {
      return res.status(400).json({ error: 'Cannot delete the last state configuration' });
    }
    const result = await pool.query(`DELETE FROM pm_state_configs WHERE code = $1 RETURNING code`, [code]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'State not found' });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to delete state config' });
  }
});

app.get('/sites', authRequired, async (req, res) => {
  try {
    const result = await pool.query(`SELECT * FROM sites ORDER BY site_name ASC`);
    return res.json(result.rows);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to list sites' });
  }
});

app.put('/sites/:id', authRequired, async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) {
    return res.status(400).json({ error: 'id required' });
  }
  const body = { ...req.body, id };
  try {
    await upsertSiteRow(pool, body);
    const row = await pool.query(`SELECT * FROM sites WHERE id = $1`, [id]);
    return res.json(row.rows[0] || null);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to save site' });
  }
});

app.delete('/sites/:id', authRequired, async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) {
    return res.status(400).json({ error: 'id required' });
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `DELETE FROM pm_items WHERE session_id IN (SELECT id FROM pm_sessions WHERE site_id = $1)`,
      [id]
    );
    await client.query(`DELETE FROM pm_sessions WHERE site_id = $1`, [id]);
    const del = await client.query(`DELETE FROM sites WHERE id = $1`, [id]);
    await client.query('COMMIT');
    if (del.rowCount === 0) {
      return res.status(404).json({ error: 'Site not found' });
    }
    return res.json({ ok: true });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    console.error(err);
    return res.status(500).json({ error: 'Failed to delete site' });
  } finally {
    client.release();
  }
});

app.get('/admin', (req, res) => {
  res.redirect(302, '/admin.html');
});

app.get('/items', authRequired, async (req, res) => {
  const { sessionId, unitType, unitIndex } = req.query;

  try {
    const result = await pool.query(
      `SELECT id, device_type, cleaned, damaged, notes, serial, asset_tag, printer_master_pod, updated_at
       FROM pm_items
       WHERE session_id = $1 AND unit_type = $2 AND unit_index = $3
       ORDER BY CASE device_type
         WHEN 'camera_tower' THEN 1
         WHEN 'signature_pad' THEN 2
         WHEN 'printer' THEN 3
         WHEN 'scanner' THEN 4
         WHEN 'ups' THEN 5
         WHEN 'handheld_scanner' THEN 6
         WHEN 'testing_station' THEN 7
         ELSE 99
       END, device_type`,
      [sessionId, unitType, unitIndex]
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error fetching items');
  }
});

app.get('/session', authRequired, async (req, res) => {
  const { sessionId } = req.query;

  try {
    const result = await pool.query(
      `SELECT pods_count, kiosks_count, first_pod_system, first_kiosk_system
       FROM pm_sessions
       WHERE id = $1`,
      [sessionId]
    );

    res.json(result.rows[0] || null);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error fetching session');
  }
});

app.post('/import-session', authRequired, async (req, res) => {
  const { session, items, site } = req.body ?? {};

  if (!session || !session.id) {
    return res.status(400).json({ error: 'Missing session payload' });
  }

  if (!Array.isArray(items)) {
    return res.status(400).json({ error: 'Missing items payload' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    if (site && site.id && site.site_name) {
      await upsertSiteRow(client, site);
    }

    await client.query(
      `INSERT INTO pm_sessions (
        id,
        site_id,
        technician_name,
        started_at,
        pods_count,
        kiosks_count,
        first_pod_system,
        first_kiosk_system,
        created_at,
        updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (id) DO UPDATE SET
        site_id = EXCLUDED.site_id,
        technician_name = EXCLUDED.technician_name,
        started_at = EXCLUDED.started_at,
        pods_count = EXCLUDED.pods_count,
        kiosks_count = EXCLUDED.kiosks_count,
        first_pod_system = EXCLUDED.first_pod_system,
        first_kiosk_system = EXCLUDED.first_kiosk_system,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at`,
      [
        session.id,
        session.site_id,
        session.technician_name,
        session.started_at,
        session.pods_count ?? 0,
        session.kiosks_count ?? 0,
        session.first_pod_system ?? null,
        session.first_kiosk_system ?? null,
        session.created_at,
        session.updated_at,
      ]
    );

    await client.query(`DELETE FROM pm_items WHERE session_id = $1`, [session.id]);

    for (const item of items) {
      await client.query(
        `INSERT INTO pm_items (
          id,
          session_id,
          unit_type,
          unit_index,
          device_type,
          cleaned,
          damaged,
          notes,
          serial,
          asset_tag,
          printer_master_pod,
          created_at,
          updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
        [
          item.id,
          item.session_id,
          item.unit_type,
          item.unit_index,
          item.device_type,
          item.cleaned ?? 0,
          item.damaged ?? 0,
          item.notes ?? null,
          item.serial ?? null,
          item.asset_tag ?? null,
          item.printer_master_pod ?? null,
          item.created_at,
          item.updated_at,
        ]
      );
    }

    for (const item of items) {
      await syncHardwareInventoryFromPmItem(client, item, session.id);
    }

    await client.query('COMMIT');

    res.json({
      ok: true,
      sessionId: session.id,
      importedItems: items.length,
    });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: 'Import failed' });
  } finally {
    client.release();
  }
});

app.post('/update-item', authRequired, async (req, res) => {
  const {
    id,
    cleaned,
    damaged,
    notes,
    serial,
    asset_tag,
    printer_master_pod,
    updated_at,
  } = req.body ?? {};

  if (!id) {
    return res.status(400).json({ error: 'Missing item id' });
  }

  try {
    const result = await pool.query(
      `UPDATE pm_items
       SET cleaned = COALESCE($1, cleaned),
           damaged = COALESCE($2, damaged),
           notes = COALESCE($3, notes),
           serial = COALESCE($4, serial),
           asset_tag = COALESCE($5, asset_tag),
           printer_master_pod = COALESCE($6, printer_master_pod),
           updated_at = COALESCE($7, updated_at)
       WHERE id = $8
       RETURNING id, session_id, device_type, cleaned, damaged, notes, serial, asset_tag, printer_master_pod, updated_at`,
      [
        cleaned === undefined ? null : cleaned,
        damaged === undefined ? null : damaged,
        notes === undefined ? null : notes,
        serial === undefined ? null : serial,
        asset_tag === undefined ? null : asset_tag,
        printer_master_pod === undefined ? null : printer_master_pod,
        updated_at === undefined ? null : updated_at,
        id,
      ]
    );

    const row = result.rows[0] || null;
    if (row) {
      await syncHardwareInventoryFromPmItem(pool, row, row.session_id);
    }

    res.json(row);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Update failed' });
  }
});

async function start() {
  try {
    await ensureAuthTables();
    await ensurePmStateConfigs();
    await ensureSitesTable();
    await ensurePmSessionsAndItemsTables();
    await ensureHardwareInventoryTable();
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Failed to start server', err);
    process.exit(1);
  }
}

start();