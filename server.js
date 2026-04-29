const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const app = express();
const PORT = Number(process.env.PORT || 3000);
const JWT_SECRET = process.env.JWT_SECRET || '';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '12h';
const SALT_ROUNDS = Number(process.env.PASSWORD_SALT_ROUNDS || 10);
const REQUIRE_AUTH = String(process.env.REQUIRE_AUTH || 'false').toLowerCase() === 'true';

app.use(express.json({ limit: '10mb' }));

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

function authRequired(req, res, next) {
  if (!REQUIRE_AUTH) return next();
  const raw = req.headers.authorization || '';
  const [scheme, token] = raw.split(' ');
  if (scheme !== 'Bearer' || !token) {
    return res.status(401).json({ error: 'Missing or invalid authorization header' });
  }
  if (!JWT_SECRET) {
    return res.status(500).json({ error: 'JWT secret is not configured on server' });
  }
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.user = payload;
    return next();
  } catch {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
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
  if (!adminEmail || !adminPassword) return;

  const existing = await pool.query(`SELECT id FROM users WHERE email = $1 LIMIT 1`, [adminEmail]);
  if (existing.rows.length > 0) return;

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
  const { session, items } = req.body ?? {};

  if (!session || !session.id) {
    return res.status(400).json({ error: 'Missing session payload' });
  }

  if (!Array.isArray(items)) {
    return res.status(400).json({ error: 'Missing items payload' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

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
       RETURNING id, device_type, cleaned, damaged, notes, serial, asset_tag, printer_master_pod, updated_at`,
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

    res.json(result.rows[0] || null);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Update failed' });
  }
});

async function start() {
  try {
    await ensureAuthTables();
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error('Failed to start server', err);
    process.exit(1);
  }
}

start();