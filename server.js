import path from 'path';
import { promises as fs, existsSync, mkdirSync } from 'fs';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { execFile } from 'child_process';
import { promisify } from 'util';
import dns from 'dns/promises';
import net from 'net';
import express from 'express';
import session from 'express-session';
import cookieParser from 'cookie-parser';
import multer from 'multer';
import bcrypt from 'bcryptjs';
import nodemailer from 'nodemailer';
import sharp from 'sharp';
import QRCode from 'qrcode';
import {
  db,
  initDb,
  getDefaultMenus,
  getDefaultProductGroupConfigs,
  getPostCounts,
  getProductCounts,
  getSetting,
  getVisitCounts,
  incrementFunnelEvent,
  incrementVisit,
  setSetting,
  SHOP_PRODUCT_GROUPS,
  DEFAULT_MEMBER_LEVEL_RULES,
  DEFAULT_MEMBER_LEVEL_POINT_RATES
} from './src/db.js';
import { resolveLanguage, t } from './src/i18n.js';

initDb();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || path.join(__dirname, 'uploads'));
if (!existsSync(UPLOAD_DIR)) {
  mkdirSync(UPLOAD_DIR, { recursive: true });
}
const BRANDING_DIR = path.join(__dirname, 'public', 'media', 'branding');
const BRANDING_ASSET_FILES = Object.freeze({
  dayHeaderSymbol: 'day-header-symbol.png',
  dayHeaderLogo: 'day-header-logo.png',
  dayFooterLogo: 'day-footer-logo.png',
  nightHeaderSymbol: 'night-header-symbol.png',
  nightHeaderLogo: 'night-header-logo.png',
  nightFooterLogo: 'night-footer-logo.png'
});
const BRANDING_WATERMARK_FILE = 'watermark-white.png';
const BRANDING_WATERMARK_LOCAL_PATH = path.join(BRANDING_DIR, BRANDING_WATERMARK_FILE);
const BRANDING_WATERMARK_URL = `/assets/media/branding/${BRANDING_WATERMARK_FILE}`;

const app = express();
const PORT = Number(process.env.PORT || 3100);
const isProduction = process.env.NODE_ENV === 'production';
function parseEnvFlag(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) {
    return Boolean(fallback);
  }
  if (['1', 'true', 'yes', 'on'].includes(raw)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(raw)) {
    return false;
  }
  return Boolean(fallback);
}

function parseCsvStringSet(value = '') {
  return new Set(
    String(value || '')
      .split(',')
      .map((item) => String(item || '').trim())
      .filter(Boolean)
  );
}

function parseCsvCountryCodeSet(value = '') {
  return new Set(
    String(value || '')
      .split(',')
      .map((item) => String(item || '').trim().toUpperCase())
      .filter((item) => /^[A-Z]{2}$/.test(item))
  );
}

function parseCsvAsnSet(value = '') {
  const parsed = new Set();
  String(value || '')
    .split(',')
    .map((item) => String(item || '').trim().toUpperCase())
    .filter(Boolean)
    .forEach((item) => {
      const normalized = item.startsWith('AS') ? item.slice(2) : item;
      if (/^[0-9]{1,10}$/.test(normalized)) {
        parsed.add(Number(normalized));
      }
    });
  return parsed;
}

function parseCsvIpSet(value = '') {
  return new Set(
    String(value || '')
      .split(',')
      .map((item) => normalizeIpAddress(item))
      .filter(Boolean)
  );
}
const isHostedEnvironment = Boolean(
  String(
    process.env.RENDER_EXTERNAL_URL ||
      process.env.RAILWAY_STATIC_URL ||
      process.env.FLY_APP_NAME ||
      process.env.HEROKU_APP_NAME ||
      ''
  ).trim()
);
const mustEnforceSecurity = isProduction || isHostedEnvironment || parseEnvFlag(process.env.ENFORCE_PROD_SECURITY, false);
const ASSET_VERSION = process.env.RENDER_GIT_COMMIT || `${Date.now()}`;
const execFileAsync = promisify(execFile);
function normalizeOriginValue(origin = '') {
  const raw = String(origin || '').trim().toLowerCase();
  if (!raw) {
    return '';
  }
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      return '';
    }
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return '';
  }
}

function resolveSessionSecret() {
  const configured = String(process.env.SESSION_SECRET || '').trim();
  if (configured) {
    return configured;
  }
  if (mustEnforceSecurity) {
    throw new Error('SESSION_SECRET environment variable is required in hosted/production mode.');
  }
  return `chronolab-dev-${crypto.randomBytes(32).toString('hex')}`;
}

const TRUSTED_CSRF_ORIGINS = new Set(
  String(process.env.TRUSTED_CSRF_ORIGINS || '')
    .split(',')
    .map((item) => normalizeOriginValue(item))
    .filter(Boolean)
);

const SESSION_SECRET = resolveSessionSecret();
const SESSION_DEFAULT_MAX_AGE_MS = 1000 * 60 * 60 * 24 * 14;
const SESSION_STORE_CLEANUP_INTERVAL_MS = 1000 * 60 * 15;
const MEMBER_IDLE_TIMEOUT_MS = resolveIdleTimeoutMs({
  envKey: 'MEMBER_IDLE_TIMEOUT_MINUTES',
  fallbackMinutes: 120,
  minMinutes: 10,
  maxMinutes: 60 * 24 * 30
});
const ADMIN_IDLE_TIMEOUT_MS = resolveIdleTimeoutMs({
  envKey: 'ADMIN_IDLE_TIMEOUT_MINUTES',
  fallbackMinutes: 30,
  minMinutes: 5,
  maxMinutes: 60 * 24 * 30
});
const AUTH_PERSIST_MEMBER_COOKIE_NAME = 'cl_member_auth';
const AUTH_PERSIST_ADMIN_COOKIE_NAME = 'cl_admin_auth';
const AUTH_PERSIST_LEGACY_COOKIE_NAME = 'cl_auth';
const AUTH_PERSIST_COOKIE_MAX_AGE_MS = SESSION_DEFAULT_MAX_AGE_MS;
const AUTH_PERSIST_HMAC_KEY = crypto
  .createHash('sha256')
  .update(`${SESSION_SECRET}:auth-cookie-v1`)
  .digest();
const CSRF_TOKEN_SIZE_BYTES = 32;
const CSRF_TOKEN_REGEX = /^[a-f0-9]{64}$/i;
const ORDER_COMPLETE_VIEW_TTL_MS = 1000 * 60 * 60;
const SUPPORT_CHAT_PRIMARY_ADMIN_USERNAME = 'admin1';
const SUPPORT_CHAT_MAX_MESSAGE_LENGTH = 1000;
const DUMMY_PASSWORD_HASH = bcrypt.hashSync('ChronoLab.Auth.Dummy.Password.2026', 10);
const ADMIN_WAF_ENABLED = parseEnvFlag(process.env.ADMIN_WAF_ENABLED, mustEnforceSecurity);
const ADMIN_OTP_ENFORCED = parseEnvFlag(process.env.ADMIN_OTP_ENFORCED, true);

function normalizeAdminEntryPath(rawPath = '') {
  const fallback = '/admin';
  const trimmed = String(rawPath || '').trim();
  if (!trimmed) {
    return fallback;
  }
  let normalized = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
  normalized = normalized.replace(/\/{2,}/g, '/');
  if (normalized.length > 1) {
    normalized = normalized.replace(/\/+$/g, '');
  }
  normalized = normalized.toLowerCase();
  if (!normalized || normalized === '/' || normalized.startsWith('/api') || normalized.startsWith('/assets') || normalized.startsWith('/uploads')) {
    return fallback;
  }
  return normalized;
}

const ADMIN_CANONICAL_PATH_PREFIX = '/admin';
const ADMIN_ENTRY_PATH_PREFIX = normalizeAdminEntryPath(
  process.env.ADMIN_ENTRY_PATH || process.env.ADMIN_ROUTE_PATH || '/admin'
);
const ADMIN_ROUTE_RANDOMIZED = ADMIN_ENTRY_PATH_PREFIX !== ADMIN_CANONICAL_PATH_PREFIX;

function isCanonicalAdminPath(pathname = '') {
  const safe = String(pathname || '').trim();
  return safe === ADMIN_CANONICAL_PATH_PREFIX || safe.startsWith(`${ADMIN_CANONICAL_PATH_PREFIX}/`);
}

function isMappedAdminPath(pathname = '') {
  const safe = String(pathname || '').trim();
  return safe === ADMIN_ENTRY_PATH_PREFIX || safe.startsWith(`${ADMIN_ENTRY_PATH_PREFIX}/`);
}

function mapCanonicalAdminPathToEntry(value = '') {
  const raw = String(value || '').trim();
  if (!ADMIN_ROUTE_RANDOMIZED || !raw) {
    return raw;
  }
  const mapLocalPath = (input = '') => {
    const local = String(input || '');
    if (isCanonicalAdminPath(local)) {
      return `${ADMIN_ENTRY_PATH_PREFIX}${local.slice(ADMIN_CANONICAL_PATH_PREFIX.length)}`;
    }
    return local;
  };

  if (raw.startsWith('/') && !raw.startsWith('//')) {
    return mapLocalPath(raw);
  }

  try {
    const parsed = new URL(raw);
    const mappedPathname = mapLocalPath(parsed.pathname);
    if (mappedPathname === parsed.pathname) {
      return raw;
    }
    parsed.pathname = mappedPathname;
    return parsed.toString();
  } catch {
    return raw;
  }
}

function rewriteAdminPathReferencesInHtml(html = '') {
  const source = typeof html === 'string' ? html : '';
  if (!ADMIN_ROUTE_RANDOMIZED || !source || !source.includes('/admin')) {
    return html;
  }
  return source.replace(
    /(^|["'=(\s])\/admin(?=(?:\/|\?|#|["')\s>]))/gm,
    (_, prefix = '') => `${prefix}${ADMIN_ENTRY_PATH_PREFIX}`
  );
}
const ADMIN_WAF_BOT_BLOCK_ENABLED = parseEnvFlag(
  process.env.ADMIN_WAF_BOT_BLOCK_ENABLED,
  ADMIN_WAF_ENABLED
);
const ADMIN_WAF_GEO_BLOCK_ENABLED = parseEnvFlag(
  process.env.ADMIN_WAF_GEO_BLOCK_ENABLED,
  ADMIN_WAF_ENABLED
);
const ADMIN_WAF_ASN_BLOCK_ENABLED = parseEnvFlag(
  process.env.ADMIN_WAF_ASN_BLOCK_ENABLED,
  ADMIN_WAF_ENABLED
);
const ADMIN_WAF_FAIL_CLOSED_ON_LOOKUP_ERROR = parseEnvFlag(
  process.env.ADMIN_WAF_FAIL_CLOSED_ON_LOOKUP_ERROR,
  false
);
const ADMIN_WAF_ALLOWED_COUNTRY_CODES = parseCsvCountryCodeSet(
  process.env.ADMIN_WAF_ALLOWED_COUNTRY_CODES ||
    (mustEnforceSecurity ? 'KR' : '')
);
const ADMIN_WAF_BLOCKED_ASNS = parseCsvAsnSet(process.env.ADMIN_WAF_BLOCKED_ASNS || '');
const ADMIN_WAF_IP_ALLOWLIST = parseCsvIpSet(
  process.env.ADMIN_WAF_IP_ALLOWLIST || process.env.ADMIN_ALLOWLIST_IPS || ''
);
const ADMIN_WAF_IP_ALLOWLIST_ENFORCED = parseEnvFlag(
  process.env.ADMIN_WAF_IP_ALLOWLIST_ENFORCED ||
    process.env.ADMIN_ALLOWLIST_IPS_ENFORCED ||
    process.env.ADMIN_IP_LOCK_ENABLED,
  false
);
const ADMIN_WAF_PROFILE_CACHE_TTL_MS = Math.max(
  5 * 60 * 1000,
  Number.parseInt(String(process.env.ADMIN_WAF_PROFILE_CACHE_TTL_MS || ''), 10) || 6 * 60 * 60 * 1000
);
const ADMIN_WAF_LOOKUP_TIMEOUT_MS = Math.max(
  1200,
  Number.parseInt(String(process.env.ADMIN_WAF_LOOKUP_TIMEOUT_MS || ''), 10) || 4000
);
const adminWafProfileCache = new Map();
const SECURITY_ALERT_NOTIFY_ENABLED = parseEnvFlag(
  process.env.SECURITY_ALERT_NOTIFY_ENABLED,
  mustEnforceSecurity
);
const SECURITY_ALERT_NOTIFY_WEBHOOK_URL = String(
  process.env.SECURITY_ALERT_NOTIFY_WEBHOOK_URL || process.env.SECURITY_ALERT_WEBHOOK_URL || ''
)
  .trim();
const SECURITY_ALERT_NOTIFY_EMAIL_RECIPIENTS = Array.from(
  parseCsvStringSet(process.env.SECURITY_ALERT_NOTIFY_EMAIL_TO || process.env.SECURITY_ALERT_EMAIL_TO || '')
)
  .map((email) => normalizeEmailAddress(email))
  .filter(Boolean);
const SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED = parseEnvFlag(
  process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED,
  false
);
const SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN = String(
  process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || ''
).trim();
const SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS = Array.from(
  parseCsvStringSet(
    process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS ||
      process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_ID ||
      process.env.TELEGRAM_CHAT_ID ||
      ''
  )
)
  .map((value) => String(value || '').trim())
  .filter((value) => /^-?[0-9]{3,20}$/.test(value));
const SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID = (() => {
  const parsed = Number.parseInt(
    String(
      process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID ||
        process.env.TELEGRAM_THREAD_ID ||
        ''
    ),
    10
  );
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
})();
const SECURITY_ALERT_NOTIFY_TELEGRAM_SILENT = parseEnvFlag(
  process.env.SECURITY_ALERT_NOTIFY_TELEGRAM_SILENT,
  false
);
const SECURITY_ALERT_NOTIFY_THROTTLE_MS = Math.max(
  10 * 1000,
  Number.parseInt(String(process.env.SECURITY_ALERT_NOTIFY_THROTTLE_MS || ''), 10) || 60 * 1000
);
const SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS = Math.max(
  SECURITY_ALERT_NOTIFY_THROTTLE_MS,
  Number.parseInt(String(process.env.SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS || ''), 10) || 10 * 60 * 1000
);
const SECURITY_ALERT_NOTIFY_INCLUDE_RAW_CODE = parseEnvFlag(
  process.env.SECURITY_ALERT_NOTIFY_INCLUDE_RAW_CODE,
  false
);
const SECURITY_ALERT_NOTIFY_TIMEOUT_MS = Math.max(
  1000,
  Number.parseInt(String(process.env.SECURITY_ALERT_NOTIFY_TIMEOUT_MS || ''), 10) || 5000
);
const securityAlertNotifyState = new Map();
const ADMIN_WAF_BLOCKED_USER_AGENT_PATTERNS = [
  /sqlmap/i,
  /acunetix/i,
  /masscan/i,
  /nmap/i,
  /nikto/i,
  /dirbuster/i,
  /gobuster/i,
  /wpscan/i,
  /python-requests/i,
  /httpclient/i,
  /libwww-perl/i,
  /go-http-client/i,
  /node-fetch/i,
  /curl\//i,
  /wget\//i,
  /headless/i,
  /\bbot\b/i,
  /\bcrawler\b/i,
  /\bspider\b/i
];

function resolveIdleTimeoutMs({
  envKey = '',
  fallbackMinutes = 60,
  minMinutes = 5,
  maxMinutes = 60 * 24
} = {}) {
  const parsed = Number.parseInt(String(process.env[String(envKey || '').trim()] || ''), 10);
  const fallback = Number.isFinite(Number(fallbackMinutes)) ? Number(fallbackMinutes) : 60;
  const min = Number.isFinite(Number(minMinutes)) ? Number(minMinutes) : 5;
  const max = Number.isFinite(Number(maxMinutes)) ? Number(maxMinutes) : 60 * 24;
  const minutes = Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
  return Math.max(min, Math.min(max, minutes)) * 60 * 1000;
}

function resolveSessionExpiryTimeMs(sess = {}) {
  const cookie = sess && typeof sess.cookie === 'object' ? sess.cookie : {};
  const maxAgeRaw =
    cookie && (cookie.originalMaxAge !== undefined ? cookie.originalMaxAge : cookie.maxAge);
  const maxAge = Number(maxAgeRaw);
  if (Number.isFinite(maxAge) && maxAge > 0) {
    return Date.now() + maxAge;
  }

  const expiresAt = cookie && cookie.expires ? new Date(cookie.expires).getTime() : NaN;
  if (Number.isFinite(expiresAt) && expiresAt > 0) {
    return expiresAt;
  }

  return Date.now() + SESSION_DEFAULT_MAX_AGE_MS;
}

class SQLiteSessionStore extends session.Store {
  constructor(options = {}) {
    super();
    this.cleanupIntervalMs = Math.max(
      60 * 1000,
      Number(options.cleanupIntervalMs || SESSION_STORE_CLEANUP_INTERVAL_MS)
    );
    this.lastCleanupAt = 0;

    this.selectStmt = db.prepare(
      `
        SELECT sess
        FROM user_sessions
        WHERE sid = ?
          AND expires_at > ?
        LIMIT 1
      `
    );
    this.upsertStmt = db.prepare(
      `
        INSERT INTO user_sessions (sid, sess, expires_at, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(sid)
        DO UPDATE SET
          sess = excluded.sess,
          expires_at = excluded.expires_at,
          updated_at = datetime('now')
      `
    );
    this.touchStmt = db.prepare(
      `
        UPDATE user_sessions
        SET expires_at = ?, updated_at = datetime('now')
        WHERE sid = ?
      `
    );
    this.destroyStmt = db.prepare('DELETE FROM user_sessions WHERE sid = ?');
    this.cleanupStmt = db.prepare('DELETE FROM user_sessions WHERE expires_at <= ?');
  }

  cleanupExpiredSessions() {
    const now = Date.now();
    if (now - this.lastCleanupAt < this.cleanupIntervalMs) {
      return;
    }
    this.cleanupStmt.run(now);
    this.lastCleanupAt = now;
  }

  get(sid, callback = () => {}) {
    try {
      this.cleanupExpiredSessions();
      const row = this.selectStmt.get(String(sid || ''), Date.now());
      if (!row || !row.sess) {
        callback(null, null);
        return;
      }
      let parsed = null;
      try {
        parsed = JSON.parse(String(row.sess || '{}'));
      } catch {
        this.destroyStmt.run(String(sid || ''));
        callback(null, null);
        return;
      }
      callback(null, parsed);
    } catch (error) {
      callback(error);
    }
  }

  set(sid, sess, callback = () => {}) {
    try {
      this.cleanupExpiredSessions();
      const normalizedSid = String(sid || '').trim();
      if (!normalizedSid) {
        callback(new Error('invalid session id'));
        return;
      }
      const expiresAt = resolveSessionExpiryTimeMs(sess);
      this.upsertStmt.run(normalizedSid, JSON.stringify(sess || {}), expiresAt);
      callback(null);
    } catch (error) {
      callback(error);
    }
  }

  touch(sid, sess, callback = () => {}) {
    try {
      this.cleanupExpiredSessions();
      const normalizedSid = String(sid || '').trim();
      if (!normalizedSid) {
        callback(new Error('invalid session id'));
        return;
      }
      const expiresAt = resolveSessionExpiryTimeMs(sess);
      const result = this.touchStmt.run(expiresAt, normalizedSid);
      if (!result || Number(result.changes || 0) === 0) {
        this.upsertStmt.run(normalizedSid, JSON.stringify(sess || {}), expiresAt);
      }
      callback(null);
    } catch (error) {
      callback(error);
    }
  }

  destroy(sid, callback = () => {}) {
    try {
      this.destroyStmt.run(String(sid || ''));
      callback(null);
    } catch (error) {
      callback(error);
    }
  }
}

function signPersistAuthPayload(payloadBase64 = '') {
  return crypto.createHmac('sha256', AUTH_PERSIST_HMAC_KEY).update(String(payloadBase64 || '')).digest('base64url');
}

function createPersistAuthToken(userId, options = {}) {
  const normalizedUserId = Number(userId || 0);
  if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0) {
    return '';
  }

  const now = Date.now();
  const normalizedLastActivityAt = Number(options.lastActivityAt || now);
  const lastActivityAt =
    Number.isFinite(normalizedLastActivityAt) && normalizedLastActivityAt > 0
      ? normalizedLastActivityAt
      : now;
  const isAdmin = options.isAdmin === true;
  const payload = {
    uid: normalizedUserId,
    adm: isAdmin ? 1 : 0,
    la: lastActivityAt,
    iat: now,
    exp: lastActivityAt + AUTH_PERSIST_COOKIE_MAX_AGE_MS
  };
  const payloadBase64 = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
  const signature = signPersistAuthPayload(payloadBase64);
  return `${payloadBase64}.${signature}`;
}

function parsePersistAuthToken(rawToken = '') {
  const token = String(rawToken || '').trim();
  if (!token) {
    return null;
  }

  const dotIndex = token.lastIndexOf('.');
  if (dotIndex <= 0 || dotIndex >= token.length - 1) {
    return null;
  }

  const payloadBase64 = token.slice(0, dotIndex);
  const providedSignature = token.slice(dotIndex + 1);
  const expectedSignature = signPersistAuthPayload(payloadBase64);
  const expectedBuffer = Buffer.from(expectedSignature);
  const providedBuffer = Buffer.from(providedSignature);
  if (
    expectedBuffer.length !== providedBuffer.length ||
    !crypto.timingSafeEqual(expectedBuffer, providedBuffer)
  ) {
    return null;
  }

  let payload = null;
  try {
    payload = JSON.parse(Buffer.from(payloadBase64, 'base64url').toString('utf8'));
  } catch {
    return null;
  }

  const userId = Number(payload?.uid || 0);
  const expiresAt = Number(payload?.exp || 0);
  const lastActivityAt = Number(payload?.la || 0);
  const isAdmin = Number(payload?.adm || 0) === 1;
  if (!Number.isInteger(userId) || userId <= 0 || !Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    return null;
  }

  return {
    userId,
    expiresAt,
    lastActivityAt: Number.isFinite(lastActivityAt) && lastActivityAt > 0 ? lastActivityAt : Date.now(),
    isAdmin
  };
}

function resolveAuthScope(rawScope = 'member') {
  return String(rawScope || '').trim().toLowerCase() === 'admin' ? 'admin' : 'member';
}

function resolveAuthScopeFromRequest(req) {
  const requestPath = String(req?.path || '').trim().toLowerCase();
  if (
    requestPath.startsWith('/admin') ||
    requestPath.startsWith('/api/admin/') ||
    (ADMIN_ROUTE_RANDOMIZED && requestPath.startsWith(ADMIN_ENTRY_PATH_PREFIX))
  ) {
    return 'admin';
  }
  return 'member';
}

function resolvePersistCookieName(authScope = 'member') {
  return resolveAuthScope(authScope) === 'admin'
    ? AUTH_PERSIST_ADMIN_COOKIE_NAME
    : AUTH_PERSIST_MEMBER_COOKIE_NAME;
}

function normalizeSessionUserId(rawValue = 0) {
  const normalizedUserId = Number(rawValue || 0);
  if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0) {
    return 0;
  }
  return normalizedUserId;
}

function normalizeSessionTimestamp(rawValue = 0, fallbackValue = 0) {
  const normalizedTimestamp = Number(rawValue || 0);
  if (Number.isFinite(normalizedTimestamp) && normalizedTimestamp > 0) {
    return normalizedTimestamp;
  }
  return Number.isFinite(Number(fallbackValue)) && Number(fallbackValue) > 0
    ? Number(fallbackValue)
    : Date.now();
}

function getScopedSessionUserId(sessionState = {}, authScope = 'member') {
  const scope = resolveAuthScope(authScope);
  if (scope === 'admin') {
    return normalizeSessionUserId(sessionState?.adminUserId);
  }
  return normalizeSessionUserId(sessionState?.memberUserId);
}

function getScopedSessionLastActivityAt(sessionState = {}, authScope = 'member') {
  const scope = resolveAuthScope(authScope);
  if (scope === 'admin') {
    return normalizeSessionTimestamp(sessionState?.adminLastActivityAt, 0);
  }
  return normalizeSessionTimestamp(sessionState?.memberLastActivityAt, 0);
}

function setScopedSessionAuthState(req, authScope = 'member', options = {}) {
  if (!req?.session) {
    return;
  }

  const scope = resolveAuthScope(authScope);
  const userId = normalizeSessionUserId(options.userId);
  const lastActivityAt = normalizeSessionTimestamp(options.lastActivityAt, Date.now());

  if (scope === 'admin') {
    req.session.adminUserId = userId || null;
    req.session.adminLastActivityAt = userId > 0 ? lastActivityAt : 0;
    req.session.adminRole = userId > 0 ? normalizeAdminRole(options.adminRole || req.session.adminRole) : '';
  } else {
    req.session.memberUserId = userId || null;
    req.session.memberLastActivityAt = userId > 0 ? lastActivityAt : 0;
  }

  // Legacy keys cleanup after auth-state split rollout.
  req.session.userId = null;
  req.session.isAdmin = false;
  req.session.lastActivityAt = 0;
}

function clearScopedSessionAuthState(req, authScope = 'member', options = {}) {
  if (!req?.session) {
    return;
  }

  const scope = resolveAuthScope(authScope);
  if (scope === 'admin') {
    req.session.adminUserId = null;
    req.session.adminRole = '';
    req.session.adminLastActivityAt = 0;
    if (options.keepOtpPending !== true) {
      clearAdminOtpPending(req);
    }
    if (options.clearOtpSetup === true) {
      clearAdminOtpSetup(req);
    }
  } else {
    req.session.memberUserId = null;
    req.session.memberLastActivityAt = 0;
  }

  // Legacy keys cleanup after auth-state split rollout.
  req.session.userId = null;
  req.session.isAdmin = false;
  req.session.lastActivityAt = 0;
}

function migrateLegacySessionAuthState(req) {
  if (!req?.session) {
    return;
  }

  const legacyUserId = normalizeSessionUserId(req.session.userId);
  if (legacyUserId <= 0) {
    return;
  }

  const legacyScope = req.session.isAdmin === true ? 'admin' : 'member';
  const hasScopedUser =
    legacyScope === 'admin'
      ? getScopedSessionUserId(req.session, 'admin') > 0
      : getScopedSessionUserId(req.session, 'member') > 0;
  if (!hasScopedUser) {
    setScopedSessionAuthState(req, legacyScope, {
      userId: legacyUserId,
      lastActivityAt: normalizeSessionTimestamp(req.session.lastActivityAt, Date.now()),
      adminRole: legacyScope === 'admin' ? normalizeAdminRole(req.session.adminRole || '') : ''
    });
  } else {
    req.session.userId = null;
    req.session.isAdmin = false;
    req.session.lastActivityAt = 0;
  }
}

function clearPersistAuthCookie(res, options = {}) {
  if (!res || typeof res.clearCookie !== 'function') {
    return;
  }

  const scope = String(options.scope || 'all').trim().toLowerCase();
  const cookieNames = [];
  if (scope === 'member') {
    cookieNames.push(AUTH_PERSIST_MEMBER_COOKIE_NAME);
  } else if (scope === 'admin') {
    cookieNames.push(AUTH_PERSIST_ADMIN_COOKIE_NAME);
  } else if (scope === 'legacy') {
    cookieNames.push(AUTH_PERSIST_LEGACY_COOKIE_NAME);
  } else {
    cookieNames.push(
      AUTH_PERSIST_MEMBER_COOKIE_NAME,
      AUTH_PERSIST_ADMIN_COOKIE_NAME,
      AUTH_PERSIST_LEGACY_COOKIE_NAME
    );
  }

  cookieNames.forEach((cookieName) => {
    res.clearCookie(cookieName, {
      path: '/',
      httpOnly: true,
      sameSite: 'lax',
      secure: mustEnforceSecurity
    });
  });
}

function setPersistAuthCookie(res, userId, options = {}) {
  if (!res || typeof res.cookie !== 'function') {
    return;
  }
  const authScope = resolveAuthScope(options.scope || (options.isAdmin === true ? 'admin' : 'member'));
  const cookieName = resolvePersistCookieName(authScope);
  const token = createPersistAuthToken(userId, options);
  if (!token) {
    clearPersistAuthCookie(res, { scope: authScope });
    return;
  }
  res.cookie(cookieName, token, {
    path: '/',
    maxAge: AUTH_PERSIST_COOKIE_MAX_AGE_MS,
    httpOnly: true,
    sameSite: 'lax',
    secure: mustEnforceSecurity
  });
  clearPersistAuthCookie(res, { scope: 'legacy' });
}

function normalizeCsrfTokenValue(rawValue = '') {
  const token = String(rawValue || '').trim().toLowerCase();
  if (!CSRF_TOKEN_REGEX.test(token)) {
    return '';
  }
  return token;
}

function ensureSessionCsrfToken(req) {
  if (!req?.session) {
    return '';
  }
  const existingToken = normalizeCsrfTokenValue(req.session.csrfToken || '');
  if (existingToken) {
    req.session.csrfToken = existingToken;
    return existingToken;
  }
  const nextToken = crypto.randomBytes(CSRF_TOKEN_SIZE_BYTES).toString('hex');
  req.session.csrfToken = nextToken;
  return nextToken;
}

function readCsrfTokenFromRequest(req) {
  const headerToken = normalizeCsrfTokenValue(req.get('x-csrf-token') || req.get('x-xsrf-token') || '');
  if (headerToken) {
    return headerToken;
  }
  if (req?.body && typeof req.body === 'object') {
    return normalizeCsrfTokenValue(req.body._csrf || '');
  }
  return '';
}

function isCsrfTokenEqual(expectedToken = '', providedToken = '') {
  const expected = normalizeCsrfTokenValue(expectedToken);
  const provided = normalizeCsrfTokenValue(providedToken);
  if (!expected || !provided) {
    return false;
  }
  const expectedBuffer = Buffer.from(expected, 'utf8');
  const providedBuffer = Buffer.from(provided, 'utf8');
  if (expectedBuffer.length !== providedBuffer.length) {
    return false;
  }
  return crypto.timingSafeEqual(expectedBuffer, providedBuffer);
}

function tryRestoreSessionFromPersistCookie(req, res, requestedScope = resolveAuthScopeFromRequest(req)) {
  if (!req?.session) {
    return false;
  }

  migrateLegacySessionAuthState(req);
  const authScope = resolveAuthScope(requestedScope);
  if (getScopedSessionUserId(req.session, authScope) > 0) {
    return true;
  }

  const scopedCookieName = resolvePersistCookieName(authScope);
  const candidateCookieNames = [scopedCookieName, AUTH_PERSIST_LEGACY_COOKIE_NAME];

  for (const cookieName of candidateCookieNames) {
    const token = String(req?.cookies?.[cookieName] || '').trim();
    if (!token) {
      continue;
    }

    const parsed = parsePersistAuthToken(token);
    if (!parsed) {
      if (cookieName === AUTH_PERSIST_LEGACY_COOKIE_NAME) {
        clearPersistAuthCookie(res, { scope: 'legacy' });
      } else {
        clearPersistAuthCookie(res, { scope: authScope });
      }
      continue;
    }

    const parsedScope = parsed.isAdmin ? 'admin' : 'member';
    if (parsedScope !== authScope) {
      continue;
    }

    setScopedSessionAuthState(req, authScope, {
      userId: parsed.userId,
      lastActivityAt: parsed.lastActivityAt,
      adminRole: authScope === 'admin' ? normalizeAdminRole(req.session.adminRole || '') : ''
    });
    if (cookieName === AUTH_PERSIST_LEGACY_COOKIE_NAME) {
      setPersistAuthCookie(res, parsed.userId, {
        scope: authScope,
        isAdmin: parsed.isAdmin,
        lastActivityAt: parsed.lastActivityAt
      });
      clearPersistAuthCookie(res, { scope: 'legacy' });
    }
    return true;
  }

  return false;
}

function getSessionIdleTimeoutMs(authScope = 'member') {
  return resolveAuthScope(authScope) === 'admin' ? ADMIN_IDLE_TIMEOUT_MS : MEMBER_IDLE_TIMEOUT_MS;
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const USERNAME_REGEX = /^[a-z0-9]{4,20}$/;
const ACCOUNT_LOOKUP_REGEX = /^[A-Za-z0-9._-]{2,40}$/;
const PASSWORD_REGEX = /^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[^A-Za-z0-9]).{8,}$/;
const DIGIT_PHONE_REGEX = /^[0-9]+$/;
const CUSTOMS_NO_REGEX = /^[A-Za-z0-9-]{6,30}$/;
const TRACKING_NUMBER_REGEX = /^[A-Za-z0-9-]{6,40}$/;
const PHONE_REGEX = /^[0-9]{8,20}$/;
const LOG_REDACTED_KEY_REGEX = /(password|pass|token|secret|otp|captcha|code|cookie|session|authorization)/i;
const LOG_MAX_STRING_LENGTH = 320;
const LOG_MAX_ARRAY_LENGTH = 30;
const LOG_MAX_OBJECT_KEYS = 40;

const ADMIN_ROLE = Object.freeze({
  PRIMARY: 'PRIMARY',
  SUB: 'SUB'
});

const ORDER_STATUS = Object.freeze({
  PENDING_REVIEW: 'PENDING_REVIEW',
  ORDER_CONFIRMED: 'ORDER_CONFIRMED',
  READY_TO_SHIP: 'READY_TO_SHIP',
  SHIPPING: 'SHIPPING',
  DELIVERED: 'DELIVERED',
  CANCELLED: 'CANCELLED'
});

const FUNNEL_EVENT = Object.freeze({
  PRODUCT_VIEW: 'product_view',
  PURCHASE_VIEW: 'purchase_view',
  ORDER_CREATED: 'order_created',
  PAYMENT_CONFIRMED: 'payment_confirmed'
});

const ADMIN_MENUS = Object.freeze([
  { id: 'admin-dashboard', labelKo: '대시보드', labelEn: 'Dashboard', path: '/admin/dashboard' },
  { id: 'admin-security', labelKo: '보안', labelEn: 'Security', path: '/admin/security' },
  { id: 'admin-site', labelKo: '사이트설정', labelEn: 'Site', path: '/admin/site' },
  { id: 'admin-menus', labelKo: '메뉴관리', labelEn: 'Menus', path: '/admin/menus' },
  { id: 'admin-members', labelKo: '회원관리', labelEn: 'Members', path: '/admin/members' },
  { id: 'admin-points', labelKo: '포인트관리', labelEn: 'Points', path: '/admin/points' },
  { id: 'admin-products', labelKo: '상품관리', labelEn: 'Products', path: '/admin/products' },
  { id: 'admin-orders', labelKo: '주문관리', labelEn: 'Orders', path: '/admin/orders' },
  { id: 'admin-sales', labelKo: '매출관리', labelEn: 'Sales', path: '/admin/sales' },
  { id: 'admin-notices', labelKo: '공지사항', labelEn: 'Notices', path: '/admin/notices' },
  { id: 'admin-news', labelKo: '뉴스', labelEn: 'News', path: '/admin/news' },
  { id: 'admin-qc', labelKo: 'QC', labelEn: 'QC', path: '/admin/qc' },
  { id: 'admin-inquiries', labelKo: '문의답변', labelEn: 'Inquiries', path: '/admin/inquiries' }
]);

const SALES_SHEET_DEFAULT_URL =
  'https://docs.google.com/spreadsheets/d/1ZBZ1BvTNTEn809EllGK1W4y9pv-neHxczny5awBqKSA/edit';
const SALES_WORKBOOK_SETTING_KEY = 'salesWorkbookV1';
const SALES_LEGACY_SHEET_SYNC_DONE_KEY = 'salesLegacySheetSyncDoneAt';
const SALES_MAIN_TABS = Object.freeze([
  { key: 'price', labelKo: '공장제 가격표', labelEn: 'Factory Price Table', scopeType: 'factory' },
  { key: 'preorder', labelKo: '선주문 정산', labelEn: 'Pre-Order Settlement', scopeType: 'round' },
  { key: 'factory', labelKo: '공장제 매출', labelEn: 'Factory Sales', scopeType: 'date' },
  { key: 'genparts', labelKo: '젠파츠 매출', labelEn: 'Gen-Parts Sales', scopeType: 'date' },
  { key: 'used', labelKo: '현지중고 매출', labelEn: 'Local Used Sales', scopeType: 'date' }
]);
const SALES_IMPORT_TABS = Object.freeze([
  { key: 'price', gid: '1876177949', labelKo: '공장제 가격표', labelEn: 'Factory Price Table' },
  { key: 'preorder', gid: '0', labelKo: '선주문 정산', labelEn: 'Pre-Order Settlement' },
  { key: 'factory', gid: '1114704757', labelKo: '공장제 매출', labelEn: 'Factory Sales' },
  { key: 'genparts', gid: '229912417', labelKo: '젠파츠 매출', labelEn: 'Gen-Parts Sales' },
  { key: 'used', gid: '17869917', labelKo: '현지중고 매출', labelEn: 'Local Used Sales' }
]);
const SALES_DEFAULT_EXCHANGE_RATE = 229;
const SALES_DEFAULT_SHIPPING_FEE_KRW = 23000;
const SALES_CNY_NAVER_SEARCH_URL =
  'https://search.naver.com/search.naver?where=nexearch&query=%EC%9C%84%EC%95%88%ED%99%94+%ED%99%98%EC%9C%A8';

const SECURITY_SECTIONS = Object.freeze(['profile', 'admins', 'logs', 'alerts']);
const MEMBER_MANAGE_SECTIONS = Object.freeze(['active', 'blocked', 'levels']);
const POINT_MANAGE_SECTIONS = Object.freeze(['signup', 'review', 'level-rates']);
const SECURITY_PAGE_SIZE = 20;
const MEMBER_PAGE_SIZE = 20;
const DEFAULT_IP_GEO_CACHE_TTL_HOURS = 24 * 30;
const DEFAULT_IP_GEO_LOOKUP_TIMEOUT_MS = 1800;
const DEFAULT_IP_GEO_MAX_LOOKUP_PER_RENDER = 8;
const IP_GEO_CACHE_TTL_HOURS = Number.isFinite(Number(process.env.IP_GEO_CACHE_TTL_HOURS))
  ? Math.max(1, Number(process.env.IP_GEO_CACHE_TTL_HOURS))
  : DEFAULT_IP_GEO_CACHE_TTL_HOURS;
const IP_GEO_LOOKUP_TIMEOUT_MS = Number.isFinite(Number(process.env.IP_GEO_LOOKUP_TIMEOUT_MS))
  ? Math.max(700, Number(process.env.IP_GEO_LOOKUP_TIMEOUT_MS))
  : DEFAULT_IP_GEO_LOOKUP_TIMEOUT_MS;
const IP_GEO_MAX_LOOKUP_PER_RENDER = Number.isFinite(Number(process.env.IP_GEO_MAX_LOOKUP_PER_RENDER))
  ? Math.max(1, Number(process.env.IP_GEO_MAX_LOOKUP_PER_RENDER))
  : DEFAULT_IP_GEO_MAX_LOOKUP_PER_RENDER;
const ipGeoLookupInFlight = new Map();
const DATE_INPUT_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const HEX_COLOR_REGEX = /^#[0-9a-fA-F]{6}$/;
const PRODUCT_GROUP_MODE = Object.freeze({
  FACTORY: 'factory',
  SIMPLE: 'simple'
});
const PRODUCT_FIELD_TYPES = new Set(['text', 'textarea', 'number']);
const PRODUCT_FILTER_BASELINE_VERSION_KEY = 'productFilterBaselineSeedV20260414';
const PRODUCT_FILTER_BASELINE_VERSION = '2026-04-14-v2';
const PRODUCT_FILTER_BASELINE_GROUP_KEYS = Object.freeze(['공장제', '젠파츠', '현지중고']);
const SALES_PRICE_FILTER_BASELINE_VERSION_KEY = 'salesWorkbookPriceFilterSeedV20260414';
const SALES_PRICE_FILTER_BASELINE_VERSION = '2026-04-14-v3';
const SALES_ORDER_WORKBOOK_SYNCED_IDS_KEY = 'salesWorkbookSyncedOrderIdsV20260417';
const SALES_ORDER_WORKBOOK_BACKFILL_VERSION_KEY = 'salesWorkbookOrderBackfillV20260417';
const SALES_ORDER_WORKBOOK_BACKFILL_VERSION = '2026-04-17-v4';
const SALES_ORDER_WORKBOOK_SYNC_MEMO_PREFIX = '[AUTO_ORDER_SYNC]';
const SALES_ORDER_WORKBOOK_MAX_SYNCED_IDS = 50000;
const SALES_ALL_DATE_TAB_KEY = 'all-sales';
const SALES_ALL_DATE_TAB_GROUP_KEY = '__all__';
const SALES_ALL_DATE_TAB_LABEL_KO = '전체 매출';
const SALES_ALL_DATE_TAB_LABEL_EN = 'All Sales';
const PRODUCT_BADGE_CODE_REGEX = /^[a-z0-9][a-z0-9-]{1,39}$/;
const PRODUCT_BADGE_CODE_MAX_LENGTH = 40;
const PRODUCT_BADGE_LABEL_MAX_LENGTH = 40;
const PRODUCT_BADGE_DEFAULT_COLOR_THEME = 'slate';
const REVIEW_POINT_DEDUCTION_BLOCK_REASON = '구매후기 삭제로인한 포인트 회수 잔고 부족';
const PRODUCT_BADGE_COLOR_THEMES = Object.freeze([
  { key: 'slate', labelKo: '기본', labelEn: 'Default' },
  { key: 'red', labelKo: '레드', labelEn: 'Red' },
  { key: 'blue', labelKo: '블루', labelEn: 'Blue' },
  { key: 'green', labelKo: '그린', labelEn: 'Green' },
  { key: 'amber', labelKo: '앰버', labelEn: 'Amber' },
  { key: 'purple', labelKo: '퍼플', labelEn: 'Purple' }
]);
const PRODUCT_BADGE_COLOR_THEME_KEY_SET = new Set(PRODUCT_BADGE_COLOR_THEMES.map((item) => item.key));
const MEMBER_LEVEL_OPERATORS = Object.freeze({
  LT: 'lt',
  LTE: 'lte',
  GT: 'gt',
  GTE: 'gte'
});

const FACTORY_DEFAULT_FIELDS = Object.freeze([
  { key: 'brand', labelKo: '브랜드', labelEn: 'Brand', type: 'text', required: true },
  { key: 'model', labelKo: '모델명', labelEn: 'Model', type: 'text', required: true },
  { key: 'sub_model', labelKo: '세부모델명', labelEn: 'Sub Model', type: 'text', required: true },
  { key: 'reference', labelKo: '레퍼런스', labelEn: 'Reference', type: 'text', required: false },
  { key: 'factory_name', labelKo: '공장', labelEn: 'Factory', type: 'text', required: false },
  { key: 'version_name', labelKo: '버전', labelEn: 'Version', type: 'text', required: false },
  { key: 'movement', labelKo: '무브먼트', labelEn: 'Movement', type: 'text', required: false },
  { key: 'case_size', labelKo: '케이스사이즈', labelEn: 'Case Size', type: 'text', required: false },
  { key: 'dial_color', labelKo: '다이얼 색', labelEn: 'Dial Color', type: 'text', required: false },
  { key: 'case_material', labelKo: '케이스 재질', labelEn: 'Case Material', type: 'text', required: false },
  { key: 'strap_material', labelKo: '브레이슬릿/스트랩 재질', labelEn: 'Bracelet/Strap Material', type: 'text', required: false },
  { key: 'features', labelKo: '특징', labelEn: 'Features', type: 'textarea', required: false },
  { key: 'price', labelKo: '가격', labelEn: 'Price', type: 'number', required: true },
  { key: 'shipping_period', labelKo: '배송기간', labelEn: 'Shipping Period', type: 'text', required: false }
]);

const COMPACT_DEFAULT_FIELDS = Object.freeze([
  { key: 'title', labelKo: '제목', labelEn: 'Title', type: 'text', required: true },
  { key: 'detailed_description', labelKo: '상세설명', labelEn: 'Detailed Description', type: 'textarea', required: true },
  { key: 'price', labelKo: '가격', labelEn: 'Price', type: 'number', required: false }
]);

const DEFAULT_THEME_COLORS = Object.freeze({
  day: Object.freeze({
    headerColor: '#0f172a',
    backgroundColor: '#f4f6fb',
    textColor: '#111827',
    mutedColor: '#5f6b7e',
    lineColor: '#d6ddea',
    cardColor: '#ffffff',
    cardDarkColor: '#0f172a',
    cardDarkTextColor: '#f8fafc',
    chipColor: '#eef2f8'
  }),
  night: Object.freeze({
    headerColor: '#0b1220',
    backgroundColor: '#070b14',
    textColor: '#f5f8ff',
    mutedColor: '#b7c3d9',
    lineColor: '#435574',
    cardColor: '#111b30',
    cardDarkColor: '#0c1424',
    cardDarkTextColor: '#f5f8ff',
    chipColor: '#1b2941'
  })
});

const LEGACY_THEME_COLORS = Object.freeze({
  day: Object.freeze({
    headerColor: '#111827',
    backgroundColor: '#f7f7f8',
    textColor: '#111213',
    mutedColor: '#6d7178',
    lineColor: '#e4e5e8',
    cardColor: '#ffffff',
    cardDarkColor: '#121318',
    cardDarkTextColor: '#f8f9fb',
    chipColor: '#f5f6f8'
  }),
  night: Object.freeze({
    headerColor: '#0f172a',
    backgroundColor: '#000000',
    textColor: '#111213',
    mutedColor: '#6d7178',
    lineColor: '#dfe2e8',
    cardColor: '#ffffff',
    cardDarkColor: '#f1f3f7',
    cardDarkTextColor: '#111213',
    chipColor: '#f3f4f6'
  })
});

const PREVIOUS_NIGHT_THEME_COLORS_V1 = Object.freeze({
  headerColor: '#ffffff',
  backgroundColor: '#000000',
  textColor: '#ffffff',
  mutedColor: '#f3f4f6',
  lineColor: '#4b5563',
  cardColor: '#4b5563',
  cardDarkColor: '#ffffff',
  cardDarkTextColor: '#000000',
  chipColor: '#0f172a'
});
const THEME_REFINED_V4_FLAG_KEY = 'themeRefinedV4Applied';
const HERO_QUICK_MENU_LIMIT = 6;
const HERO_DEFAULT_LEFT_TITLE_KO = 'Chrono Lab';
const HERO_DEFAULT_LEFT_TITLE_EN = 'Chrono Lab';
const HERO_DEFAULT_LEFT_SUBTITLE_KO = '심플하고 신뢰감 있는 시계 쇼핑 경험';
const HERO_DEFAULT_LEFT_SUBTITLE_EN = 'Simple, trustworthy watch shopping experience.';
const HERO_DEFAULT_RIGHT_TITLE_KO = '프리미엄 위치 셀렉션';
const HERO_DEFAULT_RIGHT_TITLE_EN = 'Premium Shortcut Selection';
const HERO_DEFAULT_RIGHT_SUBTITLE_KO = '결제는 계좌이체만 지원됩니다.';
const HERO_DEFAULT_RIGHT_SUBTITLE_EN = 'Bank transfer only for payment.';
const HERO_DEFAULT_LEFT_BACKGROUND_COLOR = '#eef2f8';
const HERO_DEFAULT_RIGHT_BACKGROUND_COLOR = '#0f172a';
const HERO_DEFAULT_LEFT_CTA_PATH = '/shop';
const HERO_DEFAULT_QUICK_MENUS = Object.freeze([
  { path: '/notice', labelKo: '공지사항', labelEn: 'Notice' },
  { path: '/news', labelKo: '뉴스', labelEn: 'News' },
  { path: '/shop', labelKo: '쇼핑몰', labelEn: 'Shop' },
  { path: '/qc', labelKo: 'QC', labelEn: 'QC' },
  { path: '/review', labelKo: '구매후기', labelEn: 'Reviews' },
  { path: '/inquiry', labelKo: '문의', labelEn: 'Inquiry' }
]);

const AUTH_ATTEMPT_WINDOW_MS = 10 * 60 * 1000;
const DEFAULT_AUTH_MAX_ATTEMPTS = 15;
const AUTH_ATTEMPT_BLOCK_MS = 15 * 60 * 1000;
const AUTH_ATTEMPT_IDENTIFIER_MAX_LENGTH = 120;
const AUTH_ATTEMPT_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
let lastAuthAttemptCleanupAt = 0;
const authAttemptSelectStmt = db.prepare(
  `
    SELECT attempt_count, window_started_at, blocked_until
    FROM auth_rate_limits
    WHERE bucket_key = ?
    LIMIT 1
  `
);
const authAttemptUpsertStmt = db.prepare(
  `
    INSERT INTO auth_rate_limits (
      bucket_key,
      scope_key,
      identifier,
      ip_address,
      attempt_count,
      window_started_at,
      blocked_until,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bucket_key)
    DO UPDATE SET
      scope_key = excluded.scope_key,
      identifier = excluded.identifier,
      ip_address = excluded.ip_address,
      attempt_count = excluded.attempt_count,
      window_started_at = excluded.window_started_at,
      blocked_until = excluded.blocked_until,
      updated_at = excluded.updated_at
  `
);
const authAttemptDeleteStmt = db.prepare('DELETE FROM auth_rate_limits WHERE bucket_key = ?');
const authAttemptCleanupStmt = db.prepare(
  `
    DELETE FROM auth_rate_limits
    WHERE updated_at < ?
  `
);
const EMAIL_VERIFICATION_TTL_MS = 10 * 60 * 1000;
const EMAIL_VERIFICATION_RESEND_COOLDOWN_MS = 60 * 1000;
const EMAIL_VERIFICATION_MAX_ATTEMPTS = 6;
const PASSWORD_RESET_TICKET_TTL_MS = 15 * 60 * 1000;
const ADMIN_OTP_DIGITS = 6;
const ADMIN_OTP_PERIOD_SECONDS = 30;
const ADMIN_OTP_DRIFT_WINDOWS = 1;
const ADMIN_OTP_PENDING_TTL_MS = 10 * 60 * 1000;
const ADMIN_OTP_SETUP_TTL_MS = 15 * 60 * 1000;
const ADMIN_OTP_SECRET_LENGTH = 32;
const ADMIN_OTP_BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
const ADMIN_OTP_ISSUER = String(process.env.ADMIN_OTP_ISSUER || 'Chrono LAB').trim() || 'Chrono LAB';
const emailVerificationStore = new Map();
const passwordResetTicketStore = new Map();
const DASHBOARD_STATS_CACHE_TTL_MS = Math.max(5000, Number(process.env.DASHBOARD_STATS_CACHE_TTL_MS || 30000));
const SESSION_FUNNEL_KEYS_LIMIT = 400;
const TRACKING_AUTO_POLL_MS = Math.max(60 * 1000, Number(process.env.TRACKING_AUTO_POLL_MS || 10 * 60 * 1000));
const TRACKING_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.TRACKING_REQUEST_TIMEOUT_MS || 8000)
);
const TRACKING_API_BASE = String(process.env.TRACKING_API_BASE || 'https://apis.tracker.delivery').replace(
  /\/+$/,
  ''
);

const TRACKING_CARRIERS = Object.freeze([
  { id: 'kr.cjlogistics', label: 'CJ Logistics' },
  { id: 'kr.epost', label: 'Korea Post' },
  { id: 'kr.hanjin', label: 'Hanjin' },
  { id: 'kr.logen', label: 'Logen' },
  { id: 'kr.lotte', label: 'Lotte Global' },
  { id: 'kr.cupost', label: 'CU Post' }
]);

const ALLOWED_UPLOAD_MIME = new Set([
  'image/jpeg',
  'image/png',
  'image/webp',
  'image/gif',
  'image/avif'
]);
const UPLOAD_MIME_EXTENSION_MAP = Object.freeze({
  'image/jpeg': '.jpg',
  'image/png': '.png',
  'image/webp': '.webp',
  'image/gif': '.gif',
  'image/avif': '.avif'
});
const ALLOWED_UPLOAD_EXTENSIONS = new Set(Object.values(UPLOAD_MIME_EXTENSION_MAP));
const DEFAULT_MAX_UPLOAD_FILE_SIZE_MB = 20;
const MAX_UPLOAD_IMAGE_COUNT = 20;
const configuredUploadSizeMb = Number.parseInt(String(process.env.MAX_UPLOAD_FILE_SIZE_MB || ''), 10);
const MAX_UPLOAD_FILE_SIZE_MB = Number.isInteger(configuredUploadSizeMb) && configuredUploadSizeMb > 0
  ? Math.min(30, configuredUploadSizeMb)
  : DEFAULT_MAX_UPLOAD_FILE_SIZE_MB;
const MAX_UPLOAD_FILE_SIZE_BYTES = MAX_UPLOAD_FILE_SIZE_MB * 1024 * 1024;
const WATERMARK_SUPPORTED_FORMATS = new Set(['jpeg', 'jpg', 'png', 'webp', 'avif']);
const WATERMARK_IMAGE_ALPHA = 0.18;
const WATERMARK_DOMAIN_TEXT = 'www.chronolab.co.kr';
const WATERMARK_REMOTE_FETCH_TIMEOUT_MS = 15000;
const WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS = 4;
const WATERMARK_REMOTE_FETCH_BASE_DELAY_MS = 1500;
const WATERMARK_REMOTE_FETCH_MAX_DELAY_MS = 8000;
const WATERMARK_REMOTE_FETCH_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36';
const WATERMARK_REMOTE_CURL_MAX_BUFFER_BYTES = 45 * 1024 * 1024;
const REMOTE_IMAGE_HOST_VALIDATION_CACHE_TTL_MS = 5 * 60 * 1000;
const remoteImageHostValidationCache = new Map();

let mailTransporter = null;

const uploadStorage = multer.diskStorage({
  destination: UPLOAD_DIR,
  filename: (req, file, cb) => {
    const mimeType = String(file?.mimetype || '').trim().toLowerCase();
    const safeExt = UPLOAD_MIME_EXTENSION_MAP[mimeType] || '.jpg';
    cb(null, `${Date.now()}-${Math.round(Math.random() * 1e9)}${safeExt}`);
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({ ok: true, service: 'chronolab', timestamp: new Date().toISOString() });
});

const upload = multer({
  storage: uploadStorage,
  limits: {
    fileSize: MAX_UPLOAD_FILE_SIZE_BYTES,
    files: MAX_UPLOAD_IMAGE_COUNT
  },
  fileFilter: (req, file, cb) => {
    if (!ALLOWED_UPLOAD_MIME.has(file.mimetype)) {
      cb(new Error('지원되지 않는 파일 형식입니다. JPG/PNG/WEBP/GIF/AVIF만 업로드할 수 있습니다.'));
      return;
    }
    cb(null, true);
  }
});

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.set('trust proxy', 1);
app.disable('x-powered-by');

app.use((req, res, next) => {
  if (!ADMIN_ROUTE_RANDOMIZED) {
    return next();
  }
  const currentUrl = String(req.url || '');
  if (!currentUrl.startsWith('/')) {
    return next();
  }

  const queryStartIndex = currentUrl.indexOf('?');
  const pathname = queryStartIndex >= 0 ? currentUrl.slice(0, queryStartIndex) : currentUrl;
  const queryString = queryStartIndex >= 0 ? currentUrl.slice(queryStartIndex) : '';

  if (isMappedAdminPath(pathname)) {
    const suffix = pathname.slice(ADMIN_ENTRY_PATH_PREFIX.length);
    const rewrittenPath = `${ADMIN_CANONICAL_PATH_PREFIX}${suffix}`;
    req.url = `${rewrittenPath || ADMIN_CANONICAL_PATH_PREFIX}${queryString}`;
    return next();
  }

  if (isCanonicalAdminPath(pathname)) {
    recordSecurityAlert(req, 'security.admin.hidden_route_blocked', `direct_path=${pathname.slice(0, 160)}`);
    return res.status(404).type('text/plain; charset=utf-8').send('Not Found');
  }

  return next();
});

app.use('/assets', express.static(path.join(__dirname, 'public')));
app.use('/uploads', (req, res, next) => {
  const requestedPath = String(req.path || '');
  const ext = path.extname(requestedPath).toLowerCase();
  if (ext && !ALLOWED_UPLOAD_EXTENSIONS.has(ext)) {
    return res.status(404).end();
  }
  return next();
});
app.use(
  '/uploads',
  express.static(UPLOAD_DIR, {
    setHeaders: (res, filePath) => {
      const ext = path.extname(String(filePath || '')).toLowerCase();
      if (ext === '.jpg' || ext === '.jpeg') {
        res.setHeader('content-type', 'image/jpeg');
      } else if (ext === '.png') {
        res.setHeader('content-type', 'image/png');
      } else if (ext === '.webp') {
        res.setHeader('content-type', 'image/webp');
      } else if (ext === '.gif') {
        res.setHeader('content-type', 'image/gif');
      } else if (ext === '.avif') {
        res.setHeader('content-type', 'image/avif');
      }
      res.setHeader('x-content-type-options', 'nosniff');
    }
  })
);
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '2mb' }));
app.use(cookieParser());
const sessionStore = new SQLiteSessionStore();
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    store: sessionStore,
    cookie: {
      maxAge: SESSION_DEFAULT_MAX_AGE_MS,
      httpOnly: true,
      sameSite: 'lax',
      secure: mustEnforceSecurity
    }
  })
);

app.use((req, res, next) => {
  if (!ADMIN_ROUTE_RANDOMIZED) {
    return next();
  }
  const originalRedirect = res.redirect.bind(res);
  const originalSetHeader = res.setHeader.bind(res);

  res.redirect = (statusOrUrl, urlMaybe) => {
    if (typeof statusOrUrl === 'number') {
      return originalRedirect(statusOrUrl, mapCanonicalAdminPathToEntry(urlMaybe));
    }
    return originalRedirect(mapCanonicalAdminPathToEntry(statusOrUrl));
  };

  res.setHeader = (name, value) => {
    if (String(name || '').trim().toLowerCase() === 'location' && typeof value === 'string') {
      return originalSetHeader(name, mapCanonicalAdminPathToEntry(value));
    }
    return originalSetHeader(name, value);
  };
  return next();
});

app.use((req, res, next) => {
  const incomingRequestId = String(req.get('x-request-id') || '').trim();
  const requestId = incomingRequestId && incomingRequestId.length <= 120 ? incomingRequestId : crypto.randomUUID();
  req.requestId = requestId;
  res.setHeader('x-request-id', requestId);
  return next();
});

app.use((req, res, next) => {
  if (!mustEnforceSecurity) {
    return next();
  }
  if (req.path === '/health') {
    return next();
  }
  const forwardedProto = String(req.get('x-forwarded-proto') || '')
    .split(',')[0]
    .trim()
    .toLowerCase();
  if (req.secure || forwardedProto === 'https') {
    return next();
  }
  const host = String(req.get('host') || '').trim();
  if (!host) {
    return next();
  }
  const targetPath = String(req.originalUrl || req.url || '/');
  return res.redirect(308, `https://${host}${targetPath}`);
});

const CONTENT_SECURITY_POLICY_BASE_DIRECTIVES = [
  "default-src 'self'",
  "base-uri 'self'",
  "object-src 'none'",
  "frame-ancestors 'self'",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src 'self' https://fonts.gstatic.com data:",
  "img-src 'self' data: blob: https:",
  "child-src 'self' https://*.daum.net https://*.kakao.com",
  "frame-src 'self' https://*.daum.net https://*.kakao.com",
  "connect-src 'self' https://*.daum.net https://*.kakao.com",
  "form-action 'self'"
];

function injectNonceIntoScriptTags(html, nonce) {
  const source = typeof html === 'string' ? html : '';
  if (!source || !nonce || !source.includes('<script')) {
    return html;
  }

  return source.replace(/<script\b(?![^>]*\bnonce=)([^>]*)>/gi, (match, attrs = '') => {
    return `<script${attrs} nonce="${nonce}">`;
  });
}

app.use((req, res, next) => {
  const cspNonce = crypto.randomBytes(16).toString('base64');
  res.locals.cspNonce = cspNonce;

  const originalSend = res.send.bind(res);
  res.send = (body) => {
    try {
      if (typeof body !== 'string') {
        return originalSend(body);
      }
      const contentType = String(res.getHeader('content-type') || '').toLowerCase();
      const isHtmlResponse = contentType.includes('text/html') || body.trimStart().startsWith('<!DOCTYPE html');
      if (!isHtmlResponse) {
        return originalSend(body);
      }
      const htmlWithNonce = injectNonceIntoScriptTags(body, cspNonce);
      const htmlWithAdminPathRewrite = rewriteAdminPathReferencesInHtml(htmlWithNonce);
      return originalSend(htmlWithAdminPathRewrite);
    } catch {
      return originalSend(body);
    }
  };
  return next();
});

app.use((req, res, next) => {
  const cspNonce = String(res.locals?.cspNonce || '').trim();
  const scriptSrc = cspNonce
    ? `script-src 'self' 'nonce-${cspNonce}' https://t1.daumcdn.net https://*.daumcdn.net https://*.daum.net https://*.kakao.com`
    : "script-src 'self' https://t1.daumcdn.net https://*.daumcdn.net https://*.daum.net https://*.kakao.com";
  const contentSecurityPolicy = [...CONTENT_SECURITY_POLICY_BASE_DIRECTIVES, scriptSrc].join('; ');

  res.setHeader('x-content-type-options', 'nosniff');
  res.setHeader('x-frame-options', 'SAMEORIGIN');
  res.setHeader('referrer-policy', 'strict-origin-when-cross-origin');
  res.setHeader('permissions-policy', 'geolocation=(), microphone=(), camera=()');
  res.setHeader('cross-origin-opener-policy', 'same-origin');
  res.setHeader('cross-origin-resource-policy', 'same-origin');
  res.setHeader('origin-agent-cluster', '?1');
  res.setHeader('x-permitted-cross-domain-policies', 'none');
  res.setHeader('content-security-policy', contentSecurityPolicy);
  if (mustEnforceSecurity) {
    res.setHeader('strict-transport-security', 'max-age=31536000; includeSubDomains');
  }
  return next();
});

function toKstDate() {
  const now = new Date();
  const kst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  const y = kst.getFullYear();
  const m = String(kst.getMonth() + 1).padStart(2, '0');
  const d = String(kst.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function parseMenus(rawMenus, options = {}) {
  const includeHidden = Boolean(options.includeHidden);

  try {
    const parsed = JSON.parse(rawMenus);
    if (!Array.isArray(parsed)) {
      return getDefaultMenus();
    }

    const normalizedMenus = parsed
      .filter((menu) => menu && menu.path)
      .map((menu, idx) => ({
        id: String(menu.id || `menu-${idx + 1}`),
        labelKo: String(menu.labelKo || menu.labelEn || `메뉴${idx + 1}`),
        labelEn: String(menu.labelEn || menu.labelKo || `Menu${idx + 1}`),
        path: sanitizePath(String(menu.path || '')),
        isHidden:
          menu.isHidden === true ||
          String(menu.isHidden || '').toLowerCase() === 'true' ||
          String(menu.isHidden || '') === '1'
      }))
      .filter((menu) => !menu.path.startsWith('/admin'));

    if (normalizedMenus.length === 0) {
      return getDefaultMenus();
    }

    if (includeHidden) {
      return normalizedMenus;
    }

    return normalizedMenus.filter((menu) => !menu.isHidden);
  } catch {
    return getDefaultMenus();
  }
}

function normalizeProductGroupKey(rawKey = '', fallback = '') {
  const base = String(rawKey || fallback || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[<>`"'\\/?:#]/g, '')
    .slice(0, 40);
  return base;
}

function normalizeProductFieldKey(rawKey = '', fallback = 'field') {
  const key = String(rawKey || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 40);
  return key || fallback;
}

function buildFieldKeyFromLabel(label = '', fallbackIndex = 1) {
  const normalized = normalizeProductFieldKey(label, '');
  if (normalized) {
    return normalized;
  }
  return `field_${fallbackIndex}`;
}

const PRODUCT_FIELD_KEY_ALIASES = Object.freeze({
  submodel: 'sub_model',
  factoryname: 'factory_name',
  versionname: 'version_name',
  casesize: 'case_size',
  dialcolor: 'dial_color',
  casematerial: 'case_material',
  strapmaterial: 'strap_material',
  shippingperiod: 'shipping_period',
  summary: 'detailed_description',
  description: 'detailed_description',
  detaileddescription: 'detailed_description'
});

function normalizeProductFieldAliasKey(rawKey = '') {
  const normalized = normalizeProductFieldKey(rawKey, '');
  return PRODUCT_FIELD_KEY_ALIASES[normalized] || normalized;
}

function normalizeProductFilterOption(rawOption = '') {
  return String(rawOption || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, 80);
}

function normalizeBooleanLike(rawValue, fallback = false) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return Boolean(fallback);
  }
  if (typeof rawValue === 'boolean') {
    return rawValue;
  }
  if (typeof rawValue === 'number') {
    return rawValue !== 0;
  }
  const normalized = String(rawValue).trim().toLowerCase();
  if (['1', 'true', 'on', 'yes', 'y'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'off', 'no', 'n'].includes(normalized)) {
    return false;
  }
  return Boolean(fallback);
}

function resolveRawProductFilterOptionValue(rawOption = '') {
  if (rawOption && typeof rawOption === 'object' && !Array.isArray(rawOption)) {
    return (
      rawOption.value ||
      rawOption.key ||
      rawOption.labelEn ||
      rawOption.label_en ||
      rawOption.labelKo ||
      rawOption.label_ko ||
      rawOption.label ||
      ''
    );
  }
  return rawOption;
}

const PRODUCT_FILTER_OPTION_EN_COLLATOR = new Intl.Collator('en', {
  sensitivity: 'base',
  numeric: true
});
const PRODUCT_FILTER_OPTION_KO_COLLATOR = new Intl.Collator('ko', {
  sensitivity: 'base',
  numeric: true
});

function getProductFilterOptionSortGroupRank(value = '') {
  const text = String(value || '').trim();
  if (!text) {
    return 99;
  }
  const first = text.charAt(0);
  if (/[A-Za-z0-9]/.test(first)) {
    return 0;
  }
  if (/[가-힣]/.test(first)) {
    return 2;
  }
  return 1;
}

function compareProductFilterOptionValues(left = '', right = '') {
  const leftText = String(left || '');
  const rightText = String(right || '');
  const leftRank = getProductFilterOptionSortGroupRank(leftText);
  const rightRank = getProductFilterOptionSortGroupRank(rightText);
  if (leftRank !== rightRank) {
    return leftRank - rightRank;
  }

  const collator = leftRank === 2 ? PRODUCT_FILTER_OPTION_KO_COLLATOR : PRODUCT_FILTER_OPTION_EN_COLLATOR;
  const compared = collator.compare(leftText, rightText);
  if (compared !== 0) {
    return compared;
  }
  return leftText.localeCompare(rightText, leftRank === 2 ? 'ko' : 'en');
}

function normalizeProductFilterOptionList(rawList) {
  const source = Array.isArray(rawList) ? rawList : [];
  const normalized = [];
  const seen = new Set();

  source.forEach((item) => {
    const value = normalizeProductFilterOption(resolveRawProductFilterOptionValue(item));
    if (!value) {
      return;
    }
    const dedupeKey = value.toLowerCase();
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);
    normalized.push(value);
  });

  normalized.sort(compareProductFilterOptionValues);

  return normalized;
}

function findMatchingProductFilterKey(source = {}, rawKey = '') {
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    return '';
  }
  const normalizedNeedle = normalizeProductFilterOption(rawKey);
  if (!normalizedNeedle) {
    return '';
  }
  return (
    Object.keys(source).find(
      (key) => normalizeProductFilterOption(key).toLowerCase() === normalizedNeedle.toLowerCase()
    ) || ''
  );
}

function normalizeProductFilterOptionMap(rawMap, allowedKeys = []) {
  const source = rawMap && typeof rawMap === 'object' && !Array.isArray(rawMap) ? rawMap : {};
  const allowedLookup = new Map();
  if (Array.isArray(allowedKeys) && allowedKeys.length > 0) {
    allowedKeys.forEach((item) => {
      const normalized = normalizeProductFilterOption(resolveRawProductFilterOptionValue(item));
      if (!normalized) {
        return;
      }
      allowedLookup.set(normalized.toLowerCase(), normalized);
    });
  }

  const mapped = {};
  Object.entries(source).forEach(([rawKey, rawList]) => {
    const normalizedKeySeed = normalizeProductFilterOption(resolveRawProductFilterOptionValue(rawKey));
    if (!normalizedKeySeed) {
      return;
    }
    const normalizedKey = allowedLookup.size > 0
      ? allowedLookup.get(normalizedKeySeed.toLowerCase())
      : normalizedKeySeed;
    if (!normalizedKey) {
      return;
    }

    const normalizedList = normalizeProductFilterOptionList(rawList);
    if (normalizedList.length === 0) {
      return;
    }
    mapped[normalizedKey] = normalizedList;
  });

  return Object.keys(mapped)
    .sort(compareProductFilterOptionValues)
    .reduce((acc, key) => {
      acc[key] = mapped[key];
      return acc;
    }, {});
}

function normalizeProductFilterOptionLabelEntry(rawEntry, fallbackValue = '') {
  const fallback = normalizeProductFilterOption(fallbackValue);
  if (rawEntry && typeof rawEntry === 'object' && !Array.isArray(rawEntry)) {
    const labelKo = normalizeProductFilterOption(
      rawEntry.labelKo || rawEntry.label_ko || rawEntry.ko || rawEntry.label || ''
    );
    const labelEn = normalizeProductFilterOption(
      rawEntry.labelEn || rawEntry.label_en || rawEntry.en || rawEntry.label || ''
    );
    return {
      labelKo: labelKo || fallback,
      labelEn: labelEn || fallback
    };
  }

  const normalizedLabel = normalizeProductFilterOption(rawEntry);
  const fallbackLabel = normalizedLabel || fallback;
  return {
    labelKo: fallbackLabel,
    labelEn: fallbackLabel
  };
}

function normalizeProductFilterOptionLabelMap(rawMap, allowedValues = []) {
  const source = rawMap && typeof rawMap === 'object' && !Array.isArray(rawMap) ? rawMap : {};
  const normalizedValues = normalizeProductFilterOptionList(allowedValues);
  const mapped = {};

  normalizedValues.forEach((value) => {
    const matchedKey = findMatchingProductFilterKey(source, value);
    mapped[value] = normalizeProductFilterOptionLabelEntry(
      matchedKey ? source[matchedKey] : null,
      value
    );
  });

  return mapped;
}

function normalizeProductFilterOptionLabelMapByBrand(rawMap, modelOptionMap = {}, brandOptions = []) {
  const source = rawMap && typeof rawMap === 'object' && !Array.isArray(rawMap) ? rawMap : {};
  const normalizedModelMap = normalizeProductFilterOptionMap(modelOptionMap, brandOptions);
  const mapped = {};

  Object.entries(normalizedModelMap).forEach(([brandValue, modelValues]) => {
    const matchedBrandKey = findMatchingProductFilterKey(source, brandValue);
    const rawBrandMap =
      matchedBrandKey && source[matchedBrandKey] && typeof source[matchedBrandKey] === 'object' && !Array.isArray(source[matchedBrandKey])
        ? source[matchedBrandKey]
        : {};
    mapped[brandValue] = normalizeProductFilterOptionLabelMap(rawBrandMap, modelValues);
  });

  return mapped;
}

function getProductFilterLabelByLang(labelMap = {}, rawValue = '', lang = 'ko') {
  const value = normalizeProductFilterOption(rawValue);
  if (!value) {
    return '';
  }
  const matchedKey = findMatchingProductFilterKey(labelMap, value);
  if (!matchedKey) {
    return value;
  }
  const entry = normalizeProductFilterOptionLabelEntry(labelMap[matchedKey], value);
  return lang === 'en' ? entry.labelEn || entry.labelKo || value : entry.labelKo || entry.labelEn || value;
}

function getProductFilterOptionItems(values = [], labelMap = {}, lang = 'ko', options = {}) {
  const normalizedValues = normalizeProductFilterOptionList(values);
  const items = normalizedValues.map((value) => ({
    value,
    label: getProductFilterLabelByLang(labelMap, value, lang)
  }));
  const sortByLabel =
    options === true ||
    (options && typeof options === 'object' && options.sortByLabel === true);
  if (sortByLabel) {
    items.sort((left, right) => {
      const labelCompared = compareProductFilterOptionValues(left.label, right.label);
      if (labelCompared !== 0) {
        return labelCompared;
      }
      return compareProductFilterOptionValues(left.value, right.value);
    });
  }
  return items;
}

function getGroupBrandOptions(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return [];
  }
  return normalizeProductFilterOptionList(groupConfig.brandOptions);
}

function getGroupBrandOptionLabels(groupConfig = null) {
  const brandOptions = getGroupBrandOptions(groupConfig);
  if (!groupConfig || typeof groupConfig !== 'object' || brandOptions.length === 0) {
    return {};
  }
  return normalizeProductFilterOptionLabelMap(groupConfig.brandOptionLabels, brandOptions);
}

function getGroupFactoryOptions(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return [];
  }
  return normalizeProductFilterOptionList(groupConfig.factoryOptions);
}

function getGroupFactoryOptionLabels(groupConfig = null) {
  const factoryOptions = getGroupFactoryOptions(groupConfig);
  if (!groupConfig || typeof groupConfig !== 'object' || factoryOptions.length === 0) {
    return {};
  }
  return normalizeProductFilterOptionLabelMap(groupConfig.factoryOptionLabels, factoryOptions);
}

function getGroupModelOptions(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return [];
  }
  return normalizeProductFilterOptionList(groupConfig.modelOptions);
}

function getGroupModelOptionsByBrand(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return {};
  }
  const brandOptions = getGroupBrandOptions(groupConfig);
  if (brandOptions.length === 0) {
    return {};
  }
  return normalizeProductFilterOptionMap(groupConfig.modelOptionsByBrand, brandOptions);
}

function getGroupModelOptionLabelsByBrand(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return {};
  }
  const brandOptions = getGroupBrandOptions(groupConfig);
  const modelOptionMap = getGroupModelOptionsByBrand(groupConfig);
  return normalizeProductFilterOptionLabelMapByBrand(
    groupConfig.modelOptionLabelsByBrand,
    modelOptionMap,
    brandOptions
  );
}

function getGroupModelOptionLabelsForBrand(groupConfig = null, rawBrandOption = '') {
  const modelLabelMapByBrand = getGroupModelOptionLabelsByBrand(groupConfig);
  const matchedBrandKey = findMatchingProductFilterKey(modelLabelMapByBrand, rawBrandOption);
  if (!matchedBrandKey) {
    return {};
  }
  const source = modelLabelMapByBrand[matchedBrandKey];
  return source && typeof source === 'object' && !Array.isArray(source) ? source : {};
}

function getGroupModelOptionsForBrand(groupConfig = null, rawBrandOption = '') {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return [];
  }

  const normalizedBrand = normalizeProductFilterOption(rawBrandOption);
  const modelMap = getGroupModelOptionsByBrand(groupConfig);
  const mapKeys = Object.keys(modelMap);

  if (normalizedBrand) {
    const matchedKey = mapKeys.find((item) => item.toLowerCase() === normalizedBrand.toLowerCase());
    if (matchedKey) {
      return modelMap[matchedKey];
    }
    if (mapKeys.length > 0) {
      return [];
    }
  }

  if (!normalizedBrand && mapKeys.length > 0) {
    return [];
  }

  return getGroupModelOptions(groupConfig);
}

function getGroupAllModelOptions(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return [];
  }

  const modelMap = getGroupModelOptionsByBrand(groupConfig);
  const flattened = normalizeProductFilterOptionList(
    Object.values(modelMap).flat()
  );
  if (flattened.length > 0) {
    return flattened;
  }

  return getGroupModelOptions(groupConfig);
}

function getDefaultGroupFilterSeeds() {
  const defaults = getDefaultProductGroupConfigs();
  const factorySource = defaults.find((group) => isFactoryTemplateGroup(group)) || defaults[0] || {};
  const simpleSource = defaults.find((group) => !isFactoryTemplateGroup(group)) || defaults[0] || {};

  return {
    factory: {
      brandOptions: normalizeProductFilterOptionList(factorySource.brandOptions),
      factoryOptions: normalizeProductFilterOptionList(factorySource.factoryOptions),
      modelOptions: normalizeProductFilterOptionList(factorySource.modelOptions),
      modelOptionsByBrand: normalizeProductFilterOptionMap(
        factorySource.modelOptionsByBrand,
        normalizeProductFilterOptionList(factorySource.brandOptions)
      ),
      brandOptionLabels: normalizeProductFilterOptionLabelMap(
        factorySource.brandOptionLabels,
        normalizeProductFilterOptionList(factorySource.brandOptions)
      ),
      factoryOptionLabels: normalizeProductFilterOptionLabelMap(
        factorySource.factoryOptionLabels,
        normalizeProductFilterOptionList(factorySource.factoryOptions)
      ),
      modelOptionLabelsByBrand: normalizeProductFilterOptionLabelMapByBrand(
        factorySource.modelOptionLabelsByBrand,
        normalizeProductFilterOptionMap(
          factorySource.modelOptionsByBrand,
          normalizeProductFilterOptionList(factorySource.brandOptions)
        ),
        normalizeProductFilterOptionList(factorySource.brandOptions)
      )
    },
    simple: {
      brandOptions: normalizeProductFilterOptionList(simpleSource.brandOptions),
      factoryOptions: [],
      modelOptions: normalizeProductFilterOptionList(simpleSource.modelOptions),
      modelOptionsByBrand: normalizeProductFilterOptionMap(
        simpleSource.modelOptionsByBrand,
        normalizeProductFilterOptionList(simpleSource.brandOptions)
      ),
      brandOptionLabels: normalizeProductFilterOptionLabelMap(
        simpleSource.brandOptionLabels,
        normalizeProductFilterOptionList(simpleSource.brandOptions)
      ),
      factoryOptionLabels: {},
      modelOptionLabelsByBrand: normalizeProductFilterOptionLabelMapByBrand(
        simpleSource.modelOptionLabelsByBrand,
        normalizeProductFilterOptionMap(
          simpleSource.modelOptionsByBrand,
          normalizeProductFilterOptionList(simpleSource.brandOptions)
        ),
        normalizeProductFilterOptionList(simpleSource.brandOptions)
      )
    }
  };
}

function buildMergedProductFilterOptionMap(currentMap = {}, baselineMap = {}, allowedBrands = []) {
  const normalizedCurrent = normalizeProductFilterOptionMap(currentMap, allowedBrands);
  const normalizedBaseline = normalizeProductFilterOptionMap(baselineMap, allowedBrands);
  const merged = {};

  allowedBrands.forEach((brandValue) => {
    const currentList = normalizedCurrent[brandValue] || [];
    const baselineList = normalizedBaseline[brandValue] || [];
    const mergedList = normalizeProductFilterOptionList([...currentList, ...baselineList]);
    if (mergedList.length > 0) {
      merged[brandValue] = mergedList;
    }
  });

  return normalizeProductFilterOptionMap(merged, allowedBrands);
}

function cloneFieldTemplate(template = []) {
  return template.map((field) => ({ ...field }));
}

function getFactoryDefaultFields() {
  return cloneFieldTemplate(FACTORY_DEFAULT_FIELDS);
}

function getCompactDefaultFields() {
  return cloneFieldTemplate(COMPACT_DEFAULT_FIELDS);
}

function ensureSimpleBaselineFields(fields = []) {
  const source = Array.isArray(fields) ? fields : [];
  const next = source.map((field) => ({ ...field }));
  const existingFieldKeySet = new Set(
    next
      .map((field) => normalizeProductFieldAliasKey(field?.key || ''))
      .filter(Boolean)
  );

  COMPACT_DEFAULT_FIELDS.forEach((baselineField) => {
    const baselineKey = normalizeProductFieldAliasKey(baselineField.key || '');
    if (!baselineKey || existingFieldKeySet.has(baselineKey)) {
      return;
    }
    next.push({ ...baselineField });
    existingFieldKeySet.add(baselineKey);
  });

  return next;
}

function isFactoryTemplateGroup(group = {}) {
  const key = String(group.key || '').trim();
  const labelKo = String(group.labelKo || '').trim();
  const labelEn = String(group.labelEn || '').trim().toLowerCase();
  const mode = String(group.mode || '').trim().toLowerCase();
  return (
    mode === PRODUCT_GROUP_MODE.FACTORY ||
    key === '공장제' ||
    labelKo === '공장제' ||
    labelEn.includes('factory')
  );
}

function normalizeProductGroupMatchKey(rawValue = '') {
  return String(rawValue || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[-_]/g, '')
    .replace(/[^a-z0-9가-힣]/g, '');
}

function isDomesticStockGroup(group = null) {
  const candidates =
    group && typeof group === 'object' && !Array.isArray(group)
      ? [group.key, group.labelKo, group.labelEn]
      : [group];
  const normalizedKeys = candidates
    .map((value) => normalizeProductGroupMatchKey(value))
    .filter(Boolean);

  return normalizedKeys.some((value) => value === '국내재고' || value === 'domesticstock');
}

function resolveGroupMainTopBoxState(groupConfig = null) {
  const safeGroup = groupConfig && typeof groupConfig === 'object' ? groupConfig : {};
  const fallback = isDomesticStockGroup(safeGroup);
  return normalizeBooleanLike(safeGroup.showInMainTopBox, fallback);
}

function getGroupFilterToggleState(groupConfig = null) {
  const safeGroup = groupConfig && typeof groupConfig === 'object' ? groupConfig : {};
  const factoryEnabledFallback = (
    String(safeGroup.mode || '').trim().toLowerCase() === PRODUCT_GROUP_MODE.FACTORY ||
    String(safeGroup.key || '').trim() === '현지중고' ||
    isDomesticStockGroup(safeGroup) ||
    normalizeProductFilterOptionList(safeGroup.factoryOptions).length > 0
  );

  return {
    brand: normalizeBooleanLike(safeGroup.enableBrandFilter, true),
    model: normalizeBooleanLike(safeGroup.enableModelFilter, true),
    factory: normalizeBooleanLike(safeGroup.enableFactoryFilter, factoryEnabledFallback)
  };
}

function getGroupDefaultFields(group = {}) {
  if (isFactoryTemplateGroup(group)) {
    return getFactoryDefaultFields();
  }
  return getCompactDefaultFields();
}

function isFactoryLikeFields(fields = []) {
  const keys = new Set(
    fields
      .map((field) => normalizeProductFieldAliasKey(field?.key || ''))
      .filter(Boolean)
  );
  return keys.has('brand') && keys.has('model') && keys.has('sub_model');
}

function isFactoryLikeGroup(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return false;
  }
  if (isFactoryLikeFields(Array.isArray(groupConfig.customFields) ? groupConfig.customFields : [])) {
    return true;
  }
  const key = String(groupConfig.key || '').trim();
  return key === '공장제';
}

function getNormalizedExtraFieldValueMap(rawExtraFieldsJson) {
  const raw = parseProductExtraFields(rawExtraFieldsJson);
  const normalized = {};
  Object.entries(raw).forEach(([rawKey, rawValue]) => {
    const key = normalizeProductFieldAliasKey(rawKey);
    if (!key) {
      return;
    }
    normalized[key] = String(rawValue ?? '')
      .trim()
      .slice(0, 2000);
  });
  return normalized;
}

function getProductFieldValues(product, groupConfig) {
  const safeProduct = product || {};
  const extraValues = getNormalizedExtraFieldValueMap(safeProduct.extra_fields_json);
  const legacyValues = {
    title: `${safeProduct.brand || ''} ${safeProduct.model || ''}`.trim(),
    detailed_description: String(safeProduct.features || safeProduct.sub_model || '').trim(),
    brand: String(safeProduct.brand || '').trim(),
    model: String(safeProduct.model || '').trim(),
    sub_model: String(safeProduct.sub_model || '').trim(),
    reference: String(safeProduct.reference || '').trim(),
    factory_name: String(safeProduct.factory_name || '').trim(),
    version_name: String(safeProduct.version_name || '').trim(),
    movement: String(safeProduct.movement || '').trim(),
    case_size: String(safeProduct.case_size || '').trim(),
    dial_color: String(safeProduct.dial_color || '').trim(),
    case_material: String(safeProduct.case_material || '').trim(),
    strap_material: String(safeProduct.strap_material || '').trim(),
    features: String(safeProduct.features || '').trim(),
    price: Number(safeProduct.price || 0) > 0 ? String(Number(safeProduct.price || 0)) : '',
    shipping_period: String(safeProduct.shipping_period || '').trim()
  };

  const merged = { ...legacyValues, ...extraValues };
  const fields = Array.isArray(groupConfig?.customFields) ? groupConfig.customFields : [];
  const values = {};

  fields.forEach((field) => {
    const key = normalizeProductFieldAliasKey(field.key || '');
    if (!key) {
      return;
    }
    values[key] = String(merged[key] || '').trim();
  });

  return values;
}

function normalizeFieldInputValue(rawValue, fieldType = 'text') {
  const raw = String(rawValue ?? '').trim();
  if (!raw) {
    return '';
  }

  if (fieldType === 'number') {
    const numeric = raw.replace(/[^0-9-]/g, '');
    if (!numeric) {
      return '';
    }
    const parsed = Number.parseInt(numeric, 10);
    return Number.isFinite(parsed) ? String(parsed) : '';
  }

  const limit = fieldType === 'textarea' ? 2000 : 300;
  return raw.slice(0, limit);
}

function parseProductFieldValuesFromBody(rawBody, groupConfig) {
  const body = rawBody && typeof rawBody === 'object' ? rawBody : {};
  const rawFieldValues =
    body.fieldValues && typeof body.fieldValues === 'object' && !Array.isArray(body.fieldValues)
      ? body.fieldValues
      : body.extraFields && typeof body.extraFields === 'object' && !Array.isArray(body.extraFields)
        ? body.extraFields
        : {};

  const normalizedRawMap = {};
  Object.entries(rawFieldValues).forEach(([rawKey, rawValue]) => {
    const key = normalizeProductFieldAliasKey(rawKey);
    if (!key) {
      return;
    }
    normalizedRawMap[key] = String(rawValue ?? '');
  });

  const legacyBodyValues = {
    brand: body.selectedBrandOption || body.brand,
    model: body.selectedModelOption || body.model,
    sub_model: body.subModel || body.sub_model,
    reference: body.reference,
    factory_name: body.selectedFactoryOption || body.factoryName || body.factory_name,
    version_name: body.versionName || body.version_name,
    movement: body.movement,
    case_size: body.caseSize || body.case_size,
    dial_color: body.dialColor || body.dial_color,
    case_material: body.caseMaterial || body.case_material,
    strap_material: body.strapMaterial || body.strap_material,
    features: body.features,
    price: body.price,
    shipping_period: body.shippingPeriod || body.shipping_period,
    title: body.title,
    detailed_description: body.detailedDescription || body.summary || body.description
  };

  const fieldDefinitions = Array.isArray(groupConfig?.customFields) ? groupConfig.customFields : [];
  const brandOptions = getGroupBrandOptions(groupConfig);
  const factoryOptions = getGroupFactoryOptions(groupConfig);
  const modelOptions = getGroupAllModelOptions(groupConfig);
  const values = {};

  for (const field of fieldDefinitions) {
    const key = normalizeProductFieldAliasKey(field.key || '');
    if (!key) {
      // eslint-disable-next-line no-continue
      continue;
    }

    const rawValue = normalizedRawMap[key] ?? legacyBodyValues[key] ?? '';
    const value = normalizeFieldInputValue(rawValue, field.type);
    const ignoreRequiredByFilterConfig =
      (key === 'brand' && brandOptions.length === 0) ||
      (key === 'factory_name' && factoryOptions.length === 0) ||
      (key === 'model' && modelOptions.length > 0);
    if (field.required && !value && !ignoreRequiredByFilterConfig) {
      return {
        error: `${field.labelKo || field.key} 항목은 필수입니다.`,
        values: {}
      };
    }
    values[key] = value;
  }

  return { error: '', values };
}

function mapFieldValuesToProductColumns(fieldValues, groupConfig) {
  const selectedGroupLabel = String(groupConfig?.labelKo || groupConfig?.key || '상품').trim();
  const title = String(fieldValues.title || '').trim();
  const detailedDescription = String(
    fieldValues.detailed_description || fieldValues.features || ''
  ).trim();
  const brand = String(fieldValues.brand || title || '').trim();
  const model = String(fieldValues.model || selectedGroupLabel || '').trim();
  const subModel = String(fieldValues.sub_model || detailedDescription || '').trim();

  return {
    brand,
    model,
    subModel,
    reference: String(fieldValues.reference || '').trim(),
    factoryName: String(fieldValues.factory_name || '').trim(),
    versionName: String(fieldValues.version_name || '').trim(),
    movement: String(fieldValues.movement || '').trim(),
    caseSize: String(fieldValues.case_size || '').trim(),
    dialColor: String(fieldValues.dial_color || '').trim(),
    caseMaterial: String(fieldValues.case_material || '').trim(),
    strapMaterial: String(fieldValues.strap_material || '').trim(),
    features: String(fieldValues.features || detailedDescription || '').trim(),
    price: parsePositiveInt(fieldValues.price, 0),
    shippingPeriod: String(fieldValues.shipping_period || '').trim()
  };
}

function normalizeProductGroupConfigs(rawValue) {
  const fallback = getDefaultProductGroupConfigs();
  let parsed = [];

  try {
    if (typeof rawValue === 'string') {
      const maybeParsed = JSON.parse(rawValue);
      if (Array.isArray(maybeParsed)) {
        parsed = maybeParsed;
      }
    } else if (Array.isArray(rawValue)) {
      parsed = rawValue;
    }
  } catch {
    parsed = [];
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    parsed = fallback;
  }

  const normalizedGroups = [];
  const usedGroupKeys = new Set();

  parsed.forEach((group, idx) => {
    if (!group || typeof group !== 'object') {
      return;
    }

    const fallbackGroup = fallback[idx] || fallback[0] || {};

    const labelKo = String(group.labelKo || group.labelEn || fallbackGroup?.labelKo || `분류 ${idx + 1}`)
      .trim()
      .slice(0, 60);
    const labelEn = String(group.labelEn || group.labelKo || fallbackGroup?.labelEn || `Category ${idx + 1}`)
      .trim()
      .slice(0, 60);

    const keySeed = normalizeProductGroupKey(group.key || labelKo || labelEn, fallbackGroup?.key || `분류${idx + 1}`);
    if (!keySeed) {
      return;
    }

    let key = keySeed;
    let suffix = 2;
    while (usedGroupKeys.has(key)) {
      key = normalizeProductGroupKey(`${keySeed} ${suffix}`, `${keySeed}-${suffix}`) || `${keySeed}-${suffix}`;
      suffix += 1;
    }
    usedGroupKeys.add(key);

    const defaultFieldTemplate = getGroupDefaultFields({
      key,
      labelKo,
      labelEn,
      mode: group.mode || fallbackGroup.mode || ''
    });
    const hasExplicitBrandOptions = Object.prototype.hasOwnProperty.call(group, 'brandOptions');
    const hasExplicitFactoryOptions = Object.prototype.hasOwnProperty.call(group, 'factoryOptions');
    const hasExplicitModelOptions = Object.prototype.hasOwnProperty.call(group, 'modelOptions');
    const hasExplicitModelOptionsByBrand = Object.prototype.hasOwnProperty.call(group, 'modelOptionsByBrand');
    const hasExplicitBrandOptionLabels = Object.prototype.hasOwnProperty.call(group, 'brandOptionLabels');
    const hasExplicitFactoryOptionLabels = Object.prototype.hasOwnProperty.call(group, 'factoryOptionLabels');
    const hasExplicitModelOptionLabelsByBrand = Object.prototype.hasOwnProperty.call(group, 'modelOptionLabelsByBrand');
    const hasExplicitBrandFilterToggle = Object.prototype.hasOwnProperty.call(group, 'enableBrandFilter');
    const hasExplicitModelFilterToggle = Object.prototype.hasOwnProperty.call(group, 'enableModelFilter');
    const hasExplicitFactoryFilterToggle = Object.prototype.hasOwnProperty.call(group, 'enableFactoryFilter');
    const hasExplicitMainTopBoxToggle = Object.prototype.hasOwnProperty.call(group, 'showInMainTopBox');
    const fallbackBrandOptions = normalizeProductFilterOptionList(fallbackGroup.brandOptions);
    const fallbackFactoryOptions = normalizeProductFilterOptionList(fallbackGroup.factoryOptions);
    const fallbackModelOptions = normalizeProductFilterOptionList(fallbackGroup.modelOptions);
    const sourceFields =
      Array.isArray(group.customFields) && group.customFields.length > 0
        ? group.customFields
        : Array.isArray(fallbackGroup.customFields) && fallbackGroup.customFields.length > 0
          ? fallbackGroup.customFields
          : defaultFieldTemplate;

    const usedFieldKeys = new Set();
    let customFields = sourceFields
      .filter((field) => field && typeof field === 'object')
      .map((field, fieldIndex) => {
        const fieldLabelKo = String(field.labelKo || field.labelEn || `항목 ${fieldIndex + 1}`)
          .trim()
          .slice(0, 60);
        const fieldLabelEn = String(field.labelEn || field.labelKo || `Field ${fieldIndex + 1}`)
          .trim()
          .slice(0, 60);
        const rawType = String(field.type || 'text').trim().toLowerCase();
        const type = PRODUCT_FIELD_TYPES.has(rawType) ? rawType : 'text';
        const required =
          field.required === true ||
          String(field.required || '').toLowerCase() === 'true' ||
          String(field.required || '') === '1';

        const requestedKey = normalizeProductFieldAliasKey(
          field.key || buildFieldKeyFromLabel(fieldLabelEn || fieldLabelKo, fieldIndex + 1)
        );
        const baseFieldKey = normalizeProductFieldKey(requestedKey, `field_${fieldIndex + 1}`);

        let finalFieldKey = baseFieldKey;
        let fieldSuffix = 2;
        while (usedFieldKeys.has(finalFieldKey)) {
          finalFieldKey = normalizeProductFieldKey(`${baseFieldKey}_${fieldSuffix}`, `${baseFieldKey}_${fieldSuffix}`);
          fieldSuffix += 1;
        }
        usedFieldKeys.add(finalFieldKey);

        return {
          key: finalFieldKey,
          labelKo: fieldLabelKo,
          labelEn: fieldLabelEn,
          type,
          required
        };
      });

    if (customFields.length === 0) {
      customFields = defaultFieldTemplate;
    }

    const requestedMode = String(group.mode || fallbackGroup.mode || '').trim().toLowerCase();
    const mode = (
      requestedMode === PRODUCT_GROUP_MODE.FACTORY ||
      isFactoryTemplateGroup({ key, labelKo, labelEn, mode: requestedMode }) ||
      isFactoryLikeFields(customFields)
    )
      ? PRODUCT_GROUP_MODE.FACTORY
      : PRODUCT_GROUP_MODE.SIMPLE;
    if (mode === PRODUCT_GROUP_MODE.SIMPLE) {
      customFields = ensureSimpleBaselineFields(customFields);
    }
    const brandOptions = hasExplicitBrandOptions
      ? normalizeProductFilterOptionList(group.brandOptions)
      : fallbackBrandOptions;
    const factoryOptions = hasExplicitFactoryOptions
      ? normalizeProductFilterOptionList(group.factoryOptions)
      : fallbackFactoryOptions;
    let modelOptions = hasExplicitModelOptions
      ? normalizeProductFilterOptionList(group.modelOptions)
      : fallbackModelOptions;
    const modelOptionsByBrand = hasExplicitModelOptionsByBrand
      ? normalizeProductFilterOptionMap(group.modelOptionsByBrand, brandOptions)
      : normalizeProductFilterOptionMap(fallbackGroup.modelOptionsByBrand, brandOptions);
    if (Object.keys(modelOptionsByBrand).length > 0) {
      modelOptions = [];
    }
    const fallbackBrandOptionLabels = normalizeProductFilterOptionLabelMap(
      fallbackGroup.brandOptionLabels,
      brandOptions
    );
    const fallbackFactoryOptionLabels = normalizeProductFilterOptionLabelMap(
      fallbackGroup.factoryOptionLabels,
      factoryOptions
    );
    const fallbackModelOptionLabelsByBrand = normalizeProductFilterOptionLabelMapByBrand(
      fallbackGroup.modelOptionLabelsByBrand,
      modelOptionsByBrand,
      brandOptions
    );
    const brandOptionLabels = hasExplicitBrandOptionLabels
      ? normalizeProductFilterOptionLabelMap(group.brandOptionLabels, brandOptions)
      : fallbackBrandOptionLabels;
    const factoryOptionLabels = hasExplicitFactoryOptionLabels
      ? normalizeProductFilterOptionLabelMap(group.factoryOptionLabels, factoryOptions)
      : fallbackFactoryOptionLabels;
    const modelOptionLabelsByBrand = hasExplicitModelOptionLabelsByBrand
      ? normalizeProductFilterOptionLabelMapByBrand(
          group.modelOptionLabelsByBrand,
          modelOptionsByBrand,
          brandOptions
        )
      : fallbackModelOptionLabelsByBrand;
    const factoryFilterEnabledDefault = (
      mode === PRODUCT_GROUP_MODE.FACTORY ||
      key === '현지중고' ||
      isDomesticStockGroup({ key, labelKo, labelEn }) ||
      factoryOptions.length > 0
    );
    const brandFilterEnabled = hasExplicitBrandFilterToggle
      ? normalizeBooleanLike(group.enableBrandFilter, true)
      : true;
    const modelFilterEnabled = hasExplicitModelFilterToggle
      ? normalizeBooleanLike(group.enableModelFilter, true)
      : true;
    const factoryFilterEnabled = hasExplicitFactoryFilterToggle
      ? normalizeBooleanLike(group.enableFactoryFilter, factoryFilterEnabledDefault)
      : factoryFilterEnabledDefault;
    const mainTopBoxEnabledDefault = resolveGroupMainTopBoxState({
      key,
      labelKo,
      labelEn,
      showInMainTopBox: fallbackGroup.showInMainTopBox
    });
    const showInMainTopBox = hasExplicitMainTopBoxToggle
      ? normalizeBooleanLike(group.showInMainTopBox, mainTopBoxEnabledDefault)
      : mainTopBoxEnabledDefault;

    normalizedGroups.push({
      key,
      labelKo: labelKo || key,
      labelEn: labelEn || key,
      mode,
      showInMainTopBox,
      enableBrandFilter: brandFilterEnabled,
      enableModelFilter: modelFilterEnabled,
      enableFactoryFilter: factoryFilterEnabled,
      brandOptions,
      factoryOptions,
      modelOptions,
      modelOptionsByBrand,
      brandOptionLabels,
      factoryOptionLabels,
      modelOptionLabelsByBrand,
      customFields
    });
  });

  if (normalizedGroups.length === 0) {
    return fallback;
  }

  // Keep domestic stock factory filters aligned with factory group options.
  const factoryGroupIndex = normalizedGroups.findIndex((group) => isFactoryTemplateGroup(group));
  const domesticGroupIndex = normalizedGroups.findIndex((group) => isDomesticStockGroup(group));
  if (factoryGroupIndex >= 0 && domesticGroupIndex >= 0) {
    const factoryGroup = normalizedGroups[factoryGroupIndex] || {};
    const domesticGroup = normalizedGroups[domesticGroupIndex] || {};
    const mergedFactoryOptions = normalizeProductFilterOptionList([
      ...(Array.isArray(domesticGroup.factoryOptions) ? domesticGroup.factoryOptions : []),
      ...(Array.isArray(factoryGroup.factoryOptions) ? factoryGroup.factoryOptions : [])
    ]);
    const mergedFactoryOptionLabels = normalizeProductFilterOptionLabelMap(
      {
        ...(factoryGroup.factoryOptionLabels && typeof factoryGroup.factoryOptionLabels === 'object'
          ? factoryGroup.factoryOptionLabels
          : {}),
        ...(domesticGroup.factoryOptionLabels && typeof domesticGroup.factoryOptionLabels === 'object'
          ? domesticGroup.factoryOptionLabels
          : {})
      },
      mergedFactoryOptions
    );

    if (
      JSON.stringify(mergedFactoryOptions) !== JSON.stringify(domesticGroup.factoryOptions || []) ||
      JSON.stringify(mergedFactoryOptionLabels) !== JSON.stringify(domesticGroup.factoryOptionLabels || {}) ||
      domesticGroup.enableFactoryFilter !== true
    ) {
      normalizedGroups[domesticGroupIndex] = {
        ...domesticGroup,
        enableFactoryFilter: true,
        factoryOptions: mergedFactoryOptions,
        factoryOptionLabels: mergedFactoryOptionLabels
      };
    }
  }

  return normalizedGroups;
}

function getProductGroupConfigs() {
  const fallback = getDefaultProductGroupConfigs();
  const raw = getSetting('productGroupConfigs', JSON.stringify(fallback));
  const normalized = normalizeProductGroupConfigs(raw);
  const normalizedJson = JSON.stringify(normalized);
  if (String(raw || '') !== normalizedJson) {
    setSetting('productGroupConfigs', normalizedJson);
  }
  return normalized;
}

function setProductGroupConfigs(configs) {
  const normalized = normalizeProductGroupConfigs(configs);
  setSetting('productGroupConfigs', JSON.stringify(normalized));
  return normalized;
}

function applyProductFilterBaselineSeedOnce() {
  const currentVersion = String(getSetting(PRODUCT_FILTER_BASELINE_VERSION_KEY, '') || '').trim();
  if (currentVersion === PRODUCT_FILTER_BASELINE_VERSION) {
    return;
  }

  const baselineByKey = new Map(
    getDefaultProductGroupConfigs()
      .map((group) => [normalizeProductGroupKey(group?.key || ''), group])
      .filter(([groupKey, group]) => Boolean(groupKey && group && typeof group === 'object'))
  );
  if (baselineByKey.size === 0) {
    setSetting(PRODUCT_FILTER_BASELINE_VERSION_KEY, PRODUCT_FILTER_BASELINE_VERSION);
    return;
  }

  const currentConfigs = getProductGroupConfigs();
  const nextConfigs = [...currentConfigs];
  const genpartsKey = normalizeProductGroupKey('젠파츠', '젠파츠');
  let changed = false;

  PRODUCT_FILTER_BASELINE_GROUP_KEYS.forEach((rawGroupKey) => {
    const normalizedGroupKey = normalizeProductGroupKey(rawGroupKey, rawGroupKey);
    if (!normalizedGroupKey) {
      return;
    }

    const baselineGroup = baselineByKey.get(normalizedGroupKey);
    if (!baselineGroup) {
      return;
    }

    const targetIndex = nextConfigs.findIndex(
      (group) => normalizeProductGroupKey(group?.key || '') === normalizedGroupKey
    );

    if (targetIndex < 0) {
      nextConfigs.push(JSON.parse(JSON.stringify(baselineGroup)));
      changed = true;
      return;
    }

    const targetGroup = nextConfigs[targetIndex] || {};
    const baselineBrandOptions = normalizeProductFilterOptionList(baselineGroup.brandOptions);
    const currentBrandOptions = normalizeProductFilterOptionList(targetGroup.brandOptions);
    const mergedBrandOptions = normalizeProductFilterOptionList([
      ...currentBrandOptions,
      ...baselineBrandOptions
    ]);

    const mergedModelOptionsByBrand = buildMergedProductFilterOptionMap(
      targetGroup.modelOptionsByBrand,
      baselineGroup.modelOptionsByBrand,
      mergedBrandOptions
    );
    const hasModelOptionsByBrand = Object.keys(mergedModelOptionsByBrand).length > 0;

    const baselineBrandLabelRaw =
      baselineGroup.brandOptionLabels &&
      typeof baselineGroup.brandOptionLabels === 'object' &&
      !Array.isArray(baselineGroup.brandOptionLabels)
        ? baselineGroup.brandOptionLabels
        : {};
    const currentBrandLabelRaw =
      targetGroup.brandOptionLabels &&
      typeof targetGroup.brandOptionLabels === 'object' &&
      !Array.isArray(targetGroup.brandOptionLabels)
        ? targetGroup.brandOptionLabels
        : {};
    const mergedBrandOptionLabels = normalizeProductFilterOptionLabelMap(
      { ...baselineBrandLabelRaw, ...currentBrandLabelRaw },
      mergedBrandOptions
    );

    const baselineModelLabelRawByBrand =
      baselineGroup.modelOptionLabelsByBrand &&
      typeof baselineGroup.modelOptionLabelsByBrand === 'object' &&
      !Array.isArray(baselineGroup.modelOptionLabelsByBrand)
        ? baselineGroup.modelOptionLabelsByBrand
        : {};
    const currentModelLabelRawByBrand =
      targetGroup.modelOptionLabelsByBrand &&
      typeof targetGroup.modelOptionLabelsByBrand === 'object' &&
      !Array.isArray(targetGroup.modelOptionLabelsByBrand)
        ? targetGroup.modelOptionLabelsByBrand
        : {};
    const rawMergedModelLabelsByBrand = {};
    Object.keys(mergedModelOptionsByBrand).forEach((brandValue) => {
      const baselineBrandLabelKey = findMatchingProductFilterKey(baselineModelLabelRawByBrand, brandValue);
      const currentBrandLabelKey = findMatchingProductFilterKey(currentModelLabelRawByBrand, brandValue);
      const baselineBrandLabelMap =
        baselineBrandLabelKey &&
        baselineModelLabelRawByBrand[baselineBrandLabelKey] &&
        typeof baselineModelLabelRawByBrand[baselineBrandLabelKey] === 'object' &&
        !Array.isArray(baselineModelLabelRawByBrand[baselineBrandLabelKey])
          ? baselineModelLabelRawByBrand[baselineBrandLabelKey]
          : {};
      const currentBrandLabelMap =
        currentBrandLabelKey &&
        currentModelLabelRawByBrand[currentBrandLabelKey] &&
        typeof currentModelLabelRawByBrand[currentBrandLabelKey] === 'object' &&
        !Array.isArray(currentModelLabelRawByBrand[currentBrandLabelKey])
          ? currentModelLabelRawByBrand[currentBrandLabelKey]
          : {};

      rawMergedModelLabelsByBrand[brandValue] = {
        ...baselineBrandLabelMap,
        ...currentBrandLabelMap
      };
    });
    const mergedModelOptionLabelsByBrand = normalizeProductFilterOptionLabelMapByBrand(
      rawMergedModelLabelsByBrand,
      mergedModelOptionsByBrand,
      mergedBrandOptions
    );

    let mergedFactoryOptions = [];
    let mergedFactoryOptionLabels = {};
    if (normalizedGroupKey !== genpartsKey) {
      const baselineFactoryOptions = normalizeProductFilterOptionList(baselineGroup.factoryOptions);
      const currentFactoryOptions = normalizeProductFilterOptionList(targetGroup.factoryOptions);
      mergedFactoryOptions = normalizeProductFilterOptionList([
        ...currentFactoryOptions,
        ...baselineFactoryOptions
      ]);

      const baselineFactoryLabelRaw =
        baselineGroup.factoryOptionLabels &&
        typeof baselineGroup.factoryOptionLabels === 'object' &&
        !Array.isArray(baselineGroup.factoryOptionLabels)
          ? baselineGroup.factoryOptionLabels
          : {};
      const currentFactoryLabelRaw =
        targetGroup.factoryOptionLabels &&
        typeof targetGroup.factoryOptionLabels === 'object' &&
        !Array.isArray(targetGroup.factoryOptionLabels)
          ? targetGroup.factoryOptionLabels
          : {};
      mergedFactoryOptionLabels = normalizeProductFilterOptionLabelMap(
        { ...baselineFactoryLabelRaw, ...currentFactoryLabelRaw },
        mergedFactoryOptions
      );
    }

    const baselineModelOptions = normalizeProductFilterOptionList(baselineGroup.modelOptions);
    const currentModelOptions = normalizeProductFilterOptionList(targetGroup.modelOptions);
    const mergedModelOptions = hasModelOptionsByBrand
      ? []
      : normalizeProductFilterOptionList([...currentModelOptions, ...baselineModelOptions]);

    const nextGroup = {
      ...targetGroup,
      brandOptions: mergedBrandOptions,
      factoryOptions: mergedFactoryOptions,
      modelOptions: mergedModelOptions,
      modelOptionsByBrand: mergedModelOptionsByBrand,
      brandOptionLabels: mergedBrandOptionLabels,
      factoryOptionLabels: mergedFactoryOptionLabels,
      modelOptionLabelsByBrand: mergedModelOptionLabelsByBrand
    };

    if (JSON.stringify(nextGroup) !== JSON.stringify(targetGroup)) {
      nextConfigs[targetIndex] = nextGroup;
      changed = true;
    }
  });

  if (changed) {
    setProductGroupConfigs(nextConfigs);
  }
  setSetting(PRODUCT_FILTER_BASELINE_VERSION_KEY, PRODUCT_FILTER_BASELINE_VERSION);
}

applyProductFilterBaselineSeedOnce();

function getProductGroupMap(groupConfigs = getProductGroupConfigs()) {
  return new Map(groupConfigs.map((group) => [group.key, group]));
}

function getProductGroupLabels(groupConfigs = getProductGroupConfigs(), lang = 'ko') {
  const labels = {};
  groupConfigs.forEach((group) => {
    labels[group.key] = lang === 'en' ? group.labelEn || group.key : group.labelKo || group.key;
  });
  return labels;
}

function splitProductGroupsForDisplay(groupConfigs = getProductGroupConfigs()) {
  const safeGroups = Array.isArray(groupConfigs) ? groupConfigs : [];
  const featured = [];
  const regular = [];

  safeGroups.forEach((groupConfig) => {
    const safeGroup =
      groupConfig && typeof groupConfig === 'object' && !Array.isArray(groupConfig)
        ? groupConfig
        : null;
    if (!safeGroup) {
      return;
    }
    if (resolveGroupMainTopBoxState(safeGroup)) {
      featured.push(safeGroup);
      return;
    }
    regular.push(safeGroup);
  });

  return {
    featured,
    regular
  };
}

function parseProductExtraFields(rawExtraFieldsJson) {
  try {
    const parsed = JSON.parse(String(rawExtraFieldsJson || '{}'));
    if (!parsed || typeof parsed !== 'object') {
      return {};
    }
    if (parsed.values && typeof parsed.values === 'object' && !Array.isArray(parsed.values)) {
      return parsed.values;
    }
    if (Array.isArray(parsed)) {
      return {};
    }
    return parsed;
  } catch {
    return {};
  }
}

function buildProductDisplayData(product, groupConfig) {
  const safeProduct = product || {};
  const customFields = Array.isArray(groupConfig?.customFields) ? groupConfig.customFields : [];
  const fieldValues = getProductFieldValues(safeProduct, groupConfig);
  const isFactoryLike = isFactoryLikeGroup(groupConfig);
  const title = String(fieldValues.title || fieldValues.brand || safeProduct.brand || '').trim();

  const fieldDefs = customFields
    .map((field) => {
      const key = normalizeProductFieldAliasKey(field?.key || '');
      if (!key) {
        return null;
      }
      return {
        key,
        labelKo: String(field?.labelKo || field?.key || key).trim() || key,
        labelEn: String(field?.labelEn || field?.labelKo || field?.key || key).trim() || key
      };
    })
    .filter(Boolean);

  const fieldDefByKey = new Map(fieldDefs.map((field) => [field.key, field]));
  const hasField = (key) => fieldDefByKey.has(key);
  const readField = (key) => String(fieldValues[key] || '').trim();

  const summary = String(
    readField('detailed_description') ||
      readField('features') ||
      safeProduct.sub_model ||
      ''
  ).trim();

  const featureCopy = String(
    readField('features') ||
      (isFactoryLike ? '' : readField('detailed_description')) ||
      ''
  ).trim();

  const skipKeys = new Set(['title', 'price']);
  const customPairs = fieldDefs
    .map((field) => {
      const { key } = field;
      const value = readField(key);
      if (skipKeys.has(key)) {
        return null;
      }
      if (!isFactoryLike && key === 'detailed_description') {
        return null;
      }
      if (key === 'features') {
        return null;
      }
      if (!value) {
        return {
          key,
          labelKo: field.labelKo,
          labelEn: field.labelEn,
          value: '-'
        };
      }
      return {
        key,
        labelKo: field.labelKo,
        labelEn: field.labelEn,
        value
      };
    })
    .filter(Boolean);

  const specPairs = isFactoryLike ? customPairs : [];
  const factoryMeta = [];
  if (isFactoryLike && hasField('case_material')) {
    const caseMaterial = readField('case_material');
    if (caseMaterial) factoryMeta.push(caseMaterial);
  }
  if (isFactoryLike && hasField('movement')) {
    const movement = readField('movement');
    if (movement) factoryMeta.push(movement);
  }
  const fallbackMeta = customPairs
    .map((pair) => String(pair.value || '').trim())
    .filter((value) => value && value !== '-')
    .slice(0, 2);

  return {
    title: title || `${safeProduct.brand || ''} ${safeProduct.model || ''}`.trim() || '-',
    subtitle: summary || String(safeProduct.sub_model || '').trim(),
    meta: isFactoryLike
      ? (factoryMeta.length > 0 ? factoryMeta : fallbackMeta).join(' / ')
      : customPairs.map((pair) => pair.value).filter((value) => value && value !== '-').join(' / '),
    summary: summary || String(safeProduct.features || '').trim(),
    featureCopy,
    specPairs,
    customPairs
  };
}

function decorateProductForView(product, groupConfig) {
  const display = buildProductDisplayData(product, groupConfig);
  return {
    ...product,
    display_title: display.title,
    display_subtitle: display.subtitle,
    display_meta: display.meta,
    display_summary: display.summary,
    display_fields: display.customPairs
  };
}

function escapeSvgText(raw = '') {
  return String(raw || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function getBrandingAssetUrl(assetKey = '') {
  const fileName = BRANDING_ASSET_FILES[assetKey];
  if (!fileName) {
    return '';
  }
  const localPath = path.join(BRANDING_DIR, fileName);
  if (!existsSync(localPath)) {
    return '';
  }
  return `/assets/media/branding/${fileName}`;
}

function getBrandingWatermarkUrl() {
  return existsSync(BRANDING_WATERMARK_LOCAL_PATH) ? BRANDING_WATERMARK_URL : '';
}

function buildChronoLabWatermarkSvg(width, height) {
  const safeWidth = Math.max(120, Math.floor(Number(width || 0)));
  const safeHeight = Math.max(120, Math.floor(Number(height || 0)));
  const fontSize = Math.max(12, Math.min(30, Math.round(safeWidth / 33)));
  const approxTextWidth = Math.max(Math.round(fontSize * WATERMARK_DOMAIN_TEXT.length * 0.58), fontSize * 8);
  const xStep = Math.max(approxTextWidth + Math.round(fontSize * 1.1), approxTextWidth + 16);
  const rowYList = [0.25, 0.5, 0.75].map((ratio) => Math.round(safeHeight * ratio));
  const label = escapeSvgText(WATERMARK_DOMAIN_TEXT);
  const textNodes = [];

  rowYList.forEach((rowY) => {
    for (let x = -approxTextWidth; x < safeWidth + approxTextWidth; x += xStep) {
      textNodes.push(
        `<text x="${Math.round(x)}" y="${rowY}" text-anchor="start" dominant-baseline="middle">${label}</text>`
      );
    }
  });

  return `
    <svg width="${safeWidth}" height="${safeHeight}" viewBox="0 0 ${safeWidth} ${safeHeight}" xmlns="http://www.w3.org/2000/svg">
      <style>
        text {
          font-family: 'Noto Sans KR', 'Noto Sans', sans-serif;
          font-size: ${fontSize}px;
          font-weight: 700;
          letter-spacing: 0.01em;
          fill: rgba(255, 255, 255, ${WATERMARK_IMAGE_ALPHA});
        }
      </style>
      ${textNodes.join('')}
    </svg>
  `;
}

async function buildChronoLabWatermarkOverlay(width, height) {
  const safeWidth = Math.max(120, Math.floor(Number(width || 0)));
  const safeHeight = Math.max(120, Math.floor(Number(height || 0)));
  if (!existsSync(BRANDING_WATERMARK_LOCAL_PATH)) {
    return Buffer.from(buildChronoLabWatermarkSvg(safeWidth, safeHeight));
  }

  try {
    const targetWidth = Math.max(60, Math.round(safeWidth * 0.11));
    const watermarkBuffer = await sharp(BRANDING_WATERMARK_LOCAL_PATH)
      .ensureAlpha()
      .resize({
        width: targetWidth,
        fit: 'inside',
        withoutEnlargement: false
      })
      // sharp composite opacity is not consistently honored across versions.
      // Apply alpha directly to the watermark image so rendered output is always semi-transparent.
      .linear([1, 1, 1, WATERMARK_IMAGE_ALPHA], [0, 0, 0, 0])
      .png()
      .toBuffer();
    const watermarkMeta = await sharp(watermarkBuffer).metadata();
    const watermarkWidth = Math.max(1, Math.floor(Number(watermarkMeta.width || targetWidth)));
    const watermarkHeight = Math.max(1, Math.floor(Number(watermarkMeta.height || Math.round(targetWidth * 0.22))));
    const fontSize = Math.max(10, Math.round(watermarkHeight * 0.82));
    const textWidth = Math.max(Math.round(fontSize * WATERMARK_DOMAIN_TEXT.length * 0.58), fontSize * 8);
    const imageTextGap = Math.max(10, Math.round(watermarkHeight * 0.34));
    const textTailGap = Math.max(10, Math.round(watermarkHeight * 0.34));
    const xStep = Math.max(watermarkWidth + imageTextGap + textWidth + textTailGap, watermarkWidth + 22);
    const rowYList = [0.25, 0.5, 0.75].map((ratio) => Math.round(safeHeight * ratio));
    const imageY = Math.round(-watermarkHeight / 2);
    const escapedDomainText = escapeSvgText(WATERMARK_DOMAIN_TEXT);
    const watermarkDataUri = `data:image/png;base64,${watermarkBuffer.toString('base64')}`;
    const patternNodes = [];

    rowYList.forEach((rowY) => {
      for (let x = -watermarkWidth; x < safeWidth + watermarkWidth; x += xStep) {
        const imageX = Math.round(x);
        const textX = Math.round(x + watermarkWidth + imageTextGap);
        patternNodes.push(
          `<image href="${watermarkDataUri}" x="${imageX}" y="${imageY}" width="${watermarkWidth}" height="${watermarkHeight}" transform="translate(0 ${rowY})" />`
        );
        patternNodes.push(
          `<text x="${textX}" y="${rowY}" text-anchor="start" dominant-baseline="middle">${escapedDomainText}</text>`
        );
      }
    });

    const overlaySvg = `
      <svg width="${safeWidth}" height="${safeHeight}" viewBox="0 0 ${safeWidth} ${safeHeight}" xmlns="http://www.w3.org/2000/svg">
        <style>
          text {
            font-family: 'Noto Sans KR', 'Noto Sans', sans-serif;
            font-size: ${fontSize}px;
            font-weight: 700;
            letter-spacing: 0.01em;
            fill: rgba(255, 255, 255, ${WATERMARK_IMAGE_ALPHA});
          }
        </style>
        ${patternNodes.join('')}
      </svg>
    `;

    return await sharp(Buffer.from(overlaySvg))
      .png()
      .toBuffer();
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('[chronolab:watermark:image]', error);
    return Buffer.from(buildChronoLabWatermarkSvg(safeWidth, safeHeight));
  }
}

async function applyChronoLabWatermarkToFile(filePath) {
  if (!filePath) {
    return { ok: false, reason: 'empty-path' };
  }

  try {
    const image = sharp(filePath);
    const metadata = await image.metadata();
    const width = Number(metadata.width || 0);
    const height = Number(metadata.height || 0);
    const format = String(metadata.format || '').toLowerCase();

    if (width <= 0 || height <= 0) {
      return { ok: false, reason: 'invalid-size' };
    }
    if (!WATERMARK_SUPPORTED_FORMATS.has(format)) {
      return { ok: false, reason: 'unsupported-format', format };
    }

    const watermarkOverlay = await buildChronoLabWatermarkOverlay(width, height);
    let pipeline = image.composite([{ input: watermarkOverlay, top: 0, left: 0 }]);

    if (format === 'jpeg' || format === 'jpg') {
      pipeline = pipeline.jpeg({ quality: 92, mozjpeg: true });
    } else if (format === 'png') {
      pipeline = pipeline.png({ compressionLevel: 9 });
    } else if (format === 'webp') {
      pipeline = pipeline.webp({ quality: 90 });
    } else if (format === 'avif') {
      pipeline = pipeline.avif({ quality: 56 });
    }

    const outputBuffer = await pipeline.toBuffer();
    await fs.writeFile(filePath, outputBuffer);
    return { ok: true, format };
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('[chronolab:watermark]', error);
    return { ok: false, reason: 'watermark-error' };
  }
}

async function applyChronoLabWatermarkToUploads(files = []) {
  for (const file of files) {
    if (!file?.path) {
      continue;
    }
    await applyChronoLabWatermarkToFile(file.path);
  }
}

function resolvePathInsideRoot(rootDir = '', rawRelativePath = '') {
  const root = path.resolve(String(rootDir || ''));
  if (!root) {
    return '';
  }
  const relativePath = String(rawRelativePath || '')
    .replace(/\0/g, '')
    .replace(/^[/\\]+/, '');
  if (!relativePath) {
    return '';
  }
  const resolved = path.resolve(root, relativePath);
  if (resolved === root) {
    return '';
  }
  if (!resolved.startsWith(`${root}${path.sep}`)) {
    return '';
  }
  return resolved;
}

function resolveLocalPathFromImageUrl(imageUrl = '') {
  const src = String(imageUrl || '').trim();
  if (!src.startsWith('/')) {
    return '';
  }

  const withoutFragment = src.split('#')[0].split('?')[0];
  let decodedPath = withoutFragment;
  try {
    decodedPath = decodeURIComponent(withoutFragment);
  } catch {
    decodedPath = withoutFragment;
  }

  if (decodedPath.startsWith('/uploads/')) {
    return resolvePathInsideRoot(UPLOAD_DIR, decodedPath.slice('/uploads/'.length));
  }
  if (decodedPath.startsWith('/assets/')) {
    return resolvePathInsideRoot(path.join(__dirname, 'public'), decodedPath.slice('/assets/'.length));
  }
  return '';
}

function waitForMs(ms = 0) {
  const delayMs = Number(ms);
  if (!Number.isFinite(delayMs) || delayMs <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, Math.floor(delayMs)));
}

function normalizeRemoteImageUrl(imageUrl = '') {
  const raw = String(imageUrl || '').trim();
  if (!raw) {
    return '';
  }
  const source = raw.startsWith('//') ? `https:${raw}` : raw;
  try {
    const parsed = new URL(source);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      return '';
    }
    if (parsed.username || parsed.password) {
      return '';
    }
    const hostname = String(parsed.hostname || '').trim().toLowerCase();
    if (!isAllowedRemoteImageHostname(hostname)) {
      return '';
    }
    if (mustEnforceSecurity && parsed.protocol !== 'https:') {
      return '';
    }
    return parsed.toString();
  } catch {
    return '';
  }
}

function parseRetryAfterMs(retryAfter = '') {
  const value = String(retryAfter || '').trim();
  if (!value) {
    return 0;
  }
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds > 0) {
    return Math.min(Math.floor(seconds * 1000), WATERMARK_REMOTE_FETCH_MAX_DELAY_MS);
  }
  const scheduledAt = Date.parse(value);
  if (!Number.isFinite(scheduledAt)) {
    return 0;
  }
  const diffMs = scheduledAt - Date.now();
  if (diffMs <= 0) {
    return 0;
  }
  return Math.min(Math.floor(diffMs), WATERMARK_REMOTE_FETCH_MAX_DELAY_MS);
}

function isRemoteImageUrl(imageUrl = '') {
  return Boolean(normalizeRemoteImageUrl(imageUrl));
}

function isPrivateOrReservedIpv4Address(ipv4Address = '') {
  const parts = String(ipv4Address || '')
    .trim()
    .split('.')
    .map((value) => Number.parseInt(value, 10));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
    return true;
  }
  const [a, b] = parts;
  if (a === 10 || a === 127 || a === 0) return true;
  if (a === 169 && b === 254) return true;
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 192 && b === 168) return true;
  if (a === 100 && b >= 64 && b <= 127) return true;
  if (a === 198 && (b === 18 || b === 19)) return true;
  if (a >= 224) return true;
  return false;
}

function isPrivateOrReservedIpv6Address(ipv6Address = '') {
  const normalized = String(ipv6Address || '')
    .trim()
    .toLowerCase()
    .split('%')[0];
  if (!normalized) {
    return true;
  }
  if (normalized === '::' || normalized === '::1') {
    return true;
  }
  if (normalized.startsWith('::ffff:')) {
    const mappedIpv4 = normalized.slice('::ffff:'.length);
    if (net.isIP(mappedIpv4) === 4) {
      return isPrivateOrReservedIpv4Address(mappedIpv4);
    }
    return true;
  }
  if (normalized.startsWith('fc') || normalized.startsWith('fd')) {
    return true;
  }
  if (
    normalized.startsWith('fe8') ||
    normalized.startsWith('fe9') ||
    normalized.startsWith('fea') ||
    normalized.startsWith('feb')
  ) {
    return true;
  }
  if (normalized.startsWith('ff')) {
    return true;
  }
  return false;
}

function isPrivateOrReservedIpAddress(address = '') {
  const normalized = String(address || '').trim().toLowerCase();
  const ipVersion = net.isIP(normalized);
  if (ipVersion === 4) {
    return isPrivateOrReservedIpv4Address(normalized);
  }
  if (ipVersion === 6) {
    return isPrivateOrReservedIpv6Address(normalized);
  }
  return true;
}

function isAllowedRemoteImageHostname(hostname = '') {
  const normalized = String(hostname || '').trim().toLowerCase();
  if (!normalized) {
    return false;
  }
  if (normalized === 'localhost' || normalized.endsWith('.localhost')) {
    return false;
  }
  if (
    normalized.endsWith('.local') ||
    normalized.endsWith('.internal') ||
    normalized.endsWith('.home') ||
    normalized.endsWith('.lan') ||
    normalized.endsWith('.corp')
  ) {
    return false;
  }
  const ipVersion = net.isIP(normalized);
  if (ipVersion > 0 && isPrivateOrReservedIpAddress(normalized)) {
    return false;
  }
  return true;
}

function readRemoteImageHostValidationCache(hostname = '') {
  const key = String(hostname || '').trim().toLowerCase();
  if (!key) {
    return null;
  }
  const cached = remoteImageHostValidationCache.get(key);
  if (!cached) {
    return null;
  }
  if (Date.now() - Number(cached.checkedAt || 0) > REMOTE_IMAGE_HOST_VALIDATION_CACHE_TTL_MS) {
    remoteImageHostValidationCache.delete(key);
    return null;
  }
  return Boolean(cached.allowed);
}

function writeRemoteImageHostValidationCache(hostname = '', allowed = false) {
  const key = String(hostname || '').trim().toLowerCase();
  if (!key) {
    return;
  }
  remoteImageHostValidationCache.set(key, {
    allowed: allowed === true,
    checkedAt: Date.now()
  });
}

async function isPublicRemoteImageHost(hostname = '') {
  const normalized = String(hostname || '').trim().toLowerCase();
  if (!isAllowedRemoteImageHostname(normalized)) {
    return false;
  }
  const ipVersion = net.isIP(normalized);
  if (ipVersion > 0) {
    return !isPrivateOrReservedIpAddress(normalized);
  }

  const cached = readRemoteImageHostValidationCache(normalized);
  if (cached !== null) {
    return cached;
  }

  try {
    const records = await dns.lookup(normalized, { all: true, verbatim: true });
    const addresses = Array.isArray(records)
      ? records.map((record) => String(record?.address || '').trim()).filter(Boolean)
      : [];
    if (addresses.length === 0) {
      writeRemoteImageHostValidationCache(normalized, false);
      return false;
    }
    const hasPrivateAddress = addresses.some((address) => isPrivateOrReservedIpAddress(address));
    const allowed = !hasPrivateAddress;
    writeRemoteImageHostValidationCache(normalized, allowed);
    return allowed;
  } catch {
    writeRemoteImageHostValidationCache(normalized, false);
    return false;
  }
}

function buildUploadFilenameForFormat(format = '') {
  const safeFormat = String(format || '').toLowerCase();
  const ext = safeFormat === 'jpeg' || safeFormat === 'jpg'
    ? 'jpg'
    : safeFormat === 'png'
      ? 'png'
      : safeFormat === 'webp'
        ? 'webp'
        : safeFormat === 'avif'
          ? 'avif'
          : 'jpg';
  return `wm-legacy-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.${ext}`;
}

function replaceProductImageUrlEverywhere(fromImageUrl = '', toImageUrl = '') {
  const from = String(fromImageUrl || '').trim();
  const to = String(toImageUrl || '').trim();
  if (!from || !to || from === to) {
    return 0;
  }

  const tx = db.transaction((source, target) => {
    const updatedProducts = db
      .prepare('UPDATE products SET image_path = ? WHERE image_path = ?')
      .run(target, source);
    const updatedImages = db
      .prepare('UPDATE product_images SET image_path = ? WHERE image_path = ?')
      .run(target, source);
    return Number(updatedProducts.changes || 0) + Number(updatedImages.changes || 0);
  });

  return tx(from, to);
}

async function saveWatermarkedBufferAsUpload(buffer = Buffer.alloc(0)) {
  const sourceBuffer = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
  if (sourceBuffer.length <= 0) {
    return { ok: false, reason: 'empty-body' };
  }

  const metadata = await sharp(sourceBuffer).metadata();
  const format = String(metadata.format || '').toLowerCase();
  if (!WATERMARK_SUPPORTED_FORMATS.has(format)) {
    return { ok: false, reason: 'unsupported-format', format };
  }

  const uploadDir = UPLOAD_DIR;
  await fs.mkdir(uploadDir, { recursive: true });

  const filename = buildUploadFilenameForFormat(format);
  const localPath = path.join(uploadDir, filename);
  await fs.writeFile(localPath, sourceBuffer);

  const result = await applyChronoLabWatermarkToFile(localPath);
  if (!result.ok) {
    await fs.unlink(localPath).catch(() => {});
    return { ok: false, reason: result.reason || 'watermark-failed' };
  }

  return {
    ok: true,
    imagePath: `/uploads/${filename}`,
    format
  };
}

async function downloadRemoteImageBufferWithCurl(imageUrl = '') {
  const src = normalizeRemoteImageUrl(imageUrl);
  if (!src) {
    return { ok: false, reason: 'unsupported-url' };
  }

  const timeoutSeconds = Math.max(5, Math.ceil(WATERMARK_REMOTE_FETCH_TIMEOUT_MS / 1000));
  let refererHeader = '';
  try {
    const parsed = new URL(src);
    refererHeader = `Referer: ${parsed.origin}/`;
  } catch {
    refererHeader = '';
  }

  const args = [
    '-fsSL',
    '--max-time',
    String(timeoutSeconds),
    '--connect-timeout',
    '8',
    '--retry',
    '2',
    '--retry-all-errors',
    '-A',
    WATERMARK_REMOTE_FETCH_USER_AGENT,
    '-H',
    'Accept: image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
    '-H',
    'Accept-Language: ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
  ];
  if (refererHeader) {
    args.push('-H', refererHeader);
  }
  args.push(src);

  try {
    const { stdout } = await execFileAsync('curl', args, {
      encoding: 'buffer',
      maxBuffer: WATERMARK_REMOTE_CURL_MAX_BUFFER_BYTES
    });
    const buffer = Buffer.isBuffer(stdout) ? stdout : Buffer.from(stdout || []);
    if (buffer.length <= 0) {
      return { ok: false, reason: 'empty-body' };
    }
    return { ok: true, buffer };
  } catch {
    return { ok: false, reason: 'curl-download-error' };
  }
}

async function downloadRemoteImageAsWatermarkedUpload(imageUrl = '') {
  const src = normalizeRemoteImageUrl(imageUrl);
  if (!src) {
    return { ok: false, reason: 'unsupported-url' };
  }
  let parsedUrl = null;
  try {
    parsedUrl = new URL(src);
  } catch {
    return { ok: false, reason: 'unsupported-url' };
  }
  const hasPublicHost = await isPublicRemoteImageHost(parsedUrl.hostname);
  if (!hasPublicHost) {
    return { ok: false, reason: 'blocked-private-host' };
  }

  const canUseFetch = typeof fetch === 'function';
  let reason = 'download-error';

  if (canUseFetch) {
    for (let attempt = 1; attempt <= WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS; attempt += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), WATERMARK_REMOTE_FETCH_TIMEOUT_MS);

      try {
        const response = await fetch(src, {
          method: 'GET',
          redirect: 'follow',
          headers: {
            accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
            'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            pragma: 'no-cache',
            referer: `${parsedUrl.origin}/`,
            'user-agent': WATERMARK_REMOTE_FETCH_USER_AGENT
          },
          signal: controller.signal
        });

        if (response.status === 429 || (response.status >= 500 && response.status < 600)) {
          reason = `http-${response.status}`;
          if (attempt < WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS) {
            const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
            const fallbackDelayMs = Math.min(
              WATERMARK_REMOTE_FETCH_BASE_DELAY_MS * attempt,
              WATERMARK_REMOTE_FETCH_MAX_DELAY_MS
            );
            await waitForMs(Math.max(retryAfterMs, fallbackDelayMs));
            continue;
          }
          break;
        }

        if (!response.ok) {
          return { ok: false, reason: `http-${response.status}` };
        }

        const contentType = String(response.headers.get('content-type') || '').toLowerCase();
        if (contentType && !contentType.includes('image/')) {
          return { ok: false, reason: 'non-image-content-type' };
        }

        const buffer = Buffer.from(await response.arrayBuffer());
        return await saveWatermarkedBufferAsUpload(buffer);
      } catch (error) {
        reason = error?.name === 'AbortError' ? 'download-timeout' : 'download-error';
        if (attempt < WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS) {
          const fallbackDelayMs = Math.min(
            WATERMARK_REMOTE_FETCH_BASE_DELAY_MS * attempt,
            WATERMARK_REMOTE_FETCH_MAX_DELAY_MS
          );
          await waitForMs(fallbackDelayMs);
          continue;
        }
        break;
      } finally {
        clearTimeout(timeout);
      }
    }
  }

  const curlDownloaded = await downloadRemoteImageBufferWithCurl(src);
  if (!curlDownloaded.ok) {
    return { ok: false, reason: curlDownloaded.reason || reason };
  }
  return await saveWatermarkedBufferAsUpload(curlDownloaded.buffer);
}

async function applyChronoLabWatermarkToAllProductImages() {
  const rows = db
    .prepare(
      `
        SELECT image_path
        FROM products
        WHERE COALESCE(image_path, '') != ''
        UNION
        SELECT image_path
        FROM product_images
        WHERE COALESCE(image_path, '') != ''
      `
    )
    .all();

  const uniqueUrls = [...new Set(rows.map((row) => String(row.image_path || '').trim()).filter(Boolean))];
  let processedCount = 0;
  let convertedRemoteCount = 0;
  let skippedCount = 0;
  let failedCount = 0;
  const failedReasonCounts = {};
  const addFailedReason = (reason = '') => {
    const key = String(reason || 'unknown').trim() || 'unknown';
    failedReasonCounts[key] = Number(failedReasonCounts[key] || 0) + 1;
  };

  for (const imageUrl of uniqueUrls) {
    const localPath = resolveLocalPathFromImageUrl(imageUrl);
    if (localPath) {
      try {
        await fs.access(localPath);
      } catch {
        skippedCount += 1;
        // eslint-disable-next-line no-continue
        continue;
      }

      const result = await applyChronoLabWatermarkToFile(localPath);
      if (result.ok) {
        processedCount += 1;
      } else if (result.reason === 'unsupported-format') {
        skippedCount += 1;
      } else {
        failedCount += 1;
        addFailedReason(result.reason);
      }
      // eslint-disable-next-line no-continue
      continue;
    }

    if (isRemoteImageUrl(imageUrl)) {
      const downloaded = await downloadRemoteImageAsWatermarkedUpload(imageUrl);
      if (downloaded.ok) {
        replaceProductImageUrlEverywhere(imageUrl, downloaded.imagePath);
        processedCount += 1;
        convertedRemoteCount += 1;
      } else if (downloaded.reason === 'unsupported-format') {
        skippedCount += 1;
      } else {
        failedCount += 1;
        addFailedReason(downloaded.reason);
      }
      // eslint-disable-next-line no-continue
      continue;
    }

    skippedCount += 1;
  }

  return {
    totalCount: uniqueUrls.length,
    processedCount,
    convertedRemoteCount,
    skippedCount,
    failedCount,
    failedReasonCounts
  };
}

function getAdminMenus(currentUser = null) {
  const isPrimaryAdmin = Boolean(currentUser?.isPrimaryAdmin);
  return ADMIN_MENUS
    .filter((menu) => {
      if (menu.id === 'admin-security') {
        return isPrimaryAdmin;
      }
      return true;
    })
    .map((menu) => ({ ...menu }));
}

const BLOCKED_ACCOUNT_NOTICE = '차단된 계정입니다. 관리자에게 문의하세요.';

function setFlash(req, type, message) {
  const payload = { type, message };
  req.session.flash = payload;
  req.session.popupFlash = payload;
}

function getFlash(req) {
  const flash = req.session.flash || null;
  delete req.session.flash;
  return flash;
}

function setPopupFlash(req, type, message) {
  req.session.popupFlash = { type, message };
}

function getPopupFlash(req) {
  const popupFlash = req.session.popupFlash || null;
  delete req.session.popupFlash;
  return popupFlash;
}

function normalizeEmailAddress(raw = '') {
  return String(raw || '').trim().toLowerCase();
}

function normalizeAccountName(raw = '') {
  return String(raw || '').trim();
}

function sanitizeLogValue(value, depth = 0, keyName = '') {
  if (keyName && LOG_REDACTED_KEY_REGEX.test(String(keyName))) {
    return '[redacted]';
  }

  if (value === null || value === undefined) {
    return value ?? null;
  }

  if (typeof value === 'string') {
    return value.length > LOG_MAX_STRING_LENGTH ? `${value.slice(0, LOG_MAX_STRING_LENGTH)}...[truncated]` : value;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'bigint') {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof Error) {
    return {
      name: String(value.name || 'Error'),
      message: String(value.message || ''),
      code: String(value.code || ''),
      errno: Number.isFinite(Number(value.errno)) ? Number(value.errno) : null,
      stack: String(value.stack || '').slice(0, 4000)
    };
  }

  if (Buffer.isBuffer(value)) {
    return `[buffer:${value.length}]`;
  }

  if (depth >= 3) {
    return '[max-depth]';
  }

  if (Array.isArray(value)) {
    const items = value.slice(0, LOG_MAX_ARRAY_LENGTH).map((item) => sanitizeLogValue(item, depth + 1));
    if (value.length > LOG_MAX_ARRAY_LENGTH) {
      items.push(`[+${value.length - LOG_MAX_ARRAY_LENGTH} more]`);
    }
    return items;
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value);
    const sanitized = {};
    entries.slice(0, LOG_MAX_OBJECT_KEYS).forEach(([entryKey, entryValue]) => {
      sanitized[entryKey] = sanitizeLogValue(entryValue, depth + 1, entryKey);
    });
    if (entries.length > LOG_MAX_OBJECT_KEYS) {
      sanitized.__truncatedKeys = entries.length - LOG_MAX_OBJECT_KEYS;
    }
    return sanitized;
  }

  return String(value);
}

function buildRequestLogContext(req) {
  if (!req) {
    return {};
  }

  return {
    requestId: String(req.requestId || ''),
    method: String(req.method || ''),
    path: String(req.path || ''),
    url: String(req.originalUrl || req.url || ''),
    ip: getClientIp(req),
    userId: Number(req.user?.id || 0) || null,
    isAdmin: Boolean(req.user?.isAdmin),
    adminRole: String(req.user?.adminRole || ''),
    userAgent: String(req.get('user-agent') || '').slice(0, 280),
    contentType: String(req.get('content-type') || '').slice(0, 120),
    referer: String(req.get('referer') || '').slice(0, 280),
    params: sanitizeLogValue(req.params || {}),
    query: sanitizeLogValue(req.query || {}),
    body: sanitizeLogValue(req.body || {})
  };
}

function buildErrorSnapshot(error) {
  const source = error instanceof Error ? error : new Error(String(error || 'unknown error'));
  return {
    name: String(source.name || 'Error'),
    message: String(source.message || ''),
    code: String(source.code || ''),
    errno: Number.isFinite(Number(source.errno)) ? Number(source.errno) : null,
    stack: String(source.stack || '').slice(0, 4000)
  };
}

function logDetailedError(scope, error, meta = {}) {
  const payload = {
    at: new Date().toISOString(),
    scope: String(scope || 'error'),
    error: buildErrorSnapshot(error),
    meta: sanitizeLogValue(meta || {})
  };

  try {
    // eslint-disable-next-line no-console
    console.error(`[chronolab:${payload.scope}]`, JSON.stringify(payload, null, 2));
  } catch (loggingError) {
    // eslint-disable-next-line no-console
    console.error(`[chronolab:${payload.scope}]`, payload);
    // eslint-disable-next-line no-console
    console.error('[chronolab:logging-failed]', loggingError);
  }
}

function isSqliteUniqueConstraintError(error) {
  const code = String(error?.code || '').toUpperCase();
  const message = String(error?.message || '');
  return (
    (code.includes('SQLITE_CONSTRAINT') && (code.includes('UNIQUE') || code.includes('PRIMARYKEY'))) ||
    /UNIQUE constraint failed/i.test(message)
  );
}

function extractSqliteUniqueConstraintColumns(error) {
  const message = String(error?.message || '');
  const matched = message.match(/UNIQUE constraint failed:\s*(.+)$/i);
  if (!matched) {
    return [];
  }

  return String(matched[1] || '')
    .split(',')
    .map((part) => String(part || '').trim())
    .filter(Boolean)
    .map((part) => part.split('.').slice(-1)[0])
    .map((part) => String(part || '').trim().toLowerCase())
    .filter(Boolean);
}

function resolveSignupDuplicateFieldFromSqliteError(error) {
  const columns = extractSqliteUniqueConstraintColumns(error);
  if (columns.includes('username')) {
    return 'account';
  }
  if (columns.includes('nickname')) {
    return 'nickname';
  }
  if (columns.includes('email')) {
    return 'email';
  }
  return '';
}

function getSignupDuplicateState({ account = '', nickname = '', email = '' } = {}) {
  const normalizedAccount = normalizeAccountName(account).toLowerCase();
  const normalizedNickname = String(nickname || '').trim();
  const normalizedEmail = normalizeEmailAddress(email);

  const accountExists = normalizedAccount
    ? Boolean(db.prepare('SELECT id FROM users WHERE lower(username) = lower(?) LIMIT 1').get(normalizedAccount))
    : false;
  const nicknameExists = normalizedNickname
    ? Boolean(db.prepare('SELECT id FROM users WHERE lower(nickname) = lower(?) LIMIT 1').get(normalizedNickname))
    : false;
  const emailExists = normalizedEmail
    ? Boolean(db.prepare('SELECT id FROM users WHERE lower(email) = lower(?) LIMIT 1').get(normalizedEmail))
    : false;

  return {
    accountExists,
    nicknameExists,
    emailExists
  };
}

function pickSignupDuplicateField(state = {}) {
  if (Boolean(state.accountExists)) return 'account';
  if (Boolean(state.nicknameExists)) return 'nickname';
  if (Boolean(state.emailExists)) return 'email';
  return '';
}

function getSignupDuplicateMessage(field, isEn = false) {
  if (mustEnforceSecurity) {
    return isEn ? 'This signup information cannot be used.' : '입력하신 회원 정보로 가입할 수 없습니다.';
  }
  if (field === 'account') {
    return isEn ? 'This account is already in use.' : '이미 사용중인 계정입니다.';
  }
  if (field === 'nickname') {
    return isEn ? 'This nickname is already in use.' : '이미 사용중인 닉네임입니다.';
  }
  if (field === 'email') {
    return isEn ? 'This email is already in use.' : '이미 사용중인 이메일입니다.';
  }
  return isEn ? 'This account information is already in use.' : '이미 사용중인 회원 정보입니다.';
}

function buildEmailVerificationKey(purpose = '', email = '', account = '') {
  return [String(purpose || '').trim(), normalizeEmailAddress(email), normalizeAccountName(account).toLowerCase()].join(
    '|'
  );
}

function cleanupEmailVerificationStore(nowMs = Date.now()) {
  for (const [key, row] of emailVerificationStore.entries()) {
    if (!row || Number(row.expiresAt || 0) <= nowMs) {
      emailVerificationStore.delete(key);
    }
  }
}

function cleanupPasswordResetTicketStore(nowMs = Date.now()) {
  for (const [key, row] of passwordResetTicketStore.entries()) {
    if (!row || Number(row.expiresAt || 0) <= nowMs || Number(row.usedAt || 0) > 0) {
      passwordResetTicketStore.delete(key);
    }
  }
}

function createSixDigitCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function issueEmailVerificationCode({ purpose = '', email = '', account = '', userId = 0 } = {}) {
  const normalizedPurpose = String(purpose || '').trim();
  const normalizedEmail = normalizeEmailAddress(email);
  const normalizedAccount = normalizeAccountName(account);

  if (!normalizedPurpose || !normalizedEmail) {
    return { ok: false, reason: 'invalid_target' };
  }

  const nowMs = Date.now();
  cleanupEmailVerificationStore(nowMs);

  const key = buildEmailVerificationKey(normalizedPurpose, normalizedEmail, normalizedAccount);
  const existing = emailVerificationStore.get(key);
  if (existing && Number(existing.cooldownUntil || 0) > nowMs) {
    return {
      ok: false,
      reason: 'cooldown',
      waitSeconds: Math.max(1, Math.ceil((Number(existing.cooldownUntil || 0) - nowMs) / 1000))
    };
  }

  const code = createSixDigitCode();
  const entry = {
    purpose: normalizedPurpose,
    email: normalizedEmail,
    account: normalizedAccount,
    userId: Number(userId || 0),
    code,
    attempts: 0,
    expiresAt: nowMs + EMAIL_VERIFICATION_TTL_MS,
    cooldownUntil: nowMs + EMAIL_VERIFICATION_RESEND_COOLDOWN_MS,
    verifiedAt: 0
  };
  emailVerificationStore.set(key, entry);

  return { ok: true, key, code, entry };
}

function verifyEmailVerificationCode({ purpose = '', email = '', account = '', code = '' } = {}) {
  const normalizedPurpose = String(purpose || '').trim();
  const normalizedEmail = normalizeEmailAddress(email);
  const normalizedAccount = normalizeAccountName(account);
  const normalizedCode = String(code || '').trim();
  const nowMs = Date.now();

  if (!normalizedPurpose || !normalizedEmail || !/^[0-9]{6}$/.test(normalizedCode)) {
    return { ok: false, reason: 'invalid_input' };
  }

  cleanupEmailVerificationStore(nowMs);
  const key = buildEmailVerificationKey(normalizedPurpose, normalizedEmail, normalizedAccount);
  const found = emailVerificationStore.get(key);
  if (!found) {
    return { ok: false, reason: 'not_found' };
  }

  if (Number(found.expiresAt || 0) <= nowMs) {
    emailVerificationStore.delete(key);
    return { ok: false, reason: 'expired' };
  }

  if (Number(found.attempts || 0) >= EMAIL_VERIFICATION_MAX_ATTEMPTS) {
    emailVerificationStore.delete(key);
    return { ok: false, reason: 'too_many_attempts' };
  }

  if (String(found.code || '').trim() !== normalizedCode) {
    found.attempts = Number(found.attempts || 0) + 1;
    emailVerificationStore.set(key, found);
    return { ok: false, reason: 'code_mismatch' };
  }

  found.verifiedAt = nowMs;
  emailVerificationStore.delete(key);
  return { ok: true, entry: found };
}

function createPasswordResetTicket({ userId = 0, account = '', email = '', source = '' } = {}) {
  const normalizedAccount = normalizeAccountName(account);
  const normalizedEmail = normalizeEmailAddress(email);
  const targetUserId = Number(userId || 0);
  if (!targetUserId || !normalizedAccount || !normalizedEmail) {
    return '';
  }

  cleanupPasswordResetTicketStore();
  const token = crypto.randomBytes(24).toString('hex');
  passwordResetTicketStore.set(token, {
    userId: targetUserId,
    account: normalizedAccount,
    email: normalizedEmail,
    source: String(source || '').slice(0, 50),
    issuedAt: Date.now(),
    expiresAt: Date.now() + PASSWORD_RESET_TICKET_TTL_MS,
    usedAt: 0
  });
  return token;
}

function readPasswordResetTicket(token = '') {
  const normalizedToken = String(token || '').trim();
  if (!normalizedToken) {
    return { ok: false, reason: 'missing' };
  }

  cleanupPasswordResetTicketStore();
  const row = passwordResetTicketStore.get(normalizedToken);
  if (!row) {
    return { ok: false, reason: 'invalid' };
  }
  if (Number(row.expiresAt || 0) <= Date.now()) {
    passwordResetTicketStore.delete(normalizedToken);
    return { ok: false, reason: 'expired' };
  }
  if (Number(row.usedAt || 0) > 0) {
    passwordResetTicketStore.delete(normalizedToken);
    return { ok: false, reason: 'used' };
  }
  return { ok: true, row };
}

function consumePasswordResetTicket(token = '') {
  const normalizedToken = String(token || '').trim();
  if (!normalizedToken) {
    return;
  }
  passwordResetTicketStore.delete(normalizedToken);
}

function normalizeAdminOtpCode(raw = '') {
  const digitsOnly = String(raw || '').replace(/[^0-9]/g, '').slice(0, ADMIN_OTP_DIGITS);
  return digitsOnly;
}

function normalizeBase32Secret(raw = '') {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z2-7]/g, '');
}

function createAdminOtpSecret(length = ADMIN_OTP_SECRET_LENGTH) {
  const safeLength = Math.max(16, Number.parseInt(String(length || ADMIN_OTP_SECRET_LENGTH), 10) || ADMIN_OTP_SECRET_LENGTH);
  const randomBytes = crypto.randomBytes(safeLength);
  let output = '';
  for (let idx = 0; idx < safeLength; idx += 1) {
    output += ADMIN_OTP_BASE32_ALPHABET[randomBytes[idx] % ADMIN_OTP_BASE32_ALPHABET.length];
  }
  return output;
}

function decodeBase32ToBuffer(secret = '') {
  const normalized = normalizeBase32Secret(secret);
  if (!normalized) {
    return Buffer.alloc(0);
  }

  let bits = 0;
  let value = 0;
  const bytes = [];
  for (let idx = 0; idx < normalized.length; idx += 1) {
    const current = ADMIN_OTP_BASE32_ALPHABET.indexOf(normalized[idx]);
    if (current < 0) {
      return Buffer.alloc(0);
    }
    value = (value << 5) | current;
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((value >> bits) & 0xff);
    }
  }
  return Buffer.from(bytes);
}

function buildAdminOtpAuthUri({ username = '', secret = '' } = {}) {
  const safeUser = normalizeAccountName(username) || 'admin';
  const safeSecret = normalizeBase32Secret(secret);
  const label = `${ADMIN_OTP_ISSUER}:${safeUser}`;
  return `otpauth://totp/${encodeURIComponent(label)}?secret=${safeSecret}&issuer=${encodeURIComponent(ADMIN_OTP_ISSUER)}&algorithm=SHA1&digits=${ADMIN_OTP_DIGITS}&period=${ADMIN_OTP_PERIOD_SECONDS}`;
}

function generateTotpCode(secret = '', epochMs = Date.now()) {
  const key = decodeBase32ToBuffer(secret);
  if (!key.length) {
    return '';
  }

  const epochSeconds = Math.floor(Number(epochMs || Date.now()) / 1000);
  const counter = Math.floor(epochSeconds / ADMIN_OTP_PERIOD_SECONDS);
  const counterBuffer = Buffer.alloc(8);
  counterBuffer.writeBigUInt64BE(BigInt(Math.max(counter, 0)));
  const digest = crypto.createHmac('sha1', key).update(counterBuffer).digest();
  const offset = digest[digest.length - 1] & 0x0f;
  const binaryCode = (
    ((digest[offset] & 0x7f) << 24) |
    ((digest[offset + 1] & 0xff) << 16) |
    ((digest[offset + 2] & 0xff) << 8) |
    (digest[offset + 3] & 0xff)
  );
  const modulo = 10 ** ADMIN_OTP_DIGITS;
  return String(binaryCode % modulo).padStart(ADMIN_OTP_DIGITS, '0');
}

function verifyTotpCode(secret = '', code = '') {
  const safeCode = normalizeAdminOtpCode(code);
  if (!safeCode || safeCode.length !== ADMIN_OTP_DIGITS) {
    return false;
  }
  const safeSecret = normalizeBase32Secret(secret);
  if (!safeSecret) {
    return false;
  }

  const now = Date.now();
  for (let drift = -ADMIN_OTP_DRIFT_WINDOWS; drift <= ADMIN_OTP_DRIFT_WINDOWS; drift += 1) {
    const targetEpoch = now + drift * ADMIN_OTP_PERIOD_SECONDS * 1000;
    if (generateTotpCode(safeSecret, targetEpoch) === safeCode) {
      return true;
    }
  }
  return false;
}

function clearAdminOtpPending(req) {
  if (req?.session?.adminOtpPending) {
    delete req.session.adminOtpPending;
  }
}

function readAdminOtpPending(req) {
  const pending = req?.session?.adminOtpPending;
  if (!pending || typeof pending !== 'object') {
    return null;
  }

  const userId = Number(pending.userId || 0);
  const issuedAt = Number(pending.issuedAt || 0);
  if (!Number.isInteger(userId) || userId <= 0 || issuedAt <= 0) {
    clearAdminOtpPending(req);
    return null;
  }

  if (Date.now() - issuedAt > ADMIN_OTP_PENDING_TTL_MS) {
    clearAdminOtpPending(req);
    return null;
  }

  return {
    userId,
    issuedAt,
    username: String(pending.username || '').trim()
  };
}

function setAdminOtpPending(req, userRow) {
  if (!req?.session || !userRow) {
    return;
  }
  req.session.adminOtpPending = {
    userId: Number(userRow.id),
    username: String(userRow.username || '').trim(),
    issuedAt: Date.now()
  };
}

function clearAdminOtpSetup(req) {
  if (req?.session?.adminOtpSetup) {
    delete req.session.adminOtpSetup;
  }
}

function readAdminOtpSetup(req) {
  const setup = req?.session?.adminOtpSetup;
  if (!setup || typeof setup !== 'object') {
    return null;
  }

  const userId = Number(setup.userId || 0);
  const secret = normalizeBase32Secret(setup.secret || '');
  const issuedAt = Number(setup.issuedAt || 0);
  if (!Number.isInteger(userId) || userId <= 0 || !secret || issuedAt <= 0) {
    clearAdminOtpSetup(req);
    return null;
  }

  if (Date.now() - issuedAt > ADMIN_OTP_SETUP_TTL_MS) {
    clearAdminOtpSetup(req);
    return null;
  }

  return { userId, secret, issuedAt };
}

function regenerateSessionAsync(req) {
  return new Promise((resolve, reject) => {
    if (!req?.session || typeof req.session.regenerate !== 'function') {
      resolve();
      return;
    }
    req.session.regenerate((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function snapshotAuthSessionState(req, options = {}) {
  const snapshot = {};
  if (!req?.session) {
    return snapshot;
  }

  const preserveMember = options.preserveMember === true;
  const preserveAdmin = options.preserveAdmin === true;

  if (preserveMember) {
    const memberUserId = getScopedSessionUserId(req.session, 'member');
    if (memberUserId > 0) {
      snapshot.member = {
        userId: memberUserId,
        lastActivityAt: getScopedSessionLastActivityAt(req.session, 'member')
      };
    }
  }

  if (preserveAdmin) {
    const adminUserId = getScopedSessionUserId(req.session, 'admin');
    if (adminUserId > 0) {
      snapshot.admin = {
        userId: adminUserId,
        lastActivityAt: getScopedSessionLastActivityAt(req.session, 'admin'),
        adminRole: normalizeAdminRole(req.session.adminRole || '')
      };
    }
  }

  if (preserveAdmin && options.preserveAdminOtpState !== false) {
    const otpPending = req.session.adminOtpPending;
    if (otpPending && typeof otpPending === 'object') {
      snapshot.adminOtpPending = { ...otpPending };
    }
    const otpSetup = req.session.adminOtpSetup;
    if (otpSetup && typeof otpSetup === 'object') {
      snapshot.adminOtpSetup = { ...otpSetup };
    }
  }

  const csrfToken = normalizeCsrfTokenValue(req.session.csrfToken || '');
  if (csrfToken) {
    snapshot.csrfToken = csrfToken;
  }

  return snapshot;
}

function restoreAuthSessionState(req, snapshot = {}) {
  if (!req?.session || !snapshot || typeof snapshot !== 'object') {
    return;
  }

  if (snapshot.member && typeof snapshot.member === 'object') {
    setScopedSessionAuthState(req, 'member', snapshot.member);
  }
  if (snapshot.admin && typeof snapshot.admin === 'object') {
    setScopedSessionAuthState(req, 'admin', snapshot.admin);
  }

  if (snapshot.adminOtpPending && typeof snapshot.adminOtpPending === 'object') {
    req.session.adminOtpPending = { ...snapshot.adminOtpPending };
  }
  if (snapshot.adminOtpSetup && typeof snapshot.adminOtpSetup === 'object') {
    req.session.adminOtpSetup = { ...snapshot.adminOtpSetup };
  }

  if (snapshot.csrfToken) {
    req.session.csrfToken = snapshot.csrfToken;
  }
}

async function regenerateSessionWithPreservedAuth(req, options = {}) {
  const snapshot = snapshotAuthSessionState(req, options);
  await regenerateSessionAsync(req);
  restoreAuthSessionState(req, snapshot);
}

async function setAdminAuthSession(req, res, userRow) {
  if (!req?.session || !userRow) {
    return;
  }
  await regenerateSessionWithPreservedAuth(req, { preserveMember: true });
  const nowMs = Date.now();
  setScopedSessionAuthState(req, 'admin', {
    userId: Number(userRow.id),
    adminRole: normalizeAdminRole(userRow.admin_role),
    lastActivityAt: nowMs
  });
  setPersistAuthCookie(res, Number(userRow.id), {
    scope: 'admin',
    isAdmin: true,
    lastActivityAt: nowMs
  });
  clearAdminOtpPending(req);
}

function getMailTransporter() {
  if (mailTransporter) {
    return mailTransporter;
  }

  const host = String(process.env.SMTP_HOST || '').trim();
  const port = Number.parseInt(String(process.env.SMTP_PORT || ''), 10);
  const user = String(process.env.SMTP_USER || '').trim();
  const pass = String(process.env.SMTP_PASS || '').trim();
  const hasSmtpConfig = host && Number.isInteger(port) && port > 0 && user && pass;

  if (!hasSmtpConfig) {
    return null;
  }

  const rawSecure = String(process.env.SMTP_SECURE || '').trim().toLowerCase();
  const secure =
    rawSecure === 'true' || rawSecure === '1' || rawSecure === 'yes' || rawSecure === 'on' || port === 465;

  mailTransporter = nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user, pass }
  });

  return mailTransporter;
}

function cleanupSecurityAlertNotifyState(nowMs = Date.now()) {
  if (securityAlertNotifyState.size <= 5000) {
    return;
  }
  const cleanupThresholdMs =
    Math.max(SECURITY_ALERT_NOTIFY_THROTTLE_MS, SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS) * 10;
  for (const [key, timestamp] of securityAlertNotifyState.entries()) {
    if (nowMs - Number(timestamp || 0) > cleanupThresholdMs) {
      securityAlertNotifyState.delete(key);
    }
  }
}

function normalizeSecurityAlertPathForDedupe(pathValue = '') {
  const rawPath = String(pathValue || '').trim().toLowerCase();
  if (!rawPath) {
    return '';
  }
  if (rawPath.startsWith('/admin')) {
    return '/admin/*';
  }
  if (rawPath.startsWith('/api/admin')) {
    return '/api/admin/*';
  }
  return rawPath;
}

function getSecurityAlertThrottleMs(payload = {}) {
  const reason = String(payload.reason || '').trim().toLowerCase();
  if (
    reason === 'security.admin.hidden_route_blocked' ||
    reason === 'security.admin.auth_required' ||
    reason === 'security.admin_waf.bot_signature' ||
    reason === 'security.admin_waf.payload_signature'
  ) {
    return SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS;
  }
  return SECURITY_ALERT_NOTIFY_THROTTLE_MS;
}

function getSecurityAlertDedupeKey(payload = {}) {
  const reason = String(payload.reason || '').trim().toLowerCase();
  const ipAddress = String(payload.ipAddress || '').trim().toLowerCase();
  const pathValue = normalizeSecurityAlertPathForDedupe(payload.path);
  return [reason, ipAddress, pathValue].join('|');
}

function canNotifySecurityAlert(payload = {}) {
  if (!SECURITY_ALERT_NOTIFY_ENABLED) {
    return false;
  }
  const hasTelegramTarget =
    SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED &&
    Boolean(SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN) &&
    SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS.length > 0;
  if (
    !SECURITY_ALERT_NOTIFY_WEBHOOK_URL &&
    SECURITY_ALERT_NOTIFY_EMAIL_RECIPIENTS.length === 0 &&
    !hasTelegramTarget
  ) {
    return false;
  }
  const dedupeKey = getSecurityAlertDedupeKey(payload);
  const throttleMs = getSecurityAlertThrottleMs(payload);
  const nowMs = Date.now();
  cleanupSecurityAlertNotifyState(nowMs);
  const lastNotifiedAt = Number(securityAlertNotifyState.get(dedupeKey) || 0);
  if (lastNotifiedAt > 0 && nowMs - lastNotifiedAt < throttleMs) {
    return false;
  }
  securityAlertNotifyState.set(dedupeKey, nowMs);
  return true;
}

function buildSecurityAlertMessage(payload = {}) {
  const rawReason = String(payload.reason || '').trim();
  const safeDecodePath = (value = '') => {
    const rawValue = String(value || '').trim();
    if (!rawValue) {
      return '';
    }
    let decoded = rawValue;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const next = decodeURIComponent(decoded);
        if (!next || next === decoded) {
          break;
        }
        decoded = next;
      } catch {
        break;
      }
    }
    return decoded;
  };
  const resolveReasonKo = (reason) => {
    if (!reason) return '알 수 없는 사유';
    if (reason === 'security.admin.hidden_route_blocked') return '숨김 관리자 경로 직접 접근 차단';
    if (reason === 'security.admin.auth_required') return '관리자 인증 필요 접근 차단';
    if (reason === 'security.primary_only.route' || reason === 'security.primary_only.denied') {
      return '메인관리자 전용 영역 접근 차단';
    }
    if (reason.startsWith('security.admin_waf.')) {
      if (reason.includes('country_block')) return '관리자 WAF 국가 정책 차단';
      if (reason.includes('asn_block')) return '관리자 WAF ASN 정책 차단';
      if (reason.includes('bot_signature')) return '관리자 WAF 봇 시그니처 차단';
      if (reason.includes('payload_signature')) return '관리자 WAF 요청 시그니처 차단';
      if (reason.includes('method_block')) return '관리자 WAF 메서드 정책 차단';
      if (reason.includes('network_profile_unavailable')) return '관리자 WAF 네트워크 프로필 조회 실패 차단';
      return '관리자 WAF 정책 차단';
    }
    if (reason.startsWith('auth.admin.login_')) {
      if (reason.includes('failed')) return '관리자 로그인 실패';
      if (reason.includes('blocked_account')) return '차단 계정 로그인 시도';
      if (reason.includes('throttled')) return '관리자 로그인 시도 횟수 초과 차단';
      return '관리자 로그인 보안 이벤트';
    }
    if (reason.startsWith('auth.admin.otp_')) {
      if (reason.includes('failed')) return '관리자 OTP 인증 실패';
      if (reason.includes('throttled')) return '관리자 OTP 시도 횟수 초과 차단';
      if (reason.includes('required')) return '관리자 OTP 미설정 계정 로그인 차단';
      if (reason.includes('invalid_format')) return '관리자 OTP 형식 오류';
      if (reason.includes('missing_pending')) return '관리자 OTP 세션 만료/누락';
      if (reason.includes('blocked_account')) return '차단 계정 OTP 시도';
      if (reason.includes('user_missing')) return '관리자 OTP 대상 계정 누락';
      if (reason.includes('secret_missing')) return '관리자 OTP 비밀키 누락';
      return '관리자 OTP 보안 이벤트';
    }
    return reason;
  };
  const translateDetailKo = (reason, detail, path) => {
    const normalizedReason = String(reason || '').trim().toLowerCase();
    const rawDetail = String(detail || '').trim();
    const decodedPath = safeDecodePath(path || '');
    if (normalizedReason === 'security.admin.hidden_route_blocked') {
      const matched = rawDetail.match(/^direct_path=(.+)$/i);
      const rawBlockedPath = matched ? matched[1] : decodedPath;
      const blockedPath = safeDecodePath(rawBlockedPath || '');
      return blockedPath
        ? `숨김 관리자 경로로 직접 접근 시도 (${blockedPath})`
        : '숨김 관리자 경로로 직접 접근 시도';
    }
    if (normalizedReason === 'security.admin.auth_required') {
      return '인증 없이 관리자 영역 또는 관리자 API 접근 시도';
    }
    if (normalizedReason === 'security.admin_waf.bot_signature') {
      const uaMatched = rawDetail.match(/^ua=(.+)$/i);
      const uaValue = uaMatched ? uaMatched[1] : rawDetail;
      return uaValue ? `관리자 WAF 봇 시그니처 차단 (UA: ${uaValue})` : '관리자 WAF 봇 시그니처 차단';
    }
    if (normalizedReason === 'security.admin_waf.payload_signature') {
      return '관리자 WAF 요청 시그니처 차단';
    }
    if (normalizedReason === 'security.admin_waf.method_block') {
      return '관리자 WAF 메서드 정책 차단';
    }
    return rawDetail || '-';
  };

  const nowKst = new Date().toLocaleString('ko-KR', {
    timeZone: 'Asia/Seoul',
    hour12: false
  });
  const reasonKo = resolveReasonKo(rawReason);
  const decodedPath = safeDecodePath(String(payload.path || '').trim() || 'unknown');
  const detailKo = translateDetailKo(rawReason, payload.detail, decodedPath);
  const lines = [
    '[Chrono Lab] 보안 경보',
    `발생시각(KST): ${nowKst}`,
    `경보유형: ${reasonKo}`,
    ...(SECURITY_ALERT_NOTIFY_INCLUDE_RAW_CODE ? [`경보코드: ${rawReason || 'unknown'}`] : []),
    `상세내용: ${detailKo}`,
    `IP: ${String(payload.ipAddress || '').trim() || 'unknown'}`,
    `요청메서드: ${String(payload.method || '').trim() || 'unknown'}`,
    `요청경로: ${decodedPath}`,
    `행위자: ${String(payload.actor || '').trim() || 'unknown'}`,
    `권한: ${String(payload.role || '').trim() || 'unknown'}`,
    `요청ID: ${String(payload.requestId || '').trim() || 'unknown'}`
  ];
  return lines.join('\n');
}

async function dispatchSecurityAlertWebhook(payload = {}) {
  if (!SECURITY_ALERT_NOTIFY_WEBHOOK_URL) {
    return { ok: false, reason: 'webhook_not_configured' };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SECURITY_ALERT_NOTIFY_TIMEOUT_MS);
  try {
    const response = await fetch(SECURITY_ALERT_NOTIFY_WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        source: 'chronolab',
        type: 'security_alert',
        at: new Date().toISOString(),
        ...payload
      }),
      signal: controller.signal
    });
    if (!response.ok) {
      return { ok: false, reason: `webhook_status_${response.status}` };
    }
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      reason: error?.name === 'AbortError' ? 'webhook_timeout' : 'webhook_failed'
    };
  } finally {
    clearTimeout(timer);
  }
}

async function dispatchSecurityAlertEmail(payload = {}) {
  if (SECURITY_ALERT_NOTIFY_EMAIL_RECIPIENTS.length === 0) {
    return { ok: false, reason: 'email_not_configured' };
  }
  const transporter = getMailTransporter();
  if (!transporter) {
    return { ok: false, reason: 'smtp_not_configured' };
  }

  const subject = `[Chrono Lab][보안경보] ${String(payload.reason || 'alert').slice(0, 120)}`;
  try {
    await transporter.sendMail({
      from: String(process.env.SMTP_FROM || process.env.SMTP_USER || 'no-reply@chronolab.local').trim(),
      to: SECURITY_ALERT_NOTIFY_EMAIL_RECIPIENTS.join(', '),
      subject,
      text: buildSecurityAlertMessage(payload)
    });
    return { ok: true };
  } catch {
    return { ok: false, reason: 'smtp_send_failed' };
  }
}

async function dispatchSecurityAlertTelegram(payload = {}) {
  if (!SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED) {
    return { ok: false, reason: 'telegram_disabled' };
  }
  if (!SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN) {
    return { ok: false, reason: 'telegram_token_missing' };
  }
  if (SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS.length === 0) {
    return { ok: false, reason: 'telegram_chat_id_missing' };
  }

  const endpoint = `https://api.telegram.org/bot${SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN}/sendMessage`;
  const baseMessage = buildSecurityAlertMessage(payload).slice(0, 3800);
  const sendResults = await Promise.allSettled(
    SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS.map(async (chatId) => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), SECURITY_ALERT_NOTIFY_TIMEOUT_MS);
      try {
        const bodyPayload = {
          chat_id: chatId,
          text: baseMessage,
          disable_web_page_preview: true,
          disable_notification: SECURITY_ALERT_NOTIFY_TELEGRAM_SILENT
        };
        if (Number.isInteger(SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID) && SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID > 0) {
          bodyPayload.message_thread_id = SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID;
        }

        const response = await fetch(endpoint, {
          method: 'POST',
          headers: {
            'content-type': 'application/json'
          },
          body: JSON.stringify(bodyPayload),
          signal: controller.signal
        });

        if (!response.ok) {
          return { ok: false, reason: `telegram_status_${response.status}` };
        }
        const responseBody = await response.json().catch(() => ({}));
        if (responseBody && responseBody.ok === false) {
          return { ok: false, reason: `telegram_api_error_${String(responseBody.error_code || 'unknown')}` };
        }
        return { ok: true };
      } catch (error) {
        return {
          ok: false,
          reason: error?.name === 'AbortError' ? 'telegram_timeout' : 'telegram_failed'
        };
      } finally {
        clearTimeout(timer);
      }
    })
  );

  const successCount = sendResults.filter(
    (entry) => entry.status === 'fulfilled' && entry.value && entry.value.ok
  ).length;
  if (successCount > 0) {
    return { ok: true, delivered: successCount };
  }

  const firstFailure = sendResults.find(
    (entry) => entry.status === 'fulfilled' && entry.value && !entry.value.ok
  );
  if (firstFailure && firstFailure.status === 'fulfilled') {
    return { ok: false, reason: firstFailure.value.reason || 'telegram_failed' };
  }
  return { ok: false, reason: 'telegram_failed' };
}

function queueSecurityAlertNotification(payload = {}) {
  if (!canNotifySecurityAlert(payload)) {
    return;
  }
  setImmediate(() => {
    Promise.allSettled([
      dispatchSecurityAlertWebhook(payload),
      dispatchSecurityAlertEmail(payload),
      dispatchSecurityAlertTelegram(payload)
    ]).catch(() => {});
  });
}

async function sendEmailVerificationCode({ to, code, purpose, lang = 'ko' } = {}) {
  const email = normalizeEmailAddress(to);
  const verificationCode = String(code || '').trim();
  const normalizedPurpose = String(purpose || '').trim();
  if (!email || !/^[0-9]{6}$/.test(verificationCode)) {
    return { ok: false, reason: 'invalid_payload' };
  }

  const transporter = getMailTransporter();
  if (!transporter) {
    // eslint-disable-next-line no-console
    console.warn(`[chronolab:mail] SMTP not configured. email=${email}, code=${verificationCode}`);
    return { ok: false, reason: 'smtp_not_configured' };
  }

  const isEn = lang === 'en';
  const purposeKo = normalizedPurpose === 'account-find' ? '계정 찾기' : '비밀번호 재설정';
  const purposeEn = normalizedPurpose === 'account-find' ? 'Account Recovery' : 'Password Reset';
  const subject = isEn
    ? `[Chrono Lab] ${purposeEn} verification code`
    : `[Chrono Lab] ${purposeKo} 인증번호 안내`;
  const text = isEn
    ? `Verification code: ${verificationCode}\nThis code is valid for 10 minutes.`
    : `인증번호: ${verificationCode}\n인증번호는 10분간 유효합니다.`;

  try {
    await transporter.sendMail({
      from: String(process.env.SMTP_FROM || process.env.SMTP_USER || 'no-reply@chronolab.local').trim(),
      to: email,
      subject,
      text
    });
    return { ok: true };
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('[chronolab:mail] send failed', error);
    return { ok: false, reason: 'smtp_send_failed' };
  }
}

function fileUrl(file) {
  if (!file) {
    return '';
  }
  return `/uploads/${file.filename}`;
}

function normalizeImagePath(value) {
  return String(value || '').trim();
}

function parseImagePathJson(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map((item) => normalizeImagePath(item)).filter(Boolean);
  } catch (error) {
    return [];
  }
}

function uniqueImagePathList(imagePaths = []) {
  const seen = new Set();
  const result = [];
  for (const imagePath of imagePaths) {
    const normalized = normalizeImagePath(imagePath);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function getRecordImagePaths(record) {
  if (!record || typeof record !== 'object') {
    return [];
  }

  const fromJson = parseImagePathJson(record.image_paths_json);
  if (fromJson.length > 0) {
    return uniqueImagePathList(fromJson);
  }

  const fallback = normalizeImagePath(record.image_path);
  return fallback ? [fallback] : [];
}

function withRecordImagePaths(record) {
  if (!record || typeof record !== 'object') {
    return record;
  }
  const imagePaths = getRecordImagePaths(record);
  return {
    ...record,
    image_paths: imagePaths,
    image_path: imagePaths[0] || ''
  };
}

function withRecordImagePathsList(rows = []) {
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows.map((row) => withRecordImagePaths(row));
}

function serializeImagePaths(imagePaths = []) {
  return JSON.stringify(uniqueImagePathList(imagePaths));
}

function collectUploadedFileObjects(req) {
  const files = [];

  if (Array.isArray(req?.files)) {
    files.push(...req.files);
  } else if (req?.files && typeof req.files === 'object') {
    Object.values(req.files).forEach((bucket) => {
      if (Array.isArray(bucket)) {
        files.push(...bucket);
      }
    });
  }

  if (req?.file) {
    files.push(req.file);
  }

  return files.filter((file) => file && typeof file === 'object');
}

function collectUploadedImageUrls(req) {
  const files = collectUploadedFileObjects(req);
  return uniqueImagePathList(files.map((file) => fileUrl(file)).filter(Boolean));
}

async function cleanupUploadedFiles(files = []) {
  const targets = Array.isArray(files) ? files : [];
  for (const file of targets) {
    const targetPath = String(file?.path || '').trim();
    if (!targetPath) {
      continue;
    }
    try {
      await fs.unlink(targetPath);
    } catch {
      // ignore cleanup failures
    }
  }
}

function normalizeDetectedUploadMime(format = '') {
  const normalized = String(format || '').trim().toLowerCase();
  if (normalized === 'jpeg' || normalized === 'jpg') {
    return 'image/jpeg';
  }
  if (normalized === 'png') {
    return 'image/png';
  }
  if (normalized === 'webp') {
    return 'image/webp';
  }
  if (normalized === 'gif') {
    return 'image/gif';
  }
  if (normalized === 'avif' || normalized === 'heif') {
    return 'image/avif';
  }
  return '';
}

async function detectInvalidUploadedFiles(files = []) {
  const invalidFiles = [];

  for (const file of files) {
    const targetPath = String(file?.path || '').trim();
    const mimeType = String(file?.mimetype || '').trim().toLowerCase();
    const filename = String(file?.filename || '');
    const ext = path.extname(filename).toLowerCase();

    if (!targetPath || !mimeType || !ALLOWED_UPLOAD_MIME.has(mimeType)) {
      invalidFiles.push(file);
      continue;
    }
    if (ext && !ALLOWED_UPLOAD_EXTENSIONS.has(ext)) {
      invalidFiles.push(file);
      continue;
    }

    try {
      const metadata = await sharp(targetPath).metadata();
      const detectedMime = normalizeDetectedUploadMime(metadata?.format || '');
      const expectedExt = UPLOAD_MIME_EXTENSION_MAP[mimeType] || '';
      if (!detectedMime || !ALLOWED_UPLOAD_MIME.has(detectedMime)) {
        invalidFiles.push(file);
        continue;
      }
      if (detectedMime !== mimeType) {
        invalidFiles.push(file);
        continue;
      }
      if (!expectedExt || ext !== expectedExt) {
        invalidFiles.push(file);
        continue;
      }
      if (Number(metadata?.width || 0) <= 0 || Number(metadata?.height || 0) <= 0) {
        invalidFiles.push(file);
        continue;
      }
    } catch {
      invalidFiles.push(file);
    }
  }

  return { invalidFiles };
}

async function rejectInvalidUploadedFiles(req, res) {
  const message = '업로드 이미지 검증에 실패했습니다. 이미지 파일(JPG/PNG/WEBP/GIF/AVIF)만 업로드해 주세요.';
  const acceptHeader = String(req.get('accept') || '').toLowerCase();
  if (req.path.startsWith('/api/') || req.xhr || acceptHeader.includes('application/json')) {
    return res.status(400).json({ ok: false, error: 'invalid_upload_image', message });
  }
  setFlash(req, 'error', message);
  return res.redirect(safeBackPath(req, '/main'));
}

async function validateUploadedImagePayload(req, res, next) {
  const uploadedFiles = collectUploadedFileObjects(req);
  if (uploadedFiles.length === 0) {
    return next();
  }
  const { invalidFiles } = await detectInvalidUploadedFiles(uploadedFiles);
  if (invalidFiles.length === 0) {
    return next();
  }
  await cleanupUploadedFiles(uploadedFiles);
  return rejectInvalidUploadedFiles(req, res);
}

function formatPrice(value) {
  return Number(value || 0).toLocaleString('ko-KR');
}

function normalizeOrderStatus(rawStatus = '') {
  const status = String(rawStatus || '').trim().toUpperCase();

  if (status === 'PENDING_REVIEW' || status === 'UNPAID' || status === 'PENDING_TRANSFER' || status === 'UNCHECKED') {
    return ORDER_STATUS.PENDING_REVIEW;
  }
  if (status === 'ORDER_CONFIRMED' || status === 'PAID_PREPARING' || status === 'TRANSFER_CONFIRMED' || status === 'PREPARING') {
    return ORDER_STATUS.ORDER_CONFIRMED;
  }
  if (status === 'READY_TO_SHIP' || status === 'PACKING' || status === 'PRE_SHIPPING') return ORDER_STATUS.READY_TO_SHIP;
  if (status === 'SHIPPING' || status === 'SHIPPED') return ORDER_STATUS.SHIPPING;
  if (status === 'DELIVERED' || status === 'DONE') return ORDER_STATUS.DELIVERED;
  if (status === 'CANCELLED' || status === 'CANCELED' || status === 'ORDER_CANCELLED' || status === 'ORDER_CANCELED') {
    return ORDER_STATUS.CANCELLED;
  }

  return ORDER_STATUS.PENDING_REVIEW;
}

function getOrderStatusMeta(rawStatus, lang = 'ko', audience = 'admin') {
  const status = normalizeOrderStatus(rawStatus);
  const isEn = lang === 'en';
  const isMemberView = String(audience || '').trim().toLowerCase() === 'member';

  if (status === ORDER_STATUS.PENDING_REVIEW) {
    return {
      code: status,
      label: isEn ? 'Awaiting Payment Verification' : '입금확인중',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.ORDER_CONFIRMED) {
    if (isMemberView) {
      return {
        code: status,
        label: isEn ? 'Payment Confirmed / Preparing Shipment' : '입금확인 / 출고중',
        detail: ''
      };
    }
    return {
      code: status,
      label: isEn ? 'Payment Confirmed' : '입금확인',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.READY_TO_SHIP) {
    if (isMemberView) {
      return {
        code: ORDER_STATUS.SHIPPING,
        label: isEn ? 'Shipment Complete / Shipping' : '출고완료 / 배송중',
        detail: ''
      };
    }
    return {
      code: status,
      label: isEn ? 'Shipment Complete' : '출고완료',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.SHIPPING) {
    if (isMemberView) {
      return {
        code: status,
        label: isEn ? 'Shipment Complete / Shipping' : '출고완료 / 배송중',
        detail: ''
      };
    }
    return {
      code: status,
      label: isEn ? 'Shipping' : '배송중',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.CANCELLED) {
    return {
      code: status,
      label: isEn ? 'Cancelled' : '주문취소 완료',
      detail: ''
    };
  }

  return {
    code: ORDER_STATUS.DELIVERED,
    label: isEn ? 'Delivered' : '배송완료',
    detail: ''
  };
}

function getNextOrderStatus(rawStatus) {
  const current = normalizeOrderStatus(rawStatus);

  if (current === ORDER_STATUS.PENDING_REVIEW) return ORDER_STATUS.ORDER_CONFIRMED;
  if (current === ORDER_STATUS.ORDER_CONFIRMED) return ORDER_STATUS.READY_TO_SHIP;
  if (current === ORDER_STATUS.READY_TO_SHIP) return ORDER_STATUS.SHIPPING;
  return null;
}

function getNextOrderActionLabel(rawStatus, lang = 'ko') {
  const current = normalizeOrderStatus(rawStatus);
  const isEn = lang === 'en';
  if (current === ORDER_STATUS.PENDING_REVIEW) return isEn ? 'Confirm Payment' : '입금확인';
  if (current === ORDER_STATUS.ORDER_CONFIRMED) return isEn ? 'Mark Shipment Complete' : '출고완료 처리';
  if (current === ORDER_STATUS.READY_TO_SHIP) return isEn ? 'Register Tracking & Start Shipping' : '송장등록(배송시작)';
  return '';
}

function normalizeTrackingCarrier(rawCarrier = '') {
  const input = String(rawCarrier || '').trim();
  if (!input) {
    return TRACKING_CARRIERS[0].id;
  }
  const exists = TRACKING_CARRIERS.some((carrier) => carrier.id === input);
  return exists ? input : TRACKING_CARRIERS[0].id;
}

function normalizeTrackingNumber(rawTrackingNumber = '') {
  return String(rawTrackingNumber || '').replace(/[^A-Za-z0-9-]/g, '').trim();
}

function getTrackingCarrierLabel(carrierId = '') {
  const matched = TRACKING_CARRIERS.find((carrier) => carrier.id === carrierId);
  if (matched) {
    return matched.label;
  }
  return carrierId || '-';
}

function normalizeAdminRole(rawRole = '') {
  const role = String(rawRole || '').toUpperCase();
  return role === ADMIN_ROLE.PRIMARY ? ADMIN_ROLE.PRIMARY : ADMIN_ROLE.SUB;
}

function normalizeSecuritySection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  return SECURITY_SECTIONS.includes(section) ? section : 'profile';
}

function normalizeDateInput(rawDate = '') {
  const value = String(rawDate || '').trim();
  return DATE_INPUT_REGEX.test(value) ? value : '';
}

function normalizePositivePage(rawPage = 1) {
  const parsed = Number.parseInt(String(rawPage || '1'), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 1;
  }
  return parsed;
}

function normalizeOptionalId(rawValue = '') {
  const parsed = Number.parseInt(String(rawValue || ''), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return parsed;
}

function normalizeMemberManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'blocked' || section === 'blacklist') {
    return 'blocked';
  }
  if (section === 'levels' || section === 'level-config') {
    return 'levels';
  }
  if (section === 'member-levels' || section === 'level-members') {
    return 'active';
  }
  return 'active';
}

function parseMemberManageQuery(query = {}) {
  const section = normalizeMemberManageSection(query.memberSection || query.section || '');
  const levelRules = getMemberLevelRulesSetting();
  const levelRuleIds = levelRules.map((rule) => rule.id);
  const selectedLevelRuleId = String(query.memberLevelRuleId || query.levelRuleId || '').trim();
  const levelRuleFilter =
    selectedLevelRuleId && levelRuleIds.includes(selectedLevelRuleId) ? selectedLevelRuleId : 'all';

  return {
    section,
    keyword: String(query.memberKeyword || query.keyword || '').trim().slice(0, 120),
    levelRuleFilter,
    page: normalizePositivePage(query.memberPage || query.page || 1)
  };
}

function parsePointManageQuery(query = {}) {
  return {
    section: normalizePointManageSection(query.pointSection || query.section || '')
  };
}

function clampPage(page, totalPages) {
  const safeTotal = Math.max(1, Number(totalPages) || 1);
  const safePage = normalizePositivePage(page);
  if (safePage > safeTotal) {
    return safeTotal;
  }
  return safePage;
}

function parseSecurityQuery(query = {}) {
  return {
    logFrom: normalizeDateInput(query.logFrom || ''),
    logTo: normalizeDateInput(query.logTo || ''),
    logAdminId: normalizeOptionalId(query.logAdminId || ''),
    alertFrom: normalizeDateInput(query.alertFrom || ''),
    alertTo: normalizeDateInput(query.alertTo || ''),
    alertAdminId: normalizeOptionalId(query.alertAdminId || ''),
    logPage: normalizePositivePage(query.logPage || 1),
    alertPage: normalizePositivePage(query.alertPage || 1)
  };
}

function normalizeMenuManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'groups' || section === 'group-filters' || section === 'fields') {
    return section;
  }
  return 'public';
}

function normalizeSiteManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'theme') {
    return 'theme';
  }
  return 'basic';
}

function normalizePointManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (POINT_MANAGE_SECTIONS.includes(section)) {
    return section;
  }
  return 'signup';
}

function normalizeProductManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'list') {
    return 'list';
  }
  if (section === 'badges' || section === 'badge' || section === 'labels') {
    return 'badges';
  }
  return 'upload';
}

function normalizeSalesManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'price' || section === 'factory-price' || section === 'factory') {
    return 'price';
  }
  if (section === 'preorder' || section === 'round' || section === 'batch') {
    return 'preorder';
  }
  if (section === 'daily' || section === 'summary') {
    return 'daily';
  }
  if (section === 'editor' || section === 'sheet' || section === 'workbook') {
    return 'editor';
  }
  return 'price';
}

function normalizeContentManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'list') {
    return 'list';
  }
  return 'create';
}

function normalizeInquiryManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'list') {
    return 'list';
  }
  return 'reply';
}

function normalizeAdminOrderGroupFilter(rawGroup = '', availableGroupKeys = []) {
  const normalized = normalizeProductGroupKey(rawGroup || '');
  if (!normalized || normalized === 'all') {
    return 'all';
  }

  if (Array.isArray(availableGroupKeys) && availableGroupKeys.includes(normalized)) {
    return normalized;
  }

  return 'all';
}

function normalizeAdminOrderStatusFilter(rawStatus = '') {
  const value = String(rawStatus || '').trim();
  if (!value) {
    return 'all';
  }

  const lower = value.toLowerCase();
  if (lower === 'all') {
    return 'all';
  }

  const aliasMap = {
    pending: ORDER_STATUS.PENDING_REVIEW,
    awaiting: ORDER_STATUS.PENDING_REVIEW,
    unpaid: ORDER_STATUS.PENDING_REVIEW,
    pending_review: ORDER_STATUS.PENDING_REVIEW,
    confirmed: ORDER_STATUS.ORDER_CONFIRMED,
    paid: ORDER_STATUS.ORDER_CONFIRMED,
    payment_confirmed: ORDER_STATUS.ORDER_CONFIRMED,
    order_confirmed: ORDER_STATUS.ORDER_CONFIRMED,
    ready: ORDER_STATUS.READY_TO_SHIP,
    ready_to_ship: ORDER_STATUS.READY_TO_SHIP,
    preparing: ORDER_STATUS.READY_TO_SHIP,
    shipping: ORDER_STATUS.SHIPPING,
    shipped: ORDER_STATUS.SHIPPING,
    delivered: ORDER_STATUS.DELIVERED,
    done: ORDER_STATUS.DELIVERED,
    cancelled: ORDER_STATUS.CANCELLED,
    canceled: ORDER_STATUS.CANCELLED,
    order_cancelled: ORDER_STATUS.CANCELLED,
    order_canceled: ORDER_STATUS.CANCELLED
  };

  if (aliasMap[lower]) {
    return aliasMap[lower];
  }

  const normalized = normalizeOrderStatus(value);
  const supported = new Set([
    ORDER_STATUS.PENDING_REVIEW,
    ORDER_STATUS.ORDER_CONFIRMED,
    ORDER_STATUS.READY_TO_SHIP,
    ORDER_STATUS.SHIPPING,
    ORDER_STATUS.DELIVERED,
    ORDER_STATUS.CANCELLED
  ]);
  if (supported.has(normalized)) {
    return normalized;
  }

  return 'all';
}

function getOrderStatusFilterDbValues(statusFilter = 'all') {
  const normalized = normalizeAdminOrderStatusFilter(statusFilter);
  if (normalized === 'all') {
    return [];
  }

  if (normalized === ORDER_STATUS.PENDING_REVIEW) {
    return ['PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED'];
  }
  if (normalized === ORDER_STATUS.ORDER_CONFIRMED) {
    return ['ORDER_CONFIRMED', 'PAID_PREPARING', 'TRANSFER_CONFIRMED', 'PREPARING'];
  }
  if (normalized === ORDER_STATUS.READY_TO_SHIP) {
    return ['READY_TO_SHIP', 'PACKING', 'PRE_SHIPPING'];
  }
  if (normalized === ORDER_STATUS.SHIPPING) {
    return ['SHIPPING', 'SHIPPED'];
  }
  if (normalized === ORDER_STATUS.DELIVERED) {
    return ['DELIVERED', 'DONE'];
  }
  if (normalized === ORDER_STATUS.CANCELLED) {
    return ['CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED'];
  }
  return [];
}

function parseAdminOrderManageQuery(query = {}, availableGroupKeys = []) {
  const orderGroupFilter = normalizeAdminOrderGroupFilter(query.orderGroup || '', availableGroupKeys);
  const orderStatusFilter = normalizeAdminOrderStatusFilter(query.orderStatus || '');
  let orderDateFrom = normalizeDateInput(query.orderDateFrom || '');
  let orderDateTo = normalizeDateInput(query.orderDateTo || '');

  if (orderDateFrom && orderDateTo && orderDateFrom > orderDateTo) {
    const temp = orderDateFrom;
    orderDateFrom = orderDateTo;
    orderDateTo = temp;
  }

  return {
    orderGroupFilter,
    orderStatusFilter,
    orderDateFrom,
    orderDateTo
  };
}

function parseSalesManageQuery(query = {}, availableGroupKeys = []) {
  const salesSection = normalizeSalesManageSection(query.salesSection || query.section || '');
  const salesGroupFilter = normalizeAdminOrderGroupFilter(query.salesGroup || query.group || '', availableGroupKeys);
  let salesDateFrom = normalizeDateInput(query.salesDateFrom || query.dateFrom || '');
  let salesDateTo = normalizeDateInput(query.salesDateTo || query.dateTo || '');

  if (salesDateFrom && salesDateTo && salesDateFrom > salesDateTo) {
    const temp = salesDateFrom;
    salesDateFrom = salesDateTo;
    salesDateTo = temp;
  }

  return {
    salesSection,
    salesGroupFilter,
    salesDateFrom,
    salesDateTo
  };
}

function normalizeMyPageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'info') {
    return 'info';
  }
  if (section === 'orders') {
    return 'orders';
  }
  if (section === 'profile') {
    return 'profile';
  }
  return 'info';
}

function normalizeMyPageProfileTab(rawTab = '') {
  const tab = String(rawTab || '').trim().toLowerCase();
  if (tab === 'basic') {
    return 'basic';
  }
  if (tab === 'addressbook' || tab === 'address') {
    return 'addressbook';
  }
  if (tab === 'password') {
    return 'password';
  }
  return 'basic';
}

function buildMyPageProfilePath(tab = 'basic') {
  return `/mypage?section=profile&profileTab=${normalizeMyPageProfileTab(tab)}`;
}

function parseMyPageQuery(query = {}, availableGroupKeys = []) {
  const section = normalizeMyPageSection(query.section || '');
  const profileTab = normalizeMyPageProfileTab(query.profileTab || '');
  const orderGroupFilter = normalizeAdminOrderGroupFilter(query.group || query.orderGroup || '', availableGroupKeys);
  let orderDateFrom = normalizeDateInput(query.dateFrom || query.orderDateFrom || '');
  let orderDateTo = normalizeDateInput(query.dateTo || query.orderDateTo || '');

  if (orderDateFrom && orderDateTo && orderDateFrom > orderDateTo) {
    const temp = orderDateFrom;
    orderDateFrom = orderDateTo;
    orderDateTo = temp;
  }

  return {
    section,
    profileTab,
    orderGroupFilter,
    orderDateFrom,
    orderDateTo
  };
}

function normalizeHexColor(rawColor = '', fallback = '#000000') {
  const value = String(rawColor || '').trim();
  if (HEX_COLOR_REGEX.test(value)) {
    return value.toLowerCase();
  }
  const safeFallback = String(fallback || '').trim();
  if (HEX_COLOR_REGEX.test(safeFallback)) {
    return safeFallback.toLowerCase();
  }
  return '#000000';
}

function hexToRgb(hex = '#000000') {
  const normalized = normalizeHexColor(hex, '#000000');
  return {
    r: Number.parseInt(normalized.slice(1, 3), 16),
    g: Number.parseInt(normalized.slice(3, 5), 16),
    b: Number.parseInt(normalized.slice(5, 7), 16)
  };
}

function clampHexToThemePalette(rawColor = '', fallback = '#000000') {
  return normalizeHexColor(rawColor, fallback);
}

function maybeMigrateLegacyThemeColors() {
  const keyPairs = [
    ['HeaderColor', 'headerColor'],
    ['BackgroundColor', 'backgroundColor'],
    ['TextColor', 'textColor'],
    ['MutedColor', 'mutedColor'],
    ['LineColor', 'lineColor'],
    ['CardColor', 'cardColor'],
    ['CardDarkColor', 'cardDarkColor'],
    ['CardDarkTextColor', 'cardDarkTextColor'],
    ['ChipColor', 'chipColor']
  ];

  const migrateModeIfLegacy = (modeKey = 'day') => {
    const legacy = LEGACY_THEME_COLORS[modeKey];
    const nextDefaults = DEFAULT_THEME_COLORS[modeKey];
    if (!legacy || !nextDefaults) {
      return;
    }

    let matchedLegacyCount = 0;
    let hasStoredThemeValue = false;

    for (const [settingSuffix, aliasKey] of keyPairs) {
      const settingKey = `${modeKey}${settingSuffix}`;
      const storedRaw = String(getSetting(settingKey, '') || '').trim();
      if (storedRaw) {
        hasStoredThemeValue = true;
      }
      const stored = normalizeHexColor(storedRaw, legacy[aliasKey]);
      if (stored === normalizeHexColor(legacy[aliasKey], legacy[aliasKey])) {
        matchedLegacyCount += 1;
      }
    }

    if (!hasStoredThemeValue || matchedLegacyCount === keyPairs.length) {
      for (const [settingSuffix, aliasKey] of keyPairs) {
        setSetting(`${modeKey}${settingSuffix}`, nextDefaults[aliasKey]);
      }
    }
  };

  migrateModeIfLegacy('day');
  migrateModeIfLegacy('night');

  const nightSettingMap = [
    ['HeaderColor', 'headerColor'],
    ['BackgroundColor', 'backgroundColor'],
    ['TextColor', 'textColor'],
    ['MutedColor', 'mutedColor'],
    ['LineColor', 'lineColor'],
    ['CardColor', 'cardColor'],
    ['CardDarkColor', 'cardDarkColor'],
    ['CardDarkTextColor', 'cardDarkTextColor'],
    ['ChipColor', 'chipColor']
  ];
  const matchesPreviousNightV1 = nightSettingMap.every(([suffix, alias]) => {
    const stored = normalizeHexColor(
      getSetting(`night${suffix}`, PREVIOUS_NIGHT_THEME_COLORS_V1[alias]),
      PREVIOUS_NIGHT_THEME_COLORS_V1[alias]
    );
    return stored === normalizeHexColor(PREVIOUS_NIGHT_THEME_COLORS_V1[alias], PREVIOUS_NIGHT_THEME_COLORS_V1[alias]);
  });
  if (matchesPreviousNightV1) {
    for (const [suffix, alias] of nightSettingMap) {
      setSetting(`night${suffix}`, DEFAULT_THEME_COLORS.night[alias]);
    }
  }

  const themeRefinedV4Applied = String(getSetting(THEME_REFINED_V4_FLAG_KEY, '0') || '0');
  if (themeRefinedV4Applied !== '1') {
    for (const modeKey of ['day', 'night']) {
      for (const [suffix, alias] of nightSettingMap) {
        setSetting(`${modeKey}${suffix}`, DEFAULT_THEME_COLORS[modeKey][alias]);
      }
    }
    setSetting(THEME_REFINED_V4_FLAG_KEY, '1');
  }

  setSetting('headerColor', DEFAULT_THEME_COLORS.day.headerColor);
}

maybeMigrateLegacyThemeColors();

function getThemeColorConfig(themeMode = 'day') {
  const key = themeMode === 'night' ? 'night' : 'day';
  const defaults = DEFAULT_THEME_COLORS[key];
  const legacyHeader = clampHexToThemePalette(getSetting('headerColor', defaults.headerColor), defaults.headerColor);
  const legacyBg = clampHexToThemePalette(getSetting('backgroundValue', defaults.backgroundColor), defaults.backgroundColor);

  const resolved = {
    headerColor: clampHexToThemePalette(getSetting(`${key}HeaderColor`, legacyHeader), defaults.headerColor),
    backgroundColor: clampHexToThemePalette(getSetting(`${key}BackgroundColor`, legacyBg), defaults.backgroundColor),
    textColor: clampHexToThemePalette(getSetting(`${key}TextColor`, defaults.textColor), defaults.textColor),
    mutedColor: clampHexToThemePalette(getSetting(`${key}MutedColor`, defaults.mutedColor), defaults.mutedColor),
    lineColor: clampHexToThemePalette(getSetting(`${key}LineColor`, defaults.lineColor), defaults.lineColor),
    cardColor: clampHexToThemePalette(getSetting(`${key}CardColor`, defaults.cardColor), defaults.cardColor),
    cardDarkColor: clampHexToThemePalette(getSetting(`${key}CardDarkColor`, defaults.cardDarkColor), defaults.cardDarkColor),
    cardDarkTextColor: clampHexToThemePalette(
      getSetting(`${key}CardDarkTextColor`, defaults.cardDarkTextColor),
      defaults.cardDarkTextColor
    ),
    chipColor: clampHexToThemePalette(getSetting(`${key}ChipColor`, defaults.chipColor), defaults.chipColor)
  };

  // Guardrail: keep contrast sane even if saved palette combinations clash.
  if (resolved.lineColor === resolved.cardColor || resolved.lineColor === resolved.backgroundColor) {
    resolved.lineColor = defaults.lineColor;
  }
  if (resolved.textColor === resolved.backgroundColor || resolved.textColor === resolved.cardColor) {
    resolved.textColor = defaults.textColor;
  }
  if (resolved.cardDarkTextColor === resolved.cardDarkColor) {
    resolved.cardDarkTextColor = defaults.cardDarkTextColor;
  }

  return resolved;
}

function getThemeAssetConfig(themeMode = 'day') {
  const key = themeMode === 'night' ? 'night' : 'day';
  const readAssetPath = (settingKey, fallback = '') => {
    const saved = String(getSetting(settingKey, '') || '').trim();
    if (saved) {
      return saved;
    }
    return String(fallback || '').trim();
  };

  const defaultDayHeaderLogoPath = getBrandingAssetUrl('dayHeaderLogo');
  const defaultDayHeaderSymbolPath = getBrandingAssetUrl('dayHeaderSymbol');
  const defaultDayFooterLogoPath = getBrandingAssetUrl('dayFooterLogo');
  const defaultNightHeaderLogoPath = getBrandingAssetUrl('nightHeaderLogo') || defaultDayHeaderLogoPath;
  const defaultNightHeaderSymbolPath = getBrandingAssetUrl('nightHeaderSymbol') || defaultDayHeaderSymbolPath;
  const defaultNightFooterLogoPath = getBrandingAssetUrl('nightFooterLogo') || defaultDayFooterLogoPath;

  const legacyHeaderLogoPath = String(getSetting('headerLogoPath', '') || '').trim();
  const legacyHeaderSymbolPath = String(getSetting('headerSymbolPath', '') || '').trim();
  const legacyFooterLogoPath = String(getSetting('footerLogoPath', '') || '').trim();
  const legacyBackgroundType = String(getSetting('backgroundType', 'color') || 'color').trim();
  const legacyBackgroundValue = String(getSetting('backgroundValue', '') || '').trim();
  const dayHeaderLogoPath = readAssetPath('dayHeaderLogoPath', legacyHeaderLogoPath || defaultDayHeaderLogoPath);
  const dayHeaderSymbolPath = readAssetPath(
    'dayHeaderSymbolPath',
    legacyHeaderSymbolPath || defaultDayHeaderSymbolPath
  );
  const dayFooterLogoPath = readAssetPath('dayFooterLogoPath', legacyFooterLogoPath || defaultDayFooterLogoPath);
  const nightHeaderLogoPath = readAssetPath('nightHeaderLogoPath', defaultNightHeaderLogoPath || dayHeaderLogoPath);
  const nightHeaderSymbolPath = readAssetPath(
    'nightHeaderSymbolPath',
    defaultNightHeaderSymbolPath || dayHeaderSymbolPath
  );
  const nightFooterLogoPath = readAssetPath('nightFooterLogoPath', defaultNightFooterLogoPath || dayFooterLogoPath);
  const dayBackgroundType = String(
    getSetting('dayBackgroundType', legacyBackgroundType === 'image' ? 'image' : 'color') || 'color'
  ).trim();
  const dayBackgroundImagePath = String(
    getSetting(
      'dayBackgroundImagePath',
      legacyBackgroundType === 'image' && legacyBackgroundValue && !HEX_COLOR_REGEX.test(legacyBackgroundValue)
        ? legacyBackgroundValue
        : ''
    ) || ''
  ).trim();

  const normalizeType = (rawType = 'color') => (String(rawType).trim() === 'image' ? 'image' : 'color');
  const nightBackgroundType = String(getSetting('nightBackgroundType', 'color') || 'color').trim();
  const nightBackgroundImagePath = String(getSetting('nightBackgroundImagePath', '') || '').trim();
  const backgroundType = normalizeType(getSetting(`${key}BackgroundType`, key === 'day' ? dayBackgroundType : nightBackgroundType));
  const backgroundImagePath = String(
    getSetting(
      `${key}BackgroundImagePath`,
      key === 'day' ? dayBackgroundImagePath : nightBackgroundImagePath
    ) || ''
  ).trim();

  return {
    headerLogoPath: key === 'day' ? dayHeaderLogoPath : nightHeaderLogoPath,
    headerSymbolPath: key === 'day' ? dayHeaderSymbolPath : nightHeaderSymbolPath,
    footerLogoPath: key === 'day' ? dayFooterLogoPath : nightFooterLogoPath,
    backgroundType,
    backgroundImagePath: backgroundType === 'image' ? backgroundImagePath : ''
  };
}

function buildThemeCssVars(colorConfig = {}) {
  return [
    `--bg:${colorConfig.backgroundColor || DEFAULT_THEME_COLORS.day.backgroundColor}`,
    `--text:${colorConfig.textColor || DEFAULT_THEME_COLORS.day.textColor}`,
    `--muted:${colorConfig.mutedColor || DEFAULT_THEME_COLORS.day.mutedColor}`,
    `--line:${colorConfig.lineColor || DEFAULT_THEME_COLORS.day.lineColor}`,
    `--card:${colorConfig.cardColor || DEFAULT_THEME_COLORS.day.cardColor}`,
    `--card-dark:${colorConfig.cardDarkColor || DEFAULT_THEME_COLORS.day.cardDarkColor}`,
    `--card-dark-text:${colorConfig.cardDarkTextColor || DEFAULT_THEME_COLORS.day.cardDarkTextColor}`,
    `--chip:${colorConfig.chipColor || DEFAULT_THEME_COLORS.day.chipColor}`
  ].join(';');
}

function inferSecuritySectionFromRequest(req) {
  const querySection = normalizeSecuritySection(req?.query?.section || '');
  if (querySection !== 'profile') {
    return querySection;
  }

  const pathname = String(req?.path || '');
  if (pathname.includes('/security/alerts') || pathname.includes('/security/alert/')) {
    return 'alerts';
  }
  if (pathname.includes('/security/logs')) {
    return 'logs';
  }
  if (pathname.includes('/security/sub-admin') || pathname.includes('/security/admin/')) {
    return 'admins';
  }
  if (pathname.includes('/security/profile')) {
    return 'profile';
  }
  return 'profile';
}

function normalizeIpAddress(rawIp = '') {
  const candidate = Array.isArray(rawIp)
    ? String(rawIp[0] || '').trim()
    : String(rawIp || '').split(',')[0].trim();
  if (!candidate) {
    return '';
  }
  if (candidate.startsWith('::ffff:')) {
    return candidate.slice('::ffff:'.length).trim();
  }
  return candidate;
}

function isPublicIpAddress(ipAddress = '') {
  const ip = normalizeIpAddress(ipAddress).toLowerCase();
  if (!ip || ip === 'unknown' || ip === 'localhost') {
    return false;
  }

  if (ip.includes(':')) {
    if (ip === '::' || ip === '::1') {
      return false;
    }
    if (ip.startsWith('fc') || ip.startsWith('fd') || ip.startsWith('fe80')) {
      return false;
    }
    return true;
  }

  const octets = ip.split('.').map((part) => Number(part));
  if (octets.length !== 4 || octets.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
    return false;
  }

  const [a, b] = octets;
  if (a === 10 || a === 127 || a === 0) {
    return false;
  }
  if (a === 169 && b === 254) {
    return false;
  }
  if (a === 172 && b >= 16 && b <= 31) {
    return false;
  }
  if (a === 192 && b === 168) {
    return false;
  }
  if (a === 100 && b >= 64 && b <= 127) {
    return false;
  }
  if (a >= 224) {
    return false;
  }
  return true;
}

function parseSqliteUtcToMs(rawValue = '') {
  const value = String(rawValue || '').trim();
  if (!value) {
    return 0;
  }
  const parsed = Date.parse(value.replace(' ', 'T') + 'Z');
  return Number.isFinite(parsed) ? parsed : 0;
}

function isIpGeoCacheFresh(cacheRow = null) {
  if (!cacheRow) {
    return false;
  }
  const updatedAtMs = parseSqliteUtcToMs(cacheRow.updated_at || '');
  if (!updatedAtMs) {
    return false;
  }
  return Date.now() - updatedAtMs <= IP_GEO_CACHE_TTL_HOURS * 60 * 60 * 1000;
}

function formatIpLocationDisplay(rawLocation = {}) {
  const country = String(rawLocation.country || '').trim();
  const region = String(rawLocation.region || '').trim();
  const city = String(rawLocation.city || '').trim();
  const district = String(rawLocation.district || '').trim();
  const postalCode = String(rawLocation.postalCode || rawLocation.postal_code || '').trim();

  const parts = [country, region, city, district].filter(Boolean);
  const uniqueParts = parts.filter((value, index) => parts.indexOf(value) === index);
  if (postalCode && !uniqueParts.includes(postalCode)) {
    uniqueParts.push(postalCode);
  }
  return uniqueParts.join(' / ').slice(0, 200);
}

function getIpGeoCacheMapByIps(ipList = []) {
  const normalizedIps = Array.from(
    new Set(
      (Array.isArray(ipList) ? ipList : [])
        .map((value) => normalizeIpAddress(value))
        .filter(Boolean)
    )
  );
  if (normalizedIps.length === 0) {
    return new Map();
  }

  const placeholders = normalizedIps.map(() => '?').join(', ');
  const rows = db
    .prepare(
      `
        SELECT
          ip_address,
          country,
          region,
          city,
          district,
          postal_code,
          latitude,
          longitude,
          location_display,
          source,
          updated_at
        FROM ip_geolocation_cache
        WHERE ip_address IN (${placeholders})
      `
    )
    .all(...normalizedIps);

  const cacheMap = new Map();
  for (const row of rows) {
    const normalizedIp = normalizeIpAddress(row.ip_address || '');
    if (!normalizedIp) {
      continue;
    }
    cacheMap.set(normalizedIp, row);
  }
  return cacheMap;
}

function upsertIpGeoCache(ipAddress, geoData = {}) {
  const normalizedIp = normalizeIpAddress(ipAddress);
  if (!normalizedIp) {
    return;
  }

  const country = String(geoData.country || '').trim().slice(0, 80);
  const region = String(geoData.region || '').trim().slice(0, 120);
  const city = String(geoData.city || '').trim().slice(0, 120);
  const district = String(geoData.district || '').trim().slice(0, 120);
  const postalCode = String(geoData.postalCode || '').trim().slice(0, 40);
  const latitude = Number.isFinite(Number(geoData.latitude)) ? Number(geoData.latitude) : null;
  const longitude = Number.isFinite(Number(geoData.longitude)) ? Number(geoData.longitude) : null;
  const source = String(geoData.source || '').trim().slice(0, 40);
  const locationDisplay = String(geoData.locationDisplay || formatIpLocationDisplay({
    country,
    region,
    city,
    district,
    postalCode
  }))
    .trim()
    .slice(0, 200);

  db.prepare(
    `
      INSERT INTO ip_geolocation_cache (
        ip_address,
        country,
        region,
        city,
        district,
        postal_code,
        latitude,
        longitude,
        location_display,
        source,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(ip_address) DO UPDATE SET
        country = excluded.country,
        region = excluded.region,
        city = excluded.city,
        district = excluded.district,
        postal_code = excluded.postal_code,
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        location_display = excluded.location_display,
        source = excluded.source,
        updated_at = datetime('now')
    `
  ).run(
    normalizedIp,
    country,
    region,
    city,
    district,
    postalCode,
    latitude,
    longitude,
    locationDisplay,
    source
  );
}

async function fetchJsonWithTimeout(url, timeoutMs = IP_GEO_LOOKUP_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: { Accept: 'application/json' },
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`status:${response.status}`);
    }
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function lookupIpGeoFromIpApi(ipAddress) {
  const url =
    `http://ip-api.com/json/${encodeURIComponent(ipAddress)}` +
    '?fields=status,message,country,regionName,city,district,zip,lat,lon,query';
  const payload = await fetchJsonWithTimeout(url);
  if (!payload || payload.status !== 'success') {
    return null;
  }
  return {
    country: String(payload.country || '').trim(),
    region: String(payload.regionName || '').trim(),
    city: String(payload.city || '').trim(),
    district: String(payload.district || '').trim(),
    postalCode: String(payload.zip || '').trim(),
    latitude: payload.lat,
    longitude: payload.lon,
    source: 'ip-api'
  };
}

async function lookupIpGeoFromIpWhoIs(ipAddress) {
  const url = `https://ipwho.is/${encodeURIComponent(ipAddress)}`;
  const payload = await fetchJsonWithTimeout(url);
  if (!payload || payload.success !== true) {
    return null;
  }
  return {
    country: String(payload.country || '').trim(),
    region: String(payload.region || '').trim(),
    city: String(payload.city || '').trim(),
    district: String(payload.district || '').trim(),
    postalCode: String(payload.postal || '').trim(),
    latitude: payload.latitude,
    longitude: payload.longitude,
    source: 'ipwho.is'
  };
}

async function resolveIpGeolocation(ipAddress) {
  const normalizedIp = normalizeIpAddress(ipAddress);
  if (!isPublicIpAddress(normalizedIp)) {
    return null;
  }

  const providers = [lookupIpGeoFromIpApi, lookupIpGeoFromIpWhoIs];
  for (const provider of providers) {
    try {
      const result = await provider(normalizedIp);
      if (!result) {
        continue;
      }
      const locationDisplay = formatIpLocationDisplay(result);
      return {
        ...result,
        locationDisplay
      };
    } catch {
      continue;
    }
  }
  return null;
}

function queueIpGeolocationLookup(ipAddress) {
  const normalizedIp = normalizeIpAddress(ipAddress);
  if (!isPublicIpAddress(normalizedIp)) {
    return;
  }

  if (ipGeoLookupInFlight.has(normalizedIp)) {
    return;
  }

  const existingCache = db
    .prepare(
      `
        SELECT ip_address, updated_at
        FROM ip_geolocation_cache
        WHERE ip_address = ?
        LIMIT 1
      `
    )
    .get(normalizedIp);
  if (isIpGeoCacheFresh(existingCache)) {
    return;
  }

  const task = (async () => {
    const geoData = await resolveIpGeolocation(normalizedIp);
    if (!geoData) {
      return;
    }
    upsertIpGeoCache(normalizedIp, geoData);
  })();

  ipGeoLookupInFlight.set(normalizedIp, task);
  task
    .catch(() => {})
    .finally(() => {
      ipGeoLookupInFlight.delete(normalizedIp);
    });
}

function getIpLocationLabel(ipAddress, cacheMap, lang = 'ko') {
  const normalizedIp = normalizeIpAddress(ipAddress);
  if (!normalizedIp || normalizedIp === 'unknown') {
    return '';
  }

  const cached = cacheMap.get(normalizedIp);
  if (cached) {
    const cachedLabel = String(cached.location_display || '').trim();
    if (cachedLabel) {
      return cachedLabel;
    }
    const fallbackLabel = formatIpLocationDisplay({
      country: cached.country,
      region: cached.region,
      city: cached.city,
      district: cached.district,
      postalCode: cached.postal_code
    });
    if (fallbackLabel) {
      return fallbackLabel;
    }
  }

  if (!isPublicIpAddress(normalizedIp)) {
    return lang === 'en' ? 'Local/Private IP' : '내부/사설 IP';
  }
  return lang === 'en' ? 'Resolving location...' : '위치 확인 중...';
}

function getClientIp(req) {
  const rawIp = req.headers['cf-connecting-ip'] || req.headers['x-real-ip'] || req.headers['x-forwarded-for'] || req.ip || '';
  return normalizeIpAddress(rawIp) || 'unknown';
}

function normalizeCountryCode(value = '') {
  const normalized = String(value || '').trim().toUpperCase();
  return /^[A-Z]{2}$/.test(normalized) ? normalized : '';
}

function normalizeAsnNumber(rawValue) {
  if (Number.isInteger(rawValue) && rawValue > 0) {
    return rawValue;
  }
  const normalized = String(rawValue || '')
    .trim()
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  const extracted = normalized.match(/AS\s*([0-9]{1,10})/) || normalized.match(/^([0-9]{1,10})$/);
  if (!extracted) {
    return null;
  }
  const parsed = Number.parseInt(extracted[1], 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function isAdminIpAllowlisted(ipAddress = '') {
  const normalized = normalizeIpAddress(ipAddress);
  if (!normalized) {
    return false;
  }
  return ADMIN_WAF_IP_ALLOWLIST.has(normalized);
}

function isSuspiciousAdminUserAgent(userAgent = '') {
  const normalized = String(userAgent || '').trim();
  if (!normalized) {
    return true;
  }
  return ADMIN_WAF_BLOCKED_USER_AGENT_PATTERNS.some((pattern) => pattern.test(normalized));
}

async function lookupAdminNetworkProfileFromIpApi(ipAddress = '') {
  const url =
    `http://ip-api.com/json/${encodeURIComponent(ipAddress)}` +
    '?fields=status,message,countryCode,as,query';
  const payload = await fetchJsonWithTimeout(url, ADMIN_WAF_LOOKUP_TIMEOUT_MS);
  if (!payload || String(payload.status || '').toLowerCase() !== 'success') {
    return null;
  }
  return {
    countryCode: normalizeCountryCode(payload.countryCode || ''),
    asn: normalizeAsnNumber(payload.as || ''),
    source: 'ip-api'
  };
}

async function lookupAdminNetworkProfileFromIpWhoIs(ipAddress = '') {
  const url = `https://ipwho.is/${encodeURIComponent(ipAddress)}`;
  const payload = await fetchJsonWithTimeout(url, ADMIN_WAF_LOOKUP_TIMEOUT_MS);
  if (!payload || payload.success !== true) {
    return null;
  }
  return {
    countryCode: normalizeCountryCode(payload.country_code || ''),
    asn: normalizeAsnNumber(payload?.connection?.asn),
    source: 'ipwho.is'
  };
}

async function resolveAdminNetworkProfile(ipAddress = '') {
  const normalizedIp = normalizeIpAddress(ipAddress);
  if (!isPublicIpAddress(normalizedIp)) {
    return {
      ipAddress: normalizedIp,
      countryCode: '',
      asn: null,
      source: 'private'
    };
  }

  const cached = adminWafProfileCache.get(normalizedIp);
  if (cached && Number(cached.expiresAt || 0) > Date.now()) {
    return cached.profile;
  }

  const providers = [lookupAdminNetworkProfileFromIpApi, lookupAdminNetworkProfileFromIpWhoIs];
  for (const provider of providers) {
    try {
      const profile = await provider(normalizedIp);
      if (!profile) {
        continue;
      }
      const normalizedProfile = {
        ipAddress: normalizedIp,
        countryCode: normalizeCountryCode(profile.countryCode || ''),
        asn: normalizeAsnNumber(profile.asn),
        source: String(profile.source || '').trim() || 'unknown'
      };
      adminWafProfileCache.set(normalizedIp, {
        profile: normalizedProfile,
        expiresAt: Date.now() + ADMIN_WAF_PROFILE_CACHE_TTL_MS
      });
      return normalizedProfile;
    } catch {
      continue;
    }
  }

  return null;
}

function getAdminShieldDenyMessage(req) {
  const lang = resolveLanguage(req?.query?.lang || req?.cookies?.lang, getSetting('languageDefault', 'ko'));
  if (lang === 'en') {
    return 'Access to the admin area is blocked by security policy.';
  }
  return '보안 정책에 따라 관리자 영역 접근이 차단되었습니다.';
}

function rejectAdminAccessShield(req, res, reason = 'policy_blocked') {
  const message = getAdminShieldDenyMessage(req);
  const requestPath = `${String(req.baseUrl || '')}${String(req.path || '')}`;
  const acceptHeader = String(req.get('accept') || '').toLowerCase();
  if (requestPath.startsWith('/api/') || req.xhr || acceptHeader.includes('application/json')) {
    return res.status(403).json({ ok: false, error: 'admin_access_blocked', reason, message });
  }
  return res
    .status(403)
    .type('text/html; charset=utf-8')
    .send(
      `<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Forbidden</title></head><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fb;color:#0f172a;padding:40px;"><main style="max-width:560px;margin:8vh auto;background:#fff;border:1px solid #d7deec;border-radius:14px;padding:28px;"><h1 style="margin:0 0 12px;font-size:28px;">Forbidden</h1><p style="line-height:1.6;margin:0;">${message}</p></main></body></html>`
    );
}

function detectSuspiciousAdminRequest(req) {
  const method = String(req.method || '').toUpperCase();
  if (!['GET', 'HEAD', 'POST'].includes(method)) {
    return {
      blocked: true,
      reason: 'method_block',
      detail: `method=${method}`
    };
  }

  const userAgent = String(req.get('user-agent') || '');
  if (isSuspiciousAdminUserAgent(userAgent)) {
    return {
      blocked: true,
      reason: 'bot_signature',
      detail: `ua=${userAgent.slice(0, 180)}`
    };
  }

  let requestUri = String(req.originalUrl || '').toLowerCase();
  try {
    requestUri = decodeURIComponent(requestUri);
  } catch {
    requestUri = String(req.originalUrl || '').toLowerCase();
  }
  const blockedFragments = ['../', '..%2f', '%2e%2e', '<script', 'union select', 'sleep(', 'benchmark('];
  if (blockedFragments.some((fragment) => requestUri.includes(fragment))) {
    return {
      blocked: true,
      reason: 'payload_signature',
      detail: `uri=${String(req.originalUrl || '').slice(0, 200)}`
    };
  }

  return { blocked: false, reason: '', detail: '' };
}

async function evaluateAdminAccessShield(req) {
  const requestPath = `${String(req.baseUrl || '')}${String(req.path || '')}`;
  if (!ADMIN_WAF_ENABLED) {
    return { allowed: true, reason: '', detail: '' };
  }
  if (!requestPath.startsWith('/admin') && !requestPath.startsWith('/api/admin')) {
    return { allowed: true, reason: '', detail: '' };
  }

  const clientIp = getClientIp(req);
  const allowlisted = isAdminIpAllowlisted(clientIp);
  if (ADMIN_WAF_IP_ALLOWLIST_ENFORCED) {
    if (ADMIN_WAF_IP_ALLOWLIST.size === 0) {
      return {
        allowed: false,
        reason: 'allowlist_empty',
        detail: 'ADMIN_WAF_IP_ALLOWLIST is empty while enforcement is enabled'
      };
    }
    if (!allowlisted) {
      return {
        allowed: false,
        reason: 'ip_not_allowlisted',
        detail: `ip=${clientIp}`
      };
    }
    return { allowed: true, reason: '', detail: '' };
  }

  if (allowlisted) {
    return { allowed: true, reason: '', detail: '' };
  }

  if (ADMIN_WAF_BOT_BLOCK_ENABLED) {
    const suspicious = detectSuspiciousAdminRequest(req);
    if (suspicious.blocked) {
      return {
        allowed: false,
        reason: suspicious.reason,
        detail: suspicious.detail
      };
    }
  }

  if (!ADMIN_WAF_GEO_BLOCK_ENABLED && !ADMIN_WAF_ASN_BLOCK_ENABLED) {
    return { allowed: true, reason: '', detail: '' };
  }

  if (!isPublicIpAddress(clientIp)) {
    return { allowed: true, reason: '', detail: '' };
  }

  const profile = await resolveAdminNetworkProfile(clientIp);
  if (!profile) {
    if (ADMIN_WAF_FAIL_CLOSED_ON_LOOKUP_ERROR) {
      return { allowed: false, reason: 'network_profile_unavailable', detail: `ip=${clientIp}` };
    }
    return { allowed: true, reason: '', detail: '' };
  }

  if (ADMIN_WAF_GEO_BLOCK_ENABLED && ADMIN_WAF_ALLOWED_COUNTRY_CODES.size > 0) {
    const countryCode = normalizeCountryCode(profile.countryCode || '');
    if (countryCode && !ADMIN_WAF_ALLOWED_COUNTRY_CODES.has(countryCode)) {
      return {
        allowed: false,
        reason: 'country_block',
        detail: `ip=${clientIp}, country=${countryCode}, source=${profile.source || 'unknown'}`
      };
    }
  }

  if (ADMIN_WAF_ASN_BLOCK_ENABLED && ADMIN_WAF_BLOCKED_ASNS.size > 0) {
    const asn = normalizeAsnNumber(profile.asn);
    if (asn && ADMIN_WAF_BLOCKED_ASNS.has(asn)) {
      return {
        allowed: false,
        reason: 'asn_block',
        detail: `ip=${clientIp}, asn=AS${asn}, source=${profile.source || 'unknown'}`
      };
    }
  }

  return { allowed: true, reason: '', detail: '' };
}

function logAdminActivity(req, actionType, detail = '') {
  if (!req.user?.isAdmin) {
    return;
  }
  const clientIp = getClientIp(req);

  db.prepare(
    `
      INSERT INTO admin_activity_logs (
        admin_user_id,
        admin_username,
        admin_role,
        ip_address,
        user_agent,
        method,
        path,
        action_type,
        detail
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(
    req.user.id,
    req.user.username,
    req.user.adminRole || '',
    clientIp,
    String(req.get('user-agent') || '').slice(0, 300),
    req.method,
    `${req.path}${req.url.includes('?') ? req.url.slice(req.path.length) : ''}`.slice(0, 300),
    String(actionType || '').slice(0, 80),
    String(detail || '').slice(0, 300)
  );
  queueIpGeolocationLookup(clientIp);
}

function logAdminActivityByUser(userRow, req, actionType, detail = '') {
  if (!userRow) {
    return;
  }
  const clientIp = getClientIp(req);

  db.prepare(
    `
      INSERT INTO admin_activity_logs (
        admin_user_id,
        admin_username,
        admin_role,
        ip_address,
        user_agent,
        method,
        path,
        action_type,
        detail
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(
    Number(userRow.id),
    String(userRow.username || ''),
    normalizeAdminRole(userRow.admin_role || ''),
    clientIp,
    String(req.get('user-agent') || '').slice(0, 300),
    req.method,
    req.path.slice(0, 300),
    String(actionType || '').slice(0, 80),
    String(detail || '').slice(0, 300)
  );
  queueIpGeolocationLookup(clientIp);
}

function recordSecurityAlert(req, reason, detail = '') {
  const isAdmin = Boolean(req.user?.isAdmin);
  const actorRole = isAdmin ? req.user.adminRole || ADMIN_ROLE.SUB : '';
  const actorName = isAdmin ? req.user.username : 'unknown';
  const actorId = isAdmin ? req.user.id : null;

  const clientIp = getClientIp(req);

  db.prepare(
    `
      INSERT INTO admin_security_alerts (
        actor_admin_user_id,
        actor_username,
        actor_role,
        ip_address,
        method,
        path,
        reason,
        detail
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(
    actorId,
    String(actorName || 'unknown'),
    String(actorRole || ''),
    clientIp,
    String(req.method || '').slice(0, 16),
    `${req.path}${req.url.includes('?') ? req.url.slice(req.path.length) : ''}`.slice(0, 300),
    String(reason || '').slice(0, 120),
    String(detail || '').slice(0, 300)
  );
  queueIpGeolocationLookup(clientIp);
  queueSecurityAlertNotification({
    reason: String(reason || '').slice(0, 120),
    detail: String(detail || '').slice(0, 300),
    ipAddress: clientIp,
    method: String(req.method || '').slice(0, 16),
    path: `${req.path}${req.url.includes('?') ? req.url.slice(req.path.length) : ''}`.slice(0, 300),
    actor: String(actorName || 'unknown'),
    role: String(actorRole || ''),
    requestId: String(req.requestId || '').slice(0, 120)
  });
}

function appendOrderStatusLog(orderId, orderNo, fromStatus, toStatus, eventNote = '') {
  if (!orderId || !orderNo || !toStatus) {
    return;
  }

  db.prepare(
    `
      INSERT INTO order_status_logs (order_id, order_no, from_status, to_status, event_note)
      VALUES (?, ?, ?, ?, ?)
    `
  ).run(
    Number(orderId),
    String(orderNo),
    fromStatus ? normalizeOrderStatus(fromStatus) : null,
    normalizeOrderStatus(toStatus),
    String(eventNote || '').slice(0, 200)
  );
}

let trackingPollInFlight = false;
let lastTrackingPollMs = 0;
let dashboardStatsCache = {
  expiresAt: 0,
  value: null
};

function createSalesId(prefix = 'sales') {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function parseNumericValue(value = '') {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const raw = String(value ?? '').trim();
  if (!raw) {
    return NaN;
  }

  const normalized = raw
    .replace(/,/g, '')
    .replace(/₩/g, '')
    .replace(/원/g, '')
    .replace(/krw/gi, '')
    .replace(/rmb/gi, '')
    .replace(/\s+/g, '');

  if (!/^-?\d+(\.\d+)?$/.test(normalized)) {
    return NaN;
  }
  return Number(normalized);
}

function parseNonNegativeNumber(value, fallback = 0) {
  const numeric = parseNumericValue(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return fallback;
  }
  return numeric;
}

function normalizeSalesText(value, maxLength = 120) {
  return String(value || '').trim().slice(0, maxLength);
}

function getTodayDateString() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function normalizeSalesDate(value = '') {
  const normalized = String(value || '')
    .trim()
    .replace(/[./]/g, '-');
  if (!normalized) return '';
  if (!DATE_INPUT_REGEX.test(normalized)) return '';
  return normalized;
}

function getSalesScopeMode(tabKey = '') {
  const key = String(tabKey || '').trim();
  if (!key) {
    return 'date';
  }
  const matchedTab = getSalesMainTabs().find((tab) => String(tab?.key || '').trim() === key);
  if (matchedTab?.scopeType === 'factory') return 'factory';
  if (matchedTab?.scopeType === 'round') return 'round';
  if (matchedTab?.scopeType === 'date') return 'date';
  if (key === 'preorder') return 'round';
  return 'date';
}

const SALES_PRICE_SHEET_CATEGORY_TYPES = Object.freeze({
  FACTORY: 'factory',
  BRAND: 'brand',
  MODEL: 'model'
});
const SALES_PRICE_SHEET_CATEGORY_TYPE_SET = new Set(Object.values(SALES_PRICE_SHEET_CATEGORY_TYPES));

function normalizeSalesPriceSheetCategoryType(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  if (SALES_PRICE_SHEET_CATEGORY_TYPE_SET.has(normalized)) {
    return normalized;
  }
  return SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY;
}

const SALES_DATE_TAB_LEGACY_GROUP_KEY_MAP = Object.freeze({
  '\uacf5\uc7a5\uc81c': 'factory',
  '\uc820\ud30c\uce20': 'genparts',
  '\ud604\uc9c0\uc911\uace0': 'used'
});
const SALES_PRICE_TAB_LEGACY_GROUP_KEY_MAP = Object.freeze({
  '\uacf5\uc7a5\uc81c': 'price'
});
const SALES_BASE_GROUP_TAB_CONFIGS = Object.freeze([
  { key: '공장제', labelKo: '공장제', labelEn: 'Factory' },
  { key: '젠파츠', labelKo: '젠파츠', labelEn: 'Gen Parts' },
  { key: '현지중고', labelKo: '현지중고', labelEn: 'Local Used' }
]);

const SALES_DYNAMIC_TAB_KEY_PREFIX = 'group';
const SALES_DYNAMIC_TAB_SLUG_REGEX = /[^a-z0-9\uac00-\ud7a3]+/g;

function buildSalesDateTabKeyByGroupKey(rawGroupKey = '') {
  const groupKey = normalizeProductGroupKey(rawGroupKey || '');
  if (!groupKey) {
    return '';
  }

  const legacyKey = SALES_DATE_TAB_LEGACY_GROUP_KEY_MAP[groupKey];
  if (legacyKey) {
    return legacyKey;
  }

  const slug = String(groupKey)
    .trim()
    .toLowerCase()
    .replace(SALES_DYNAMIC_TAB_SLUG_REGEX, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
  return slug ? `${SALES_DYNAMIC_TAB_KEY_PREFIX}-${slug}` : '';
}

function buildSalesPriceTabKeyByGroupKey(rawGroupKey = '') {
  const groupKey = normalizeProductGroupKey(rawGroupKey || '');
  if (!groupKey) {
    return '';
  }

  const legacyKey = SALES_PRICE_TAB_LEGACY_GROUP_KEY_MAP[groupKey];
  if (legacyKey) {
    return legacyKey;
  }

  const slug = String(groupKey)
    .trim()
    .toLowerCase()
    .replace(SALES_DYNAMIC_TAB_SLUG_REGEX, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
  return slug ? `price-${slug}` : '';
}

function getObservedProductGroupKeys() {
  try {
    const rows = db
      .prepare(
        `
          SELECT DISTINCT category_group
          FROM products
          WHERE TRIM(COALESCE(category_group, '')) != ''
          ORDER BY category_group ASC
        `
      )
      .all();

    return rows
      .map((row) => normalizeProductGroupKey(row?.category_group || ''))
      .filter(Boolean)
      .filter((groupKey, index, arr) => arr.indexOf(groupKey) === index);
  } catch {
    return [];
  }
}

function getSalesMainTabs(groupConfigs = null) {
  const sourceGroups =
    Array.isArray(groupConfigs) && groupConfigs.length > 0
      ? groupConfigs
      : getProductGroupConfigs();
  const safeGroups =
    Array.isArray(sourceGroups) && sourceGroups.length > 0
      ? sourceGroups
      : SHOP_PRODUCT_GROUPS.map((groupKey) => ({
          key: groupKey,
          labelKo: groupKey,
          labelEn: groupKey
        }));
  const safeGroupMap = new Map();
  safeGroups.forEach((group) => {
    const key = normalizeProductGroupKey(group?.key || '');
    if (!key || safeGroupMap.has(key)) {
      return;
    }
    safeGroupMap.set(key, {
      ...group,
      key,
      labelKo: String(group?.labelKo || key).trim() || key,
      labelEn: String(group?.labelEn || group?.labelKo || key).trim() || key
    });
  });
  SALES_BASE_GROUP_TAB_CONFIGS.forEach((baseGroup) => {
    const key = normalizeProductGroupKey(baseGroup?.key || '');
    if (!key || safeGroupMap.has(key)) {
      return;
    }
    safeGroupMap.set(key, {
      key,
      labelKo: String(baseGroup?.labelKo || key).trim() || key,
      labelEn: String(baseGroup?.labelEn || baseGroup?.labelKo || key).trim() || key
    });
  });
  const mergedGroups = [...safeGroupMap.values()];

  const roundTabs = SALES_MAIN_TABS.filter((item) => item.scopeType === 'round').map((item) => ({ ...item }));
  const priceTabs = [];
  const dateTabs = [];
  const legacyTabByKey = new Map(SALES_MAIN_TABS.map((item) => [item.key, item]));
  const allDateTab = {
    key: SALES_ALL_DATE_TAB_KEY,
    labelKo: SALES_ALL_DATE_TAB_LABEL_KO,
    labelEn: SALES_ALL_DATE_TAB_LABEL_EN,
    scopeType: 'date',
    groupKey: SALES_ALL_DATE_TAB_GROUP_KEY,
    readOnly: true,
    aggregateType: 'all'
  };
  const usedTabKeys = new Set(
    [...roundTabs.map((item) => String(item.key || '').trim()), SALES_ALL_DATE_TAB_KEY].filter(Boolean)
  );

  mergedGroups.forEach((group, index) => {
    const groupKey = normalizeProductGroupKey(group?.key || '');
    if (!groupKey) {
      return;
    }

    const labelKoBase = String(group?.labelKo || groupKey).trim() || groupKey;
    const labelEnBase = String(group?.labelEn || labelKoBase).trim() || labelKoBase;

    const defaultPriceTabKey = buildSalesPriceTabKeyByGroupKey(groupKey) || `price-${index + 1}`;
    let priceTabKey = defaultPriceTabKey;
    let priceKeySuffix = 2;
    while (usedTabKeys.has(priceTabKey)) {
      priceTabKey = `${defaultPriceTabKey}-${priceKeySuffix}`;
      priceKeySuffix += 1;
    }
    usedTabKeys.add(priceTabKey);

    const legacyPriceKey = SALES_PRICE_TAB_LEGACY_GROUP_KEY_MAP[groupKey] || '';
    const legacyPriceTab = legacyPriceKey ? legacyTabByKey.get(legacyPriceKey) : null;
    priceTabs.push({
      key: priceTabKey,
      labelKo: String(legacyPriceTab?.labelKo || `${labelKoBase} 가격표`),
      labelEn: String(legacyPriceTab?.labelEn || `${labelEnBase} Price Table`),
      scopeType: 'factory',
      groupKey
    });

    const defaultDateTabKey = buildSalesDateTabKeyByGroupKey(groupKey) || `${SALES_DYNAMIC_TAB_KEY_PREFIX}-${index + 1}`;
    let dateTabKey = defaultDateTabKey;
    let dateKeySuffix = 2;
    while (usedTabKeys.has(dateTabKey)) {
      dateTabKey = `${defaultDateTabKey}-${dateKeySuffix}`;
      dateKeySuffix += 1;
    }
    usedTabKeys.add(dateTabKey);
    const legacyKey = SALES_DATE_TAB_LEGACY_GROUP_KEY_MAP[groupKey] || '';
    const legacyTab = legacyKey ? legacyTabByKey.get(legacyKey) : null;
    dateTabs.push({
      key: dateTabKey,
      labelKo: String(legacyTab?.labelKo || `${labelKoBase} 매출`),
      labelEn: String(legacyTab?.labelEn || `${labelEnBase} Sales`),
      scopeType: 'date',
      groupKey
    });
  });

  return [...priceTabs, ...roundTabs, allDateTab, ...dateTabs];
}

function findSalesTabByScopeAndGroup(tabs = [], scopeType = 'date', groupKey = '') {
  const normalizedGroupKey = normalizeProductGroupKey(groupKey || '');
  if (!normalizedGroupKey) {
    return null;
  }
  return (
    (Array.isArray(tabs) ? tabs : []).find(
      (tab) =>
        String(tab?.scopeType || '').trim() === String(scopeType || '').trim() &&
        normalizeProductGroupKey(tab?.groupKey || '') === normalizedGroupKey
    ) || null
  );
}

function migrateSalesWorkbookTabByRename(workbook = {}, previousTabDef = null, nextTabDef = null) {
  if (!workbook || typeof workbook !== 'object' || !workbook.tabs || typeof workbook.tabs !== 'object') {
    return false;
  }

  const previousTabKey = normalizeSalesText(previousTabDef?.key || '', 80);
  const nextTabKey = normalizeSalesText(nextTabDef?.key || '', 80);
  if (!nextTabKey) {
    return false;
  }

  const applyTabMeta = (targetTab) => {
    if (!targetTab || typeof targetTab !== 'object') {
      return;
    }
    targetTab.key = nextTabKey;
    targetTab.labelKo = String(nextTabDef?.labelKo || targetTab.labelKo || nextTabKey);
    targetTab.labelEn = String(nextTabDef?.labelEn || targetTab.labelEn || targetTab.labelKo || nextTabKey);
    targetTab.scopeType = String(nextTabDef?.scopeType || targetTab.scopeType || 'date');
  };

  if (previousTabKey && previousTabKey !== nextTabKey) {
    const sourceTab = workbook.tabs[previousTabKey];
    if (sourceTab && typeof sourceTab === 'object') {
      const targetTab = workbook.tabs[nextTabKey];
      if (targetTab && typeof targetTab === 'object') {
        const sourceScopes =
          sourceTab.scopeType === 'factory'
            ? (Array.isArray(sourceTab.groups) ? sourceTab.groups : [])
            : (Array.isArray(sourceTab.rounds) ? sourceTab.rounds : []);
        const targetScopes =
          targetTab.scopeType === 'factory'
            ? (Array.isArray(targetTab.groups) ? targetTab.groups : [])
            : (Array.isArray(targetTab.rounds) ? targetTab.rounds : []);
        if (targetTab.scopeType === 'factory') {
          targetTab.groups = [...targetScopes, ...sourceScopes];
        } else {
          targetTab.rounds = [...targetScopes, ...sourceScopes];
        }
      } else {
        workbook.tabs[nextTabKey] = {
          ...sourceTab
        };
      }
      delete workbook.tabs[previousTabKey];
    }
  }

  const migratedTab = workbook.tabs[nextTabKey];
  if (!migratedTab || typeof migratedTab !== 'object') {
    return false;
  }

  applyTabMeta(migratedTab);
  return true;
}

function normalizeSalesSettingValues(raw = {}, fallback = {}) {
  const now = new Date().toISOString();
  return {
    exchangeRate: Number(
      parseNonNegativeNumber(
        raw?.exchangeRate,
        parseNonNegativeNumber(fallback?.exchangeRate, SALES_DEFAULT_EXCHANGE_RATE)
      ).toFixed(2)
    ),
    shippingFeeKrw: Math.round(
      parseNonNegativeNumber(
        raw?.shippingFeeKrw,
        parseNonNegativeNumber(fallback?.shippingFeeKrw, SALES_DEFAULT_SHIPPING_FEE_KRW)
      )
    ),
    fxSource: normalizeSalesText(raw?.fxSource, 40) || normalizeSalesText(fallback?.fxSource, 40) || 'manual',
    fxUpdatedAt: normalizeSalesText(raw?.fxUpdatedAt, 40) || normalizeSalesText(fallback?.fxUpdatedAt, 40),
    updatedAt: normalizeSalesText(raw?.updatedAt, 40) || normalizeSalesText(fallback?.updatedAt, 40) || now
  };
}

function extractGoogleSheetId(sourceUrl = '') {
  const value = String(sourceUrl || '').trim();
  if (!value) return '';

  const directMatch = value.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  if (directMatch?.[1]) {
    return directMatch[1];
  }

  if (/^[a-zA-Z0-9-_]{20,}$/.test(value)) {
    return value;
  }
  return '';
}

function parseGoogleVizResponseJson(rawText = '') {
  const value = String(rawText || '').trim();
  if (!value) {
    return null;
  }

  const start = value.indexOf('{');
  const end = value.lastIndexOf('}');
  if (start < 0 || end <= start) {
    return null;
  }

  try {
    return JSON.parse(value.slice(start, end + 1));
  } catch {
    return null;
  }
}

function normalizeGoogleSheetTable(rawTable = {}) {
  const cols = Array.isArray(rawTable?.cols) ? rawTable.cols : [];
  const rows = Array.isArray(rawTable?.rows) ? rawTable.rows : [];

  const headers = cols.map((col, idx) => String(col?.label || col?.id || `COL${idx + 1}`).trim());
  const normalizedRows = rows
    .map((row) => {
      const rowCells = Array.isArray(row?.c) ? row.c : [];
      return headers.map((_, idx) => {
        const cell = rowCells[idx] || null;
        const raw = cell && Object.prototype.hasOwnProperty.call(cell, 'v') ? cell.v : '';
        const display = cell && Object.prototype.hasOwnProperty.call(cell, 'f') ? String(cell.f || '') : String(raw ?? '');
        return display.trim();
      });
    })
    .filter((row) => row.some((cell) => String(cell || '').trim() !== ''));

  return {
    headers,
    rows: normalizedRows
  };
}

function normalizeSalesHeaderLabel(value = '') {
  return String(value || '').toLowerCase().replace(/\s+/g, '').replace(/\n/g, '');
}

function detectSalesColumnIndexes(headers = []) {
  const normalized = headers.map((header) => normalizeSalesHeaderLabel(header));
  const findBy = (keywords = []) =>
    normalized.findIndex((label) => keywords.some((keyword) => label.includes(normalizeSalesHeaderLabel(keyword))));

  return {
    no: findBy(['no']),
    factory: findBy(['공장']),
    brand: findBy(['브랜드']),
    model: findBy(['모델']),
    reference: findBy(['레퍼런스']),
    spec: findBy(['종류', '사이즈']),
    costRmb: findBy(['원가(rmb)', '원가rmb', '시계원가(rmb)', '파츠원가(rmb)']),
    saleKrw: findBy(['판매가격(krw)', '판매가격', '판매가']),
    qty: findBy(['주문량', '수량', '수주']),
    rate: findBy(['적용환율', '환율krw', '적용환율krw']),
    shippingKrw: findBy(['배송비krw', '배송비'])
  };
}

function createDefaultSalesRow(partial = {}) {
  return {
    id: normalizeSalesText(partial.id, 80) || createSalesId('row'),
    factory: normalizeSalesText(partial.factory, 60),
    brand: normalizeSalesText(partial.brand, 80),
    model: normalizeSalesText(partial.model, 120),
    reference: normalizeSalesText(partial.reference, 120),
    spec: normalizeSalesText(partial.spec, 160),
    costRmb: parseNonNegativeNumber(partial.costRmb, 0),
    saleKrw: parseNonNegativeNumber(partial.saleKrw, 0),
    quantity: Math.max(1, Math.floor(parseNonNegativeNumber(partial.quantity, 1) || 1)),
    memo: normalizeSalesText(partial.memo, 500)
  };
}

function normalizeSalesSheetName(value = '', maxLength = 120) {
  return normalizeSalesText(value, maxLength).replace(/\s+/g, ' ').trim();
}

function normalizeSalesSheetNameKey(value = '') {
  return normalizeSalesSheetName(value, 120).toLowerCase();
}

function compareSalesSheetNames(left = '', right = '') {
  return String(left || '').localeCompare(String(right || ''), 'ko', {
    sensitivity: 'base'
  });
}

function normalizeSalesScopeTaxonomy(rawTaxonomy = {}, rows = []) {
  const sourceBrands = Array.isArray(rawTaxonomy?.brands) ? rawTaxonomy.brands : [];
  const brandMap = new Map();

  const upsertBrand = (rawBrandName = '') => {
    const brandName = normalizeSalesSheetName(rawBrandName, 80);
    if (!brandName) return null;
    const brandKey = normalizeSalesSheetNameKey(brandName);
    if (!brandMap.has(brandKey)) {
      brandMap.set(brandKey, { name: brandName, models: [] });
    }
    return brandMap.get(brandKey);
  };

  const upsertModel = (brandEntry, rawModelName = '') => {
    if (!brandEntry) return;
    const modelName = normalizeSalesSheetName(rawModelName, 120);
    if (!modelName) return;
    const modelKey = normalizeSalesSheetNameKey(modelName);
    const hasModel = brandEntry.models.some((item) => normalizeSalesSheetNameKey(item) === modelKey);
    if (!hasModel) {
      brandEntry.models.push(modelName);
    }
  };

  sourceBrands.forEach((brandItem) => {
    const brandEntry = upsertBrand(brandItem?.name || '');
    const models = Array.isArray(brandItem?.models) ? brandItem.models : [];
    models.forEach((modelName) => upsertModel(brandEntry, modelName));
  });

  rows.forEach((row) => {
    const brandEntry = upsertBrand(row?.brand || '');
    upsertModel(brandEntry, row?.model || '');
  });

  const brands = [...brandMap.values()]
    .map((brandItem) => ({
      name: brandItem.name,
      models: [...brandItem.models].sort(compareSalesSheetNames)
    }))
    .sort((a, b) => compareSalesSheetNames(a.name, b.name));

  return { brands };
}

function buildDefaultPriceScopes(groupKey = '') {
  const groupConfigs = getProductGroupConfigs();
  const normalizedGroupKey = normalizeProductGroupKey(groupKey || '');
  const factoryLikeGroup =
    (normalizedGroupKey
      ? groupConfigs.find((group) => normalizeProductGroupKey(group?.key || '') === normalizedGroupKey)
      : null) ||
    groupConfigs.find((group) => String(group.key || '') === '공장제') ||
    groupConfigs.find((group) => String(group.mode || '') === PRODUCT_GROUP_MODE.FACTORY) ||
    null;

  const factoryNames = normalizeProductFilterOptionList(factoryLikeGroup?.factoryOptions || []);
  const scopeNames = factoryNames.length > 0 ? factoryNames : ['기본'];
  return scopeNames.map((name, idx) => ({
    id: createSalesId(`scope-p-${idx + 1}`),
    name: normalizeSalesText(name, 80) || `Factory ${idx + 1}`,
    categoryType: SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY,
    taxonomy: { brands: [] },
    rows: []
  }));
}

function buildSalesTaxonomyBaselineByGroupConfig(groupConfig = null) {
  if (!groupConfig || typeof groupConfig !== 'object') {
    return { brands: [] };
  }

  const brandOptions = getGroupBrandOptions(groupConfig);
  const modelOptionMap = getGroupModelOptionsByBrand(groupConfig);
  const hasModelOptionMap = Object.keys(modelOptionMap).length > 0;
  const fallbackModelOptions = getGroupModelOptions(groupConfig);

  const brands = brandOptions
    .map((brandValue) => {
      const brandName = normalizeSalesSheetName(brandValue, 80);
      if (!brandName) {
        return null;
      }
      const sourceModels = hasModelOptionMap
        ? getGroupModelOptionsForBrand(groupConfig, brandValue)
        : fallbackModelOptions;
      const models = normalizeProductFilterOptionList(sourceModels)
        .map((modelValue) => normalizeSalesSheetName(modelValue, 120))
        .filter(Boolean)
        .sort(compareSalesSheetNames);
      return {
        name: brandName,
        models
      };
    })
    .filter(Boolean)
    .sort((a, b) => compareSalesSheetNames(a.name, b.name));

  return { brands };
}

function mergeSalesTaxonomyWithBaseline(currentTaxonomy = {}, rows = [], baselineTaxonomy = { brands: [] }) {
  const current = normalizeSalesScopeTaxonomy(currentTaxonomy, rows);
  const baselineBrands = Array.isArray(baselineTaxonomy?.brands) ? baselineTaxonomy.brands : [];
  const brandMap = new Map();

  const upsertBrand = (rawBrandName = '') => {
    const brandName = normalizeSalesSheetName(rawBrandName, 80);
    if (!brandName) {
      return null;
    }
    const brandKey = normalizeSalesSheetNameKey(brandName);
    if (!brandMap.has(brandKey)) {
      brandMap.set(brandKey, { name: brandName, models: [] });
    }
    return brandMap.get(brandKey);
  };

  const upsertModel = (brandEntry, rawModelName = '') => {
    if (!brandEntry) {
      return;
    }
    const modelName = normalizeSalesSheetName(rawModelName, 120);
    if (!modelName) {
      return;
    }
    const modelKey = normalizeSalesSheetNameKey(modelName);
    const duplicated = brandEntry.models.some((item) => normalizeSalesSheetNameKey(item) === modelKey);
    if (!duplicated) {
      brandEntry.models.push(modelName);
    }
  };

  (Array.isArray(current.brands) ? current.brands : []).forEach((brandItem) => {
    const brandEntry = upsertBrand(brandItem?.name || '');
    const models = Array.isArray(brandItem?.models) ? brandItem.models : [];
    models.forEach((modelName) => upsertModel(brandEntry, modelName));
  });

  baselineBrands.forEach((brandItem) => {
    const brandEntry = upsertBrand(brandItem?.name || '');
    const models = Array.isArray(brandItem?.models) ? brandItem.models : [];
    models.forEach((modelName) => upsertModel(brandEntry, modelName));
  });

  const brands = [...brandMap.values()]
    .map((brandItem) => ({
      name: brandItem.name,
      models: [...brandItem.models].sort(compareSalesSheetNames)
    }))
    .sort((a, b) => compareSalesSheetNames(a.name, b.name));

  return { brands };
}

function createDefaultRoundName(tabKey = '', index = 1) {
  if (tabKey === 'preorder') {
    return `${index}차`;
  }
  if (getSalesScopeMode(tabKey) === 'date') {
    return getTodayDateString();
  }
  return index === 1 ? '기본' : `회차 ${index}`;
}

function buildDefaultSalesWorkbook() {
  const now = new Date().toISOString();
  const salesMainTabs = getSalesMainTabs();
  const defaultSettings = normalizeSalesSettingValues({
    exchangeRate: SALES_DEFAULT_EXCHANGE_RATE,
    shippingFeeKrw: SALES_DEFAULT_SHIPPING_FEE_KRW,
    fxSource: 'manual',
    fxUpdatedAt: '',
    updatedAt: now
  });
  const tabs = {};
  for (const tab of salesMainTabs) {
    const tabMode = getSalesScopeMode(tab.key);
    const tabSettings = { ...defaultSettings };
    if (tab.scopeType === 'factory') {
      tabs[tab.key] = {
        key: tab.key,
        labelKo: tab.labelKo,
        labelEn: tab.labelEn,
        scopeType: tab.scopeType,
        settings: tabSettings,
        groups: buildDefaultPriceScopes(tab.groupKey || '')
      };
      continue;
    }

    const defaultScopeDate = tabMode === 'date' ? getTodayDateString() : '';
    tabs[tab.key] = {
      key: tab.key,
      labelKo: tab.labelKo,
      labelEn: tab.labelEn,
      scopeType: tab.scopeType,
      settings: tabSettings,
      rounds: [
        {
          id: createSalesId(`round-${tab.key}`),
          name: createDefaultRoundName(tab.key, 1),
          date: defaultScopeDate,
          settings: {
            ...tabSettings,
            baseDate: defaultScopeDate
          },
          rows: []
        }
      ]
    };
  }

  return {
    version: 2,
    globals: {
      ...defaultSettings
    },
    tabs,
    meta: {
      importedFrom: 'local-workbook',
      importedAt: '',
      updatedAt: now
    }
  };
}

function normalizeSalesScope(rawScope = {}, index = 0, tabKey = '', scopeType = 'round', fallbackSettings = {}) {
  const fallbackPrefix = scopeType === 'factory' ? 'scope' : 'round';
  const fallbackName = scopeType === 'factory' ? `Factory ${index + 1}` : createDefaultRoundName(tabKey, index + 1);
  const rows = Array.isArray(rawScope?.rows) ? rawScope.rows : [];
  const scopeMode = getSalesScopeMode(tabKey);

  const normalizedRows = rows
    .map((row) => createDefaultSalesRow(row))
    .filter((row) => {
      const hasText = [row.factory, row.brand, row.model, row.reference, row.spec, row.memo].some((value) => value);
      const hasNumeric = row.costRmb > 0 || row.saleKrw > 0 || row.quantity > 1;
      return hasText || hasNumeric;
    });

  const normalizedName = normalizeSalesText(rawScope?.name, 80) || fallbackName;
  const explicitDate = normalizeSalesDate(rawScope?.date || rawScope?.baseDate || rawScope?.settings?.baseDate || '');
  const inferredDate = normalizeSalesDate(normalizedName);
  const scopeDate = explicitDate || (scopeMode === 'date' ? inferredDate || getTodayDateString() : '');
  const scopeSettings = normalizeSalesSettingValues(
    {
      ...(rawScope?.settings && typeof rawScope.settings === 'object' ? rawScope.settings : {}),
      exchangeRate: rawScope?.settings?.exchangeRate ?? rawScope?.exchangeRate,
      shippingFeeKrw: rawScope?.settings?.shippingFeeKrw ?? rawScope?.shippingFeeKrw,
      fxSource: rawScope?.settings?.fxSource ?? rawScope?.fxSource,
      fxUpdatedAt: rawScope?.settings?.fxUpdatedAt ?? rawScope?.fxUpdatedAt,
      updatedAt: rawScope?.settings?.updatedAt ?? rawScope?.updatedAt
    },
    fallbackSettings
  );

  const normalizedScope = {
    id: normalizeSalesText(rawScope?.id, 80) || createSalesId(`${fallbackPrefix}-${index + 1}`),
    name: scopeMode === 'date' ? scopeDate || normalizedName : normalizedName,
    date: scopeDate,
    settings: {
      ...scopeSettings,
      baseDate: scopeDate
    },
    rows: normalizedRows
  };
  if (scopeType === 'factory') {
    normalizedRows.forEach((row) => {
      if (!normalizeSalesSheetName(row.factory, 60)) {
        row.factory = normalizedScope.name;
      }
    });
    normalizedScope.categoryType = SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY;
    normalizedScope.taxonomy = normalizeSalesScopeTaxonomy(rawScope?.taxonomy || {}, normalizedRows);
  }
  return normalizedScope;
}

function rebuildAllSalesAggregateScopes(tabs = {}, workbookGlobals = {}, fallbackSettings = {}) {
  const dateBuckets = new Map();
  Object.entries(tabs || {}).forEach(([tabKey, tab]) => {
    if (String(tabKey || '').trim() === SALES_ALL_DATE_TAB_KEY) {
      return;
    }
    if (getSalesScopeMode(tabKey) !== 'date') {
      return;
    }
    const scopes = getSalesScopeList(tab);
    if (!Array.isArray(scopes) || scopes.length === 0) {
      return;
    }
    const sourceLabel = normalizeSalesText(tab?.labelKo || tab?.key || '', 60);
    scopes.forEach((scope) => {
      const scopeDate = normalizeSalesDate(scope?.settings?.baseDate || scope?.date || scope?.name || '');
      if (!scopeDate) {
        return;
      }
      if (!dateBuckets.has(scopeDate)) {
        dateBuckets.set(scopeDate, []);
      }
      const rows = Array.isArray(scope?.rows) ? scope.rows : [];
      rows.forEach((row) => {
        const normalizedRow = createDefaultSalesRow(row);
        if (!normalizedRow.factory && sourceLabel) {
          normalizedRow.factory = sourceLabel;
        }
        dateBuckets.get(scopeDate).push(normalizedRow);
      });
    });
  });

  const sortedDates = [...dateBuckets.keys()].sort((left, right) => {
    if (left === right) return 0;
    return left < right ? 1 : -1;
  });

  if (sortedDates.length === 0) {
    sortedDates.push(getTodayDateString());
  }

  return sortedDates.map((scopeDate, index) =>
    normalizeSalesScope(
      {
        id: createSalesId(`round-${SALES_ALL_DATE_TAB_KEY}-${index + 1}`),
        name: scopeDate,
        date: scopeDate,
        settings: {
          ...normalizeSalesSettingValues(fallbackSettings || {}, workbookGlobals || {}),
          baseDate: scopeDate
        },
        rows: dateBuckets.get(scopeDate) || []
      },
      index,
      SALES_ALL_DATE_TAB_KEY,
      'round',
      fallbackSettings
    )
  );
}

function normalizeSalesWorkbook(rawWorkbook = null) {
  const fallback = buildDefaultSalesWorkbook();
  const salesMainTabs = getSalesMainTabs();
  const source = rawWorkbook && typeof rawWorkbook === 'object' ? rawWorkbook : {};
  const sourceTabs = source.tabs && typeof source.tabs === 'object' ? source.tabs : {};
  const sourceGlobals = source.globals && typeof source.globals === 'object' ? source.globals : {};
  const sourceMeta = source.meta && typeof source.meta === 'object' ? source.meta : {};
  const now = new Date().toISOString();

  const globals = normalizeSalesSettingValues(sourceGlobals, {
    exchangeRate: SALES_DEFAULT_EXCHANGE_RATE,
    shippingFeeKrw: SALES_DEFAULT_SHIPPING_FEE_KRW,
    fxSource: 'manual',
    fxUpdatedAt: '',
    updatedAt: now
  });

  const tabs = {};
  for (const tab of salesMainTabs) {
    const rawTab = sourceTabs[tab.key] && typeof sourceTabs[tab.key] === 'object' ? sourceTabs[tab.key] : {};
    const tabSettings = normalizeSalesSettingValues(rawTab.settings || {}, globals);
    if (tab.scopeType === 'factory') {
      const rawGroups = Array.isArray(rawTab.groups)
        ? rawTab.groups
        : Array.isArray(rawTab.scopes)
          ? rawTab.scopes
          : [];
      let groups = rawGroups.map((group, index) => normalizeSalesScope(group, index, tab.key, 'factory', tabSettings));
      if (groups.length === 0) {
        groups = fallback.tabs[tab.key].groups.map((group, index) =>
          normalizeSalesScope(group, index, tab.key, 'factory', tabSettings)
        );
      }
      tabs[tab.key] = {
        key: tab.key,
        labelKo: tab.labelKo,
        labelEn: tab.labelEn,
        scopeType: tab.scopeType,
        settings: tabSettings,
        groups
      };
      continue;
    }

    const rawRounds = Array.isArray(rawTab.rounds)
      ? rawTab.rounds
      : Array.isArray(rawTab.scopes)
        ? rawTab.scopes
        : [];
    let rounds = rawRounds.map((round, index) => normalizeSalesScope(round, index, tab.key, 'round', tabSettings));
    if (rounds.length === 0) {
      rounds = fallback.tabs[tab.key].rounds.map((round, index) =>
        normalizeSalesScope(round, index, tab.key, 'round', tabSettings)
      );
    }

    tabs[tab.key] = {
      key: tab.key,
      labelKo: tab.labelKo,
      labelEn: tab.labelEn,
      scopeType: tab.scopeType,
      settings: tabSettings,
      rounds
    };
  }

  const allDateTab = tabs[SALES_ALL_DATE_TAB_KEY];
  if (allDateTab && typeof allDateTab === 'object') {
    allDateTab.rounds = rebuildAllSalesAggregateScopes(
      tabs,
      globals,
      normalizeSalesSettingValues(allDateTab.settings || {}, globals)
    );
  }

  return {
    version: 2,
    globals,
    tabs,
    meta: {
      importedFrom:
        normalizeSalesText(sourceMeta.importedFrom, 400) || 'local-workbook',
      importedAt: normalizeSalesText(sourceMeta.importedAt, 40),
      updatedAt: now
    }
  };
}

function getSalesWorkbook() {
  const rawSetting = String(getSetting(SALES_WORKBOOK_SETTING_KEY, '') || '').trim();
  let parsed = null;
  if (rawSetting) {
    try {
      parsed = JSON.parse(rawSetting);
    } catch {
      parsed = null;
    }
  }

  const normalized = normalizeSalesWorkbook(parsed);
  const normalizedJson = JSON.stringify(normalized);
  if (!rawSetting || rawSetting !== normalizedJson) {
    setSetting(SALES_WORKBOOK_SETTING_KEY, normalizedJson);
  }
  return normalized;
}

function saveSalesWorkbook(inputWorkbook = null, options = {}) {
  const normalized = normalizeSalesWorkbook(inputWorkbook);
  const importedFrom = normalizeSalesText(options.importedFrom || normalized.meta?.importedFrom || '', 400);
  const importedAt = normalizeSalesText(options.importedAt || normalized.meta?.importedAt || '', 40);
  const now = new Date().toISOString();

  const next = {
    ...normalized,
    globals: {
      ...normalized.globals,
      updatedAt: now
    },
    meta: {
      importedFrom: importedFrom || 'local-workbook',
      importedAt,
      updatedAt: now
    }
  };

  setSetting(SALES_WORKBOOK_SETTING_KEY, JSON.stringify(next));
  return next;
}

function syncSalesWorkbookPriceFilters(options = {}) {
  const force = options && options.force === true;
  const respectVersion = options && options.respectVersion === true;
  const markVersion = options && options.markVersion === false ? false : true;
  const currentVersion = String(getSetting(SALES_PRICE_FILTER_BASELINE_VERSION_KEY, '') || '').trim();

  if (!force && respectVersion && currentVersion === SALES_PRICE_FILTER_BASELINE_VERSION) {
    return {
      changed: false,
      skipped: true,
      workbook: getSalesWorkbook(),
      stats: {
        tabsProcessed: 0,
        scopesAdded: 0,
        namesUpdated: 0,
        taxonomiesUpdated: 0,
        categoryTypesFixed: 0
      }
    };
  }

  const productGroupConfigs = getProductGroupConfigs();
  const salesTabs = getSalesMainTabs(productGroupConfigs).filter(
    (tabInfo) => String(tabInfo?.scopeType || '').trim() === 'factory'
  );
  if (salesTabs.length === 0) {
    if (markVersion) {
      setSetting(SALES_PRICE_FILTER_BASELINE_VERSION_KEY, SALES_PRICE_FILTER_BASELINE_VERSION);
    }
    return {
      changed: false,
      skipped: false,
      workbook: getSalesWorkbook(),
      stats: {
        tabsProcessed: 0,
        scopesAdded: 0,
        namesUpdated: 0,
        taxonomiesUpdated: 0,
        categoryTypesFixed: 0
      }
    };
  }

  const workbook = getSalesWorkbook();
  const workbookTabs = workbook?.tabs && typeof workbook.tabs === 'object' ? workbook.tabs : {};
  let changed = false;
  const stats = {
    tabsProcessed: 0,
    scopesAdded: 0,
    namesUpdated: 0,
    taxonomiesUpdated: 0,
    categoryTypesFixed: 0
  };

  salesTabs.forEach((tabInfo) => {
    const tabKey = String(tabInfo?.key || '').trim();
    if (!tabKey || !workbookTabs[tabKey] || typeof workbookTabs[tabKey] !== 'object') {
      return;
    }
    stats.tabsProcessed += 1;
    const tab = workbookTabs[tabKey];
    if (!Array.isArray(tab.groups)) {
      tab.groups = [];
      changed = true;
    }

    const normalizedGroupKey = normalizeProductGroupKey(tabInfo?.groupKey || '');
    const groupConfig = normalizedGroupKey
      ? productGroupConfigs.find(
          (group) => normalizeProductGroupKey(group?.key || '') === normalizedGroupKey
        )
      : null;
    if (!groupConfig) {
      return;
    }

    const baselineTaxonomy = buildSalesTaxonomyBaselineByGroupConfig(groupConfig);
    const baselineFactoryNames = normalizeProductFilterOptionList(groupConfig.factoryOptions)
      .map((value) => normalizeSalesSheetName(value, 80))
      .filter(Boolean);
    const hasGroups = Array.isArray(tab.groups) && tab.groups.length > 0;

    if (!hasGroups) {
      const scopeNames = baselineFactoryNames.length > 0 ? baselineFactoryNames : ['기본'];
      tab.groups = scopeNames.map((scopeName, idx) => ({
        id: createSalesId(`scope-p-seed-${idx + 1}`),
        name: scopeName,
        categoryType: SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY,
        taxonomy: mergeSalesTaxonomyWithBaseline({}, [], baselineTaxonomy),
        rows: []
      }));
      changed = true;
      stats.scopesAdded += tab.groups.length;
    } else if (baselineFactoryNames.length > 0) {
      const placeholderKeySet = new Set(['기본', 'factory 1', 'factory']);
      if (tab.groups.length === 1) {
        const onlyScope = tab.groups[0] && typeof tab.groups[0] === 'object' ? tab.groups[0] : null;
        const onlyScopeName = normalizeSalesSheetName(onlyScope?.name || '', 80);
        const onlyScopeKey = normalizeSalesSheetNameKey(onlyScopeName);
        const rowCount = Array.isArray(onlyScope?.rows) ? onlyScope.rows.length : 0;
        if (
          onlyScope &&
          rowCount === 0 &&
          placeholderKeySet.has(onlyScopeKey) &&
          onlyScopeName !== baselineFactoryNames[0]
        ) {
          onlyScope.name = baselineFactoryNames[0];
          changed = true;
          stats.namesUpdated += 1;
        }
      }

      const existingScopeNameKeys = new Set(
        tab.groups
          .map((scope) => normalizeSalesSheetNameKey(scope?.name || ''))
          .filter(Boolean)
      );
      baselineFactoryNames.forEach((factoryName) => {
        const factoryKey = normalizeSalesSheetNameKey(factoryName);
        if (!factoryKey || existingScopeNameKeys.has(factoryKey)) {
          return;
        }
        tab.groups.push({
          id: createSalesId(`scope-p-seed-${tab.groups.length + 1}`),
          name: factoryName,
          categoryType: SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY,
          taxonomy: mergeSalesTaxonomyWithBaseline({}, [], baselineTaxonomy),
          rows: []
        });
        existingScopeNameKeys.add(factoryKey);
        changed = true;
        stats.scopesAdded += 1;
      });
    }

    tab.groups.forEach((scope) => {
      if (!scope || typeof scope !== 'object') {
        return;
      }

      const mergedTaxonomy = mergeSalesTaxonomyWithBaseline(
        scope.taxonomy || {},
        Array.isArray(scope.rows) ? scope.rows : [],
        baselineTaxonomy
      );
      if (JSON.stringify(scope.taxonomy || {}) !== JSON.stringify(mergedTaxonomy)) {
        scope.taxonomy = mergedTaxonomy;
        changed = true;
        stats.taxonomiesUpdated += 1;
      }

      if (
        normalizeSalesPriceSheetCategoryType(scope.categoryType) !==
        SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY
      ) {
        scope.categoryType = SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY;
        changed = true;
        stats.categoryTypesFixed += 1;
      }
    });
  });

  const savedWorkbook = changed
    ? saveSalesWorkbook(workbook, { importedFrom: 'local-workbook' })
    : workbook;

  if (markVersion) {
    setSetting(SALES_PRICE_FILTER_BASELINE_VERSION_KEY, SALES_PRICE_FILTER_BASELINE_VERSION);
  }

  return {
    changed,
    skipped: false,
    workbook: savedWorkbook,
    stats
  };
}

function applySalesWorkbookPriceFilterBaselineSeedOnce() {
  syncSalesWorkbookPriceFilters({
    force: false,
    respectVersion: true,
    markVersion: true
  });
}

function syncSalesWorkbookPriceFiltersSafely(options = {}) {
  try {
    return syncSalesWorkbookPriceFilters(options);
  } catch (error) {
    console.error('[sales] workbook filter sync failed:', error);
    return null;
  }
}

applySalesWorkbookPriceFilterBaselineSeedOnce();
applySalesWorkbookOrderBackfillSeedOnce();

function getSalesScopeList(tab = null) {
  if (!tab || typeof tab !== 'object') {
    return [];
  }
  if (tab.scopeType === 'factory') {
    return Array.isArray(tab.groups) ? tab.groups : [];
  }
  return Array.isArray(tab.rounds) ? tab.rounds : [];
}

function getEffectiveSalesSettings(tab = {}, scope = {}, globals = {}) {
  const tabMode = getSalesScopeMode(tab?.key || '');
  if (tabMode === 'factory') {
    return normalizeSalesSettingValues(tab?.settings || {}, globals || {});
  }
  const scopeSettings = scope?.settings && typeof scope.settings === 'object' ? scope.settings : {};
  return normalizeSalesSettingValues(scopeSettings, tab?.settings || globals || {});
}

function buildSalesRowComputed(row = {}, settings = {}) {
  const exchangeRate = parseNonNegativeNumber(settings.exchangeRate, SALES_DEFAULT_EXCHANGE_RATE);
  const shippingFeeKrw = parseNonNegativeNumber(settings.shippingFeeKrw, SALES_DEFAULT_SHIPPING_FEE_KRW);
  const costRmb = parseNonNegativeNumber(row.costRmb, 0);
  const saleKrw = parseNonNegativeNumber(row.saleKrw, 0);
  const quantity = Math.max(1, Math.floor(parseNonNegativeNumber(row.quantity, 1) || 1));

  const costKrw = Math.round(costRmb * exchangeRate);
  const finalCostKrw = costKrw + Math.round(shippingFeeKrw);
  const marginKrw = saleKrw - finalCostKrw;

  return {
    costKrw,
    finalCostKrw,
    marginKrw,
    totalCostKrw: finalCostKrw * quantity,
    totalSalesKrw: saleKrw * quantity,
    totalMarginKrw: marginKrw * quantity
  };
}

function buildSalesScopeSummary(scope = {}, settings = {}) {
  const rows = Array.isArray(scope.rows) ? scope.rows : [];
  let totalQty = 0;
  let totalCostKrw = 0;
  let totalSalesKrw = 0;
  let totalMarginKrw = 0;

  for (const row of rows) {
    const quantity = Math.max(1, Math.floor(parseNonNegativeNumber(row.quantity, 1) || 1));
    const computed = buildSalesRowComputed(row, settings);
    totalQty += quantity;
    totalCostKrw += computed.totalCostKrw;
    totalSalesKrw += computed.totalSalesKrw;
    totalMarginKrw += computed.totalMarginKrw;
  }

  return {
    rowCount: rows.length,
    totalQty,
    totalCostKrw,
    totalSalesKrw,
    totalMarginKrw
  };
}

function buildSalesWorkbookPayload(workbook) {
  const normalized = normalizeSalesWorkbook(workbook);
  const salesMainTabs = getSalesMainTabs();

  const tabs = salesMainTabs.map((tabInfo) => {
    const tab = normalized.tabs[tabInfo.key];
    const scopes = getSalesScopeList(tab).map((scope) => {
      const effectiveSettings = getEffectiveSalesSettings(tab, scope, normalized.globals);
      return {
        id: scope.id,
        name: scope.name,
        date: normalizeSalesDate(scope?.settings?.baseDate || scope?.date || ''),
        categoryType:
          tabInfo.scopeType === 'factory'
            ? normalizeSalesPriceSheetCategoryType(scope?.categoryType || '')
            : '',
        taxonomy:
          tabInfo.scopeType === 'factory'
            ? normalizeSalesScopeTaxonomy(scope?.taxonomy || {}, scope?.rows || [])
            : undefined,
        settings: effectiveSettings,
        summary: buildSalesScopeSummary(scope, effectiveSettings),
        rows: scope.rows.map((row) => ({
          ...row,
          computed: buildSalesRowComputed(row, effectiveSettings)
        }))
      };
    });
    return {
      key: tabInfo.key,
      labelKo: tabInfo.labelKo,
      labelEn: tabInfo.labelEn,
      scopeType: tabInfo.scopeType,
      settings: normalizeSalesSettingValues(tab?.settings || {}, normalized.globals),
      scopes
    };
  });

  return {
    workbook: normalized,
    globals: normalized.globals,
    tabs
  };
}

function normalizeSalesMatchToken(value = '') {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[^a-z0-9가-힣]/g, '');
}

function isSalesTokenMatch(a = '', b = '') {
  const left = normalizeSalesMatchToken(a);
  const right = normalizeSalesMatchToken(b);
  if (!left || !right) {
    return false;
  }
  return left === right || left.includes(right) || right.includes(left);
}

function getSalesTabKeyForCategoryGroup(categoryGroup = '', groupConfigs = null) {
  const normalized = normalizeProductGroupKey(categoryGroup || '');
  if (!normalized) {
    return '';
  }

  const tabs = getSalesMainTabs(groupConfigs).filter(
    (tab) => String(tab?.scopeType || '') === 'date'
  );
  const matched = tabs.find(
    (tab) => String(tab?.scopeType || '') === 'date' && normalizeProductGroupKey(tab?.groupKey || '') === normalized
  );
  if (matched) {
    return String(matched.key || '').trim();
  }

  const matchedByToken = tabs.find((tab) =>
    isSalesTokenMatch(tab?.groupKey || '', normalized)
  );
  if (matchedByToken) {
    return String(matchedByToken.key || '').trim();
  }

  return '';
}

function getSalesWorkbookSyncedOrderIds() {
  const rawValue = String(getSetting(SALES_ORDER_WORKBOOK_SYNCED_IDS_KEY, '[]') || '').trim();
  if (!rawValue) {
    return [];
  }

  try {
    const parsed = JSON.parse(rawValue);
    if (!Array.isArray(parsed)) {
      return [];
    }
    const deduped = [...new Set(
      parsed
        .map((value) => Number.parseInt(String(value ?? ''), 10))
        .filter((value) => Number.isInteger(value) && value > 0)
    )];
    deduped.sort((left, right) => left - right);
    return deduped;
  } catch {
    return [];
  }
}

function setSalesWorkbookSyncedOrderIds(orderIds = []) {
  const normalized = [...new Set(
    (Array.isArray(orderIds) ? orderIds : [])
      .map((value) => Number.parseInt(String(value ?? ''), 10))
      .filter((value) => Number.isInteger(value) && value > 0)
  )].sort((left, right) => left - right);

  const clipped =
    normalized.length > SALES_ORDER_WORKBOOK_MAX_SYNCED_IDS
      ? normalized.slice(normalized.length - SALES_ORDER_WORKBOOK_MAX_SYNCED_IDS)
      : normalized;

  setSetting(SALES_ORDER_WORKBOOK_SYNCED_IDS_KEY, JSON.stringify(clipped));
  return clipped;
}

function buildSalesOrderSyncMemoToken(orderId = 0) {
  const resolvedOrderId = Number.parseInt(String(orderId ?? ''), 10);
  if (!Number.isInteger(resolvedOrderId) || resolvedOrderId <= 0) {
    return '';
  }
  return `${SALES_ORDER_WORKBOOK_SYNC_MEMO_PREFIX}#${resolvedOrderId}`;
}

function appendSalesOrderSyncMemo(existingMemo = '', orderId = 0, orderNo = '') {
  const token = buildSalesOrderSyncMemoToken(orderId);
  if (!token) {
    return normalizeSalesText(existingMemo || '', 500);
  }
  const currentMemo = normalizeSalesText(existingMemo || '', 500);
  if (currentMemo.includes(token)) {
    return currentMemo;
  }
  const orderText = String(orderNo || '').trim();
  const tokenWithOrderNo = orderText ? `${token}(${orderText})` : token;
  return normalizeSalesText(currentMemo ? `${currentMemo} ${tokenWithOrderNo}` : tokenWithOrderNo, 500);
}

function resolveSalesWorkbookScopeDateFromOrder(order = {}) {
  const saleDate = normalizeSalesDate(order?.sale_date || '');
  if (saleDate) {
    return saleDate;
  }
  const explicitDate = normalizeSalesDate(order?.sales_scope_date || '');
  if (explicitDate) {
    return explicitDate;
  }
  return getTodayDateString();
}

function ensureSalesDateScopeForOrderSync(tab = {}, scopeDate = '', workbookGlobals = {}) {
  if (!tab || typeof tab !== 'object') {
    return null;
  }

  const normalizedScopeDate = normalizeSalesDate(scopeDate || '') || getTodayDateString();
  if (!Array.isArray(tab.rounds)) {
    tab.rounds = [];
  }

  const existing = tab.rounds.find(
    (scope) =>
      normalizeSalesDate(scope?.settings?.baseDate || scope?.date || scope?.name || '') === normalizedScopeDate
  );
  if (existing) {
    if (!existing.settings || typeof existing.settings !== 'object') {
      existing.settings = {};
    }
    existing.settings = normalizeSalesSettingValues(existing.settings, tab.settings || workbookGlobals || {});
    existing.settings.baseDate = normalizedScopeDate;
    existing.date = normalizedScopeDate;
    existing.name = normalizedScopeDate;
    if (!Array.isArray(existing.rows)) {
      existing.rows = [];
    }
    return existing;
  }

  const tabSettings = normalizeSalesSettingValues(tab.settings || {}, workbookGlobals || {});
  const nextScope = {
    id: createSalesId(`round-${tab.key || 'date'}`),
    name: normalizedScopeDate,
    date: normalizedScopeDate,
    settings: {
      ...tabSettings,
      baseDate: normalizedScopeDate
    },
    rows: []
  };
  tab.rounds.push(nextScope);
  tab.rounds.sort((left, right) => {
    const leftDate = normalizeSalesDate(left?.settings?.baseDate || left?.date || left?.name || '');
    const rightDate = normalizeSalesDate(right?.settings?.baseDate || right?.date || right?.name || '');
    if (leftDate === rightDate) return 0;
    return leftDate < rightDate ? 1 : -1;
  });
  return nextScope;
}

function syncPaidOrdersToSalesWorkbook(options = {}) {
  const targetOrderIds = [...new Set(
    (Array.isArray(options?.orderIds) ? options.orderIds : [])
      .map((value) => Number.parseInt(String(value ?? ''), 10))
      .filter((value) => Number.isInteger(value) && value > 0)
  )];
  const forceResync = options?.forceResync === true;
  const syncedOrderIdSet = new Set(getSalesWorkbookSyncedOrderIds());
  const groupConfigs = getProductGroupConfigs();
  const workbook = getSalesWorkbook();
  const workbookTabs = workbook?.tabs && typeof workbook.tabs === 'object' ? workbook.tabs : {};

  const whereParts = [
    "UPPER(TRIM(o.status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')"
  ];
  const params = [];
  if (targetOrderIds.length > 0) {
    whereParts.push(`o.id IN (${targetOrderIds.map(() => '?').join(', ')})`);
    params.push(...targetOrderIds);
  }

  const rows = db
    .prepare(
      `
        SELECT
          o.id,
          o.order_no,
          o.product_id,
          o.quantity,
          o.total_price,
          o.sales_tab_key,
          o.sales_scope_date,
          o.sales_cost_rmb_snapshot,
          date(datetime(COALESCE(o.checked_at, o.created_at), '+9 hours')) AS sale_date,
          p.category_group,
          p.brand,
          p.model,
          p.sub_model,
          p.reference,
          p.factory_name
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE ${whereParts.join(' AND ')}
        ORDER BY o.id ASC
      `
    )
    .all(...params);

  let workbookChanged = false;
  let syncedIdsChanged = false;
  let orderSnapshotChanged = false;
  let addedRows = 0;
  let updatedRows = 0;

  for (const order of rows) {
    const orderId = Number.parseInt(String(order?.id ?? ''), 10);
    if (!Number.isInteger(orderId) || orderId <= 0) {
      continue;
    }
    const resolvedTabKey =
      normalizeSalesText(order?.sales_tab_key || '', 80) ||
      getSalesTabKeyForCategoryGroup(order?.category_group || '', groupConfigs);
    const tab = resolvedTabKey ? workbookTabs[resolvedTabKey] : null;
    if (!tab || typeof tab !== 'object' || getSalesScopeMode(resolvedTabKey) !== 'date') {
      continue;
    }

    const scopeDate = resolveSalesWorkbookScopeDateFromOrder(order);
    const currentScopeDate = normalizeSalesDate(order?.sales_scope_date || '');
    const currentTabKey = normalizeSalesText(order?.sales_tab_key || '', 80);
    const snapshotOutdated = currentTabKey !== resolvedTabKey || currentScopeDate !== scopeDate;
    const shouldEvaluate =
      forceResync ||
      !syncedOrderIdSet.has(orderId) ||
      snapshotOutdated;
    if (!shouldEvaluate) {
      continue;
    }

    const scope = ensureSalesDateScopeForOrderSync(tab, scopeDate, workbook.globals || {});
    if (!scope) {
      continue;
    }
    if (!Array.isArray(scope.rows)) {
      scope.rows = [];
    }

    const token = buildSalesOrderSyncMemoToken(orderId);
    const scopeList = Array.isArray(tab.rounds) ? tab.rounds : [];
    const tokenScopes = token
      ? scopeList.filter(
          (candidate) =>
            candidate &&
            Array.isArray(candidate.rows) &&
            candidate.rows.some((row) => String(row?.memo || '').includes(token))
        )
      : [];

    const hasTokenInTargetScope = tokenScopes.some((candidate) => candidate === scope);
    if (hasTokenInTargetScope) {
      if (!syncedOrderIdSet.has(orderId)) {
        syncedOrderIdSet.add(orderId);
        syncedIdsChanged = true;
      }
      if (snapshotOutdated) {
        db.prepare(
          `
            UPDATE orders
            SET
              sales_tab_key = ?,
              sales_scope_date = ?,
              sales_synced_at = datetime('now')
            WHERE id = ?
          `
        ).run(resolvedTabKey, scopeDate, orderId);
        orderSnapshotChanged = true;
      }
      continue;
    }

    if ((forceResync || snapshotOutdated) && tokenScopes.length > 0) {
      let movedRowCount = 0;
      tokenScopes.forEach((candidateScope) => {
        if (!candidateScope || !Array.isArray(candidateScope.rows) || candidateScope === scope) {
          return;
        }
        const remainedRows = [];
        candidateScope.rows.forEach((row) => {
          if (String(row?.memo || '').includes(token)) {
            scope.rows.push(row);
            movedRowCount += 1;
          } else {
            remainedRows.push(row);
          }
        });
        candidateScope.rows = remainedRows;
      });

      if (movedRowCount > 0) {
        workbookChanged = true;
        updatedRows += movedRowCount;
        if (!syncedOrderIdSet.has(orderId)) {
          syncedOrderIdSet.add(orderId);
          syncedIdsChanged = true;
        }
        if (snapshotOutdated) {
          db.prepare(
            `
              UPDATE orders
              SET
                sales_tab_key = ?,
                sales_scope_date = ?,
                sales_synced_at = datetime('now')
              WHERE id = ?
            `
          ).run(resolvedTabKey, scopeDate, orderId);
          orderSnapshotChanged = true;
        }
        continue;
      }
    }

    const quantity = Math.max(1, Math.floor(parseNonNegativeNumber(order?.quantity, 1) || 1));
    const totalPrice = Math.max(0, Math.round(parseNonNegativeNumber(order?.total_price, 0)));
    const unitSalePrice = quantity > 0 ? Math.round(totalPrice / quantity) : totalPrice;
    const costRmbSnapshot = parseNonNegativeNumber(order?.sales_cost_rmb_snapshot, 0);
    scope.rows.push(createDefaultSalesRow({
      factory: normalizeSalesText(order?.factory_name || '', 60) || normalizeSalesText(scope?.name || '', 60),
      brand: normalizeSalesText(order?.brand || '', 80),
      model: normalizeSalesText(order?.model || '', 120),
      reference: normalizeSalesText(order?.reference || '', 120),
      spec: normalizeSalesText(order?.sub_model || '', 160),
      costRmb: costRmbSnapshot,
      saleKrw: unitSalePrice,
      quantity,
      memo: appendSalesOrderSyncMemo('', orderId, order?.order_no || '')
    }));
    workbookChanged = true;
    addedRows += 1;

    if (!syncedOrderIdSet.has(orderId)) {
      syncedOrderIdSet.add(orderId);
      syncedIdsChanged = true;
    }

    if (snapshotOutdated) {
      db.prepare(
        `
          UPDATE orders
          SET
            sales_tab_key = ?,
            sales_scope_date = ?,
            sales_synced_at = datetime('now')
          WHERE id = ?
        `
      ).run(resolvedTabKey, scopeDate, orderId);
      orderSnapshotChanged = true;
    }
  }

  const savedWorkbook = workbookChanged
    ? saveSalesWorkbook(workbook, { importedFrom: 'local-workbook' })
    : workbook;

  if (syncedIdsChanged) {
    setSalesWorkbookSyncedOrderIds([...syncedOrderIdSet]);
  }

  return {
    changed: workbookChanged || syncedIdsChanged || orderSnapshotChanged,
    workbook: savedWorkbook,
    stats: {
      processed: rows.length,
      addedRows,
      updatedRows,
      syncedIdsChanged,
      orderSnapshotChanged
    }
  };
}

function syncPaidOrdersToSalesWorkbookSafely(options = {}) {
  try {
    return syncPaidOrdersToSalesWorkbook(options);
  } catch (error) {
    console.error('[sales] paid order workbook sync failed:', error);
    return null;
  }
}

function removeOrdersFromSalesWorkbook(options = {}) {
  const targetOrderIds = [...new Set(
    (Array.isArray(options?.orderIds) ? options.orderIds : [])
      .map((value) => Number.parseInt(String(value ?? ''), 10))
      .filter((value) => Number.isInteger(value) && value > 0)
  )];
  if (targetOrderIds.length === 0) {
    return {
      changed: false,
      workbook: getSalesWorkbook(),
      stats: { removedRows: 0, syncedIdsChanged: false }
    };
  }

  const tokenList = targetOrderIds
    .map((orderId) => buildSalesOrderSyncMemoToken(orderId))
    .filter(Boolean);
  if (tokenList.length === 0) {
    return {
      changed: false,
      workbook: getSalesWorkbook(),
      stats: { removedRows: 0, syncedIdsChanged: false }
    };
  }

  const workbook = getSalesWorkbook();
  const workbookTabs = workbook?.tabs && typeof workbook.tabs === 'object' ? workbook.tabs : {};

  let removedRows = 0;
  Object.values(workbookTabs).forEach((tab) => {
    if (!tab || typeof tab !== 'object' || !Array.isArray(tab.rounds)) {
      return;
    }
    tab.rounds.forEach((scope) => {
      if (!scope || typeof scope !== 'object' || !Array.isArray(scope.rows) || scope.rows.length === 0) {
        return;
      }
      const beforeLength = scope.rows.length;
      scope.rows = scope.rows.filter((row) => {
        const memo = String(row?.memo || '');
        return !tokenList.some((token) => memo.includes(token));
      });
      removedRows += Math.max(0, beforeLength - scope.rows.length);
    });
  });

  const syncedOrderIdSet = new Set(getSalesWorkbookSyncedOrderIds());
  let syncedIdsChanged = false;
  targetOrderIds.forEach((orderId) => {
    if (syncedOrderIdSet.delete(orderId)) {
      syncedIdsChanged = true;
    }
  });
  if (syncedIdsChanged) {
    setSalesWorkbookSyncedOrderIds([...syncedOrderIdSet]);
  }

  const workbookChanged = removedRows > 0;
  const savedWorkbook = workbookChanged
    ? saveSalesWorkbook(workbook, { importedFrom: 'local-workbook' })
    : workbook;

  return {
    changed: workbookChanged || syncedIdsChanged,
    workbook: savedWorkbook,
    stats: {
      removedRows,
      syncedIdsChanged
    }
  };
}

function removeOrdersFromSalesWorkbookSafely(options = {}) {
  try {
    return removeOrdersFromSalesWorkbook(options);
  } catch (error) {
    console.error('[sales] paid order workbook remove failed:', error);
    return null;
  }
}

function applySalesWorkbookOrderBackfillSeedOnce() {
  const currentVersion = String(
    getSetting(SALES_ORDER_WORKBOOK_BACKFILL_VERSION_KEY, '') || ''
  ).trim();
  if (currentVersion === SALES_ORDER_WORKBOOK_BACKFILL_VERSION) {
    return;
  }

  const result = syncPaidOrdersToSalesWorkbookSafely({ forceResync: true });
  if (result) {
    setSetting(
      SALES_ORDER_WORKBOOK_BACKFILL_VERSION_KEY,
      SALES_ORDER_WORKBOOK_BACKFILL_VERSION
    );
  }
}

function resolveSalesScopeDate(scope = {}) {
  return normalizeSalesDate(scope?.settings?.baseDate || scope?.date || scope?.name || '');
}

function pickSalesScopeForDate(tab = {}, tabKey = '', targetDate = '') {
  const scopes = getSalesScopeList(tab);
  if (!Array.isArray(scopes) || scopes.length === 0) {
    return null;
  }

  if (getSalesScopeMode(tabKey) !== 'date') {
    return scopes[0];
  }

  const normalizedTargetDate = normalizeSalesDate(targetDate || '');
  const datedScopes = scopes
    .map((scope) => ({
      scope,
      scopeDate: resolveSalesScopeDate(scope)
    }))
    .filter((item) => item.scopeDate);

  if (datedScopes.length === 0) {
    return scopes[0];
  }

  if (normalizedTargetDate) {
    const exact = datedScopes.find((item) => item.scopeDate === normalizedTargetDate);
    if (exact) {
      return exact.scope;
    }

    const previousOrSame = datedScopes
      .filter((item) => item.scopeDate <= normalizedTargetDate)
      .sort((a, b) => (a.scopeDate < b.scopeDate ? 1 : -1));
    if (previousOrSame.length > 0) {
      return previousOrSame[0].scope;
    }
  }

  datedScopes.sort((a, b) => (a.scopeDate < b.scopeDate ? 1 : -1));
  return datedScopes[0].scope;
}

function scoreSalesRowForProduct(row = {}, product = {}) {
  const productReference = normalizeSalesMatchToken(product.reference || '');
  const productBrand = normalizeSalesMatchToken(product.brand || '');
  const productModel = normalizeSalesMatchToken(product.model || '');
  const productFactory = normalizeSalesMatchToken(product.factory_name || '');
  const productSubModel = normalizeSalesMatchToken(product.sub_model || '');

  const rowReference = normalizeSalesMatchToken(row.reference || '');
  const rowBrand = normalizeSalesMatchToken(row.brand || '');
  const rowModel = normalizeSalesMatchToken(row.model || '');
  const rowFactory = normalizeSalesMatchToken(row.factory || '');
  const rowSpec = normalizeSalesMatchToken(row.spec || row.model || '');

  let score = 0;
  let strongMatched = false;

  const referenceMatched =
    Boolean(productReference) && Boolean(rowReference) && isSalesTokenMatch(productReference, rowReference);
  if (referenceMatched) {
    score += 100;
    strongMatched = true;
  }

  const brandMatched = Boolean(productBrand) && Boolean(rowBrand) && isSalesTokenMatch(productBrand, rowBrand);
  const modelMatched = Boolean(productModel) && Boolean(rowModel) && isSalesTokenMatch(productModel, rowModel);
  if (brandMatched) score += 20;
  if (modelMatched) score += 30;
  if (brandMatched && modelMatched) {
    score += 20;
    strongMatched = true;
  }

  if (productFactory && rowFactory && isSalesTokenMatch(productFactory, rowFactory)) {
    score += 8;
  }
  if (productSubModel && rowSpec && isSalesTokenMatch(productSubModel, rowSpec)) {
    score += 6;
  }

  if (!strongMatched) {
    return 0;
  }
  return score;
}

function findBestSalesRowForProduct(rows = [], product = {}) {
  let bestRow = null;
  let bestScore = 0;

  for (const row of Array.isArray(rows) ? rows : []) {
    const score = scoreSalesRowForProduct(row, product);
    if (score > bestScore) {
      bestRow = row;
      bestScore = score;
    }
  }

  return {
    row: bestRow,
    score: bestScore
  };
}

function buildOrderSalesSnapshot(input = {}) {
  const product = input?.product && typeof input.product === 'object' ? input.product : {};
  const quantity = Math.max(1, Math.floor(parseNonNegativeNumber(input.quantity, 1) || 1));
  const totalPrice = Math.max(0, Math.round(parseNonNegativeNumber(input.totalPrice, 0)));
  const baseDate = normalizeSalesDate(input.baseDate || '') || toKstDate();
  const tabKey = getSalesTabKeyForCategoryGroup(product.category_group || '');

  const fallbackSnapshot = {
    sales_tab_key: tabKey,
    sales_scope_id: '',
    sales_scope_name: '',
    sales_scope_date: baseDate,
    sales_exchange_rate_snapshot: SALES_DEFAULT_EXCHANGE_RATE,
    sales_shipping_fee_krw_snapshot: SALES_DEFAULT_SHIPPING_FEE_KRW,
    sales_cost_rmb_snapshot: 0,
    sales_cost_krw_snapshot: 0,
    sales_margin_krw_snapshot: totalPrice,
    sales_real_margin_krw_snapshot: totalPrice,
    sales_synced_at: new Date().toISOString()
  };

  if (!tabKey) {
    return fallbackSnapshot;
  }

  const workbook = getSalesWorkbook();
  const tab = workbook?.tabs?.[tabKey];
  if (!tab || typeof tab !== 'object') {
    return fallbackSnapshot;
  }

  const scope = pickSalesScopeForDate(tab, tabKey, baseDate);
  const scopeDate = resolveSalesScopeDate(scope || {}) || baseDate;
  const settings = getEffectiveSalesSettings(tab, scope || {}, workbook?.globals || {});
  const rows = Array.isArray(scope?.rows) ? scope.rows : [];
  const matched = findBestSalesRowForProduct(rows, product);
  const matchedRow = matched.row;
  const costRmb = parseNonNegativeNumber(matchedRow?.costRmb, 0);

  const computed = buildSalesRowComputed(
    {
      costRmb,
      saleKrw: quantity > 0 ? Math.round(totalPrice / quantity) : totalPrice,
      quantity
    },
    settings
  );

  const marginKrw = totalPrice - Math.round(computed.totalCostKrw);
  return {
    sales_tab_key: tabKey,
    sales_scope_id: String(scope?.id || ''),
    sales_scope_name: String(scope?.name || ''),
    sales_scope_date: scopeDate,
    sales_exchange_rate_snapshot: Number(
      parseNonNegativeNumber(settings.exchangeRate, SALES_DEFAULT_EXCHANGE_RATE).toFixed(2)
    ),
    sales_shipping_fee_krw_snapshot: Math.round(
      parseNonNegativeNumber(settings.shippingFeeKrw, SALES_DEFAULT_SHIPPING_FEE_KRW)
    ),
    sales_cost_rmb_snapshot: Number(costRmb.toFixed(4)),
    sales_cost_krw_snapshot: Math.round(computed.totalCostKrw),
    sales_margin_krw_snapshot: Math.round(marginKrw),
    sales_real_margin_krw_snapshot: Math.round(marginKrw),
    sales_synced_at: new Date().toISOString()
  };
}

function buildSalesRowFromSheetRow(row = [], indexes = {}, fallbackFactory = '') {
  const valueAt = (idx) => (idx >= 0 ? String(row[idx] || '').trim() : '');
  const quantityRaw = valueAt(indexes.qty);
  const quantity = Math.max(1, Math.floor(parseNonNegativeNumber(quantityRaw, 1) || 1));

  return createDefaultSalesRow({
    factory: valueAt(indexes.factory) || fallbackFactory,
    brand: valueAt(indexes.brand),
    model: valueAt(indexes.model),
    reference: valueAt(indexes.reference),
    spec: valueAt(indexes.spec),
    costRmb: parseNonNegativeNumber(valueAt(indexes.costRmb), 0),
    saleKrw: parseNonNegativeNumber(valueAt(indexes.saleKrw), 0),
    quantity
  });
}

function extractRoundNameFromTitle(title = '', tabKey = '') {
  const value = String(title || '').trim();
  const roundMatch = value.match(/(\d+\s*차)/);
  if (roundMatch?.[1]) {
    return roundMatch[1].replace(/\s+/g, '');
  }
  const dateMatch = normalizeSalesDate(value);
  if (dateMatch) {
    return dateMatch;
  }
  if (tabKey === 'preorder') {
    return '1차';
  }
  if (getSalesScopeMode(tabKey) === 'date') {
    return getTodayDateString();
  }
  return '기본';
}

function buildSalesWorkbookFromSheetSnapshot(snapshot = {}) {
  const base = buildDefaultSalesWorkbook();
  const salesMainTabs = getSalesMainTabs();
  const tabs = Array.isArray(snapshot.tabs) ? snapshot.tabs : [];
  let detectedExchangeRate = null;
  let detectedShippingFeeKrw = null;

  const nextTabs = { ...base.tabs };

  for (const importedTab of tabs) {
    if (!importedTab || importedTab.status !== 'ok') continue;
    const tabInfo = salesMainTabs.find((item) => item.key === importedTab.key);
    if (!tabInfo) continue;

    const headers = Array.isArray(importedTab.headers) ? importedTab.headers : [];
    const rows = Array.isArray(importedTab.rows) ? importedTab.rows : [];
    const indexes = detectSalesColumnIndexes(headers);

    for (const row of rows) {
      if (!Array.isArray(row)) continue;
      if (detectedExchangeRate === null && indexes.rate >= 0) {
        const value = parseNonNegativeNumber(String(row[indexes.rate] || '').trim(), NaN);
        if (Number.isFinite(value) && value > 0) {
          detectedExchangeRate = value;
        }
      }
      if (detectedShippingFeeKrw === null && indexes.shippingKrw >= 0) {
        const value = parseNonNegativeNumber(String(row[indexes.shippingKrw] || '').trim(), NaN);
        if (Number.isFinite(value) && value > 0) {
          detectedShippingFeeKrw = value;
        }
      }
    }

    if (tabInfo.scopeType === 'factory') {
      const groupMap = new Map();
      rows.forEach((row) => {
        if (!Array.isArray(row)) return;
        const noValue = indexes.no >= 0 ? String(row[indexes.no] || '').trim() : '';
        const brand = indexes.brand >= 0 ? String(row[indexes.brand] || '').trim() : '';
        const model = indexes.model >= 0 ? String(row[indexes.model] || '').trim() : '';
        const cost = indexes.costRmb >= 0 ? parseNonNegativeNumber(String(row[indexes.costRmb] || '').trim(), NaN) : NaN;
        const sale = indexes.saleKrw >= 0 ? parseNonNegativeNumber(String(row[indexes.saleKrw] || '').trim(), NaN) : NaN;
        const isLikelyHeader = [brand, model].some((cell) => normalizeSalesHeaderLabel(cell).includes('브랜드'));
        const hasData = !!brand || !!model || Number.isFinite(cost) || Number.isFinite(sale);
        if (!hasData || isLikelyHeader || /^no\.?$/i.test(noValue)) {
          return;
        }

        const factoryName =
          (indexes.factory >= 0 ? String(row[indexes.factory] || '').trim() : '') ||
          '기본';
        if (!groupMap.has(factoryName)) {
          groupMap.set(factoryName, {
            id: createSalesId('scope-price'),
            name: normalizeSalesText(factoryName, 80) || '기본',
            categoryType: SALES_PRICE_SHEET_CATEGORY_TYPES.FACTORY,
            rows: []
          });
        }

        const mappedRow = buildSalesRowFromSheetRow(row, indexes, factoryName);
        mappedRow.factory = mappedRow.factory || factoryName;
        groupMap.get(factoryName).rows.push(mappedRow);
      });

      const groups = [...groupMap.values()];
      nextTabs[tabInfo.key] = {
        key: tabInfo.key,
        labelKo: tabInfo.labelKo,
        labelEn: tabInfo.labelEn,
        scopeType: tabInfo.scopeType,
        groups: groups.length > 0 ? groups : base.tabs[tabInfo.key].groups
      };
      continue;
    }

    const rounds = [];
    let currentRound = {
      id: createSalesId(`round-${tabInfo.key}`),
      name: extractRoundNameFromTitle(headers[1] || headers[0] || '', tabInfo.key),
      rows: []
    };

    rows.forEach((row) => {
      if (!Array.isArray(row)) return;
      const noValue = indexes.no >= 0 ? String(row[indexes.no] || '').trim() : '';
      const factory = indexes.factory >= 0 ? String(row[indexes.factory] || '').trim() : '';
      const brand = indexes.brand >= 0 ? String(row[indexes.brand] || '').trim() : '';
      const model = indexes.model >= 0 ? String(row[indexes.model] || '').trim() : '';
      const cost = indexes.costRmb >= 0 ? parseNonNegativeNumber(String(row[indexes.costRmb] || '').trim(), NaN) : NaN;
      const sale = indexes.saleKrw >= 0 ? parseNonNegativeNumber(String(row[indexes.saleKrw] || '').trim(), NaN) : NaN;
      const hasData = !!factory || !!brand || !!model || Number.isFinite(cost) || Number.isFinite(sale);
      const noDateLike = /^\d{4}[-./]\d{1,2}[-./]\d{1,2}$/.test(noValue);

      if (noDateLike && !hasData) {
        if (currentRound.rows.length > 0) {
          rounds.push(currentRound);
        }
        currentRound = {
          id: createSalesId(`round-${tabInfo.key}`),
          name: noValue.replace(/[./]/g, '-'),
          rows: []
        };
        return;
      }

      if (!hasData || /^no\.?$/i.test(noValue) || normalizeSalesHeaderLabel(brand).includes('브랜드')) {
        return;
      }
      currentRound.rows.push(buildSalesRowFromSheetRow(row, indexes));
    });

    if (currentRound.rows.length > 0 || rounds.length === 0) {
      rounds.push(currentRound);
    }

    nextTabs[tabInfo.key] = {
      key: tabInfo.key,
      labelKo: tabInfo.labelKo,
      labelEn: tabInfo.labelEn,
      scopeType: tabInfo.scopeType,
      rounds
    };
  }

  const importedWorkbook = {
    version: 2,
    globals: {
      exchangeRate:
        Number.isFinite(detectedExchangeRate) && detectedExchangeRate > 0
          ? Number(detectedExchangeRate.toFixed(2))
          : SALES_DEFAULT_EXCHANGE_RATE,
      shippingFeeKrw:
        Number.isFinite(detectedShippingFeeKrw) && detectedShippingFeeKrw > 0
          ? Math.round(detectedShippingFeeKrw)
          : SALES_DEFAULT_SHIPPING_FEE_KRW,
      fxSource: 'sheet',
      fxUpdatedAt: '',
      updatedAt: new Date().toISOString()
    },
    tabs: nextTabs,
    meta: {
      importedFrom: normalizeSalesText(snapshot.sourceUrl || SALES_SHEET_DEFAULT_URL, 400),
      importedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }
  };

  return normalizeSalesWorkbook(importedWorkbook);
}

async function fetchSalesImportTab(sheetId, tabConfig) {
  const url = new URL(`https://docs.google.com/spreadsheets/d/${encodeURIComponent(sheetId)}/gviz/tq`);
  url.searchParams.set('tqx', 'out:json');
  url.searchParams.set('gid', String(tabConfig.gid));

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      accept: 'text/plain'
    }
  });

  if (!response.ok) {
    throw new Error(`sheet fetch failed(${response.status})`);
  }

  const raw = await response.text();
  const parsed = parseGoogleVizResponseJson(raw);
  if (!parsed || parsed.status !== 'ok') {
    throw new Error('invalid gviz response');
  }

  const table = normalizeGoogleSheetTable(parsed.table || {});
  return {
    key: tabConfig.key,
    gid: tabConfig.gid,
    labelKo: tabConfig.labelKo,
    labelEn: tabConfig.labelEn,
    headers: table.headers,
    rows: table.rows,
    status: 'ok'
  };
}

async function importSalesWorkbookFromGoogleSheet() {
  const sourceUrl = SALES_SHEET_DEFAULT_URL;
  const sheetId = extractGoogleSheetId(SALES_SHEET_DEFAULT_URL);
  if (!sheetId) {
    throw new Error('invalid sales sheet id');
  }
  const previousWorkbook = getSalesWorkbook();

  const tabs = await Promise.all(
    SALES_IMPORT_TABS.map(async (tab) => {
      try {
        return await fetchSalesImportTab(sheetId, tab);
      } catch (error) {
        return {
          key: tab.key,
          gid: tab.gid,
          labelKo: tab.labelKo,
          labelEn: tab.labelEn,
          headers: [],
          rows: [],
          status: 'error',
          errorMessage: error instanceof Error ? error.message : 'failed to import'
        };
      }
    })
  );
  const importedTabKeySet = new Set(
    tabs
      .filter((tab) => tab && tab.status === 'ok')
      .map((tab) => String(tab.key || '').trim())
      .filter(Boolean)
  );

  const workbook = buildSalesWorkbookFromSheetSnapshot({
    sourceUrl,
    sheetId,
    tabs
  });
  const mergedTabs = workbook.tabs && typeof workbook.tabs === 'object' ? { ...workbook.tabs } : {};
  const previousTabs = previousWorkbook?.tabs && typeof previousWorkbook.tabs === 'object'
    ? previousWorkbook.tabs
    : {};
  getSalesMainTabs().forEach((tab) => {
    const tabKey = String(tab?.key || '').trim();
    if (!tabKey || importedTabKeySet.has(tabKey)) {
      return;
    }
    const previousTab = previousTabs[tabKey];
    if (previousTab && typeof previousTab === 'object') {
      mergedTabs[tabKey] = previousTab;
    }
  });
  workbook.tabs = mergedTabs;

  return saveSalesWorkbook(workbook, {
    importedFrom: sourceUrl,
    importedAt: new Date().toISOString()
  });
}

async function syncSalesWorkbookFromLegacySheetOnce() {
  const alreadySyncedAt = String(getSetting(SALES_LEGACY_SHEET_SYNC_DONE_KEY, '') || '').trim();
  if (alreadySyncedAt) {
    return;
  }

  try {
    await importSalesWorkbookFromGoogleSheet();
    const syncedAt = new Date().toISOString();
    setSetting(SALES_LEGACY_SHEET_SYNC_DONE_KEY, syncedAt);
    // eslint-disable-next-line no-console
    console.log('[sales] legacy google sheet data synced once and disabled.');
  } catch (error) {
    logDetailedError(
      'sales-legacy-sheet-sync-failed',
      error instanceof Error ? error : new Error(String(error)),
      {}
    );
  }
}

async function fetchCnyKrwExchangeRate() {
  if (typeof fetch !== 'function') {
    throw new Error('fetch unavailable');
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const response = await fetch(SALES_CNY_NAVER_SEARCH_URL, {
      method: 'GET',
      headers: {
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'user-agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
      },
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`naver fx fetch failed(${response.status})`);
    }

    const html = await response.text();
    const cashBuyMatch =
      html.match(
        /<strong class="item_title">시세정보<\/strong>[\s\S]*?현찰\s*살때<\/dt>\s*<dd>\s*<span class="text">([^<]+)<\/span>/i
      ) ||
      html.match(/현찰\s*살때<\/dt>\s*<dd>\s*<span class="text">([^<]+)<\/span>/i);
    const krwRate = parseNonNegativeNumber(cashBuyMatch?.[1] || '', NaN);
    if (!Number.isFinite(krwRate) || krwRate <= 0) {
      throw new Error('invalid naver hana cash-buy rate');
    }

    const updatedAt = new Date().toISOString();

    return {
      exchangeRate: Number(krwRate.toFixed(4)),
      updatedAt,
      provider: 'naver-hanabank-cash-buy'
    };
  } finally {
    clearTimeout(timeout);
  }
}

function isDeliveredState(payload) {
  const stateId = String(payload?.state?.id || '').toLowerCase();
  const stateText = String(payload?.state?.text || payload?.state?.name || '').toLowerCase();
  if (stateId.includes('delivered') || stateText.includes('delivered') || stateText.includes('배송완료')) {
    return true;
  }

  const latestProgress = Array.isArray(payload?.progresses) ? payload.progresses[0] : null;
  const progressText = String(
    latestProgress?.status?.text || latestProgress?.description || latestProgress?.status?.id || ''
  ).toLowerCase();

  return progressText.includes('delivered') || progressText.includes('배송완료');
}

function formatTrackingEventDateTime(rawValue) {
  const value = String(rawValue || '').trim();
  if (!value) {
    return '';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '';
  }
  const formatted = parsed.toLocaleString('sv-SE', {
    timeZone: 'Asia/Seoul',
    hour12: false
  });
  return formatted.slice(0, 16).replace('T', ' ');
}

function splitTrackingEventDateTime(rawValue) {
  const dateTime = formatTrackingEventDateTime(rawValue);
  if (!dateTime) {
    return { dateText: '-', timeText: '-' };
  }
  const [dateText = '-', timeText = '-'] = dateTime.split(' ');
  return {
    dateText: String(dateText || '-').trim() || '-',
    timeText: String(timeText || '-').trim() || '-'
  };
}

function getLatestTrackingProgress(payload) {
  const progresses = Array.isArray(payload?.progresses) ? payload.progresses : [];
  if (progresses.length === 0) {
    return null;
  }

  let latest = null;
  let latestMs = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < progresses.length; i += 1) {
    const item = progresses[i];
    const timeMs = Date.parse(String(item?.time || '').trim());
    if (Number.isFinite(timeMs)) {
      if (!Number.isFinite(latestMs) || timeMs > latestMs) {
        latest = item;
        latestMs = timeMs;
      }
      continue;
    }
    if (!latest) {
      latest = item;
    }
  }
  return latest || progresses[progresses.length - 1] || null;
}

function pickFirstTrackingText(values = []) {
  for (let i = 0; i < values.length; i += 1) {
    const value = String(values[i] || '').trim();
    if (value) {
      return value;
    }
  }
  return '';
}

function buildTrackingLatestEventSummary(payload) {
  if (!payload || typeof payload !== 'object') {
    return '';
  }
  const latestProgress = getLatestTrackingProgress(payload);
  const statusText =
    pickFirstTrackingText([
      latestProgress?.status?.text,
      latestProgress?.status?.name,
      latestProgress?.status?.id,
      latestProgress?.state?.text,
      latestProgress?.description,
      payload?.state?.text,
      payload?.state?.name,
      payload?.state?.id
    ]) || '조회대기중';
  const locationText = pickFirstTrackingText([
    latestProgress?.location?.name,
    latestProgress?.location?.address,
    latestProgress?.location?.text,
    latestProgress?.locationName,
    latestProgress?.location,
    payload?.from?.name
  ]);
  const { dateText, timeText } = splitTrackingEventDateTime(
    pickFirstTrackingText([latestProgress?.time, latestProgress?.dateTime, latestProgress?.datetime, payload?.state?.time])
  );

  const chunks = [];
  chunks.push(`상태: ${statusText}`);
  if (locationText) {
    chunks.push(`위치: ${locationText}`);
  }
  chunks.push(`날짜: ${dateText}`);
  chunks.push(`시간: ${timeText}`);
  return chunks.join(' · ').slice(0, 260);
}

async function fetchTrackingPayload(carrierId, trackingNumber) {
  if (typeof fetch !== 'function') {
    return null;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TRACKING_REQUEST_TIMEOUT_MS);
  const url = `${TRACKING_API_BASE}/carriers/${encodeURIComponent(carrierId)}/tracks/${encodeURIComponent(
    trackingNumber
  )}`;

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: { accept: 'application/json' },
      signal: controller.signal
    });

    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function pollTrackingAndAutoCompleteOrders(force = false) {
  if (trackingPollInFlight) {
    return;
  }

  const nowMs = Date.now();
  if (!force && nowMs - lastTrackingPollMs < TRACKING_AUTO_POLL_MS) {
    return;
  }
  lastTrackingPollMs = nowMs;
  trackingPollInFlight = true;

  try {
    const targets = db
      .prepare(
        `
          SELECT id, order_no, status, tracking_carrier, tracking_number
          FROM orders
          WHERE status = ? AND tracking_number != ''
          ORDER BY id ASC
          LIMIT 100
        `
      )
      .all(ORDER_STATUS.SHIPPING);

    for (const item of targets) {
      const carrierId = normalizeTrackingCarrier(item.tracking_carrier);
      const trackingNumber = normalizeTrackingNumber(item.tracking_number);
      if (!carrierId || !trackingNumber) {
        continue;
      }

      const payload = await fetchTrackingPayload(carrierId, trackingNumber);
      const latestEvent = buildTrackingLatestEventSummary(payload);

      db.prepare(
        `
          UPDATE orders
          SET
            tracking_last_event = ?,
            tracking_last_checked_at = datetime('now', '+9 hours')
          WHERE id = ?
        `
      ).run(latestEvent, item.id);

      if (payload && isDeliveredState(payload)) {
        const updated = db.prepare(
          `
            UPDATE orders
            SET
              status = ?,
              delivered_at = COALESCE(delivered_at, datetime('now')),
              tracking_last_event = ?,
              tracking_last_checked_at = datetime('now', '+9 hours')
            WHERE id = ? AND status = ?
          `
        ).run(ORDER_STATUS.DELIVERED, latestEvent || 'Delivered', item.id, ORDER_STATUS.SHIPPING);

        if (updated.changes > 0) {
          appendOrderStatusLog(
            item.id,
            item.order_no,
            ORDER_STATUS.SHIPPING,
            ORDER_STATUS.DELIVERED,
            `tracking:auto:${latestEvent || 'delivered'}`
          );
          awardDeliveredOrderPoints(item.id);
        }
      }
    }
  } finally {
    trackingPollInFlight = false;
  }
}

function generateOrderNo() {
  const datePart = toKstDate().replaceAll('-', '');
  const random = Math.random().toString(36).slice(2, 8).toUpperCase();
  return `CL-${datePart}-${random}`;
}

function parseMemberUidSequence(rawUid = '') {
  const matched = String(rawUid || '').trim().match(/^U(\d{1,12})$/i);
  if (!matched) {
    return 0;
  }
  const parsed = Number.parseInt(matched[1], 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return 0;
  }
  return parsed;
}

function formatMemberUid(sequence = 1) {
  const safe = Number.isInteger(sequence) && sequence > 0 ? sequence : 1;
  return `U${String(safe).padStart(5, '0')}`;
}

function generateNextMemberUid() {
  const latest = db
    .prepare(
      `
        SELECT member_uid
        FROM users
        WHERE is_admin = 0
          AND COALESCE(TRIM(member_uid), '') != ''
        ORDER BY CAST(SUBSTR(member_uid, 2) AS INTEGER) DESC
        LIMIT 1
      `
    )
    .get();
  const latestSequence = parseMemberUidSequence(latest?.member_uid || '');
  return formatMemberUid(latestSequence + 1);
}

function issueSignupCaptcha(req) {
  const left = Math.floor(Math.random() * 9) + 1;
  const right = Math.floor(Math.random() * 9) + 1;
  const answer = String(left + right);
  const issuedAt = Date.now();

  if (req.session) {
    req.session.signupCaptcha = { answer, issuedAt };
  }

  return {
    promptKo: `${left} + ${right} = ?`,
    promptEn: `${left} + ${right} = ?`
  };
}

function readSignupCaptcha(req) {
  if (!req.session || !req.session.signupCaptcha || typeof req.session.signupCaptcha !== 'object') {
    return null;
  }

  const answer = String(req.session.signupCaptcha.answer || '').trim();
  const issuedAt = Number(req.session.signupCaptcha.issuedAt || 0);
  if (!answer || !Number.isFinite(issuedAt) || issuedAt <= 0) {
    return null;
  }

  return { answer, issuedAt };
}

function clearSignupCaptcha(req) {
  if (req.session && req.session.signupCaptcha) {
    delete req.session.signupCaptcha;
  }
}

function getMemberCartCount(userId) {
  const targetUserId = Number(userId || 0);
  if (!Number.isInteger(targetUserId) || targetUserId <= 0) {
    return 0;
  }

  const hasCartTable = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'cart_items' LIMIT 1")
    .get();

  if (!hasCartTable) {
    return 0;
  }

  const row = db.prepare('SELECT COUNT(*) AS count FROM cart_items WHERE user_id = ?').get(targetUserId);
  return Number(row?.count || 0);
}

function getMemberCartSummary(userId, lang = 'ko') {
  const targetUserId = Number(userId || 0);
  if (!Number.isInteger(targetUserId) || targetUserId <= 0) {
    return { items: [], itemCount: 0, quantityTotal: 0, priceTotal: 0 };
  }

  const hasCartTable = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'cart_items' LIMIT 1")
    .get();
  if (!hasCartTable) {
    return { items: [], itemCount: 0, quantityTotal: 0, priceTotal: 0 };
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, lang);

  const rows = db
    .prepare(
      `
        SELECT
          c.id AS cart_item_id,
          c.product_id,
          c.quantity,
          c.created_at,
          c.updated_at,
          p.category_group,
          p.brand,
          p.model,
          p.sub_model,
          p.price,
          p.image_path,
          p.shipping_period,
          p.is_active,
          p.extra_fields_json
        FROM cart_items c
        JOIN products p ON p.id = c.product_id
        WHERE c.user_id = ?
        ORDER BY c.id DESC
      `
    )
    .all(targetUserId);

  const items = rows.map((row) => {
    const decorated = decorateProductForView(row, productGroupMap.get(row.category_group));
    const quantity = parsePositiveInt(row.quantity, 1);
    const lineTotal = Number(row.price || 0) * quantity;
    return {
      ...decorated,
      cart_item_id: Number(row.cart_item_id),
      quantity,
      lineTotal,
      is_active: Number(row.is_active || 0) === 1,
      category_group_label: groupLabelMap[row.category_group] || row.category_group
    };
  });

  const quantityTotal = items.reduce((sum, item) => sum + Number(item.quantity || 0), 0);
  const priceTotal = items.reduce((sum, item) => sum + Number(item.lineTotal || 0), 0);
  return {
    items,
    itemCount: items.length,
    quantityTotal,
    priceTotal
  };
}

function maskUsername(username = '') {
  if (username.length <= 2) {
    return `${username.slice(0, 1)}*`;
  }
  return `${username.slice(0, 2)}${'*'.repeat(username.length - 2)}`;
}

function sanitizePath(pathValue = '') {
  const rawPath = String(pathValue || '').trim();
  if (!rawPath) {
    return '/main';
  }

  // Block absolute/protocol-relative URLs (e.g. https://..., //example.com)
  if (/^(?:[a-z][a-z0-9+.-]*:)?\/\//i.test(rawPath)) {
    return '/main';
  }

  const sanitized = rawPath.replace(/[\u0000-\u001f\u007f]/g, '');
  if (!sanitized) {
    return '/main';
  }

  return sanitized.startsWith('/') ? sanitized : `/${sanitized}`;
}

function normalizeSupportChatMessage(rawValue = '') {
  const message = String(rawValue || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim();
  return message.slice(0, SUPPORT_CHAT_MAX_MESSAGE_LENGTH);
}

function resolveSupportAssignedAdminUserId() {
  const preferred = db
    .prepare(
      `
        SELECT id
        FROM users
        WHERE is_admin = 1
          AND lower(username) = lower(?)
        LIMIT 1
      `
    )
    .get(SUPPORT_CHAT_PRIMARY_ADMIN_USERNAME);
  if (preferred && Number(preferred.id) > 0) {
    return Number(preferred.id);
  }

  const primary = db
    .prepare(
      `
        SELECT id
        FROM users
        WHERE is_admin = 1
          AND admin_role = 'PRIMARY'
        ORDER BY id ASC
        LIMIT 1
      `
    )
    .get();
  if (primary && Number(primary.id) > 0) {
    return Number(primary.id);
  }

  const fallback = db
    .prepare(
      `
        SELECT id
        FROM users
        WHERE is_admin = 1
        ORDER BY id ASC
        LIMIT 1
      `
    )
    .get();
  return fallback && Number(fallback.id) > 0 ? Number(fallback.id) : 0;
}

function ensureSupportChatThreadForMember(memberUserId) {
  const safeMemberUserId = Number.parseInt(String(memberUserId || ''), 10);
  if (!Number.isInteger(safeMemberUserId) || safeMemberUserId <= 0) {
    return null;
  }

  let thread = db
    .prepare(
      `
        SELECT *
        FROM support_chat_threads
        WHERE member_user_id = ?
        LIMIT 1
      `
    )
    .get(safeMemberUserId);

  if (!thread) {
    const assignedAdminUserId = resolveSupportAssignedAdminUserId();
    db.prepare(
      `
        INSERT INTO support_chat_threads (
          member_user_id,
          assigned_admin_user_id,
          created_at,
          updated_at
        )
        VALUES (?, ?, datetime('now'), datetime('now'))
      `
    ).run(safeMemberUserId, assignedAdminUserId > 0 ? assignedAdminUserId : null);

    thread = db
      .prepare(
        `
          SELECT *
          FROM support_chat_threads
          WHERE member_user_id = ?
          LIMIT 1
        `
      )
      .get(safeMemberUserId);
  }

  if (
    thread &&
    (!Number.isInteger(Number(thread.assigned_admin_user_id)) || Number(thread.assigned_admin_user_id) <= 0)
  ) {
    const assignedAdminUserId = resolveSupportAssignedAdminUserId();
    if (assignedAdminUserId > 0) {
      db.prepare(
        `
          UPDATE support_chat_threads
          SET assigned_admin_user_id = ?, updated_at = datetime('now')
          WHERE id = ?
        `
      ).run(assignedAdminUserId, thread.id);
      thread = { ...thread, assigned_admin_user_id: assignedAdminUserId };
    }
  }

  return thread || null;
}

function mapSupportChatMessageRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    id: Number(row.id || 0),
    threadId: Number(row.thread_id || 0),
    senderUserId: Number(row.sender_user_id || 0),
    senderRole: String(row.sender_role || '').trim(),
    messageText: String(row.message_text || ''),
    createdAt: String(row.created_at || ''),
    memberReadAt: String(row.member_read_at || ''),
    adminReadAt: String(row.admin_read_at || '')
  }));
}

function getSupportChatMessagesByThreadId(threadId, limit = 120) {
  const safeThreadId = Number.parseInt(String(threadId || ''), 10);
  const safeLimit = Math.max(1, Math.min(400, Number.parseInt(String(limit || ''), 10) || 120));
  if (!Number.isInteger(safeThreadId) || safeThreadId <= 0) {
    return [];
  }

  const rows = db
    .prepare(
      `
        SELECT id, thread_id, sender_user_id, sender_role, message_text, member_read_at, admin_read_at, created_at
        FROM support_chat_messages
        WHERE thread_id = ?
        ORDER BY id ASC
        LIMIT ?
      `
    )
    .all(safeThreadId, safeLimit);

  return mapSupportChatMessageRows(rows);
}

function canAdminAccessSupportThread(adminUser = null, thread = null) {
  return Boolean(adminUser && adminUser.isAdmin && thread);
}

function getAdminSupportChatUnreadCount(adminUser = null) {
  if (!adminUser || !adminUser.isAdmin) {
    return 0;
  }

  const row = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM support_chat_messages m
        JOIN support_chat_threads t ON t.id = m.thread_id
        WHERE m.sender_role = 'member'
          AND m.admin_read_at IS NULL
      `
    )
    .get();
  return Number(row?.count || 0);
}

function getMemberSupportChatUnreadCount(memberUserId = 0) {
  const safeMemberUserId = Number.parseInt(String(memberUserId || ''), 10);
  if (!Number.isInteger(safeMemberUserId) || safeMemberUserId <= 0) {
    return 0;
  }
  const row = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM support_chat_messages m
        JOIN support_chat_threads t ON t.id = m.thread_id
        WHERE t.member_user_id = ?
          AND m.sender_role = 'admin'
          AND m.member_read_at IS NULL
      `
    )
    .get(safeMemberUserId);
  return Number(row?.count || 0);
}

function normalizeKakaoChannelPath(pathname = '') {
  const rawPath = String(pathname || '').trim();
  if (!rawPath) {
    return '';
  }
  const normalized = rawPath.replace(/\/+/g, '/');
  const segments = normalized.split('/').filter(Boolean);
  if (segments.length === 0) {
    return '';
  }
  const channelToken = String(segments[0] || '').trim();
  if (!/^_[a-z0-9._-]{2,80}$/i.test(channelToken)) {
    return '';
  }
  return `/${channelToken}/chat`;
}

function resolveKakaoChatUrl(contactInfo = '', explicitValue = '') {
  const parseFromUrlText = (textValue = '') => {
    const match = String(textValue || '').match(/https?:\/\/pf\.kakao\.com\/[^\s"'<>]+/iu);
    if (!match) {
      return '';
    }
    try {
      const parsed = new URL(match[0]);
      if (String(parsed.hostname || '').toLowerCase() !== 'pf.kakao.com') {
        return '';
      }
      const pathValue = normalizeKakaoChannelPath(parsed.pathname || '');
      if (!pathValue) {
        return '';
      }
      return `https://pf.kakao.com${pathValue}`;
    } catch (error) {
      return '';
    }
  };

  const parseFromIdText = (textValue = '') => {
    const text = String(textValue || '');
    const atMatch = text.match(/@([a-z0-9._-]{2,80})/iu);
    if (atMatch && atMatch[1]) {
      return `https://pf.kakao.com/_${atMatch[1]}/chat`;
    }

    const namedMatch = text.match(/(?:카카오톡|kakaotalk|kakao)\s*[:：]\s*([a-z0-9._-]{2,80})/iu);
    if (namedMatch && namedMatch[1]) {
      const token = String(namedMatch[1] || '').trim();
      if (token.startsWith('_')) {
        return `https://pf.kakao.com/${token}/chat`;
      }
      return `https://pf.kakao.com/_${token}/chat`;
    }

    const underscoreTokenMatch = text.match(/(?:^|[\s,|/])(_[a-z0-9._-]{2,80})(?:$|[\s,|/])/iu);
    if (underscoreTokenMatch && underscoreTokenMatch[1]) {
      return `https://pf.kakao.com/${underscoreTokenMatch[1]}/chat`;
    }
    return '';
  };

  const explicitText = String(explicitValue || '').trim();
  const contactText = String(contactInfo || '').trim();
  const fromExplicitUrl = parseFromUrlText(explicitText);
  if (fromExplicitUrl) {
    return fromExplicitUrl;
  }
  const fromContactUrl = parseFromUrlText(contactText);
  if (fromContactUrl) {
    return fromContactUrl;
  }
  return parseFromIdText(`${explicitText} ${contactText}`.trim());
}

function normalizeHeroLeftBackgroundType(rawType = 'color') {
  return String(rawType || '').trim().toLowerCase() === 'image' ? 'image' : 'color';
}

function normalizeHeroQuickMenuPath(rawPath = '') {
  const candidate = String(rawPath || '').trim();
  if (!candidate) {
    return '';
  }
  const pathValue = sanitizePath(candidate);
  if (!pathValue || pathValue.startsWith('/admin')) {
    return '';
  }
  return pathValue;
}

function normalizeHeroQuickMenuPathList(rawValues = []) {
  const sourceValues = Array.isArray(rawValues) ? rawValues : [];
  const normalized = [];
  const seen = new Set();

  const pushPath = (rawPath) => {
    const pathValue = normalizeHeroQuickMenuPath(rawPath);
    if (!pathValue) {
      return;
    }
    const dedupeKey = pathValue.toLowerCase();
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);
    normalized.push(pathValue);
  };

  sourceValues.forEach((rawPath) => pushPath(rawPath));

  return normalized.slice(0, HERO_QUICK_MENU_LIMIT);
}

function getDefaultHeroQuickMenuPaths() {
  return HERO_DEFAULT_QUICK_MENUS.map((item) => normalizeHeroQuickMenuPath(item.path))
    .filter(Boolean)
    .slice(0, HERO_QUICK_MENU_LIMIT);
}

function hasSiteSettingKey(settingKey = '') {
  const key = String(settingKey || '').trim();
  if (!key) {
    return false;
  }
  const row = db
    .prepare(
      `
        SELECT 1
        FROM site_settings
        WHERE setting_key = ?
        LIMIT 1
      `
    )
    .get(key);
  return Boolean(row);
}

function getHeroQuickMenuPathsSetting() {
  if (!hasSiteSettingKey('heroQuickMenuPaths')) {
    return getDefaultHeroQuickMenuPaths();
  }

  const rawValue = String(getSetting('heroQuickMenuPaths', '') || '').trim();
  let sourceValues = [];
  if (rawValue) {
    try {
      const parsed = JSON.parse(rawValue);
      if (Array.isArray(parsed)) {
        sourceValues = parsed;
      } else {
        sourceValues = rawValue.split(',');
      }
    } catch {
      sourceValues = rawValue.split(',');
    }
  }
  return normalizeHeroQuickMenuPathList(sourceValues);
}

function getHeroQuickMenuOptions() {
  const publicMenus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), {
    includeHidden: true
  });
  const menuMap = new Map();

  const pushOption = (item = {}) => {
    const pathValue = normalizeHeroQuickMenuPath(item.path);
    if (!pathValue) {
      return;
    }
    const dedupeKey = pathValue.toLowerCase();
    if (menuMap.has(dedupeKey)) {
      return;
    }
    const labelKo = String(item.labelKo || item.labelEn || pathValue).trim() || pathValue;
    const labelEn = String(item.labelEn || item.labelKo || pathValue).trim() || labelKo;
    menuMap.set(dedupeKey, {
      path: pathValue,
      labelKo,
      labelEn,
      isHidden: Boolean(item.isHidden)
    });
  };

  publicMenus.forEach((menuItem) => pushOption(menuItem));
  HERO_DEFAULT_QUICK_MENUS.forEach((menuItem) => pushOption(menuItem));

  return [...menuMap.values()];
}

function buildHeroLeftPaneStyle(backgroundType = 'color', backgroundColor = '#eef2f8', backgroundImagePath = '') {
  const nextType = normalizeHeroLeftBackgroundType(backgroundType);
  const safeColor = normalizeHexColor(backgroundColor, HERO_DEFAULT_LEFT_BACKGROUND_COLOR);
  const styleParts = [`background-color: ${safeColor};`];

  if (nextType === 'image' && String(backgroundImagePath || '').trim()) {
    const safePath = String(backgroundImagePath || '').trim().replace(/'/g, '%27');
    styleParts.push(`background-image: url('${safePath}');`);
    styleParts.push('background-size: cover;');
    styleParts.push('background-position: center;');
    styleParts.push('background-repeat: no-repeat;');
  }

  return styleParts.join(' ');
}

function getMainHeroSettings(lang = 'ko') {
  const isEn = lang === 'en';
  const leftTitleKo =
    String(getSetting('heroLeftTitleKo', HERO_DEFAULT_LEFT_TITLE_KO) || '').trim() ||
    HERO_DEFAULT_LEFT_TITLE_KO;
  const leftTitleEn =
    String(getSetting('heroLeftTitleEn', HERO_DEFAULT_LEFT_TITLE_EN) || '').trim() ||
    HERO_DEFAULT_LEFT_TITLE_EN;
  const leftSubtitleKo =
    String(getSetting('heroLeftSubtitleKo', HERO_DEFAULT_LEFT_SUBTITLE_KO) || '').trim() ||
    HERO_DEFAULT_LEFT_SUBTITLE_KO;
  const leftSubtitleEn =
    String(getSetting('heroLeftSubtitleEn', HERO_DEFAULT_LEFT_SUBTITLE_EN) || '').trim() ||
    HERO_DEFAULT_LEFT_SUBTITLE_EN;
  const rightTitleKo =
    String(getSetting('heroRightTitleKo', HERO_DEFAULT_RIGHT_TITLE_KO) || '').trim() ||
    HERO_DEFAULT_RIGHT_TITLE_KO;
  const rightTitleEn =
    String(getSetting('heroRightTitleEn', HERO_DEFAULT_RIGHT_TITLE_EN) || '').trim() ||
    HERO_DEFAULT_RIGHT_TITLE_EN;
  const rightSubtitleKo =
    String(getSetting('heroRightSubtitleKo', HERO_DEFAULT_RIGHT_SUBTITLE_KO) || '').trim() ||
    HERO_DEFAULT_RIGHT_SUBTITLE_KO;
  const rightSubtitleEn =
    String(getSetting('heroRightSubtitleEn', HERO_DEFAULT_RIGHT_SUBTITLE_EN) || '').trim() ||
    HERO_DEFAULT_RIGHT_SUBTITLE_EN;

  const leftBackgroundType = normalizeHeroLeftBackgroundType(
    getSetting('heroLeftBackgroundType', 'color')
  );
  const leftBackgroundColor = normalizeHexColor(
    getSetting('heroLeftBackgroundColor', HERO_DEFAULT_LEFT_BACKGROUND_COLOR),
    HERO_DEFAULT_LEFT_BACKGROUND_COLOR
  );
  const leftBackgroundImagePath = String(getSetting('heroLeftBackgroundImagePath', '') || '').trim();
  const leftPaneStyle = buildHeroLeftPaneStyle(
    leftBackgroundType,
    leftBackgroundColor,
    leftBackgroundImagePath
  );
  const rightBackgroundColor = normalizeHexColor(
    getSetting('heroRightBackgroundColor', HERO_DEFAULT_RIGHT_BACKGROUND_COLOR),
    HERO_DEFAULT_RIGHT_BACKGROUND_COLOR
  );
  const rightPaneStyle = `background: ${rightBackgroundColor};`;

  const quickMenuOptions = getHeroQuickMenuOptions();
  const quickMenuOptionMap = new Map(
    quickMenuOptions.map((item) => [String(item.path || '').toLowerCase(), item])
  );
  const quickMenuFallbackMap = new Map(
    HERO_DEFAULT_QUICK_MENUS.map((item) => [String(item.path || '').toLowerCase(), item])
  );
  const leftCtaPath = normalizeHeroQuickMenuPath(
    getSetting('heroLeftCtaPath', HERO_DEFAULT_LEFT_CTA_PATH)
  ) || HERO_DEFAULT_LEFT_CTA_PATH;
  const leftCtaMatchedOption =
    quickMenuOptionMap.get(String(leftCtaPath || '').toLowerCase()) ||
    quickMenuFallbackMap.get(String(leftCtaPath || '').toLowerCase()) ||
    null;
  const leftCtaLabelKo = String(
    leftCtaMatchedOption?.labelKo || leftCtaMatchedOption?.labelEn || leftCtaPath
  ).trim();
  const leftCtaLabelEn = String(
    leftCtaMatchedOption?.labelEn || leftCtaMatchedOption?.labelKo || leftCtaPath
  ).trim();
  const quickMenuPaths = getHeroQuickMenuPathsSetting();
  const quickMenus = quickMenuPaths.map((pathValue) => {
    const matchedOption =
      quickMenuOptionMap.get(String(pathValue || '').toLowerCase()) ||
      quickMenuFallbackMap.get(String(pathValue || '').toLowerCase()) ||
      null;
    const labelKo = String(matchedOption?.labelKo || matchedOption?.labelEn || pathValue).trim() || pathValue;
    const labelEn = String(matchedOption?.labelEn || matchedOption?.labelKo || pathValue).trim() || labelKo;
    return {
      path: pathValue,
      labelKo,
      labelEn,
      label: isEn ? (labelEn || labelKo || pathValue) : (labelKo || labelEn || pathValue)
    };
  });

  return {
    leftTitleKo,
    leftTitleEn,
    leftTitle: isEn ? (leftTitleEn || leftTitleKo) : (leftTitleKo || leftTitleEn),
    leftSubtitleKo,
    leftSubtitleEn,
    leftSubtitle: isEn ? (leftSubtitleEn || leftSubtitleKo) : (leftSubtitleKo || leftSubtitleEn),
    leftCtaPath,
    leftCtaLabelKo,
    leftCtaLabelEn,
    leftCtaLabel: isEn
      ? `Go to ${leftCtaLabelEn || leftCtaLabelKo || 'Menu'}`
      : `${leftCtaLabelKo || leftCtaLabelEn || '메뉴'} 바로가기`,
    leftBackgroundType,
    leftBackgroundColor,
    leftBackgroundImagePath: leftBackgroundType === 'image' ? leftBackgroundImagePath : '',
    leftPaneStyle,
    rightTitleKo,
    rightTitleEn,
    rightTitle: isEn ? (rightTitleEn || rightTitleKo) : (rightTitleKo || rightTitleEn),
    rightSubtitleKo,
    rightSubtitleEn,
    rightSubtitle: isEn ? (rightSubtitleEn || rightSubtitleKo) : (rightSubtitleKo || rightSubtitleEn),
    rightBackgroundColor,
    rightPaneStyle,
    quickMenuPaths,
    quickMenuOptions,
    quickMenus
  };
}

function parsePositiveInt(rawValue, fallback = 1) {
  const parsed = Number.parseInt(String(rawValue ?? ''), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseNonNegativeInt(rawValue, fallback = 0) {
  const parsed = Number.parseInt(String(rawValue ?? ''), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function normalizeProductBadgeLabel(rawValue = '', fallback = '') {
  const normalized = String(rawValue || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, PRODUCT_BADGE_LABEL_MAX_LENGTH);
  if (normalized) {
    return normalized;
  }
  return String(fallback || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, PRODUCT_BADGE_LABEL_MAX_LENGTH);
}

function normalizeProductBadgeCode(rawValue = '', fallback = 'badge') {
  const normalize = (value) =>
    String(value || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, '-')
      .replace(/-{2,}/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, PRODUCT_BADGE_CODE_MAX_LENGTH);

  const fromRaw = normalize(rawValue);
  if (PRODUCT_BADGE_CODE_REGEX.test(fromRaw)) {
    return fromRaw;
  }

  const fromFallback = normalize(fallback);
  if (PRODUCT_BADGE_CODE_REGEX.test(fromFallback)) {
    return fromFallback;
  }

  return 'badge';
}

function normalizeProductBadgeColorTheme(rawValue = '', fallback = PRODUCT_BADGE_DEFAULT_COLOR_THEME) {
  const candidate = String(rawValue || '').trim().toLowerCase();
  if (PRODUCT_BADGE_COLOR_THEME_KEY_SET.has(candidate)) {
    return candidate;
  }

  const fallbackCandidate = String(fallback || '').trim().toLowerCase();
  if (PRODUCT_BADGE_COLOR_THEME_KEY_SET.has(fallbackCandidate)) {
    return fallbackCandidate;
  }

  return PRODUCT_BADGE_DEFAULT_COLOR_THEME;
}

function getProductBadgeColorThemeOptions() {
  return PRODUCT_BADGE_COLOR_THEMES.map((item) => ({
    key: item.key,
    labelKo: item.labelKo,
    labelEn: item.labelEn
  }));
}

function buildProductBadgeCodeFromLabels(labelKo = '', labelEn = '', fallback = 'badge') {
  const labelEnCode = normalizeProductBadgeCode(labelEn, '');
  if (PRODUCT_BADGE_CODE_REGEX.test(labelEnCode)) {
    return labelEnCode;
  }

  const labelKoCode = normalizeProductBadgeCode(labelKo, '');
  if (PRODUCT_BADGE_CODE_REGEX.test(labelKoCode)) {
    return labelKoCode;
  }

  return normalizeProductBadgeCode(fallback, 'badge');
}

function makeUniqueProductBadgeCode(baseCode = '', excludeId = 0) {
  const safeExcludeId = Number(excludeId || 0);
  const base = normalizeProductBadgeCode(baseCode, 'badge');

  const hasCodeConflict = (candidateCode) => {
    if (safeExcludeId > 0) {
      return Boolean(
        db.prepare('SELECT id FROM product_badge_defs WHERE code = ? AND id != ? LIMIT 1').get(candidateCode, safeExcludeId)
      );
    }
    return Boolean(db.prepare('SELECT id FROM product_badge_defs WHERE code = ? LIMIT 1').get(candidateCode));
  };

  if (!hasCodeConflict(base)) {
    return base;
  }

  for (let suffix = 2; suffix < 10000; suffix += 1) {
    const suffixText = `-${suffix}`;
    const headLength = Math.max(2, PRODUCT_BADGE_CODE_MAX_LENGTH - suffixText.length);
    const candidate = `${base.slice(0, headLength)}${suffixText}`;
    if (!hasCodeConflict(candidate)) {
      return candidate;
    }
  }

  return `${base.slice(0, 30)}-${Date.now().toString(36).slice(-6)}`;
}

function getProductBadgeDefinitions() {
  return db
    .prepare(
      `
        SELECT id, code, label_ko, label_en, color_theme, sort_order
        FROM product_badge_defs
        ORDER BY sort_order ASC, id ASC
      `
    )
    .all()
    .map((row) => ({
      id: Number(row.id),
      code: normalizeProductBadgeCode(row.code, 'badge'),
      label_ko: normalizeProductBadgeLabel(row.label_ko, row.label_en || ''),
      label_en: normalizeProductBadgeLabel(row.label_en, row.label_ko || ''),
      color_theme: normalizeProductBadgeColorTheme(row.color_theme, PRODUCT_BADGE_DEFAULT_COLOR_THEME),
      sort_order: parseNonNegativeInt(row.sort_order, 0)
    }));
}

function normalizeRequestedBadgeIds(rawValue, allowedIdSet = null) {
  const candidates = Array.isArray(rawValue) ? rawValue : [rawValue];
  const unique = [...new Set(candidates
    .map((item) => Number.parseInt(String(item ?? ''), 10))
    .filter((id) => Number.isInteger(id) && id > 0))];

  if (allowedIdSet instanceof Set) {
    return unique.filter((id) => allowedIdSet.has(id));
  }
  return unique;
}

function getProductBadgeMapByProductIds(productIds = []) {
  const uniqueIds = [...new Set(
    (Array.isArray(productIds) ? productIds : [productIds])
      .map((id) => Number.parseInt(String(id ?? ''), 10))
      .filter((id) => Number.isInteger(id) && id > 0)
  )];
  const badgeMap = new Map();

  if (uniqueIds.length === 0) {
    return badgeMap;
  }

  const placeholders = uniqueIds.map(() => '?').join(', ');
  const rows = db
    .prepare(
      `
        SELECT
          pb.product_id,
          d.id AS badge_id,
          d.code,
          d.label_ko,
          d.label_en,
          d.color_theme,
          d.sort_order
        FROM product_badges pb
        JOIN product_badge_defs d ON d.id = pb.badge_def_id
        WHERE pb.product_id IN (${placeholders})
        ORDER BY d.sort_order ASC, d.id ASC
      `
    )
    .all(...uniqueIds);

  rows.forEach((row) => {
    const productId = Number(row.product_id);
    const current = badgeMap.get(productId) || [];
    current.push({
      id: Number(row.badge_id),
      code: normalizeProductBadgeCode(row.code, 'badge'),
      label_ko: normalizeProductBadgeLabel(row.label_ko, row.label_en || ''),
      label_en: normalizeProductBadgeLabel(row.label_en, row.label_ko || ''),
      color_theme: normalizeProductBadgeColorTheme(row.color_theme, PRODUCT_BADGE_DEFAULT_COLOR_THEME),
      sort_order: parseNonNegativeInt(row.sort_order, 0)
    });
    badgeMap.set(productId, current);
  });

  return badgeMap;
}

function attachProductBadges(products = []) {
  const list = Array.isArray(products) ? products : [];
  if (list.length === 0) {
    return [];
  }

  const badgeMap = getProductBadgeMapByProductIds(list.map((item) => item?.id));
  return list.map((item) => {
    const productId = Number(item?.id || 0);
    return {
      ...item,
      product_badges: badgeMap.get(productId) || []
    };
  });
}

function replaceProductBadgeLinks(productId, badgeIds = []) {
  const targetProductId = Number(productId || 0);
  if (!Number.isInteger(targetProductId) || targetProductId <= 0) {
    return;
  }

  const uniqueBadgeIds = [...new Set(
    (Array.isArray(badgeIds) ? badgeIds : [])
      .map((id) => Number.parseInt(String(id ?? ''), 10))
      .filter((id) => Number.isInteger(id) && id > 0)
  )];

  const tx = db.transaction((resolvedProductId, resolvedBadgeIds) => {
    db.prepare('DELETE FROM product_badges WHERE product_id = ?').run(resolvedProductId);
    if (resolvedBadgeIds.length === 0) {
      return;
    }
    const insert = db.prepare('INSERT INTO product_badges (product_id, badge_def_id) VALUES (?, ?)');
    resolvedBadgeIds.forEach((badgeId) => {
      insert.run(resolvedProductId, badgeId);
    });
  });

  tx(targetProductId, uniqueBadgeIds);
}

function parsePointRate(rawValue, fallback = 0) {
  const parsed = Number.parseFloat(String(rawValue ?? '').replace(/,/g, '').trim());
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  if (parsed > 100) {
    return 100;
  }
  return Number(parsed.toFixed(2));
}

function getSignupBonusPointsSetting() {
  return parseNonNegativeInt(getSetting('signupBonusPoints', '10000'), 10000);
}

function getReviewRewardPointsSetting() {
  return parseNonNegativeInt(getSetting('reviewRewardPoints', '5000'), 5000);
}

function getLegacyPurchasePointRateSetting() {
  return parsePointRate(getSetting('purchasePointRate', '0'), 0);
}

function calculateEarnedPoints(totalPrice, pointRate) {
  const amount = Number(totalPrice || 0);
  const rate = parsePointRate(pointRate, 0);
  if (!Number.isFinite(amount) || amount <= 0 || rate <= 0) {
    return 0;
  }
  return Math.max(0, Math.floor((amount * rate) / 100));
}

function awardDeliveredOrderPoints(orderId) {
  const targetOrderId = Number(orderId || 0);
  if (!Number.isInteger(targetOrderId) || targetOrderId <= 0) {
    return { awardedPoints: 0, memberUserId: 0 };
  }

  return db.transaction((resolvedOrderId) => {
    const order = db
      .prepare(
        `
          SELECT
            id,
            status,
            total_price,
            created_by_user_id,
            awarded_points,
            points_awarded_at,
            point_rate_snapshot,
            sales_margin_krw_snapshot,
            sales_cost_krw_snapshot,
            sales_synced_at
          FROM orders
          WHERE id = ?
          LIMIT 1
        `
      )
      .get(resolvedOrderId);

    if (!order || normalizeOrderStatus(order.status) !== ORDER_STATUS.DELIVERED) {
      return { awardedPoints: 0, memberUserId: 0 };
    }

    const memberUserId = Number(order.created_by_user_id || 0);
    const hasPointRecord =
      Boolean(order.points_awarded_at) || parseNonNegativeInt(order.awarded_points, 0) > 0;
    if (memberUserId <= 0 || hasPointRecord) {
      return { awardedPoints: 0, memberUserId: 0 };
    }

    const purchasePointRate = parsePointRate(order.point_rate_snapshot, getLegacyPurchasePointRateSetting());
    const pointsToAward = calculateEarnedPoints(order.total_price, purchasePointRate);
    if (pointsToAward <= 0) {
      return { awardedPoints: 0, memberUserId: 0 };
    }

    const userUpdated = db
      .prepare('UPDATE users SET reward_points = reward_points + ? WHERE id = ? AND is_admin = 0')
      .run(pointsToAward, memberUserId);
    if (userUpdated.changes === 0) {
      return { awardedPoints: 0, memberUserId: 0 };
    }

    const hasSalesSnapshot = Boolean(String(order.sales_synced_at || '').trim());
    const marginSnapshot = hasSalesSnapshot
      ? Math.round(Number(order.sales_margin_krw_snapshot || 0))
      : Math.round(
          parseNonNegativeNumber(order.total_price, 0) -
          parseNonNegativeNumber(order.sales_cost_krw_snapshot, 0)
        );
    const realMarginSnapshot = Math.round(marginSnapshot - pointsToAward);

    db.prepare(
      `
        UPDATE orders
        SET
          awarded_points = ?,
          points_awarded_at = COALESCE(points_awarded_at, datetime('now')),
          sales_real_margin_krw_snapshot = ?,
          sales_synced_at = COALESCE(sales_synced_at, datetime('now'))
        WHERE id = ?
      `
    ).run(pointsToAward, realMarginSnapshot, resolvedOrderId);

    return { awardedPoints: pointsToAward, memberUserId };
  })(targetOrderId);
}

function normalizeMemberLevelOperator(rawOperator = '') {
  const operator = String(rawOperator || '').trim().toLowerCase();
  if (operator === MEMBER_LEVEL_OPERATORS.LT || operator === '<' || operator === '미만') {
    return MEMBER_LEVEL_OPERATORS.LT;
  }
  if (operator === MEMBER_LEVEL_OPERATORS.LTE || operator === '<=' || operator === '이하') {
    return MEMBER_LEVEL_OPERATORS.LTE;
  }
  if (operator === MEMBER_LEVEL_OPERATORS.GT || operator === '>' || operator === '초과') {
    return MEMBER_LEVEL_OPERATORS.GT;
  }
  if (operator === MEMBER_LEVEL_OPERATORS.GTE || operator === '>=' || operator === '이상') {
    return MEMBER_LEVEL_OPERATORS.GTE;
  }
  return MEMBER_LEVEL_OPERATORS.GTE;
}

function getMemberLevelOperatorLabel(rawOperator = '', lang = 'ko') {
  const operator = normalizeMemberLevelOperator(rawOperator);
  if (operator === MEMBER_LEVEL_OPERATORS.LT) {
    return lang === 'en' ? 'less than (<)' : '미만 (<)';
  }
  if (operator === MEMBER_LEVEL_OPERATORS.LTE) {
    return lang === 'en' ? 'or below (<=)' : '이하 (<=)';
  }
  if (operator === MEMBER_LEVEL_OPERATORS.GT) {
    return lang === 'en' ? 'greater than (>)' : '초과 (>)';
  }
  return lang === 'en' ? 'or above (>=)' : '이상 (>=)';
}

function getMemberLevelOperatorPriority(rawOperator = '') {
  const operator = normalizeMemberLevelOperator(rawOperator);
  if (operator === MEMBER_LEVEL_OPERATORS.GTE) return 0;
  if (operator === MEMBER_LEVEL_OPERATORS.GT) return 1;
  if (operator === MEMBER_LEVEL_OPERATORS.LTE) return 2;
  return 3;
}

function buildDefaultMemberLevelRules() {
  return DEFAULT_MEMBER_LEVEL_RULES.map((rule, index) => ({
    id: String(rule.id || `level-${index + 1}`),
    nameKo: String(rule.nameKo || rule.name || `등급${index + 1}`).trim().slice(0, 40) || `등급${index + 1}`,
    nameEn: String(rule.nameEn || rule.name || `Level ${index + 1}`).trim().slice(0, 40) || `Level ${index + 1}`,
    name: String(rule.nameKo || rule.name || `등급${index + 1}`).trim().slice(0, 40) || `등급${index + 1}`,
    colorTheme: normalizeProductBadgeColorTheme(rule.colorTheme || rule.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME),
    operator: normalizeMemberLevelOperator(rule.operator || MEMBER_LEVEL_OPERATORS.GTE),
    thresholdAmount: parseNonNegativeInt(rule.thresholdAmount, 0)
  }));
}

function normalizeMemberLevelName(rawName = '', fallbackName = '') {
  const name = String(rawName || '').trim().slice(0, 40);
  if (name) {
    return name;
  }
  return String(fallbackName || '').trim().slice(0, 40);
}

function normalizeMemberLevelColorTheme(rawValue = '', fallback = PRODUCT_BADGE_DEFAULT_COLOR_THEME) {
  return normalizeProductBadgeColorTheme(rawValue, fallback);
}

function getMemberLevelDisplayName(rule = null, lang = 'ko') {
  const safeRule = rule && typeof rule === 'object' ? rule : {};
  const nameKo = normalizeMemberLevelName(safeRule.nameKo || safeRule.name || '', '');
  const nameEn = normalizeMemberLevelName(safeRule.nameEn || safeRule.name || '', '');
  if (lang === 'en') {
    return nameEn || nameKo || 'Unassigned';
  }
  return nameKo || nameEn || '미지정';
}

function parseMemberLevelThresholdAmount(rawAmount = '', fallback = 0) {
  const digits = String(rawAmount ?? '')
    .replace(/[^0-9]/g, '')
    .trim();
  if (!digits) {
    return parseNonNegativeInt(fallback, 0);
  }
  return parseNonNegativeInt(digits, parseNonNegativeInt(fallback, 0));
}

function buildUniqueMemberLevelId(rawBase = '', existingIds = []) {
  const normalizedBase = String(rawBase || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 54);
  const base = normalizedBase || 'level';
  const used = new Set((Array.isArray(existingIds) ? existingIds : []).map((item) => String(item || '').trim()));

  let candidate = base;
  let seq = 2;
  while (used.has(candidate)) {
    candidate = `${base}-${seq}`;
    seq += 1;
  }
  return candidate.slice(0, 64);
}

function parseMemberLevelRules(rawValue = '', fallbackRules = []) {
  let parsed = [];
  try {
    const maybeParsed = JSON.parse(String(rawValue || '[]'));
    if (Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = [];
  }

  const usedIds = new Set();
  const normalized = parsed
    .filter((item) => item && typeof item === 'object')
    .map((item, index) => {
      const idCandidate = String(item.id || '').trim().slice(0, 64);
      let levelId = idCandidate || `level-${index + 1}`;
      if (usedIds.has(levelId)) {
        levelId = `${levelId}-${index + 1}`;
      }
      usedIds.add(levelId);
      const nameKo = normalizeMemberLevelName(item.nameKo || item.name || item.label || '', `등급${index + 1}`);
      const nameEn = normalizeMemberLevelName(item.nameEn || item.name || item.label || '', `Level ${index + 1}`);
      return {
        id: levelId,
        nameKo,
        nameEn,
        name: nameKo,
        colorTheme: normalizeMemberLevelColorTheme(item.colorTheme || item.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME),
        operator: normalizeMemberLevelOperator(item.operator || item.condition || item.direction),
        thresholdAmount: parseNonNegativeInt(item.thresholdAmount ?? item.amount ?? item.threshold, 0)
      };
    })
    .filter((item) => item.nameKo || item.nameEn);

  if (normalized.length > 0) {
    return normalized;
  }

  return fallbackRules.map((rule, index) => ({
    id: String(rule.id || `level-${index + 1}`),
    nameKo: normalizeMemberLevelName(rule.nameKo || rule.name || '', `등급${index + 1}`),
    nameEn: normalizeMemberLevelName(rule.nameEn || rule.name || '', `Level ${index + 1}`),
    name: normalizeMemberLevelName(rule.nameKo || rule.name || '', `등급${index + 1}`),
    colorTheme: normalizeMemberLevelColorTheme(rule.colorTheme || rule.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME),
    operator: normalizeMemberLevelOperator(rule.operator || MEMBER_LEVEL_OPERATORS.GTE),
    thresholdAmount: parseNonNegativeInt(rule.thresholdAmount, 0)
  }));
}

function getMemberLevelRulesSetting() {
  const fallbackRules = buildDefaultMemberLevelRules();
  const rawValue = getSetting('memberLevelRules', JSON.stringify(fallbackRules));
  return parseMemberLevelRules(rawValue, fallbackRules);
}

function getMemberLevelIncludedGroupsSetting(availableGroups = []) {
  const fallbackGroups = availableGroups.length > 0 ? [...availableGroups] : [...SHOP_PRODUCT_GROUPS];
  let parsed = [];
  try {
    const maybeParsed = JSON.parse(
      String(getSetting('memberLevelIncludedGroups', JSON.stringify(fallbackGroups)) || '[]')
    );
    if (Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = [];
  }

  const normalized = parsed
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .filter((item, index, arr) => arr.indexOf(item) === index);

  const onlyAllowed = normalized.filter((item) => fallbackGroups.includes(item));
  if (onlyAllowed.length > 0) {
    return onlyAllowed;
  }
  return fallbackGroups;
}

function getMemberLevelPointRateMapSetting(levelRules = []) {
  let parsed = {};
  try {
    const maybeParsed = JSON.parse(
      String(
        getSetting('memberLevelPointRates', JSON.stringify(DEFAULT_MEMBER_LEVEL_POINT_RATES)) || '{}'
      )
    );
    if (maybeParsed && typeof maybeParsed === 'object' && !Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = {};
  }

  const nextMap = {};
  levelRules.forEach((rule) => {
    nextMap[rule.id] = parsePointRate(parsed[rule.id], parsePointRate(DEFAULT_MEMBER_LEVEL_POINT_RATES[rule.id], 0));
  });
  return nextMap;
}

function getRawMemberLevelPointRateSetting() {
  let parsed = {};
  try {
    const maybeParsed = JSON.parse(
      String(
        getSetting('memberLevelPointRates', JSON.stringify(DEFAULT_MEMBER_LEVEL_POINT_RATES)) || '{}'
      )
    );
    if (maybeParsed && typeof maybeParsed === 'object' && !Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = {};
  }
  return parsed;
}

function saveMemberLevelPointRates(levelRules = [], sourceMap = {}) {
  const nextMap = {};
  const safeSource = sourceMap && typeof sourceMap === 'object' && !Array.isArray(sourceMap)
    ? sourceMap
    : {};
  levelRules.forEach((rule) => {
    nextMap[rule.id] = parsePointRate(
      safeSource[rule.id],
      parsePointRate(DEFAULT_MEMBER_LEVEL_POINT_RATES[rule.id], 0)
    );
  });
  setSetting('memberLevelPointRates', JSON.stringify(nextMap));
  return nextMap;
}

function resolveMemberLevelByAmount(totalAmount, levelRules = []) {
  const amount = Number(totalAmount || 0);
  if (!Number.isFinite(amount) || amount < 0 || levelRules.length === 0) {
    return null;
  }

  const matched = levelRules.filter((rule) => {
    const operator = normalizeMemberLevelOperator(rule.operator || MEMBER_LEVEL_OPERATORS.GTE);
    const threshold = Number(rule.thresholdAmount || 0);
    if (operator === MEMBER_LEVEL_OPERATORS.LT) {
      return amount < threshold;
    }
    if (operator === MEMBER_LEVEL_OPERATORS.LTE) {
      return amount <= threshold;
    }
    if (operator === MEMBER_LEVEL_OPERATORS.GT) {
      return amount > threshold;
    }
    return amount >= threshold;
  });

  if (matched.length === 0) {
    return null;
  }

  matched.sort((a, b) => {
    const thresholdDiff = Number(b.thresholdAmount || 0) - Number(a.thresholdAmount || 0);
    if (thresholdDiff !== 0) {
      return thresholdDiff;
    }
    return getMemberLevelOperatorPriority(a.operator) - getMemberLevelOperatorPriority(b.operator);
  });

  return matched[0];
}

function getMemberAccumulatedPurchaseAmount(userId, includedGroups = []) {
  const targetUserId = Number(userId || 0);
  if (!Number.isInteger(targetUserId) || targetUserId <= 0) {
    return 0;
  }
  if (!Array.isArray(includedGroups) || includedGroups.length === 0) {
    return 0;
  }

  const placeholders = includedGroups.map(() => '?').join(', ');
  const row = db
    .prepare(
      `
        SELECT COALESCE(SUM(o.total_price), 0) AS total_amount
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.created_by_user_id = ?
          AND UPPER(TRIM(o.status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
          AND p.category_group IN (${placeholders})
      `
    )
    .get(targetUserId, ...includedGroups);

  return Number(row?.total_amount || 0);
}

function getMemberAccumulatedTotalsMap(userIds = [], includedGroups = []) {
  const uniqueUserIds = [...new Set((Array.isArray(userIds) ? userIds : [])
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item > 0))];

  const resultMap = new Map();
  if (uniqueUserIds.length === 0 || !Array.isArray(includedGroups) || includedGroups.length === 0) {
    return resultMap;
  }

  const userPlaceholders = uniqueUserIds.map(() => '?').join(', ');
  const groupPlaceholders = includedGroups.map(() => '?').join(', ');
  const rows = db
    .prepare(
      `
        SELECT
          o.created_by_user_id AS user_id,
          COALESCE(SUM(o.total_price), 0) AS total_amount
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.created_by_user_id IN (${userPlaceholders})
          AND UPPER(TRIM(o.status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
          AND p.category_group IN (${groupPlaceholders})
        GROUP BY o.created_by_user_id
      `
    )
    .all(...uniqueUserIds, ...includedGroups);

  rows.forEach((row) => {
    resultMap.set(Number(row.user_id), Number(row.total_amount || 0));
  });
  return resultMap;
}

function getMemberPointProfile(userId, lang = 'ko') {
  const groupConfigs = getProductGroupConfigs();
  const availableGroups = groupConfigs.map((group) => group.key);
  const includedGroups = getMemberLevelIncludedGroupsSetting(availableGroups);
  const levelRules = getMemberLevelRulesSetting();
  const pointRateMap = getMemberLevelPointRateMapSetting(levelRules);
  const totalAmount = getMemberAccumulatedPurchaseAmount(userId, includedGroups);
  const levelRule = resolveMemberLevelByAmount(totalAmount, levelRules);
  const levelId = levelRule?.id || '';
  const levelNameKo = normalizeMemberLevelName(levelRule?.nameKo || levelRule?.name || '', '');
  const levelNameEn = normalizeMemberLevelName(levelRule?.nameEn || levelRule?.name || '', '');
  const levelDisplayName = getMemberLevelDisplayName(levelRule, lang);
  const levelColorTheme = normalizeMemberLevelColorTheme(levelRule?.colorTheme || levelRule?.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME);
  const pointRate = levelId
    ? parsePointRate(pointRateMap[levelId], 0)
    : getLegacyPurchasePointRateSetting();

  return {
    totalAmount,
    levelRule,
    levelId,
    levelName: levelNameKo,
    levelNameKo,
    levelNameEn,
    levelDisplayName,
    levelColorTheme,
    pointRate,
    includedGroups,
    availableGroups,
    levelRules,
    pointRateMap
  };
}

function buildAdminProductSubmission(rawBody, groupConfig) {
  const fields =
    Array.isArray(groupConfig?.customFields) && groupConfig.customFields.length > 0
      ? groupConfig.customFields
      : getGroupDefaultFields(groupConfig || {});
  const safeGroupConfig = {
    ...(groupConfig || {}),
    customFields: fields
  };
  const safeBody = rawBody && typeof rawBody === 'object' ? rawBody : {};
  const brandOptions = getGroupBrandOptions(safeGroupConfig);
  const factoryOptions = getGroupFactoryOptions(safeGroupConfig);
  const modelOptionsByBrand = getGroupModelOptionsByBrand(safeGroupConfig);
  const hasModelOptionsByBrand = Object.keys(modelOptionsByBrand).length > 0;
  const fallbackModelOptions = getGroupModelOptions(safeGroupConfig);
  const selectedBrandOption = normalizeProductFilterOption(safeBody.selectedBrandOption || '');
  const selectedFactoryOption = normalizeProductFilterOption(safeBody.selectedFactoryOption || '');
  const selectedModelOption = normalizeProductFilterOption(safeBody.selectedModelOption || '');

  const parsed = parseProductFieldValuesFromBody(rawBody, safeGroupConfig);
  if (parsed.error) {
    return {
      error: parsed.error
    };
  }

  const fieldValues = parsed.values || {};
  if (Object.keys(fieldValues).length === 0) {
    return {
      error: '업로드 항목이 비어 있습니다. 분류 필드를 확인해 주세요.'
    };
  }

  if (brandOptions.length > 0) {
    if (!selectedBrandOption) {
      return {
        error: '브랜드 필터를 선택해 주세요.'
      };
    }

    const isValidBrand = brandOptions.some(
      (option) => option.toLowerCase() === selectedBrandOption.toLowerCase()
    );
    if (!isValidBrand) {
      return {
        error: '선택한 브랜드 필터가 유효하지 않습니다.'
      };
    }
    fieldValues.brand = selectedBrandOption;
  }

  if (factoryOptions.length > 0) {
    if (!selectedFactoryOption) {
      return {
        error: '공장 필터를 선택해 주세요.'
      };
    }

    const isValidFactory = factoryOptions.some(
      (option) => option.toLowerCase() === selectedFactoryOption.toLowerCase()
    );
    if (!isValidFactory) {
      return {
        error: '선택한 공장 필터가 유효하지 않습니다.'
      };
    }
    fieldValues.factory_name = selectedFactoryOption;
  }

  const selectedBrandForModel = normalizeProductFilterOption(selectedBrandOption || fieldValues.brand || '');
  const modelOptions = hasModelOptionsByBrand
    ? getGroupModelOptionsForBrand(safeGroupConfig, selectedBrandForModel)
    : fallbackModelOptions;
  const shouldValidateModelFilter = hasModelOptionsByBrand || modelOptions.length > 0;

  if (hasModelOptionsByBrand && !selectedBrandForModel) {
    return {
      error: '브랜드를 먼저 선택해 주세요.'
    };
  }

  if (hasModelOptionsByBrand && selectedBrandForModel && modelOptions.length === 0) {
    return {
      error: '선택한 브랜드에 등록된 모델이 없습니다. 메뉴관리 > 분류 필터에서 모델을 먼저 추가해 주세요.'
    };
  }

  if (shouldValidateModelFilter) {
    if (!selectedModelOption) {
      return {
        error: '모델 필터를 선택해 주세요.'
      };
    }

    const isValidModel = modelOptions.some(
      (option) => option.toLowerCase() === selectedModelOption.toLowerCase()
    );
    if (!isValidModel) {
      return {
        error: '선택한 모델 필터가 유효하지 않습니다.'
      };
    }
    fieldValues.model = selectedModelOption;
  }

  const mapped = mapFieldValuesToProductColumns(fieldValues, safeGroupConfig);
  if (!mapped.brand) {
    return {
      error: '브랜드/제목 항목을 입력해 주세요.'
    };
  }

  if (!mapped.model) {
    return {
      error: '모델 정보가 없습니다. 분류 설정 또는 입력값을 확인해 주세요.'
    };
  }

  if (isFactoryLikeGroup(safeGroupConfig) && mapped.price <= 0) {
    return {
      error: '공장제형 분류는 가격 항목을 1 이상으로 입력해 주세요.'
    };
  }

  return {
    error: '',
    fieldValues,
    mapped,
    extraFieldsJson: JSON.stringify({ values: fieldValues })
  };
}

function normalizePhone(rawPhone = '') {
  return String(rawPhone).replace(/[^0-9]/g, '');
}

function normalizePostcode(rawPostcode = '') {
  return String(rawPostcode || '')
    .replace(/[^0-9]/g, '')
    .slice(0, 5);
}

function normalizeAddressText(rawValue = '', maxLength = 160) {
  return String(rawValue || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, maxLength);
}

function composeFullAddress(postcode = '', addressBase = '', addressDetail = '') {
  const safePostcode = normalizePostcode(postcode);
  const safeAddressBase = normalizeAddressText(addressBase, 160);
  const safeAddressDetail = normalizeAddressText(addressDetail, 120);

  const chunks = [];
  if (safePostcode) {
    chunks.push(`[${safePostcode}]`);
  }
  if (safeAddressBase) {
    chunks.push(safeAddressBase);
  }
  if (safeAddressDetail) {
    chunks.push(safeAddressDetail);
  }
  return chunks.join(' ').trim();
}

function validateStructuredAddress({ postcode = '', addressBase = '', addressDetail = '' }, options = {}) {
  const requireFilled = options.requireFilled !== false;
  const safePostcode = normalizePostcode(postcode);
  const safeAddressBase = normalizeAddressText(addressBase, 160);
  const safeAddressDetail = normalizeAddressText(addressDetail, 120);
  const hasAny = Boolean(safePostcode || safeAddressBase || safeAddressDetail);

  if (!hasAny && !requireFilled) {
    return { ok: true, empty: true, postcode: '', addressBase: '', addressDetail: '' };
  }

  if (!hasAny && requireFilled) {
    return { ok: false, message: '주소를 입력해 주세요.' };
  }

  if (!safePostcode || safePostcode.length !== 5) {
    return { ok: false, message: '우편번호 5자리를 입력해 주세요.' };
  }

  if (!safeAddressBase || safeAddressBase.length < 4 || safeAddressBase.length > 160) {
    return { ok: false, message: '기본주소는 4~160자 범위로 입력해 주세요.' };
  }

  if (safeAddressDetail.length > 120) {
    return { ok: false, message: '상세주소는 120자 이하로 입력해 주세요.' };
  }

  return {
    ok: true,
    empty: false,
    postcode: safePostcode,
    addressBase: safeAddressBase,
    addressDetail: safeAddressDetail
  };
}

function getAddressBookEntries(userId) {
  return db
    .prepare(
      `
        SELECT id, label, postcode, address_base, address_detail, is_default
        FROM address_book
        WHERE user_id = ?
        ORDER BY is_default DESC, updated_at DESC, id DESC
      `
    )
    .all(userId)
    .map((row) => ({
      id: Number(row.id),
      label: String(row.label || ''),
      postcode: String(row.postcode || ''),
      addressBase: String(row.address_base || ''),
      addressDetail: String(row.address_detail || ''),
      isDefault: Number(row.is_default) === 1,
      fullAddress: composeFullAddress(row.postcode, row.address_base, row.address_detail)
    }));
}

function updateUserDefaultAddress(userId, postcode = '', addressBase = '', addressDetail = '') {
  const safePostcode = normalizePostcode(postcode);
  const safeAddressBase = normalizeAddressText(addressBase, 160);
  const safeAddressDetail = normalizeAddressText(addressDetail, 120);
  const fullAddress = composeFullAddress(safePostcode, safeAddressBase, safeAddressDetail);

  db.prepare(
    `
      UPDATE users
      SET default_postcode = ?,
          default_address_base = ?,
          default_address_detail = ?,
          default_address = ?
      WHERE id = ?
    `
  ).run(safePostcode, safeAddressBase, safeAddressDetail, fullAddress, userId);
}

function incrementFunnelEventUniqueInSession(req, eventKey, scopeKey = '') {
  const key = String(eventKey || '').trim();
  if (!key) {
    return;
  }

  const today = toKstDate();
  const scope = String(scopeKey || '').trim();
  const sessionKey = `${today}:${key}:${scope}`;

  if (!req.session) {
    incrementFunnelEvent(today, key);
    return;
  }

  if (!req.session.funnelEventMemo || typeof req.session.funnelEventMemo !== 'object') {
    req.session.funnelEventMemo = {};
  }

  const memo = req.session.funnelEventMemo;
  if (memo[sessionKey]) {
    return;
  }

  memo[sessionKey] = Date.now();
  const keys = Object.keys(memo);
  if (keys.length > SESSION_FUNNEL_KEYS_LIMIT) {
    keys
      .sort((a, b) => Number(memo[a] || 0) - Number(memo[b] || 0))
      .slice(0, keys.length - SESSION_FUNNEL_KEYS_LIMIT)
      .forEach((oldKey) => {
        delete memo[oldKey];
      });
  }

  incrementFunnelEvent(today, key);
}

function safeBackPath(req, fallback = '/main') {
  const toSafeLocalPath = (value = '') => {
    const candidate = String(value || '').trim();
    if (!candidate.startsWith('/')) {
      return '';
    }
    if (candidate.startsWith('//') || candidate.startsWith('/\\')) {
      return '';
    }
    if (/[\u0000-\u001f\u007f]/.test(candidate)) {
      return '';
    }
    return candidate;
  };

  const referer = String(req.get('referer') || '');
  if (!referer) {
    return fallback;
  }

  try {
    const parsed = new URL(referer);
    const currentHost = req.get('host');
    if (currentHost && parsed.host !== currentHost) {
      return fallback;
    }
    return toSafeLocalPath(`${parsed.pathname}${parsed.search}`) || fallback;
  } catch {
    const safePath = toSafeLocalPath(referer);
    if (safePath) {
      return safePath;
    }
    return fallback;
  }
}

function rememberOrderCompleteAccess(req, orderNo = '') {
  if (!req?.session) {
    return;
  }
  const safeOrderNo = String(orderNo || '').trim().slice(0, 120);
  if (!safeOrderNo) {
    return;
  }
  req.session.orderCompleteAccess = {
    orderNo: safeOrderNo,
    issuedAt: Date.now()
  };
}

function hasRecentOrderCompleteAccess(req, orderNo = '') {
  const safeOrderNo = String(orderNo || '').trim();
  if (!safeOrderNo) {
    return false;
  }
  const accessState = req?.session?.orderCompleteAccess;
  if (!accessState || typeof accessState !== 'object') {
    return false;
  }
  const storedOrderNo = String(accessState.orderNo || '').trim();
  const issuedAt = Number(accessState.issuedAt || 0);
  if (!storedOrderNo || storedOrderNo !== safeOrderNo) {
    return false;
  }
  if (!Number.isFinite(issuedAt) || issuedAt <= 0) {
    return false;
  }
  return Date.now() - issuedAt <= ORDER_COMPLETE_VIEW_TTL_MS;
}

function parseOriginFromHeaderUrl(rawUrl = '') {
  const raw = String(rawUrl || '').trim();
  if (!raw) {
    return '';
  }
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      return '';
    }
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return '';
  }
}

function getTrustedOriginSet(req) {
  const trusted = new Set(TRUSTED_CSRF_ORIGINS);
  const host = String(req.get('host') || '').trim().toLowerCase();
  if (!host) {
    return trusted;
  }
  trusted.add(`https://${host}`);
  trusted.add(`http://${host}`);
  return trusted;
}

function shouldEnforceOriginValidation(req) {
  const method = String(req.method || '').toUpperCase();
  if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
    return false;
  }
  if (req.path === '/health') {
    return false;
  }
  if (req.path.startsWith('/assets') || req.path.startsWith('/uploads')) {
    return false;
  }
  return true;
}

function hasAuthenticatedRequestSession(req) {
  if (Number(req?.user?.id || 0) > 0) {
    return true;
  }
  const authScope = resolveAuthScopeFromRequest(req);
  return getScopedSessionUserId(req?.session, authScope) > 0;
}

function shouldEnforceCsrfTokenValidation(req) {
  if (!shouldEnforceOriginValidation(req)) {
    return false;
  }
  const contentType = String(req.get('content-type') || '').toLowerCase();
  if (contentType.startsWith('multipart/form-data')) {
    return false;
  }
  return hasAuthenticatedRequestSession(req);
}

function rejectInvalidOriginRequest(req, res) {
  const message = '보안 검증에 실패했습니다. 페이지를 새로고침한 뒤 다시 시도해 주세요.';
  const acceptHeader = String(req.get('accept') || '').toLowerCase();
  if (req.path.startsWith('/api/') || req.xhr || acceptHeader.includes('application/json')) {
    return res.status(403).json({ ok: false, error: 'invalid_origin', message });
  }
  setFlash(req, 'error', message);
  return res.redirect(safeBackPath(req, '/main'));
}

function rejectInvalidCsrfTokenRequest(req, res) {
  const message = '보안 토큰이 유효하지 않습니다. 페이지를 새로고침한 뒤 다시 시도해 주세요.';
  const acceptHeader = String(req.get('accept') || '').toLowerCase();
  if (req.path.startsWith('/api/') || req.xhr || acceptHeader.includes('application/json')) {
    return res.status(403).json({ ok: false, error: 'invalid_csrf_token', message });
  }
  setFlash(req, 'error', message);
  return res.redirect(safeBackPath(req, '/main'));
}

function requireAuthenticatedMultipartCsrf(req, res, next) {
  const method = String(req.method || '').toUpperCase();
  if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
    return next();
  }
  if (!hasAuthenticatedRequestSession(req)) {
    return next();
  }
  const contentType = String(req.get('content-type') || '').toLowerCase();
  if (!contentType.startsWith('multipart/form-data')) {
    return next();
  }
  const expectedToken = ensureSessionCsrfToken(req);
  const providedToken = readCsrfTokenFromRequest(req);
  if (isCsrfTokenEqual(expectedToken, providedToken)) {
    return validateUploadedImagePayload(req, res, next).catch((error) => next(error));
  }
  void cleanupUploadedFiles(collectUploadedFileObjects(req));
  return rejectInvalidCsrfTokenRequest(req, res);
}

function normalizeClientIp(req) {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '')
    .split(',')
    .map((item) => String(item || '').trim())
    .filter(Boolean)[0];
  const candidate = String(req?.ip || forwarded || '').trim().toLowerCase();
  if (!candidate) {
    return 'unknown';
  }
  return candidate.slice(0, 120);
}

function normalizeAuthAttemptIdentifier(rawValue = '') {
  const text = String(rawValue || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[\u0000-\u001f\u007f]/g, '');
  if (!text) {
    return '';
  }
  return text.slice(0, AUTH_ATTEMPT_IDENTIFIER_MAX_LENGTH);
}

function buildAuthAttemptBucketKey(scopeKey = '', ipAddress = '', identifier = '') {
  const source = `${String(scopeKey || '').trim().toLowerCase()}|${String(ipAddress || '').trim().toLowerCase()}|${String(identifier || '').trim().toLowerCase()}`;
  return crypto.createHash('sha256').update(source).digest('hex');
}

function cleanupAuthAttemptStore(nowMs) {
  if (nowMs - lastAuthAttemptCleanupAt < AUTH_ATTEMPT_CLEANUP_INTERVAL_MS) {
    return;
  }
  const retentionMs = Math.max(windowMsSafeFloor(AUTH_ATTEMPT_WINDOW_MS), windowMsSafeFloor(AUTH_ATTEMPT_BLOCK_MS)) * 3;
  authAttemptCleanupStmt.run(nowMs - retentionMs);
  lastAuthAttemptCleanupAt = nowMs;
}

function windowMsSafeFloor(value) {
  const parsed = Number.parseInt(String(value || 0), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return 60 * 1000;
  }
  return Math.max(60 * 1000, parsed);
}

function consumeAuthAttempt(req, key, limit = DEFAULT_AUTH_MAX_ATTEMPTS, windowMs = AUTH_ATTEMPT_WINDOW_MS, options = {}) {
  const nowMs = Date.now();
  cleanupAuthAttemptStore(nowMs);

  const scopeKey = String(key || 'auth').trim().slice(0, 80) || 'auth';
  const ipAddress = normalizeClientIp(req);
  const identifier = normalizeAuthAttemptIdentifier(options?.identifier || '');
  const bucketKey = buildAuthAttemptBucketKey(scopeKey, ipAddress, identifier);
  const safeLimit = Math.max(1, Number.parseInt(String(limit || DEFAULT_AUTH_MAX_ATTEMPTS), 10) || DEFAULT_AUTH_MAX_ATTEMPTS);
  const safeWindowMs = windowMsSafeFloor(windowMs);
  const found = authAttemptSelectStmt.get(bucketKey);

  if (found && Number(found.blocked_until || 0) > nowMs) {
    return {
      allowed: false,
      retryAfterMs: Math.max(1000, Number(found.blocked_until || 0) - nowMs)
    };
  }

  const foundWindowStartedAt = Number(found?.window_started_at || 0);
  const isExpiredWindow = !found || foundWindowStartedAt <= 0 || nowMs - foundWindowStartedAt > safeWindowMs;
  const nextAttemptCount = isExpiredWindow ? 1 : Number(found.attempt_count || 0) + 1;
  const blockedUntil = nextAttemptCount > safeLimit ? nowMs + AUTH_ATTEMPT_BLOCK_MS : 0;

  authAttemptUpsertStmt.run(
    bucketKey,
    scopeKey,
    identifier,
    ipAddress,
    nextAttemptCount,
    isExpiredWindow ? nowMs : foundWindowStartedAt,
    blockedUntil,
    nowMs
  );

  if (blockedUntil > 0) {
    return {
      allowed: false,
      retryAfterMs: Math.max(1000, blockedUntil - nowMs)
    };
  }
  return { allowed: true, retryAfterMs: 0 };
}

function resetAuthAttempt(req, key, options = {}) {
  const scopeKey = String(key || 'auth').trim().slice(0, 80) || 'auth';
  const ipAddress = normalizeClientIp(req);
  const identifier = normalizeAuthAttemptIdentifier(options?.identifier || '');
  const bucketKey = buildAuthAttemptBucketKey(scopeKey, ipAddress, identifier);
  authAttemptDeleteStmt.run(bucketKey);
}

function authAttemptGuard({
  key,
  redirectPath,
  limit = DEFAULT_AUTH_MAX_ATTEMPTS,
  windowMs = AUTH_ATTEMPT_WINDOW_MS,
  identifierResolver = null,
  onBlocked = null
}) {
  return (req, res, next) => {
    const identifier = typeof identifierResolver === 'function' ? identifierResolver(req) : '';
    const result = consumeAuthAttempt(req, key, limit, windowMs, { identifier });
    if (!result.allowed) {
      const waitSeconds = Math.max(1, Math.ceil(Number(result.retryAfterMs || 0) / 1000));
      if (typeof onBlocked === 'function') {
        try {
          onBlocked(req, {
            key: String(key || ''),
            identifier: String(identifier || ''),
            retryAfterMs: Number(result.retryAfterMs || 0),
            waitSeconds
          });
        } catch {
          // noop
        }
      }
      const message = `시도가 너무 많습니다. ${waitSeconds}초 후 다시 시도해 주세요.`;
      const acceptHeader = String(req.get('accept') || '').toLowerCase();
      if (req.path.startsWith('/api/') || req.xhr || acceptHeader.includes('application/json')) {
        return res.status(429).json({ ok: false, error: 'too_many_attempts', message, retryAfterSeconds: waitSeconds });
      }
      setFlash(req, 'error', message);
      return res.redirect(redirectPath);
    }
    return next();
  };
}

function asyncRoute(handler) {
  return (req, res, next) => {
    Promise.resolve(handler(req, res, next)).catch(next);
  };
}

function loadUser(req, res, next) {
  migrateLegacySessionAuthState(req);
  const authScope = resolveAuthScopeFromRequest(req);
  const hasScopedSession = getScopedSessionUserId(req?.session, authScope) > 0;
  if (!hasScopedSession && !tryRestoreSessionFromPersistCookie(req, res, authScope)) {
    req.user = null;
    return next();
  }

  const activeUserId = getScopedSessionUserId(req?.session, authScope);
  if (activeUserId <= 0) {
    req.user = null;
    return next();
  }

  const nowMs = Date.now();
  const lastActivityAt = getScopedSessionLastActivityAt(req.session, authScope);
  const idleTimeoutMs = getSessionIdleTimeoutMs(authScope);
  if (lastActivityAt > 0 && nowMs - lastActivityAt > idleTimeoutMs) {
    clearScopedSessionAuthState(req, authScope, {
      clearOtpSetup: authScope === 'admin'
    });
    clearPersistAuthCookie(res, { scope: authScope });
    req.user = null;
    return next();
  }

  const user = db
    .prepare(
      `
        SELECT
          id,
          email,
          username,
          nickname,
          full_name,
          phone,
          customs_clearance_no,
          default_address,
          default_postcode,
          default_address_base,
          default_address_detail,
          profile_image_path,
          reward_points,
          is_admin,
          admin_role,
          admin_otp_enabled,
          is_blocked,
          blocked_reason,
          blocked_at,
          created_at
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(activeUserId);

  if (!user) {
    clearScopedSessionAuthState(req, authScope);
    clearPersistAuthCookie(res, { scope: authScope });
    req.user = null;
    return next();
  }

  const isAdminUser = Number(user.is_admin) === 1;
  if ((authScope === 'admin' && !isAdminUser) || (authScope === 'member' && isAdminUser)) {
    clearScopedSessionAuthState(req, authScope);
    clearPersistAuthCookie(res, { scope: authScope });
    req.user = null;
    return next();
  }

  if (Number(user.is_blocked) === 1) {
    clearScopedSessionAuthState(req, authScope, {
      clearOtpSetup: authScope === 'admin'
    });
    clearPersistAuthCookie(res, { scope: authScope });
    req.user = null;
    setFlash(req, 'error', BLOCKED_ACCOUNT_NOTICE);
    return next();
  }

  req.user = {
    id: Number(user.id),
    email: user.email,
    username: user.username,
    nickname: user.nickname || user.username || '',
    fullName: user.full_name || '',
    phone: user.phone || '',
    customsClearanceNo: user.customs_clearance_no || '',
    defaultAddress: user.default_address || '',
    defaultPostcode: user.default_postcode || '',
    defaultAddressBase: user.default_address_base || '',
    defaultAddressDetail: user.default_address_detail || '',
    profileImagePath: user.profile_image_path || '',
    rewardPoints: Number(user.reward_points || 0),
    isAdmin: Number(user.is_admin) === 1,
    adminRole: Number(user.is_admin) === 1 ? normalizeAdminRole(user.admin_role) : '',
    isAdminOtpEnabled: Number(user.is_admin) === 1 && Number(user.admin_otp_enabled || 0) === 1,
    isBlocked: Number(user.is_blocked) === 1,
    blockedReason: user.blocked_reason || '',
    blockedAt: user.blocked_at || null,
    isPrimaryAdmin:
      Number(user.is_admin) === 1 && normalizeAdminRole(user.admin_role) === ADMIN_ROLE.PRIMARY,
    createdAt: user.created_at
  };

  setScopedSessionAuthState(req, authScope, {
    userId: req.user.id,
    adminRole: req.user.adminRole,
    lastActivityAt: nowMs
  });
  ensureSessionCsrfToken(req);
  setPersistAuthCookie(res, req.user.id, {
    scope: authScope,
    isAdmin: req.user.isAdmin,
    lastActivityAt: nowMs
  });

  return next();
}

app.use(loadUser);

app.use('/admin', (req, res, next) => {
  evaluateAdminAccessShield(req)
    .then((result) => {
      if (!result || result.allowed) {
        return next();
      }
      recordSecurityAlert(
        req,
        `security.admin_waf.${String(result.reason || 'blocked').slice(0, 60)}`,
        String(result.detail || '').slice(0, 280)
      );
      return rejectAdminAccessShield(req, res, result.reason || 'policy_blocked');
    })
    .catch(() => next());
});

app.use('/api/admin', (req, res, next) => {
  evaluateAdminAccessShield(req)
    .then((result) => {
      if (!result || result.allowed) {
        return next();
      }
      recordSecurityAlert(
        req,
        `security.admin_waf.${String(result.reason || 'blocked').slice(0, 60)}`,
        String(result.detail || '').slice(0, 280)
      );
      return rejectAdminAccessShield(req, res, result.reason || 'policy_blocked');
    })
    .catch(() => next());
});

app.use((req, res, next) => {
  if (!shouldEnforceOriginValidation(req)) {
    return next();
  }

  const trustedOrigins = getTrustedOriginSet(req);
  const origin = normalizeOriginValue(req.get('origin'));
  if (origin) {
    if (trustedOrigins.has(origin)) {
      return next();
    }
    return rejectInvalidOriginRequest(req, res);
  }

  const refererOrigin = parseOriginFromHeaderUrl(req.get('referer'));
  if (refererOrigin && !trustedOrigins.has(refererOrigin)) {
    return rejectInvalidOriginRequest(req, res);
  }

  const secFetchSite = String(req.get('sec-fetch-site') || '').trim().toLowerCase();
  if (secFetchSite === 'cross-site') {
    return rejectInvalidOriginRequest(req, res);
  }

  return next();
});

app.use((req, res, next) => {
  if (!shouldEnforceCsrfTokenValidation(req)) {
    return next();
  }
  const expectedToken = ensureSessionCsrfToken(req);
  const providedToken = readCsrfTokenFromRequest(req);
  if (isCsrfTokenEqual(expectedToken, providedToken)) {
    return next();
  }
  return rejectInvalidCsrfTokenRequest(req, res);
});

app.use((req, res, next) => {
  if (req.path === '/health') {
    return next();
  }

  const fallbackLanguage = getSetting('languageDefault', 'ko');
  const lang = resolveLanguage(req.query.lang || req.cookies.lang, fallbackLanguage);
  if (req.query.lang === 'ko' || req.query.lang === 'en') {
    res.cookie('lang', lang, { maxAge: 1000 * 60 * 60 * 24 * 365, sameSite: 'lax' });
  }

  const themeMode = req.cookies.themeMode === 'night' ? 'night' : 'day';
  const today = toKstDate();

  const isTrackableVisitPath =
    !req.path.startsWith('/admin') &&
    !req.path.startsWith('/assets') &&
    !req.path.startsWith('/uploads') &&
    !req.path.startsWith('/set-lang') &&
    req.path !== '/toggle-theme' &&
    req.path !== '/favicon.ico';

  if (isTrackableVisitPath) {
    if (req.session.lastVisitDate !== today) {
      incrementVisit(today, Boolean(req.user));
      req.session.lastVisitDate = today;
    }
  }

  if (
    !req.path.startsWith('/assets') &&
    !req.path.startsWith('/uploads') &&
    req.path !== '/health'
  ) {
    void pollTrackingAndAutoCompleteOrders(false);
  }

  const publicMenus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())));
  const isAdminPage = req.path.startsWith('/admin') && req.path !== '/admin/login';
  const menus = isAdminPage && Boolean(req.user?.isAdmin) ? getAdminMenus(req.user) : publicMenus;
  const memberCartCount = req.user && !req.user.isAdmin ? getMemberCartCount(req.user.id) : 0;

  const dayThemeColors = getThemeColorConfig('day');
  const nightThemeColors = getThemeColorConfig('night');
  const dayThemeAssets = getThemeAssetConfig('day');
  const nightThemeAssets = getThemeAssetConfig('night');
  const heroSettings = getMainHeroSettings(lang);
  const activeThemeColors = themeMode === 'night' ? nightThemeColors : dayThemeColors;
  const activeThemeAssets = themeMode === 'night' ? nightThemeAssets : dayThemeAssets;

  const headerColor = activeThemeColors.headerColor;
  const backgroundType = activeThemeAssets.backgroundType;
  const backgroundValue =
    activeThemeAssets.backgroundType === 'image' && activeThemeAssets.backgroundImagePath
      ? activeThemeAssets.backgroundImagePath
      : activeThemeColors.backgroundColor;
  const themeCssVars = buildThemeCssVars(activeThemeColors);
  const hasBackgroundImage = backgroundType === 'image' && Boolean(activeThemeAssets.backgroundImagePath);

  let backgroundStyle = `background: ${activeThemeColors.backgroundColor} !important;`;
  if (hasBackgroundImage) {
    backgroundStyle = `background-color: ${activeThemeColors.backgroundColor} !important; background-image: url('${activeThemeAssets.backgroundImagePath}') !important; background-size: cover !important; background-position: center !important;`;
  }

  const shouldRenderPopupNotices = req.path === '/main';
  const popupNoticeRows = shouldRenderPopupNotices
    ? db
        .prepare(
          `
            SELECT id, title, content, image_path
            FROM notices
            WHERE is_popup = 1 AND COALESCE(is_hidden, 0) = 0
            ORDER BY id DESC
          `
        )
        .all()
    : [];
  const footerNotices = db
    .prepare(
      `
        SELECT id, title
        FROM notices
        WHERE COALESCE(is_hidden, 0) = 0
        ORDER BY id DESC
        LIMIT 6
      `
    )
    .all();

  const visitCounts = getVisitCounts(today);
  const productCounts = getProductCounts(today);
  const postCounts = getPostCounts(today);
  const contactInfoSetting = getSetting('contactInfo', '');
  const kakaoChatUrl = resolveKakaoChatUrl(
    contactInfoSetting,
    String(getSetting('kakaoChatUrl', '') || '').trim()
  );
  const supportChatAdminUnreadCount = req.user?.isAdmin ? getAdminSupportChatUnreadCount(req.user) : 0;
  const supportChatMemberUnreadCount =
    req.user && !req.user.isAdmin ? getMemberSupportChatUnreadCount(req.user.id) : 0;

  res.locals.ctx = {
    assetVersion: ASSET_VERSION,
    lang,
    t: (key) => t(lang, key),
    themeMode,
    currentUser: req.user,
    cartCount: memberCartCount,
    isAdmin: Boolean(req.user?.isAdmin),
    isPrimaryAdmin: Boolean(req.user?.isPrimaryAdmin),
    isAdminRoute: req.path.startsWith('/admin'),
    isAdminPage: isAdminPage && Boolean(req.user?.isAdmin),
    supportChatAdminUnreadCount,
    supportChatMemberUnreadCount,
    csrfToken: req.session?.csrfToken || '',
    flash: getFlash(req),
    popupFlash: getPopupFlash(req),
    formatPrice,
    menus,
    settings: {
      siteName: getSetting('siteName', 'Chrono Lab'),
      headerColor,
      headerLogoPath: activeThemeAssets.headerLogoPath,
      headerSymbolPath: activeThemeAssets.headerSymbolPath,
      footerLogoPath: activeThemeAssets.footerLogoPath,
      backgroundType,
      backgroundValue,
      backgroundStyle,
      themeCssVars,
      dayThemeColors,
      nightThemeColors,
      dayThemeAssets,
      nightThemeAssets,
      watermarkLogoPath: getBrandingWatermarkUrl(),
      bankAccountInfo: getSetting('bankAccountInfo', ''),
      signupBonusPoints: getSignupBonusPointsSetting(),
      reviewRewardPoints: getReviewRewardPointsSetting(),
      purchasePointRate: getLegacyPurchasePointRateSetting(),
      contactInfo: contactInfoSetting,
      kakaoChatUrl,
      supportChatAdminUsername: SUPPORT_CHAT_PRIMARY_ADMIN_USERNAME,
      businessInfo: getSetting('businessInfo', ''),
      footerBrandCopyKo: getSetting('footerBrandCopyKo', '심플하고 신뢰할 수 있는 시계 쇼핑.'),
      footerBrandCopyEn: getSetting('footerBrandCopyEn', 'Simple. Clean. Trusted watch shopping.'),
      heroLeftTitle: heroSettings.leftTitle,
      heroLeftTitleKo: heroSettings.leftTitleKo,
      heroLeftTitleEn: heroSettings.leftTitleEn,
      heroLeftSubtitle: heroSettings.leftSubtitle,
      heroLeftSubtitleKo: heroSettings.leftSubtitleKo,
      heroLeftSubtitleEn: heroSettings.leftSubtitleEn,
      heroLeftCtaPath: heroSettings.leftCtaPath,
      heroLeftCtaLabel: heroSettings.leftCtaLabel,
      heroLeftCtaLabelKo: heroSettings.leftCtaLabelKo,
      heroLeftCtaLabelEn: heroSettings.leftCtaLabelEn,
      heroLeftBackgroundType: heroSettings.leftBackgroundType,
      heroLeftBackgroundColor: heroSettings.leftBackgroundColor,
      heroLeftBackgroundImagePath: heroSettings.leftBackgroundImagePath,
      heroLeftPaneStyle: heroSettings.leftPaneStyle,
      heroRightTitle: heroSettings.rightTitle,
      heroRightTitleKo: heroSettings.rightTitleKo,
      heroRightTitleEn: heroSettings.rightTitleEn,
      heroRightSubtitle: heroSettings.rightSubtitle,
      heroRightSubtitleKo: heroSettings.rightSubtitleKo,
      heroRightSubtitleEn: heroSettings.rightSubtitleEn,
      heroRightBackgroundColor: heroSettings.rightBackgroundColor,
      heroRightPaneStyle: heroSettings.rightPaneStyle,
      heroQuickMenus: heroSettings.quickMenus,
      heroQuickMenuPaths: heroSettings.quickMenuPaths,
      maxUploadFileSizeMb: MAX_UPLOAD_FILE_SIZE_MB,
      maxUploadImageCount: MAX_UPLOAD_IMAGE_COUNT
    },
    metrics: {
      visitToday: visitCounts.today,
      visitTotal: visitCounts.total,
      productToday: productCounts.today,
      productTotal: productCounts.total,
      postToday: postCounts.today,
      postTotal: postCounts.total
    },
    popupNotices: popupNoticeRows.map((popupNotice) => ({
      id: Number(popupNotice.id),
      title: popupNotice.title,
      content: popupNotice.content,
      imagePath: popupNotice.image_path || ''
    })),
    footerNotices
  };
  res.locals.requestPath = req.path;

  next();
});

app.use((req, res, next) => {
  if (!req.path.startsWith('/admin')) {
    return next();
  }

  if (!req.user?.isAdmin) {
    return next();
  }

  if (req.path === '/admin/login') {
    return next();
  }

  if (req.path === '/admin/logout') {
    logAdminActivity(req, 'LOGOUT_REQUEST', 'admin logout requested');
    return next();
  }

  const actionType = req.method === 'GET' ? 'VIEW' : 'ACTION';
  logAdminActivity(req, actionType, `${req.method} ${req.path}`);
  return next();
});

function requireAuth(req, res, next) {
  if (!req.user) {
    if (!req.session.flash) {
      setFlash(req, 'error', '로그인이 필요합니다.');
    }
    return res.redirect('/login');
  }
  return next();
}

function requireAdmin(req, res, next) {
  if (!req.user?.isAdmin) {
    recordSecurityAlert(
      req,
      'security.admin.auth_required',
      req.user ? 'non-admin user attempted admin route access' : 'anonymous admin route access'
    );
    if (!req.session.flash) {
      setFlash(
        req,
        'error',
        req.user ? '관리자 계정으로 로그인해 주세요.' : '관리자 로그인이 필요합니다.'
      );
    }
    return res.redirect('/admin/login');
  }
  if (ADMIN_OTP_ENFORCED && !req.user.isAdminOtpEnabled) {
    recordSecurityAlert(req, 'security.admin.otp_required', `uid=${Number(req.user?.id || 0) || 'unknown'}`);
    clearScopedSessionAuthState(req, 'admin', { clearOtpSetup: true });
    clearPersistAuthCookie(res, { scope: 'admin' });
    setFlash(req, 'error', '관리자 OTP 설정이 필수입니다. 다시 로그인해 주세요.');
    return res.redirect('/admin/login');
  }
  return next();
}

function requirePrimaryAdmin(req, res, next) {
  if (!req.user?.isAdmin) {
    setFlash(req, 'error', req.user ? '관리자 계정으로 로그인해 주세요.' : '관리자 로그인이 필요합니다.');
    return res.redirect('/admin/login');
  }

  if (!req.user.isPrimaryAdmin) {
    recordSecurityAlert(req, 'security.primary_only.route', 'sub-admin attempted primary-only route');
    logAdminActivity(req, 'SECURITY_DENIED', 'primary-only security route');
    setFlash(req, 'error', '이 페이지는 메인관리자만 접근할 수 있습니다.');
    return res.redirect('/admin/dashboard');
  }

  return next();
}

function serializeSupportChatThreadRow(thread = {}) {
  return {
    id: Number(thread.id || 0),
    memberUserId: Number(thread.member_user_id || 0),
    assignedAdminUserId: Number(thread.assigned_admin_user_id || 0),
    memberUsername: String(thread.member_username || ''),
    memberNickname: String(thread.member_nickname || ''),
    memberFullName: String(thread.member_full_name || ''),
    assignedAdminUsername: String(thread.assigned_admin_username || ''),
    unreadCount: Number(thread.unread_count || 0),
    lastMessageText: String(thread.last_message_text || ''),
    lastMessageAt: String(thread.last_message_at || ''),
    lastSenderRole: String(thread.last_sender_role || ''),
    createdAt: String(thread.created_at || ''),
    updatedAt: String(thread.updated_at || ''),
    lastMemberMessageAt: String(thread.last_member_message_at || ''),
    lastAdminMessageAt: String(thread.last_admin_message_at || '')
  };
}

app.get('/api/support-chat/thread', requireAuth, (req, res) => {
  if (!req.user || req.user.isAdmin) {
    return res.status(403).json({ ok: false, error: 'member_only' });
  }

  const thread = ensureSupportChatThreadForMember(req.user.id);
  if (!thread) {
    return res.status(500).json({ ok: false, error: 'support_thread_create_failed' });
  }

  db.prepare(
    `
      UPDATE support_chat_messages
      SET member_read_at = datetime('now')
      WHERE thread_id = ?
        AND sender_role = 'admin'
        AND member_read_at IS NULL
    `
  ).run(thread.id);

  const messages = getSupportChatMessagesByThreadId(thread.id, 200);
  const assignedAdmin = db
    .prepare(
      `
        SELECT username, full_name
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(Number(thread.assigned_admin_user_id || 0));

  return res.json({
    ok: true,
    thread: {
      id: Number(thread.id || 0),
      assignedAdminUsername: String(assignedAdmin?.username || SUPPORT_CHAT_PRIMARY_ADMIN_USERNAME),
      assignedAdminName: String(assignedAdmin?.full_name || assignedAdmin?.username || SUPPORT_CHAT_PRIMARY_ADMIN_USERNAME)
    },
    messages,
    unreadCount: getMemberSupportChatUnreadCount(req.user.id)
  });
});

app.post('/api/support-chat/message', requireAuth, (req, res) => {
  if (!req.user || req.user.isAdmin) {
    return res.status(403).json({ ok: false, error: 'member_only' });
  }

  const messageText = normalizeSupportChatMessage(req.body?.message || '');
  if (!messageText) {
    return res.status(400).json({ ok: false, error: 'message_required' });
  }

  const thread = ensureSupportChatThreadForMember(req.user.id);
  if (!thread) {
    return res.status(500).json({ ok: false, error: 'support_thread_create_failed' });
  }

  const insertResult = db.prepare(
    `
      INSERT INTO support_chat_messages (
        thread_id,
        sender_user_id,
        sender_role,
        message_text,
        member_read_at,
        created_at
      )
      VALUES (?, ?, 'member', ?, datetime('now'), datetime('now'))
    `
  ).run(thread.id, req.user.id, messageText);

  db.prepare(
    `
      UPDATE support_chat_threads
      SET updated_at = datetime('now'),
          last_member_message_at = datetime('now'),
          assigned_admin_user_id = CASE
            WHEN COALESCE(assigned_admin_user_id, 0) <= 0 THEN ?
            ELSE assigned_admin_user_id
          END
      WHERE id = ?
    `
  ).run(resolveSupportAssignedAdminUserId() || null, thread.id);

  const message = mapSupportChatMessageRows(
    db
      .prepare(
        `
          SELECT id, thread_id, sender_user_id, sender_role, message_text, member_read_at, admin_read_at, created_at
          FROM support_chat_messages
          WHERE id = ?
          LIMIT 1
        `
      )
      .all(Number(insertResult.lastInsertRowid || 0))
  )[0] || null;

  return res.json({ ok: true, message });
});

app.get('/api/support-chat/unread-count', requireAuth, (req, res) => {
  if (!req.user || req.user.isAdmin) {
    return res.status(403).json({ ok: false, error: 'member_only' });
  }
  return res.json({ ok: true, unreadCount: getMemberSupportChatUnreadCount(req.user.id) });
});

app.get('/api/admin/support-chat/unread-count', requireAdmin, (req, res) =>
  res.json({ ok: true, unreadCount: getAdminSupportChatUnreadCount(req.user) })
);

app.get('/api/admin/support-chat/threads', requireAdmin, (req, res) => {
  const rows = db
    .prepare(
      `
        SELECT
          t.*,
          u.username AS member_username,
          u.nickname AS member_nickname,
          u.full_name AS member_full_name,
          a.username AS assigned_admin_username,
          (
            SELECT COUNT(*)
            FROM support_chat_messages sm
            WHERE sm.thread_id = t.id
              AND sm.sender_role = 'member'
              AND sm.admin_read_at IS NULL
          ) AS unread_count,
          (
            SELECT sm2.message_text
            FROM support_chat_messages sm2
            WHERE sm2.thread_id = t.id
            ORDER BY sm2.id DESC
            LIMIT 1
          ) AS last_message_text,
          (
            SELECT sm2.created_at
            FROM support_chat_messages sm2
            WHERE sm2.thread_id = t.id
            ORDER BY sm2.id DESC
            LIMIT 1
          ) AS last_message_at,
          (
            SELECT sm2.sender_role
            FROM support_chat_messages sm2
            WHERE sm2.thread_id = t.id
            ORDER BY sm2.id DESC
            LIMIT 1
          ) AS last_sender_role
        FROM support_chat_threads t
        JOIN users u ON u.id = t.member_user_id
        LEFT JOIN users a ON a.id = t.assigned_admin_user_id
        ORDER BY COALESCE(last_message_at, t.updated_at, t.created_at) DESC, t.id DESC
        LIMIT 200
      `
    )
    .all();

  return res.json({
    ok: true,
    threads: rows.map((row) => serializeSupportChatThreadRow(row)),
    unreadCount: getAdminSupportChatUnreadCount(req.user)
  });
});

app.get('/api/admin/support-chat/thread/:threadId/messages', requireAdmin, (req, res) => {
  const threadId = Number.parseInt(String(req.params.threadId || ''), 10);
  if (!Number.isInteger(threadId) || threadId <= 0) {
    return res.status(400).json({ ok: false, error: 'invalid_thread_id' });
  }

  const threadRow = db
    .prepare(
      `
        SELECT
          t.*,
          u.username AS member_username,
          u.nickname AS member_nickname,
          u.full_name AS member_full_name,
          a.username AS assigned_admin_username
        FROM support_chat_threads t
        JOIN users u ON u.id = t.member_user_id
        LEFT JOIN users a ON a.id = t.assigned_admin_user_id
        WHERE t.id = ?
        LIMIT 1
      `
    )
    .get(threadId);

  if (!threadRow) {
    return res.status(404).json({ ok: false, error: 'thread_not_found' });
  }
  if (!canAdminAccessSupportThread(req.user, threadRow)) {
    return res.status(403).json({ ok: false, error: 'access_denied' });
  }

  db.prepare(
    `
      UPDATE support_chat_messages
      SET admin_read_at = datetime('now')
      WHERE thread_id = ?
        AND sender_role = 'member'
        AND admin_read_at IS NULL
    `
  ).run(threadId);

  const messages = getSupportChatMessagesByThreadId(threadId, 300);
  return res.json({
    ok: true,
    thread: serializeSupportChatThreadRow(threadRow),
    messages,
    unreadCount: getAdminSupportChatUnreadCount(req.user)
  });
});

app.post('/api/admin/support-chat/thread/:threadId/message', requireAdmin, (req, res) => {
  const threadId = Number.parseInt(String(req.params.threadId || ''), 10);
  if (!Number.isInteger(threadId) || threadId <= 0) {
    return res.status(400).json({ ok: false, error: 'invalid_thread_id' });
  }

  const threadRow = db
    .prepare(
      `
        SELECT *
        FROM support_chat_threads
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(threadId);
  if (!threadRow) {
    return res.status(404).json({ ok: false, error: 'thread_not_found' });
  }
  if (!canAdminAccessSupportThread(req.user, threadRow)) {
    return res.status(403).json({ ok: false, error: 'access_denied' });
  }

  const messageText = normalizeSupportChatMessage(req.body?.message || '');
  if (!messageText) {
    return res.status(400).json({ ok: false, error: 'message_required' });
  }

  const insertResult = db.prepare(
    `
      INSERT INTO support_chat_messages (
        thread_id,
        sender_user_id,
        sender_role,
        message_text,
        admin_read_at,
        created_at
      )
      VALUES (?, ?, 'admin', ?, datetime('now'), datetime('now'))
    `
  ).run(threadId, Number(req.user.id || 0), messageText);

  db.prepare(
    `
      UPDATE support_chat_threads
      SET updated_at = datetime('now'),
          last_admin_message_at = datetime('now'),
          assigned_admin_user_id = CASE
            WHEN COALESCE(assigned_admin_user_id, 0) <= 0 THEN ?
            ELSE assigned_admin_user_id
          END
      WHERE id = ?
    `
  ).run(Number(req.user.id || 0), threadId);

  const message = mapSupportChatMessageRows(
    db
      .prepare(
        `
          SELECT id, thread_id, sender_user_id, sender_role, message_text, member_read_at, admin_read_at, created_at
          FROM support_chat_messages
          WHERE id = ?
          LIMIT 1
        `
      )
      .all(Number(insertResult.lastInsertRowid || 0))
  )[0] || null;

  return res.json({
    ok: true,
    message,
    unreadCount: getAdminSupportChatUnreadCount(req.user)
  });
});

function getSecurityAlertReasonLabel(reasonCode = '', lang = 'ko') {
  const isEn = lang === 'en';
  const reason = String(reasonCode || '').trim();
  const labels = {
    'security.primary_only.denied': isEn
      ? 'Primary-only area access attempt'
      : '메인관리자 전용 영역 접근 시도',
    'security.primary_only.route': isEn
      ? 'Primary-only route execution attempt'
      : '메인관리자 전용 기능 실행 시도'
  };

  return labels[reason] || reason || (isEn ? 'Unknown' : '알 수 없음');
}

app.get('/', (req, res) => {
  res.redirect('/main');
});

app.get('/notices', (req, res) => res.redirect('/notice'));
app.get('/reviews', (req, res) => res.redirect('/review'));
app.get('/inquiries', (req, res) => res.redirect('/inquiry'));

app.get('/set-lang/:lang', (req, res) => {
  const language = resolveLanguage(req.params.lang, 'ko');
  res.cookie('lang', language, { maxAge: 1000 * 60 * 60 * 24 * 365, sameSite: 'lax' });
  res.redirect(safeBackPath(req, '/main'));
});

app.get('/toggle-theme', (req, res) => {
  const current = req.cookies.themeMode === 'night' ? 'night' : 'day';
  const nextTheme = current === 'night' ? 'day' : 'night';
  res.cookie('themeMode', nextTheme, { maxAge: 1000 * 60 * 60 * 24 * 365, sameSite: 'lax' });
  res.redirect(safeBackPath(req, '/main'));
});

app.get('/main', (req, res) => {
  const productGroupConfigs = getProductGroupConfigs();
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const { featured: featuredTopGroups, regular: regularGroups } = splitProductGroupsForDisplay(productGroupConfigs);
  const loadGroupPreviewProducts = (groupKey, limit = 4) => {
    const safeLimit = Math.max(1, Math.min(12, parseNonNegativeInt(limit, 4)));
    const rows = db
      .prepare(
        `
          SELECT
            id,
            category_group,
            brand,
            model,
            sub_model,
            price,
            image_path,
            shipping_period,
            case_material,
            movement,
            features,
            extra_fields_json,
            is_sold_out
          FROM products
          WHERE is_active = 1 AND category_group = ?
          ORDER BY id DESC
          LIMIT ?
        `
      )
      .all(groupKey, safeLimit);

    return attachProductBadges(
      rows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)))
    );
  };

  const groupedProducts = regularGroups.map((groupConfig) => ({
    groupName: groupConfig.key,
    products: loadGroupPreviewProducts(groupConfig.key, 4)
  }));
  const featuredTopProducts = featuredTopGroups.map((groupConfig) => ({
    groupName: groupConfig.key,
    products: loadGroupPreviewProducts(groupConfig.key, 6)
  }));

  const latestNotices = db
    .prepare(
      `
        SELECT id, title, created_at
        FROM notices
        WHERE COALESCE(is_hidden, 0) = 0
        ORDER BY id DESC
        LIMIT 5
      `
    )
    .all();

  const latestNews = db
    .prepare(
      `
        SELECT id, title, content, image_path, created_at
        FROM news_posts
        WHERE COALESCE(is_hidden, 0) = 0
        ORDER BY id DESC
        LIMIT 3
      `
    )
    .all();

  res.render('main', {
    title: 'Main',
    groupedProducts,
    featuredTopProducts,
    latestNotices,
    latestNews,
    productGroupConfigs,
    groupLabelMap: getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang)
  });
});

app.get('/shop', (req, res) => {
  const productGroupConfigs = getProductGroupConfigs();
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const fallbackGroups = productGroupConfigs.map((groupConfig) => groupConfig.key);
  const SHOP_ALL_GROUP_KEY = 'all';
  if (fallbackGroups.length === 0) {
    fallbackGroups.push(...SHOP_PRODUCT_GROUPS);
  }
  const { featured: featuredShopGroups } = splitProductGroupsForDisplay(productGroupConfigs);
  const featuredGroupKeys = featuredShopGroups
    .map((groupConfig) => String(groupConfig.key || '').trim())
    .filter(Boolean);
  const featuredGroupKeySet = new Set(featuredGroupKeys);
  const shopFilterFeaturedGroups = fallbackGroups.filter((groupKey) => featuredGroupKeySet.has(groupKey));
  const shopFilterRegularGroups = fallbackGroups.filter((groupKey) => !featuredGroupKeySet.has(groupKey));

  const groupRaw = String(req.query.group || '').trim();
  const isAllGroupRequested = groupRaw.toLowerCase() === SHOP_ALL_GROUP_KEY;
  const group = (
    !groupRaw ||
    isAllGroupRequested ||
    !fallbackGroups.includes(groupRaw)
  )
    ? SHOP_ALL_GROUP_KEY
    : groupRaw;
  const isAllGroup = group === SHOP_ALL_GROUP_KEY;
  const selectedGroupConfig = isAllGroup
    ? {
        key: SHOP_ALL_GROUP_KEY,
        labelKo: '전체',
        labelEn: 'All',
        mode: PRODUCT_GROUP_MODE.SIMPLE,
        brandOptions: [],
        factoryOptions: [],
        modelOptions: [],
        modelOptionsByBrand: {},
        brandOptionLabels: {},
        factoryOptionLabels: {},
        modelOptionLabelsByBrand: {},
        customFields: []
      }
    : (productGroupMap.get(group) || {
        key: group,
        labelKo: group,
        labelEn: group,
        mode: PRODUCT_GROUP_MODE.SIMPLE,
        brandOptions: [],
        factoryOptions: [],
        modelOptions: [],
        modelOptionsByBrand: {},
        brandOptionLabels: {},
        factoryOptionLabels: {},
        modelOptionLabelsByBrand: {},
        customFields: []
      });
  const selectedBrandRaw = normalizeProductFilterOption(req.query.brand || '');
  const selectedFactoryRaw = normalizeProductFilterOption(req.query.factory || '');
  const selectedModelRaw = normalizeProductFilterOption(req.query.model || '');
  const selectedGroupFilterToggles = getGroupFilterToggleState(selectedGroupConfig);
  const allGroupFilterToggles = productGroupConfigs.reduce(
    (acc, groupConfig) => {
      const toggles = getGroupFilterToggleState(groupConfig);
      return {
        brand: acc.brand || toggles.brand,
        model: acc.model || toggles.model,
        factory: acc.factory || toggles.factory
      };
    },
    { brand: false, model: false, factory: false }
  );

  const mergeFilterLabelMap = (target = {}, source = {}) => {
    if (!source || typeof source !== 'object' || Array.isArray(source)) return;
    Object.entries(source).forEach(([rawKey, value]) => {
      const safeKey = normalizeProductFilterOption(rawKey);
      if (!safeKey || Object.prototype.hasOwnProperty.call(target, safeKey)) return;
      target[safeKey] = value;
    });
  };

  const mergeModelLabelMapByBrand = (target = {}, source = {}) => {
    if (!source || typeof source !== 'object' || Array.isArray(source)) return;
    Object.entries(source).forEach(([rawBrand, rawMap]) => {
      const safeBrand = normalizeProductFilterOption(rawBrand);
      if (!safeBrand || !rawMap || typeof rawMap !== 'object' || Array.isArray(rawMap)) return;
      if (!target[safeBrand]) {
        target[safeBrand] = {};
      }
      mergeFilterLabelMap(target[safeBrand], rawMap);
    });
  };
  const mergeModelOptionMapByBrand = (target = {}, source = {}) => {
    if (!source || typeof source !== 'object' || Array.isArray(source)) return;
    Object.entries(source).forEach(([rawBrand, rawList]) => {
      const safeBrand = normalizeProductFilterOption(rawBrand);
      if (!safeBrand) return;
      const normalizedList = normalizeProductFilterOptionList(rawList);
      if (!target[safeBrand]) {
        target[safeBrand] = [];
      }
      target[safeBrand] = normalizeProductFilterOptionList([
        ...target[safeBrand],
        ...normalizedList
      ]);
    });
  };

  let supportsBrandFilter = false;
  let supportsFactoryFilter = false;
  let supportsModelFilter = false;
  let brands = [];
  let factories = [];
  let models = [];
  let brand = '';
  let factory = '';
  let model = '';
  let brandItems = [];
  let factoryItems = [];
  let modelItems = [];

  if (isAllGroup) {
    const mergedBrandLabels = {};
    const mergedFactoryLabels = {};
    const mergedModelLabelsByBrand = {};
    const mergedBrandOptionSet = new Set();
    const mergedFactoryOptionSet = new Set();
    const mergedModelOptionMapByBrand = {};
    const defaultFilterSeeds = getDefaultGroupFilterSeeds();
    supportsBrandFilter = allGroupFilterToggles.brand;
    supportsModelFilter = allGroupFilterToggles.model;
    supportsFactoryFilter = allGroupFilterToggles.factory;

    productGroupConfigs.forEach((groupConfig) => {
      const toggles = getGroupFilterToggleState(groupConfig);
      if (supportsBrandFilter && toggles.brand) {
        getGroupBrandOptions(groupConfig).forEach((value) => {
          const safeValue = normalizeProductFilterOption(value);
          if (safeValue) mergedBrandOptionSet.add(safeValue);
        });
        mergeFilterLabelMap(mergedBrandLabels, getGroupBrandOptionLabels(groupConfig));
      }
      if (supportsFactoryFilter && toggles.factory) {
        getGroupFactoryOptions(groupConfig).forEach((value) => {
          const safeValue = normalizeProductFilterOption(value);
          if (safeValue) mergedFactoryOptionSet.add(safeValue);
        });
        mergeFilterLabelMap(mergedFactoryLabels, getGroupFactoryOptionLabels(groupConfig));
      }
      if (supportsModelFilter && toggles.model) {
        mergeModelLabelMapByBrand(mergedModelLabelsByBrand, getGroupModelOptionLabelsByBrand(groupConfig));
        mergeModelOptionMapByBrand(mergedModelOptionMapByBrand, getGroupModelOptionsByBrand(groupConfig));
      }
    });
    if (supportsBrandFilter) {
      getGroupBrandOptions(defaultFilterSeeds.factory || {}).forEach((value) => {
        const safeValue = normalizeProductFilterOption(value);
        if (safeValue) mergedBrandOptionSet.add(safeValue);
      });
      getGroupBrandOptions(defaultFilterSeeds.simple || {}).forEach((value) => {
        const safeValue = normalizeProductFilterOption(value);
        if (safeValue) mergedBrandOptionSet.add(safeValue);
      });
      mergeFilterLabelMap(mergedBrandLabels, defaultFilterSeeds.factory?.brandOptionLabels || {});
      mergeFilterLabelMap(mergedBrandLabels, defaultFilterSeeds.simple?.brandOptionLabels || {});
    }
    if (supportsFactoryFilter) {
      getGroupFactoryOptions(defaultFilterSeeds.factory || {}).forEach((value) => {
        const safeValue = normalizeProductFilterOption(value);
        if (safeValue) mergedFactoryOptionSet.add(safeValue);
      });
      mergeFilterLabelMap(mergedFactoryLabels, defaultFilterSeeds.factory?.factoryOptionLabels || {});
    }
    if (supportsModelFilter) {
      mergeModelLabelMapByBrand(mergedModelLabelsByBrand, defaultFilterSeeds.factory?.modelOptionLabelsByBrand || {});
      mergeModelLabelMapByBrand(mergedModelLabelsByBrand, defaultFilterSeeds.simple?.modelOptionLabelsByBrand || {});
      mergeModelOptionMapByBrand(mergedModelOptionMapByBrand, defaultFilterSeeds.factory?.modelOptionsByBrand || {});
      mergeModelOptionMapByBrand(mergedModelOptionMapByBrand, defaultFilterSeeds.simple?.modelOptionsByBrand || {});
    }

    if (supportsBrandFilter) {
      const discoveredBrands = db
        .prepare(
          `
            SELECT DISTINCT brand
            FROM products
            WHERE is_active = 1
            ORDER BY brand ASC
          `
        )
        .all()
        .map((row) => normalizeProductFilterOption(row.brand))
        .filter(Boolean);
      const discoveredBrandOptions = normalizeProductFilterOptionList(discoveredBrands);
      const mergedBrandOptions = normalizeProductFilterOptionList(Array.from(mergedBrandOptionSet));
      brands = mergedBrandOptions.length > 0 ? mergedBrandOptions : discoveredBrandOptions;
      brand = brands.some((item) => item.toLowerCase() === selectedBrandRaw.toLowerCase()) ? selectedBrandRaw : '';
      brandItems = getProductFilterOptionItems(brands, mergedBrandLabels, res.locals.ctx.lang);
    } else {
      brands = [];
      brand = '';
      brandItems = [];
    }

    if (supportsModelFilter && brand) {
      const matchedModelMapBrand = findMatchingProductFilterKey(mergedModelOptionMapByBrand, brand);
      const configuredModelsForBrand = matchedModelMapBrand
        ? normalizeProductFilterOptionList(mergedModelOptionMapByBrand[matchedModelMapBrand] || [])
        : [];
      const discoveredModels = db
        .prepare(
          `
            SELECT DISTINCT model
            FROM products
            WHERE is_active = 1
              AND brand = ?
            ORDER BY model ASC
          `
        )
        .all(brand)
        .map((row) => normalizeProductFilterOption(row.model))
        .filter(Boolean);
      const discoveredModelOptions = normalizeProductFilterOptionList(discoveredModels);
      models = configuredModelsForBrand.length > 0 ? configuredModelsForBrand : discoveredModelOptions;
    } else if (supportsModelFilter && !supportsBrandFilter) {
      const configuredAllModels = normalizeProductFilterOptionList(
        Object.values(mergedModelOptionMapByBrand).flat()
      );
      if (configuredAllModels.length > 0) {
        models = configuredAllModels;
      } else {
        const discoveredModels = db
          .prepare(
            `
              SELECT DISTINCT model
              FROM products
              WHERE is_active = 1
                AND TRIM(COALESCE(model, '')) != ''
              ORDER BY model ASC
            `
          )
          .all()
          .map((row) => normalizeProductFilterOption(row.model))
          .filter(Boolean);
        models = normalizeProductFilterOptionList(discoveredModels);
      }
    } else {
      models = [];
    }
    model = supportsModelFilter && models.some((item) => item.toLowerCase() === selectedModelRaw.toLowerCase())
      ? selectedModelRaw
      : '';
    const modelOptionLabels = (() => {
      if (!supportsModelFilter) return {};
      if (!supportsBrandFilter) {
        const flattenedLabelMap = {};
        Object.values(mergedModelLabelsByBrand).forEach((entry) => {
          if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return;
          mergeFilterLabelMap(flattenedLabelMap, entry);
        });
        return flattenedLabelMap;
      }
      if (!brand) return {};
      const matchedBrandKey = findMatchingProductFilterKey(mergedModelLabelsByBrand, brand);
      if (!matchedBrandKey) return {};
      const source = mergedModelLabelsByBrand[matchedBrandKey];
      return source && typeof source === 'object' && !Array.isArray(source) ? source : {};
    })();
    modelItems = supportsModelFilter
      ? getProductFilterOptionItems(models, modelOptionLabels, res.locals.ctx.lang)
      : [];

    if (supportsFactoryFilter) {
      const mergedFactoryOptions = normalizeProductFilterOptionList(Array.from(mergedFactoryOptionSet));
      const factoryWhere = ['is_active = 1', "TRIM(COALESCE(factory_name, '')) != ''"];
      const factoryParams = [];
      if (supportsBrandFilter && brand) {
        factoryWhere.push('brand = ?');
        factoryParams.push(brand);
      }
      if (supportsModelFilter && model) {
        factoryWhere.push('model = ?');
        factoryParams.push(model);
      }
      const discoveredFactoriesFiltered = db
        .prepare(
          `
            SELECT DISTINCT factory_name
            FROM products
            WHERE ${factoryWhere.join(' AND ')}
            ORDER BY factory_name ASC
          `
        )
        .all(...factoryParams)
        .map((row) => normalizeProductFilterOption(row.factory_name))
        .filter(Boolean);
      const discoveredFactoryOptions = normalizeProductFilterOptionList(discoveredFactoriesFiltered);
      factories = mergedFactoryOptions.length > 0 ? mergedFactoryOptions : discoveredFactoryOptions;
      factory = factories.some((item) => item.toLowerCase() === selectedFactoryRaw.toLowerCase())
        ? selectedFactoryRaw
        : '';
      factoryItems = getProductFilterOptionItems(
        factories,
        mergedFactoryLabels,
        res.locals.ctx.lang,
        { sortByLabel: true }
      );
    } else {
      factories = [];
      factory = '';
      factoryItems = [];
    }
  } else {
    const factoryTemplateGroup = isFactoryLikeGroup(selectedGroupConfig);
    const groupFactorySeedOptions = getGroupFactoryOptions(selectedGroupConfig);
    const discoveredFactories = db
      .prepare(
        `
          SELECT DISTINCT factory_name
          FROM products
          WHERE is_active = 1
            AND category_group = ?
            AND TRIM(COALESCE(factory_name, '')) != ''
          ORDER BY factory_name ASC
        `
      )
      .all(group)
      .map((row) => normalizeProductFilterOption(row.factory_name))
      .filter(Boolean);
    const discoveredFactoryOptions = normalizeProductFilterOptionList(discoveredFactories);
    const isDomesticStock = isDomesticStockGroup(selectedGroupConfig) || isDomesticStockGroup(group);
    supportsFactoryFilter = (
      factoryTemplateGroup ||
      groupFactorySeedOptions.length > 0 ||
      String(group || '').trim() === '현지중고' ||
      isDomesticStock ||
      discoveredFactoryOptions.length > 0
    );
    const discoveredBrands = db
      .prepare(
        `
          SELECT DISTINCT brand
          FROM products
          WHERE is_active = 1 AND category_group = ?
          ORDER BY brand ASC
        `
      )
      .all(group)
      .map((row) => normalizeProductFilterOption(row.brand))
      .filter(Boolean);
    const discoveredBrandOptions = normalizeProductFilterOptionList(discoveredBrands);
    const configuredBrands = getGroupBrandOptions(selectedGroupConfig);
    const configuredBrandLabels = getGroupBrandOptionLabels(selectedGroupConfig);
    supportsBrandFilter = selectedGroupFilterToggles.brand;
    brands = supportsBrandFilter
      ? (configuredBrands.length > 0 ? configuredBrands : discoveredBrandOptions)
      : [];

    const configuredFactories = groupFactorySeedOptions;
    const configuredFactoryLabels = getGroupFactoryOptionLabels(selectedGroupConfig);
    factories = (selectedGroupFilterToggles.factory && supportsFactoryFilter)
      ? configuredFactories.length > 0
        ? configuredFactories
        : discoveredFactoryOptions
      : [];
    supportsFactoryFilter = selectedGroupFilterToggles.factory && supportsFactoryFilter;

    brand = supportsBrandFilter && brands.some((item) => item.toLowerCase() === selectedBrandRaw.toLowerCase())
      ? selectedBrandRaw
      : '';
    factory = factories.some((item) => item.toLowerCase() === selectedFactoryRaw.toLowerCase())
      ? selectedFactoryRaw
      : '';
    const modelOptionMap = getGroupModelOptionsByBrand(selectedGroupConfig);
    const hasModelOptionMap = Object.keys(modelOptionMap).length > 0;
    const fallbackModelOptions = getGroupModelOptions(selectedGroupConfig);
    supportsModelFilter = selectedGroupFilterToggles.model && (
      hasModelOptionMap || fallbackModelOptions.length > 0 || brands.length > 0
    );
    models = brand
      ? getGroupModelOptionsForBrand(selectedGroupConfig, brand)
      : (supportsModelFilter && !supportsBrandFilter)
        ? (() => {
            const configuredAllModels = getGroupAllModelOptions(selectedGroupConfig);
            if (configuredAllModels.length > 0) {
              return configuredAllModels;
            }
            const discoveredModels = db
              .prepare(
                `
                  SELECT DISTINCT model
                  FROM products
                  WHERE is_active = 1
                    AND category_group = ?
                    AND TRIM(COALESCE(model, '')) != ''
                  ORDER BY model ASC
                `
              )
              .all(group)
              .map((row) => normalizeProductFilterOption(row.model))
              .filter(Boolean);
            return normalizeProductFilterOptionList(discoveredModels);
          })()
        : hasModelOptionMap
          ? []
          : fallbackModelOptions;
    if (!supportsModelFilter) {
      models = [];
    }
    model = supportsModelFilter && models.some((item) => item.toLowerCase() === selectedModelRaw.toLowerCase())
      ? selectedModelRaw
      : '';
    const modelOptionLabels = supportsBrandFilter
      ? getGroupModelOptionLabelsForBrand(selectedGroupConfig, brand)
      : (() => {
          const flattenedLabelMap = {};
          const rawByBrand = getGroupModelOptionLabelsByBrand(selectedGroupConfig);
          Object.values(rawByBrand).forEach((entry) => {
            if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return;
            mergeFilterLabelMap(flattenedLabelMap, entry);
          });
          return flattenedLabelMap;
        })();
    brandItems = supportsBrandFilter
      ? getProductFilterOptionItems(brands, configuredBrandLabels, res.locals.ctx.lang)
      : [];
    factoryItems = getProductFilterOptionItems(
      factories,
      configuredFactoryLabels,
      res.locals.ctx.lang,
      { sortByLabel: true }
    );
    modelItems = supportsModelFilter
      ? getProductFilterOptionItems(models, modelOptionLabels, res.locals.ctx.lang)
      : [];
  }

  const where = ['is_active = 1'];
  const params = [];

  if (!isAllGroup) {
    where.push('category_group = ?');
    params.push(group);
  }

  if (brand) {
    where.push('brand = ?');
    params.push(brand);
  }

  if (factory) {
    where.push('factory_name = ?');
    params.push(factory);
  }

  if (model) {
    where.push('model = ?');
    params.push(model);
  }

  const productRows = db
    .prepare(
      `
        SELECT
          id,
          category_group,
          brand,
          model,
          sub_model,
          price,
          image_path,
          shipping_period,
          case_material,
          movement,
          features,
          extra_fields_json,
          is_sold_out
        FROM products
        WHERE ${where.join(' AND ')}
        ORDER BY id DESC
      `
    )
    .all(...params);
  const products = attachProductBadges(
    productRows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)))
  );

  res.render('shop', {
    title: 'Shop',
    group,
    productGroups: fallbackGroups,
    shopFilterRegularGroups,
    shopFilterFeaturedGroups,
    productGroupConfigs,
    selectedGroupConfig,
    supportsBrandFilter,
    supportsModelFilter,
    supportsFactoryFilter,
    groupLabelMap: getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang),
    brand,
    factory,
    model,
    brands,
    factories,
    models,
    brandItems,
    factoryItems,
    modelItems,
    products
  });
});

app.get('/cart', requireAuth, (req, res) => {
  const cartSummary = getMemberCartSummary(req.user.id, res.locals.ctx.lang);
  return res.render('cart', {
    title: res.locals.ctx.lang === 'en' ? 'Cart' : '장바구니',
    cartItems: cartSummary.items,
    cartSummary
  });
});

app.post('/shop/item/:id/cart', requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const isEn = res.locals.ctx.lang === 'en';
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '잘못된 상품입니다.');
    return res.redirect('/shop');
  }

  const product = db
    .prepare('SELECT id, is_sold_out FROM products WHERE id = ? AND is_active = 1 LIMIT 1')
    .get(id);
  if (!product) {
    setFlash(req, 'error', '상품을 찾을 수 없습니다.');
    return res.redirect('/shop');
  }
  if (Number(product.is_sold_out || 0) === 1) {
    setFlash(req, 'error', isEn ? 'This item is sold out and cannot be added to cart.' : '판매완료 상품은 장바구니에 담을 수 없습니다.');
    return res.redirect(`/shop/item/${id}`);
  }

  const requestedQty = parsePositiveInt(req.body.quantity, 1);
  const quantity = Math.min(99, Math.max(1, requestedQty));
  const existing = db
    .prepare('SELECT id, quantity FROM cart_items WHERE user_id = ? AND product_id = ? LIMIT 1')
    .get(req.user.id, id);

  if (existing) {
    const nextQuantity = Math.min(99, Number(existing.quantity || 0) + quantity);
    db.prepare('UPDATE cart_items SET quantity = ?, updated_at = datetime(\'now\') WHERE id = ?').run(
      nextQuantity,
      existing.id
    );
  } else {
    db.prepare(
      `
        INSERT INTO cart_items (user_id, product_id, quantity, created_at, updated_at)
        VALUES (?, ?, ?, datetime('now'), datetime('now'))
      `
    ).run(req.user.id, id, quantity);
  }

  setFlash(req, 'success', '장바구니에 담았습니다.');
  return res.redirect(safeBackPath(req, `/shop/item/${id}`));
});

app.post('/cart/item/:itemId/update', requireAuth, (req, res) => {
  const itemId = Number(req.params.itemId);
  if (!Number.isInteger(itemId) || itemId <= 0) {
    setFlash(req, 'error', '잘못된 요청입니다.');
    return res.redirect('/cart');
  }

  const cartItem = db
    .prepare('SELECT id, quantity FROM cart_items WHERE id = ? AND user_id = ? LIMIT 1')
    .get(itemId, req.user.id);
  if (!cartItem) {
    setFlash(req, 'error', '장바구니 항목을 찾을 수 없습니다.');
    return res.redirect('/cart');
  }

  const quantityInput = parseNonNegativeInt(req.body.quantity, Number(cartItem.quantity || 1));
  if (quantityInput <= 0) {
    db.prepare('DELETE FROM cart_items WHERE id = ? AND user_id = ?').run(itemId, req.user.id);
    setFlash(req, 'success', '장바구니에서 삭제되었습니다.');
    return res.redirect('/cart');
  }

  const nextQuantity = Math.min(99, Math.max(1, quantityInput));
  db.prepare('UPDATE cart_items SET quantity = ?, updated_at = datetime(\'now\') WHERE id = ? AND user_id = ?').run(
    nextQuantity,
    itemId,
    req.user.id
  );
  setFlash(req, 'success', '수량이 변경되었습니다.');
  return res.redirect('/cart');
});

app.post('/cart/item/:itemId/remove', requireAuth, (req, res) => {
  const itemId = Number(req.params.itemId);
  if (!Number.isInteger(itemId) || itemId <= 0) {
    setFlash(req, 'error', '잘못된 요청입니다.');
    return res.redirect('/cart');
  }

  db.prepare('DELETE FROM cart_items WHERE id = ? AND user_id = ?').run(itemId, req.user.id);
  setFlash(req, 'success', '장바구니에서 삭제되었습니다.');
  return res.redirect('/cart');
});

app.get('/shop/item/:id', (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).render('simple-error', { title: 'Error', message: '잘못된 상품입니다.' });
  }

  const product = db.prepare('SELECT * FROM products WHERE id = ? AND is_active = 1 LIMIT 1').get(id);

  if (!product) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '상품을 찾을 수 없습니다.' });
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const productGroupConfig = productGroupMap.get(product.category_group) || {
    key: product.category_group,
    labelKo: product.category_group,
    labelEn: product.category_group,
    mode: PRODUCT_GROUP_MODE.SIMPLE,
    brandOptions: [],
    factoryOptions: [],
    modelOptions: [],
    modelOptionsByBrand: {},
    brandOptionLabels: {},
    factoryOptionLabels: {},
    modelOptionLabelsByBrand: {},
    customFields: []
  };

  incrementFunnelEventUniqueInSession(req, FUNNEL_EVENT.PRODUCT_VIEW, `product:${id}`);

  const imageRows = db
    .prepare(
      `
        SELECT image_path
        FROM product_images
        WHERE product_id = ?
        ORDER BY sort_order ASC, id ASC
      `
    )
    .all(product.id);
  const imageList = imageRows.map((row) => row.image_path).filter(Boolean);
  if (imageList.length === 0 && product.image_path) {
    imageList.push(product.image_path);
  }

  const SIMILAR_LIMIT = 6;
  const normalizedModel = String(product.model || '')
    .trim()
    .toLowerCase();
  const seenSimilarIds = new Set([Number(product.id)]);
  const similarRows = [];
  const appendUniqueSimilarRows = (rows = []) => {
    rows.forEach((row) => {
      const rowId = Number.parseInt(String(row?.id ?? ''), 10);
      if (!Number.isInteger(rowId) || rowId <= 0 || seenSimilarIds.has(rowId)) {
        return;
      }
      if (similarRows.length >= SIMILAR_LIMIT) {
        return;
      }
      seenSimilarIds.add(rowId);
      similarRows.push(row);
    });
  };
  const appendSimilarRowsFromGroup = (groupKey = '') => {
    const requestedGroupKey = String(groupKey || '').trim();
    const normalizedGroupKey = normalizeProductGroupKey(requestedGroupKey);
    if (!normalizedGroupKey || similarRows.length >= SIMILAR_LIMIT) {
      return;
    }
    const resolvedGroupValue = (
      productGroupConfigs.find(
        (groupConfig) => normalizeProductGroupKey(groupConfig?.key || '') === normalizedGroupKey
      )?.key || requestedGroupKey || normalizedGroupKey
    );
    const excludedIds = [...seenSimilarIds];
    const excludedPlaceholders = excludedIds.map(() => '?').join(', ');
    const baseParams = [product.brand, resolvedGroupValue, ...excludedIds];

    if (normalizedModel) {
      const exactRows = db
        .prepare(
          `
            SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
            FROM products
            WHERE is_active = 1
              AND brand = ?
              AND category_group = ?
              AND id NOT IN (${excludedPlaceholders})
              AND LOWER(TRIM(COALESCE(model, ''))) = ?
            ORDER BY id DESC
            LIMIT ?
          `
        )
        .all(...baseParams, normalizedModel, SIMILAR_LIMIT - similarRows.length);
      appendUniqueSimilarRows(exactRows);
    }

    if (similarRows.length >= SIMILAR_LIMIT) {
      return;
    }

    const fallbackExcludedIds = [...seenSimilarIds];
    const fallbackExcludedPlaceholders = fallbackExcludedIds.map(() => '?').join(', ');
    const fallbackBaseParams = [product.brand, resolvedGroupValue, ...fallbackExcludedIds];
    const fallbackRows = normalizedModel
      ? db
          .prepare(
            `
              SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
              FROM products
              WHERE is_active = 1
                AND brand = ?
                AND category_group = ?
                AND id NOT IN (${fallbackExcludedPlaceholders})
                AND LOWER(TRIM(COALESCE(model, ''))) != ?
              ORDER BY RANDOM()
              LIMIT ?
            `
          )
          .all(...fallbackBaseParams, normalizedModel, SIMILAR_LIMIT - similarRows.length)
      : db
          .prepare(
            `
              SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
              FROM products
              WHERE is_active = 1
                AND brand = ?
                AND category_group = ?
                AND id NOT IN (${fallbackExcludedPlaceholders})
              ORDER BY RANDOM()
              LIMIT ?
            `
          )
          .all(...fallbackBaseParams, SIMILAR_LIMIT - similarRows.length);
    appendUniqueSimilarRows(fallbackRows);
  };

  const resolveConfiguredGroupKey = (matcher, fallbackKey = '') => {
    const matchedGroup = productGroupConfigs.find((groupConfig) => {
      if (!groupConfig || typeof groupConfig !== 'object') {
        return false;
      }
      return matcher(groupConfig);
    });
    const resolved = String(matchedGroup?.key || fallbackKey || '').trim();
    return normalizeProductGroupKey(resolved) ? resolved : '';
  };
  const isLocalUsedGroup = (groupConfig = null) => {
    const safeGroup = groupConfig && typeof groupConfig === 'object' ? groupConfig : {};
    const normalizedValues = [safeGroup.key, safeGroup.labelKo, safeGroup.labelEn]
      .map((value) => normalizeProductGroupMatchKey(value))
      .filter(Boolean);
    return normalizedValues.some((value) => value === '현지중고' || value === 'localused' || value === 'used');
  };
  const prioritizedGroupKeys = [];
  const prioritizedGroupKeySet = new Set();
  const appendPrioritizedGroup = (groupKey = '') => {
    const resolvedGroupKey = String(groupKey || '').trim();
    const normalizedGroupKey = normalizeProductGroupKey(resolvedGroupKey);
    if (!normalizedGroupKey || prioritizedGroupKeySet.has(normalizedGroupKey)) {
      return;
    }
    prioritizedGroupKeySet.add(normalizedGroupKey);
    prioritizedGroupKeys.push(resolvedGroupKey);
  };

  appendPrioritizedGroup(product.category_group || '');
  appendPrioritizedGroup(resolveConfiguredGroupKey((groupConfig) => isDomesticStockGroup(groupConfig), '국내재고'));
  appendPrioritizedGroup(resolveConfiguredGroupKey((groupConfig) => isFactoryTemplateGroup(groupConfig), '공장제'));
  appendPrioritizedGroup(resolveConfiguredGroupKey((groupConfig) => isLocalUsedGroup(groupConfig), '현지중고'));
  prioritizedGroupKeys.forEach((groupKey) => {
    if (similarRows.length >= SIMILAR_LIMIT) {
      return;
    }
    appendSimilarRowsFromGroup(groupKey);
  });

  const badgeMap = getProductBadgeMapByProductIds([product.id, ...similarRows.map((row) => row.id)]);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const productWithBadges = {
    ...product,
    product_badges: badgeMap.get(Number(product.id)) || []
  };
  const similar = similarRows.map((row) => ({
    ...decorateProductForView(row, productGroupMap.get(row.category_group)),
    category_group_label: groupLabelMap[row.category_group] || row.category_group,
    product_badges: badgeMap.get(Number(row.id)) || []
  }));
  const productDisplay = buildProductDisplayData(productWithBadges, productGroupConfig);

  res.render('product-detail', {
    title: 'Product',
    product: productWithBadges,
    productDisplay,
    productGroupConfig,
    isFactoryLikeProduct: isFactoryLikeGroup(productGroupConfig),
    productGroupLabel: groupLabelMap[product.category_group] || product.category_group,
    similar,
    imageList
  });
});

app.get('/shop/item/:id/purchase', requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const isEn = res.locals.ctx.lang === 'en';
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).render('simple-error', { title: 'Error', message: isEn ? 'Invalid product.' : '잘못된 상품입니다.' });
  }

  const product = db
    .prepare(
      `
        SELECT
          id,
          category_group,
          brand,
          model,
          sub_model,
          reference,
          factory_name,
          price,
          shipping_period,
          is_sold_out
        FROM products
        WHERE id = ? AND is_active = 1
        LIMIT 1
      `
    )
    .get(id);

  if (!product) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: isEn ? 'Product not found.' : '상품을 찾을 수 없습니다.' });
  }

  if (Number(product.is_sold_out || 0) === 1) {
    return res.status(409).render('simple-error', {
      title: isEn ? 'Sold Out' : '판매완료',
      message: isEn ? 'This item is sold out and cannot be purchased.' : '판매완료 상품은 구매할 수 없습니다.'
    });
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const productGroupLabel = productGroupLabelMap[product.category_group] || product.category_group;

  incrementFunnelEventUniqueInSession(req, FUNNEL_EVENT.PURCHASE_VIEW, `product:${id}`);
  const addressBookEntries = getAddressBookEntries(req.user.id);
  const defaultAddressEntry = addressBookEntries.find((entry) => entry.isDefault) || null;
  const defaultPostcode = req.user.defaultPostcode || defaultAddressEntry?.postcode || '';
  const defaultAddressBase = req.user.defaultAddressBase || defaultAddressEntry?.addressBase || '';
  const defaultAddressDetail = req.user.defaultAddressDetail || defaultAddressEntry?.addressDetail || '';

  const formData = {
    buyerName: req.user.fullName || '',
    buyerContact: req.user.phone || '',
    customsClearanceNo: req.user.customsClearanceNo || '',
    buyerPostcode: defaultPostcode,
    buyerAddressBase: defaultAddressBase,
    buyerAddressDetail: defaultAddressDetail,
    addressBookId: defaultAddressEntry ? String(defaultAddressEntry.id) : '',
    addressLabel: '',
    saveToAddressBook: '',
    setAsDefaultAddress: '',
    quantity: 1,
    useRewardPoints: '0'
  };
  const memberPointProfile = getMemberPointProfile(req.user.id, res.locals.ctx.lang);
  const purchasePointRate = memberPointProfile.pointRate;
  const availableRewardPoints = parseNonNegativeInt(req.user.rewardPoints, 0);
  const pointUseUnit = 1000;
  const defaultUseRewardPoints = Math.max(
    0,
    Math.floor(Math.min(availableRewardPoints, Number(product.price || 0)) / pointUseUnit) * pointUseUnit
  );
  formData.useRewardPoints = String(defaultUseRewardPoints);
  return res.render('purchase-form', {
    title: 'Purchase',
    product,
    formData,
    addressBookEntries,
    productGroupLabel,
    availableRewardPoints,
    purchasePointRate,
    memberLevelName: memberPointProfile.levelDisplayName,
    expectedPoints: calculateEarnedPoints(product.price, purchasePointRate)
  });
});

app.post('/shop/item/:id/purchase', requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const isEn = res.locals.ctx.lang === 'en';
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).render('simple-error', { title: 'Error', message: isEn ? 'Invalid product.' : '잘못된 상품입니다.' });
  }

  const product = db
    .prepare(
      `
        SELECT
          id,
          category_group,
          brand,
          model,
          sub_model,
          reference,
          factory_name,
          price,
          shipping_period,
          is_sold_out
        FROM products
        WHERE id = ? AND is_active = 1
        LIMIT 1
      `
    )
    .get(id);

  if (!product) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: isEn ? 'Product not found.' : '상품을 찾을 수 없습니다.' });
  }

  if (Number(product.is_sold_out || 0) === 1) {
    return res.status(409).render('simple-error', {
      title: isEn ? 'Sold Out' : '판매완료',
      message: isEn ? 'This item is sold out and cannot be purchased.' : '판매완료 상품은 구매할 수 없습니다.'
    });
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const productGroupLabel = productGroupLabelMap[product.category_group] || product.category_group;

  const buyerName = String(req.body.buyerName || '').trim();
  const buyerContact = String(req.body.buyerContact || '').trim();
  const addressBookIdRaw = String(req.body.addressBookId || '').trim();
  const selectedAddressBookId = parsePositiveInt(addressBookIdRaw, 0);
  let buyerPostcode = normalizePostcode(req.body.buyerPostcode || '');
  let buyerAddressBase = normalizeAddressText(req.body.buyerAddressBase || '', 160);
  let buyerAddressDetail = normalizeAddressText(req.body.buyerAddressDetail || '', 120);
  const customsClearanceNo = String(req.body.customsClearanceNo || '').trim();
  const addressLabel = normalizeAddressText(req.body.addressLabel || '', 40);
  const saveToAddressBook = String(req.body.saveToAddressBook || '') === '1';
  const setAsDefaultAddress = String(req.body.setAsDefaultAddress || '') === '1';
  const quantity = parsePositiveInt(req.body.quantity, 1);
  const pointUseUnit = 1000;
  const normalizePointUseUnit = (value) =>
    Math.max(0, Math.floor(parseNonNegativeInt(value, 0) / pointUseUnit) * pointUseUnit);
  const useRewardPointsRequested = normalizePointUseUnit(
    String(req.body.useRewardPoints ?? '')
      .replace(/[^0-9]/g, '')
  );
  const addressBookEntries = getAddressBookEntries(req.user.id);
  const selectedAddressBookEntry =
    selectedAddressBookId > 0
      ? addressBookEntries.find((entry) => entry.id === selectedAddressBookId) || null
      : null;
  const getCurrentRewardPoints = () => {
    const row = db
      .prepare('SELECT reward_points FROM users WHERE id = ? AND is_admin = 0 LIMIT 1')
      .get(req.user.id);
    return parseNonNegativeInt(row?.reward_points, parseNonNegativeInt(req.user.rewardPoints, 0));
  };
  let availableRewardPoints = getCurrentRewardPoints();

  if ((!buyerPostcode || !buyerAddressBase) && selectedAddressBookEntry) {
    buyerPostcode = selectedAddressBookEntry.postcode;
    buyerAddressBase = selectedAddressBookEntry.addressBase;
    buyerAddressDetail = selectedAddressBookEntry.addressDetail;
  }

  const validatedAddress = validateStructuredAddress({
    postcode: buyerPostcode,
    addressBase: buyerAddressBase,
    addressDetail: buyerAddressDetail
  });
  const buyerAddress = validatedAddress.ok
    ? composeFullAddress(validatedAddress.postcode, validatedAddress.addressBase, validatedAddress.addressDetail)
    : '';

  const formData = {
    buyerName,
    buyerContact,
    customsClearanceNo,
    buyerPostcode,
    buyerAddressBase,
    buyerAddressDetail,
    addressBookId: selectedAddressBookId > 0 ? String(selectedAddressBookId) : '',
    addressLabel,
    saveToAddressBook: saveToAddressBook ? '1' : '',
    setAsDefaultAddress: setAsDefaultAddress ? '1' : '',
    quantity,
    useRewardPoints: String(useRewardPointsRequested)
  };
  const memberPointProfile = getMemberPointProfile(req.user.id, res.locals.ctx.lang);
  const purchasePointRate = memberPointProfile.pointRate;
  const orderSubtotalPreview = Math.max(0, Math.round(Number(product.price || 0) * quantity));
  const maxUsableRewardPointsPreview = normalizePointUseUnit(
    Math.max(0, Math.min(availableRewardPoints, orderSubtotalPreview))
  );
  const appliedRewardPointsPreview = Math.min(useRewardPointsRequested, maxUsableRewardPointsPreview);
  const payableAmountPreview = Math.max(0, orderSubtotalPreview - appliedRewardPointsPreview);

  const renderWithError = (message) => {
    res.locals.ctx.popupFlash = { type: 'error', message };
    return res.render('purchase-form', {
      title: 'Purchase',
      product,
      formData,
      addressBookEntries,
      productGroupLabel,
      availableRewardPoints,
      purchasePointRate,
      memberLevelName: memberPointProfile.levelDisplayName,
      expectedPoints: calculateEarnedPoints(payableAmountPreview, purchasePointRate)
    });
  };

  if (!buyerName || !buyerContact || !customsClearanceNo) {
    return renderWithError('필수 입력값을 모두 작성해 주세요.');
  }

  const normalizedContact = normalizePhone(buyerContact);
  if (!DIGIT_PHONE_REGEX.test(normalizedContact) || normalizedContact.length < 8) {
    return renderWithError('연락처 형식이 올바르지 않습니다.');
  }

  if (!validatedAddress.ok) {
    return renderWithError(validatedAddress.message || '주소를 확인해 주세요.');
  }

  if (buyerAddress.length < 5 || buyerAddress.length > 220) {
    return renderWithError('주소는 5~220자 범위로 입력해 주세요.');
  }

  if (!CUSTOMS_NO_REGEX.test(customsClearanceNo)) {
    return renderWithError('통관번호 형식이 올바르지 않습니다. (영문/숫자 6~30자)');
  }

  if (useRewardPointsRequested > maxUsableRewardPointsPreview) {
    if (isEn) {
      return renderWithError(
        `You can use up to ${maxUsableRewardPointsPreview.toLocaleString('ko-KR')} points for this order.`
      );
    }
    return renderWithError(
      `이번 주문에 사용할 수 있는 최대 포인트는 ${maxUsableRewardPointsPreview.toLocaleString('ko-KR')}P 입니다.`
    );
  }

  let orderNo = generateOrderNo();
  while (db.prepare('SELECT id FROM orders WHERE order_no = ? LIMIT 1').get(orderNo)) {
    orderNo = generateOrderNo();
  }

  const appliedRewardPoints = appliedRewardPointsPreview;
  const orderSubtotal = orderSubtotalPreview;
  const totalPrice = Math.max(0, orderSubtotal - appliedRewardPoints);
  const salesSnapshot = buildOrderSalesSnapshot({
    product,
    quantity,
    totalPrice,
    baseDate: toKstDate()
  });
  const createdOrderResult = db.transaction(() => {
    if (appliedRewardPoints > 0) {
      const deducted = db
        .prepare(
          `
            UPDATE users
            SET reward_points = reward_points - ?
            WHERE id = ? AND is_admin = 0 AND reward_points >= ?
          `
        )
        .run(appliedRewardPoints, req.user.id, appliedRewardPoints);
      if (deducted.changes === 0) {
        return {
          ok: false,
          message: isEn
            ? 'Your available points changed. Please check points and try again.'
            : '보유 포인트가 변경되었습니다. 사용 포인트를 확인 후 다시 시도해 주세요.'
        };
      }
    }

    const createdOrder = db.prepare(
      `
        INSERT INTO orders (
          order_no,
          product_id,
          buyer_name,
          buyer_contact,
          buyer_address,
          customs_clearance_no,
          bank_depositor_name,
          quantity,
          total_price,
          used_points,
          status,
          point_rate_snapshot,
          point_level_id,
          point_level_name,
          sales_tab_key,
          sales_scope_id,
          sales_scope_name,
          sales_scope_date,
          sales_exchange_rate_snapshot,
          sales_shipping_fee_krw_snapshot,
          sales_cost_rmb_snapshot,
          sales_cost_krw_snapshot,
          sales_margin_krw_snapshot,
          sales_real_margin_krw_snapshot,
          sales_synced_at,
          created_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      orderNo,
      product.id,
      buyerName,
      normalizedContact,
      buyerAddress,
      customsClearanceNo,
      buyerName,
      quantity,
      totalPrice,
      appliedRewardPoints,
      ORDER_STATUS.PENDING_REVIEW,
      purchasePointRate,
      memberPointProfile.levelId || '',
      memberPointProfile.levelName || '',
      salesSnapshot.sales_tab_key,
      salesSnapshot.sales_scope_id,
      salesSnapshot.sales_scope_name,
      salesSnapshot.sales_scope_date,
      salesSnapshot.sales_exchange_rate_snapshot,
      salesSnapshot.sales_shipping_fee_krw_snapshot,
      salesSnapshot.sales_cost_rmb_snapshot,
      salesSnapshot.sales_cost_krw_snapshot,
      salesSnapshot.sales_margin_krw_snapshot,
      salesSnapshot.sales_real_margin_krw_snapshot,
      salesSnapshot.sales_synced_at,
      req.user.id
    );
    return { ok: true, createdOrder };
  })();

  if (!createdOrderResult.ok) {
    availableRewardPoints = getCurrentRewardPoints();
    return renderWithError(createdOrderResult.message || '주문 처리에 실패했습니다. 다시 시도해 주세요.');
  }

  const createdOrder = createdOrderResult.createdOrder;

  appendOrderStatusLog(
    Number(createdOrder.lastInsertRowid),
    orderNo,
    null,
    ORDER_STATUS.PENDING_REVIEW,
    'order:member:created'
  );

  if (saveToAddressBook || setAsDefaultAddress) {
    const fallbackLabel = addressLabel || selectedAddressBookEntry?.label || '기본주소';
    const saveAddressTx = db.transaction(() => {
      const now = db.prepare("SELECT datetime('now') AS now").get().now;
      let targetAddressId = selectedAddressBookEntry?.id || 0;

      if (targetAddressId > 0) {
        db.prepare(
          `
            UPDATE address_book
            SET label = ?, postcode = ?, address_base = ?, address_detail = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
          `
        ).run(
          fallbackLabel,
          validatedAddress.postcode,
          validatedAddress.addressBase,
          validatedAddress.addressDetail,
          now,
          targetAddressId,
          req.user.id
        );
      } else {
        const inserted = db.prepare(
          `
            INSERT INTO address_book (
              user_id,
              label,
              postcode,
              address_base,
              address_detail,
              is_default,
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
          `
        ).run(
          req.user.id,
          fallbackLabel,
          validatedAddress.postcode,
          validatedAddress.addressBase,
          validatedAddress.addressDetail,
          now,
          now
        );
        targetAddressId = Number(inserted.lastInsertRowid);
      }

      const currentDefault = db
        .prepare('SELECT id FROM address_book WHERE user_id = ? AND is_default = 1 LIMIT 1')
        .get(req.user.id);
      const shouldSetDefault = setAsDefaultAddress || !currentDefault;
      if (shouldSetDefault && targetAddressId > 0) {
        db.prepare('UPDATE address_book SET is_default = 0 WHERE user_id = ?').run(req.user.id);
        db.prepare('UPDATE address_book SET is_default = 1, updated_at = ? WHERE id = ? AND user_id = ?').run(
          now,
          targetAddressId,
          req.user.id
        );
        updateUserDefaultAddress(
          req.user.id,
          validatedAddress.postcode,
          validatedAddress.addressBase,
          validatedAddress.addressDetail
        );
      }
    });
    saveAddressTx();
  }

  incrementFunnelEvent(toKstDate(), FUNNEL_EVENT.ORDER_CREATED);
  rememberOrderCompleteAccess(req, orderNo);
  return res.redirect(`/shop/order-complete/${orderNo}`);
});

app.post('/order/create', requireAuth, (req, res) => {
  const productId = Number(req.body.productId);
  const buyerName = String(req.body.buyerName || '').trim();
  const buyerContact = String(req.body.buyerContact || '').trim();
  const buyerAddress = String(req.body.buyerAddress || '').trim();
  const bankDepositorName = String(req.body.bankDepositorName || '').trim();
  const customsClearanceNo = String(req.body.customsClearanceNo || '').trim();
  const quantity = parsePositiveInt(req.body.quantity, 1);
  const normalizedContact = normalizePhone(buyerContact);

  if (!productId || !buyerName || !buyerContact || !buyerAddress || !bankDepositorName) {
    setFlash(req, 'error', '필수 입력값을 모두 작성해 주세요.');
    return res.redirect(`/shop/item/${productId || ''}`);
  }

  if (!DIGIT_PHONE_REGEX.test(normalizedContact) || normalizedContact.length < 8) {
    setFlash(req, 'error', '연락처 형식이 올바르지 않습니다.');
    return res.redirect(`/shop/item/${productId || ''}`);
  }

  if (buyerAddress.length < 5 || buyerAddress.length > 200) {
    setFlash(req, 'error', '주소는 5~200자 범위로 입력해 주세요.');
    return res.redirect(`/shop/item/${productId || ''}`);
  }

  if (customsClearanceNo && !CUSTOMS_NO_REGEX.test(customsClearanceNo)) {
    setFlash(req, 'error', '통관번호 형식이 올바르지 않습니다. (영문/숫자 6~30자)');
    return res.redirect(`/shop/item/${productId || ''}`);
  }

  const product = db
    .prepare(
      `
        SELECT
          id,
          category_group,
          brand,
          model,
          sub_model,
          reference,
          factory_name,
          price
        FROM products
        WHERE id = ? AND is_active = 1
        LIMIT 1
      `
    )
    .get(productId);
  if (!product) {
    setFlash(req, 'error', '유효하지 않은 상품입니다.');
    return res.redirect('/shop');
  }

  let orderNo = generateOrderNo();
  while (db.prepare('SELECT id FROM orders WHERE order_no = ? LIMIT 1').get(orderNo)) {
    orderNo = generateOrderNo();
  }

  const totalPrice = Number(product.price) * quantity;
  const salesSnapshot = buildOrderSalesSnapshot({
    product,
    quantity,
    totalPrice,
    baseDate: toKstDate()
  });
  const memberPointProfile = req.user ? getMemberPointProfile(req.user.id, res.locals.ctx.lang) : null;
  const pointRateSnapshot = memberPointProfile ? memberPointProfile.pointRate : 0;
  const pointLevelIdSnapshot = memberPointProfile?.levelId || '';
  const pointLevelNameSnapshot = memberPointProfile?.levelNameKo || memberPointProfile?.levelName || '';

  const createdOrder = db.prepare(
    `
      INSERT INTO orders (
        order_no,
        product_id,
        buyer_name,
        buyer_contact,
        buyer_address,
        customs_clearance_no,
        bank_depositor_name,
        quantity,
        total_price,
        status,
        point_rate_snapshot,
        point_level_id,
        point_level_name,
        sales_tab_key,
        sales_scope_id,
        sales_scope_name,
        sales_scope_date,
        sales_exchange_rate_snapshot,
        sales_shipping_fee_krw_snapshot,
        sales_cost_rmb_snapshot,
        sales_cost_krw_snapshot,
        sales_margin_krw_snapshot,
        sales_real_margin_krw_snapshot,
        sales_synced_at,
        created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(
    orderNo,
    productId,
    buyerName,
    normalizedContact,
    buyerAddress,
    customsClearanceNo,
    bankDepositorName,
    quantity,
    totalPrice,
    ORDER_STATUS.PENDING_REVIEW,
    pointRateSnapshot,
    pointLevelIdSnapshot,
    pointLevelNameSnapshot,
    salesSnapshot.sales_tab_key,
    salesSnapshot.sales_scope_id,
    salesSnapshot.sales_scope_name,
    salesSnapshot.sales_scope_date,
    salesSnapshot.sales_exchange_rate_snapshot,
    salesSnapshot.sales_shipping_fee_krw_snapshot,
    salesSnapshot.sales_cost_rmb_snapshot,
    salesSnapshot.sales_cost_krw_snapshot,
    salesSnapshot.sales_margin_krw_snapshot,
    salesSnapshot.sales_real_margin_krw_snapshot,
    salesSnapshot.sales_synced_at,
    req.user ? req.user.id : null
  );

  appendOrderStatusLog(
    Number(createdOrder.lastInsertRowid),
    orderNo,
    null,
    ORDER_STATUS.PENDING_REVIEW,
    req.user ? 'order:member:created' : 'order:guest:created'
  );

  incrementFunnelEvent(toKstDate(), FUNNEL_EVENT.ORDER_CREATED);
  rememberOrderCompleteAccess(req, orderNo);
  res.redirect(`/shop/order-complete/${orderNo}`);
});

app.get('/shop/order-complete/:orderNo', (req, res) => {
  const orderNo = String(req.params.orderNo || '').trim();
  const order = db
    .prepare(
      `
        SELECT o.*, p.brand, p.model, p.sub_model
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.order_no = ?
        LIMIT 1
      `
    )
    .get(orderNo);

  if (!order) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '주문을 찾을 수 없습니다.' });
  }

  const orderOwnerUserId = Number(order.created_by_user_id || 0);
  const viewerUserId = Number(req.user?.id || 0);
  const canViewByRole = Boolean(req.user?.isAdmin) || (orderOwnerUserId > 0 && viewerUserId === orderOwnerUserId);
  const canViewGuestOrder = orderOwnerUserId <= 0 && hasRecentOrderCompleteAccess(req, orderNo);
  if (!canViewByRole && !canViewGuestOrder) {
    return res.status(403).render('simple-error', {
      title: 'Forbidden',
      message: '주문 정보 접근 권한이 없습니다.'
    });
  }

  const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang, 'member');
  const isMemberOrder = Number(order.created_by_user_id || 0) > 0;
  const purchasePointRate = isMemberOrder
    ? parsePointRate(order.point_rate_snapshot, getLegacyPurchasePointRateSetting())
    : 0;
  const usedPointsApplied = parseNonNegativeInt(order.used_points, 0);
  const expectedPoints = isMemberOrder ? calculateEarnedPoints(order.total_price, purchasePointRate) : 0;
  const awardedPoints = parseNonNegativeInt(order.awarded_points, 0);
  const orderSubtotalAmount = parseNonNegativeInt(order.total_price, 0) + usedPointsApplied;

  res.render('order-complete', {
    title: 'Order Complete',
    order,
    statusMeta,
    isMemberOrder,
    purchasePointRate,
    memberLevelName: String(order.point_level_name || '').trim(),
    usedPointsApplied,
    orderSubtotalAmount,
    expectedPoints,
    awardedPoints
  });
});

app.get('/mypage', requireAuth, (req, res) => {
  const productGroupConfigs = getProductGroupConfigs();
  const productGroupKeys = productGroupConfigs.map((group) => group.key);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const myPageOptions = parseMyPageQuery(req.query || {}, productGroupKeys);

  const profile = db
    .prepare(
      `
        SELECT
          id,
          username,
          email,
          nickname,
          full_name,
          phone,
          customs_clearance_no,
          default_address,
          default_postcode,
          default_address_base,
          default_address_detail,
          profile_image_path,
          reward_points
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(req.user.id);

  if (!profile) {
    clearScopedSessionAuthState(req, 'member');
    clearPersistAuthCookie(res, { scope: 'member' });
    setFlash(req, 'error', '사용자 정보를 확인할 수 없어 다시 로그인해 주세요.');
    return res.redirect('/login');
  }

  const summaryRow = db
    .prepare(
      `
        SELECT
          COALESCE(SUM(CASE WHEN status != ? THEN total_price ELSE 0 END), 0) AS total_purchase_amount,
          COALESCE(SUM(awarded_points), 0) AS total_earned_points
        FROM orders
        WHERE created_by_user_id = ?
      `
    )
    .get(ORDER_STATUS.PENDING_REVIEW, req.user.id);

  const usedPoints = 0;
  const availablePoints = Number(profile.reward_points || 0);
  const addressBookEntries = getAddressBookEntries(req.user.id);
  const myPageSummary = {
    totalPurchaseAmount: Number(summaryRow?.total_purchase_amount || 0),
    totalEarnedPoints: Number(summaryRow?.total_earned_points || 0),
    usedPoints,
    availablePoints,
    cartCount: getMemberCartCount(req.user.id)
  };
  const memberPointProfile = getMemberPointProfile(req.user.id, res.locals.ctx.lang);
  const memberLevelInfo = {
    name: memberPointProfile.levelDisplayName || (res.locals.ctx.lang === 'en' ? 'Unassigned' : '미지정'),
    colorTheme: memberPointProfile.levelColorTheme || PRODUCT_BADGE_DEFAULT_COLOR_THEME,
    pointRate: parsePointRate(memberPointProfile.pointRate, 0)
  };

  const ordersQuery = [
    'SELECT o.*, p.category_group, p.brand, p.model, p.sub_model,',
    '       r.id AS review_id, r.title AS review_title, r.created_at AS review_created_at',
    'FROM orders o',
    'JOIN products p ON p.id = o.product_id',
    'LEFT JOIN reviews r ON r.order_id = o.id AND r.user_id = o.created_by_user_id',
    'WHERE o.created_by_user_id = ?'
  ];
  const orderParams = [req.user.id];

  if (myPageOptions.orderGroupFilter !== 'all') {
    ordersQuery.push('AND p.category_group = ?');
    orderParams.push(myPageOptions.orderGroupFilter);
  }
  if (myPageOptions.orderDateFrom) {
    ordersQuery.push("AND date(datetime(o.created_at, '+9 hours')) >= ?");
    orderParams.push(myPageOptions.orderDateFrom);
  }
  if (myPageOptions.orderDateTo) {
    ordersQuery.push("AND date(datetime(o.created_at, '+9 hours')) <= ?");
    orderParams.push(myPageOptions.orderDateTo);
  }
  ordersQuery.push('ORDER BY o.id DESC');

  const baseOrders = db.prepare(ordersQuery.join('\n')).all(...orderParams);

  const latestLogMap = new Map();
  if (baseOrders.length > 0) {
    const orderIds = baseOrders.map((item) => Number(item.id)).filter((id) => Number.isInteger(id) && id > 0);
    if (orderIds.length > 0) {
      const placeholders = orderIds.map(() => '?').join(', ');
      const latestRows = db
        .prepare(
          `
            SELECT l.order_id, l.event_note, l.created_at
            FROM order_status_logs l
            JOIN (
              SELECT order_id, MAX(id) AS max_id
              FROM order_status_logs
              WHERE order_id IN (${placeholders})
              GROUP BY order_id
            ) latest ON latest.max_id = l.id
          `
        )
        .all(...orderIds);

      for (const row of latestRows) {
        latestLogMap.set(Number(row.order_id), {
          event_note: String(row.event_note || ''),
          created_at: String(row.created_at || '')
        });
      }
    }
  }

  const orders = baseOrders.map((order) => {
    const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang, 'member');
    const latestLog = latestLogMap.get(Number(order.id)) || { event_note: '', created_at: '' };
    const hasReview = Number(order.review_id || 0) > 0;
    const canWriteReview =
      normalizeOrderStatus(order.status) === ORDER_STATUS.DELIVERED && !hasReview;
    return {
      ...order,
      status_code: statusMeta.code,
      status_label: statusMeta.label,
      status_detail: statusMeta.detail,
      category_group_label: groupLabelMap[order.category_group] || order.category_group,
      tracking_carrier_label: getTrackingCarrierLabel(order.tracking_carrier),
      latest_event_note: latestLog.event_note,
      latest_event_at: latestLog.created_at,
      review_id: hasReview ? Number(order.review_id) : 0,
      review_title: String(order.review_title || ''),
      review_created_at: String(order.review_created_at || ''),
      has_review: hasReview,
      can_write_review: canWriteReview
    };
  });

  res.render('mypage', {
    title: 'My Page',
    orders,
    myPageSection: myPageOptions.section,
    myPageProfileTab: myPageOptions.profileTab,
    myPageFilters: {
      group: myPageOptions.orderGroupFilter,
      dateFrom: myPageOptions.orderDateFrom,
      dateTo: myPageOptions.orderDateTo
    },
    myPageGroups: productGroupConfigs.map((group) => ({
      key: group.key,
      label: groupLabelMap[group.key] || group.key
    })),
    myPageSummary,
    profileForm: {
      username: profile.username || req.user.username,
      email: profile.email || req.user.email || '',
      nickname: profile.nickname || profile.username || req.user.nickname || req.user.username,
      fullName: profile.full_name || '',
      phone: profile.phone || '',
      customsClearanceNo: profile.customs_clearance_no || '',
      defaultAddress: profile.default_address || '',
      defaultPostcode: profile.default_postcode || '',
      defaultAddressBase: profile.default_address_base || '',
      defaultAddressDetail: profile.default_address_detail || '',
      rewardPoints: Number(profile.reward_points || 0),
      profileImagePath: profile.profile_image_path || ''
    },
    memberLevelInfo,
    addressBookEntries
  });
});

app.post('/mypage/profile/update', requireAuth, (req, res) => {
  const backPath = buildMyPageProfilePath(req.query.profileTab || req.body.profileTab || 'basic');
  const fullName = String(req.body.fullName || '').trim();
  const phone = normalizePhone(req.body.phone || '');
  const customsClearanceNo = String(req.body.customsClearanceNo || '').trim();
  const defaultPostcode = normalizePostcode(req.body.defaultPostcode || '');
  const defaultAddressBase = normalizeAddressText(req.body.defaultAddressBase || '', 160);
  const defaultAddressDetail = normalizeAddressText(req.body.defaultAddressDetail || '', 120);
  const addressValidation = validateStructuredAddress(
    {
      postcode: defaultPostcode,
      addressBase: defaultAddressBase,
      addressDetail: defaultAddressDetail
    },
    { requireFilled: false }
  );

  if (fullName.length > 80) {
    setFlash(req, 'error', '이름은 80자 이하로 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (phone && !PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '연락처 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (customsClearanceNo && !CUSTOMS_NO_REGEX.test(customsClearanceNo)) {
    setFlash(req, 'error', '통관번호 형식이 올바르지 않습니다. (영문/숫자/하이픈 6~30자)');
    return res.redirect(backPath);
  }

  if (!addressValidation.ok) {
    setFlash(req, 'error', addressValidation.message || '주소를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const safePostcode = addressValidation.postcode || '';
  const safeAddressBase = addressValidation.addressBase || '';
  const safeAddressDetail = addressValidation.addressDetail || '';
  const defaultAddress = composeFullAddress(safePostcode, safeAddressBase, safeAddressDetail);

  db.prepare(
    `
      UPDATE users
      SET full_name = ?,
          phone = ?,
          customs_clearance_no = ?,
          default_address = ?,
          default_postcode = ?,
          default_address_base = ?,
          default_address_detail = ?
      WHERE id = ?
    `
  ).run(
    fullName,
    phone,
    customsClearanceNo,
    defaultAddress,
    safePostcode,
    safeAddressBase,
    safeAddressDetail,
    req.user.id
  );

  if (!addressValidation.empty) {
    const syncDefaultAddressTx = db.transaction(() => {
      const now = db.prepare("SELECT datetime('now') AS now").get().now;
      const existingDefault = db
        .prepare(
          `
            SELECT id, label
            FROM address_book
            WHERE user_id = ? AND is_default = 1
            ORDER BY id DESC
            LIMIT 1
          `
        )
        .get(req.user.id);
      const baseLabel = String(existingDefault?.label || '').trim() || '기본주소';
      let targetId = Number(existingDefault?.id || 0);

      if (targetId > 0) {
        db.prepare(
          `
            UPDATE address_book
            SET label = ?, postcode = ?, address_base = ?, address_detail = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
          `
        ).run(baseLabel, safePostcode, safeAddressBase, safeAddressDetail, now, targetId, req.user.id);
      } else {
        const inserted = db.prepare(
          `
            INSERT INTO address_book (
              user_id,
              label,
              postcode,
              address_base,
              address_detail,
              is_default,
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
          `
        ).run(req.user.id, baseLabel, safePostcode, safeAddressBase, safeAddressDetail, now, now);
        targetId = Number(inserted.lastInsertRowid || 0);
      }

      if (targetId > 0) {
        db.prepare('UPDATE address_book SET is_default = 0 WHERE user_id = ? AND id != ?').run(req.user.id, targetId);
      }
    });
    syncDefaultAddressTx();
  }

  setFlash(req, 'success', '정보 설정이 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/mypage/address-book/add', requireAuth, (req, res) => {
  const backPath = buildMyPageProfilePath(req.query.profileTab || req.body.profileTab || 'addressbook');
  const label = normalizeAddressText(req.body.label || '', 40);
  const postcode = normalizePostcode(req.body.postcode || '');
  const addressBase = normalizeAddressText(req.body.addressBase || '', 160);
  const addressDetail = normalizeAddressText(req.body.addressDetail || '', 120);
  const setAsDefault = String(req.body.setAsDefault || '') === '1';

  if (!label) {
    setFlash(req, 'error', '주소록 이름을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const addressValidation = validateStructuredAddress({
    postcode,
    addressBase,
    addressDetail
  });
  if (!addressValidation.ok) {
    setFlash(req, 'error', addressValidation.message || '주소를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const createAddressTx = db.transaction(() => {
    const now = db.prepare("SELECT datetime('now') AS now").get().now;
    const inserted = db.prepare(
      `
        INSERT INTO address_book (
          user_id,
          label,
          postcode,
          address_base,
          address_detail,
          is_default,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
      `
    ).run(
      req.user.id,
      label,
      addressValidation.postcode,
      addressValidation.addressBase,
      addressValidation.addressDetail,
      now,
      now
    );

    const newAddressId = Number(inserted.lastInsertRowid || 0);
    const existingDefault = db
      .prepare('SELECT id FROM address_book WHERE user_id = ? AND is_default = 1 LIMIT 1')
      .get(req.user.id);
    const shouldSetDefault = setAsDefault || !existingDefault;

    if (shouldSetDefault && newAddressId > 0) {
      db.prepare('UPDATE address_book SET is_default = 0 WHERE user_id = ?').run(req.user.id);
      db.prepare('UPDATE address_book SET is_default = 1, updated_at = ? WHERE id = ? AND user_id = ?').run(
        now,
        newAddressId,
        req.user.id
      );
      updateUserDefaultAddress(
        req.user.id,
        addressValidation.postcode,
        addressValidation.addressBase,
        addressValidation.addressDetail
      );
    }
  });
  createAddressTx();

  setFlash(req, 'success', '주소록이 추가되었습니다.');
  return res.redirect(backPath);
});

app.post('/mypage/address-book/:id/default', requireAuth, (req, res) => {
  const backPath = buildMyPageProfilePath(req.query.profileTab || req.body.profileTab || 'addressbook');
  const addressId = Number(req.params.id);
  if (!Number.isInteger(addressId) || addressId <= 0) {
    setFlash(req, 'error', '유효하지 않은 주소록 항목입니다.');
    return res.redirect(backPath);
  }

  const targetAddress = db
    .prepare(
      `
        SELECT id, postcode, address_base, address_detail
        FROM address_book
        WHERE id = ? AND user_id = ?
        LIMIT 1
      `
    )
    .get(addressId, req.user.id);
  if (!targetAddress) {
    setFlash(req, 'error', '주소록 항목을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const setDefaultTx = db.transaction(() => {
    const now = db.prepare("SELECT datetime('now') AS now").get().now;
    db.prepare('UPDATE address_book SET is_default = 0 WHERE user_id = ?').run(req.user.id);
    db.prepare('UPDATE address_book SET is_default = 1, updated_at = ? WHERE id = ? AND user_id = ?').run(
      now,
      addressId,
      req.user.id
    );
    updateUserDefaultAddress(
      req.user.id,
      targetAddress.postcode,
      targetAddress.address_base,
      targetAddress.address_detail
    );
  });
  setDefaultTx();

  setFlash(req, 'success', '기본주소로 설정되었습니다.');
  return res.redirect(backPath);
});

app.post('/mypage/address-book/:id/delete', requireAuth, (req, res) => {
  const backPath = buildMyPageProfilePath(req.query.profileTab || req.body.profileTab || 'addressbook');
  const addressId = Number(req.params.id);
  if (!Number.isInteger(addressId) || addressId <= 0) {
    setFlash(req, 'error', '유효하지 않은 주소록 항목입니다.');
    return res.redirect(backPath);
  }

  const targetAddress = db
    .prepare(
      `
        SELECT id, is_default
        FROM address_book
        WHERE id = ? AND user_id = ?
        LIMIT 1
      `
    )
    .get(addressId, req.user.id);
  if (!targetAddress) {
    setFlash(req, 'error', '주소록 항목을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const deleteAddressTx = db.transaction(() => {
    db.prepare('DELETE FROM address_book WHERE id = ? AND user_id = ?').run(addressId, req.user.id);

    if (Number(targetAddress.is_default) !== 1) {
      return;
    }

    const nextAddress = db
      .prepare(
        `
          SELECT id, postcode, address_base, address_detail
          FROM address_book
          WHERE user_id = ?
          ORDER BY updated_at DESC, id DESC
          LIMIT 1
        `
      )
      .get(req.user.id);

    if (!nextAddress) {
      updateUserDefaultAddress(req.user.id, '', '', '');
      return;
    }

    const now = db.prepare("SELECT datetime('now') AS now").get().now;
    db.prepare('UPDATE address_book SET is_default = 0 WHERE user_id = ?').run(req.user.id);
    db.prepare('UPDATE address_book SET is_default = 1, updated_at = ? WHERE id = ? AND user_id = ?').run(
      now,
      nextAddress.id,
      req.user.id
    );
    updateUserDefaultAddress(req.user.id, nextAddress.postcode, nextAddress.address_base, nextAddress.address_detail);
  });
  deleteAddressTx();

  setFlash(req, 'success', '주소록 항목이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/mypage/profile/avatar', requireAuth, upload.single('profileImage'), requireAuthenticatedMultipartCsrf, (req, res) => {
  const backPath = '/mypage?section=info';

  if (!req.file) {
    setFlash(req, 'error', '프로필 이미지를 선택해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('UPDATE users SET profile_image_path = ? WHERE id = ?').run(fileUrl(req.file), req.user.id);
  setFlash(req, 'success', '프로필 이미지가 변경되었습니다.');
  return res.redirect(backPath);
});

app.post(
  '/mypage/profile/password',
  requireAuth,
  asyncRoute(async (req, res) => {
    const backPath = buildMyPageProfilePath(req.query.profileTab || req.body.profileTab || 'password');
    const currentPassword = String(req.body.currentPassword || '');
    const newPassword = String(req.body.newPassword || '');
    const newPasswordConfirm = String(req.body.newPasswordConfirm || '');

    if (!currentPassword || !newPassword || !newPasswordConfirm) {
      setFlash(req, 'error', '현재 비밀번호와 새 비밀번호를 모두 입력해 주세요.');
      return res.redirect(backPath);
    }

    if (newPassword !== newPasswordConfirm) {
      setFlash(req, 'error', '새 비밀번호 확인이 일치하지 않습니다.');
      return res.redirect(backPath);
    }

    if (!PASSWORD_REGEX.test(newPassword)) {
      setFlash(req, 'error', '새 비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
      return res.redirect(backPath);
    }

    const user = db.prepare('SELECT id, password_hash FROM users WHERE id = ? LIMIT 1').get(req.user.id);
    if (!user) {
      clearScopedSessionAuthState(req, 'member');
      clearPersistAuthCookie(res, { scope: 'member' });
      setFlash(req, 'error', '사용자 정보를 찾을 수 없습니다. 다시 로그인해 주세요.');
      return res.redirect('/login');
    }

    const validCurrent = await bcrypt.compare(currentPassword, user.password_hash);
    if (!validCurrent) {
      setFlash(req, 'error', '현재 비밀번호가 올바르지 않습니다.');
      return res.redirect(backPath);
    }

    const nextHash = await bcrypt.hash(newPassword, 10);
    db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(nextHash, req.user.id);

    setFlash(req, 'success', '비밀번호가 변경되었습니다.');
    return res.redirect(backPath);
  })
);

app.get('/notice', (req, res) => {
  const isAdminViewer = Boolean(req.user?.isAdmin);
  const noticeRows = isAdminViewer
    ? db
        .prepare(
          `
            SELECT id, title, image_path, is_popup, created_at, image_paths_json
            FROM notices
            ORDER BY id DESC
          `
        )
        .all()
    : db
        .prepare(
          `
            SELECT id, title, image_path, is_popup, created_at, image_paths_json
            FROM notices
            WHERE COALESCE(is_hidden, 0) = 0
            ORDER BY id DESC
          `
        )
        .all();
  const notices = withRecordImagePathsList(noticeRows);
  res.render('notice-list', { title: 'Notice', notices });
});

app.get('/notice/:id', (req, res) => {
  const id = Number(req.params.id);
  const noticeRow = db.prepare('SELECT * FROM notices WHERE id = ? LIMIT 1').get(id);
  if (!noticeRow || (Number(noticeRow.is_hidden || 0) === 1 && !req.user?.isAdmin)) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '공지사항이 없습니다.' });
  }
  const notice = withRecordImagePaths(noticeRow);
  res.render('notice-detail', { title: 'Notice Detail', notice });
});

app.get('/news', (req, res) => {
  const isAdminViewer = Boolean(req.user?.isAdmin);
  const newsRows = isAdminViewer
    ? db
        .prepare(
          `
            SELECT id, title, content, image_path, created_at, image_paths_json
            FROM news_posts
            ORDER BY id DESC
          `
        )
        .all()
    : db
        .prepare(
          `
            SELECT id, title, content, image_path, created_at, image_paths_json
            FROM news_posts
            WHERE COALESCE(is_hidden, 0) = 0
            ORDER BY id DESC
          `
        )
        .all();
  const newsPosts = withRecordImagePathsList(newsRows);

  res.render('news-list', { title: 'News', newsPosts });
});

app.get('/news/:id', (req, res) => {
  const id = Number(req.params.id);
  const newsPostRow = db.prepare('SELECT * FROM news_posts WHERE id = ? LIMIT 1').get(id);

  if (!newsPostRow || (Number(newsPostRow.is_hidden || 0) === 1 && !req.user?.isAdmin)) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '뉴스 게시글이 없습니다.' });
  }
  const newsPost = withRecordImagePaths(newsPostRow);

  const relatedNewsWhere = req.user?.isAdmin ? 'WHERE id != ?' : 'WHERE id != ? AND COALESCE(is_hidden, 0) = 0';
  const relatedNews = db
    .prepare(
      `
        SELECT id, title, created_at
        FROM news_posts
        ${relatedNewsWhere}
        ORDER BY id DESC
        LIMIT 5
      `
    )
    .all(newsPost.id);

  res.render('news-detail', { title: 'News Detail', newsPost, relatedNews });
});

app.get('/qc', (req, res) => {
  const orderNo = String(req.query.orderNo || '').trim();
  const isAdminViewer = Boolean(req.user?.isAdmin);
  let itemRows = [];
  if (orderNo) {
    itemRows = isAdminViewer
      ? db
          .prepare('SELECT * FROM qc_items WHERE order_no = ? ORDER BY id DESC')
          .all(orderNo)
      : db
          .prepare('SELECT * FROM qc_items WHERE order_no = ? AND COALESCE(is_hidden, 0) = 0 ORDER BY id DESC')
          .all(orderNo);
  } else {
    itemRows = isAdminViewer
      ? db.prepare('SELECT * FROM qc_items ORDER BY id DESC LIMIT 30').all()
      : db.prepare('SELECT * FROM qc_items WHERE COALESCE(is_hidden, 0) = 0 ORDER BY id DESC LIMIT 30').all();
  }
  const items = withRecordImagePathsList(itemRows);

  res.render('qc', { title: 'QC', orderNo, items });
});

function getReviewOrderForUser(orderId, userId) {
  const safeOrderId = Number(orderId || 0);
  const safeUserId = Number(userId || 0);
  if (!Number.isInteger(safeOrderId) || safeOrderId <= 0 || !Number.isInteger(safeUserId) || safeUserId <= 0) {
    return null;
  }

  return db
    .prepare(
      `
        SELECT
          o.id,
          o.order_no,
          o.product_id,
          o.status,
          o.created_by_user_id,
          p.brand,
          p.model,
          p.sub_model
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.id = ?
          AND o.created_by_user_id = ?
        LIMIT 1
      `
    )
    .get(safeOrderId, safeUserId);
}

function getExistingReviewByOrder(orderId, userId) {
  const safeOrderId = Number(orderId || 0);
  const safeUserId = Number(userId || 0);
  if (!Number.isInteger(safeOrderId) || safeOrderId <= 0 || !Number.isInteger(safeUserId) || safeUserId <= 0) {
    return null;
  }

  return db
    .prepare(
      `
        SELECT id
        FROM reviews
        WHERE order_id = ?
          AND user_id = ?
        LIMIT 1
      `
    )
    .get(safeOrderId, safeUserId);
}

function getOwnedReviewWithOrder(reviewId, userId) {
  const safeReviewId = Number(reviewId || 0);
  const safeUserId = Number(userId || 0);
  if (!Number.isInteger(safeReviewId) || safeReviewId <= 0 || !Number.isInteger(safeUserId) || safeUserId <= 0) {
    return null;
  }

  return db
    .prepare(
      `
        SELECT
          r.id,
          r.user_id,
          r.order_id AS linked_order_id,
          r.product_id,
          r.title,
          r.content,
          r.image_path,
          r.reward_points_awarded,
          r.created_at,
          o.id AS order_row_id,
          o.order_no,
          o.status AS order_status,
          o.created_by_user_id AS order_owner_user_id,
          o.product_id AS order_product_id,
          p.brand,
          p.model,
          p.sub_model
        FROM reviews r
        LEFT JOIN orders o ON o.id = r.order_id
        LEFT JOIN products p ON p.id = COALESCE(o.product_id, r.product_id)
        WHERE r.id = ?
          AND r.user_id = ?
        LIMIT 1
      `
    )
    .get(safeReviewId, safeUserId);
}

app.get('/review', (req, res) => {
  const reviews = db
    .prepare(
      `
        SELECT r.id, r.title, r.content, r.image_path, r.created_at, u.username, p.brand, p.model, p.sub_model
        FROM reviews r
        JOIN users u ON u.id = r.user_id
        LEFT JOIN products p ON p.id = r.product_id
        ORDER BY r.id DESC
      `
    )
    .all();

  res.render('review-list', { title: 'Review', reviews, maskUsername });
});

app.get('/review/new', requireAuth, (req, res) => {
  const myPageOrdersPath = '/mypage?section=orders';
  const orderId = normalizeOptionalId(req.query.orderId || '');
  if (orderId <= 0) {
    setFlash(req, 'error', '구매후기는 마이페이지 구매목록에서만 작성할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const reviewOrder = getReviewOrderForUser(orderId, req.user.id);
  if (!reviewOrder) {
    setFlash(req, 'error', '구매 내역을 확인할 수 없습니다.');
    return res.redirect(myPageOrdersPath);
  }

  if (normalizeOrderStatus(reviewOrder.status) !== ORDER_STATUS.DELIVERED) {
    setFlash(req, 'error', '배송완료 이후에만 구매후기를 작성할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const existingReview = getExistingReviewByOrder(reviewOrder.id, req.user.id);
  if (existingReview) {
    setFlash(req, 'error', '이미 작성한 후기입니다. 수정 화면으로 이동합니다.');
    return res.redirect(`/review/${existingReview.id}/edit`);
  }

  res.render('review-form', {
    title: 'Write Review',
    mode: 'create',
    formAction: '/review/new',
    reviewOrder,
    reviewRewardPoints: getReviewRewardPointsSetting(),
    reviewForm: {
      title: '',
      content: '',
      image_path: ''
    },
    backPath: myPageOrdersPath
  });
});

app.post('/review/new', requireAuth, upload.single('image'), requireAuthenticatedMultipartCsrf, (req, res) => {
  const myPageOrdersPath = '/mypage?section=orders';
  const orderId = normalizeOptionalId(req.body.orderId || req.query.orderId || '');
  const redirectToWrite = orderId > 0 ? `/review/new?orderId=${orderId}` : myPageOrdersPath;
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();

  if (orderId <= 0) {
    setFlash(req, 'error', '구매후기 작성 대상 주문을 찾을 수 없습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const reviewOrder = getReviewOrderForUser(orderId, req.user.id);
  if (!reviewOrder) {
    setFlash(req, 'error', '구매 내역을 확인할 수 없습니다.');
    return res.redirect(myPageOrdersPath);
  }

  if (normalizeOrderStatus(reviewOrder.status) !== ORDER_STATUS.DELIVERED) {
    setFlash(req, 'error', '배송완료 이후에만 구매후기를 작성할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const existingReview = getExistingReviewByOrder(reviewOrder.id, req.user.id);
  if (existingReview) {
    setFlash(req, 'error', '이미 작성한 후기입니다. 수정 화면으로 이동합니다.');
    return res.redirect(`/review/${existingReview.id}/edit`);
  }

  if (!title || !content) {
    setFlash(req, 'error', '제목과 내용을 입력해 주세요.');
    return res.redirect(redirectToWrite);
  }
  if (title.length > 120) {
    setFlash(req, 'error', '제목은 120자 이하로 입력해 주세요.');
    return res.redirect(redirectToWrite);
  }
  if (content.length > 5000) {
    setFlash(req, 'error', '내용은 5000자 이하로 입력해 주세요.');
    return res.redirect(redirectToWrite);
  }

  const reviewRewardPoints = getReviewRewardPointsSetting();
  const insertReviewWithPointTx = db.transaction((resolvedUserId, resolvedOrder, resolvedTitle, resolvedContent, resolvedImagePath, resolvedRewardPoints) => {
    db.prepare(
      `
        INSERT INTO reviews (user_id, order_id, product_id, title, content, image_path, reward_points_awarded)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      resolvedUserId,
      resolvedOrder.id,
      Number(resolvedOrder.product_id || 0) > 0 ? Number(resolvedOrder.product_id) : null,
      resolvedTitle,
      resolvedContent,
      resolvedImagePath,
      resolvedRewardPoints
    );

    if (resolvedRewardPoints > 0) {
      const pointCredit = db
        .prepare('UPDATE users SET reward_points = reward_points + ? WHERE id = ? AND is_admin = 0')
        .run(resolvedRewardPoints, resolvedUserId);
      if (pointCredit.changes === 0) {
        throw new Error('review_reward_point_credit_failed');
      }
    }
  });

  try {
    insertReviewWithPointTx(req.user.id, reviewOrder, title, content, fileUrl(req.file), reviewRewardPoints);
  } catch (error) {
    const message = String(error?.message || '');
    if (message.includes('UNIQUE constraint failed: reviews.order_id')) {
      const existingConflictReview = getExistingReviewByOrder(reviewOrder.id, req.user.id);
      if (existingConflictReview) {
        setFlash(req, 'error', '이미 작성한 후기입니다. 수정 화면으로 이동합니다.');
        return res.redirect(`/review/${existingConflictReview.id}/edit`);
      }
      setFlash(req, 'error', '이미 작성한 후기입니다.');
      return res.redirect(myPageOrdersPath);
    }
    console.error('[review.new] failed to save review with reward points:', error);
    setFlash(req, 'error', '구매후기 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.');
    return res.redirect(redirectToWrite);
  }

  const successMessage = reviewRewardPoints > 0
    ? `구매후기가 등록되었습니다. ${formatPrice(reviewRewardPoints)}P가 지급되었습니다.`
    : '구매후기가 등록되었습니다.';
  setFlash(req, 'success', successMessage);
  res.redirect(myPageOrdersPath);
});

app.get('/review/:id/edit', requireAuth, (req, res) => {
  const myPageOrdersPath = '/mypage?section=orders';
  const reviewId = normalizeOptionalId(req.params.id || '');
  if (reviewId <= 0) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const reviewForm = getOwnedReviewWithOrder(reviewId, req.user.id);
  if (!reviewForm) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const linkedOrderId = Number(reviewForm.linked_order_id || 0);
  const orderOwnerUserId = Number(reviewForm.order_owner_user_id || 0);
  if (linkedOrderId <= 0 || orderOwnerUserId !== Number(req.user.id)) {
    setFlash(req, 'error', '구매내역에 연결된 후기만 수정할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const reviewOrder = {
    id: Number(reviewForm.order_row_id || linkedOrderId),
    order_no: reviewForm.order_no || '',
    status: reviewForm.order_status || '',
    product_id: Number(reviewForm.order_product_id || reviewForm.product_id || 0),
    brand: reviewForm.brand || '',
    model: reviewForm.model || '',
    sub_model: reviewForm.sub_model || ''
  };

  res.render('review-form', {
    title: 'Edit Review',
    mode: 'edit',
    formAction: `/review/${reviewForm.id}/edit`,
    reviewOrder,
    reviewRewardPoints: getReviewRewardPointsSetting(),
    reviewForm,
    backPath: myPageOrdersPath
  });
});

app.post('/review/:id/edit', requireAuth, upload.single('image'), requireAuthenticatedMultipartCsrf, (req, res) => {
  const myPageOrdersPath = '/mypage?section=orders';
  const reviewId = normalizeOptionalId(req.params.id || '');
  if (reviewId <= 0) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const reviewForm = getOwnedReviewWithOrder(reviewId, req.user.id);
  if (!reviewForm) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const linkedOrderId = Number(reviewForm.linked_order_id || 0);
  const orderOwnerUserId = Number(reviewForm.order_owner_user_id || 0);
  if (linkedOrderId <= 0 || orderOwnerUserId !== Number(req.user.id)) {
    setFlash(req, 'error', '구매내역에 연결된 후기만 수정할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  if (!title || !content) {
    setFlash(req, 'error', '제목과 내용을 입력해 주세요.');
    return res.redirect(`/review/${reviewId}/edit`);
  }
  if (title.length > 120) {
    setFlash(req, 'error', '제목은 120자 이하로 입력해 주세요.');
    return res.redirect(`/review/${reviewId}/edit`);
  }
  if (content.length > 5000) {
    setFlash(req, 'error', '내용은 5000자 이하로 입력해 주세요.');
    return res.redirect(`/review/${reviewId}/edit`);
  }

  const shouldRemoveImage = String(req.body.removeImage || '').trim() === '1';
  const nextImagePath = req.file ? fileUrl(req.file) : shouldRemoveImage ? '' : String(reviewForm.image_path || '');
  const nextProductId = Number(reviewForm.order_product_id || reviewForm.product_id || 0);

  db.prepare(
    `
      UPDATE reviews
      SET order_id = ?,
          product_id = ?,
          title = ?,
          content = ?,
          image_path = ?
      WHERE id = ?
        AND user_id = ?
    `
  ).run(
    linkedOrderId,
    nextProductId > 0 ? nextProductId : null,
    title,
    content,
    nextImagePath,
    reviewId,
    req.user.id
  );

  setFlash(req, 'success', '구매후기가 수정되었습니다.');
  res.redirect(myPageOrdersPath);
});

app.post('/review/:id/delete', requireAuth, (req, res) => {
  const myPageOrdersPath = '/mypage?section=orders';
  const reviewId = normalizeOptionalId(req.params.id || '');
  if (reviewId <= 0) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const reviewForm = getOwnedReviewWithOrder(reviewId, req.user.id);
  if (!reviewForm) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '후기를 찾을 수 없습니다.' });
  }

  const linkedOrderId = Number(reviewForm.linked_order_id || 0);
  const orderOwnerUserId = Number(reviewForm.order_owner_user_id || 0);
  if (linkedOrderId <= 0 || orderOwnerUserId !== Number(req.user.id)) {
    setFlash(req, 'error', '구매내역에 연결된 후기만 삭제할 수 있습니다.');
    return res.redirect(myPageOrdersPath);
  }

  const awardedReviewPoints = parseNonNegativeInt(reviewForm.reward_points_awarded, 0);
  let deleteResult = {
    blockedByPointShortage: false,
    deductedPoints: 0
  };

  const deleteReviewWithPointRollbackTx = db.transaction((resolvedReviewId, resolvedUserId, resolvedAwardedPoints) => {
    const member = db
      .prepare(
        `
          SELECT id, is_admin, reward_points
          FROM users
          WHERE id = ?
          LIMIT 1
        `
      )
      .get(resolvedUserId);
    if (!member || Number(member.is_admin) === 1) {
      throw new Error('review_delete_member_not_found');
    }

    const availablePoints = parseNonNegativeInt(member.reward_points, 0);
    let blockedByPointShortage = false;
    let deductedPoints = 0;

    if (resolvedAwardedPoints > 0) {
      if (availablePoints >= resolvedAwardedPoints) {
        deductedPoints = resolvedAwardedPoints;
        const deductResult = db
          .prepare('UPDATE users SET reward_points = reward_points - ? WHERE id = ? AND is_admin = 0')
          .run(resolvedAwardedPoints, resolvedUserId);
        if (deductResult.changes === 0) {
          throw new Error('review_delete_point_deduction_failed');
        }
      } else {
        blockedByPointShortage = true;
        deductedPoints = availablePoints;
        const blockResult = db
          .prepare(
            `
              UPDATE users
              SET
                reward_points = 0,
                is_blocked = 1,
                blocked_reason = ?,
                blocked_at = datetime('now')
              WHERE id = ? AND is_admin = 0
            `
          )
          .run(REVIEW_POINT_DEDUCTION_BLOCK_REASON, resolvedUserId);
        if (blockResult.changes === 0) {
          throw new Error('review_delete_member_block_failed');
        }
      }
    }

    const deleted = db.prepare('DELETE FROM reviews WHERE id = ? AND user_id = ?').run(resolvedReviewId, resolvedUserId);
    if (deleted.changes === 0) {
      throw new Error('review_delete_target_missing');
    }

    return {
      blockedByPointShortage,
      deductedPoints
    };
  });

  try {
    deleteResult = deleteReviewWithPointRollbackTx(reviewId, req.user.id, awardedReviewPoints);
  } catch (error) {
    console.error('[review.delete] failed to rollback review reward points:', error);
    setFlash(req, 'error', '구매후기 삭제 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.');
    return res.redirect(myPageOrdersPath);
  }

  if (deleteResult.blockedByPointShortage) {
    setFlash(
      req,
      'success',
      `구매후기가 삭제되었습니다. 지급 포인트 ${formatPrice(awardedReviewPoints)}P 회수 잔고가 부족해 계정이 블락 처리되었습니다.`
    );
  } else if (awardedReviewPoints > 0) {
    setFlash(req, 'success', `구매후기가 삭제되었습니다. 지급된 ${formatPrice(awardedReviewPoints)}P가 회수되었습니다.`);
  } else {
    setFlash(req, 'success', '구매후기가 삭제되었습니다.');
  }

  res.redirect(myPageOrdersPath);
});

app.get('/inquiry', (req, res) => {
  const isAdminViewer = Boolean(req.user?.isAdmin);
  const inquiryWhere = isAdminViewer ? '' : 'WHERE COALESCE(i.is_hidden, 0) = 0';
  const inquiries = db
    .prepare(
      `
        SELECT i.id, i.title, i.created_at, i.reply_content, u.username, i.user_id, i.is_hidden
        FROM inquiries i
        JOIN users u ON u.id = i.user_id
        ${inquiryWhere}
        ORDER BY i.id DESC
      `
    )
    .all()
    .map((row) => ({
      ...row,
      writer: maskUsername(row.username),
      canOpen: Boolean(req.user && (req.user.isAdmin || req.user.id === Number(row.user_id)))
    }));

  res.render('inquiry-list', { title: 'Inquiry', inquiries });
});

app.get('/inquiry/new', requireAuth, (req, res) => {
  res.render('inquiry-form', { title: 'Write Inquiry' });
});

app.post('/inquiry/new', requireAuth, upload.array('image', 20), requireAuthenticatedMultipartCsrf, (req, res) => {
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();

  if (!title || !content) {
    setFlash(req, 'error', '제목과 내용을 입력해 주세요.');
    return res.redirect('/inquiry/new');
  }

  const uploadedImages = collectUploadedImageUrls(req);
  const imagePath = uploadedImages[0] || '';
  const imagePathsJson = serializeImagePaths(uploadedImages);

  db.prepare(
    `
      INSERT INTO inquiries (user_id, title, content, image_path, image_paths_json)
      VALUES (?, ?, ?, ?, ?)
    `
  ).run(req.user.id, title, content, imagePath, imagePathsJson);

  setFlash(req, 'success', '문의가 등록되었습니다.');
  res.redirect('/inquiry');
});

app.get('/inquiry/:id', (req, res) => {
  const id = Number(req.params.id);
  const inquiryRow = db
    .prepare(
      `
        SELECT i.*, u.username
        FROM inquiries i
        JOIN users u ON u.id = i.user_id
        WHERE i.id = ?
        LIMIT 1
      `
    )
    .get(id);

  if (!inquiryRow || (Number(inquiryRow.is_hidden || 0) === 1 && !req.user?.isAdmin)) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '문의를 찾을 수 없습니다.' });
  }
  const inquiry = withRecordImagePaths(inquiryRow);

  const canOpen = Boolean(req.user && (req.user.isAdmin || req.user.id === Number(inquiry.user_id)));

  res.render('inquiry-detail', {
    title: 'Inquiry Detail',
    inquiry,
    canOpen,
    writerMasked: maskUsername(inquiry.username)
  });
});

app.get('/signup', (req, res) => {
  const captcha = issueSignupCaptcha(req);
  res.render('signup', {
    title: 'Sign up',
    signupCaptchaPrompt: res.locals.ctx.lang === 'en' ? captcha.promptEn : captcha.promptKo
  });
});

app.get(
  '/api/signup/availability',
  authAttemptGuard({
    key: 'signup-availability',
    redirectPath: '/signup',
    limit: 40,
    windowMs: 10 * 60 * 1000,
    identifierResolver: (req) => {
      const account = normalizeAccountName(req.query?.account || '').toLowerCase();
      const nickname = String(req.query?.nickname || '').trim().toLowerCase();
      const email = normalizeEmailAddress(req.query?.email || '');
      return `${account}|${nickname}|${email}`;
    }
  }),
  (req, res) => {
    try {
      const account = normalizeAccountName(req.query.account || '').toLowerCase();
      const nickname = String(req.query.nickname || '').trim();
      const email = normalizeEmailAddress(req.query.email || '');

      const shouldCheckAccount = Boolean(account) && USERNAME_REGEX.test(account);
      const shouldCheckNickname = Boolean(nickname) && nickname.length >= 2 && nickname.length <= 40;
      const shouldCheckEmail = Boolean(email) && EMAIL_REGEX.test(email);

      const checks = {};
      if (account) {
        checks.account = {
          value: account,
          valid: shouldCheckAccount,
          exists: false
        };
      }
      if (nickname) {
        checks.nickname = {
          value: nickname,
          valid: shouldCheckNickname,
          exists: false
        };
      }
      if (email) {
        checks.email = {
          value: email,
          valid: shouldCheckEmail,
          exists: false
        };
      }

      if (!mustEnforceSecurity) {
        const duplicateState = getSignupDuplicateState({
          account: shouldCheckAccount ? account : '',
          nickname: shouldCheckNickname ? nickname : '',
          email: shouldCheckEmail ? email : ''
        });
        if (checks.account) {
          checks.account.exists = shouldCheckAccount ? Boolean(duplicateState.accountExists) : false;
        }
        if (checks.nickname) {
          checks.nickname.exists = shouldCheckNickname ? Boolean(duplicateState.nicknameExists) : false;
        }
        if (checks.email) {
          checks.email.exists = shouldCheckEmail ? Boolean(duplicateState.emailExists) : false;
        }
      }

      res.setHeader('Cache-Control', 'no-store');
      return res.json({ ok: true, checks });
    } catch (error) {
      logDetailedError('signup-availability-failed', error, {
        request: buildRequestLogContext(req)
      });
      return res.status(500).json({
        ok: false,
        error: 'availability_check_failed',
        message: '중복 확인 중 오류가 발생했습니다.'
      });
    }
  }
);

app.post(
  '/signup',
  authAttemptGuard({
    key: 'signup',
    redirectPath: '/signup',
    limit: 12,
    identifierResolver: (req) => `${String(req.body?.account || req.body?.username || '').trim().toLowerCase()}|${normalizeEmailAddress(req.body?.email || '')}`
  }),
  asyncRoute(async (req, res) => {
  const email = normalizeEmailAddress(req.body.email || '');
  const account = normalizeAccountName(req.body.account || req.body.username || '').toLowerCase();
  const nickname = String(req.body.nickname || '').trim();
  const phone = normalizePhone(req.body.phone || '');
  const password = String(req.body.password || '');
  const passwordConfirm = String(req.body.passwordConfirm || '');
  const captchaAnswer = String(req.body.captchaAnswer || '').trim();
  const honeypotValue = String(req.body.website || '').trim();
  const agreed = req.body.agreedTerms === 'on';
  const captcha = readSignupCaptcha(req);

  if (honeypotValue) {
    setFlash(req, 'error', '요청이 차단되었습니다.');
    return res.redirect('/signup');
  }

  if (!email || !account || !nickname || !phone || !password || !passwordConfirm) {
    setFlash(req, 'error', '필수 항목을 입력해 주세요.');
    return res.redirect('/signup');
  }

  if (!captcha || Date.now() - captcha.issuedAt < 1200) {
    setFlash(req, 'error', '자동등록방지 검증이 만료되었습니다. 다시 시도해 주세요.');
    return res.redirect('/signup');
  }

  if (!captchaAnswer || captchaAnswer !== captcha.answer) {
    setFlash(req, 'error', '자동등록방지 답변이 올바르지 않습니다.');
    return res.redirect('/signup');
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect('/signup');
  }

  if (!USERNAME_REGEX.test(account)) {
    setFlash(req, 'error', '계정은 4~20자 영문 소문자/숫자만 사용 가능합니다.');
    return res.redirect('/signup');
  }

  if (nickname.length < 2 || nickname.length > 40) {
    setFlash(req, 'error', '닉네임은 2~40자로 입력해 주세요.');
    return res.redirect('/signup');
  }

  if (!PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '핸드폰번호 형식이 올바르지 않습니다.');
    return res.redirect('/signup');
  }

  if (!PASSWORD_REGEX.test(password)) {
    setFlash(req, 'error', '비밀번호는 영문 대/소문자, 숫자, 특수문자를 포함해 8자 이상이어야 합니다.');
    return res.redirect('/signup');
  }

  if (password !== passwordConfirm) {
    setFlash(req, 'error', '비밀번호 확인이 일치하지 않습니다.');
    return res.redirect('/signup');
  }

  if (!agreed) {
    setFlash(req, 'error', '약관 동의가 필요합니다.');
    return res.redirect('/signup');
  }

  const duplicateState = getSignupDuplicateState({ account, nickname, email });
  const duplicateField = pickSignupDuplicateField(duplicateState);
  if (duplicateField) {
    setFlash(req, 'error', getSignupDuplicateMessage(duplicateField, false));
    return res.redirect('/signup');
  }

  try {
    const hash = await bcrypt.hash(password, 10);
    const signupBonusPoints = getSignupBonusPointsSetting();
    const createMember = db.transaction((nextEmail, nextAccount, nextNickname, nextPhone, nextHash, bonusPoints) => {
      const memberUid = generateNextMemberUid();
      const inserted = db
        .prepare(
          'INSERT INTO users (member_uid, email, username, nickname, phone, password_hash, agreed_terms, is_admin) VALUES (?, ?, ?, ?, ?, ?, 1, 0)'
        )
        .run(memberUid, nextEmail, nextAccount, nextNickname, nextPhone, nextHash);

      const userId = Number(inserted.lastInsertRowid);
      if (bonusPoints > 0) {
        db.prepare('UPDATE users SET reward_points = reward_points + ? WHERE id = ?').run(bonusPoints, userId);
      }
      return userId;
    });

    const createdUserId = createMember(email, account, nickname, phone, hash, signupBonusPoints);

    await regenerateSessionWithPreservedAuth(req, {
      preserveAdmin: true,
      preserveAdminOtpState: true
    });
    const nowMs = Date.now();
    setScopedSessionAuthState(req, 'member', {
      userId: createdUserId,
      lastActivityAt: nowMs
    });
    setPersistAuthCookie(res, createdUserId, {
      scope: 'member',
      isAdmin: false,
      lastActivityAt: nowMs
    });
    clearSignupCaptcha(req);
    resetAuthAttempt(req, 'signup', {
      identifier: `${account}|${email}`
    });

    const isEn = res.locals.ctx.lang === 'en';
    const successMessage =
      signupBonusPoints > 0
        ? isEn
          ? `Sign up complete. ${signupBonusPoints.toLocaleString()} points have been credited.`
          : `회원가입이 완료되었습니다. ${signupBonusPoints.toLocaleString()}포인트가 지급되었습니다.`
        : isEn
          ? 'Sign up complete.'
          : '회원가입이 완료되었습니다.';
    setFlash(req, 'success', successMessage);
    return res.redirect('/main');
  } catch (error) {
    if (isSqliteUniqueConstraintError(error)) {
      const duplicateFieldFromDb = resolveSignupDuplicateFieldFromSqliteError(error);
      setFlash(req, 'error', getSignupDuplicateMessage(duplicateFieldFromDb, false));
      logDetailedError('signup-duplicate-race', error, {
        request: buildRequestLogContext(req),
        duplicateField: duplicateFieldFromDb || 'unknown'
      });
      return res.redirect('/signup');
    }

    logDetailedError('signup-create-failed', error, {
      request: buildRequestLogContext(req)
    });
    setFlash(req, 'error', '회원가입 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.');
    return res.redirect('/signup');
  }
  })
);

app.get('/account/find', (req, res) => {
  const result = req.session.accountFindResult || null;
  let resultPayload = null;

  if (result && Number(result.expiresAt || 0) > Date.now()) {
    resultPayload = {
      account: String(result.account || '').trim(),
      email: normalizeEmailAddress(result.email || ''),
      resetTicket: String(result.resetTicket || '').trim()
    };
  } else if (result) {
    delete req.session.accountFindResult;
  }

  const email = normalizeEmailAddress(req.query.email || '');
  const step = req.query.step === 'verify' ? 'verify' : 'request';

  res.render('account-find', {
    title: res.locals.ctx.lang === 'en' ? 'Find Account' : '계정찾기',
    flow: {
      step,
      email,
      result: req.query.result === '1' ? resultPayload : null
    }
  });
});

app.post(
  '/account/find/send-code',
  authAttemptGuard({
    key: 'account-find-send',
    redirectPath: '/account/find',
    limit: 8,
    identifierResolver: (req) => normalizeEmailAddress(req.body?.email || '')
  }),
  asyncRoute(async (req, res) => {
    if (req.session.accountFindResult) {
      delete req.session.accountFindResult;
    }

    const email = normalizeEmailAddress(req.body.email || '');
    if (!EMAIL_REGEX.test(email)) {
      setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
      return res.redirect('/account/find');
    }

    const target = db
      .prepare(
        `
          SELECT id, email, username
          FROM users
          WHERE is_admin = 0
            AND lower(email) = lower(?)
          LIMIT 1
        `
      )
      .get(email);

    let debugCode = '';
    if (target) {
      const issued = issueEmailVerificationCode({
        purpose: 'account-find',
        email: target.email,
        account: target.username,
        userId: target.id
      });
      if (issued.ok) {
        debugCode = issued.code;
        const sent = await sendEmailVerificationCode({
          to: target.email,
          code: issued.code,
          purpose: 'account-find',
          lang: res.locals.ctx.lang
        });
        if (!mustEnforceSecurity && !isProduction && !sent.ok && sent.reason === 'smtp_not_configured') {
          setFlash(req, 'success', `[개발모드] 인증번호: ${issued.code}`);
        }
      }
    }
    if (mustEnforceSecurity) {
      await waitForMs(250);
    }
    if (!(!mustEnforceSecurity && !isProduction && debugCode)) {
      setFlash(req, 'success', '입력하신 정보가 맞으면 인증번호를 이메일로 전송했습니다.');
    }
    return res.redirect(`/account/find?step=verify&email=${encodeURIComponent(email)}`);
  })
);

app.post(
  '/account/find/verify-code',
  authAttemptGuard({
    key: 'account-find-verify',
    redirectPath: '/account/find',
    limit: 12,
    identifierResolver: (req) => normalizeEmailAddress(req.body?.email || '')
  }),
  asyncRoute(async (req, res) => {
    const email = normalizeEmailAddress(req.body.email || '');
    const code = String(req.body.code || '').trim();
    if (!EMAIL_REGEX.test(email)) {
      setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
      return res.redirect('/account/find');
    }
    if (!/^[0-9]{6}$/.test(code)) {
      setFlash(req, 'error', '6자리 인증번호를 입력해 주세요.');
      return res.redirect(`/account/find?step=verify&email=${encodeURIComponent(email)}`);
    }

    const target = db
      .prepare(
        `
          SELECT id, email, username
          FROM users
          WHERE is_admin = 0
            AND lower(email) = lower(?)
          LIMIT 1
        `
      )
      .get(email);

    if (!target) {
      if (mustEnforceSecurity) {
        await waitForMs(250);
      }
      setFlash(req, 'error', mustEnforceSecurity ? '인증번호가 올바르지 않거나 만료되었습니다.' : '가입된 계정을 찾을 수 없습니다.');
      return res.redirect('/account/find');
    }

    const verified = verifyEmailVerificationCode({
      purpose: 'account-find',
      email: target.email,
      account: target.username,
      code
    });

    if (!verified.ok) {
      if (mustEnforceSecurity) {
        await waitForMs(250);
      }
      const reasonMessage =
        mustEnforceSecurity
          ? '인증번호가 올바르지 않거나 만료되었습니다.'
          : verified.reason === 'expired'
            ? '인증번호가 만료되었습니다. 다시 발급해 주세요.'
            : verified.reason === 'too_many_attempts'
              ? '인증 시도 횟수를 초과했습니다. 다시 인증번호를 발급해 주세요.'
              : '인증번호가 올바르지 않습니다.';
      setFlash(req, 'error', reasonMessage);
      return res.redirect(`/account/find?step=verify&email=${encodeURIComponent(email)}`);
    }

    const resetTicket = createPasswordResetTicket({
      userId: target.id,
      account: target.username,
      email: target.email,
      source: 'account-find'
    });
    if (!resetTicket) {
      setFlash(req, 'error', '비밀번호 재설정 준비 중 오류가 발생했습니다.');
      return res.redirect('/account/find');
    }

    req.session.accountFindResult = {
      account: target.username,
      email: target.email,
      resetTicket,
      expiresAt: Date.now() + PASSWORD_RESET_TICKET_TTL_MS
    };
    resetAuthAttempt(req, 'account-find-send', { identifier: normalizeEmailAddress(email) });
    resetAuthAttempt(req, 'account-find-verify', { identifier: normalizeEmailAddress(email) });
    setFlash(req, 'success', '인증이 완료되었습니다.');
    return res.redirect('/account/find?result=1');
  })
);

app.get('/password/reset', (req, res) => {
  const account = normalizeAccountName(req.query.account || '');
  const email = normalizeEmailAddress(req.query.email || '');
  const step = req.query.step === 'verify' ? 'verify' : 'request';
  const ticket = String(req.query.ticket || '').trim();

  let reset = null;
  if (ticket) {
    const found = readPasswordResetTicket(ticket);
    if (found.ok) {
      reset = {
        ticket,
        account: found.row.account,
        email: found.row.email
      };
    } else {
      setFlash(req, 'error', '비밀번호 재설정 링크가 만료되었거나 유효하지 않습니다.');
      return res.redirect('/password/reset');
    }
  }

  res.render('password-reset', {
    title: res.locals.ctx.lang === 'en' ? 'Reset Password' : '비밀번호찾기',
    flow: {
      step: reset ? 'reset' : step,
      account,
      email,
      reset
    }
  });
});

app.post(
  '/password/reset/send-code',
  authAttemptGuard({
    key: 'password-reset-send',
    redirectPath: '/password/reset',
    limit: 10,
    identifierResolver: (req) => `${normalizeAccountName(req.body?.account || '').toLowerCase()}|${normalizeEmailAddress(req.body?.email || '')}`
  }),
  asyncRoute(async (req, res) => {
    const account = normalizeAccountName(req.body.account || '');
    const email = normalizeEmailAddress(req.body.email || '');

    if (!account || !email) {
      setFlash(req, 'error', '계정과 이메일을 모두 입력해 주세요.');
      return res.redirect('/password/reset');
    }

    if (!ACCOUNT_LOOKUP_REGEX.test(account)) {
      setFlash(req, 'error', '계정 형식이 올바르지 않습니다.');
      return res.redirect('/password/reset');
    }

    if (!EMAIL_REGEX.test(email)) {
      setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
      return res.redirect('/password/reset');
    }

    const target = db
      .prepare(
        `
          SELECT id, email, username
          FROM users
          WHERE is_admin = 0
            AND username = ?
            AND lower(email) = lower(?)
          LIMIT 1
        `
      )
      .get(account, email);

    let debugCode = '';
    if (target) {
      const issued = issueEmailVerificationCode({
        purpose: 'password-reset',
        email: target.email,
        account: target.username,
        userId: target.id
      });
      if (issued.ok) {
        debugCode = issued.code;
        const sent = await sendEmailVerificationCode({
          to: target.email,
          code: issued.code,
          purpose: 'password-reset',
          lang: res.locals.ctx.lang
        });
        if (!mustEnforceSecurity && !isProduction && !sent.ok && sent.reason === 'smtp_not_configured') {
          setFlash(req, 'success', `[개발모드] 인증번호: ${issued.code}`);
        }
      }
    }
    if (mustEnforceSecurity) {
      await waitForMs(250);
    }

    if (!(!mustEnforceSecurity && !isProduction && debugCode)) {
      setFlash(req, 'success', '입력하신 정보가 맞으면 인증번호를 이메일로 전송했습니다.');
    }

    return res.redirect(
      `/password/reset?step=verify&account=${encodeURIComponent(account)}&email=${encodeURIComponent(email)}`
    );
  })
);

app.post(
  '/password/reset/verify-code',
  authAttemptGuard({
    key: 'password-reset-verify',
    redirectPath: '/password/reset',
    limit: 14,
    identifierResolver: (req) => `${normalizeAccountName(req.body?.account || '').toLowerCase()}|${normalizeEmailAddress(req.body?.email || '')}`
  }),
  asyncRoute(async (req, res) => {
    const account = normalizeAccountName(req.body.account || '');
    const email = normalizeEmailAddress(req.body.email || '');
    const code = String(req.body.code || '').trim();

    if (!account || !email || !code) {
      setFlash(req, 'error', '계정/이메일/인증번호를 모두 입력해 주세요.');
      return res.redirect('/password/reset');
    }

    const target = db
      .prepare(
        `
          SELECT id, email, username
          FROM users
          WHERE is_admin = 0
            AND username = ?
            AND lower(email) = lower(?)
          LIMIT 1
        `
      )
      .get(account, email);

    if (!target) {
      if (mustEnforceSecurity) {
        await waitForMs(250);
      }
      setFlash(req, 'error', mustEnforceSecurity ? '인증번호가 올바르지 않거나 만료되었습니다.' : '입력한 계정/이메일 정보와 일치하는 회원이 없습니다.');
      return res.redirect('/password/reset');
    }

    const verified = verifyEmailVerificationCode({
      purpose: 'password-reset',
      email: target.email,
      account: target.username,
      code
    });

    if (!verified.ok) {
      if (mustEnforceSecurity) {
        await waitForMs(250);
      }
      const reasonMessage =
        mustEnforceSecurity
          ? '인증번호가 올바르지 않거나 만료되었습니다.'
          : verified.reason === 'expired'
            ? '인증번호가 만료되었습니다. 다시 발급해 주세요.'
            : verified.reason === 'too_many_attempts'
              ? '인증 시도 횟수를 초과했습니다. 다시 인증번호를 발급해 주세요.'
              : '인증번호가 올바르지 않습니다.';
      setFlash(req, 'error', reasonMessage);
      return res.redirect(
        `/password/reset?step=verify&account=${encodeURIComponent(account)}&email=${encodeURIComponent(email)}`
      );
    }

    const ticket = createPasswordResetTicket({
      userId: target.id,
      account: target.username,
      email: target.email,
      source: 'password-reset'
    });
    if (!ticket) {
      setFlash(req, 'error', '비밀번호 재설정 준비 중 오류가 발생했습니다.');
      return res.redirect('/password/reset');
    }

    resetAuthAttempt(req, 'password-reset-send', { identifier: `${account.toLowerCase()}|${email}` });
    resetAuthAttempt(req, 'password-reset-verify', { identifier: `${account.toLowerCase()}|${email}` });
    setFlash(req, 'success', '이메일 인증이 완료되었습니다. 새 비밀번호를 입력해 주세요.');
    return res.redirect(`/password/reset?ticket=${encodeURIComponent(ticket)}`);
  })
);

app.post(
  '/password/reset/update',
  authAttemptGuard({
    key: 'password-reset-update',
    redirectPath: '/password/reset',
    limit: 18,
    identifierResolver: (req) => String(req.body?.ticket || '').trim().slice(0, 120)
  }),
  asyncRoute(async (req, res) => {
    const ticket = String(req.body.ticket || '').trim();
    const newPassword = String(req.body.password || '');
    const passwordConfirm = String(req.body.passwordConfirm || '');

    if (!ticket || !newPassword || !passwordConfirm) {
      setFlash(req, 'error', '필수 항목을 모두 입력해 주세요.');
      return res.redirect('/password/reset');
    }

    const ticketResult = readPasswordResetTicket(ticket);
    if (!ticketResult.ok) {
      setFlash(req, 'error', '비밀번호 재설정 링크가 만료되었거나 유효하지 않습니다.');
      return res.redirect('/password/reset');
    }

    if (!PASSWORD_REGEX.test(newPassword)) {
      setFlash(req, 'error', '비밀번호는 영문 대/소문자, 숫자, 특수문자를 포함해 8자 이상이어야 합니다.');
      return res.redirect(`/password/reset?ticket=${encodeURIComponent(ticket)}`);
    }

    if (newPassword !== passwordConfirm) {
      setFlash(req, 'error', '비밀번호 확인이 일치하지 않습니다.');
      return res.redirect(`/password/reset?ticket=${encodeURIComponent(ticket)}`);
    }

    const user = db
      .prepare(
        `
          SELECT id, username, email, is_admin
          FROM users
          WHERE id = ?
          LIMIT 1
        `
      )
      .get(ticketResult.row.userId);

    if (!user || Number(user.is_admin || 0) === 1) {
      consumePasswordResetTicket(ticket);
      setFlash(
        req,
        'error',
        mustEnforceSecurity
          ? '비밀번호 재설정 요청이 유효하지 않습니다. 다시 시도해 주세요.'
          : '비밀번호를 변경할 계정을 찾을 수 없습니다.'
      );
      return res.redirect('/password/reset');
    }

    const nextHash = await bcrypt.hash(newPassword, 10);
    db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(nextHash, user.id);
    consumePasswordResetTicket(ticket);

    if (req.session.accountFindResult) {
      delete req.session.accountFindResult;
    }

    resetAuthAttempt(req, 'password-reset-update', {
      identifier: String(ticket || '').trim().slice(0, 120)
    });
    setFlash(req, 'success', '비밀번호가 재설정되었습니다. 새 비밀번호로 로그인해 주세요.');
    return res.redirect(`/login?account=${encodeURIComponent(user.username)}`);
  })
);

app.get('/login', (req, res) => {
  const prefilledAccount = normalizeAccountName(req.query.account || '').slice(0, 40);
  res.render('login', { title: 'Login', prefilledAccount });
});

app.post(
  '/login',
  authAttemptGuard({
    key: 'login',
    redirectPath: '/login',
    limit: 15,
    identifierResolver: (req) => String(req.body?.account || req.body?.username || '').trim().toLowerCase()
  }),
  asyncRoute(async (req, res) => {
  const account = String(req.body.account || req.body.username || '').trim();
  const password = String(req.body.password || '');

  if (!account || !password) {
    setFlash(req, 'error', '계정과 비밀번호를 입력해 주세요.');
    return res.redirect('/login');
  }

  const user = db
    .prepare(
      'SELECT id, username, password_hash, is_admin, admin_role, is_blocked, blocked_reason FROM users WHERE username = ? LIMIT 1'
    )
    .get(account);

  const valid = await bcrypt.compare(password, user?.password_hash || DUMMY_PASSWORD_HASH);
  if (!user || !valid) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
  }

  if (Number(user.is_admin) === 1) {
    if (mustEnforceSecurity) {
      setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
      return res.redirect('/login');
    }
    setFlash(req, 'error', '관리자 계정은 어드민 로그인 페이지를 이용해 주세요.');
    return res.redirect('/admin/login');
  }

  if (Number(user.is_blocked) === 1) {
    setFlash(req, 'error', BLOCKED_ACCOUNT_NOTICE);
    return res.redirect('/login');
  }

  await regenerateSessionWithPreservedAuth(req, {
    preserveAdmin: true,
    preserveAdminOtpState: true
  });
  const nowMs = Date.now();
  setScopedSessionAuthState(req, 'member', {
    userId: Number(user.id),
    lastActivityAt: nowMs
  });
  setPersistAuthCookie(res, Number(user.id), {
    scope: 'member',
    isAdmin: false,
    lastActivityAt: nowMs
  });
  resetAuthAttempt(req, 'login', { identifier: account.toLowerCase() });

  res.redirect('/main');
  })
);

app.post('/logout', (req, res) => {
  clearScopedSessionAuthState(req, 'member');
  clearPersistAuthCookie(res, { scope: 'member' });
  res.redirect('/main');
});

app.get('/admin/login', (req, res) => {
  if (req.user?.isAdmin && ADMIN_OTP_ENFORCED && !req.user.isAdminOtpEnabled) {
    clearScopedSessionAuthState(req, 'admin', { clearOtpSetup: true });
    clearPersistAuthCookie(res, { scope: 'admin' });
    setFlash(req, 'error', '관리자 OTP 필수 정책이 적용되어 다시 로그인해야 합니다.');
    return res.redirect('/admin/login');
  }
  if (req.user?.isAdmin) {
    return res.redirect('/admin/dashboard');
  }
  if (readAdminOtpPending(req)) {
    return res.redirect('/admin/otp/verify');
  }
  res.render('admin-login', { title: 'Admin Login' });
});

app.post(
  '/admin/login',
  authAttemptGuard({
    key: 'admin-login',
    redirectPath: '/admin/login',
    limit: 10,
    identifierResolver: (req) => String(req.body?.account || req.body?.username || '').trim().toLowerCase(),
    onBlocked: (req, context) => {
      const identifier = String(context?.identifier || '').slice(0, 120);
      recordSecurityAlert(
        req,
        'auth.admin.login_throttled',
        `retry_after=${Number(context?.waitSeconds || 0)}s, account=${identifier || 'unknown'}`
      );
    }
  }),
  asyncRoute(async (req, res) => {
  const account = String(req.body.account || req.body.username || '').trim();
  const password = String(req.body.password || '');
  clearAdminOtpPending(req);

  const user = db
    .prepare(
      `
        SELECT
          id,
          username,
          password_hash,
          is_admin,
          admin_role,
          is_blocked,
          blocked_reason,
          admin_otp_secret,
          admin_otp_enabled
        FROM users
        WHERE username = ?
        LIMIT 1
      `
    )
    .get(account);

  const valid = await bcrypt.compare(password, user?.password_hash || DUMMY_PASSWORD_HASH);
  if (!user || Number(user.is_admin) !== 1 || !valid) {
    recordSecurityAlert(
      req,
      'auth.admin.login_failed',
      `account=${String(account || '').slice(0, 120) || 'unknown'}`
    );
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/admin/login');
  }

  if (Number(user.is_blocked) === 1) {
    recordSecurityAlert(
      req,
      'auth.admin.login_blocked_account',
      `account=${String(account || '').slice(0, 120)}, reason=${String(user.blocked_reason || '').slice(0, 120)}`
    );
    setFlash(req, 'error', BLOCKED_ACCOUNT_NOTICE);
    return res.redirect('/admin/login');
  }

  const hasOtpEnabled =
    Number(user.admin_otp_enabled || 0) === 1 &&
    normalizeBase32Secret(user.admin_otp_secret || '').length >= 16;
  if (ADMIN_OTP_ENFORCED && !hasOtpEnabled) {
    recordSecurityAlert(req, 'auth.admin.otp_required', `account=${String(account || '').slice(0, 120)}`);
    setFlash(req, 'error', '관리자 계정은 OTP 설정이 필수입니다. 메인관리자에게 OTP 설정 상태를 확인해 주세요.');
    return res.redirect('/admin/login');
  }
  if (hasOtpEnabled) {
    setAdminOtpPending(req, user);
    logAdminActivityByUser(user, req, 'LOGIN_OTP_PENDING', 'password verified; otp required');
    setFlash(req, 'success', '구글 OTP 인증번호를 입력해 주세요.');
    return res.redirect('/admin/otp/verify');
  }

  await setAdminAuthSession(req, res, user);
  resetAuthAttempt(req, 'admin-login', { identifier: account.toLowerCase() });

  logAdminActivityByUser(user, req, 'LOGIN_SUCCESS', 'admin login success');

  res.redirect('/admin/dashboard');
  })
);

app.get('/admin/otp/verify', (req, res) => {
  if (req.user?.isAdmin) {
    return res.redirect('/admin/dashboard');
  }

  const pending = readAdminOtpPending(req);
  if (!pending) {
    setFlash(req, 'error', 'OTP 인증이 만료되었습니다. 다시 로그인해 주세요.');
    return res.redirect('/admin/login');
  }

  return res.render('admin-otp-verify', {
    title: 'Admin OTP Verify',
    pending
  });
});

app.post(
  '/admin/otp/verify',
  authAttemptGuard({
    key: 'admin-otp-verify',
    redirectPath: '/admin/otp/verify',
    limit: 12,
    identifierResolver: (req) => {
      const pending = readAdminOtpPending(req);
      return pending ? `uid:${pending.userId}` : '';
    },
    onBlocked: (req, context) => {
      recordSecurityAlert(
        req,
        'auth.admin.otp_throttled',
        `retry_after=${Number(context?.waitSeconds || 0)}s, identifier=${String(context?.identifier || '').slice(0, 120) || 'unknown'}`
      );
    }
  }),
  asyncRoute(async (req, res) => {
    const pending = readAdminOtpPending(req);
    if (!pending) {
      recordSecurityAlert(req, 'auth.admin.otp_missing_pending', 'otp verify requested without pending session');
      setFlash(req, 'error', 'OTP 인증이 만료되었습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    const code = normalizeAdminOtpCode(req.body.code || '');
    if (code.length !== ADMIN_OTP_DIGITS) {
      recordSecurityAlert(req, 'auth.admin.otp_invalid_format', `uid=${pending.userId}`);
      setFlash(req, 'error', '6자리 OTP 인증번호를 입력해 주세요.');
      return res.redirect('/admin/otp/verify');
    }

    const user = db
      .prepare(
        `
          SELECT
            id,
            username,
            admin_role,
            is_admin,
            is_blocked,
            blocked_reason,
            admin_otp_secret,
            admin_otp_enabled
          FROM users
          WHERE id = ?
          LIMIT 1
        `
      )
      .get(pending.userId);

    if (!user || Number(user.is_admin || 0) !== 1) {
      recordSecurityAlert(req, 'auth.admin.otp_user_missing', `uid=${pending.userId}`);
      clearAdminOtpPending(req);
      setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    if (Number(user.is_blocked || 0) === 1) {
      recordSecurityAlert(req, 'auth.admin.otp_blocked_account', `uid=${pending.userId}`);
      clearAdminOtpPending(req);
      setFlash(req, 'error', BLOCKED_ACCOUNT_NOTICE);
      return res.redirect('/admin/login');
    }

    const secret = normalizeBase32Secret(user.admin_otp_secret || '');
    const isOtpEnabled = Number(user.admin_otp_enabled || 0) === 1 && secret.length >= 16;
    if (!isOtpEnabled) {
      recordSecurityAlert(req, 'auth.admin.otp_secret_missing', `uid=${pending.userId}`);
      clearAdminOtpPending(req);
      setFlash(req, 'error', 'OTP 설정을 찾을 수 없습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    const verified = verifyTotpCode(secret, code);
    if (!verified) {
      recordSecurityAlert(req, 'auth.admin.otp_failed', `uid=${pending.userId}`);
      setFlash(req, 'error', 'OTP 인증번호가 올바르지 않습니다.');
      return res.redirect('/admin/otp/verify');
    }

    await setAdminAuthSession(req, res, user);
    resetAuthAttempt(req, 'admin-login', { identifier: String(user.username || '').trim().toLowerCase() });
    resetAuthAttempt(req, 'admin-otp-verify', { identifier: `uid:${user.id}` });
    logAdminActivityByUser(user, req, 'LOGIN_SUCCESS', 'admin login success via otp');
    setFlash(req, 'success', '관리자 로그인되었습니다.');
    return res.redirect('/admin/dashboard');
  })
);

app.post('/admin/change-password', requirePrimaryAdmin, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/security?section=profile');
  const currentPassword = String(req.body.currentPassword || '');
  const newPassword = String(req.body.newPassword || '');
  const newPasswordConfirm = String(req.body.newPasswordConfirm || '');

  if (!currentPassword || !newPassword || !newPasswordConfirm) {
    setFlash(req, 'error', '현재 비밀번호와 새 비밀번호를 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (newPassword !== newPasswordConfirm) {
    setFlash(req, 'error', '새 비밀번호 확인이 일치하지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PASSWORD_REGEX.test(newPassword)) {
    setFlash(req, 'error', '새 비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
    return res.redirect(backPath);
  }

  const admin = db
    .prepare('SELECT id, password_hash FROM users WHERE id = ? AND is_admin = 1 LIMIT 1')
    .get(req.user.id);

  if (!admin) {
    setFlash(req, 'error', '어드민 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const validCurrent = await bcrypt.compare(currentPassword, admin.password_hash);
  if (!validCurrent) {
    setFlash(req, 'error', '현재 비밀번호가 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  const nextHash = await bcrypt.hash(newPassword, 10);
  db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(nextHash, req.user.id);

  setFlash(req, 'success', '어드민 비밀번호가 변경되었습니다.');
  return res.redirect(backPath);
}));

app.get('/admin/logout', requireAdmin, (req, res) => {
  return res.redirect('/admin/dashboard');
});

app.post('/admin/logout', requireAdmin, (req, res) => {
  clearScopedSessionAuthState(req, 'admin', { clearOtpSetup: true });
  clearPersistAuthCookie(res, { scope: 'admin' });
  res.redirect('/admin/login');
});

function getVisitRangeSummary(baseDate, columnName) {
  const allowedColumn = new Set(['visit_count', 'member_visit_count', 'guest_visit_count']);
  const safeColumn = allowedColumn.has(columnName) ? columnName : 'visit_count';

  const sumRange = (days) => {
    const offsetDays = Math.max(days - 1, 0);
    const row = db
      .prepare(
        `
          SELECT COALESCE(SUM(${safeColumn}), 0) AS count
          FROM daily_visits
          WHERE visit_date BETWEEN date(?, '-${offsetDays} day') AND ?
        `
      )
      .get(baseDate, baseDate);
    return Number(row?.count || 0);
  };

  const totalRow = db
    .prepare(
      `
        SELECT COALESCE(SUM(${safeColumn}), 0) AS count
        FROM daily_visits
      `
    )
    .get();

  return {
    today: sumRange(1),
    week: sumRange(7),
    month: sumRange(30),
    year: sumRange(365),
    total: Number(totalRow?.count || 0)
  };
}

function getMemberSignupRangeSummary(baseDate) {
  const sumRange = (days) => {
    const offsetDays = Math.max(days - 1, 0);
    const row = db
      .prepare(
        `
          SELECT COUNT(*) AS count
          FROM users
          WHERE is_admin = 0
            AND date(datetime(created_at, '+9 hours')) BETWEEN date(?, '-${offsetDays} day') AND date(?)
        `
      )
      .get(baseDate, baseDate);
    return Number(row?.count || 0);
  };

  const totalRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM users
        WHERE is_admin = 0
      `
    )
    .get();

  return {
    today: sumRange(1),
    week: sumRange(7),
    month: sumRange(30),
    year: sumRange(365),
    total: Number(totalRow?.count || 0)
  };
}

function getCachedAdminDashboardStats(force = false) {
  const now = Date.now();
  if (!force && dashboardStatsCache.value && dashboardStatsCache.expiresAt > now) {
    return dashboardStatsCache.value;
  }

  const fresh = buildAdminDashboardStats();
  dashboardStatsCache = {
    value: fresh,
    expiresAt: now + DASHBOARD_STATS_CACHE_TTL_MS
  };
  return fresh;
}

function buildAdminDashboardStats() {
  const today = toKstDate();
  const toCount = (value) => Number(value || 0);
  const toAmount = (value) => Number(value || 0);
  const toRate = (numerator, denominator) => {
    const safeDenominator = Number(denominator || 0);
    if (safeDenominator <= 0) {
      return 0;
    }
    return Number(((Number(numerator || 0) / safeDenominator) * 100).toFixed(1));
  };

  const usersRow = db
    .prepare(
      `
        SELECT
          SUM(CASE WHEN is_admin = 0 AND is_blocked = 0 THEN 1 ELSE 0 END) AS active_member_count,
          SUM(CASE WHEN is_admin = 0 AND is_blocked = 1 THEN 1 ELSE 0 END) AS blocked_member_count,
          SUM(CASE WHEN is_admin = 0 THEN 1 ELSE 0 END) AS total_member_count
        FROM users
      `
    )
    .get();

  const actionRow = db
    .prepare(
      `
        SELECT
          SUM(CASE WHEN o.status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_review_count,
          SUM(
            CASE
              WHEN o.status = 'PENDING_REVIEW' AND datetime(o.created_at) <= datetime('now', '-12 hour') THEN 1
              ELSE 0
            END
          ) AS payment_waiting_count,
          SUM(
            CASE
              WHEN o.status IN ('READY_TO_SHIP', 'SHIPPING', 'DELIVERED')
                AND NOT EXISTS (
                  SELECT 1
                  FROM qc_items q
                  WHERE q.order_no = o.order_no
                )
              THEN 1
              ELSE 0
            END
          ) AS qc_missing_count
        FROM orders o
      `
    )
    .get();

  const orderRow = db
    .prepare(
      `
        SELECT
          COUNT(*) AS total_count,
          SUM(CASE WHEN status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_review_count,
          SUM(CASE WHEN status = 'ORDER_CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_count,
          SUM(CASE WHEN status = 'READY_TO_SHIP' THEN 1 ELSE 0 END) AS ready_to_ship_count,
          SUM(CASE WHEN status = 'SHIPPING' THEN 1 ELSE 0 END) AS shipping_count,
          SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
        FROM orders
      `
    )
    .get();

  const delayRow = db
    .prepare(
      `
        SELECT
          SUM(
            CASE
              WHEN status = 'PENDING_REVIEW' AND datetime(created_at) <= datetime('now', '-1 day') THEN 1
              ELSE 0
            END
          ) AS pending_over_24h,
          SUM(
            CASE
              WHEN status = 'ORDER_CONFIRMED'
                AND datetime(COALESCE(checked_at, created_at)) <= datetime('now', '-2 day')
              THEN 1
              ELSE 0
            END
          ) AS confirmed_over_48h,
          SUM(
            CASE
              WHEN status = 'READY_TO_SHIP'
                AND datetime(COALESCE(ready_to_ship_at, checked_at, created_at)) <= datetime('now', '-3 day')
              THEN 1
              ELSE 0
            END
          ) AS ready_over_72h,
          SUM(
            CASE
              WHEN status = 'SHIPPING'
                AND datetime(COALESCE(shipping_started_at, ready_to_ship_at, checked_at, created_at)) <= datetime('now', '-7 day')
              THEN 1
              ELSE 0
            END
          ) AS shipping_over_7d
        FROM orders
      `
    )
    .get();

  const paymentRow = db
    .prepare(
      `
        SELECT
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
              THEN 1
              ELSE 0
            END
          ) AS total_count,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
              THEN total_price
              ELSE 0
            END
          ) AS total_amount,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) = date(?)
              THEN 1
              ELSE 0
            END
          ) AS today_count,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) = date(?)
              THEN total_price
              ELSE 0
            END
          ) AS today_amount,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-6 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS week_count,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-6 day') AND date(?)
              THEN total_price
              ELSE 0
            END
          ) AS week_amount,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-29 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS month_count,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-29 day') AND date(?)
              THEN total_price
              ELSE 0
            END
          ) AS month_amount
        FROM orders
      `
    )
    .get(today, today, today, today, today, today, today, today, today, today);

  const inquiryOpsRow = db
    .prepare(
      `
        SELECT
          SUM(CASE WHEN COALESCE(TRIM(reply_content), '') = '' THEN 1 ELSE 0 END) AS waiting_count,
          SUM(
            CASE
              WHEN COALESCE(TRIM(reply_content), '') = ''
                AND datetime(created_at) <= datetime('now', '-1 day')
              THEN 1
              ELSE 0
            END
          ) AS over_24h_count,
          SUM(
            CASE
              WHEN COALESCE(TRIM(reply_content), '') != ''
                AND date(datetime(replied_at, '+9 hours')) = date(?)
              THEN 1
              ELSE 0
            END
          ) AS replied_today_count,
          AVG(
            CASE
              WHEN COALESCE(TRIM(reply_content), '') != '' AND replied_at IS NOT NULL
              THEN (julianday(replied_at) - julianday(created_at)) * 24
              ELSE NULL
            END
          ) AS avg_reply_hours
        FROM inquiries
      `
    )
    .get(today);

  const funnelRows = db
    .prepare(
      `
        SELECT
          event_key,
          SUM(
            CASE
              WHEN event_date BETWEEN date(?, '-29 day') AND date(?) THEN event_count
              ELSE 0
            END
          ) AS window_count,
          SUM(event_count) AS total_count
        FROM daily_funnel_events
        GROUP BY event_key
      `
    )
    .all(today, today);

  const funnelFallbackRow = db
    .prepare(
      `
        SELECT
          COUNT(*) AS order_created_total,
          SUM(
            CASE
              WHEN date(datetime(created_at, '+9 hours')) BETWEEN date(?, '-29 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS order_created_window,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
              THEN 1
              ELSE 0
            END
          ) AS payment_confirmed_total,
          SUM(
            CASE
              WHEN UPPER(TRIM(status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-29 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS payment_confirmed_window
        FROM orders
      `
    )
    .get(today, today, today, today);

  const funnelMap = new Map(
    funnelRows.map((row) => [
      String(row.event_key || ''),
      {
        windowCount: toCount(row.window_count),
        totalCount: toCount(row.total_count)
      }
    ])
  );

  const getFunnelCount = (eventKey, scope = 'window') => {
    const bucket = funnelMap.get(eventKey) || { windowCount: 0, totalCount: 0 };
    return scope === 'total' ? toCount(bucket.totalCount) : toCount(bucket.windowCount);
  };

  const orderCreatedWindow = Math.max(
    getFunnelCount(FUNNEL_EVENT.ORDER_CREATED, 'window'),
    toCount(funnelFallbackRow?.order_created_window)
  );
  const orderCreatedTotal = Math.max(
    getFunnelCount(FUNNEL_EVENT.ORDER_CREATED, 'total'),
    toCount(funnelFallbackRow?.order_created_total)
  );
  const paymentConfirmedWindow = Math.max(
    getFunnelCount(FUNNEL_EVENT.PAYMENT_CONFIRMED, 'window'),
    toCount(funnelFallbackRow?.payment_confirmed_window)
  );
  const paymentConfirmedTotal = Math.max(
    getFunnelCount(FUNNEL_EVENT.PAYMENT_CONFIRMED, 'total'),
    toCount(funnelFallbackRow?.payment_confirmed_total)
  );

  const orderGroupRows = db
    .prepare(
      `
        SELECT
          p.category_group,
          COUNT(*) AS total_count,
          SUM(CASE WHEN o.status = 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS pending_review_count,
          SUM(CASE WHEN o.status = 'ORDER_CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_count,
          SUM(CASE WHEN o.status = 'READY_TO_SHIP' THEN 1 ELSE 0 END) AS ready_to_ship_count,
          SUM(CASE WHEN o.status = 'SHIPPING' THEN 1 ELSE 0 END) AS shipping_count,
          SUM(CASE WHEN o.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
        FROM orders o
        JOIN products p ON p.id = o.product_id
        GROUP BY p.category_group
      `
    )
    .all();

  const orderGroupMap = new Map(
    orderGroupRows.map((row) => [
      String(row.category_group || ''),
      {
        groupName: String(row.category_group || ''),
        totalCount: Number(row.total_count || 0),
        pendingReview: Number(row.pending_review_count || 0),
        confirmed: Number(row.confirmed_count || 0),
        readyToShip: Number(row.ready_to_ship_count || 0),
        shipping: Number(row.shipping_count || 0),
        delivered: Number(row.delivered_count || 0)
      }
    ])
  );

  const configuredGroupNames = getProductGroupConfigs().map((group) => group.key);
  const observedOrderGroupNames = orderGroupRows
    .map((row) => String(row.category_group || '').trim())
    .filter(Boolean)
    .filter((groupName, idx, arr) => arr.indexOf(groupName) === idx);
  const orderGroupNames = [
    ...configuredGroupNames,
    ...observedOrderGroupNames.filter((groupName) => !configuredGroupNames.includes(groupName))
  ];

  const orderByGroup = orderGroupNames.map((groupName) => {
    const found = orderGroupMap.get(groupName);
    return (
      found || {
        groupName,
        totalCount: 0,
        pendingReview: 0,
        confirmed: 0,
        readyToShip: 0,
        shipping: 0,
        delivered: 0
      }
    );
  });

  const boardRow = db
    .prepare(
      `
        SELECT
          (SELECT COUNT(*) FROM notices) AS notice_count,
          (SELECT COUNT(*) FROM news_posts) AS news_count,
          (SELECT COUNT(*) FROM qc_items) AS qc_count,
          (SELECT COUNT(*) FROM reviews) AS review_count,
          (SELECT COUNT(*) FROM inquiries) AS inquiry_count
      `
    )
    .get();

  const orderAlertRow = db
    .prepare(
      `
        SELECT
          SUM(
            CASE
              WHEN status = 'PENDING_REVIEW' AND datetime(created_at) <= datetime('now', '-1 day') THEN 1
              ELSE 0
            END
          ) AS stale_pending_count,
          SUM(
            CASE
              WHEN status = 'SHIPPING'
                AND COALESCE(datetime(shipping_started_at), datetime(created_at)) <= datetime('now', '-7 day')
              THEN 1
              ELSE 0
            END
          ) AS long_shipping_count
        FROM orders
      `
    )
    .get();

  const shopGroupRows = db
    .prepare(
      `
        SELECT
          category_group,
          SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_count,
          SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS hidden_count
        FROM products
        GROUP BY category_group
      `
    )
    .all();

  const groupMap = new Map(
    shopGroupRows.map((row) => [String(row.category_group || ''), {
      groupName: String(row.category_group || ''),
      totalCount: Number(row.active_count || 0) + Number(row.hidden_count || 0),
      activeCount: Number(row.active_count || 0),
      hiddenCount: Number(row.hidden_count || 0)
    }])
  );

  const observedShopGroupNames = shopGroupRows
    .map((row) => String(row.category_group || '').trim())
    .filter(Boolean)
    .filter((groupName, idx, arr) => arr.indexOf(groupName) === idx);
  const shopGroupNames = [
    ...configuredGroupNames,
    ...observedShopGroupNames.filter((groupName) => !configuredGroupNames.includes(groupName))
  ];

  const shopByGroup = shopGroupNames.map((groupName) => {
    const found = groupMap.get(groupName);
    return {
      groupName,
      totalCount: found ? found.totalCount : 0,
      activeCount: found ? found.activeCount : 0,
      hiddenCount: found ? found.hiddenCount : 0
    };
  });

  const shopTotalRow = db
    .prepare(
      `
        SELECT
          COUNT(*) AS total_count,
          SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_count
        FROM products
      `
    )
    .get();

  const pendingInquiries = toCount(inquiryOpsRow?.waiting_count);
  const overdueInquiries = toCount(inquiryOpsRow?.over_24h_count);
  const actionsSummary = {
    uncheckedOrders: toCount(actionRow?.pending_review_count),
    paymentWaiting: toCount(actionRow?.payment_waiting_count),
    pendingInquiries,
    qcPending: toCount(actionRow?.qc_missing_count),
    totalUrgent:
      toCount(actionRow?.payment_waiting_count) +
      overdueInquiries +
      toCount(actionRow?.qc_missing_count)
  };

  return {
    users: {
      active: toCount(usersRow?.active_member_count),
      blocked: toCount(usersRow?.blocked_member_count),
      total: toCount(usersRow?.total_member_count)
    },
    signupCounts: getMemberSignupRangeSummary(today),
    memberVisits: getVisitRangeSummary(today, 'member_visit_count'),
    guestVisits: getVisitRangeSummary(today, 'guest_visit_count'),
    totalVisits: getVisitRangeSummary(today, 'visit_count'),
    boardCounts: {
      notice: toCount(boardRow?.notice_count),
      news: toCount(boardRow?.news_count),
      qc: toCount(boardRow?.qc_count),
      review: toCount(boardRow?.review_count),
      inquiry: toCount(boardRow?.inquiry_count)
    },
    shopCounts: {
      total: toCount(shopTotalRow?.total_count),
      active: toCount(shopTotalRow?.active_count),
      byGroup: shopByGroup
    },
    orderCounts: {
      total: toCount(orderRow?.total_count),
      pendingReview: toCount(orderRow?.pending_review_count),
      confirmed: toCount(orderRow?.confirmed_count),
      readyToShip: toCount(orderRow?.ready_to_ship_count),
      shipping: toCount(orderRow?.shipping_count),
      delivered: toCount(orderRow?.delivered_count),
      byGroup: orderByGroup
    },
    orderAlerts: {
      stalePending: toCount(orderAlertRow?.stale_pending_count),
      longShipping: toCount(orderAlertRow?.long_shipping_count)
    },
    actionItems: actionsSummary,
    delayWarnings: {
      pendingOver24h: toCount(delayRow?.pending_over_24h),
      confirmedOver48h: toCount(delayRow?.confirmed_over_48h),
      readyOver72h: toCount(delayRow?.ready_over_72h),
      shippingOver7d: toCount(delayRow?.shipping_over_7d)
    },
    inquiryOps: {
      pending: pendingInquiries,
      overdue24h: overdueInquiries,
      repliedToday: toCount(inquiryOpsRow?.replied_today_count),
      avgReplyHours: Number(toCount(inquiryOpsRow?.avg_reply_hours).toFixed(1))
    },
    paymentPerformance: {
      todayCount: toCount(paymentRow?.today_count),
      todayAmount: toAmount(paymentRow?.today_amount),
      weekCount: toCount(paymentRow?.week_count),
      weekAmount: toAmount(paymentRow?.week_amount),
      monthCount: toCount(paymentRow?.month_count),
      monthAmount: toAmount(paymentRow?.month_amount),
      totalCount: toCount(paymentRow?.total_count),
      totalAmount: toAmount(paymentRow?.total_amount),
      confirmRate: toRate(paymentRow?.total_count, orderRow?.total_count)
    },
    conversionFunnel: {
      windowDays: 30,
      window: {
        productViews: getFunnelCount(FUNNEL_EVENT.PRODUCT_VIEW, 'window'),
        purchaseViews: getFunnelCount(FUNNEL_EVENT.PURCHASE_VIEW, 'window'),
        orderCreated: orderCreatedWindow,
        paymentConfirmed: paymentConfirmedWindow
      },
      total: {
        productViews: getFunnelCount(FUNNEL_EVENT.PRODUCT_VIEW, 'total'),
        purchaseViews: getFunnelCount(FUNNEL_EVENT.PURCHASE_VIEW, 'total'),
        orderCreated: orderCreatedTotal,
        paymentConfirmed: paymentConfirmedTotal
      },
      rates: {
        viewToPurchase: toRate(
          getFunnelCount(FUNNEL_EVENT.PURCHASE_VIEW, 'window'),
          getFunnelCount(FUNNEL_EVENT.PRODUCT_VIEW, 'window')
        ),
        purchaseToOrder: toRate(
          orderCreatedWindow,
          getFunnelCount(FUNNEL_EVENT.PURCHASE_VIEW, 'window')
        ),
        orderToPayment: toRate(paymentConfirmedWindow, orderCreatedWindow),
        viewToPayment: toRate(
          paymentConfirmedWindow,
          getFunnelCount(FUNNEL_EVENT.PRODUCT_VIEW, 'window')
        )
      }
    }
  };
}

function buildSecurityPanelData(lang = 'ko', options = {}) {
  const isEn = lang === 'en';
  const logFilters = {
    from: normalizeDateInput(options.logFrom || ''),
    to: normalizeDateInput(options.logTo || ''),
    adminId: normalizeOptionalId(options.logAdminId || '')
  };
  const alertFilters = {
    from: normalizeDateInput(options.alertFrom || ''),
    to: normalizeDateInput(options.alertTo || ''),
    adminId: normalizeOptionalId(options.alertAdminId || '')
  };

  const logWhere = ['1=1'];
  const logParams = [];
  if (logFilters.adminId > 0) {
    logWhere.push('admin_user_id = ?');
    logParams.push(logFilters.adminId);
  }
  if (logFilters.from) {
    logWhere.push("date(datetime(created_at, '+9 hours')) >= date(?)");
    logParams.push(logFilters.from);
  }
  if (logFilters.to) {
    logWhere.push("date(datetime(created_at, '+9 hours')) <= date(?)");
    logParams.push(logFilters.to);
  }
  const logWhereSql = logWhere.join(' AND ');

  const totalLogsRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM admin_activity_logs
        WHERE ${logWhereSql}
      `
    )
    .get(...logParams);
  const totalLogCount = Number(totalLogsRow?.count || 0);
  const logTotalPages = Math.max(1, Math.ceil(totalLogCount / SECURITY_PAGE_SIZE));
  const logPage = clampPage(options.logPage, logTotalPages);
  const logOffset = (logPage - 1) * SECURITY_PAGE_SIZE;

  const adminUsers = db
    .prepare(
      `
        SELECT
          u.id,
          u.username,
          u.full_name,
          u.email,
          u.phone,
          u.admin_role,
          u.admin_otp_enabled,
          u.created_at,
          (
            SELECT MAX(l.created_at)
            FROM admin_activity_logs l
            WHERE l.admin_user_id = u.id
              AND l.action_type = 'LOGIN_SUCCESS'
          ) AS last_login_at
        FROM users u
        WHERE u.is_admin = 1
        ORDER BY
          CASE WHEN u.admin_role = 'PRIMARY' THEN 0 ELSE 1 END ASC,
          u.id ASC
      `
    )
    .all()
    .map((row) => {
      const role = normalizeAdminRole(row.admin_role);
      return {
        ...row,
        admin_role: role,
        admin_role_label:
          role === ADMIN_ROLE.PRIMARY
            ? (isEn ? 'Primary Admin' : '메인관리자')
            : (isEn ? 'Sub Admin' : '서브관리자'),
        is_primary: role === ADMIN_ROLE.PRIMARY,
        otp_enabled: Number(row.admin_otp_enabled || 0) === 1
      };
    });

  const subAdminLogRows = db
    .prepare(
      `
        SELECT
          id,
          admin_user_id,
          admin_username,
          admin_role,
          ip_address,
          user_agent,
          method,
          path,
          action_type,
          detail,
          created_at
        FROM admin_activity_logs
        WHERE ${logWhereSql}
        ORDER BY id DESC
        LIMIT ?
        OFFSET ?
      `
    )
    .all(...logParams, SECURITY_PAGE_SIZE, logOffset);

  const alertWhere = ['1=1'];
  const alertParams = [];
  if (alertFilters.adminId > 0) {
    alertWhere.push('actor_admin_user_id = ?');
    alertParams.push(alertFilters.adminId);
  }
  if (alertFilters.from) {
    alertWhere.push("date(datetime(created_at, '+9 hours')) >= date(?)");
    alertParams.push(alertFilters.from);
  }
  if (alertFilters.to) {
    alertWhere.push("date(datetime(created_at, '+9 hours')) <= date(?)");
    alertParams.push(alertFilters.to);
  }
  const alertWhereSql = alertWhere.join(' AND ');

  const totalAlertsRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM admin_security_alerts
        WHERE ${alertWhereSql}
      `
    )
    .get(...alertParams);
  const totalAlertCount = Number(totalAlertsRow?.count || 0);
  const alertTotalPages = Math.max(1, Math.ceil(totalAlertCount / SECURITY_PAGE_SIZE));
  const alertPage = clampPage(options.alertPage, alertTotalPages);
  const alertOffset = (alertPage - 1) * SECURITY_PAGE_SIZE;

  const securityAlertRows = db
    .prepare(
      `
        SELECT
          id,
          actor_admin_user_id,
          actor_username,
          actor_role,
          ip_address,
          method,
          path,
          reason,
          detail,
          created_at,
          resolved_at
        FROM admin_security_alerts
        WHERE ${alertWhereSql}
        ORDER BY
          CASE WHEN resolved_at IS NULL THEN 0 ELSE 1 END ASC,
          id DESC
        LIMIT ?
        OFFSET ?
      `
    )
    .all(...alertParams, SECURITY_PAGE_SIZE, alertOffset);

  const ipCandidates = [
    ...subAdminLogRows.map((row) => normalizeIpAddress(row.ip_address || '')),
    ...securityAlertRows.map((row) => normalizeIpAddress(row.ip_address || ''))
  ].filter(Boolean);
  const ipGeoCacheMap = getIpGeoCacheMapByIps(ipCandidates);
  const queuedLookupIps = new Set();
  let queuedLookupCount = 0;

  const tryQueueIpGeoLookup = (ipAddress) => {
    const normalizedIp = normalizeIpAddress(ipAddress);
    if (!isPublicIpAddress(normalizedIp)) {
      return;
    }
    if (queuedLookupCount >= IP_GEO_MAX_LOOKUP_PER_RENDER) {
      return;
    }
    const cached = ipGeoCacheMap.get(normalizedIp);
    if (cached && isIpGeoCacheFresh(cached)) {
      return;
    }
    if (queuedLookupIps.has(normalizedIp)) {
      return;
    }
    queuedLookupIps.add(normalizedIp);
    queuedLookupCount += 1;
    queueIpGeolocationLookup(normalizedIp);
  };

  const subAdminLogs = subAdminLogRows.map((row) => {
    const normalizedIp = normalizeIpAddress(row.ip_address || '');
    tryQueueIpGeoLookup(normalizedIp);
    return {
      ...row,
      ip_address: normalizedIp || String(row.ip_address || ''),
      ip_location: getIpLocationLabel(normalizedIp, ipGeoCacheMap, lang),
      user_agent: String(row.user_agent || '')
    };
  });

  const securityAlerts = securityAlertRows.map((row) => {
    const normalizedIp = normalizeIpAddress(row.ip_address || '');
    tryQueueIpGeoLookup(normalizedIp);
    return {
      ...row,
      ip_address: normalizedIp || String(row.ip_address || ''),
      ip_location: getIpLocationLabel(normalizedIp, ipGeoCacheMap, lang),
      reason_label: getSecurityAlertReasonLabel(row.reason, lang)
    };
  });

  const unresolvedAlertsRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM admin_security_alerts
        WHERE resolved_at IS NULL
          AND ${alertWhereSql}
      `
    )
    .get(...alertParams);

  return {
    adminUsers,
    subAdminLogs,
    securityAlerts,
    unresolvedAlertsCount: Number(unresolvedAlertsRow?.count || 0),
    filters: {
      log: logFilters,
      alert: alertFilters
    },
    pagination: {
      log: {
        page: logPage,
        totalPages: logTotalPages,
        totalCount: totalLogCount,
        pageSize: SECURITY_PAGE_SIZE
      },
      alert: {
        page: alertPage,
        totalPages: alertTotalPages,
        totalCount: totalAlertCount,
        pageSize: SECURITY_PAGE_SIZE
      }
    }
  };
}

function buildMemberManagePanelData(lang = 'ko', options = {}) {
  const availableGroupsRaw = getProductGroupConfigs();
  const availableGroupKeys = availableGroupsRaw.map((group) => group.key);
  const availableGroups = availableGroupsRaw.map((group) => ({
    key: group.key,
    label: lang === 'en' ? (group.labelEn || group.key) : (group.labelKo || group.key)
  }));
  const includedGroups = getMemberLevelIncludedGroupsSetting(availableGroupKeys);
  const levelRules = getMemberLevelRulesSetting();
  const pointRateMap = getMemberLevelPointRateMapSetting(levelRules);

  const filters = {
    section: normalizeMemberManageSection(options.section || ''),
    keyword: String(options.keyword || '').trim().slice(0, 120)
  };
  filters.levelRuleFilter = String(options.levelRuleFilter || 'all');
  if (filters.levelRuleFilter !== 'all' && !levelRules.some((rule) => rule.id === filters.levelRuleFilter)) {
    filters.levelRuleFilter = 'all';
  }

  const decorateRowsWithLevel = (rows = []) => {
    const userIds = rows.map((row) => Number(row.id)).filter((id) => Number.isInteger(id) && id > 0);
    const totalMap = getMemberAccumulatedTotalsMap(userIds, includedGroups);
    return rows.map((row) => {
      const totalAmount = Number(totalMap.get(Number(row.id)) || 0);
      const levelRule = resolveMemberLevelByAmount(totalAmount, levelRules);
      const levelId = levelRule?.id || '';
      const levelNameKo = normalizeMemberLevelName(levelRule?.nameKo || levelRule?.name || '', '');
      const levelNameEn = normalizeMemberLevelName(levelRule?.nameEn || levelRule?.name || '', '');
      const levelName = levelRule
        ? getMemberLevelDisplayName(levelRule, lang)
        : (lang === 'en' ? 'Unassigned' : '미지정');
      const levelColorTheme = normalizeMemberLevelColorTheme(levelRule?.colorTheme || levelRule?.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME);
      const levelPointRate = levelId
        ? parsePointRate(pointRateMap[levelId], 0)
        : getLegacyPurchasePointRateSetting();
      const availablePoints = parseNonNegativeInt(row.reward_points, 0);
      const orderAwardedPoints = parseNonNegativeInt(row.awarded_points_total, 0);
      const usedPoints = Math.max(0, orderAwardedPoints - availablePoints);
      const receivedPoints = availablePoints + usedPoints;
      return {
        ...row,
        member_uid: String(row.member_uid || '').trim(),
        nickname: String(row.nickname || '').trim() || String(row.username || '').trim(),
        agreed_terms: Number(row.agreed_terms) === 1,
        is_blocked: Number(row.is_blocked) === 1,
        order_count: Number(row.order_count || 0),
        level_id: levelId,
        level_name: levelName,
        level_name_ko: levelNameKo,
        level_name_en: levelNameEn,
        level_color_theme: levelColorTheme,
        level_point_rate: levelPointRate,
        level_threshold_amount: Number(levelRule?.thresholdAmount || 0),
        level_operator: levelRule?.operator || MEMBER_LEVEL_OPERATORS.GTE,
        level_operator_label: getMemberLevelOperatorLabel(levelRule?.operator || MEMBER_LEVEL_OPERATORS.GTE, lang),
        level_applied_amount: totalAmount,
        awarded_points_total: orderAwardedPoints,
        used_points_total: usedPoints,
        reward_points_available: availablePoints,
        reward_points_received: receivedPoints
      };
    });
  };

  const buildLevelSummary = (memberRows = []) => {
    const countMap = new Map();
    memberRows.forEach((member) => {
      const key = member.level_id || '__unassigned__';
      countMap.set(key, Number(countMap.get(key) || 0) + 1);
    });
    const summaries = levelRules.map((rule) => ({
      id: rule.id,
      name: getMemberLevelDisplayName(rule, lang),
      nameKo: normalizeMemberLevelName(rule.nameKo || rule.name || '', ''),
      nameEn: normalizeMemberLevelName(rule.nameEn || rule.name || '', ''),
      colorTheme: normalizeMemberLevelColorTheme(rule.colorTheme || rule.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME),
      operator: rule.operator,
      operatorLabel: getMemberLevelOperatorLabel(rule.operator, lang),
      thresholdAmount: Number(rule.thresholdAmount || 0),
      pointRate: parsePointRate(pointRateMap[rule.id], 0),
      memberCount: Number(countMap.get(rule.id) || 0)
    }));
    summaries.push({
      id: '__unassigned__',
      name: lang === 'en' ? 'Unassigned' : '미지정',
      nameKo: '미지정',
      nameEn: 'Unassigned',
      colorTheme: PRODUCT_BADGE_DEFAULT_COLOR_THEME,
      operator: '',
      operatorLabel: '',
      thresholdAmount: 0,
      pointRate: getLegacyPurchasePointRateSetting(),
      memberCount: Number(countMap.get('__unassigned__') || 0)
    });
    return summaries;
  };

  const where = ['u.is_admin = 0'];
  const params = [];
  if (filters.section === 'blocked') {
    where.push('u.is_blocked = 1');
  } else if (filters.section === 'active') {
    where.push('u.is_blocked = 0');
  }

  if (filters.keyword) {
    where.push(
      '(u.member_uid LIKE ? OR u.username LIKE ? OR u.nickname LIKE ? OR u.full_name LIKE ? OR u.email LIKE ? OR u.phone LIKE ?)'
    );
    const likeKeyword = `%${filters.keyword}%`;
    params.push(likeKeyword, likeKeyword, likeKeyword, likeKeyword, likeKeyword, likeKeyword);
  }

  const whereSql = where.join(' AND ');
  const baseMemberSelectSql = `
    SELECT
      u.id,
      u.member_uid,
      u.username,
      u.nickname,
      u.full_name,
      u.email,
      u.phone,
      u.agreed_terms,
      u.is_blocked,
      u.blocked_reason,
      u.blocked_at,
      u.reward_points,
      u.created_at,
      (
        SELECT COUNT(*)
        FROM orders o
        WHERE o.created_by_user_id = u.id
      ) AS order_count,
      (
        SELECT COALESCE(SUM(o.awarded_points), 0)
        FROM orders o
        WHERE o.created_by_user_id = u.id
      ) AS awarded_points_total
    FROM users u
    WHERE ${whereSql}
    ORDER BY u.id DESC
  `;

  let members = [];
  let page = 1;
  let totalPages = 1;
  let totalCount = 0;

  if (filters.section === 'levels') {
    members = [];
    totalCount = 0;
    totalPages = 1;
    page = 1;
  } else {
    const allRows = db.prepare(baseMemberSelectSql).all(...params);
    let decoratedRows = decorateRowsWithLevel(allRows);
    if (filters.levelRuleFilter !== 'all') {
      decoratedRows = decoratedRows.filter((member) => member.level_id === filters.levelRuleFilter);
    }
    totalCount = decoratedRows.length;
    totalPages = Math.max(1, Math.ceil(totalCount / MEMBER_PAGE_SIZE));
    page = clampPage(options.page, totalPages);
    const offset = (page - 1) * MEMBER_PAGE_SIZE;
    members = decoratedRows.slice(offset, offset + MEMBER_PAGE_SIZE);
  }

  const allMembersForSummary = decorateRowsWithLevel(
    db
      .prepare(
        `
          SELECT
            u.id,
            u.member_uid,
            u.username,
            u.nickname,
            u.full_name,
            u.email,
            u.phone,
            u.agreed_terms,
            u.is_blocked,
            u.blocked_reason,
            u.blocked_at,
            u.reward_points,
            u.created_at,
            (
              SELECT COUNT(*)
              FROM orders o
              WHERE o.created_by_user_id = u.id
            ) AS order_count,
            (
              SELECT COALESCE(SUM(o.awarded_points), 0)
              FROM orders o
              WHERE o.created_by_user_id = u.id
            ) AS awarded_points_total
          FROM users u
          WHERE u.is_admin = 0
          ORDER BY u.id DESC
        `
      )
      .all()
  );
  const levelSummaries = buildLevelSummary(allMembersForSummary);
  const levelRulesWithRates = levelRules.map((rule) => ({
    ...rule,
    nameKo: normalizeMemberLevelName(rule.nameKo || rule.name || '', ''),
    nameEn: normalizeMemberLevelName(rule.nameEn || rule.name || '', ''),
    displayName: getMemberLevelDisplayName(rule, lang),
    colorTheme: normalizeMemberLevelColorTheme(rule.colorTheme || rule.color_theme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME),
    pointRate: parsePointRate(pointRateMap[rule.id], 0),
    operatorLabel: getMemberLevelOperatorLabel(rule.operator, lang)
  }));

  return {
    members,
    filters,
    levelConfig: {
      availableGroups,
      includedGroups,
      rules: levelRulesWithRates,
      pointRates: pointRateMap
    },
    levelSummaries,
    pagination: {
      page,
      totalPages,
      totalCount,
      pageSize: MEMBER_PAGE_SIZE
    }
  };
}

function createSalesMetricBucket() {
  return {
    orderCount: 0,
    salesKrw: 0,
    costKrw: 0,
    marginKrw: 0,
    realMarginKrw: 0
  };
}

function buildAdminSalesDailyData(lang = 'ko', options = {}) {
  const groupConfigs = Array.isArray(options.groupConfigs) && options.groupConfigs.length > 0
    ? options.groupConfigs
    : getProductGroupConfigs();
  const availableGroupKeys = groupConfigs.map((group) => group.key);
  const groupFilter = normalizeAdminOrderGroupFilter(options.groupFilter || '', availableGroupKeys);
  const dateFrom = normalizeDateInput(options.dateFrom || '');
  const dateTo = normalizeDateInput(options.dateTo || '');

  const whereParts = [
    "UPPER(TRIM(o.status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED', 'CANCELLED', 'CANCELED', 'ORDER_CANCELLED', 'ORDER_CANCELED')"
  ];
  const params = [];

  if (dateFrom) {
    whereParts.push("date(datetime(COALESCE(o.checked_at, o.created_at), '+9 hours')) >= date(?)");
    params.push(dateFrom);
  }
  if (dateTo) {
    whereParts.push("date(datetime(COALESCE(o.checked_at, o.created_at), '+9 hours')) <= date(?)");
    params.push(dateTo);
  }
  if (groupFilter !== 'all') {
    whereParts.push('p.category_group = ?');
    params.push(groupFilter);
  }

  const rows = db
    .prepare(
      `
        SELECT
          date(datetime(COALESCE(o.checked_at, o.created_at), '+9 hours')) AS sale_date,
          p.category_group AS category_group,
          COUNT(*) AS order_count,
          COALESCE(SUM(o.total_price), 0) AS sales_total_krw,
          COALESCE(SUM(COALESCE(o.sales_cost_krw_snapshot, 0)), 0) AS cost_total_krw,
          COALESCE(
            SUM(
              CASE
                WHEN COALESCE(TRIM(o.sales_synced_at), '') = '' THEN
                  o.total_price - COALESCE(o.sales_cost_krw_snapshot, 0)
                ELSE
                  COALESCE(
                    o.sales_margin_krw_snapshot,
                    o.total_price - COALESCE(o.sales_cost_krw_snapshot, 0)
                  )
              END
            ),
            0
          ) AS margin_total_krw,
          COALESCE(
            SUM(
              CASE
                WHEN COALESCE(TRIM(o.sales_synced_at), '') = '' THEN
                  o.total_price - COALESCE(o.sales_cost_krw_snapshot, 0)
                ELSE
                  COALESCE(
                    o.sales_real_margin_krw_snapshot,
                    COALESCE(
                      o.sales_margin_krw_snapshot,
                      o.total_price - COALESCE(o.sales_cost_krw_snapshot, 0)
                    )
                  )
              END
            ),
            0
          ) AS real_margin_total_krw
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE ${whereParts.join(' AND ')}
        GROUP BY sale_date, p.category_group
        ORDER BY sale_date DESC, p.category_group ASC
      `
    )
    .all(...params);

  const observedGroupKeys = rows
    .map((row) => normalizeProductGroupKey(row.category_group || ''))
    .filter(Boolean)
    .filter((groupName, idx, arr) => arr.indexOf(groupName) === idx);

  let selectedGroupKeys = [];
  if (groupFilter === 'all') {
    selectedGroupKeys = [
      ...availableGroupKeys,
      ...observedGroupKeys.filter((groupName) => !availableGroupKeys.includes(groupName))
    ];
  } else {
    selectedGroupKeys = [groupFilter];
  }

  const labelsFromConfig = getProductGroupLabels(groupConfigs, lang);
  const groups = selectedGroupKeys.map((groupKey) => ({
    key: groupKey,
    label: labelsFromConfig[groupKey] || groupKey
  }));

  const dailyMap = new Map();
  const rangeTotals = createSalesMetricBucket();

  for (const row of rows) {
    const saleDate = normalizeSalesDate(row.sale_date || '');
    const groupKey = normalizeProductGroupKey(row.category_group || '');
    if (!saleDate || !groupKey) {
      continue;
    }
    if (!dailyMap.has(saleDate)) {
      dailyMap.set(saleDate, {
        date: saleDate,
        metricsByGroup: {},
        total: createSalesMetricBucket()
      });
    }

    const dateBucket = dailyMap.get(saleDate);
    const groupBucket = dateBucket.metricsByGroup[groupKey] || createSalesMetricBucket();
    groupBucket.orderCount += Number(row.order_count || 0);
    groupBucket.salesKrw += Number(row.sales_total_krw || 0);
    groupBucket.costKrw += Number(row.cost_total_krw || 0);
    groupBucket.marginKrw += Number(row.margin_total_krw || 0);
    groupBucket.realMarginKrw += Number(row.real_margin_total_krw || 0);
    dateBucket.metricsByGroup[groupKey] = groupBucket;

    dateBucket.total.orderCount += Number(row.order_count || 0);
    dateBucket.total.salesKrw += Number(row.sales_total_krw || 0);
    dateBucket.total.costKrw += Number(row.cost_total_krw || 0);
    dateBucket.total.marginKrw += Number(row.margin_total_krw || 0);
    dateBucket.total.realMarginKrw += Number(row.real_margin_total_krw || 0);

    rangeTotals.orderCount += Number(row.order_count || 0);
    rangeTotals.salesKrw += Number(row.sales_total_krw || 0);
    rangeTotals.costKrw += Number(row.cost_total_krw || 0);
    rangeTotals.marginKrw += Number(row.margin_total_krw || 0);
    rangeTotals.realMarginKrw += Number(row.real_margin_total_krw || 0);
  }

  const dailyRows = Array.from(dailyMap.values())
    .map((item) => {
      const metricsByGroup = {};
      groups.forEach((group) => {
        metricsByGroup[group.key] = item.metricsByGroup[group.key] || createSalesMetricBucket();
      });
      return {
        date: item.date,
        metricsByGroup,
        total: item.total
      };
    })
    .sort((a, b) => (a.date < b.date ? 1 : -1));

  return {
    filters: {
      groupFilter,
      dateFrom,
      dateTo
    },
    groups,
    rows: dailyRows,
    totals: rangeTotals
  };
}

function buildAdminDashboardViewData(lang = 'ko', options = {}) {
  const securityOptions = options.securityOptions || {};
  const memberOptions = options.memberOptions || {};
  const salesOptions = options.salesOptions || {};
  const includeDashboardStats = options.includeDashboardStats !== false;
  const editProductId = normalizeOptionalId(options.productEditId || 0);
  const publicMenus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), { includeHidden: true });
  const dayThemeColors = getThemeColorConfig('day');
  const nightThemeColors = getThemeColorConfig('night');
  const dayThemeAssets = getThemeAssetConfig('day');
  const nightThemeAssets = getThemeAssetConfig('night');
  const heroSettings = getMainHeroSettings(lang);
  const contactInfoSetting = getSetting('contactInfo', '');
  const kakaoChatUrl = resolveKakaoChatUrl(
    contactInfoSetting,
    String(getSetting('kakaoChatUrl', '') || '').trim()
  );
  const settings = {
    siteName: getSetting('siteName', 'Chrono Lab'),
    headerColor: dayThemeColors.headerColor,
    headerLogoPath: dayThemeAssets.headerLogoPath,
    headerSymbolPath: dayThemeAssets.headerSymbolPath,
    footerLogoPath: dayThemeAssets.footerLogoPath,
    backgroundType: dayThemeAssets.backgroundType,
    backgroundValue:
      dayThemeAssets.backgroundType === 'image' && dayThemeAssets.backgroundImagePath
        ? dayThemeAssets.backgroundImagePath
        : dayThemeColors.backgroundColor,
    dayThemeColors,
    nightThemeColors,
    dayThemeAssets,
    nightThemeAssets,
    watermarkLogoPath: getBrandingWatermarkUrl(),
    bankAccountInfo: getSetting('bankAccountInfo', ''),
    signupBonusPoints: getSignupBonusPointsSetting(),
    reviewRewardPoints: getReviewRewardPointsSetting(),
    purchasePointRate: getLegacyPurchasePointRateSetting(),
    contactInfo: contactInfoSetting,
    kakaoChatUrl,
    businessInfo: getSetting('businessInfo', ''),
    footerBrandCopyKo: getSetting('footerBrandCopyKo', '심플하고 신뢰할 수 있는 시계 쇼핑.'),
    footerBrandCopyEn: getSetting('footerBrandCopyEn', 'Simple. Clean. Trusted watch shopping.'),
    languageDefault: getSetting('languageDefault', 'ko'),
    menusJson: JSON.stringify(publicMenus, null, 2),
    heroLeftTitleKo: heroSettings.leftTitleKo,
    heroLeftTitleEn: heroSettings.leftTitleEn,
    heroLeftSubtitleKo: heroSettings.leftSubtitleKo,
    heroLeftSubtitleEn: heroSettings.leftSubtitleEn,
    heroLeftCtaPath: heroSettings.leftCtaPath,
    heroLeftBackgroundType: heroSettings.leftBackgroundType,
    heroLeftBackgroundColor: heroSettings.leftBackgroundColor,
    heroLeftBackgroundImagePath: heroSettings.leftBackgroundImagePath,
    heroRightTitleKo: heroSettings.rightTitleKo,
    heroRightTitleEn: heroSettings.rightTitleEn,
    heroRightSubtitleKo: heroSettings.rightSubtitleKo,
    heroRightSubtitleEn: heroSettings.rightSubtitleEn,
    heroRightBackgroundColor: heroSettings.rightBackgroundColor,
    heroQuickMenuPaths: heroSettings.quickMenuPaths
  };

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupKeys = productGroupConfigs.map((group) => group.key);
  const salesGroupFilter = normalizeAdminOrderGroupFilter(salesOptions.groupFilter || '', productGroupKeys);
  const salesDateFrom = normalizeDateInput(salesOptions.dateFrom || '');
  const salesDateTo = normalizeDateInput(salesOptions.dateTo || '');
  const orderGroupFilter = normalizeAdminOrderGroupFilter(options.orderGroupFilter || '', productGroupKeys);
  const orderStatusFilter = normalizeAdminOrderStatusFilter(options.orderStatusFilter || '');
  const orderDateFrom = normalizeDateInput(options.orderDateFrom || '');
  const orderDateTo = normalizeDateInput(options.orderDateTo || '');
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, lang);
  const productBadgeDefinitions = getProductBadgeDefinitions();
  const productBadgeColorThemes = getProductBadgeColorThemeOptions();
  const products = attachProductBadges(
    db
      .prepare('SELECT * FROM products ORDER BY id DESC LIMIT 100')
      .all()
      .map((item) => decorateProductForView(item, productGroupMap.get(item.category_group)))
  );

  let editingProduct = null;
  if (editProductId > 0) {
    const editRow = db.prepare('SELECT * FROM products WHERE id = ? LIMIT 1').get(editProductId);
    if (editRow) {
      const editGroupConfig = productGroupMap.get(editRow.category_group) || productGroupConfigs[0] || {
        key: editRow.category_group,
        labelKo: editRow.category_group,
        labelEn: editRow.category_group,
        mode: PRODUCT_GROUP_MODE.SIMPLE,
        brandOptions: [],
        factoryOptions: [],
        modelOptions: [],
        modelOptionsByBrand: {},
        brandOptionLabels: {},
        factoryOptionLabels: {},
        modelOptionLabelsByBrand: {},
        customFields: []
      };
      const imageRows = db
        .prepare(
          `
            SELECT image_path
            FROM product_images
            WHERE product_id = ?
            ORDER BY sort_order ASC, id ASC
          `
        )
        .all(editRow.id);
      const imageList = imageRows.map((row) => row.image_path).filter(Boolean);
      if (imageList.length === 0 && editRow.image_path) {
        imageList.push(editRow.image_path);
      }

      editingProduct = {
        ...decorateProductForView(editRow, editGroupConfig),
        product_badges: getProductBadgeMapByProductIds([editRow.id]).get(Number(editRow.id)) || [],
        groupConfig: editGroupConfig,
        fieldValues: getProductFieldValues(editRow, editGroupConfig),
        imageList
      };
    }
  }
  const ordersQuery = [
    'SELECT o.*, p.category_group, p.brand, p.model, p.sub_model',
    'FROM orders o',
    'JOIN products p ON p.id = o.product_id'
  ];
  const orderParams = [];
  const orderWhereParts = [];
  if (orderGroupFilter !== 'all') {
    orderWhereParts.push('p.category_group = ?');
    orderParams.push(orderGroupFilter);
  }
  const orderStatusDbValues = getOrderStatusFilterDbValues(orderStatusFilter);
  if (orderStatusDbValues.length > 0) {
    orderWhereParts.push(`UPPER(TRIM(o.status)) IN (${orderStatusDbValues.map(() => '?').join(', ')})`);
    orderParams.push(...orderStatusDbValues);
  }
  if (orderDateFrom) {
    orderWhereParts.push("date(datetime(o.created_at, '+9 hours')) >= ?");
    orderParams.push(orderDateFrom);
  }
  if (orderDateTo) {
    orderWhereParts.push("date(datetime(o.created_at, '+9 hours')) <= ?");
    orderParams.push(orderDateTo);
  }
  if (orderWhereParts.length > 0) {
    ordersQuery.push(`WHERE ${orderWhereParts.join(' AND ')}`);
  }
  ordersQuery.push('ORDER BY o.id DESC');
  ordersQuery.push('LIMIT 100');

  const orders = db
    .prepare(ordersQuery.join('\n'))
    .all(...orderParams)
    .map((order) => {
      const normalizedStatus = normalizeOrderStatus(order.status);
      const statusMeta = getOrderStatusMeta(normalizedStatus, lang);
      const nextStatus = getNextOrderStatus(normalizedStatus);
      const nextActionLabel = getNextOrderActionLabel(normalizedStatus, lang);
      return {
        ...order,
        status: normalizedStatus,
        status_code: statusMeta.code,
        status_label: statusMeta.label,
        status_detail: statusMeta.detail,
        next_status: nextStatus,
        next_action_label: nextActionLabel,
        tracking_carrier_label: getTrackingCarrierLabel(order.tracking_carrier),
        category_group_label: groupLabelMap[order.category_group] || order.category_group
      };
    });

  const orderTimelineMap = new Map();
  if (orders.length > 0) {
    const orderIds = orders.map((item) => Number(item.id)).filter((id) => Number.isInteger(id) && id > 0);
    if (orderIds.length > 0) {
      const placeholders = orderIds.map(() => '?').join(', ');
      const logs = db
        .prepare(
          `
            SELECT order_id, from_status, to_status, event_note, created_at
            FROM order_status_logs
            WHERE order_id IN (${placeholders})
            ORDER BY id DESC
          `
        )
        .all(...orderIds);

      for (const log of logs) {
        const key = Number(log.order_id);
        const current = orderTimelineMap.get(key) || [];
        if (current.length < 4) {
          const fromStatus = log.from_status ? normalizeOrderStatus(log.from_status) : '';
          const toStatus = normalizeOrderStatus(log.to_status);
          const fromMeta = fromStatus ? getOrderStatusMeta(fromStatus, lang) : null;
          const toMeta = getOrderStatusMeta(toStatus, lang);
          current.push({
            from_status: fromStatus,
            from_status_label: fromMeta ? fromMeta.label : '',
            to_status: toStatus,
            to_status_label: toMeta.label,
            event_note: String(log.event_note || ''),
            created_at: String(log.created_at || '')
          });
          orderTimelineMap.set(key, current);
        }
      }
    }
  }

  const ordersWithTimeline = orders.map((order) => ({
    ...order,
    status_logs: orderTimelineMap.get(Number(order.id)) || []
  }));

  const noticeRows = db.prepare('SELECT * FROM notices ORDER BY id DESC LIMIT 50').all();
  const newsRows = db.prepare('SELECT * FROM news_posts ORDER BY id DESC LIMIT 50').all();
  const qcRows = db.prepare('SELECT * FROM qc_items ORDER BY id DESC LIMIT 50').all();
  const inquiryRows = db
    .prepare(
      `
        SELECT i.*, u.username
        FROM inquiries i
        JOIN users u ON u.id = i.user_id
        ORDER BY i.id DESC
        LIMIT 50
      `
    )
    .all();
  const notices = withRecordImagePathsList(noticeRows);
  const newsPosts = withRecordImagePathsList(newsRows);
  const qcs = withRecordImagePathsList(qcRows);
  const inquiries = withRecordImagePathsList(inquiryRows);
  const securityPanelData = buildSecurityPanelData(lang, securityOptions);
  const memberManagePanelData = buildMemberManagePanelData(lang, memberOptions);
  const salesDailyData = buildAdminSalesDailyData(lang, {
    groupConfigs: productGroupConfigs,
    groupFilter: salesGroupFilter,
    dateFrom: salesDateFrom,
    dateTo: salesDateTo
  });

  return {
    settings,
    publicMenus,
    products,
    editingProduct,
    productBadgeDefinitions,
    productBadgeColorThemes,
    orders: ordersWithTimeline,
    orderGroupFilter,
    orderStatusFilter,
    orderDateFrom,
    orderDateTo,
    notices,
    newsPosts,
    qcs,
    inquiries,
    securityPanelData,
    memberManagePanelData,
    salesDailyData,
    salesGroupFilter,
    salesDateFrom,
    salesDateTo,
    heroQuickMenuOptions: heroSettings.quickMenuOptions,
    dashboardStats: includeDashboardStats ? getCachedAdminDashboardStats() : null,
    trackingCarriers: TRACKING_CARRIERS,
    formatPrice,
    productGroups: productGroupConfigs.map((group) => group.key),
    productGroupConfigs,
    groupLabelMap,
    salesMainTabs: getSalesMainTabs(productGroupConfigs)
  };
}

function renderAdminDashboard(req, res, activeTab, extraData = {}) {
  const productGroupConfigs = getProductGroupConfigs();
  const menuFieldGroupKeys = productGroupConfigs.map((group) => String(group.key || '').trim()).filter(Boolean);
  const rawMenuFieldGroupFilter = String(extraData.menuFieldGroupFilter || req.query.fieldGroup || '').trim();
  const menuFieldGroupFilter = menuFieldGroupKeys.includes(rawMenuFieldGroupFilter)
    ? rawMenuFieldGroupFilter
    : '';
  const orderFilters = parseAdminOrderManageQuery(
    {
      orderGroup: extraData.orderGroupFilter || req.query.orderGroup || '',
      orderStatus: extraData.orderStatusFilter || req.query.orderStatus || '',
      orderDateFrom: extraData.orderDateFrom || req.query.orderDateFrom || '',
      orderDateTo: extraData.orderDateTo || req.query.orderDateTo || ''
    },
    productGroupConfigs.map((group) => group.key)
  );
  const salesFilters = parseSalesManageQuery(
    {
      salesSection: extraData.salesSection || req.query.section || '',
      salesGroup: extraData.salesGroupFilter || req.query.salesGroup || '',
      salesDateFrom: extraData.salesDateFrom || req.query.salesDateFrom || '',
      salesDateTo: extraData.salesDateTo || req.query.salesDateTo || ''
    },
    productGroupConfigs.map((group) => group.key)
  );

  const viewData = buildAdminDashboardViewData(
    res.locals.ctx.lang,
    {
      securityOptions: extraData.securityOptions || parseSecurityQuery(req.query || {}),
      memberOptions: extraData.memberOptions || parseMemberManageQuery(req.query || {}),
      includeDashboardStats: activeTab === 'dashboard',
      productEditId: extraData.productEditId || 0,
      orderGroupFilter: orderFilters.orderGroupFilter,
      orderStatusFilter: orderFilters.orderStatusFilter,
      orderDateFrom: orderFilters.orderDateFrom,
      orderDateTo: orderFilters.orderDateTo,
      salesOptions: {
        groupFilter: salesFilters.salesGroupFilter,
        dateFrom: salesFilters.salesDateFrom,
        dateTo: salesFilters.salesDateTo
      }
    }
  );
  return res.render('admin-dashboard', {
    title: 'Admin Dashboard',
    activeTab,
    securitySection: normalizeSecuritySection(extraData.securitySection),
    securityAccessDenied: Boolean(extraData.securityAccessDenied),
    siteSection: normalizeSiteManageSection(extraData.siteSection || req.query.section || ''),
    menuSection: normalizeMenuManageSection(extraData.menuSection || ''),
    productSection: normalizeProductManageSection(extraData.productSection || ''),
    salesSection: normalizeSalesManageSection(extraData.salesSection || req.query.section || ''),
    pointSection: normalizePointManageSection(extraData.pointSection || req.query.pointSection || req.query.section || ''),
    noticeSection: normalizeContentManageSection(extraData.noticeSection || req.query.section || ''),
    newsSection: normalizeContentManageSection(extraData.newsSection || req.query.section || ''),
    qcSection: normalizeContentManageSection(extraData.qcSection || req.query.section || ''),
    inquirySection: normalizeInquiryManageSection(extraData.inquirySection || req.query.section || ''),
    menuFieldGroupFilter,
    orderGroupFilter: orderFilters.orderGroupFilter,
    orderStatusFilter: orderFilters.orderStatusFilter,
    orderDateFrom: orderFilters.orderDateFrom,
    orderDateTo: orderFilters.orderDateTo,
    salesGroupFilter: salesFilters.salesGroupFilter,
    salesDateFrom: salesFilters.salesDateFrom,
    salesDateTo: salesFilters.salesDateTo,
    ...viewData
  });
}

function handleSecurityDenied(req, res, securitySection = 'profile', securityOptions = {}) {
  recordSecurityAlert(req, 'security.primary_only.denied', 'sub-admin attempted security area access');
  logAdminActivity(req, 'SECURITY_DENIED', `blocked security route: ${req.path}`);

  return res.status(403).render('admin-dashboard', {
    title: 'Admin Dashboard',
    activeTab: 'security',
    securitySection: normalizeSecuritySection(securitySection),
    securityAccessDenied: true,
    salesSection: 'price',
    pointSection: normalizePointManageSection(req.query.pointSection || req.query.section || ''),
    ...buildAdminDashboardViewData(res.locals.ctx.lang, {
      securityOptions,
      memberOptions: parseMemberManageQuery(req.query || {}),
      includeDashboardStats: false
    })
  });
}

app.get('/admin', (req, res) => {
  if (req.user?.isAdmin) {
    return res.redirect('/admin/dashboard');
  }
  if (readAdminOtpPending(req)) {
    return res.redirect('/admin/otp/verify');
  }
  return res.render('admin-login', { title: 'Admin Login' });
});

app.get('/admin/dashboard', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'dashboard'));
app.use('/admin/security', requireAdmin, (req, res, next) => {
  if (!req.user?.isPrimaryAdmin) {
    return handleSecurityDenied(
      req,
      res,
      inferSecuritySectionFromRequest(req),
      parseSecurityQuery(req.query || {})
    );
  }
  return next();
});

app.get('/admin/security', requireAdmin, (req, res) => {
  const securitySection = normalizeSecuritySection(req.query.section || 'profile');
  return renderAdminDashboard(req, res, 'security', {
    securitySection,
    securityAccessDenied: false,
    securityOptions: parseSecurityQuery(req.query || {})
  });
});

app.post('/admin/security/profile/update', requirePrimaryAdmin, (req, res) => {
  const backPath = '/admin/security?section=profile';
  const username = String(req.body.username || '').trim();
  const fullName = String(req.body.fullName || '').trim();
  const email = String(req.body.email || '').trim();
  const phone = normalizePhone(req.body.phone || '');

  if (!username || !fullName || !email || !phone) {
    setFlash(req, 'error', '필수 항목을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '계정은 4~20자 영문 소문자/숫자만 사용 가능합니다.');
    return res.redirect(backPath);
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '전화번호 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  try {
    db.prepare(
      `
        UPDATE users
        SET username = ?, full_name = ?, email = ?, phone = ?
        WHERE id = ? AND is_admin = 1 AND admin_role = 'PRIMARY'
      `
    ).run(username, fullName, email, phone, req.user.id);

    logAdminActivity(req, 'SECURITY_PROFILE_UPDATE', `updated primary profile:${username}`);
    setFlash(req, 'success', '메인관리자 정보가 업데이트되었습니다.');
    return res.redirect(backPath);
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 계정입니다.');
    return res.redirect(backPath);
  }
});

app.post('/admin/security/profile/password', requirePrimaryAdmin, asyncRoute(async (req, res) => {
  const backPath = '/admin/security?section=profile';
  const currentPassword = String(req.body.currentPassword || '');
  const newPassword = String(req.body.newPassword || '');
  const newPasswordConfirm = String(req.body.newPasswordConfirm || '');

  if (!currentPassword || !newPassword || !newPasswordConfirm) {
    setFlash(req, 'error', '현재 비밀번호와 새 비밀번호를 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (newPassword !== newPasswordConfirm) {
    setFlash(req, 'error', '새 비밀번호 확인이 일치하지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PASSWORD_REGEX.test(newPassword)) {
    setFlash(req, 'error', '새 비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
    return res.redirect(backPath);
  }

  const admin = db
    .prepare("SELECT id, password_hash FROM users WHERE id = ? AND is_admin = 1 AND admin_role = 'PRIMARY' LIMIT 1")
    .get(req.user.id);

  if (!admin) {
    setFlash(req, 'error', '메인관리자 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const validCurrent = await bcrypt.compare(currentPassword, admin.password_hash);
  if (!validCurrent) {
    setFlash(req, 'error', '현재 비밀번호가 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  const nextHash = await bcrypt.hash(newPassword, 10);
  db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(nextHash, req.user.id);

  logAdminActivity(req, 'SECURITY_PROFILE_PASSWORD', 'updated primary password');
  setFlash(req, 'success', '메인관리자 비밀번호가 변경되었습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/security/sub-admin/create', requirePrimaryAdmin, asyncRoute(async (req, res) => {
  const backPath = '/admin/security?section=admins';
  const username = String(req.body.username || '').trim();
  const fullName = String(req.body.fullName || '').trim();
  const email = String(req.body.email || '').trim();
  const phone = normalizePhone(req.body.phone || '');
  const password = String(req.body.password || '');
  const passwordConfirm = String(req.body.passwordConfirm || '');

  if (!username || !fullName || !email || !phone || !password || !passwordConfirm) {
    setFlash(req, 'error', '필수 항목을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '계정은 4~20자 영문 소문자/숫자만 사용 가능합니다.');
    return res.redirect(backPath);
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '전화번호 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PASSWORD_REGEX.test(password)) {
    setFlash(req, 'error', '비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
    return res.redirect(backPath);
  }

  if (password !== passwordConfirm) {
    setFlash(req, 'error', '비밀번호 확인이 일치하지 않습니다.');
    return res.redirect(backPath);
  }

  try {
    const hash = await bcrypt.hash(password, 10);
    db.prepare(
      `
        INSERT INTO users (email, username, full_name, phone, password_hash, agreed_terms, is_admin, admin_role)
        VALUES (?, ?, ?, ?, ?, 1, 1, 'SUB')
      `
    ).run(email, username, fullName, phone, hash);

    logAdminActivity(req, 'SECURITY_SUB_ADMIN_CREATE', `created sub-admin:${username}`);
    setFlash(req, 'success', '서브관리자 계정이 추가되었습니다.');
    return res.redirect(backPath);
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 계정입니다.');
    return res.redirect(backPath);
  }
}));

app.post('/admin/security/admin/:id/update', requirePrimaryAdmin, (req, res) => {
  const backPath = '/admin/security?section=admins';
  const targetId = Number(req.params.id);
  const username = String(req.body.username || '').trim();
  const fullName = String(req.body.fullName || '').trim();
  const email = String(req.body.email || '').trim();
  const phone = normalizePhone(req.body.phone || '');

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 관리자입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, admin_role
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeAdminRole(target.admin_role) === ADMIN_ROLE.PRIMARY && targetId !== req.user.id) {
    setFlash(req, 'error', '메인관리자 타겟은 수정할 수 없습니다.');
    return res.redirect(backPath);
  }

  if (!username || !fullName || !email || !phone) {
    setFlash(req, 'error', '필수 항목을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '계정은 4~20자 영문 소문자/숫자만 사용 가능합니다.');
    return res.redirect(backPath);
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (!PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '전화번호 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  try {
    db.prepare(
      `
        UPDATE users
        SET username = ?, full_name = ?, email = ?, phone = ?
        WHERE id = ? AND is_admin = 1
      `
    ).run(username, fullName, email, phone, targetId);

    logAdminActivity(req, 'SECURITY_ADMIN_UPDATE', `updated admin:${targetId}`);
    setFlash(req, 'success', '관리자 정보가 수정되었습니다.');
    return res.redirect(backPath);
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 계정입니다.');
    return res.redirect(backPath);
  }
});

app.post('/admin/security/admin/:id/password', requirePrimaryAdmin, asyncRoute(async (req, res) => {
  const backPath = '/admin/security?section=admins';
  const targetId = Number(req.params.id);
  const newPassword = String(req.body.newPassword || '');
  const newPasswordConfirm = String(req.body.newPasswordConfirm || '');

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 관리자입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, admin_role
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeAdminRole(target.admin_role) === ADMIN_ROLE.PRIMARY && targetId !== req.user.id) {
    setFlash(req, 'error', '메인관리자 타겟은 비밀번호 변경이 불가합니다.');
    return res.redirect(backPath);
  }

  if (!newPassword || !newPasswordConfirm) {
    setFlash(req, 'error', '새 비밀번호와 확인값을 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!PASSWORD_REGEX.test(newPassword)) {
    setFlash(req, 'error', '비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
    return res.redirect(backPath);
  }

  if (newPassword !== newPasswordConfirm) {
    setFlash(req, 'error', '비밀번호 확인이 일치하지 않습니다.');
    return res.redirect(backPath);
  }

  const nextHash = await bcrypt.hash(newPassword, 10);
  db.prepare('UPDATE users SET password_hash = ? WHERE id = ? AND is_admin = 1').run(nextHash, targetId);

  logAdminActivity(req, 'SECURITY_ADMIN_PASSWORD', `reset password for admin:${targetId}`);
  setFlash(req, 'success', '관리자 비밀번호가 변경되었습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/security/admin/:id/otp-reset', requirePrimaryAdmin, (req, res) => {
  const backPath = '/admin/security?section=admins';
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 관리자입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT
          id,
          username,
          admin_role,
          admin_otp_enabled,
          admin_otp_secret
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const otpEnabled =
    Number(target.admin_otp_enabled || 0) === 1 &&
    normalizeBase32Secret(target.admin_otp_secret || '').length >= 16;
  if (!otpEnabled) {
    setFlash(req, 'error', '이미 OTP가 비활성화된 계정입니다.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE users
      SET
        admin_otp_secret = '',
        admin_otp_enabled = 0,
        admin_otp_enabled_at = NULL
      WHERE id = ? AND is_admin = 1
    `
  ).run(targetId);

  if (targetId === Number(req.user?.id || 0)) {
    clearAdminOtpSetup(req);
    clearAdminOtpPending(req);
  }

  logAdminActivity(req, 'SECURITY_ADMIN_OTP_RESET', `reset otp for admin:${target.username || targetId}`);
  setFlash(req, 'success', '관리자 OTP가 초기화되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/security/admin/:id/delete', requirePrimaryAdmin, (req, res) => {
  const backPath = '/admin/security?section=admins';
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 관리자입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, admin_role
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (targetId === req.user.id) {
    setFlash(req, 'error', '본인 계정은 삭제할 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeAdminRole(target.admin_role) === ADMIN_ROLE.PRIMARY) {
    setFlash(req, 'error', '메인관리자 계정은 삭제할 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM users WHERE id = ? AND is_admin = 1').run(targetId);
  logAdminActivity(req, 'SECURITY_ADMIN_DELETE', `deleted sub-admin:${target.username || targetId}`);
  setFlash(req, 'success', '서브관리자 계정이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/security/alert/:id/resolve', requirePrimaryAdmin, (req, res) => {
  const backPath = '/admin/security?section=alerts';
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 알림입니다.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE admin_security_alerts
      SET resolved_at = COALESCE(resolved_at, datetime('now'))
      WHERE id = ?
    `
  ).run(id);

  logAdminActivity(req, 'SECURITY_ALERT_RESOLVE', `resolved alert:${id}`);
  setFlash(req, 'success', '보안 알림을 확인 처리했습니다.');
  return res.redirect(backPath);
});

app.get('/admin/otp', requireAdmin, asyncRoute(async (req, res) => {
  const setupState = readAdminOtpSetup(req);
  const adminRow = db
    .prepare(
      `
        SELECT
          id,
          username,
          admin_otp_enabled,
          admin_otp_secret,
          admin_otp_enabled_at
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(req.user.id);

  if (!adminRow) {
    clearScopedSessionAuthState(req, 'admin', { clearOtpSetup: true });
    clearPersistAuthCookie(res, { scope: 'admin' });
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect('/admin/login');
  }

  const otpEnabled = Number(adminRow.admin_otp_enabled || 0) === 1
    && normalizeBase32Secret(adminRow.admin_otp_secret || '').length >= 16;
  if (otpEnabled && setupState) {
    clearAdminOtpSetup(req);
  }

  const activeSetup = !otpEnabled ? readAdminOtpSetup(req) : null;
  const otpAuthUri = activeSetup
    ? buildAdminOtpAuthUri({ username: adminRow.username, secret: activeSetup.secret })
    : '';
  let otpSetupQrDataUrl = '';
  if (otpAuthUri) {
    try {
      otpSetupQrDataUrl = await QRCode.toDataURL(otpAuthUri, {
        errorCorrectionLevel: 'M',
        margin: 1,
        width: 224
      });
    } catch {
      otpSetupQrDataUrl = '';
    }
  }

  return res.render('admin-otp', {
    title: 'Admin OTP',
    otpState: {
      enabled: otpEnabled,
      enabledAt: adminRow.admin_otp_enabled_at || '',
      setupSecret: activeSetup?.secret || '',
      setupUri: otpAuthUri,
      setupQrDataUrl: otpSetupQrDataUrl
    }
  });
}));

app.post('/admin/otp/setup/start', requireAdmin, (req, res) => {
  const backPath = '/admin/otp';
  const adminRow = db
    .prepare(
      `
        SELECT id, username, admin_otp_enabled, admin_otp_secret
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(req.user.id);
  if (!adminRow) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect('/admin/login');
  }

  const otpEnabled = Number(adminRow.admin_otp_enabled || 0) === 1
    && normalizeBase32Secret(adminRow.admin_otp_secret || '').length >= 16;
  if (otpEnabled) {
    setFlash(req, 'error', '이미 OTP가 활성화되어 있습니다. 해제 후 다시 설정해 주세요.');
    return res.redirect(backPath);
  }

  req.session.adminOtpSetup = {
    userId: Number(adminRow.id),
    secret: createAdminOtpSecret(),
    issuedAt: Date.now()
  };
  setFlash(req, 'success', 'OTP 설정 키가 생성되었습니다. 인증 앱에 등록 후 6자리 코드를 입력해 주세요.');
  return res.redirect(backPath);
});

app.post('/admin/otp/setup/cancel', requireAdmin, (req, res) => {
  clearAdminOtpSetup(req);
  setFlash(req, 'success', 'OTP 설정이 취소되었습니다.');
  return res.redirect('/admin/otp');
});

app.post('/admin/otp/setup/confirm', requireAdmin, (req, res) => {
  const backPath = '/admin/otp';
  const setupState = readAdminOtpSetup(req);
  if (!setupState || setupState.userId !== Number(req.user.id)) {
    clearAdminOtpSetup(req);
    setFlash(req, 'error', 'OTP 설정 세션이 만료되었습니다. 다시 시작해 주세요.');
    return res.redirect(backPath);
  }

  const code = normalizeAdminOtpCode(req.body.code || '');
  if (code.length !== ADMIN_OTP_DIGITS) {
    setFlash(req, 'error', '6자리 OTP 인증번호를 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!verifyTotpCode(setupState.secret, code)) {
    setFlash(req, 'error', 'OTP 인증번호가 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE users
      SET
        admin_otp_secret = ?,
        admin_otp_enabled = 1,
        admin_otp_enabled_at = datetime('now')
      WHERE id = ? AND is_admin = 1
    `
  ).run(setupState.secret, req.user.id);

  clearAdminOtpSetup(req);
  logAdminActivity(req, 'SECURITY_OTP_ENABLE', 'google otp enabled');
  setFlash(req, 'success', '구글 OTP 연동이 완료되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/otp/disable', requireAdmin, asyncRoute(async (req, res) => {
  const backPath = '/admin/otp';
  const currentPassword = String(req.body.currentPassword || '');
  const code = normalizeAdminOtpCode(req.body.code || '');
  if (!currentPassword || code.length !== ADMIN_OTP_DIGITS) {
    setFlash(req, 'error', '현재 비밀번호와 6자리 OTP 인증번호를 입력해 주세요.');
    return res.redirect(backPath);
  }

  const adminRow = db
    .prepare(
      `
        SELECT id, password_hash, admin_otp_enabled, admin_otp_secret
        FROM users
        WHERE id = ? AND is_admin = 1
        LIMIT 1
      `
    )
    .get(req.user.id);

  if (!adminRow) {
    setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다.');
    return res.redirect('/admin/login');
  }

  const otpSecret = normalizeBase32Secret(adminRow.admin_otp_secret || '');
  const otpEnabled = Number(adminRow.admin_otp_enabled || 0) === 1 && otpSecret.length >= 16;
  if (!otpEnabled) {
    setFlash(req, 'error', '활성화된 OTP 설정이 없습니다.');
    return res.redirect(backPath);
  }

  const validCurrentPassword = await bcrypt.compare(currentPassword, adminRow.password_hash);
  if (!validCurrentPassword) {
    setFlash(req, 'error', '현재 비밀번호가 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (!verifyTotpCode(otpSecret, code)) {
    setFlash(req, 'error', 'OTP 인증번호가 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE users
      SET
        admin_otp_secret = '',
        admin_otp_enabled = 0,
        admin_otp_enabled_at = NULL
      WHERE id = ? AND is_admin = 1
    `
  ).run(req.user.id);

  clearAdminOtpSetup(req);
  logAdminActivity(req, 'SECURITY_OTP_DISABLE', 'google otp disabled');
  setFlash(req, 'success', '구글 OTP가 해제되었습니다.');
  return res.redirect(backPath);
}));

app.get('/admin/members', requireAdmin, (req, res) => {
  return renderAdminDashboard(req, res, 'members', {
    memberOptions: parseMemberManageQuery(req.query || {})
  });
});

app.post('/admin/member/:id/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members');
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 회원입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, is_admin
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target || Number(target.is_admin) === 1) {
    setFlash(req, 'error', '회원 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const username = String(req.body.username || '').trim();
  const nickname = String(req.body.nickname || '').trim();
  const fullName = String(req.body.fullName || '').trim();
  const email = String(req.body.email || '').trim();
  const phone = normalizePhone(req.body.phone || '');
  const agreedTerms = req.body.agreedTerms === 'on' ? 1 : 0;

  if (!username || !nickname || !email) {
    setFlash(req, 'error', '계정, 닉네임, 이메일은 필수입니다.');
    return res.redirect(backPath);
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '계정은 4~20자 영문 소문자/숫자만 사용 가능합니다.');
    return res.redirect(backPath);
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  if (nickname.length < 2 || nickname.length > 40) {
    setFlash(req, 'error', '닉네임은 2~40자로 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (phone && !PHONE_REGEX.test(phone)) {
    setFlash(req, 'error', '전화번호 형식이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  try {
    db.prepare(
      `
        UPDATE users
        SET username = ?, nickname = ?, full_name = ?, email = ?, phone = ?, agreed_terms = ?
        WHERE id = ? AND is_admin = 0
      `
    ).run(username, nickname, fullName, email, phone, agreedTerms, targetId);
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 계정입니다.');
    return res.redirect(backPath);
  }

  logAdminActivity(req, 'MEMBER_UPDATE', `member:${targetId} updated by ${req.user?.username || 'admin'}`);
  setFlash(req, 'success', '회원 정보가 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/member/:id/password', requireAdmin, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/members');
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 회원입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, is_admin
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target || Number(target.is_admin) === 1) {
    setFlash(req, 'error', '회원 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const newPassword = String(req.body.newPassword || '');
  const newPasswordConfirm = String(req.body.newPasswordConfirm || '');

  if (!newPassword || !newPasswordConfirm) {
    setFlash(req, 'error', '새 비밀번호와 확인값을 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!PASSWORD_REGEX.test(newPassword)) {
    setFlash(req, 'error', '비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
    return res.redirect(backPath);
  }

  if (newPassword !== newPasswordConfirm) {
    setFlash(req, 'error', '비밀번호 확인이 일치하지 않습니다.');
    return res.redirect(backPath);
  }

  const nextHash = await bcrypt.hash(newPassword, 10);
  db.prepare('UPDATE users SET password_hash = ? WHERE id = ? AND is_admin = 0').run(nextHash, targetId);

  logAdminActivity(req, 'MEMBER_PASSWORD_RESET', `member:${targetId} password reset`);
  setFlash(req, 'success', '회원 비밀번호가 재설정되었습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/member/:id/block', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=active');
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 회원입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, username, is_admin, is_blocked
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target || Number(target.is_admin) === 1) {
    setFlash(req, 'error', '회원 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (Number(target.is_blocked) === 1) {
    setFlash(req, 'error', '이미 블락된 계정입니다.');
    return res.redirect(backPath);
  }

  const blockReason = String(req.body.blockReason || '').trim().slice(0, 120);
  db.prepare(
    `
      UPDATE users
      SET
        is_blocked = 1,
        blocked_reason = ?,
        blocked_at = datetime('now')
      WHERE id = ? AND is_admin = 0
    `
  ).run(blockReason, targetId);

  logAdminActivity(req, 'MEMBER_BLOCK', `member:${targetId} blocked${blockReason ? ` reason:${blockReason}` : ''}`);
  setFlash(req, 'success', '회원 계정이 블락 처리되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/member/:id/unblock', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=blocked');
  const targetId = Number(req.params.id);

  if (!Number.isInteger(targetId) || targetId <= 0) {
    setFlash(req, 'error', '유효하지 않은 회원입니다.');
    return res.redirect(backPath);
  }

  const target = db
    .prepare(
      `
        SELECT id, is_admin, is_blocked
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(targetId);

  if (!target || Number(target.is_admin) === 1) {
    setFlash(req, 'error', '회원 계정을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (Number(target.is_blocked) !== 1) {
    setFlash(req, 'error', '블락 상태인 계정이 아닙니다.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE users
      SET
        is_blocked = 0,
        blocked_reason = '',
        blocked_at = NULL
      WHERE id = ? AND is_admin = 0
    `
  ).run(targetId);

  logAdminActivity(req, 'MEMBER_UNBLOCK', `member:${targetId} unblocked`);
  setFlash(req, 'success', '회원 계정 블락이 해제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/member-level/groups', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=levels');
  const productGroups = getProductGroupConfigs().map((group) => group.key);
  const submitted = req.body.includedGroups ?? req.body['includedGroups[]'] ?? [];
  const selectedGroups = (Array.isArray(submitted) ? submitted : [submitted])
    .map((group) => String(group || '').trim())
    .filter((group, index, arr) => group && arr.indexOf(group) === index)
    .filter((group) => productGroups.includes(group));

  if (selectedGroups.length === 0) {
    setFlash(req, 'error', '누적금액 적용 쇼핑몰을 최소 1개 이상 선택해 주세요.');
    return res.redirect(backPath);
  }

  setSetting('memberLevelIncludedGroups', JSON.stringify(selectedGroups));
  logAdminActivity(req, 'MEMBER_LEVEL_GROUPS_UPDATE', `groups:${selectedGroups.join(',')}`);
  setFlash(req, 'success', '누적금액 적용 쇼핑몰 설정이 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/member-level/add', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=levels');
  const levelNameKo = normalizeMemberLevelName(req.body.levelNameKo || req.body.levelName || '');
  const levelNameEn = normalizeMemberLevelName(
    req.body.levelNameEn || req.body.levelNameKo || req.body.levelName || '',
    levelNameKo
  );
  const colorTheme = normalizeMemberLevelColorTheme(req.body.colorTheme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME);
  const thresholdAmount = parseMemberLevelThresholdAmount(req.body.thresholdAmount || '0', 0);
  const operator = normalizeMemberLevelOperator(req.body.operator || MEMBER_LEVEL_OPERATORS.GTE);

  if (!levelNameKo || !levelNameEn) {
    setFlash(req, 'error', '등급 명칭(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const currentRules = getMemberLevelRulesSetting();
  const nextLevelId = buildUniqueMemberLevelId(levelNameEn || levelNameKo, currentRules.map((rule) => rule.id));
  const nextRules = [
    ...currentRules,
    {
      id: nextLevelId,
      nameKo: levelNameKo,
      nameEn: levelNameEn,
      name: levelNameKo,
      colorTheme,
      operator,
      thresholdAmount
    }
  ];
  setSetting('memberLevelRules', JSON.stringify(nextRules));

  const sourceRateMap = getRawMemberLevelPointRateSetting();
  sourceRateMap[nextLevelId] = parsePointRate(sourceRateMap[nextLevelId], 0);
  saveMemberLevelPointRates(nextRules, sourceRateMap);

  logAdminActivity(req, 'MEMBER_LEVEL_ADD', `level:${nextLevelId}`);
  setFlash(req, 'success', '회원 등급이 추가되었습니다. 포인트 퍼센테이지는 포인트관리에서 설정해 주세요.');
  return res.redirect(backPath);
});

app.post('/admin/member-level/:id/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=levels');
  const levelId = String(req.params.id || '').trim();
  const levelNameKo = normalizeMemberLevelName(req.body.levelNameKo || req.body.levelName || '');
  const levelNameEn = normalizeMemberLevelName(
    req.body.levelNameEn || req.body.levelNameKo || req.body.levelName || '',
    levelNameKo
  );
  const colorTheme = normalizeMemberLevelColorTheme(req.body.colorTheme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME);
  const thresholdAmount = parseMemberLevelThresholdAmount(req.body.thresholdAmount || '0', 0);
  const operator = normalizeMemberLevelOperator(req.body.operator || MEMBER_LEVEL_OPERATORS.GTE);

  if (!levelId) {
    setFlash(req, 'error', '유효하지 않은 등급입니다.');
    return res.redirect(backPath);
  }

  if (!levelNameKo || !levelNameEn) {
    setFlash(req, 'error', '등급 명칭(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const currentRules = getMemberLevelRulesSetting();
  const targetIndex = currentRules.findIndex((rule) => rule.id === levelId);
  if (targetIndex < 0) {
    setFlash(req, 'error', '수정할 등급을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextRules = [...currentRules];
  nextRules[targetIndex] = {
    ...nextRules[targetIndex],
    nameKo: levelNameKo,
    nameEn: levelNameEn,
    name: levelNameKo,
    colorTheme,
    operator,
    thresholdAmount
  };
  setSetting('memberLevelRules', JSON.stringify(nextRules));
  saveMemberLevelPointRates(nextRules, getRawMemberLevelPointRateSetting());

  logAdminActivity(req, 'MEMBER_LEVEL_UPDATE', `level:${levelId}`);
  setFlash(req, 'success', '회원 등급 조건이 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/member-level/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/members?memberSection=levels');
  const levelId = String(req.params.id || '').trim();

  if (!levelId) {
    setFlash(req, 'error', '유효하지 않은 등급입니다.');
    return res.redirect(backPath);
  }

  const currentRules = getMemberLevelRulesSetting();
  const nextRules = currentRules.filter((rule) => rule.id !== levelId);
  if (nextRules.length === currentRules.length) {
    setFlash(req, 'error', '삭제할 등급을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (nextRules.length === 0) {
    setFlash(req, 'error', '최소 1개 이상의 회원 등급이 필요합니다.');
    return res.redirect(backPath);
  }

  setSetting('memberLevelRules', JSON.stringify(nextRules));
  const sourceRateMap = getRawMemberLevelPointRateSetting();
  delete sourceRateMap[levelId];
  saveMemberLevelPointRates(nextRules, sourceRateMap);

  logAdminActivity(req, 'MEMBER_LEVEL_DELETE', `level:${levelId}`);
  setFlash(req, 'success', '회원 등급이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.get('/admin/points', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'points', {
    pointSection: parsePointManageQuery(req.query || {}).section
  })
);

app.post('/admin/points/signup', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/points?section=signup');
  const signupBonusPoints = parseNonNegativeInt(
    String(req.body.signupBonusPoints ?? '0').replace(/[^0-9]/g, ''),
    0
  );

  setSetting('signupBonusPoints', String(signupBonusPoints));
  logAdminActivity(req, 'POINTS_SIGNUP_UPDATE', `signup_bonus:${signupBonusPoints}`);
  setFlash(req, 'success', '회원가입 포인트가 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/points/review', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/points?section=review');
  const reviewRewardPoints = parseNonNegativeInt(
    String(req.body.reviewRewardPoints ?? '0').replace(/[^0-9]/g, ''),
    0
  );

  setSetting('reviewRewardPoints', String(reviewRewardPoints));
  logAdminActivity(req, 'POINTS_REVIEW_UPDATE', `review_reward:${reviewRewardPoints}`);
  setFlash(req, 'success', '구매후기 포인트가 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/points/level-rates', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/points?section=level-rates');
  const levelRules = getMemberLevelRulesSetting();

  if (levelRules.length === 0) {
    setFlash(req, 'error', '설정된 회원 등급이 없습니다. 회원관리 > 회원레벨 설정에서 먼저 등급을 만들어 주세요.');
    return res.redirect(backPath);
  }

  const nextRateMap = {};
  levelRules.forEach((rule) => {
    const fieldName = `rate_${rule.id}`;
    nextRateMap[rule.id] = parsePointRate(req.body[fieldName], 0);
  });
  saveMemberLevelPointRates(levelRules, nextRateMap);

  logAdminActivity(req, 'POINTS_LEVEL_RATE_UPDATE', `levels:${levelRules.length}`);
  setFlash(
    req,
    'success',
    '등급별 포인트 퍼센테이지가 저장되었습니다. 변경된 값은 이후 신규 주문부터 적용됩니다.'
  );
  return res.redirect(backPath);
});

app.get('/admin/site', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'site', {
    siteSection: normalizeSiteManageSection(req.query.section || '')
  })
);
app.get('/admin/menus', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'menus', {
    menuSection: normalizeMenuManageSection(req.query.section || '')
  })
);
app.get('/admin/products', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'products', {
    productSection: normalizeProductManageSection(req.query.section || ''),
    productEditId: normalizeOptionalId(req.query.editId || '')
  })
);
app.get('/admin/sales', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'sales'));
app.get('/admin/notices', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'notices', {
    noticeSection: req.query.section || 'create'
  })
);
app.get('/admin/news', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'news', {
    newsSection: req.query.section || 'create'
  })
);
app.get('/admin/qc', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'qc', {
    qcSection: req.query.section || 'create'
  })
);
app.get('/admin/orders', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'orders', {
    orderGroupFilter: req.query.orderGroup || 'all',
    orderStatusFilter: req.query.orderStatus || 'all',
    orderDateFrom: req.query.orderDateFrom || '',
    orderDateTo: req.query.orderDateTo || ''
  })
);
app.get('/admin/inquiries', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'inquiries', {
    inquirySection: req.query.section || 'reply'
  })
);

app.get(
  '/admin/sales/data',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const syncResult = syncPaidOrdersToSalesWorkbookSafely({
      forceResync: false
    });
    const workbook = syncResult?.workbook || getSalesWorkbook();
    const payload = buildSalesWorkbookPayload(workbook);
    return res.json({
      ok: true,
      mainTabs: getSalesMainTabs(),
      ...payload
    });
  })
);

app.post(
  '/admin/sales/sync-filters',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const syncResult = syncSalesWorkbookPriceFiltersSafely({
      force: true,
      respectVersion: false,
      markVersion: true
    });
    if (!syncResult || !syncResult.workbook) {
      return res.status(500).json({
        ok: false,
        error: 'sales_filter_sync_failed',
        message: '분류 필터 동기화 중 오류가 발생했습니다.'
      });
    }
    const payload = buildSalesWorkbookPayload(syncResult.workbook);

    logAdminActivity(req, 'SALES_FILTER_SYNC', `changed:${syncResult.changed ? '1' : '0'}`);
    return res.json({
      ok: true,
      changed: Boolean(syncResult.changed),
      stats: syncResult.stats,
      mainTabs: getSalesMainTabs(),
      ...payload
    });
  })
);

app.post(
  '/admin/sales/workbook',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const incomingWorkbook = req.body?.workbook && typeof req.body.workbook === 'object'
      ? req.body.workbook
      : req.body;
    const savedWorkbook = saveSalesWorkbook(incomingWorkbook);
    logAdminActivity(req, 'SALES_SAVE', 'sales workbook saved');

    return res.json({
      ok: true,
      mainTabs: getSalesMainTabs(),
      ...buildSalesWorkbookPayload(savedWorkbook)
    });
  })
);

app.post(
  '/admin/sales/fx-sync',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const fx = await fetchCnyKrwExchangeRate();
    const workbook = getSalesWorkbook();
    const tabKey = normalizeSalesText(req.body?.tabKey, 40);
    const scopeId = normalizeSalesText(req.body?.scopeId, 120);
    const requestedScopeDate = normalizeSalesDate(req.body?.scopeDate || '');
    const targetTab = workbook.tabs && tabKey ? workbook.tabs[tabKey] : null;
    const applyToGlobalFallback = () => {
      workbook.globals = normalizeSalesSettingValues(
        {
          ...(workbook.globals || {}),
          exchangeRate: fx.exchangeRate,
          fxSource: fx.provider,
          fxUpdatedAt: fx.updatedAt,
          updatedAt: fx.updatedAt
        },
        workbook.globals || {}
      );
    };

    if (!targetTab) {
      applyToGlobalFallback();
    } else if (getSalesScopeMode(tabKey) === 'factory') {
      targetTab.settings = normalizeSalesSettingValues(
        {
          ...(targetTab.settings || {}),
          exchangeRate: fx.exchangeRate,
          fxSource: fx.provider,
          fxUpdatedAt: fx.updatedAt,
          updatedAt: fx.updatedAt
        },
        workbook.globals || {}
      );
    } else {
      const scopes = getSalesScopeList(targetTab);
      const targetScope =
        scopes.find((scope) => String(scope.id || '') === scopeId) ||
        scopes.find((scope) =>
          normalizeSalesDate(scope?.settings?.baseDate || scope?.date || scope?.name || '') === requestedScopeDate
        ) ||
        null;
      if (!targetScope) {
        applyToGlobalFallback();
      } else {
        targetScope.settings = normalizeSalesSettingValues(
          {
            ...(targetScope.settings || {}),
            exchangeRate: fx.exchangeRate,
            fxSource: fx.provider,
            fxUpdatedAt: fx.updatedAt,
            updatedAt: fx.updatedAt
          },
          targetTab.settings || workbook.globals || {}
        );
        const nextScopeDate =
          requestedScopeDate ||
          normalizeSalesDate(targetScope?.settings?.baseDate || targetScope?.date || targetScope?.name || '');
        if (nextScopeDate) {
          targetScope.settings.baseDate = nextScopeDate;
          targetScope.date = nextScopeDate;
          if (getSalesScopeMode(tabKey) === 'date') {
            targetScope.name = nextScopeDate;
          }
        }
      }
    }
    const savedWorkbook = saveSalesWorkbook(workbook);

    logAdminActivity(req, 'SALES_FX_SYNC', `tab:${tabKey || 'global'} scope:${scopeId || '-'} CNY/KRW=${fx.exchangeRate}`);
    return res.json({
      ok: true,
      mainTabs: getSalesMainTabs(),
      ...buildSalesWorkbookPayload(savedWorkbook)
    });
  })
);

app.post(
  '/admin/sales/import-sheet',
  requireAdmin,
  (req, res) => res.status(410).json({
    ok: false,
    error: 'legacy_sheet_import_removed',
    message: '구글시트 연동 기능은 종료되었습니다.'
  })
);

app.post('/admin/menu/add', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus');
  const labelKo = String(req.body.labelKo || '').trim();
  const labelEn = String(req.body.labelEn || '').trim();
  const menuPath = sanitizePath(String(req.body.path || '').trim());

  if (!labelKo || !labelEn || !menuPath) {
    setFlash(req, 'error', '메뉴 이름과 경로를 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (menuPath.startsWith('/admin')) {
    setFlash(req, 'error', 'admin 경로는 공개 메뉴로 추가할 수 없습니다.');
    return res.redirect(backPath);
  }

  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), { includeHidden: true });
  const id = `menu-${Date.now()}`;
  menus.push({ id, labelKo, labelEn, path: menuPath, isHidden: false });
  setSetting('menus', JSON.stringify(menus));

  setFlash(req, 'success', '메뉴가 추가되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/menu/update/:id', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=public');
  const id = String(req.params.id || '');
  const labelKo = String(req.body.labelKo || '').trim();
  const labelEn = String(req.body.labelEn || '').trim();
  const menuPath = sanitizePath(String(req.body.path || '').trim());

  if (!id) {
    setFlash(req, 'error', '메뉴를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (!labelKo || !labelEn || !menuPath) {
    setFlash(req, 'error', '메뉴 이름과 경로를 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (menuPath.startsWith('/admin')) {
    setFlash(req, 'error', 'admin 경로는 공개 메뉴로 설정할 수 없습니다.');
    return res.redirect(backPath);
  }

  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), { includeHidden: true });
  const targetIndex = menus.findIndex((menu) => menu.id === id);

  if (targetIndex < 0) {
    setFlash(req, 'error', '메뉴를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const hasDuplicatePath = menus.some((menu) => menu.id !== id && menu.path === menuPath);
  if (hasDuplicatePath) {
    setFlash(req, 'error', '이미 사용 중인 경로입니다.');
    return res.redirect(backPath);
  }

  menus[targetIndex] = {
    ...menus[targetIndex],
    labelKo,
    labelEn,
    path: menuPath
  };

  setSetting('menus', JSON.stringify(menus));
  setFlash(req, 'success', '메뉴 정보가 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/menu/remove/:id', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus');
  const id = String(req.params.id || '');
  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), { includeHidden: true });
  const nextMenus = menus.filter((menu) => menu.id !== id);
  if (nextMenus.length === 0) {
    setFlash(req, 'error', '최소 1개 이상의 메뉴는 유지되어야 합니다.');
    return res.redirect(backPath);
  }
  if (!nextMenus.some((menu) => !menu.isHidden)) {
    setFlash(req, 'error', '최소 1개 이상의 메뉴는 표시 상태여야 합니다.');
    return res.redirect(backPath);
  }
  setSetting('menus', JSON.stringify(nextMenus));
  setFlash(req, 'success', '메뉴가 삭제되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/menu/toggle/:id', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus');
  const id = String(req.params.id || '');
  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())), { includeHidden: true });
  const targetIndex = menus.findIndex((menu) => menu.id === id);

  if (targetIndex < 0) {
    setFlash(req, 'error', '메뉴를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const target = menus[targetIndex];
  const nextHidden = !Boolean(target.isHidden);

  if (nextHidden) {
    const visibleCount = menus.filter((menu) => !menu.isHidden).length;
    if (visibleCount <= 1) {
      setFlash(req, 'error', '최소 1개 이상의 메뉴는 표시 상태여야 합니다.');
      return res.redirect(backPath);
    }
  }

  menus[targetIndex] = {
    ...target,
    isHidden: nextHidden
  };

  setSetting('menus', JSON.stringify(menus));
  setFlash(req, 'success', nextHidden ? '메뉴를 숨김 처리했습니다.' : '메뉴를 다시 표시합니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/add', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=groups');
  const labelKo = String(req.body.labelKo || '').trim().slice(0, 60);
  const labelEn = String(req.body.labelEn || '').trim().slice(0, 60);
  const templateType = String(req.body.templateType || '').trim().toLowerCase();
  const showInMainTopBox = req.body.showInMainTopBox === 'on';

  if (!labelKo || !labelEn) {
    setFlash(req, 'error', '분류명(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const requestedKey = normalizeProductGroupKey(req.body.key || labelKo, labelKo);
  if (!requestedKey) {
    setFlash(req, 'error', '분류 키를 생성할 수 없습니다. 이름을 확인해 주세요.');
    return res.redirect(backPath);
  }
  if (configs.some((group) => group.key === requestedKey)) {
    setFlash(req, 'error', '이미 같은 분류 키가 존재합니다. 다른 이름으로 생성해 주세요.');
    return res.redirect(backPath);
  }

  const useFactoryTemplate =
    templateType === PRODUCT_GROUP_MODE.FACTORY || requestedKey === '공장제' || labelKo === '공장제';
  const enableFactoryFilterByDefault = (
    useFactoryTemplate ||
    requestedKey === '현지중고' ||
    labelKo === '현지중고' ||
    isDomesticStockGroup({ key: requestedKey, labelKo, labelEn })
  );
  const filterSeeds = getDefaultGroupFilterSeeds();
  const nextConfigs = [...configs, {
    key: requestedKey,
    labelKo,
    labelEn,
    mode: useFactoryTemplate ? PRODUCT_GROUP_MODE.FACTORY : PRODUCT_GROUP_MODE.SIMPLE,
    showInMainTopBox,
    enableBrandFilter: true,
    enableModelFilter: true,
    enableFactoryFilter: enableFactoryFilterByDefault,
    brandOptions: useFactoryTemplate ? filterSeeds.factory.brandOptions : filterSeeds.simple.brandOptions,
    factoryOptions: useFactoryTemplate ? filterSeeds.factory.factoryOptions : [],
    modelOptions: useFactoryTemplate ? filterSeeds.factory.modelOptions : filterSeeds.simple.modelOptions,
    modelOptionsByBrand: useFactoryTemplate
      ? filterSeeds.factory.modelOptionsByBrand
      : filterSeeds.simple.modelOptionsByBrand,
    brandOptionLabels: useFactoryTemplate
      ? filterSeeds.factory.brandOptionLabels
      : filterSeeds.simple.brandOptionLabels,
    factoryOptionLabels: useFactoryTemplate
      ? filterSeeds.factory.factoryOptionLabels
      : {},
    modelOptionLabelsByBrand: useFactoryTemplate
      ? filterSeeds.factory.modelOptionLabelsByBrand
      : filterSeeds.simple.modelOptionLabelsByBrand,
    customFields: useFactoryTemplate ? getFactoryDefaultFields() : getCompactDefaultFields()
  }];
  setProductGroupConfigs(nextConfigs);
  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });

  setFlash(req, 'success', '쇼핑몰 분류가 추가되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=groups');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const nextGroupKeyInput = String(req.body.nextGroupKey || req.body.key || '').trim();
  const labelKo = String(req.body.labelKo || '').trim().slice(0, 60);
  const labelEn = String(req.body.labelEn || '').trim().slice(0, 60);

  if (!groupKey) {
    setFlash(req, 'error', '수정할 분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }
  if (!labelKo || !labelEn) {
    setFlash(req, 'error', '분류명(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '수정할 분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }
  const nextGroupKey = normalizeProductGroupKey(nextGroupKeyInput || groupKey, labelKo || groupKey);
  if (!nextGroupKey) {
    setFlash(req, 'error', '분류 키를 생성할 수 없습니다. 분류 키 입력값을 확인해 주세요.');
    return res.redirect(backPath);
  }
  const hasDuplicatedKey = configs.some(
    (group, index) => index !== targetIndex && normalizeProductGroupKey(group?.key || '') === nextGroupKey
  );
  if (hasDuplicatedKey) {
    setFlash(req, 'error', '이미 사용 중인 분류 키입니다. 다른 키를 입력해 주세요.');
    return res.redirect(backPath);
  }

  const keyChanged = nextGroupKey !== groupKey;
  const previousTabs = getSalesMainTabs(configs);
  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...nextGroups[targetIndex],
    key: nextGroupKey,
    labelKo,
    labelEn
  };
  const nextTabs = getSalesMainTabs(nextGroups);
  const previousDateTab = findSalesTabByScopeAndGroup(previousTabs, 'date', groupKey);
  const nextDateTab = findSalesTabByScopeAndGroup(nextTabs, 'date', nextGroupKey);
  const previousPriceTab = findSalesTabByScopeAndGroup(previousTabs, 'factory', groupKey);
  const nextPriceTab = findSalesTabByScopeAndGroup(nextTabs, 'factory', nextGroupKey);

  const migrationStats = {
    keyChanged,
    productsUpdated: 0,
    orderSnapshotsUpdated: 0,
    workbookMigrated: false
  };

  const updateTx = db.transaction(() => {
    if (keyChanged) {
      migrationStats.productsUpdated = db
        .prepare(
          `
            UPDATE products
            SET category_group = ?
            WHERE category_group = ?
          `
        )
        .run(nextGroupKey, groupKey).changes;
    }

    setProductGroupConfigs(nextGroups);

    const workbook = getSalesWorkbook();
    let workbookChanged = false;
    workbookChanged = migrateSalesWorkbookTabByRename(workbook, previousPriceTab, nextPriceTab) || workbookChanged;
    workbookChanged = migrateSalesWorkbookTabByRename(workbook, previousDateTab, nextDateTab) || workbookChanged;
    if (workbookChanged) {
      saveSalesWorkbook(workbook, { importedFrom: 'local-workbook' });
      migrationStats.workbookMigrated = true;
    }

    if (
      keyChanged &&
      previousDateTab &&
      nextDateTab &&
      normalizeSalesText(previousDateTab.key || '', 80) &&
      normalizeSalesText(nextDateTab.key || '', 80) &&
      String(previousDateTab.key) !== String(nextDateTab.key)
    ) {
      migrationStats.orderSnapshotsUpdated = db
        .prepare(
          `
            UPDATE orders
            SET sales_tab_key = ?
            WHERE sales_tab_key = ?
          `
        )
        .run(String(nextDateTab.key || ''), String(previousDateTab.key || '')).changes;
    }
  });

  try {
    updateTx();
  } catch (error) {
    console.error('[product-group] update failed:', error);
    setFlash(req, 'error', '분류 수정 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.');
    return res.redirect(backPath);
  }

  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });
  if (keyChanged) {
    syncPaidOrdersToSalesWorkbookSafely({ forceResync: true });
  }
  logAdminActivity(
    req,
    'PRODUCT_GROUP_UPDATE',
    keyChanged ? `key:${groupKey}->${nextGroupKey}` : `key:${groupKey}`
  );

  const detailMessage = keyChanged
    ? ` (상품 ${migrationStats.productsUpdated}건, 주문 스냅샷 ${migrationStats.orderSnapshotsUpdated}건 동기화)`
    : '';
  setFlash(req, 'success', `쇼핑몰 분류 정보가 수정되었습니다.${detailMessage}`);
  return res.redirect(backPath);
});

app.post('/admin/product-group/layout/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=groups');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');

  if (!groupKey) {
    setFlash(req, 'error', '분류 키를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...nextGroups[targetIndex],
    showInMainTopBox: req.body.showInMainTopBox === 'on'
  };
  setProductGroupConfigs(nextGroups);

  setFlash(req, 'success', '분류 레이아웃 설정이 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/reorder', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=groups');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const direction = String(req.body.direction || '').trim().toLowerCase();

  if (!groupKey || !['up', 'down'].includes(direction)) {
    setFlash(req, 'error', '분류 순서 변경 요청이 올바르지 않습니다.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if ((direction === 'up' && targetIndex === 0) || (direction === 'down' && targetIndex === configs.length - 1)) {
    return res.redirect(backPath);
  }

  const swapIndex = direction === 'up' ? targetIndex - 1 : targetIndex + 1;
  const nextGroups = [...configs];
  [nextGroups[targetIndex], nextGroups[swapIndex]] = [nextGroups[swapIndex], nextGroups[targetIndex]];
  setProductGroupConfigs(nextGroups);
  setFlash(req, 'success', '분류 순서가 변경되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/remove/:key', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=groups');
  const key = normalizeProductGroupKey(req.params.key || '');
  const configs = getProductGroupConfigs();
  const target = configs.find((group) => group.key === key);

  if (!target) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (configs.length <= 1) {
    setFlash(req, 'error', '최소 1개의 분류는 유지되어야 합니다.');
    return res.redirect(backPath);
  }

  const linkedProductRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM products
        WHERE category_group = ?
      `
    )
    .get(key);
  if (Number(linkedProductRow?.count || 0) > 0) {
    setFlash(req, 'error', '해당 분류에 등록된 상품이 있어 삭제할 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextConfigs = configs.filter((group) => group.key !== key);
  setProductGroupConfigs(nextConfigs);
  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });
  setFlash(req, 'success', '쇼핑몰 분류가 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/filter/visibility', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=group-filters');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  if (!groupKey) {
    setFlash(req, 'error', '분류 키를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const enableBrandFilter = req.body.enableBrandFilter === 'on';
  const enableModelFilter = req.body.enableModelFilter === 'on';
  const enableFactoryFilter = req.body.enableFactoryFilter === 'on';

  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...nextGroups[targetIndex],
    enableBrandFilter,
    enableModelFilter,
    enableFactoryFilter
  };

  setProductGroupConfigs(nextGroups);
  setFlash(req, 'success', '분류 필터 적용 설정이 저장되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/field/add', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=fields');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const labelKo = String(req.body.labelKo || '').trim().slice(0, 60);
  const labelEn = String(req.body.labelEn || '').trim().slice(0, 60);
  const rawType = String(req.body.fieldType || 'text').trim().toLowerCase();
  const fieldType = PRODUCT_FIELD_TYPES.has(rawType) ? rawType : 'text';
  const required = req.body.required === 'on';

  if (!groupKey || !labelKo || !labelEn) {
    setFlash(req, 'error', '필드 추가에 필요한 값을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '필드를 추가할 분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  const existingFields = Array.isArray(targetGroup.customFields) ? targetGroup.customFields : [];
  const requestedKey = normalizeProductFieldAliasKey(
    req.body.fieldKey || buildFieldKeyFromLabel(labelEn || labelKo, existingFields.length + 1),
  );
  const finalKey = normalizeProductFieldKey(requestedKey, `field_${existingFields.length + 1}`);
  if (existingFields.some((field) => normalizeProductFieldAliasKey(field.key) === finalKey)) {
    setFlash(req, 'error', '같은 필드 키가 이미 존재합니다. 키를 변경해 주세요.');
    return res.redirect(backPath);
  }

  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...targetGroup,
    customFields: [
      ...existingFields,
      { key: finalKey, labelKo, labelEn, type: fieldType, required }
    ]
  };

  setProductGroupConfigs(nextGroups);
  setFlash(req, 'success', '업로드 필드가 추가되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/field/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=fields');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const fieldKey = normalizeProductFieldAliasKey(req.body.fieldKey || '');
  const labelKo = String(req.body.labelKo || '').trim().slice(0, 60);
  const labelEn = String(req.body.labelEn || '').trim().slice(0, 60);
  const rawType = String(req.body.fieldType || 'text').trim().toLowerCase();
  const fieldType = PRODUCT_FIELD_TYPES.has(rawType) ? rawType : 'text';
  const required = req.body.required === 'on';

  if (!groupKey || !fieldKey || !labelKo || !labelEn) {
    setFlash(req, 'error', '필드 수정값을 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  const existingFields = Array.isArray(targetGroup.customFields) ? targetGroup.customFields : [];
  const fieldIndex = existingFields.findIndex(
    (field) => normalizeProductFieldAliasKey(field.key || '') === fieldKey
  );
  if (fieldIndex < 0) {
    setFlash(req, 'error', '수정할 필드를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextFields = [...existingFields];
  nextFields[fieldIndex] = {
    ...nextFields[fieldIndex],
    labelKo,
    labelEn,
    type: fieldType,
    required
  };

  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...targetGroup,
    customFields: nextFields
  };

  setProductGroupConfigs(nextGroups);
  setFlash(req, 'success', '필드 정보가 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/field/remove', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=fields');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const fieldKey = normalizeProductFieldAliasKey(req.body.fieldKey || '');

  if (!groupKey || !fieldKey) {
    setFlash(req, 'error', '분류와 필드 키를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  const existingFields = Array.isArray(targetGroup.customFields) ? targetGroup.customFields : [];
  if (existingFields.length <= 1) {
    setFlash(req, 'error', '최소 1개 이상의 업로드 필드는 유지되어야 합니다.');
    return res.redirect(backPath);
  }

  const nextFields = existingFields.filter(
    (field) => normalizeProductFieldAliasKey(field.key || '') !== fieldKey
  );
  if (nextFields.length === existingFields.length) {
    setFlash(req, 'error', '삭제할 필드를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextGroups = [...configs];
  nextGroups[targetIndex] = {
    ...targetGroup,
    customFields: nextFields
  };

  setProductGroupConfigs(nextGroups);
  setFlash(req, 'success', '업로드 필드가 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-group/filter/add', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=group-filters');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const filterType = String(req.body.filterType || 'brand').trim().toLowerCase();
  const optionLabelKo = normalizeProductFilterOption(req.body.optionLabelKo || req.body.labelKo || '');
  const optionLabelEn = normalizeProductFilterOption(req.body.optionLabelEn || req.body.labelEn || '');
  const optionValue = normalizeProductFilterOption(
    req.body.optionValue || optionLabelEn || optionLabelKo || ''
  );
  const brandValue = normalizeProductFilterOption(req.body.brandValue || '');

  if (!groupKey || !optionValue) {
    setFlash(req, 'error', '필터 추가에 필요한 값을 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!['brand', 'factory', 'model'].includes(filterType)) {
    setFlash(req, 'error', '지원되지 않는 필터 타입입니다.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  if (filterType === 'model' && !brandValue) {
    setFlash(req, 'error', '모델 필터 추가 시 브랜드를 선택해 주세요.');
    return res.redirect(backPath);
  }

  const brandOptions = getGroupBrandOptions(targetGroup);
  const selectedBrand = brandOptions.find((item) => item.toLowerCase() === brandValue.toLowerCase()) || '';
  if (filterType === 'model' && !selectedBrand) {
    setFlash(req, 'error', '등록된 브랜드 중에서 모델 대상 브랜드를 선택해 주세요.');
    return res.redirect(backPath);
  }

  const currentList = (() => {
    if (filterType === 'factory') {
      return getGroupFactoryOptions(targetGroup);
    }
    if (filterType === 'model') {
      return getGroupModelOptionsForBrand(targetGroup, selectedBrand);
    }
    return brandOptions;
  })();
  const duplicated = currentList.some((item) => item.toLowerCase() === optionValue.toLowerCase());
  if (duplicated) {
    setFlash(req, 'error', '이미 등록된 필터 값입니다.');
    return res.redirect(backPath);
  }

  const nextList = [...currentList, optionValue];
  const nextGroups = [...configs];
  const currentModelMap = getGroupModelOptionsByBrand(targetGroup);
  const nextModelMap = { ...currentModelMap };
  const currentBrandLabelMap = getGroupBrandOptionLabels(targetGroup);
  const currentFactoryLabelMap = getGroupFactoryOptionLabels(targetGroup);
  const currentModelLabelMapByBrand = getGroupModelOptionLabelsByBrand(targetGroup);
  const nextLabelEntry = {
    labelKo: optionLabelKo || optionValue,
    labelEn: optionLabelEn || optionValue
  };
  let nextBrandLabelMap = currentBrandLabelMap;
  let nextFactoryLabelMap = currentFactoryLabelMap;
  let nextModelLabelMapByBrand = currentModelLabelMapByBrand;

  if (filterType === 'model') {
    nextModelMap[selectedBrand] = normalizeProductFilterOptionList(nextList);
    const matchedBrandKey = findMatchingProductFilterKey(currentModelLabelMapByBrand, selectedBrand);
    const currentBrandModelLabelMap =
      matchedBrandKey &&
      currentModelLabelMapByBrand[matchedBrandKey] &&
      typeof currentModelLabelMapByBrand[matchedBrandKey] === 'object' &&
      !Array.isArray(currentModelLabelMapByBrand[matchedBrandKey])
        ? currentModelLabelMapByBrand[matchedBrandKey]
        : {};
    const rawNextModelLabelMapByBrand = {
      ...currentModelLabelMapByBrand,
      [selectedBrand]: {
        ...currentBrandModelLabelMap,
        [optionValue]: nextLabelEntry
      }
    };
    nextModelLabelMapByBrand = normalizeProductFilterOptionLabelMapByBrand(
      rawNextModelLabelMapByBrand,
      nextModelMap,
      brandOptions
    );
  } else if (filterType === 'brand') {
    const rawNextBrandLabelMap = {
      ...currentBrandLabelMap,
      [optionValue]: nextLabelEntry
    };
    nextBrandLabelMap = normalizeProductFilterOptionLabelMap(rawNextBrandLabelMap, nextList);
    nextModelLabelMapByBrand = normalizeProductFilterOptionLabelMapByBrand(
      currentModelLabelMapByBrand,
      nextModelMap,
      nextList
    );
  } else if (filterType === 'factory') {
    const rawNextFactoryLabelMap = {
      ...currentFactoryLabelMap,
      [optionValue]: nextLabelEntry
    };
    nextFactoryLabelMap = normalizeProductFilterOptionLabelMap(rawNextFactoryLabelMap, nextList);
  }

  nextGroups[targetIndex] = {
    ...targetGroup,
    ...(filterType === 'factory'
      ? {
          factoryOptions: nextList,
          factoryOptionLabels: nextFactoryLabelMap
        }
      : filterType === 'model'
        ? {
            modelOptionsByBrand: nextModelMap,
            modelOptions: [],
            modelOptionLabelsByBrand: nextModelLabelMapByBrand
          }
        : {
            brandOptions: nextList,
            brandOptionLabels: nextBrandLabelMap,
            modelOptionLabelsByBrand: nextModelLabelMapByBrand
          })
  };

  setProductGroupConfigs(nextGroups);
  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });
  setFlash(
    req,
    'success',
    filterType === 'factory'
      ? '공장 필터가 추가되었습니다.'
      : filterType === 'model'
        ? '모델 필터가 추가되었습니다. (브랜드별)'
        : '브랜드 필터가 추가되었습니다.'
  );
  return res.redirect(backPath);
});

app.post('/admin/product-group/filter/remove', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=group-filters');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const filterType = String(req.body.filterType || 'brand').trim().toLowerCase();
  const optionValue = normalizeProductFilterOption(req.body.optionValue || '');
  const brandValue = normalizeProductFilterOption(req.body.brandValue || '');

  if (!groupKey || !optionValue || !['brand', 'factory', 'model'].includes(filterType)) {
    setFlash(req, 'error', '필터 삭제 요청값을 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  const brandOptions = getGroupBrandOptions(targetGroup);
  const selectedBrand = brandOptions.find((item) => item.toLowerCase() === brandValue.toLowerCase()) || '';
  if (filterType === 'model' && !selectedBrand) {
    setFlash(req, 'error', '삭제할 모델의 브랜드를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const currentList = (() => {
    if (filterType === 'factory') {
      return getGroupFactoryOptions(targetGroup);
    }
    if (filterType === 'model') {
      return getGroupModelOptionsForBrand(targetGroup, selectedBrand);
    }
    return brandOptions;
  })();
  const nextList = currentList.filter((item) => item.toLowerCase() !== optionValue.toLowerCase());

  if (nextList.length === currentList.length) {
    setFlash(req, 'error', '삭제할 필터 값을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextGroups = [...configs];
  const currentModelMap = getGroupModelOptionsByBrand(targetGroup);
  const nextModelMap = { ...currentModelMap };
  const currentBrandLabelMap = getGroupBrandOptionLabels(targetGroup);
  const currentFactoryLabelMap = getGroupFactoryOptionLabels(targetGroup);
  const currentModelLabelMapByBrand = getGroupModelOptionLabelsByBrand(targetGroup);
  let nextBrandLabelMap = currentBrandLabelMap;
  let nextFactoryLabelMap = currentFactoryLabelMap;
  let nextModelLabelMapByBrand = currentModelLabelMapByBrand;

  if (filterType === 'model') {
    if (nextList.length === 0) {
      delete nextModelMap[selectedBrand];
    } else {
      nextModelMap[selectedBrand] = normalizeProductFilterOptionList(nextList);
    }
    const matchedBrandKey = findMatchingProductFilterKey(currentModelLabelMapByBrand, selectedBrand);
    const currentBrandModelLabelMap =
      matchedBrandKey &&
      currentModelLabelMapByBrand[matchedBrandKey] &&
      typeof currentModelLabelMapByBrand[matchedBrandKey] === 'object' &&
      !Array.isArray(currentModelLabelMapByBrand[matchedBrandKey])
        ? currentModelLabelMapByBrand[matchedBrandKey]
        : {};
    const rawBrandModelLabelMap = { ...currentBrandModelLabelMap };
    const matchedModelLabelKey = findMatchingProductFilterKey(rawBrandModelLabelMap, optionValue);
    if (matchedModelLabelKey) {
      delete rawBrandModelLabelMap[matchedModelLabelKey];
    }
    const rawNextModelLabelMapByBrand = { ...currentModelLabelMapByBrand };
    if (nextList.length === 0) {
      const matchedBrandLabelKey = findMatchingProductFilterKey(rawNextModelLabelMapByBrand, selectedBrand);
      if (matchedBrandLabelKey) {
        delete rawNextModelLabelMapByBrand[matchedBrandLabelKey];
      }
    } else {
      rawNextModelLabelMapByBrand[selectedBrand] = rawBrandModelLabelMap;
    }
    nextModelLabelMapByBrand = normalizeProductFilterOptionLabelMapByBrand(
      rawNextModelLabelMapByBrand,
      nextModelMap,
      brandOptions
    );
  }
  if (filterType === 'brand') {
    const matchedBrandKey = Object.keys(nextModelMap).find(
      (item) => item.toLowerCase() === optionValue.toLowerCase()
    );
    if (matchedBrandKey) {
      delete nextModelMap[matchedBrandKey];
    }
    const matchedBrandLabelKey = findMatchingProductFilterKey(currentBrandLabelMap, optionValue);
    const rawNextBrandLabelMap = { ...currentBrandLabelMap };
    if (matchedBrandLabelKey) {
      delete rawNextBrandLabelMap[matchedBrandLabelKey];
    }
    nextBrandLabelMap = normalizeProductFilterOptionLabelMap(rawNextBrandLabelMap, nextList);

    const rawNextModelLabelMapByBrand = { ...currentModelLabelMapByBrand };
    const matchedModelBrandLabelKey = findMatchingProductFilterKey(rawNextModelLabelMapByBrand, optionValue);
    if (matchedModelBrandLabelKey) {
      delete rawNextModelLabelMapByBrand[matchedModelBrandLabelKey];
    }
    nextModelLabelMapByBrand = normalizeProductFilterOptionLabelMapByBrand(
      rawNextModelLabelMapByBrand,
      nextModelMap,
      nextList
    );
  }
  if (filterType === 'factory') {
    const matchedFactoryLabelKey = findMatchingProductFilterKey(currentFactoryLabelMap, optionValue);
    const rawNextFactoryLabelMap = { ...currentFactoryLabelMap };
    if (matchedFactoryLabelKey) {
      delete rawNextFactoryLabelMap[matchedFactoryLabelKey];
    }
    nextFactoryLabelMap = normalizeProductFilterOptionLabelMap(rawNextFactoryLabelMap, nextList);
  }

  nextGroups[targetIndex] = {
    ...targetGroup,
    ...(filterType === 'factory'
      ? {
          factoryOptions: nextList,
          factoryOptionLabels: nextFactoryLabelMap
        }
      : filterType === 'model'
        ? {
            modelOptionsByBrand: nextModelMap,
            modelOptions: [],
            modelOptionLabelsByBrand: nextModelLabelMapByBrand
          }
        : {
            brandOptions: nextList,
            modelOptionsByBrand: nextModelMap,
            brandOptionLabels: nextBrandLabelMap,
            modelOptionLabelsByBrand: nextModelLabelMapByBrand
          })
  };

  setProductGroupConfigs(nextGroups);
  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });
  setFlash(
    req,
    'success',
    filterType === 'factory'
      ? '공장 필터가 삭제되었습니다.'
      : filterType === 'model'
        ? '모델 필터가 삭제되었습니다.'
        : '브랜드 필터가 삭제되었습니다.'
  );
  return res.redirect(backPath);
});

app.post('/admin/product-group/filter/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus?section=group-filters');
  const groupKey = normalizeProductGroupKey(req.body.groupKey || '');
  const filterType = String(req.body.filterType || 'brand').trim().toLowerCase();
  const optionValue = normalizeProductFilterOption(req.body.optionValue || '');
  const optionLabelKo = normalizeProductFilterOption(req.body.optionLabelKo || req.body.labelKo || '');
  const optionLabelEn = normalizeProductFilterOption(req.body.optionLabelEn || req.body.labelEn || '');
  const nextOptionValue = normalizeProductFilterOption(
    req.body.nextOptionValue || optionLabelEn || optionLabelKo || optionValue
  );
  const brandValue = normalizeProductFilterOption(req.body.brandValue || '');

  if (!groupKey || !optionValue || !nextOptionValue || !['brand', 'factory', 'model'].includes(filterType)) {
    setFlash(req, 'error', '필터 수정 요청값을 확인해 주세요.');
    return res.redirect(backPath);
  }

  const configs = getProductGroupConfigs();
  const targetIndex = configs.findIndex((group) => group.key === groupKey);
  if (targetIndex < 0) {
    setFlash(req, 'error', '분류를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const targetGroup = configs[targetIndex];
  const brandOptions = getGroupBrandOptions(targetGroup);
  const selectedBrand = brandOptions.find((item) => item.toLowerCase() === brandValue.toLowerCase()) || '';
  if (filterType === 'model' && !selectedBrand) {
    setFlash(req, 'error', '수정할 모델의 브랜드를 확인해 주세요.');
    return res.redirect(backPath);
  }

  const currentList = (() => {
    if (filterType === 'factory') {
      return getGroupFactoryOptions(targetGroup);
    }
    if (filterType === 'model') {
      return getGroupModelOptionsForBrand(targetGroup, selectedBrand);
    }
    return brandOptions;
  })();

  const matchedCurrentValue = currentList.find(
    (item) => item.toLowerCase() === optionValue.toLowerCase()
  ) || '';
  if (!matchedCurrentValue) {
    setFlash(req, 'error', '수정할 필터 값을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const duplicated = currentList.some(
    (item) =>
      item.toLowerCase() === nextOptionValue.toLowerCase() &&
      item.toLowerCase() !== matchedCurrentValue.toLowerCase()
  );
  if (duplicated) {
    setFlash(req, 'error', '같은 값의 필터가 이미 존재합니다.');
    return res.redirect(backPath);
  }

  const replacedList = currentList.map((item) =>
    item.toLowerCase() === matchedCurrentValue.toLowerCase() ? nextOptionValue : item
  );
  const nextList = normalizeProductFilterOptionList(replacedList);
  const nextLabelEntry = {
    labelKo: optionLabelKo || nextOptionValue,
    labelEn: optionLabelEn || nextOptionValue
  };

  const currentBrandLabelMap = getGroupBrandOptionLabels(targetGroup);
  const currentFactoryLabelMap = getGroupFactoryOptionLabels(targetGroup);
  const currentModelMap = getGroupModelOptionsByBrand(targetGroup);
  const currentModelLabelMapByBrand = getGroupModelOptionLabelsByBrand(targetGroup);
  const nextGroups = [...configs];

  if (filterType === 'brand') {
    const rawNextBrandLabelMap = { ...currentBrandLabelMap };
    const matchedBrandLabelKey = findMatchingProductFilterKey(rawNextBrandLabelMap, matchedCurrentValue);
    if (matchedBrandLabelKey) {
      delete rawNextBrandLabelMap[matchedBrandLabelKey];
    }
    rawNextBrandLabelMap[nextOptionValue] = nextLabelEntry;

    const rawNextModelMap = {};
    Object.entries(currentModelMap).forEach(([brandKey, modelList]) => {
      const normalizedBrandKey = normalizeProductFilterOption(brandKey);
      if (!normalizedBrandKey) {
        return;
      }
      const nextBrandKey =
        normalizedBrandKey.toLowerCase() === matchedCurrentValue.toLowerCase()
          ? nextOptionValue
          : normalizedBrandKey;
      rawNextModelMap[nextBrandKey] = normalizeProductFilterOptionList(modelList);
    });
    const nextModelMap = normalizeProductFilterOptionMap(rawNextModelMap, nextList);

    const rawNextModelLabelMapByBrand = { ...currentModelLabelMapByBrand };
    const matchedModelBrandKey = findMatchingProductFilterKey(rawNextModelLabelMapByBrand, matchedCurrentValue);
    const matchedTargetBrandKey = findMatchingProductFilterKey(rawNextModelLabelMapByBrand, nextOptionValue);
    if (matchedModelBrandKey && matchedModelBrandKey !== matchedTargetBrandKey) {
      const movingLabelMap = rawNextModelLabelMapByBrand[matchedModelBrandKey];
      delete rawNextModelLabelMapByBrand[matchedModelBrandKey];
      rawNextModelLabelMapByBrand[nextOptionValue] = movingLabelMap;
    }

    nextGroups[targetIndex] = {
      ...targetGroup,
      brandOptions: nextList,
      modelOptionsByBrand: nextModelMap,
      brandOptionLabels: normalizeProductFilterOptionLabelMap(rawNextBrandLabelMap, nextList),
      modelOptionLabelsByBrand: normalizeProductFilterOptionLabelMapByBrand(
        rawNextModelLabelMapByBrand,
        nextModelMap,
        nextList
      )
    };
  } else if (filterType === 'factory') {
    const rawNextFactoryLabelMap = { ...currentFactoryLabelMap };
    const matchedFactoryLabelKey = findMatchingProductFilterKey(rawNextFactoryLabelMap, matchedCurrentValue);
    if (matchedFactoryLabelKey) {
      delete rawNextFactoryLabelMap[matchedFactoryLabelKey];
    }
    rawNextFactoryLabelMap[nextOptionValue] = nextLabelEntry;

    nextGroups[targetIndex] = {
      ...targetGroup,
      factoryOptions: nextList,
      factoryOptionLabels: normalizeProductFilterOptionLabelMap(rawNextFactoryLabelMap, nextList)
    };
  } else {
    const nextModelMap = {
      ...currentModelMap,
      [selectedBrand]: nextList
    };
    const matchedBrandModelLabelKey = findMatchingProductFilterKey(currentModelLabelMapByBrand, selectedBrand);
    const currentBrandModelLabelMap =
      matchedBrandModelLabelKey &&
      currentModelLabelMapByBrand[matchedBrandModelLabelKey] &&
      typeof currentModelLabelMapByBrand[matchedBrandModelLabelKey] === 'object' &&
      !Array.isArray(currentModelLabelMapByBrand[matchedBrandModelLabelKey])
        ? currentModelLabelMapByBrand[matchedBrandModelLabelKey]
        : {};
    const rawNextBrandModelLabelMap = { ...currentBrandModelLabelMap };
    const matchedModelLabelKey = findMatchingProductFilterKey(rawNextBrandModelLabelMap, matchedCurrentValue);
    if (matchedModelLabelKey) {
      delete rawNextBrandModelLabelMap[matchedModelLabelKey];
    }
    rawNextBrandModelLabelMap[nextOptionValue] = nextLabelEntry;
    const rawNextModelLabelMapByBrand = {
      ...currentModelLabelMapByBrand,
      [selectedBrand]: rawNextBrandModelLabelMap
    };

    nextGroups[targetIndex] = {
      ...targetGroup,
      modelOptionsByBrand: nextModelMap,
      modelOptions: [],
      modelOptionLabelsByBrand: normalizeProductFilterOptionLabelMapByBrand(
        rawNextModelLabelMapByBrand,
        nextModelMap,
        brandOptions
      )
    };
  }

  setProductGroupConfigs(nextGroups);
  syncSalesWorkbookPriceFiltersSafely({
    force: true,
    respectVersion: false,
    markVersion: true
  });
  setFlash(
    req,
    'success',
    filterType === 'factory'
      ? '공장 필터가 수정되었습니다.'
      : filterType === 'model'
        ? '모델 필터가 수정되었습니다.'
        : '브랜드 필터가 수정되었습니다.'
  );
  return res.redirect(backPath);
});

app.post(
  '/admin/settings',
  requireAdmin,
  upload.fields([
    { name: 'dayHeaderLogo', maxCount: 1 },
    { name: 'nightHeaderLogo', maxCount: 1 },
    { name: 'dayHeaderSymbol', maxCount: 1 },
    { name: 'nightHeaderSymbol', maxCount: 1 },
    { name: 'dayFooterLogo', maxCount: 1 },
    { name: 'nightFooterLogo', maxCount: 1 },
    { name: 'dayBackgroundImage', maxCount: 1 },
    { name: 'nightBackgroundImage', maxCount: 1 },
    { name: 'headerLogo', maxCount: 1 },
    { name: 'headerSymbol', maxCount: 1 },
    { name: 'footerLogo', maxCount: 1 },
    { name: 'backgroundImage', maxCount: 1 },
    { name: 'heroLeftBackgroundImage', maxCount: 1 }
  ]),
  requireAuthenticatedMultipartCsrf,
  (req, res) => {
    const hasThemePayload = [
      req.body.dayHeaderColor,
      req.body.dayBackgroundColor,
      req.body.nightHeaderColor,
      req.body.nightBackgroundColor,
      req.body.dayBackgroundType,
      req.body.nightBackgroundType,
      req.files?.dayHeaderLogo?.[0],
      req.files?.nightHeaderLogo?.[0],
      req.files?.dayHeaderSymbol?.[0],
      req.files?.nightHeaderSymbol?.[0],
      req.files?.dayFooterLogo?.[0],
      req.files?.nightFooterLogo?.[0],
      req.files?.dayBackgroundImage?.[0],
      req.files?.nightBackgroundImage?.[0],
      req.files?.headerLogo?.[0],
      req.files?.headerSymbol?.[0],
      req.files?.footerLogo?.[0],
      req.files?.backgroundImage?.[0]
    ].some(Boolean);
    const settingsSection = normalizeSiteManageSection(
      req.body.settingsSection || req.query.section || (hasThemePayload ? 'theme' : 'basic')
    );
    const backPath = safeBackPath(req, `/admin/site?section=${settingsSection}`);

    if (settingsSection === 'basic') {
      const siteName = String(req.body.siteName || getSetting('siteName', 'Chrono Lab')).trim();
      const bankAccountInfo = String(req.body.bankAccountInfo || '').trim();
      const contactInfo = String(req.body.contactInfo || '').trim();
      const kakaoChatUrlInput = String(req.body.kakaoChatUrl || '').trim();
      const businessInfo = String(req.body.businessInfo || '').trim();
      const footerBrandCopyKo = String(req.body.footerBrandCopyKo || '').trim().slice(0, 300);
      const footerBrandCopyEn = String(req.body.footerBrandCopyEn || '').trim().slice(0, 300);
      const languageDefault = resolveLanguage(req.body.languageDefault || getSetting('languageDefault', 'ko'), 'ko');
      const heroLeftTitleKo = String(req.body.heroLeftTitleKo || '')
        .trim()
        .slice(0, 120);
      const heroLeftTitleEn = String(req.body.heroLeftTitleEn || '')
        .trim()
        .slice(0, 120);
      const heroLeftSubtitleKo = String(req.body.heroLeftSubtitleKo || '')
        .trim()
        .slice(0, 220);
      const heroLeftSubtitleEn = String(req.body.heroLeftSubtitleEn || '')
        .trim()
        .slice(0, 220);
      const heroRightTitleKo = String(req.body.heroRightTitleKo || '')
        .trim()
        .slice(0, 120);
      const heroRightTitleEn = String(req.body.heroRightTitleEn || '')
        .trim()
        .slice(0, 120);
      const heroRightSubtitleKo = String(req.body.heroRightSubtitleKo || '')
        .trim()
        .slice(0, 240);
      const heroRightSubtitleEn = String(req.body.heroRightSubtitleEn || '')
        .trim()
        .slice(0, 240);
      const heroLeftBackgroundType = normalizeHeroLeftBackgroundType(req.body.heroLeftBackgroundType || 'color');
      const heroLeftBackgroundColor = normalizeHexColor(
        req.body.heroLeftBackgroundColor || getSetting('heroLeftBackgroundColor', HERO_DEFAULT_LEFT_BACKGROUND_COLOR),
        HERO_DEFAULT_LEFT_BACKGROUND_COLOR
      );
      const heroRightBackgroundColor = normalizeHexColor(
        req.body.heroRightBackgroundColor || getSetting('heroRightBackgroundColor', HERO_DEFAULT_RIGHT_BACKGROUND_COLOR),
        HERO_DEFAULT_RIGHT_BACKGROUND_COLOR
      );
      const heroLeftCtaPath = normalizeHeroQuickMenuPath(req.body.heroLeftCtaPath || HERO_DEFAULT_LEFT_CTA_PATH) ||
        HERO_DEFAULT_LEFT_CTA_PATH;
      const heroQuickMenuRawValues = Array.from({ length: HERO_QUICK_MENU_LIMIT }, (_, index) =>
        String(req.body[`heroQuickMenuPath${index + 1}`] || '')
      );
      const heroQuickMenuPaths = normalizeHeroQuickMenuPathList(heroQuickMenuRawValues);
      const resolvedKakaoChatUrl = resolveKakaoChatUrl('', kakaoChatUrlInput);
      if (kakaoChatUrlInput && !resolvedKakaoChatUrl) {
        setFlash(
          req,
          'error',
          '카카오톡 채널 주소 형식이 올바르지 않습니다. pf.kakao.com 주소 또는 @채널아이디를 입력해 주세요.'
        );
        return res.redirect(backPath);
      }

      setSetting('siteName', siteName || 'Chrono Lab');
      setSetting('bankAccountInfo', bankAccountInfo);
      setSetting('contactInfo', contactInfo);
      setSetting('kakaoChatUrl', resolvedKakaoChatUrl);
      setSetting('businessInfo', businessInfo);
      setSetting('footerBrandCopyKo', footerBrandCopyKo);
      setSetting('footerBrandCopyEn', footerBrandCopyEn);
      setSetting('languageDefault', languageDefault);
      setSetting(
        'heroLeftTitleKo',
        heroLeftTitleKo || getSetting('heroLeftTitleKo', HERO_DEFAULT_LEFT_TITLE_KO)
      );
      setSetting(
        'heroLeftTitleEn',
        heroLeftTitleEn || getSetting('heroLeftTitleEn', HERO_DEFAULT_LEFT_TITLE_EN)
      );
      setSetting(
        'heroLeftSubtitleKo',
        heroLeftSubtitleKo || getSetting('heroLeftSubtitleKo', HERO_DEFAULT_LEFT_SUBTITLE_KO)
      );
      setSetting(
        'heroLeftSubtitleEn',
        heroLeftSubtitleEn || getSetting('heroLeftSubtitleEn', HERO_DEFAULT_LEFT_SUBTITLE_EN)
      );
      setSetting(
        'heroRightTitleKo',
        heroRightTitleKo || getSetting('heroRightTitleKo', HERO_DEFAULT_RIGHT_TITLE_KO)
      );
      setSetting(
        'heroRightTitleEn',
        heroRightTitleEn || getSetting('heroRightTitleEn', HERO_DEFAULT_RIGHT_TITLE_EN)
      );
      setSetting(
        'heroRightSubtitleKo',
        heroRightSubtitleKo || getSetting('heroRightSubtitleKo', HERO_DEFAULT_RIGHT_SUBTITLE_KO)
      );
      setSetting(
        'heroRightSubtitleEn',
        heroRightSubtitleEn || getSetting('heroRightSubtitleEn', HERO_DEFAULT_RIGHT_SUBTITLE_EN)
      );
      setSetting('heroLeftBackgroundType', heroLeftBackgroundType);
      setSetting('heroLeftBackgroundColor', heroLeftBackgroundColor);
      setSetting('heroLeftCtaPath', heroLeftCtaPath);
      setSetting('heroRightBackgroundColor', heroRightBackgroundColor);
      setSetting('heroQuickMenuPaths', JSON.stringify(heroQuickMenuPaths));

      const heroLeftBackgroundImageFile = req.files?.heroLeftBackgroundImage?.[0];
      if (heroLeftBackgroundImageFile) {
        setSetting('heroLeftBackgroundImagePath', fileUrl(heroLeftBackgroundImageFile));
      }
      if (heroLeftBackgroundType === 'color') {
        setSetting('heroLeftBackgroundImagePath', '');
      }

      if (typeof req.body.menusJson === 'string') {
        try {
          const parsedMenus = parseMenus(req.body.menusJson, { includeHidden: true });
          if (!parsedMenus.some((menu) => !menu.isHidden)) {
            setFlash(req, 'error', '표시 상태 메뉴를 최소 1개 이상 유지해 주세요.');
            return res.redirect(backPath);
          }
          setSetting('menus', JSON.stringify(parsedMenus));
        } catch {
          setFlash(req, 'error', '메뉴 JSON 형식이 올바르지 않습니다.');
          return res.redirect(backPath);
        }
      }

      setFlash(req, 'success', '기본 설정이 저장되었습니다.');
      return res.redirect(backPath);
    }

    const dayThemeDefaults = DEFAULT_THEME_COLORS.day;
    const nightThemeDefaults = DEFAULT_THEME_COLORS.night;
    const dayThemeCurrent = getThemeColorConfig('day');
    const nightThemeCurrent = getThemeColorConfig('night');
    const dayBackgroundType = String(
      req.body.dayBackgroundType || req.body.backgroundType || getSetting('dayBackgroundType', 'color')
    ).trim() === 'image'
      ? 'image'
      : 'color';
    const nightBackgroundType = String(
      req.body.nightBackgroundType || getSetting('nightBackgroundType', dayBackgroundType)
    ).trim() === 'image'
      ? 'image'
      : 'color';

    const dayHeaderColor = clampHexToThemePalette(
      req.body.dayHeaderColor || req.body.headerColor || dayThemeCurrent.headerColor || dayThemeDefaults.headerColor,
      dayThemeDefaults.headerColor
    );
    const dayBackgroundColor = clampHexToThemePalette(
      req.body.dayBackgroundColor || req.body.backgroundColor || dayThemeCurrent.backgroundColor || dayThemeDefaults.backgroundColor,
      dayThemeDefaults.backgroundColor
    );
    const dayTextColor = clampHexToThemePalette(
      req.body.dayTextColor || dayThemeCurrent.textColor || dayThemeDefaults.textColor,
      dayThemeDefaults.textColor
    );
    const dayMutedColor = clampHexToThemePalette(
      req.body.dayMutedColor || dayThemeCurrent.mutedColor || dayThemeDefaults.mutedColor,
      dayThemeDefaults.mutedColor
    );
    const dayLineColor = clampHexToThemePalette(
      req.body.dayLineColor || dayThemeCurrent.lineColor || dayThemeDefaults.lineColor,
      dayThemeDefaults.lineColor
    );
    const dayCardColor = clampHexToThemePalette(
      req.body.dayCardColor || dayThemeCurrent.cardColor || dayThemeDefaults.cardColor,
      dayThemeDefaults.cardColor
    );
    const dayCardDarkColor = clampHexToThemePalette(
      req.body.dayCardDarkColor || dayThemeCurrent.cardDarkColor || dayThemeDefaults.cardDarkColor,
      dayThemeDefaults.cardDarkColor
    );
    const dayCardDarkTextColor = clampHexToThemePalette(
      req.body.dayCardDarkTextColor || dayThemeCurrent.cardDarkTextColor || dayThemeDefaults.cardDarkTextColor,
      dayThemeDefaults.cardDarkTextColor
    );
    const dayChipColor = clampHexToThemePalette(
      req.body.dayChipColor || dayThemeCurrent.chipColor || dayThemeDefaults.chipColor,
      dayThemeDefaults.chipColor
    );

    const nightHeaderColor = clampHexToThemePalette(
      req.body.nightHeaderColor || nightThemeCurrent.headerColor || nightThemeDefaults.headerColor,
      nightThemeDefaults.headerColor
    );
    const nightBackgroundColor = clampHexToThemePalette(
      req.body.nightBackgroundColor || nightThemeCurrent.backgroundColor || nightThemeDefaults.backgroundColor,
      nightThemeDefaults.backgroundColor
    );
    const nightTextColor = clampHexToThemePalette(
      req.body.nightTextColor || nightThemeCurrent.textColor || nightThemeDefaults.textColor,
      nightThemeDefaults.textColor
    );
    const nightMutedColor = clampHexToThemePalette(
      req.body.nightMutedColor || nightThemeCurrent.mutedColor || nightThemeDefaults.mutedColor,
      nightThemeDefaults.mutedColor
    );
    const nightLineColor = clampHexToThemePalette(
      req.body.nightLineColor || nightThemeCurrent.lineColor || nightThemeDefaults.lineColor,
      nightThemeDefaults.lineColor
    );
    const nightCardColor = clampHexToThemePalette(
      req.body.nightCardColor || nightThemeCurrent.cardColor || nightThemeDefaults.cardColor,
      nightThemeDefaults.cardColor
    );
    const nightCardDarkColor = clampHexToThemePalette(
      req.body.nightCardDarkColor || nightThemeCurrent.cardDarkColor || nightThemeDefaults.cardDarkColor,
      nightThemeDefaults.cardDarkColor
    );
    const nightCardDarkTextColor = clampHexToThemePalette(
      req.body.nightCardDarkTextColor || nightThemeCurrent.cardDarkTextColor || nightThemeDefaults.cardDarkTextColor,
      nightThemeDefaults.cardDarkTextColor
    );
    const nightChipColor = clampHexToThemePalette(
      req.body.nightChipColor || nightThemeCurrent.chipColor || nightThemeDefaults.chipColor,
      nightThemeDefaults.chipColor
    );

    setSetting('dayHeaderColor', dayHeaderColor);
    setSetting('dayBackgroundColor', dayBackgroundColor);
    setSetting('dayTextColor', dayTextColor);
    setSetting('dayMutedColor', dayMutedColor);
    setSetting('dayLineColor', dayLineColor);
    setSetting('dayCardColor', dayCardColor);
    setSetting('dayCardDarkColor', dayCardDarkColor);
    setSetting('dayCardDarkTextColor', dayCardDarkTextColor);
    setSetting('dayChipColor', dayChipColor);

    setSetting('nightHeaderColor', nightHeaderColor);
    setSetting('nightBackgroundColor', nightBackgroundColor);
    setSetting('nightTextColor', nightTextColor);
    setSetting('nightMutedColor', nightMutedColor);
    setSetting('nightLineColor', nightLineColor);
    setSetting('nightCardColor', nightCardColor);
    setSetting('nightCardDarkColor', nightCardDarkColor);
    setSetting('nightCardDarkTextColor', nightCardDarkTextColor);
    setSetting('nightChipColor', nightChipColor);

    setSetting('dayBackgroundType', dayBackgroundType);
    setSetting('nightBackgroundType', nightBackgroundType);

    const dayHeaderLogoFile = req.files?.dayHeaderLogo?.[0] || req.files?.headerLogo?.[0];
    const nightHeaderLogoFile = req.files?.nightHeaderLogo?.[0];
    const dayHeaderSymbolFile = req.files?.dayHeaderSymbol?.[0] || req.files?.headerSymbol?.[0];
    const nightHeaderSymbolFile = req.files?.nightHeaderSymbol?.[0];
    const dayFooterLogoFile = req.files?.dayFooterLogo?.[0] || req.files?.footerLogo?.[0];
    const nightFooterLogoFile = req.files?.nightFooterLogo?.[0];
    const dayBackgroundImageFile = req.files?.dayBackgroundImage?.[0] || req.files?.backgroundImage?.[0];
    const nightBackgroundImageFile = req.files?.nightBackgroundImage?.[0];

    if (dayHeaderLogoFile) setSetting('dayHeaderLogoPath', fileUrl(dayHeaderLogoFile));
    if (nightHeaderLogoFile) setSetting('nightHeaderLogoPath', fileUrl(nightHeaderLogoFile));
    if (dayHeaderSymbolFile) setSetting('dayHeaderSymbolPath', fileUrl(dayHeaderSymbolFile));
    if (nightHeaderSymbolFile) setSetting('nightHeaderSymbolPath', fileUrl(nightHeaderSymbolFile));
    if (dayFooterLogoFile) setSetting('dayFooterLogoPath', fileUrl(dayFooterLogoFile));
    if (nightFooterLogoFile) setSetting('nightFooterLogoPath', fileUrl(nightFooterLogoFile));
    if (dayBackgroundImageFile) setSetting('dayBackgroundImagePath', fileUrl(dayBackgroundImageFile));
    if (nightBackgroundImageFile) setSetting('nightBackgroundImagePath', fileUrl(nightBackgroundImageFile));

    if (dayBackgroundType === 'color') {
      setSetting('dayBackgroundImagePath', '');
    }
    if (nightBackgroundType === 'color') {
      setSetting('nightBackgroundImagePath', '');
    }

    // Legacy aliases keep old code/data compatible
    setSetting('headerColor', dayHeaderColor);
    setSetting('headerLogoPath', getSetting('dayHeaderLogoPath', ''));
    setSetting('headerSymbolPath', getSetting('dayHeaderSymbolPath', ''));
    setSetting('footerLogoPath', getSetting('dayFooterLogoPath', ''));
    setSetting('backgroundType', dayBackgroundType);
    setSetting(
      'backgroundValue',
      dayBackgroundType === 'image'
        ? getSetting('dayBackgroundImagePath', '')
        : dayBackgroundColor
    );

    setFlash(req, 'success', '테마 설정이 저장되었습니다.');
    return res.redirect(backPath);
  }
);

app.post('/admin/product/create', requireAdmin, upload.array('images', MAX_UPLOAD_IMAGE_COUNT), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=upload');
  const productGroupConfigs = getProductGroupConfigs();
  const defaultFilterSeeds = getDefaultGroupFilterSeeds();
  const fallbackGroupKey = productGroupConfigs[0]?.key || SHOP_PRODUCT_GROUPS[0] || '공장제';
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const categoryGroupRaw = String(req.body.categoryGroup || fallbackGroupKey).trim();
  const selectedGroup = productGroupMap.get(categoryGroupRaw) || productGroupConfigs[0] || {
    key: fallbackGroupKey,
    labelKo: fallbackGroupKey,
    labelEn: fallbackGroupKey,
    mode: PRODUCT_GROUP_MODE.SIMPLE,
    brandOptions: defaultFilterSeeds.simple.brandOptions,
    factoryOptions: [],
    modelOptions: defaultFilterSeeds.simple.modelOptions,
    modelOptionsByBrand: defaultFilterSeeds.simple.modelOptionsByBrand,
    brandOptionLabels: defaultFilterSeeds.simple.brandOptionLabels,
    factoryOptionLabels: {},
    modelOptionLabelsByBrand: defaultFilterSeeds.simple.modelOptionLabelsByBrand,
    customFields: getGroupDefaultFields({ key: fallbackGroupKey, labelKo: fallbackGroupKey, labelEn: fallbackGroupKey })
  };
  const categoryGroup = selectedGroup.key;
  const selectableBadgeDefs = getProductBadgeDefinitions();
  const selectableBadgeIdSet = new Set(selectableBadgeDefs.map((item) => Number(item.id)));
  const selectedBadgeIds = normalizeRequestedBadgeIds(req.body.badgeIds, selectableBadgeIdSet);
  const submission = buildAdminProductSubmission(req.body, selectedGroup);
  if (submission.error) {
    setFlash(req, 'error', submission.error);
    return res.redirect(backPath);
  }

  const {
    brand,
    model,
    subModel,
    reference,
    factoryName,
    versionName,
    movement,
    caseSize,
    dialColor,
    caseMaterial,
    strapMaterial,
    features,
    price,
    shippingPeriod
  } = submission.mapped;
  const extraFieldsJson = submission.extraFieldsJson;

  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = uploadedFiles.map((file) => fileUrl(file)).filter(Boolean);
  const primaryImage = uploadedImages[0] || '';

  const inserted = db.prepare(
    `
      INSERT INTO products (
        category_group,
        brand,
        model,
        sub_model,
        reference,
        factory_name,
        version_name,
        movement,
        case_size,
        dial_color,
        case_material,
        strap_material,
        features,
        price,
        shipping_period,
        image_path,
        extra_fields_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  ).run(
    categoryGroup,
    brand,
    model,
    subModel,
    reference,
    factoryName,
    versionName,
    movement,
    caseSize,
    dialColor,
    caseMaterial,
    strapMaterial,
    features,
    price,
    shippingPeriod,
    primaryImage,
    extraFieldsJson
  );
  const productId = Number(inserted.lastInsertRowid);

  if (uploadedImages.length > 0) {
    const insertImage = db.prepare(
      `
        INSERT INTO product_images (product_id, image_path, sort_order)
        VALUES (?, ?, ?)
      `
    );

    uploadedImages.forEach((src, index) => {
      insertImage.run(productId, src, index);
    });
  }

  replaceProductBadgeLinks(productId, selectedBadgeIds);

  setPopupFlash(req, 'success', '상품이 등록되었습니다.');
  res.redirect(backPath);
}));

app.post('/admin/product/:id/update', requireAdmin, upload.array('images', MAX_UPLOAD_IMAGE_COUNT), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 상품입니다.');
    return res.redirect(backPath);
  }

  const product = db.prepare('SELECT id FROM products WHERE id = ? LIMIT 1').get(id);
  if (!product) {
    setFlash(req, 'error', '상품을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const defaultFilterSeeds = getDefaultGroupFilterSeeds();
  const fallbackGroupKey = productGroupConfigs[0]?.key || SHOP_PRODUCT_GROUPS[0] || '공장제';
  const categoryGroupRaw = String(req.body.categoryGroup || fallbackGroupKey).trim();
  const selectedGroup = productGroupMap.get(categoryGroupRaw) || productGroupConfigs[0] || {
    key: fallbackGroupKey,
    labelKo: fallbackGroupKey,
    labelEn: fallbackGroupKey,
    mode: PRODUCT_GROUP_MODE.SIMPLE,
    brandOptions: defaultFilterSeeds.simple.brandOptions,
    factoryOptions: [],
    modelOptions: defaultFilterSeeds.simple.modelOptions,
    modelOptionsByBrand: defaultFilterSeeds.simple.modelOptionsByBrand,
    brandOptionLabels: defaultFilterSeeds.simple.brandOptionLabels,
    factoryOptionLabels: {},
    modelOptionLabelsByBrand: defaultFilterSeeds.simple.modelOptionLabelsByBrand,
    customFields: getGroupDefaultFields({ key: fallbackGroupKey, labelKo: fallbackGroupKey, labelEn: fallbackGroupKey })
  };
  const selectableBadgeDefs = getProductBadgeDefinitions();
  const selectableBadgeIdSet = new Set(selectableBadgeDefs.map((item) => Number(item.id)));
  const selectedBadgeIds = normalizeRequestedBadgeIds(req.body.badgeIds, selectableBadgeIdSet);

  const submission = buildAdminProductSubmission(req.body, selectedGroup);
  if (submission.error) {
    setFlash(req, 'error', submission.error);
    return res.redirect(backPath);
  }

  const {
    brand,
    model,
    subModel,
    reference,
    factoryName,
    versionName,
    movement,
    caseSize,
    dialColor,
    caseMaterial,
    strapMaterial,
    features,
    price,
    shippingPeriod
  } = submission.mapped;

  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = uploadedFiles.map((file) => fileUrl(file)).filter(Boolean);
  const normalizeImagePathList = (rawList) => {
    const list = [];
    const seen = new Set();
    (Array.isArray(rawList) ? rawList : []).forEach((rawPath) => {
      const safePath = String(rawPath || '').trim();
      if (!safePath) return;
      const dedupeKey = safePath.toLowerCase();
      if (seen.has(dedupeKey)) return;
      seen.add(dedupeKey);
      list.push(safePath);
    });
    return list;
  };
  const parseExistingImageOrderPayload = (rawValue) => {
    const rawText = String(rawValue || '').trim();
    if (!rawText) return [];

    const candidateTexts = [rawText];
    try {
      const decoded = decodeURIComponent(rawText);
      if (decoded && decoded !== rawText) {
        candidateTexts.push(decoded);
      }
    } catch {
      // ignore malformed URI sequence and fall back to original text
    }

    for (const candidateText of candidateTexts) {
      try {
        const parsed = JSON.parse(candidateText);
        return normalizeImagePathList(parsed);
      } catch {
        // try next candidate
      }
    }

    return [];
  };

  const currentImageRows = db
    .prepare(
      `
        SELECT image_path
        FROM product_images
        WHERE product_id = ?
        ORDER BY sort_order ASC, id ASC
      `
    )
    .all(id);
  const currentImageList = normalizeImagePathList(currentImageRows.map((row) => row.image_path));
  if (currentImageList.length === 0) {
    const baseImage = db.prepare('SELECT image_path FROM products WHERE id = ? LIMIT 1').get(id);
    const fallbackImagePath = String(baseImage?.image_path || '').trim();
    if (fallbackImagePath) {
      currentImageList.push(fallbackImagePath);
    }
  }

  const hasExistingImageOrderPayload = Object.prototype.hasOwnProperty.call(req.body || {}, 'existingImageOrderJson');
  const requestedExistingImageOrder = parseExistingImageOrderPayload(req.body?.existingImageOrderJson);
  const currentImageSet = new Set(currentImageList.map((pathValue) => pathValue.toLowerCase()));
  const keptExistingImageList = hasExistingImageOrderPayload
    ? requestedExistingImageOrder.filter((pathValue) => currentImageSet.has(pathValue.toLowerCase()))
    : currentImageList;

  if (keptExistingImageList.length + uploadedImages.length > MAX_UPLOAD_IMAGE_COUNT) {
    setFlash(req, 'error', `이미지는 최대 ${MAX_UPLOAD_IMAGE_COUNT}장까지 유지할 수 있습니다. 기존 이미지를 일부 삭제하거나 새 업로드 수를 줄여 주세요.`);
    return res.redirect(backPath);
  }

  const finalImageList = [...keptExistingImageList, ...uploadedImages];
  const imagePath = finalImageList[0] || '';

  db.prepare(
    `
      UPDATE products
      SET
        category_group = ?,
        brand = ?,
        model = ?,
        sub_model = ?,
        reference = ?,
        factory_name = ?,
        version_name = ?,
        movement = ?,
        case_size = ?,
        dial_color = ?,
        case_material = ?,
        strap_material = ?,
        features = ?,
        price = ?,
        shipping_period = ?,
        image_path = ?,
        extra_fields_json = ?
      WHERE id = ?
    `
  ).run(
    selectedGroup.key,
    brand,
    model,
    subModel,
    reference,
    factoryName,
    versionName,
    movement,
    caseSize,
    dialColor,
    caseMaterial,
    strapMaterial,
    features,
    price,
    shippingPeriod,
    imagePath,
    submission.extraFieldsJson,
    id
  );

  db.prepare('DELETE FROM product_images WHERE product_id = ?').run(id);
  if (finalImageList.length > 0) {
    const insertImage = db.prepare(
      `
        INSERT INTO product_images (product_id, image_path, sort_order)
        VALUES (?, ?, ?)
      `
    );
    finalImageList.forEach((src, index) => {
      insertImage.run(id, src, index);
    });
  }

  replaceProductBadgeLinks(id, selectedBadgeIds);

  setFlash(req, 'success', '상품 정보가 수정되었습니다.');
  return res.redirect('/admin/products?section=list');
}));

app.post('/admin/product/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 상품입니다.');
    return res.redirect(backPath);
  }

  const exists = db.prepare('SELECT id FROM products WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', '상품을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM products WHERE id = ?').run(id);
  setFlash(req, 'success', '상품이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product/watermark/apply-all', requireAdmin, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=list');
  const result = await applyChronoLabWatermarkToAllProductImages();
  const failedReasonSummary = Object.entries(result.failedReasonCounts || {})
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
    .slice(0, 3)
    .map(([reason, count]) => `${reason} ${count}건`)
    .join(', ');
  setFlash(
    req,
    'success',
    `워터마크 일괄 적용 완료: 총 ${result.totalCount}개 중 ${result.processedCount}개 처리(원격 ${result.convertedRemoteCount}개 변환), ${result.skippedCount}개 스킵, ${result.failedCount}개 실패${failedReasonSummary ? ` (${failedReasonSummary})` : ''}`
  );
  return res.redirect(backPath);
}));

app.post('/admin/product/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=list');
  const id = Number(req.params.id);
  const product = db.prepare('SELECT id, is_active FROM products WHERE id = ? LIMIT 1').get(id);
  if (!product) {
    setFlash(req, 'error', '상품을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextState = Number(product.is_active) === 1 ? 0 : 1;
  db.prepare('UPDATE products SET is_active = ? WHERE id = ?').run(nextState, id);
  setFlash(req, 'success', '상품 노출 상태를 변경했습니다.');
  res.redirect(backPath);
});

app.post('/admin/product/:id/sold-out-toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 상품입니다.');
    return res.redirect(backPath);
  }

  const product = db.prepare('SELECT id, is_sold_out FROM products WHERE id = ? LIMIT 1').get(id);
  if (!product) {
    setFlash(req, 'error', '상품을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextState = Number(product.is_sold_out || 0) === 1 ? 0 : 1;
  db.prepare('UPDATE products SET is_sold_out = ? WHERE id = ?').run(nextState, id);
  setFlash(
    req,
    'success',
    nextState === 1 ? '상품을 판매완료 처리했습니다.' : '상품 판매완료를 해제했습니다.'
  );
  return res.redirect(backPath);
});

app.post('/admin/product-badge/create', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=badges');
  const labelKo = normalizeProductBadgeLabel(req.body.labelKo || '');
  const labelEn = normalizeProductBadgeLabel(req.body.labelEn || '');
  const colorTheme = normalizeProductBadgeColorTheme(req.body.colorTheme || '', PRODUCT_BADGE_DEFAULT_COLOR_THEME);

  if (!labelKo || !labelEn) {
    setFlash(req, 'error', '배지명(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const maxSortOrderRow = db
    .prepare('SELECT COALESCE(MAX(sort_order), 0) AS max_sort_order FROM product_badge_defs')
    .get();
  const fallbackSortOrder = parseNonNegativeInt(maxSortOrderRow?.max_sort_order, 0) + 1;
  const sortOrder = parseNonNegativeInt(req.body.sortOrder, fallbackSortOrder);
  const requestedCode = normalizeProductBadgeCode(
    req.body.code || '',
    buildProductBadgeCodeFromLabels(labelKo, labelEn, 'badge')
  );
  const code = makeUniqueProductBadgeCode(requestedCode);

  db.prepare(
    `
      INSERT INTO product_badge_defs (code, label_ko, label_en, color_theme, sort_order, updated_at)
      VALUES (?, ?, ?, ?, ?, datetime('now'))
    `
  ).run(code, labelKo, labelEn, colorTheme, sortOrder);

  setFlash(req, 'success', '상품 배지가 추가되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-badge/:id/update', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=badges');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 배지입니다.');
    return res.redirect(backPath);
  }

  const existing = db
    .prepare('SELECT id, code, color_theme, sort_order FROM product_badge_defs WHERE id = ? LIMIT 1')
    .get(id);
  if (!existing) {
    setFlash(req, 'error', '배지를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const labelKo = normalizeProductBadgeLabel(req.body.labelKo || '');
  const labelEn = normalizeProductBadgeLabel(req.body.labelEn || '');
  if (!labelKo || !labelEn) {
    setFlash(req, 'error', '배지명(KR/EN)을 모두 입력해 주세요.');
    return res.redirect(backPath);
  }

  const sortOrder = parseNonNegativeInt(req.body.sortOrder, parseNonNegativeInt(existing.sort_order, 0));
  const colorTheme = normalizeProductBadgeColorTheme(
    req.body.colorTheme || '',
    existing.color_theme || PRODUCT_BADGE_DEFAULT_COLOR_THEME
  );
  const requestedCode = normalizeProductBadgeCode(
    req.body.code || existing.code || '',
    buildProductBadgeCodeFromLabels(labelKo, labelEn, existing.code || 'badge')
  );
  const code = makeUniqueProductBadgeCode(requestedCode, id);

  db.prepare(
    `
      UPDATE product_badge_defs
      SET code = ?, label_ko = ?, label_en = ?, color_theme = ?, sort_order = ?, updated_at = datetime('now')
      WHERE id = ?
    `
  ).run(code, labelKo, labelEn, colorTheme, sortOrder, id);

  setFlash(req, 'success', '상품 배지가 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/product-badge/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=badges');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 배지입니다.');
    return res.redirect(backPath);
  }

  const existing = db.prepare('SELECT id FROM product_badge_defs WHERE id = ? LIMIT 1').get(id);
  if (!existing) {
    setFlash(req, 'error', '배지를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM product_badge_defs WHERE id = ?').run(id);
  setFlash(req, 'success', '상품 배지가 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/notice/create', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, (req, res) => {
  const backPath = safeBackPath(req, '/admin/notices?section=create');
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  const isPopup = req.body.isPopup === 'on' ? 1 : 0;

  if (!title || !content) {
    setFlash(req, 'error', '공지 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedImages = collectUploadedImageUrls(req);
  const imagePath = uploadedImages[0] || '';
  const imagePathsJson = serializeImagePaths(uploadedImages);

  db.prepare('INSERT INTO notices (title, content, image_path, image_paths_json, is_popup) VALUES (?, ?, ?, ?, ?)').run(
    title,
    content,
    imagePath,
    imagePathsJson,
    isPopup
  );

  setFlash(req, 'success', '공지사항이 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/notice/:id/update', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, (req, res) => {
  const backPath = safeBackPath(req, '/admin/notices?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 공지입니다.');
    return res.redirect(backPath);
  }

  const existing = db.prepare('SELECT id, image_path, image_paths_json FROM notices WHERE id = ? LIMIT 1').get(id);
  if (!existing) {
    setFlash(req, 'error', '공지사항을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  const isPopup = req.body.isPopup === 'on' ? 1 : 0;
  if (!title || !content) {
    setFlash(req, 'error', '공지 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedImages = collectUploadedImageUrls(req);
  const existingImages = getRecordImagePaths(existing);
  const nextImagePaths = uploadedImages.length > 0 ? uploadedImages : existingImages;
  const nextImagePath = nextImagePaths[0] || '';
  const nextImagePathsJson = serializeImagePaths(nextImagePaths);

  db.prepare(
    `
      UPDATE notices
      SET title = ?, content = ?, image_path = ?, image_paths_json = ?, is_popup = ?
      WHERE id = ?
    `
  ).run(title, content, nextImagePath, nextImagePathsJson, isPopup, id);

  setFlash(req, 'success', '공지사항이 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/notice/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/notices?section=list');
  const id = Number(req.params.id);
  const item = db.prepare('SELECT id, is_hidden FROM notices WHERE id = ? LIMIT 1').get(id);
  if (!item) {
    setFlash(req, 'error', '공지사항을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextHidden = Number(item.is_hidden || 0) === 1 ? 0 : 1;
  db.prepare('UPDATE notices SET is_hidden = ? WHERE id = ?').run(nextHidden, id);
  setFlash(req, 'success', nextHidden ? '공지사항을 숨김 처리했습니다.' : '공지사항을 다시 표시합니다.');
  return res.redirect(backPath);
});

app.post('/admin/notice/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/notices?section=list');
  const id = Number(req.params.id);
  const exists = db.prepare('SELECT id FROM notices WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', '공지사항을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM notices WHERE id = ?').run(id);
  setFlash(req, 'success', '공지사항이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/news/create', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/news?section=create');
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();

  if (!title || !content) {
    setFlash(req, 'error', '뉴스 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = collectUploadedImageUrls(req);
  const imagePath = uploadedImages[0] || '';
  const imagePathsJson = serializeImagePaths(uploadedImages);

  db.prepare('INSERT INTO news_posts (title, content, image_path, image_paths_json) VALUES (?, ?, ?, ?)').run(
    title,
    content,
    imagePath,
    imagePathsJson
  );

  setFlash(req, 'success', '뉴스가 등록되었습니다.');
  res.redirect(backPath);
}));

app.post('/admin/news/:id/update', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/news?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 뉴스입니다.');
    return res.redirect(backPath);
  }

  const existing = db.prepare('SELECT id, image_path, image_paths_json FROM news_posts WHERE id = ? LIMIT 1').get(id);
  if (!existing) {
    setFlash(req, 'error', '뉴스 게시글을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  if (!title || !content) {
    setFlash(req, 'error', '뉴스 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = collectUploadedImageUrls(req);
  const existingImages = getRecordImagePaths(existing);
  const nextImagePaths = uploadedImages.length > 0 ? uploadedImages : existingImages;
  const nextImagePath = nextImagePaths[0] || '';
  const nextImagePathsJson = serializeImagePaths(nextImagePaths);

  db.prepare(
    `
      UPDATE news_posts
      SET title = ?, content = ?, image_path = ?, image_paths_json = ?
      WHERE id = ?
    `
  ).run(title, content, nextImagePath, nextImagePathsJson, id);

  setFlash(req, 'success', '뉴스 게시글이 수정되었습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/news/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/news?section=list');
  const id = Number(req.params.id);
  const item = db.prepare('SELECT id, is_hidden FROM news_posts WHERE id = ? LIMIT 1').get(id);
  if (!item) {
    setFlash(req, 'error', '뉴스 게시글을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextHidden = Number(item.is_hidden || 0) === 1 ? 0 : 1;
  db.prepare('UPDATE news_posts SET is_hidden = ? WHERE id = ?').run(nextHidden, id);
  setFlash(req, 'success', nextHidden ? '뉴스 게시글을 숨김 처리했습니다.' : '뉴스 게시글을 다시 표시합니다.');
  return res.redirect(backPath);
});

app.post('/admin/news/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/news?section=list');
  const id = Number(req.params.id);
  const exists = db.prepare('SELECT id FROM news_posts WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', '뉴스 게시글을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM news_posts WHERE id = ?').run(id);
  setFlash(req, 'success', '뉴스 게시글이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/qc/create', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/qc?section=create');
  const orderNo = String(req.body.orderNo || '').trim();
  const note = String(req.body.note || '').trim();
  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = collectUploadedImageUrls(req);

  if (!orderNo || uploadedImages.length === 0) {
    setFlash(req, 'error', '주문번호와 이미지를 입력해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('INSERT INTO qc_items (order_no, image_path, image_paths_json, note) VALUES (?, ?, ?, ?)').run(
    orderNo,
    uploadedImages[0] || '',
    serializeImagePaths(uploadedImages),
    note
  );

  setFlash(req, 'success', 'QC 항목이 등록되었습니다.');
  res.redirect(backPath);
}));

app.post('/admin/qc/:id/update', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/qc?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 QC 항목입니다.');
    return res.redirect(backPath);
  }

  const existing = db.prepare('SELECT id, image_path, image_paths_json FROM qc_items WHERE id = ? LIMIT 1').get(id);
  if (!existing) {
    setFlash(req, 'error', 'QC 항목을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const orderNo = String(req.body.orderNo || '').trim();
  const note = String(req.body.note || '').trim();
  if (!orderNo) {
    setFlash(req, 'error', '주문번호를 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedFiles = Array.isArray(req.files) ? req.files : [];
  await applyChronoLabWatermarkToUploads(uploadedFiles);
  const uploadedImages = collectUploadedImageUrls(req);
  const existingImages = getRecordImagePaths(existing);
  const nextImagePaths = uploadedImages.length > 0 ? uploadedImages : existingImages;
  const nextImagePath = nextImagePaths[0] || '';
  const nextImagePathsJson = serializeImagePaths(nextImagePaths);

  if (!nextImagePath) {
    setFlash(req, 'error', 'QC 이미지를 등록해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare(
    `
      UPDATE qc_items
      SET order_no = ?, note = ?, image_path = ?, image_paths_json = ?
      WHERE id = ?
    `
  ).run(orderNo, note, nextImagePath, nextImagePathsJson, id);

  setFlash(req, 'success', 'QC 항목이 수정되었습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/qc/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/qc?section=list');
  const id = Number(req.params.id);
  const item = db.prepare('SELECT id, is_hidden FROM qc_items WHERE id = ? LIMIT 1').get(id);
  if (!item) {
    setFlash(req, 'error', 'QC 항목을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextHidden = Number(item.is_hidden || 0) === 1 ? 0 : 1;
  db.prepare('UPDATE qc_items SET is_hidden = ? WHERE id = ?').run(nextHidden, id);
  setFlash(req, 'success', nextHidden ? 'QC 항목을 숨김 처리했습니다.' : 'QC 항목을 다시 표시합니다.');
  return res.redirect(backPath);
});

app.post('/admin/qc/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/qc?section=list');
  const id = Number(req.params.id);
  const exists = db.prepare('SELECT id FROM qc_items WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', 'QC 항목을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM qc_items WHERE id = ?').run(id);
  setFlash(req, 'success', 'QC 항목이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/order/:id/confirm', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db
    .prepare(
      `
        SELECT
          id,
          order_no,
          status,
          total_price,
          created_by_user_id,
          awarded_points,
          points_awarded_at,
          point_rate_snapshot,
          sales_margin_krw_snapshot,
          sales_cost_krw_snapshot,
          sales_synced_at
        FROM orders
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const current = normalizeOrderStatus(order.status);
  if (current !== ORDER_STATUS.PENDING_REVIEW) {
    setFlash(req, 'error', '입금확인중 상태에서만 입금확인 처리할 수 있습니다.');
    return res.redirect(backPath);
  }

  const memberUserId = Number(order.created_by_user_id || 0);

  const confirmResult = db.transaction(() => {
    const updated = db
      .prepare(
        `
          UPDATE orders
          SET status = ?, checked_at = datetime('now')
          WHERE id = ? AND status = ?
        `
      )
      .run(ORDER_STATUS.ORDER_CONFIRMED, id, ORDER_STATUS.PENDING_REVIEW);

    if (updated.changes === 0) {
      return { updated: 0 };
    }

    const awardedPointsForMargin = parseNonNegativeInt(order.awarded_points, 0);
    const hasSalesSnapshot = Boolean(String(order.sales_synced_at || '').trim());
    const marginSnapshot = hasSalesSnapshot
      ? Math.round(Number(order.sales_margin_krw_snapshot || 0))
      : Math.round(
          parseNonNegativeNumber(order.total_price, 0) -
          parseNonNegativeNumber(order.sales_cost_krw_snapshot, 0)
        );
    const realMarginSnapshot = Math.round(marginSnapshot - awardedPointsForMargin);
    db.prepare(
      `
        UPDATE orders
        SET
          sales_real_margin_krw_snapshot = ?,
          sales_synced_at = datetime('now')
        WHERE id = ?
      `
    ).run(realMarginSnapshot, id);

    return { updated: updated.changes };
  })();

  if (confirmResult.updated === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(order.id, order.order_no, ORDER_STATUS.PENDING_REVIEW, ORDER_STATUS.ORDER_CONFIRMED, 'admin:confirm');
  incrementFunnelEvent(toKstDate(), FUNNEL_EVENT.PAYMENT_CONFIRMED);
  syncPaidOrdersToSalesWorkbookSafely({ orderIds: [id], forceResync: true });

  const shouldMentionPointCredit =
    memberUserId > 0 && parsePointRate(order.point_rate_snapshot, getLegacyPurchasePointRateSetting()) > 0;
  setFlash(
    req,
    'success',
    shouldMentionPointCredit
      ? '입금확인 처리되었습니다. 포인트는 배송완료 후 자동 적립됩니다.'
      : '입금확인 처리되었습니다.'
  );
  return res.redirect(backPath);
});

app.post('/admin/order/:id/cancel', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db
    .prepare(
      `
        SELECT
          id,
          order_no,
          status,
          created_by_user_id,
          used_points
        FROM orders
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const currentStatus = normalizeOrderStatus(order.status);
  const cancellableStatuses = new Set([ORDER_STATUS.PENDING_REVIEW, ORDER_STATUS.ORDER_CONFIRMED]);
  if (!cancellableStatuses.has(currentStatus)) {
    setFlash(req, 'error', '입금확인중/입금확인 상태에서만 주문취소가 가능합니다.');
    return res.redirect(backPath);
  }

  const statusDbValues = getOrderStatusFilterDbValues(currentStatus);
  if (!Array.isArray(statusDbValues) || statusDbValues.length === 0) {
    setFlash(req, 'error', '주문 상태 확인 중 오류가 발생했습니다.');
    return res.redirect(backPath);
  }

  const usedPoints = parseNonNegativeInt(order.used_points, 0);
  const memberUserId = Number(order.created_by_user_id || 0);

  const cancelResult = db.transaction(() => {
    const updated = db
      .prepare(
        `
          UPDATE orders
          SET status = ?
          WHERE id = ? AND UPPER(TRIM(status)) IN (${statusDbValues.map(() => '?').join(', ')})
        `
      )
      .run(ORDER_STATUS.CANCELLED, id, ...statusDbValues);

    if (updated.changes === 0) {
      return { updated: 0, refundedPoints: 0 };
    }

    let refundedPoints = 0;
    if (memberUserId > 0 && usedPoints > 0) {
      const pointRefunded = db
        .prepare('UPDATE users SET reward_points = reward_points + ? WHERE id = ? AND is_admin = 0')
        .run(usedPoints, memberUserId);
      if (pointRefunded.changes > 0) {
        refundedPoints = usedPoints;
      }
    }

    return { updated: updated.changes, refundedPoints };
  })();

  if (cancelResult.updated === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(order.id, order.order_no, currentStatus, ORDER_STATUS.CANCELLED, 'admin:cancel');
  removeOrdersFromSalesWorkbookSafely({ orderIds: [id] });

  const refundedPointText = cancelResult.refundedPoints > 0
    ? ` 사용 포인트 ${formatPrice(cancelResult.refundedPoints)}P가 환급되었습니다.`
    : '';
  setFlash(req, 'success', `주문취소 처리되었습니다.${refundedPointText}`.trim());
  return res.redirect(backPath);
});

app.post('/admin/order/:id/ready', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, order_no, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const current = normalizeOrderStatus(order.status);
  if (current !== ORDER_STATUS.ORDER_CONFIRMED) {
    setFlash(req, 'error', '입금확인 상태에서만 출고완료 처리할 수 있습니다.');
    return res.redirect(backPath);
  }

  const updated = db.prepare(
    `
      UPDATE orders
      SET status = ?, ready_to_ship_at = datetime('now')
      WHERE id = ? AND status = ?
    `
  ).run(ORDER_STATUS.READY_TO_SHIP, id, ORDER_STATUS.ORDER_CONFIRMED);

  if (updated.changes === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(order.id, order.order_no, ORDER_STATUS.ORDER_CONFIRMED, ORDER_STATUS.READY_TO_SHIP, 'admin:ready');

  setFlash(req, 'success', '출고완료 처리되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/order/:id/start-shipping', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, order_no, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const current = normalizeOrderStatus(order.status);
  if (current !== ORDER_STATUS.READY_TO_SHIP) {
    setFlash(req, 'error', '출고완료 상태에서만 송장등록(배송시작) 처리할 수 있습니다.');
    return res.redirect(backPath);
  }

  const trackingCarrier = normalizeTrackingCarrier(req.body.trackingCarrier);
  const trackingNumber = normalizeTrackingNumber(req.body.trackingNumber);

  if (!trackingNumber) {
    setFlash(req, 'error', '송장번호를 입력해 주세요.');
    return res.redirect(backPath);
  }

  if (!TRACKING_NUMBER_REGEX.test(trackingNumber)) {
    setFlash(req, 'error', '송장번호 형식이 올바르지 않습니다. (영문/숫자/- 6~40자)');
    return res.redirect(backPath);
  }

  const updated = db.prepare(
    `
      UPDATE orders
      SET
        status = ?,
        tracking_carrier = ?,
        tracking_number = ?,
        tracking_last_event = '',
        tracking_last_checked_at = NULL,
        shipping_started_at = datetime('now')
      WHERE id = ? AND status = ?
    `
  ).run(ORDER_STATUS.SHIPPING, trackingCarrier, trackingNumber, id, ORDER_STATUS.READY_TO_SHIP);

  if (updated.changes === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(
    order.id,
    order.order_no,
    ORDER_STATUS.READY_TO_SHIP,
    ORDER_STATUS.SHIPPING,
    `admin:shipping-start:${trackingCarrier}:${trackingNumber}`
  );

  setFlash(req, 'success', '송장등록(배송시작) 처리되었습니다. 자동 배송조회가 시작됩니다.');
  void pollTrackingAndAutoCompleteOrders(true);
  return res.redirect(backPath);
});

app.post('/admin/order/:id/update-tracking', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, order_no, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeOrderStatus(order.status) !== ORDER_STATUS.SHIPPING) {
    setFlash(req, 'error', '배송중 상태에서만 송장 수정이 가능합니다.');
    return res.redirect(backPath);
  }

  const trackingCarrier = normalizeTrackingCarrier(req.body.trackingCarrier);
  const trackingNumber = normalizeTrackingNumber(req.body.trackingNumber);

  if (!trackingNumber || !TRACKING_NUMBER_REGEX.test(trackingNumber)) {
    setFlash(req, 'error', '송장번호 형식이 올바르지 않습니다. (영문/숫자/- 6~40자)');
    return res.redirect(backPath);
  }

  const updated = db.prepare(
    `
      UPDATE orders
      SET
        tracking_carrier = ?,
        tracking_number = ?,
        tracking_last_event = '',
        tracking_last_checked_at = NULL
      WHERE id = ? AND status = ?
    `
  ).run(trackingCarrier, trackingNumber, id, ORDER_STATUS.SHIPPING);

  if (updated.changes === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(
    order.id,
    order.order_no,
    ORDER_STATUS.SHIPPING,
    ORDER_STATUS.SHIPPING,
    `admin:tracking-update:${trackingCarrier}:${trackingNumber}`
  );

  setFlash(req, 'success', '송장 정보가 수정되었습니다. 자동 조회를 다시 실행합니다.');
  void pollTrackingAndAutoCompleteOrders(true);
  return res.redirect(backPath);
});

app.post('/admin/order/:id/sync-tracking', requireAdmin, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db
    .prepare('SELECT id, order_no, status, tracking_carrier, tracking_number FROM orders WHERE id = ? LIMIT 1')
    .get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeOrderStatus(order.status) !== ORDER_STATUS.SHIPPING) {
    setFlash(req, 'error', '배송중 상태에서만 조회할 수 있습니다.');
    return res.redirect(backPath);
  }

  if (!normalizeTrackingNumber(order.tracking_number)) {
    setFlash(req, 'error', '송장번호가 등록되지 않았습니다.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(
    order.id,
    order.order_no,
    ORDER_STATUS.SHIPPING,
    ORDER_STATUS.SHIPPING,
    'admin:tracking-sync'
  );
  await pollTrackingAndAutoCompleteOrders(true);
  setFlash(req, 'success', '송장 상태 조회를 요청했습니다.');
  return res.redirect(backPath);
}));

app.post('/admin/order/:id/mark-delivered', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, order_no, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  if (normalizeOrderStatus(order.status) !== ORDER_STATUS.SHIPPING) {
    setFlash(req, 'error', '배송중 상태에서만 수동 배송완료 처리가 가능합니다.');
    return res.redirect(backPath);
  }

  const updated = db.prepare(
    `
      UPDATE orders
      SET
        status = ?,
        delivered_at = datetime('now'),
        tracking_last_checked_at = datetime('now', '+9 hours'),
        tracking_last_event = CASE
          WHEN COALESCE(tracking_last_event, '') = '' THEN '관리자 수동 배송완료 처리'
          ELSE tracking_last_event
        END
      WHERE id = ? AND status = ?
    `
  ).run(ORDER_STATUS.DELIVERED, id, ORDER_STATUS.SHIPPING);

  if (updated.changes === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(order.id, order.order_no, ORDER_STATUS.SHIPPING, ORDER_STATUS.DELIVERED, 'admin:delivered-manual');

  const awardResult = awardDeliveredOrderPoints(order.id);
  if (awardResult.awardedPoints > 0) {
    setFlash(
      req,
      'success',
      `수동 배송완료 처리되었습니다. 회원에게 ${formatPrice(awardResult.awardedPoints)}포인트가 적립되었습니다.`
    );
    return res.redirect(backPath);
  }

  setFlash(req, 'success', '수동 배송완료 처리되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/inquiry/:id/reply', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/inquiries?section=reply');
  const id = Number(req.params.id);
  const exists = db.prepare('SELECT id FROM inquiries WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', '문의를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const replyContent = String(req.body.replyContent || '').trim();

  if (!replyContent) {
    setFlash(req, 'error', '답변 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('UPDATE inquiries SET reply_content = ?, replied_at = datetime(\'now\') WHERE id = ?').run(
    replyContent,
    id
  );

  setFlash(req, 'success', '문의 답변이 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/inquiry/:id/update', requireAdmin, upload.array('image', 20), requireAuthenticatedMultipartCsrf, (req, res) => {
  const backPath = safeBackPath(req, '/admin/inquiries?section=list');
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    setFlash(req, 'error', '유효하지 않은 문의입니다.');
    return res.redirect(backPath);
  }

  const existing = db
    .prepare('SELECT id, image_path, image_paths_json FROM inquiries WHERE id = ? LIMIT 1')
    .get(id);
  if (!existing) {
    setFlash(req, 'error', '문의를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  const replyContent = String(req.body.replyContent || '').trim();
  if (!title || !content) {
    setFlash(req, 'error', '문의 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const uploadedImages = collectUploadedImageUrls(req);
  const existingImages = getRecordImagePaths(existing);
  const nextImagePaths = uploadedImages.length > 0 ? uploadedImages : existingImages;
  const nextImagePath = nextImagePaths[0] || '';
  const nextImagePathsJson = serializeImagePaths(nextImagePaths);
  db.prepare(
    `
      UPDATE inquiries
      SET
        title = ?,
        content = ?,
        image_path = ?,
        image_paths_json = ?,
        reply_content = ?,
        replied_at = CASE
          WHEN ? != '' THEN COALESCE(replied_at, datetime('now'))
          ELSE NULL
        END
      WHERE id = ?
    `
  ).run(title, content, nextImagePath, nextImagePathsJson, replyContent, replyContent, id);

  setFlash(req, 'success', '문의 항목이 수정되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/inquiry/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/inquiries?section=list');
  const id = Number(req.params.id);
  const item = db.prepare('SELECT id, is_hidden FROM inquiries WHERE id = ? LIMIT 1').get(id);
  if (!item) {
    setFlash(req, 'error', '문의를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextHidden = Number(item.is_hidden || 0) === 1 ? 0 : 1;
  db.prepare('UPDATE inquiries SET is_hidden = ? WHERE id = ?').run(nextHidden, id);
  setFlash(req, 'success', nextHidden ? '문의 항목을 숨김 처리했습니다.' : '문의 항목을 다시 표시합니다.');
  return res.redirect(backPath);
});

app.post('/admin/inquiry/:id/delete', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/inquiries?section=list');
  const id = Number(req.params.id);
  const exists = db.prepare('SELECT id FROM inquiries WHERE id = ? LIMIT 1').get(id);
  if (!exists) {
    setFlash(req, 'error', '문의를 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  db.prepare('DELETE FROM inquiries WHERE id = ?').run(id);
  setFlash(req, 'success', '문의 항목이 삭제되었습니다.');
  return res.redirect(backPath);
});

app.use((req, res) => {
  res.status(404).render('simple-error', { title: 'Not Found', message: '페이지를 찾을 수 없습니다.' });
});

app.use((error, req, res, next) => {
  logDetailedError('request-failed', error, {
    request: buildRequestLogContext(req)
  });

  const isUnsupportedType = Boolean(error?.message?.includes('지원되지 않는 파일 형식'));
  const isFileTooLarge = error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE';
  const isFileCountExceeded = error instanceof multer.MulterError && error.code === 'LIMIT_FILE_COUNT';
  const isUnexpectedUploadField = error instanceof multer.MulterError && error.code === 'LIMIT_UNEXPECTED_FILE';
  const acceptHeader = String(req.get('accept') || '').toLowerCase();
  const contentTypeHeader = String(req.get('content-type') || '').toLowerCase();
  const wantsJsonResponse = (
    req.path === '/health' ||
    req.path.startsWith('/api/') ||
    req.xhr === true ||
    acceptHeader.includes('application/json') ||
    contentTypeHeader.includes('application/json')
  );
  const message = isUnsupportedType
    ? error.message
    : isFileTooLarge
      ? `업로드 파일은 최대 ${MAX_UPLOAD_FILE_SIZE_MB}MB까지 가능합니다. 파일 크기를 줄여 다시 시도해 주세요.`
      : isFileCountExceeded
        ? `이미지는 한 번에 최대 ${MAX_UPLOAD_IMAGE_COUNT}장까지 업로드할 수 있습니다.`
        : isUnexpectedUploadField
          ? '지원되지 않는 업로드 항목이 포함되어 요청이 차단되었습니다.'
      : '일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.';

  if (wantsJsonResponse) {
    if (isFileTooLarge) {
      return res.status(413).json({ ok: false, error: 'file_too_large', message, maxMb: MAX_UPLOAD_FILE_SIZE_MB });
    }
    if (isFileCountExceeded) {
      return res.status(400).json({ ok: false, error: 'file_count_exceeded', message, maxCount: MAX_UPLOAD_IMAGE_COUNT });
    }
    if (isUnexpectedUploadField) {
      return res.status(400).json({ ok: false, error: 'unexpected_upload_field', message });
    }
    if (isUnsupportedType) {
      return res.status(400).json({ ok: false, error: 'unsupported_file_type', message });
    }
    return res.status(500).json({ ok: false, error: 'internal_server_error', message });
  }

  if (req.method === 'POST') {
    setFlash(req, 'error', message);
    return res.redirect(safeBackPath(req, '/main'));
  }

  return res.status(500).render('simple-error', { title: 'Error', message });
});

process.on('unhandledRejection', (reason) => {
  logDetailedError('unhandled-rejection', reason instanceof Error ? reason : new Error(String(reason)), {
    reason: sanitizeLogValue(reason)
  });
});

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Chrono Lab server running on http://localhost:${PORT}`);
});

void syncSalesWorkbookFromLegacySheetOnce();

setInterval(() => {
  void pollTrackingAndAutoCompleteOrders(false);
}, TRACKING_AUTO_POLL_MS);
