import path from 'path';
import { promises as fs, existsSync, mkdirSync } from 'fs';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { execFile } from 'child_process';
import { promisify } from 'util';
import express from 'express';
import session from 'express-session';
import cookieParser from 'cookie-parser';
import multer from 'multer';
import bcrypt from 'bcryptjs';
import nodemailer from 'nodemailer';
import sharp from 'sharp';
import {
  db,
  initDb,
  getDefaultMenus,
  getDefaultProductGroupConfigs,
  getPostCounts,
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
const ASSET_VERSION = process.env.RENDER_GIT_COMMIT || `${Date.now()}`;
const execFileAsync = promisify(execFile);

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const USERNAME_REGEX = /^[a-z0-9]{4,20}$/;
const ACCOUNT_LOOKUP_REGEX = /^[A-Za-z0-9._-]{2,40}$/;
const PASSWORD_REGEX = /^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[^A-Za-z0-9]).{8,}$/;
const DIGIT_PHONE_REGEX = /^[0-9]+$/;
const CUSTOMS_NO_REGEX = /^[A-Za-z0-9-]{6,30}$/;
const TRACKING_NUMBER_REGEX = /^[A-Za-z0-9-]{6,40}$/;
const PHONE_REGEX = /^[0-9]{8,20}$/;

const ADMIN_ROLE = Object.freeze({
  PRIMARY: 'PRIMARY',
  SUB: 'SUB'
});

const ORDER_STATUS = Object.freeze({
  PENDING_REVIEW: 'PENDING_REVIEW',
  ORDER_CONFIRMED: 'ORDER_CONFIRMED',
  READY_TO_SHIP: 'READY_TO_SHIP',
  SHIPPING: 'SHIPPING',
  DELIVERED: 'DELIVERED'
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
const POINT_MANAGE_SECTIONS = Object.freeze(['signup', 'level-rates']);
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
const PRODUCT_BADGE_CODE_REGEX = /^[a-z0-9][a-z0-9-]{1,39}$/;
const PRODUCT_BADGE_CODE_MAX_LENGTH = 40;
const PRODUCT_BADGE_LABEL_MAX_LENGTH = 40;
const PRODUCT_BADGE_DEFAULT_COLOR_THEME = 'slate';
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

const AUTH_ATTEMPT_WINDOW_MS = 10 * 60 * 1000;
const DEFAULT_AUTH_MAX_ATTEMPTS = 15;
const authAttemptStore = new Map();
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
const MAX_UPLOAD_FILE_SIZE_BYTES = 100 * 1024 * 1024;
const MAX_UPLOAD_FILE_SIZE_MB = Math.round(MAX_UPLOAD_FILE_SIZE_BYTES / (1024 * 1024));
const WATERMARK_SUPPORTED_FORMATS = new Set(['jpeg', 'jpg', 'png', 'webp', 'avif']);
const WATERMARK_IMAGE_ALPHA = 0.18;
const WATERMARK_DOMAIN_TEXT = 'www.chronolab.co.kr';
const WATERMARK_REMOTE_FETCH_TIMEOUT_MS = 15000;
const WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS = 4;
const WATERMARK_REMOTE_FETCH_BASE_DELAY_MS = 1500;
const WATERMARK_REMOTE_FETCH_MAX_DELAY_MS = 8000;
const WATERMARK_REMOTE_FETCH_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36';
const WATERMARK_REMOTE_CURL_MAX_BUFFER_BYTES = 45 * 1024 * 1024;

let mailTransporter = null;

const uploadStorage = multer.diskStorage({
  destination: UPLOAD_DIR,
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || '').toLowerCase();
    const safeExt = ext && ext.length <= 10 ? ext : '.jpg';
    cb(null, `${Date.now()}-${Math.round(Math.random() * 1e9)}${safeExt}`);
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({ ok: true, service: 'chronolab', timestamp: new Date().toISOString() });
});

const upload = multer({
  storage: uploadStorage,
  limits: { fileSize: MAX_UPLOAD_FILE_SIZE_BYTES },
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

app.use('/assets', express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(UPLOAD_DIR));
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '2mb' }));
app.use(cookieParser());
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'chronolab-local-secret',
    resave: false,
    saveUninitialized: false,
    cookie: {
      maxAge: 1000 * 60 * 60 * 24 * 14,
      httpOnly: true,
      sameSite: 'lax',
      secure: isProduction
    }
  })
);

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

  const collator = new Intl.Collator('en', { sensitivity: 'base', numeric: true });
  normalized.sort((a, b) => {
    const compared = collator.compare(a, b);
    if (compared !== 0) {
      return compared;
    }
    return String(a).localeCompare(String(b), 'en');
  });

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

  const collator = new Intl.Collator('en', { sensitivity: 'base', numeric: true });
  return Object.keys(mapped)
    .sort((a, b) => {
      const compared = collator.compare(a, b);
      if (compared !== 0) {
        return compared;
      }
      return String(a).localeCompare(String(b), 'en');
    })
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

function getProductFilterOptionItems(values = [], labelMap = {}, lang = 'ko') {
  const normalizedValues = normalizeProductFilterOptionList(values);
  return normalizedValues.map((value) => ({
    value,
    label: getProductFilterLabelByLang(labelMap, value, lang)
  }));
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

    normalizedGroups.push({
      key,
      labelKo: labelKo || key,
      labelEn: labelEn || key,
      mode,
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

function resolveLocalPathFromImageUrl(imageUrl = '') {
  const src = String(imageUrl || '').trim();
  if (!src.startsWith('/')) {
    return '';
  }
  if (src.startsWith('/uploads/')) {
    return path.join(UPLOAD_DIR, src.slice('/uploads/'.length));
  }
  if (src.startsWith('/assets/')) {
    return path.join(__dirname, 'public', src.slice('/assets/'.length));
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

  const canUseFetch = typeof fetch === 'function';
  let reason = 'download-error';

  if (canUseFetch) {
    for (let attempt = 1; attempt <= WATERMARK_REMOTE_FETCH_MAX_ATTEMPTS; attempt += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), WATERMARK_REMOTE_FETCH_TIMEOUT_MS);

      try {
        const parsedUrl = new URL(src);
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

function setFlash(req, type, message) {
  req.session.flash = { type, message };
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

function setAdminAuthSession(req, userRow) {
  if (!req?.session || !userRow) {
    return;
  }
  req.session.userId = Number(userRow.id);
  req.session.isAdmin = true;
  req.session.adminRole = normalizeAdminRole(userRow.admin_role);
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

function collectUploadedImageUrls(req) {
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

  return uniqueImagePathList(files.map((file) => fileUrl(file)).filter(Boolean));
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
  if (section === 'daily' || section === 'summary') {
    return 'daily';
  }
  return 'editor';
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
    done: ORDER_STATUS.DELIVERED
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
    ORDER_STATUS.DELIVERED
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
  if (key === 'price') return 'factory';
  if (key === 'preorder') return 'round';
  return 'date';
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

function buildDefaultPriceScopes() {
  const groupConfigs = getProductGroupConfigs();
  const factoryLikeGroup =
    groupConfigs.find((group) => String(group.key || '') === '공장제') ||
    groupConfigs.find((group) => String(group.mode || '') === PRODUCT_GROUP_MODE.FACTORY) ||
    null;

  const factoryNames = normalizeProductFilterOptionList(factoryLikeGroup?.factoryOptions || []);
  const scopeNames = factoryNames.length > 0 ? factoryNames : ['기본'];
  return scopeNames.map((name, idx) => ({
    id: createSalesId(`scope-p-${idx + 1}`),
    name: normalizeSalesText(name, 80) || `Factory ${idx + 1}`,
    rows: []
  }));
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
  const defaultSettings = normalizeSalesSettingValues({
    exchangeRate: SALES_DEFAULT_EXCHANGE_RATE,
    shippingFeeKrw: SALES_DEFAULT_SHIPPING_FEE_KRW,
    fxSource: 'manual',
    fxUpdatedAt: '',
    updatedAt: now
  });
  const tabs = {};
  for (const tab of SALES_MAIN_TABS) {
    const tabMode = getSalesScopeMode(tab.key);
    const tabSettings = { ...defaultSettings };
    if (tab.scopeType === 'factory') {
      tabs[tab.key] = {
        key: tab.key,
        labelKo: tab.labelKo,
        labelEn: tab.labelEn,
        scopeType: tab.scopeType,
        settings: tabSettings,
        groups: buildDefaultPriceScopes()
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
      importedFrom: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
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

  return {
    id: normalizeSalesText(rawScope?.id, 80) || createSalesId(`${fallbackPrefix}-${index + 1}`),
    name: scopeMode === 'date' ? scopeDate || normalizedName : normalizedName,
    date: scopeDate,
    settings: {
      ...scopeSettings,
      baseDate: scopeDate
    },
    rows: normalizedRows
  };
}

function normalizeSalesWorkbook(rawWorkbook = null) {
  const fallback = buildDefaultSalesWorkbook();
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
  for (const tab of SALES_MAIN_TABS) {
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

  return {
    version: 2,
    globals,
    tabs,
    meta: {
      importedFrom:
        normalizeSalesText(sourceMeta.importedFrom, 400) ||
        normalizeSalesText(getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL), 400),
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
      importedFrom: importedFrom || normalizeSalesText(getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL), 400),
      importedAt,
      updatedAt: now
    }
  };

  setSetting(SALES_WORKBOOK_SETTING_KEY, JSON.stringify(next));
  return next;
}

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

  const tabs = SALES_MAIN_TABS.map((tabInfo) => {
    const tab = normalized.tabs[tabInfo.key];
    const scopes = getSalesScopeList(tab).map((scope) => {
      const effectiveSettings = getEffectiveSalesSettings(tab, scope, normalized.globals);
      return {
        id: scope.id,
        name: scope.name,
        date: normalizeSalesDate(scope?.settings?.baseDate || scope?.date || ''),
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

function getSalesTabKeyForCategoryGroup(categoryGroup = '') {
  const normalized = normalizeProductGroupKey(categoryGroup || '');
  if (normalized === '공장제') return 'factory';
  if (normalized === '젠파츠') return 'genparts';
  if (normalized === '현지중고') return 'used';
  return '';
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
  const tabs = Array.isArray(snapshot.tabs) ? snapshot.tabs : [];
  let detectedExchangeRate = null;
  let detectedShippingFeeKrw = null;

  const nextTabs = { ...base.tabs };

  for (const importedTab of tabs) {
    if (!importedTab || importedTab.status !== 'ok') continue;
    const tabInfo = SALES_MAIN_TABS.find((item) => item.key === importedTab.key);
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
  const sourceUrl = getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL);
  const sheetId = extractGoogleSheetId(sourceUrl) || extractGoogleSheetId(SALES_SHEET_DEFAULT_URL);
  if (!sheetId) {
    throw new Error('invalid sales sheet id');
  }

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

  const workbook = buildSalesWorkbookFromSheetSnapshot({
    sourceUrl,
    sheetId,
    tabs
  });
  return saveSalesWorkbook(workbook, {
    importedFrom: sourceUrl,
    importedAt: new Date().toISOString()
  });
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

async function fetchTrackingPayload(carrierId, trackingNumber) {
  if (typeof fetch !== 'function') {
    return null;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TRACKING_REQUEST_TIMEOUT_MS);
  const url = `${TRACKING_API_BASE}/trackers/${encodeURIComponent(carrierId)}/${encodeURIComponent(
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
      const latestEvent = String(
        payload?.state?.text || payload?.state?.name || payload?.progresses?.[0]?.status?.text || ''
      ).slice(0, 200);

      db.prepare(
        `
          UPDATE orders
          SET
            tracking_last_event = ?,
            tracking_last_checked_at = datetime('now')
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
              tracking_last_checked_at = datetime('now')
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
  if (!pathValue) {
    return '/main';
  }
  if (pathValue.startsWith('/')) {
    return pathValue;
  }
  return `/${pathValue}`;
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
    name: String(rule.name || `등급${index + 1}`).trim().slice(0, 40) || `등급${index + 1}`,
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
      const name = String(item.name || item.label || '').trim().slice(0, 40) || `등급${index + 1}`;
      return {
        id: levelId,
        name,
        operator: normalizeMemberLevelOperator(item.operator || item.condition || item.direction),
        thresholdAmount: parseNonNegativeInt(item.thresholdAmount ?? item.amount ?? item.threshold, 0)
      };
    })
    .filter((item) => item.name);

  if (normalized.length > 0) {
    return normalized;
  }

  return fallbackRules.map((rule, index) => ({
    id: String(rule.id || `level-${index + 1}`),
    name: String(rule.name || `등급${index + 1}`).trim().slice(0, 40) || `등급${index + 1}`,
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
          AND UPPER(TRIM(o.status)) != ?
          AND p.category_group IN (${placeholders})
      `
    )
    .get(targetUserId, ORDER_STATUS.PENDING_REVIEW, ...includedGroups);

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
          AND UPPER(TRIM(o.status)) != ?
          AND p.category_group IN (${groupPlaceholders})
        GROUP BY o.created_by_user_id
      `
    )
    .all(...uniqueUserIds, ORDER_STATUS.PENDING_REVIEW, ...includedGroups);

  rows.forEach((row) => {
    resultMap.set(Number(row.user_id), Number(row.total_amount || 0));
  });
  return resultMap;
}

function getMemberPointProfile(userId) {
  const groupConfigs = getProductGroupConfigs();
  const availableGroups = groupConfigs.map((group) => group.key);
  const includedGroups = getMemberLevelIncludedGroupsSetting(availableGroups);
  const levelRules = getMemberLevelRulesSetting();
  const pointRateMap = getMemberLevelPointRateMapSetting(levelRules);
  const totalAmount = getMemberAccumulatedPurchaseAmount(userId, includedGroups);
  const levelRule = resolveMemberLevelByAmount(totalAmount, levelRules);
  const levelId = levelRule?.id || '';
  const levelName = levelRule?.name || '';
  const pointRate = levelId
    ? parsePointRate(pointRateMap[levelId], 0)
    : getLegacyPurchasePointRateSetting();

  return {
    totalAmount,
    levelRule,
    levelId,
    levelName,
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
    return `${parsed.pathname}${parsed.search}` || fallback;
  } catch {
    if (referer.startsWith('/')) {
      return referer;
    }
    return fallback;
  }
}

function cleanupAuthAttemptStore(nowMs) {
  if (authAttemptStore.size <= 1000) {
    return;
  }
  for (const [key, row] of authAttemptStore.entries()) {
    if (nowMs - row.windowStart > AUTH_ATTEMPT_WINDOW_MS) {
      authAttemptStore.delete(key);
    }
  }
}

function consumeAuthAttempt(req, key, limit = DEFAULT_AUTH_MAX_ATTEMPTS, windowMs = AUTH_ATTEMPT_WINDOW_MS) {
  const nowMs = Date.now();
  cleanupAuthAttemptStore(nowMs);

  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  const storeKey = `${key}:${ip}`;
  const found = authAttemptStore.get(storeKey);

  if (!found || nowMs - found.windowStart > windowMs) {
    authAttemptStore.set(storeKey, { count: 1, windowStart: nowMs });
    return { allowed: true };
  }

  found.count += 1;
  authAttemptStore.set(storeKey, found);

  return { allowed: found.count <= limit };
}

function resetAuthAttempt(req, key) {
  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  authAttemptStore.delete(`${key}:${ip}`);
}

function authAttemptGuard({ key, redirectPath, limit = DEFAULT_AUTH_MAX_ATTEMPTS }) {
  return (req, res, next) => {
    const result = consumeAuthAttempt(req, key, limit);
    if (!result.allowed) {
      setFlash(req, 'error', '시도가 너무 많습니다. 잠시 후 다시 시도해 주세요.');
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
  if (!req.session.userId) {
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
    .get(req.session.userId);

  if (!user) {
    req.session.userId = null;
    req.session.isAdmin = false;
    req.session.adminRole = '';
    req.user = null;
    return next();
  }

  if (Number(user.is_blocked) === 1) {
    req.session.userId = null;
    req.session.isAdmin = false;
    req.session.adminRole = '';
    req.user = null;
    setFlash(req, 'error', '차단된 계정입니다. 관리자에게 문의해 주세요.');
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

  req.session.isAdmin = req.user.isAdmin;

  return next();
}

app.use(loadUser);

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

  const popupNotice = db
    .prepare(
      `
        SELECT id, title, content, image_path
        FROM notices
        WHERE is_popup = 1 AND COALESCE(is_hidden, 0) = 0
        ORDER BY id DESC
        LIMIT 1
      `
    )
    .get();
  const footerNotices = db
    .prepare(
      `
        SELECT id, title
        FROM notices
        WHERE COALESCE(is_hidden, 0) = 0
        ORDER BY id DESC
        LIMIT 5
      `
    )
    .all();

  const visitCounts = getVisitCounts(today);
  const postCounts = getPostCounts(today);

  res.locals.ctx = {
    assetVersion: ASSET_VERSION,
    lang,
    t: (key) => t(lang, key),
    themeMode,
    currentUser: req.user,
    cartCount: memberCartCount,
    isAdmin: Boolean(req.user?.isAdmin),
    isPrimaryAdmin: Boolean(req.user?.isPrimaryAdmin),
    isAdminPage: isAdminPage && Boolean(req.user?.isAdmin),
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
      purchasePointRate: getLegacyPurchasePointRateSetting(),
      contactInfo: getSetting('contactInfo', ''),
      businessInfo: getSetting('businessInfo', '')
    },
    metrics: {
      visitToday: visitCounts.today,
      visitTotal: visitCounts.total,
      postToday: postCounts.today,
      postTotal: postCounts.total
    },
    popupNotice: popupNotice
      ? {
          id: Number(popupNotice.id),
          title: popupNotice.title,
          content: popupNotice.content,
          imagePath: popupNotice.image_path || ''
        }
      : null,
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
    if (!req.session.flash) {
      setFlash(
        req,
        'error',
        req.user ? '관리자 계정으로 로그인해 주세요.' : '관리자 로그인이 필요합니다.'
      );
    }
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
  const groupedProducts = productGroupConfigs.map((groupConfig) => {
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
          LIMIT 4
        `
      )
      .all(groupConfig.key);

    return {
      groupName: groupConfig.key,
      products: attachProductBadges(
        rows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)))
      )
    };
  });

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
  if (fallbackGroups.length === 0) {
    fallbackGroups.push(...SHOP_PRODUCT_GROUPS);
  }

  const groupRaw = String(req.query.group || '').trim();
  const group = fallbackGroups.includes(groupRaw) ? groupRaw : fallbackGroups[0];
  const selectedGroupConfig = productGroupMap.get(group) || {
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
  };
  const factoryTemplateGroup = isFactoryLikeGroup(selectedGroupConfig);
  const groupFactorySeedOptions = getGroupFactoryOptions(selectedGroupConfig);
  const supportsFactoryFilter = (
    factoryTemplateGroup ||
    groupFactorySeedOptions.length > 0 ||
    String(group || '').trim() === '현지중고'
  );
  const selectedBrandRaw = normalizeProductFilterOption(req.query.brand || '');
  const selectedFactoryRaw = supportsFactoryFilter
    ? normalizeProductFilterOption(req.query.factory || '')
    : '';
  const selectedModelRaw = normalizeProductFilterOption(req.query.model || '');

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
  const brands = configuredBrands.length > 0 ? configuredBrands : discoveredBrandOptions;

  const discoveredFactories = supportsFactoryFilter
    ? db
        .prepare(
          `
            SELECT DISTINCT factory_name
            FROM products
            WHERE is_active = 1 AND category_group = ?
            ORDER BY factory_name ASC
          `
        )
        .all(group)
        .map((row) => normalizeProductFilterOption(row.factory_name))
        .filter(Boolean)
    : [];
  const discoveredFactoryOptions = normalizeProductFilterOptionList(discoveredFactories);
  const configuredFactories = groupFactorySeedOptions;
  const configuredFactoryLabels = getGroupFactoryOptionLabels(selectedGroupConfig);
  const factories = supportsFactoryFilter
    ? configuredFactories.length > 0
      ? configuredFactories
      : discoveredFactoryOptions
    : [];

  const brand = brands.some((item) => item.toLowerCase() === selectedBrandRaw.toLowerCase()) ? selectedBrandRaw : '';
  const factory = factories.some((item) => item.toLowerCase() === selectedFactoryRaw.toLowerCase())
    ? selectedFactoryRaw
    : '';
  const modelOptionMap = getGroupModelOptionsByBrand(selectedGroupConfig);
  const hasModelOptionMap = Object.keys(modelOptionMap).length > 0;
  const fallbackModelOptions = getGroupModelOptions(selectedGroupConfig);
  const models = brand
    ? getGroupModelOptionsForBrand(selectedGroupConfig, brand)
    : hasModelOptionMap
      ? []
      : fallbackModelOptions;
  const model = models.some((item) => item.toLowerCase() === selectedModelRaw.toLowerCase()) ? selectedModelRaw : '';
  const modelOptionLabels = getGroupModelOptionLabelsForBrand(selectedGroupConfig, brand);
  const brandItems = getProductFilterOptionItems(brands, configuredBrandLabels, res.locals.ctx.lang);
  const factoryItems = getProductFilterOptionItems(factories, configuredFactoryLabels, res.locals.ctx.lang);
  const modelItems = getProductFilterOptionItems(models, modelOptionLabels, res.locals.ctx.lang);

  const where = ['is_active = 1', 'category_group = ?'];
  const params = [group];

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
    productGroupConfigs,
    selectedGroupConfig,
    supportsModelFilter: hasModelOptionMap || fallbackModelOptions.length > 0 || brands.length > 0,
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

  const SIMILAR_LIMIT = 4;
  const normalizedModel = String(product.model || '')
    .trim()
    .toLowerCase();

  let similarRows = [];

  if (normalizedModel) {
    similarRows = db
      .prepare(
        `
          SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
          FROM products
          WHERE is_active = 1
            AND brand = ?
            AND id != ?
            AND LOWER(TRIM(COALESCE(model, ''))) = ?
          ORDER BY id DESC
          LIMIT ?
        `
      )
      .all(product.brand, product.id, normalizedModel, SIMILAR_LIMIT);
  }

  const remainingSimilarCount = SIMILAR_LIMIT - similarRows.length;
  if (remainingSimilarCount > 0) {
    const fallbackRows = normalizedModel
      ? db
          .prepare(
            `
              SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
              FROM products
              WHERE is_active = 1
                AND brand = ?
                AND id != ?
                AND LOWER(TRIM(COALESCE(model, ''))) != ?
              ORDER BY RANDOM()
              LIMIT ?
            `
          )
          .all(product.brand, product.id, normalizedModel, remainingSimilarCount)
      : db
          .prepare(
            `
              SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json, is_sold_out
              FROM products
              WHERE is_active = 1
                AND brand = ?
                AND id != ?
              ORDER BY RANDOM()
              LIMIT ?
            `
          )
          .all(product.brand, product.id, remainingSimilarCount);

    similarRows = [...similarRows, ...fallbackRows];
  }

  const badgeMap = getProductBadgeMapByProductIds([product.id, ...similarRows.map((row) => row.id)]);
  const productWithBadges = {
    ...product,
    product_badges: badgeMap.get(Number(product.id)) || []
  };
  const similar = similarRows.map((row) => ({
    ...decorateProductForView(row, productGroupMap.get(row.category_group)),
    product_badges: badgeMap.get(Number(row.id)) || []
  }));
  const productDisplay = buildProductDisplayData(productWithBadges, productGroupConfig);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);

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
    quantity: 1
  };
  const memberPointProfile = getMemberPointProfile(req.user.id);
  const purchasePointRate = memberPointProfile.pointRate;
  return res.render('purchase-form', {
    title: 'Purchase',
    product,
    formData,
    addressBookEntries,
    productGroupLabel,
    purchasePointRate,
    memberLevelName: memberPointProfile.levelName,
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
  const addressBookEntries = getAddressBookEntries(req.user.id);
  const selectedAddressBookEntry =
    selectedAddressBookId > 0
      ? addressBookEntries.find((entry) => entry.id === selectedAddressBookId) || null
      : null;

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
    quantity
  };
  const memberPointProfile = getMemberPointProfile(req.user.id);
  const purchasePointRate = memberPointProfile.pointRate;

  const renderWithError = (message) => {
    res.locals.ctx.flash = { type: 'error', message };
    return res.render('purchase-form', {
      title: 'Purchase',
      product,
      formData,
      addressBookEntries,
      productGroupLabel,
      purchasePointRate,
      memberLevelName: memberPointProfile.levelName,
      expectedPoints: calculateEarnedPoints(product.price * quantity, purchasePointRate)
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
    product.id,
    buyerName,
    normalizedContact,
    buyerAddress,
    customsClearanceNo,
    buyerName,
    quantity,
    totalPrice,
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

  return res.redirect(`/shop/order-complete/${orderNo}`);
});

app.post('/order/create', (req, res) => {
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
  const memberPointProfile = req.user ? getMemberPointProfile(req.user.id) : null;
  const pointRateSnapshot = memberPointProfile ? memberPointProfile.pointRate : 0;
  const pointLevelIdSnapshot = memberPointProfile?.levelId || '';
  const pointLevelNameSnapshot = memberPointProfile?.levelName || '';

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

  const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang, 'member');
  const isMemberOrder = Number(order.created_by_user_id || 0) > 0;
  const purchasePointRate = isMemberOrder
    ? parsePointRate(order.point_rate_snapshot, getLegacyPurchasePointRateSetting())
    : 0;
  const expectedPoints = isMemberOrder ? calculateEarnedPoints(order.total_price, purchasePointRate) : 0;
  const awardedPoints = parseNonNegativeInt(order.awarded_points, 0);

  res.render('order-complete', {
    title: 'Order Complete',
    order,
    statusMeta,
    isMemberOrder,
    purchasePointRate,
    memberLevelName: String(order.point_level_name || '').trim(),
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
    req.session.userId = null;
    req.session.isAdmin = false;
    req.session.adminRole = '';
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
  const memberPointProfile = getMemberPointProfile(req.user.id);
  const memberLevelInfo = {
    name: memberPointProfile.levelName || (res.locals.ctx.lang === 'en' ? 'Unassigned' : '미지정'),
    pointRate: parsePointRate(memberPointProfile.pointRate, 0)
  };

  const ordersQuery = [
    'SELECT o.*, p.category_group, p.brand, p.model, p.sub_model',
    'FROM orders o',
    'JOIN products p ON p.id = o.product_id',
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
    return {
      ...order,
      status_code: statusMeta.code,
      status_label: statusMeta.label,
      status_detail: statusMeta.detail,
      category_group_label: groupLabelMap[order.category_group] || order.category_group,
      tracking_carrier_label: getTrackingCarrierLabel(order.tracking_carrier),
      latest_event_note: latestLog.event_note,
      latest_event_at: latestLog.created_at
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

app.post('/mypage/profile/avatar', requireAuth, upload.single('profileImage'), (req, res) => {
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
      req.session.userId = null;
      req.session.isAdmin = false;
      req.session.adminRole = '';
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
  const products = db
    .prepare('SELECT id, brand, model, sub_model FROM products WHERE is_active = 1 ORDER BY id DESC')
    .all();
  res.render('review-form', { title: 'Write Review', products });
});

app.post('/review/new', requireAuth, upload.single('image'), (req, res) => {
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  const productId = req.body.productId ? Number(req.body.productId) : null;

  if (!title || !content) {
    setFlash(req, 'error', '제목과 내용을 입력해 주세요.');
    return res.redirect('/review/new');
  }

  db.prepare(
    `
      INSERT INTO reviews (user_id, product_id, title, content, image_path)
      VALUES (?, ?, ?, ?, ?)
    `
  ).run(req.user.id, productId || null, title, content, fileUrl(req.file));

  setFlash(req, 'success', '후기가 등록되었습니다.');
  res.redirect('/review');
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

app.post('/inquiry/new', requireAuth, upload.array('image', 20), (req, res) => {
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

app.post(
  '/signup',
  authAttemptGuard({ key: 'signup', redirectPath: '/signup', limit: 12 }),
  asyncRoute(async (req, res) => {
  const email = String(req.body.email || '').trim();
  const account = String(req.body.account || req.body.username || '').trim();
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

    req.session.userId = createdUserId;
    req.session.isAdmin = false;
    clearSignupCaptcha(req);
    resetAuthAttempt(req, 'signup');

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
    res.redirect('/main');
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 계정입니다.');
    res.redirect('/signup');
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
  authAttemptGuard({ key: 'account-find-send', redirectPath: '/account/find', limit: 8 }),
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

    if (!target) {
      setFlash(req, 'error', '가입된 계정을 찾을 수 없습니다.');
      return res.redirect('/account/find');
    }

    const issued = issueEmailVerificationCode({
      purpose: 'account-find',
      email: target.email,
      account: target.username,
      userId: target.id
    });

    if (!issued.ok && issued.reason === 'cooldown') {
      setFlash(req, 'error', `인증번호 재전송은 ${issued.waitSeconds}초 후에 가능합니다.`);
      return res.redirect(`/account/find?step=verify&email=${encodeURIComponent(email)}`);
    }
    if (!issued.ok) {
      setFlash(req, 'error', '인증번호 발급 중 오류가 발생했습니다.');
      return res.redirect('/account/find');
    }

    const sent = await sendEmailVerificationCode({
      to: target.email,
      code: issued.code,
      purpose: 'account-find',
      lang: res.locals.ctx.lang
    });

    if (!sent.ok) {
      if (!isProduction && sent.reason === 'smtp_not_configured') {
        setFlash(req, 'success', `[개발모드] 인증번호: ${issued.code}`);
      } else {
        setFlash(req, 'error', '인증번호 메일 발송에 실패했습니다. 관리자에게 문의해 주세요.');
        return res.redirect('/account/find');
      }
    } else {
      setFlash(req, 'success', '인증번호를 이메일로 전송했습니다.');
    }

    return res.redirect(`/account/find?step=verify&email=${encodeURIComponent(email)}`);
  })
);

app.post(
  '/account/find/verify-code',
  authAttemptGuard({ key: 'account-find-verify', redirectPath: '/account/find', limit: 12 }),
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
      setFlash(req, 'error', '가입된 계정을 찾을 수 없습니다.');
      return res.redirect('/account/find');
    }

    const verified = verifyEmailVerificationCode({
      purpose: 'account-find',
      email: target.email,
      account: target.username,
      code
    });

    if (!verified.ok) {
      const reasonMessage =
        verified.reason === 'expired'
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
    resetAuthAttempt(req, 'account-find-send');
    resetAuthAttempt(req, 'account-find-verify');
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
  authAttemptGuard({ key: 'password-reset-send', redirectPath: '/password/reset', limit: 10 }),
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

    if (!target) {
      setFlash(req, 'error', '입력한 계정/이메일 정보와 일치하는 회원이 없습니다.');
      return res.redirect('/password/reset');
    }

    const issued = issueEmailVerificationCode({
      purpose: 'password-reset',
      email: target.email,
      account: target.username,
      userId: target.id
    });
    if (!issued.ok && issued.reason === 'cooldown') {
      setFlash(req, 'error', `인증번호 재전송은 ${issued.waitSeconds}초 후에 가능합니다.`);
      return res.redirect(
        `/password/reset?step=verify&account=${encodeURIComponent(account)}&email=${encodeURIComponent(email)}`
      );
    }
    if (!issued.ok) {
      setFlash(req, 'error', '인증번호 발급 중 오류가 발생했습니다.');
      return res.redirect('/password/reset');
    }

    const sent = await sendEmailVerificationCode({
      to: target.email,
      code: issued.code,
      purpose: 'password-reset',
      lang: res.locals.ctx.lang
    });

    if (!sent.ok) {
      if (!isProduction && sent.reason === 'smtp_not_configured') {
        setFlash(req, 'success', `[개발모드] 인증번호: ${issued.code}`);
      } else {
        setFlash(req, 'error', '인증번호 메일 발송에 실패했습니다. 관리자에게 문의해 주세요.');
        return res.redirect('/password/reset');
      }
    } else {
      setFlash(req, 'success', '인증번호를 이메일로 전송했습니다.');
    }

    return res.redirect(
      `/password/reset?step=verify&account=${encodeURIComponent(account)}&email=${encodeURIComponent(email)}`
    );
  })
);

app.post(
  '/password/reset/verify-code',
  authAttemptGuard({ key: 'password-reset-verify', redirectPath: '/password/reset', limit: 14 }),
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
      setFlash(req, 'error', '입력한 계정/이메일 정보와 일치하는 회원이 없습니다.');
      return res.redirect('/password/reset');
    }

    const verified = verifyEmailVerificationCode({
      purpose: 'password-reset',
      email: target.email,
      account: target.username,
      code
    });

    if (!verified.ok) {
      const reasonMessage =
        verified.reason === 'expired'
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

    resetAuthAttempt(req, 'password-reset-send');
    resetAuthAttempt(req, 'password-reset-verify');
    setFlash(req, 'success', '이메일 인증이 완료되었습니다. 새 비밀번호를 입력해 주세요.');
    return res.redirect(`/password/reset?ticket=${encodeURIComponent(ticket)}`);
  })
);

app.post(
  '/password/reset/update',
  authAttemptGuard({ key: 'password-reset-update', redirectPath: '/password/reset', limit: 18 }),
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
      setFlash(req, 'error', '비밀번호를 변경할 계정을 찾을 수 없습니다.');
      return res.redirect('/password/reset');
    }

    const nextHash = await bcrypt.hash(newPassword, 10);
    db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(nextHash, user.id);
    consumePasswordResetTicket(ticket);

    if (req.session.accountFindResult) {
      delete req.session.accountFindResult;
    }

    resetAuthAttempt(req, 'password-reset-update');
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
  authAttemptGuard({ key: 'login', redirectPath: '/login', limit: 15 }),
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

  if (!user) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
  }

  if (Number(user.is_admin) === 1) {
    setFlash(req, 'error', '관리자 계정은 어드민 로그인 페이지를 이용해 주세요.');
    return res.redirect('/admin/login');
  }

  if (Number(user.is_blocked) === 1) {
    const blockedReason = String(user.blocked_reason || '').trim();
    setFlash(
      req,
      'error',
      blockedReason ? `차단된 계정입니다. (${blockedReason})` : '차단된 계정입니다. 관리자에게 문의해 주세요.'
    );
    return res.redirect('/login');
  }

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
  }

  req.session.userId = Number(user.id);
  req.session.isAdmin = Number(user.is_admin) === 1;
  req.session.adminRole = Number(user.is_admin) === 1 ? normalizeAdminRole(user.admin_role) : '';
  resetAuthAttempt(req, 'login');

  setFlash(req, 'success', '로그인되었습니다.');
  res.redirect('/main');
  })
);

app.post('/logout', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/main');
  });
});

app.get('/admin/login', (req, res) => {
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
  authAttemptGuard({ key: 'admin-login', redirectPath: '/admin/login', limit: 10 }),
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

  if (!user || Number(user.is_admin) !== 1) {
    setFlash(req, 'error', '어드민 계정이 아닙니다.');
    return res.redirect('/admin/login');
  }

  if (Number(user.is_blocked) === 1) {
    const blockedReason = String(user.blocked_reason || '').trim();
    setFlash(
      req,
      'error',
      blockedReason ? `차단된 계정입니다. (${blockedReason})` : '차단된 계정입니다. 메인관리자에게 문의해 주세요.'
    );
    return res.redirect('/admin/login');
  }

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/admin/login');
  }

  const hasOtpEnabled =
    Number(user.admin_otp_enabled || 0) === 1 &&
    normalizeBase32Secret(user.admin_otp_secret || '').length >= 16;
  if (hasOtpEnabled) {
    setAdminOtpPending(req, user);
    logAdminActivityByUser(user, req, 'LOGIN_OTP_PENDING', 'password verified; otp required');
    setFlash(req, 'success', '구글 OTP 인증번호를 입력해 주세요.');
    return res.redirect('/admin/otp/verify');
  }

  setAdminAuthSession(req, user);
  resetAuthAttempt(req, 'admin-login');

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
  authAttemptGuard({ key: 'admin-otp-verify', redirectPath: '/admin/otp/verify', limit: 12 }),
  (req, res) => {
    const pending = readAdminOtpPending(req);
    if (!pending) {
      setFlash(req, 'error', 'OTP 인증이 만료되었습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    const code = normalizeAdminOtpCode(req.body.code || '');
    if (code.length !== ADMIN_OTP_DIGITS) {
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
      clearAdminOtpPending(req);
      setFlash(req, 'error', '관리자 계정을 찾을 수 없습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    if (Number(user.is_blocked || 0) === 1) {
      clearAdminOtpPending(req);
      const blockedReason = String(user.blocked_reason || '').trim();
      setFlash(
        req,
        'error',
        blockedReason ? `차단된 계정입니다. (${blockedReason})` : '차단된 계정입니다. 메인관리자에게 문의해 주세요.'
      );
      return res.redirect('/admin/login');
    }

    const secret = normalizeBase32Secret(user.admin_otp_secret || '');
    const isOtpEnabled = Number(user.admin_otp_enabled || 0) === 1 && secret.length >= 16;
    if (!isOtpEnabled) {
      clearAdminOtpPending(req);
      setFlash(req, 'error', 'OTP 설정을 찾을 수 없습니다. 다시 로그인해 주세요.');
      return res.redirect('/admin/login');
    }

    const verified = verifyTotpCode(secret, code);
    if (!verified) {
      setFlash(req, 'error', 'OTP 인증번호가 올바르지 않습니다.');
      return res.redirect('/admin/otp/verify');
    }

    setAdminAuthSession(req, user);
    resetAuthAttempt(req, 'admin-login');
    resetAuthAttempt(req, 'admin-otp-verify');
    logAdminActivityByUser(user, req, 'LOGIN_SUCCESS', 'admin login success via otp');
    setFlash(req, 'success', '관리자 로그인되었습니다.');
    return res.redirect('/admin/dashboard');
  }
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
  req.session.destroy(() => {
    res.redirect('/admin/login');
  });
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
          SUM(CASE WHEN status != 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS total_count,
          SUM(CASE WHEN status != 'PENDING_REVIEW' THEN total_price ELSE 0 END) AS total_amount,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) = date(?)
              THEN 1
              ELSE 0
            END
          ) AS today_count,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) = date(?)
              THEN total_price
              ELSE 0
            END
          ) AS today_amount,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-6 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS week_count,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-6 day') AND date(?)
              THEN total_price
              ELSE 0
            END
          ) AS week_amount,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
                AND date(datetime(COALESCE(checked_at, created_at), '+9 hours')) BETWEEN date(?, '-29 day') AND date(?)
              THEN 1
              ELSE 0
            END
          ) AS month_count,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
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
          SUM(CASE WHEN status != 'PENDING_REVIEW' THEN 1 ELSE 0 END) AS payment_confirmed_total,
          SUM(
            CASE
              WHEN status != 'PENDING_REVIEW'
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
      const levelName = levelRule?.name || (lang === 'en' ? 'Unassigned' : '미지정');
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
      name: rule.name,
      operator: rule.operator,
      operatorLabel: getMemberLevelOperatorLabel(rule.operator, lang),
      thresholdAmount: Number(rule.thresholdAmount || 0),
      pointRate: parsePointRate(pointRateMap[rule.id], 0),
      memberCount: Number(countMap.get(rule.id) || 0)
    }));
    summaries.push({
      id: '__unassigned__',
      name: lang === 'en' ? 'Unassigned' : '미지정',
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
    "UPPER(TRIM(o.status)) NOT IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED')"
  ];
  const params = [];

  if (dateFrom) {
    whereParts.push("date(datetime(o.created_at, '+9 hours')) >= date(?)");
    params.push(dateFrom);
  }
  if (dateTo) {
    whereParts.push("date(datetime(o.created_at, '+9 hours')) <= date(?)");
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
          date(datetime(o.created_at, '+9 hours')) AS sale_date,
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
    purchasePointRate: getLegacyPurchasePointRateSetting(),
    contactInfo: getSetting('contactInfo', ''),
    businessInfo: getSetting('businessInfo', ''),
    salesSheetUrl: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
    languageDefault: getSetting('languageDefault', 'ko'),
    menusJson: JSON.stringify(publicMenus, null, 2)
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
    dashboardStats: includeDashboardStats ? getCachedAdminDashboardStats() : null,
    trackingCarriers: TRACKING_CARRIERS,
    formatPrice,
    productGroups: productGroupConfigs.map((group) => group.key),
    productGroupConfigs,
    groupLabelMap,
    salesMainTabs: SALES_MAIN_TABS,
    salesSheetDefaultUrl: SALES_SHEET_DEFAULT_URL
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
    salesSection: 'editor',
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

app.get('/admin/otp', requireAdmin, (req, res) => {
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
    req.session.userId = null;
    req.session.isAdmin = false;
    req.session.adminRole = '';
    clearAdminOtpSetup(req);
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

  return res.render('admin-otp', {
    title: 'Admin OTP',
    otpState: {
      enabled: otpEnabled,
      enabledAt: adminRow.admin_otp_enabled_at || '',
      setupSecret: activeSetup?.secret || '',
      setupUri: otpAuthUri
    }
  });
});

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
  const levelName = normalizeMemberLevelName(req.body.levelName || '');
  const thresholdAmount = parseMemberLevelThresholdAmount(req.body.thresholdAmount || '0', 0);
  const operator = normalizeMemberLevelOperator(req.body.operator || MEMBER_LEVEL_OPERATORS.GTE);

  if (!levelName) {
    setFlash(req, 'error', '등급 명칭을 입력해 주세요.');
    return res.redirect(backPath);
  }

  const currentRules = getMemberLevelRulesSetting();
  const nextLevelId = buildUniqueMemberLevelId(levelName, currentRules.map((rule) => rule.id));
  const nextRules = [
    ...currentRules,
    {
      id: nextLevelId,
      name: levelName,
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
  const levelName = normalizeMemberLevelName(req.body.levelName || '');
  const thresholdAmount = parseMemberLevelThresholdAmount(req.body.thresholdAmount || '0', 0);
  const operator = normalizeMemberLevelOperator(req.body.operator || MEMBER_LEVEL_OPERATORS.GTE);

  if (!levelId) {
    setFlash(req, 'error', '유효하지 않은 등급입니다.');
    return res.redirect(backPath);
  }

  if (!levelName) {
    setFlash(req, 'error', '등급 명칭을 입력해 주세요.');
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
    name: levelName,
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
    const shouldImportFromSheet = req.query.importFromSheet === '1';
    let workbook = null;
    const rawStoredWorkbook = String(getSetting(SALES_WORKBOOK_SETTING_KEY, '') || '').trim();

    if (shouldImportFromSheet) {
      workbook = await importSalesWorkbookFromGoogleSheet();
      logAdminActivity(req, 'SALES_IMPORT', 'sales workbook imported from google sheet');
    } else if (!rawStoredWorkbook) {
      try {
        workbook = await importSalesWorkbookFromGoogleSheet();
      } catch {
        workbook = getSalesWorkbook();
      }
    } else {
      workbook = getSalesWorkbook();
    }

    const payload = buildSalesWorkbookPayload(workbook);
    return res.json({
      ok: true,
      sourceUrl: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
      mainTabs: SALES_MAIN_TABS,
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
      sourceUrl: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
      mainTabs: SALES_MAIN_TABS,
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
      sourceUrl: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
      mainTabs: SALES_MAIN_TABS,
      ...buildSalesWorkbookPayload(savedWorkbook)
    });
  })
);

app.post(
  '/admin/sales/import-sheet',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const workbook = await importSalesWorkbookFromGoogleSheet();
    logAdminActivity(req, 'SALES_IMPORT', 'sales workbook imported from google sheet');
    return res.json({
      ok: true,
      sourceUrl: getSetting('salesSheetUrl', SALES_SHEET_DEFAULT_URL),
      mainTabs: SALES_MAIN_TABS,
      ...buildSalesWorkbookPayload(workbook)
    });
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
  const filterSeeds = getDefaultGroupFilterSeeds();
  const nextConfigs = [...configs, {
    key: requestedKey,
    labelKo,
    labelEn,
    mode: useFactoryTemplate ? PRODUCT_GROUP_MODE.FACTORY : PRODUCT_GROUP_MODE.SIMPLE,
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

  setFlash(req, 'success', '쇼핑몰 분류가 추가되었습니다.');
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
  setFlash(req, 'success', '쇼핑몰 분류가 삭제되었습니다.');
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
    { name: 'backgroundImage', maxCount: 1 }
  ]),
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
      const businessInfo = String(req.body.businessInfo || '').trim();
      const languageDefault = resolveLanguage(req.body.languageDefault || getSetting('languageDefault', 'ko'), 'ko');

      setSetting('siteName', siteName || 'Chrono Lab');
      setSetting('bankAccountInfo', bankAccountInfo);
      setSetting('contactInfo', contactInfo);
      setSetting('businessInfo', businessInfo);
      setSetting('languageDefault', languageDefault);

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

app.post('/admin/product/create', requireAdmin, upload.array('images', 20), asyncRoute(async (req, res) => {
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

app.post('/admin/product/:id/update', requireAdmin, upload.array('images', 20), asyncRoute(async (req, res) => {
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

  let imagePath = '';
  if (uploadedImages.length > 0) {
    imagePath = uploadedImages[0];
  } else {
    const existingImage = db
      .prepare(
        `
          SELECT image_path
          FROM product_images
          WHERE product_id = ?
          ORDER BY sort_order ASC, id ASC
          LIMIT 1
        `
      )
      .get(id);
    imagePath = String(existingImage?.image_path || '').trim();
    if (!imagePath) {
      const baseImage = db.prepare('SELECT image_path FROM products WHERE id = ? LIMIT 1').get(id);
      imagePath = String(baseImage?.image_path || '').trim();
    }
  }

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

  if (uploadedImages.length > 0) {
    db.prepare('DELETE FROM product_images WHERE product_id = ?').run(id);
    const insertImage = db.prepare(
      `
        INSERT INTO product_images (product_id, image_path, sort_order)
        VALUES (?, ?, ?)
      `
    );
    uploadedImages.forEach((src, index) => {
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

app.post('/admin/notice/create', requireAdmin, upload.array('image', 20), (req, res) => {
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

app.post('/admin/notice/:id/update', requireAdmin, upload.array('image', 20), (req, res) => {
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

app.post('/admin/news/create', requireAdmin, upload.array('image', 20), asyncRoute(async (req, res) => {
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

app.post('/admin/news/:id/update', requireAdmin, upload.array('image', 20), asyncRoute(async (req, res) => {
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

app.post('/admin/qc/create', requireAdmin, upload.array('image', 20), asyncRoute(async (req, res) => {
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

app.post('/admin/qc/:id/update', requireAdmin, upload.array('image', 20), asyncRoute(async (req, res) => {
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

app.post('/admin/order/:id/sync-tracking', requireAdmin, (req, res) => {
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
  void pollTrackingAndAutoCompleteOrders(true);
  setFlash(req, 'success', '송장 상태 조회를 요청했습니다.');
  return res.redirect(backPath);
});

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
        tracking_last_checked_at = datetime('now'),
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

app.post('/admin/inquiry/:id/update', requireAdmin, upload.array('image', 20), (req, res) => {
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
  // eslint-disable-next-line no-console
  console.error('[chronolab:error]', error);

  const isUnsupportedType = Boolean(error?.message?.includes('지원되지 않는 파일 형식'));
  const isFileTooLarge = error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE';
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
      : '일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.';

  if (wantsJsonResponse) {
    if (isFileTooLarge) {
      return res.status(413).json({ ok: false, error: 'file_too_large', message, maxMb: MAX_UPLOAD_FILE_SIZE_MB });
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

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Chrono Lab server running on http://localhost:${PORT}`);
});

setInterval(() => {
  void pollTrackingAndAutoCompleteOrders(false);
}, TRACKING_AUTO_POLL_MS);
