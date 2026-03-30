import path from 'path';
import { promises as fs } from 'fs';
import { fileURLToPath } from 'url';
import express from 'express';
import session from 'express-session';
import cookieParser from 'cookie-parser';
import multer from 'multer';
import bcrypt from 'bcryptjs';
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
  SHOP_PRODUCT_GROUPS
} from './src/db.js';
import { resolveLanguage, t } from './src/i18n.js';

initDb();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT || 3100);
const isProduction = process.env.NODE_ENV === 'production';
const ASSET_VERSION = process.env.RENDER_GIT_COMMIT || `${Date.now()}`;

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const USERNAME_REGEX = /^[A-Za-z0-9_]{4,20}$/;
const PASSWORD_REGEX = /^(?=.*[A-Za-z])(?=.*[0-9]).{8,}$/;
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
  { id: 'admin-members', labelKo: '회원관리', labelEn: 'Members', path: '/admin/members' },
  { id: 'admin-site', labelKo: '사이트설정', labelEn: 'Site', path: '/admin/site' },
  { id: 'admin-menus', labelKo: '메뉴관리', labelEn: 'Menus', path: '/admin/menus' },
  { id: 'admin-products', labelKo: '상품관리', labelEn: 'Products', path: '/admin/products' },
  { id: 'admin-notices', labelKo: '공지사항', labelEn: 'Notices', path: '/admin/notices' },
  { id: 'admin-news', labelKo: '뉴스', labelEn: 'News', path: '/admin/news' },
  { id: 'admin-qc', labelKo: 'QC', labelEn: 'QC', path: '/admin/qc' },
  { id: 'admin-orders', labelKo: '주문관리', labelEn: 'Orders', path: '/admin/orders' },
  { id: 'admin-inquiries', labelKo: '문의답변', labelEn: 'Inquiries', path: '/admin/inquiries' }
]);

const SECURITY_SECTIONS = Object.freeze(['profile', 'admins', 'logs', 'alerts']);
const SECURITY_PAGE_SIZE = 20;
const MEMBER_PAGE_SIZE = 20;
const DATE_INPUT_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const HEX_COLOR_REGEX = /^#[0-9a-fA-F]{6}$/;
const PRODUCT_GROUP_MODE = Object.freeze({
  FACTORY: 'factory',
  SIMPLE: 'simple'
});
const PRODUCT_FIELD_TYPES = new Set(['text', 'textarea', 'number']);

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
  { key: 'detailed_description', labelKo: '상세설명', labelEn: 'Detailed Description', type: 'textarea', required: true }
]);

const DEFAULT_THEME_COLORS = Object.freeze({
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

const AUTH_ATTEMPT_WINDOW_MS = 10 * 60 * 1000;
const DEFAULT_AUTH_MAX_ATTEMPTS = 15;
const authAttemptStore = new Map();
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

const uploadStorage = multer.diskStorage({
  destination: path.join(__dirname, 'uploads'),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || '').toLowerCase();
    const safeExt = ext && ext.length <= 10 ? ext : '.jpg';
    cb(null, `${Date.now()}-${Math.round(Math.random() * 1e9)}${safeExt}`);
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({ ok: true, service: 'chrono-lab', timestamp: new Date().toISOString() });
});

const upload = multer({
  storage: uploadStorage,
  limits: { fileSize: 10 * 1024 * 1024 },
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
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'chrono-lab-local-secret',
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

function cloneFieldTemplate(template = []) {
  return template.map((field) => ({ ...field }));
}

function getFactoryDefaultFields() {
  return cloneFieldTemplate(FACTORY_DEFAULT_FIELDS);
}

function getCompactDefaultFields() {
  return cloneFieldTemplate(COMPACT_DEFAULT_FIELDS);
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
    brand: body.brand,
    model: body.model,
    sub_model: body.subModel || body.sub_model,
    reference: body.reference,
    factory_name: body.factoryName || body.factory_name,
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
  const values = {};

  for (const field of fieldDefinitions) {
    const key = normalizeProductFieldAliasKey(field.key || '');
    if (!key) {
      // eslint-disable-next-line no-continue
      continue;
    }

    const rawValue = normalizedRawMap[key] ?? legacyBodyValues[key] ?? '';
    const value = normalizeFieldInputValue(rawValue, field.type);
    if (field.required && !value) {
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

    const mode = isFactoryLikeFields(customFields) ? PRODUCT_GROUP_MODE.FACTORY : PRODUCT_GROUP_MODE.SIMPLE;

    normalizedGroups.push({
      key,
      labelKo: labelKo || key,
      labelEn: labelEn || key,
      mode,
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
  const summary = String(
    fieldValues.detailed_description || fieldValues.features || safeProduct.sub_model || ''
  ).trim();
  const skipKeys = new Set(['title', 'detailed_description']);
  if (isFactoryLike) {
    [
      'brand',
      'model',
      'sub_model',
      'reference',
      'factory_name',
      'version_name',
      'movement',
      'case_size',
      'dial_color',
      'case_material',
      'strap_material',
      'features',
      'price',
      'shipping_period'
    ].forEach((key) => skipKeys.add(key));
  }
  const customPairs = customFields
    .map((field) => {
      const key = normalizeProductFieldAliasKey(field.key || '');
      const value = String(fieldValues[key] || '').trim();
      if (!value || skipKeys.has(key)) {
        return null;
      }
      return {
        key,
        labelKo: field.labelKo || field.key,
        labelEn: field.labelEn || field.labelKo || field.key,
        value
      };
    })
    .filter(Boolean);

  return {
    title: title || `${safeProduct.brand || ''} ${safeProduct.model || ''}`.trim() || '-',
    subtitle: summary || String(safeProduct.sub_model || '').trim(),
    meta: isFactoryLike
      ? [safeProduct.case_material, safeProduct.movement].filter(Boolean).join(' / ')
      : customPairs.map((pair) => pair.value).join(' / '),
    summary: summary || String(safeProduct.features || '').trim(),
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

function buildChronoLabWatermarkSvg(width, height) {
  const safeWidth = Math.max(120, Math.floor(Number(width || 0)));
  const safeHeight = Math.max(120, Math.floor(Number(height || 0)));
  const fontSize = Math.max(12, Math.min(26, Math.round(safeWidth / 34)));
  const repeatCount = Math.max(4, Math.ceil(safeWidth / (fontSize * 5.2)));
  const lineText = escapeSvgText(Array.from({ length: repeatCount }).fill('Chrono Lab').join('   '));
  const yRows = [0.25, 0.5, 0.75];

  const textLines = yRows
    .map(
      (ratio) => `
        <text x="50%" y="${Math.round(safeHeight * ratio)}" text-anchor="middle" dominant-baseline="middle">
          ${lineText}
        </text>
      `
    )
    .join('');

  return `
    <svg width="${safeWidth}" height="${safeHeight}" viewBox="0 0 ${safeWidth} ${safeHeight}" xmlns="http://www.w3.org/2000/svg">
      <style>
        text {
          font-family: 'Noto Sans KR', 'Noto Sans', sans-serif;
          font-size: ${fontSize}px;
          font-weight: 700;
          letter-spacing: 0.04em;
          fill: rgba(255, 255, 255, 0.16);
          stroke: rgba(0, 0, 0, 0.16);
          stroke-width: 0.6;
        }
      </style>
      ${textLines}
    </svg>
  `;
}

async function applyChronoLabWatermarkToFile(filePath) {
  if (!filePath) {
    return;
  }

  try {
    const image = sharp(filePath);
    const metadata = await image.metadata();
    const width = Number(metadata.width || 0);
    const height = Number(metadata.height || 0);
    const format = String(metadata.format || '').toLowerCase();

    if (width <= 0 || height <= 0) {
      return;
    }
    if (!['jpeg', 'jpg', 'png', 'webp', 'avif'].includes(format)) {
      return;
    }

    const watermarkSvg = buildChronoLabWatermarkSvg(width, height);
    let pipeline = image.composite([{ input: Buffer.from(watermarkSvg), top: 0, left: 0 }]);

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
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('[chrono-lab:watermark]', error);
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
    return path.join(__dirname, 'uploads', src.slice('/uploads/'.length));
  }
  if (src.startsWith('/assets/')) {
    return path.join(__dirname, 'public', src.slice('/assets/'.length));
  }
  return '';
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
  let skippedCount = 0;

  for (const imageUrl of uniqueUrls) {
    const localPath = resolveLocalPathFromImageUrl(imageUrl);
    if (!localPath) {
      skippedCount += 1;
      // eslint-disable-next-line no-continue
      continue;
    }

    try {
      await fs.access(localPath);
    } catch {
      skippedCount += 1;
      // eslint-disable-next-line no-continue
      continue;
    }

    await applyChronoLabWatermarkToFile(localPath);
    processedCount += 1;
  }

  return {
    totalCount: uniqueUrls.length,
    processedCount,
    skippedCount
  };
}

function getAdminMenus(currentUser = null) {
  const isPrimaryAdmin = Boolean(currentUser?.isPrimaryAdmin);
  return ADMIN_MENUS
    .filter((menu) => (menu.id === 'admin-security' ? isPrimaryAdmin : true))
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

function fileUrl(file) {
  if (!file) {
    return '';
  }
  return `/uploads/${file.filename}`;
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

function getOrderStatusMeta(rawStatus, lang = 'ko') {
  const status = normalizeOrderStatus(rawStatus);
  const isEn = lang === 'en';

  if (status === ORDER_STATUS.PENDING_REVIEW) {
    return {
      code: status,
      label: isEn ? 'Awaiting Payment Verification' : '입금확인중',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.ORDER_CONFIRMED) {
    return {
      code: status,
      label: isEn ? 'Payment Confirmed' : '입금확인',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.READY_TO_SHIP) {
    return {
      code: status,
      label: isEn ? 'Preparing Shipment' : '출고중',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.SHIPPING) {
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
  if (current === ORDER_STATUS.ORDER_CONFIRMED) return isEn ? 'Mark Preparing Shipment' : '출고중 처리';
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
  return 'active';
}

function parseMemberManageQuery(query = {}) {
  return {
    section: normalizeMemberManageSection(query.memberSection || query.section || ''),
    keyword: String(query.memberKeyword || query.keyword || '').trim().slice(0, 120),
    page: normalizePositivePage(query.memberPage || query.page || 1)
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
  if (section === 'groups' || section === 'fields') {
    return section;
  }
  return 'public';
}

function normalizeProductManageSection(rawSection = '') {
  const section = String(rawSection || '').trim().toLowerCase();
  if (section === 'list') {
    return 'list';
  }
  return 'upload';
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

function getThemeColorConfig(themeMode = 'day') {
  const key = themeMode === 'night' ? 'night' : 'day';
  const defaults = DEFAULT_THEME_COLORS[key];
  const legacyHeader = normalizeHexColor(getSetting('headerColor', defaults.headerColor), defaults.headerColor);
  const legacyBg = normalizeHexColor(getSetting('backgroundValue', defaults.backgroundColor), defaults.backgroundColor);

  return {
    headerColor: normalizeHexColor(getSetting(`${key}HeaderColor`, legacyHeader), defaults.headerColor),
    backgroundColor: normalizeHexColor(getSetting(`${key}BackgroundColor`, legacyBg), defaults.backgroundColor),
    textColor: normalizeHexColor(getSetting(`${key}TextColor`, defaults.textColor), defaults.textColor),
    mutedColor: normalizeHexColor(getSetting(`${key}MutedColor`, defaults.mutedColor), defaults.mutedColor),
    lineColor: normalizeHexColor(getSetting(`${key}LineColor`, defaults.lineColor), defaults.lineColor),
    cardColor: normalizeHexColor(getSetting(`${key}CardColor`, defaults.cardColor), defaults.cardColor),
    cardDarkColor: normalizeHexColor(getSetting(`${key}CardDarkColor`, defaults.cardDarkColor), defaults.cardDarkColor),
    cardDarkTextColor: normalizeHexColor(
      getSetting(`${key}CardDarkTextColor`, defaults.cardDarkTextColor),
      defaults.cardDarkTextColor
    ),
    chipColor: normalizeHexColor(getSetting(`${key}ChipColor`, defaults.chipColor), defaults.chipColor)
  };
}

function getThemeAssetConfig(themeMode = 'day') {
  const key = themeMode === 'night' ? 'night' : 'day';
  const legacyHeaderLogoPath = String(getSetting('headerLogoPath', '') || '').trim();
  const legacyHeaderSymbolPath = String(getSetting('headerSymbolPath', '') || '').trim();
  const legacyFooterLogoPath = String(getSetting('footerLogoPath', '') || '').trim();
  const legacyBackgroundType = String(getSetting('backgroundType', 'color') || 'color').trim();
  const legacyBackgroundValue = String(getSetting('backgroundValue', '') || '').trim();
  const dayHeaderLogoPath = String(
    getSetting('dayHeaderLogoPath', legacyHeaderLogoPath) || ''
  ).trim();
  const dayHeaderSymbolPath = String(
    getSetting('dayHeaderSymbolPath', legacyHeaderSymbolPath) || ''
  ).trim();
  const dayFooterLogoPath = String(
    getSetting('dayFooterLogoPath', legacyFooterLogoPath) || ''
  ).trim();
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
  const backgroundType = normalizeType(
    getSetting(`${key}BackgroundType`, key === 'day' ? dayBackgroundType : dayBackgroundType)
  );
  const backgroundImagePath = String(
    getSetting(
      `${key}BackgroundImagePath`,
      key === 'day' ? dayBackgroundImagePath : dayBackgroundImagePath
    ) || ''
  ).trim();

  return {
    headerLogoPath: String(
      getSetting(`${key}HeaderLogoPath`, key === 'day' ? dayHeaderLogoPath : dayHeaderLogoPath) || ''
    ).trim(),
    headerSymbolPath: String(
      getSetting(`${key}HeaderSymbolPath`, key === 'day' ? dayHeaderSymbolPath : dayHeaderSymbolPath) || ''
    ).trim(),
    footerLogoPath: String(
      getSetting(`${key}FooterLogoPath`, key === 'day' ? dayFooterLogoPath : dayFooterLogoPath) || ''
    ).trim(),
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

function getClientIp(req) {
  return String(req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim() || 'unknown';
}

function logAdminActivity(req, actionType, detail = '') {
  if (!req.user?.isAdmin) {
    return;
  }

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
    getClientIp(req),
    String(req.get('user-agent') || '').slice(0, 300),
    req.method,
    `${req.path}${req.url.includes('?') ? req.url.slice(req.path.length) : ''}`.slice(0, 300),
    String(actionType || '').slice(0, 80),
    String(detail || '').slice(0, 300)
  );
}

function logAdminActivityByUser(userRow, req, actionType, detail = '') {
  if (!userRow) {
    return;
  }

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
    getClientIp(req),
    String(req.get('user-agent') || '').slice(0, 300),
    req.method,
    req.path.slice(0, 300),
    String(actionType || '').slice(0, 80),
    String(detail || '').slice(0, 300)
  );
}

function recordSecurityAlert(req, reason, detail = '') {
  const isAdmin = Boolean(req.user?.isAdmin);
  const actorRole = isAdmin ? req.user.adminRole || ADMIN_ROLE.SUB : '';
  const actorName = isAdmin ? req.user.username : 'unknown';
  const actorId = isAdmin ? req.user.id : null;

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
    getClientIp(req),
    String(req.method || '').slice(0, 16),
    `${req.path}${req.url.includes('?') ? req.url.slice(req.path.length) : ''}`.slice(0, 300),
    String(reason || '').slice(0, 120),
    String(detail || '').slice(0, 300)
  );
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

function buildAdminProductSubmission(rawBody, groupConfig) {
  const fields =
    Array.isArray(groupConfig?.customFields) && groupConfig.customFields.length > 0
      ? groupConfig.customFields
      : getGroupDefaultFields(groupConfig || {});
  const safeGroupConfig = {
    ...(groupConfig || {}),
    customFields: fields
  };

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
        SELECT id, email, username, full_name, phone, is_admin, admin_role, is_blocked, blocked_reason, blocked_at, created_at
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
    fullName: user.full_name || '',
    phone: user.phone || '',
    isAdmin: Number(user.is_admin) === 1,
    adminRole: Number(user.is_admin) === 1 ? normalizeAdminRole(user.admin_role) : '',
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
    .prepare('SELECT id, title, content, image_path FROM notices WHERE is_popup = 1 ORDER BY id DESC LIMIT 1')
    .get();
  const footerNotices = db
    .prepare('SELECT id, title FROM notices ORDER BY id DESC LIMIT 5')
    .all();

  const visitCounts = getVisitCounts(today);
  const postCounts = getPostCounts(today);

  res.locals.ctx = {
    assetVersion: ASSET_VERSION,
    lang,
    t: (key) => t(lang, key),
    themeMode,
    currentUser: req.user,
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
      bankAccountInfo: getSetting('bankAccountInfo', ''),
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
            extra_fields_json
          FROM products
          WHERE is_active = 1 AND category_group = ?
          ORDER BY id DESC
          LIMIT 4
        `
      )
      .all(groupConfig.key);

    return {
      groupName: groupConfig.key,
      products: rows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)))
    };
  });

  const latestNotices = db
    .prepare(
      `
        SELECT id, title, created_at
        FROM notices
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
    customFields: []
  };
  const canUseBrandModelFilter = isFactoryLikeGroup(selectedGroupConfig);
  const brand = canUseBrandModelFilter ? String(req.query.brand || '').trim() : '';
  const model = canUseBrandModelFilter ? String(req.query.model || '').trim() : '';

  const brands = canUseBrandModelFilter
    ? db
        .prepare(
          `
            SELECT DISTINCT brand
            FROM products
            WHERE is_active = 1 AND category_group = ?
            ORDER BY brand ASC
          `
        )
        .all(group)
        .map((row) => row.brand)
    : [];

  const models = brand
    ? db
        .prepare(
          `
            SELECT DISTINCT model
            FROM products
            WHERE is_active = 1 AND category_group = ? AND brand = ?
            ORDER BY model ASC
          `
        )
        .all(group, brand)
        .map((row) => row.model)
    : [];

  const where = ['is_active = 1', 'category_group = ?'];
  const params = [group];

  if (brand) {
    where.push('brand = ?');
    params.push(brand);
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
          extra_fields_json
        FROM products
        WHERE ${where.join(' AND ')}
        ORDER BY id DESC
      `
    )
    .all(...params);
  const products = productRows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)));

  res.render('shop', {
    title: 'Shop',
    group,
    productGroups: fallbackGroups,
    productGroupConfigs,
    selectedGroupConfig,
    supportsBrandModelFilter: canUseBrandModelFilter,
    groupLabelMap: getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang),
    brand,
    model,
    brands,
    models,
    products
  });
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

  let similarRows = [];
  if (isFactoryLikeGroup(productGroupConfig)) {
    similarRows = db
      .prepare(
        `
          SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json
          FROM products
          WHERE is_active = 1 AND brand = ? AND id != ?
          ORDER BY id DESC
          LIMIT 6
        `
      )
      .all(product.brand, product.id);
  } else {
    similarRows = db
      .prepare(
        `
          SELECT id, category_group, brand, model, sub_model, price, image_path, extra_fields_json
          FROM products
          WHERE is_active = 1 AND category_group = ? AND id != ?
          ORDER BY id DESC
          LIMIT 6
        `
      )
      .all(product.category_group, product.id);
  }

  const similar = similarRows.map((row) => decorateProductForView(row, productGroupMap.get(row.category_group)));
  const productDisplay = buildProductDisplayData(product, productGroupConfig);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);

  res.render('product-detail', {
    title: 'Product',
    product,
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
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).render('simple-error', { title: 'Error', message: '잘못된 상품입니다.' });
  }

  const product = db
    .prepare('SELECT id, category_group, brand, model, sub_model, price, shipping_period FROM products WHERE id = ? AND is_active = 1 LIMIT 1')
    .get(id);

  if (!product) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '상품을 찾을 수 없습니다.' });
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const productGroupLabel = productGroupLabelMap[product.category_group] || product.category_group;

  incrementFunnelEventUniqueInSession(req, FUNNEL_EVENT.PURCHASE_VIEW, `product:${id}`);

  const formData = {
    buyerName: req.user.username || '',
    buyerContact: '',
    customsClearanceNo: '',
    buyerAddress: '',
    quantity: 1
  };

  return res.render('purchase-form', { title: 'Purchase', product, formData, productGroupLabel });
});

app.post('/shop/item/:id/purchase', requireAuth, (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).render('simple-error', { title: 'Error', message: '잘못된 상품입니다.' });
  }

  const product = db
    .prepare('SELECT id, category_group, brand, model, sub_model, price, shipping_period FROM products WHERE id = ? AND is_active = 1 LIMIT 1')
    .get(id);

  if (!product) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '상품을 찾을 수 없습니다.' });
  }

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupLabelMap = getProductGroupLabels(productGroupConfigs, res.locals.ctx.lang);
  const productGroupLabel = productGroupLabelMap[product.category_group] || product.category_group;

  const buyerName = String(req.body.buyerName || '').trim();
  const buyerContact = String(req.body.buyerContact || '').trim();
  const buyerAddress = String(req.body.buyerAddress || '').trim();
  const customsClearanceNo = String(req.body.customsClearanceNo || '').trim();
  const quantity = parsePositiveInt(req.body.quantity, 1);

  const formData = {
    buyerName,
    buyerContact,
    customsClearanceNo,
    buyerAddress,
    quantity
  };

  const renderWithError = (message) => {
    res.locals.ctx.flash = { type: 'error', message };
    return res.render('purchase-form', { title: 'Purchase', product, formData, productGroupLabel });
  };

  if (!buyerName || !buyerContact || !buyerAddress || !customsClearanceNo) {
    return renderWithError('필수 입력값을 모두 작성해 주세요.');
  }

  const normalizedContact = normalizePhone(buyerContact);
  if (!DIGIT_PHONE_REGEX.test(normalizedContact) || normalizedContact.length < 8) {
    return renderWithError('연락처 형식이 올바르지 않습니다.');
  }

  if (buyerAddress.length < 5 || buyerAddress.length > 200) {
    return renderWithError('주소는 5~200자 범위로 입력해 주세요.');
  }

  if (!CUSTOMS_NO_REGEX.test(customsClearanceNo)) {
    return renderWithError('통관번호 형식이 올바르지 않습니다. (영문/숫자 6~30자)');
  }

  let orderNo = generateOrderNo();
  while (db.prepare('SELECT id FROM orders WHERE order_no = ? LIMIT 1').get(orderNo)) {
    orderNo = generateOrderNo();
  }

  const totalPrice = Number(product.price) * quantity;
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
        created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    req.user.id
  );

  appendOrderStatusLog(
    Number(createdOrder.lastInsertRowid),
    orderNo,
    null,
    ORDER_STATUS.PENDING_REVIEW,
    'order:member:created'
  );

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

  const product = db.prepare('SELECT id, price FROM products WHERE id = ? AND is_active = 1 LIMIT 1').get(productId);
  if (!product) {
    setFlash(req, 'error', '유효하지 않은 상품입니다.');
    return res.redirect('/shop');
  }

  let orderNo = generateOrderNo();
  while (db.prepare('SELECT id FROM orders WHERE order_no = ? LIMIT 1').get(orderNo)) {
    orderNo = generateOrderNo();
  }

  const totalPrice = Number(product.price) * quantity;

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
        created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

  const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang);

  res.render('order-complete', { title: 'Order Complete', order, statusMeta });
});

app.get('/mypage', requireAuth, (req, res) => {
  const baseOrders = db
    .prepare(
      `
        SELECT o.*, p.brand, p.model, p.sub_model
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.created_by_user_id = ?
        ORDER BY o.id DESC
      `
    )
    .all(req.user.id);

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
    const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang);
    const latestLog = latestLogMap.get(Number(order.id)) || { event_note: '', created_at: '' };
    return {
      ...order,
      status_code: statusMeta.code,
      status_label: statusMeta.label,
      status_detail: statusMeta.detail,
      tracking_carrier_label: getTrackingCarrierLabel(order.tracking_carrier),
      latest_event_note: latestLog.event_note,
      latest_event_at: latestLog.created_at
    };
  });

  res.render('mypage', { title: 'My Page', orders });
});

app.get('/notice', (req, res) => {
  const notices = db
    .prepare('SELECT id, title, image_path, is_popup, created_at FROM notices ORDER BY id DESC')
    .all();
  res.render('notice-list', { title: 'Notice', notices });
});

app.get('/notice/:id', (req, res) => {
  const id = Number(req.params.id);
  const notice = db.prepare('SELECT * FROM notices WHERE id = ? LIMIT 1').get(id);
  if (!notice) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '공지사항이 없습니다.' });
  }
  res.render('notice-detail', { title: 'Notice Detail', notice });
});

app.get('/news', (req, res) => {
  const newsPosts = db
    .prepare(
      `
        SELECT id, title, content, image_path, created_at
        FROM news_posts
        ORDER BY id DESC
      `
    )
    .all();

  res.render('news-list', { title: 'News', newsPosts });
});

app.get('/news/:id', (req, res) => {
  const id = Number(req.params.id);
  const newsPost = db.prepare('SELECT * FROM news_posts WHERE id = ? LIMIT 1').get(id);

  if (!newsPost) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '뉴스 게시글이 없습니다.' });
  }

  const relatedNews = db
    .prepare(
      `
        SELECT id, title, created_at
        FROM news_posts
        WHERE id != ?
        ORDER BY id DESC
        LIMIT 5
      `
    )
    .all(newsPost.id);

  res.render('news-detail', { title: 'News Detail', newsPost, relatedNews });
});

app.get('/qc', (req, res) => {
  const orderNo = String(req.query.orderNo || '').trim();
  const items = orderNo
    ? db
        .prepare('SELECT * FROM qc_items WHERE order_no = ? ORDER BY id DESC')
        .all(orderNo)
    : db.prepare('SELECT * FROM qc_items ORDER BY id DESC LIMIT 30').all();

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
  const inquiries = db
    .prepare(
      `
        SELECT i.id, i.title, i.created_at, i.reply_content, u.username, i.user_id
        FROM inquiries i
        JOIN users u ON u.id = i.user_id
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

app.post('/inquiry/new', requireAuth, upload.single('image'), (req, res) => {
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();

  if (!title || !content) {
    setFlash(req, 'error', '제목과 내용을 입력해 주세요.');
    return res.redirect('/inquiry/new');
  }

  db.prepare(
    `
      INSERT INTO inquiries (user_id, title, content, image_path)
      VALUES (?, ?, ?, ?)
    `
  ).run(req.user.id, title, content, fileUrl(req.file));

  setFlash(req, 'success', '문의가 등록되었습니다.');
  res.redirect('/inquiry');
});

app.get('/inquiry/:id', (req, res) => {
  const id = Number(req.params.id);
  const inquiry = db
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

  if (!inquiry) {
    return res.status(404).render('simple-error', { title: 'Not Found', message: '문의를 찾을 수 없습니다.' });
  }

  const canOpen = Boolean(req.user && (req.user.isAdmin || req.user.id === Number(inquiry.user_id)));

  res.render('inquiry-detail', {
    title: 'Inquiry Detail',
    inquiry,
    canOpen,
    writerMasked: maskUsername(inquiry.username)
  });
});

app.get('/signup', (req, res) => {
  res.render('signup', { title: 'Sign up' });
});

app.post(
  '/signup',
  authAttemptGuard({ key: 'signup', redirectPath: '/signup', limit: 12 }),
  asyncRoute(async (req, res) => {
  const email = String(req.body.email || '').trim();
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');
  const passwordConfirm = String(req.body.passwordConfirm || '');
  const agreed = req.body.agreedTerms === 'on';

  if (!email || !username || !password || !passwordConfirm) {
    setFlash(req, 'error', '필수 항목을 입력해 주세요.');
    return res.redirect('/signup');
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
    return res.redirect('/signup');
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '아이디는 4~20자 영문/숫자/언더스코어만 사용 가능합니다.');
    return res.redirect('/signup');
  }

  if (!PASSWORD_REGEX.test(password)) {
    setFlash(req, 'error', '비밀번호는 영문/숫자 포함 8자 이상이어야 합니다.');
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
    const result = db.prepare(
      'INSERT INTO users (email, username, password_hash, agreed_terms, is_admin) VALUES (?, ?, ?, 1, 0)'
    ).run(email, username, hash);

    req.session.userId = Number(result.lastInsertRowid);
    req.session.isAdmin = false;
    resetAuthAttempt(req, 'signup');

    setFlash(req, 'success', '회원가입이 완료되었습니다.');
    res.redirect('/main');
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 아이디입니다.');
    res.redirect('/signup');
  }
  })
);

app.get('/login', (req, res) => {
  res.render('login', { title: 'Login' });
});

app.post(
  '/login',
  authAttemptGuard({ key: 'login', redirectPath: '/login', limit: 15 }),
  asyncRoute(async (req, res) => {
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');

  if (!username || !password) {
    setFlash(req, 'error', '아이디와 비밀번호를 입력해 주세요.');
    return res.redirect('/login');
  }

  const user = db
    .prepare(
      'SELECT id, username, password_hash, is_admin, admin_role, is_blocked, blocked_reason FROM users WHERE username = ? LIMIT 1'
    )
    .get(username);

  if (!user) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
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
  res.render('admin-login', { title: 'Admin Login' });
});

app.post(
  '/admin/login',
  authAttemptGuard({ key: 'admin-login', redirectPath: '/admin/login', limit: 10 }),
  asyncRoute(async (req, res) => {
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');

  const user = db
    .prepare(
      'SELECT id, username, password_hash, is_admin, admin_role, is_blocked, blocked_reason FROM users WHERE username = ? LIMIT 1'
    )
    .get(username);

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

  req.session.userId = Number(user.id);
  req.session.isAdmin = true;
  req.session.adminRole = normalizeAdminRole(user.admin_role);
  resetAuthAttempt(req, 'admin-login');

  logAdminActivityByUser(user, req, 'LOGIN_SUCCESS', 'admin login success');

  res.redirect('/admin/dashboard');
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
        is_primary: role === ADMIN_ROLE.PRIMARY
      };
    });

  const subAdminLogs = db
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
    .all(...logParams, SECURITY_PAGE_SIZE, logOffset)
    .map((row) => ({
      ...row,
      user_agent: String(row.user_agent || '')
    }));

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

  const securityAlerts = db
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
    .all(...alertParams, SECURITY_PAGE_SIZE, alertOffset)
    .map((row) => ({
      ...row,
      reason_label: getSecurityAlertReasonLabel(row.reason, lang)
    }));

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
  const filters = {
    section: normalizeMemberManageSection(options.section || ''),
    keyword: String(options.keyword || '').trim().slice(0, 120)
  };

  const where = ['u.is_admin = 0'];
  const params = [];

  if (filters.section === 'blocked') {
    where.push('u.is_blocked = 1');
  } else {
    where.push('u.is_blocked = 0');
  }

  if (filters.keyword) {
    where.push('(u.username LIKE ? OR u.full_name LIKE ? OR u.email LIKE ? OR u.phone LIKE ?)');
    const likeKeyword = `%${filters.keyword}%`;
    params.push(likeKeyword, likeKeyword, likeKeyword, likeKeyword);
  }

  const whereSql = where.join(' AND ');

  const totalRow = db
    .prepare(
      `
        SELECT COUNT(*) AS count
        FROM users u
        WHERE ${whereSql}
      `
    )
    .get(...params);
  const totalCount = Number(totalRow?.count || 0);
  const totalPages = Math.max(1, Math.ceil(totalCount / MEMBER_PAGE_SIZE));
  const page = clampPage(options.page, totalPages);
  const offset = (page - 1) * MEMBER_PAGE_SIZE;

  const members = db
    .prepare(
      `
        SELECT
          u.id,
          u.username,
          u.full_name,
          u.email,
          u.phone,
          u.agreed_terms,
          u.is_blocked,
          u.blocked_reason,
          u.blocked_at,
          u.created_at,
          (
            SELECT COUNT(*)
            FROM orders o
            WHERE o.created_by_user_id = u.id
          ) AS order_count
        FROM users u
        WHERE ${whereSql}
        ORDER BY u.id DESC
        LIMIT ?
        OFFSET ?
      `
    )
    .all(...params, MEMBER_PAGE_SIZE, offset)
    .map((row) => ({
      ...row,
      agreed_terms: Number(row.agreed_terms) === 1,
      is_blocked: Number(row.is_blocked) === 1,
      order_count: Number(row.order_count || 0)
    }));

  return {
    members,
    filters,
    pagination: {
      page,
      totalPages,
      totalCount,
      pageSize: MEMBER_PAGE_SIZE
    }
  };
}

function buildAdminDashboardViewData(lang = 'ko', options = {}) {
  const securityOptions = options.securityOptions || {};
  const memberOptions = options.memberOptions || {};
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
    bankAccountInfo: getSetting('bankAccountInfo', ''),
    contactInfo: getSetting('contactInfo', ''),
    businessInfo: getSetting('businessInfo', ''),
    languageDefault: getSetting('languageDefault', 'ko'),
    menusJson: JSON.stringify(publicMenus, null, 2)
  };

  const productGroupConfigs = getProductGroupConfigs();
  const productGroupKeys = productGroupConfigs.map((group) => group.key);
  const orderGroupFilter = normalizeAdminOrderGroupFilter(options.orderGroupFilter || '', productGroupKeys);
  const orderStatusFilter = normalizeAdminOrderStatusFilter(options.orderStatusFilter || '');
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const groupLabelMap = getProductGroupLabels(productGroupConfigs, lang);
  const products = db
    .prepare('SELECT * FROM products ORDER BY id DESC LIMIT 100')
    .all()
    .map((item) => decorateProductForView(item, productGroupMap.get(item.category_group)));

  let editingProduct = null;
  if (editProductId > 0) {
    const editRow = db.prepare('SELECT * FROM products WHERE id = ? LIMIT 1').get(editProductId);
    if (editRow) {
      const editGroupConfig = productGroupMap.get(editRow.category_group) || productGroupConfigs[0] || {
        key: editRow.category_group,
        labelKo: editRow.category_group,
        labelEn: editRow.category_group,
        mode: PRODUCT_GROUP_MODE.SIMPLE,
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

  const notices = db.prepare('SELECT * FROM notices ORDER BY id DESC LIMIT 50').all();
  const newsPosts = db.prepare('SELECT * FROM news_posts ORDER BY id DESC LIMIT 50').all();
  const qcs = db.prepare('SELECT * FROM qc_items ORDER BY id DESC LIMIT 50').all();
  const inquiries = db
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
  const securityPanelData = buildSecurityPanelData(lang, securityOptions);
  const memberManagePanelData = buildMemberManagePanelData(lang, memberOptions);

  return {
    settings,
    publicMenus,
    products,
    editingProduct,
    orders: ordersWithTimeline,
    orderGroupFilter,
    orderStatusFilter,
    notices,
    newsPosts,
    qcs,
    inquiries,
    securityPanelData,
    memberManagePanelData,
    dashboardStats: includeDashboardStats ? getCachedAdminDashboardStats() : null,
    trackingCarriers: TRACKING_CARRIERS,
    formatPrice,
    productGroups: productGroupConfigs.map((group) => group.key),
    productGroupConfigs,
    groupLabelMap
  };
}

function renderAdminDashboard(req, res, activeTab, extraData = {}) {
  const productGroupConfigs = getProductGroupConfigs();
  const orderGroupFilter = normalizeAdminOrderGroupFilter(
    extraData.orderGroupFilter || req.query.orderGroup || '',
    productGroupConfigs.map((group) => group.key)
  );
  const orderStatusFilter = normalizeAdminOrderStatusFilter(
    extraData.orderStatusFilter || req.query.orderStatus || ''
  );

  const viewData = buildAdminDashboardViewData(
    res.locals.ctx.lang,
    {
      securityOptions: extraData.securityOptions || parseSecurityQuery(req.query || {}),
      memberOptions: extraData.memberOptions || parseMemberManageQuery(req.query || {}),
      includeDashboardStats: activeTab === 'dashboard',
      productEditId: extraData.productEditId || 0,
      orderGroupFilter,
      orderStatusFilter
    }
  );
  return res.render('admin-dashboard', {
    title: 'Admin Dashboard',
    activeTab,
    securitySection: normalizeSecuritySection(extraData.securitySection),
    securityAccessDenied: Boolean(extraData.securityAccessDenied),
    menuSection: normalizeMenuManageSection(extraData.menuSection || ''),
    productSection: normalizeProductManageSection(extraData.productSection || ''),
    orderGroupFilter,
    orderStatusFilter,
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
    setFlash(req, 'error', '아이디는 4~20자 영문/숫자/언더스코어만 사용 가능합니다.');
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
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 아이디입니다.');
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
    setFlash(req, 'error', '아이디는 4~20자 영문/숫자/언더스코어만 사용 가능합니다.');
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
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 아이디입니다.');
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
    setFlash(req, 'error', '아이디는 4~20자 영문/숫자/언더스코어만 사용 가능합니다.');
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
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 아이디입니다.');
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
  const fullName = String(req.body.fullName || '').trim();
  const email = String(req.body.email || '').trim();
  const phone = normalizePhone(req.body.phone || '');
  const agreedTerms = req.body.agreedTerms === 'on' ? 1 : 0;

  if (!username || !email) {
    setFlash(req, 'error', '아이디와 이메일은 필수입니다.');
    return res.redirect(backPath);
  }

  if (!USERNAME_REGEX.test(username)) {
    setFlash(req, 'error', '아이디는 4~20자 영문/숫자/언더스코어만 사용 가능합니다.');
    return res.redirect(backPath);
  }

  if (!EMAIL_REGEX.test(email)) {
    setFlash(req, 'error', '이메일 형식이 올바르지 않습니다.');
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
        SET username = ?, full_name = ?, email = ?, phone = ?, agreed_terms = ?
        WHERE id = ? AND is_admin = 0
      `
    ).run(username, fullName, email, phone, agreedTerms, targetId);
  } catch {
    setFlash(req, 'error', '이미 사용 중인 이메일 또는 아이디입니다.');
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

app.get('/admin/site', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'site'));
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
app.get('/admin/notices', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'notices'));
app.get('/admin/news', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'news'));
app.get('/admin/qc', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'qc'));
app.get('/admin/orders', requireAdmin, (req, res) =>
  renderAdminDashboard(req, res, 'orders', {
    orderGroupFilter: req.query.orderGroup || 'all',
    orderStatusFilter: req.query.orderStatus || 'all'
  })
);
app.get('/admin/inquiries', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'inquiries'));

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
  const nextConfigs = [...configs, {
    key: requestedKey,
    labelKo,
    labelEn,
    mode: useFactoryTemplate ? PRODUCT_GROUP_MODE.FACTORY : PRODUCT_GROUP_MODE.SIMPLE,
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
    const backPath = safeBackPath(req, '/admin/site');
    const siteName = String(req.body.siteName || 'Chrono Lab').trim();
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
    const dayThemeDefaults = DEFAULT_THEME_COLORS.day;
    const nightThemeDefaults = DEFAULT_THEME_COLORS.night;

    const dayHeaderColor = normalizeHexColor(
      req.body.dayHeaderColor || req.body.headerColor || '',
      dayThemeDefaults.headerColor
    );
    const dayBackgroundColor = normalizeHexColor(
      req.body.dayBackgroundColor || req.body.backgroundColor || '',
      dayThemeDefaults.backgroundColor
    );
    const dayTextColor = normalizeHexColor(req.body.dayTextColor || '', dayThemeDefaults.textColor);
    const dayMutedColor = normalizeHexColor(req.body.dayMutedColor || '', dayThemeDefaults.mutedColor);
    const dayLineColor = normalizeHexColor(req.body.dayLineColor || '', dayThemeDefaults.lineColor);
    const dayCardColor = normalizeHexColor(req.body.dayCardColor || '', dayThemeDefaults.cardColor);
    const dayCardDarkColor = normalizeHexColor(req.body.dayCardDarkColor || '', dayThemeDefaults.cardDarkColor);
    const dayCardDarkTextColor = normalizeHexColor(
      req.body.dayCardDarkTextColor || '',
      dayThemeDefaults.cardDarkTextColor
    );
    const dayChipColor = normalizeHexColor(req.body.dayChipColor || '', dayThemeDefaults.chipColor);

    const nightHeaderColor = normalizeHexColor(req.body.nightHeaderColor || '', nightThemeDefaults.headerColor);
    const nightBackgroundColor = normalizeHexColor(
      req.body.nightBackgroundColor || '',
      nightThemeDefaults.backgroundColor
    );
    const nightTextColor = normalizeHexColor(req.body.nightTextColor || '', nightThemeDefaults.textColor);
    const nightMutedColor = normalizeHexColor(req.body.nightMutedColor || '', nightThemeDefaults.mutedColor);
    const nightLineColor = normalizeHexColor(req.body.nightLineColor || '', nightThemeDefaults.lineColor);
    const nightCardColor = normalizeHexColor(req.body.nightCardColor || '', nightThemeDefaults.cardColor);
    const nightCardDarkColor = normalizeHexColor(req.body.nightCardDarkColor || '', nightThemeDefaults.cardDarkColor);
    const nightCardDarkTextColor = normalizeHexColor(
      req.body.nightCardDarkTextColor || '',
      nightThemeDefaults.cardDarkTextColor
    );
    const nightChipColor = normalizeHexColor(req.body.nightChipColor || '', nightThemeDefaults.chipColor);

    const bankAccountInfo = String(req.body.bankAccountInfo || '').trim();
    const contactInfo = String(req.body.contactInfo || '').trim();
    const businessInfo = String(req.body.businessInfo || '').trim();
    const languageDefault = resolveLanguage(req.body.languageDefault || 'ko', 'ko');

    setSetting('siteName', siteName || 'Chrono Lab');
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

    // Backward-compatible legacy keys
    setSetting('headerColor', dayHeaderColor);
    setSetting('bankAccountInfo', bankAccountInfo);
    setSetting('contactInfo', contactInfo);
    setSetting('businessInfo', businessInfo);
    setSetting('languageDefault', languageDefault);

    if (req.body.menusJson) {
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

    setFlash(req, 'success', '사이트 설정이 저장되었습니다.');
    res.redirect(backPath);
  }
);

app.post('/admin/product/create', requireAdmin, upload.array('images', 20), asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/products?section=upload');
  const productGroupConfigs = getProductGroupConfigs();
  const fallbackGroupKey = productGroupConfigs[0]?.key || SHOP_PRODUCT_GROUPS[0] || '공장제';
  const productGroupMap = getProductGroupMap(productGroupConfigs);
  const categoryGroupRaw = String(req.body.categoryGroup || fallbackGroupKey).trim();
  const selectedGroup = productGroupMap.get(categoryGroupRaw) || productGroupConfigs[0] || {
    key: fallbackGroupKey,
    labelKo: fallbackGroupKey,
    labelEn: fallbackGroupKey,
    mode: PRODUCT_GROUP_MODE.SIMPLE,
    customFields: getGroupDefaultFields({ key: fallbackGroupKey, labelKo: fallbackGroupKey, labelEn: fallbackGroupKey })
  };
  const categoryGroup = selectedGroup.key;
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
  const fallbackGroupKey = productGroupConfigs[0]?.key || SHOP_PRODUCT_GROUPS[0] || '공장제';
  const categoryGroupRaw = String(req.body.categoryGroup || fallbackGroupKey).trim();
  const selectedGroup = productGroupMap.get(categoryGroupRaw) || productGroupConfigs[0] || {
    key: fallbackGroupKey,
    labelKo: fallbackGroupKey,
    labelEn: fallbackGroupKey,
    mode: PRODUCT_GROUP_MODE.SIMPLE,
    customFields: getGroupDefaultFields({ key: fallbackGroupKey, labelKo: fallbackGroupKey, labelEn: fallbackGroupKey })
  };

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
  setFlash(
    req,
    'success',
    `워터마크 일괄 적용 완료: 총 ${result.totalCount}개 중 ${result.processedCount}개 처리, ${result.skippedCount}개 스킵`
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

app.post('/admin/notice/create', requireAdmin, upload.single('image'), (req, res) => {
  const backPath = safeBackPath(req, '/admin/notices');
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();
  const isPopup = req.body.isPopup === 'on' ? 1 : 0;

  if (!title || !content) {
    setFlash(req, 'error', '공지 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('INSERT INTO notices (title, content, image_path, is_popup) VALUES (?, ?, ?, ?)').run(
    title,
    content,
    fileUrl(req.file),
    isPopup
  );

  setFlash(req, 'success', '공지사항이 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/news/create', requireAdmin, upload.single('image'), (req, res) => {
  const backPath = safeBackPath(req, '/admin/news');
  const title = String(req.body.title || '').trim();
  const content = String(req.body.content || '').trim();

  if (!title || !content) {
    setFlash(req, 'error', '뉴스 제목과 내용을 입력해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('INSERT INTO news_posts (title, content, image_path) VALUES (?, ?, ?)').run(
    title,
    content,
    fileUrl(req.file)
  );

  setFlash(req, 'success', '뉴스가 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/qc/create', requireAdmin, upload.single('image'), (req, res) => {
  const backPath = safeBackPath(req, '/admin/qc');
  const orderNo = String(req.body.orderNo || '').trim();
  const note = String(req.body.note || '').trim();

  if (!orderNo || !req.file) {
    setFlash(req, 'error', '주문번호와 이미지를 입력해 주세요.');
    return res.redirect(backPath);
  }

  db.prepare('INSERT INTO qc_items (order_no, image_path, note) VALUES (?, ?, ?)').run(
    orderNo,
    fileUrl(req.file),
    note
  );

  setFlash(req, 'success', 'QC 항목이 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/order/:id/confirm', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, order_no, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const current = normalizeOrderStatus(order.status);
  if (current !== ORDER_STATUS.PENDING_REVIEW) {
    setFlash(req, 'error', '입금확인중 상태에서만 입금확인 처리할 수 있습니다.');
    return res.redirect(backPath);
  }

  const updated = db.prepare(
    `
      UPDATE orders
      SET status = ?, checked_at = datetime('now')
      WHERE id = ? AND status = ?
    `
  ).run(ORDER_STATUS.ORDER_CONFIRMED, id, ORDER_STATUS.PENDING_REVIEW);

  if (updated.changes === 0) {
    setFlash(req, 'error', '이미 처리된 주문입니다. 페이지를 새로고침해 주세요.');
    return res.redirect(backPath);
  }

  appendOrderStatusLog(order.id, order.order_no, ORDER_STATUS.PENDING_REVIEW, ORDER_STATUS.ORDER_CONFIRMED, 'admin:confirm');
  incrementFunnelEvent(toKstDate(), FUNNEL_EVENT.PAYMENT_CONFIRMED);

  setFlash(req, 'success', '입금확인 처리되었습니다.');
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
    setFlash(req, 'error', '입금확인 상태에서만 출고중 처리할 수 있습니다.');
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

  setFlash(req, 'success', '출고중 상태로 변경되었습니다.');
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
    setFlash(req, 'error', '출고중 상태에서만 송장등록(배송시작) 처리할 수 있습니다.');
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

  setFlash(req, 'success', '수동 배송완료 처리되었습니다.');
  return res.redirect(backPath);
});

app.post('/admin/inquiry/:id/reply', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/inquiries');
  const id = Number(req.params.id);
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

app.use((req, res) => {
  res.status(404).render('simple-error', { title: 'Not Found', message: '페이지를 찾을 수 없습니다.' });
});

app.use((error, req, res, next) => {
  // eslint-disable-next-line no-console
  console.error('[chrono-lab:error]', error);

  const message = error?.message?.includes('지원되지 않는 파일 형식')
    ? error.message
    : '일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.';

  if (req.path === '/health' || req.accepts('json') === 'json') {
    return res.status(500).json({ ok: false, error: 'internal_server_error' });
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
