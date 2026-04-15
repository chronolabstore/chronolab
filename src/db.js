import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import Database from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import { PRODUCT_SEED_ITEMS } from './product-seeds.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'chronolab.db');
const dbDir = path.dirname(dbPath);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

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

const nodeEnv = String(process.env.NODE_ENV || '').trim().toLowerCase();
const renderExternalUrl = String(process.env.RENDER_EXTERNAL_URL || '').trim().toLowerCase();
const renderServiceName = String(process.env.RENDER_SERVICE_NAME || '').trim().toLowerCase();
const renderGitBranch = String(process.env.RENDER_GIT_BRANCH || '').trim().toLowerCase();
const isRenderStaging =
  renderExternalUrl.includes('chronolab-staging.onrender.com') ||
  renderServiceName.includes('staging') ||
  renderGitBranch === 'staging';
const defaultMaintenanceEnabled = nodeEnv !== 'production' || isRenderStaging;
const defaultSeedEnabled = nodeEnv === 'development' || nodeEnv === 'dev';

const shouldBootstrapSeedData = parseEnvFlag(
  process.env.ENABLE_BOOTSTRAP_SEED,
  defaultSeedEnabled
);

const shouldRunStartupDataMaintenance = parseEnvFlag(
  process.env.ENABLE_STARTUP_DATA_MAINTENANCE,
  defaultMaintenanceEnabled
);

export const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

const DEFAULT_FILTER_BRAND_MODEL_SEEDS = Object.freeze([
  Object.freeze({
    value: 'Audemars Piguet',
    labelKo: '오데마피게',
    labelEn: 'Audemars Piguet',
    models: Object.freeze([
      Object.freeze({ value: 'Royal Oak', labelKo: '로얄오크', labelEn: 'Royal Oak' }),
      Object.freeze({ value: 'Offshore', labelKo: '오프셔', labelEn: 'Offshore' })
    ])
  }),
  Object.freeze({
    value: 'Patek Philippe',
    labelKo: '파텍필립',
    labelEn: 'Patek Philippe',
    models: Object.freeze([
      Object.freeze({ value: 'Nautilus', labelKo: '노틸러스', labelEn: 'Nautilus' }),
      Object.freeze({ value: 'Aquanaut', labelKo: '아쿠아넛', labelEn: 'Aquanaut' })
    ])
  }),
  Object.freeze({
    value: 'Rolex',
    labelKo: '롤렉스',
    labelEn: 'Rolex',
    models: Object.freeze([
      Object.freeze({ value: 'Datejust', labelKo: '데이저스트', labelEn: 'Datejust' }),
      Object.freeze({ value: 'Day-Date', labelKo: '데이데이트', labelEn: 'Day-Date' }),
      Object.freeze({ value: 'Submariner', labelKo: '서브마리너', labelEn: 'Submariner' }),
      Object.freeze({ value: 'Daytona', labelKo: '데이토나', labelEn: 'Daytona' }),
      Object.freeze({ value: 'GMT-Master', labelKo: 'GMT마스터', labelEn: 'GMT-Master' }),
      Object.freeze({ value: 'Yacht-Master', labelKo: '요트마스터', labelEn: 'Yacht-Master' })
    ])
  }),
  Object.freeze({
    value: 'Cartier',
    labelKo: '까르띠에',
    labelEn: 'Cartier',
    models: Object.freeze([
      Object.freeze({ value: 'Santos', labelKo: '산토스', labelEn: 'Santos' })
    ])
  })
]);

const DEFAULT_FACTORY_FILTER_OPTIONS = Object.freeze([
  'VS',
  'APS',
  'DD',
  'PP',
  'PPM',
  'BV',
  'RC',
  'RG',
  'Gold'
]);

const DEFAULT_FILTER_BRAND_OPTIONS = Object.freeze(
  DEFAULT_FILTER_BRAND_MODEL_SEEDS.map((item) => item.value)
);

const DEFAULT_FILTER_BRAND_OPTION_LABELS = Object.freeze(
  DEFAULT_FILTER_BRAND_MODEL_SEEDS.reduce((acc, item) => {
    acc[item.value] = {
      labelKo: item.labelKo,
      labelEn: item.labelEn
    };
    return acc;
  }, {})
);

const DEFAULT_FILTER_MODEL_OPTIONS_BY_BRAND = Object.freeze(
  DEFAULT_FILTER_BRAND_MODEL_SEEDS.reduce((acc, item) => {
    acc[item.value] = item.models.map((model) => model.value);
    return acc;
  }, {})
);

const DEFAULT_FILTER_MODEL_OPTION_LABELS_BY_BRAND = Object.freeze(
  DEFAULT_FILTER_BRAND_MODEL_SEEDS.reduce((acc, item) => {
    acc[item.value] = item.models.reduce((modelAcc, model) => {
      modelAcc[model.value] = {
        labelKo: model.labelKo,
        labelEn: model.labelEn
      };
      return modelAcc;
    }, {});
    return acc;
  }, {})
);

const DEFAULT_FACTORY_FILTER_OPTION_LABELS = Object.freeze(
  DEFAULT_FACTORY_FILTER_OPTIONS.reduce((acc, value) => {
    acc[value] = {
      labelKo: value,
      labelEn: value
    };
    return acc;
  }, {})
);

export const DEFAULT_PRODUCT_GROUP_CONFIGS = [
  {
    key: '공장제',
    labelKo: '공장제',
    labelEn: 'Factory',
    mode: 'factory',
    showInMainTopBox: false,
    enableBrandFilter: true,
    enableModelFilter: true,
    enableFactoryFilter: true,
    brandOptions: [...DEFAULT_FILTER_BRAND_OPTIONS],
    factoryOptions: [...DEFAULT_FACTORY_FILTER_OPTIONS],
    modelOptions: [],
    modelOptionsByBrand: { ...DEFAULT_FILTER_MODEL_OPTIONS_BY_BRAND },
    brandOptionLabels: { ...DEFAULT_FILTER_BRAND_OPTION_LABELS },
    factoryOptionLabels: { ...DEFAULT_FACTORY_FILTER_OPTION_LABELS },
    modelOptionLabelsByBrand: { ...DEFAULT_FILTER_MODEL_OPTION_LABELS_BY_BRAND },
    customFields: [
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
    ]
  },
  {
    key: '젠파츠',
    labelKo: '젠파츠',
    labelEn: 'Gen Parts',
    mode: 'simple',
    showInMainTopBox: false,
    enableBrandFilter: true,
    enableModelFilter: true,
    enableFactoryFilter: false,
    brandOptions: [...DEFAULT_FILTER_BRAND_OPTIONS],
    factoryOptions: [],
    modelOptions: [],
    modelOptionsByBrand: { ...DEFAULT_FILTER_MODEL_OPTIONS_BY_BRAND },
    brandOptionLabels: { ...DEFAULT_FILTER_BRAND_OPTION_LABELS },
    factoryOptionLabels: {},
    modelOptionLabelsByBrand: { ...DEFAULT_FILTER_MODEL_OPTION_LABELS_BY_BRAND },
    customFields: [
      { key: 'title', labelKo: '제목', labelEn: 'Title', type: 'text', required: true },
      { key: 'detailed_description', labelKo: '상세설명', labelEn: 'Detailed Description', type: 'textarea', required: true },
      { key: 'price', labelKo: '가격', labelEn: 'Price', type: 'number', required: false }
    ]
  },
  {
    key: '현지중고',
    labelKo: '현지중고',
    labelEn: 'Local Used',
    mode: 'simple',
    showInMainTopBox: false,
    enableBrandFilter: true,
    enableModelFilter: true,
    enableFactoryFilter: true,
    brandOptions: [...DEFAULT_FILTER_BRAND_OPTIONS],
    factoryOptions: [...DEFAULT_FACTORY_FILTER_OPTIONS],
    modelOptions: [],
    modelOptionsByBrand: { ...DEFAULT_FILTER_MODEL_OPTIONS_BY_BRAND },
    brandOptionLabels: { ...DEFAULT_FILTER_BRAND_OPTION_LABELS },
    factoryOptionLabels: { ...DEFAULT_FACTORY_FILTER_OPTION_LABELS },
    modelOptionLabelsByBrand: { ...DEFAULT_FILTER_MODEL_OPTION_LABELS_BY_BRAND },
    customFields: [
      { key: 'title', labelKo: '제목', labelEn: 'Title', type: 'text', required: true },
      { key: 'detailed_description', labelKo: '상세설명', labelEn: 'Detailed Description', type: 'textarea', required: true },
      { key: 'price', labelKo: '가격', labelEn: 'Price', type: 'number', required: false }
    ]
  }
];

export const SHOP_PRODUCT_GROUPS = DEFAULT_PRODUCT_GROUP_CONFIGS.map((group) => group.key);

export const DEFAULT_MEMBER_LEVEL_RULES = [
  {
    id: 'starter',
    nameKo: '입문자',
    nameEn: 'Beginner',
    name: '입문자',
    colorTheme: 'red',
    operator: 'lt',
    thresholdAmount: 2000000
  },
  {
    id: 'collector',
    nameKo: '수집가',
    nameEn: 'Collector',
    name: '수집가',
    colorTheme: 'blue',
    operator: 'gte',
    thresholdAmount: 2000000
  },
  {
    id: 'enthusiast',
    nameKo: '애호가',
    nameEn: 'Enthusiast',
    name: '애호가',
    colorTheme: 'green',
    operator: 'gte',
    thresholdAmount: 10000000
  },
  {
    id: 'expert',
    nameKo: '전문가',
    nameEn: 'Expert',
    name: '전문가',
    colorTheme: 'amber',
    operator: 'gte',
    thresholdAmount: 30000000
  }
];

export const DEFAULT_MEMBER_LEVEL_POINT_RATES = {
  starter: 0.5,
  collector: 1,
  enthusiast: 1.5,
  expert: 2
};

const defaultMenus = [
  { id: 'home', labelKo: '메인페이지', labelEn: 'Main', path: '/main', isHidden: false },
  { id: 'notice', labelKo: '공지사항', labelEn: 'Notice', path: '/notice', isHidden: false },
  { id: 'news', labelKo: '뉴스', labelEn: 'News', path: '/news', isHidden: false },
  { id: 'shop', labelKo: '쇼핑몰', labelEn: 'Shop', path: '/shop', isHidden: false },
  { id: 'qc', labelKo: 'QC', labelEn: 'QC', path: '/qc', isHidden: false },
  { id: 'review', labelKo: '구매후기', labelEn: 'Reviews', path: '/review', isHidden: false },
  { id: 'inquiry', labelKo: '문의', labelEn: 'Inquiry', path: '/inquiry', isHidden: false }
];

const defaultSettings = {
  siteName: 'Chrono Lab',
  headerColor: '#0f172a',
  dayHeaderColor: '#0f172a',
  dayBackgroundColor: '#f4f6fb',
  dayTextColor: '#111827',
  dayMutedColor: '#5f6b7e',
  dayLineColor: '#d6ddea',
  dayCardColor: '#ffffff',
  dayCardDarkColor: '#0f172a',
  dayCardDarkTextColor: '#f8fafc',
  dayChipColor: '#eef2f8',
  nightHeaderColor: '#0b1220',
  nightBackgroundColor: '#070b14',
  nightTextColor: '#f5f8ff',
  nightMutedColor: '#b7c3d9',
  nightLineColor: '#435574',
  nightCardColor: '#111b30',
  nightCardDarkColor: '#0c1424',
  nightCardDarkTextColor: '#f5f8ff',
  nightChipColor: '#1b2941',
  dayBackgroundType: 'color',
  dayBackgroundImagePath: '',
  nightBackgroundType: 'color',
  nightBackgroundImagePath: '',
  dayHeaderLogoPath: '',
  nightHeaderLogoPath: '',
  dayHeaderSymbolPath: '',
  nightHeaderSymbolPath: '',
  dayFooterLogoPath: '',
  nightFooterLogoPath: '',
  headerLogoPath: '',
  headerSymbolPath: '',
  footerLogoPath: '',
  backgroundType: 'color',
  backgroundValue: '#f4f6fb',
  menus: JSON.stringify(defaultMenus),
  bankAccountInfo: '입금계좌: 은행명 000-0000-0000 (예금주: Chrono Lab)',
  signupBonusPoints: '10000',
  reviewRewardPoints: '5000',
  purchasePointRate: '0',
  memberLevelIncludedGroups: JSON.stringify(SHOP_PRODUCT_GROUPS),
  memberLevelRules: JSON.stringify(DEFAULT_MEMBER_LEVEL_RULES),
  memberLevelPointRates: JSON.stringify(DEFAULT_MEMBER_LEVEL_POINT_RATES),
  contactInfo: '고객센터: 010-0000-0000 / 카카오톡: @chronolab',
  businessInfo: '상호: Chrono Lab | 대표: Chrono Team | 사업자번호: 000-00-00000',
  footerBrandCopyKo: '심플하고 신뢰할 수 있는 시계 쇼핑.',
  footerBrandCopyEn: 'Simple. Clean. Trusted watch shopping.',
  heroLeftTitleKo: 'Chrono Lab',
  heroLeftTitleEn: 'Chrono Lab',
  heroLeftSubtitleKo: '심플하고 신뢰감 있는 시계 쇼핑 경험',
  heroLeftSubtitleEn: 'Simple, trustworthy watch shopping experience.',
  heroLeftCtaPath: '/shop',
  heroLeftBackgroundType: 'color',
  heroLeftBackgroundColor: '#eef2f8',
  heroLeftBackgroundImagePath: '',
  heroRightTitleKo: '프리미엄 위치 셀렉션',
  heroRightTitleEn: 'Premium Shortcut Selection',
  heroRightSubtitleKo: '결제는 계좌이체만 지원됩니다.',
  heroRightSubtitleEn: 'Bank transfer only for payment.',
  heroRightBackgroundColor: '#0f172a',
  heroQuickMenuPaths: JSON.stringify([
    '/notice',
    '/news',
    '/shop',
    '/qc',
    '/review',
    '/inquiry'
  ]),
  languageDefault: 'ko',
  productGroupConfigs: JSON.stringify(DEFAULT_PRODUCT_GROUP_CONFIGS),
  productBadgeSeedV1: '0'
};

const HEX_COLOR_REGEX = /^#[0-9a-fA-F]{6}$/;

function upsertDefaultSetting(key, value) {
  db.prepare('INSERT OR IGNORE INTO site_settings (setting_key, setting_value) VALUES (?, ?)').run(
    key,
    String(value)
  );
}

function upsertMetric(key, value = 0) {
  db.prepare('INSERT OR IGNORE INTO metrics (metric_key, metric_value) VALUES (?, ?)').run(key, value);
}

function ensureSignupBonusBaseline() {
  if (!shouldRunStartupDataMaintenance) {
    return;
  }
  const row = db
    .prepare('SELECT setting_value FROM site_settings WHERE setting_key = ? LIMIT 1')
    .get('signupBonusPoints');
  const raw = String(row?.setting_value ?? '').trim();
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    setSetting('signupBonusPoints', '10000');
  }
}

function applyMemberLevelThemeBaselineV20260415() {
  const markerKey = 'memberLevelThemeBaselineSeedV20260415';
  if (String(getSetting(markerKey, '0') || '0') === '1') {
    return;
  }

  const baselineRules = [
    { id: 'starter', nameKo: '입문자', nameEn: 'Beginner', colorTheme: 'red' },
    { id: 'collector', nameKo: '수집가', nameEn: 'Collector', colorTheme: 'blue' },
    { id: 'enthusiast', nameKo: '애호가', nameEn: 'Enthusiast', colorTheme: 'green' },
    { id: 'expert', nameKo: '전문가', nameEn: 'Expert', colorTheme: 'amber' }
  ];

  const baselineById = new Map(
    baselineRules.map((rule) => [String(rule.id || '').trim().toLowerCase(), rule])
  );

  const baselineAliases = new Map([
    ['입문자', 'starter'],
    ['beginner', 'starter'],
    ['수집가', 'collector'],
    ['collector', 'collector'],
    ['애호가', 'enthusiast'],
    ['enthusiast', 'enthusiast'],
    ['전문가', 'expert'],
    ['expert', 'expert']
  ]);

  const rawValue = String(getSetting('memberLevelRules', JSON.stringify(DEFAULT_MEMBER_LEVEL_RULES)) || '[]');
  let parsed = [];
  try {
    const maybeParsed = JSON.parse(rawValue);
    if (Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = [];
  }

  const sourceRules = parsed.length > 0 ? parsed : DEFAULT_MEMBER_LEVEL_RULES;
  let changed = false;
  const nextRules = sourceRules.map((rawRule = {}, index) => {
    const safeRule = rawRule && typeof rawRule === 'object' ? rawRule : {};
    const normalizedId = String(safeRule.id || '').trim().toLowerCase();
    let baseline = baselineById.get(normalizedId);

    if (!baseline) {
      const nameCandidates = [
        String(safeRule.nameKo || '').trim().toLowerCase(),
        String(safeRule.nameEn || '').trim().toLowerCase(),
        String(safeRule.name || '').trim().toLowerCase(),
        String(safeRule.label || '').trim().toLowerCase()
      ].filter(Boolean);

      for (const candidate of nameCandidates) {
        const matchedBaselineId = baselineAliases.get(candidate);
        if (matchedBaselineId) {
          baseline = baselineById.get(matchedBaselineId);
          break;
        }
      }
    }

    if (!baseline && sourceRules.length === baselineRules.length && index < baselineRules.length) {
      baseline = baselineRules[index];
    }

    if (!baseline) {
      return safeRule;
    }

    const normalizedTheme = String(safeRule.colorTheme || safeRule.color_theme || '').trim().toLowerCase();
    const nextRule = {
      ...safeRule,
      nameKo: baseline.nameKo,
      nameEn: baseline.nameEn,
      name: baseline.nameKo,
      colorTheme: baseline.colorTheme
    };

    if (
      String(safeRule.nameKo || safeRule.name || '').trim() !== baseline.nameKo ||
      String(safeRule.nameEn || safeRule.name || '').trim() !== baseline.nameEn ||
      normalizedTheme !== baseline.colorTheme
    ) {
      changed = true;
    }
    return nextRule;
  });

  if (changed) {
    setSetting('memberLevelRules', JSON.stringify(nextRules));
  }
  setSetting(markerKey, '1');
}

function migrateLegacyThemeAssetSettings() {
  if (!shouldRunStartupDataMaintenance) {
    return;
  }
  const legacyHeaderLogoPath = String(getSetting('headerLogoPath', '') || '').trim();
  const legacyHeaderSymbolPath = String(getSetting('headerSymbolPath', '') || '').trim();
  const legacyFooterLogoPath = String(getSetting('footerLogoPath', '') || '').trim();
  const legacyBackgroundType = String(getSetting('backgroundType', 'color') || 'color').trim() === 'image'
    ? 'image'
    : 'color';
  const legacyBackgroundValue = String(getSetting('backgroundValue', '') || '').trim();
  const legacyBackgroundImagePath =
    legacyBackgroundType === 'image' &&
    legacyBackgroundValue &&
    !HEX_COLOR_REGEX.test(legacyBackgroundValue)
      ? legacyBackgroundValue
      : '';

  const dayHeaderLogoPath = String(getSetting('dayHeaderLogoPath', '') || '').trim();
  const dayHeaderSymbolPath = String(getSetting('dayHeaderSymbolPath', '') || '').trim();
  const dayFooterLogoPath = String(getSetting('dayFooterLogoPath', '') || '').trim();
  let dayBackgroundType = String(getSetting('dayBackgroundType', '') || '').trim();
  const dayBackgroundImagePath = String(getSetting('dayBackgroundImagePath', '') || '').trim();

  if (!dayHeaderLogoPath && legacyHeaderLogoPath) setSetting('dayHeaderLogoPath', legacyHeaderLogoPath);
  if (!dayHeaderSymbolPath && legacyHeaderSymbolPath) setSetting('dayHeaderSymbolPath', legacyHeaderSymbolPath);
  if (!dayFooterLogoPath && legacyFooterLogoPath) setSetting('dayFooterLogoPath', legacyFooterLogoPath);

  if (dayBackgroundType !== 'color' && dayBackgroundType !== 'image') {
    dayBackgroundType = legacyBackgroundType;
    setSetting('dayBackgroundType', dayBackgroundType);
  }
  if (!dayBackgroundImagePath && legacyBackgroundImagePath) {
    setSetting('dayBackgroundImagePath', legacyBackgroundImagePath);
    if (dayBackgroundType !== 'image') {
      dayBackgroundType = 'image';
      setSetting('dayBackgroundType', 'image');
    }
  }

  const resolvedDayHeaderLogoPath = String(getSetting('dayHeaderLogoPath', legacyHeaderLogoPath) || '').trim();
  const resolvedDayHeaderSymbolPath = String(getSetting('dayHeaderSymbolPath', legacyHeaderSymbolPath) || '').trim();
  const resolvedDayFooterLogoPath = String(getSetting('dayFooterLogoPath', legacyFooterLogoPath) || '').trim();
  const resolvedDayBackgroundType = String(getSetting('dayBackgroundType', dayBackgroundType || 'color') || 'color').trim() === 'image'
    ? 'image'
    : 'color';
  const resolvedDayBackgroundImagePath = String(getSetting('dayBackgroundImagePath', dayBackgroundImagePath || '') || '').trim();

  if (!String(getSetting('nightHeaderLogoPath', '') || '').trim() && resolvedDayHeaderLogoPath) {
    setSetting('nightHeaderLogoPath', resolvedDayHeaderLogoPath);
  }
  if (!String(getSetting('nightHeaderSymbolPath', '') || '').trim() && resolvedDayHeaderSymbolPath) {
    setSetting('nightHeaderSymbolPath', resolvedDayHeaderSymbolPath);
  }
  if (!String(getSetting('nightFooterLogoPath', '') || '').trim() && resolvedDayFooterLogoPath) {
    setSetting('nightFooterLogoPath', resolvedDayFooterLogoPath);
  }

  const nightBackgroundTypeRaw = String(getSetting('nightBackgroundType', '') || '').trim();
  const nightBackgroundImagePath = String(getSetting('nightBackgroundImagePath', '') || '').trim();
  if (nightBackgroundTypeRaw !== 'color' && nightBackgroundTypeRaw !== 'image') {
    setSetting('nightBackgroundType', resolvedDayBackgroundType);
  }
  if (!nightBackgroundImagePath && resolvedDayBackgroundImagePath) {
    setSetting('nightBackgroundImagePath', resolvedDayBackgroundImagePath);
    if (legacyBackgroundType === 'image' && nightBackgroundTypeRaw === 'color') {
      setSetting('nightBackgroundType', 'image');
    }
  }
}

function normalizePath(pathValue = '') {
  let nextPath = String(pathValue || '').trim();
  if (!nextPath) {
    return '/main';
  }

  if (!nextPath.startsWith('/')) {
    nextPath = `/${nextPath}`;
  }

  if (nextPath === '/') nextPath = '/main';
  if (nextPath === '/notices') nextPath = '/notice';
  if (nextPath === '/reviews') nextPath = '/review';
  if (nextPath === '/inquiries') nextPath = '/inquiry';

  return nextPath;
}

function normalizeMenus(rawMenus) {
  let parsed = [];
  try {
    const maybeParsed = JSON.parse(rawMenus);
    if (Array.isArray(maybeParsed)) {
      parsed = maybeParsed;
    }
  } catch {
    parsed = [];
  }

  const filtered = parsed
    .filter((menu) => menu && typeof menu === 'object')
    .map((menu, idx) => ({
      id: String(menu.id || `custom-${idx + 1}`),
      labelKo: String(menu.labelKo || menu.labelEn || `메뉴${idx + 1}`),
      labelEn: String(menu.labelEn || menu.labelKo || `Menu${idx + 1}`),
      path: normalizePath(menu.path),
      isHidden:
        menu.isHidden === true ||
        String(menu.isHidden || '').toLowerCase() === 'true' ||
        String(menu.isHidden || '') === '1'
    }))
    .filter((menu) => !menu.path.startsWith('/admin'));

  const usedByDefault = new Set();
  const coreMenus = defaultMenus.map((defaultMenu) => {
    const foundIndex = filtered.findIndex(
      (menu) => menu.id === defaultMenu.id || normalizePath(menu.path) === defaultMenu.path
    );

    if (foundIndex >= 0) {
      usedByDefault.add(foundIndex);
      return {
        ...defaultMenu,
        labelKo: filtered[foundIndex].labelKo || defaultMenu.labelKo,
        labelEn: filtered[foundIndex].labelEn || defaultMenu.labelEn,
        isHidden: Boolean(filtered[foundIndex].isHidden)
      };
    }

    return { ...defaultMenu };
  });

  const extras = filtered.filter((_, idx) => !usedByDefault.has(idx));
  return [...coreMenus, ...extras];
}

function ensureProductsCategoryColumn() {
  const columns = db.prepare('PRAGMA table_info(products)').all();
  const hasCategoryGroup = columns.some((column) => column.name === 'category_group');

  if (!hasCategoryGroup) {
    db.prepare("ALTER TABLE products ADD COLUMN category_group TEXT NOT NULL DEFAULT '공장제'").run();
  }
}

function ensureProductsExtraFieldsColumn() {
  const columns = db.prepare('PRAGMA table_info(products)').all();
  const hasExtraFields = columns.some((column) => column.name === 'extra_fields_json');

  if (!hasExtraFields) {
    db.prepare("ALTER TABLE products ADD COLUMN extra_fields_json TEXT NOT NULL DEFAULT '{}'").run();
  }
}

function ensureProductsSoldOutColumn() {
  const columns = db.prepare('PRAGMA table_info(products)').all();
  const hasSoldOut = columns.some((column) => column.name === 'is_sold_out');

  if (!hasSoldOut) {
    db.prepare('ALTER TABLE products ADD COLUMN is_sold_out INTEGER NOT NULL DEFAULT 0').run();
  }
}

function ensureProductBadgeTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS product_badge_defs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE,
      label_ko TEXT NOT NULL,
      label_en TEXT NOT NULL,
      color_theme TEXT NOT NULL DEFAULT 'slate',
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS product_badges (
      product_id INTEGER NOT NULL,
      badge_def_id INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (product_id, badge_def_id),
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      FOREIGN KEY (badge_def_id) REFERENCES product_badge_defs(id) ON DELETE CASCADE
    );
  `);

  db.prepare('CREATE INDEX IF NOT EXISTS idx_product_badges_product_id ON product_badges (product_id)').run();
  db.prepare('CREATE INDEX IF NOT EXISTS idx_product_badges_badge_def_id ON product_badges (badge_def_id)').run();

  const badgeDefColumns = db.prepare('PRAGMA table_info(product_badge_defs)').all();
  const hasColorTheme = badgeDefColumns.some((column) => column.name === 'color_theme');
  if (!hasColorTheme) {
    db.prepare("ALTER TABLE product_badge_defs ADD COLUMN color_theme TEXT NOT NULL DEFAULT 'slate'").run();
  }

  db.prepare(
    `
      UPDATE product_badge_defs
      SET color_theme = 'slate'
      WHERE color_theme IS NULL OR TRIM(color_theme) = ''
    `
  ).run();

  const applyDefaultColorTheme = db.prepare(
    `
      UPDATE product_badge_defs
      SET color_theme = ?
      WHERE code = ?
        AND (color_theme IS NULL OR TRIM(color_theme) = '' OR LOWER(TRIM(color_theme)) = 'slate')
    `
  );
  applyDefaultColorTheme.run('red', 'domestic-stock');
  applyDefaultColorTheme.run('blue', 'same-day-dispatch');
  applyDefaultColorTheme.run('green', 'made-to-order');
}

function seedDefaultProductBadgesOnce() {
  const seedFlag = String(getSetting('productBadgeSeedV1', '0') || '0');
  if (seedFlag === '1') {
    return;
  }

  const countRow = db.prepare('SELECT COUNT(*) AS count FROM product_badge_defs').get();
  if (Number(countRow?.count || 0) === 0) {
    const defaults = [
      { code: 'domestic-stock', labelKo: '국내재고', labelEn: 'Domestic Stock', colorTheme: 'red' },
      { code: 'same-day-dispatch', labelKo: '당일발송', labelEn: 'Same-Day Dispatch', colorTheme: 'blue' },
      { code: 'made-to-order', labelKo: '주문제작', labelEn: 'Made to Order', colorTheme: 'green' }
    ];
    const insert = db.prepare(
      `
        INSERT INTO product_badge_defs (code, label_ko, label_en, color_theme, sort_order, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
      `
    );
    defaults.forEach((item, index) => {
      insert.run(item.code, item.labelKo, item.labelEn, item.colorTheme, index + 1);
    });
  }

  setSetting('productBadgeSeedV1', '1');
}

function ensureUserAdminProfileColumns() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE users ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('full_name', "full_name TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('phone', "phone TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('admin_role', "admin_role TEXT NOT NULL DEFAULT ''");
}

function ensureAdminOtpColumns() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE users ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('admin_otp_secret', "admin_otp_secret TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('admin_otp_enabled', 'admin_otp_enabled INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('admin_otp_enabled_at', 'admin_otp_enabled_at TEXT');
}

function ensureUserMemberProfileColumns() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE users ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('customs_clearance_no', "customs_clearance_no TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('default_address', "default_address TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('default_postcode', "default_postcode TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('default_address_base', "default_address_base TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('default_address_detail', "default_address_detail TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('nickname', "nickname TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('profile_image_path', "profile_image_path TEXT NOT NULL DEFAULT ''");
  if (shouldRunStartupDataMaintenance) {
    db.prepare("UPDATE users SET nickname = username WHERE COALESCE(TRIM(nickname), '') = ''").run();
    db.prepare(
      `
        UPDATE users
        SET default_address_base = default_address
        WHERE COALESCE(TRIM(default_address_base), '') = ''
          AND COALESCE(TRIM(default_address), '') != ''
      `
    ).run();
  }
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

function ensureUserMemberUidColumn() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  if (!columnNames.has('member_uid')) {
    db.prepare("ALTER TABLE users ADD COLUMN member_uid TEXT NOT NULL DEFAULT ''").run();
    columnNames.add('member_uid');
  }

  db.prepare('CREATE INDEX IF NOT EXISTS idx_users_member_uid ON users (member_uid)').run();

  if (!shouldRunStartupDataMaintenance) {
    return;
  }

  const usedRows = db
    .prepare(
      `
        SELECT member_uid
        FROM users
        WHERE is_admin = 0
          AND COALESCE(TRIM(member_uid), '') != ''
      `
    )
    .all();
  const usedSeq = new Set(
    usedRows
      .map((row) => parseMemberUidSequence(row.member_uid))
      .filter((num) => Number.isInteger(num) && num > 0)
  );

  const missingRows = db
    .prepare(
      `
        SELECT id
        FROM users
        WHERE is_admin = 0
          AND COALESCE(TRIM(member_uid), '') = ''
        ORDER BY datetime(created_at) ASC, id ASC
      `
    )
    .all();

  if (missingRows.length === 0) {
    return;
  }

  const assignTx = db.transaction(() => {
    let cursor = 1;
    for (const row of missingRows) {
      while (usedSeq.has(cursor)) {
        cursor += 1;
      }
      db.prepare('UPDATE users SET member_uid = ? WHERE id = ? AND is_admin = 0').run(
        formatMemberUid(cursor),
        row.id
      );
      usedSeq.add(cursor);
      cursor += 1;
    }
  });

  assignTx();
}

function ensureAddressBookTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS address_book (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      label TEXT NOT NULL,
      postcode TEXT NOT NULL DEFAULT '',
      address_base TEXT NOT NULL DEFAULT '',
      address_detail TEXT NOT NULL DEFAULT '',
      is_default INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_address_book_user_id ON address_book (user_id);
    CREATE INDEX IF NOT EXISTS idx_address_book_user_default ON address_book (user_id, is_default);
  `);

  const columns = db.prepare('PRAGMA table_info(address_book)').all();
  if (columns.length > 0) {
    const columnNames = new Set(columns.map((column) => column.name));

    if (!columnNames.has('postcode')) {
      db.prepare("ALTER TABLE address_book ADD COLUMN postcode TEXT NOT NULL DEFAULT ''").run();
    }
    if (!columnNames.has('address_base')) {
      db.prepare("ALTER TABLE address_book ADD COLUMN address_base TEXT NOT NULL DEFAULT ''").run();
    }
    if (!columnNames.has('address_detail')) {
      db.prepare("ALTER TABLE address_book ADD COLUMN address_detail TEXT NOT NULL DEFAULT ''").run();
    }
    if (!columnNames.has('updated_at')) {
      db.prepare("ALTER TABLE address_book ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''").run();
      if (shouldRunStartupDataMaintenance) {
        db.prepare("UPDATE address_book SET updated_at = datetime('now') WHERE COALESCE(TRIM(updated_at), '') = ''").run();
      }
    }
  }
}

function ensureUserPointColumns() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const hasRewardPoints = columns.some((column) => column.name === 'reward_points');

  if (!hasRewardPoints) {
    db.prepare('ALTER TABLE users ADD COLUMN reward_points INTEGER NOT NULL DEFAULT 0').run();
  }
}

function ensureUserBlockColumns() {
  const columns = db.prepare('PRAGMA table_info(users)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE users ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('is_blocked', 'is_blocked INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('blocked_reason', "blocked_reason TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('blocked_at', 'blocked_at TEXT');
}

function ensureOrdersCustomsColumn() {
  const columns = db.prepare('PRAGMA table_info(orders)').all();
  const hasCustomsNo = columns.some((column) => column.name === 'customs_clearance_no');

  if (!hasCustomsNo) {
    db.prepare("ALTER TABLE orders ADD COLUMN customs_clearance_no TEXT NOT NULL DEFAULT ''").run();
  }
}

function ensureOrdersTrackingColumns() {
  const columns = db.prepare('PRAGMA table_info(orders)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE orders ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('tracking_carrier', "tracking_carrier TEXT NOT NULL DEFAULT 'kr.cjlogistics'");
  addColumnIfMissing('tracking_number', "tracking_number TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('tracking_last_event', "tracking_last_event TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('tracking_last_checked_at', 'tracking_last_checked_at TEXT');
  addColumnIfMissing('checked_at', 'checked_at TEXT');
  addColumnIfMissing('ready_to_ship_at', 'ready_to_ship_at TEXT');
  addColumnIfMissing('shipping_started_at', 'shipping_started_at TEXT');
  addColumnIfMissing('delivered_at', 'delivered_at TEXT');
}

function ensureOrdersPointColumns() {
  const columns = db.prepare('PRAGMA table_info(orders)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE orders ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('awarded_points', 'awarded_points INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('points_awarded_at', 'points_awarded_at TEXT');
  addColumnIfMissing('point_rate_snapshot', 'point_rate_snapshot REAL NOT NULL DEFAULT 0');
  addColumnIfMissing('point_level_id', "point_level_id TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('point_level_name', "point_level_name TEXT NOT NULL DEFAULT ''");
}

function ensureOrdersSalesSnapshotColumns() {
  const columns = db.prepare('PRAGMA table_info(orders)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  const addColumnIfMissing = (name, ddl) => {
    if (!columnNames.has(name)) {
      db.prepare(`ALTER TABLE orders ADD COLUMN ${ddl}`).run();
      columnNames.add(name);
    }
  };

  addColumnIfMissing('sales_tab_key', "sales_tab_key TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('sales_scope_id', "sales_scope_id TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('sales_scope_name', "sales_scope_name TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('sales_scope_date', "sales_scope_date TEXT NOT NULL DEFAULT ''");
  addColumnIfMissing('sales_exchange_rate_snapshot', 'sales_exchange_rate_snapshot REAL NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_shipping_fee_krw_snapshot', 'sales_shipping_fee_krw_snapshot INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_cost_rmb_snapshot', 'sales_cost_rmb_snapshot REAL NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_cost_krw_snapshot', 'sales_cost_krw_snapshot INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_margin_krw_snapshot', 'sales_margin_krw_snapshot INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_real_margin_krw_snapshot', 'sales_real_margin_krw_snapshot INTEGER NOT NULL DEFAULT 0');
  addColumnIfMissing('sales_synced_at', 'sales_synced_at TEXT');
}

function ensureReviewsOrderColumn() {
  const columns = db.prepare('PRAGMA table_info(reviews)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  if (!columnNames.has('order_id')) {
    db.prepare('ALTER TABLE reviews ADD COLUMN order_id INTEGER').run();
  }
  if (!columnNames.has('reward_points_awarded')) {
    db.prepare('ALTER TABLE reviews ADD COLUMN reward_points_awarded INTEGER NOT NULL DEFAULT 0').run();
  }

  db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_order_id ON reviews (order_id)').run();
  db.prepare('CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_order_unique ON reviews (order_id)').run();
}

function ensureDailyVisitSplitColumns() {
  const columns = db.prepare('PRAGMA table_info(daily_visits)').all();
  const columnNames = new Set(columns.map((column) => column.name));

  if (!columnNames.has('member_visit_count')) {
    db.prepare('ALTER TABLE daily_visits ADD COLUMN member_visit_count INTEGER NOT NULL DEFAULT 0').run();
  }

  if (!columnNames.has('guest_visit_count')) {
    db.prepare('ALTER TABLE daily_visits ADD COLUMN guest_visit_count INTEGER NOT NULL DEFAULT 0').run();
  }
}

function ensureContentVisibilityColumns() {
  const contentTargets = [
    { table: 'notices', column: 'is_hidden' },
    { table: 'news_posts', column: 'is_hidden' },
    { table: 'qc_items', column: 'is_hidden' },
    { table: 'inquiries', column: 'is_hidden' }
  ];

  for (const target of contentTargets) {
    const columns = db.prepare(`PRAGMA table_info(${target.table})`).all();
    const hasColumn = columns.some((column) => column.name === target.column);
    if (!hasColumn) {
      db.prepare(`ALTER TABLE ${target.table} ADD COLUMN ${target.column} INTEGER NOT NULL DEFAULT 0`).run();
    }
  }
}

function ensureContentImagePathsJsonColumns() {
  const contentTargets = ['notices', 'news_posts', 'qc_items', 'inquiries'];

  for (const table of contentTargets) {
    const columns = db.prepare(`PRAGMA table_info(${table})`).all();
    const hasColumn = columns.some((column) => column.name === 'image_paths_json');
    if (!hasColumn) {
      db.prepare(`ALTER TABLE ${table} ADD COLUMN image_paths_json TEXT NOT NULL DEFAULT '[]'`).run();
    }
  }
}

function ensureDailyFunnelEventsTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS daily_funnel_events (
      event_date TEXT NOT NULL,
      event_key TEXT NOT NULL,
      event_count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (event_date, event_key)
    );
  `);
}

function backfillDailyFunnelEventsFromOrders() {
  db.prepare(
    `
      INSERT INTO daily_funnel_events (event_date, event_key, event_count)
      SELECT
        date(datetime(created_at, '+9 hours')) AS event_date,
        'order_created' AS event_key,
        COUNT(*) AS event_count
      FROM orders
      GROUP BY date(datetime(created_at, '+9 hours'))
      ON CONFLICT(event_date, event_key)
      DO UPDATE SET
        event_count = MAX(daily_funnel_events.event_count, excluded.event_count)
    `
  ).run();

  db.prepare(
    `
      INSERT INTO daily_funnel_events (event_date, event_key, event_count)
      SELECT
        date(datetime(COALESCE(checked_at, created_at), '+9 hours')) AS event_date,
        'payment_confirmed' AS event_key,
        COUNT(*) AS event_count
      FROM orders
      WHERE status != 'PENDING_REVIEW'
      GROUP BY date(datetime(COALESCE(checked_at, created_at), '+9 hours'))
      ON CONFLICT(event_date, event_key)
      DO UPDATE SET
        event_count = MAX(daily_funnel_events.event_count, excluded.event_count)
    `
  ).run();
}

function ensureOrderStatusLogTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS order_status_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id INTEGER NOT NULL,
      order_no TEXT NOT NULL,
      from_status TEXT,
      to_status TEXT NOT NULL,
      event_note TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
    );
  `);

  db.prepare(
    `
      CREATE INDEX IF NOT EXISTS idx_order_status_logs_order_created
      ON order_status_logs (order_id, id DESC)
    `
  ).run();
}

function ensureAdminSecurityTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS admin_activity_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      admin_user_id INTEGER,
      admin_username TEXT NOT NULL,
      admin_role TEXT NOT NULL DEFAULT '',
      ip_address TEXT NOT NULL DEFAULT '',
      user_agent TEXT NOT NULL DEFAULT '',
      method TEXT NOT NULL DEFAULT '',
      path TEXT NOT NULL DEFAULT '',
      action_type TEXT NOT NULL DEFAULT '',
      detail TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (admin_user_id) REFERENCES users(id) ON DELETE SET NULL
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS admin_security_alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      actor_admin_user_id INTEGER,
      actor_username TEXT NOT NULL,
      actor_role TEXT NOT NULL DEFAULT '',
      ip_address TEXT NOT NULL DEFAULT '',
      method TEXT NOT NULL DEFAULT '',
      path TEXT NOT NULL DEFAULT '',
      reason TEXT NOT NULL DEFAULT '',
      detail TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      resolved_at TEXT,
      FOREIGN KEY (actor_admin_user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS ip_geolocation_cache (
      ip_address TEXT PRIMARY KEY,
      country TEXT NOT NULL DEFAULT '',
      region TEXT NOT NULL DEFAULT '',
      city TEXT NOT NULL DEFAULT '',
      district TEXT NOT NULL DEFAULT '',
      postal_code TEXT NOT NULL DEFAULT '',
      latitude REAL,
      longitude REAL,
      location_display TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL DEFAULT '',
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  db.prepare(
    `
      CREATE INDEX IF NOT EXISTS idx_admin_activity_logs_created
      ON admin_activity_logs (created_at DESC, id DESC)
    `
  ).run();

  db.prepare(
    `
      CREATE INDEX IF NOT EXISTS idx_admin_security_alerts_created
      ON admin_security_alerts (created_at DESC, id DESC)
    `
  ).run();

  db.prepare(
    `
      CREATE INDEX IF NOT EXISTS idx_ip_geolocation_cache_updated
      ON ip_geolocation_cache (updated_at DESC)
    `
  ).run();
}

function normalizeOrderStatuses() {
  if (!shouldRunStartupDataMaintenance) {
    return;
  }
  db.prepare(
    `
      UPDATE orders
      SET status = 'PENDING_REVIEW'
      WHERE status IN ('PENDING_REVIEW', 'UNPAID', 'PENDING_TRANSFER', 'UNCHECKED')
    `
  ).run();
  db.prepare(
    `
      UPDATE orders
      SET status = 'ORDER_CONFIRMED'
      WHERE status IN ('ORDER_CONFIRMED', 'PAID_PREPARING', 'TRANSFER_CONFIRMED', 'PREPARING')
    `
  ).run();
  db.prepare(
    `
      UPDATE orders
      SET status = 'READY_TO_SHIP'
      WHERE status IN ('READY_TO_SHIP', 'PACKING', 'PRE_SHIPPING')
    `
  ).run();
  db.prepare("UPDATE orders SET status = 'SHIPPING' WHERE status IN ('SHIPPED', 'SHIPPING')").run();
  db.prepare("UPDATE orders SET status = 'DELIVERED' WHERE status IN ('DONE', 'DELIVERED')").run();
}

function ensureAdminUser() {
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM users WHERE is_admin = 1').get();
  if (countRow.count > 0) {
    return;
  }

  const bootstrapPassword = String(process.env.BOOTSTRAP_ADMIN_PASSWORD || '').trim();
  const bootstrapUsername = String(process.env.BOOTSTRAP_ADMIN_USERNAME || 'admin')
    .trim()
    .toLowerCase();
  const bootstrapEmail = String(process.env.BOOTSTRAP_ADMIN_EMAIL || 'admin@chronolab.local')
    .trim()
    .toLowerCase();

  const usernameValid = /^[a-z0-9]{4,20}$/.test(bootstrapUsername);
  const emailValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(bootstrapEmail);

  if (!bootstrapPassword) {
    console.warn('[chronolab:bootstrap] no admin account exists and BOOTSTRAP_ADMIN_PASSWORD is not set.');
    return;
  }
  if (bootstrapPassword.length < 12 || !/[a-z]/.test(bootstrapPassword) || !/[A-Z]/.test(bootstrapPassword) || !/[0-9]/.test(bootstrapPassword) || !/[^A-Za-z0-9]/.test(bootstrapPassword)) {
    console.warn('[chronolab:bootstrap] BOOTSTRAP_ADMIN_PASSWORD does not meet minimum complexity requirements.');
    return;
  }
  if (!usernameValid || !emailValid) {
    console.warn('[chronolab:bootstrap] invalid BOOTSTRAP_ADMIN_USERNAME or BOOTSTRAP_ADMIN_EMAIL.');
    return;
  }

  const passwordHash = bcrypt.hashSync(bootstrapPassword, 10);
  db.prepare(
    `
      INSERT INTO users (email, username, full_name, phone, password_hash, agreed_terms, is_admin, admin_role)
      VALUES (?, ?, ?, '', ?, 1, 1, 'PRIMARY')
    `
  ).run(bootstrapEmail, bootstrapUsername, 'Main Admin', passwordHash);
}

function resetStagingMainAdminPasswordOnce() {
  if (!isRenderStaging) {
    return;
  }

  const markerKey = 'stagingMainAdminCredentialResetV20260414V6';
  if (String(getSetting(markerKey, '0') || '0') === '1') {
    return;
  }

  const targetUsername = 'admin';
  const nextPassword = 'Admin!234';
  const adminNamedUser = db
    .prepare(
      `
        SELECT id, email, is_admin
        FROM users
        WHERE lower(username) = ?
        LIMIT 1
      `
    )
    .get(targetUsername);

  let targetAdminId = 0;
  if (adminNamedUser && Number(adminNamedUser.is_admin || 0) === 1) {
    targetAdminId = Number(adminNamedUser.id || 0);
  } else if (adminNamedUser) {
    // If "admin" exists as a member account, promote it to admin for staging recovery.
    targetAdminId = Number(adminNamedUser.id || 0);
    db.prepare(
      `
        UPDATE users
        SET is_admin = 1, admin_role = 'PRIMARY'
        WHERE id = ?
      `
    ).run(targetAdminId);
  } else {
    const primaryOrFirstAdmin = db
      .prepare(
        `
          SELECT id
          FROM users
          WHERE is_admin = 1
          ORDER BY
            CASE
              WHEN admin_role = 'PRIMARY' THEN 0
              ELSE 1
            END,
            id ASC
          LIMIT 1
        `
      )
      .get();
    targetAdminId = Number(primaryOrFirstAdmin?.id || 0);
    if (targetAdminId > 0) {
      db.prepare('UPDATE users SET username = ? WHERE id = ?').run(targetUsername, targetAdminId);
    } else {
      const fallbackEmail = `admin+staging-${Date.now()}@chronolab.local`;
      const initialHash = bcrypt.hashSync(nextPassword, 10);
      const inserted = db.prepare(
        `
          INSERT INTO users (email, username, full_name, phone, password_hash, agreed_terms, is_admin, admin_role)
          VALUES (?, ?, ?, '', ?, 1, 1, 'PRIMARY')
        `
      ).run(fallbackEmail, targetUsername, 'Staging Admin', initialHash);
      targetAdminId = Number(inserted.lastInsertRowid || 0);
    }
  }

  const targetAdmin = db
    .prepare(
      `
        SELECT id, is_admin
        FROM users
        WHERE id = ?
        LIMIT 1
      `
    )
    .get(targetAdminId);
  if (!targetAdmin) {
    return;
  }

  const passwordHash = bcrypt.hashSync(nextPassword, 10);
  db.prepare(
    `
      UPDATE users
      SET
        password_hash = ?,
        is_blocked = 0,
        blocked_reason = '',
        admin_otp_secret = '',
        admin_otp_enabled = 0,
        admin_otp_enabled_at = NULL
      WHERE id = ?
    `
  ).run(passwordHash, targetAdmin.id);
  setSetting(markerKey, '1');
}

function normalizeAdminRoles() {
  if (!shouldRunStartupDataMaintenance) {
    return;
  }
  db.prepare("UPDATE users SET admin_role = '' WHERE is_admin = 0 AND admin_role != ''").run();

  const admins = db
    .prepare(
      `
        SELECT id, username
        FROM users
        WHERE is_admin = 1
        ORDER BY id ASC
      `
    )
    .all();

  if (admins.length === 0) {
    return;
  }

  const preferredPrimary =
    admins.find((admin) => String(admin.username || '').toLowerCase() === 'admin') || admins[0];

  db.prepare("UPDATE users SET admin_role = 'SUB' WHERE is_admin = 1").run();
  db.prepare("UPDATE users SET admin_role = 'PRIMARY' WHERE id = ?").run(preferredPrimary.id);
  db.prepare("UPDATE users SET full_name = username WHERE is_admin = 1 AND COALESCE(full_name, '') = ''").run();
}

function ensureDemoMemberUser() {
  const existing = db.prepare('SELECT id FROM users WHERE username = ? LIMIT 1').get('demo_member');
  if (existing) {
    return Number(existing.id);
  }

  const passwordHash = bcrypt.hashSync('Demo1234', 10);
  const inserted = db
    .prepare(
      `
        INSERT INTO users (email, username, password_hash, agreed_terms, is_admin)
        VALUES (?, ?, ?, 1, 0)
      `
    )
    .run('demo@chronolab.local', 'demo_member', passwordHash);

  return Number(inserted.lastInsertRowid);
}

function seedProducts() {
  const insertProduct = db.prepare(
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
        image_path
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  );

  const updateProductById = db.prepare(
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
        is_active = 1
      WHERE id = ?
    `
  );

  const findByReference = db.prepare('SELECT id FROM products WHERE reference = ? LIMIT 1');
  const clearProductImages = db.prepare('DELETE FROM product_images WHERE product_id = ?');
  const insertProductImage = db.prepare(
    `
      INSERT OR IGNORE INTO product_images (product_id, image_path, sort_order)
      VALUES (?, ?, ?)
    `
  );

  const tx = db.transaction(() => {
    for (const item of PRODUCT_SEED_ITEMS) {
      const images = Array.isArray(item.images) ? [...new Set(item.images.filter(Boolean))] : [];
      const primaryImage = images[0] || '';

      const values = [
        item.categoryGroup,
        item.brand,
        item.model,
        item.subModel,
        item.reference,
        item.factoryName,
        item.versionName,
        item.movement,
        item.caseSize,
        item.dialColor,
        item.caseMaterial,
        item.strapMaterial,
        item.features,
        Number(item.price || 0),
        item.shippingPeriod,
        primaryImage
      ];

      const exists = findByReference.get(item.reference);
      let productId = null;

      if (!exists) {
        const inserted = insertProduct.run(...values);
        productId = Number(inserted.lastInsertRowid);
      } else {
        productId = Number(exists.id);
        updateProductById.run(...values, productId);
      }

      clearProductImages.run(productId);
      images.forEach((src, index) => {
        insertProductImage.run(productId, src, index);
      });
    }

    const oldSampleRows = db
      .prepare(
        `
          SELECT id
          FROM products
          WHERE image_path LIKE '/assets/media/watches/%'
        `
      )
      .all();

    for (const row of oldSampleRows) {
      const hasLinked = db
        .prepare('SELECT COUNT(*) AS count FROM product_images WHERE product_id = ?')
        .get(row.id);

      if (Number(hasLinked.count) === 0) {
        db.prepare('UPDATE products SET is_active = 0 WHERE id = ?').run(row.id);
      }
    }
  });

  tx();
}

function seedNotices() {
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM notices').get();
  if (countRow.count > 0) {
    return;
  }

  const insert = db.prepare(
    `
      INSERT INTO notices (title, content, image_path, is_popup)
      VALUES (?, ?, ?, ?)
    `
  );

  const rows = [
    [
      '[중요] 3월 주문/검수 일정 안내',
      '3월 말 물류량 증가로 일부 공장제 상품의 QC 및 출고 일정이 2~3일 지연될 수 있습니다. 급한 일정은 문의 게시판으로 접수해 주세요.',
      '/assets/media/notice/notice-schedule.svg',
      1
    ],
    [
      '계좌이체 확인 절차 안내',
      '입금 시 구매번호와 입금자명을 정확히 입력해 주세요. 어드민에서 구매번호와 신청자명 기준으로 확인 후 상태를 업데이트합니다.',
      '/assets/media/notice/notice-transfer.svg',
      0
    ],
    [
      '회원 후기 정책 업데이트',
      '구매후기 게시판은 로그인 회원만 작성 가능하며, 이미지 업로드를 권장합니다. 광고성 글은 운영정책에 따라 삭제될 수 있습니다.',
      '/assets/media/notice/notice-review.svg',
      0
    ]
  ];

  const tx = db.transaction(() => {
    for (const row of rows) {
      insert.run(...row);
    }
  });
  tx();
}

function seedNews() {
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM news_posts').get();
  if (countRow.count > 0) {
    return;
  }

  const insert = db.prepare(
    `
      INSERT INTO news_posts (title, content, image_path)
      VALUES (?, ?, ?)
    `
  );

  const rows = [
    [
      'Chrono Lab 주간 셀렉션: Submariner 라인업 비교',
      '이번 주는 Submariner Black, Starbucks, Vintage 톤 컬렉션을 비교했습니다. 무브먼트 안정성과 케이스 마감 완성도를 중심으로 선별했습니다. 상세 스펙은 쇼핑몰 탭에서 확인 가능합니다.',
      '/assets/media/news/news-submariner.svg'
    ],
    [
      '젠파츠 라인 신규 입고 일정',
      '젠파츠 라인은 소량 커스텀 기반이라 입고 주기가 유동적입니다. 4월 1주차에는 Datejust/Seamaster 라인이 우선 입고될 예정입니다.',
      '/assets/media/news/news-genparts.svg'
    ],
    [
      '현지중고 카테고리 오픈',
      '현지중고 카테고리를 새롭게 추가했습니다. 즉시 출고 가능한 재고 중심으로 구성했으며, 상태 등급과 보유 스펙을 투명하게 공개합니다.',
      '/assets/media/news/news-used.svg'
    ],
    [
      'QC 프로세스 개선 공지',
      'QC 페이지에서 구매번호 검색 시 업로드된 이미지를 더 빠르게 확인할 수 있도록 구조를 개선했습니다. 검수 이미지와 코멘트를 동시에 확인할 수 있습니다.',
      '/assets/media/news/news-qc.svg'
    ]
  ];

  const tx = db.transaction(() => {
    for (const row of rows) {
      insert.run(...row);
    }
  });
  tx();
}

function seedOrdersAndQc(demoUserId) {
  const orderCount = db.prepare('SELECT COUNT(*) AS count FROM orders').get();
  if (orderCount.count === 0) {
    const products = db
      .prepare(
        `
          SELECT id, price, brand, model
          FROM products
          ORDER BY id ASC
          LIMIT 2
        `
      )
      .all();

    if (products.length > 0) {
      const insertOrder = db.prepare(
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
      );

      products.forEach((product, idx) => {
        const orderNo = `CL-DEMO-000${idx + 1}`;
        insertOrder.run(
          orderNo,
          product.id,
          idx === 0 ? '김크로노' : '이랩',
          idx === 0 ? '01012345678' : '01087654321',
          idx === 0 ? '서울특별시 강남구 테헤란로 101' : '부산광역시 해운대구 센텀동로 45',
          idx === 0 ? 'P220312345678' : 'P220312345679',
          idx === 0 ? '김크로노' : '이랩',
          1,
          Number(product.price),
          idx === 0 ? 'ORDER_CONFIRMED' : 'SHIPPING',
          demoUserId
        );
      });
    }
  }

  const qcCount = db.prepare('SELECT COUNT(*) AS count FROM qc_items').get();
  if (qcCount.count > 0) {
    return;
  }

  const orders = db.prepare('SELECT order_no FROM orders ORDER BY id ASC LIMIT 2').all();
  if (orders.length === 0) {
    return;
  }

  const insertQc = db.prepare(
    `
      INSERT INTO qc_items (order_no, image_path, note)
      VALUES (?, ?, ?)
    `
  );

  orders.forEach((order, idx) => {
    insertQc.run(
      order.order_no,
      idx === 0 ? '/assets/media/qc/qc-demo-1.svg' : '/assets/media/qc/qc-demo-2.svg',
      idx === 0 ? '케이스/브레이슬릿 유격 점검 완료' : '다이얼/핸즈 정렬 점검 완료'
    );
  });
}

function seedReviews(demoUserId) {
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM reviews').get();
  if (countRow.count > 0) {
    return;
  }

  const products = db.prepare('SELECT id FROM products ORDER BY id ASC LIMIT 3').all();
  if (products.length === 0) {
    return;
  }

  const insert = db.prepare(
    `
      INSERT INTO reviews (user_id, product_id, title, content, image_path)
      VALUES (?, ?, ?, ?, ?)
    `
  );

  const rows = [
    [
      demoUserId,
      products[0]?.id || null,
      '배송/패키징 만족합니다',
      '주문 후 응대가 빨랐고 포장 상태도 깔끔했습니다. 실물 마감도 사진과 큰 차이 없어서 만족합니다.',
      '/assets/media/review/review-1.svg'
    ],
    [
      demoUserId,
      products[1]?.id || null,
      'QC 사진 확인 후 안심 구매',
      'QC에서 요청한 부분을 자세히 찍어줘서 안심하고 입금했습니다. 다음 구매도 동일하게 진행할 예정입니다.',
      '/assets/media/review/review-2.svg'
    ],
    [
      demoUserId,
      products[2]?.id || null,
      '데일리로 쓰기 좋습니다',
      '착용감이 좋아서 데일리로 계속 사용 중입니다. 스트랩 피팅도 안정적이었습니다.',
      '/assets/media/review/review-3.svg'
    ]
  ];

  const tx = db.transaction(() => {
    for (const row of rows) {
      insert.run(...row);
    }
  });
  tx();
}

function seedInquiries(demoUserId) {
  const countRow = db.prepare('SELECT COUNT(*) AS count FROM inquiries').get();
  if (countRow.count > 0) {
    return;
  }

  const insert = db.prepare(
    `
      INSERT INTO inquiries (user_id, title, content, image_path, reply_content, replied_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `
  );

  const now = "datetime('now')";

  insert.run(
    demoUserId,
    'Submariner 재고 문의',
    '공장제 Submariner Starbucks 현재 재고가 있는지, 입금 후 평균 배송일이 어느 정도인지 문의드립니다.',
    '/assets/media/news/news-submariner.svg',
    '현재 재고 보유 중이며 입금 확인 후 평균 7~14일 내 출고됩니다.',
    db.prepare(`SELECT ${now} AS now`).get().now
  );

  insert.run(
    demoUserId,
    '현지중고 라인 상태 등급 문의',
    '현지중고 카테고리 상품 상태 기준(A/B/C) 상세 기준표가 있을까요?',
    '',
    null,
    null
  );
}

export function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL UNIQUE,
      username TEXT NOT NULL UNIQUE,
      nickname TEXT NOT NULL DEFAULT '',
      full_name TEXT NOT NULL DEFAULT '',
      phone TEXT NOT NULL DEFAULT '',
      customs_clearance_no TEXT NOT NULL DEFAULT '',
      default_address TEXT NOT NULL DEFAULT '',
      default_postcode TEXT NOT NULL DEFAULT '',
      default_address_base TEXT NOT NULL DEFAULT '',
      default_address_detail TEXT NOT NULL DEFAULT '',
      profile_image_path TEXT NOT NULL DEFAULT '',
      reward_points INTEGER NOT NULL DEFAULT 0,
      password_hash TEXT NOT NULL,
      agreed_terms INTEGER NOT NULL DEFAULT 1,
      is_admin INTEGER NOT NULL DEFAULT 0,
      admin_role TEXT NOT NULL DEFAULT '',
      admin_otp_secret TEXT NOT NULL DEFAULT '',
      admin_otp_enabled INTEGER NOT NULL DEFAULT 0,
      admin_otp_enabled_at TEXT,
      is_blocked INTEGER NOT NULL DEFAULT 0,
      blocked_reason TEXT NOT NULL DEFAULT '',
      blocked_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      category_group TEXT NOT NULL DEFAULT '공장제',
      brand TEXT NOT NULL,
      model TEXT NOT NULL,
      sub_model TEXT,
      reference TEXT,
      factory_name TEXT,
      version_name TEXT,
      movement TEXT,
      case_size TEXT,
      dial_color TEXT,
      case_material TEXT,
      strap_material TEXT,
      features TEXT,
      price INTEGER NOT NULL,
      shipping_period TEXT,
      image_path TEXT,
      extra_fields_json TEXT NOT NULL DEFAULT '{}',
      is_sold_out INTEGER NOT NULL DEFAULT 0,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS product_images (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      image_path TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_product_images_unique
      ON product_images (product_id, image_path);

    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_no TEXT NOT NULL UNIQUE,
      product_id INTEGER NOT NULL,
      buyer_name TEXT NOT NULL,
      buyer_contact TEXT NOT NULL,
      buyer_address TEXT NOT NULL,
      customs_clearance_no TEXT NOT NULL DEFAULT '',
      bank_depositor_name TEXT NOT NULL,
      quantity INTEGER NOT NULL DEFAULT 1,
      total_price INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
      tracking_carrier TEXT NOT NULL DEFAULT 'kr.cjlogistics',
      tracking_number TEXT NOT NULL DEFAULT '',
      tracking_last_event TEXT NOT NULL DEFAULT '',
      tracking_last_checked_at TEXT,
      checked_at TEXT,
      ready_to_ship_at TEXT,
      shipping_started_at TEXT,
      delivered_at TEXT,
      awarded_points INTEGER NOT NULL DEFAULT 0,
      points_awarded_at TEXT,
      sales_tab_key TEXT NOT NULL DEFAULT '',
      sales_scope_id TEXT NOT NULL DEFAULT '',
      sales_scope_name TEXT NOT NULL DEFAULT '',
      sales_scope_date TEXT NOT NULL DEFAULT '',
      sales_exchange_rate_snapshot REAL NOT NULL DEFAULT 0,
      sales_shipping_fee_krw_snapshot INTEGER NOT NULL DEFAULT 0,
      sales_cost_rmb_snapshot REAL NOT NULL DEFAULT 0,
      sales_cost_krw_snapshot INTEGER NOT NULL DEFAULT 0,
      sales_margin_krw_snapshot INTEGER NOT NULL DEFAULT 0,
      sales_real_margin_krw_snapshot INTEGER NOT NULL DEFAULT 0,
      sales_synced_at TEXT,
      created_by_user_id INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS cart_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      UNIQUE (user_id, product_id)
    );

    CREATE TABLE IF NOT EXISTS address_book (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      label TEXT NOT NULL,
      postcode TEXT NOT NULL DEFAULT '',
      address_base TEXT NOT NULL DEFAULT '',
      address_detail TEXT NOT NULL DEFAULT '',
      is_default INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_address_book_user_id ON address_book (user_id);
    CREATE INDEX IF NOT EXISTS idx_address_book_user_default ON address_book (user_id, is_default);

    CREATE TABLE IF NOT EXISTS notices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      image_paths_json TEXT NOT NULL DEFAULT '[]',
      is_popup INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS news_posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      image_paths_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS qc_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_no TEXT NOT NULL,
      image_path TEXT NOT NULL,
      image_paths_json TEXT NOT NULL DEFAULT '[]',
      note TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      order_id INTEGER,
      product_id INTEGER,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      reward_points_awarded INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS inquiries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      image_paths_json TEXT NOT NULL DEFAULT '[]',
      reply_content TEXT,
      replied_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS site_settings (
      setting_key TEXT PRIMARY KEY,
      setting_value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS metrics (
      metric_key TEXT PRIMARY KEY,
      metric_value INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS daily_visits (
      visit_date TEXT PRIMARY KEY,
      visit_count INTEGER NOT NULL DEFAULT 0,
      member_visit_count INTEGER NOT NULL DEFAULT 0,
      guest_visit_count INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS user_sessions (
      sid TEXT PRIMARY KEY,
      sess TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at
      ON user_sessions (expires_at);
  `);

  ensureProductsCategoryColumn();
  ensureProductsExtraFieldsColumn();
  ensureProductsSoldOutColumn();
  ensureProductBadgeTables();
  ensureUserAdminProfileColumns();
  ensureAdminOtpColumns();
  ensureUserMemberProfileColumns();
  ensureUserMemberUidColumn();
  ensureAddressBookTable();
  ensureUserPointColumns();
  ensureUserBlockColumns();
  ensureOrdersCustomsColumn();
  ensureOrdersTrackingColumns();
  ensureOrdersPointColumns();
  ensureOrdersSalesSnapshotColumns();
  ensureReviewsOrderColumn();
  ensureDailyVisitSplitColumns();
  ensureContentVisibilityColumns();
  ensureContentImagePathsJsonColumns();
  ensureDailyFunnelEventsTable();
  ensureAdminSecurityTables();
  normalizeOrderStatuses();

  for (const [key, value] of Object.entries(defaultSettings)) {
    upsertDefaultSetting(key, value);
  }
  ensureSignupBonusBaseline();
  applyMemberLevelThemeBaselineV20260415();
  if (shouldRunStartupDataMaintenance) {
    seedDefaultProductBadgesOnce();
  }

  migrateLegacyThemeAssetSettings();

  if (shouldRunStartupDataMaintenance) {
    const menuRow = db
      .prepare('SELECT setting_value FROM site_settings WHERE setting_key = ? LIMIT 1')
      .get('menus');
    const normalizedMenus = normalizeMenus(menuRow?.setting_value || JSON.stringify(defaultMenus));
    setSetting('menus', JSON.stringify(normalizedMenus));
  }

  upsertMetric('totalVisits', 0);

  ensureAdminUser();
  resetStagingMainAdminPasswordOnce();
  normalizeAdminRoles();
  if (shouldBootstrapSeedData) {
    const demoUserId = ensureDemoMemberUser();
    ensureUserMemberUidColumn();

    seedProducts();
    seedNotices();
    seedNews();
    seedOrdersAndQc(demoUserId);
    seedReviews(demoUserId);
    seedInquiries(demoUserId);
    backfillDailyFunnelEventsFromOrders();
  }

  ensureOrderStatusLogTable();
  if (shouldRunStartupDataMaintenance) {
    db.prepare(
      `
        INSERT INTO order_status_logs (order_id, order_no, from_status, to_status, event_note, created_at)
        SELECT
          o.id,
          o.order_no,
          NULL,
          o.status,
          'bootstrap',
          o.created_at
        FROM orders o
        WHERE NOT EXISTS (
          SELECT 1
          FROM order_status_logs l
          WHERE l.order_id = o.id
        )
      `
    ).run();
  }
}

export function getSetting(key, fallback = '') {
  const row = db
    .prepare('SELECT setting_value FROM site_settings WHERE setting_key = ? LIMIT 1')
    .get(key);
  if (!row) {
    return fallback;
  }
  return row.setting_value;
}

export function setSetting(key, value) {
  db.prepare(
    `
      INSERT INTO site_settings (setting_key, setting_value)
      VALUES (?, ?)
      ON CONFLICT(setting_key)
      DO UPDATE SET setting_value = excluded.setting_value
    `
  ).run(key, String(value));
}

export function incrementVisit(visitDate, isMember = false) {
  const memberCount = isMember ? 1 : 0;
  const guestCount = isMember ? 0 : 1;
  db.prepare(
    `
      INSERT INTO daily_visits (visit_date, visit_count, member_visit_count, guest_visit_count)
      VALUES (?, 1, ?, ?)
      ON CONFLICT(visit_date)
      DO UPDATE SET
        visit_count = daily_visits.visit_count + 1,
        member_visit_count = daily_visits.member_visit_count + excluded.member_visit_count,
        guest_visit_count = daily_visits.guest_visit_count + excluded.guest_visit_count
    `
  ).run(visitDate, memberCount, guestCount);

  db.prepare('UPDATE metrics SET metric_value = metric_value + 1 WHERE metric_key = ?').run('totalVisits');
}

export function incrementFunnelEvent(eventDate, eventKey, count = 1) {
  const key = String(eventKey || '').trim().slice(0, 60);
  const amount = Number(count);
  if (!key || !Number.isFinite(amount) || amount <= 0) {
    return;
  }

  db.prepare(
    `
      INSERT INTO daily_funnel_events (event_date, event_key, event_count)
      VALUES (?, ?, ?)
      ON CONFLICT(event_date, event_key)
      DO UPDATE SET event_count = daily_funnel_events.event_count + excluded.event_count
    `
  ).run(eventDate, key, Math.floor(amount));
}

export function getVisitCounts(visitDate) {
  const todayRow = db.prepare('SELECT visit_count FROM daily_visits WHERE visit_date = ?').get(visitDate);
  const totalRow = db
    .prepare('SELECT metric_value FROM metrics WHERE metric_key = ? LIMIT 1')
    .get('totalVisits');

  return {
    today: todayRow ? Number(todayRow.visit_count) : 0,
    total: totalRow ? Number(totalRow.metric_value) : 0
  };
}

export function getPostCounts(visitDate) {
  const todayPosts = db
    .prepare(
      `
        SELECT (
          (SELECT COUNT(*) FROM notices WHERE date(created_at, '+9 hours') = ?) +
          (SELECT COUNT(*) FROM news_posts WHERE date(created_at, '+9 hours') = ?) +
          (SELECT COUNT(*) FROM reviews WHERE date(created_at, '+9 hours') = ?) +
          (SELECT COUNT(*) FROM inquiries WHERE date(created_at, '+9 hours') = ?) +
          (SELECT COUNT(*) FROM qc_items WHERE date(created_at, '+9 hours') = ?)
        ) AS count
      `
    )
    .get(visitDate, visitDate, visitDate, visitDate, visitDate);

  const totalPosts = db
    .prepare(
      `
        SELECT (
          (SELECT COUNT(*) FROM notices) +
          (SELECT COUNT(*) FROM news_posts) +
          (SELECT COUNT(*) FROM reviews) +
          (SELECT COUNT(*) FROM inquiries) +
          (SELECT COUNT(*) FROM qc_items)
        ) AS count
      `
    )
    .get();

  return {
    today: Number(todayPosts.count || 0),
    total: Number(totalPosts.count || 0)
  };
}

export function getDefaultMenus() {
  return defaultMenus;
}

export function getDefaultProductGroupConfigs() {
  return JSON.parse(JSON.stringify(DEFAULT_PRODUCT_GROUP_CONFIGS));
}
