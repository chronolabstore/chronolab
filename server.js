import path from 'path';
import { fileURLToPath } from 'url';
import express from 'express';
import session from 'express-session';
import cookieParser from 'cookie-parser';
import multer from 'multer';
import bcrypt from 'bcryptjs';
import {
  db,
  initDb,
  getDefaultMenus,
  getPostCounts,
  getSetting,
  getVisitCounts,
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

const ORDER_STATUS = Object.freeze({
  UNPAID: 'UNPAID',
  PAID_PREPARING: 'PAID_PREPARING',
  SHIPPING: 'SHIPPING',
  DELIVERED: 'DELIVERED'
});

const ADMIN_MENUS = Object.freeze([
  { id: 'admin-security', labelKo: '보안', labelEn: 'Security', path: '/admin/security' },
  { id: 'admin-site', labelKo: '사이트설정', labelEn: 'Site', path: '/admin/site' },
  { id: 'admin-menus', labelKo: '메뉴관리', labelEn: 'Menus', path: '/admin/menus' },
  { id: 'admin-products', labelKo: '상품관리', labelEn: 'Products', path: '/admin/products' },
  { id: 'admin-notices', labelKo: '공지사항', labelEn: 'Notices', path: '/admin/notices' },
  { id: 'admin-news', labelKo: '뉴스', labelEn: 'News', path: '/admin/news' },
  { id: 'admin-qc', labelKo: 'QC', labelEn: 'QC', path: '/admin/qc' },
  { id: 'admin-orders', labelKo: '주문관리', labelEn: 'Orders', path: '/admin/orders' },
  { id: 'admin-inquiries', labelKo: '문의답변', labelEn: 'Inquiries', path: '/admin/inquiries' }
]);

const AUTH_ATTEMPT_WINDOW_MS = 10 * 60 * 1000;
const DEFAULT_AUTH_MAX_ATTEMPTS = 15;
const authAttemptStore = new Map();

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

function parseMenus(rawMenus) {
  try {
    const parsed = JSON.parse(rawMenus);
    if (!Array.isArray(parsed)) {
      return getDefaultMenus();
    }

    const menus = parsed
      .filter((menu) => menu && menu.path)
      .map((menu, idx) => ({
        id: String(menu.id || `menu-${idx + 1}`),
        labelKo: String(menu.labelKo || menu.labelEn || `메뉴${idx + 1}`),
        labelEn: String(menu.labelEn || menu.labelKo || `Menu${idx + 1}`),
        path: sanitizePath(String(menu.path || ''))
      }))
      .filter((menu) => !menu.path.startsWith('/admin'));

    return menus.length > 0 ? menus : getDefaultMenus();
  } catch {
    return getDefaultMenus();
  }
}

function getAdminMenus() {
  return ADMIN_MENUS.map((menu) => ({ ...menu }));
}

function setFlash(req, type, message) {
  req.session.flash = { type, message };
}

function getFlash(req) {
  const flash = req.session.flash || null;
  delete req.session.flash;
  return flash;
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

  if (status === 'UNPAID' || status === 'PENDING_TRANSFER') return ORDER_STATUS.UNPAID;
  if (status === 'PAID_PREPARING' || status === 'TRANSFER_CONFIRMED' || status === 'PREPARING') {
    return ORDER_STATUS.PAID_PREPARING;
  }
  if (status === 'SHIPPING' || status === 'SHIPPED') return ORDER_STATUS.SHIPPING;
  if (status === 'DELIVERED' || status === 'DONE') return ORDER_STATUS.DELIVERED;

  return ORDER_STATUS.UNPAID;
}

function getOrderStatusMeta(rawStatus, lang = 'ko') {
  const status = normalizeOrderStatus(rawStatus);
  const isEn = lang === 'en';

  if (status === ORDER_STATUS.UNPAID) {
    return {
      code: status,
      label: isEn ? 'Unpaid' : '미입금',
      detail: ''
    };
  }

  if (status === ORDER_STATUS.PAID_PREPARING) {
    return {
      code: status,
      label: isEn ? 'Payment Confirmed' : '입금확인',
      detail: isEn ? 'Preparing item' : '상품준비중'
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

  if (current === ORDER_STATUS.UNPAID) return ORDER_STATUS.PAID_PREPARING;
  if (current === ORDER_STATUS.PAID_PREPARING) return ORDER_STATUS.SHIPPING;
  if (current === ORDER_STATUS.SHIPPING) return ORDER_STATUS.DELIVERED;
  return null;
}

function getNextOrderActionLabel(rawStatus) {
  const current = normalizeOrderStatus(rawStatus);
  if (current === ORDER_STATUS.UNPAID) return '입금확인';
  if (current === ORDER_STATUS.PAID_PREPARING) return '배송시작';
  if (current === ORDER_STATUS.SHIPPING) return '배송완료';
  return '';
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

function normalizePhone(rawPhone = '') {
  return String(rawPhone).replace(/[^0-9]/g, '');
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
    .prepare('SELECT id, email, username, is_admin, created_at FROM users WHERE id = ? LIMIT 1')
    .get(req.session.userId);

  if (!user) {
    req.session.userId = null;
    req.session.isAdmin = false;
    req.user = null;
    return next();
  }

  req.user = {
    id: Number(user.id),
    email: user.email,
    username: user.username,
    isAdmin: Number(user.is_admin) === 1,
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
      incrementVisit(today);
      req.session.lastVisitDate = today;
    }
  }

  const publicMenus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())));
  const isAdminPage = req.path.startsWith('/admin') && req.path !== '/admin/login';
  const menus = isAdminPage && Boolean(req.user?.isAdmin) ? getAdminMenus() : publicMenus;

  const headerColor = getSetting('headerColor', '#111827');
  const backgroundType = getSetting('backgroundType', 'color');
  const backgroundValue = getSetting('backgroundValue', '#f7f7f8');

  let backgroundStyle = `background: ${backgroundValue};`;
  if (backgroundType === 'image' && backgroundValue) {
    backgroundStyle = `background-image: url('${backgroundValue}'); background-size: cover; background-position: center;`;
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
    isAdminPage: isAdminPage && Boolean(req.user?.isAdmin),
    flash: getFlash(req),
    formatPrice,
    menus,
    settings: {
      siteName: getSetting('siteName', 'Chrono Lab'),
      headerColor,
      headerLogoPath: getSetting('headerLogoPath', ''),
      headerSymbolPath: getSetting('headerSymbolPath', ''),
      footerLogoPath: getSetting('footerLogoPath', ''),
      backgroundType,
      backgroundValue,
      backgroundStyle,
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

function requireAuth(req, res, next) {
  if (!req.user) {
    setFlash(req, 'error', '로그인이 필요합니다.');
    return res.redirect('/login');
  }
  return next();
}

function requireAdmin(req, res, next) {
  if (!req.user || !req.user.isAdmin) {
    return res.status(404).render('simple-error', {
      title: 'Not Found',
      message: '페이지를 찾을 수 없습니다.'
    });
  }
  return next();
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
  const groupedProducts = SHOP_PRODUCT_GROUPS.map((groupName) => ({
    groupName,
    products: db
      .prepare(
        `
          SELECT id, brand, model, sub_model, price, image_path, shipping_period
          FROM products
          WHERE is_active = 1 AND category_group = ?
          ORDER BY id DESC
          LIMIT 4
        `
      )
      .all(groupName)
  }));

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

  res.render('main', { title: 'Main', groupedProducts, latestNotices, latestNews });
});

app.get('/shop', (req, res) => {
  const groupRaw = String(req.query.group || '').trim();
  const group = SHOP_PRODUCT_GROUPS.includes(groupRaw) ? groupRaw : SHOP_PRODUCT_GROUPS[0];
  const canUseBrandModelFilter = group === '공장제';
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

  const products = db
    .prepare(
      `
        SELECT id, category_group, brand, model, sub_model, price, image_path, shipping_period, case_material, movement
        FROM products
        WHERE ${where.join(' AND ')}
        ORDER BY id DESC
      `
    )
    .all(...params);

  res.render('shop', {
    title: 'Shop',
    group,
    productGroups: SHOP_PRODUCT_GROUPS,
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

  const similar = db
    .prepare(
      `
        SELECT id, brand, model, sub_model, price, image_path
        FROM products
        WHERE is_active = 1 AND brand = ? AND id != ?
        ORDER BY id DESC
        LIMIT 6
      `
    )
    .all(product.brand, product.id);

  res.render('product-detail', { title: 'Product', product, similar, imageList });
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

  if (product.category_group !== '공장제') {
    setFlash(req, 'error', '공장제 상품만 해당 구매 페이지를 이용할 수 있습니다.');
    return res.redirect(`/shop/item/${id}`);
  }

  const formData = {
    buyerName: req.user.username || '',
    buyerContact: '',
    customsClearanceNo: '',
    buyerAddress: '',
    quantity: 1
  };

  return res.render('purchase-form', { title: 'Purchase', product, formData });
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

  if (product.category_group !== '공장제') {
    setFlash(req, 'error', '공장제 상품만 해당 구매 페이지를 이용할 수 있습니다.');
    return res.redirect(`/shop/item/${id}`);
  }

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
    return res.render('purchase-form', { title: 'Purchase', product, formData });
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
  db.prepare(
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
    ORDER_STATUS.UNPAID,
    req.user.id
  );

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

  db.prepare(
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
    ORDER_STATUS.UNPAID,
    req.user ? req.user.id : null
  );

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
  const orders = db
    .prepare(
      `
        SELECT o.*, p.brand, p.model, p.sub_model
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.created_by_user_id = ?
        ORDER BY o.id DESC
      `
    )
    .all(req.user.id)
    .map((order) => {
      const statusMeta = getOrderStatusMeta(order.status, res.locals.ctx.lang);
      return {
        ...order,
        status_code: statusMeta.code,
        status_label: statusMeta.label,
        status_detail: statusMeta.detail
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
    .prepare('SELECT id, username, password_hash, is_admin FROM users WHERE username = ? LIMIT 1')
    .get(username);

  if (!user) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
  }

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/login');
  }

  req.session.userId = Number(user.id);
  req.session.isAdmin = Number(user.is_admin) === 1;
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
    return res.redirect('/admin/security');
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
    .prepare('SELECT id, password_hash, is_admin FROM users WHERE username = ? LIMIT 1')
    .get(username);

  if (!user || Number(user.is_admin) !== 1) {
    setFlash(req, 'error', '어드민 계정이 아닙니다.');
    return res.redirect('/admin/login');
  }

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    setFlash(req, 'error', '로그인 정보가 올바르지 않습니다.');
    return res.redirect('/admin/login');
  }

  req.session.userId = Number(user.id);
  req.session.isAdmin = true;
  resetAuthAttempt(req, 'admin-login');

  res.redirect('/admin/security');
  })
);

app.post('/admin/change-password', requireAdmin, asyncRoute(async (req, res) => {
  const backPath = safeBackPath(req, '/admin/security');
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

function buildAdminDashboardViewData() {
  const publicMenus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())));
  const settings = {
    siteName: getSetting('siteName', 'Chrono Lab'),
    headerColor: getSetting('headerColor', '#111827'),
    headerLogoPath: getSetting('headerLogoPath', ''),
    headerSymbolPath: getSetting('headerSymbolPath', ''),
    footerLogoPath: getSetting('footerLogoPath', ''),
    backgroundType: getSetting('backgroundType', 'color'),
    backgroundValue: getSetting('backgroundValue', '#f7f7f8'),
    bankAccountInfo: getSetting('bankAccountInfo', ''),
    contactInfo: getSetting('contactInfo', ''),
    businessInfo: getSetting('businessInfo', ''),
    languageDefault: getSetting('languageDefault', 'ko'),
    menusJson: JSON.stringify(publicMenus, null, 2)
  };

  const products = db.prepare('SELECT * FROM products ORDER BY id DESC LIMIT 100').all();
  const orders = db
    .prepare(
      `
        SELECT o.*, p.brand, p.model, p.sub_model
        FROM orders o
        JOIN products p ON p.id = o.product_id
        ORDER BY o.id DESC
        LIMIT 100
      `
    )
    .all()
    .map((order) => {
      const statusMeta = getOrderStatusMeta(order.status, 'ko');
      const nextStatus = getNextOrderStatus(order.status);
      const nextActionLabel = getNextOrderActionLabel(order.status);
      return {
        ...order,
        status_code: statusMeta.code,
        status_label: statusMeta.label,
        status_detail: statusMeta.detail,
        next_status: nextStatus,
        next_action_label: nextActionLabel
      };
    });
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

  return {
    settings,
    publicMenus,
    products,
    orders,
    notices,
    newsPosts,
    qcs,
    inquiries,
    formatPrice,
    productGroups: SHOP_PRODUCT_GROUPS
  };
}

function renderAdminDashboard(req, res, activeTab) {
  const viewData = buildAdminDashboardViewData();
  return res.render('admin-dashboard', {
    title: 'Admin Dashboard',
    activeTab,
    ...viewData
  });
}

app.get('/admin', requireAdmin, (req, res) => {
  res.redirect('/admin/security');
});

app.get('/admin/security', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'security'));
app.get('/admin/site', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'site'));
app.get('/admin/menus', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'menus'));
app.get('/admin/products', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'products'));
app.get('/admin/notices', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'notices'));
app.get('/admin/news', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'news'));
app.get('/admin/qc', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'qc'));
app.get('/admin/orders', requireAdmin, (req, res) => renderAdminDashboard(req, res, 'orders'));
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

  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())));
  const id = `menu-${Date.now()}`;
  menus.push({ id, labelKo, labelEn, path: menuPath });
  setSetting('menus', JSON.stringify(menus));

  setFlash(req, 'success', '메뉴가 추가되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/menu/remove/:id', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/menus');
  const id = String(req.params.id || '');
  const menus = parseMenus(getSetting('menus', JSON.stringify(getDefaultMenus())));
  const nextMenus = menus.filter((menu) => menu.id !== id);
  if (nextMenus.length === 0) {
    setFlash(req, 'error', '최소 1개 이상의 메뉴는 유지되어야 합니다.');
    return res.redirect(backPath);
  }
  setSetting('menus', JSON.stringify(nextMenus));
  setFlash(req, 'success', '메뉴가 삭제되었습니다.');
  res.redirect(backPath);
});

app.post(
  '/admin/settings',
  requireAdmin,
  upload.fields([
    { name: 'headerLogo', maxCount: 1 },
    { name: 'headerSymbol', maxCount: 1 },
    { name: 'footerLogo', maxCount: 1 },
    { name: 'backgroundImage', maxCount: 1 }
  ]),
  (req, res) => {
    const backPath = safeBackPath(req, '/admin/site');
    const siteName = String(req.body.siteName || 'Chrono Lab').trim();
    const headerColor = String(req.body.headerColor || '#111827').trim();
    const backgroundType = String(req.body.backgroundType || 'color').trim();
    const backgroundColor = String(req.body.backgroundColor || '#f7f7f8').trim();
    const bankAccountInfo = String(req.body.bankAccountInfo || '').trim();
    const contactInfo = String(req.body.contactInfo || '').trim();
    const businessInfo = String(req.body.businessInfo || '').trim();
    const languageDefault = resolveLanguage(req.body.languageDefault || 'ko', 'ko');

    setSetting('siteName', siteName || 'Chrono Lab');
    setSetting('headerColor', headerColor || '#111827');
    setSetting('backgroundType', backgroundType === 'image' ? 'image' : 'color');
    setSetting('backgroundValue', backgroundType === 'image' ? getSetting('backgroundValue', '#f7f7f8') : backgroundColor);
    setSetting('bankAccountInfo', bankAccountInfo);
    setSetting('contactInfo', contactInfo);
    setSetting('businessInfo', businessInfo);
    setSetting('languageDefault', languageDefault);

    if (req.body.menusJson) {
      try {
        const parsedMenus = parseMenus(req.body.menusJson);
        setSetting('menus', JSON.stringify(parsedMenus));
      } catch {
        setFlash(req, 'error', '메뉴 JSON 형식이 올바르지 않습니다.');
        return res.redirect(backPath);
      }
    }

    const headerLogoFile = req.files?.headerLogo?.[0];
    const headerSymbolFile = req.files?.headerSymbol?.[0];
    const footerLogoFile = req.files?.footerLogo?.[0];
    const backgroundImageFile = req.files?.backgroundImage?.[0];

    if (headerLogoFile) {
      setSetting('headerLogoPath', fileUrl(headerLogoFile));
    }
    if (headerSymbolFile) {
      setSetting('headerSymbolPath', fileUrl(headerSymbolFile));
    }
    if (footerLogoFile) {
      setSetting('footerLogoPath', fileUrl(footerLogoFile));
    }
    if (backgroundImageFile) {
      setSetting('backgroundType', 'image');
      setSetting('backgroundValue', fileUrl(backgroundImageFile));
    }

    setFlash(req, 'success', '사이트 설정이 저장되었습니다.');
    res.redirect(backPath);
  }
);

app.post('/admin/product/create', requireAdmin, upload.array('images', 20), (req, res) => {
  const backPath = safeBackPath(req, '/admin/products');
  const categoryGroupRaw = String(req.body.categoryGroup || SHOP_PRODUCT_GROUPS[0]).trim();
  const categoryGroup = SHOP_PRODUCT_GROUPS.includes(categoryGroupRaw)
    ? categoryGroupRaw
    : SHOP_PRODUCT_GROUPS[0];
  const brand = String(req.body.brand || '').trim();
  const model = String(req.body.model || '').trim();
  const subModel = String(req.body.subModel || '').trim();
  const reference = String(req.body.reference || '').trim();
  const factoryName = String(req.body.factoryName || '').trim();
  const versionName = String(req.body.versionName || '').trim();
  const movement = String(req.body.movement || '').trim();
  const caseSize = String(req.body.caseSize || '').trim();
  const dialColor = String(req.body.dialColor || '').trim();
  const caseMaterial = String(req.body.caseMaterial || '').trim();
  const strapMaterial = String(req.body.strapMaterial || '').trim();
  const features = String(req.body.features || '').trim();
  const price = parsePositiveInt(req.body.price, 0);
  const shippingPeriod = String(req.body.shippingPeriod || '').trim();

  if (!brand || !model || !subModel || price <= 0) {
    setFlash(req, 'error', '브랜드/모델/세부모델/가격은 필수입니다.');
    return res.redirect(backPath);
  }

  const uploadedImages = Array.isArray(req.files) ? req.files.map((file) => fileUrl(file)).filter(Boolean) : [];
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
        image_path
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    primaryImage
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

  setFlash(req, 'success', '상품이 등록되었습니다.');
  res.redirect(backPath);
});

app.post('/admin/product/:id/toggle', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/products');
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

app.post('/admin/order/:id/status', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const status = normalizeOrderStatus(req.body.status || ORDER_STATUS.UNPAID);
  const allowed = new Set(Object.values(ORDER_STATUS));

  if (!allowed.has(status)) {
    setFlash(req, 'error', '허용되지 않은 상태값입니다.');
    return res.redirect(backPath);
  }

  db.prepare('UPDATE orders SET status = ? WHERE id = ?').run(status, id);
  setFlash(req, 'success', '주문 상태를 업데이트했습니다.');
  res.redirect(backPath);
});

app.post('/admin/order/:id/next', requireAdmin, (req, res) => {
  const backPath = safeBackPath(req, '/admin/orders');
  const id = Number(req.params.id);
  const order = db.prepare('SELECT id, status FROM orders WHERE id = ? LIMIT 1').get(id);

  if (!order) {
    setFlash(req, 'error', '주문을 찾을 수 없습니다.');
    return res.redirect(backPath);
  }

  const nextStatus = getNextOrderStatus(order.status);
  if (!nextStatus) {
    setFlash(req, 'error', '이미 배송완료 상태입니다.');
    return res.redirect(backPath);
  }

  db.prepare('UPDATE orders SET status = ? WHERE id = ?').run(nextStatus, id);
  setFlash(req, 'success', '주문 상태를 다음 단계로 변경했습니다.');
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
