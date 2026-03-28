import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import { PRODUCT_SEED_ITEMS } from './product-seeds.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'chronolab.db');

export const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

export const SHOP_PRODUCT_GROUPS = ['공장제', '젠파츠', '현지중고'];

const defaultMenus = [
  { id: 'home', labelKo: '메인페이지', labelEn: 'Main', path: '/main' },
  { id: 'notice', labelKo: '공지사항', labelEn: 'Notice', path: '/notice' },
  { id: 'news', labelKo: '뉴스', labelEn: 'News', path: '/news' },
  { id: 'shop', labelKo: '쇼핑몰', labelEn: 'Shop', path: '/shop' },
  { id: 'qc', labelKo: 'QC', labelEn: 'QC', path: '/qc' },
  { id: 'review', labelKo: '구매후기', labelEn: 'Reviews', path: '/review' },
  { id: 'inquiry', labelKo: '문의', labelEn: 'Inquiry', path: '/inquiry' }
];

const defaultSettings = {
  siteName: 'Chrono Lab',
  headerColor: '#111827',
  headerLogoPath: '',
  headerSymbolPath: '',
  footerLogoPath: '',
  backgroundType: 'color',
  backgroundValue: '#f7f7f8',
  menus: JSON.stringify(defaultMenus),
  bankAccountInfo: '입금계좌: 은행명 000-0000-0000 (예금주: Chrono Lab)',
  contactInfo: '고객센터: 010-0000-0000 / 카카오톡: @chronolab',
  businessInfo: '상호: Chrono Lab | 대표: Chrono Team | 사업자번호: 000-00-00000',
  languageDefault: 'ko'
};

function upsertDefaultSetting(key, value) {
  db.prepare('INSERT OR IGNORE INTO site_settings (setting_key, setting_value) VALUES (?, ?)').run(
    key,
    String(value)
  );
}

function upsertMetric(key, value = 0) {
  db.prepare('INSERT OR IGNORE INTO metrics (metric_key, metric_value) VALUES (?, ?)').run(key, value);
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
      path: normalizePath(menu.path)
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
        labelEn: filtered[foundIndex].labelEn || defaultMenu.labelEn
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
}

function normalizeOrderStatuses() {
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

  const passwordHash = bcrypt.hashSync('Admin123!', 10);
  db.prepare(
    `
      INSERT INTO users (email, username, full_name, phone, password_hash, agreed_terms, is_admin, admin_role)
      VALUES (?, ?, ?, '', ?, 1, 1, 'PRIMARY')
    `
  ).run('admin@chronolab.local', 'admin', 'Main Admin', passwordHash);
}

function normalizeAdminRoles() {
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
      full_name TEXT NOT NULL DEFAULT '',
      phone TEXT NOT NULL DEFAULT '',
      password_hash TEXT NOT NULL,
      agreed_terms INTEGER NOT NULL DEFAULT 1,
      is_admin INTEGER NOT NULL DEFAULT 0,
      admin_role TEXT NOT NULL DEFAULT '',
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
      created_by_user_id INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS notices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      is_popup INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS news_posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS qc_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_no TEXT NOT NULL,
      image_path TEXT NOT NULL,
      note TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      product_id INTEGER,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS inquiries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      image_path TEXT,
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
  `);

  ensureProductsCategoryColumn();
  ensureUserAdminProfileColumns();
  ensureOrdersCustomsColumn();
  ensureOrdersTrackingColumns();
  ensureDailyVisitSplitColumns();
  ensureAdminSecurityTables();
  normalizeOrderStatuses();

  for (const [key, value] of Object.entries(defaultSettings)) {
    upsertDefaultSetting(key, value);
  }

  const menuRow = db
    .prepare('SELECT setting_value FROM site_settings WHERE setting_key = ? LIMIT 1')
    .get('menus');
  const normalizedMenus = normalizeMenus(menuRow?.setting_value || JSON.stringify(defaultMenus));
  setSetting('menus', JSON.stringify(normalizedMenus));

  upsertMetric('totalVisits', 0);

  ensureAdminUser();
  normalizeAdminRoles();
  const demoUserId = ensureDemoMemberUser();

  seedProducts();
  seedNotices();
  seedNews();
  seedOrdersAndQc(demoUserId);
  seedReviews(demoUserId);
  seedInquiries(demoUserId);

  ensureOrderStatusLogTable();
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
