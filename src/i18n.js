export const messages = {
  ko: {
    homeHeroTitle: 'Chrono Lab',
    homeHeroSubtitle: '심플하고 신뢰감 있는 시계 쇼핑 경험',
    welcome: '프리미엄 워치 셀렉션',
    orderBankOnly: '결제는 계좌이체만 지원됩니다.',
    login: '로그인',
    logout: '로그아웃',
    signup: '회원가입',
    myPage: '마이페이지',
    admin: '어드민',
    goToShop: '쇼핑몰 바로가기',
    footerVisitorsToday: '오늘 방문자',
    footerVisitorsTotal: '전체 방문자',
    footerProductsToday: '오늘 게시 상품',
    footerProductsTotal: '총 게시 상품',
    footerPostsToday: '오늘 게시물',
    footerPostsTotal: '총 게시물',
    buyNow: '구매하기',
    orderComplete: '구매 신청 완료',
    orderNo: '구매번호',
    inquiryLocked: '비공개 문의로 본문은 작성자/어드민만 확인할 수 있습니다.'
  },
  en: {
    homeHeroTitle: 'Chrono Lab',
    homeHeroSubtitle: 'Simple and clean watch shopping experience',
    welcome: 'Premium Watch Selection',
    orderBankOnly: 'Bank transfer only.',
    login: 'Login',
    logout: 'Logout',
    signup: 'Sign up',
    myPage: 'My account',
    admin: 'Admin',
    goToShop: 'Go to Shop',
    footerVisitorsToday: 'Visitors Today',
    footerVisitorsTotal: 'Visitors Total',
    footerProductsToday: 'Products Today',
    footerProductsTotal: 'Products Total',
    footerPostsToday: 'Posts Today',
    footerPostsTotal: 'Posts Total',
    buyNow: 'Buy now',
    orderComplete: 'Order request completed',
    orderNo: 'Order No.',
    inquiryLocked: 'Private inquiry. Only owner/admin can view details.'
  }
};

export function resolveLanguage(rawLang, fallback = 'ko') {
  if (rawLang === 'en') {
    return 'en';
  }
  if (rawLang === 'ko') {
    return 'ko';
  }
  return fallback === 'en' ? 'en' : 'ko';
}

export function t(lang, key) {
  return messages[lang]?.[key] || messages.ko[key] || key;
}
