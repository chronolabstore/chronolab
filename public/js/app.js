(function () {
  function getMobileDesktopScale() {
    if (typeof window === 'undefined') {
      return 1;
    }

    var hasMatchMedia = typeof window.matchMedia === 'function';
    var isCoarsePointer = hasMatchMedia ? window.matchMedia('(pointer: coarse)').matches : false;
    if (!isCoarsePointer) {
      return 1;
    }

    var screenWidth = Number(window.screen && window.screen.width) || 0;
    var screenHeight = Number(window.screen && window.screen.height) || 0;
    var shortEdge = Math.min(screenWidth, screenHeight);
    var viewportWidth = Number(window.innerWidth) || 0;
    var visualScale = Number(window.visualViewport && window.visualViewport.scale) || 1;

    if (!shortEdge || !viewportWidth) {
      return 1;
    }

    if (shortEdge <= 540 && visualScale > 0 && visualScale < 0.95) {
      return Math.min(2.8, Math.max(1.2, 1 / visualScale));
    }

    var ratio = viewportWidth / shortEdge;
    if (shortEdge <= 540 && ratio > 1.2) {
      return Math.min(2.8, Math.max(1.2, ratio));
    }

    return 1;
  }

  function applyMobileDesktopFix() {
    if (typeof document === 'undefined' || !document.body) {
      return;
    }

    var scale = getMobileDesktopScale();
    if (scale > 1) {
      document.body.classList.add('mobile-desktop-fix');
      document.documentElement.style.setProperty('--mobile-desktop-scale', String(scale));
      return;
    }

    document.body.classList.remove('mobile-desktop-fix');
    document.documentElement.style.removeProperty('--mobile-desktop-scale');
  }

  function initMobileDesktopFix() {
    applyMobileDesktopFix();
    window.addEventListener('resize', applyMobileDesktopFix);
    window.addEventListener('orientationchange', applyMobileDesktopFix);
  }

  function closePopup() {
    var popup = document.getElementById('noticePopup');
    if (popup) {
      popup.classList.add('hidden');
    }
  }

  function initPopup() {
    var popup = document.getElementById('noticePopup');
    if (!popup) {
      return;
    }

    var popupId = popup.getAttribute('data-popup-id');
    if (!popupId) {
      return;
    }

    var key = 'chronolab-popup-hide-' + popupId;
    var savedUntil = Number(localStorage.getItem(key) || '0');
    if (savedUntil > Date.now()) {
      return;
    }

    popup.classList.remove('hidden');

    popup.addEventListener('click', function (event) {
      var actionTarget = event.target.closest('[data-popup-action]');
      if (!actionTarget) {
        if (event.target === popup) {
          closePopup();
        }
        return;
      }

      var action = actionTarget.getAttribute('data-popup-action');
      if (action === 'hide7') {
        var sevenDays = 1000 * 60 * 60 * 24 * 7;
        localStorage.setItem(key, String(Date.now() + sevenDays));
        closePopup();
      }

      if (action === 'close' || action === 'confirm') {
        closePopup();
      }
    });
  }

  function initProductGallery() {
    var galleries = document.querySelectorAll('[data-gallery]');
    if (!galleries.length) {
      return;
    }

    galleries.forEach(function (gallery) {
      var mainImage = gallery.querySelector('[data-gallery-main]');
      var thumbs = Array.prototype.slice.call(gallery.querySelectorAll('[data-gallery-thumb]'));
      var prevButton = gallery.querySelector('[data-gallery-prev]');
      var nextButton = gallery.querySelector('[data-gallery-next]');

      if (!mainImage || !thumbs.length) {
        return;
      }

      var currentIndex = 0;

      function update(index) {
        if (index < 0 || index >= thumbs.length) {
          return;
        }

        currentIndex = index;
        var nextSrc = thumbs[index].getAttribute('data-src');
        if (nextSrc) {
          mainImage.setAttribute('src', nextSrc);
        }

        thumbs.forEach(function (thumb, thumbIndex) {
          thumb.classList.toggle('active', thumbIndex === currentIndex);
        });
      }

      thumbs.forEach(function (thumb, index) {
        thumb.addEventListener('click', function () {
          update(index);
        });
      });

      if (prevButton) {
        prevButton.addEventListener('click', function () {
          var nextIndex = currentIndex === 0 ? thumbs.length - 1 : currentIndex - 1;
          update(nextIndex);
        });
      }

      if (nextButton) {
        nextButton.addEventListener('click', function () {
          var nextIndex = currentIndex === thumbs.length - 1 ? 0 : currentIndex + 1;
          update(nextIndex);
        });
      }

      if (thumbs.length <= 1) {
        if (prevButton) prevButton.setAttribute('hidden', 'hidden');
        if (nextButton) nextButton.setAttribute('hidden', 'hidden');
      }
    });
  }

  function initApp() {
    initMobileDesktopFix();
    initPopup();
    initProductGallery();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})();
