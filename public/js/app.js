(function () {
  function closeNoticePopup() {
    var popup = document.getElementById('noticePopup');
    if (popup) {
      popup.classList.add('hidden');
    }
  }

  function closeFlashPopup() {
    var popup = document.getElementById('flashPopup');
    if (popup) {
      popup.classList.add('hidden');
    }
  }

  function initNoticePopup() {
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
          closeNoticePopup();
        }
        return;
      }

      var action = actionTarget.getAttribute('data-popup-action');
      if (action === 'hide7') {
        var sevenDays = 1000 * 60 * 60 * 24 * 7;
        localStorage.setItem(key, String(Date.now() + sevenDays));
        closeNoticePopup();
      }

      if (action === 'close' || action === 'confirm') {
        closeNoticePopup();
      }
    });
  }

  function initFlashPopup() {
    var popup = document.getElementById('flashPopup');
    if (!popup) {
      return;
    }

    popup.classList.remove('hidden');
    popup.addEventListener('click', function (event) {
      var actionTarget = event.target.closest('[data-flash-popup-action]');
      if (!actionTarget) {
        if (event.target === popup) {
          closeFlashPopup();
        }
        return;
      }

      closeFlashPopup();
    });
  }

  function initProductGallery() {
    var galleries = document.querySelectorAll('[data-gallery]');
    if (!galleries.length) {
      return;
    }

    galleries.forEach(function (gallery) {
      var mainImage = gallery.querySelector('[data-gallery-main]');
      var mainWrap = gallery.querySelector('.detail-main-wrap');
      var thumbs = Array.prototype.slice.call(gallery.querySelectorAll('[data-gallery-thumb]'));
      var prevButton = gallery.querySelector('[data-gallery-prev]');
      var nextButton = gallery.querySelector('[data-gallery-next]');

      if (!mainImage || !thumbs.length) {
        return;
      }

      var currentIndex = 0;

      function syncImageMode() {
        if (!mainWrap || !mainImage) {
          return;
        }

        var naturalWidth = Number(mainImage.naturalWidth || 0);
        var naturalHeight = Number(mainImage.naturalHeight || 0);
        if (!naturalWidth || !naturalHeight) {
          mainWrap.classList.remove('gallery-landscape', 'gallery-square', 'gallery-portrait');
          return;
        }

        var ratio = naturalWidth / naturalHeight;
        mainWrap.classList.remove('gallery-landscape', 'gallery-square', 'gallery-portrait');
        if (ratio >= 1.2) {
          mainWrap.classList.add('gallery-landscape');
          return;
        }
        if (ratio <= 0.88) {
          mainWrap.classList.add('gallery-portrait');
          return;
        }
        mainWrap.classList.add('gallery-square');
      }

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

        if (mainImage.complete) {
          syncImageMode();
        }
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

      mainImage.addEventListener('load', syncImageMode);
      syncImageMode();
    });
  }

  function initAdminInlineImagePreviews() {
    if (!window.location.pathname.startsWith('/admin')) {
      return;
    }

    var isEn = document.documentElement.lang === 'en';
    var fileInputs = document.querySelectorAll('input[type="file"][accept*="image"]');
    if (!fileInputs.length) {
      return;
    }

    fileInputs.forEach(function (input) {
      if (input.dataset.inlinePreviewBound === '1') {
        return;
      }
      input.dataset.inlinePreviewBound = '1';

      if (input.name === 'images' && input.closest('[data-admin-product-form]')) {
        return;
      }

      var wrap = document.createElement('div');
      wrap.className = 'inline-file-preview-wrap';
      wrap.hidden = true;

      var title = document.createElement('p');
      title.className = 'muted-label';
      title.textContent = isEn ? 'Image preview (click to enlarge)' : '이미지 미리보기 (클릭 시 확대)';

      var grid = document.createElement('div');
      grid.className = 'inline-file-preview-grid';

      wrap.appendChild(title);
      wrap.appendChild(grid);
      input.insertAdjacentElement('afterend', wrap);

      var objectUrls = [];
      function clearPreview() {
        objectUrls.forEach(function (url) {
          URL.revokeObjectURL(url);
        });
        objectUrls = [];
        grid.innerHTML = '';
        wrap.hidden = true;
      }

      input.addEventListener('change', function () {
        clearPreview();
        var files = Array.prototype.slice
          .call(input.files || [])
          .filter(function (file) {
            return typeof file.type === 'string' && file.type.startsWith('image/');
          });

        if (!files.length) {
          return;
        }

        wrap.hidden = false;
        files.forEach(function (file) {
          var url = URL.createObjectURL(file);
          objectUrls.push(url);
          var preview = document.createElement('img');
          preview.className = 'inline-file-preview-image zoomable-media';
          preview.setAttribute('src', url);
          preview.setAttribute('alt', file.name || 'preview');
          grid.appendChild(preview);
        });
      });
    });
  }

  function initImageLightbox() {
    var lightbox = document.getElementById('imageLightbox');
    if (!lightbox) {
      return;
    }

    var stage = lightbox.querySelector('[data-lightbox-stage]');
    var image = lightbox.querySelector('[data-lightbox-image]');
    var zoomText = lightbox.querySelector('[data-lightbox-zoom]');
    if (!stage || !image) {
      return;
    }

    var minScale = 1;
    var maxScale = 4;
    var scale = 1;
    var translateX = 0;
    var translateY = 0;
    var dragging = false;
    var startX = 0;
    var startY = 0;
    var originX = 0;
    var originY = 0;
    var touchDrag = false;
    var pinchZoom = false;
    var pinchStartDistance = 0;
    var pinchStartScale = 1;

    function clamp(value, min, max) {
      return Math.min(max, Math.max(min, value));
    }

    function setZoomText() {
      if (!zoomText) {
        return;
      }
      zoomText.textContent = Math.round(scale * 100) + '%';
    }

    function render() {
      image.style.transform = 'translate(' + translateX + 'px, ' + translateY + 'px) scale(' + scale + ')';
      setZoomText();
      stage.classList.toggle('is-zoomed', scale > 1.02);
    }

    function resetTransform() {
      scale = 1;
      translateX = 0;
      translateY = 0;
      render();
    }

    function closeLightbox() {
      lightbox.classList.add('hidden');
      lightbox.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('lightbox-open');
      image.setAttribute('src', '');
      dragging = false;
      touchDrag = false;
      pinchZoom = false;
    }

    function openLightbox(src, alt) {
      if (!src) {
        return;
      }
      image.setAttribute('src', src);
      image.setAttribute('alt', alt || 'image');
      lightbox.classList.remove('hidden');
      lightbox.setAttribute('aria-hidden', 'false');
      document.body.classList.add('lightbox-open');
      resetTransform();
    }

    function applyZoom(nextScale) {
      var clamped = clamp(nextScale, minScale, maxScale);
      if (clamped === scale) {
        return;
      }
      scale = clamped;
      if (scale <= 1) {
        translateX = 0;
        translateY = 0;
      }
      render();
    }

    function zoomBy(delta) {
      applyZoom(scale + delta);
    }

    function isEligibleImage(img) {
      if (!img || img.closest('#imageLightbox')) {
        return false;
      }
      if (img.hasAttribute('data-no-lightbox')) {
        return false;
      }
      if (img.closest('[data-gallery-thumb]')) {
        return false;
      }
      if (img.closest('header') || img.closest('.site-footer')) {
        return false;
      }
      if (img.closest('.page-container') || img.closest('.popup-card') || img.closest('.inline-file-preview-wrap')) {
        return true;
      }
      return false;
    }

    function markZoomableMedia(root) {
      var scopedRoot = root && root.querySelectorAll ? root : document;
      scopedRoot.querySelectorAll('img').forEach(function (img) {
        if (isEligibleImage(img)) {
          img.classList.add('zoomable-media');
        }
      });
      scopedRoot.querySelectorAll('canvas.watermark-preview-canvas').forEach(function (canvas) {
        canvas.classList.add('zoomable-media');
      });
    }

    markZoomableMedia(document);

    var observer = new MutationObserver(function (mutations) {
      mutations.forEach(function (mutation) {
        Array.prototype.forEach.call(mutation.addedNodes || [], function (node) {
          if (!node || node.nodeType !== 1) {
            return;
          }
          if (node.matches && node.matches('img') && isEligibleImage(node)) {
            node.classList.add('zoomable-media');
          }
          markZoomableMedia(node);
        });
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });

    document.addEventListener('click', function (event) {
      if (!lightbox.classList.contains('hidden') && event.target.closest('#imageLightbox')) {
        var actionTarget = event.target.closest('[data-lightbox-action]');
        if (!actionTarget) {
          return;
        }
        var action = actionTarget.getAttribute('data-lightbox-action');
        if (action === 'close') {
          closeLightbox();
        } else if (action === 'zoom-in') {
          zoomBy(0.2);
        } else if (action === 'zoom-out') {
          zoomBy(-0.2);
        } else if (action === 'reset') {
          resetTransform();
        }
        return;
      }

      var previewCanvas = event.target.closest('canvas.watermark-preview-canvas');
      if (previewCanvas) {
        event.preventDefault();
        openLightbox(previewCanvas.toDataURL('image/png'), 'preview');
        return;
      }

      var targetImg = event.target.closest('img');
      if (!targetImg || !isEligibleImage(targetImg)) {
        return;
      }

      var src = targetImg.currentSrc || targetImg.getAttribute('src') || '';
      if (!src) {
        return;
      }

      if (targetImg.closest('a')) {
        event.preventDefault();
      }
      openLightbox(src, targetImg.getAttribute('alt') || 'image');
    });

    lightbox.addEventListener('click', function (event) {
      if (event.target === lightbox) {
        closeLightbox();
      }
    });

    stage.addEventListener(
      'wheel',
      function (event) {
        if (lightbox.classList.contains('hidden')) {
          return;
        }
        event.preventDefault();
        zoomBy(event.deltaY < 0 ? 0.15 : -0.15);
      },
      { passive: false }
    );

    stage.addEventListener('mousedown', function (event) {
      if (scale <= 1) {
        return;
      }
      dragging = true;
      startX = event.clientX;
      startY = event.clientY;
      originX = translateX;
      originY = translateY;
      stage.classList.add('is-dragging');
      event.preventDefault();
    });

    window.addEventListener('mousemove', function (event) {
      if (!dragging) {
        return;
      }
      translateX = originX + (event.clientX - startX);
      translateY = originY + (event.clientY - startY);
      render();
    });

    window.addEventListener('mouseup', function () {
      dragging = false;
      stage.classList.remove('is-dragging');
    });

    stage.addEventListener(
      'touchstart',
      function (event) {
        if (!event.touches) {
          return;
        }

        if (event.touches.length === 2) {
          pinchZoom = true;
          touchDrag = false;
          var dx = event.touches[0].clientX - event.touches[1].clientX;
          var dy = event.touches[0].clientY - event.touches[1].clientY;
          pinchStartDistance = Math.hypot(dx, dy) || 1;
          pinchStartScale = scale;
          return;
        }

        if (scale <= 1 || event.touches.length !== 1) {
          return;
        }
        touchDrag = true;
        startX = event.touches[0].clientX;
        startY = event.touches[0].clientY;
        originX = translateX;
        originY = translateY;
      },
      { passive: true }
    );

    stage.addEventListener(
      'touchmove',
      function (event) {
        if (!event.touches) {
          return;
        }

        if (pinchZoom && event.touches.length === 2) {
          var dx = event.touches[0].clientX - event.touches[1].clientX;
          var dy = event.touches[0].clientY - event.touches[1].clientY;
          var nextDistance = Math.hypot(dx, dy) || 1;
          var ratio = nextDistance / (pinchStartDistance || 1);
          applyZoom(pinchStartScale * ratio);
          event.preventDefault();
          return;
        }

        if (!touchDrag || event.touches.length !== 1) {
          return;
        }
        translateX = originX + (event.touches[0].clientX - startX);
        translateY = originY + (event.touches[0].clientY - startY);
        render();
        event.preventDefault();
      },
      { passive: false }
    );

    stage.addEventListener('touchend', function (event) {
      touchDrag = false;
      if (!event.touches || event.touches.length < 2) {
        pinchZoom = false;
      }
    });

    stage.addEventListener('touchcancel', function () {
      touchDrag = false;
      pinchZoom = false;
    });

    document.addEventListener('keydown', function (event) {
      if (lightbox.classList.contains('hidden')) {
        return;
      }
      if (event.key === 'Escape') {
        closeLightbox();
        return;
      }
      if (event.key === '+' || event.key === '=') {
        zoomBy(0.2);
        event.preventDefault();
        return;
      }
      if (event.key === '-') {
        zoomBy(-0.2);
        event.preventDefault();
        return;
      }
      if (event.key === '0') {
        resetTransform();
        event.preventDefault();
      }
    });
  }

  function initApp() {
    initNoticePopup();
    initFlashPopup();
    initProductGallery();
    initAdminInlineImagePreviews();
    initImageLightbox();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})();
