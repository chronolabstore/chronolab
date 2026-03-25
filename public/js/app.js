(function () {
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
    initPopup();
    initProductGallery();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})();
