(function () {
  function getNoticePopupRoot() {
    return document.getElementById('noticePopupDeck') || document.getElementById('noticePopup');
  }

  function closeNoticePopup() {
    var popup = getNoticePopupRoot();
    if (popup) {
      popup.classList.add('hidden');
      window.setTimeout(initPrimaryInputAutofocus, 0);
    }
  }

  function closeFlashPopup() {
    var popup = document.getElementById('flashPopup');
    if (popup) {
      popup.classList.add('hidden');
      window.setTimeout(initPrimaryInputAutofocus, 0);
    }
  }

  function getPopupLocaleConfig() {
    var htmlLang = String(document.documentElement.lang || 'ko').toLowerCase();
    var isEnglish = htmlLang.indexOf('en') === 0;
    var isChinese = htmlLang.indexOf('zh') === 0;
    return {
      isEnglish: isEnglish,
      isChinese: isChinese,
      okLabel: isEnglish ? 'OK' : isChinese ? '确定' : '확인',
      cancelLabel: isEnglish ? 'Cancel' : isChinese ? '取消' : '취소',
      noticeTitle: isEnglish ? 'Notice' : isChinese ? '通知' : '안내',
      confirmTitle: isEnglish ? 'Confirm' : isChinese ? '确认' : '확인'
    };
  }

  function normalizePopupOptions(options) {
    if (typeof options === 'string') {
      return { message: String(options || '') };
    }
    if (!options || typeof options !== 'object') {
      return {};
    }
    return options;
  }

  function ensureActionAlertPopup() {
    var existing = document.getElementById('actionAlertPopup');
    if (existing) {
      return existing;
    }
    var popup = document.createElement('div');
    popup.id = 'actionAlertPopup';
    popup.className = 'popup-backdrop popup-alert-backdrop hidden';
    popup.innerHTML =
      '<div class="popup-card popup-alert-card success">' +
      '  <h3 data-action-alert-title></h3>' +
      '  <p data-action-alert-message></p>' +
      '  <div class="popup-actions">' +
      '    <button type="button" class="chip dark" data-action-alert-confirm>OK</button>' +
      '  </div>' +
      '</div>';
    document.body.appendChild(popup);
    return popup;
  }

  function ensureActionConfirmPopup() {
    var existing = document.getElementById('actionConfirmPopup');
    if (existing) {
      return existing;
    }
    var popup = document.createElement('div');
    popup.id = 'actionConfirmPopup';
    popup.className = 'popup-backdrop popup-alert-backdrop hidden';
    popup.innerHTML =
      '<div class="popup-card popup-alert-card">' +
      '  <h3 data-action-confirm-title></h3>' +
      '  <p data-action-confirm-message></p>' +
      '  <div class="popup-actions">' +
      '    <button type="button" class="chip ghost" data-action-confirm-cancel></button>' +
      '    <button type="button" class="chip dark" data-action-confirm-ok></button>' +
      '  </div>' +
      '</div>';
    document.body.appendChild(popup);
    return popup;
  }

  function ensureActionPromptPopup() {
    var existing = document.getElementById('actionPromptPopup');
    if (existing) {
      return existing;
    }
    var popup = document.createElement('div');
    popup.id = 'actionPromptPopup';
    popup.className = 'popup-backdrop popup-alert-backdrop hidden';
    popup.innerHTML =
      '<div class="popup-card popup-alert-card">' +
      '  <h3 data-action-prompt-title></h3>' +
      '  <p data-action-prompt-message></p>' +
      '  <input type="text" class="popup-prompt-input" data-action-prompt-input />' +
      '  <div class="popup-actions">' +
      '    <button type="button" class="chip ghost" data-action-prompt-cancel></button>' +
      '    <button type="button" class="chip dark" data-action-prompt-ok></button>' +
      '  </div>' +
      '</div>';
    document.body.appendChild(popup);
    return popup;
  }

  function showActionAlertPopup(options) {
    var locale = getPopupLocaleConfig();
    var config = normalizePopupOptions(options);
    var popup = ensureActionAlertPopup();
    var titleNode = popup.querySelector('[data-action-alert-title]');
    var messageNode = popup.querySelector('[data-action-alert-message]');
    var confirmButton = popup.querySelector('[data-action-alert-confirm]');
    var card = popup.querySelector('.popup-alert-card');
    var popupType = config.type === 'error' ? 'error' : 'success';

    if (card) {
      card.classList.remove('success', 'error');
      card.classList.add(popupType);
    }
    if (titleNode) {
      titleNode.textContent = String(config.title || locale.noticeTitle);
    }
    if (messageNode) {
      messageNode.textContent = String(config.message || '');
    }
    if (confirmButton) {
      confirmButton.textContent = String(config.confirmLabel || locale.okLabel);
    }

    return new Promise(function (resolve) {
      function closePopup(confirmed) {
        popup.classList.add('hidden');
        popup.removeEventListener('click', onClick);
        document.removeEventListener('keydown', onKeyDown);
        window.setTimeout(initPrimaryInputAutofocus, 0);
        resolve(Boolean(confirmed));
      }

      function onClick(event) {
        var confirmTarget = event.target.closest('[data-action-alert-confirm]');
        if (confirmTarget) {
          closePopup(true);
          return;
        }
        if (event.target === popup) {
          closePopup(false);
        }
      }

      function onKeyDown(event) {
        if (popup.classList.contains('hidden')) {
          return;
        }
        if (event.key === 'Enter') {
          event.preventDefault();
          closePopup(true);
        }
        if (event.key === 'Escape') {
          event.preventDefault();
          closePopup(false);
        }
      }

      popup.addEventListener('click', onClick);
      document.addEventListener('keydown', onKeyDown);
      popup.classList.remove('hidden');
      if (confirmButton) {
        confirmButton.focus();
      }
    });
  }

  function showActionConfirmPopup(options) {
    var locale = getPopupLocaleConfig();
    var config = normalizePopupOptions(options);
    var popup = ensureActionConfirmPopup();
    var titleNode = popup.querySelector('[data-action-confirm-title]');
    var messageNode = popup.querySelector('[data-action-confirm-message]');
    var cancelButton = popup.querySelector('[data-action-confirm-cancel]');
    var confirmButton = popup.querySelector('[data-action-confirm-ok]');

    if (titleNode) {
      titleNode.textContent = String(config.title || locale.confirmTitle);
    }
    if (messageNode) {
      messageNode.textContent = String(config.message || '');
    }
    if (cancelButton) {
      cancelButton.textContent = String(config.cancelLabel || locale.cancelLabel);
    }
    if (confirmButton) {
      confirmButton.textContent = String(config.confirmLabel || locale.okLabel);
    }

    return new Promise(function (resolve) {
      function closePopup(confirmed) {
        popup.classList.add('hidden');
        popup.removeEventListener('click', onClick);
        document.removeEventListener('keydown', onKeyDown);
        window.setTimeout(initPrimaryInputAutofocus, 0);
        resolve(Boolean(confirmed));
      }

      function onClick(event) {
        if (event.target.closest('[data-action-confirm-ok]')) {
          closePopup(true);
          return;
        }
        if (event.target.closest('[data-action-confirm-cancel]')) {
          closePopup(false);
          return;
        }
        if (event.target === popup) {
          closePopup(false);
        }
      }

      function onKeyDown(event) {
        if (popup.classList.contains('hidden')) {
          return;
        }
        if (event.key === 'Enter') {
          event.preventDefault();
          closePopup(true);
        }
        if (event.key === 'Escape') {
          event.preventDefault();
          closePopup(false);
        }
      }

      popup.addEventListener('click', onClick);
      document.addEventListener('keydown', onKeyDown);
      popup.classList.remove('hidden');
      if (confirmButton) {
        confirmButton.focus();
      }
    });
  }

  function showActionPromptPopup(options) {
    var locale = getPopupLocaleConfig();
    var config = normalizePopupOptions(options);
    var popup = ensureActionPromptPopup();
    var titleNode = popup.querySelector('[data-action-prompt-title]');
    var messageNode = popup.querySelector('[data-action-prompt-message]');
    var inputNode = popup.querySelector('[data-action-prompt-input]');
    var cancelButton = popup.querySelector('[data-action-prompt-cancel]');
    var confirmButton = popup.querySelector('[data-action-prompt-ok]');
    var maxLength = Number(config.maxLength || 0);

    if (titleNode) {
      titleNode.textContent = String(config.title || locale.confirmTitle);
    }
    if (messageNode) {
      messageNode.textContent = String(config.message || '');
    }
    if (inputNode) {
      inputNode.value = String(config.initialValue || '');
      inputNode.placeholder = String(config.placeholder || '');
      inputNode.maxLength = maxLength > 0 ? maxLength : 200;
    }
    if (cancelButton) {
      cancelButton.textContent = String(config.cancelLabel || locale.cancelLabel);
    }
    if (confirmButton) {
      confirmButton.textContent = String(config.confirmLabel || locale.okLabel);
    }

    return new Promise(function (resolve) {
      function closePopup(value) {
        popup.classList.add('hidden');
        popup.removeEventListener('click', onClick);
        document.removeEventListener('keydown', onKeyDown);
        window.setTimeout(initPrimaryInputAutofocus, 0);
        resolve(value);
      }

      function confirmPopup() {
        closePopup(inputNode ? String(inputNode.value || '') : '');
      }

      function onClick(event) {
        if (event.target.closest('[data-action-prompt-ok]')) {
          confirmPopup();
          return;
        }
        if (event.target.closest('[data-action-prompt-cancel]')) {
          closePopup(null);
          return;
        }
        if (event.target === popup) {
          closePopup(null);
        }
      }

      function onKeyDown(event) {
        if (popup.classList.contains('hidden')) {
          return;
        }
        if (event.key === 'Enter') {
          event.preventDefault();
          confirmPopup();
        }
        if (event.key === 'Escape') {
          event.preventDefault();
          closePopup(null);
        }
      }

      popup.addEventListener('click', onClick);
      document.addEventListener('keydown', onKeyDown);
      popup.classList.remove('hidden');
      if (inputNode) {
        inputNode.focus();
        inputNode.select();
      } else if (confirmButton) {
        confirmButton.focus();
      }
    });
  }

  function registerChronoPopupApi() {
    if (!window.ChronoPopup || typeof window.ChronoPopup !== 'object') {
      window.ChronoPopup = {};
    }
    window.ChronoPopup.alert = function (options) {
      return showActionAlertPopup(options);
    };
    window.ChronoPopup.confirm = function (options) {
      return showActionConfirmPopup(options);
    };
    window.ChronoPopup.prompt = function (options) {
      return showActionPromptPopup(options);
    };
  }

  function isElementVisible(element) {
    if (!element || element.hidden) {
      return false;
    }
    var style = window.getComputedStyle(element);
    if (style.display === 'none' || style.visibility === 'hidden') {
      return false;
    }
    return element.getClientRects().length > 0;
  }

  function isEditableField(element) {
    if (!element || element.disabled || element.readOnly) {
      return false;
    }
    var tagName = String(element.tagName || '').toUpperCase();
    if (tagName === 'TEXTAREA' || tagName === 'SELECT') {
      return true;
    }
    if (tagName !== 'INPUT') {
      return false;
    }
    var blockedTypes = {
      hidden: true,
      checkbox: true,
      radio: true,
      file: true,
      submit: true,
      button: true,
      reset: true,
      image: true,
      range: true,
      color: true
    };
    var inputType = String(element.type || 'text').toLowerCase();
    return !blockedTypes[inputType];
  }

  function hasOpenBlockingLayer() {
    var overlays = [
      '#flashPopup',
      '#noticePopupDeck',
      '#noticePopup',
      '#imageLightbox',
      '#actionAlertPopup',
      '#actionConfirmPopup',
      '#actionPromptPopup'
    ];
    return overlays.some(function (selector) {
      var layer = document.querySelector(selector);
      if (!layer || layer.classList.contains('hidden')) {
        return false;
      }
      return isElementVisible(layer);
    });
  }

  function focusField(element) {
    if (!element) {
      return;
    }
    try {
      element.focus({ preventScroll: true });
    } catch (_) {
      element.focus();
    }
  }

  function findFirstEditableField(scope) {
    if (!scope) {
      return null;
    }
    var candidates = scope.querySelectorAll('input, textarea, select');
    for (var index = 0; index < candidates.length; index += 1) {
      var field = candidates[index];
      if (!isEditableField(field)) {
        continue;
      }
      if (!isElementVisible(field)) {
        continue;
      }
      return field;
    }
    return null;
  }

  function initPrimaryInputAutofocus() {
    if (hasOpenBlockingLayer()) {
      return;
    }
    var active = document.activeElement;
    if (active && active !== document.body && active !== document.documentElement) {
      if (isElementVisible(active)) {
        return;
      }
    }

    var explicitTarget = document.querySelector('[data-autofocus-primary]');
    if (explicitTarget && isEditableField(explicitTarget) && isElementVisible(explicitTarget)) {
      focusField(explicitTarget);
      return;
    }

    var main = document.querySelector('main');
    if (!main || !isElementVisible(main)) {
      return;
    }

    var forms = main.querySelectorAll('form');
    for (var formIndex = 0; formIndex < forms.length; formIndex += 1) {
      var form = forms[formIndex];
      if (!isElementVisible(form)) {
        continue;
      }
      var firstFieldInForm = findFirstEditableField(form);
      if (firstFieldInForm) {
        focusField(firstFieldInForm);
        return;
      }
    }

    var firstFieldInMain = findFirstEditableField(main);
    if (firstFieldInMain) {
      focusField(firstFieldInMain);
    }
  }

  function initDetailsFieldAutofocus() {
    document.addEventListener(
      'toggle',
      function (event) {
        var details = event.target;
        if (!details || details.tagName !== 'DETAILS' || !details.open) {
          return;
        }
        if (hasOpenBlockingLayer()) {
          return;
        }
        var firstField = findFirstEditableField(details);
        if (!firstField) {
          return;
        }
        window.setTimeout(function () {
          if (!details.open) {
            return;
          }
          focusField(firstField);
        }, 0);
      },
      true
    );
  }

  function initNoticePopup() {
    var popup = getNoticePopupRoot();
    if (!popup) {
      return;
    }

    var popupCards = Array.prototype.slice.call(popup.querySelectorAll('[data-popup-card][data-popup-id]'));
    if (!popupCards.length) {
      var legacyPopupId = popup.getAttribute('data-popup-id');
      if (!legacyPopupId) {
        return;
      }

      var legacyHideKey = 'chronolab-popup-hide-' + legacyPopupId;
      var legacySavedUntil = Number(localStorage.getItem(legacyHideKey) || '0');
      if (legacySavedUntil > Date.now()) {
        return;
      }
      popup.classList.remove('hidden');
      popupCards = [popup];
      popup.addEventListener('click', function (event) {
        var actionTarget = event.target.closest('[data-popup-action]');
        if (!actionTarget) {
          if (event.target === popup) {
            closeNoticePopup();
          }
          return;
        }

        var action = actionTarget.getAttribute('data-popup-action');
        var hideKey = 'chronolab-popup-hide-' + legacyPopupId;
        if (action === 'hide7') {
          localStorage.setItem(hideKey, String(Date.now() + 1000 * 60 * 60 * 24 * 7));
        }
        closeNoticePopup();
      });
      return;
    }

    var navPrev = popup.querySelector('[data-popup-nav="prev"]');
    var navNext = popup.querySelector('[data-popup-nav="next"]');
    var popupViewport = popup.querySelector('[data-popup-viewport]') || popup;
    var popupStack = popup.querySelector('[data-popup-stack]');
    var popupMeta = popup.querySelector('[data-popup-meta]');
    var popupCurrent = popup.querySelector('[data-popup-current]');
    var popupTotal = popup.querySelector('[data-popup-total]');

    function hideCardElement(card) {
      if (!card) {
        return;
      }
      card.hidden = true;
      card.classList.remove('is-active', 'is-next', 'is-next-2', 'is-previous');
    }

    function showCardElement(card) {
      if (!card) {
        return;
      }
      card.hidden = false;
    }

    var now = Date.now();
    var visibleCards = popupCards.filter(function (card) {
      var popupId = card.getAttribute('data-popup-id');
      if (!popupId) {
        hideCardElement(card);
        return false;
      }
      var hideKey = 'chronolab-popup-hide-' + popupId;
      var savedUntil = Number(localStorage.getItem(hideKey) || '0');
      var hiddenForSevenDays = savedUntil > now;
      if (hiddenForSevenDays) {
        hideCardElement(card);
        return false;
      }
      showCardElement(card);
      return true;
    });

    if (!visibleCards.length) {
      return;
    }
    popup.classList.remove('hidden');

    var currentIndex = 0;
    var wheelLockUntil = 0;
    var swipePointerId = null;
    var swipeStartX = 0;
    var swipeStartY = 0;
    var swipeDeltaX = 0;
    var swipeDeltaY = 0;
    var swipeTracking = false;

    function canGoPrev() {
      return currentIndex > 0;
    }

    function canGoNext() {
      return currentIndex < visibleCards.length - 1;
    }

    function updateDeckMeta() {
      if (popupTotal) {
        popupTotal.textContent = String(visibleCards.length);
      }
      if (popupCurrent) {
        popupCurrent.textContent = String(currentIndex + 1);
      }
      if (popupMeta) {
        popupMeta.hidden = visibleCards.length <= 1;
      }
      if (navPrev) {
        navPrev.hidden = visibleCards.length <= 1;
        navPrev.disabled = !canGoPrev();
      }
      if (navNext) {
        navNext.hidden = visibleCards.length <= 1;
        navNext.disabled = !canGoNext();
      }
    }

    function updateDeckCards() {
      visibleCards.forEach(function (card, index) {
        card.classList.remove('is-active', 'is-next', 'is-next-2', 'is-previous');
        if (index === currentIndex) {
          card.classList.add('is-active');
          var activeBody = card.querySelector('[data-popup-scrollable]');
          if (activeBody) {
            activeBody.scrollTop = 0;
          }
          return;
        }
        if (index === currentIndex + 1) {
          card.classList.add('is-next');
          return;
        }
        if (index === currentIndex + 2) {
          card.classList.add('is-next-2');
          return;
        }
        if (index < currentIndex) {
          card.classList.add('is-previous');
        }
      });
      syncActiveCardLayout();
      updateDeckMeta();
    }

    function syncDeckStackHeight() {
      if (!popupStack || !visibleCards.length) {
        return;
      }
      var activeCard = visibleCards[currentIndex];
      if (!activeCard) {
        return;
      }
      var nextHeight = Math.ceil(activeCard.getBoundingClientRect().height || activeCard.offsetHeight || 0);
      if (nextHeight > 0) {
        popupStack.style.height = String(nextHeight) + 'px';
      }
    }

    function syncScrollableBody(card) {
      if (!card) {
        return;
      }
      var scrollableBody = card.querySelector('[data-popup-scrollable]');
      if (!scrollableBody) {
        return;
      }
      scrollableBody.classList.remove('is-overflow');
      var overflowByHeight = scrollableBody.scrollHeight - scrollableBody.clientHeight > 1;
      if (overflowByHeight) {
        scrollableBody.classList.add('is-overflow');
      }
    }

    function syncActiveCardLayout() {
      requestAnimationFrame(function () {
        syncDeckStackHeight();
        syncScrollableBody(visibleCards[currentIndex]);
      });
    }

    function goPrev() {
      if (!canGoPrev()) {
        return;
      }
      currentIndex -= 1;
      updateDeckCards();
    }

    function goNext() {
      if (!canGoNext()) {
        return;
      }
      currentIndex += 1;
      updateDeckCards();
    }

    function dismissNoticeCard(card, hideForSevenDays) {
      if (!card) {
        return;
      }
      var popupId = card.getAttribute('data-popup-id');
      if (!popupId) {
        return;
      }
      var hideKey = 'chronolab-popup-hide-' + popupId;
      if (hideForSevenDays) {
        localStorage.setItem(hideKey, String(Date.now() + 1000 * 60 * 60 * 24 * 7));
      }
      hideCardElement(card);

      var removedIndex = visibleCards.indexOf(card);
      if (removedIndex !== -1) {
        visibleCards.splice(removedIndex, 1);
      }

      if (!visibleCards.length) {
        closeNoticePopup();
        return;
      }
      if (currentIndex > visibleCards.length - 1) {
        currentIndex = visibleCards.length - 1;
      }
      updateDeckCards();
    }

    popup.addEventListener('click', function (event) {
      if (event.target === popup) {
        closeNoticePopup();
      }
    });

    visibleCards.forEach(function (card) {
      var buttons = Array.prototype.slice.call(card.querySelectorAll('[data-popup-action]'));
      buttons.forEach(function (button) {
        button.addEventListener('click', function (event) {
          event.preventDefault();
          event.stopPropagation();
          var action = button.getAttribute('data-popup-action');
          if (action === 'hide7') {
            dismissNoticeCard(card, true);
            return;
          }
          if (action === 'close' || action === 'confirm') {
            dismissNoticeCard(card, false);
          }
        });
      });
    });

    if (navPrev) {
      navPrev.addEventListener('click', function (event) {
        event.preventDefault();
        goPrev();
      });
    }
    if (navNext) {
      navNext.addEventListener('click', function (event) {
        event.preventDefault();
        goNext();
      });
    }

    popupViewport.addEventListener(
      'wheel',
      function (event) {
        if (visibleCards.length <= 1) {
          return;
        }
        var contentScroller =
          event.target && typeof event.target.closest === 'function'
            ? event.target.closest('[data-popup-scrollable], .popup-notice-content')
            : null;
        if (contentScroller && !contentScroller.hasAttribute('data-popup-scrollable')) {
          contentScroller = contentScroller.closest('[data-popup-scrollable]') || contentScroller;
        }
        if (contentScroller && Math.abs(event.deltaY) >= Math.abs(event.deltaX)) {
          var maxScrollTop = contentScroller.scrollHeight - contentScroller.clientHeight;
          if (maxScrollTop > 1) {
            var isScrollingDown = event.deltaY > 0;
            var isAtTop = contentScroller.scrollTop <= 1;
            var isAtBottom = contentScroller.scrollTop >= maxScrollTop - 1;
            if ((isScrollingDown && !isAtBottom) || (!isScrollingDown && !isAtTop)) {
              return;
            }
          }
        }
        var nowMs = Date.now();
        if (nowMs < wheelLockUntil) {
          event.preventDefault();
          return;
        }
        var delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
        if (Math.abs(delta) < 20) {
          return;
        }
        event.preventDefault();
        if (delta > 0) {
          goNext();
        } else {
          goPrev();
        }
        wheelLockUntil = Date.now() + 260;
      },
      { passive: false }
    );

    popupViewport.addEventListener('pointerdown', function (event) {
      if (visibleCards.length <= 1) {
        return;
      }
      if (event.pointerType === 'mouse' && Number(event.button) !== 0) {
        return;
      }
      if (event.target.closest('[data-popup-action]') || event.target.closest('[data-popup-nav]')) {
        return;
      }
      swipePointerId = event.pointerId;
      swipeStartX = event.clientX;
      swipeStartY = event.clientY;
      swipeDeltaX = 0;
      swipeDeltaY = 0;
      swipeTracking = true;
    });

    popupViewport.addEventListener(
      'pointermove',
      function (event) {
        if (!swipeTracking || event.pointerId !== swipePointerId) {
          return;
        }
        swipeDeltaX = event.clientX - swipeStartX;
        swipeDeltaY = event.clientY - swipeStartY;
        if (Math.abs(swipeDeltaX) > Math.abs(swipeDeltaY) && Math.abs(swipeDeltaX) > 12) {
          event.preventDefault();
        }
      },
      { passive: false }
    );

    function finishSwipe() {
      if (!swipeTracking) {
        return;
      }
      var directionX = swipeDeltaX;
      var absX = Math.abs(swipeDeltaX);
      var absY = Math.abs(swipeDeltaY);
      swipeTracking = false;
      swipePointerId = null;
      swipeDeltaX = 0;
      swipeDeltaY = 0;
      if (absX < 36 || absX <= absY) {
        return;
      }
      if (directionX < 0) {
        goNext();
      } else {
        goPrev();
      }
    }

    popupViewport.addEventListener('pointerup', finishSwipe);
    popupViewport.addEventListener('pointercancel', finishSwipe);

    document.addEventListener('keydown', function (event) {
      if (popup.classList.contains('hidden')) {
        return;
      }
      if (event.key === 'ArrowLeft') {
        goPrev();
        return;
      }
      if (event.key === 'ArrowRight') {
        goNext();
        return;
      }
      if (event.key === 'Escape') {
        closeNoticePopup();
      }
    });

    visibleCards.forEach(function (card) {
      var images = card.querySelectorAll('img');
      images.forEach(function (image) {
        image.addEventListener('load', function () {
          if (card !== visibleCards[currentIndex]) {
            return;
          }
          syncActiveCardLayout();
        });
      });
    });

    window.addEventListener('resize', function () {
      if (popup.classList.contains('hidden')) {
        return;
      }
      syncActiveCardLayout();
    });

    updateDeckCards();
  }

  function initFlashPopup() {
    var popup = document.getElementById('flashPopup');
    if (!popup) {
      return;
    }
    var confirmButton = popup.querySelector('[data-flash-popup-action="confirm"]');

    popup.classList.remove('hidden');
    if (confirmButton) {
      confirmButton.focus();
    }
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

    document.addEventListener('keydown', function (event) {
      if (popup.classList.contains('hidden')) {
        return;
      }
      if (event.key === 'Enter') {
        event.preventDefault();
        if (confirmButton) {
          confirmButton.click();
          return;
        }
        closeFlashPopup();
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        closeFlashPopup();
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
      var mainWrap = gallery.querySelector('.detail-main-wrap');
      var thumbs = Array.prototype.slice.call(gallery.querySelectorAll('[data-gallery-thumb]'));
      var prevButton = gallery.querySelector('[data-gallery-prev]');
      var nextButton = gallery.querySelector('[data-gallery-next]');

      if (!mainImage || !thumbs.length) {
        return;
      }

      var currentIndex = 0;
      var touchTracking = false;
      var touchStartX = 0;
      var touchStartY = 0;
      var touchDeltaX = 0;
      var touchDeltaY = 0;
      var suppressNextClick = false;
      var SWIPE_MIN_DISTANCE = 36;
      var SWIPE_MAX_VERTICAL_DISTANCE = 88;

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

      function goPrev() {
        var nextIndex = currentIndex === 0 ? thumbs.length - 1 : currentIndex - 1;
        update(nextIndex);
      }

      function goNext() {
        var nextIndex = currentIndex === thumbs.length - 1 ? 0 : currentIndex + 1;
        update(nextIndex);
      }

      thumbs.forEach(function (thumb, index) {
        thumb.addEventListener('click', function () {
          update(index);
        });
      });

      if (prevButton) {
        prevButton.addEventListener('click', function () {
          goPrev();
        });
      }

      if (nextButton) {
        nextButton.addEventListener('click', function () {
          goNext();
        });
      }

      if (mainWrap) {
        mainWrap.addEventListener('touchstart', function (event) {
          if (!event.touches || event.touches.length !== 1) {
            touchTracking = false;
            return;
          }
          touchTracking = true;
          touchStartX = event.touches[0].clientX;
          touchStartY = event.touches[0].clientY;
          touchDeltaX = 0;
          touchDeltaY = 0;
        });

        mainWrap.addEventListener(
          'touchmove',
          function (event) {
            if (!touchTracking || !event.touches || event.touches.length !== 1) {
              return;
            }
            touchDeltaX = event.touches[0].clientX - touchStartX;
            touchDeltaY = event.touches[0].clientY - touchStartY;

            if (Math.abs(touchDeltaX) > Math.abs(touchDeltaY) && Math.abs(touchDeltaX) > 12) {
              event.preventDefault();
            }
          },
          { passive: false }
        );

        mainWrap.addEventListener('touchend', function () {
          if (!touchTracking) {
            return;
          }

          touchTracking = false;
          if (Math.abs(touchDeltaX) < SWIPE_MIN_DISTANCE) {
            return;
          }
          if (Math.abs(touchDeltaY) > SWIPE_MAX_VERTICAL_DISTANCE) {
            return;
          }
          if (Math.abs(touchDeltaX) <= Math.abs(touchDeltaY)) {
            return;
          }

          suppressNextClick = true;
          if (touchDeltaX < 0) {
            goNext();
          } else {
            goPrev();
          }

          window.setTimeout(function () {
            suppressNextClick = false;
          }, 260);
        });

        mainWrap.addEventListener('touchcancel', function () {
          touchTracking = false;
        });

        mainWrap.addEventListener('click', function (event) {
          if (!suppressNextClick) {
            return;
          }
          event.preventDefault();
          event.stopPropagation();
          suppressNextClick = false;
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

  function initInlineImagePreviews() {
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
        // Product upload uses its own watermark-preview pipeline.
        return;
      }

      if (input.classList.contains('mypage-avatar-input')) {
        // Avatar upload is an immediate-submit flow.
        return;
      }

      var wrap = document.createElement('div');
      wrap.className = 'inline-file-preview-wrap';
      wrap.hidden = true;

      var title = document.createElement('p');
      title.className = 'muted-label';
      title.textContent = isEn ? 'Image preview (click to enlarge)' : '이미지 미리보기 (클릭 시 확대)';

      var hint = document.createElement('p');
      hint.className = 'muted-label';
      hint.textContent = input.multiple
        ? (isEn ? 'Drag images to reorder. Use X to remove.' : '이미지를 드래그해 순서를 바꾸고, X로 선택 취소할 수 있습니다.')
        : (isEn ? 'Use X to remove selected image.' : 'X 버튼으로 선택한 이미지를 취소할 수 있습니다.');

      var grid = document.createElement('div');
      grid.className = 'inline-file-preview-grid';

      wrap.appendChild(title);
      wrap.appendChild(hint);
      wrap.appendChild(grid);
      input.insertAdjacentElement('afterend', wrap);

      var selectedEntries = [];
      var dragItem = null;
      var dragPointerId = null;
      var dragStartX = 0;
      var dragStartY = 0;
      var dragActivated = false;
      var lastDragFinishedAt = 0;

      function toImageFiles(fileList) {
        return Array.prototype.slice.call(fileList || []).filter(function (file) {
          return typeof file.type === 'string' && file.type.startsWith('image/');
        });
      }

      function revokeEntryUrls(entries) {
        (entries || []).forEach(function (entry) {
          if (entry && entry.url) {
            URL.revokeObjectURL(entry.url);
          }
        });
      }

      function syncInputFiles(nextEntries) {
        if (!input || typeof DataTransfer !== 'function') {
          return false;
        }
        try {
          var transfer = new DataTransfer();
          (nextEntries || []).forEach(function (entry) {
            if (entry && entry.file) {
              transfer.items.add(entry.file);
            }
          });
          input.files = transfer.files;
          return true;
        } catch (error) {
          return false;
        }
      }

      function updateOrderLabels() {
        Array.prototype.forEach.call(grid.querySelectorAll('[data-inline-preview-order]'), function (node, index) {
          node.textContent = String(index + 1);
        });
      }

      function renderPreview() {
        grid.innerHTML = '';
        if (!selectedEntries.length) {
          wrap.hidden = true;
          return;
        }

        wrap.hidden = false;
        selectedEntries.forEach(function (entry) {
          var item = document.createElement('div');
          item.className = 'watermark-preview-item inline-file-preview-item';
          item.dataset.inlinePreviewId = entry.id;

          var order = document.createElement('span');
          order.className = 'watermark-preview-order inline-file-preview-order';
          order.setAttribute('data-inline-preview-order', '1');
          order.textContent = '1';

          var removeBtn = document.createElement('button');
          removeBtn.type = 'button';
          removeBtn.className = 'watermark-preview-remove-btn inline-file-preview-remove-btn';
          removeBtn.setAttribute('data-inline-preview-remove', '1');
          removeBtn.setAttribute('data-inline-preview-remove-id', entry.id);
          removeBtn.setAttribute('aria-label', isEn ? 'Remove image' : '이미지 선택 취소');
          removeBtn.textContent = '×';

          var preview = document.createElement('img');
          preview.className = 'inline-file-preview-image zoomable-media';
          preview.setAttribute('src', entry.url);
          preview.setAttribute('alt', entry.file && entry.file.name ? entry.file.name : 'preview');

          var name = document.createElement('span');
          name.className = 'watermark-preview-name inline-file-preview-name';
          name.textContent = entry.file && entry.file.name ? entry.file.name : (isEn ? 'image' : '이미지');

          item.appendChild(order);
          item.appendChild(removeBtn);
          item.appendChild(preview);
          item.appendChild(name);
          grid.appendChild(item);
        });

        updateOrderLabels();
      }

      function resetPreview() {
        revokeEntryUrls(selectedEntries);
        selectedEntries = [];
        renderPreview();
      }

      function rebuildEntriesFromDomAndSync() {
        var orderedIds = Array.prototype.map.call(
          grid.querySelectorAll('.inline-file-preview-item'),
          function (item) {
            return String(item.dataset.inlinePreviewId || '').trim();
          }
        ).filter(Boolean);

        if (!orderedIds.length) {
          updateOrderLabels();
          return;
        }

        var entryMap = new Map(
          selectedEntries.map(function (entry) {
            return [entry.id, entry];
          })
        );

        selectedEntries = orderedIds
          .map(function (id) {
            return entryMap.get(id);
          })
          .filter(function (entry) {
            return entry && entry.file;
          });

        if (!syncInputFiles(selectedEntries)) {
          input.value = '';
          resetPreview();
          showActionAlertPopup({
            type: 'error',
            title: isEn ? 'Notice' : '안내',
            message: isEn
              ? 'Your browser does not support file reorder/removal for this input. Please reselect images.'
              : '현재 브라우저에서는 이미지 순서/삭제 반영을 지원하지 않습니다. 이미지를 다시 선택해 주세요.'
          });
          return;
        }

        updateOrderLabels();
      }

      function startDrag(event) {
        if (!input.multiple || !grid || !event || !event.target) return;
        if (event.target.closest('[data-inline-preview-remove]')) return;
        if (event.pointerType === 'mouse' && Number(event.button) !== 0) return;

        var item = event.target.closest('.inline-file-preview-item');
        if (!item || item.parentElement !== grid) return;

        dragItem = item;
        dragPointerId = event.pointerId;
        dragStartX = event.clientX;
        dragStartY = event.clientY;
        dragActivated = false;
      }

      function moveDrag(event) {
        if (!dragItem || !grid || event.pointerId !== dragPointerId) return;

        var dx = event.clientX - dragStartX;
        var dy = event.clientY - dragStartY;

        if (!dragActivated) {
          if (Math.abs(dx) < 6 && Math.abs(dy) < 6) {
            return;
          }
          dragActivated = true;
          dragItem.classList.add('is-dragging');
          dragItem.style.pointerEvents = 'none';
          if (dragItem.setPointerCapture) {
            try {
              dragItem.setPointerCapture(event.pointerId);
            } catch (error) {
              // ignore
            }
          }
        }

        dragItem.style.transform = 'translate(' + dx + 'px, ' + dy + 'px)';

        var hoverNode = document.elementFromPoint(event.clientX, event.clientY);
        var targetItem = hoverNode ? hoverNode.closest('.inline-file-preview-item') : null;
        if (!targetItem || targetItem === dragItem || targetItem.parentElement !== grid) {
          event.preventDefault();
          return;
        }

        var allItems = Array.prototype.slice.call(grid.querySelectorAll('.inline-file-preview-item'));
        var dragIndex = allItems.indexOf(dragItem);
        var targetIndex = allItems.indexOf(targetItem);
        if (dragIndex < 0 || targetIndex < 0 || dragIndex === targetIndex) {
          event.preventDefault();
          return;
        }

        var rect = targetItem.getBoundingClientRect();
        var placeAfter = event.clientX > rect.left + rect.width / 2;
        if (placeAfter) {
          grid.insertBefore(dragItem, targetItem.nextSibling);
        } else {
          grid.insertBefore(dragItem, targetItem);
        }
        updateOrderLabels();
        event.preventDefault();
      }

      function finishDrag(event) {
        if (!dragItem) return;
        if (event && typeof event.pointerId === 'number' && event.pointerId !== dragPointerId) {
          return;
        }

        if (dragActivated) {
          dragItem.classList.remove('is-dragging');
          dragItem.style.transform = '';
          dragItem.style.pointerEvents = '';
          lastDragFinishedAt = Date.now();
          rebuildEntriesFromDomAndSync();
        }

        dragItem = null;
        dragPointerId = null;
        dragActivated = false;
      }

      function removeById(id) {
        var targetId = String(id || '').trim();
        if (!targetId) return;

        var removedEntries = selectedEntries.filter(function (entry) {
          return entry && entry.id === targetId;
        });
        if (!removedEntries.length) return;

        selectedEntries = selectedEntries.filter(function (entry) {
          return entry && entry.id !== targetId;
        });
        revokeEntryUrls(removedEntries);

        if (!syncInputFiles(selectedEntries)) {
          input.value = '';
          resetPreview();
          showActionAlertPopup({
            type: 'error',
            title: isEn ? 'Notice' : '안내',
            message: isEn
              ? 'Your browser does not support file reorder/removal for this input. Please reselect images.'
              : '현재 브라우저에서는 이미지 순서/삭제 반영을 지원하지 않습니다. 이미지를 다시 선택해 주세요.'
          });
          return;
        }

        renderPreview();
      }

      grid.addEventListener('click', function (event) {
        var removeBtn = event.target.closest('[data-inline-preview-remove]');
        if (removeBtn) {
          event.preventDefault();
          event.stopPropagation();
          removeById(removeBtn.getAttribute('data-inline-preview-remove-id') || '');
          return;
        }

        if (Date.now() - lastDragFinishedAt < 320) {
          event.preventDefault();
          event.stopPropagation();
        }
      }, true);

      if (input.multiple) {
        grid.addEventListener('pointerdown', startDrag);
        grid.addEventListener('pointermove', moveDrag);
        grid.addEventListener('pointerup', finishDrag);
        grid.addEventListener('pointercancel', finishDrag);
        window.addEventListener('pointerup', finishDrag);
        window.addEventListener('pointercancel', finishDrag);
      }

      input.addEventListener('change', function () {
        revokeEntryUrls(selectedEntries);
        selectedEntries = toImageFiles(input.files).map(function (file, index) {
          return {
            id: 'inline-preview-' + Date.now() + '-' + index + '-' + Math.random().toString(36).slice(2, 8),
            file: file,
            url: URL.createObjectURL(file)
          };
        });
        renderPreview();
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
    var isOpen = false;
    var hasHistoryEntry = false;

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

    function closeLightbox(options) {
      var opts = options || {};
      if (!isOpen && lightbox.classList.contains('hidden')) {
        return;
      }

      if (!opts.fromPopstate && hasHistoryEntry) {
        hasHistoryEntry = false;
        window.history.back();
        return;
      }

      lightbox.classList.add('hidden');
      lightbox.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('lightbox-open');
      image.setAttribute('src', '');
      dragging = false;
      touchDrag = false;
      pinchZoom = false;
      isOpen = false;
      hasHistoryEntry = false;
    }

    function openLightbox(src, alt) {
      if (!src) {
        return;
      }

      if (!isOpen) {
        try {
          window.history.pushState({ __chronoLightbox: true }, '', window.location.href);
          hasHistoryEntry = true;
        } catch (error) {
          hasHistoryEntry = false;
        }
      }

      image.setAttribute('src', src);
      image.setAttribute('alt', alt || 'image');
      lightbox.classList.remove('hidden');
      lightbox.setAttribute('aria-hidden', 'false');
      document.body.classList.add('lightbox-open');
      resetTransform();
      isOpen = true;
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
      if (img.classList.contains('zoomable-media')) {
        return true;
      }
      if (img.hasAttribute('data-lightbox-enabled')) {
        return true;
      }
      return false;
    }

    function markZoomableMedia(root) {
      var scopedRoot = root && root.querySelectorAll ? root : document;
      if (scopedRoot.matches && scopedRoot.matches('img[data-lightbox-enabled]')) {
        scopedRoot.classList.add('zoomable-media');
      }
      scopedRoot.querySelectorAll('img[data-lightbox-enabled]').forEach(function (img) {
        img.classList.add('zoomable-media');
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

    window.addEventListener('popstate', function () {
      if (!isOpen) {
        return;
      }
      closeLightbox({ fromPopstate: true });
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

  function initPasswordVisibilityToggles() {
    var isEn = document.documentElement.lang === 'en';

    function getEyeSvg(isVisible) {
      if (isVisible) {
        return (
          '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true">' +
          '<path d="M1.5 12s3.75-7.5 10.5-7.5S22.5 12 22.5 12 18.75 19.5 12 19.5 1.5 12 1.5 12Z"></path>' +
          '<circle cx="12" cy="12" r="3.25"></circle>' +
          '</svg>'
        );
      }
      return (
        '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true">' +
        '<path d="M10.733 5.076A10.744 10.744 0 0 1 12 5c7 0 11 7 11 7a21.76 21.76 0 0 1-2.424 3.618"></path>' +
        '<path d="M14.084 14.158a3 3 0 0 1-4.242-4.242"></path>' +
        '<path d="m2 2 20 20"></path>' +
        '<path d="M6.71 6.708A21.758 21.758 0 0 0 1 12s4 7 11 7a10.745 10.745 0 0 0 5.294-1.292"></path>' +
        '</svg>'
      );
    }

    function bindPasswordInput(input) {
      if (!input || input.tagName !== 'INPUT') {
        return;
      }
      if (input.dataset.passwordToggleBound === '1') {
        return;
      }
      if (!input.parentNode) {
        return;
      }

      var wrapper = document.createElement('div');
      wrapper.className = 'password-field-wrap';
      input.parentNode.insertBefore(wrapper, input);
      wrapper.appendChild(input);

      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'password-toggle-btn';
      button.setAttribute('aria-label', isEn ? 'Show password' : '비밀번호 보기');
      button.setAttribute('aria-pressed', 'false');
      button.innerHTML = getEyeSvg(false);
      wrapper.appendChild(button);

      input.classList.add('with-password-toggle');
      input.dataset.passwordToggleBound = '1';

      button.addEventListener('click', function () {
        var shouldShow = input.type === 'password';
        input.type = shouldShow ? 'text' : 'password';
        button.classList.toggle('is-visible', shouldShow);
        button.setAttribute('aria-pressed', shouldShow ? 'true' : 'false');
        button.setAttribute('aria-label', shouldShow
          ? (isEn ? 'Hide password' : '비밀번호 숨기기')
          : (isEn ? 'Show password' : '비밀번호 보기'));
        button.innerHTML = getEyeSvg(shouldShow);
      });
    }

    document.querySelectorAll('input[type="password"]').forEach(bindPasswordInput);

    var observer = new MutationObserver(function (mutations) {
      mutations.forEach(function (mutation) {
        Array.prototype.forEach.call(mutation.addedNodes || [], function (node) {
          if (!node || node.nodeType !== 1) {
            return;
          }
          if (node.matches && node.matches('input[type="password"]')) {
            bindPasswordInput(node);
          }
          if (node.querySelectorAll) {
            node.querySelectorAll('input[type="password"]').forEach(bindPasswordInput);
          }
        });
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }

  function initCsrfRequestProtection() {
    var tokenMeta = document.querySelector('meta[name="csrf-token"]');
    var csrfToken = tokenMeta ? String(tokenMeta.getAttribute('content') || '').trim() : '';
    if (!csrfToken) {
      return;
    }

    function shouldProtectMethod(method) {
      var normalized = String(method || '').toUpperCase();
      return normalized === 'POST' || normalized === 'PUT' || normalized === 'PATCH' || normalized === 'DELETE';
    }

    function ensureFormToken(form) {
      if (!form || form.tagName !== 'FORM') {
        return;
      }
      var method = String(form.getAttribute('method') || 'GET').toUpperCase();
      if (!shouldProtectMethod(method)) {
        return;
      }
      var tokenInput = form.querySelector('input[name="_csrf"]');
      if (!tokenInput) {
        tokenInput = document.createElement('input');
        tokenInput.type = 'hidden';
        tokenInput.name = '_csrf';
        form.appendChild(tokenInput);
      }
      tokenInput.value = csrfToken;
    }

    document.querySelectorAll('form').forEach(ensureFormToken);

    var formObserver = new MutationObserver(function (mutations) {
      mutations.forEach(function (mutation) {
        Array.prototype.forEach.call(mutation.addedNodes || [], function (node) {
          if (!node || node.nodeType !== 1) {
            return;
          }
          if (node.matches && node.matches('form')) {
            ensureFormToken(node);
          }
          if (node.querySelectorAll) {
            node.querySelectorAll('form').forEach(ensureFormToken);
          }
        });
      });
    });
    formObserver.observe(document.body, { childList: true, subtree: true });

    if (typeof window.fetch === 'function' && window.__chronolabCsrfWrappedFetch !== true) {
      var originalFetch = window.fetch.bind(window);
      window.fetch = function (input, init) {
        var requestInit = init ? Object.assign({}, init) : {};
        var requestMethod = String(
          requestInit.method || (input && typeof input === 'object' && 'method' in input ? input.method : 'GET')
        ).toUpperCase();
        if (shouldProtectMethod(requestMethod)) {
          var headers = new Headers(
            requestInit.headers || (input && typeof input === 'object' && 'headers' in input ? input.headers : undefined)
          );
          if (!headers.has('x-csrf-token')) {
            headers.set('x-csrf-token', csrfToken);
          }
          requestInit.headers = headers;
        }
        return originalFetch(input, requestInit);
      };
      window.__chronolabCsrfWrappedFetch = true;
    }
  }

  function initDeclarativeConfirmForms() {
    var locale = getPopupLocaleConfig();
    var forms = document.querySelectorAll('form[data-confirm-message]');
    forms.forEach(function (form) {
      if (form.dataset.popupConfirmBound === '1') {
        return;
      }
      form.dataset.popupConfirmBound = '1';
      form.addEventListener('submit', function (event) {
        if (form.dataset.popupConfirmBypass === '1') {
          form.dataset.popupConfirmBypass = '';
          return;
        }
        var message = String(form.getAttribute('data-confirm-message') || '').trim();
        if (!message) {
          return;
        }
        event.preventDefault();
        var submitter = event.submitter || null;
        showActionConfirmPopup({
          title: String(form.getAttribute('data-confirm-title') || locale.confirmTitle),
          message: message,
          confirmLabel: String(form.getAttribute('data-confirm-ok-label') || locale.okLabel),
          cancelLabel: String(form.getAttribute('data-confirm-cancel-label') || locale.cancelLabel)
        }).then(function (confirmed) {
          if (!confirmed) {
            return;
          }
          form.dataset.popupConfirmBypass = '1';
          if (typeof form.requestSubmit === 'function') {
            if (submitter) {
              form.requestSubmit(submitter);
            } else {
              form.requestSubmit();
            }
            return;
          }
          form.submit();
        });
      });
    });
  }

  function initMemberSupportChat() {
    var openButton = document.querySelector('[data-member-chat-open]');
    var panel = document.querySelector('[data-member-chat-panel]');
    if (!openButton || !panel) {
      return;
    }

    var closeButton = panel.querySelector('[data-member-chat-close]');
    var messagesRoot = panel.querySelector('[data-member-chat-messages]');
    var assignedAdminLabel = panel.querySelector('[data-member-chat-assigned]');
    var form = panel.querySelector('[data-member-chat-form]');
    var textarea = form ? form.querySelector('textarea[name="message"]') : null;
    var submitButton = form ? form.querySelector('button[type="submit"]') : null;
    var unreadBadge = openButton.querySelector('[data-member-chat-unread-badge]');
    var threadId = 0;
    var isPanelOpen = false;
    var isSubmitInFlight = false;
    var lastSubmittedSignature = '';
    var lastSubmittedAt = 0;
    var pollTimerId = null;
    var unreadPollTimerId = null;
    var isLoadingThread = false;
    var PANEL_POLL_MS = 1200;
    var UNREAD_POLL_MS = 2500;

    function formatTime(rawValue) {
      var text = String(rawValue || '').trim();
      if (!text) {
        return '-';
      }
      return text.replace('T', ' ').replace(/Z$/i, '').slice(0, 16);
    }

    function setUnreadBadge(count) {
      if (!unreadBadge) {
        return;
      }
      var safeCount = Math.max(0, Number(count || 0));
      unreadBadge.textContent = safeCount > 99 ? '99+' : String(safeCount);
      unreadBadge.classList.toggle('hidden', safeCount <= 0);
    }

    function createMessageBubble(message) {
      var role = String(message && message.senderRole ? message.senderRole : '').toLowerCase();
      var isMember = role === 'member';
      var item = document.createElement('article');
      item.className = 'support-chat-msg ' + (isMember ? 'member' : 'admin');

      var body = document.createElement('div');
      body.textContent = String(message && message.messageText ? message.messageText : '');
      item.appendChild(body);

      var meta = document.createElement('small');
      meta.className = 'support-chat-msg-meta';
      meta.textContent = formatTime(message && message.createdAt ? message.createdAt : '');
      item.appendChild(meta);

      return item;
    }

    function renderMessages(messages) {
      if (!messagesRoot) {
        return;
      }
      messagesRoot.innerHTML = '';
      var list = Array.isArray(messages) ? messages : [];
      if (!list.length) {
        var empty = document.createElement('p');
        empty.className = 'support-chat-empty';
        empty.textContent = document.documentElement.lang === 'en'
          ? 'Start a chat with admin.'
          : '관리자와 상담을 시작해보세요.';
        messagesRoot.appendChild(empty);
        return;
      }
      list.forEach(function (message) {
        messagesRoot.appendChild(createMessageBubble(message));
      });
      messagesRoot.scrollTop = messagesRoot.scrollHeight;
    }

    function fetchJson(url, options) {
      return window.fetch(
        url,
        Object.assign(
          {
            headers: {
              Accept: 'application/json'
            }
          },
          options || {}
        )
      ).then(function (response) {
        return response
          .json()
          .catch(function () {
            return { ok: false };
          })
          .then(function (payload) {
            if (!response.ok || !payload || payload.ok === false) {
              throw new Error((payload && payload.message) || 'request_failed');
            }
            return payload;
          });
      });
    }

    function refreshUnreadCount() {
      fetchJson('/api/support-chat/unread-count')
        .then(function (payload) {
          setUnreadBadge(payload.unreadCount || 0);
        })
        .catch(function () {});
    }

    function loadThread() {
      if (isLoadingThread) {
        return Promise.resolve();
      }
      isLoadingThread = true;
      return fetchJson('/api/support-chat/thread')
        .then(function (payload) {
          var nextThreadId = Number(payload && payload.thread ? payload.thread.id : 0);
          threadId = Number.isFinite(nextThreadId) ? nextThreadId : 0;
          if (assignedAdminLabel) {
            assignedAdminLabel.textContent = document.documentElement.lang === 'en'
              ? 'Assigned: Chrono Lab Admin'
              : '담당: Chrono Lab 관리자';
          }
          renderMessages(payload.messages || []);
          setUnreadBadge(payload.unreadCount || 0);
        })
        .catch(function () {})
        .finally(function () {
          isLoadingThread = false;
        });
    }

    function stopPanelPolling() {
      if (pollTimerId) {
        window.clearInterval(pollTimerId);
        pollTimerId = null;
      }
    }

    function startPanelPolling() {
      stopPanelPolling();
      pollTimerId = window.setInterval(function () {
        if (!isPanelOpen) {
          return;
        }
        loadThread();
      }, PANEL_POLL_MS);
    }

    function openPanel() {
      isPanelOpen = true;
      panel.classList.remove('hidden');
      loadThread().then(function () {
        if (textarea) {
          textarea.focus();
        }
      });
      startPanelPolling();
    }

    function closePanel() {
      isPanelOpen = false;
      panel.classList.add('hidden');
      stopPanelPolling();
    }

    openButton.addEventListener('click', function () {
      if (isPanelOpen) {
        closePanel();
        return;
      }
      openPanel();
    });

    if (closeButton) {
      closeButton.addEventListener('click', closePanel);
    }

    if (form && textarea) {
      textarea.addEventListener('keydown', function (event) {
        if (event.isComposing || Number(event.keyCode) === 229 || event.repeat) {
          return;
        }
        if (event.key === 'Enter' && !event.shiftKey) {
          event.preventDefault();
          if (isSubmitInFlight) {
            return;
          }
          if (typeof form.requestSubmit === 'function') {
            form.requestSubmit();
          } else {
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
          }
        }
      });

      form.addEventListener('submit', function (event) {
        event.preventDefault();
        if (isSubmitInFlight) {
          return;
        }
        var messageText = String(textarea.value || '').trim();
        if (!messageText) {
          return;
        }
        var submitSignature = String(threadId || 0) + '::' + messageText;
        var now = Date.now();
        if (submitSignature === lastSubmittedSignature && now - lastSubmittedAt < 1200) {
          return;
        }
        lastSubmittedSignature = submitSignature;
        lastSubmittedAt = now;
        isSubmitInFlight = true;
        if (submitButton) {
          submitButton.disabled = true;
        }
        fetchJson('/api/support-chat/message', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ message: messageText })
        })
          .then(function () {
            textarea.value = '';
            return loadThread();
          })
          .catch(function () {})
          .finally(function () {
            isSubmitInFlight = false;
            if (submitButton) {
              submitButton.disabled = false;
            }
          });
      });
    }

    unreadPollTimerId = window.setInterval(function () {
      if (isPanelOpen) {
        return;
      }
      refreshUnreadCount();
    }, UNREAD_POLL_MS);

    window.addEventListener('focus', function () {
      if (isPanelOpen) {
        loadThread();
        return;
      }
      refreshUnreadCount();
    });

    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState !== 'visible') {
        return;
      }
      if (isPanelOpen) {
        loadThread();
        return;
      }
      refreshUnreadCount();
    });

    refreshUnreadCount();
  }

  function initLoginRequiredActions() {
    var htmlLang = String(document.documentElement.lang || 'ko').toLowerCase();
    var isEnglish = htmlLang.indexOf('en') === 0;
    var isChinese = htmlLang.indexOf('zh') === 0;
    var loginRequiredTitle = isEnglish ? 'Login Required' : isChinese ? '需要登录' : '로그인 필요';
    var infoTitle = isEnglish ? 'Information' : isChinese ? '通知' : '안내';
    var loginRequiredMessage = isEnglish
      ? 'Login is required. You will be redirected to the login page.'
      : isChinese
      ? '此功能需要登录。将跳转到登录页面。'
      : '로그인이 필요한 기능입니다. 로그인 페이지로 이동합니다.';
    var kakaoUnavailableMessage = isEnglish
      ? 'Kakao channel is not configured yet.'
      : isChinese
      ? 'Kakao 频道地址尚未设置。'
      : '카카오톡 채널 주소가 아직 설정되지 않았습니다.';
    var loginRequiredButtons = document.querySelectorAll('[data-login-required-action]');
    loginRequiredButtons.forEach(function (button) {
      button.addEventListener('click', function (event) {
        event.preventDefault();
        showActionAlertPopup({
          type: 'error',
          title: loginRequiredTitle,
          message: loginRequiredMessage,
          confirmLabel: isEnglish ? 'OK' : isChinese ? '确定' : '확인'
        }).then(function (confirmed) {
          if (confirmed) {
            window.location.assign('/login');
          }
        });
      });
    });
    var kakaoUnavailableButtons = document.querySelectorAll('[data-kakao-chat-unavailable]');
    kakaoUnavailableButtons.forEach(function (button) {
      button.addEventListener('click', function (event) {
        event.preventDefault();
        showActionAlertPopup({
          type: 'success',
          title: infoTitle,
          message: kakaoUnavailableMessage,
          confirmLabel: isEnglish ? 'OK' : isChinese ? '确定' : '확인'
        });
      });
    });
  }

  function initAdminSupportChat() {
    var openButtons = Array.prototype.slice.call(document.querySelectorAll('[data-admin-chat-open]'));
    var panel = document.querySelector('[data-admin-chat-panel]');
    if (!openButtons.length || !panel) {
      return;
    }

    var closeButton = panel.querySelector('[data-admin-chat-close]');
    var threadListRoot = panel.querySelector('[data-admin-chat-thread-list]');
    var threadTitleRoot = panel.querySelector('[data-admin-chat-thread-title]');
    var messagesRoot = panel.querySelector('[data-admin-chat-messages]');
    var form = panel.querySelector('[data-admin-chat-form]');
    var textarea = form ? form.querySelector('textarea[name="message"]') : null;
    var submitButton = form ? form.querySelector('button[type="submit"]') : null;
    var floatingUnreadBadge = document.querySelector('[data-admin-chat-unread-badge]');

    var threads = [];
    var selectedThreadId = 0;
    var isPanelOpen = false;
    var isSubmitInFlight = false;
    var lastSubmittedSignature = '';
    var lastSubmittedAt = 0;
    var threadPollTimerId = null;
    var unreadPollTimerId = null;
    var isRefreshingThreads = false;
    var PANEL_POLL_MS = 1200;
    var UNREAD_POLL_MS = 2500;

    function formatTime(rawValue) {
      var text = String(rawValue || '').trim();
      if (!text) {
        return '-';
      }
      return text.replace('T', ' ').replace(/Z$/i, '').slice(0, 16);
    }

    function setUnreadBadges(count) {
      var safeCount = Math.max(0, Number(count || 0));
      var textValue = safeCount > 99 ? '99+' : String(safeCount);
      [floatingUnreadBadge].forEach(function (badge) {
        if (!badge) {
          return;
        }
        badge.textContent = textValue;
        badge.classList.toggle('hidden', safeCount <= 0);
      });
    }

    function fetchJson(url, options) {
      return window.fetch(
        url,
        Object.assign(
          {
            headers: {
              Accept: 'application/json'
            }
          },
          options || {}
        )
      ).then(function (response) {
        return response
          .json()
          .catch(function () {
            return { ok: false };
          })
          .then(function (payload) {
            if (!response.ok || !payload || payload.ok === false) {
              throw new Error((payload && payload.message) || 'request_failed');
            }
            return payload;
          });
      });
    }

    function renderThreadTitle(thread) {
      if (!threadTitleRoot) {
        return;
      }
      threadTitleRoot.innerHTML = '';
      if (!thread || !thread.id) {
        var emptyTitle = document.createElement('strong');
        emptyTitle.textContent = document.documentElement.lang === 'en'
          ? 'Select a member chat'
          : '회원 채팅을 선택하세요';
        threadTitleRoot.appendChild(emptyTitle);
        return;
      }

      var titleStrong = document.createElement('strong');
      var name = thread.memberNickname || thread.memberFullName || thread.memberUsername || '-';
      titleStrong.textContent = name;
      var subtitle = document.createElement('small');
      subtitle.textContent = (document.documentElement.lang === 'en' ? 'Account' : '계정') + ': ' + (thread.memberUsername || '-');
      threadTitleRoot.appendChild(titleStrong);
      threadTitleRoot.appendChild(subtitle);
    }

    function renderMessages(messages) {
      if (!messagesRoot) {
        return;
      }
      messagesRoot.innerHTML = '';
      var list = Array.isArray(messages) ? messages : [];
      if (!list.length) {
        var empty = document.createElement('p');
        empty.className = 'support-chat-empty';
        empty.textContent = document.documentElement.lang === 'en'
          ? 'No messages yet.'
          : '아직 메시지가 없습니다.';
        messagesRoot.appendChild(empty);
        return;
      }
      list.forEach(function (message) {
        var role = String(message && message.senderRole ? message.senderRole : '').toLowerCase();
        var item = document.createElement('article');
        item.className = 'support-chat-msg ' + (role === 'admin' ? 'member' : 'admin');
        var body = document.createElement('div');
        body.textContent = String(message && message.messageText ? message.messageText : '');
        item.appendChild(body);
        var meta = document.createElement('small');
        meta.className = 'support-chat-msg-meta';
        meta.textContent = formatTime(message && message.createdAt ? message.createdAt : '');
        item.appendChild(meta);
        messagesRoot.appendChild(item);
      });
      messagesRoot.scrollTop = messagesRoot.scrollHeight;
    }

    function setComposerEnabled(enabled) {
      var canUse = Boolean(enabled && selectedThreadId > 0);
      if (textarea) {
        textarea.disabled = !canUse;
      }
      if (submitButton) {
        submitButton.disabled = !canUse;
      }
    }

    function renderThreadList() {
      if (!threadListRoot) {
        return;
      }
      threadListRoot.innerHTML = '';
      if (!threads.length) {
        var empty = document.createElement('p');
        empty.className = 'support-chat-empty';
        empty.textContent = document.documentElement.lang === 'en'
          ? 'No member chats yet.'
          : '아직 회원 채팅이 없습니다.';
        threadListRoot.appendChild(empty);
        return;
      }
      threads.forEach(function (thread) {
        var button = document.createElement('button');
        button.type = 'button';
        button.className = 'support-admin-thread-item' + (thread.id === selectedThreadId ? ' active' : '');
        button.setAttribute('data-thread-id', String(thread.id));

        var head = document.createElement('div');
        head.className = 'support-admin-thread-item-head';
        var strong = document.createElement('strong');
        strong.textContent = thread.memberUsername || '-';
        head.appendChild(strong);
        if (Number(thread.unreadCount || 0) > 0) {
          var badge = document.createElement('span');
          badge.className = 'support-admin-thread-unread';
          badge.textContent = Number(thread.unreadCount) > 99 ? '99+' : String(thread.unreadCount);
          head.appendChild(badge);
        }
        button.appendChild(head);

        var time = document.createElement('p');
        time.className = 'support-admin-thread-time';
        time.textContent = formatTime(
          thread.lastMemberMessageAt || thread.lastMessageAt || thread.updatedAt || thread.createdAt
        );
        button.appendChild(time);

        threadListRoot.appendChild(button);
      });
    }

    function refreshThreads() {
      if (isRefreshingThreads) {
        return Promise.resolve();
      }
      isRefreshingThreads = true;
      return fetchJson('/api/admin/support-chat/threads')
        .then(function (payload) {
          threads = Array.isArray(payload.threads) ? payload.threads : [];
          setUnreadBadges(payload.unreadCount || 0);
          if (!threads.some(function (item) { return item.id === selectedThreadId; })) {
            selectedThreadId = threads.length ? Number(threads[0].id || 0) : 0;
          }
          renderThreadList();
          if (selectedThreadId > 0) {
            return loadThreadMessages(selectedThreadId);
          }
          renderThreadTitle(null);
          renderMessages([]);
          setComposerEnabled(false);
          return null;
        })
        .catch(function () {
          return null;
        })
        .finally(function () {
          isRefreshingThreads = false;
        });
    }

    function loadThreadMessages(threadId) {
      var safeId = Number(threadId || 0);
      if (!safeId) {
        return Promise.resolve();
      }
      return fetchJson('/api/admin/support-chat/thread/' + encodeURIComponent(String(safeId)) + '/messages')
        .then(function (payload) {
          var thread = payload.thread || null;
          renderThreadTitle(thread);
          renderMessages(payload.messages || []);
          setUnreadBadges(payload.unreadCount || 0);
          setComposerEnabled(true);
        })
        .catch(function () {});
    }

    function refreshUnreadOnly() {
      fetchJson('/api/admin/support-chat/unread-count')
        .then(function (payload) {
          setUnreadBadges(payload.unreadCount || 0);
        })
        .catch(function () {});
    }

    function stopPanelPolling() {
      if (threadPollTimerId) {
        window.clearInterval(threadPollTimerId);
        threadPollTimerId = null;
      }
    }

    function startPanelPolling() {
      stopPanelPolling();
      threadPollTimerId = window.setInterval(function () {
        if (!isPanelOpen) {
          return;
        }
        refreshThreads();
      }, PANEL_POLL_MS);
    }

    function openPanel() {
      isPanelOpen = true;
      panel.classList.remove('hidden');
      refreshThreads().then(function () {
        if (textarea && !textarea.disabled) {
          textarea.focus();
        }
      });
      startPanelPolling();
    }

    function closePanel() {
      isPanelOpen = false;
      panel.classList.add('hidden');
      stopPanelPolling();
    }

    openButtons.forEach(function (button) {
      button.addEventListener('click', function () {
        if (isPanelOpen) {
          closePanel();
          return;
        }
        openPanel();
      });
    });

    if (closeButton) {
      closeButton.addEventListener('click', closePanel);
    }

    if (threadListRoot) {
      threadListRoot.addEventListener('click', function (event) {
        var target = event.target.closest('[data-thread-id]');
        if (!target) {
          return;
        }
        var nextThreadId = Number(target.getAttribute('data-thread-id') || 0);
        if (!nextThreadId || nextThreadId === selectedThreadId) {
          return;
        }
        selectedThreadId = nextThreadId;
        renderThreadList();
        loadThreadMessages(selectedThreadId);
      });
    }

    if (form && textarea) {
      textarea.addEventListener('keydown', function (event) {
        if (event.isComposing || Number(event.keyCode) === 229 || event.repeat) {
          return;
        }
        if (event.key === 'Enter' && !event.shiftKey) {
          event.preventDefault();
          if (isSubmitInFlight) {
            return;
          }
          if (typeof form.requestSubmit === 'function') {
            form.requestSubmit();
          } else {
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
          }
        }
      });

      form.addEventListener('submit', function (event) {
        event.preventDefault();
        if (isSubmitInFlight) {
          return;
        }
        var messageText = String(textarea.value || '').trim();
        if (!messageText || !selectedThreadId) {
          return;
        }
        var submitSignature = String(selectedThreadId || 0) + '::' + messageText;
        var now = Date.now();
        if (submitSignature === lastSubmittedSignature && now - lastSubmittedAt < 1200) {
          return;
        }
        lastSubmittedSignature = submitSignature;
        lastSubmittedAt = now;
        isSubmitInFlight = true;
        if (submitButton) {
          submitButton.disabled = true;
        }
        fetchJson('/api/admin/support-chat/thread/' + encodeURIComponent(String(selectedThreadId)) + '/message', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ message: messageText })
        })
          .then(function () {
            textarea.value = '';
            return loadThreadMessages(selectedThreadId).then(function () {
              return refreshThreads();
            });
          })
          .catch(function () {})
          .finally(function () {
            isSubmitInFlight = false;
            if (submitButton) {
              submitButton.disabled = false;
            }
          });
      });
    }

    unreadPollTimerId = window.setInterval(function () {
      if (isPanelOpen) {
        return;
      }
      refreshUnreadOnly();
    }, UNREAD_POLL_MS);

    window.addEventListener('focus', function () {
      if (isPanelOpen) {
        refreshThreads();
        return;
      }
      refreshUnreadOnly();
    });

    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState !== 'visible') {
        return;
      }
      if (isPanelOpen) {
        refreshThreads();
        return;
      }
      refreshUnreadOnly();
    });
    refreshUnreadOnly();
  }

  function initApp() {
    registerChronoPopupApi();
    initCsrfRequestProtection();
    initDeclarativeConfirmForms();
    initDetailsFieldAutofocus();
    initNoticePopup();
    initFlashPopup();
    initProductGallery();
    initInlineImagePreviews();
    initImageLightbox();
    initPasswordVisibilityToggles();
    initLoginRequiredActions();
    initMemberSupportChat();
    initAdminSupportChat();
    initPrimaryInputAutofocus();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})();
