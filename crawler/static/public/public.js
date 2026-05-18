'use strict';

// ── Autocomplete ─────────────────────────────────────────────────────────────

function initAutocomplete(inputId, dropdownId, formId) {
  const input = document.getElementById(inputId);
  const dropdown = document.getElementById(dropdownId);
  if (!input || !dropdown) return;

  let timer = null;
  let activeIndex = -1;
  let items = [];

  function show(results) {
    items = results;
    activeIndex = -1;
    dropdown.innerHTML = '';
    if (!results.length) { dropdown.classList.add('hidden'); return; }

    results.forEach((r, i) => {
      const div = document.createElement('div');
      div.className = 'autocomplete-item';
      div.setAttribute('role', 'option');
      div.innerHTML = `
        <div class="ac-icon ${r.type}">
          <i class="fa-solid ${r.type === 'job' ? 'fa-briefcase' : 'fa-building'}"></i>
        </div>
        <div class="min-w-0">
          <div class="ac-label">${escHtml(r.label)}</div>
          ${r.sub ? `<div class="ac-sub">${escHtml(r.sub)}</div>` : ''}
        </div>`;
      div.addEventListener('mousedown', (e) => {
        e.preventDefault();
        select(r, i);
      });
      dropdown.appendChild(div);
    });
    dropdown.classList.remove('hidden');
  }

  function hide() {
    dropdown.classList.add('hidden');
    activeIndex = -1;
  }

  function highlight(idx) {
    const els = dropdown.querySelectorAll('.autocomplete-item');
    els.forEach((el, i) => el.classList.toggle('active', i === idx));
    activeIndex = idx;
  }

  function select(result) {
    input.value = result.label;
    hide();
    const form = document.getElementById(formId);
    if (form) form.submit();
  }

  input.addEventListener('input', () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (q.length < 2) { hide(); return; }
    timer = setTimeout(() => {
      fetch(`/api/autocomplete?q=${encodeURIComponent(q)}`)
        .then(r => r.ok ? r.json() : [])
        .then(show)
        .catch(() => hide());
    }, 250);
  });

  input.addEventListener('keydown', (e) => {
    const els = dropdown.querySelectorAll('.autocomplete-item');
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      highlight(Math.min(activeIndex + 1, els.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      highlight(Math.max(activeIndex - 1, -1));
    } else if (e.key === 'Enter' && activeIndex >= 0) {
      e.preventDefault();
      select(items[activeIndex]);
    } else if (e.key === 'Escape') {
      hide();
    }
  });

  input.addEventListener('blur', () => setTimeout(hide, 150));
  input.addEventListener('focus', () => {
    if (input.value.trim().length >= 2 && items.length) dropdown.classList.remove('hidden');
  });
}

// ── Mobile nav toggle ────────────────────────────────────────────────────────

function initMobileNav() {
  const btn = document.getElementById('mobile-menu-btn');
  const menu = document.getElementById('mobile-menu');
  if (!btn || !menu) return;
  btn.addEventListener('click', () => menu.classList.toggle('hidden'));
}

// ── Subscribe modal ──────────────────────────────────────────────────────────

function initSubscribeModal() {
  const modal = document.getElementById('subscribe-modal');
  if (!modal) return;

  function open() {
    modal.classList.remove('hidden');
    modal.classList.add('flex');
    document.body.style.overflow = 'hidden';
  }
  function close() {
    modal.classList.add('hidden');
    modal.classList.remove('flex');
    document.body.style.overflow = '';
  }

  document.querySelectorAll('[data-open-subscribe]').forEach(el => el.addEventListener('click', open));
  document.getElementById('subscribe-modal-close')?.addEventListener('click', close);
  modal.addEventListener('click', (e) => { if (e.target === modal) close(); });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });

  // Modal form submit
  const modalForm = document.getElementById('modal-subscribe-form');
  const modalMsg = document.getElementById('modal-subscribe-msg');
  if (modalForm) {
    modalForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const data = new FormData(modalForm);
      await submitSubscribe(data, modalMsg, modalForm);
    });
  }

  // Footer form submit
  const footerForm = document.getElementById('footer-subscribe-form');
  const footerMsg = document.getElementById('footer-subscribe-msg');
  if (footerForm) {
    footerForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const data = new FormData(footerForm);
      await submitSubscribe(data, footerMsg, footerForm);
    });
  }
}

async function submitSubscribe(formData, msgEl, formEl) {
  try {
    const res = await fetch('/subscribe', { method: 'POST', body: formData });
    const json = await res.json();
    if (json.ok) {
      showMsg(msgEl, '✓ You\'re subscribed! We\'ll notify you of new local jobs.', 'ok');
      formEl.reset();
    } else {
      showMsg(msgEl, json.error || 'Something went wrong. Please try again.', 'err');
    }
  } catch {
    showMsg(msgEl, 'Network error. Please try again.', 'err');
  }
}

function showMsg(el, text, type) {
  if (!el) return;
  el.textContent = text;
  el.className = `mt-2 text-sm ${type === 'ok' ? 'text-green-600' : 'text-red-600'}`;
  el.classList.remove('hidden');
}

// ── Utility ──────────────────────────────────────────────────────────────────

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Init ─────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  initAutocomplete('hero-search', 'hero-autocomplete', 'hero-search-form');
  initAutocomplete('jobs-search', 'jobs-autocomplete', 'jobs-search-form');
  initMobileNav();
  initSubscribeModal();
});
