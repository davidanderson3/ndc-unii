/* Minimal serverless browser for ndc_unii_rxnorm chunked data */
(function(){
  const $ = (sel) => document.querySelector(sel);
  const qEl = $('#q');
  const statusEl = $('#status');
  const resultsEl = $('#results');
  const searchBtn = document.getElementById('searchBtn');

  // Cache of loaded buckets
  const cache = new Map(); // bucket -> array of records
  let bucketIndex = null;  // { bucket_size: 'first3digits', buckets: { '123': count, ... } }

  // Utilities
  const digits = (s) => (s||'').replace(/\D+/g, '');
  const bucketOf = (ndcOrQueryDigits) => (ndcOrQueryDigits || '').slice(0,3);
  const isNumeric = (s) => /^[0-9]+$/.test(s || '');

  // URL query sync
  function getParam(name){
    const u = new URL(window.location.href);
    return u.searchParams.get(name) || '';
  }
  function setParam(name, value){
    const u = new URL(window.location.href);
    if (value) u.searchParams.set(name, value); else u.searchParams.delete(name);
    history.replaceState(null, '', u.toString());
  }

  function setStatus(txt){ statusEl.textContent = txt || ''; }

  async function loadIndex(){
    if (bucketIndex) return bucketIndex;
    try {
      const res = await fetch('data/index.json', { cache: 'no-store' });
      if(!res.ok) throw new Error('index load failed');
      bucketIndex = await res.json();
      return bucketIndex;
    } catch (e){
      setStatus('No data/index.json found');
      throw e;
    }
  }

  async function loadBucket(bucket){
    if (cache.has(bucket)) return cache.get(bucket);
    setStatus(`Loading ${bucket}…`);
    const url = `data/ndc_${bucket}.json`;
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if(!res.ok) throw new Error('bucket not found');
      const data = await res.json();
      cache.set(bucket, data);
      setStatus(`${data.length} records`);
      return data;
    } catch (e) {
      cache.set(bucket, []);
      setStatus('0 records');
      return [];
    }
  }

  // Removed full-scan helpers since we now support NDC-only search

  function render(records, query){
    resultsEl.innerHTML = '';
    if (!records || records.length === 0){
      const div = document.createElement('div');
      div.className = 'empty';
      div.textContent = 'No matches.';
      resultsEl.appendChild(div);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const rec of records){
      const card = document.createElement('div');
      card.className = 'card';

      // Title row: drug name first
      const titleRow = document.createElement('div');
      titleRow.className = 'row';
      const nameEl = document.createElement('div');
      nameEl.className = 'name';
      nameEl.textContent = rec.str || '';
      titleRow.appendChild(nameEl);
      card.appendChild(titleRow);

      // Details rows: NDC and TTY
      const ndcLine = document.createElement('div');
      ndcLine.className = 'kv';
      ndcLine.innerHTML = `<strong>NDC</strong>: <span class="ndc">${rec.ndc}</span>`;
      card.appendChild(ndcLine);
      const ttyLine = document.createElement('div');
      ttyLine.className = 'kv';
      ttyLine.innerHTML = `<strong>TTY</strong>: <span class="tty">${rec.tty || ''}</span>`;
      card.appendChild(ttyLine);

      // Ingredient sections by TTY with flags and UNII
      const ingredients = Array.isArray(rec.ingredients) ? rec.ingredients : [];
      const byIN  = ingredients.filter(i => i && i.tty === 'IN');
      const byPIN = ingredients.filter(i => i && i.tty === 'PIN');

      function appendTTYSection(label, list){
        if (!list || !list.length) return;
        const lab = document.createElement('div');
        lab.className = 'section-label';
        lab.innerHTML = `<strong>${label}</strong>`;
        card.appendChild(lab);
        for (const ing of list){
          const block = document.createElement('div');
          block.className = 'ing-block';
          const name = document.createElement('div');
          name.className = 'ing-name';
          name.innerHTML = `<strong>${ing.str || ''}</strong>`;
          block.appendChild(name);
          const flags = document.createElement('div');
          flags.className = 'flags-list';
          const ck = (b, text) => `<div>${b ? '✅' : '❌'} ${text}</div>`;
          flags.innerHTML = [
            ck(!!ing.active_ingredient, 'active ingredient'),
            ck(!!ing.active_moiety, 'active moiety'),
            ck(!!ing.basis_of_strength, 'basis of strength substance')
          ].join('');
          block.appendChild(flags);
          const unii = document.createElement('div');
          unii.className = 'unii';
          const uniiVal = ing.unii ? ing.unii : '-';
          unii.innerHTML = `<strong>UNII</strong>: ${uniiVal}`;
          block.appendChild(unii);
          card.appendChild(block);
        }
      }

      appendTTYSection('Ingredient (IN):', byIN);
      appendTTYSection('Precise Ingredient (PIN):', byPIN);

      frag.appendChild(card);
    }
    resultsEl.appendChild(frag);
  }

  function filterRecords(records, query){
    if (!query) return [];
    const qd = digits(query.trim());
    if (!qd) return [];
    return records.filter(rec => digits(rec.ndc).includes(qd));
  }

  async function onInput(){
    const query = qEl.value || '';
    setParam('q', query);
    const qTrim = query.trim();
    const qd = digits(qTrim);

    if (!qd){
      setStatus('Ready');
      resultsEl.innerHTML = '<div class="hint">Enter a normalized (11-digit) NDC. Type 3+ digits to begin.</div>';
      return;
    }

    if (qd.length < 3){
      setStatus('Type 3+ digits');
      resultsEl.innerHTML = '<div class="hint">Enter at least the first 3 NDC digits.</div>';
      return;
    }

    await loadIndex();
    const bucket = bucketOf(qd);
    const data = await loadBucket(bucket);
    const filtered = filterRecords(data, qTrim);
    setStatus(`${filtered.length} matches`);
    render(filtered, qTrim);
  }

  qEl.addEventListener('input', debounce(onInput, 150));
  if (searchBtn){ searchBtn.addEventListener('click', () => onInput()); }

  // Basic debounce
  function debounce(fn, ms){
    let t; return function(){ clearTimeout(t); t = setTimeout(() => fn.apply(this, arguments), ms); };
  }

  // Initial state: restore from URL if present
  const initialQ = getParam('q');
  if (initialQ){
    qEl.value = initialQ;
    onInput();
  } else {
    setStatus('Ready');
    resultsEl.innerHTML = '<div class="hint">Provide chunks in web/data via the build script, then search by NDC.</div>';
  }

  window.addEventListener('popstate', () => {
    const q = getParam('q');
    if (q !== qEl.value){
      qEl.value = q;
      onInput();
    }
  });
})();
