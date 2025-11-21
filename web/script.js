/* Minimal serverless browser for ndc_unii_rxnorm chunked data */
(function(){
  const $ = (sel) => document.querySelector(sel);
  const qEl = $('#q');
  const statusEl = $('#status');
  const spinnerEl = $('#spinner');
  const resultsEl = $('#results');
  const searchBtn = document.getElementById('searchBtn');

  // Cache of loaded buckets
  const cache = new Map(); // bucket -> array of records
  let bucketIndex = null;  // { bucket_size: 'first3digits', buckets: { '123': count, ... } }
  let searchIndex = null;  // { records: [ { bucket, ndc, rxcui, name, unii: [] }, ... ] }

  // Utilities
  const digits = (s) => (s||'').replace(/\D+/g, '');
  const bucketOf = (ndcOrQueryDigits) => (ndcOrQueryDigits || '').slice(0,3);
  const isNumeric = (s) => /^[0-9]+$/.test(s || '');
  const tokens = (s) => (s || '').toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);

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
  function setLoading(on){ if (spinnerEl){ spinnerEl.classList.toggle('show', !!on); } }

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

  async function loadSearchIndex(){
    if (searchIndex) return searchIndex;
    const res = await fetch('data/search_index.json', { cache: 'no-store' });
    if(!res.ok) throw new Error('search_index load failed');
    searchIndex = await res.json();
    return searchIndex;
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

  function groupRecords(records){
    const merged = new Map();
    for (const rec of records || []){
      const key = JSON.stringify([rec.str || '', rec.tty || '', rec.rxcui || '', rec.ingredients || []]);
      if (!merged.has(key)){
        merged.set(key, { base: rec, ndcs: [] });
      }
      merged.get(key).ndcs.push(rec.ndc);
    }
    return Array.from(merged.values(), ({ base, ndcs }) => ({
      ...base,
      ndcs: ndcs.sort(),
    }));
  }

  function render(records, query){
    resultsEl.innerHTML = '';
    if (!records || records.length === 0){
      const div = document.createElement('div');
      div.className = 'empty';
      div.textContent = 'No matches.';
      resultsEl.appendChild(div);
      return;
    }
    const grouped = groupRecords(records);
    const frag = document.createDocumentFragment();
    for (const rec of grouped){
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
      const ndcList = rec.ndcs && rec.ndcs.length ? rec.ndcs.join(', ') : rec.ndc;
      ndcLine.innerHTML = `<strong>NDC</strong>: <span class="ndc">${ndcList}</span>`;
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
    const q = query.trim().toLowerCase();
    const qd = digits(q);
    const qdOk = qd.length >= 3;
    const hasLetters = /[a-z]/i.test(q);
    const qTokens = tokens(q);
    return records.filter(rec => {
      const ndcDigits = digits(rec.ndc);
      const name = (rec.str || '').toLowerCase();
      const rxcui = (rec.rxcui || '').toString().toLowerCase();
      const uniis = new Set();
      for (const ing of rec.ingredients || []){
        if (ing && ing.unii){
          uniis.add(String(ing.unii).toLowerCase());
        }
      }
      const hasUNII = uniis.has(q);
      const ndcMatch = (!hasLetters && qdOk) ? ndcDigits.includes(qd) : false;
      const nameMatch = qTokens.length ? qTokens.every(t => name.includes(t)) : name.includes(q);
      const rxcuiMatch = rxcui && rxcui.includes(q);
      return ndcMatch || nameMatch || hasUNII || rxcuiMatch;
    });
  }

  async function onInput(){
    const query = qEl.value || '';
    setParam('q', query);
    const qTrim = query.trim();
    const qd = digits(qTrim);
    const hasLetters = /[a-z]/i.test(qTrim);
    setLoading(true);

    try {
      if (!qd && !hasLetters){
        setStatus('Ready');
        resultsEl.innerHTML = '<div class="hint">Enter an NDC (3+ digits) or search by name, RxCUI, or UNII.</div>';
        return;
      }

      // NDC-only search path
      if (!hasLetters && qd.length > 0 && qd.length < 3){
        setStatus('Type 3+ digits');
        resultsEl.innerHTML = '<div class="hint">Enter at least the first 3 NDC digits.</div>';
        return;
      }

      await loadIndex();

      // Try search index first (covers name/RxCUI/UNII and numeric RxCUIs);
      // fall back to NDC bucket if nothing found and query looks like NDC digits.
      let bucketsToLoad = new Set();
      try {
        const idx = await loadSearchIndex();
        const qLower = qTrim.toLowerCase();
        const qTokens = tokens(qLower);
        for (const rec of idx.records || []){
          const name = (rec.name || '').toLowerCase();
          const rxcui = (rec.rxcui || '').toString().toLowerCase();
          const uniis = (rec.unii || []).map(u => String(u).toLowerCase());
          const ndcDigits = digits(rec.ndc);
          const ndcMatch = (!hasLetters && qd.length >= 3) ? ndcDigits.includes(qd) : false;
          const nameMatch = qTokens.length ? qTokens.every(t => name.includes(t)) : (qLower && name.includes(qLower));
          const rxcuiMatch = qLower && rxcui.includes(qLower);
          const uniiMatch = qLower && uniis.includes(qLower);
          if (ndcMatch || nameMatch || rxcuiMatch || uniiMatch){
            if (rec.bucket) bucketsToLoad.add(rec.bucket);
          }
        }
      } catch (e){
        setStatus('Search index missing; ensure web/data/search_index.json exists');
        render([], qTrim);
        return;
      }

      if (bucketsToLoad.size === 0 && !hasLetters && qd.length >= 3){
        bucketsToLoad.add(bucketOf(qd));
      }

      if (bucketsToLoad.size === 0){
        setStatus('0 matches');
        render([], qTrim);
        return;
      }

      const combined = [];
      for (const b of bucketsToLoad){
        const data = await loadBucket(b);
        combined.push(...data);
      }

      const filtered = filterRecords(combined, qTrim);
      setStatus(`${filtered.length} matches`);
      render(filtered, qTrim);
    } finally {
      setLoading(false);
    }
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
    resultsEl.innerHTML = '<div class="hint">Provide chunks in web/data via the build script, then search by NDC, name, RxCUI, or UNII.</div>';
  }

  window.addEventListener('popstate', () => {
    const q = getParam('q');
    if (q !== qEl.value){
      qEl.value = q;
      onInput();
    }
  });

  // Example searches (picked from current data) to make the UI discoverable
  const examples = [
    {
      label: 'NDC',
      items: [
        { query: '00002-7715-01', text: '00002-7715-01 (insulin glargine pen)' }, // insulin glargine
        { query: '00003-0893-21', text: '00003-0893-21 (apixaban 2.5 mg tablet)' }, // apixaban
      ],
    },
    {
      label: 'Name',
      items: [
        { query: 'fluoxetine 20 mg tablet', text: 'fluoxetine 20 mg tablet' }, // fluoxetine
        { query: 'lisinopril 20 mg', text: 'lisinopril 20 mg (with HCTZ)' }, // lisinopril/HCTZ combos
      ],
    },
    {
      label: 'RxCUI',
      items: [
        { query: '197886', text: '197886 (lisinopril/hydrochlorothiazide 20/12.5)' }, // lisinopril/HCTZ
        { query: '1364441', text: '1364441 (apixaban 2.5 mg)' }, // apixaban
      ],
    },
    {
      label: 'UNII',
      items: [
        { query: '9100L32L2N', text: '9100L32L2N (metformin)' }, // metformin
        { query: '3Z9Y7UWC1J', text: '3Z9Y7UWC1J (apixaban)' }, // apixaban
      ],
    },
  ];

  function renderExamples(){
    const container = document.getElementById('examples');
    if (!container) return;
    container.innerHTML = '';
    for (const group of examples){
      const block = document.createElement('div');
      block.className = 'example-group';
      const title = document.createElement('div');
      title.className = 'example-title';
      title.textContent = group.label;
      block.appendChild(title);

      const chips = document.createElement('div');
      chips.className = 'example-chips';
      for (const item of group.items){
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'example-chip';
        btn.textContent = item.text;
        btn.addEventListener('click', () => {
          qEl.value = item.query;
          onInput();
          qEl.focus();
        });
        chips.appendChild(btn);
      }
      block.appendChild(chips);
      container.appendChild(block);
    }
  }

  renderExamples();
})();
