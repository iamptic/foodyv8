(() => {
  const $=(s,r=document)=>r.querySelector(s); const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));
  const tg = window.Telegram?.WebApp; if (tg){ tg.expand(); const apply=()=>{const s=tg.colorScheme||'dark';document.documentElement.dataset.theme=s;}; apply(); tg.onEvent?.('themeChanged',apply); }
  const cfg = window.__FOODY__ || {}; const FOODY_API = cfg.FOODY_API || "https://foodyback-production.up.railway.app";

  const url = new URL(location.href);
  const inUrlRID = url.searchParams.get('rid'); const inUrlKEY = url.searchParams.get('key');

  const state = {
    restaurant_id: inUrlRID || localStorage.getItem('rid') || '',
    api_key:       inUrlKEY || localStorage.getItem('key') || ''
  };

  const gate = $('#gate'); const app = $('#app');
  const toastBox = $('#toast');
  const toast=(m)=>{ const el=document.createElement('div'); el.className='toast'; el.textContent=m; toastBox.appendChild(el); setTimeout(()=>el.remove(),3200); };

  // Tabs in gate
  const tabReg = $('#tabReg'), tabLogin = $('#tabLogin'), regForm = $('#regForm'), loginForm = $('#loginForm'), regResult = $('#regResult');
  tabReg.onclick = ()=>{ tabReg.classList.add('active'); tabLogin.classList.remove('active'); regForm.classList.remove('hidden'); regResult.classList.add('hidden'); loginForm.classList.add('hidden'); };
  tabLogin.onclick = ()=>{ tabLogin.classList.add('active'); tabReg.classList.remove('active'); loginForm.classList.remove('hidden'); regForm.classList.add('hidden'); regResult.classList.add('hidden'); };

  function setCreds(rid,key){
    state.restaurant_id = rid; state.api_key = key;
    localStorage.setItem('rid', rid); localStorage.setItem('key', key);
    gate.classList.add('hidden'); app.classList.remove('hidden');
    initApp();
  }
  function showGate(){
    app.classList.add('hidden'); gate.classList.remove('hidden');
  }

  // API helpers
  const jget  = (p)=> fetch(FOODY_API+p,{ headers:{'X-Foody-Key': state.api_key } }).then(r=>{ if(!r.ok) throw new Error(r.statusText); return r.json(); });
  const jpost = (p,b)=> fetch(FOODY_API+p,{ method:'POST',headers:{'Content-Type':'application/json','X-Foody-Key': state.api_key}, body: JSON.stringify(b)}).then(r=>{ if(!r.ok) throw new Error(r.statusText); return r.json(); });
  const jput  = (p,b)=> fetch(FOODY_API+p,{ method:'PUT', headers:{'Content-Type':'application/json','X-Foody-Key': state.api_key}, body: JSON.stringify(b)}).then(r=>{ if(!r.ok) throw new Error(r.statusText); return r.json(); });

  // Registration
  regForm.addEventListener('submit', async (e)=>{
    e.preventDefault();
    const f = e.target;
    try{
      const resp = await fetch(FOODY_API+'/api/v1/merchant/register_public', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name: f.name.value, phone: f.phone.value })});
      if(!resp.ok) throw new Error('register');
      const data = await resp.json();
      $('#ridOut').textContent = data.restaurant_id;
      $('#keyOut').textContent = data.api_key;
      regResult.classList.remove('hidden');
      toast('Ресторан зарегистрирован');
    }catch(_){ toast('Не удалось зарегистрировать'); }
  });
  $('#copyKeys').onclick = async ()=>{
    const txt = 'Restaurant ID: '+$('#ridOut').textContent+'\nAPI Key: '+$('#keyOut').textContent;
    try{ await navigator.clipboard.writeText(txt); toast('Скопировано'); }catch{ toast('Не удалось скопировать'); }
  };
  $('#saveAndEnter').onclick = ()=> setCreds($('#ridOut').textContent, $('#keyOut').textContent);

  // Login
  loginForm.addEventListener('submit', (e)=>{
    e.preventDefault();
    const f=e.target; setCreds(f.rid.value.trim(), f.key.value.trim());
  });
  $('#logoutBtn').onclick = ()=>{ localStorage.removeItem('rid'); localStorage.removeItem('key'); state.restaurant_id=''; state.api_key=''; showGate(); };

  // App logic (stats + offers + upload)
  const metricSel = $('#metric'); const chartCanvas = $('#chart'); const ctx = chartCanvas.getContext('2d');
  function drawChart(points){
    ctx.clearRect(0,0,chartCanvas.width,chartCanvas.height);
    const W = chartCanvas.clientWidth; const H = chartCanvas.height; chartCanvas.width=W;
    const xs = points.map(p=>new Date(p.x).getTime()); const ys = points.map(p=>p.y);
    const minX = Math.min(...xs, Date.now()-7*864e5), maxX = Math.max(...xs, Date.now());
    const minY = 0, maxY = Math.max(1, ...ys);
    const px = (t)=> (W-32) * ( (t - minX) / (maxX - minX || 1) ) + 16;
    const py = (v)=> H-24 - (H-48) * ( (v - minY) / (maxY - minY || 1) );
    ctx.strokeStyle = 'rgba(180,200,220,.15)'; ctx.lineWidth=1; ctx.beginPath(); ctx.moveTo(16, H-24); ctx.lineTo(W-16, H-24); ctx.stroke();
    ctx.strokeStyle = '#4ab5f1'; ctx.lineWidth=2; ctx.beginPath();
    points.forEach((p,i)=>{ const x=px(new Date(p.x).getTime()), y=py(p.y); if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    ctx.stroke();
    ctx.fillStyle = '#61d39f';
    points.forEach(p=>{ const x=px(new Date(p.x).getTime()), y=py(p.y); ctx.beginPath(); ctx.arc(x,y,3,0,Math.PI*2); ctx.fill(); });
  }
  async function loadStats(){
    try{
      const metric = metricSel.value;
      const data = await jget(`/api/v1/merchant/stats?restaurant_id=${state.restaurant_id}&metric=${metric}`);
      drawChart(data.points||[]);
    }catch(_){ /* ignore for gate */ }
  }
  metricSel?.addEventListener('change', loadStats);

  // Offers
  const offersBox = $('#offers'); const search = $('#search'); let offers = [];
  function renderOffers(){
    if(!offersBox) return;
    const q = (search?.value||'').toLowerCase();
    offersBox.innerHTML = '';
    const list = offers.filter(o => !q || (o.title||'').toLowerCase().includes(q));
    if(!list.length){ offersBox.innerHTML = '<div class="subtitle">Нет офферов</div>'; return; }
    list.forEach(o=>{
      const el = document.createElement('div'); el.className='item';
      el.innerHTML = `
        <img src="${o.image_url||''}" alt="">
        <div>
          <div class="title">${o.title||'—'} ${o.status!=='active'?'<span class="badge">'+o.status+'</span>':''}</div>
          <div class="sub">₽ ${(o.price_cents||0)/100} • осталось ${o.qty_left??'—'} / ${o.qty_total??'—'}</div>
        </div>
        <div class="actions">
          <button class="btn" data-act="edit">Ред.</button>
          <button class="btn" data-act="${o.status==='active'?'archive':'activate'}">${o.status==='active'?'В архив':'Активировать'}</button>
          <button class="btn" data-act="delete">Удалить</button>
        </div>
      `;
      el.querySelector('[data-act="edit"]').onclick = ()=> openEdit(o);
      (o.status==='active' ? el.querySelector('[data-act="archive"]') : el.querySelector('[data-act="activate"]')).onclick = ()=> changeStatus(o, o.status==='active'?'archive':'activate');
      el.querySelector('[data-act="delete"]').onclick = ()=> removeOffer(o);
      offersBox.appendChild(el);
    });
  }
  async function loadOffers(){
    try{
      offers = await jget(`/api/v1/merchant/offers?restaurant_id=${state.restaurant_id}`);
      renderOffers();
    }catch(err){
      // Если токены невалидны — показываем gate
      showGate(); toast('Нужно войти');
    }
  }
  search?.addEventListener('input', renderOffers);
  async function changeStatus(o, action){ await jpost('/api/v1/merchant/offers/status',{ restaurant_id: state.restaurant_id, offer_id:o.id, action }).catch(()=>toast('Ошибка')); await loadOffers(); }
  async function removeOffer(o){ await jpost('/api/v1/merchant/offers/delete',{ restaurant_id: state.restaurant_id, offer_id:o.id }).catch(()=>toast('Ошибка')); toast('Удалено'); await loadOffers(); }

  // Create
  $('#createBtn')?.addEventListener('click', ()=> $('#offerForm').scrollIntoView({behavior:'smooth'}));
  $('#offerForm')?.addEventListener('submit', async (e)=>{
    e.preventDefault();
    const f = e.target;
    const payload = {
      restaurant_id: state.restaurant_id,
      title: f.title.value,
      price_cents: Math.round((+f.price.value||0)*100),
      original_price_cents: Math.round((+f.price_old.value||0)*100) || null,
      qty_total: +f.qty_total.value||0,
      qty_left: +f.qty_left.value||0,
      expires_at: f.expires_at.value || null,
      description: f.description.value || '',
      image_url: f.image_url.value || ''
    };
    try{
      await jpost('/api/v1/merchant/offers', payload);
      toast('Сохранено ✅'); f.reset(); $('#preview').style.display='none'; await loadOffers();
    }catch{ toast('Ошибка'); }
  });

  // Edit
  const modal = $('#modal'); $('#modalClose')?.addEventListener('click', ()=> modal.classList.add('hidden'));
  function openEdit(o){
    modal.classList.remove('hidden');
    const f = $('#editForm');
    f.offer_id.value = o.id;
    f.title.value = o.title||'';
    f.price.value = (o.price_cents||0)/100;
    f.price_old.value = (o.original_price_cents||0)/100;
    f.qty_total.value = o.qty_total||0;
    f.qty_left.value = o.qty_left||0;
    f.expires_at.value = o.expires_at||'';
    f.image_url.value = o.image_url||'';
    f.description.value = o.description||'';
  }
  $('#editForm')?.addEventListener('submit', async (e)=>{
    e.preventDefault();
    const f = e.target;
    const id = +f.offer_id.value;
    const payload = {
      restaurant_id: state.restaurant_id,
      title: f.title.value,
      price_cents: Math.round((+f.price.value||0)*100),
      original_price_cents: Math.round((+f.price_old.value||0)*100) || null,
      qty_total: +f.qty_total.value||0,
      qty_left: +f.qty_left.value||0,
      expires_at: f.expires_at.value || null,
      image_url: f.image_url.value || '',
      description: f.description.value || ''
    };
    try{
      await jput(`/api/v1/merchant/offers/${id}`, payload);
      toast('Сохранено ✅'); modal.classList.add('hidden'); await loadOffers();
    }catch{ toast('Ошибка'); }
  });

  // CSV
  $('#exportBtn')?.addEventListener('click', ()=>{
    if(!state.restaurant_id){ return showGate(); }
    const url = `${FOODY_API}/api/v1/merchant/offers/csv?restaurant_id=${state.restaurant_id}`;
    const a = document.createElement('a'); a.href = url; a.target = '_blank';
    a.download = `offers_${state.restaurant_id}.csv`; a.rel = 'noopener'; a.click();
  });

  // Upload (presign)
  const dz = $('#dropzone'); const fileInput = $('#fileInput'); const preview = $('#preview'); const uploadBtn = $('#uploadBtn');
  let chosenFile = null;
  dz?.addEventListener('click', ()=> fileInput.click());
  ['dragenter','dragover'].forEach(ev => dz?.addEventListener(ev, e => { e.preventDefault(); dz.classList.add('drag'); }));
  ['dragleave','drop'].forEach(ev => dz?.addEventListener(ev, e => { e.preventDefault(); dz.classList.remove('drag'); }));
  dz?.addEventListener('drop', e => { chosenFile = (e.dataTransfer.files||[])[0]||null; showPreview(); });
  fileInput?.addEventListener('change', e => { chosenFile = (e.target.files||[])[0]||null; showPreview(); });
  function showPreview(){ if(!chosenFile){ if(preview){ preview.style.display='none'; preview.src=''; } return; } const url = URL.createObjectURL(chosenFile); if(preview){ preview.src=url; preview.style.display='block'; } }
  async function resizeImage(file, maxSide=1600, quality=0.85){
    if (!file?.type?.startsWith('image/')) return file;
    const img = document.createElement('img');
    await new Promise((res,rej)=>{ img.onload=res; img.onerror=rej; img.src=URL.createObjectURL(file); });
    const ratio = img.width/img.height; let w=img.width, h=img.height;
    if (Math.max(w,h)>maxSide){ if(w>=h){ w=maxSide; h=Math.round(maxSide/ratio); } else { h=maxSide; w=Math.round(maxSide*ratio); } }
    const canvas = document.createElement('canvas'); canvas.width=w; canvas.height=h;
    const ctx = canvas.getContext('2d'); ctx.drawImage(img,0,0,w,h);
    const blob = await new Promise(res=> canvas.toBlob(res, file.type || 'image/jpeg', quality));
    return new File([blob], file.name.replace(/\.(\w+)$/i,'.jpg'), { type: file.type || 'image/jpeg' });
  }
  async function uploadWithPresign(file){
    const presign = await jpost('/api/v1/merchant/uploads/presign', { filename:file.name, content_type:file.type, restaurant_id: state.restaurant_id });
    const form = new FormData();
    Object.entries(presign.fields||{}).forEach(([k,v])=> form.append(k,v));
    form.append('Content-Type', file.type); form.append('file', file);
    const s3 = await fetch(presign.upload_url, { method:'POST', body: form });
    if(!s3.ok) throw new Error('upload');
    return presign.public_url;
  }
  uploadBtn?.addEventListener('click', async ()=>{
    if(!chosenFile){ return alert('Выберите файл'); }
    try{
      toast('Загружаем...');
      const resized = await resizeImage(chosenFile, 1600, 0.85);
      const url = await uploadWithPresign(resized);
      document.querySelector('input[name="image_url"]').value = url;
      if(preview) preview.src = url;
      toast('Фото загружено');
    }catch(e){ console.error(e); toast('Ошибка загрузки'); }
  });

  async function initApp(){
    await Promise.all([loadOffers(), loadStats()]).catch(()=>{});
  }

  // Boot
  if(state.restaurant_id && state.api_key){ setCreds(state.restaurant_id, state.api_key); }
  else { showGate(); }
})();