(() => {
  const $ = (s,r=document)=>r.querySelector(s);
  const tg = window.Telegram?.WebApp; if (tg){ tg.expand(); const apply=()=>{const s=tg.colorScheme||'dark';document.documentElement.dataset.theme=s;}; apply(); tg.onEvent?.('themeChanged',apply); }
  const API = (window.__FOODY__&&window.__FOODY__.FOODY_API)||"https://foodyback-production.up.railway.app";

  let offers=[];
  const grid = $('#grid'), q = $('#q');

  function render(){
    grid.innerHTML = '';
    const qs = (q.value||'').toLowerCase();
    const list = offers.filter(o => !qs || (o.title||'').toLowerCase().includes(qs));
    if (!list.length){ grid.innerHTML = '<div class="card"><div class="p">Нет офферов</div></div>'; return; }
    list.forEach(o=>{
      const price = (o.price_cents||0)/100, old = (o.original_price_cents||0)/100;
      const disc = old>0? Math.round((1-price/old)*100):0;
      const el = document.createElement('div'); el.className='card';
      el.innerHTML = '<img src="'+(o.image_url||'')+'" alt="">' +
        '<div class="p"><div class="price">'+price.toFixed(0)+' ₽'+(old?'<span class="badge">-'+disc+'%</span>':'')+'</div>' +
        '<div>'+(o.title||'—')+'</div>' +
        '<div class="meta"><span>Осталось: '+(o.qty_left??'—')+'</span></div></div>';
      el.onclick = ()=>open(o); grid.appendChild(el);
    });
  }

  function open(o){
    $('#sTitle').textContent = o.title||'—';
    $('#sImg').src = o.image_url||'';
    $('#sPrice').textContent = ((o.price_cents||0)/100).toFixed(0)+' ₽';
    const old=(o.original_price_cents||0)/100; $('#sOld').textContent = old? (old.toFixed(0)+' ₽') : '—';
    $('#sQty').textContent = (o.qty_left??'—') + ' / ' + (o.qty_total??'—');
    $('#sExp').textContent = o.expires_at? new Date(o.expires_at).toLocaleString('ru-RU') : '—';
    $('#sDesc').textContent = o.description||'';
    $('#sheet').classList.remove('hidden');
    $('#reserveBtn').onclick = async ()=>{
      try{
        const resp = await fetch(API+'/api/v1/public/reserve',{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ offer_id: o.id||o.offer_id, name:'TG', phone:'' }) });
        if(!resp.ok) throw new Error('reserve');
        toast('Забронировано ✅');
      }catch(_){ toast('Не удалось забронировать'); }
    };
  }
  $('#sheetClose').onclick = ()=>$('#sheet').classList.add('hidden');
  $('#refresh').onclick = load;
  q.oninput = render;

  const toastBox = document.getElementById('toast');
  const toast = (m)=>{ const el=document.createElement('div'); el.className='toast'; el.textContent=m; toastBox.appendChild(el); setTimeout(()=>el.remove(),3200); };

  async function load(){ offers = await fetch(API+'/api/v1/offers').then(r=>r.json()).catch(()=>[]); render(); }
  load();
})();