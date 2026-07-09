package main

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>OxiDB Exchange — live order books</title>
<style>
  :root{--bg:#0b0e14;--panel:#12161f;--line:#1f2530;--tx:#e6e9ef;--dim:#8b93a7;
        --up:#26a269;--down:#e0483e;--accent:#7aa2f7}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--tx);
       font:13px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
  header{display:flex;align-items:center;gap:14px;padding:14px 20px;
         border-bottom:1px solid var(--line);background:var(--panel);position:sticky;top:0;z-index:5}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.2px}
  header h1 span{color:var(--accent)}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--down)}
  .dot.on{background:var(--up);box-shadow:0 0 8px var(--up)}
  .stat{margin-left:auto;color:var(--dim)}
  .stat b{color:var(--tx)}
  .main{display:flex;gap:16px;align-items:flex-start;padding:16px;width:100%}
  .grid{flex:1;display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:12px}
  .side{flex:0 0 50%;width:50%;position:sticky;top:66px;height:calc(100vh - 82px)}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden;
        cursor:pointer;transition:border-color .15s}
  .card:hover{border-color:var(--accent)}
  .card.sel{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset}
  .head{display:flex;align-items:baseline;gap:8px;padding:10px 14px;
        border-bottom:1px solid var(--line);transition:background .35s}
  .head .sym{font-weight:650;font-size:14px;letter-spacing:.5px}
  .head .px{margin-left:auto;font-size:17px;font-weight:650;font-variant-numeric:tabular-nums}
  .head .arrow{font-size:11px}
  .head.up{background:#12241a}.head.down{background:#241416}
  .up .arrow,.up .px{color:var(--up)}.down .arrow,.down .px{color:var(--down)}
  .colh{display:flex;justify-content:space-between;padding:4px 14px;font-size:10px;
        color:var(--dim);text-transform:uppercase;letter-spacing:.4px}
  .book .row{position:relative;display:flex;justify-content:space-between;
             padding:3px 14px;font-variant-numeric:tabular-nums;font-size:12.5px;overflow:hidden}
  .book .bar{position:absolute;right:0;top:0;bottom:0;opacity:.16;z-index:0}
  .book .row span{position:relative;z-index:1}
  .ask .bar{background:var(--down)}.bid .bar{background:var(--up)}
  .ask .price{color:var(--down)}.bid .price{color:var(--up)}
  .book .q{color:var(--dim)}
  .spread{text-align:center;padding:5px 14px;font-size:11px;color:var(--dim);
          background:#0e1219;border-top:1px solid var(--line);border-bottom:1px solid var(--line)}
  .spread b{color:var(--tx)}
  .empty{color:var(--dim);font-size:12px;padding:6px 14px;text-align:center}
  .chartlabel{display:flex;justify-content:space-between;padding:6px 14px 2px;
              font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.4px}
  .chartlabel .chg{font-weight:600}
  .card canvas{width:100%;height:72px;display:block;padding:0 6px 8px}
  /* right-hand full chart */
  .full{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px 16px;
        height:100%;display:flex;flex-direction:column}
  .full .bar{display:flex;align-items:baseline;gap:10px;margin-bottom:8px;flex:0 0 auto}
  .full .bar .sym{font-size:19px;font-weight:650}
  .full .bar .px{font-size:19px;font-weight:650;font-variant-numeric:tabular-nums}
  .full .bar .chg{font-size:14px;font-weight:600}
  .full .bar .hint{margin-left:auto;color:var(--dim);font-size:11px}
  .ctrls{display:flex;align-items:center;gap:10px;margin-bottom:8px;flex:0 0 auto}
  .tfbtns{display:flex;gap:6px}
  .tfbtn{background:#0e1219;border:1px solid var(--line);color:var(--dim);border-radius:6px;
         padding:3px 10px;font-size:12px;cursor:pointer}
  .tfbtn:hover{color:var(--tx)}
  .tfbtn.on{background:var(--accent);border-color:var(--accent);color:#0b0e14;font-weight:650}
  .csel{margin-left:auto;background:#0e1219;border:1px solid var(--line);color:var(--tx);
        border-radius:6px;padding:4px 8px;font-size:12px;cursor:pointer}
  .full canvas{width:100%;flex:1 1 auto;min-height:0;display:block}
  /* bottom metrics */
  .metrics{padding:16px 16px 0}
  .mhead{font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:.5px;
         margin:6px 2px 10px}
  .mhead b{color:var(--accent)}
  .mgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
  .stat{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
  .stat .v{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}
  .stat .k{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.4px;margin-top:4px}
  @media(max-width:980px){.main{flex-direction:column}
    .side{width:100%;flex:1 1 auto;position:static;height:70vh}}
</style>
</head>
<body>
<header>
  <div class="dot" id="dot"></div>
  <h1>OxiDB <span>Exchange</span> — order books · candlesticks · engine metrics</h1>
  <div class="stat"><b id="total">0</b> trades · <span id="rate">0</span>/s</div>
</header>
<section class="metrics">
  <div class="mhead">OxiDB engine metrics · <b>auto-refresh 5s</b> (rates from Prometheus counters)</div>
  <div class="mgrid">
    <div class="stat"><div class="v" id="s_traders">–</div><div class="k">Traders</div></div>
    <div class="stat"><div class="v" id="s_trades">–</div><div class="k">Trades / s</div></div>
    <div class="stat"><div class="v" id="s_conf">–</div><div class="k">Conflicts / s</div></div>
    <div class="stat"><div class="v" id="s_ins">–</div><div class="k">Insert / s</div></div>
    <div class="stat"><div class="v" id="s_find">–</div><div class="k">Find / s</div></div>
    <div class="stat"><div class="v" id="s_upd">–</div><div class="k">Update / s</div></div>
    <div class="stat"><div class="v" id="s_del">–</div><div class="k">Delete / s</div></div>
    <div class="stat"><div class="v" id="s_mem">–</div><div class="k">Memory RSS (MB)</div></div>
    <div class="stat"><div class="v" id="s_cpu">–</div><div class="k">CPU (% of core)</div></div>
  </div>
</section>
<div class="main">
  <div class="grid" id="grid"></div>
  <aside class="side">
    <div class="full">
      <div class="bar"><span class="sym" id="fSym">–</span>
        <span class="px" id="fPx"></span><span class="chg" id="fChg"></span>
        <span class="hint">OHLCV from live trades</span></div>
      <div class="ctrls">
        <div class="tfbtns" id="tfbtns"></div>
        <select id="ctype" class="csel">
          <option value="candle">Candlestick</option>
          <option value="line">Line</option>
          <option value="area">Area</option>
        </select>
      </div>
      <canvas id="fcv"></canvas>
    </div>
  </aside>
</div>
<script>
  var UP="#26a269", DOWN="#e0483e", DIM="#8b93a7", LINE="#1f2530", ACC="#7aa2f7", YEL="#e5c07b";
  var prev={}, cards={}, dot=document.getElementById("dot");
  var grid=document.getElementById("grid");
  var lastTotal=0, lastAt=0, selSym=null;
  var TFS=[{m:1,l:"1m"},{m:5,l:"5m"},{m:15,l:"15m"},{m:60,l:"1h"},{m:240,l:"4h"}];
  var tf=15, chartType="candle", fullData=[];

  function fmt(p){
    if(p>=1000) return p.toLocaleString(undefined,{maximumFractionDigits:2});
    if(p>=1)    return p.toFixed(3);
    return p.toFixed(6);
  }
  function hm(ts){ var d=new Date(ts*1000);
    return ("0"+d.getHours()).slice(-2)+":"+("0"+d.getMinutes()).slice(-2); }
  // Axis label that disambiguates across days: on multi-day spans the sampled
  // ticks can land exactly 24h apart and all read the same clock time, so add
  // the date when the visible window is long.
  function tlabel(ts, span){
    var d=new Date(ts*1000);
    var md=(d.getMonth()+1)+"/"+d.getDate();
    var t=("0"+d.getHours()).slice(-2)+":"+("0"+d.getMinutes()).slice(-2);
    if(span>3*86400) return md;
    if(span>12*3600) return md+" "+t;
    return t;
  }

  function makeCard(sym){
    var el=document.createElement("div"); el.className="card";
    el.innerHTML='<div class="head"><span class="sym">'+sym+'</span>'+
      '<span class="px">–</span><span class="arrow"></span></div>'+
      '<div class="colh"><span>price</span><span>size</span></div>'+
      '<div class="book"></div>'+
      '<div class="chartlabel"><span>2s candles</span><span class="chg"></span></div>'+
      '<canvas></canvas>';
    el.addEventListener("click", function(){ selectSym(sym); });
    grid.appendChild(el);
    return {el:el, head:el.querySelector(".head"), px:el.querySelector(".px"),
            arrow:el.querySelector(".arrow"), book:el.querySelector(".book"),
            mini:el.querySelector("canvas"), chg:el.querySelector(".chg")};
  }
  function rows(levels, cls, maxq){
    if(!levels||!levels.length) return "";
    return levels.map(function(l){
      var w=Math.max(3, l.q/maxq*100).toFixed(0);
      return '<div class="row '+cls+'"><div class="bar" style="width:'+w+'%"></div>'+
        '<span class="price">'+fmt(l.p)+'</span><span class="q">'+l.q.toFixed(2)+'</span></div>';
    }).join("");
  }
  function renderBook(c, s){
    var all=(s.asks||[]).concat(s.bids||[]);
    var maxq=1; all.forEach(function(l){ if(l.q>maxq) maxq=l.q; });
    var asks=(s.asks||[]).slice().reverse();
    var mid;
    if(s.asks&&s.asks.length&&s.bids&&s.bids.length){
      mid='<b>'+fmt(s.asks[0].p)+'</b> / <b>'+fmt(s.bids[0].p)+'</b> · spread '+fmt(s.asks[0].p-s.bids[0].p);
    } else { mid='last <b>'+fmt(s.price)+'</b>'; }
    var html=rows(asks,"ask",maxq)+'<div class="spread">'+mid+'</div>'+rows(s.bids||[],"bid",maxq);
    if(!asks.length && !(s.bids||[]).length) html='<div class="empty">no resting orders</div>'+
      '<div class="spread">last <b>'+fmt(s.price)+'</b></div>';
    c.book.innerHTML=html;
  }

  function drawMini(c, candles){
    var cv=c.mini; if(!cv) return;
    var dpr=window.devicePixelRatio||1, W=cv.clientWidth, H=cv.clientHeight;
    if(!W) return;
    cv.width=W*dpr; cv.height=H*dpr;
    var g=cv.getContext("2d"); g.setTransform(dpr,0,0,dpr,0,0); g.clearRect(0,0,W,H);
    if(!candles||!candles.length) return;
    var lo=Infinity, hi=-Infinity;
    candles.forEach(function(k){ if(k.l<lo)lo=k.l; if(k.h>hi)hi=k.h; });
    if(hi<=lo) hi=lo+1;
    var pad=(hi-lo)*0.08; lo-=pad; hi+=pad;
    var n=candles.length, cw=W/n, bw=Math.max(1, cw*0.62);
    function py(p){ return 4+(hi-p)/(hi-lo)*(H-8); }
    for(var j=0;j<n;j++){
      var k=candles[j], cx=j*cw+cw/2, up=k.c>=k.o, col=up?UP:DOWN;
      g.strokeStyle=col; g.fillStyle=col;
      g.beginPath(); g.moveTo(cx,py(k.h)); g.lineTo(cx,py(k.l)); g.stroke();
      var yo=py(k.o), yc=py(k.c), top=Math.min(yo,yc), h=Math.max(1,Math.abs(yc-yo));
      g.fillRect(cx-bw/2, top, bw, h);
    }
    var first=candles[0], last=candles[n-1];
    var chg=first.o>0?((last.c-first.o)/first.o*100):0;
    c.chg.textContent=(chg>=0?"+":"")+chg.toFixed(2)+"%"; c.chg.style.color=chg>=0?UP:DOWN;
  }

  function drawFull(){
    var candles=fullData;
    var cv=document.getElementById("fcv");
    var dpr=window.devicePixelRatio||1, W=cv.clientWidth, H=cv.clientHeight;
    cv.width=W*dpr; cv.height=H*dpr;
    var g=cv.getContext("2d"); g.setTransform(dpr,0,0,dpr,0,0); g.clearRect(0,0,W,H);
    if(!candles||!candles.length) return;
    var padL=8, padR=70, padT=12, volH=64, gap=10, axisH=16;
    var cTop=padT, cBot=H-volH-gap-axisH, volTop=cBot+gap, volBot=H-axisH;
    var lo=Infinity, hi=-Infinity, maxV=0;
    candles.forEach(function(k){ if(k.l<lo)lo=k.l; if(k.h>hi)hi=k.h; if(k.v>maxV)maxV=k.v; });
    if(hi<=lo) hi=lo+1;
    var pad=(hi-lo)*0.08; lo-=pad; hi+=pad;
    var plotW=W-padL-padR, n=candles.length, cw=plotW/n, bw=Math.max(1, Math.min(cw*0.7, 16));
    function py(p){ return cTop+(hi-p)/(hi-lo)*(cBot-cTop); }
    function cx(i){ return padL+i*cw+cw/2; }
    var last=candles[n-1], first=candles[0], trend=last.c>=first.o?UP:DOWN;
    // price grid + labels
    g.font="10px -apple-system,sans-serif"; g.textBaseline="middle";
    for(var i=0;i<=4;i++){
      var pr=hi-(hi-lo)*i/4, y=py(pr);
      g.strokeStyle=LINE; g.beginPath(); g.moveTo(padL,y); g.lineTo(W-padR,y); g.stroke();
      g.fillStyle=DIM; g.textAlign="left"; g.fillText(fmt(pr), W-padR+6, y);
    }
    // volume bars (all chart types)
    if(maxV>0){ for(var j=0;j<n;j++){ var k=candles[j], x=cx(j);
      var vh=k.v/maxV*(volBot-volTop); g.fillStyle=(k.c>=k.o?UP:DOWN);
      g.globalAlpha=.5; g.fillRect(x-bw/2, volBot-vh, bw, vh); g.globalAlpha=1; } }
    // price series
    if(chartType==="candle"){
      for(var c=0;c<n;c++){
        var kc=candles[c], xc=cx(c), col=kc.c>=kc.o?UP:DOWN;
        g.strokeStyle=col; g.fillStyle=col;
        g.beginPath(); g.moveTo(xc,py(kc.h)); g.lineTo(xc,py(kc.l)); g.stroke();
        var yo=py(kc.o), yc=py(kc.c), top=Math.min(yo,yc), bh=Math.max(1,Math.abs(yc-yo));
        g.fillRect(xc-bw/2, top, bw, bh);
      }
    } else {
      if(chartType==="area"){
        var grad=g.createLinearGradient(0,cTop,0,cBot);
        grad.addColorStop(0, trend+"55"); grad.addColorStop(1, trend+"05");
        g.fillStyle=grad; g.beginPath(); g.moveTo(cx(0),cBot);
        for(var a=0;a<n;a++){ g.lineTo(cx(a),py(candles[a].c)); }
        g.lineTo(cx(n-1),cBot); g.closePath(); g.fill();
      }
      g.strokeStyle=trend; g.lineWidth=1.7; g.beginPath();
      for(var p=0;p<n;p++){ var xp=cx(p), yp=py(candles[p].c); if(p===0)g.moveTo(xp,yp); else g.lineTo(xp,yp); }
      g.stroke(); g.lineWidth=1;
    }
    // time axis labels
    g.fillStyle=DIM; g.textBaseline="alphabetic"; g.textAlign="center";
    var span=candles[n-1].ts-candles[0].ts, labels=Math.min(7, n);
    for(var t=0;t<labels;t++){
      var idx=Math.round(t/(labels-1||1)*(n-1));
      g.fillText(tlabel(candles[idx].ts, span), cx(idx), H-4);
    }
    var ly=py(last.c);
    g.strokeStyle=trend; g.setLineDash([4,3]);
    g.beginPath(); g.moveTo(padL,ly); g.lineTo(W-padR,ly); g.stroke(); g.setLineDash([]);
    var chg=first.o>0?((last.c-first.o)/first.o*100):0;
    document.getElementById("fPx").textContent=fmt(last.c);
    var chgEl=document.getElementById("fChg");
    chgEl.textContent=(chg>=0?"+":"")+chg.toFixed(2)+"%"; chgEl.style.color=chg>=0?UP:DOWN;
  }

  function selectSym(sym){
    selSym=sym; document.getElementById("fSym").textContent=sym;
    for(var s in cards){ cards[s].el.classList.toggle("sel", s===sym); }
    loadFull();
  }
  function loadFull(){
    if(!selSym) return; var sym=selSym, q=tf;
    fetch("/candles24?sym="+sym+"&tf="+q).then(function(r){return r.json();}).then(function(d){
      if(selSym===sym && tf===q){ fullData=d.candles||[]; drawFull(); }
    }).catch(function(){});
  }
  document.getElementById("ctype").addEventListener("change", function(){
    chartType=this.value; drawFull();
  });
  // timeframe buttons
  (function(){
    var box=document.getElementById("tfbtns");
    TFS.forEach(function(t){
      var b=document.createElement("button"); b.className="tfbtn"+(t.m===tf?" on":"");
      b.textContent=t.l; b.dataset.m=t.m;
      b.addEventListener("click", function(){
        tf=t.m;
        [].forEach.call(box.children, function(x){ x.classList.toggle("on", +x.dataset.m===tf); });
        loadFull();
      });
      box.appendChild(b);
    });
  })();

  function pollCandles(){
    fetch("/allcandles?n=40").then(function(r){return r.json();}).then(function(m){
      for(var sym in m){ var c=cards[sym]; if(c) drawMini(c, m[sym]); }
    }).catch(function(){});
    loadFull();
  }
  setInterval(pollCandles, 2000);
  window.addEventListener("resize", function(){ pollCandles(); });

  // ---- engine metrics: numeric grid, refresh 5s ---------------------------
  var mPrev=null;
  function setN(id, v){ document.getElementById(id).textContent=v; }
  function pollMetrics(){
    fetch("/metrics-json").then(function(r){return r.json();}).then(function(m){
      // gauges — show immediately
      setN("s_traders", (m.traders||0).toFixed(0));
      setN("s_mem", (m.rss_mb||0).toFixed(0));
      setN("s_cpu", (m.cpu_pct||0).toFixed(0)+"%");
      if(mPrev){
        var dt=(m.at-mPrev.at)/1000; if(dt<=0) dt=1;
        function rate(k){ return Math.max(0,((m[k]||0)-(mPrev[k]||0))/dt); }
        setN("s_trades", rate(m.trades_count!==undefined?"trades_count":"commits").toFixed(0));
        setN("s_conf",   rate("conflicts").toFixed(0));
        setN("s_ins",    rate("insert").toFixed(0));
        setN("s_find",   rate("find").toFixed(0));
        setN("s_upd",    rate("update").toFixed(0));
        setN("s_del",    rate("delete").toFixed(0));
      }
      mPrev=m;
    }).catch(function(){});
  }
  setInterval(pollMetrics, 5000);
  pollMetrics();

  function connect(){
    var ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");
    ws.onopen=function(){ dot.classList.add("on"); };
    ws.onclose=function(){ dot.classList.remove("on"); setTimeout(connect,1000); };
    ws.onmessage=function(ev){
      var d=JSON.parse(ev.data);
      d.symbols.forEach(function(s){
        var c=cards[s.sym]||(cards[s.sym]=makeCard(s.sym));
        var p=prev[s.sym];
        c.px.textContent=fmt(s.price);
        if(p!==undefined && s.price!==p){
          var up=s.price>p;
          c.head.classList.remove("up","down"); void c.head.offsetWidth;
          c.head.classList.add(up?"up":"down");
          c.arrow.textContent=up?"▲":"▼";
        }
        prev[s.sym]=s.price;
        renderBook(c, s);
      });
      document.getElementById("total").textContent=d.total.toLocaleString();
      if(lastAt){ var dt=(d.at-lastAt)/1000;
        if(dt>0) document.getElementById("rate").textContent=Math.max(0,((d.total-lastTotal)/dt)).toFixed(0); }
      lastTotal=d.total; lastAt=d.at;
      if(!selSym && d.symbols.length) selectSym(d.symbols[0].sym);
    };
  }
  connect();
  pollCandles();
</script>
</body>
</html>`
