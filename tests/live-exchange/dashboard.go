package main

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>OxiDB Exchange — live</title>
<style>
  :root{--bg:#0b0e14;--panel:#12161f;--line:#1f2530;--tx:#e6e9ef;--dim:#8b93a7;
        --up:#26a269;--down:#e0483e;--accent:#7aa2f7}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--tx);
       font:14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
  header{display:flex;align-items:center;gap:14px;padding:14px 20px;
         border-bottom:1px solid var(--line);background:var(--panel)}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.2px}
  header h1 span{color:var(--accent)}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--down)}
  .dot.on{background:var(--up);box-shadow:0 0 8px var(--up)}
  .stat{margin-left:auto;color:var(--dim)}
  .stat b{color:var(--tx)}
  main{display:grid;grid-template-columns:2fr 1fr;gap:16px;padding:16px;max-width:1100px;margin:0 auto}
  @media(max-width:820px){main{grid-template-columns:1fr}}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
        padding:12px 14px;transition:background .35s}
  .card .sym{color:var(--dim);font-weight:600;font-size:12px;letter-spacing:.5px}
  .card .px{font-size:20px;font-weight:650;margin-top:4px;font-variant-numeric:tabular-nums}
  .card.up{background:#12241a}.card.down{background:#241416}
  .card .arrow{font-size:12px;margin-left:6px}
  .up .arrow{color:var(--up)}.down .arrow{color:var(--down)}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}
  .panel h2{font-size:12px;color:var(--dim);margin:0;padding:10px 14px;
            border-bottom:1px solid var(--line);letter-spacing:.5px;text-transform:uppercase}
  .trades{max-height:520px;overflow:auto}
  .trade{display:flex;gap:8px;padding:7px 14px;border-bottom:1px solid var(--line);
         font-variant-numeric:tabular-nums;animation:flash .5s}
  .trade .s{width:52px;font-weight:600}
  .trade .p{width:88px;text-align:right;color:var(--accent)}
  .trade .q{width:64px;text-align:right;color:var(--dim)}
  .trade .u{flex:1;text-align:right;color:var(--dim);font-size:12px}
  @keyframes flash{from{background:#1a2030}to{background:transparent}}
</style>
</head>
<body>
<header>
  <div class="dot" id="dot"></div>
  <h1>OxiDB <span>Exchange</span> — live (prices formed by traders)</h1>
  <div class="stat"><b id="total">0</b> trades · <span id="rate">0</span>/s</div>
</header>
<main>
  <div>
    <div class="grid" id="grid"></div>
  </div>
  <div class="panel">
    <h2>Recent trades</h2>
    <div class="trades" id="trades"></div>
  </div>
</main>
<script>
  var prev = {}, cards = {}, lastTotal = 0, lastAt = 0;
  var grid = document.getElementById("grid");
  var tradesEl = document.getElementById("trades");
  var dot = document.getElementById("dot");

  function fmt(p){
    if(p >= 1000) return p.toLocaleString(undefined,{maximumFractionDigits:2});
    if(p >= 1)    return p.toFixed(3);
    return p.toFixed(6);
  }
  function card(sym){
    var el = document.createElement("div");
    el.className = "card";
    el.innerHTML = '<div class="sym">'+sym+'</div><div class="px"><span class="v">–</span><span class="arrow"></span></div>';
    grid.appendChild(el);
    return el;
  }
  function connect(){
    var ws = new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");
    ws.onopen = function(){ dot.classList.add("on"); };
    ws.onclose = function(){ dot.classList.remove("on"); setTimeout(connect, 1000); };
    ws.onmessage = function(ev){
      var d = JSON.parse(ev.data);
      d.symbols.forEach(function(s){
        var el = cards[s.sym] || (cards[s.sym] = card(s.sym));
        var p = prev[s.sym];
        el.querySelector(".v").textContent = fmt(s.price);
        if(p !== undefined && s.price !== p){
          var up = s.price > p;
          el.classList.remove("up","down");
          void el.offsetWidth;                 // restart transition
          el.classList.add(up?"up":"down");
          el.querySelector(".arrow").textContent = up?"▲":"▼";
        }
        prev[s.sym] = s.price;
      });
      // recent trades
      tradesEl.innerHTML = "";
      (d.trades||[]).forEach(function(t){
        var row = document.createElement("div");
        row.className = "trade";
        row.innerHTML = '<span class="s">'+t.sym+'</span>'+
          '<span class="p">'+fmt(t.price)+'</span>'+
          '<span class="q">'+t.qty.toFixed(2)+'</span>'+
          '<span class="u">'+t.buyer+' ← '+t.seller+'</span>';
        tradesEl.appendChild(row);
      });
      // stats
      document.getElementById("total").textContent = d.total.toLocaleString();
      if(lastAt){
        var dt = (d.at - lastAt)/1000;
        if(dt > 0) document.getElementById("rate").textContent = Math.max(0,((d.total-lastTotal)/dt)).toFixed(0);
      }
      lastTotal = d.total; lastAt = d.at;
    };
  }
  connect();
</script>
</body>
</html>`
