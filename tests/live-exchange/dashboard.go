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
         border-bottom:1px solid var(--line);background:var(--panel);position:sticky;top:0;z-index:2}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.2px}
  header h1 span{color:var(--accent)}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--down)}
  .dot.on{background:var(--up);box-shadow:0 0 8px var(--up)}
  .stat{margin-left:auto;color:var(--dim)}
  .stat b{color:var(--tx)}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));
        gap:12px;padding:16px;max-width:1280px;margin:0 auto}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}
  .head{display:flex;align-items:baseline;gap:8px;padding:11px 14px;
        border-bottom:1px solid var(--line);transition:background .35s}
  .head .sym{font-weight:650;font-size:14px;letter-spacing:.5px}
  .head .px{margin-left:auto;font-size:18px;font-weight:650;font-variant-numeric:tabular-nums}
  .head .arrow{font-size:12px}
  .head.up{background:#12241a}.head.down{background:#241416}
  .up .arrow,.up .px{color:var(--up)}.down .arrow,.down .px{color:var(--down)}
  .tlist{min-height:34px}
  .trade{display:flex;gap:8px;padding:5px 14px;border-bottom:1px solid #171b24;
         font-variant-numeric:tabular-nums;font-size:12.5px}
  .trade:last-child{border-bottom:none}
  .trade.new{animation:flash .6s}
  .trade .side{width:34px;font-weight:600}
  .trade.buy .side{color:var(--up)}.trade.sell .side{color:var(--down)}
  .trade .p{width:82px;text-align:right;color:var(--accent)}
  .trade .q{width:56px;text-align:right;color:var(--dim)}
  .trade .u{flex:1;text-align:right;color:var(--dim);font-size:11px;
            overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .empty{color:var(--dim);font-size:12px;padding:8px 14px}
  @keyframes flash{from{background:#1a2030}to{background:transparent}}
</style>
</head>
<body>
<header>
  <div class="dot" id="dot"></div>
  <h1>OxiDB <span>Exchange</span> — live · recent trades per symbol</h1>
  <div class="stat"><b id="total">0</b> trades · <span id="rate">0</span>/s</div>
</header>
<div class="grid" id="grid"></div>
<script>
  var prev={}, cards={}, lastTop={}, lastTotal=0, lastAt=0;
  var grid=document.getElementById("grid"), dot=document.getElementById("dot");

  function fmt(p){
    if(p>=1000) return p.toLocaleString(undefined,{maximumFractionDigits:2});
    if(p>=1)    return p.toFixed(3);
    return p.toFixed(6);
  }
  function shortUser(u){ return u ? u.replace("user-","u") : "?"; }
  function makeCard(sym){
    var el=document.createElement("div"); el.className="card";
    el.innerHTML='<div class="head"><span class="sym">'+sym+'</span>'+
      '<span class="px">–</span><span class="arrow"></span></div>'+
      '<div class="tlist"></div>';
    grid.appendChild(el);
    return {head:el.querySelector(".head"), px:el.querySelector(".px"),
            arrow:el.querySelector(".arrow"), list:el.querySelector(".tlist")};
  }
  function tradeKey(t){ return t.buyer+"|"+t.seller+"|"+t.price+"|"+t.qty; }

  function connect(){
    var ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws");
    ws.onopen=function(){ dot.classList.add("on"); };
    ws.onclose=function(){ dot.classList.remove("on"); setTimeout(connect,1000); };
    ws.onmessage=function(ev){
      var d=JSON.parse(ev.data);
      d.symbols.forEach(function(s){
        var c=cards[s.sym]||(cards[s.sym]=makeCard(s.sym));
        // price + flash
        var p=prev[s.sym];
        c.px.textContent=fmt(s.price);
        if(p!==undefined && s.price!==p){
          var up=s.price>p;
          c.head.classList.remove("up","down"); void c.head.offsetWidth;
          c.head.classList.add(up?"up":"down");
          c.arrow.textContent=up?"▲":"▼";
        }
        prev[s.sym]=s.price;
        // this symbol's recent trades
        var trs=s.trades||[];
        var newTop=trs.length?tradeKey(trs[0]):"";
        var isNew=newTop && newTop!==lastTop[s.sym];
        lastTop[s.sym]=newTop;
        if(trs.length===0){
          c.list.innerHTML='<div class="empty">no trades yet</div>';
        }else{
          c.list.innerHTML=trs.map(function(t,i){
            var buy=t.buyer, sell=t.seller;
            var flash=(i===0 && isNew)?" new":"";
            // show as a print: price/qty and who traded
            return '<div class="trade buy'+flash+'">'+
              '<span class="side">▲</span>'+
              '<span class="p">'+fmt(t.price)+'</span>'+
              '<span class="q">'+t.qty.toFixed(2)+'</span>'+
              '<span class="u">'+shortUser(buy)+' ← '+shortUser(sell)+'</span></div>';
          }).join("");
        }
      });
      document.getElementById("total").textContent=d.total.toLocaleString();
      if(lastAt){ var dt=(d.at-lastAt)/1000;
        if(dt>0) document.getElementById("rate").textContent=Math.max(0,((d.total-lastTotal)/dt)).toFixed(0); }
      lastTotal=d.total; lastAt=d.at;
    };
  }
  connect();
</script>
</body>
</html>`
