import type { Metadata } from "next"
export const metadata: Metadata = { title: "E-commerce recipe" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Recipes · 2 of 6</p>
<h2>E-commerce — orders, carts, refunds</h2>

<h3>Schema</h3>
<pre><code class="lang-bash">products: {sku, name, price, stock, sold}
carts:    {_id, user_id, items:[{sku, qty}], total, status}
orders:   {_id, user_id, items, total, status}
accounts: {user_id, balance}</code></pre>

<h3>add_to_cart</h3>
<pre><code class="lang-rust">proc add_to_cart(user_id, sku, qty) {
    let p = find_one(<span class="str">"products"</span>, {sku: sku})
    <span class="kw">if</span> p == <span class="kw">null</span>          { <span class="kw">abort</span> <span class="str">"product not found"</span> }
    <span class="kw">if</span> p.stock &lt; qty      { <span class="kw">abort</span> <span class="str">"out of stock"</span> }

    let cart = find_one(<span class="str">"carts"</span>, {user_id: user_id, status: <span class="str">"open"</span>})
    <span class="kw">if</span> cart == <span class="kw">null</span> {
        insert(<span class="str">"carts"</span>, {
            user_id: user_id, status: <span class="str">"open"</span>, total: p.price * qty,
            items: [{sku: sku, qty: qty, price: p.price}]
        })
    } <span class="kw">else</span> {
        update(<span class="str">"carts"</span>, {_id: cart._id}, {
            $push: {items: {sku: sku, qty: qty, price: p.price}},
            $inc: {total: p.price * qty}
        })
    }
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>checkout</h3>
<pre><code class="lang-rust">proc checkout(user_id) {
    let cart = find_one(<span class="str">"carts"</span>, {user_id: user_id, status: <span class="str">"open"</span>})
    <span class="kw">if</span> cart == <span class="kw">null</span>      { <span class="kw">abort</span> <span class="str">"no open cart"</span> }
    <span class="kw">if</span> cart.total &lt;= <span class="num">0</span>   { <span class="kw">abort</span> <span class="str">"empty cart"</span> }

    let acc = find_one(<span class="str">"accounts"</span>, {user_id: user_id})
    <span class="kw">if</span> acc.balance &lt; cart.total { <span class="kw">abort</span> <span class="str">"insufficient funds"</span> }

    <span class="co">// Reserve stock</span>
    <span class="kw">for</span> item <span class="kw">in</span> cart.items {
        update(<span class="str">"products"</span>, {sku: item.sku}, {$inc: {stock: -item.qty, sold: item.qty}})
    }

    <span class="co">// Charge buyer</span>
    update(<span class="str">"accounts"</span>, {user_id: user_id}, {$inc: {balance: -cart.total}})

    <span class="co">// Convert cart to order</span>
    let order_id = insert(<span class="str">"orders"</span>, {
        user_id: user_id, items: cart.items, total: cart.total, status: <span class="str">"confirmed"</span>
    })
    update(<span class="str">"carts"</span>, {_id: cart._id}, {$set: {status: <span class="str">"converted"</span>, order_id: order_id}})

    <span class="kw">return</span> {ok: <span class="kw">true</span>, order_id: order_id, charged: cart.total}
}</code></pre>

<h3>refund</h3>
<pre><code class="lang-rust">proc refund(order_id, reason, actor) {
    let order = find_one(<span class="str">"orders"</span>, {_id: order_id})
    <span class="kw">if</span> order == <span class="kw">null</span>            { <span class="kw">abort</span> <span class="str">"order not found"</span> }
    <span class="kw">if</span> order.status == <span class="str">"refunded"</span> { <span class="kw">abort</span> <span class="str">"already refunded"</span> }

    <span class="co">// Refund money</span>
    update(<span class="str">"accounts"</span>, {user_id: order.user_id}, {$inc: {balance: order.total}})

    <span class="co">// Restock</span>
    <span class="kw">for</span> item <span class="kw">in</span> order.items {
        update(<span class="str">"products"</span>, {sku: item.sku}, {$inc: {stock: item.qty, sold: -item.qty}})
    }

    update(<span class="str">"orders"</span>, {_id: order_id}, {$set: {status: <span class="str">"refunded"</span>, refund_reason: reason}})
    insert(<span class="str">"audit_log"</span>, {action: <span class="str">"refund"</span>, target: order_id, actor: actor, amount: order.total})
    <span class="kw">return</span> {ok: <span class="kw">true</span>, refunded: order.total}
}</code></pre>

<h3>cancel_cart</h3>
<pre><code class="lang-rust">proc cancel_cart(user_id) {
    let cart = find_one(<span class="str">"carts"</span>, {user_id: user_id, status: <span class="str">"open"</span>})
    <span class="kw">if</span> cart == <span class="kw">null</span> { <span class="kw">return</span> {ok: <span class="kw">true</span>, was_empty: <span class="kw">true</span>} }
    update(<span class="str">"carts"</span>, {_id: cart._id}, {$set: {status: <span class="str">"cancelled"</span>}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>top_products</h3>
<pre><code class="lang-rust">proc top_products(n) {
    <span class="kw">return</span> aggregate(<span class="str">"products"</span>, [
        {$match: {sold: {$gt: <span class="num">0</span>}}},
        {$sort: {sold: -<span class="num">1</span>}},
        {$limit: n},
        {$project: {sku: <span class="num">1</span>, name: <span class="num">1</span>, sold: <span class="num">1</span>}}
    ])
}</code></pre>

<div class="docs-prevnext">
  <a href="/oxiscript/recipes/banking/" class="prev"><div class="label">Previous</div><div class="title">← Banking</div></a>
  <a href="/oxiscript/recipes/inventory/" class="next"><div class="label">Next</div><div class="title">Inventory →</div></a>
</div>` }} />
}
