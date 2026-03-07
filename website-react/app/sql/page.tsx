import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "SQL Support",
  description: `Standard SQL alongside JSON queries. Collections are tables.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="sql" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg> SQL Support</h2>
    <p class="section-desc">Standard SQL alongside JSON queries. Collections are tables.</p>

    <pre><code class="lang-sql"><span class="kw">SELECT</span> name, age, department
<span class="kw">FROM</span> users
<span class="kw">WHERE</span> age >= <span class="num">25</span> <span class="kw">AND</span> department = <span class="str">'Engineering'</span>
<span class="kw">ORDER BY</span> age <span class="kw">DESC</span>
<span class="kw">LIMIT</span> <span class="num">10</span>;

<span class="kw">INSERT INTO</span> users (name, age) <span class="kw">VALUES</span> (<span class="str">'Alice'</span>, <span class="num">30</span>);

<span class="kw">UPDATE</span> users <span class="kw">SET</span> age = <span class="num">31</span> <span class="kw">WHERE</span> name = <span class="str">'Alice'</span>;

<span class="kw">DELETE FROM</span> users <span class="kw">WHERE</span> status = <span class="str">'inactive'</span>;

<span class="co">-- Aggregation</span>
<span class="kw">SELECT</span> department, <span class="fn">COUNT</span>(*), <span class="fn">AVG</span>(salary)
<span class="kw">FROM</span> users
<span class="kw">GROUP BY</span> department;

<span class="co">-- Joins</span>
<span class="kw">SELECT</span> u.name, o.total
<span class="kw">FROM</span> users u
<span class="kw">JOIN</span> orders o <span class="kw">ON</span> u._id = o.user_id;

<span class="co">-- Index management</span>
<span class="kw">CREATE INDEX</span> idx_email <span class="kw">ON</span> users(email);
<span class="kw">DROP INDEX</span> idx_email <span class="kw">ON</span> users;

<span class="co">-- Database management</span>
<span class="kw">CREATE DATABASE</span> myapp;
<span class="kw">USE</span> myapp;
<span class="kw">SHOW DATABASES</span>;</code></pre>
  </div>
</section>` }} />
}