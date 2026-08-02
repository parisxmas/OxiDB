// 04_pagination.js — skip + limit + total count for paginated UIs.
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const posts = db.collection("posts");

  await posts.delete({});
  await posts.insertMany(
    Array.from({ length: 47 }, (_, i) => ({
      slug: `post-${i + 1}`,
      title: `Post #${i + 1}`,
      created_at: Date.now() - i * 86400000,
    })),
  );

  const PAGE_SIZE = 10;
  const total = await posts.count();
  const pages = Math.ceil(total / PAGE_SIZE);
  console.log(`${total} posts in ${pages} pages of ${PAGE_SIZE}`);

  for (let page = 1; page <= pages; page++) {
    const rows = await posts.find({}, {
      sort: { created_at: -1 },
      skip: (page - 1) * PAGE_SIZE,
      limit: PAGE_SIZE,
    });
    console.log(`page ${page}: ${rows.map((r) => r.slug).join(", ")}`);
  }
})().catch((e) => { console.error(e); process.exit(1); });
