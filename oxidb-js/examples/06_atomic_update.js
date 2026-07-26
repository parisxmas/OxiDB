// 06_atomic_update.js — $inc + $push + $addToSet in ONE update call
//   (OCC validates one document version, so combine ops per doc).
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const posts = db.collection("posts");

  await posts.delete({});
  await posts.insert({
    slug: "intro-to-node",
    title: "Intro to Node.js",
    likes: 0,
    tags: ["beginner"],
  });

  // Single round-trip: bump likes, append a tag, dedupe-add a tag.
  await posts.update(
    { slug: "intro-to-node" },
    {
      $inc:      { likes: 1 },
      $push:     { tags: "tutorial" },
      $addToSet: { tags: "node" },     // skipped if already present
    },
  );

  console.log(await posts.findOne({ slug: "intro-to-node" }));
})().catch((e) => { console.error(e); process.exit(1); });
