using System.Text;

namespace BugTracker;

/// Full-text search over the bug corpus with Okapi BM25 ranking.
///
/// The corpus is small — a bug tracker, not a web index — so scoring in-process
/// on each query is cheaper and simpler than maintaining an inverted index, and
/// it gives true BM25: the best matches first, not just "contains the word".
/// The title is boosted (its tokens count several times) so a hit in the title
/// outranks the same hit buried in the body.
public static class Bm25
{
    // Standard Okapi parameters. k1 controls term-frequency saturation, b the
    // document-length normalization.
    private const double K1 = 1.2;
    private const double B = 0.75;
    // How many times title tokens are counted relative to body tokens.
    private const int TitleBoost = 3;

    public sealed record Doc(
        int Id, string Title, string Body, string Status, string Reporter,
        DateTime CreatedUtc, DateTime UpdatedUtc, int CommentCount);

    public sealed record Hit(Doc Doc, double Score);

    /// Score every doc against the query and return them with their BM25 score
    /// (unsorted — the caller filters score &gt; 0 and orders).
    public static List<Hit> Rank(IReadOnlyList<Doc> docs, string query)
    {
        var terms = Tokenize(query).Distinct().ToList();
        if (terms.Count == 0 || docs.Count == 0)
            return docs.Select(d => new Hit(d, 0)).ToList();

        // Tokenize each doc once, with the title repeated for its boost.
        var docTokens = new List<List<string>>(docs.Count);
        foreach (var d in docs)
        {
            var toks = new List<string>();
            var title = Tokenize(d.Title).ToList();
            for (var i = 0; i < TitleBoost; i++) toks.AddRange(title);
            toks.AddRange(Tokenize(d.Body));
            docTokens.Add(toks);
        }

        int n = docs.Count;
        double avgdl = docTokens.Average(t => (double)t.Count);
        if (avgdl <= 0) return docs.Select(d => new Hit(d, 0)).ToList();

        // Document frequency of each query term (how many docs contain it).
        var docFreq = new Dictionary<string, int>();
        foreach (var term in terms)
            docFreq[term] = docTokens.Count(t => t.Contains(term));

        // Precompute IDF per term.
        var idf = new Dictionary<string, double>();
        foreach (var term in terms)
        {
            int df = docFreq[term];
            // BM25 IDF with the +1 guard so it never goes negative.
            idf[term] = Math.Log(1 + (n - df + 0.5) / (df + 0.5));
        }

        var hits = new List<Hit>(n);
        for (var i = 0; i < n; i++)
        {
            var tokens = docTokens[i];
            int len = tokens.Count;
            var tf = new Dictionary<string, int>();
            foreach (var t in tokens)
                tf[t] = tf.TryGetValue(t, out var c) ? c + 1 : 1;

            double score = 0;
            foreach (var term in terms)
            {
                if (!tf.TryGetValue(term, out var f) || f == 0) continue;
                double denom = f + K1 * (1 - B + B * (len / avgdl));
                score += idf[term] * (f * (K1 + 1)) / denom;
            }
            hits.Add(new Hit(docs[i], score));
        }
        return hits;
    }

    /// Lowercase, split on anything that is not a letter or digit (Unicode-aware,
    /// so Turkish and other scripts tokenize correctly), drop 1-char tokens.
    private static IEnumerable<string> Tokenize(string? s)
    {
        if (string.IsNullOrEmpty(s)) yield break;
        var sb = new StringBuilder();
        foreach (var ch in s)
        {
            if (char.IsLetterOrDigit(ch))
            {
                sb.Append(char.ToLowerInvariant(ch));
            }
            else if (sb.Length > 0)
            {
                if (sb.Length >= 2) yield return sb.ToString();
                sb.Clear();
            }
        }
        if (sb.Length >= 2) yield return sb.ToString();
    }
}
