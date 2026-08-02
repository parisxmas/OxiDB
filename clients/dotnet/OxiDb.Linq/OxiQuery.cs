using System.Linq.Expressions;

namespace OxiDb.Linq;

/// <summary>
/// Intermediate representation of a translated LINQ query. The translator
/// builds one of these; the executor turns it into wire calls.
/// </summary>
internal sealed class OxiQuery
{
    /// <summary>Mongo-style filter document. Keys are field names, values are literals or {$op: ...}.</summary>
    public Dictionary<string, object?>? Filter { get; set; }

    /// <summary>Sort spec. Keys are field names, values are 1 (asc) or -1 (desc).</summary>
    public Dictionary<string, int>? Sort { get; set; }

    public int? Skip { get; set; }
    public int? Take { get; set; }

    /// <summary>What to return at the end of the pipeline.</summary>
    public OxiResultKind ResultKind { get; set; } = OxiResultKind.List;

    /// <summary>For First / FirstOrDefault / Single / SingleOrDefault and predicates on them.</summary>
    public bool DefaultIfEmpty { get; set; }
    public bool SingleResult { get; set; }

    /// <summary>For Sum / Min / Max / Average — the field selector lambda.</summary>
    public LambdaExpression? AggregateSelector { get; set; }

    /// <summary>For Select — client-side projection lambda (applied after fetching).</summary>
    public LambdaExpression? Projection { get; set; }
}

internal enum OxiResultKind
{
    List,
    First,
    Single,
    Count,
    Any,
    Sum,
    Min,
    Max,
    Average,
}
