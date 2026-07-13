using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace OxiDb.EFCore.SpecTests;

public class NorthwindWhereQueryOxiDbTest
    : NorthwindWhereQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindWhereQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // ── expected translation failures, matching the SQLite provider ─────────
    // Constructed-object / tuple equality is reference equality in LINQ and
    // is not translated by first-party providers either.

    public override Task Where_compare_constructed_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_constructed_equal(async));

    public override Task Where_compare_constructed_multi_value_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_constructed_multi_value_equal(async));

    public override Task Where_compare_constructed_multi_value_not_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_constructed_multi_value_not_equal(async));

    public override Task Where_compare_tuple_constructed_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_constructed_equal(async));

    public override Task Where_compare_tuple_constructed_multi_value_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_constructed_multi_value_equal(async));

    public override Task Where_compare_tuple_constructed_multi_value_not_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_constructed_multi_value_not_equal(async));

    public override Task Where_compare_tuple_create_constructed_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_equal(async));

    public override Task Where_compare_tuple_create_constructed_multi_value_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_multi_value_equal(async));

    public override Task Where_compare_tuple_create_constructed_multi_value_not_equal(bool async)
        => AssertTranslationFailed(() => base.Where_compare_tuple_create_constructed_multi_value_not_equal(async));

    // DateTimeOffset is not a mapped store type (TIMESTAMP is DateTime).
    public override Task Where_datetimeoffset_utcnow(bool async)
        => AssertTranslationFailed(() => base.Where_datetimeoffset_utcnow(async));

    public override Task Where_datetimeoffset_utcnow_component(bool async)
        => AssertTranslationFailed(() => base.Where_datetimeoffset_utcnow_component(async));

    public override Task Where_datetimeoffset_now_component(bool async)
        => AssertTranslationFailed(() => base.Where_datetimeoffset_now_component(async));

}
