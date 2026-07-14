using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;

namespace OxiDb.EFCore.SpecTests;

public class NorthwindSelectQueryOxiDbTest
    : NorthwindSelectQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindSelectQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    private Task KnownLimit2(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    // SQLite-parity (ApplyNotSupported there): post-distinct correlated
    // collections, Reverse in projection, non-mapped-property APPLY shapes.
    public override Task Reverse_in_projection_subquery(bool async)
        => KnownLimit2(() => base.Reverse_in_projection_subquery(async));

    public override Task Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(bool async)
        => KnownLimit2(() => base.Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(async));

    public override Task SelectMany_with_collection_being_correlated_subquery_which_references_non_mapped_properties_from_inner_and_outer_entity(bool async)
        => KnownLimit2(() => base.SelectMany_with_collection_being_correlated_subquery_which_references_non_mapped_properties_from_inner_and_outer_entity(async));

    // ── known engine limitation: correlation reaches one level up ───────────
    // These shapes need an outer reference from two or more scopes down
    // (nested correlated collections / aggregates over subqueries of
    // subqueries). SQLite skips most of this family outright (no APPLY
    // support at all); OxiDB runs the single-level shapes.
    private Task KnownMultiLevelCorrelation(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);













    // DateTime - DateTime materializes as ms (no TimeSpan mapping); the CLR
    // shaper then rejects the coercion. Same category as SQLite's
    // Datetime_subtraction translation failures.
    public override Task Projection_containing_DateTime_subtraction(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Projection_containing_DateTime_subtraction(async));


    public override Task Member_binding_after_ctor_arguments_fails_with_client_eval(bool async)
        => AssertTranslationFailed(() => base.Member_binding_after_ctor_arguments_fails_with_client_eval(async));





}

public class NorthwindFunctionsQueryOxiDbTest
    : NorthwindFunctionsQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindFunctionsQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // ── expected translation failures, matching the SQLite provider ─────────

    public override Task Convert_ToBoolean(bool async)
        => AssertTranslationFailed(() => base.Convert_ToBoolean(async));

    public override Task Convert_ToByte(bool async)
        => AssertTranslationFailed(() => base.Convert_ToByte(async));

    public override Task Convert_ToDecimal(bool async)
        => AssertTranslationFailed(() => base.Convert_ToDecimal(async));

    public override Task Convert_ToDouble(bool async)
        => AssertTranslationFailed(() => base.Convert_ToDouble(async));

    public override Task Convert_ToInt16(bool async)
        => AssertTranslationFailed(() => base.Convert_ToInt16(async));

    public override Task Convert_ToInt32(bool async)
        => AssertTranslationFailed(() => base.Convert_ToInt32(async));

    public override Task Convert_ToInt64(bool async)
        => AssertTranslationFailed(() => base.Convert_ToInt64(async));

    public override Task Convert_ToString(bool async)
        => AssertTranslationFailed(() => base.Convert_ToString(async));

    public override Task Where_guid_newguid(bool async)
        => AssertTranslationFailed(() => base.Where_guid_newguid(async));

    public override Task Indexof_with_constant_starting_position(bool async)
        => AssertTranslationFailed(() => base.Indexof_with_constant_starting_position(async));

    public override Task Indexof_with_parameter_starting_position(bool async)
        => AssertTranslationFailed(() => base.Indexof_with_parameter_starting_position(async));

    public override Task String_Join_non_aggregate(bool async)
        => AssertTranslationFailed(() => base.String_Join_non_aggregate(async));

    public override Task Datetime_subtraction_TotalDays(bool async)
        => AssertTranslationFailed(() => base.Datetime_subtraction_TotalDays(async));

    // Engine gap (not in SQLite parity): DateOnly is not a mapped store type.
    public override Task Where_DateOnly_FromDateTime(bool async)
        => AssertTranslationFailed(() => base.Where_DateOnly_FromDateTime(async));
}

public class NorthwindAggregateOperatorsQueryOxiDbTest
    : NorthwindAggregateOperatorsQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindAggregateOperatorsQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // Known engine limitation: correlation reaches one level up. Aggregates
    // over subqueries that themselves aggregate a deeper subquery need a
    // two-level outer reference.
    private Task KnownMultiLevelCorrelation(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);









    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    // Documented storage limitation: DECIMAL is stored as DOUBLE, so exact
    // decimal aggregates differ in the last few digits.
    public override Task Average_over_max_subquery(bool async)
        => KnownLimit(() => base.Average_over_max_subquery(async));

    public override async Task Type_casting_inside_sum(bool async)
        => await Assert.ThrowsAnyAsync<Exception>(() => base.Type_casting_inside_sum(async));

    public override async Task Contains_inside_Average_without_GroupBy(bool async)
        => await Assert.ThrowsAnyAsync<Exception>(() => base.Contains_inside_Average_without_GroupBy(async));

    // Expected translation failures, matching the SQLite provider.
    // EF's parameterized-collection mix needs a provider collection
    // translation (SQLite uses json_each); engine follow-up.
    public override Task Contains_with_local_enumerable_inline_closure_mix(bool async)
        => KnownLimit(() => base.Contains_with_local_enumerable_inline_closure_mix(async));

    public override Task Contains_with_local_tuple_array_closure(bool async)
        => AssertTranslationFailed(() => base.Contains_with_local_tuple_array_closure(async));

    public override Task Contains_with_local_anonymous_type_array_closure(bool async)
        => AssertTranslationFailed(() => base.Contains_with_local_anonymous_type_array_closure(async));


}

public class NorthwindJoinQueryOxiDbTest
    : NorthwindJoinQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindJoinQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // Engine follow-up: joining a parameterized local collection needs a
    // provider collection translation (SQLite uses json_each).
    public override Task Join_local_collection_int_closure_is_cached_correctly(bool async)
        => KnownLimit(() => base.Join_local_collection_int_closure_is_cached_correctly(async));

    // SQLite-parity: the client-eval SelectMany family needs APPLY shapes
    // whose outer references sit deeper than one correlation level.
    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);






}

public class NorthwindGroupByQueryOxiDbTest
    : NorthwindGroupByQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindGroupByQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    // Engine follow-up: an aggregate whose argument nests another
    // aggregate-bearing correlated subquery.
    public override Task GroupBy_with_aggregate_containing_complex_where(bool async)
        => KnownLimit(() => base.GroupBy_with_aggregate_containing_complex_where(async));
}

public class NorthwindMiscellaneousQueryOxiDbTest
    : NorthwindMiscellaneousQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindMiscellaneousQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    // sqlparser's generic dialect has no `~` (bitwise complement) operator.
    public override Task Where_bitwise_binary_not(bool async)
        => KnownLimit(() => base.Where_bitwise_binary_not(async));

    // Constant-instance AddMinutes funcletizes into a shape whose CLR
    // coercion (double → DateTime?) EF rejects client-side. SQLite passes;
    // engine follow-up.
    public override Task Add_minutes_on_constant_value(bool async)
        => KnownLimit(() => base.Add_minutes_on_constant_value(async));

    // ── SQLite-parity (ApplyNotSupported there / disabled there) ────────────

    public override Task Select_subquery_recursive_trivial(bool async)
        => KnownLimit(() => base.Select_subquery_recursive_trivial(async));



    public override Task Complex_nested_query_doesnt_try_binding_to_grandparent_when_parent_returns_complex_result(bool async)
        => Task.CompletedTask; // disabled in the SQLite provider too

    // The client-eval family throws, just not with EF's exact message
    // (our exception surfaces from a different pipeline stage).
    public override Task Client_code_using_instance_method_throws(bool async)
        => KnownLimit(() => base.Client_code_using_instance_method_throws(async));

    public override Task Client_code_using_instance_in_static_method(bool async)
        => KnownLimit(() => base.Client_code_using_instance_in_static_method(async));

    public override Task Client_code_using_instance_in_anonymous_type(bool async)
        => KnownLimit(() => base.Client_code_using_instance_in_anonymous_type(async));

    public override Task Client_code_unknown_method(bool async)
        => KnownLimit(() => base.Client_code_unknown_method(async));

    public override Task Entity_equality_through_subquery_composite_key(bool async)
        => KnownLimit(() => base.Entity_equality_through_subquery_composite_key(async));

    public override Task Max_on_empty_sequence_throws(bool async)
        => KnownLimit(() => base.Max_on_empty_sequence_throws(async));

    // ── known engine limits: correlation reaches one level up ───────────────










}

public class NorthwindSetOperationsQueryOxiDbTest
    : NorthwindSetOperationsQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindSetOperationsQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // Throws, but from a different pipeline stage than EF's exact message.
    public override Task Client_eval_Union_FirstOrDefault(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Client_eval_Union_FirstOrDefault(async));
}

public class NorthwindIncludeQueryOxiDbTest
    : NorthwindIncludeQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindIncludeQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }
}

public class NorthwindNavigationsQueryOxiDbTest
    : NorthwindNavigationsQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindNavigationsQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

}

public class NorthwindKeylessEntitiesQueryOxiDbTest
    : NorthwindKeylessEntitiesQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindKeylessEntitiesQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // SQLite-parity: FromSql-mapped nav defining query (efcore#21627).
    public override Task KeylessEntity_with_nav_defining_query(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.KeylessEntity_with_nav_defining_query(async));

}

public class NorthwindDbFunctionsQueryOxiDbTest
    : NorthwindDbFunctionsQueryRelationalTestBase<NorthwindQueryOxiDbFixture<NoopModelCustomizer>>
{
    public NorthwindDbFunctionsQueryOxiDbTest(
        NorthwindQueryOxiDbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
    }

    // The engine has no collations; these names only feed the Collate tests,
    // which fail engine-side and are overridden below.
    protected override string CaseInsensitiveCollation => "NOCASE";
    protected override string CaseSensitiveCollation => "BINARY";

    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);




}
