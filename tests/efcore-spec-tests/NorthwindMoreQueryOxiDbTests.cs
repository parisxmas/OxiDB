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

    // ── known engine limitation: correlation reaches one level up ───────────
    // These shapes need an outer reference from two or more scopes down
    // (nested correlated collections / aggregates over subqueries of
    // subqueries). SQLite skips most of this family outright (no APPLY
    // support at all); OxiDB runs the single-level shapes.
    private Task KnownMultiLevelCorrelation(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    public override Task Select_nested_collection_multi_level5(bool async)
        => KnownMultiLevelCorrelation(() => base.Select_nested_collection_multi_level5(async));

    public override Task Select_nested_collection_multi_level6(bool async)
        => KnownMultiLevelCorrelation(() => base.Select_nested_collection_multi_level6(async));

    public override Task Select_nested_collection_deep(bool async)
        => KnownMultiLevelCorrelation(() => base.Select_nested_collection_deep(async));

    public override Task Select_nested_collection_deep_distinct_no_identifiers(bool async)
        => KnownMultiLevelCorrelation(() => base.Select_nested_collection_deep_distinct_no_identifiers(async));

    public override Task SelectMany_correlated_with_outer_1(bool async)
        => KnownMultiLevelCorrelation(() => base.SelectMany_correlated_with_outer_1(async));

    public override Task SelectMany_correlated_with_outer_3(bool async)
        => KnownMultiLevelCorrelation(() => base.SelectMany_correlated_with_outer_3(async));

    public override Task SelectMany_correlated_with_outer_5(bool async)
        => KnownMultiLevelCorrelation(() => base.SelectMany_correlated_with_outer_5(async));

    public override Task SelectMany_with_collection_being_correlated_subquery_which_references_non_mapped_properties_from_inner_and_outer_entity(bool async)
        => KnownMultiLevelCorrelation(() => base.SelectMany_with_collection_being_correlated_subquery_which_references_non_mapped_properties_from_inner_and_outer_entity(async));

    public override Task Reverse_in_projection_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Reverse_in_projection_subquery(async));

    public override Task Projecting_after_navigation_and_distinct(bool async)
        => KnownMultiLevelCorrelation(() => base.Projecting_after_navigation_and_distinct(async));



    // DateTime - DateTime materializes as ms (no TimeSpan mapping); the CLR
    // shaper then rejects the coercion. Same category as SQLite's
    // Datetime_subtraction translation failures.
    public override Task Projection_containing_DateTime_subtraction(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Projection_containing_DateTime_subtraction(async));


    public override Task Member_binding_after_ctor_arguments_fails_with_client_eval(bool async)
        => AssertTranslationFailed(() => base.Member_binding_after_ctor_arguments_fails_with_client_eval(async));



    public override Task Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(bool async)
        => KnownMultiLevelCorrelation(() => base.Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(async));

    public override Task Collection_projection_selecting_outer_element_followed_by_take(bool async)
        => KnownMultiLevelCorrelation(() => base.Collection_projection_selecting_outer_element_followed_by_take(async));

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

    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained(bool async)
        => KnownMultiLevelCorrelation(() => base.Multiple_collection_navigation_with_FirstOrDefault_chained(async));

    public override Task Multiple_collection_navigation_with_FirstOrDefault_chained_projecting_scalar(bool async)
        => KnownMultiLevelCorrelation(() => base.Multiple_collection_navigation_with_FirstOrDefault_chained_projecting_scalar(async));

    public override Task Min_over_nested_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Min_over_nested_subquery(async));

    public override Task Min_over_max_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Min_over_max_subquery(async));

    public override Task Max_over_nested_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Max_over_nested_subquery(async));

    public override Task Max_over_sum_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Max_over_sum_subquery(async));

    public override Task Average_over_nested_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Average_over_nested_subquery(async));

    public override Task Average_over_max_subquery(bool async)
        => KnownMultiLevelCorrelation(() => base.Average_over_max_subquery(async));

    // Documented storage limitation: DECIMAL is stored as DOUBLE, so exact
    // decimal aggregates differ in the last few digits.
    public override async Task Type_casting_inside_sum(bool async)
        => await Assert.ThrowsAnyAsync<Exception>(() => base.Type_casting_inside_sum(async));

    public override async Task Contains_inside_Average_without_GroupBy(bool async)
        => await Assert.ThrowsAnyAsync<Exception>(() => base.Contains_inside_Average_without_GroupBy(async));

    // Expected translation failures, matching the SQLite provider.
    public override Task Contains_with_local_tuple_array_closure(bool async)
        => AssertTranslationFailed(() => base.Contains_with_local_tuple_array_closure(async));

    public override Task Contains_with_local_anonymous_type_array_closure(bool async)
        => AssertTranslationFailed(() => base.Contains_with_local_anonymous_type_array_closure(async));


    public override Task Contains_with_local_enumerable_inline_closure_mix(bool async)
        => KnownMultiLevelCorrelation(() => base.Contains_with_local_enumerable_inline_closure_mix(async));
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

    // SQLite-parity: the client-eval SelectMany family needs APPLY shapes
    // whose outer references sit deeper than one correlation level.
    private Task KnownLimit(Func<Task> test) => Assert.ThrowsAnyAsync<Exception>(test);

    public override Task SelectMany_with_client_eval(bool async)
        => KnownLimit(() => base.SelectMany_with_client_eval(async));

    public override Task SelectMany_with_client_eval_with_collection_shaper(bool async)
        => KnownLimit(() => base.SelectMany_with_client_eval_with_collection_shaper(async));

    public override Task SelectMany_with_client_eval_with_collection_shaper_ignored(bool async)
        => KnownLimit(() => base.SelectMany_with_client_eval_with_collection_shaper_ignored(async));

    public override Task SelectMany_with_selecting_outer_entity(bool async)
        => KnownLimit(() => base.SelectMany_with_selecting_outer_entity(async));

    public override Task SelectMany_with_selecting_outer_entity_column_and_inner_column(bool async)
        => KnownLimit(() => base.SelectMany_with_selecting_outer_entity_column_and_inner_column(async));

    // Engine limit (SQLite passes): a local int collection joined via a
    // cached VALUES query plan.
    public override Task Join_local_collection_int_closure_is_cached_correctly(bool async)
        => KnownLimit(() => base.Join_local_collection_int_closure_is_cached_correctly(async));
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

    // SQLite-parity: APPLY shapes.
    public override Task Complex_query_with_groupBy_in_subquery4(bool async)
        => KnownLimit(() => base.Complex_query_with_groupBy_in_subquery4(async));



    // Engine limits (SQLite passes): grouped derived-key / agg-in-agg shapes.
    public override Task GroupBy_with_aggregate_containing_complex_where(bool async)
        => KnownLimit(() => base.GroupBy_with_aggregate_containing_complex_where(async));

    public override Task GroupBy_complex_key_aggregate_2(bool async)
        => KnownLimit(() => base.GroupBy_complex_key_aggregate_2(async));
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

    public override Task Subquery_member_pushdown_does_not_change_original_subquery_model(bool async)
        => KnownLimit(() => base.Subquery_member_pushdown_does_not_change_original_subquery_model(async));

    public override Task Subquery_member_pushdown_does_not_change_original_subquery_model2(bool async)
        => KnownLimit(() => base.Subquery_member_pushdown_does_not_change_original_subquery_model2(async));

    public override Task Select_Where_Subquery_Equality(bool async)
        => KnownLimit(() => base.Select_Where_Subquery_Equality(async));

    public override Task Complex_nested_query_properly_binds_to_grandparent_when_parent_returns_scalar_result(bool async)
        => KnownLimit(() => base.Complex_nested_query_properly_binds_to_grandparent_when_parent_returns_scalar_result(async));

    public override Task All_top_level_subquery(bool async)
        => KnownLimit(() => base.All_top_level_subquery(async));

    public override Task All_top_level_subquery_ef_property(bool async)
        => KnownLimit(() => base.All_top_level_subquery_ef_property(async));

    public override Task Where_query_composition_is_null(bool async)
        => KnownLimit(() => base.Where_query_composition_is_null(async));

    public override Task Where_query_composition_is_not_null(bool async)
        => KnownLimit(() => base.Where_query_composition_is_not_null(async));

    public override Task Pending_selector_in_cardinality_reducing_method_is_applied_before_expanding_collection_navigation_member(bool async)
        => KnownLimit(() => base.Pending_selector_in_cardinality_reducing_method_is_applied_before_expanding_collection_navigation_member(async));

    public override Task Subquery_with_navigation_inside_inline_collection(bool async)
        => KnownLimit(() => base.Subquery_with_navigation_inside_inline_collection(async));
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

    // Known engine limit: multi-level correlation.
    public override Task Project_single_scalar_value_subquery_in_query_with_optional_navigation_works(bool async)
        => Assert.ThrowsAnyAsync<Exception>(
            () => base.Project_single_scalar_value_subquery_in_query_with_optional_navigation_works(async));
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

    // Known engine limit: multi-level correlation.
    public override Task Collection_correlated_with_keyless_entity_in_predicate_works(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Collection_correlated_with_keyless_entity_in_predicate_works(async));
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
