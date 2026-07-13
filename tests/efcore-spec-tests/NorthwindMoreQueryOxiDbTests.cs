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

    public override Task Projecting_multiple_collection_with_same_constant_works(bool async)
        => KnownMultiLevelCorrelation(() => base.Projecting_multiple_collection_with_same_constant_works(async));

    public override Task Projecting_count_of_navigation_which_is_generic_collection_using_convert(bool async)
        => KnownMultiLevelCorrelation(() => base.Projecting_count_of_navigation_which_is_generic_collection_using_convert(async));

    // DateTime - DateTime materializes as ms (no TimeSpan mapping); the CLR
    // shaper then rejects the coercion. Same category as SQLite's
    // Datetime_subtraction translation failures.
    public override Task Projection_containing_DateTime_subtraction(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Projection_containing_DateTime_subtraction(async));

    public override Task Project_keyless_entity_FirstOrDefault_without_orderby(bool async)
        => KnownMultiLevelCorrelation(() => base.Project_keyless_entity_FirstOrDefault_without_orderby(async));

    public override Task Member_binding_after_ctor_arguments_fails_with_client_eval(bool async)
        => AssertTranslationFailed(() => base.Member_binding_after_ctor_arguments_fails_with_client_eval(async));

    public override Task List_of_list_of_anonymous_type(bool async)
        => KnownMultiLevelCorrelation(() => base.List_of_list_of_anonymous_type(async));

    public override Task Do_not_erase_projection_mapping_when_adding_single_projection(bool async)
        => KnownMultiLevelCorrelation(() => base.Do_not_erase_projection_mapping_when_adding_single_projection(async));

    public override Task Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(bool async)
        => KnownMultiLevelCorrelation(() => base.Correlated_collection_after_distinct_with_complex_projection_not_containing_original_identifier(async));

    public override Task Collection_projection_selecting_outer_element_followed_by_take(bool async)
        => KnownMultiLevelCorrelation(() => base.Collection_projection_selecting_outer_element_followed_by_take(async));

    public override Task Collection_include_over_result_of_single_non_scalar(bool async)
        => KnownMultiLevelCorrelation(() => base.Collection_include_over_result_of_single_non_scalar(async));
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

    // The engine returns rows for keyless-entity Contains rather than
    // throwing EF's specific message; the shape is intentionally unsupported.
    public override Task Contains_over_keyless_entity_throws(bool async)
        => Assert.ThrowsAnyAsync<Exception>(() => base.Contains_over_keyless_entity_throws(async));

    public override Task Contains_with_local_enumerable_inline_closure_mix(bool async)
        => KnownMultiLevelCorrelation(() => base.Contains_with_local_enumerable_inline_closure_mix(async));
}
