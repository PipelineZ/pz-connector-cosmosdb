using Apache.Arrow.Types;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class ColumnPlanTests
{
    [Fact]
    public void Schema_types_and_id_nullability()
    {
        var plan = new ColumnPlan([ColumnSpec.Of("n", ColumnKind.Int64), ColumnSpec.Of("address.city", ColumnKind.String), ColumnSpec.Of("id", ColumnKind.String)]);
        Assert.Equal(["n", "address.city", "id"], plan.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int64Type>(plan.Schema.FieldsList[0].DataType);
        Assert.True(plan.Schema.FieldsList[0].IsNullable);
        Assert.False(plan.Schema.FieldsList[2].IsNullable);
    }

    [Fact]
    public void Top_level_projection_is_distinct_first_segments_plus_id()
    {
        var plan = new ColumnPlan([ColumnSpec.Of("address.city", ColumnKind.String), ColumnSpec.Of("address.zip", ColumnKind.String), ColumnSpec.Of("n", ColumnKind.Int64)]);
        Assert.Equal(["address", "n", "id"], plan.TopLevelProjection());
    }

    [Fact]
    public void Project_keeps_hint_order_and_falls_back_on_unknown_names()
    {
        var plan = new ColumnPlan([ColumnSpec.Of("a", ColumnKind.Int64), ColumnSpec.Of("b", ColumnKind.String), ColumnSpec.Of("id", ColumnKind.String)]);
        Assert.Equal(["id", "a"], plan.Project(["id", "a"]).Columns.Select(c => c.Name));
        Assert.Same(plan, plan.Project(["zzz"]));
        Assert.Same(plan, plan.Project(null));
        // Cosmos DB property names are case-sensitive, so a hint differing only by case names a
        // property this plan does not have: unusable, not a match.
        Assert.Same(plan, plan.Project(["A"]));
    }

    // ColumnKind is `internal`; InternalsVisibleTo makes it usable inside this assembly's method
    // bodies, but a *public* Theory method may not declare an internal type in its signature (CS0051
    // -- accessibility is about the declared member's signature, not assembly friendship), so the
    // expected kind is passed through InlineData as its underlying int and cast back inside the body.
    [Theory]
    [InlineData("string", (int)ColumnKind.String)]
    [InlineData("varchar", (int)ColumnKind.String)]
    [InlineData("int64", (int)ColumnKind.Int64)]
    [InlineData("bigint", (int)ColumnKind.Int64)]
    [InlineData("int32", (int)ColumnKind.Int32)]
    [InlineData("double", (int)ColumnKind.Double)]
    [InlineData("decimal", (int)ColumnKind.Decimal)]
    [InlineData("bool", (int)ColumnKind.Boolean)]
    [InlineData("timestamp", (int)ColumnKind.Timestamp)]
    [InlineData("date", (int)ColumnKind.Date)]
    [InlineData("json", (int)ColumnKind.Json)]
    public void Kinds_parse(string text, int kind)
    {
        Assert.True(ColumnPlan.TryParseKind(text, out var parsed));
        Assert.Equal((ColumnKind)kind, parsed);
    }

    [Fact]
    public void Name_validation_refuses_prefix_clashes_case_clashes_and_too_many()
    {
        var errors = new List<string>();
        ColumnPlan.ValidateNames(["a", "a.b", "X", "x"], "p", errors);
        Assert.Contains(errors, e => e.Contains("'a' is both a value and the parent of 'a.b'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("differ only by case", StringComparison.Ordinal));

        errors.Clear();
        ColumnPlan.ValidateNames(Enumerable.Range(0, ColumnPlan.MaxColumns + 1).Select(i => $"c{i}").ToList(), "p", errors);
        Assert.Contains(errors, e => e.Contains($"more than the {ColumnPlan.MaxColumns}", StringComparison.Ordinal));
    }

    /// <summary>An exactly repeated name is a reported error, once, not a throw out of the
    /// dictionary every caller keys its columns by.</summary>
    [Fact]
    public void Name_validation_refuses_an_exact_duplicate_once()
    {
        var errors = new List<string>();
        ColumnPlan.ValidateNames(["x", "y", "x", "x"], "p", errors);
        var error = Assert.Single(errors);
        Assert.Equal("p: column 'x' is declared more than once", error);
    }
}
