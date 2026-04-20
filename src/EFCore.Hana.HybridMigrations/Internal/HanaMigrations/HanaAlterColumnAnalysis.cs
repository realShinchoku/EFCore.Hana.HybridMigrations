namespace EFCore.Hana.HybridMigrations;

internal enum HanaAlterColumnStrategy
{
    NoOp,
    SimpleAlter,
    RecreateColumn,
    RecreateColumnWithIndexes,
    Blocked
}

internal sealed record HanaAlterColumnAnalysis(
    HanaAlterColumnStrategy Strategy,
    string TempColumnName,
    IReadOnlyList<string> BlockingReasons)
{
    public bool RequiresRecreate
        => Strategy is HanaAlterColumnStrategy.RecreateColumn or HanaAlterColumnStrategy.RecreateColumnWithIndexes;
}

internal sealed record HanaPreflightValidation(
    string Name,
    string InvalidPredicate,
    string ErrorMessage);

internal sealed record HanaConversionPlan(
    string AssignmentExpression,
    IReadOnlyList<HanaPreflightValidation> Validations);

internal sealed record HanaColumnDependencyMap(
    string? PrimaryKeyName,
    IReadOnlyList<string> ForeignKeyNames,
    IReadOnlyList<string> UniqueConstraintNames,
    IReadOnlyList<HanaIndexDefinition> ActiveIndexes);

internal sealed record HanaPendingIndexRecreate(
    HanaIndexDefinition Index,
    int LastRelevantAlterIndex);