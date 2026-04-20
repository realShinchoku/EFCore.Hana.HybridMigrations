using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal sealed record HanaIndexDefinition(
    string Name,
    string? Schema,
    string Table,
    string[] Columns,
    bool IsUnique,
    bool[]? IsDescending,
    string? Filter)
{
    public string Key => BuildKey(Schema, Table, Name);

    public static HanaIndexDefinition FromOperation(CreateIndexOperation operation)
        => new(
            operation.Name,
            operation.Schema,
            operation.Table,
            operation.Columns.ToArray(),
            operation.IsUnique,
#if HANA_EFCORE_6
            null,
#else
            operation.IsDescending?.ToArray(),
#endif
            operation.Filter);

    public CreateIndexOperation ToOperation()
        => new()
        {
            Name = Name,
            Schema = Schema,
            Table = Table,
            Columns = Columns.ToArray(),
            IsUnique = IsUnique,
#if !HANA_EFCORE_6
            IsDescending = IsDescending?.ToArray(),
#endif
            Filter = Filter
        };

    public bool ContainsColumn(string columnName)
        => Columns.Any(column => string.Equals(column, columnName, StringComparison.Ordinal));

    public static string BuildKey(string? schema, string table, string name)
        => $"{schema ?? string.Empty}|{table}|{name}";
}
