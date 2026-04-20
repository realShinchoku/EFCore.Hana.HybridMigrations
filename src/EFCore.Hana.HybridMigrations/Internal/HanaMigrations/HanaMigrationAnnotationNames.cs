using System.Text.Json;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal static class HanaMigrationAnnotationNames
{
    public const string PrimaryKeyName = "HanaHybrid:PrimaryKeyName";
    public const string ForeignKeyNames = "HanaHybrid:ForeignKeyNames";
    public const string UniqueConstraintNames = "HanaHybrid:UniqueConstraintNames";
    public const string IndexDefinitions = "HanaHybrid:IndexDefinitions";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General);

    public static string SerializeNames(IEnumerable<string> names)
        => JsonSerializer.Serialize(
            names
                .Where(static name => !string.IsNullOrWhiteSpace(name))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(static name => name, StringComparer.Ordinal)
                .ToArray(),
            JsonOptions);

    public static IReadOnlyList<string> DeserializeNames(object? value)
    {
        if (value is not string json || string.IsNullOrWhiteSpace(json)) return [];

        return JsonSerializer.Deserialize<string[]>(json, JsonOptions) ?? [];
    }

    public static string SerializeIndexes(IEnumerable<CreateIndexOperation> indexes)
        => JsonSerializer.Serialize(
            indexes
                .Select(HanaIndexDefinition.FromOperation)
                .DistinctBy(static index => index.Key)
                .OrderBy(static index => index.Key, StringComparer.Ordinal)
                .ToArray(),
            JsonOptions);

    public static IReadOnlyList<HanaIndexDefinition> DeserializeIndexes(object? value)
    {
        if (value is not string json || string.IsNullOrWhiteSpace(json)) return [];

        return JsonSerializer.Deserialize<HanaIndexDefinition[]>(json, JsonOptions) ?? [];
    }
}