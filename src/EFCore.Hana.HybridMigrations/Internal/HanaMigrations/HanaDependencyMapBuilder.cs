using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaDependencyMapBuilder
{
    public static HanaColumnDependencyMap Build(
        IReadOnlyList<MigrationOperation> operations,
        int alterIndex,
        AlterColumnOperation operation,
        IEnumerable<string>? inactiveIndexKeys = null)
    {
        var primaryKeyName = operation[HanaMigrationAnnotationNames.PrimaryKeyName] as string;
        var foreignKeys = new HashSet<string>(
            HanaMigrationAnnotationNames.DeserializeNames(operation[HanaMigrationAnnotationNames.ForeignKeyNames]),
            StringComparer.Ordinal);
        var uniqueConstraints = new HashSet<string>(
            HanaMigrationAnnotationNames.DeserializeNames(
                operation[HanaMigrationAnnotationNames.UniqueConstraintNames]),
            StringComparer.Ordinal);
        var indexes = HanaMigrationAnnotationNames
            .DeserializeIndexes(operation[HanaMigrationAnnotationNames.IndexDefinitions])
            .ToDictionary(static index => index.Key, StringComparer.Ordinal);

        foreach (var inactiveKey in inactiveIndexKeys ?? []) indexes.Remove(inactiveKey);

        for (var index = 0; index < alterIndex; index++)
            switch (operations[index])
            {
                case DropPrimaryKeyOperation dropPrimaryKey
                    when IsSameTable(dropPrimaryKey.Schema, dropPrimaryKey.Table, operation.Schema, operation.Table) &&
                         string.Equals(dropPrimaryKey.Name, primaryKeyName, StringComparison.Ordinal):
                    primaryKeyName = null;
                    break;

                case AddPrimaryKeyOperation addPrimaryKey
                    when IsSameTable(addPrimaryKey.Schema, addPrimaryKey.Table, operation.Schema, operation.Table) &&
                         addPrimaryKey.Columns.Contains(operation.Name, StringComparer.Ordinal):
                    primaryKeyName = addPrimaryKey.Name;
                    break;

                case DropForeignKeyOperation dropForeignKey
                    when IsSameTable(dropForeignKey.Schema, dropForeignKey.Table, operation.Schema, operation.Table):
                    foreignKeys.Remove(dropForeignKey.Name);
                    break;

                case AddForeignKeyOperation addForeignKey
                    when IsDependentForeignKey(addForeignKey, operation) ||
                         IsPrincipalForeignKey(addForeignKey, operation):
                    foreignKeys.Add(addForeignKey.Name);
                    break;

                case DropUniqueConstraintOperation dropUniqueConstraint
                    when IsSameTable(dropUniqueConstraint.Schema,
                        dropUniqueConstraint.Table,
                        operation.Schema,
                        operation.Table):
                    uniqueConstraints.Remove(dropUniqueConstraint.Name);
                    break;

                case AddUniqueConstraintOperation addUniqueConstraint
                    when IsSameTable(addUniqueConstraint.Schema,
                             addUniqueConstraint.Table,
                             operation.Schema,
                             operation.Table) &&
                         addUniqueConstraint.Columns.Contains(operation.Name, StringComparer.Ordinal):
                    uniqueConstraints.Add(addUniqueConstraint.Name);
                    break;

                case DropIndexOperation { Table: not null } dropIndex
                    when IsSameTable(dropIndex.Schema, dropIndex.Table, operation.Schema, operation.Table):
                    indexes.Remove(HanaIndexDefinition.BuildKey(dropIndex.Schema, dropIndex.Table, dropIndex.Name));
                    break;

                case CreateIndexOperation createIndex
                    when IsSameTable(createIndex.Schema, createIndex.Table, operation.Schema, operation.Table) &&
                         createIndex.Columns.Contains(operation.Name, StringComparer.Ordinal):
                    var indexDefinition = HanaIndexDefinition.FromOperation(createIndex);
                    indexes[indexDefinition.Key] = indexDefinition;
                    break;
            }

        return new HanaColumnDependencyMap(
            primaryKeyName,
            foreignKeys.OrderBy(static name => name, StringComparer.Ordinal).ToArray(),
            uniqueConstraints.OrderBy(static name => name, StringComparer.Ordinal).ToArray(),
            indexes.Values
                .OrderBy(static definition => definition.Key, StringComparer.Ordinal)
                .ToArray());
    }

    private static bool IsDependentForeignKey(AddForeignKeyOperation addForeignKey, AlterColumnOperation operation)
        => IsSameTable(addForeignKey.Schema, addForeignKey.Table, operation.Schema, operation.Table) &&
           addForeignKey.Columns.Contains(operation.Name, StringComparer.Ordinal);

    private static bool IsPrincipalForeignKey(AddForeignKeyOperation addForeignKey, AlterColumnOperation operation)
        => IsSameTable(addForeignKey.PrincipalSchema, addForeignKey.PrincipalTable, operation.Schema,
               operation.Table) &&
           addForeignKey.PrincipalColumns is not null &&
           addForeignKey.PrincipalColumns.Contains(operation.Name, StringComparer.Ordinal);

    private static bool IsSameTable(string? leftSchema, string leftTable, string? rightSchema, string rightTable)
        => string.Equals(leftSchema, rightSchema, StringComparison.Ordinal) &&
           string.Equals(leftTable, rightTable, StringComparison.Ordinal);
}
