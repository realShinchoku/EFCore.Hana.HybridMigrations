using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaAlterColumnAnalyzer
{
    public static HanaAlterColumnAnalysis Analyze(
        AlterColumnOperation operation,
        HanaColumnDependencyMap dependencyMap,
        HanaMigrationPolicy policy)
    {
        if (operation.OldColumn is null)
            return new HanaAlterColumnAnalysis(
                HanaAlterColumnStrategy.Blocked,
                string.Empty,
                ["AlterColumnOperation.OldColumn is required for SAP HANA safe migrations."]);

        var blockingReasons = GetBlockingReasons(operation, dependencyMap, policy);
        if (blockingReasons.Count > 0)
            return new HanaAlterColumnAnalysis(HanaAlterColumnStrategy.Blocked, string.Empty, blockingReasons);

        var oldType = HanaStoreTypeDescriptor.From(operation.OldColumn);
        var newType = HanaStoreTypeDescriptor.From(operation);

        if (IsNoOp(operation.OldColumn, operation))
            return new HanaAlterColumnAnalysis(HanaAlterColumnStrategy.NoOp, string.Empty, []);

        if (CanUseSimpleAlter(operation.OldColumn, operation, oldType, newType))
            return new HanaAlterColumnAnalysis(HanaAlterColumnStrategy.SimpleAlter, string.Empty, []);

        if (!SupportsRecreate(operation.OldColumn, operation, oldType, newType))
            return new HanaAlterColumnAnalysis(
                HanaAlterColumnStrategy.Blocked,
                string.Empty,
                [
                    $"Unsupported SAP HANA conversion from '{operation.OldColumn.ColumnType ?? operation.OldColumn.ClrType.Name}' to '{operation.ColumnType ?? operation.ClrType.Name}' on '{operation.Table}.{operation.Name}'."
                ]);

        var strategy = dependencyMap.ActiveIndexes.Count > 0
            ? HanaAlterColumnStrategy.RecreateColumnWithIndexes
            : HanaAlterColumnStrategy.RecreateColumn;

        return new HanaAlterColumnAnalysis(strategy, BuildTempColumnName(operation), []);
    }

    private static List<string> GetBlockingReasons(
        AlterColumnOperation operation,
        HanaColumnDependencyMap dependencyMap,
        HanaMigrationPolicy policy)
    {
        var reasons = new List<string>();

        if (!string.IsNullOrWhiteSpace(operation.ComputedColumnSql) ||
            !string.IsNullOrWhiteSpace(operation.OldColumn?.ComputedColumnSql))
            reasons.Add(
                $"Computed column '{operation.Table}.{operation.Name}' is not supported by HanaHybrid alter-column automation.");

        if (policy.BlockOnPrimaryKey && !string.IsNullOrWhiteSpace(dependencyMap.PrimaryKeyName))
            reasons.Add(
                $"Column '{operation.Table}.{operation.Name}' is part of primary key '{dependencyMap.PrimaryKeyName}'. Drop and recreate the key explicitly before altering the column.");

        if (policy.BlockOnForeignKey && dependencyMap.ForeignKeyNames.Count > 0)
            reasons.Add(
                $"Column '{operation.Table}.{operation.Name}' is referenced by foreign keys: {string.Join(", ", dependencyMap.ForeignKeyNames)}.");

        if (policy.BlockOnUniqueConstraint && dependencyMap.UniqueConstraintNames.Count > 0)
            reasons.Add(
                $"Column '{operation.Table}.{operation.Name}' participates in unique constraints: {string.Join(", ", dependencyMap.UniqueConstraintNames)}.");

        if (!policy.AutoDropRecreateIndexes && dependencyMap.ActiveIndexes.Count > 0)
            reasons.Add(
                $"Column '{operation.Table}.{operation.Name}' is covered by indexes and AutoDropRecreateIndexes is disabled.");

        return reasons;
    }

    private static bool CanUseSimpleAlter(
        ColumnOperation oldColumn,
        AlterColumnOperation newColumn,
        HanaStoreTypeDescriptor oldType,
        HanaStoreTypeDescriptor newType)
    {
        if (HasIdentity(oldColumn) != HasIdentity(newColumn)) return false;

        if (!string.IsNullOrWhiteSpace(oldColumn.ComputedColumnSql) ||
            !string.IsNullOrWhiteSpace(newColumn.ComputedColumnSql))
            return false;

        if (oldType.Family != newType.Family) return false;

        if (oldType.Family == HanaTypeFamily.String)
        {
            if (!string.Equals(oldType.StoreTypeName, newType.StoreTypeName, StringComparison.Ordinal)) return false;

            if (IsStringShrink(oldColumn, newColumn)) return false;

            return true;
        }

        if (!string.Equals(oldColumn.ColumnType, newColumn.ColumnType, StringComparison.Ordinal)) return false;

        return true;
    }

    private static bool SupportsRecreate(
        ColumnOperation oldColumn,
        AlterColumnOperation newColumn,
        HanaStoreTypeDescriptor oldType,
        HanaStoreTypeDescriptor newType)
    {
        if (oldType.Family == HanaTypeFamily.Unknown || newType.Family == HanaTypeFamily.Unknown) return false;

        if (oldType.Family == HanaTypeFamily.Binary || newType.Family == HanaTypeFamily.Binary) return false;

        if (oldType.Family == newType.Family) return true;

        if (oldType.Family == HanaTypeFamily.String)
            return newType is
                   {
                       Family: HanaTypeFamily.String or HanaTypeFamily.Boolean or HanaTypeFamily.Date
                       or HanaTypeFamily.DateTime
                   } ||
                   newType.IsNumericFamily;

        if (newType.Family == HanaTypeFamily.String)
            return oldType is { Family: HanaTypeFamily.Boolean or HanaTypeFamily.Date or HanaTypeFamily.DateTime } ||
                   oldType.IsNumericFamily;

        if (oldType.IsNumericFamily && newType.IsNumericFamily) return true;

        if (oldType.Family == HanaTypeFamily.Date && newType.Family == HanaTypeFamily.DateTime) return true;

        return false;
    }

    private static bool IsNoOp(ColumnOperation oldColumn, AlterColumnOperation newColumn)
        => string.Equals(oldColumn.ColumnType, newColumn.ColumnType, StringComparison.Ordinal) &&
           oldColumn.IsNullable == newColumn.IsNullable &&
           Equals(oldColumn.DefaultValue, newColumn.DefaultValue) &&
           string.Equals(oldColumn.DefaultValueSql, newColumn.DefaultValueSql, StringComparison.Ordinal) &&
           oldColumn.MaxLength == newColumn.MaxLength &&
           oldColumn.Precision == newColumn.Precision &&
           oldColumn.Scale == newColumn.Scale &&
           string.Equals(oldColumn.Comment, newColumn.Comment, StringComparison.Ordinal) &&
           string.Equals(oldColumn.Collation, newColumn.Collation, StringComparison.Ordinal) &&
           HasIdentity(oldColumn) == HasIdentity(newColumn);

    private static bool AreNonStructuralValuesEqual(ColumnOperation oldColumn, AlterColumnOperation newColumn)
        => oldColumn.IsNullable == newColumn.IsNullable &&
           Equals(oldColumn.DefaultValue, newColumn.DefaultValue) &&
           string.Equals(oldColumn.DefaultValueSql, newColumn.DefaultValueSql, StringComparison.Ordinal) &&
           oldColumn.MaxLength == newColumn.MaxLength &&
           oldColumn.Precision == newColumn.Precision &&
           oldColumn.Scale == newColumn.Scale &&
           string.Equals(oldColumn.Comment, newColumn.Comment, StringComparison.Ordinal) &&
           string.Equals(oldColumn.Collation, newColumn.Collation, StringComparison.Ordinal);

    private static bool IsStringLengthIncrease(ColumnOperation oldColumn, AlterColumnOperation newColumn)
        => oldColumn.MaxLength is int oldLength &&
           newColumn.MaxLength is int newLength &&
           newLength >= oldLength;

    private static bool IsStringShrink(ColumnOperation oldColumn, AlterColumnOperation newColumn)
        => oldColumn.MaxLength is int oldLength &&
           newColumn.MaxLength is int newLength &&
           newLength < oldLength;

    private static bool HasIdentity(Annotatable operation)
        => operation["Hana:Autoincrement"] is true;

    private static string BuildTempColumnName(AlterColumnOperation operation)
    {
        var input = $"{operation.Schema}|{operation.Table}|{operation.Name}";
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)))[..8];
        var prefix = operation.Name.Length > 40 ? operation.Name[..40] : operation.Name;
        return $"{prefix}_TMP_{hash}";
    }
}
