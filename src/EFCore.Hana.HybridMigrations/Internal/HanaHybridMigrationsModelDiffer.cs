using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
#if HANA_EFCORE_6
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Update;
#endif
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaHybridMigrationsModelDiffer(
    IRelationalTypeMappingSource typeMappingSource,
    IMigrationsAnnotationProvider migrationsAnnotationProvider,
#if HANA_EFCORE_9 || HANA_EFCORE_10
    IRelationalAnnotationProvider relationalAnnotationProvider,
#endif
#pragma warning disable EF1001
#if HANA_EFCORE_6
    IChangeDetector changeDetector,
    IUpdateAdapterFactory updateAdapterFactory,
#else
    IRowIdentityMapFactory rowIdentityMapFactory,
#endif
    CommandBatchPreparerDependencies commandBatchPreparerDependencies)
#if HANA_EFCORE_6
    : MigrationsModelDiffer(typeMappingSource,
        migrationsAnnotationProvider,
        changeDetector,
        updateAdapterFactory,
        commandBatchPreparerDependencies)
#elif HANA_EFCORE_7 || HANA_EFCORE_8
    : MigrationsModelDiffer(typeMappingSource,
        migrationsAnnotationProvider,
        rowIdentityMapFactory,
        commandBatchPreparerDependencies)
#else
    : MigrationsModelDiffer(typeMappingSource,
        migrationsAnnotationProvider,
        relationalAnnotationProvider,
        rowIdentityMapFactory,
        commandBatchPreparerDependencies)
#endif
{
    protected override IEnumerable<MigrationOperation> Diff(
        IColumn source,
        IColumn target,
        DiffContext diffContext)
    {
        if (source.StoreType == target.StoreType && source.IsNullable != target.IsNullable)
        {
            var operation = CreateAlterColumnOperation(source, target);
            EnrichAlterColumnOperation(source, operation);
            yield return operation;
            yield break;
        }

        foreach (var operation in base.Diff(source, target, diffContext))
        {
            if (operation is AlterColumnOperation alterColumnOperation)
                EnrichAlterColumnOperation(source, alterColumnOperation);

            yield return operation;
        }
    }

    private static AlterColumnOperation CreateAlterColumnOperation(IColumn source, IColumn target)
    {
        var operation = new AlterColumnOperation
        {
            Name = target.Name,
            Schema = target.Table.Schema,
            Table = target.Table.Name,
            ClrType = ResolveClrType(target),
            ColumnType = target.StoreType,
            IsUnicode = target.IsUnicode,
            IsFixedLength = target.IsFixedLength,
            MaxLength = target.MaxLength,
            Precision = target.Precision,
            Scale = target.Scale,
            IsRowVersion = target.IsRowVersion,
            IsNullable = target.IsNullable,
            DefaultValue = target.DefaultValue,
            DefaultValueSql = target.DefaultValueSql,
            ComputedColumnSql = target.ComputedColumnSql,
            IsStored = target.IsStored,
            Comment = target.Comment,
            Collation = target.Collation,
            OldColumn = CreateOldColumn(source)
        };

        CopyAutoincrementAnnotation(target, operation);
        return operation;
    }

    private static AddColumnOperation CreateOldColumn(IColumn source)
    {
        var oldColumn = new AddColumnOperation
        {
            Name = source.Name,
            Schema = source.Table.Schema,
            Table = source.Table.Name,
            ClrType = ResolveClrType(source),
            ColumnType = source.StoreType,
            IsUnicode = source.IsUnicode,
            IsFixedLength = source.IsFixedLength,
            MaxLength = source.MaxLength,
            Precision = source.Precision,
            Scale = source.Scale,
            IsRowVersion = source.IsRowVersion,
            IsNullable = source.IsNullable,
            DefaultValue = source.DefaultValue,
            DefaultValueSql = source.DefaultValueSql,
            ComputedColumnSql = source.ComputedColumnSql,
            IsStored = source.IsStored,
            Comment = source.Comment,
            Collation = source.Collation
        };

        CopyAutoincrementAnnotation(source, oldColumn);
        return oldColumn;
    }

    private static void EnrichAlterColumnOperation(IColumn source, AlterColumnOperation operation)
    {
        if (source.Table.PrimaryKey?.Columns.Contains(source) == true)
            operation.AddAnnotation(HanaMigrationAnnotationNames.PrimaryKeyName, source.Table.PrimaryKey.Name);

        var foreignKeys = source.Table.ForeignKeyConstraints
            .Where(foreignKey => foreignKey.Columns.Contains(source) || foreignKey.PrincipalColumns.Contains(source))
            .Select(foreignKey => foreignKey.Name)
#if !HANA_EFCORE_6
            .Concat(source.Table.ReferencingForeignKeyConstraints
                .Where(foreignKey => foreignKey.PrincipalColumns.Contains(source))
                .Select(foreignKey => foreignKey.Name))
#endif
            ;
        operation.AddAnnotation(HanaMigrationAnnotationNames.ForeignKeyNames,
            HanaMigrationAnnotationNames.SerializeNames(foreignKeys));

        var uniqueConstraints = source.Table.UniqueConstraints
            .Where(uniqueConstraint => uniqueConstraint.Columns.Contains(source))
            .Select(uniqueConstraint => uniqueConstraint.Name);
        operation.AddAnnotation(HanaMigrationAnnotationNames.UniqueConstraintNames,
            HanaMigrationAnnotationNames.SerializeNames(uniqueConstraints));

        var indexes = source.Table.Indexes
            .Where(index => index.Columns.Contains(source))
            .Select(CreateIndexOperation.CreateFrom);
        operation.AddAnnotation(HanaMigrationAnnotationNames.IndexDefinitions,
            HanaMigrationAnnotationNames.SerializeIndexes(indexes));
    }

    private static Type ResolveClrType(IColumn column)
    {
        var mapping = column.PropertyMappings.FirstOrDefault();
        if (mapping is not null) return mapping.Property.ClrType;

#if HANA_EFCORE_6
        return typeof(object);
#else
        return column.ProviderClrType;
#endif
    }

    private static void CopyAutoincrementAnnotation(IColumn column, MigrationOperation target)
    {
        var hasAutoincrement = column.PropertyMappings
            .Any(mapping => mapping.Property.FindAnnotation("Hana:Autoincrement")?.Value is true);
        if (hasAutoincrement) target.AddAnnotation("Hana:Autoincrement", true);
    }
}
