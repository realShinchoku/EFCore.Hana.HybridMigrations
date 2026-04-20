#pragma warning disable EF1001
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Sap.EntityFrameworkCore.Hana.Migrations;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaHybridMigrationsSqlGenerator(
    MigrationsSqlGeneratorDependencies dependencies,
    IRelationalAnnotationProvider relationalAnnotationProvider)
    : HanaMigrationsSqlGenerator(dependencies, relationalAnnotationProvider)
{
    private int _currentOperationIndex;

    private IReadOnlyList<MigrationOperation>? _currentOperations;
    private Dictionary<string, HanaPendingIndexRecreate>? _pendingIndexRecreates;
    private HashSet<int>? _skippedOperationIndices;

    internal ISqlGenerationHelper SqlGenerationHelper => Dependencies.SqlGenerationHelper;

    internal string StatementTerminator => Dependencies.SqlGenerationHelper.StatementTerminator;

    internal string DelimitColumn(string columnName) => Dependencies.SqlGenerationHelper.DelimitIdentifier(columnName);

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("CREATE COLUMN TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            CreateTableColumns(operation, model, builder);

            if (operation.PrimaryKey is not null
                || operation.ForeignKeys.Count > 0
                || operation.UniqueConstraints.Count > 0
                || operation.CheckConstraints.Count > 0)
                CreateTableConstraints(operation, model, builder);
        }

        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD (");

        ColumnDefinition(operation.Schema, operation.Table, operation.Name, operation, model, builder);

        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP (")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var newName = operation.NewName
                      ?? throw new InvalidOperationException("RenameColumnOperation.NewName is required.");

        builder
            .Append("RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(".")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(newName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);

        EndStatement(builder);
    }

    protected override void Generate(
        RenameTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var newName = operation.NewName
                      ?? throw new InvalidOperationException("RenameTableOperation.NewName is required.");

        builder
            .Append("RENAME TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(newName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);

        EndStatement(builder);
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append("CREATE ");

        if (operation.IsUnique) builder.Append("UNIQUE ");

        builder
            .Append("INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" ON ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" (");

        for (var index = 0; index < operation.Columns.Length; index++)
        {
            if (index > 0) builder.Append(", ");

            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Columns[index]));

#if !HANA_EFCORE_6
            if (operation.IsDescending is { Length: > 0 } && operation.IsDescending[index]) builder.Append(" DESC");
#endif
        }

        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void Generate(
        EnsureSchemaOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    public override IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model,
        MigrationsSqlGenerationOptions options)
    {
        if (operations.All(static operation => operation is not AlterColumnOperation and not DropColumnOperation))
            return base.Generate(operations, model, options);

        var previousOperations = _currentOperations;
        var previousSkipped = _skippedOperationIndices;
        var previousPending = _pendingIndexRecreates;
        var previousIndex = _currentOperationIndex;

        _currentOperations = operations;
        _skippedOperationIndices = [];
        _pendingIndexRecreates = new Dictionary<string, HanaPendingIndexRecreate>(StringComparer.Ordinal);

        try
        {
            var commands = new List<MigrationCommand>();

            for (var index = 0; index < operations.Count; index++)
            {
                if (_skippedOperationIndices.Contains(index)) continue;

                _currentOperationIndex = index;
                var operation = operations[index];

                if (operation is AlterColumnOperation alterColumnOperation)
                {
                    var builder = new MigrationCommandListBuilder(Dependencies);
                    Generate(alterColumnOperation, model, builder);
                    commands.AddRange(builder.GetCommandList());
                    continue;
                }

                if (operation is DropColumnOperation dropColumnOperation)
                {
                    var builder = new MigrationCommandListBuilder(Dependencies);
                    Generate(dropColumnOperation, model, builder);
                    commands.AddRange(builder.GetCommandList());
                    continue;
                }

                commands.AddRange(base.Generate([operation], model, options));
            }

            return commands;
        }
        finally
        {
            _currentOperations = previousOperations;
            _skippedOperationIndices = previousSkipped;
            _pendingIndexRecreates = previousPending;
            _currentOperationIndex = previousIndex;
        }
    }

    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var policy = ResolvePolicy();
        var operations = _currentOperations ?? [operation];
        var operationIndex = _currentOperations is null ? 0 : _currentOperationIndex;
        IEnumerable<string> pendingIndexKeys = _pendingIndexRecreates is null ? [] : _pendingIndexRecreates.Keys;

        var dependencyMap = HanaDependencyMapBuilder.Build(operations, operationIndex, operation, pendingIndexKeys);
        var analysis = HanaAlterColumnAnalyzer.Analyze(operation, dependencyMap, policy);
        if (analysis.Strategy == HanaAlterColumnStrategy.Blocked)
            throw new InvalidOperationException(string.Join(" ", analysis.BlockingReasons));

        var indexesToDrop = Array.Empty<HanaIndexDefinition>();
        var indexesToRecreate = CollectPendingIndexRecreates(operationIndex);

        if (_currentOperations is not null)
        {
            var plan = BuildIndexManagementPlan(operations, operationIndex, dependencyMap.ActiveIndexes);
            indexesToDrop = plan.IndexesToDrop;
            indexesToRecreate = indexesToRecreate.Concat(plan.IndexesToRecreateNow).ToArray();
        }

        HanaAlterColumnSqlBuilder.Build(
            this,
            operation,
            model,
            builder,
            analysis,
            indexesToDrop,
            indexesToRecreate,
            policy);
    }

    internal void EmitAddColumn(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => Generate(operation, model, builder);

    internal void EmitDropColumn(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => Generate(operation, model, builder);

    internal void EmitRenameColumn(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => Generate(operation, model, builder);

    internal void EmitAlterColumn(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ALTER (");

        ColumnDefinition(operation.Schema, operation.Table, operation.Name, operation, model, builder);

        builder
            .Append(")")
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);

        EndStatement(builder);
    }

    internal void EmitDropIndex(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => Generate(
            new DropIndexOperation
            {
                Name = operation.Name,
                Schema = operation.Schema,
                Table = operation.Table
            },
            model,
            builder);

    internal void EmitCreateIndex(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
        => Generate(operation, model, builder);

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var columnType = operation.ColumnType
                         ?? GetColumnType(schema, table, name, operation, model)
                         ?? throw new InvalidOperationException(
                             $"Unable to resolve SAP HANA store type for column '{table}.{name}'.");

        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
            .Append(" ")
            .Append(columnType);

        if (operation["Hana:Autoincrement"] is not null) builder.Append(" GENERATED BY DEFAULT AS IDENTITY");

        builder.Append(operation.IsNullable ? " NULL" : " NOT NULL");

        if (operation.DefaultValueSql is not null)
        {
            builder.Append(" DEFAULT ").Append(operation.DefaultValueSql);
        }
        else if (operation.DefaultValue is not null)
        {
            if (operation.DefaultValue is bool boolValue)
            {
                builder.Append(" DEFAULT ").Append(boolValue ? "TRUE" : "FALSE");
                return;
            }

            var mapping = Dependencies.TypeMappingSource.FindMapping(operation.ClrType, columnType);
            builder.Append(" DEFAULT ").Append(mapping!.GenerateSqlLiteral(operation.DefaultValue));
        }
    }

    private HanaMigrationPolicy ResolvePolicy()
    {
        var options = Dependencies.CurrentContext.Context.GetService<IDbContextOptions>();
        return options.FindExtension<HanaHybridMigrationsOptionsExtension>()?.Policy ?? HanaMigrationPolicy.Default;
    }

    private (HanaIndexDefinition[] IndexesToDrop, HanaIndexDefinition[] IndexesToRecreateNow) BuildIndexManagementPlan(
        IReadOnlyList<MigrationOperation> operations,
        int currentIndex,
        IReadOnlyList<HanaIndexDefinition> activeIndexes)
    {
        if (_pendingIndexRecreates is null) return ([], []);

        var indexesToDrop = new List<HanaIndexDefinition>();
        var indexesToRecreateNow = new List<HanaIndexDefinition>();

        foreach (var indexDefinition in activeIndexes)
        {
            if (_pendingIndexRecreates.ContainsKey(indexDefinition.Key)) continue;

            indexesToDrop.Add(indexDefinition);

            var nextDropIndex = FindNextOperationIndex(
                operations,
                currentIndex + 1,
                operation => operation is DropIndexOperation { Table: not null } dropIndex &&
                             IsSameIndex(dropIndex.Schema, dropIndex.Table, dropIndex.Name, indexDefinition));
            if (nextDropIndex >= 0) _skippedOperationIndices?.Add(nextDropIndex);

            var nextCreateIndex = FindNextOperationIndex(
                operations,
                currentIndex + 1,
                operation => operation is CreateIndexOperation createIndex &&
                             IsSameIndex(createIndex.Schema, createIndex.Table, createIndex.Name, indexDefinition));
            if (nextCreateIndex >= 0) continue;

            var lastRelevantAlterIndex = FindLastRelevantAlterIndex(operations, currentIndex, indexDefinition);
            if (lastRelevantAlterIndex > currentIndex)
            {
                _pendingIndexRecreates[indexDefinition.Key] =
                    new HanaPendingIndexRecreate(indexDefinition, lastRelevantAlterIndex);
                continue;
            }

            indexesToRecreateNow.Add(indexDefinition);
        }

        return (indexesToDrop.ToArray(), indexesToRecreateNow.ToArray());
    }

    private HanaIndexDefinition[] CollectPendingIndexRecreates(int currentIndex)
    {
        if (_pendingIndexRecreates is null || _pendingIndexRecreates.Count == 0) return [];

        var dueIndexes = _pendingIndexRecreates.Values
            .Where(plan => plan.LastRelevantAlterIndex == currentIndex)
            .Select(plan => plan.Index)
            .ToArray();

        foreach (var dueIndex in dueIndexes) _pendingIndexRecreates.Remove(dueIndex.Key);

        return dueIndexes;
    }

    private static int FindLastRelevantAlterIndex(
        IReadOnlyList<MigrationOperation> operations,
        int currentIndex,
        HanaIndexDefinition indexDefinition)
    {
        var lastIndex = currentIndex;

        for (var index = currentIndex + 1; index < operations.Count; index++)
        {
            if (operations[index] is not AlterColumnOperation alterColumn) continue;

            if (!IsSameTable(alterColumn.Schema, alterColumn.Table, indexDefinition.Schema,
                    indexDefinition.Table)) continue;

            if (indexDefinition.Columns.Contains(alterColumn.Name, StringComparer.Ordinal)) lastIndex = index;
        }

        return lastIndex;
    }

    private static int FindNextOperationIndex(
        IReadOnlyList<MigrationOperation> operations,
        int startIndex,
        Func<MigrationOperation, bool> predicate)
    {
        for (var index = startIndex; index < operations.Count; index++)
            if (predicate(operations[index]))
                return index;

        return -1;
    }

    private static bool IsSameIndex(string? schema, string table, string name, HanaIndexDefinition indexDefinition)
        => string.Equals(schema, indexDefinition.Schema, StringComparison.Ordinal) &&
           string.Equals(table, indexDefinition.Table, StringComparison.Ordinal) &&
           string.Equals(name, indexDefinition.Name, StringComparison.Ordinal);

    private static bool IsSameTable(string? leftSchema, string leftTable, string? rightSchema, string rightTable)
        => string.Equals(leftSchema, rightSchema, StringComparison.Ordinal) &&
           string.Equals(leftTable, rightTable, StringComparison.Ordinal);
}
#pragma warning restore EF1001
