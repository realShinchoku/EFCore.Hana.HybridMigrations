using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Hana.HybridMigrations;

internal static class HanaAlterColumnSqlBuilder
{
    public static void Build(
        HanaHybridMigrationsSqlGenerator generator,
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        HanaAlterColumnAnalysis analysis,
        IReadOnlyList<HanaIndexDefinition> indexesToDrop,
        IReadOnlyList<HanaIndexDefinition> indexesToRecreate,
        HanaMigrationPolicy policy)
    {
        if (analysis.Strategy == HanaAlterColumnStrategy.NoOp) return;

        AppendComment(builder,
            $"HanaHybrid: {analysis.Strategy} for {FormatQualifiedColumn(generator.SqlGenerationHelper, operation.Schema, operation.Table, operation.Name)}");

        if (analysis.Strategy == HanaAlterColumnStrategy.SimpleAlter)
        {
            BuildSimpleAlter(generator, operation, model, builder, policy);
            return;
        }

        BuildRecreate(generator, operation, model, builder, analysis, indexesToDrop, indexesToRecreate, policy);
    }

    private static void BuildSimpleAlter(
        HanaHybridMigrationsSqlGenerator generator,
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        HanaMigrationPolicy policy)
    {
        if (policy.EnablePreflightValidation)
            EmitValidationBlock(
                generator.SqlGenerationHelper,
                operation.Schema,
                operation.Table,
                builder,
                HanaSqlExpressionFactory.CreateSimpleAlterValidations(
                    generator.DelimitColumn(operation.Name),
                    operation));

        generator.EmitAlterColumn(operation, model, builder);
    }

    private static void BuildRecreate(
        HanaHybridMigrationsSqlGenerator generator,
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        HanaAlterColumnAnalysis analysis,
        IReadOnlyList<HanaIndexDefinition> indexesToDrop,
        IReadOnlyList<HanaIndexDefinition> indexesToRecreate,
        HanaMigrationPolicy policy)
    {
        var sourceColumnExpression = generator.DelimitColumn(operation.Name);
        var conversionPlan = HanaSqlExpressionFactory.CreateConversionPlan(
            sourceColumnExpression,
            operation.OldColumn,
            operation,
            policy);

        var tempAddOperation = CreateTempAddColumnOperation(operation, analysis.TempColumnName);
        generator.EmitAddColumn(tempAddOperation, model, builder);

        if (policy.EnablePreflightValidation)
            EmitValidationBlock(
                generator.SqlGenerationHelper,
                operation.Schema,
                operation.Table,
                builder,
                conversionPlan.Validations);

        AppendComment(builder, $"HanaHybrid: copy data into temporary column {analysis.TempColumnName}");
        builder
            .Append("UPDATE ")
            .Append(generator.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" SET ")
            .Append(generator.DelimitColumn(analysis.TempColumnName))
            .Append(" = ")
            .Append(conversionPlan.AssignmentExpression)
            .AppendLine(generator.StatementTerminator);
        builder.EndCommand();

        foreach (var index in indexesToDrop)
        {
            AppendComment(builder, $"HanaHybrid: drop index {index.Name} before column swap");
            generator.EmitDropIndex(index.ToOperation(), model, builder);
        }

        generator.EmitDropColumn(
            new DropColumnOperation
            {
                Name = operation.Name,
                Schema = operation.Schema,
                Table = operation.Table
            },
            model,
            builder);

        generator.EmitRenameColumn(
            new RenameColumnOperation
            {
                Schema = operation.Schema,
                Table = operation.Table,
                Name = analysis.TempColumnName,
                NewName = operation.Name
            },
            model,
            builder);

        var finalAlterOperation = CreateFinalAlterOperation(operation, tempAddOperation);
        if (finalAlterOperation is not null) generator.EmitAlterColumn(finalAlterOperation, model, builder);

        foreach (var index in indexesToRecreate)
        {
            AppendComment(builder, $"HanaHybrid: recreate index {index.Name} after column swap");
            generator.EmitCreateIndex(index.ToOperation(), model, builder);
        }
    }

    private static AddColumnOperation CreateTempAddColumnOperation(AlterColumnOperation operation,
        string tempColumnName)
    {
        var tempColumn = new AddColumnOperation
        {
            Name = tempColumnName,
            Schema = operation.Schema,
            Table = operation.Table,
            ClrType = operation.ClrType,
            ColumnType = operation.ColumnType,
            IsUnicode = operation.IsUnicode,
            IsFixedLength = operation.IsFixedLength,
            MaxLength = operation.MaxLength,
            Precision = operation.Precision,
            Scale = operation.Scale,
            IsRowVersion = operation.IsRowVersion,
            IsNullable = true,
            DefaultValue = null,
            DefaultValueSql = null,
            ComputedColumnSql = operation.ComputedColumnSql,
            IsStored = operation.IsStored,
            Comment = operation.Comment,
            Collation = operation.Collation
        };

        if (operation["Hana:Autoincrement"] is not null)
            tempColumn.AddAnnotation("Hana:Autoincrement", operation["Hana:Autoincrement"]!);

        return tempColumn;
    }

    private static AlterColumnOperation? CreateFinalAlterOperation(
        AlterColumnOperation operation,
        AddColumnOperation tempAddOperation)
    {
        if (operation is { IsNullable: true, DefaultValue: null, DefaultValueSql: null })
            return null;

        var finalOperation = new AlterColumnOperation
        {
            Name = operation.Name,
            Schema = operation.Schema,
            Table = operation.Table,
            ClrType = operation.ClrType,
            ColumnType = operation.ColumnType,
            IsUnicode = operation.IsUnicode,
            IsFixedLength = operation.IsFixedLength,
            MaxLength = operation.MaxLength,
            Precision = operation.Precision,
            Scale = operation.Scale,
            IsRowVersion = operation.IsRowVersion,
            IsNullable = operation.IsNullable,
            DefaultValue = operation.DefaultValue,
            DefaultValueSql = operation.DefaultValueSql,
            ComputedColumnSql = operation.ComputedColumnSql,
            IsStored = operation.IsStored,
            Comment = operation.Comment,
            Collation = operation.Collation,
            OldColumn = new AddColumnOperation
            {
                Name = tempAddOperation.Name,
                Schema = tempAddOperation.Schema,
                Table = tempAddOperation.Table,
                ClrType = tempAddOperation.ClrType,
                ColumnType = tempAddOperation.ColumnType,
                IsUnicode = tempAddOperation.IsUnicode,
                IsFixedLength = tempAddOperation.IsFixedLength,
                MaxLength = tempAddOperation.MaxLength,
                Precision = tempAddOperation.Precision,
                Scale = tempAddOperation.Scale,
                IsRowVersion = tempAddOperation.IsRowVersion,
                IsNullable = tempAddOperation.IsNullable,
                DefaultValue = tempAddOperation.DefaultValue,
                DefaultValueSql = tempAddOperation.DefaultValueSql,
                ComputedColumnSql = tempAddOperation.ComputedColumnSql,
                IsStored = tempAddOperation.IsStored,
                Comment = tempAddOperation.Comment,
                Collation = tempAddOperation.Collation
            }
        };

        if (operation["Hana:Autoincrement"] is not null)
            finalOperation.AddAnnotation("Hana:Autoincrement", operation["Hana:Autoincrement"]!);

        return finalOperation;
    }

    private static void EmitValidationBlock(
        ISqlGenerationHelper sqlGenerationHelper,
        string? schema,
        string table,
        MigrationCommandListBuilder builder,
        IReadOnlyList<HanaPreflightValidation> validations)
    {
        if (validations.Count == 0) return;

        AppendComment(builder, "HanaHybrid: preflight validation");
        builder.AppendLine("DO BEGIN");
        using (builder.Indent())
        {
            builder.AppendLine("DECLARE invalid_count BIGINT;");

            foreach (var validation in validations)
            {
                builder
                    .Append("SELECT COUNT(*) INTO invalid_count FROM ")
                    .Append(sqlGenerationHelper.DelimitIdentifier(table, schema))
                    .Append(" WHERE ")
                    .Append(validation.InvalidPredicate)
                    .AppendLine(sqlGenerationHelper.StatementTerminator);

                builder
                    .Append("IF :invalid_count > 0 THEN SIGNAL SQL_ERROR_CODE 10001 SET MESSAGE_TEXT = ")
                    .Append(Quote(validation.ErrorMessage))
                    .AppendLine(sqlGenerationHelper.StatementTerminator);
                builder.AppendLine("END IF;");
            }
        }

        builder.AppendLine("END;");
        builder.EndCommand();
    }

    private static string FormatQualifiedColumn(
        ISqlGenerationHelper sqlGenerationHelper,
        string? schema,
        string table,
        string column)
        => $"{sqlGenerationHelper.DelimitIdentifier(table, schema)}.{sqlGenerationHelper.DelimitIdentifier(column)}";

    private static void AppendComment(MigrationCommandListBuilder builder, string message)
    {
        builder.Append("-- ").AppendLine(message);
    }

    private static string Quote(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
