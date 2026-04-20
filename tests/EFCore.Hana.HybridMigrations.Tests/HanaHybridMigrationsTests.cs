using EFCore.Hana.HybridMigrations;

using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Sap.EntityFrameworkCore.Hana;

namespace EFCore.Hana.HybridMigrations.Tests;

public sealed class HanaHybridMigrationsTests
{
    [Fact]
    public void Analyzer_blocks_primary_key_column()
    {
        var operation = CreateAlterOperation(
            "CODE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(string),
            "NVARCHAR(100)");

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap("PK_HANA_SAMPLE", [], [], []),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.Blocked);
        result.BlockingReasons.Should()
            .Contain(reason => reason.Contains("primary key", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Analyzer_blocks_foreign_key_column()
    {
        var operation = CreateAlterOperation(
            "TENANT_ID",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(null, ["FK_HANA_SAMPLE_TENANT"], [], []),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.Blocked);
        result.BlockingReasons.Should()
            .Contain(reason => reason.Contains("foreign keys", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Analyzer_blocks_unique_constraint_column()
    {
        var operation = CreateAlterOperation(
            "CODE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(null, [], ["UQ_HANA_SAMPLE_CODE"], []),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.Blocked);
        result.BlockingReasons.Should()
            .Contain(reason => reason.Contains("unique constraints", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Analyzer_chooses_recreate_with_indexes_for_type_change()
    {
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT",
            newNullable: false);

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(
                null,
                [],
                [],
                [
                    new HanaIndexDefinition("IX_HANA_SAMPLE_LEGACY_VALUE", null, "HANA_SAMPLE", ["LEGACY_VALUE"], false,
                        null, null)
                ]),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.RecreateColumnWithIndexes);
        result.TempColumnName.Should().Contain("_TMP_");
    }

    [Fact]
    public void Analyzer_blocks_indexed_column_when_auto_drop_recreate_indexes_is_disabled()
    {
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT");

        var policy = HanaMigrationPolicy.Default with { AutoDropRecreateIndexes = false };
        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(
                null,
                [],
                [],
                [
                    new HanaIndexDefinition("IX_HANA_SAMPLE_LEGACY_VALUE", null, "HANA_SAMPLE", ["LEGACY_VALUE"], false,
                        null, null)
                ]),
            policy);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.Blocked);
        result.BlockingReasons.Should()
            .Contain(reason => reason.Contains("AutoDropRecreateIndexes", StringComparison.Ordinal));
    }

    [Fact]
    public void Analyzer_returns_simple_alter_for_string_length_increase()
    {
        var operation = CreateAlterOperation(
            "TITLE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            oldMaxLength: 50,
            newClrType: typeof(string),
            newColumnType: "NVARCHAR(100)",
            newMaxLength: 100);

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(null, [], [], []),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.SimpleAlter);
    }

    [Fact]
    public void Analyzer_returns_noop_for_identical_definition()
    {
        var operation = CreateAlterOperation(
            "TITLE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            oldMaxLength: 50,
            newClrType: typeof(string),
            newColumnType: "NVARCHAR(50)",
            newMaxLength: 50);

        var result = HanaAlterColumnAnalyzer.Analyze(
            operation,
            new HanaColumnDependencyMap(null, [], [], []),
            HanaMigrationPolicy.Default);

        result.Strategy.Should().Be(HanaAlterColumnStrategy.NoOp);
    }

    [Fact]
    public void Dependency_map_builder_respects_explicit_index_drop_before_alter()
    {
        var alterOperation = CreateAlterOperation(
            "CODE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");
        alterOperation.AddAnnotation(
            HanaMigrationAnnotationNames.IndexDefinitions,
            HanaMigrationAnnotationNames.SerializeIndexes(
            [
                new CreateIndexOperation
                {
                    Name = "IX_HANA_SAMPLE_CODE",
                    Table = "HANA_SAMPLE",
                    Columns = ["CODE"]
                }
            ]));

        MigrationOperation[] operations =
        [
            new DropIndexOperation
            {
                Name = "IX_HANA_SAMPLE_CODE",
                Table = "HANA_SAMPLE"
            },
            alterOperation
        ];

        var result = HanaDependencyMapBuilder.Build(operations, 1, alterOperation);

        result.ActiveIndexes.Should().BeEmpty();
    }

    [Fact]
    public void Dependency_map_builder_tracks_explicit_foreign_key_add_before_alter()
    {
        var alterOperation = CreateAlterOperation(
            "TENANT_ID",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");

        MigrationOperation[] operations =
        [
            new AddForeignKeyOperation
            {
                Name = "FK_HANA_SAMPLE_TENANT",
                Table = "HANA_SAMPLE",
                Columns = ["TENANT_ID"],
                PrincipalTable = "TENANT",
                PrincipalColumns = ["ID"]
            },
            alterOperation
        ];

        var result = HanaDependencyMapBuilder.Build(operations, 1, alterOperation);

        result.ForeignKeyNames.Should().ContainSingle("FK_HANA_SAMPLE_TENANT");
    }

    [Fact]
    public void Dependency_map_builder_removes_unique_constraint_after_explicit_drop()
    {
        var alterOperation = CreateAlterOperation(
            "CODE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");
        alterOperation.AddAnnotation(
            HanaMigrationAnnotationNames.UniqueConstraintNames,
            HanaMigrationAnnotationNames.SerializeNames(["UQ_HANA_SAMPLE_CODE"]));

        MigrationOperation[] operations =
        [
            new DropUniqueConstraintOperation
            {
                Name = "UQ_HANA_SAMPLE_CODE",
                Table = "HANA_SAMPLE"
            },
            alterOperation
        ];

        var result = HanaDependencyMapBuilder.Build(operations, 1, alterOperation);

        result.UniqueConstraintNames.Should().BeEmpty();
    }

    [Fact]
    public void Sql_expression_factory_builds_safe_string_to_bigint_conversion()
    {
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT",
            newNullable: false);

        var plan = HanaSqlExpressionFactory.CreateConversionPlan("\"LEGACY_VALUE\"", operation.OldColumn, operation,
            HanaMigrationPolicy.Default);

        plan.AssignmentExpression.Should().Contain("TO_BIGINT");
        plan.Validations.Should().Contain(validation => validation.Name == "numeric-conversion");
        plan.Validations.Should().Contain(validation => validation.Name == "not-null");
    }

    [Fact]
    public void Sql_expression_factory_builds_string_to_boolean_conversion()
    {
        var operation = CreateAlterOperation(
            "IS_ACTIVE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(20)",
            typeof(bool),
            "BOOLEAN");

        var plan = HanaSqlExpressionFactory.CreateConversionPlan("\"IS_ACTIVE\"", operation.OldColumn, operation,
            HanaMigrationPolicy.Default);

        plan.AssignmentExpression.Should().Contain("THEN TRUE");
        plan.AssignmentExpression.Should().Contain("THEN FALSE");
        plan.Validations.Should().Contain(validation => validation.Name == "boolean-conversion");
    }

    [Fact]
    public void Sql_expression_factory_builds_decimal_conversion_validation()
    {
        var operation = CreateAlterOperation(
            "AMOUNT",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(decimal),
            "DECIMAL(10,2)",
            newNullable: false,
            newPrecision: 10,
            newScale: 2);

        var plan = HanaSqlExpressionFactory.CreateConversionPlan("\"AMOUNT\"", operation.OldColumn, operation,
            HanaMigrationPolicy.Default);

        plan.AssignmentExpression.Should().Contain("TO_DECIMAL");
        plan.Validations.Should().Contain(validation => validation.Name == "decimal-conversion");
        plan.Validations.Should().Contain(validation => validation.Name == "not-null");
    }

    [Fact]
    public void Sql_expression_factory_adds_validation_for_strict_string_shrink()
    {
        var operation = CreateAlterOperation(
            "TITLE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            oldMaxLength: 100,
            newClrType: typeof(string),
            newColumnType: "NVARCHAR(32)",
            newMaxLength: 32);

        var plan = HanaSqlExpressionFactory.CreateConversionPlan("\"TITLE\"", operation.OldColumn, operation,
            HanaMigrationPolicy.Default);

        plan.AssignmentExpression.Should().Be("\"TITLE\"");
        plan.Validations.Should().ContainSingle(validation => validation.Name == "string-overflow");
    }

    [Fact]
    public void Sql_expression_factory_truncates_string_when_policy_allows()
    {
        var policy = HanaMigrationPolicy.Default with { AllowStringShrinkTruncate = true };
        var operation = CreateAlterOperation(
            "TITLE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            oldMaxLength: 100,
            newClrType: typeof(string),
            newColumnType: "NVARCHAR(32)",
            newMaxLength: 32);

        var plan = HanaSqlExpressionFactory.CreateConversionPlan("\"TITLE\"", operation.OldColumn, operation, policy);

        plan.AssignmentExpression.Should().Contain("LEFT(");
        plan.Validations.Should().BeEmpty();
    }

    [Fact]
    public void Generator_emits_simple_alter_preflight_when_switching_to_not_null()
    {
        using var context = CreateTenantContext();
        var generator = context.GetService<IMigrationsSqlGenerator>().Should()
            .BeOfType<HanaHybridMigrationsSqlGenerator>().Subject;
        var operation = CreateAlterOperation(
            "TITLE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            oldNullable: true,
            newClrType: typeof(string),
            newColumnType: "NVARCHAR(50)",
            newNullable: false);

        var commands = generator.Generate([operation], context.Model, MigrationsSqlGenerationOptions.Default);
        var sql = string.Join(Environment.NewLine, commands.Select(command => command.CommandText));

        sql.Should().Contain("DO BEGIN");
        sql.Should().Contain("contains NULL values and cannot be changed to NOT NULL");
        sql.Should().Contain("ALTER TABLE");
        sql.Should().Contain("NOT NULL");
    }

    [Fact]
    public void Generator_emits_recreate_flow_with_preflight_and_index_rebuild()
    {
        using var context = CreateTenantContext();
        var generator = context.GetService<IMigrationsSqlGenerator>().Should()
            .BeOfType<HanaHybridMigrationsSqlGenerator>().Subject;
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT",
            newNullable: false);
        operation.AddAnnotation(
            HanaMigrationAnnotationNames.IndexDefinitions,
            HanaMigrationAnnotationNames.SerializeIndexes(
            [
                new CreateIndexOperation
                {
                    Name = "IX_HANA_SAMPLE_LEGACY_VALUE",
                    Table = "HANA_SAMPLE",
                    Columns = ["LEGACY_VALUE"]
                }
            ]));

        var commands = generator.Generate([operation], context.Model, MigrationsSqlGenerationOptions.Default);
        var sql = string.Join(Environment.NewLine, commands.Select(command => command.CommandText));

        sql.Should().Contain("DO BEGIN");
        sql.Should().Contain("TO_BIGINT");
        sql.Should().Contain("RENAME COLUMN");
        sql.Should().Contain("DROP INDEX");
        sql.Should().Contain("CREATE INDEX");
        sql.Should().Contain("_TMP_");
    }

    [Fact]
    public void Generator_blocks_foreign_key_alter_with_clear_message()
    {
        using var context = CreateTenantContext();
        var generator = context.GetService<IMigrationsSqlGenerator>().Should()
            .BeOfType<HanaHybridMigrationsSqlGenerator>().Subject;
        var operation = CreateAlterOperation(
            "TENANT_ID",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(50)",
            typeof(long),
            "BIGINT");
        operation.AddAnnotation(
            HanaMigrationAnnotationNames.ForeignKeyNames,
            HanaMigrationAnnotationNames.SerializeNames(["FK_HANA_SAMPLE_TENANT"]));

        var act = () => generator.Generate([operation], context.Model, MigrationsSqlGenerationOptions.Default);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*FK_HANA_SAMPLE_TENANT*");
    }

    [Fact]
    public void Generator_skips_duplicate_future_index_drop_and_create_operations()
    {
        using var context = CreateTenantContext();
        var generator = context.GetService<IMigrationsSqlGenerator>().Should()
            .BeOfType<HanaHybridMigrationsSqlGenerator>().Subject;
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT");

        var explicitCreateIndex = new CreateIndexOperation
        {
            Name = "IX_HANA_SAMPLE_LEGACY_VALUE",
            Table = "HANA_SAMPLE",
            Columns = ["LEGACY_VALUE"]
        };

        operation.AddAnnotation(
            HanaMigrationAnnotationNames.IndexDefinitions,
            HanaMigrationAnnotationNames.SerializeIndexes([explicitCreateIndex]));

        MigrationOperation[] operations =
        [
            operation,
            new DropIndexOperation
            {
                Name = "IX_HANA_SAMPLE_LEGACY_VALUE",
                Table = "HANA_SAMPLE"
            },
            explicitCreateIndex
        ];

        var commands = generator.Generate(operations, context.Model, MigrationsSqlGenerationOptions.Default);
        var sql = string.Join(Environment.NewLine, commands.Select(command => command.CommandText));

        CountOccurrences(sql, "DROP INDEX").Should().Be(1);
        CountOccurrences(sql, "CREATE INDEX").Should().Be(1);
    }

    [Fact]
    public void Generator_adds_final_default_alter_after_recreate()
    {
        using var context = CreateTenantContext();
        var generator = context.GetService<IMigrationsSqlGenerator>().Should()
            .BeOfType<HanaHybridMigrationsSqlGenerator>().Subject;
        var operation = CreateAlterOperation(
            "LEGACY_VALUE",
            "HANA_SAMPLE",
            typeof(string),
            "NVARCHAR(100)",
            typeof(long),
            "BIGINT",
            newNullable: false,
            newDefaultValue: 0L);

        var commands = generator.Generate([operation], context.Model, MigrationsSqlGenerationOptions.Default);
        var sql = string.Join(Environment.NewLine, commands.Select(command => command.CommandText));

        sql.Should().Contain("DEFAULT");
        sql.Should().Contain("BIGINT");
        sql.Should().Contain("NOT NULL");
    }

    private static TestDbContext CreateTenantContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
        optionsBuilder.UseHana(
            "Server=localhost:30015;UserID=TEST;Password=TEST;Current Schema=HANA_HYBRID_TEST",
            options => options.MigrationsHistoryTable("__EFMigrationsHistory"));
        optionsBuilder.UseHanaHybridMigrations(HanaMigrationPolicy.Default);
        return new TestDbContext(optionsBuilder.Options);
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SampleEntity>(entity =>
            {
                entity.ToTable("SAMPLE");
                entity.HasKey(sample => sample.Id);
                entity.Property(sample => sample.Id).HasColumnName("ID");
            });
        }
    }

    private sealed class SampleEntity
    {
        public long Id { get; init; }
    }

    private static AlterColumnOperation CreateAlterOperation(
        string name,
        string table,
        Type oldClrType,
        string oldColumnType,
        Type newClrType,
        string newColumnType,
        bool oldNullable = true,
        bool newNullable = true,
        int? oldMaxLength = null,
        int? newMaxLength = null,
        int? oldPrecision = null,
        int? newPrecision = null,
        int? oldScale = null,
        int? newScale = null,
        object? newDefaultValue = null)
    {
        return new AlterColumnOperation
        {
            Name = name,
            Table = table,
            ClrType = newClrType,
            ColumnType = newColumnType,
            IsNullable = newNullable,
            MaxLength = newMaxLength,
            Precision = newPrecision,
            Scale = newScale,
            DefaultValue = newDefaultValue,
            OldColumn = new AddColumnOperation
            {
                Name = name,
                Table = table,
                ClrType = oldClrType,
                ColumnType = oldColumnType,
                IsNullable = oldNullable,
                MaxLength = oldMaxLength,
                Precision = oldPrecision,
                Scale = oldScale
            }
        };
    }

    private static int CountOccurrences(string value, string token)
    {
        var count = 0;
        var index = 0;

        while ((index = value.IndexOf(token, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += token.Length;
        }

        return count;
    }
}
