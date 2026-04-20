#pragma warning disable EF1001
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaHybridHistoryRepository : HistoryRepository
{
    private readonly ISqlGenerationHelper _sql;
    private readonly IRelationalTypeMappingSource _types;

    public HanaHybridHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
        _sql = dependencies.SqlGenerationHelper;
        _types = dependencies.TypeMappingSource;
    }

    protected override string ExistsSql
    {
        get
        {
            var tableNameLiteral = StringLiteral(TableName.ToUpperInvariant());

            if (TableSchema is null)
                return $"""
                        SELECT COUNT(*)
                        FROM SYS.TABLES
                        WHERE TABLE_NAME = {tableNameLiteral}
                          AND SCHEMA_NAME = CURRENT_SCHEMA
                        """;

            return $"""
                    SELECT COUNT(*)
                    FROM SYS.TABLES
                    WHERE TABLE_NAME = {tableNameLiteral}
                      AND SCHEMA_NAME = {StringLiteral(TableSchema.ToUpperInvariant())}
                    """;
        }
    }

#if HANA_EFCORE_9 || HANA_EFCORE_10
    public override LockReleaseBehavior LockReleaseBehavior
        => LockReleaseBehavior.Connection;
#endif

    protected override bool InterpretExistsResult(object? value)
        => Convert.ToInt64(value) > 0L;

    public override string GetCreateIfNotExistsScript()
    {
        var ddl = EscapeDynamicSql(GetCreateScript());
        var tableNameLiteral = StringLiteral(TableName.ToUpperInvariant());

        if (TableSchema is null)
            return $"""
                    DO
                    BEGIN
                        DECLARE table_count INTEGER;

                        SELECT COUNT(*)
                          INTO table_count
                          FROM SYS.TABLES
                         WHERE TABLE_NAME = {tableNameLiteral}
                           AND SCHEMA_NAME = CURRENT_SCHEMA;

                        IF :table_count = 0 THEN
                            EXECUTE IMMEDIATE '{ddl}';
                        END IF;
                    END;
                    """;

        return $"""
                DO
                BEGIN
                    DECLARE table_count INTEGER;

                    SELECT COUNT(*)
                      INTO table_count
                      FROM SYS.TABLES
                     WHERE TABLE_NAME = {tableNameLiteral}
                       AND SCHEMA_NAME = {StringLiteral(TableSchema.ToUpperInvariant())};

                    IF :table_count = 0 THEN
                        EXECUTE IMMEDIATE '{ddl}';
                    END IF;
                END;
                """;
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
    {
        var table = _sql.DelimitIdentifier(TableName, TableSchema);
        var column = _sql.DelimitIdentifier(MigrationIdColumnName);

        return $"""
                DO
                BEGIN
                    DECLARE migration_count INTEGER;

                    SELECT COUNT(*)
                      INTO migration_count
                      FROM {table}
                     WHERE {column} = {StringLiteral(migrationId)};

                    IF :migration_count = 0 THEN
                """;
    }

    public override string GetBeginIfExistsScript(string migrationId)
    {
        var table = _sql.DelimitIdentifier(TableName, TableSchema);
        var column = _sql.DelimitIdentifier(MigrationIdColumnName);

        return $"""
                DO
                BEGIN
                    DECLARE migration_count INTEGER;

                    SELECT COUNT(*)
                      INTO migration_count
                      FROM {table}
                     WHERE {column} = {StringLiteral(migrationId)};

                    IF :migration_count > 0 THEN
                """;
    }

    public override string GetEndIfScript()
        => """
              END IF;
           END;
           """;

#if HANA_EFCORE_9 || HANA_EFCORE_10
    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => HanaHybridMigrationsDatabaseLock.Acquire(Dependencies, this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default)
        => HanaHybridMigrationsDatabaseLock.AcquireAsync(Dependencies, this, cancellationToken);
#endif

    private string StringLiteral(string value)
        => _types.FindMapping(typeof(string))!.GenerateSqlLiteral(value);

    private static string EscapeDynamicSql(string sql)
        => sql.Trim().TrimEnd(';').Replace("'", "''", StringComparison.Ordinal);
}
#pragma warning restore EF1001
