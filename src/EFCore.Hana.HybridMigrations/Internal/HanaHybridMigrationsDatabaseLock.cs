#pragma warning disable EF1001
#if HANA_EFCORE_9 || HANA_EFCORE_10
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Sap.Data.Hana;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaHybridMigrationsDatabaseLock : IMigrationsDatabaseLock
{
    private readonly HanaConnection _connection;
    private readonly HanaTransaction _transaction;
    private bool _disposed;

    private HanaHybridMigrationsDatabaseLock(
        IHistoryRepository historyRepository,
        HanaConnection connection,
        HanaTransaction transaction)
    {
        HistoryRepository = historyRepository;
        _connection = connection;
        _transaction = transaction;
    }

    public IHistoryRepository HistoryRepository { get; }

    public IMigrationsDatabaseLock ReacquireIfNeeded(bool connectionReopened, bool? transactionRestarted)
        => this;

    public Task<IMigrationsDatabaseLock> ReacquireIfNeededAsync(
        bool connectionReopened,
        bool? transactionRestarted,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(this);

    public void Dispose()
    {
        if (_disposed) return;

        _disposed = true;
        try
        {
            _transaction.Dispose();
        }
        finally
        {
            _connection.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;

        _disposed = true;
        return new ValueTask(DisposeAsyncCore());
    }

    public static IMigrationsDatabaseLock Acquire(
        HistoryRepositoryDependencies dependencies,
        IHistoryRepository historyRepository)
    {
        var policy = ResolvePolicy(dependencies);
        var connection = new HanaConnection(dependencies.Connection.ConnectionString);

        try
        {
            connection.Open();
            EnsureLockInfrastructure(connection, policy);

            var transaction = connection.BeginTransaction();
            using var command = CreateAcquireCommand(connection, transaction, policy);
            command.ExecuteScalar();

            return new HanaHybridMigrationsDatabaseLock(historyRepository, connection, transaction);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    public static async Task<IMigrationsDatabaseLock> AcquireAsync(
        HistoryRepositoryDependencies dependencies,
        IHistoryRepository historyRepository,
        CancellationToken cancellationToken)
    {
        var policy = ResolvePolicy(dependencies);
        var connection = new HanaConnection(dependencies.Connection.ConnectionString);

        try
        {
            await connection.OpenAsync(cancellationToken);
            await EnsureLockInfrastructureAsync(connection, policy, cancellationToken);

            var transaction = connection.BeginTransaction();
            await using var command = CreateAcquireCommand(connection, transaction, policy);
            await command.ExecuteScalarAsync(cancellationToken);

            return new HanaHybridMigrationsDatabaseLock(historyRepository, connection, transaction);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private async Task DisposeAsyncCore()
    {
        try
        {
            await _transaction.DisposeAsync();
        }
        finally
        {
            await _connection.DisposeAsync();
        }
    }

    private static HanaCommand CreateAcquireCommand(
        HanaConnection connection,
        HanaTransaction transaction,
        HanaMigrationPolicy policy)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
             SELECT "ID"
             FROM {DelimitIdentifier(policy.LockTableName)}
             WHERE "ID" = {policy.LockRowId}
             FOR UPDATE NOWAIT
             """;
        return command;
    }

    private static void EnsureLockInfrastructure(HanaConnection connection, HanaMigrationPolicy policy)
    {
        var tableNameLiteral = StringLiteral(policy.LockTableName);
        var tableNameIdentifier = DelimitIdentifier(policy.LockTableName);

        using var createCommand = connection.CreateCommand();
        createCommand.CommandText =
            $"""
             DO
             BEGIN
                 DECLARE table_count INTEGER;

                 SELECT COUNT(*)
                   INTO table_count
                   FROM SYS.TABLES
                  WHERE TABLE_NAME = {tableNameLiteral}
                    AND SCHEMA_NAME = CURRENT_SCHEMA;

                 IF :table_count = 0 THEN
                     EXECUTE IMMEDIATE 'CREATE ROW TABLE {EscapeDynamicSql(tableNameIdentifier)} ("ID" INTEGER PRIMARY KEY)';
                 END IF;
             END;
             """;
        createCommand.ExecuteNonQuery();

        using var seedCommand = connection.CreateCommand();
        seedCommand.CommandText =
            $"""
             MERGE INTO {tableNameIdentifier} AS T
             USING (SELECT {policy.LockRowId} AS "ID" FROM DUMMY) AS S
             ON (T."ID" = S."ID")
             WHEN NOT MATCHED THEN
                 INSERT ("ID") VALUES (S."ID")
             """;
        seedCommand.ExecuteNonQuery();
    }

    private static async Task EnsureLockInfrastructureAsync(
        HanaConnection connection,
        HanaMigrationPolicy policy,
        CancellationToken cancellationToken)
    {
        var tableNameLiteral = StringLiteral(policy.LockTableName);
        var tableNameIdentifier = DelimitIdentifier(policy.LockTableName);

        await using var createCommand = connection.CreateCommand();
        createCommand.CommandText =
            $"""
             DO
             BEGIN
                 DECLARE table_count INTEGER;

                 SELECT COUNT(*)
                   INTO table_count
                   FROM SYS.TABLES
                  WHERE TABLE_NAME = {tableNameLiteral}
                    AND SCHEMA_NAME = CURRENT_SCHEMA;

                 IF :table_count = 0 THEN
                     EXECUTE IMMEDIATE 'CREATE ROW TABLE {EscapeDynamicSql(tableNameIdentifier)} ("ID" INTEGER PRIMARY KEY)';
                 END IF;
             END;
             """;
        await createCommand.ExecuteNonQueryAsync(cancellationToken);

        await using var seedCommand = connection.CreateCommand();
        seedCommand.CommandText =
            $"""
             MERGE INTO {tableNameIdentifier} AS T
             USING (SELECT {policy.LockRowId} AS "ID" FROM DUMMY) AS S
             ON (T."ID" = S."ID")
             WHEN NOT MATCHED THEN
                 INSERT ("ID") VALUES (S."ID")
             """;
        await seedCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    private static HanaMigrationPolicy ResolvePolicy(HistoryRepositoryDependencies dependencies)
    {
        var options = dependencies.CurrentContext.Context.GetService<IDbContextOptions>();
        var policy = options.FindExtension<HanaHybridMigrationsOptionsExtension>()?.Policy
                     ?? HanaMigrationPolicy.Default;

        if (string.IsNullOrWhiteSpace(policy.LockTableName))
            throw new InvalidOperationException("HanaMigrationPolicy.LockTableName must not be empty.");

        if (policy.LockRowId <= 0)
            throw new InvalidOperationException("HanaMigrationPolicy.LockRowId must be greater than zero.");

        return policy;
    }

    private static string DelimitIdentifier(string identifier)
        => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    private static string StringLiteral(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    private static string EscapeDynamicSql(string sql)
        => sql.Replace("'", "''", StringComparison.Ordinal);
}
#endif
#pragma warning restore EF1001
