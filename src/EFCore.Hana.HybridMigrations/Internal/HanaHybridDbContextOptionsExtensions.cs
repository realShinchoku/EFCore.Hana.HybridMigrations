using EFCore.Hana.HybridMigrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EFCore.Hana.HybridMigrations;

/// <summary>
/// Registers SAP HANA hybrid migrations services for an EF Core context.
/// </summary>
public static class HanaHybridDbContextOptionsExtensions
{
    /// <summary>
    /// Replaces EF Core migration services with SAP HANA hybrid implementations.
    /// </summary>
    /// <param name="builder">The context options builder.</param>
    /// <param name="policy">Optional migration safety policy.</param>
    /// <returns>The same options builder.</returns>
    public static DbContextOptionsBuilder UseHanaHybridMigrations(
        this DbContextOptionsBuilder builder,
        HanaMigrationPolicy? policy = null)
    {
        var resolvedPolicy = policy ?? HanaMigrationPolicy.Default;
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(
            HanaHybridMigrationsOptionsExtension.WithPolicy(resolvedPolicy));
        builder.ReplaceService<IHistoryRepository, HanaHybridHistoryRepository>();
        builder.ReplaceService<HistoryRepository, HanaHybridHistoryRepository>();
        builder.ReplaceService<IMigrationsSqlGenerator, HanaHybridMigrationsSqlGenerator>();
        builder.ReplaceService<IMigrationsModelDiffer, HanaHybridMigrationsModelDiffer>();
        return builder;
    }

    /// <summary>
    /// Replaces EF Core migration services with SAP HANA hybrid implementations.
    /// </summary>
    /// <typeparam name="TContext">The EF Core context type.</typeparam>
    /// <param name="builder">The context options builder.</param>
    /// <param name="policy">Optional migration safety policy.</param>
    /// <returns>The same options builder.</returns>
    public static DbContextOptionsBuilder<TContext> UseHanaHybridMigrations<TContext>(
        this DbContextOptionsBuilder<TContext> builder,
        HanaMigrationPolicy? policy = null)
        where TContext : DbContext
    {
        ((DbContextOptionsBuilder)builder).UseHanaHybridMigrations(policy);
        return builder;
    }
}
