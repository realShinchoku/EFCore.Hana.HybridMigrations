using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Hana.HybridMigrations;

/// <summary>
/// Registers SAP HANA hybrid migrations services for EF Core design-time tooling.
/// </summary>
public sealed class HanaHybridDesignTimeServices : IDesignTimeServices
{
    /// <inheritdoc />
    public void ConfigureDesignTimeServices(IServiceCollection services)
    {
        services.AddScoped<IHistoryRepository, HanaHybridHistoryRepository>();
        services.AddScoped<IMigrationsSqlGenerator, HanaHybridMigrationsSqlGenerator>();
        services.AddScoped<IMigrationsModelDiffer, HanaHybridMigrationsModelDiffer>();
    }
}
