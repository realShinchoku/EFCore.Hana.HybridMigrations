using System.Globalization;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaHybridMigrationsOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public HanaHybridMigrationsOptionsExtension()
        : this(HanaMigrationPolicy.Default)
    {
    }

    private HanaHybridMigrationsOptionsExtension(HanaMigrationPolicy policy)
    {
        Policy = policy with { };
    }

    public HanaMigrationPolicy Policy { get; }

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
    }

    public void Validate(IDbContextOptions options)
    {
    }

    public static HanaHybridMigrationsOptionsExtension WithPolicy(HanaMigrationPolicy policy)
        => new(policy);

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension)
        : DbContextOptionsExtensionInfo(extension)
    {
        private HanaHybridMigrationsOptionsExtension TypedExtension
            => (HanaHybridMigrationsOptionsExtension)Extension;

        public override bool IsDatabaseProvider => false;

        public override string LogFragment
            => $"HanaHybridMigrations({TypedExtension.Policy}) ";

        public override int GetServiceProviderHashCode()
            => HashCode.Combine(
                TypedExtension.Policy.AllowStringShrinkTruncate,
                TypedExtension.Policy.BlockOnPrimaryKey,
                TypedExtension.Policy.BlockOnForeignKey,
                TypedExtension.Policy.BlockOnUniqueConstraint,
                TypedExtension.Policy.AutoDropRecreateIndexes,
                TypedExtension.Policy.EnablePreflightValidation,
                TypedExtension.Policy.LockTableName,
                TypedExtension.Policy.LockRowId);

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo &&
               otherInfo.TypedExtension.Policy == TypedExtension.Policy;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["hana-hybrid-migrations"] = GetServiceProviderHashCode().ToString(CultureInfo.InvariantCulture);
        }
    }
}
