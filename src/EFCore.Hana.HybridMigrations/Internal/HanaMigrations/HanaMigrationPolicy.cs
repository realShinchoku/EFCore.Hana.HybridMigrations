namespace EFCore.Hana.HybridMigrations;

/// <summary>
/// Controls the safety policy used by the SAP HANA hybrid migrations services.
/// </summary>
public sealed record HanaMigrationPolicy
{
    /// <summary>
    /// Default policy: validate data before unsafe conversions, block constrained
    /// columns, and auto-drop/recreate non-constraint indexes when needed.
    /// </summary>
    public static HanaMigrationPolicy Default { get; } = new();

    /// <summary>
    /// Allows string shrink operations to truncate values instead of failing
    /// preflight validation.
    /// </summary>
    public bool AllowStringShrinkTruncate { get; init; }

    /// <summary>
    /// Blocks automated alter-column operations on primary-key columns.
    /// </summary>
    public bool BlockOnPrimaryKey { get; init; } = true;

    /// <summary>
    /// Blocks automated alter-column operations on columns participating in foreign keys.
    /// </summary>
    public bool BlockOnForeignKey { get; init; } = true;

    /// <summary>
    /// Blocks automated alter-column operations on columns participating in unique constraints.
    /// </summary>
    public bool BlockOnUniqueConstraint { get; init; } = true;

    /// <summary>
    /// Automatically drops and recreates normal indexes around recreate-column migrations.
    /// </summary>
    public bool AutoDropRecreateIndexes { get; init; } = true;

    /// <summary>
    /// Emits validation blocks before data conversions or nullable-to-required changes.
    /// </summary>
    public bool EnablePreflightValidation { get; init; } = true;

    /// <summary>
    /// HANA table used to serialize concurrent migrations.
    /// </summary>
    public string LockTableName { get; init; } = "__EFMigrationsLock";

    /// <summary>
    /// Lock row id used inside <see cref="LockTableName" />.
    /// </summary>
    public int LockRowId { get; init; } = 1;
}
