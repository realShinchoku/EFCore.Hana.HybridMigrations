using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal enum HanaTypeFamily
{
    Unknown,
    String,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Boolean,
    Date,
    DateTime,
    Binary
}

internal sealed record HanaStoreTypeDescriptor(
    HanaTypeFamily Family,
    string StoreTypeName,
    int? MaxLength,
    int? Precision,
    int? Scale)
{
    public bool IsIntegerFamily
        => Family is HanaTypeFamily.TinyInt or HanaTypeFamily.SmallInt or HanaTypeFamily.Integer
            or HanaTypeFamily.BigInt;

    public bool IsNumericFamily
        => IsIntegerFamily || Family == HanaTypeFamily.Decimal;

    public static HanaStoreTypeDescriptor From(ColumnOperation operation)
    {
        var columnType = operation.ColumnType;
        var normalizedType = NormalizeStoreType(columnType);

        if (operation.ClrType == typeof(string))
            return new HanaStoreTypeDescriptor(HanaTypeFamily.String, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(bool) || normalizedType is "boolean")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Boolean, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(byte[]))
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Binary, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(DateTime))
            return new HanaStoreTypeDescriptor(
                IsDateStoreType(normalizedType) ? HanaTypeFamily.Date : HanaTypeFamily.DateTime,
                normalizedType,
                operation.MaxLength,
                operation.Precision,
                operation.Scale);

        if (operation.ClrType == typeof(long) || normalizedType is "bigint")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.BigInt, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(int) || normalizedType is "integer" or "int")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Integer, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(short) || normalizedType is "smallint")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.SmallInt, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(byte) || normalizedType is "tinyint")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.TinyInt, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (operation.ClrType == typeof(decimal) || normalizedType is "decimal" or "smalldecimal")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Decimal, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (IsStringStoreType(normalizedType))
            return new HanaStoreTypeDescriptor(HanaTypeFamily.String, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (normalizedType is "timestamp" or "seconddate" or "longdate")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.DateTime, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (IsDateStoreType(normalizedType))
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Date, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        if (normalizedType is "varbinary" or "binary" or "blob")
            return new HanaStoreTypeDescriptor(HanaTypeFamily.Binary, normalizedType, operation.MaxLength,
                operation.Precision, operation.Scale);

        return new HanaStoreTypeDescriptor(HanaTypeFamily.Unknown, normalizedType, operation.MaxLength,
            operation.Precision, operation.Scale);
    }

    public static bool IsStringStoreType(string normalizedType)
        => normalizedType is "nvarchar" or "varchar" or "nchar" or "char" or "shorttext" or "alphanum" or "text";

    public static bool IsDateStoreType(string normalizedType)
        => normalizedType is "date";

    public static string NormalizeStoreType(string? columnType)
    {
        if (string.IsNullOrWhiteSpace(columnType)) return string.Empty;

        var trimmed = columnType.Trim();
        var separatorIndex = trimmed.IndexOfAny(['(', ' ']);
        return (separatorIndex >= 0 ? trimmed[..separatorIndex] : trimmed).Trim().ToLowerInvariant();
    }
}