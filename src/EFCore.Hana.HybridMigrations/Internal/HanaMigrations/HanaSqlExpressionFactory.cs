using System.Globalization;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Hana.HybridMigrations;

internal sealed class HanaSqlExpressionFactory
{
    public static IReadOnlyList<HanaPreflightValidation> CreateSimpleAlterValidations(
        string sourceColumnExpression,
        AlterColumnOperation operation)
    {
        if (operation.OldColumn is null || !operation.OldColumn.IsNullable || operation.IsNullable) return [];

        return
        [
            new HanaPreflightValidation(
                "not-null",
                $"{sourceColumnExpression} IS NULL",
                $"Column '{operation.Table}.{operation.Name}' contains NULL values and cannot be changed to NOT NULL.")
        ];
    }

    public static HanaConversionPlan CreateConversionPlan(
        string sourceColumnExpression,
        ColumnOperation source,
        AlterColumnOperation target,
        HanaMigrationPolicy policy)
    {
        var sourceType = HanaStoreTypeDescriptor.From(source);
        var targetType = HanaStoreTypeDescriptor.From(target);

        if (sourceType.Family == HanaTypeFamily.String)
            return BuildFromString(sourceColumnExpression, source, target, targetType, policy);

        if (targetType.Family == HanaTypeFamily.String) return BuildToString(sourceColumnExpression, target, policy);

        if (sourceType.IsNumericFamily && targetType.IsNumericFamily)
            return BuildNumericConversion(sourceColumnExpression, target, sourceType, targetType);

        if (sourceType.Family == HanaTypeFamily.Date && targetType.Family == HanaTypeFamily.DateTime)
            return FinalizePlan(
                $"CAST({sourceColumnExpression} AS {target.ColumnType})",
                [],
                sourceColumnExpression,
                target);

        if (sourceType.Family == targetType.Family)
            return FinalizePlan(sourceColumnExpression, [], sourceColumnExpression, target);

        throw new InvalidOperationException(
            $"Unsupported SAP HANA conversion from '{source.ColumnType ?? source.ClrType.Name}' to '{target.ColumnType ?? target.ClrType.Name}'.");
    }

    private static HanaConversionPlan BuildFromString(
        string sourceColumnExpression,
        ColumnOperation source,
        AlterColumnOperation target,
        HanaStoreTypeDescriptor targetType,
        HanaMigrationPolicy policy)
    {
        if (targetType.Family == HanaTypeFamily.String)
            return BuildStringToString(sourceColumnExpression, source, target, policy);

        var trimmed = $"TRIM({sourceColumnExpression})";
        var validations = new List<HanaPreflightValidation>();

        switch (targetType.Family)
        {
            case HanaTypeFamily.Integer:
            case HanaTypeFamily.SmallInt:
            case HanaTypeFamily.TinyInt:
            case HanaTypeFamily.BigInt:
            {
                const string integerRegex = "^[+-]?[0-9]+$";
                var matchesIntegerPattern = $"{trimmed} LIKE_REGEXPR {Quote(integerRegex)}";
                var numericValue = $"TO_DECIMAL({trimmed}, 38, 0)";
                var rangePredicate = BuildNumericRangePredicate(numericValue, targetType);
                var patternFailure = $"CASE WHEN {matchesIntegerPattern} THEN 0 ELSE 1 END = 1";
                var rangeFailure =
                    $"CASE WHEN {matchesIntegerPattern} THEN CASE WHEN {rangePredicate} THEN 1 ELSE 0 END ELSE 0 END = 1";
                var isConvertible =
                    $"CASE WHEN {matchesIntegerPattern} THEN CASE WHEN {rangePredicate} THEN 0 ELSE 1 END ELSE 0 END = 1";
                validations.Add(
                    new HanaPreflightValidation(
                        "numeric-conversion",
                        $"{sourceColumnExpression} IS NOT NULL AND {trimmed} <> '' AND ({patternFailure} OR {rangeFailure})",
                        $"Column '{target.Table}.{target.Name}' contains values that cannot be converted to {target.ColumnType}."));

                var assignment =
                    $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {isConvertible} THEN TO_{targetType.StoreTypeName.ToUpperInvariant()}({trimmed}) ELSE NULL END";
                return FinalizePlan(assignment, validations, sourceColumnExpression, target);
            }

            case HanaTypeFamily.Decimal:
            {
                var decimalRegex = BuildDecimalRegex(targetType.Precision, targetType.Scale);
                var matchesDecimalPattern = $"{trimmed} LIKE_REGEXPR {Quote(decimalRegex)}";
                validations.Add(
                    new HanaPreflightValidation(
                        "decimal-conversion",
                        $"{sourceColumnExpression} IS NOT NULL AND {trimmed} <> '' AND CASE WHEN {matchesDecimalPattern} THEN 0 ELSE 1 END = 1",
                        $"Column '{target.Table}.{target.Name}' contains values that cannot be converted to {target.ColumnType}."));

                var assignment = targetType.Precision is int precision
                    ? $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {matchesDecimalPattern} THEN TO_DECIMAL({trimmed}, {precision}, {targetType.Scale ?? 0}) ELSE NULL END"
                    : $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {matchesDecimalPattern} THEN TO_DECIMAL({trimmed}) ELSE NULL END";
                return FinalizePlan(assignment, validations, sourceColumnExpression, target);
            }

            case HanaTypeFamily.Boolean:
            {
                var normalized = $"UPPER({trimmed})";
                validations.Add(
                    new HanaPreflightValidation(
                        "boolean-conversion",
                        $"{sourceColumnExpression} IS NOT NULL AND {trimmed} <> '' AND {normalized} NOT IN ('TRUE', 'T', 'YES', 'Y', '1', 'FALSE', 'F', 'NO', 'N', '0')",
                        $"Column '{target.Table}.{target.Name}' contains values that cannot be converted to BOOLEAN."));

                var assignment =
                    $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {normalized} IN ('TRUE', 'T', 'YES', 'Y', '1') THEN TRUE WHEN {normalized} IN ('FALSE', 'F', 'NO', 'N', '0') THEN FALSE ELSE NULL END";
                return FinalizePlan(assignment, validations, sourceColumnExpression, target);
            }

            case HanaTypeFamily.Date:
            {
                const string dateRegex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";
                var matchesDatePattern = $"{trimmed} LIKE_REGEXPR {Quote(dateRegex)}";
                validations.Add(
                    new HanaPreflightValidation(
                        "date-conversion",
                        $"{sourceColumnExpression} IS NOT NULL AND {trimmed} <> '' AND CASE WHEN {matchesDatePattern} THEN 0 ELSE 1 END = 1",
                        $"Column '{target.Table}.{target.Name}' contains values that cannot be converted to DATE."));

                var assignment =
                    $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {matchesDatePattern} THEN TO_DATE({trimmed}, 'YYYY-MM-DD') ELSE NULL END";
                return FinalizePlan(assignment, validations, sourceColumnExpression, target);
            }

            case HanaTypeFamily.DateTime:
            {
                const string dateOnlyRegex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";
                const string timestampRegex =
                    "^[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,7})?)?$";
                var matchesDateOnlyPattern = $"{trimmed} LIKE_REGEXPR {Quote(dateOnlyRegex)}";
                var matchesTimestampPattern = $"{trimmed} LIKE_REGEXPR {Quote(timestampRegex)}";
                validations.Add(
                    new HanaPreflightValidation(
                        "datetime-conversion",
                        $"{sourceColumnExpression} IS NOT NULL AND {trimmed} <> '' AND CASE WHEN {matchesTimestampPattern} THEN 0 ELSE 1 END = 1",
                        $"Column '{target.Table}.{target.Name}' contains values that cannot be converted to {target.ColumnType}."));

                var assignment =
                    $"CASE WHEN {sourceColumnExpression} IS NULL OR {trimmed} = '' THEN NULL WHEN {matchesDateOnlyPattern} THEN TO_TIMESTAMP({trimmed} || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS') WHEN {matchesTimestampPattern} THEN TO_TIMESTAMP(REPLACE({trimmed}, 'T', ' '), 'YYYY-MM-DD HH24:MI:SS.FF7') ELSE NULL END";
                return FinalizePlan(assignment, validations, sourceColumnExpression, target);
            }

            default:
                throw new InvalidOperationException(
                    $"Unsupported string conversion target '{target.ColumnType ?? target.ClrType.Name}'.");
        }
    }

    private static HanaConversionPlan BuildStringToString(
        string sourceColumnExpression,
        ColumnOperation source,
        AlterColumnOperation target,
        HanaMigrationPolicy policy)
    {
        if (source.MaxLength is int oldLength &&
            target.MaxLength is int newLength &&
            newLength < oldLength)
        {
            if (!policy.AllowStringShrinkTruncate)
                return FinalizePlan(
                    sourceColumnExpression,
                    [
                        new HanaPreflightValidation(
                            "string-overflow",
                            $"{sourceColumnExpression} IS NOT NULL AND LENGTH({sourceColumnExpression}) > {newLength}",
                            $"Column '{target.Table}.{target.Name}' contains values longer than {newLength} and AllowStringShrinkTruncate is disabled.")
                    ],
                    sourceColumnExpression,
                    target);

            return FinalizePlan(
                $"CASE WHEN {sourceColumnExpression} IS NULL THEN NULL ELSE LEFT({sourceColumnExpression}, {newLength}) END",
                [],
                sourceColumnExpression,
                target);
        }

        return FinalizePlan(sourceColumnExpression, [], sourceColumnExpression, target);
    }

    private static HanaConversionPlan BuildToString(
        string sourceColumnExpression,
        AlterColumnOperation target,
        HanaMigrationPolicy policy)
    {
        var expression =
            $"CASE WHEN {sourceColumnExpression} IS NULL THEN NULL ELSE TO_NVARCHAR({sourceColumnExpression}) END";
        var validations = new List<HanaPreflightValidation>();

        if (target.MaxLength is int maxLength)
        {
            if (policy.AllowStringShrinkTruncate)
                expression =
                    $"CASE WHEN {sourceColumnExpression} IS NULL THEN NULL ELSE LEFT(TO_NVARCHAR({sourceColumnExpression}), {maxLength}) END";
            else
                validations.Add(
                    new HanaPreflightValidation(
                        "string-overflow",
                        $"{sourceColumnExpression} IS NOT NULL AND LENGTH(TO_NVARCHAR({sourceColumnExpression})) > {maxLength}",
                        $"Column '{target.Table}.{target.Name}' contains values longer than {maxLength}."));
        }

        return FinalizePlan(expression, validations, sourceColumnExpression, target);
    }

    private static HanaConversionPlan BuildNumericConversion(
        string sourceColumnExpression,
        AlterColumnOperation target,
        HanaStoreTypeDescriptor sourceType,
        HanaStoreTypeDescriptor targetType)
    {
        var validations = new List<HanaPreflightValidation>();

        if (targetType.IsIntegerFamily)
        {
            var rangePredicate = BuildNumericRangePredicate(sourceColumnExpression, targetType);
            var invalidPredicate = sourceType.Family == HanaTypeFamily.Decimal
                ? $"{sourceColumnExpression} IS NOT NULL AND ({rangePredicate} OR ROUND({sourceColumnExpression}, 0) <> {sourceColumnExpression})"
                : $"{sourceColumnExpression} IS NOT NULL AND ({rangePredicate})";

            validations.Add(
                new HanaPreflightValidation(
                    "numeric-range",
                    invalidPredicate,
                    $"Column '{target.Table}.{target.Name}' contains numeric values outside the target {target.ColumnType} range."));
        }
        else if (targetType is { Family: HanaTypeFamily.Decimal, Precision: int precision })
        {
            var integerDigits = precision - (targetType.Scale ?? 0);
            var rangePredicate =
                $"ABS({sourceColumnExpression}) >= POWER(10, {integerDigits.ToString(CultureInfo.InvariantCulture)})";
            var scalePredicate = targetType.Scale is int scale
                ? $"ROUND({sourceColumnExpression}, {scale.ToString(CultureInfo.InvariantCulture)}) <> {sourceColumnExpression}"
                : "FALSE";
            validations.Add(
                new HanaPreflightValidation(
                    "decimal-range",
                    $"{sourceColumnExpression} IS NOT NULL AND ({rangePredicate} OR {scalePredicate})",
                    $"Column '{target.Table}.{target.Name}' contains numeric values outside the target {target.ColumnType} precision or scale."));
        }

        return FinalizePlan(
            $"CAST({sourceColumnExpression} AS {target.ColumnType})",
            validations,
            sourceColumnExpression,
            target);
    }

    private static HanaConversionPlan FinalizePlan(
        string assignmentExpression,
        IReadOnlyList<HanaPreflightValidation> validations,
        string sourceColumnExpression,
        AlterColumnOperation target)
    {
        if (target.IsNullable) return new HanaConversionPlan(assignmentExpression, validations);

        var allValidations = validations.ToList();
        allValidations.Add(
            new HanaPreflightValidation(
                "not-null",
                $"{sourceColumnExpression} IS NULL OR ({assignmentExpression}) IS NULL",
                $"Column '{target.Table}.{target.Name}' would violate NOT NULL after conversion."));
        return new HanaConversionPlan(assignmentExpression, allValidations);
    }

    private static string BuildNumericRangePredicate(string valueExpression, HanaStoreTypeDescriptor targetType)
        => targetType.Family switch
        {
            HanaTypeFamily.TinyInt => $"{valueExpression} < 0 OR {valueExpression} > 255",
            HanaTypeFamily.SmallInt => $"{valueExpression} < -32768 OR {valueExpression} > 32767",
            HanaTypeFamily.Integer => $"{valueExpression} < -2147483648 OR {valueExpression} > 2147483647",
            HanaTypeFamily.BigInt =>
                $"{valueExpression} < -9223372036854775808 OR {valueExpression} > 9223372036854775807",
            _ => "FALSE"
        };

    private static string BuildDecimalRegex(int? precision, int? scale)
    {
        if (precision is not int resolvedPrecision) return @"^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$";

        var resolvedScale = scale ?? 0;
        var integerDigits = Math.Max(resolvedPrecision - resolvedScale, 1);
        return resolvedScale == 0
            ? $@"^[+-]?[0-9]{{1,{integerDigits}}}$"
            : $@"^[+-]?(?:(?:[0-9]{{1,{integerDigits}}}(?:\.[0-9]{{1,{resolvedScale}}})?)|(?:\.[0-9]{{1,{resolvedScale}}}))$";
    }

    private static string Quote(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
