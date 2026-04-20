# EFCore.Hana.HybridMigrations

[![NuGet](https://img.shields.io/nuget/v/EFCore.Hana.HybridMigrations.svg)](https://www.nuget.org/packages/EFCore.Hana.HybridMigrations)
[![CI](https://github.com/realShinchoku/EFCore.Hana.HybridMigrations/actions/workflows/ci.yml/badge.svg)](https://github.com/realShinchoku/EFCore.Hana.HybridMigrations/actions/workflows/ci.yml)
[![Publish](https://github.com/realShinchoku/EFCore.Hana.HybridMigrations/actions/workflows/publish.yml/badge.svg)](https://github.com/realShinchoku/EFCore.Hana.HybridMigrations/actions/workflows/publish.yml)
[![License](https://img.shields.io/github/license/realShinchoku/EFCore.Hana.HybridMigrations.svg)](LICENSE)

`EFCore.Hana.HybridMigrations` extends Entity Framework Core migrations for SAP HANA with safer alter-column handling, HANA-friendly migration history SQL, and version-aligned support for the SAP EF Core provider.

This package is intended for applications that already use `Sap.EntityFrameworkCore.Hana.v*.0` and want more predictable migration behavior for schema changes that are difficult to express safely with default provider SQL generation.

This is an unofficial community package. It is not affiliated with, sponsored by, or endorsed by SAP.

## Features

- Safer alter-column SQL generation for SAP HANA
- Preflight validation for schema changes that may be destructive
- Recreate-column flow for supported conversions
- Automatic drop/recreate of regular indexes around column replacement
- HANA-compatible migration history SQL blocks
- Migration database lock support for EF Core 9 and later

## Compatibility

Use the package major version that matches your SAP HANA provider major.
Published package versions follow the pinned `Microsoft.EntityFrameworkCore.Relational` patch version for each supported EF Core line.

| Package version | EF Core | SAP provider |
| --- | --- | --- |
| `6.x` | `6.x` | `Sap.EntityFrameworkCore.Hana.v6.0` |
| `7.x` | `7.x` | `Sap.EntityFrameworkCore.Hana.v7.0` |
| `8.x` | `8.x` | `Sap.EntityFrameworkCore.Hana.v8.0` |
| `9.x` | `9.x` | `Sap.EntityFrameworkCore.Hana.v9.0` |
| `10.x` | `10.x` | `Sap.EntityFrameworkCore.Hana.v10.0` |

## Installation

Install the package line that matches your EF Core and SAP HANA provider version.
The package major version must match the major version of EF Core and the SAP HANA provider.

```powershell
dotnet add package EFCore.Hana.HybridMigrations --version 10.*
```

## Usage

Register the SAP HANA provider as usual, then enable the hybrid migration services:

```csharp
using EFCore.Hana.HybridMigrations;
using Microsoft.EntityFrameworkCore;
using Sap.EntityFrameworkCore.Hana;

services.AddDbContext<AppDbContext>(options =>
{
    options.UseHana(
        connectionString,
        hana => hana.MigrationsHistoryTable("__EFMigrationsHistory"));

    options.UseHanaHybridMigrations();
});
```

You can also provide a policy to control migration behavior:

```csharp
options.UseHanaHybridMigrations(
    HanaMigrationPolicy.Default with
    {
        AllowStringShrinkTruncate = false,
        AutoDropRecreateIndexes = true,
        LockTableName = "__EFMigrationsLock",
        LockRowId = 1
    });
```

## Migration Policy

`HanaMigrationPolicy` exposes the main safety switches used by the package:

- `AllowStringShrinkTruncate`
- `BlockOnPrimaryKey`
- `BlockOnForeignKey`
- `BlockOnUniqueConstraint`
- `AutoDropRecreateIndexes`
- `EnablePreflightValidation`
- `LockTableName`
- `LockRowId`

## What This Package Changes

The package replaces selected EF Core migration services used by the SAP HANA provider in order to improve migration generation for common schema changes, especially around `AlterColumn`.

Typical scenarios covered:

- string length changes
- supported type conversion flows via temporary column replacement
- index handling during column recreation
- migration history SQL generation compatible with HANA SQLScript blocks

It does not replace your application model, migrations, or SAP provider configuration. It only changes the migration pipeline behavior used at runtime and design time.

## Development

Run tests for a specific supported provider major:

```powershell
dotnet test .\tests\EFCore.Hana.HybridMigrations.Tests\EFCore.Hana.HybridMigrations.Tests.csproj -p:HanaEfCoreMajor=10
```

Run the full supported matrix:

```powershell
.\scripts\pack-all.ps1
```

Build targets used by the repository:

- `HanaEfCoreMajor=6` and `7` -> `net6.0`
- `HanaEfCoreMajor=8` and `9` -> `net8.0`
- `HanaEfCoreMajor=10` -> `net10.0`

## License

This repository is licensed under the [MIT License](LICENSE).

SAP provider packages and SAP HANA client assemblies remain subject to SAP's own license terms. This repository does not vendor, redistribute, or relicense SAP binaries.
