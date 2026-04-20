# EFCore.Hana.HybridMigrations

Hybrid SAP HANA migrations support for Entity Framework Core.

This package replaces selected EF Core migration services for the SAP HANA EF Core provider. It focuses on safer HANA DDL generation for common migration cases that are awkward or risky with provider defaults:

- direct HANA DDL for create, add/drop column, rename column/table, and indexes
- safer alter-column handling with preflight validation
- recreate-column flow for supported type conversions
- automatic normal index drop/recreate around column swaps
- migration history SQL blocks compatible with HANA SQLScript
- migration database lock support for EF Core 9 and later

This is an unofficial community package. It is not affiliated with, sponsored by, or endorsed by SAP.

## Package Lines

Use the package major version that matches your EF Core and SAP HANA provider major.

| Package major | SAP provider package |
| --- | --- |
| `6.x` | `Sap.EntityFrameworkCore.Hana.v6.0` |
| `7.x` | `Sap.EntityFrameworkCore.Hana.v7.0` |
| `8.x` | `Sap.EntityFrameworkCore.Hana.v8.0` |
| `9.x` | `Sap.EntityFrameworkCore.Hana.v9.0` |
| `10.x` | `Sap.EntityFrameworkCore.Hana.v10.0` |

## Usage

```csharp
using Microsoft.EntityFrameworkCore;
using Sap.EntityFrameworkCore.Hana;
using EFCore.Hana.HybridMigrations;

services.AddDbContext<AppDbContext>(builder =>
{
    builder.UseHana(
        connectionString,
        options => options.MigrationsHistoryTable("__EFMigrationsHistory"));

    builder.UseHanaHybridMigrations();
});
```

Use a policy when you need stricter or looser migration behavior:

```csharp
builder.UseHanaHybridMigrations(
    HanaMigrationPolicy.Default with
    {
        AllowStringShrinkTruncate = false,
        AutoDropRecreateIndexes = true,
        LockTableName = "__EFMigrationsLock"
    });
```

## Build And Test

Build/test one provider major:

```powershell
dotnet test .\tests\EFCore.Hana.HybridMigrations.Tests\EFCore.Hana.HybridMigrations.Tests.csproj -p:HanaEfCoreMajor=10
```

Run the full supported matrix:

```powershell
.\scripts\pack-all.ps1
```

The build matrix uses:

- `HanaEfCoreMajor=6` and `7`: `net6.0`
- `HanaEfCoreMajor=8` and `9`: `net8.0`
- `HanaEfCoreMajor=10`: `net10.0`

The test project sets `RollForward=Major` so older test TFMs can run on newer installed runtimes during local validation.

## GitHub Actions

The repository includes two workflows:

- `.github/workflows/ci.yml` builds and tests the full `6..10` matrix on pull requests.
- `.github/workflows/publish.yml` runs whenever `main` is updated, publishes every package line `6..10`, and creates git tags automatically.

### Automatic versioning and tags

Every successful push to `main` publishes all supported package lines:

- `6.x` line using `HanaEfCoreMajor=6`
- `7.x` line using `HanaEfCoreMajor=7`
- `8.x` line using `HanaEfCoreMajor=8`
- `9.x` line using `HanaEfCoreMajor=9`
- `10.x` line using `HanaEfCoreMajor=10`

For each line, the workflow:

- looks for the latest existing tag matching `v<major>.0.*`
- increments the patch number
- packs the project with that version
- pushes the package to nuget.org
- creates and pushes the matching git tag on the same `main` commit

Examples of tags the workflow creates automatically:

- `v6.0.3`
- `v7.0.3`
- `v8.0.3`
- `v9.0.3`
- `v10.0.3`

### Trusted publishing setup

Before the publish workflow can push to nuget.org, create a trusted publishing policy on nuget.org with:

- **Repository Owner**: your GitHub owner/org
- **Repository**: your GitHub repo name
- **Workflow File**: `publish.yml`
- **Environment**: `release`

Then create the GitHub environment `release` and add secret:

- `NUGET_USER` = your nuget.org username

The workflow uses `NuGet/login@v1` and `id-token: write`, so no long-lived NuGet API key is required.

## License

This project's source code is MIT licensed.

SAP packages such as `Sap.EntityFrameworkCore.Hana.v10.0` and the bundled SAP HANA .NET client assemblies are governed by SAP's own license terms. This repository does not vendor or relicense SAP binaries.
