param(
    [string] $Configuration = "Release"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$solution = Join-Path $root "EFCore.Hana.HybridMigrations.slnx"
$project = Join-Path $root "src\EFCore.Hana.HybridMigrations\EFCore.Hana.HybridMigrations.csproj"
$artifacts = Join-Path $root "artifacts"

New-Item -ItemType Directory -Force -Path $artifacts | Out-Null

function Invoke-CheckedDotNet {
    dotnet @args
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet command failed with exit code ${LASTEXITCODE}: dotnet $args"
    }
}

foreach ($major in 6, 7, 8, 9, 10) {
    Invoke-CheckedDotNet test $solution -c $Configuration -p:HanaEfCoreMajor=$major --nologo
    Invoke-CheckedDotNet pack $project -c $Configuration -p:HanaEfCoreMajor=$major --no-build -o $artifacts --nologo
}
