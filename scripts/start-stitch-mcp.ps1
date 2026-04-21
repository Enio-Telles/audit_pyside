Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $repoRoot ".env"

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string[]]$Keys
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $lines = Get-Content -LiteralPath $Path
    foreach ($rawLine in $lines) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }

        $separatorIndex = $line.IndexOf("=")
        if ($separatorIndex -le 0) {
            continue
        }

        $key = $line.Substring(0, $separatorIndex).Trim()
        if ($key -notin $Keys) {
            continue
        }

        $value = $line.Substring($separatorIndex + 1).Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if ($value) {
            return $value
        }
    }

    return $null
}

if (-not $env:STITCH_API_KEY) {
    $resolvedApiKey = Get-DotEnvValue -Path $envPath -Keys @("STITCH_API_KEY", "Stitch_API")
    if ($resolvedApiKey) {
        $env:STITCH_API_KEY = $resolvedApiKey
    }
}

if (-not $env:STITCH_API_KEY) {
    Write-Error "STITCH_API_KEY nao foi encontrada no ambiente nem em $envPath"
}

& npx.cmd -y @_davideast/stitch-mcp proxy
