Set-Location $PSScriptRoot
if ((Test-Path ".\pnpm-lock.yaml") -and (Get-Command pnpm -ErrorAction SilentlyContinue)) {
    pnpm dev
}
else {
    npm run dev
}
