# ============================================================================
# Script d'Arrêt - Pipeline Streaming
# ============================================================================

Write-Host "============================================" -ForegroundColor Red
Write-Host "  ARRÊT DU PIPELINE STREAMING" -ForegroundColor Red
Write-Host "============================================" -ForegroundColor Red
Write-Host ""

$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$dockerPath = Join-Path $scriptPath "docker"

if (Test-Path $dockerPath) {
    Set-Location $dockerPath
} else {
    Set-Location $scriptPath
}

Write-Host "⏳ Arrêt des conteneurs..." -ForegroundColor Yellow
docker-compose down

Write-Host ""
Write-Host "✅ Pipeline arrêté" -ForegroundColor Green
Write-Host ""

# Option pour supprimer les volumes
$removeVolumes = Read-Host "Supprimer les données persistantes (volumes)? (o/N)"
if ($removeVolumes -eq "o" -or $removeVolumes -eq "O") {
    Write-Host "🗑️ Suppression des volumes..." -ForegroundColor Yellow
    docker-compose down -v
    Write-Host "✅ Volumes supprimés" -ForegroundColor Green
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Pour redémarrer: .\start_pipeline.ps1" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
