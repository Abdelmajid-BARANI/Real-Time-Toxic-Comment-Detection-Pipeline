# ============================================================================
# Script de Démarrage - Pipeline Streaming Commentaires Toxiques
# ============================================================================
# Usage: .\start_pipeline.ps1
# ============================================================================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  PIPELINE STREAMING - COMMENTAIRES TOXIQUES" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Vérifier Docker
Write-Host "[1/5] Vérification de Docker..." -ForegroundColor Yellow
$dockerRunning = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker n'est pas démarré!" -ForegroundColor Red
    Write-Host "   Veuillez démarrer Docker Desktop et relancer ce script." -ForegroundColor Red
    exit 1
}
Write-Host "✅ Docker est opérationnel" -ForegroundColor Green

# Naviguer vers le dossier docker
Write-Host ""
Write-Host "[2/5] Navigation vers le dossier docker..." -ForegroundColor Yellow
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$dockerPath = Join-Path $scriptPath "docker"

if (Test-Path $dockerPath) {
    Set-Location $dockerPath
    Write-Host "✅ Dossier docker trouvé: $dockerPath" -ForegroundColor Green
} else {
    Write-Host "❌ Dossier docker non trouvé!" -ForegroundColor Red
    exit 1
}

# Arrêter les anciens conteneurs
Write-Host ""
Write-Host "[3/5] Arrêt des conteneurs existants..." -ForegroundColor Yellow
docker-compose down --remove-orphans 2>&1 | Out-Null
Write-Host "✅ Conteneurs arrêtés" -ForegroundColor Green

# Construire et démarrer
Write-Host ""
Write-Host "[4/5] Construction et démarrage des services..." -ForegroundColor Yellow
Write-Host "   Cela peut prendre quelques minutes..." -ForegroundColor Gray
Write-Host ""

docker-compose up --build -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Erreur lors du démarrage!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ Services démarrés avec succès!" -ForegroundColor Green

# Attendre que les services soient prêts
Write-Host ""
Write-Host "[5/5] Attente du démarrage des services..." -ForegroundColor Yellow

$services = @(
    @{Name="Kafka"; Container="kafka"; Port=9092; Check={docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1}},
    @{Name="PostgreSQL"; Container="postgres"; Port=5432; Check={docker exec postgres pg_isready -U postgres 2>&1}},
    @{Name="Streamlit"; Container="streamlit"; Port=8501; Check={$true}}
)

foreach ($service in $services) {
    Write-Host "   Attente de $($service.Name)..." -ForegroundColor Gray -NoNewline
    $retries = 0
    $maxRetries = 30
    
    while ($retries -lt $maxRetries) {
        $result = & $service.Check
        if ($LASTEXITCODE -eq 0 -or $result -eq $true) {
            Write-Host " ✅" -ForegroundColor Green
            break
        }
        Start-Sleep -Seconds 2
        $retries++
    }
    
    if ($retries -eq $maxRetries) {
        Write-Host " ⏳ (peut nécessiter plus de temps)" -ForegroundColor Yellow
    }
}

# Afficher l'architecture
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  ARCHITECTURE DU PIPELINE" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Facebook API" -ForegroundColor White
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  Producer → data.raw.stream" -ForegroundColor Yellow
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  Spark Normalizer → data.cleaned.stream" -ForegroundColor Blue
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  Spark Features → data.features.stream" -ForegroundColor Blue
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  Spark Model → data.predictions.stream" -ForegroundColor Blue
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  PostgreSQL" -ForegroundColor Green
Write-Host "       ↓" -ForegroundColor Gray
Write-Host "  Streamlit Dashboard" -ForegroundColor Magenta
Write-Host ""

# Afficher les URLs
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  INTERFACES DISPONIBLES" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  📊 Dashboard Streamlit: " -NoNewline -ForegroundColor White
Write-Host "http://localhost:8501" -ForegroundColor Green
Write-Host "  🔍 Kafka UI:           " -NoNewline -ForegroundColor White
Write-Host "http://localhost:8080" -ForegroundColor Green
Write-Host "  🗄️  PostgreSQL:         " -NoNewline -ForegroundColor White
Write-Host "localhost:5432" -ForegroundColor Green
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Afficher les logs en temps réel
Write-Host "📋 Affichage des logs (Ctrl+C pour arrêter)..." -ForegroundColor Yellow
Write-Host ""

docker-compose logs -f --tail=50
