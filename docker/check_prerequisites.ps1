# ============================================================================
# Script de Vérification et Configuration
# ============================================================================
# Vérifie les prérequis et prépare l'environnement

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  VÉRIFICATION DES PRÉREQUIS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

$errors = 0

# 1. Vérifier Docker
Write-Host "[1/4] Vérification de Docker..." -ForegroundColor Yellow
$dockerVersion = docker --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Docker: $dockerVersion" -ForegroundColor Green
} else {
    Write-Host "❌ Docker non trouvé!" -ForegroundColor Red
    Write-Host "   Téléchargez Docker Desktop: https://www.docker.com/products/docker-desktop" -ForegroundColor Gray
    $errors++
}

# 2. Vérifier Docker Compose
Write-Host ""
Write-Host "[2/4] Vérification de Docker Compose..." -ForegroundColor Yellow
$composeVersion = docker-compose --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Docker Compose: $composeVersion" -ForegroundColor Green
} else {
    Write-Host "❌ Docker Compose non trouvé!" -ForegroundColor Red
    $errors++
}

# 3. Vérifier les ports
Write-Host ""
Write-Host "[3/4] Vérification des ports..." -ForegroundColor Yellow
$ports = @(5432, 8080, 8501, 9092)
foreach ($port in $ports) {
    $connection = Test-NetConnection -ComputerName localhost -Port $port -WarningAction SilentlyContinue 2>&1
    if ($connection.TcpTestSucceeded) {
        Write-Host "⚠️  Port $port déjà utilisé" -ForegroundColor Yellow
    } else {
        Write-Host "✅ Port $port disponible" -ForegroundColor Green
    }
}

# 4. Vérifier les fichiers
Write-Host ""
Write-Host "[4/4] Vérification des fichiers..." -ForegroundColor Yellow
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$requiredFiles = @(
    "docker-compose.yml",
    "init.sql",
    "producer/Dockerfile",
    "producer/producer.py",
    "spark/Dockerfile",
    "spark/stream_normalizer.py",
    "spark/feature_builder.py",
    "spark/model_serving.py",
    "streamlit/Dockerfile",
    "streamlit/dashboard.py"
)

$missingFiles = @()
foreach ($file in $requiredFiles) {
    $filePath = Join-Path $scriptPath $file
    if (Test-Path $filePath) {
        Write-Host "✅ $file" -ForegroundColor Green
    } else {
        Write-Host "❌ $file manquant!" -ForegroundColor Red
        $missingFiles += $file
        $errors++
    }
}

# 5. Copier le modèle ML (optionnel)
Write-Host ""
Write-Host "[Bonus] Configuration du modèle ML..." -ForegroundColor Yellow
$modelSource = Join-Path (Split-Path $scriptPath -Parent) "model"
$modelDest = Join-Path $scriptPath "model"

if (Test-Path $modelSource) {
    if (-not (Test-Path $modelDest)) {
        New-Item -ItemType Directory -Path $modelDest | Out-Null
    }
    Copy-Item -Path "$modelSource\*" -Destination $modelDest -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "✅ Modèle ML copié vers docker/model/" -ForegroundColor Green
} else {
    Write-Host "⚠️  Modèle ML non trouvé (mode règles utilisé)" -ForegroundColor Yellow
}

# Résumé
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  RÉSUMÉ" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

if ($errors -eq 0) {
    Write-Host "✅ Tous les prérequis sont satisfaits!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Pour démarrer le pipeline:" -ForegroundColor White
    Write-Host "   cd docker" -ForegroundColor Gray
    Write-Host "   docker-compose up --build" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Ou utilisez:" -ForegroundColor White
    Write-Host "   .\start_pipeline.ps1" -ForegroundColor Gray
} else {
    Write-Host "❌ $errors erreur(s) détectée(s)" -ForegroundColor Red
    Write-Host "   Veuillez corriger les problèmes ci-dessus." -ForegroundColor Gray
}

Write-Host ""
