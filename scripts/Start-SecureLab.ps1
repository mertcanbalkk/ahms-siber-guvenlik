param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$dockerPath = if ($dockerCommand) { $dockerCommand.Source } else { Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe' }
if (-not (Test-Path -LiteralPath $dockerPath)) { throw 'Docker Desktop bulunamadı.' }
function Invoke-Docker {
    param([string[]]$DockerArguments)
    & $dockerPath @DockerArguments
    if ($LASTEXITCODE -ne 0) { throw "Docker adımı başarısız: $($DockerArguments -join ' ')" }
}
Push-Location -LiteralPath $projectRoot
try {
    Invoke-Docker -DockerArguments @('info','--format','{{.ServerVersion}}')
    foreach ($name in @('gateway-keys.json','producer-key.json')) {
        if (-not (Test-Path -LiteralPath (Join-Path $projectRoot "security\$name"))) {
            throw 'HMAC anahtarı eksik. Yeni kurulumda önce npm run init çalıştırın; mevcut anahtarları sıfırlamayın.'
        }
    }
    Write-Host 'Sertifikalar hazırlanıyor; mevcut sertifikalar varsa doğrulanıp korunur.'
    Invoke-Docker -DockerArguments @('compose','--profile','setup','build','cert-init')
    Invoke-Docker -DockerArguments @('compose','--profile','setup','run','--no-deps','--rm','cert-init')
    Invoke-Docker -DockerArguments @('compose','config','--quiet')
    Write-Host 'Yeni servis imajları hazırlanıyor. Bu adımda çalışan servisler durdurulmaz.'
    Invoke-Docker -DockerArguments @('compose','build','ai','gateway','simulator','reader','security-test')
    Write-Host 'Şifreli Kafka bağlantısına geçiliyor; panel kısa süre yeniden bağlanacak.'
    Invoke-Docker -DockerArguments @('compose','stop','simulator','reader')
    Invoke-Docker -DockerArguments @('compose','up','-d','--wait','--wait-timeout','240','zookeeper','kafka','ai','gateway')
    Invoke-Docker -DockerArguments @('compose','run','--no-deps','--rm','simulator','python','telemetry_producer.py','--scenario','normal','--count','1','--interval','0')
    Invoke-Docker -DockerArguments @('compose','run','--no-deps','--rm','security-test')
    Invoke-Docker -DockerArguments @('compose','--profile','demo','up','--no-deps','-d','simulator')
    Write-Host 'Kurulum ve 11 güvenlik kontrolü tamamlandı. Panel: http://127.0.0.1:3000'
} finally {
    Pop-Location
}
