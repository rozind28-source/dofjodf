<#
  Crypto Screener — запуск под Windows (PowerShell).

  Правый клик → «Выполнить с помощью PowerShell», либо в консоли:
      powershell -ExecutionPolicy Bypass -File .\start.ps1

  Если скрипты запрещены политикой, второй параметр обходит её только для этого файла.
#>

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

# ВАЖНО про кодировку: этот файл обязан быть сохранён как UTF-8 **с BOM**.
# Windows PowerShell 5.1 читает .ps1 без BOM как ANSI (в русской локали —
# cp1251), кириллица превращается в нечитаемую последовательность, и парсер
# ломается на первом же «)» внутри строки: Unexpected token ')'.
# PowerShell 7 читает UTF-8 и без BOM, но BOM не мешает ни одной из версий.
#
# Проверяется так:  Get-Content start.ps1 -Encoding Byte -TotalCount 3
# должно дать  239 187 191  (EF BB BF).
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $OutputEncoding = [System.Text.Encoding]::UTF8
} catch { }

function Test-Python {
    foreach ($cmd in @("python", "py", "python3")) {
        try {
            $v = & $cmd --version 2>&1
            if ($v -match "Python (\d+)\.(\d+)") {
                $major = [int]$Matches[1]; $minor = [int]$Matches[2]
                if ($major -gt 3 -or ($major -eq 3 -and $minor -ge 10)) {
                    Write-Host "  Python: $v  (команда: $cmd)" -ForegroundColor Green
                    return $cmd
                }
                Write-Host "  Найден $v, но нужно 3.10+" -ForegroundColor Yellow
            }
        } catch { }
    }
    Write-Host "  Python 3.10+ не найден. Установите с https://python.org" -ForegroundColor Red
    Write-Host "  При установке ОБЯЗАТЕЛЬНО поставьте галочку 'Add python.exe to PATH'." -ForegroundColor Red
    return $null
}

Write-Host ""
Write-Host "=== Crypto Screener ===" -ForegroundColor Cyan
$py = Test-Python
if (-not $py) { Read-Host "Нажмите Enter для выхода"; exit 1 }

# --- зависимости ---
Write-Host ""
Write-Host "Проверяю зависимости..." -ForegroundColor Cyan
# Проверяем через app.preflight, а не голым "import ccxt": импорт проходит и на
# старом ccxt, в котором нет классов aster/hyperliquid, — биржи тогда молча не
# подключатся, а проверка отрапортует «всё хорошо».
$check = "import sys; sys.path.insert(0,'.'); from app.preflight import check; " +
         "sys.exit(0 if check().ok else 1)"
& $py -c $check 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Устанавливаю из requirements.txt (первый раз займёт минуту)..." -ForegroundColor Yellow
    & $py -m pip install --upgrade pip --quiet
    & $py -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Не удалось установить зависимости." -ForegroundColor Red
        Read-Host "Нажмите Enter для выхода"; exit 1
    }
    $report = "import sys; sys.path.insert(0,'.'); from app.preflight import check, format_report; " +
              "r = check(); print(format_report(r)); sys.exit(0 if r.ok else 1)"
    & $py -c $report
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  После установки зависимости всё ещё неполные." -ForegroundColor Red
        Write-Host "  Подробности: python doctor.py" -ForegroundColor Yellow
        Read-Host "Нажмите Enter для выхода"; exit 1
    }
} else {
    Write-Host "  Все пакеты на месте" -ForegroundColor Green
}

# --- меню ---
Write-Host ""
Write-Host "Как запускаем?" -ForegroundColor Cyan
Write-Host "  1) ДЕМО — без интернета и без бирж (данные из записанного снимка рынка)"
Write-Host "     Самый надёжный способ посмотреть интерфейс прямо сейчас."
Write-Host ""
Write-Host "  2) ДИАГНОСТИКА — проверить, почему биржи не подключаются"
Write-Host "     Пройдёт по слоям: DNS / TLS / HTTPS / ccxt."
Write-Host ""
Write-Host "  3) LIVE лёгкий — 4 биржи, ~400 МБ памяти"
Write-Host "     Binance Futures, Bybit, OKX, Gate.io"
Write-Host ""
Write-Host "  4) LIVE полный — 8 бирж (включая MEXC, Aster, Hyperliquid)"
Write-Host "     Нужно 2-3 ГБ свободной памяти."
Write-Host ""
Write-Host "  5) LIVE с прокси — если биржи блокируют регион"
Write-Host ""
$choice = Read-Host "Введите номер (1-5)"

$port = Read-Host "Порт [8000]"
if (-not $port) { $port = "8000" }
$env:PORT = $port
$url = "http://localhost:$port"

switch ($choice) {
    "1" {
        $env:MODE = "replay"
        Write-Host ""
        Write-Host "Открываю $url  (Ctrl+C — остановить)" -ForegroundColor Green
        Start-Process $url
        & $py run.py
    }
    "2" {
        Write-Host ""
        & $py doctor.py
        Read-Host "Нажмите Enter для выхода"
    }
    "3" {
        $env:MODE = "live"
        $env:EXCHANGES = "binanceusdm,bybit,okx,gate"
        $env:TOP_N = "60"
        $env:BOOKS = "25"
        Write-Host ""
        Write-Host "Открываю $url  (Ctrl+C — остановить)" -ForegroundColor Green
        Write-Host "Первые 3-5 минут часть метрик пуста: качаются свечи и копится история." -ForegroundColor Yellow
        Start-Sleep -Seconds 3
        Start-Process $url
        & $py run.py
    }
    "4" {
        $env:MODE = "live"
        $env:TOP_N = "150"
        $env:BOOKS = "40"
        Write-Host ""
        Write-Host "Открываю $url  (Ctrl+C — остановить)" -ForegroundColor Green
        Write-Host "Первые 3-5 минут часть метрик пуста: качаются свечи и копится история." -ForegroundColor Yellow
        Start-Sleep -Seconds 3
        Start-Process $url
        & $py run.py
    }
    "5" {
        $p = Read-Host "Адрес прокси (например http://127.0.0.1:1080)"
        if (-not $p) { Write-Host "Прокси не указан." -ForegroundColor Red; Read-Host "Enter"; exit 1 }
        $env:MODE = "live"
        $env:PROXY_URL = $p
        $env:EXCHANGES = "binanceusdm,bybit,okx,gate"
        $env:TOP_N = "60"
        $env:BOOKS = "25"
        Write-Host ""
        Write-Host "Открываю $url через прокси $p" -ForegroundColor Green
        Start-Sleep -Seconds 3
        Start-Process $url
        & $py run.py
    }
    default {
        Write-Host "Не понял выбор, запускаю демо." -ForegroundColor Yellow
        $env:MODE = "replay"
        Start-Process $url
        & $py run.py
    }
}
