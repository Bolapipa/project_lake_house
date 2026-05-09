param(
    [string]$SourceDir = "C:\Users\Arthur\Desktop\project_data_lake",
    [string]$DestinationDir = "docs/images/project_data_lake",
    [string]$ReadmePath = "README.md"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path

$resolvedSource = Resolve-Path -LiteralPath $SourceDir -ErrorAction SilentlyContinue
if (-not $resolvedSource) {
    throw "Source path not found: $SourceDir"
}
$sourceFullPath = $resolvedSource.Path

$destinationFullPath = Join-Path $repoRoot $DestinationDir
$readmeFullPath = Join-Path $repoRoot $ReadmePath

if (-not (Test-Path -LiteralPath $destinationFullPath)) {
    New-Item -ItemType Directory -Path $destinationFullPath -Force | Out-Null
}

if (-not (Test-Path -LiteralPath $readmeFullPath)) {
    throw "README path not found: $readmeFullPath"
}

$supportedExtensions = @(".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg")

$sourceImages = Get-ChildItem -LiteralPath $sourceFullPath -File |
    Where-Object { $supportedExtensions -contains $_.Extension.ToLowerInvariant() } |
    Sort-Object Name

foreach ($image in $sourceImages) {
    $targetImagePath = Join-Path $destinationFullPath $image.Name
    Copy-Item -LiteralPath $image.FullName -Destination $targetImagePath -Force
}

$destinationImages = Get-ChildItem -LiteralPath $destinationFullPath -File |
    Where-Object { $supportedExtensions -contains $_.Extension.ToLowerInvariant() } |
    Sort-Object Name

$startMarker = "<!-- START:PROJECT_DATA_LAKE_IMAGES -->"
$endMarker = "<!-- END:PROJECT_DATA_LAKE_IMAGES -->"

$blockLines = @(
    $startMarker,
    "### Galeria automatica - project_data_lake",
    "",
    '> Bloco atualizado automaticamente por `scripts/sync_project_data_lake_images.ps1`.',
    "",
    "| Arquivo | Preview |",
    "| --- | --- |"
)

foreach ($file in $destinationImages) {
    $altText = (($file.BaseName -replace "[_-]+", " ") -replace "\s+", " ").Trim()
    if ([string]::IsNullOrWhiteSpace($altText)) {
        $altText = $file.Name
    }

    $relativeImagePath = "$DestinationDir/$($file.Name)" -replace "\\", "/"
    $inlineCodeTick = [char]96
    $blockLines += "| $inlineCodeTick$($file.Name)$inlineCodeTick | ![$altText]($relativeImagePath) |"
}

$blockLines += @(
    "",
    $endMarker
)

$newBlock = $blockLines -join [Environment]::NewLine
$readmeContent = Get-Content -LiteralPath $readmeFullPath -Raw -Encoding UTF8
$markerPattern = "(?s)$([regex]::Escape($startMarker)).*?$([regex]::Escape($endMarker))"

if ([regex]::IsMatch($readmeContent, $markerPattern)) {
    $updatedReadme = [regex]::Replace(
        $readmeContent,
        $markerPattern,
        [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $newBlock },
        1
    )
}
else {
    $updatedReadme = $readmeContent.TrimEnd() + [Environment]::NewLine + [Environment]::NewLine + $newBlock + [Environment]::NewLine
}

$normalizedReadme = $updatedReadme.TrimEnd("`r", "`n") + [Environment]::NewLine
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($readmeFullPath, $normalizedReadme, $utf8NoBom)

Write-Host ("Synchronized {0} image(s) from '{1}' to '{2}'." -f $sourceImages.Count, $sourceFullPath, $destinationFullPath)
Write-Host ("README gallery updated at '{0}'." -f $readmeFullPath)
