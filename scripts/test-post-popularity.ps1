param(
    [string]$Tests = 'PostInteraction*Test,InteractionOutboxTest,PostPopularity*Test,MixedFeed*Test,FeedCursorCodecTest,HomeScreenCursorTest,FeedServiceTest,FeedItemHydratorTest'
)

# Focused verification while unrelated vector tests have constructor/API drift.
# Integration tests opt in through POPULARITY_TEST_* environment variables.
$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$verificationRoot = Join-Path $repositoryRoot 'target/popularity-verification'
New-Item -ItemType Directory -Path $verificationRoot -Force | Out-Null
[xml]$pom = Get-Content -LiteralPath (Join-Path $repositoryRoot 'pom.xml') -Raw
$namespace = $pom.DocumentElement.NamespaceURI
$namespaces = New-Object System.Xml.XmlNamespaceManager($pom.NameTable)
$namespaces.AddNamespace('m', $namespace)

function Set-PomChild($parent, [string]$name, [string]$value) {
    $child = $parent.SelectSingleNode("m:$name", $namespaces)
    if ($null -eq $child) {
        $child = $pom.CreateElement($name, $namespace)
        [void]$parent.AppendChild($child)
    }
    $child.InnerText = $value
    return $child
}

$build = $pom.SelectSingleNode('/m:project/m:build', $namespaces)
[void](Set-PomChild $build 'sourceDirectory' (Join-Path $repositoryRoot 'src/main/java'))
[void](Set-PomChild $build 'testSourceDirectory' (Join-Path $repositoryRoot 'src/test/java'))
[void](Set-PomChild $build 'directory' $verificationRoot)
foreach ($resource in $build.SelectNodes('m:resources/m:resource/m:directory', $namespaces)) {
    if (-not [System.IO.Path]::IsPathRooted($resource.InnerText)) {
        $resource.InnerText = Join-Path $repositoryRoot $resource.InnerText
    }
}
$testResources = $pom.CreateElement('testResources', $namespace)
$testResource = $pom.CreateElement('testResource', $namespace)
[void](Set-PomChild $testResource 'directory' (Join-Path $repositoryRoot 'src/test/resources'))
[void]$testResources.AppendChild($testResource)
[void]$build.AppendChild($testResources)
$compiler = $build.SelectSingleNode("m:plugins/m:plugin[m:artifactId='maven-compiler-plugin']", $namespaces)
$configuration = $compiler.SelectSingleNode('m:configuration', $namespaces)
if ($null -eq $configuration) {
    $configuration = $pom.CreateElement('configuration', $namespace)
    [void]$compiler.AppendChild($configuration)
}
$includes = $pom.CreateElement('testIncludes', $namespace)
foreach ($pattern in @('PostInteraction*Test', 'InteractionOutboxTest', 'PostPopularity*Test',
        'MixedFeed*Test', 'FeedCursorCodecTest', 'HomeScreenCursorTest', 'FeedServiceTest', 'FeedItemHydratorTest')) {
    $include = $pom.CreateElement('testInclude', $namespace)
    $include.InnerText = "**/$pattern.java"
    [void]$includes.AppendChild($include)
}
[void]$configuration.AppendChild($includes)
$temporaryPom = Join-Path $repositoryRoot 'target/post-popularity-tests.pom.xml'
$pom.Save($temporaryPom)

Push-Location $repositoryRoot
$previousPreference = $ErrorActionPreference
try {
    # Windows PowerShell treats native stderr (including JVM warnings) as errors.
    # Maven's process exit code determines success; do not abort on harmless warnings.
    $ErrorActionPreference = 'Continue'
    & (Join-Path $repositoryRoot 'mvnw.cmd') '-f' $temporaryPom "-Dtest=$Tests" 'test'
    $testExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousPreference
    Pop-Location
}
exit $testExitCode
