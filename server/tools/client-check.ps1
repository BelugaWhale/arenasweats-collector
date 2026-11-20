# ArenaSweats client check: read-only. A tester runs it with one line in PowerShell:
#   irm https://arenasweats.lol/collector/check | iex
# It checks how this PC's League client can prove an account (never sending a token), and which
# sites load from here. It saves arenasweats-login-check-2.txt on the Desktop to send back.
& {
# ArenaSweats login check 2. Read-only: it changes nothing in League or on this PC (apart from writing its report).
#
# 1. Open League and log in.
# 2. Open PowerShell normally (not as administrator) and run:
#      powershell -ExecutionPolicy Bypass -File .\login-check-2.ps1
# 3. Send the file arenasweats-login-check-2.txt from your Desktop.
#
# It never prints, saves or sends a password or token. For signed tokens it prints only the issuer, key ID, claim
# names and region values, and it checks signatures on this PC. The only internet requests fetch public signing
# keys (from auth.riotgames.com and, if the token names another issuer, from that issuer).
param([string]$ClientName = 'LeagueClientUx')

$ErrorActionPreference = 'Continue'
$lines = New-Object System.Collections.Generic.List[string]
function Say([string]$text) { $lines.Add($text); Write-Host $text }
function YesNo($value) { if ($value) { 'yes' } else { 'no' } }

# Riot's public signing keys (https://auth.riotgames.com/jwks.json, read 18 Nov 2026), for when that site can't be reached.
$RiotJwksSaved = '{"keys":[{"kty":"RSA","e":"AQAB","kid":"rso-prod-2024-11","n":"ynnJynfsN9aSla2r33k6U0Qo4dnDTYtRPPqU1RgAwgcCznJtFIyFy1ihI5d_kTDFTW7YiJ_0A0QIJ6jUcjlYxqKyFqndK645dOW9BAD3_zYPCHwnw2EV-MvcV4dj_mXsa-gFhFqrvEAgowXohL-fNg1wADea1kWCRylGJcrpMm4x8nVmoLo7FKJO039Ms_ng-WtghlReqzyyia5zqdWPQw7muJfYbQ833wONnCm8Aj7uCvB1HeoFdVI2edYoPsUjGWYOdg63nFiMsqKT-Ab9_SXnKBMj35qrYhO_evxiv9K2PH7lfUas5Wl8HMSP25OgWcrXVUSuThlCHgyHPd6DBw"},{"kty":"RSA","e":"AQAB","kid":"atlas_notifier","n":"jiINun3fRrh3_OWSj1yCZXhaMxzJDrlbBTJFUjzya6G6PxrzuC9rFvEZTRP1HvhZc3lXGP3Q2vvFbFFQ_JXsdcWLMriZRwnvj5eezBbAFS1688ED1VFQxYmNdaLj2KyXpwuGsJ3NW-woP4VkfWcz2SjlzSubTr8CrQB4IfzUqU2S2B6Lgf7bhO-do_EKNdJVYhpYkrvClYVdzRRDWnq5EsYSYZS4v4Zhnxo6ca5LAnwszT--lidg-7R6W1_pg1_iu3lDhCbH7h9kealq4NxOIbQc3eXhGrzxBPunU-jn2cqSE2yTVAfMaxfNZKNxrLcdN6sNHhouq8S6NFBpf7t9tQ"}]}'
$RiotIssuer = 'https://auth.riotgames.com'

# Values printed only for these names, and only when they look like an issuer, key ID or region.
$ValueKeys = @('iss', 'aud', 'alg', 'kid', 'typ', 'cpid', 'pid', 'active', 'region', 'r', 'p', 'platform', 'platformId', 'state', 'currentPlatformId', 'webRegion', 'locale', 'webLanguage', 'competitiveRegion')
$ArgValueNames = @('region', 'rso_platform_id', 'locale', 'rso-auth.url', 'rso-auth.client', 'rso_platform_name')

function Safe-Value($value) {
    if ($null -eq $value) { return 'null' }
    if ($value -is [bool]) { return "$value".ToLower() }
    $text = [string]$value
    if ($text -match '^[A-Za-z0-9_.:/+\-]{1,100}$' -and $text -notmatch '[A-Za-z0-9_\-]{33,}') { return $text }
    return '<not shown>'
}

function Safe-Text($value) {
    # Free text (error messages): JWTs and long values removed, shortened.
    if ($null -eq $value) { return '' }
    $text = [regex]::Replace([string]$value, 'eyJ[A-Za-z0-9_\-]*\.[A-Za-z0-9_\-]*\.?[A-Za-z0-9_\-]*', '<jwt>')
    $text = [regex]::Replace($text, '[A-Za-z0-9_\-+/=]{24,}', '<long value>')
    if ($text.Length -gt 160) { $text = $text.Substring(0, 160) + '...' }
    return $text
}

function Names($object) {
    if ($null -eq $object -or $object -is [string] -or $object -is [ValueType]) { return '' }
    return (($object.PSObject.Properties | ForEach-Object { $_.Name }) -join ', ')
}

Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class AsCheck {
    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr OpenProcess(uint access, bool inherit, int pid);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    static extern bool QueryFullProcessImageName(IntPtr handle, int flags, StringBuilder name, ref int size);
    [DllImport("ntdll.dll")] static extern int NtQueryInformationProcess(IntPtr handle, int infoClass, IntPtr info, int length, out int returned);
    [DllImport("advapi32.dll", SetLastError = true)] static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);
    [DllImport("advapi32.dll", SetLastError = true)] static extern bool GetTokenInformation(IntPtr token, int infoClass, out int info, int length, out int returned);

    public static string TryOpen(int pid, uint access) {
        IntPtr handle = OpenProcess(access, false, pid);
        if (handle == IntPtr.Zero) return "refused (Windows error " + Marshal.GetLastWin32Error() + ")";
        CloseHandle(handle);
        return "allowed";
    }
    public static string ImagePath(int pid) {
        IntPtr handle = OpenProcess(0x1000, false, pid);
        if (handle == IntPtr.Zero) return null;
        try {
            StringBuilder name = new StringBuilder(32768);
            int size = name.Capacity;
            return QueryFullProcessImageName(handle, 0, name, ref size) ? name.ToString() : null;
        } finally { CloseHandle(handle); }
    }
    // The command line with "limited information" access only, as the China collector and ArenaSweats Desktop read it.
    public static string CommandLine(int pid, out string problem) {
        problem = null;
        IntPtr handle = OpenProcess(0x1000, false, pid);
        if (handle == IntPtr.Zero) { problem = "process refused (Windows error " + Marshal.GetLastWin32Error() + ")"; return null; }
        IntPtr buffer = Marshal.AllocHGlobal(70000);
        try {
            int returned;
            int status = NtQueryInformationProcess(handle, 60, buffer, 70000, out returned);
            if (status != 0) { problem = "query refused (status 0x" + status.ToString("X8") + ")"; return null; }
            int length = (ushort)Marshal.ReadInt16(buffer);
            IntPtr text = Marshal.ReadIntPtr(buffer, IntPtr.Size);
            return Marshal.PtrToStringUni(text, length / 2);
        } finally { Marshal.FreeHGlobal(buffer); CloseHandle(handle); }
    }
    public static string Elevation(int pid) {
        IntPtr handle = OpenProcess(0x1000, false, pid);
        if (handle == IntPtr.Zero) return "unknown (process refused)";
        try {
            IntPtr token;
            if (!OpenProcessToken(handle, 0x0008, out token)) return "unknown (token refused, Windows error " + Marshal.GetLastWin32Error() + ")";
            try {
                int elevated, returned;
                if (!GetTokenInformation(token, 20, out elevated, 4, out returned)) return "unknown";
                return elevated != 0 ? "runs as administrator" : "runs without administrator rights";
            } finally { CloseHandle(token); }
        } finally { CloseHandle(handle); }
    }
}
'@

function Decode-Base64Url([string]$text) {
    $text = $text.Replace('-', '+').Replace('_', '/')
    switch ($text.Length % 4) { 2 { $text += '==' } 3 { $text += '=' } }
    return [Convert]::FromBase64String($text)
}

function Read-Jwt([string]$token) {
    if (-not $token) { return $null }
    $parts = $token.Split('.')
    if ($parts.Count -ne 3) { return $null }
    try {
        $header = [Text.Encoding]::UTF8.GetString((Decode-Base64Url $parts[0])) | ConvertFrom-Json
        $claims = [Text.Encoding]::UTF8.GetString((Decode-Base64Url $parts[1])) | ConvertFrom-Json
    } catch { return $null }
    return @{ Header = $header; Claims = $claims; Parts = $parts }
}

function Describe-Object([string]$prefix, $object, [int]$depth) {
    foreach ($property in $object.PSObject.Properties) {
        $name = $property.Name
        $value = $property.Value
        if ($value -is [System.Array]) {
            Say "    $prefix$name : list of $($value.Count)"
            $index = 0
            foreach ($item in ($value | Select-Object -First 4)) {
                if ($item -is [System.Management.Automation.PSCustomObject] -and $depth -lt 3) {
                    Describe-Object "$prefix$name[$index]." $item ($depth + 1)
                } elseif ($ValueKeys -contains $name) {
                    Say "    $prefix$name[$index] = $(Safe-Value $item)"
                }
                $index++
            }
        } elseif ($value -is [System.Management.Automation.PSCustomObject]) {
            Say "    $prefix$name : object"
            if ($depth -lt 3) { Describe-Object "$prefix$name." $value ($depth + 1) }
        } elseif ($ValueKeys -contains $name) {
            Say "    $prefix$name = $(Safe-Value $value)"
        } else {
            Say "    $prefix$name"
        }
    }
}

function Describe-Jwt([string]$label, $token) {
    if (-not ($token -is [string]) -or -not $token) { Say "  $label : absent"; return $null }
    $jwt = Read-Jwt $token
    if (-not $jwt) { Say "  $label : present, not a JWT ($($token.Length) characters)"; return $null }
    $header = $jwt.Header
    Say "  $label : JWT, alg=$(Safe-Value $header.alg) kid=$(Safe-Value $header.kid) typ=$(Safe-Value $header.typ)"
    Say "  $label header fields: $(Names $header)"
    Say "  $label claims:"
    Describe-Object '' $jwt.Claims 0
    $claims = $jwt.Claims
    if ($claims.exp -is [ValueType] -and $claims.iat -is [ValueType]) { Say "    (lifetime: $([int64]$claims.exp - [int64]$claims.iat) seconds)" }
    return $jwt
}

function Lcu-Get([int]$port, [string]$password, [string]$path) {
    $request = [System.Net.HttpWebRequest]::Create("https://127.0.0.1:$port$path")
    $request.ServerCertificateValidationCallback = { $true }    # the League client's own certificate; this request only
    $request.Headers.Add('Authorization', 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("riot:$password")))
    $request.Accept = 'application/json'
    $request.Timeout = 8000
    $response = $null
    try { $response = $request.GetResponse() } catch {
        $exception = $_.Exception
        while ($exception -and -not ($exception -is [System.Net.WebException])) { $exception = $exception.InnerException }
        if ($exception) { $response = $exception.Response }
        if (-not $response) { return @{ Status = 0; Body = $null; Problem = (Safe-Text $_.Exception.Message) } }
    }
    $reader = New-Object System.IO.StreamReader($response.GetResponseStream(), [Text.Encoding]::UTF8)
    $text = $reader.ReadToEnd()
    $response.Close()
    $body = $null
    try { $body = $text | ConvertFrom-Json } catch { }
    return @{ Status = [int]$response.StatusCode; Body = $body; Problem = $null }
}

function Get-Json([string]$url) {
    try { return @{ Body = (Invoke-RestMethod -Uri $url -TimeoutSec 15 -UseBasicParsing); Problem = $null } }
    catch { return @{ Body = $null; Problem = (Safe-Text $_.Exception.Message) } }
}

function Verify-Jwt($jwt, $jwk) {
    $data = [Text.Encoding]::ASCII.GetBytes($jwt.Parts[0] + '.' + $jwt.Parts[1])
    $signature = Decode-Base64Url $jwt.Parts[2]
    try {
        if ($jwt.Header.alg -eq 'RS256' -and $jwk.kty -eq 'RSA') {
            $modulus = Decode-Base64Url $jwk.n
            if ($modulus[0] -eq 0) { $modulus = $modulus[1..($modulus.Length - 1)] }
            $parameters = New-Object System.Security.Cryptography.RSAParameters
            $parameters.Modulus = [byte[]]$modulus
            $parameters.Exponent = [byte[]](Decode-Base64Url $jwk.e)
            $rsa = New-Object System.Security.Cryptography.RSACryptoServiceProvider
            $rsa.ImportParameters($parameters)
            return (YesNo $rsa.VerifyData($data, 'SHA256', $signature))
        }
        if ($jwt.Header.alg -eq 'ES256' -and $jwk.kty -eq 'EC') {
            $point = New-Object System.Security.Cryptography.ECPoint
            $point.X = Decode-Base64Url $jwk.x
            $point.Y = Decode-Base64Url $jwk.y
            $parameters = New-Object System.Security.Cryptography.ECParameters
            $parameters.Curve = [System.Security.Cryptography.ECCurve+NamedCurves]::nistP256
            $parameters.Q = $point
            $ecdsa = [System.Security.Cryptography.ECDsa]::Create($parameters)
            return (YesNo $ecdsa.VerifyData($data, $signature, [System.Security.Cryptography.HashAlgorithmName]::SHA256))
        }
        return "not checked (alg $(Safe-Value $jwt.Header.alg), key type $(Safe-Value $jwk.kty))"
    } catch { return "not checked ($(Safe-Text $_.Exception.Message))" }
}

function Check-Signature([string]$source, $keys, $jwt) {
    $key = $keys | Where-Object { $_.kid -eq $jwt.Header.kid } | Select-Object -First 1
    if (-not $key) {
        Say "    $source : $(@($keys).Count) keys, token's key ID not among them"
        return 'key not found'
    }
    $result = Verify-Jwt $jwt $key
    Say "    $source : key found, signature valid: $result"
    return $result
}

function Command-Line-Values([string]$line) {
    $found = [ordered]@{}
    foreach ($match in [regex]::Matches($line, '--([A-Za-z0-9_.\-]+)(=([^\s"]*))?')) {
        $name = $match.Groups[1].Value
        if (-not $found.Contains($name)) { $found[$name] = $match.Groups[3].Value }
    }
    return $found
}

[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
$answers = [ordered]@{}

Say 'ArenaSweats login check 2'
Say "Time (UTC): $([DateTime]::UtcNow.ToString('yyyy-MM-dd HH:mm'))"
Say "Windows: $([Environment]::OSVersion.VersionString), PowerShell $($PSVersionTable.PSVersion), 64-bit: $(YesNo ([Environment]::Is64BitProcess))"
$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
Say "This PowerShell runs as administrator: $(YesNo $admin)"
Say "curl.exe available: $(YesNo (Get-Command curl.exe -ErrorAction SilentlyContinue))"

Say ''
Say '== 1. Programs =='
$related = Get-Process | Where-Object { $_.ProcessName -match 'league|lol|riot|wegame|tcls|tencent|^ace|sguard|tenprotect|tersafe|^tp|^client$|qq' } |
    ForEach-Object { $_.ProcessName } | Sort-Object -Unique
Say "Related programs running: $($related -join ', ')"
$uxProcesses = @(Get-Process $ClientName -ErrorAction SilentlyContinue)
Say "$ClientName.exe running: $($uxProcesses.Count)"
$clientFolders = New-Object System.Collections.Generic.List[string]
$commandLines = New-Object System.Collections.Generic.List[object]
foreach ($process in $uxProcesses) {
    $id = $process.Id
    Say "  process $id"
    Say "    Get-Process path readable: $(YesNo $process.Path)"
    $image = [AsCheck]::ImagePath($id)
    Say "    path (limited access): $(if ($image) { $image } else { 'refused' })"
    if ($image) { $clientFolders.Add((Split-Path $image)) }
    Say "    open with limited access: $([AsCheck]::TryOpen($id, 0x1000))"
    Say "    open with memory read access: $([AsCheck]::TryOpen($id, 0x0410))"
    $elevation = [AsCheck]::Elevation($id)
    Say "    client $elevation"
    $wmi = Get-CimInstance Win32_Process -Filter "ProcessId=$id" -ErrorAction SilentlyContinue
    Say "    command line from WMI (the pasted command's way): $(if ($wmi.CommandLine) { 'readable' } else { 'not readable' })"
    $problem = $null
    $line = [AsCheck]::CommandLine($id, [ref]$problem)
    Say "    command line with limited access (Desktop's and the collector's way): $(if ($line) { 'readable' } else { $problem })"
    $answers['Client'] = $elevation
    $answers['Client path readable by Get-Process (old pasted command)'] = YesNo $process.Path
    $answers['Client command line readable: WMI / limited access (Desktop)'] = "$(YesNo $wmi.CommandLine) / $(YesNo $line)"
    if (-not $line) { $line = $wmi.CommandLine }
    if ($line) {
        $values = Command-Line-Values $line
        Say "    argument names: $($values.Keys -join ', ')"
        foreach ($name in $ArgValueNames) { if ($values.Contains($name)) { Say "    --$name = $(Safe-Value $values[$name])" } }
        $port = 0
        if ([int]::TryParse($values['app-port'], [ref]$port) -and $values['remoting-auth-token']) {
            $commandLines.Add(@{ Port = $port; Password = $values['remoting-auth-token']; Source = "command line of process $id" })
            Say '    has --app-port and --remoting-auth-token: yes'
        } else { Say '    has --app-port and --remoting-auth-token: no' }
    }
}
foreach ($process in @(Get-Process LeagueClient -ErrorAction SilentlyContinue)) {
    $image = [AsCheck]::ImagePath($process.Id)
    if ($image) { $clientFolders.Add((Split-Path $image)) }
}

Say ''
Say '== 2. Lockfiles =='
$lockfiles = New-Object System.Collections.Generic.List[object]
$checked = @{}
foreach ($folder in $clientFolders) {
    $current = $folder
    for ($level = 0; $level -le 2 -and $current; $level++) {
        $path = Join-Path $current 'lockfile'
        if (-not $checked.ContainsKey($path)) {
            $checked[$path] = $true
            if (Test-Path -LiteralPath $path) {
                $stream = [IO.File]::Open($path, 'Open', 'Read', 'ReadWrite'); $reader = New-Object IO.StreamReader($stream); $fields = $reader.ReadToEnd().Trim().Split(':'); $reader.Close()
                $port = 0
                $usable = $fields.Count -ge 5 -and [int]::TryParse($fields[2], [ref]$port)
                Say "  found: $path ($($fields.Count) fields, usable: $(YesNo $usable))"
                if ($usable) { $lockfiles.Add(@{ Port = $port; Password = $fields[3]; Source = "lockfile $path" }) }
            } else { Say "  none: $path" }
        }
        $current = Split-Path $current
    }
}
if ($clientFolders.Count -eq 0) { Say '  no client folder known, so no lockfile looked for' }
$answers['Usable lockfiles found'] = $lockfiles.Count

Say ''
Say '== 3. The new pasted link command =='
$uxPath = (Get-Process $ClientName -ErrorAction SilentlyContinue).Path
$besideUx = if ($uxPath) { Join-Path (Split-Path @($uxPath)[0]) 'lockfile' } else { $null }
if ($besideUx -and (Test-Path -LiteralPath $besideUx)) { $method = 'lockfile beside the client' }
else {
    $wmiLine = "$((Get-CimInstance Win32_Process -Filter "Name='$ClientName.exe'" -ErrorAction SilentlyContinue).CommandLine)"
    $hasPort = [regex]::Match($wmiLine, '--app-port=(\d+)').Success
    $hasPassword = [regex]::Match($wmiLine, '--remoting-auth-token=([\w-]+)').Success
    $method = if ($hasPort -and $hasPassword) { 'client command line (WMI)' } else { 'nothing: it would say "Open League and log in first"' }
}
Say "  The new command would use: $method"
$answers['Pasted command (new) finds the client by'] = $method

Say ''
Say '== 4. League client API access =='
$working = $null
foreach ($group in $lockfiles, $commandLines) {
    foreach ($source in $group) {
        $result = Lcu-Get $source.Port $source.Password '/lol-summoner/v1/current-summoner'
        Say "  $($source.Source): HTTP $($result.Status) $($result.Problem)"
        if ($result.Status -eq 200 -and -not $working) { $working = $source; $summoner = $result.Body }
    }
}
$answers['League API reachable via'] = if ($working) { $working.Source -replace ' [A-Z]:\\.*', '' } else { 'nothing' }
if (-not $working) {
    Say '  No way to reach the League client API worked. Is League open and logged in?'
} else {
    $port = $working.Port
    $password = $working.Password
    Say "  current summoner: gameName present: $(YesNo $summoner.gameName), tagLine present: $(YesNo $summoner.tagLine), puuid present: $(YesNo $summoner.puuid)"
    $answers['Riot ID (gameName#tagLine) on this account'] = YesNo ($summoner.gameName -and $summoner.tagLine)

    Say ''
    Say '== 5. Region values =='
    $regionLocale = Lcu-Get $port $password '/riotclient/region-locale'
    Say "  /riotclient/region-locale: HTTP $($regionLocale.Status)"
    if ($regionLocale.Body) { Describe-Object '' $regionLocale.Body 0 }
    $answers['region-locale region / webRegion'] = "$(Safe-Value $regionLocale.Body.region) / $(Safe-Value $regionLocale.Body.webRegion)"
    foreach ($path in '/lol-platform-config/v1/namespaces/LoginDataPacket/platformId', '/lol-platform-config/v1/namespaces/LoginDataPacket/competitiveRegion') {
        $result = Lcu-Get $port $password $path
        $value = if ($result.Status -eq 200) { Safe-Value $result.Body } else { '-' }
        Say "  $path : HTTP $($result.Status), value $value"
        if ($path -like '*platformId') { $answers['LoginDataPacket platformId'] = $value }
    }
    $chat = Lcu-Get $port $password '/lol-chat/v1/me'
    Say "  /lol-chat/v1/me: HTTP $($chat.Status) platformId $(Safe-Value $chat.Body.platformId)"
    $arguments = Lcu-Get $port $password '/riotclient/command-line-args'
    $argumentNames = @($arguments.Body | ForEach-Object { if ($_ -is [string] -and $_ -match '^--([A-Za-z0-9_.\-]+)') { $Matches[1] } })
    Say "  /riotclient/command-line-args: HTTP $($arguments.Status), has rso_platform_id: $(YesNo ($argumentNames -contains 'rso_platform_id'))"

    Say ''
    Say '== 6. ID token (the proof ArenaSweats checks today) =='
    $idToken = Lcu-Get $port $password '/lol-rso-auth/v1/authorization/id-token'
    Say "  /lol-rso-auth/v1/authorization/id-token: HTTP $($idToken.Status) $($idToken.Problem)"
    if ($idToken.Status -ne 200 -and $idToken.Body) { Say "  message: $(Safe-Text $idToken.Body.message)" }
    $answers['ID token'] = 'absent'
    if ($idToken.Body) {
        Say "  response fields: $(Names $idToken.Body)"
        $jwt = Describe-Jwt 'id token' $idToken.Body.token
        if ($jwt) {
            $claims = $jwt.Claims
            Say "    sub equals the current summoner's puuid: $(YesNo ($claims.sub -and $claims.sub -eq $summoner.puuid))"
            $active = @($claims.lol_region | Where-Object { $_.active } | ForEach-Object { Safe-Value $_.cpid })
            $answers['ID token'] = "iss $(Safe-Value $claims.iss), alg $(Safe-Value $jwt.Header.alg), kid $(Safe-Value $jwt.Header.kid), active lol_region $($active -join '/')"
            $answers['ID token has acct.game_name and acct.tag_line'] = YesNo ($claims.acct.game_name -and $claims.acct.tag_line)
            Say '  Signature checks (on this PC; the token is not sent anywhere):'
            $live = Get-Json "$RiotIssuer/jwks.json"
            if ($live.Body) { $riotLive = Check-Signature 'Riot keys from auth.riotgames.com' $live.Body.keys $jwt }
            else { Say "    Riot keys from auth.riotgames.com: not reachable ($($live.Problem))"; $riotLive = 'site not reachable' }
            $riotSaved = Check-Signature "Riot keys saved in this script" ($RiotJwksSaved | ConvertFrom-Json).keys $jwt
            $answers['Signed by Riot (live keys / saved keys)'] = "$riotLive / $riotSaved"
            $answers['auth.riotgames.com reachable from this PC'] = YesNo $live.Body
            $issuer = [string]$claims.iss
            if ($issuer -match '^https://[A-Za-z0-9.\-]+(:[0-9]+)?(/[A-Za-z0-9._\-/]*)?$' -and $issuer.TrimEnd('/') -ne $RiotIssuer) {
                $base = $issuer.TrimEnd('/')
                $discovery = Get-Json "$base/.well-known/openid-configuration"
                Say "    $base/.well-known/openid-configuration: $(if ($discovery.Body) { 'reachable, jwks_uri ' + (Safe-Value $discovery.Body.jwks_uri) } else { 'not reachable (' + $discovery.Problem + ')' })"
                $keyUrls = @("$base/jwks.json")
                if ($discovery.Body.jwks_uri) { $keyUrls = @([string]$discovery.Body.jwks_uri) + $keyUrls }
                $issuerResult = 'keys not reachable'
                foreach ($url in ($keyUrls | Select-Object -Unique)) {
                    $keys = Get-Json $url
                    if ($keys.Body) { $issuerResult = Check-Signature "issuer keys from $(Safe-Value $url)" $keys.Body.keys $jwt; break }
                    Say "    $(Safe-Value $url): not reachable ($($keys.Problem))"
                }
                $answers["Signed by its own issuer's published keys"] = $issuerResult
            }
        }
    }

    Say ''
    Say '== 7. Other proofs the client offers =='
    $authorization = Lcu-Get $port $password '/lol-rso-auth/v1/authorization'
    Say "  /lol-rso-auth/v1/authorization: HTTP $($authorization.Status), fields: $(Names $authorization.Body), currentPlatformId $(Safe-Value $authorization.Body.currentPlatformId)"
    $access = Lcu-Get $port $password '/lol-rso-auth/v1/authorization/access-token'
    Say "  /lol-rso-auth/v1/authorization/access-token: HTTP $($access.Status), fields: $(Names $access.Body)"
    if ($access.Body) { $null = Describe-Jwt 'access token' $access.Body.token }
    $userinfo = Lcu-Get $port $password '/lol-rso-auth/v1/authorization/userinfo'
    Say "  /lol-rso-auth/v1/authorization/userinfo: HTTP $($userinfo.Status), fields: $(Names $userinfo.Body)"
    if ($userinfo.Body.userInfo -is [string]) { $null = Describe-Jwt 'userinfo' $userinfo.Body.userInfo }
    $entitlements = Lcu-Get $port $password '/entitlements/v1/token'
    Say "  /entitlements/v1/token: HTTP $($entitlements.Status), fields: $(Names $entitlements.Body)"
    if ($entitlements.Body) { $null = Describe-Jwt 'entitlements token' $entitlements.Body.token }
    $session = Lcu-Get $port $password '/lol-login/v1/session'
    Say "  /lol-login/v1/session: HTTP $($session.Status), fields: $(Names $session.Body), state $(Safe-Value $session.Body.state)"
    if ($session.Body) {
        foreach ($field in 'idToken', 'gasToken', 'userAuthToken', 'accessToken') {
            $value = $session.Body.$field
            if ($value -is [string] -and $value) { $null = Describe-Jwt "login session $field" $value }
        }
    }
    $leagueSession = Lcu-Get $port $password '/lol-league-session/v1/league-session-token'
    Say "  /lol-league-session/v1/league-session-token: HTTP $($leagueSession.Status)"
    if ($leagueSession.Body -is [string]) { $null = Describe-Jwt 'league session token' $leagueSession.Body }
    if ($summoner.puuid) {
        $lookup = Lcu-Get $port $password "/lol-summoner/v2/summoners/puuid/$($summoner.puuid)"
        Say "  summoner lookup by puuid (as an observer would): HTTP $($lookup.Status), profileIconId present: $(YesNo ($null -ne $lookup.Body.profileIconId))"
        $answers['Summoner lookup by puuid shows the profile icon'] = YesNo ($null -ne $lookup.Body.profileIconId)
    }
}

Say ''
Say '== 8. In-game data (only while a game runs) =='
if (Get-Process 'League of Legends' -ErrorAction SilentlyContinue) {
    $request = [System.Net.HttpWebRequest]::Create('https://127.0.0.1:2999/liveclientdata/gamestats')
    $request.ServerCertificateValidationCallback = { $true }
    $request.Timeout = 5000
    try { $response = $request.GetResponse(); Say "  Live Client Data API: HTTP $([int]$response.StatusCode)"; $response.Close() }
    catch { Say "  Live Client Data API: $(Safe-Text $_.Exception.Message)" }
} else { Say '  no game running' }

Say ''
Say '== Answers =='
foreach ($key in $answers.Keys) { Say "  $key : $($answers[$key])" }

$file = Join-Path ([Environment]::GetFolderPath('Desktop')) 'arenasweats-login-check-2.txt'
$lines | Out-File -FilePath $file -Encoding UTF8
Write-Host ''
Write-Host "Saved to $file. Please send that file."
}
& {
# ArenaSweats reach check: which sites load from this PC, and how fast. Read-only; it only downloads
# small public files and adds the results to the login check report on the Desktop.
$ErrorActionPreference = 'Continue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$file = Join-Path ([Environment]::GetFolderPath('Desktop')) 'arenasweats-login-check-2.txt'
$targets = [ordered]@{
    'ArenaSweats site'               = 'https://arenasweats.lol/'
    'ArenaSweats collector endpoint' = 'https://arenasweats.lol/collector/healthz'
    'Collector download (ours)'      = 'https://arenasweats.lol/collector/download/ArenaSweatsCollector.exe'
    'Collector download (GitHub)'    = 'https://github.com/BelugaWhale/arenasweats-collector/releases/latest/download/ArenaSweatsCollector.exe'
    'Riot images (ddragon)'          = 'https://ddragon.leagueoflegends.com/api/versions.json'
    'CommunityDragon raw'            = 'https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/champion-icons/1.png'
    'CommunityDragon cdn'            = 'https://cdn.communitydragon.org/latest/champion/1/square'
    'Google Fonts'                   = 'https://fonts.googleapis.com/css2?family=Montserrat'
    'Google Tag Manager'             = 'https://www.googletagmanager.com/gtag/js?id=G-TEST'
    'Stripe script'                  = 'https://js.stripe.com/v3/'
    'Stripe checkout'                = 'https://checkout.stripe.com/'
    'Discord'                        = 'https://discord.com/api/v10/gateway'
}
$out = New-Object System.Collections.Generic.List[string]
$out.Add('')
$out.Add('== Reach check (what loads from this PC) ==')
foreach ($name in $targets.Keys) {
    $url = $targets[$name]
    $watch = [Diagnostics.Stopwatch]::StartNew()
    try {
        $request = [Net.HttpWebRequest]::Create($url)
        $request.Method = 'GET'
        $request.Timeout = 15000
        $request.ReadWriteTimeout = 15000
        $request.AddRange(0, 65535)             # at most 64 KB of each file
        $request.UserAgent = 'arenasweats-reach-check'
        $response = $request.GetResponse()
        $stream = $response.GetResponseStream()
        $buffer = New-Object byte[] 65536
        $total = 0
        while (($read = $stream.Read($buffer, 0, $buffer.Length)) -gt 0 -and $total -lt 65536) { $total += $read }
        $response.Close()
        $line = '{0,-32} OK   {1,5} ms  HTTP {2}  {3} bytes' -f $name, $watch.ElapsedMilliseconds, [int]$response.StatusCode, $total
    } catch [Net.WebException] {
        if ($_.Exception.Response) {     # the site answered, just not with the file: it is reachable
            $line = '{0,-32} OK   {1,5} ms  HTTP {2} (site reached)' -f $name, $watch.ElapsedMilliseconds, [int]$_.Exception.Response.StatusCode
        } else {
            $line = '{0,-32} FAIL {1,5} ms  {2}' -f $name, $watch.ElapsedMilliseconds, [string]$_.Exception.Status
        }
    } catch {
        $line = '{0,-32} FAIL {1,5} ms  {2}' -f $name, $watch.ElapsedMilliseconds, $_.Exception.GetType().Name
    }
    $out.Add($line)
    Write-Host $line
}
Add-Content -Path $file -Value $out -Encoding UTF8
Write-Host "Added to $file"
}
Write-Host 'Done. Please send arenasweats-login-check-2.txt from your Desktop.'
