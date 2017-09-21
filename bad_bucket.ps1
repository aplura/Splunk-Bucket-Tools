param(
    [Parameter(Mandatory=$true)][string]$src = "D:\Splunk\var\lib\splunk", 
    [Parameter(Mandatory=$true)][string]$dest = "D:\bad_buckets"
);

$directory = dir -Directory $src |sls ".*"

function directory-summary($dir=".") { 
  get-childitem $dir | 
    % { $f = $_ ; 
        get-childitem -r $_.FullName | 
           measure-object -property length -sum | 
             select @{Name="Name";Expression={$f}},Sum}
}

function Get-DiskUsage ([string]$path=".") {
    $groupedList = Get-ChildItem -Recurse -File $path | Group-Object directoryName | select name,@{name='length'; expression={($_.group | Measure-Object -sum length).sum } }
    foreach ($dn in $groupedList) {
        New-Object psobject -Property @{ directoryName=$dn.name; length=($groupedList | where { $_.name -like "$($dn.name)*" } | Measure-Object -Sum length).sum }
    }
}

foreach ($idx in $directory)
{
    # Hot / Warm
    if(Test-Path "$src\$idx\db")
    {
        #$buckets = dir -Directory "$src\$idx\db" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 
        $buckets = directory-summary -dir "$src\$idx\db\"
        
        foreach ($test in $buckets)
        {
            if($test.Sum)
            {
                if($test.Name -match "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?")
                {
                    $bucket = $test.Name
                    $size = $test.Sum
                    if($test.Sum -lt 6000)
                    {
                        Write-Host "Moving Bucket: $src\$idx\db\$bucket to $dest\$idx\db\$bucket size: $size"
                        robocopy.exe "$src\$idx\db\$bucket" "$dest\$idx\db\$bucket" /E /MOVE
                        #Write-Host ($test | Format-Table | Out-String)
                    }
                }
            }
        }
    }
    # Cold
    if(Test-Path "$src\$idx\colddb")
    {
        #$buckets = dir -Directory "$src\$idx\db" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 
        $buckets = directory-summary -dir "$src\$idx\colddb\"
        
        foreach ($test in $buckets)
        {
            if($test.Sum)
            {
                if($test.Name -match "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?")
                {
                    $bucket = $test.Name
                    $size = $test.Sum
                    if($test.Sum -lt 6000)
                    {
                        Write-Host "Moving Bucket: $src\$idx\colddb\$bucket to $dest\$idx\colddb\$bucket size: $size"
                        robocopy.exe "$src\$idx\colddb\$bucket" "$dest\$idx\colddb\$bucket" /E /MOVE
                        #Write-Host ($test | Format-Table | Out-String)
                    }
                }
            }
        }
    }
    
}
