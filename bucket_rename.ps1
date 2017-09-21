param(
    [Parameter(Mandatory=$true)][string]$src = "D:\Splunk\var\lib\splunk"
    #[Parameter(Mandatory=$true)][string]$dest = "D:\bad_buckets"
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
    #Write-Host "here"
    # Hot / Warm
    if(Test-Path "$src\$idx\db")
    {
        $buckets = dir -Directory "$src\$idx\db" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 
        #$buckets = directory-summary -dir "$src\$idx\db\"
        
        foreach ($test in $buckets)
        {
            if($test.Line -match "^[dr]b_\d+_\d+_\d+$")
            {
				$guid = "_42A83E31-D348-40A7-BE80-BC7219BAFF7A"
				
                #Write-Host ($test | Format-Table | Out-String)
				Write-Host "Making bucket='$src\$idx\db\$test' a clustered bucket dest=$src\$idx\db\$test$guid"
				robocopy.exe "$src\$idx\db\$test" "$src\$idx\db\$test$guid" /E /MOVE
            }
            
        }
    }
	
	
    # Cold
	if(Test-Path "$src\$idx\colddb")
    {
        $buckets = dir -Directory "$src\$idx\colddb" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 
        #$buckets = directory-summary -dir "$src\$idx\colddb\"
        
        foreach ($test in $buckets)
        {
            if($test.Line -match "^[dr]b_\d+_\d+_\d+$")
            {
                #Write-Host ($test | Format-Table | Out-String)
				$guid = "_42A83E31-D348-40A7-BE80-BC7219BAFF7A"
				Write-Host "Making bucket='$src\$idx\colddb\$test' a clustered bucket dest=$src\$idx\colddb\$test$guid"
				robocopy.exe "$src\$idx\db\$test" "$src\$idx\colddb\$test$guid" /E /MOVE
            }
            
        }
    }
    
    
    
}
