# Take in params
param(
    [Parameter(Mandatory=$true)][string]$old_path = "D:\Splunk\var\lib.bad_data\splunk", 
    [Parameter(Mandatory=$true)][string]$new_path = "D:\Splunk\var\lib\splunk", 
    [string]$log = "F:\recovery.log",
    [int]$threads = "30"
);

.".\Invoke-Parallel.ps1";


$directory = dir -Directory $old_path |sls ".*"
foreach ($idx in $directory)
{
    if(Test-Path "$old_path\$idx\db")
    {
        $buckets = dir -Directory "$old_path\$idx\db" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 


        $buckets | invoke-parallel -ImportVariables -throttle $threads -ScriptBlock { 
            function mylog
            {
                param([Parameter(Mandatory=$true)][string]$log_line)
                if($log)
                {
                    #$log_line | Out-File $log -Append
                    # lets put it to console too
                    Write-Host $log_line
                }
                else
                {
                    Write-Host $log_line
                }
            }

            function fix_bucket
            {
                param
                (
                    [Parameter(Mandatory=$true)][string]$idx,
                    [Parameter(Mandatory=$true)][string]$dest_idx,
                    [Parameter(Mandatory=$true)][string]$bucket
                )
                mylog -log_line "$(Get-Date) action=Executing bucket=$bucket index=$idx"

                # Check to see if the bucket is bad or not
                $checkLog = d:\splunk\bin\splunk check-rawdata-format -bucketPath $idx\$bucket 2>&1

                if($checkLog -like "*ERROR*")
                {
                    mylog -log_line "$(Get-Date) command=check-rawdata $checkLog"

                    mylog -log_line "$(Get-Date) command=check-rawdata action=check_raw Bucket=$bucket index=$idx status=bad_header" 

                    # Lets run Splunk's export tool against the bad bucket.
                    $exportLog = d:\splunk\bin\splunk cmd exporttool "$idx\$bucket" "$dest_idx\$bucket.csv" -csv | Wait-Process
                    mylog -log_line "$(Get-Date) command=exporttool action=expot Bucket=$bucket index=$idx new_path=$dest_idx\$bucket.csv status=exportComplete"
        
                    # Now we run import tool to rebuild the bucket
                    $importLog = d:\splunk\bin\splunk cmd importtool "$dest_idx\$bucket" "$dest_idx\$bucket.csv" | Wait-Process
                    mylog -log_line "$(Get-Date) command=importtool action=import Bucket=$bucket index=$idx new_path=$dest_idx\$bucket status=importComplete" 

                    # Now lets cleanup after ourselves
                    Remove-Item $dest_idx\$bucket.csv
                    mylog -log_line "$(Get-Date) command=Remove-Item action=delete path=$dest_idx\$bucket.csv msg='Removed the export file'"

                }

                # Good buckets go here
                else
                {
                    mylog -log_line "$(Get-Date) command=check-rawdata action=check_raw Bucket=$bucket index=$idx status=Okay"
                    # Should we mkae a copy of the bucket to the migration location?
                }

            }

            mylog -log_line 'Started Warm buckets. Bucket=$_ src=$old_path\$idx\db dest=$new_path\$idx\db'
            fix_bucket -idx "$old_path\$idx\db" -dest_idx "$new_path\$idx\db" -bucket $_

        }

    }

    if(Test-Path "$old_path\$idx\colddb")
    {
        $buckets = dir -Directory "$old_path\$idx\colddb" | sls "[dr]b_\d+_\d+_\d+(?:_[\w-]+)?" 

        $buckets | invoke-parallel -ImportVariables -throttle $threads -ScriptBlock { 

            # lets log things!
            function mylog
            {
                param([Parameter(Mandatory=$true)][string]$log_line)
                if($log)
                {
                    #$log_line | Out-File $log -Append
                    # lets put it to console too
                    Write-Host $log_line
                }
                else
                {
                    Write-Host $log_line
                }
            }

            function fix_bucket
            {
                param
                (
                    [Parameter(Mandatory=$true)][string]$idx,
                    [Parameter(Mandatory=$true)][string]$dest_idx,
                    [Parameter(Mandatory=$true)][string]$bucket
                )
                mylog -log_line "$(Get-Date) action=Executing bucket=$bucket index=$idx"

                # Check to see if the bucket is bad or not
                $checkLog = d:\splunk\bin\splunk check-rawdata-format -bucketPath $idx\$bucket 2>&1

                if($checkLog -like "*ERROR*")
                {
                    mylog -log_line "$(Get-Date) command=check-rawdata $checkLog"

                    mylog -log_line "$(Get-Date) command=check-rawdata action=check_raw Bucket=$bucket index=$idx status=bad_header" 

                    # Lets run Splunk's export tool against the bad bucket.
                    $exportLog = d:\splunk\bin\splunk cmd exporttool "$idx\$bucket" "$dest_idx\$bucket.csv" -csv | Wait-Process
                    mylog -log_line "$(Get-Date) command=exporttool action=expot Bucket=$bucket index=$idx new_path=$dest_idx\$bucket.csv status=exportComplete"
        
                    # Now we run import tool to rebuild the bucket
                    $importLog = d:\splunk\bin\splunk cmd importtool "$dest_idx\$bucket" "$dest_idx\$bucket.csv" | Wait-Process
                    mylog -log_line "$(Get-Date) command=importtool action=import Bucket=$bucket index=$idx new_path=$dest_idx\$bucket status=importComplete" 

                    # Now lets cleanup after ourselves
                    Remove-Item "$dest_idx\$bucket.csv"
                    mylog -log_line "$(Get-Date) command=Remove-Item action=delete path=$dest_idx\$bucket.csv msg='Removed the export file'"

                }

                # Good buckets go here
                else
                {
                    mylog -log_line "$(Get-Date) command=check-rawdata action=check_raw Bucket=$bucket index=$idx status=Okay"
                    # Should we mkae a copy of the bucket to the migration location?
                }

            }
            mylog -log_line 'Started Cold buckets. Bucket=$_ src=$old_path\$idx\colddb dest=$new_path\$idx\colddb'
            fix_bucket -idx "$old_path\$idx\colddb" -dest_idx "$new_path\$idx\colddb" -bucket $_

        }

        #foreach ($i in $buckets)
        #{
    
            #fix_bucket -idx $idx -dest_idx $dest_idx -bucket $i
            #Write-Host "Test! $old_path\$idx\colddb\$i"
            #break
        #}
    }
}


