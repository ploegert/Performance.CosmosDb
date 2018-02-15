

$workingPath="C:\vso\Data.Performance.CosmosTests\src\loadtesting.Console.Threaded";

$processes=4;
$Threads=40;
$DocsPerTask=50000;
$throughput=20000;


dotnet publish -c Release -o c:\temp\publish

# Setup
For($i = 0; $i -lt $processes; $i++)
{
    $dest = "C:\Temp\$i"
    remove-item -Path $dest -Force -Recurse -Confirm:$false
    Copy-Item -Path C:\Temp\Publish\ -Recurse -Destination $dest -Container

}

For($i = 0; $i -lt $processes; $i++)
{
    $workingDir = "C:\Temp\$i"
    start-process -FilePath "dotnet" -WorkingDirectory $workingDir -Args ".\loadtesting.Console.Threaded.dll /CosmosDb:CollectionThroughput=$throughput /CosmosDb:NumberOfDocumentsPerTask=$DocsPerTask /CosmosDb:DegreeOfParallelism=$Threads /CosmosDb:SummaryFile=C:\\temp\\Summary-$i.csv /CosmosDb:DetailsFile=C:\\temp\\Perf-$i.csv"
}


# start-process -FilePath "dotnet" -WorkingDirectory $workingPath -Args "run -c release /CosmosDb:CollectionThroughput=$throughput /CosmosDb:NumberOfDocumentsPerTask=$DocsPerTask /CosmosDb:DegreeOfParallelism=$Threads"
# timeout 1
# start-process -FilePath "dotnet" -WorkingDirectory $workingPath -Args "run -c release /CosmosDb:CollectionThroughput=$throughput /CosmosDb:NumberOfDocumentsPerTask=$DocsPerTask /CosmosDb:DegreeOfParallelism=$Threads"

#dotnet run /CosmosDb:CollectionThroughput=10000 /CosmosDb:NumberOfDocumentsPerTask=100 /CosmosDb:DegreeOfParallelism=5