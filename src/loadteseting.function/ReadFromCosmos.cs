// Much of the code was derived from: 
//      https://github.com/Azure/azure-documentdb-dotnet/tree/master/samples/documentdb-benchmark
//      https://docs.microsoft.com/en-us/azure/cosmos-db/performance-testing
//
// the rest was evoved by justin.j.ploegert@jci.com on ~2/15/2018

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Threading;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;

using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace loadteseting.function
{
    public sealed class ReadFromCosmos
    {
        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        private const int MinThreadPoolSize = 100;

        private Stopwatch _GlobalWatch;
        private int _taskCount;
        private int _pendingTaskCount;
        private long _documentsInserted;
        private ConcurrentDictionary<int, double> _requestUnitsConsumed = new ConcurrentDictionary<int, double>();

        private readonly DocumentClient _client;
        private Database _database;
        private DocumentCollection _dataCollection;

        private static TraceWriter _logger;
        private static CosmosDbSettings _settings;

        [FunctionName("ReadFromCosmos")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, TraceWriter log, ExecutionContext context)
        {
            _logger = log;

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            _logger.Info("C# HTTP trigger function processed a request.");


            _settings = new CosmosDbSettings(config.GetSection("CosmosDb"));

            //string name = req.Query["name"];

            //string requestBody = new StreamReader(req.Body).ReadToEnd();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;


            try
            {
                using (var client = new DocumentClient(_settings.DatabaseUri, _settings.DatabaseKey, ConnectionPolicy))
                {
                    PrintMOD();
                    var program = new ReadFromCosmos(client);
                    //this._client = client;
                    program.RunAsync().Wait();
                    log.Info("");
                    _logger.Info("DocumentDBBenchmark completed successfully.");
                }
            }
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                _logger.Info($"Samples failed with exception:{e}");
            }
            finally
            {
                _logger.Info("Press any key to exit...");
                Console.ReadLine();
            }


            return new OkResult();

            //return name != null
            //    ? (ActionResult)new OkObjectResult($"Hello, {name}")
            //    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private ReadFromCosmos(DocumentClient client)
        {
            this._client = client;

            // Set Thread size
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);
        }



        private async Task RunAsync()
        {
            // Setup DocDB
            int currentCollectionThroughput = await SetupDb();

            // Setup Parallism - Number of Docs to Insert per task
            _taskCount = SetupParallelism(currentCollectionThroughput, _settings.DegreeOfParallelism);
            _pendingTaskCount = _taskCount;
            var numberOfDocumentsPerTask = int.Parse(_settings.NumberOfDocumentsPerTask);

            var tasks = new List<Task>();
            tasks.Add(this.LogOutputStats(_logger));

            _logger.Info($"Starting in [{_settings.RunMode}] Mode with TaskCount={_taskCount}, each with {numberOfDocumentsPerTask.ToString()} docs");
            _logger.Info("===================================================================");

            //===========================================================================================================
            // MULTI-THREAD - Begining Inserts

            _GlobalWatch = new Stopwatch();
            _GlobalWatch.Start();

            
            int lastItemStart = 0;
            for (var i = 0; i < _taskCount; i++)
            {
                // Setup Indexing
                var start = lastItemStart + 1;
                var end = lastItemStart + numberOfDocumentsPerTask;
                //_logger.Info($"Task[{i}] ==> StartIndex: {start}; EndIndex: {end}");

                // Write or Read
                switch ((_settings.RunMode).ToLower())
                {
                    case "write":
                        _logger.Info($"Creating Task {i} in [READ] Mode...");
                        tasks.Add(this.InsertGeneratedDocument(i, numberOfDocumentsPerTask, start));
                        break;
                    case "read":
                        _logger.Info($"Creating Task {i} in [WRITE] Mode...");
                        tasks.Add(this.ReadRandoDocuments(i, _taskCount, numberOfDocumentsPerTask));
                        break;
                }

                lastItemStart = end;
            }

            await Task.WhenAll(tasks);
            _GlobalWatch.Stop();
            // End of Insert
            //===========================================================================================================

            // Final Cleanup
            if (_settings.ShouldCleanupOnFinish)
            {
                await DropDatabase();
            }
        }

        private async Task InsertGeneratedDocument(int taskId, long numberOfDocumentsToInsert, int startIndex)
        {
            _requestUnitsConsumed[taskId] = 0;

            // iterate the number of Docs to Insert for this Task.
            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                var idString = startIndex + i;

                Dictionary<string, object> newDictionary = GetRandoDictDocument(idString.ToString());

                await AddItemAsync(newDictionary, taskId);

            }

            Interlocked.Decrement(ref this._pendingTaskCount);
        }

        private async Task ReadRandoDocuments(int taskId, int numberOfTasks, int numberOfDocumentsToRead)
        {
            _requestUnitsConsumed[taskId] = 0;
            Random r = new Random();

            //_logger.Info($"Beginning Task {taskId} to read {numberOfDocumentsToRead}");

            // iterate the number of Docs to Insert for this Task.
            for (var i = 0; i < numberOfDocumentsToRead; i++)
            {
                int x = r.Next(0, 1000000);
                await GetItemByIdAsync(x.ToString(), x.ToString(), taskId);
            }

            Interlocked.Decrement(ref this._pendingTaskCount);
        }

        private int SetupParallelism(int currentCollectionThroughput, int DegreeOfParallelism)
        {
            // Setup Parallism
            int taskCount;
            // int degreeOfParallelism = _settings.DegreeOfParallelism;

            if (DegreeOfParallelism == -1)
            {
                // set TaskCount = 10 for each 10k RUs, minimum 1, maximum 250
                taskCount = Math.Max(currentCollectionThroughput / 1000, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = DegreeOfParallelism;
            }
            return taskCount;
        }

        #region CosmosDBOperations

        private async Task<int> SetupDb()
        {
            // Determine if DB/Collection Exists
            _dataCollection = GetCollectionIfExists(_settings.DatabaseName, _settings.CollectionName);
            int currentCollectionThroughput = 0;

            // Remove DB if cleanup flag is set
            if (_settings.ShouldCleanupOnStart || _dataCollection == null)
            {
                // Assert DB does not exist
                _database = GetDatabaseIfExists(_settings.DatabaseName);
                if (_database != null)
                {
                    await DropDatabase();
                }

                // Create Database
                _logger.Warning($"Creating database   ==> {_settings.DatabaseName}");
                _database = await _client.CreateDatabaseAsync(new Database { Id = _settings.DatabaseName });

                // Create Collection
                _logger.Warning($"Creating collection ==> {_settings.CollectionName} with {_settings.CollectionThroughput} RU/s");
                _dataCollection = await this.CreatePartitionedCollectionAsync(_settings.DatabaseName, _settings.CollectionName);

                // Set throughput based upon setting
                currentCollectionThroughput = _settings.CollectionThroughput;
            }
            else
            {
                // Get Existing DB & Collection
                OfferV2 offer = (OfferV2)_client.CreateOfferQuery().Where(o => o.ResourceLink == _dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                currentCollectionThroughput = offer.Content.OfferThroughput;

                if (_settings.CollectionThroughput != currentCollectionThroughput)
                {
                    _logger.Warning($"Configured Throughput in Settings [{_settings.CollectionThroughput}] does not match CosmosDb setting of [{currentCollectionThroughput}]");
                    _logger.Info("Press any key to modify the settings...");
                    Console.ReadLine();

                    // Update the Offer to match the Throughput Number
                    offer = new OfferV2(offer, _settings.CollectionThroughput);
                    await _client.ReplaceOfferAsync(offer);

                    var updatedOffer = (OfferV2)_client.CreateOfferQuery().Where(o => o.ResourceLink == _dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                    currentCollectionThroughput = updatedOffer.Content.OfferThroughput;
                }

                currentCollectionThroughput = offer.Content.OfferThroughput;
                _logger.Info($"Found collection ==> {_settings.CollectionName} with {currentCollectionThroughput} RU/s");
            }
            return currentCollectionThroughput;
        }

        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return _client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        private Database GetDatabaseIfExists(string databaseName)
        {
            return _client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        private async Task<DocumentCollection> CreatePartitionedCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;

            if (_settings.CollectionPartitionKey == null)
            {
                throw new Exception("You must configure a partition key");
            }
            collection.PartitionKey.Paths.Add(_settings.CollectionPartitionKey);


            // Show user cost of running this test
            double estimatedCostPerMonth = 0.06 * _settings.CollectionThroughput;
            double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);
            _logger.Info($"The collection will cost an estimated ${Math.Round(estimatedCostPerHour, 2)} per hour (${Math.Round(estimatedCostPerMonth, 2)} per month)");
            //_logger.Info("Press enter to continue ...");
            //Console.ReadLine();

            return await _client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                collection,
                new RequestOptions { OfferThroughput = _settings.CollectionThroughput });
        }

        private async Task AddItemAsync(Dictionary<string, object> item, int taskId)
        {
            var stopwatch = Stopwatch.StartNew();
            //===================================================================
            try
            {
                var docURL = UriFactory.CreateDocumentCollectionUri(_settings.DatabaseName, _settings.CollectionName);
                var requestOptions = new RequestOptions() { };

                // Do the work!
                ResourceResponse<Document> response = await _client.CreateDocumentAsync(docURL, item, requestOptions);

                //string partition = response.SessionToken.Split(':')[0];
                _requestUnitsConsumed[taskId] += response.RequestCharge;

                Interlocked.Increment(ref this._documentsInserted);
            }
            catch (Exception e)
            {
                if (e is DocumentClientException)
                {
                    DocumentClientException de = (DocumentClientException)e;
                    if (de.StatusCode != HttpStatusCode.Forbidden)
                    {
                        _logger.Info($"Failed to write {JsonConvert.SerializeObject(item)}. Exception was {e}");
                        Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(item), e);
                    }
                    else
                    {
                        Interlocked.Increment(ref this._documentsInserted);
                    }
                }
            }
            //===================================================================

            stopwatch.Stop();
            //_logger.LogInformation("{{ Action: {@Action}, TimeElapsed: {Elapsed:000} ms }}", "AddItemAsync", stopwatch.Elapsed.Milliseconds);
        }

        // Example: Item item = await CosmosDbRepository<Item>.GetItemByIdAsync(id,partitionKeyValue);
        // If id is null, throw ArgumentNullException
        public async Task GetItemByIdAsync(string id, string partitionKeyValue, int taskId)
        {
            try
            {
                if (string.IsNullOrEmpty(id))
                {
                    throw new ArgumentNullException("Cannot search for an id that is empty or null.");
                }

                var requestOptions = new RequestOptions();

                if (partitionKeyValue != null)
                { requestOptions.PartitionKey = new PartitionKey(partitionKeyValue); }

                var docUrl = UriFactory.CreateDocumentUri(_settings.DatabaseName, _settings.CollectionName, id);

                // Do the work!
                ResourceResponse<Document> response = await _client.ReadDocumentAsync(docUrl, requestOptions);

                //string partition = response.SessionToken.Split(':')[0];
                _requestUnitsConsumed[taskId] += response.RequestCharge;
                //_logger.Info($"RU for {id} is {_requestUnitsConsumed[taskId]}");

                Interlocked.Increment(ref this._documentsInserted);


                //return (T)(dynamic)document;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    //return null;
                }
                else
                {
                    throw;
                }
            }
        }

        public async Task DropDatabase()
        {
            _logger.Info("Deleting Database ==> {0}", _settings.DatabaseName);
            await _client.DeleteDatabaseAsync(_database.SelfLink);
        }



        #endregion

        #region HelperFunctions

        private Dictionary<string, object> GetRandoDictDocument(string id)
        {
            Dictionary<string, object> newDictionary = new Dictionary<string, object>();
            //Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);

            newDictionary["id"] = $"{id}";
            newDictionary["objectType"] = "User";
            newDictionary["userId"] = GetRandoString(16);
            newDictionary["userFirstName"] = GetRandoString(64);
            newDictionary["userLastName"] = GetRandoString(64);
            newDictionary["password"] = GetRandoString(8);
            newDictionary["jwt"] = GetRandoString(850);

            return newDictionary;

        }

        public JObject GetRandoDocument(int id)
        {
            var obj = new UserDocument()
            {
                Id = id.ToString(),
                ObjectType = "User",
                UserId = GetRandoString(16),
                UserFirstName = GetRandoString(64),
                UserLastName = GetRandoString(64),
                Password = GetRandoString(8),
                Jwt = GetRandoString(850)
            };

            return JObject.FromObject(obj);
        }

        private string GetRandoString(int msgSize = 8)
        {
            var msgBytes = new byte[msgSize];
            var r = new Random();
            r.NextBytes(msgBytes);
            return Convert.ToBase64String(msgBytes);
        }


        //TraceWriter log
        private async Task LogOutputStats(TraceWriter log)
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            // Setup a rando id so we can compare runs

            var runnerid = new Guid();
            runnerid = Guid.NewGuid();

            var RUList = new List<double>();
            var IOList = new List<double>();

            log.Info($"Pending Task Count: {_pendingTaskCount}");
            while (this._pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;

                //using (StreamWriter sw = new StreamWriter(new FileStream(_settings.DetailsFile, FileMode.Append)))
                //{
                    var elapsed = CalculateTimeElapsed(_GlobalWatch);

                    foreach (int taskId in _requestUnitsConsumed.Keys)
                    {
                        requestUnits += _requestUnitsConsumed[taskId];
                        var taskRUs = Math.Round(_requestUnitsConsumed[taskId] / seconds);
                        var taskRUsPerSecond = Math.Round(taskRUs / seconds);

                        //log.Info($"{runnerid.ToString()},{Environment.ProcessorCount},{_settings.CollectionThroughput},{_settings.DegreeOfParallelism},{_settings.NumberOfDocumentsPerTask},{taskRUs},{taskRUsPerSecond},{elapsed}");
                    }

                    long currentCount = this._documentsInserted;
                    ruPerSecond = (requestUnits / seconds);
                    ruPerMonth = ruPerSecond * 86400 * 30;

                    var io = Math.Round(this._documentsInserted / seconds);
                    var ru = Math.Round(ruPerSecond);
                    var maxru = Math.Round(ruPerMonth / (1000 * 1000 * 1000));

                    RUList.Add(ru);
                    IOList.Add(io);

                    log.Info(
                        $"Read {currentCount} docs @ {io} reads/s, {ru} RU/s ({maxru}B max monthly 1KB reads), PendingTasks: {this._pendingTaskCount}, TimeElapsed: {elapsed} (min:sec.ms)");

                    lastCount = _documentsInserted;
                    lastSeconds = seconds;
                    lastRequestUnits = requestUnits;
                //}
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            var IOFin = Math.Round(this._documentsInserted / watch.Elapsed.TotalSeconds);
            var RUFin = Math.Round(ruPerSecond);
            var RUMax = Math.Round(ruPerMonth / (1000 * 1000 * 1000));

            var RUAvg = Math.Round(CalcSum(RUList) / RUList.Count);
            var IOAvg = Math.Round(CalcSum(IOList) / IOList.Count);
            var elap = CalculateTimeElapsed(_GlobalWatch);

            _logger.Info("");
            _logger.Info("Summary:");
            _logger.Info("--------------------------------------------------------------------- ");
            _logger.Info($" {lastCount} docs @ {IOFin} {_settings.RunMode}/s, {RUFin} RU/s ({RUMax}B max monthly 1KB reads)");
            _logger.Info("");
            _logger.Info($"CollectionThroughput*:        {_settings.CollectionThroughput}");
            _logger.Info($"Document Template*:           {_settings.DocumentTemplateFile}");
            _logger.Info($"Degree of parallelism*:       {_settings.DegreeOfParallelism}");
            _logger.Info($"Number of Docs to Read/Task*: {_settings.NumberOfDocumentsPerTask}");
            _logger.Info($"Number of Cores:              {Environment.ProcessorCount}");
            _logger.Info($"Is 64 Bit Process:            {Environment.Is64BitProcess}");
            _logger.Info("");
            _logger.Info($"  FINAL:    {IOFin} {_settings.RunMode}/s, {RUFin} RU/s");
            _logger.Info($"  AVERAGES: {IOAvg} {_settings.RunMode}/s, {RUAvg} RU/s");
            _logger.Info($"  ELAPSED:  {elap} (min:sec.ms)");
            _logger.Info($"--------------------------------------------------------------------- ");

            // Add Header to Summary file            
            if (!File.Exists(_settings.SummaryFile))
            {
                using (StreamWriter sw = new StreamWriter(new FileStream(_settings.SummaryFile, FileMode.Create)))
                {
                    sw.WriteLine(
                        "RunGuid,cores,collectionThroughput,NumberThreads, NumberofDocsPerTask,FinalReadsPerSec,FinalRUPerSec,AverageReadsPerSec,AverageRUPerSec,ElapsedTime");
                }
            }

            using (StreamWriter sw = new StreamWriter(new FileStream(_settings.SummaryFile, FileMode.Append)))
            {
                sw.WriteLine(
                    $"{runnerid.ToString()},{Environment.ProcessorCount},{_settings.CollectionThroughput},{_settings.DegreeOfParallelism},{_settings.NumberOfDocumentsPerTask},{IOFin},{RUFin},{IOAvg},{RUAvg},{elap}");
            }
        }


        private static double CalcSum(List<double> list)
        {
            double sum = 0;
            foreach (int num in list)
            {
                sum += num;
            }
            return sum;
        }

        private static void PrintMOD()
        {
            _logger.Info($"Summary:");
            _logger.Info($"--------------------------------------------------------------------- ");
            _logger.Info($"Endpoint: {0}", _settings.DatabaseUri.AbsolutePath);
            _logger.Info($"Collection : {_settings.DatabaseName}.{_settings.CollectionName} at {_settings.CollectionThroughput} request units per second");
            _logger.Info($"CollectionThroughput*:        {_settings.CollectionThroughput}");
            _logger.Info($"Document Template*:           {_settings.DocumentTemplateFile}");
            _logger.Info($"Degree of parallelism*:       {_settings.DegreeOfParallelism}");
            _logger.Info($"Number of Docs to Read/Task*: {_settings.NumberOfDocumentsPerTask}");
            _logger.Info($"Number of Cores:              {Environment.ProcessorCount}");
            _logger.Info($"Is 64 Bit Process:            {Environment.Is64BitProcess}");
            _logger.Info($"--------------------------------------------------------------------- ");
            _logger.Info("");
        }
        private static string CalculateTimeElapsed(Stopwatch timer)
        {
            int minutes = (int)timer.Elapsed.TotalMinutes;
            double fsec = 60 * (timer.Elapsed.TotalMinutes - minutes);
            int sec = (int)fsec;
            double ms = Math.Truncate(1000 * (fsec - sec));
            return String.Format("{0}:{1:D2}.{2}", minutes, sec, ms);
        }
        #endregion






    }
}
