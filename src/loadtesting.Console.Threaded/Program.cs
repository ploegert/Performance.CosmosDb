// Much of the code was derived from: 
//      https://github.com/Azure/azure-documentdb-dotnet/tree/master/samples/documentdb-benchmark
//      https://docs.microsoft.com/en-us/azure/cosmos-db/performance-testing
//
// the rest was evoved by justin.j.ploegert@jci.com on ~2/15/2018

using System.Text;

namespace loadtesting.Console.Threaded
{
    using System;
    using System.Collections.Generic;
    using loadtesting.Console.Threaded.Models;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public sealed class Program
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

        private static ILogger _logger;
        private static CosmosDbSettings _settings;
        //private static readonly string InstanceId = Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;
        private const int MinThreadPoolSize = 100;

        private Stopwatch _GlobalWatch;
        private int _taskCount;
        private int _pendingTaskCount;
        private long _documentsInserted;
        private ConcurrentDictionary<int, double> _requestUnitsConsumed = new ConcurrentDictionary<int, double>();

        private readonly DocumentClient _client;
        private Database _database;
        private DocumentCollection _dataCollection;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client)
        {
            this._client = client;
            
            // Set Thread size
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);
        }

        /// <summary>
        /// Main method for the sample.
        /// </summary>
        /// <param name="args">command line arguments.</param>
        public static void Main(string[] args)
        {

            // create service collection
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddOptions();

            // build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false)
                .AddCommandLine(args)
                .Build();
            _settings = new CosmosDbSettings(configuration.GetSection("CosmosDb"));

            // Build Up logger
            var loggerFactory = new LoggerFactory()
                .AddConsole()
                .AddDebug();
            serviceCollection.AddSingleton<ILoggerFactory>(loggerFactory);
            _logger = loggerFactory.CreateLogger("ThreadLoader");

            
            try
            {
                using (var client = new DocumentClient(_settings.DatabaseUri,_settings.DatabaseKey,ConnectionPolicy))
                {
                    PrintMOD();
                    var program = new Program(client);
                    program.RunAsync().Wait();
                    Console.WriteLine("");
                    Console.WriteLine("DocumentDBBenchmark completed successfully.");
                }
            }
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine("Samples failed with exception:{0}", e);
            }
            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadLine();
            }
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
            tasks.Add(this.LogOutputStats());

            Console.WriteLine("Starting in [{0}] Mode with TaskCount={1}, each with {2} docs", _settings.RunMode, _taskCount, numberOfDocumentsPerTask.ToString());
            Console.WriteLine("===================================================================");

            //===========================================================================================================
            // MULTI-THREAD - Begining Inserts

            _GlobalWatch = new Stopwatch();
            _GlobalWatch.Start();

            Console.WriteLine($"Creating Tasks in [{_settings.RunMode}] Mode...");
            int lastItemStart = 0;
            for (var i = 0; i < _taskCount; i++)
            {
                // Setup Indexing
                var start = lastItemStart + 1;
                var end = lastItemStart + numberOfDocumentsPerTask;
                //Console.WriteLine($"Task[{i}] ==> StartIndex: {start}; EndIndex: {end}");

                // Write or Read
                switch ((_settings.RunMode).ToLower())
                {
                    case "write":
                        tasks.Add(this.InsertGeneratedDocument(i, numberOfDocumentsPerTask, start));
                        break;
                    case "read":
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
                _logger.LogWarning("Creating database   ==> {0}", _settings.DatabaseName);
                _database = await _client.CreateDatabaseAsync(new Database { Id = _settings.DatabaseName });

                // Create Collection
                _logger.LogWarning("Creating collection ==> {0} with {1} RU/s", _settings.CollectionName, _settings.CollectionThroughput);
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
                    _logger.LogWarning($"Configured Throughput in Settings [{_settings.CollectionThroughput}] does not match CosmosDb setting of [{currentCollectionThroughput}]");
                    Console.WriteLine("Press any key to modify the settings...");
                    Console.ReadLine();

                    // Update the Offer to match the Throughput Number
                    offer = new OfferV2(offer, _settings.CollectionThroughput);
                    await _client.ReplaceOfferAsync(offer);

                    var updatedOffer = (OfferV2)_client.CreateOfferQuery().Where(o => o.ResourceLink == _dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                    currentCollectionThroughput = updatedOffer.Content.OfferThroughput;
                }

                currentCollectionThroughput = offer.Content.OfferThroughput;
                Console.WriteLine("Found collection ==> {0} with {1} RU/s", _settings.CollectionName, currentCollectionThroughput);
            }
            return currentCollectionThroughput;
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested collection</returns>
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return _client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        private Database GetDatabaseIfExists(string databaseName)
        {
            return _client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
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
            Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", Math.Round(estimatedCostPerHour, 2), Math.Round(estimatedCostPerMonth, 2));
            //Console.WriteLine("Press enter to continue ...");
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
                        Console.WriteLine("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(item), e);
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
            Console.WriteLine("Deleting Database ==> {0}", _settings.DatabaseName);
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



        private async Task LogOutputStats()
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

            while (this._pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;

                using (StreamWriter sw = new StreamWriter(new FileStream(_settings.DetailsFile, FileMode.Append)))
                {
                    var elapsed = CalculateTimeElapsed(_GlobalWatch);

                    foreach (int taskId in _requestUnitsConsumed.Keys)
                    {
                        requestUnits += _requestUnitsConsumed[taskId];
                        var taskRUs = Math.Round(_requestUnitsConsumed[taskId] / seconds);
                        var taskRUsPerSecond = Math.Round(taskRUs / seconds);

                        sw.WriteLine(
                            $"{runnerid.ToString()},{Environment.ProcessorCount},{_settings.CollectionThroughput},{_settings.DegreeOfParallelism},{_settings.NumberOfDocumentsPerTask},{taskRUs},{taskRUsPerSecond},{elapsed}");
                    }

                    long currentCount = this._documentsInserted;
                    ruPerSecond = (requestUnits / seconds);
                    ruPerMonth = ruPerSecond * 86400 * 30;

                    var io = Math.Round(this._documentsInserted / seconds);
                    var ru = Math.Round(ruPerSecond);
                    var maxru = Math.Round(ruPerMonth / (1000 * 1000 * 1000));

                    RUList.Add(ru);
                    IOList.Add(io);

                    Console.WriteLine(
                        "Read {0} docs @ {1} reads/s, {2} RU/s ({3}B max monthly 1KB reads), PendingTasks: {4}, TimeElapsed: {5} (min:sec.ms)",
                        currentCount, io, ru, maxru, this._pendingTaskCount, elapsed);

                    lastCount = _documentsInserted;
                    lastSeconds = seconds;
                    lastRequestUnits = requestUnits;
                }
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

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(" {0} docs @ {1} {2}/s, {3} RU/s ({4}B max monthly 1KB reads)", lastCount, IOFin,
                _settings.RunMode, RUFin, RUMax);
            Console.WriteLine();
            Console.WriteLine("CollectionThroughput*:        {0}", _settings.CollectionThroughput);
            Console.WriteLine("Document Template*:           {0}", _settings.DocumentTemplateFile);
            Console.WriteLine("Degree of parallelism*:       {0}", _settings.DegreeOfParallelism);
            Console.WriteLine("Number of Docs to Read/Task*: {0}", _settings.NumberOfDocumentsPerTask);
            Console.WriteLine("Number of Cores:              {0}", Environment.ProcessorCount);
            Console.WriteLine("Is 64 Bit Process:            {0}", Environment.Is64BitProcess);
            Console.WriteLine();
            Console.WriteLine("  FINAL:    {0} {1}/s, {2} RU/s", IOFin, _settings.RunMode, RUFin);
            Console.WriteLine("  AVERAGES: {0} {1}/s, {2} RU/s", IOAvg, _settings.RunMode, RUAvg);
            Console.WriteLine("  ELAPSED:  {0} (min:sec.ms)", elap);
            Console.WriteLine("--------------------------------------------------------------------- ");
           
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
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Endpoint: {0}", _settings.DatabaseUri.AbsolutePath);
            Console.WriteLine("Collection : {0}.{1} at {2} request units per second", _settings.DatabaseName, _settings.CollectionName, _settings.CollectionThroughput);
            Console.WriteLine("CollectionThroughput*:        {0}", _settings.CollectionThroughput);
            Console.WriteLine("Document Template*:           {0}", _settings.DocumentTemplateFile);
            Console.WriteLine("Degree of parallelism*:       {0}", _settings.DegreeOfParallelism);
            Console.WriteLine("Number of Docs to Read/Task*: {0}", _settings.NumberOfDocumentsPerTask);
            Console.WriteLine("Number of Cores:              {0}", Environment.ProcessorCount);
            Console.WriteLine("Is 64 Bit Process:            {0}", Environment.Is64BitProcess);
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine();
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
