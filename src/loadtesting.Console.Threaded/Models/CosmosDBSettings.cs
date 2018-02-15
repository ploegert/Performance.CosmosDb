namespace loadtesting.Console.Threaded.Models
{
    using System;
    using Microsoft.Extensions.Configuration;

    public class CosmosDbSettings
    {
        public CosmosDbSettings(IConfiguration configuration)
        {
            try
            {
                DatabaseName = configuration.GetSection("DatabaseName").Value;
                CollectionName = configuration.GetSection("CollectionName").Value;
                DatabaseUri = new Uri($"https://{configuration.GetSection("Account").Value}.documents.azure.com:443/");
                DatabaseKey = configuration.GetSection("Key").Value;

                CollectionThroughput = int.Parse(configuration.GetSection("CollectionThroughput").Value);
                ShouldCleanupOnStart = bool.Parse(configuration.GetSection("ShouldCleanupOnStart").Value);
                ShouldCleanupOnFinish = bool.Parse(configuration.GetSection("ShouldCleanupOnFinish").Value);
                DegreeOfParallelism = int.Parse(configuration.GetSection("DegreeOfParallelism").Value);
                //NumberOfDocumentsToInsert = configuration.GetSection("NumberOfDocumentsToInsert").Value;
                NumberOfDocumentsPerTask = configuration.GetSection("NumberOfDocumentsPerTask").Value;
                DocumentTemplateFile = configuration.GetSection("DocumentTemplateFile").Value;
                CollectionPartitionKey = configuration.GetSection("CollectionPartitionKey").Value;
                RunMode = configuration.GetSection("RunMode").Value ?? "Read";
                SummaryFile = configuration.GetSection("SummaryFile").Value;
                DetailsFile = configuration.GetSection("DetailsFile").Value;
            }
            catch
            {
                throw new MissingFieldException("IConfiguration missing a valid Azure Cosmos DB field appsettings.json");
            }
        }
        

        public string DatabaseName { get; private set; }
        public string CollectionName { get; private set; }
        public Uri DatabaseUri { get; private set; }
        public string DatabaseKey { get; private set; }

        public int CollectionThroughput { get; private set; }

        public bool ShouldCleanupOnStart { get; private set; }
        public bool ShouldCleanupOnFinish { get; private set; }

        public int DegreeOfParallelism { get; private set; }
        //public string NumberOfDocumentsToInsert { get; private set; }
        public string NumberOfDocumentsPerTask { get; private set; }
        //public int numberOfTasks { get; private set; }


        public string CollectionPartitionKey { get; private set; }
        public string DocumentTemplateFile { get; private set; }

        public string RunMode { get; private set; }
        public string SummaryFile { get; private set; }
        public string DetailsFile { get; private set; }

    }
}
