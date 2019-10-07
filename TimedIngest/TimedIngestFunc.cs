using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Kusto.Data;
using Kusto.Ingest;
using System.Web;
using System.Globalization;

namespace TimedIngest
{
    public static class TimedIngestFunc
    {
        private static KustoConnectionStringBuilder connection;
        private static IKustoQueuedIngestClient adx;

        private static Object _lock = new Object();
        private static bool _initialized = false;

        private static readonly string CREATIONDATEKEY = "creationTime";

        private static string GetEnvVariable(String name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }

        [FunctionName("TimedIngestFunc")]
        public static async Task RunAsync([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            if(!TryInitialize(log))
            {
                log.LogError("Could not initialize, cancel request");
                throw new Exception("Could not initialize, cancel request");
            }

            //check for blob created
            var eventType = eventGridEvent.EventType;

            if(!eventType.Equals("Microsoft.Storage.BlobCreated"))
            {
                log.LogWarning($"The eventgrid type {eventType} is not supported");
                return;
            }

            var dataOfEventGrid = (JObject)eventGridEvent.Data;

            String urlOfToBeInsertedBlob = HttpUtility.UrlDecode(dataOfEventGrid.GetValue("url").Value<String>());

            log.LogTrace($"URL found: {urlOfToBeInsertedBlob}");

            if (urlOfToBeInsertedBlob.Contains("azuretmpfolder"))
            {
                log.LogInformation($"Nothing to insert because the container {urlOfToBeInsertedBlob} has been blacklisted");
                return;
            }

            DateTime insertDate = ExtractInsertDate(urlOfToBeInsertedBlob, log);

            var minDate = DateTime.ParseExact(GetEnvVariable("MindateString"), GetEnvVariable("MindateStringPattern"), CultureInfo.InvariantCulture);

            if (insertDate < minDate)
            {
                log.LogWarning($"The blob {urlOfToBeInsertedBlob} is too old (configured min: {GetEnvVariable("MindateString")}, actual: {insertDate})");
                return;
            }

            long contentLength = dataOfEventGrid.GetValue("contentLength").Value<long>();
            if(contentLength == 0)
            {
                //Event grid sends an 0 blob size when blob has been created and then a second event when the blob has been finished. 
                // -> the first event has to be ignored
                log.LogWarning($"Found an empty blob {urlOfToBeInsertedBlob} which has been raised by event grid");
                log.LogMetric("EmptyBlobEvent", 1);
                return;
            }


            try
            {
                await TriggerIngestCommand(insertDate, urlOfToBeInsertedBlob, contentLength, log);
            }
            catch(Exception e)
            {
                log.LogError($"Error while trying to insert blob {urlOfToBeInsertedBlob} because of message: {e.Message}");
                throw e;
            }
            finally
            {
                log.LogTrace($"Triggered insertion of blob {urlOfToBeInsertedBlob}");
            }
        }

        private static bool TryInitialize(ILogger log)
        {
            lock (_lock)
            {
                if (!_initialized)
                {
                    string kustoIngestUrl = GetEnvVariable("KustoIngestUrl");
                    string clientId = GetEnvVariable("ClientId");
                    string clientSecret = GetEnvVariable("ClientSecret");
                    string tenantId = GetEnvVariable("TenantId");

                    if (String.IsNullOrWhiteSpace(kustoIngestUrl) 
                        || String.IsNullOrWhiteSpace(clientId)
                        || String.IsNullOrWhiteSpace(clientSecret)
                        || String.IsNullOrWhiteSpace(tenantId))
                    {
                        log.LogError($"Could not initialize the Kusto client because the connection parameters are wrong (url: {kustoIngestUrl} clientId: {clientId}, tenant: {tenantId}");

                        return false;
                    }

                    //Initialize adx
                    connection =
                        new KustoConnectionStringBuilder(GetEnvVariable("KustoIngestUrl")).WithAadApplicationKeyAuthentication(
                            applicationClientId: GetEnvVariable("ClientId"),
                            applicationKey: GetEnvVariable("ClientSecret"),
                            authority: GetEnvVariable("TenantId"));

                    adx = KustoIngestFactory.CreateQueuedIngestClient(connection);

                    if (adx != null)
                    {
                        _initialized = true;
                        log.LogInformation("Function successfully initialized");

                        return true;
                    }
                    else
                    {
                        log.LogWarning("Function not successfully initialized");
                    }
                }
                else
                {
                    return true;
                }
            }

            return false;
        }

        private static Task TriggerIngestCommand(DateTime insertDate, string urlOfToBeInsertedBlob, long rawDatasize, ILogger log)
        {
            var ingestProperties = new KustoIngestionProperties(GetEnvVariable("KustoDatabase"), GetEnvVariable("KustoTableName"));

            switch (GetEnvVariable("KustoMappingType"))
            {
                case "json":
                    ingestProperties.JSONMappingReference = GetEnvVariable("KustoMappingRef");
                    break;
                case "csv":
                    ingestProperties.CSVMappingReference = GetEnvVariable("KustoMappingRef");
                    break;
                case "avro":
                    ingestProperties.AvroMappingReference = GetEnvVariable("KustoMappingRef");
                    break;
                default:
                    ingestProperties.JSONMappingReference = GetEnvVariable("KustoMappingRef");
                    break;
            }

            ingestProperties.AdditionalProperties.Add(CREATIONDATEKEY, insertDate.ToString());
            ingestProperties.AdditionalTags = new List<String> { insertDate.ToString() };

            StorageSourceOptions sourceOptions = new StorageSourceOptions();
            sourceOptions.Size = rawDatasize;
            sourceOptions.DeleteSourceOnSuccess = Boolean.Parse(GetEnvVariable("DeleteAfterInsert"));

            log.LogTrace($"INGEST URL:{urlOfToBeInsertedBlob} SIZE:{rawDatasize} INSERTDATE:{insertDate}");

            return adx.IngestFromStorageAsync(urlOfToBeInsertedBlob + Environment.GetEnvironmentVariable("SasToken"), ingestProperties, sourceOptions);
        }

        private static DateTime ExtractInsertDate(string urlOfToBeInsertedBlob, ILogger log)
        {
            DateTime result = DateTime.MinValue;

            var datePrefixBlob = GetEnvVariable("DatePrefixBlob");

            if (String.IsNullOrWhiteSpace(urlOfToBeInsertedBlob) || !urlOfToBeInsertedBlob.Contains(datePrefixBlob))
            {
                log.LogError($"The url {urlOfToBeInsertedBlob} does not contain the {datePrefixBlob} prefix.");
                return result;
            }

            var dateFormat = GetEnvVariable("DateFormat");

            String dateString = urlOfToBeInsertedBlob.Split(datePrefixBlob)[1].Substring(0, dateFormat.Length);

            if (!DateTime.TryParseExact(dateString, dateFormat, null,
                               DateTimeStyles.AllowWhiteSpaces |
                               DateTimeStyles.AdjustToUniversal,
                               out result))
            {
                log.LogError($"The {dateString} could not be parsed using the {dateFormat}.");
            }

            return result;
        }
    }
}
