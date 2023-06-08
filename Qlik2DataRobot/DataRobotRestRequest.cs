using System.Collections.Generic;
using Newtonsoft.Json;
using NLog;
using System;
// using System.Data;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using static System.ValueTuple;
using System.ComponentModel;

namespace Qlik2DataRobot
{
    class DataRobotRestRequest
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private int reqHash;

        public DataRobotRestRequest(int _reqHash)
        {
            reqHash = _reqHash;
        }


        /// <summary>
        /// Create a datarobot dataset
        /// </summary>
        public async Task<MemoryStream> CreateDatasetAsync(string baseAddress, string token, MemoryStream data, string datasetName, string datasetId = "")
        {

            Logger.Trace($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Trace($"{reqHash} - Configured Client");

            var requestContent = new MultipartFormDataContent("----");

            Logger.Trace($"{reqHash} - Building Request Headers");

            var fileContent = new StreamContent(data);
            // May cause an issue to have filename as a header?
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + datasetName + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");

            Logger.Trace($"{reqHash} - Headers: {fileContent.Headers}");

            // Try adding this back in if the dataset name isn't set
            // requestContent.Add(datasetContent);
            requestContent.Add(fileContent);

            Logger.Trace($"{reqHash} - Headers: {requestContent.Headers}");
            Logger.Trace($"{reqHash} - Finished Building Request");

            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace($"{reqHash} - Finished Setting Up Stream");

            // If we passed in a datasetId, create a new version of that dataset
            // Otherwise create a new dataset from file
            var url = "datasets";
            if (datasetId != "") { url = $"{url}/{datasetId}/versions"; }
            url = $"{url}/fromFile/";

            Logger.Trace($"{reqHash} - Sending to url: {url}");

            try
            {
                HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync(url, requestContent), null);
                Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");
                Logger.Debug($"{reqHash} - Upload Finished - Starting Registration");

                string responseContent = await response.Content.ReadAsStringAsync();
                Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);

                if (responseobj.ContainsKey("catalogId"))
                {
                    Logger.Info($"{reqHash} - Successfully pushed Dataset");
                    dynamic catalogId;
                    responseobj.TryGetValue("catalogId", out catalogId);
                    Logger.Info($"{reqHash} - Dataset ID (): {catalogId}");
                } else
                {
                    Logger.Error($"{reqHash} - Dataset not uploaded properly.");
                }

                if (responseobj.ContainsKey("catalogVersionId"))
                {
                    Logger.Info($"{reqHash} - Successfully pushed Dataset");
                    dynamic catalogVersionId;
                    responseobj.TryGetValue("catalogVersionId", out catalogVersionId);
                    Logger.Info($"{reqHash} - Dataset Version ID (): {catalogVersionId}");
                }

                streamWriter.WriteLine("{\"status\":\"success\",\"response\":" + responseContent + "}");
            }
            catch (Exception e)
            {
                Logger.Warn($"{reqHash} - Create dataset Error");
                Logger.Warn($"{reqHash} - Error: {e.Message}");
                streamWriter.WriteLine("{\"status\":\"error\"}");
            }

            streamWriter.Flush();
            outStream.Position = 0;

            return outStream;
        }

        public async Task<string> SendDataset(string baseAddress, string token, MemoryStream data, string datasetName, StreamWriter streamWriter, string datasetId = "")
        {
            Logger.Info($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Info($"{reqHash} - Configured Client");

            var requestContent = new MultipartFormDataContent("----");

            Logger.Trace($"{reqHash} - Building Request Headers");

            var fileContent = new StreamContent(data);
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + datasetName + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");

            Logger.Trace($"{reqHash} - Headers: {fileContent.Headers}");
            requestContent.Add(fileContent);

            Logger.Trace($"{reqHash} - Headers: {requestContent.Headers}");
            Logger.Trace($"{reqHash} - Finished Building Request");

            Logger.Info($"{reqHash} - Dataset ID: '{datasetId}'");
            var url = "datasets";
            if (String.IsNullOrEmpty(datasetId))
            {
                url = $"{url}/fromFile/";
            } else
            {
                url = $"{url}/{datasetId}/versions/fromFile/";

            }

            Logger.Trace($"{reqHash} - Sending to url: {url}");
            dynamic catalogId;
            dynamic catalogVersionId;


            HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync(url, requestContent), null);
            Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");
            Logger.Debug($"{reqHash} - Upload Finished - Starting Registration");

            string responseContent = await response.Content.ReadAsStringAsync();
            Logger.Trace($"{reqHash} - Response Content: {responseContent}");

            Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);

            if (responseobj.ContainsKey("catalogId"))
            {
                Logger.Info($"{reqHash} - Successfully pushed Dataset");
                responseobj.TryGetValue("catalogId", out catalogId);
                Logger.Info($"{reqHash} - Dataset ID (): {catalogId}");
            } else
            {
                throw new NullReferenceException("catalogId is null.");
            }

            if (responseobj.ContainsKey("catalogVersionId"))
            {
                Logger.Info($"{reqHash} - Successfully pushed Dataset");
                responseobj.TryGetValue("catalogVersionId", out catalogVersionId);
                Logger.Info($"{reqHash} - Dataset Version ID (): {catalogVersionId}");
            }

            return catalogId;
        }

        public async Task<string> CheckDatasetStatus(string baseAddress, string token, string datasetId, StreamWriter streamWriter)
        {
            Logger.Trace($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Trace($"{reqHash} - Configured Client");

            var requestContent = new MultipartFormDataContent("----");

            Logger.Trace($"{reqHash} - Building Request Headers");
            Logger.Trace($"{reqHash} - Headers: {requestContent.Headers}");
            Logger.Trace($"{reqHash} - Finished Building Request");


            string url = $"datasets/{datasetId}/";

            Logger.Trace($"{reqHash} - Sending to url: {url}");
            dynamic processingState = "PENDING";

            while (processingState == "RUNNING" || processingState == "PENDING")
            {
                Thread.Sleep(5000);
                try
                {
                    HttpResponseMessage response = await checkRedirectAuth(client, await client.GetAsync(url), null);
                    Logger.Trace($"{reqHash} - Checking Registration");
                    Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");

                    string responseContent = await response.Content.ReadAsStringAsync();
                    Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);

                    if (responseobj.ContainsKey("processingState"))
                    {
                        responseobj.TryGetValue("processingState", out processingState);
                        Logger.Info($"{reqHash} - Dataset State: {processingState}");
                    }

                }
                catch (Exception e)
                {
                    Logger.Warn($"{reqHash} - Create dataset Error");
                    Logger.Warn($"{reqHash} - Error: {e.Message}");
                    processingState = "ERROR";
                    return processingState;
                }
            }


            return processingState;
        }

        public class actualsOptions
        {
            public string datasetId { get; set; }
            public string associationIdColumn { get; set; }
            public string actualValueColumn { get; set; }
        }

        public async Task<string> SubmitActuals(string baseAddress, string token, string deploymentId, string datasetId, string associationIdColumn, string actualValueColumn)
        {
            Logger.Trace($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Trace($"{reqHash} - Configured Client");

            actualsOptions options = new actualsOptions
            {
                datasetId = datasetId,
                associationIdColumn = associationIdColumn,
                actualValueColumn = actualValueColumn
            };
            Logger.Trace($"{reqHash} - Building form");

            string json = JsonConvert.SerializeObject(options);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            string url = $"deployments/{deploymentId}/actuals/fromDataset/";

            Console.Write(url);
            Logger.Trace($"{reqHash} - Sending to url: {url}");

            try
            {
                HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync(url, content), null);
                Logger.Trace($"{reqHash} - Submitting Actuals");
                Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");

                string responseContent = await response.Content.ReadAsStringAsync();
            }
            catch (Exception e)
            {
                Logger.Warn($"{reqHash} - Create dataset Error");
                Logger.Warn($"{reqHash} - Error: {e.Message}");
                return "error";
            }

            Logger.Trace($"{reqHash} - : {url}");


            return "success";
        }

        public class intakeOptions
        {
            public string type { get; set; }
            public string datasetId {get; set; }
        }

        public class batchOptions
        {
            public string deploymentId { get; set; }
            public bool skipDriftTracking { get; set; }
            public bool predictionWarningEnabled { get; set; }
            public string chunkSize { get; set; }
            public bool includePredictionStatus { get; set; }
            public intakeOptions intakeSettings { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public int? maxExplanations { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public double? thresholdHigh { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public double? thresholdLow { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public string passthroughColumnsSet { get; set; }
        }

        class redirectLinks {
            public string self {get; set;}
            public string download {get; set;}
        }

        public async Task<string> SubmitBatch(
            string baseAddress,
            string token,
            string deploymentId,
            string datasetId,
            string passthroughColumnsSet,
            bool skipDriftTracking = false,
            bool predictionWarningEnabled = false,
            string chunkSize = "auto",
            bool includePredictionStatus = true,
            int maxCodes = 0,
            double thresholdHigh = 0,
            double thresholdLow = 0,
            bool explain = false
            )
        {
            Logger.Trace($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);

            Logger.Trace($"{reqHash} - Configuring batch options");
            batchOptions options;
            if (explain == false)
            {
                Logger.Info($"{reqHash} - Configuring batch options without explain");
                options = new batchOptions
                {
                    deploymentId = deploymentId,
                    skipDriftTracking = skipDriftTracking,
                    predictionWarningEnabled = predictionWarningEnabled,
                    chunkSize = chunkSize,
                    includePredictionStatus = includePredictionStatus,
                    intakeSettings = new intakeOptions
                    {
                        type = "dataset",
                        datasetId = datasetId
                    },
                    passthroughColumnsSet = passthroughColumnsSet
                };
                
            } else
            {
                options = new batchOptions
                {
                    deploymentId = deploymentId,
                    skipDriftTracking = skipDriftTracking,
                    predictionWarningEnabled = predictionWarningEnabled,
                    chunkSize = chunkSize,
                    includePredictionStatus = includePredictionStatus,
                    intakeSettings = new intakeOptions
                    {
                        type = "dataset",
                        datasetId = datasetId
                    },
                    maxExplanations = maxCodes,
                    thresholdHigh = thresholdHigh,
                    thresholdLow = thresholdLow,
                    passthroughColumnsSet = passthroughColumnsSet
                };
            }
            Logger.Info($"{reqHash} - Made options");
            Logger.Info($"{reqHash} - Serializing options");
            string json = JsonConvert.SerializeObject(options);
            Console.WriteLine($"Sending BatchPrediction requestion with the following config: {json}");
            var httpContent = new StringContent(json, Encoding.UTF8, "application/json");
            Logger.Info($"{reqHash} - Built httpContent");
            string uri = $"batchPredictions/";
            Logger.Info($"{reqHash} - Sending to {uri}");
            HttpResponseMessage response = await client.PostAsync(uri, httpContent);
            
            string responseContent = await response.Content.ReadAsStringAsync();
            Logger.Info($"{reqHash} - resp: {JsonConvert.SerializeObject(responseContent)}");
            Logger.Trace($"{reqHash} - Parsing response: {responseContent}");
            Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);
            dynamic status;
            dynamic jobId;
            responseobj.TryGetValue("status", out status);
            responseobj.TryGetValue("id", out jobId);

            string jobIdStr = Convert.ToString(jobId);

            Logger.Trace($"{reqHash} - status: {status}");
            Logger.Trace($"{reqHash} - jobId: {jobId}");

            int counter=0;
            while (counter < 60 && !(status.Contains("COMPLETE") || status.Contains("ERROR") || status.Contains("ABORT"))) {
                counter = counter + 1;
                Logger.Trace($"{reqHash} - status: {status}");
                Thread.Sleep(5000);
                Logger.Trace($"{reqHash} - trying again");
                if (jobIdStr != "" && (status == "INITIALIZING" || status == "RUNNING")) {
                    Logger.Trace($"{reqHash} - Getting job {jobIdStr} details");
                    Uri statusUrl = new Uri($"{baseAddress}batchPredictions/{jobIdStr}");
                    Logger.Trace($"{reqHash} - status url: {statusUrl}");
                    response = await checkRedirectAuth(client, await client.GetAsync(statusUrl), statusUrl);
                    responseContent = await response.Content.ReadAsStringAsync();
                    responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);
                    responseobj.TryGetValue("status", out status);
                } else {
                }
            }

            if (!status.Contains("COMPLETE")) {
                throw new ApplicationException("Prediction Job didn't finish successfully.");
            }


            Uri downloadUrl = new Uri($"{baseAddress}batchPredictions/{jobIdStr}/download");
            Logger.Trace($"{reqHash} - download url: {$"{baseAddress}batchPredictions/{jobIdStr}/download"}");
            response = await checkRedirectAuth(client, await client.GetAsync(downloadUrl), downloadUrl);
            responseContent = await response.Content.ReadAsStringAsync();

            return responseContent;
        }

        /// <summary>
        /// Send actual values to a deployment
        /// </summary>
        public async Task<MemoryStream> ScoreBatchAsync(string host,
            string token,
            MemoryStream data,
            string deploymentId,
            string keyField,
            string passthroughColumnsSet,
            string dataset_name = "Batch",
            string datasetId = "",
            int maxCodes = 0,
            double thresholdHigh = 0,
            double thresholdLow = 0,
            bool explain = false)
        {

            Logger.Trace($"{reqHash} - Starting Send Batch");
            Logger.Info($"{reqHash} - {explain}");
            Logger.Info($"{reqHash} - {maxCodes}");
            Logger.Info($"{reqHash} - {thresholdHigh}");
            Logger.Info($"{reqHash} - {thresholdLow}");
            Logger.Info($"{reqHash} - {passthroughColumnsSet}");
            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);
            Logger.Info($"{reqHash} - Got mem stream and writer");
            Logger.Trace($"{reqHash} - Sending Dataset");
            string catalogId = await SendDataset(host, token, data, dataset_name, streamWriter, datasetId);

            Logger.Trace($"{reqHash} - Checking Dataset Status");
            string status = await CheckDatasetStatus(host, token, catalogId, streamWriter);

            if (status != "COMPLETED")
            {
                Logger.Error($"Dataset failed to register: {status}");
                streamWriter.WriteLine("{\"status\":\"error\",\"response\":{\"id\":\"" + catalogId + "\"}}");
                streamWriter.Flush();
                outStream.Position = 0;
                return outStream;
            }

            Logger.Trace($"{reqHash} - Starting Batch Scoring");
            string csv = await SubmitBatch(host, token, deploymentId, catalogId,
                maxCodes: maxCodes, 
                thresholdHigh: thresholdHigh, 
                thresholdLow: thresholdLow, 
                explain: explain,
                passthroughColumnsSet: passthroughColumnsSet);
            Logger.Trace($"{reqHash} - Finished Batch Scoring");


            ResponseSpecification resp = new ResponseSpecification
            {
                status = "success",
                csvdata = csv
            };
            
            streamWriter.WriteLine(JsonConvert.SerializeObject(resp));
            streamWriter.Flush();
            outStream.Position = 0;
            return outStream;
        }

        
        /// <summary>
        /// Send actual values to a deployment
        /// </summary>
        public async Task<MemoryStream> SendActualsAsync(string host, string token, MemoryStream data, string deploymentId, string keyField, string dataset_name = "Actuals", string datasetId = "", string actualValueColumn = "target", string associationIdColumn = "id")
        {

            Logger.Trace($"{reqHash} - Starting Send Actuals");
            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace($"{reqHash} - Sending Dataset");
            string catalogId = await SendDataset(host, token, data, dataset_name, streamWriter, datasetId);

            Logger.Trace($"{reqHash} - Checking Dataset Status");
            string status = await CheckDatasetStatus(host, token, catalogId, streamWriter);

            if (status != "COMPLETED") {
                Logger.Error($"Dataset failed to register: {status}");
                streamWriter.WriteLine("{\"status\":\"error\",\"response\":{\"id\":\"" + catalogId + "\"}}");
                streamWriter.Flush();
                outStream.Position = 0;
                return outStream;
            }

            Logger.Trace($"{reqHash} - Sending Actuals");
            string actualStatus = await SubmitActuals(host, token, deploymentId, catalogId, associationIdColumn, actualValueColumn);
            Logger.Trace($"{reqHash} - Send Actuals - {actualStatus}");

            streamWriter.WriteLine("{\"status\":\"" + actualStatus + "\",\"response\":{\"id\":\"" + catalogId + "\"}}");
            streamWriter.Flush();
            outStream.Position = 0;
            return outStream;
        }

        /// <summary>
        /// Send actual values to a deployment
        /// </summary>
        public async Task<MemoryStream> CreateDatasetAsync2(string baseAddress, string token, MemoryStream data, string datasetName, string datasetId = "")
        {
            Logger.Trace($"{reqHash} - Starting Send dataset method 2");
            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace($"{reqHash} - Sending Dataset");
            string catalogId = await SendDataset(baseAddress, token, data, datasetName, streamWriter);

            Logger.Trace($"{reqHash} - Checking Dataset Status");
            string status = await CheckDatasetStatus(baseAddress, token, catalogId, streamWriter);
            if (status != "COMPLETED") {
                Logger.Error($"Dataset failed to register: {status}");
                streamWriter.WriteLine("{\"status\":\"error\",\"response\":{\"id\":\"err\"}}");
                streamWriter.Flush();
                outStream.Position = 0;
                return outStream;
            }

            streamWriter.WriteLine("{\"status\":\"success\",\"response\":{\"id\":\"" + catalogId + "\"}}");
            streamWriter.Flush();
            outStream.Position = 0;
            return outStream;
        }


        /// <summary>
        /// Create a datarobot project
        /// </summary>
        public async Task<MemoryStream> CreateProjectsAsync(string baseAddress, string token, MemoryStream data, string projectName, string filename)
        {

            Logger.Trace($"{reqHash} - Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Trace($"{reqHash} - Configured Client");
            var requestContent = new MultipartFormDataContent("----");
            
            var projectContent = new StringContent(projectName);
            projectContent.Headers.Add("Content-Disposition", "form-data; name=\"projectName\"");

            var fileContent = new StreamContent(data);
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + filename + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");

            requestContent.Add(projectContent);
            requestContent.Add(fileContent);

            Logger.Trace($"{reqHash} - Finished Building Request");

            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace($"{reqHash} - Finished Setting Up Stream");

            try
            {
                HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync("projects/", requestContent), null);
                Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");
                Logger.Trace($"{reqHash} - Location: {response.Headers.Location}");
                var status = "";
                Logger.Debug($"{reqHash} - Upload Finished - DataRobot Analysis Starting");
                for (int i = 0; i < 500; i++)
                {
                    status = await CheckStatusAsync(client, response.Headers.Location);
                    Logger.Trace($"{reqHash} - Status: {status}");
                    Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(status);
                    if (responseobj.ContainsKey("id"))
                    {
                        break;
                    }
                    Thread.Sleep(1000);
                }
                streamWriter.WriteLine("{\"status\":\"success\",\"response\":" + status +"}");
            }
            catch (Exception e)
            {
                Logger.Warn($"{reqHash} - Create Project Error");
                Logger.Warn($"{reqHash} - Error: {e.Message}");
                streamWriter.WriteLine("{\"status\":\"error\"}");
            }

            streamWriter.Flush();
            outStream.Position = 0;

            return outStream;
        }

        /// <summary>
        /// Check status of the DataRobot project validation checks
        /// </summary>
        public async Task<String> CheckStatusAsync(HttpClient client, Uri location)
        {

            HttpResponseMessage response = await checkRedirectAuth(client, await client.GetAsync(location), location);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync();
        }

        /// <summary>
        /// Process request against the prediction API
        /// </summary>
        public async Task<MemoryStream> PredictApiAsync(MemoryStream data, string api_token, string datarobot_key, string host, string deployment_id = null, string project_id = null, string model_id = null, string keyField = null, bool explain = false, int maxCodes = 0, double thresholdHigh = 0, double thresholdLow = 0)
        {

            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            client.Timeout = new System.TimeSpan(0, 5, 0);

            Uri uri;

            if(deployment_id != null)
            {
                Logger.Trace($"{reqHash} - Deployment Score:{deployment_id}");
                string param = "";
                if (keyField != null) param += $"passthroughColumns={keyField}&";
                if (explain == true)
                {
                    if (maxCodes != 0) param += $"maxCodes={maxCodes}&";
                    if (thresholdHigh != 0) param += $"thresholdHigh={thresholdHigh}&";
                    if (thresholdLow != 0) param += $"thresholdLow={thresholdLow}";
                    if (param != "") param = "?" + param;
                    uri = new Uri($"{host}/predApi/v1.0/deployments/{deployment_id}/predictionExplanations{param}");
                    
                    Logger.Trace($"{reqHash} - URL:{uri}");

                }
                else
                {
                    if (param != "") param = "?" + param;
                    uri = new Uri($"{host}/predApi/v1.0/deployments/{deployment_id}/predictions{param}");
                    Logger.Trace($"{reqHash} - URL:{uri}");
                }
                
            }
            else
            {
                Logger.Trace($"{reqHash} - Project/Model Score:{project_id} {model_id}");
                uri = new Uri($"{host}/predApi/v1.0/{project_id}/{model_id}/predict");
            }

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, uri);
            if(datarobot_key != null)
            {
                message.Headers.Add("datarobot-key", datarobot_key);
            }
            
            
            message.Headers.Authorization = new AuthenticationHeaderValue("Token", api_token);

            var verheader = $"QlikConnector/{Assembly.GetExecutingAssembly().GetName().Version.Major}.{Assembly.GetExecutingAssembly().GetName().Version.Minor}.{Assembly.GetExecutingAssembly().GetName().Version.Revision}";
            Logger.Trace($"{reqHash} - Request Version Header: {verheader}");
            message.Headers.Add("X-DataRobot-API-Consumer", verheader);

            message.Content = new StreamContent(data);
            message.Content.Headers.Add("Content-Type", "text/csv; charset=UTF-8");

            HttpResponseMessage response = client.SendAsync(message).Result;

            Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");

            Stream rdata = await response.Content.ReadAsStreamAsync();

            var mdata = new MemoryStream();
            rdata.Position = 0;
            rdata.CopyTo(mdata);

            return mdata;
        }

        /// <summary>
        /// Process request against the Time Series Prediction API
        /// </summary>
        public async Task<MemoryStream> TimeSeriesAsync(MemoryStream data, string api_token, string datarobot_key, string host, string deployment_id = null, string project_id = null, string model_id = null, string forecast_point = null)
        {

            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            client.Timeout = new System.TimeSpan(0, 5, 0);

            Uri uri;

            if (deployment_id != null)
            {
                Logger.Trace($"{reqHash} - Deployment Score:{deployment_id}");
                if (forecast_point != null)
                {
                    string param = $"?forecastPoint={forecast_point}";
                    uri = new Uri($"{host}/predApi/v1.0/deployments/{deployment_id}/timeSeriesPredictions{param}");

                    Logger.Trace($"{reqHash} - URL:{uri}");

                }
                else
                {
                    uri = new Uri($"{host}/predApi/v1.0/deployments/{deployment_id}/timeSeriesPredictions");
                }

            }
            else
            {
                Logger.Trace($"{reqHash} - Project/Model Score:{project_id} {model_id}");
                uri = new Uri($"{host}/predApi/v1.0/{project_id}/{model_id}/timeSeriesPredictions");
            }

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, uri);
            if (datarobot_key != null)
            {
                message.Headers.Add("datarobot-key", datarobot_key);
            }


            message.Headers.Authorization = new AuthenticationHeaderValue("Token", api_token);

            var verheader = $"QlikConnector/{Assembly.GetExecutingAssembly().GetName().Version.Major}.{Assembly.GetExecutingAssembly().GetName().Version.Minor}.{Assembly.GetExecutingAssembly().GetName().Version.Revision}";
            Logger.Trace($"{reqHash} - Request Version Header: {verheader}");
            message.Headers.Add("X-DataRobot-API-Consumer", verheader);

            message.Content = new StreamContent(data);
            message.Content.Headers.Add("Content-Type", "text/csv; charset=UTF-8");

            HttpResponseMessage response = client.SendAsync(message).Result;

            Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");

            Stream rdata = await response.Content.ReadAsStreamAsync();

            var mdata = new MemoryStream();
            rdata.Position = 0;
            rdata.CopyTo(mdata);

            return mdata;
        }

        /// <summary>
        /// Configure the HTTPClient
        /// </summary>
        public bool ConfigureAsync(HttpClient client, string baseAddress, string token)
        {

            try
            {
                Logger.Trace($"{reqHash} - Base Address: {baseAddress}");
                if (token is string)
                {
                    Logger.Trace(token);
                }

                client.Timeout = new System.TimeSpan(0, 5, 0);
                Logger.Debug($"{reqHash} - Timeout: {client.Timeout}");

                client.BaseAddress = new Uri(baseAddress);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", token);

                var verheader = $"QlikConnector/{Assembly.GetExecutingAssembly().GetName().Version.Major}.{Assembly.GetExecutingAssembly().GetName().Version.Minor}.{Assembly.GetExecutingAssembly().GetName().Version.Revision}";
                Logger.Trace($"{reqHash} - Request Version Header: {verheader}");
                client.DefaultRequestHeaders.Add("X-DataRobot-API-Consumer", verheader);

            }
            catch (Exception e)
            {
                Logger.Warn($"{reqHash} - Request Configuration Error: {e.Message}");
            }

            return true;

        }

        /// <summary>
        /// Checks for redirection and reconfigure authorization headers
        /// </summary>
        public async Task<HttpResponseMessage> checkRedirectAuth(HttpClient client, HttpResponseMessage response, Uri location)
        {
            var finalresponse = response;
            if (response.StatusCode == HttpStatusCode.Unauthorized)
            {
                // Authorization header has been set, but the server reports that it is missing.
                // It was probably stripped out due to a redirect.

                var finalRequestUri = response.RequestMessage.RequestUri; // contains the final location after following the redirect.

                if (finalRequestUri != location || location == null) // detect that a redirect actually did occur.
                {
                    // If this is public facing, add tests here to determine if Url should be trusted
                    finalresponse = await client.GetAsync(finalRequestUri);
                }
            }

            return finalresponse;
        }

       

    }

   
}
