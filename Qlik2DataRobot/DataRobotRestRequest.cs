using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;


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
            var datasetRequestContent = new MultipartFormDataContent("----");
            var emptyDatasetContent = new StringContent(datasetName);
            emptyDatasetContent.Headers.Add("Content-Type", "Disposition");

            var requestContent = new MultipartFormDataContent("----");
            
            Logger.Trace($"{reqHash} - Building Request Headers");
            var datasetContent = new StringContent(datasetName);
            datasetContent.Headers.Add("Content-Disposition", "form-data; name=\"datasetName\"");

            var fileContent = new StreamContent(data);
            // May cause an issue to have filename as a header?
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + datasetName + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");


            // Try adding this back in if the dataset name isn't set
            // requestContent.Add(datasetContent);
            requestContent.Add(fileContent);

            Logger.Trace($"{reqHash} - Finished Building Request");

            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace($"{reqHash} - Finished Setting Up Stream");

            // If we passed in a datasetId, create a new version of that dataset
            // Otherwise create a new dataset from file
            var url = "datasets";
            if (datasetId != "") { url = $"{url}/{datasetId}/version"; } 
            url = $"{url}/fromFile/";

            try
            {
                HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync(url, requestContent), null);
                Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");
                Logger.Debug($"{reqHash} - Upload Finished - Starting Registration");

                string responseContent = await response.Content.ReadAsStringAsync();
                Dictionary<string, dynamic> responseobj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseContent);

                if (responseobj.ContainsKey("catalogVersionId"))
                {
                    Logger.Info($"{reqHash} - Successfully pushed Dataset");

                    string catalogVersionId = responseobj.GetValueOrDefault("catalogVersionId");
                    Logger.Info($"{reqHash} - Dataset Version ID (): {catalogVersionId}");


                    string catalogId = responseobj.GetValueOrDefault("catalogId");
                    Logger.Info($"{reqHash} - Dataset ID (): {catalogId}");
                }

                streamWriter.WriteLine("{\"status\":\"success\",\"response\":" + responseContent +"}");
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
