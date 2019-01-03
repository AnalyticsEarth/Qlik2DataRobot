using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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
        public async Task<MemoryStream> PredictApiAsync(MemoryStream data, string api_token, string datarobot_key, string username, string host, string deployment_id = null, string project_id = null, string model_id = null)
        {

            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            client.Timeout = new System.TimeSpan(0, 5, 0);

            Uri uri;

            if(deployment_id != null)
            {
                Logger.Trace($"{reqHash} - Deployment Score:{deployment_id}");
                uri = new Uri($"{host}/predApi/v1.0/deployments/{deployment_id}/predictions");
            }
            else
            {
                Logger.Trace($"{reqHash} - Project/Model Score:{project_id} {model_id}");
                uri = new Uri($"{host}/predApi/v1.0/{project_id}/{model_id}/predict");
            }

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, uri);
            message.Headers.Add("datarobot-key", datarobot_key);
            
            message.Headers.Authorization = new AuthenticationHeaderValue("Token", api_token);

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
