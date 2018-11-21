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

        //HttpClient client;  

        /* public async Task<string> ListProjectsAsync()
         {
             HttpResponseMessage response = await client.GetAsync("projects");
             response.EnsureSuccessStatusCode();

             // return URI of the created resource.
             return await response.Content.ReadAsStringAsync();
         }*/

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
                //var result = await response.Content.ReadAsStringAsync();
                var status = "";

                //TODO: Break this loop and exit on finished project or error
                for (int i = 0; i < 500; i++)
                {
                    //ConfigureAsync(client, baseAddress, token);
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

        public async Task<String> CheckStatusAsync(HttpClient client, Uri location)
        {

            HttpResponseMessage response = await checkRedirectAuth(client, await client.GetAsync(location), location);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync();
        }

        public async Task<MemoryStream> PredictApiAsync(MemoryStream data, string api_token, string datarobot_key, string username, string host, string project_id, string model_id)
        {

            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            client.Timeout = new System.TimeSpan(0, 5, 0);          

            var uri = new Uri($"{host}/predApi/v1.0/{project_id}/{model_id}/predict");

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, uri);
            message.Headers.Authorization = new AuthenticationHeaderValue("Basic",
        Convert.ToBase64String(
            System.Text.ASCIIEncoding.ASCII.GetBytes(
                string.Format("{0}:{1}", username, api_token))));
            message.Headers.Add("datarobot-key", datarobot_key);

            message.Content = new StreamContent(data);
            message.Content.Headers.Add("Content-Type", "text/csv");


            HttpResponseMessage response = client.SendAsync(message).Result;

            Logger.Trace($"{reqHash} - Status Code: {response.StatusCode}");

            //Logger.Trace(await response.Content.ReadAsStringAsync());
            Stream rdata = await response.Content.ReadAsStreamAsync();

            var mdata = new MemoryStream();
            rdata.Position = 0;
            rdata.CopyTo(mdata);


            return mdata;
        }

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
