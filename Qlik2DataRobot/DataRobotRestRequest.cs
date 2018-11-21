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

            Logger.Trace("Create Client");
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            ConfigureAsync(client, baseAddress, token);
            Logger.Trace("Configured Client");
            var requestContent = new MultipartFormDataContent("----");
            
            var projectContent = new StringContent(projectName);
            projectContent.Headers.Add("Content-Disposition", "form-data; name=\"projectName\"");

            var fileContent = new StreamContent(data);
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + filename + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");

            requestContent.Add(projectContent);
            requestContent.Add(fileContent);

            Logger.Trace("Finished Building Request");

            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            Logger.Trace("Finished Setting Up Stream");

            try
            {
                
                HttpResponseMessage response = await checkRedirectAuth(client, await client.PostAsync("projects/", requestContent), null);
                Logger.Trace(response.StatusCode);
                Logger.Trace(response.Headers.Location);
                var result = await response.Content.ReadAsStringAsync();
                Logger.Trace(result);
                var status = "";

                //TODO: Break this loop and exit on finished project or error
                for (int i = 0; i < 500; i++)
                {
                    //ConfigureAsync(client, baseAddress, token);
                    status = await CheckStatusAsync(client, response.Headers.Location);
                    Logger.Trace(status);
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
                Logger.Warn(e, "Create Project Error");
                Logger.Warn(e.Message);
                streamWriter.WriteLine("{\"status\":\"error\"}");
            }

            streamWriter.Flush();
            outStream.Position = 0;

            return outStream;

        }

        public async Task<String> CheckStatusAsync(HttpClient client, Uri location)
        {
            
            //var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient(); ;
            HttpResponseMessage response = await checkRedirectAuth(client, await client.GetAsync(location), location);

            //Logger.Trace(location);
            //Logger.Trace(await response.Content.ReadAsStringAsync());
            //Logger.Trace(response.StatusCode);
            response.EnsureSuccessStatusCode();

            // return URI of the created resource.
            
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<MemoryStream> PredictApiAsync(MemoryStream data, string api_token, string datarobot_key, string username, string host, string project_id, string model_id)
        {
            //client = new HttpClient();
            var client = Qlik2DataRobotHttpClientFactory.clientFactory.CreateClient();
            client.Timeout = new System.TimeSpan(0, 5, 0);
            //client.BaseAddress = new Uri(host);
            

            var uri = new Uri($"{host}/predApi/v1.0/{project_id}/{model_id}/predict");

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, uri);
            message.Headers.Authorization = new AuthenticationHeaderValue("Basic",
        Convert.ToBase64String(
            System.Text.ASCIIEncoding.ASCII.GetBytes(
                string.Format("{0}:{1}", username, api_token))));
            message.Headers.Add("datarobot-key", datarobot_key);

            message.Content = new StreamContent(data);
            message.Content.Headers.Add("Content-Type", "text/csv");


            HttpResponseMessage response = client.SendAsync(message).Result; //await 



            Logger.Trace(response.StatusCode);

            Logger.Trace(await response.Content.ReadAsStringAsync());
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
                Logger.Trace(baseAddress);
                if (token is string)
                {
                    Logger.Trace(token);
                }

                //client = new HttpClient(); //handler
                client.Timeout = new System.TimeSpan(0, 5, 0);
                Logger.Debug("Timeout: {0}", client.Timeout);


                client.BaseAddress = new Uri(baseAddress);
                string tokenh = "Token " + token;
                //client.DefaultRequestHeaders.Add("Authorization", tokenh);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", token);
            }
            catch (Exception e)
            {
                Logger.Warn(e, "Request Configuration Error");
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
