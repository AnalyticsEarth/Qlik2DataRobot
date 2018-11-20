using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;


namespace Qlik2DataRobot
{
    class DataRobotRestRequest
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        HttpClient client;  

        public async Task<string> ListProjectsAsync()
        {
            HttpResponseMessage response = await client.GetAsync("projects");
            response.EnsureSuccessStatusCode();

            // return URI of the created resource.
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<MemoryStream> CreateProjectsAsync(MemoryStream data, string projectName, string filename)
        {

            var requestContent = new MultipartFormDataContent("----");

            var projectContent = new StringContent(projectName);
            projectContent.Headers.Add("Content-Disposition", "form-data; name=\"projectName\"");

            var fileContent = new StreamContent(data);
            fileContent.Headers.Add("Content-Disposition", "form-data; name=\"file\"; filename=\"" + filename + "\"");
            fileContent.Headers.Add("Content-Encoding", "zip");
            fileContent.Headers.Add("Content-Type", "application/zip");

            requestContent.Add(projectContent);
            requestContent.Add(fileContent);

            

            MemoryStream outStream = new MemoryStream();
            var streamWriter = new StreamWriter(outStream);

            try
            {
                HttpResponseMessage response = await client.PostAsync("projects", requestContent);
                Logger.Trace(response.StatusCode);
                Logger.Trace(response.Headers.Location);
                var result = await response.Content.ReadAsStringAsync();
                Logger.Trace(result);
                var status = "";

                //TODO: Break this loop and exit on finished project or error
                for (int i = 0; i < 500; i++)
                {
                    status = await CheckStatusAsync(response.Headers.Location);
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
                streamWriter.WriteLine("{\"status\":\"error\"}");
            }

            streamWriter.Flush();
            outStream.Position = 0;

            return outStream;

        }

        public async Task<String> CheckStatusAsync(Uri location)
        {
            HttpResponseMessage response = await client.GetAsync(location);
            response.EnsureSuccessStatusCode();

            // return URI of the created resource.
            Logger.Trace(response.StatusCode);
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<MemoryStream> PredictApiAsync(MemoryStream data, string api_token, string datarobot_key, string username, string host, string project_id, string model_id)
        {
            client = new HttpClient();
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

        public bool ConfigureAsync(string baseAddress, string token)
        {
            Logger.Trace(baseAddress);
            if(token is string)
            {
                Logger.Trace(token);
            }

            client = new HttpClient(); //handler
            client.Timeout = new System.TimeSpan(0,5,0);
            Logger.Debug("Timeout: {0}", client.Timeout);
           

            client.BaseAddress = new Uri(baseAddress);
            string tokenh = "Token " + token;
            //client.DefaultRequestHeaders.Add("Authorization", tokenh);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", token);

            try
            {

            }
            catch (Exception e)
            {
                Logger.Warn(e, "Request Configuration Error");
            }

            return true;

        }

    }

   
}
