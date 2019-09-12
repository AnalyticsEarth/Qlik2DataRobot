using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using NLog;
using Qlik.Sse;
using Newtonsoft.Json;
using CsvHelper;
using System.Reflection;

namespace Qlik2DataRobot
{
    /// <summary>
    /// The BasicExampleConnector inherits the generated class Qlik.Sse.Connector.ConnectorBase
    /// </summary>
    class Qlik2DataRobotConnector : Connector.ConnectorBase
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public class ParameterData
        {
            public DataType DataType;
            public string ParamName;
        }

        public Qlik2DataRobotConnector()
        {
            var appSettings = ConfigurationManager.AppSettings;

        }

        private static readonly Capabilities ConnectorCapabilities = new Capabilities
        {
            PluginIdentifier = "Qlik2DataRobot",
            PluginVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString(),
            AllowScript = true,
            Functions =
            {
            
            }
        };

        public override Task<Capabilities> GetCapabilities(Empty request, ServerCallContext context)
        {
            if (Logger.IsTraceEnabled)
            {
                Logger.Trace("-- GetCapabilities --");

                TraceServerCallContext(context);
            }
            else
            {
                Logger.Debug("GetCapabilites called");
            }

            return Task.FromResult(ConnectorCapabilities);
        }


        /// <summary>
        /// All requests are processed through evaluate script, however in the context of this connector, the script is a JSON notation string which contains the metadata required to correctly process the attached data.
        /// </summary>
        public override async Task EvaluateScript(IAsyncStreamReader<global::Qlik.Sse.BundledRows> requestStream, IServerStreamWriter<global::Qlik.Sse.BundledRows> responseStream, ServerCallContext context)
        {
            ScriptRequestHeader scriptHeader;
            CommonRequestHeader commonHeader;

            Qlik2DataRobotMetrics.RequestCounter.Inc();
            int reqHash = requestStream.GetHashCode();

            try
            {
                var header = GetHeader(context.RequestHeaders, "qlik-scriptrequestheader-bin");
                scriptHeader = ScriptRequestHeader.Parser.ParseFrom(header);

                var commonRequestHeader = GetHeader(context.RequestHeaders, "qlik-commonrequestheader-bin");
                commonHeader = CommonRequestHeader.Parser.ParseFrom(commonRequestHeader);

                Logger.Info($"{reqHash} - EvaluateScript called from client ({context.Peer}), hashid ({reqHash})");
                Logger.Debug($"{reqHash} - EvaluateScript header info: AppId ({commonHeader.AppId}), UserId ({commonHeader.UserId}), Cardinality ({commonHeader.Cardinality} rows)");
            }
            catch (Exception e)
            {
                Logger.Error($"EvaluateScript with hashid ({reqHash}) failed: {e.Message}");
                throw new RpcException(new Status(StatusCode.DataLoss, e.Message));
            }

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var paramnames = $"{reqHash} - EvaluateScript call with hashid({reqHash}) got Param names: ";

                foreach (var param in scriptHeader.Params)
                {
                    paramnames += $" {param.Name}";
                }
                Logger.Trace("{0}", paramnames);

                Logger.Trace(scriptHeader.Script);
                Dictionary< string,dynamic> config = JsonConvert.DeserializeObject<Dictionary<string,dynamic>>(scriptHeader.Script);

                var Params = GetParams(scriptHeader.Params.ToArray());

                string keyname = null;
                if(config.ContainsKey("keyfield"))
                {
                    keyname = Convert.ToString(config["keyfield"]);

                }
                

                ResultDataColumn keyField = new ResultDataColumn();
                var rowdatastream = await ConvertBundledRowsToCSV(Params, requestStream, context, keyField, keyname);
                Logger.Debug($"{reqHash} - Input Data Size: {rowdatastream.Length}");
                
                var outData = await SelectFunction(config, rowdatastream, reqHash);
                rowdatastream = null;

                bool shouldCache = false;

                if (config.ContainsKey("should_cache"))
                {
                    shouldCache = config["should_cache"];
                }

                bool inc_details = false;
                bool rawExplain = false;
                if (config.ContainsKey("inc_details"))
                {
                    inc_details = config["inc_details"];
                }

                if (config.ContainsKey("explain"))
                {
                    rawExplain = config["explain"]["return_raw"];
                }

                await GenerateResult(outData, responseStream, context, reqHash, cacheResultInQlik: shouldCache, keyField:keyField, keyname:keyname, includeDetail: inc_details, rawExplain:rawExplain);
                outData = null;
                stopwatch.Stop();
                Logger.Debug($"{reqHash} - Took {stopwatch.ElapsedMilliseconds} ms, hashid ({reqHash})");
                Qlik2DataRobotMetrics.DurHist.Observe(stopwatch.ElapsedMilliseconds/1000);
            }
            catch (Exception e)
            {
                Logger.Error($"{reqHash} - ERROR: {e.Message}");
                throw new RpcException(new Status(StatusCode.InvalidArgument, $"{e.Message}"));
            }
            finally
            {
                 
            }

            GC.Collect();
        }


        /// <summary>
        /// Select the functiona based upon the request specification
        /// </summary>
        private async Task<MemoryStream> SelectFunction(Dictionary<string, dynamic> config, MemoryStream rowdatastream, int reqHash)
        {
           
            Logger.Info($"{reqHash} - Start DataRobot");
            DataRobotRestRequest dr = new DataRobotRestRequest(reqHash);
           
            string api_token = Convert.ToString(config["auth_config"]["api_token"]);

            MemoryStream result = new MemoryStream();
            switch (config["request_type"])
            {
                case "createproject":
                    Logger.Info($"{reqHash} - Create Project");
                    string project_name = Convert.ToString(config["project_name"]);

                    var zippedstream = await CompressStream(rowdatastream, project_name, reqHash);
                    
                    Logger.Info($"{reqHash} - Zipped Data Size: {zippedstream.Length}");

                    string endpoint = Convert.ToString(config["auth_config"]["endpoint"]);
                    if (endpoint.Substring(endpoint.Length - 2) != "/") endpoint = endpoint + "/";
                    
                    result = await dr.CreateProjectsAsync(endpoint, api_token, zippedstream, project_name, project_name + ".zip");
                    break;

                case "predictapi":
                    Logger.Info($"{reqHash} - Predict API");
                    string datarobot_key = null;
                    if (config["auth_config"].ContainsKey("datarobot_key")){
                        datarobot_key = Convert.ToString(config["auth_config"]["datarobot_key"]);
                    }
                    
                    string username = Convert.ToString(config["auth_config"]["username"]);
                    string host = Convert.ToString(config["auth_config"]["endpoint"]);
                    string project_id = null;
                    string model_id = null;
                    string deployment_id = null;

                    if (config.ContainsKey("deployment_id"))
                    {
                        deployment_id = Convert.ToString(config["deployment_id"]);
                    }

                    if (config.ContainsKey("project_id") && config.ContainsKey("model_id"))
                    {
                        project_id = Convert.ToString(config["project_id"]);
                        model_id = Convert.ToString(config["model_id"]);
                    }

                    int maxCodes = 0;
                    double thresholdHigh = 0;
                    double thresholdLow = 0;
                    bool explain = false;

                    if (config.ContainsKey("explain"))
                    {
                        maxCodes = config["explain"]["max_codes"];
                        thresholdHigh = config["explain"]["threshold_high"];
                        thresholdLow = config["explain"]["threshold_low"];
                        explain = true;
                    }

                    result = await dr.PredictApiAsync(rowdatastream, api_token, datarobot_key, username, host, deployment_id:deployment_id, project_id:project_id, model_id:model_id, explain:explain, maxCodes:maxCodes, thresholdHigh:thresholdHigh, thresholdLow:thresholdLow);
                    break;

                default:
                    break;
            }

            Logger.Info($"{reqHash} - DataRobot Finish");
            return result;
        }


        /// <summary>
        /// Compress data sent using ZIP encoding
        /// </summary>
        private async Task<MemoryStream> CompressStream(MemoryStream inData, string filename, int reqHash)
        {
            Logger.Debug($"{reqHash} - Start Compress");
            var outStream = new MemoryStream();
            
                using (var archive = new ZipArchive(outStream, ZipArchiveMode.Create, true))
                {
                    var fileInArchive = archive.CreateEntry(filename + ".csv", System.IO.Compression.CompressionLevel.Optimal);
                    using (var entryStream = fileInArchive.Open())
                    inData.CopyTo(entryStream);
                }
                
            
            outStream.Flush();
            outStream.Position = 0;
            return await Task.FromResult(outStream);
        }


        /// <summary>
        /// Convert the input data into a CSV file within memory stream
        /// </summary>
        private async Task<MemoryStream> ConvertBundledRowsToCSV(ParameterData[] Parameters, IAsyncStreamReader<global::Qlik.Sse.BundledRows> requestStream, ServerCallContext context, ResultDataColumn keyField, string keyname)
        {
            int reqHash = requestStream.GetHashCode();
            Logger.Debug($"{reqHash} - Start Create CSV");

            var memStream = new MemoryStream();
            var streamWriter = new StreamWriter(memStream);
            var tw = TextWriter.Synchronized(streamWriter);
            var csv = new CsvWriter(tw);

            var keyindex = 0;

            for (int i = 0; i < Parameters.Length; i++)
            {
                var param = Parameters[i];
                if(keyname != null)
                {
                    if (param.ParamName == keyname)
                    {
                        keyindex = i;
                        keyField.Name = param.ParamName;
                        keyField.DataType = param.DataType;
                        switch (param.DataType)
                        {
                            case DataType.Numeric:
                            case DataType.Dual:
                                keyField.Numerics = new List<double>();
                                break;
                            case DataType.String:
                                keyField.Strings = new List<string>();
                                break;

                        }
                    }
                }

                csv.WriteField(param.ParamName);
            }

            if (keyField.Name == null && keyname != null) 
            {
                throw new Exception("The keyfield was not found in the source data, please ensure you are including this field in the dataset sent from Qlik.");
            }


            csv.NextRecord();
            Logger.Debug($"{reqHash} - Finished Header");
            int a = 0;

            while (await requestStream.MoveNext())
            {
                foreach (var Row in requestStream.Current.Rows)
                {

                    for (int i = 0; i < Parameters.Length; i++)
                    {
                        
                        var param = Parameters[i];
                        var dual = Row.Duals[i];
                        switch (param.DataType)
                        {
                            case DataType.Numeric:

                                if (keyindex == i && keyname != null)
                                {
                                    keyField.Numerics.Add(dual.NumData);
                                }
                                csv.WriteField(dual.NumData.ToString());
                                break;
                            case DataType.String:
                                if (keyindex == i && keyname != null)
                                {
                                    keyField.Strings.Add(dual.StrData);
                                }
                                csv.WriteField(dual.StrData);
                                break;
                            case DataType.Dual:
                                if (keyindex == i && keyname != null)
                                {
                                    keyField.Numerics.Add(dual.NumData);
                                }
                                csv.WriteField(dual.NumData.ToString());
                                break;
                        }
                        
                    }

                    a++;
                    csv.NextRecord();
                } 
            }
            csv.Flush();
            tw.Flush();
            streamWriter.Flush();
            memStream.Flush();

            memStream.Position = 0;
            Logger.Debug($"{reqHash} - Rows" + a);

            if (a == 0)
            {
                throw new Exception("There were no rows in the table sent from Qlik. Check that the table has at least 1 row of data.");
            }

            return await Task.FromResult(memStream);
        }

        /// <summary>
        /// Data structure for the result data column
        /// </summary>
        public class ResultDataColumn
        {
            public string Name;
            public DataType DataType;
            public List<double> Numerics;
            public List<string> Strings;
        }

        /// <summary>
        /// Return the results from connector to Qlik Engine
        /// </summary>
        private async Task GenerateResult(MemoryStream returnedData, IServerStreamWriter<global::Qlik.Sse.BundledRows> responseStream, ServerCallContext context, int reqHash,
            bool failIfWrongDataTypeInFirstCol = false, DataType expectedFirstDataType = DataType.Numeric, bool cacheResultInQlik = true, ResultDataColumn keyField = null, string keyname = null, bool includeDetail = false, bool rawExplain = false)
        {
            
            int nrOfCols = 0;
            int nrOfRows = 0;
            List<ResultDataColumn> resultDataColumns = new List<ResultDataColumn>();

            Logger.Info($"{reqHash} - Generate Results");

            if (true)
            {
                Logger.Debug($"{reqHash} - Extract JSON");
                //Convert the stream (json) to dictionary
                Logger.Info($"{reqHash} - Returned Datasize: {returnedData.Length}");
                
                StreamReader sr = new StreamReader(returnedData);
                returnedData.Position = 0;
                var data = sr.ReadToEnd();
                Dictionary<string, dynamic> response = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(data);
                Logger.Trace($"{reqHash} - Returned Data: {data}");

                if (response.ContainsKey("data"))
                {
                    //Prediction Column
                    var a = new ResultDataColumn();
                    a.Name = "Prediction";
                    a.DataType = DataType.String;
                    a.Strings = new List<string>();

                    var pe = new ResultDataColumn();
                    if (rawExplain == true)
                    {
                        
                        pe.Name = $"Prediction Explanations";
                        pe.DataType = DataType.String;
                        pe.Strings = new List<string>();
                        
                    }

                    //The first row will determine which fields to return in table for prediction values
                    bool fieldListAgreed = false;
                    


                    //Loop through each response in array (one for each row of input data)
                    foreach (dynamic p in response["data"])
                    {
                        a.Strings.Add(Convert.ToString(p["prediction"]));

                        if(includeDetail == true)
                        {
                            if (fieldListAgreed == false)
                            {
                                foreach (dynamic pv in p["predictionValues"])
                                {
                                    var pvi = new ResultDataColumn();
                                    pvi.Name = $"Prediction value for label: {pv["label"]}";
                                    pvi.DataType = DataType.String;
                                    pvi.Strings = new List<string>();
                                    resultDataColumns.Add(pvi);
                                }

                                fieldListAgreed = true;
                                Logger.Trace($"{reqHash} - Columns: {resultDataColumns.Count}");
                            }

                            //Loop through each predicted value and insert the row values to the column
                            int index = 0;

                            foreach (dynamic pv in p["predictionValues"])
                            {
                                resultDataColumns[index].Strings.Add(Convert.ToString(pv["value"]));
                                index++;
                            }
                            
                        }

                        if (rawExplain == true)
                        {
                            pe.Strings.Add(Convert.ToString(p["predictionExplanations"]));
                        }

                    }

                    if (keyname != null) resultDataColumns.Add(keyField);

                    if (rawExplain == true)
                    {
                        resultDataColumns.Add(pe);
                    }

                    
                    resultDataColumns.Add(a);

                    

                }
                else if(response.ContainsKey("response"))
                {
                    var a = new ResultDataColumn();
                    a.Name = "Result";
                    a.DataType = DataType.String;
                    a.Strings = new List<string>();
                    a.Strings.Add(Convert.ToString(response["response"]["id"]));

                    resultDataColumns.Add(a);
                } else
                {
                    if (response.ContainsKey("message"))
                    {
                        throw new Exception($"The following error message was returned from DataRobot: {response["message"]}");
                    }
                    else
                    {
                        throw new Exception($"An Unknown Error Occured: {data}");
                    }
                    
                }

                nrOfRows = resultDataColumns[0].DataType == DataType.String ? resultDataColumns[0].Strings.Count : resultDataColumns[0].Numerics.Count;
                nrOfCols = resultDataColumns.Count;
                Logger.Debug($"{reqHash} - Result Number of Columns: {nrOfCols}");

            }


            if (resultDataColumns != null)
            {
                if (failIfWrongDataTypeInFirstCol && expectedFirstDataType != resultDataColumns[0].DataType)
                {
                    string msg = $"Result datatype mismatch in first column, expected {expectedFirstDataType}, got {resultDataColumns[0].DataType}";
                    Logger.Warn($"{reqHash} - {msg}");
                    throw new RpcException(new Status(StatusCode.InvalidArgument, $"{msg}"));
                }

                //Send TableDescription header
                TableDescription tableDesc = new TableDescription
                {
                    NumberOfRows = nrOfRows
                };

                for (int col = 0; col < nrOfCols; col++)
                {
                    if (String.IsNullOrEmpty(resultDataColumns[col].Name))
                    {
                        tableDesc.Fields.Add(new FieldDescription
                        {
                            DataType = resultDataColumns[col].DataType
                        });
                    }
                    else
                    {
                        tableDesc.Fields.Add(new FieldDescription
                        {
                            DataType = resultDataColumns[col].DataType,
                            Name = resultDataColumns[col].Name
                        });
                    }
                }

                var tableMetadata = new Metadata
                {
                    { new Metadata.Entry("qlik-tabledescription-bin", MessageExtensions.ToByteArray(tableDesc)) }
                };

                if (!cacheResultInQlik)
                {
                    tableMetadata.Add("qlik-cache", "no-store");
                }

                await context.WriteResponseHeadersAsync(tableMetadata);

                // Send data
                var bundledRows = new BundledRows();

                for (int i = 0; i < nrOfRows; i++)
                {
                    var row = new Row();

                    for (int col = 0; col < nrOfCols; col++)
                    {
                        if (resultDataColumns[col].DataType == DataType.Numeric)
                        {
                            row.Duals.Add(new Dual() { NumData = resultDataColumns[col].Numerics[i] });
                        }
                        else if (resultDataColumns[col].DataType == DataType.String)
                        {
                            row.Duals.Add(new Dual() { StrData = resultDataColumns[col].Strings[i] ?? "" });
                        }
                    }
                    bundledRows.Rows.Add(row);
                    if (((i + 1) % 2000) == 0)
                    {
                        // Send a bundle
                        await responseStream.WriteAsync(bundledRows);
                        bundledRows = null;
                        bundledRows = new BundledRows();
                    }
                }

                if (bundledRows.Rows.Count() > 0)
                {
                    // Send last bundle
                    await responseStream.WriteAsync(bundledRows);
                    bundledRows = null;
                }
            }
        }

        byte[] GetHeader(Metadata Headers, string Key)
        {
            foreach (var Header in Headers)
            {
                if (Header.Key == Key)
                {
                    return Header.ValueBytes;
                }
            }
            return null;
        }

        private string[] GetParamNames(Parameter[] Parameters)
        {
            return Parameters
                        .Select((_, index) => string.Format($"arg{index + 1}"))
                        .ToArray();
        }

        ParameterData[] GetParams(Parameter[] Parameters)
        {
            return Parameters
                        .Select((Param) =>
                        {
                            var p = new ParameterData()
                            {
                                DataType = Param.DataType,
                                ParamName = Param.Name
                            };

                            return p;
                        })
                        .ToArray();
        }

        private static Dictionary<String, String> TraceServerCallContext(ServerCallContext context)
        {
            Dictionary<String, String> headerInfo = new Dictionary<String, String>();

            var authContext = context.AuthContext;

            Logger.Trace($"ServerCallContext.Method : {context.Method}");
            Logger.Trace($"ServerCallContext.Host : {context.Host}");
            Logger.Trace($"ServerCallContext.Peer : {context.Peer}");

            headerInfo.Add("Method", context.Method);
            headerInfo.Add("Host", context.Host);
            headerInfo.Add("Peer", context.Peer);

            foreach (var contextRequestHeader in context.RequestHeaders)
            {
                Logger.Trace(
                    $"{contextRequestHeader.Key} : {(contextRequestHeader.IsBinary ? "<binary>" : contextRequestHeader.Value)}");

                if (contextRequestHeader.Key == "qlik-functionrequestheader-bin")
                {
                    var functionRequestHeader = new FunctionRequestHeader();
                    functionRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"FunctionRequestHeader.FunctionId : {functionRequestHeader.FunctionId}");
                    Logger.Trace($"FunctionRequestHeader.Version : {functionRequestHeader.Version}");

                    headerInfo.Add("FunctionId", functionRequestHeader.FunctionId.ToString());
                    headerInfo.Add("Version", functionRequestHeader.Version);
                }
                else if (contextRequestHeader.Key == "qlik-commonrequestheader-bin")
                {
                    var commonRequestHeader = new CommonRequestHeader();
                    commonRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"CommonRequestHeader.AppId : {commonRequestHeader.AppId}");
                    Logger.Trace($"CommonRequestHeader.Cardinality : {commonRequestHeader.Cardinality}");
                    Logger.Trace($"CommonRequestHeader.UserId : {commonRequestHeader.UserId}");

                    headerInfo.Add("AppId", commonRequestHeader.AppId);
                    headerInfo.Add("Cardinality", commonRequestHeader.Cardinality.ToString());
                    headerInfo.Add("UserId", commonRequestHeader.UserId);


                }
                else if (contextRequestHeader.Key == "qlik-scriptrequestheader-bin")
                {
                    var scriptRequestHeader = new ScriptRequestHeader();
                    scriptRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"ScriptRequestHeader.FunctionType : {scriptRequestHeader.FunctionType}");
                    Logger.Trace($"ScriptRequestHeader.ReturnType : {scriptRequestHeader.ReturnType}");

                    int paramIdx = 0;

                    foreach (var parameter in scriptRequestHeader.Params)
                    {
                        Logger.Trace($"ScriptRequestHeader.Params[{paramIdx}].Name : {parameter.Name}");
                        Logger.Trace($"ScriptRequestHeader.Params[{paramIdx}].DataType : {parameter.DataType}");
                        ++paramIdx;
                    }
                    Logger.Trace($"CommonRequestHeader.Script : {scriptRequestHeader.Script}");
                }
            }

            Logger.Trace($"ServerCallContext.AuthContext.IsPeerAuthenticated : {authContext.IsPeerAuthenticated}");
            Logger.Trace(
                $"ServerCallContext.AuthContext.PeerIdentityPropertyName : {authContext.PeerIdentityPropertyName}");
            foreach (var authContextProperty in authContext.Properties)
            {
                var loggedValue = authContextProperty.Value;
                var firstLineLength = loggedValue.IndexOf('\n');

                if (firstLineLength > 0)
                {
                    loggedValue = loggedValue.Substring(0, firstLineLength) + "<truncated at linefeed>";
                }

                Logger.Trace($"{authContextProperty.Name} : {loggedValue}");
            }
            return headerInfo;
        }
    }
}