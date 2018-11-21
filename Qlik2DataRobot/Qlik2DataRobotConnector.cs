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

namespace Qlik2DataRobot
{
    /// <summary>
    /// The BasicExampleConnector inherits the generated class Qlik.Sse.Connector.ConnectorBase
    /// </summary>
    class Qlik2DataRobotConnector : Connector.ConnectorBase
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private static string logType;

        public class ParameterData
        {
            public DataType DataType;
            public string ParamName;
        }

        public Qlik2DataRobotConnector()
        {
            var appSettings = ConfigurationManager.AppSettings;

            logType = appSettings["logType"];

        }

        private enum FunctionConstant
        {
            LogAsSeenStrCheck,
            LogAsSeenStrEcho,
            LogAsSeenCheck,
            LogAsSeenEcho
        };

        private static readonly Capabilities ConnectorCapabilities = new Capabilities
        {
            PluginIdentifier = "Qlik2DataRobot",
            PluginVersion = "1.0.0",
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

        

        public override async Task EvaluateScript(IAsyncStreamReader<global::Qlik.Sse.BundledRows> requestStream, IServerStreamWriter<global::Qlik.Sse.BundledRows> responseStream, ServerCallContext context)
        {
            ScriptRequestHeader scriptHeader;
            CommonRequestHeader commonHeader;


            Qlik2DataRobotMetrics.RequestCounter.Inc();

            int reqHash = requestStream.GetHashCode();

            if (!(ConnectorCapabilities.AllowScript))
            {
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Script evaluations disabled"));
            }

            try
            {
                

                var header = GetHeader(context.RequestHeaders, "qlik-scriptrequestheader-bin");
                scriptHeader = ScriptRequestHeader.Parser.ParseFrom(header);

                var commonRequestHeader = GetHeader(context.RequestHeaders, "qlik-commonrequestheader-bin");
                commonHeader = CommonRequestHeader.Parser.ParseFrom(commonRequestHeader);

                Logger.Info($"EvaluateScript called from client ({context.Peer}), hashid ({reqHash})");
                Logger.Debug($"EvaluateScript header info: AppId ({commonHeader.AppId}), UserId ({commonHeader.UserId}), Cardinality ({commonHeader.Cardinality} rows)");
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

                var paramnames = $"EvaluateScript call with hashid({reqHash}) got Param names: ";

                foreach (var param in scriptHeader.Params)
                {
                    paramnames += $" {param.Name}";
                }
                Logger.Trace("{0}", paramnames);

                Logger.Trace(scriptHeader.Script);
                Dictionary< string,dynamic> config = JsonConvert.DeserializeObject<Dictionary<string,dynamic>>(scriptHeader.Script);

                string project_name = config.ContainsKey("project_name") ? Convert.ToString(config["project_name"]) : Convert.ToString(config["project_id"]);

                var Params = GetParams(scriptHeader.Params.ToArray());

                string keyname = null;
                if(config.ContainsKey("keyfield"))
                {
                    keyname = Convert.ToString(config["keyfield"]);

                }
                

                ResultDataColumn keyField = new ResultDataColumn();
                var rowdatastream = await ConvertBundledRowsToObject(Params, requestStream, context, keyField, keyname);
                Logger.Debug($"Input Data Size: {rowdatastream.Length}");
                

                var outData = await SelectFunction(config, rowdatastream);
                rowdatastream = null;
                await GenerateResult(outData, responseStream, context, cacheResultInQlik: false, keyField:keyField, keyname:keyname);
                outData = null;
                stopwatch.Stop();
                Logger.Debug($"Took {stopwatch.ElapsedMilliseconds} ms, hashid ({reqHash})");
                Qlik2DataRobotMetrics.DurHist.Observe(stopwatch.ElapsedMilliseconds/1000);
            }
            catch (Exception e)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, $"{e.Message}"));
            }
            finally
            {
                 
            }

            GC.Collect();
        }

        private async Task<MemoryStream> SelectFunction(Dictionary<string, dynamic> config, MemoryStream rowdatastream)
        {
            Logger.Info("Start DataRobot");
            DataRobotRestRequest dr = new DataRobotRestRequest();
           
            string api_token = Convert.ToString(config["auth_config"]["api_token"]);

            MemoryStream result = new MemoryStream(); //ideally would not declare this here, error on return
            switch (config["request_type"])
            {
                case "createproject":
                    Logger.Info("Create Project");
                    string project_name = Convert.ToString(config["project_name"]);

                    var zippedstream = await CompressStream(rowdatastream, project_name);
                    
                    Logger.Info($"Zipped Data Size: {zippedstream.Length}");

                    string endpoint = Convert.ToString(config["auth_config"]["endpoint"]);
                    if (endpoint.Substring(endpoint.Length - 2) != "/") endpoint = endpoint + "/";
                    
                    result = await dr.CreateProjectsAsync(endpoint, api_token, zippedstream, project_name, project_name + ".zip");
                    break;

                case "predictapi":
                    Logger.Info("Predict API");
                    string datarobot_key = Convert.ToString(config["auth_config"]["datarobot_key"]);
                    string username = Convert.ToString(config["auth_config"]["username"]);
                    string host = Convert.ToString(config["api_host"]);
                    string project_id = Convert.ToString(config["project_id"]);
                    string model_id = Convert.ToString(config["model_id"]);

                    result = await dr.PredictApiAsync(rowdatastream, api_token, datarobot_key, username, host, project_id, model_id);
                    break;

                default:
                    break;
            }

            Logger.Info("DataRobot Finish");
            return result;
        }

        

        private async Task<MemoryStream> CompressStream(MemoryStream inData, string filename)
        {
            Logger.Debug("Start Compress");
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

        private async Task<MemoryStream> ConvertBundledRowsToObject(ParameterData[] Parameters, IAsyncStreamReader<global::Qlik.Sse.BundledRows> requestStream, ServerCallContext context, ResultDataColumn keyField, string keyname)
        {
            Logger.Debug("Start Create CSV");

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
            csv.NextRecord();
            Logger.Debug("Finished Header");
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
                                if (keyindex == i && keyname != null) keyField.Numerics.Add(dual.NumData);
                                csv.WriteField(dual.NumData.ToString());
                                break;
                            case DataType.String:
                                if (keyindex == i && keyname != null) keyField.Strings.Add(dual.StrData);
                                csv.WriteField(dual.StrData);
                                break;
                            case DataType.Dual:
                                if (keyindex == i && keyname != null) keyField.Numerics.Add(dual.NumData);
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
            Logger.Debug("Rows" + a);
            return await Task.FromResult(memStream);
        }

        public class ResultDataColumn
        {
            public string Name;
            public DataType DataType;
            public List<double> Numerics;
            public List<string> Strings;
        }

        private async Task GenerateResult(MemoryStream returnedData, IServerStreamWriter<global::Qlik.Sse.BundledRows> responseStream, ServerCallContext context,
            bool failIfWrongDataTypeInFirstCol = false, DataType expectedFirstDataType = DataType.Numeric, bool cacheResultInQlik = true, ResultDataColumn keyField = null, string keyname = null)
        {
            int nrOfCols = 0;
            int nrOfRows = 0;
            List<ResultDataColumn> resultDataColumns = new List<ResultDataColumn>();

            Logger.Info("Generate Results");

            if (true)
            {
                Logger.Debug("Extract JSON");
                //Convert the stream (json) to dictionary
                Logger.Info($"Returned Datasize: {returnedData.Length}");
                StreamReader sr = new StreamReader(returnedData);
                returnedData.Position = 0;
                var data = sr.ReadToEnd();
                Dictionary<string, dynamic> response = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(data);

                if (response.ContainsKey("data"))
                {
                    var a = new ResultDataColumn();
                    a.Name = "Prediction";
                    a.DataType = DataType.String;
                    a.Strings = new List<string>();
                    foreach(dynamic p in response["data"])
                    {
                        a.Strings.Add(Convert.ToString(p["prediction"]));
                    }

                    if(keyname != null)
                    {
                      /*  if (keyField.DataType == DataType.String)
                        {
                            Console.WriteLine("String");
                            Console.WriteLine(keyField.Strings.Count);
                        }
                        else
                        {
                            Console.WriteLine("Numeric");
                            Console.WriteLine(keyField.Numerics.Count);
                        }*/
                    }

                    if (keyname != null) resultDataColumns.Add(keyField);
                    resultDataColumns.Add(a);

                }
                else
                {
                    var a = new ResultDataColumn();
                    a.Name = "Result";
                    a.DataType = DataType.String;
                    a.Strings = new List<string>();
                    a.Strings.Add(Convert.ToString(response["response"]["id"]));

                    resultDataColumns.Add(a);
                }

                nrOfRows = resultDataColumns[0].DataType == DataType.String ? resultDataColumns[0].Strings.Count : resultDataColumns[0].Numerics.Count;
                nrOfCols = resultDataColumns.Count;
                Logger.Debug($"Result Number of Columns: {nrOfCols}");

            }


            if (resultDataColumns != null)
            {
                if (failIfWrongDataTypeInFirstCol && expectedFirstDataType != resultDataColumns[0].DataType)
                {
                    string msg = $"Rserve result datatype mismatch in first column, expected {expectedFirstDataType}, got {resultDataColumns[0].DataType}";
                    Logger.Warn($"{msg}");
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