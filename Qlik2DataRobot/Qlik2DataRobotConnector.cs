using CsvHelper;
using CsvHelper.Configuration;
using Google.Protobuf;
using Grpc.Core;
using Newtonsoft.Json;
using NLog;
using Qlik.Sse;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

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
        public override async Task EvaluateScript(IAsyncStreamReader<BundledRows> requestStream, IServerStreamWriter<BundledRows> responseStream, ServerCallContext context)
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
                RequestSpecification config = JsonConvert.DeserializeObject<RequestSpecification>(scriptHeader.Script);

                var Params = GetParams(scriptHeader.Params.ToArray());

                string keyname = null;
                if (config.keyfield != null)
                {
                    keyname = config.keyfield;

                }


                ResultDataColumn keyField = new ResultDataColumn();
                var rowdatastream = await ConvertBundledRowsToCSV(Params, requestStream, context, keyField, keyname, config.timestamp_field, config.timestamp_format);
                Logger.Debug($"{reqHash} - Input Data Size: {rowdatastream.Length}");

                var outData = await SelectFunction(config, rowdatastream, reqHash);
                rowdatastream = null;

                bool shouldCache = config.should_cache;

                bool inc_details = config.inc_details;
                bool rawExplain = false;
                bool shouldExplain = false;
                int max_codes = 0;

                if (config.explain != null)
                {
                    shouldExplain = true;
                    rawExplain = config.explain.return_raw;
                    max_codes = config.explain.max_codes;
                }

                string request_type = config.request_type;

                await GenerateResult(request_type, outData, responseStream, context, reqHash, cacheResultInQlik: shouldCache, keyField: keyField, keyname: keyname, includeDetail: inc_details, shouldExplain: shouldExplain, rawExplain: rawExplain, explain_max: max_codes);
                outData = null;
                stopwatch.Stop();
                Logger.Debug($"{reqHash} - Took {stopwatch.ElapsedMilliseconds} ms, hashid ({reqHash})");
                Qlik2DataRobotMetrics.DurHist.Observe(stopwatch.ElapsedMilliseconds / 1000);
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
        private async Task<MemoryStream> SelectFunction(RequestSpecification config, MemoryStream rowdatastream, int reqHash)
        {
            Logger.Info($"{reqHash} - Start DataRobot");
            DataRobotRestRequest dr = new DataRobotRestRequest(reqHash);

            string api_token = Convert.ToString(config.auth_config.api_token);
            string datarobot_key = config.auth_config.datarobot_key;
            string host = config.auth_config.endpoint;
            string mlops_host = config.mlops_endpoint;
            if (host.Substring(host.Length - 2) != "/") host = host + "/";
            if (!String.IsNullOrEmpty(mlops_host) && mlops_host.Substring(mlops_host.Length - 2) != "/") mlops_host = mlops_host + "/";

            string project_id = config.project_id;
            string project_name = Convert.ToString(config.project_name);

            string model_id = config.model_id;
            string deployment_id = config.deployment_id;
            string keyField = config.keyfield;
            string dataset_name = Convert.ToString(config.dataset_name);
            string dataset_id = Convert.ToString(config.dataset_id);

            string association_id_name = Convert.ToString(config.association_id_name);
            string target_name = Convert.ToString(config.target_name);

            string passthroughColumnsSet = config.passthroughColumnsSet;

            MemoryStream result = new MemoryStream();

            string[] zipped_request_types = { "actuals", "dataset", "datasetversion", "createproject", "batchpred" };
            Logger.Info($"{reqHash} - Entering if");
            if (zipped_request_types.Contains(config.request_type))
            {
                var zip_stream = await CompressStream(rowdatastream, dataset_name, reqHash);
                Logger.Info($"{reqHash} - Zipped Data Size: {zip_stream.Length}");

                switch (config.request_type)
                {
                    case "actuals":
                        Logger.Info($"{reqHash} - Sending actuals");
                        Logger.Info($"{reqHash} - dataset_id (optional): '{dataset_id}'");
                        if (String.IsNullOrEmpty(dataset_name))
                        {
                            dataset_name = "Actuals";
                        }

                        Logger.Trace($"{reqHash} - Dataset name: '{dataset_name}'");

                        result = await dr.SendActualsAsync(host, api_token, zip_stream, deployment_id, keyField, dataset_name, dataset_id, associationIdColumn: association_id_name, actualValueColumn: target_name);
                        break;

                    case "batchpred":
                        Logger.Info($"{reqHash} - Sending batch prediction");
                        Logger.Info($"{reqHash} - dataset_id (optional): '{dataset_id}'");
                        if (String.IsNullOrEmpty(dataset_name))
                        {
                            dataset_name = "Batch Prediction";
                        }

                        Logger.Trace($"{reqHash} - Dataset name: '{dataset_name}'");

                        int maxCodes = 0;
                        double thresholdHigh = 0.75;
                        double thresholdLow = 0.25;
                        bool explain = false;
                        if (config.explain != null)
                        {
                            Logger.Info($"{reqHash} - {JsonConvert.SerializeObject(config.explain)}");
                            maxCodes = config.explain.max_codes;
                            thresholdHigh = config.explain.threshold_high;
                            thresholdLow = config.explain.threshold_low;
                            explain = true;
                        }

                        result = await dr.ScoreBatchAsync(host, api_token, zip_stream, deployment_id, keyField,
                        passthroughColumnsSet, dataset_name, dataset_id,
                            maxCodes,
                        thresholdHigh,
                        thresholdLow,
                        explain);
                        break;

                    case "dataset":
                        Logger.Info($"{reqHash} - Create dataset");
                        Logger.Info($"{reqHash} - Dataset name - {dataset_name}");
                        result = await dr.CreateDatasetAsync2(host, api_token, zip_stream, dataset_name, datasetId: "");
                        break;

                    case "datasetversion":
                        Logger.Info($"{reqHash} - Create dataset");
                        Logger.Info($"{reqHash} - Dataset name - {dataset_name}");
                        Logger.Info($"{reqHash} - Dataset ID from Config: {dataset_id}");
                        result = await dr.CreateDatasetAsync(host, api_token, zip_stream, dataset_name, dataset_id);
                        break;

                    case "createproject":
                        Logger.Info($"{reqHash} - Create Project");
                        result = await dr.CreateProjectsAsync(host, api_token, zip_stream, project_name, project_name + ".zip");
                        break;

                    default:
                        break;
                }
            }
            else
            {
                switch (config.request_type)
                {
                    case "predictapi":
                        Logger.Info($"{reqHash} - Predict API");

                        int maxCodes = 0;
                        double thresholdHigh = 0;
                        double thresholdLow = 0;
                        bool explain = false;

                        if (config.explain != null)
                        {
                            maxCodes = config.explain.max_codes;
                            thresholdHigh = config.explain.threshold_high;
                            thresholdLow = config.explain.threshold_low;
                            explain = true;
                        }

                        result = await dr.PredictApiAsync(rowdatastream, api_token, datarobot_key, host, deployment_id: deployment_id, project_id: project_id, model_id: model_id, keyField: keyField, explain: explain, maxCodes: maxCodes, thresholdHigh: thresholdHigh, thresholdLow: thresholdLow);
                        break;

                    case "timeseries":
                        Logger.Info($"{reqHash} - Time Series Prediction API");

                        string forecast_point = null;



                        if (config.forecast_point != null)
                        {
                            forecast_point = Convert.ToString(config.forecast_point);
                            //forecast_point = config.forecast_point.ToString("s");
                        }
                        result = await dr.TimeSeriesAsync(rowdatastream, api_token, datarobot_key, host, deployment_id: deployment_id, project_id: project_id, model_id: model_id, forecast_point: forecast_point);
                        break;

                    default:
                        break;
                }
            }

            Logger.Info($"{reqHash} - DataRobot Finish");
            Logger.Trace($"{reqHash} - Result {result}");
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
        private async Task<MemoryStream> ConvertBundledRowsToCSV(ParameterData[] Parameters, IAsyncStreamReader<BundledRows> requestStream, ServerCallContext context, ResultDataColumn keyField, string keyname, string timestamp_field = "", string timestamp_format = "s")
        {
            int reqHash = requestStream.GetHashCode();
            Logger.Debug($"{reqHash} - Start Create CSV");

            var memStream = new MemoryStream();
            var streamWriter = new StreamWriter(memStream);
            var tw = TextWriter.Synchronized(streamWriter);
            var config = new CsvConfiguration(CultureInfo.CurrentCulture);
            var csv = new CsvWriter(tw, config);

            var keyindex = 0;

            for (int i = 0; i < Parameters.Length; i++)
            {
                var param = Parameters[i];
                if (keyname != null)
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
                            case DataType.Dual:
                                if (param.ParamName == timestamp_field)
                                {
                                    Logger.Trace($"{reqHash} - Timestamp: {dual.NumData} {dual.StrData}");
                                    var date = (new DateTime(1900, 1, 1)).AddMilliseconds(dual.NumData * 86400000);
                                    csv.WriteField(date.ToString(timestamp_format));
                                }
                                else
                                {
                                    csv.WriteField(dual.NumData.ToString());
                                }
                                break;
                            case DataType.String:
                                csv.WriteField(dual.StrData);
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
        private async Task GenerateResult(string request_type, MemoryStream returnedData, IServerStreamWriter<BundledRows> responseStream, ServerCallContext context, int reqHash,
            bool failIfWrongDataTypeInFirstCol = false, DataType expectedFirstDataType = DataType.Numeric, bool cacheResultInQlik = true, ResultDataColumn keyField = null, string keyname = null, bool includeDetail = false, bool shouldExplain = false, bool rawExplain = false, int explain_max = 0)
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
                Logger.Info($"{reqHash} - Request Type: {request_type}");

                StreamReader sr = new StreamReader(returnedData);
                returnedData.Position = 0;
                var data = sr.ReadToEnd();
                //Dictionary<string, dynamic> response = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(data);
                ResponseSpecification response = JsonConvert.DeserializeObject<ResponseSpecification>(data);

                if (response.csvdata != null)
                {
                    var reader = new StringReader(response.csvdata);
                    var config = new CsvConfiguration(CultureInfo.InvariantCulture);
                    /*  CsvConfiguration(CultureInfo.InvariantCulture)
                  {
                      HasHeaderRecord = true,
                  };*/
                    var csvReader = new CsvReader(reader, config);

                    csvReader.Read();
                    csvReader.ReadHeader();

                    var bundledRows = new BundledRows();

                    Logger.Info("Reading records");
                    var records = csvReader.GetRecords<dynamic>().ToList();

                    int count = records.Count;
                    int i = 0;
                    foreach (dynamic record in records)
                    {
                        int j = 0;
                        var row = new Row();
                        foreach (dynamic field in record)
                        {
                            j += 1;
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(field.Value) ?? "" });

                        }

                        bundledRows.Rows.Add(row);

                        if (i == 0)
                        {
                            nrOfCols = j;

                            foreach (dynamic field in record)
                            {
                                var a = new ResultDataColumn();
                                a.Name = field.Key;
                                a.DataType = DataType.String;
                                resultDataColumns.Add(a);
                            }
                            Logger.Info("Sending headers");
                            Logger.Info($"Headers count: {count}");
                            Logger.Info($"Headers nrOfCols: {nrOfCols}");
                            Logger.Info($"Headers resultDataColumns: {resultDataColumns}");
                            await GenerateAndSendHeadersAsync(context, count, nrOfCols, resultDataColumns, cacheResultInQlik);
                            Logger.Info("Sent headers");
                        }
                        i += 1;

                        if (i % 1000 == 0)
                        {
                            nrOfRows = i;
                            Logger.Info("Sending batch");
                            await responseStream.WriteAsync(bundledRows);
                            bundledRows = new BundledRows();
                        }
                    }
                    nrOfRows = i;
                    if (i > 0)
                    {
                        Logger.Info($"Sending final batch");

                        await responseStream.WriteAsync(bundledRows);
                        bundledRows = null;
                    }

                }
                else if (response.data != null)
                {

                    Logger.Trace($"{reqHash} - Response Data: {response.data}");

                    //Sort the response by RowId
                    List<DataSpecification> sortedData = response.data.OrderBy(o => o.rowId).ToList();

                    //Return Raw Explain First So Works In Chart Expression
                    if (request_type != "timeseries")
                    {
                        if (shouldExplain && rawExplain)
                        {
                            var pe = new ResultDataColumn();
                            pe.Name = $"Prediction Explanations";
                            pe.DataType = DataType.String;
                            resultDataColumns.Add(pe);
                        }
                    }


                    //Prediction Column
                    var a = new ResultDataColumn();
                    a.Name = "Prediction";
                    a.DataType = DataType.String;
                    //a.Strings = new List<string>();
                    resultDataColumns.Add(a);

                    //Include Keyfield
                    if (keyname != null) resultDataColumns.Add(keyField);

                    //The first row will determine which fields to return in table for prediction values
                    if (includeDetail == true)
                    {
                        foreach (PredictionValueSpecification pv in sortedData[0].predictionValues)
                        {
                            var pvi = new ResultDataColumn();
                            pvi.Name = $"Prediction value for label: {pv.label}";
                            pvi.DataType = DataType.String;
                            resultDataColumns.Add(pvi);
                        }
                    }


                    // Add Time Series Columns to resultData
                    if (request_type == "timeseries")
                    {
                        //Row Id
                        var rowId = new ResultDataColumn();
                        rowId.Name = "rowId";
                        rowId.DataType = DataType.String;

                        //Time Series Columns
                        var seriesId = new ResultDataColumn();
                        seriesId.Name = "seriesId";
                        seriesId.DataType = DataType.String;

                        var forecastPoint = new ResultDataColumn();
                        forecastPoint.Name = "forecastPoint";
                        forecastPoint.DataType = DataType.String;

                        var timestamp = new ResultDataColumn();
                        timestamp.Name = "timestamp";
                        timestamp.DataType = DataType.String;

                        var forecastDistance = new ResultDataColumn();
                        forecastDistance.Name = "forecastDistance";
                        forecastDistance.DataType = DataType.String;

                        var originalFormatTimestamp = new ResultDataColumn();
                        originalFormatTimestamp.Name = "originalFormatTimestamp";
                        originalFormatTimestamp.DataType = DataType.String;



                        resultDataColumns.Add(rowId);
                        resultDataColumns.Add(seriesId);
                        resultDataColumns.Add(forecastPoint);
                        resultDataColumns.Add(timestamp);
                        resultDataColumns.Add(forecastDistance);
                        resultDataColumns.Add(originalFormatTimestamp);

                    }



                    if (request_type != "timeseries")
                    {
                        if (shouldExplain && !rawExplain)
                        {
                            for (int j = 0; j < explain_max; j++)
                            {
                                foreach (string field in new[] { "label", "feature", "featureValue", "strength", "qualitativeStrength" })
                                {
                                    var pe = new ResultDataColumn();
                                    pe.Name = $"PE_{j + 1}_{field}";
                                    pe.DataType = DataType.String;
                                    resultDataColumns.Add(pe);
                                }
                            }
                        }
                    }


                    nrOfRows = sortedData.Count;
                    nrOfCols = resultDataColumns.Count;
                    Logger.Debug($"{reqHash} - Result Number of Columns: {nrOfCols}");

                    await GenerateAndSendHeadersAsync(context, nrOfRows, nrOfCols, resultDataColumns, cacheResultInQlik);

                    Logger.Trace($"{reqHash} - Start Loop");

                    // Send data
                    var bundledRows = new BundledRows();

                    //Loop through each response in array (one for each row of input data)
                    int i = 0;
                    foreach (DataSpecification p in sortedData)
                    {
                        var row = new Row();

                        if (request_type != "timeseries")
                        {
                            if (shouldExplain && rawExplain)
                            {
                                if (p.predictionExplanations != null)
                                {
                                    row.Duals.Add(new Dual() { StrData = JsonConvert.SerializeObject(p.predictionExplanations) ?? "" });
                                }
                                else
                                {
                                    row.Duals.Add(new Dual() { StrData = "[]" });
                                }

                            }
                        }


                        //Prediction Column
                        row.Duals.Add(new Dual() { StrData = Convert.ToString(p.prediction) ?? "" });
                        Logger.Trace($"{reqHash} - In Loop RowId: {p.rowId}");

                        //KeyField Column
                        if (p.passthroughValues != null)
                        {
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.passthroughValues[keyField.Name]) ?? "" });
                        }

                        //Include Details
                        if (includeDetail == true)
                        {
                            //Loop through each predicted value and insert the row values to the column
                            foreach (PredictionValueSpecification pv in p.predictionValues)
                            {
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pv.value) ?? "" });
                            }
                        }

                        //Timeseries field
                        if (request_type == "timeseries")
                        {
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.rowId) ?? "" });
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.seriesId) ?? "" });
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.forecastPoint) ?? "" });
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.timestamp) ?? "" });
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.forecastDistance) ?? "" });
                            row.Duals.Add(new Dual() { StrData = Convert.ToString(p.originalFormatTimestamp) ?? "" });
                        }

                        //Include Prediction Explanations
                        if (shouldExplain && !rawExplain)
                        {
                            foreach (PredictionExplanationSpecification pe in p.predictionExplanations)
                            {
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pe.label) ?? "" });
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pe.feature) ?? "" });
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pe.featureValue) ?? "" });
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pe.strength) ?? "" });
                                row.Duals.Add(new Dual() { StrData = Convert.ToString(pe.qualitativeStrength) ?? "" });
                            }

                            for (int j = p.predictionExplanations.Count; j < explain_max; j++)
                            {
                                row.Duals.Add(new Dual() { });
                                row.Duals.Add(new Dual() { });
                                row.Duals.Add(new Dual() { });
                                row.Duals.Add(new Dual() { });
                                row.Duals.Add(new Dual() { });
                            }
                        }

                        //Add the row the bundle
                        bundledRows.Rows.Add(row);

                        //When we get a batch of 2000 rows, send the response 
                        if (((i + 1) % 2000) == 0)
                        {
                            // Send a bundle
                            await responseStream.WriteAsync(bundledRows);
                            //Reset the bundle
                            bundledRows = null;
                            bundledRows = new BundledRows();
                        }
                        i++;
                    }

                    //Send any left over rows after the final loop
                    if (bundledRows.Rows.Count() > 0)
                    {
                        // Send last bundle
                        await responseStream.WriteAsync(bundledRows);
                        bundledRows = null;
                    }


                }
                else if (response.response != null)
                {
                    Logger.Trace($"{reqHash} - Processing Status Response for Project ID: {response.response.id}");
                    var a = new ResultDataColumn();
                    a.Name = "Result";
                    a.DataType = DataType.String;

                    resultDataColumns.Add(a);

                    await GenerateAndSendHeadersAsync(context, 1, 1, resultDataColumns, cacheResultInQlik);

                    var bundledRows = new BundledRows();
                    var row = new Row();
                    row.Duals.Add(new Dual() { StrData = Convert.ToString(response.response.id) ?? "" });
                    bundledRows.Rows.Add(row);
                    await responseStream.WriteAsync(bundledRows);
                    bundledRows = null;



                }
                else
                {
                    if (response.message != null)
                    {
                        throw new Exception($"The following error message was returned from DataRobot: {response.message}");
                    }
                    else
                    {
                        throw new Exception($"An Unknown Error Occured: {data}");
                    }

                }



            }

        }

        private async Task GenerateAndSendHeadersAsync(ServerCallContext context, int nrOfRows, int nrOfCols, List<ResultDataColumn> resultDataColumns, bool cacheResultInQlik)
        {

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
            Logger.Info("Generating Metadata");
            var tableMetadata = new Metadata
                    {
                        { new Metadata.Entry("qlik-tabledescription-bin", MessageExtensions.ToByteArray(tableDesc)) }
                    };
            Logger.Info($"Generating Metadata");
            if (!cacheResultInQlik)
            {
                tableMetadata.Add("qlik-cache", "no-store");
            }

            await context.WriteResponseHeadersAsync(tableMetadata);

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
