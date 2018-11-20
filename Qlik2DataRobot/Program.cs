using System;
using System.Collections.Generic;
using System.IO;
using Grpc.Core;
using NLog;
using System.Configuration;
using System.Threading;
using Prometheus;

namespace Qlik2DataRobot
{
    static class Qlik2DataRobotMetrics
    {
        public static MetricServer MetricServer;
        public static readonly Gauge UpGauge = Metrics.CreateGauge("Qlik2DataRobot_IsLogging", "Specifies whether logging is enabled or not, based up error status of the logging connector. 1: Enabled, 0: Disabled");
        public static readonly Counter RequestCounter = Metrics.CreateCounter("Qlik2DataRobot_RequestCounter", "Counts the number of requests made to the connector");
        public static readonly Histogram DurHist = Metrics.CreateHistogram("Qlik2DataRobot_DurationHist", "The Length of Requests", new HistogramConfiguration
        {
            Buckets = new [] {0.0,1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50,60,90,120,240,480}
        }
        );
    }

    class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            
            var appSettings = ConfigurationManager.AppSettings;

            try
            {
                var enableMetricEndpoint = Convert.ToBoolean(appSettings["enableMetricEndpoint"]);
                var metricEndpointPort = Convert.ToInt32(appSettings["metricEndpointPort"]);

                if (enableMetricEndpoint)
                {
                    Qlik2DataRobotMetrics.MetricServer = new MetricServer(metricEndpointPort);
                    Qlik2DataRobotMetrics.MetricServer.Start();
                    //Logger.Info($"Metric Service listening on port:{metricEndpointPort}");
                    Qlik2DataRobotMetrics.UpGauge.Set(1);
                }      
            }
            catch (Exception e)
            {
                Logger.Error($"ERROR: {e.Message}");
            }

            printMessage(Convert.ToInt32(appSettings["grpcPort"]), Convert.ToInt32(appSettings["metricEndpointPort"]));

            Logger.Info(
                $"{Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location)} uses NLog. Set log level by adding or changing logger rules in NLog.config, setting minLevel=\"Info\" or \"Debug\" or \"Trace\".");

            Logger.Info(
                $"Changes to NLog config are immediately reflected in running application, unless you change the setting autoReload=\"true\".");
            Logger.Info($"Logging Enabled - Fatal:{Logger.IsFatalEnabled} Error:{Logger.IsErrorEnabled} Warn:{Logger.IsWarnEnabled} Info:{Logger.IsInfoEnabled} Debug:{Logger.IsDebugEnabled} Trace:{Logger.IsTraceEnabled}");




            var grpcHost = appSettings["grpcHost"];
            var grpcPort = Convert.ToInt32(appSettings["grpcPort"]);
            var certificateFolder = appSettings["certificateFolder"];

            ServerCredentials sslCredentials = null;

            Logger.Info("Looking for certificates according to certificateFolderFullPath in config file.");

            if (certificateFolder.Length > 3)
            {
                var rootCertPath = Path.Combine(certificateFolder, @"root_cert.pem");
                var serverCertPath = Path.Combine(certificateFolder, @"sse_server_cert.pem");
                var serverKeyPath = Path.Combine(certificateFolder, @"sse_server_key.pem");
                if (File.Exists(rootCertPath) &&
                    File.Exists(serverCertPath) &&
                    File.Exists(serverKeyPath))
                {
                    var rootCert = File.ReadAllText(rootCertPath);
                    var serverCert = File.ReadAllText(serverCertPath);
                    var serverKey = File.ReadAllText(serverKeyPath);
                    var serverKeyPair = new KeyCertificatePair(serverCert, serverKey);
                    sslCredentials = new SslServerCredentials(new List<KeyCertificatePair>() { serverKeyPair }, rootCert, true);

                    Logger.Info($"Path to certificates ({certificateFolder}) and certificate files found. Opening secure channel with mutual authentication.");
                }
                else
                {
                    Logger.Error($"Path to certificates ({certificateFolder}) not found or files missing. The gRPC server will not be started.");
                    sslCredentials = null;
                }
            }
            else
            {
                Logger.Info("No certificates defined. Opening insecure channel.");
                sslCredentials = ServerCredentials.Insecure;
            }

            if (sslCredentials != null)
            {
                var server = new Grpc.Core.Server
                {
                    Services = { Qlik.Sse.Connector.BindService(new Qlik2DataRobotConnector()) },
                    Ports = { new ServerPort(grpcHost, grpcPort, sslCredentials) }
                };

                server.Start();
                Logger.Info($"gRPC listening on port {grpcPort}");

                //Logger.Info("Press any key to stop gRPC server and exit...");

                try {
                      while(true) {
                        Thread.Sleep(10000);
                      }
                    } finally {
                      Logger.Info("Shutting down Connector");
                      server.ShutdownAsync().Wait();

                    }
                
            }
            else
            {
                //Logger.Info("Press any key to exit...");

                //Console.ReadKey();
            }

        }

        static void printMessage(int qlikPort, int metricPort)
        {
            string qPort = qlikPort.ToString().PadLeft(8);
            string mPort = metricPort.ToString().PadLeft(8);

            Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.ForegroundColor = ConsoleColor.White;
            Console.Clear();
            Console.Write(@"                                                                                
 ############################################################################## 
 #   ___  _ _ _      ____    ____        _        ____       _           _    # 
 #  / _ \| (_) | __ |___ \  |  _ \  __ _| |_ __ _|  _ \ ___ | |__   ___ | |_  # 
 # | | | | | | |/ /   __) | | | | |/ _` | __/ _` | |_) / _ \| '_ \ / _ \| __| # 
 # | |_| | | |   <   / __/  | |_| | (_| | || (_| |  _ < (_) | |_) | (_) | |_  # 
 #  \__\_\_|_|_|\_\ |_____| |____/ \__,_|\__\__,_|_| \_\___/|_.__/ \___/ \__| # 
 #                                                                            # 
 ############################################################################## 
 #                                     #                                      # 
 #      Qlik Analytic Connector        #      Prometheus Metric Service       # 
 #                                     #                                      # 
 #           Port: {0}            #            Port: {1}            # 
 #                                     #                                      # 
 ############################################################################## 
                                                                                

", qPort, mPort);

            //Console.ResetColor();
        }
    }
}
