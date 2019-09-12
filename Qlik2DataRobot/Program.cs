using System;
using System.Collections.Generic;
using System.IO;
using Grpc.Core;
using NLog;
using System.Configuration;
using System.Threading;
using Prometheus;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace Qlik2DataRobot
{
   

    class Program
    {
        

        static void Main(string[] args)
        {
            ConnectorLauncher launcher = new ConnectorLauncher();
            launcher.Launch(false);
            Thread.Sleep(Timeout.Infinite);
        }

        
    }
}
