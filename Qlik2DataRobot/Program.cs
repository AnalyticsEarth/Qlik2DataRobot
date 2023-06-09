
/* Unmerged change from project 'Qlik2DataRobotWin'
Before:
using System;
using System.Collections.Generic;
using System.IO;
using Grpc.Core;
using NLog;
using System.Configuration;
using System.Threading;
using Prometheus;
After:
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NLog;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
*/
using System.Threading;
/* Unmerged change from project 'Qlik2DataRobotWin'
Before:
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
After:
using System.Reflection;
using System.Threading;
*/


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
