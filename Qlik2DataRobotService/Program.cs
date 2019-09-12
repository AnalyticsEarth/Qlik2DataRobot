using Qlik2DataRobot;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Qlik2DataRobotService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            //ConnectorLauncher launcher = new ConnectorLauncher();
            //launcher.Launch(true);
            //Thread.Sleep(Timeout.Infinite);

            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new Qlik2DataRobotService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
