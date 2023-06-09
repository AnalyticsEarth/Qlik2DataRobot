using System.ServiceProcess;

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
