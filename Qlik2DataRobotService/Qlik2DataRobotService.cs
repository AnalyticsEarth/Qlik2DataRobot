using Qlik2DataRobot;
using System.ServiceProcess;

namespace Qlik2DataRobotService
{
    public partial class Qlik2DataRobotService : ServiceBase
    {
        ConnectorLauncher launcher;

        public Qlik2DataRobotService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            launcher = new ConnectorLauncher();
            launcher.Launch(true);
        }

        protected override void OnStop()
        {
            launcher.Shutdown();
        }
    }
}
