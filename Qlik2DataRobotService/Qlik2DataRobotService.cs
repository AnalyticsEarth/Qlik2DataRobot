using Qlik2DataRobot;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

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
