using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using log4net;

namespace ProgressiveIndexing.Run
{
    class Program
    {
        static int jobId = 0;
        static bool errorSimulationFlag = false;
        static long failRecordId = 0;
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        static async Task Main()
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;

            bool success = true;

            try
            {
                Console.InputEncoding = Encoding.UTF8;
                Console.OutputEncoding = Encoding.UTF8;

                log4net.Config.XmlConfigurator.Configure();

                errorSimulationFlag = ConfigHelper.GetBoolean("ErrorSimulationFlag");
                jobId = ConfigHelper.GetInt("ErrorSimulationJobId");
                failRecordId = ConfigHelper.GetLong("ErrorSimulationRecordId");

                if (jobId <= 0)
                {
                    Console.WriteLine("JobID không hợp lệ!");
                    return;
                }

                log.Info($"Started. JobId = {jobId}.");

                AppDomain.CurrentDomain.UnhandledException += (s, e) =>
                {
                    Console.WriteLine("🔴 UNHANDLED EXCEPTION: " + ((Exception)e.ExceptionObject).Message);
                };

                var folders = new Dictionary<long, List<long>>
                {
                    { 576210, new List<long>{ 576210, 576211, 576212, 576213, 576214, 576215, 576216, 576217, 576218, 576219, 576220, 576221, 576222, 576223, 576224, 576225, 576226, 576227, 576228, 576229, 576230, 576231, 576232, 576233, 576234, 576235, 576236, 576237, 576238, 576239, 576240, 576241, 576242, 576243, 576244, 576245, 576246, 576247, 576248, 576249, 576250, 576251, 576252, 576253, 576254, 576255, 576256, 576257, 576258, 576259, 576260, 576261, 576262, 576263, 576264, 576265, 576266, 576267, 576268, 576269, 576270, 576271, 576272, 576273, 576274, 576275, 576276, 576277, 576278, 576279, 576280, 576281, 576282, 576283, 576284, 576285, 576286, 576287, 576288, 576289, 576290, 576291, 576292, 576293, 576294, 576295, 576296, 576297, 576298, 576299, 576300, 576301, 576302, 576303, 576304, 576305, 576306, 576307, 576308, 576309 } },
                    { 576302, new List<long>{ 586210, 586211, 586212, 586213, 586214, 586215, 586216, 586217, 586218, 586219, 586220, 586221, 586222, 586223, 586224, 586225, 586226, 586227, 586228, 586229, 586230, 586231, 586232, 586233, 586234, 586235, 586236, 586237, 586238, 586239, 586240, 586241, 586242, 586243, 586244, 586245, 586246, 586247, 586248, 586249, 586250, 586251, 586252, 586253, 586254, 586255, 586256, 586257, 586258, 586259, 586260, 586261, 586262, 586263, 586264, 586265, 586266, 586267, 586268, 586269, 586270, 586271, 586272, 586273, 586274, 586275, 586276, 586277, 586278, 586279, 586280, 586281, 586282, 586283, 586284, 586285, 586286, 586287, 586288, 586289, 586290, 586291, 586292, 586293, 586294, 586295, 586296, 586297, 586298, 586299, 586300, 586301, 586302, 586303, 586304, 586305, 586306, 586307, 586308, 586309 } }
                };

                var job = new JobIndexer(jobId, folders);
                await job.RunAsync(errorSimulationFlag, failRecordId);
            }
            catch (Exception ex)
            {
                log.Error(ex.Message, ex);
                success = false;
                Console.WriteLine("\n❌ Lỗi không mong muốn: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
                log.Error(ex.Message, ex);
            }
            finally
            {
                if (success)
                {
                    log.Info($"Completed with Success! JobId = {jobId}.");
                    log.Info($"DONE! JobId = {jobId}.");
                }
                else log.Info($"Completed with Failure. JobId = {jobId}.");
            }
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Exception ex = (Exception)e.ExceptionObject;
            Console.WriteLine("\n🔴 UNHANDLED EXCEPTION:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Environment.Exit(1);
        }

        private static void TaskScheduler_UnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            Console.WriteLine("\n🔴 UNOBSERVED TASK EXCEPTION:");
            Console.WriteLine(e.Exception.Message);
            Console.WriteLine(e.Exception.StackTrace);
            e.SetObserved();
        }
    }
}

