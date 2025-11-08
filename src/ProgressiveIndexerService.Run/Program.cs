using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using log4net;

namespace ProgressiveIndexerService.Run
{
    class Program
    {
        static int jobId = 0;
        static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        static async Task Main()
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;

            try
            {
                Console.InputEncoding = Encoding.UTF8;
                Console.OutputEncoding = Encoding.UTF8;

                log4net.Config.XmlConfigurator.Configure();

                jobId = ConfigHelper.GetInt("ErrorSimulationJobId");

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

                List<long> oids = new() { 576210, 576211, 576212, 576213, 576214, 576215, 576216, 576217, 576218, 576219, 576220, 576221, 576222, 576223, 576224, 576225, 576226, 576227, 576228, 576229, 576230, 576231, 576232, 576233, 576234, 576235, 576236, 576237, 576238, 576239, 576240, 576241, 576242, 576243, 576244, 576245, 576246, 576247, 576248, 576249, 576250, 576251, 576252, 576253, 576254, 576255, 576256, 576257, 576258, 576259, 576260, 576261, 576262, 576263, 576264, 576265, 576266, 576267, 576268, 576269, 576270, 576271, 576272, 576273, 576274, 576275, 576276, 576277, 576278, 576279, 576280, 576281, 576282, 576283, 576284, 576285, 576286, 576287, 576288, 576289, 576290, 576291, 576292, 576293, 576294, 576295, 576296, 576297, 576298, 576299, 576300, 576301, 576302, 576303, 576304, 576305, 576306, 576307, 576308, 576309 };

                var jobIndexer = new JobIndexer(jobId, oids);
                await jobIndexer.RunAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n❌ Lỗi không mong muốn: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                log.Info($"DONE! JobId = {jobId}.");
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
