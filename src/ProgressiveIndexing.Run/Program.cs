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
        // Read config or defaults
        static int jobId = 0;
        static bool wantError = false;
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

                // Read config or defaults
                jobId = ConfigHelper.GetInt("JobId", 2);
                wantError = ConfigHelper.GetBoolean("WantToThrowError", true);
                failRecordId = ConfigHelper.GetLong("FailRecordId", 576212);

                var folders = new Dictionary<long, List<long>>
                {
                    { 576210, new List<long>{ 37174 } },
                    { 576302, new List<long>{ 37173, 57617, 576212 } },
                    { 576303, new List<long>{ 37175 } },
                };

                var job = new JobIndexer(jobId, folders);
                await job.RunAsync(wantError, failRecordId);
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
                    log.Info($"Completed with Success! JobId={jobId}.");
                    log.Info($"DONE! JobId={jobId}.");
                }
                else log.Info($"Completed with Failure. JobId={jobId}.");
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

