using System;
using System.Threading;

using DemoApp.Demos;

using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Serilog;

namespace DemoApp
{
    internal sealed class Program
    {
        private const string BrokerEndpoints = "127.0.0.1";
        private const string Topic = "dotnext-moscow-2017";

        private static void Main(string[] args)
        {
            var globalScope = ConfigureServices();

            var loggerFactory = globalScope.ServiceProvider.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<Program>();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
                                          {
                                              logger.LogInformation("Application is shutting down...");
                                              cts.Cancel();
                                              globalScope.Dispose();

                                              eventArgs.Cancel = true;
                                          };

            var app = new CommandLineApplication { Name = "DemoApp" };
            app.HelpOption("-h|--help");
            app.OnExecute(
                () =>
                    {
                        Console.WriteLine("Demo application.");
                        app.ShowHelp();
                        return 0;
                    });

            app.Command(
                "producer-demo",
                config =>
                    {
                        config.HelpOption("-h|--help");
                        config.OnExecute(
                            () =>
                                {
                                    using (var demo = new ProducerDemo(logger, BrokerEndpoints, Topic))
                                    {
                                        demo.RunAsync(cts.Token).GetAwaiter().GetResult();
                                        return 0;
                                    }
                                });
                    });

            app.Command(
                "batched-demo",
                config =>
                    {
                        config.HelpOption("-h|--help");
                        config.OnExecute(
                            () =>
                                {
                                    using (var demo = new BatchedConsumerDemo(logger, BrokerEndpoints, new[] { Topic }, 3))
                                    {
                                        demo.RunAsync(cts.Token).GetAwaiter().GetResult();
                                        return 0;
                                    }
                                });
                    });

            var exitCode = 0;
            try
            {
                exitCode = app.Execute(args);
            }
            catch (CommandParsingException ex)
            {
                ex.Command.ShowHelp();
                exitCode = 1;
            }
            catch (Exception ex)
            {
                logger.LogCritical(new EventId(), ex, "Unexpected error occured. See logs for details.");
                exitCode = -1;
            }
            finally
            {
                logger.LogInformation("DemoApp is shutting down with code {exitCode}.", exitCode);
            }

            Environment.Exit(exitCode);
        }

        private static Serilog.ILogger SetupSerilog()
            => Log.Logger = new LoggerConfiguration()
                   .WriteTo.Console()
                   .CreateLogger();

        private static IServiceScope ConfigureServices()
        {
            var logger = SetupSerilog();

            var services = new ServiceCollection();
            services.AddLogging(builder => builder.AddSerilog(logger, true));

            return services.BuildServiceProvider().CreateScope();
        }
    }
}