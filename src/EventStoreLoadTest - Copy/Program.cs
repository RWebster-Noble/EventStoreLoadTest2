using EventStore.ClientAPI;
using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStoreLoadTestProducer
{
    public static class Program
    {
        private const int DefaultPort = 1113;
        //private const int TotalStreams = 0; // 4 million
        //private const int EventsPerStream = 0;
        private const string StreamName = "TEST_STREAM-";


        //private static Random rnd = new Random();

        public static bool Ready { get; set; }
        public static IEventStoreConnection Connection;

        //private const string EventStoreLiveUri = "tcp://smo:changeit@eventstore-api.arnoldclark.com:1115";

        //static List<Model> _models = new List<Model>(TOTAL_STREAMS);


        public static void Main(string[] args)
        {
            Console.Title = "Event Store Load Test: Producer";
            var settings = ConnectionSettings.Create();

            //settings.UseConsoleLogger();
            //settings.EnableVerboseLogging();

            //settings.KeepReconnecting();
            ////settings.KeepRetrying();
            //settings.LimitReconnectionsTo(1);
            //settings.LimitAttemptsForOperationTo(1);
            //settings.LimitRetriesForOperationTo(1);
            //settings.SetOperationTimeoutTo(new TimeSpan(0, 0, 0, 2));

            //settings.LimitReconnectionsTo(0); ;
            //settings.LimitAttemptsForOperationTo(1);
            //settings.LimitRetriesForOperationTo(1);
            //settings.SetOperationTimeoutTo(TimeSpan.FromSeconds(1));
            //settings.WithConnectionTimeoutOf(TimeSpan.FromSeconds(1));
            //settings.SetReconnectionDelayTo(TimeSpan.FromSeconds(1));
            //settings.LimitOperationsQueueTo(1);


            try
            {
                CreateConnection(settings);
                for (int i = 0; i < 5; i++)
                {
                    RaiseEventFor(i);
                }

                //Console.WriteLine($"Producer: Begin Ganerating {TotalStreams} Streams with {EventsPerStream + 1} events in each, totaling {TotalStreams * (EventsPerStream + 1)} events, at {DateTime.Now}");
                //for (int i = 1; i <= TotalStreams; i++)
                ////Parallel.For(0, TOTAL_STREAMS, i =>
                //{
                //    RaiseEventFor(i);


                //    //for (int j = 0; j < EventsPerStream; j++)
                //    //{
                //    //    //int randomStream = rnd.Next(Math.Max(streamNumber - 10, 1), streamNumber);
                //    //    //int actual = (int)Math.Ceiling((1.0 / ((double)streamNumber)+1) * (double)randomStream);
                //    //    int randomStream = i;
                //    //    string thisStreamName = StreamName + randomStream;
                //    //    connection.AppendToStreamAsync(thisStreamName,
                //    //        ExpectedVersion.Any,
                //    //        RaiseEventFor(i)).Wait();
                //    //}
                //    //Thread.Sleep(20);

                //    if (i % 10000 == 0)
                //        Console.WriteLine($"Events For Stream: {i} at {DateTime.Now}");
                //}//);
                //Console.WriteLine($"{TotalStreams} Streams with {EventsPerStream + 1} events in each, totaling {TotalStreams*(EventsPerStream + 1)} events. Made at {DateTime.Now}");
                //RaiseEventFor(1);
                Console.WriteLine("Press 'x' to quit or 'e' to create an event");

                //while (true)
                //{
                //    RaiseEventFor(1);
                //    Thread.Sleep(5000);
                //}
                string input;
                do
                {
                    input = Console.ReadLine();
                    switch (input)
                    {
                        case "e":
                            RaiseEventFor(1);
                            break;
                    }
                } while (input != "x");

            }
            finally
            {
               Connection?.Close(); 
            }
        }

        private static void CreateConnection(ConnectionSettingsBuilder settings)
        {
            Ready = false;
            var waitTillConnectionConfirmed = new EventWaitHandle(false, EventResetMode.ManualReset);

            //Connection?.Close();

            var newConnection = EventStoreConnection.Create(settings, new IPEndPoint(IPAddress.Loopback, DefaultPort));
            //var newConnection = EventStoreConnection.Create(settings, new Uri(EventStoreLiveUri));

            newConnection.Connected += (sender, a) =>
            {
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        var eventData = new EventData(
                            Guid.NewGuid(),
                            "connectionTest",
                            true,
                            null,
                            null
                        );
                        newConnection.AppendToStreamAsync("connectionTest", ExpectedVersion.Any, eventData).Wait();
                        Ready = true;
                        Console.WriteLine("Connected");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                    waitTillConnectionConfirmed.Set();
                });

                Console.WriteLine("startConnected");
            };
            newConnection.Reconnecting += (sender, a) =>
            {
                Ready = false;
                Console.WriteLine("Reconnecting");
            };
            newConnection.Closed += (sender, a) =>
            {
                Ready = false;
                Console.WriteLine("Closed");
                waitTillConnectionConfirmed.Set();
                CreateConnection(settings); 
            };
            newConnection.Disconnected += (sender, a) =>
            {
                Ready = false;
                Console.WriteLine("Disconnected");
            };

            newConnection.ConnectAsync().Wait();

            waitTillConnectionConfirmed.WaitOne();

            Connection = newConnection;
        }

        private static void RaiseEventFor(int i)
        {
            if(!Ready)
            {
                Console.WriteLine("Conneection Not Ready");
                return;
            }

            Console.WriteLine("Writing Data");

            var evt =  new EventData(
                Guid.NewGuid(),
                "eventType",
                true,
                Encoding.ASCII.GetBytes("{\"e\" : " + i + "}"),
                null
                );

            try
            {
                Connection.AppendToStreamAsync(StreamName + i, ExpectedVersion.Any, evt).Wait();
                Console.WriteLine("Event Written Successfully");
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception while writing event. Exception:");
                Console.WriteLine(e);
            }
        }
    }


    
}
