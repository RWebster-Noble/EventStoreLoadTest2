using EventStore.ClientAPI;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace EventStoreLoadTestConsumer
{

    public static class Program
    {
        private const int DefaultPort = 1113;
        private const int TotalStreams = 1; // 4 million
        //private const string StreamName = "TEST_STREAM-";
        private const string StreamName = "$et-TEST_STREAM";
        //private static Random _rnd = new Random();

        //private const string EventStoreLiveUri = "tcp://smo:changeit@eventstore-api.arnoldclark.com:1115";

        public static IEventStoreConnection Connection { get; private set; }


        //public delegate void ChangedEventHandler(object sender, EventArgs e);
        //public event ChangedEventHandler 
        //public event ChangedEventHandler EsConnectedEvent { get; set; }

        public delegate void EsConnectedEventHandeler(IEventStoreConnection connection);

        public static event EsConnectedEventHandeler EsConnectedEvent;

        //static List<PersistentModel> _models = new List<PersistentModel>(TOTAL_STREAMS);

        public static void Main(string[] args)
        {
            Console.Title = "Event Store Load Test: Consumer";
            var settings = ConnectionSettings.Create();

            //settings.UseConsoleLogger();
            //settings.EnableVerboseLogging();

            //settings.KeepReconnecting();
            //settings.KeepRetrying();

            settings.LimitReconnectionsTo(1);
            settings.LimitAttemptsForOperationTo(1);
            settings.LimitRetriesForOperationTo(1);
            settings.SetOperationTimeoutTo(new TimeSpan(0, 0, 0, 2));



            //settings.LimitReconnectionsTo(0);;
            //settings.LimitAttemptsForOperationTo(1);
            //settings.LimitRetriesForOperationTo(1);
            //settings.SetOperationTimeoutTo(TimeSpan.FromSeconds(1));
            //settings.WithConnectionTimeoutOf(TimeSpan.FromSeconds(1));
            //settings.SetReconnectionDelayTo(TimeSpan.FromSeconds(1));
            //settings.LimitOperationsQueueTo(1);


            try
            {
                CreateConnection(settings);

                Console.WriteLine($"Consumer: Begin Subscribing to using {TotalStreams} streams at: {DateTime.Now}");
                for (int i = 1; i <= TotalStreams; i++)
                //Parallel.For(0, TotalStreams, i =>
                {
                    //_models.Add(new PersistentModel(connection, thisStreamName));
                    //var unused = new PersistentModel(StreamName + i, Connection);
                    //var unused2 = new CatchUpModel(StreamName + i, Connection);
                    //var unused = new PersistentModel("$et-eventType", Connection);
                    var unused2 = new CatchUpModel("$et-eventType", Connection);
                    //Thread.Sleep(20);

                    if (i % 10000 == 0)
                        Console.WriteLine($"PersistentModel & Subscription: {i}, Total: {PersistentModel.Total} at: {DateTime.Now}");
                    Console.WriteLine($"CatchUpModel & Subscription: {i}, Total: {CatchUpModel.Total} at: {DateTime.Now}");
                }
                //);

                //Thread.Sleep(1 * 60 * 1000);

                //Console.WriteLine($"All Done!, total: { + _models.Sum(x => x.EventCount)} at , {DateTime.Now}");
                Console.WriteLine($"All Done!, Persistent total: {PersistentModel.Total} at {DateTime.Now}");
                Console.WriteLine($"All Done!, CatchUp total: {PersistentModel.Total} at {DateTime.Now}");


                Console.WriteLine("Press x to quit");
                while (Console.ReadLine() != "x")
                {
                    Console.WriteLine($"Persistent total now: {PersistentModel.Total}");
                    Console.WriteLine($"CatchUp total now: {CatchUpModel.Total}");
                }
            }
            finally
            {
                Connection?.Close();
            }
        }

        private static readonly EventWaitHandle WaitTillDoneReSubscribing = new EventWaitHandle(true, EventResetMode.ManualReset);

        private static void CreateConnection(ConnectionSettingsBuilder settings)
        {
            
            WaitTillDoneReSubscribing.WaitOne();

            Connection?.Dispose();
            //var newConnection = EventStoreConnection.Create(settings, new Uri(EventStoreLiveUri));
            var newConnection = EventStoreConnection.Create(settings, new IPEndPoint(IPAddress.Loopback, DefaultPort));

            newConnection.Connected += (connection, a) =>
            {
                WaitTillDoneReSubscribing.Reset();
                Console.WriteLine("Connected");

                //if (_esConnectedTask == null || _esConnectedTask.IsCompleted)
                //{
                Task.Factory.StartNew(() =>
                {
                    EsConnectedEvent?.Invoke(newConnection);
                    Console.WriteLine("Reconnected all subscriptions");
                    WaitTillDoneReSubscribing.Set();
                });
                //}
                //else
                //    Console.WriteLine("ALREADY RECONNECTING!");

                Console.WriteLine("EsConnectedEvent Invoked");

            };
            newConnection.Reconnecting += (sender, a) =>
            {
                Console.WriteLine("Reconnecting");
            };
            newConnection.Closed += (sender, a) =>
            {
                //waitTillDoneReSubscribing.Set();
                Console.WriteLine("Closed");
                //var esClosedTask = new Task(() =>
                //{
                CreateConnection(settings);
                //});
                //esClosedTask.Start();
            };
            //newConnection.Disconnected += (sender, a) =>
            //{
            //    Console.WriteLine("Disconnected");
            //};

            newConnection.ConnectAsync().Wait();

            Connection = newConnection;
        }
    }
}
