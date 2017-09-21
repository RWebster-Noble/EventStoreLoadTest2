using EventStore.ClientAPI;
using System;
using System.Threading;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;

namespace EventStoreLoadTestConsumer
{    
    public class PersistentModel
    {
        private readonly string _streamName;
        private int EventCount { get; set; }
        public static int Total;
        const string Group = "G1";

        private long? _checkpoint;
        private EventStorePersistentSubscriptionBase _subscription;

        public bool SubscriptionUpToDate { get; private set; }

        public PersistentModel(string streamName, IEventStoreConnection connection)
        {
            _streamName = streamName;
            //CatchUpAndSubscribeToStream();
            SubscribeToStream(connection);
        }

        private void EventAppeared(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase, ResolvedEvent resolvedEvent)
        {
            Console.WriteLine(eventStorePersistentSubscriptionBase.GetHashCode());
            EventCount++;
            Interlocked.Increment(ref Total);

            //if (Total % 10000 == 0)
            Console.WriteLine($"Persistant: Event from Stream {resolvedEvent.Event.EventStreamId} processed, Total: {Total}, {DateTime.Now}");
            //Encoding.ASCII.GetString(x.Event.Data)
            
            _checkpoint = resolvedEvent.Event.EventNumber;
        }

        private void CreateSubscription(IEventStoreConnection connection)
        {
            PersistentSubscriptionSettings settings = PersistentSubscriptionSettings.Create().StartFromCurrent();
            try
            {
                connection.CreatePersistentSubscriptionAsync(_streamName, Group, settings, new UserCredentials("admin", "changeit")).Wait();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException != null && (ex.InnerException.GetType() != typeof(InvalidOperationException)
                    && ex.InnerException?.Message != $"Subscription group {Group} on stream {_streamName} already exists"))
                {
                    Console.WriteLine("Persistant Subscription: CreateSubscription exception :" + ex);
                    throw;
                }
            }
        }

        private void SubscribeToStream(IEventStoreConnection connection)
        {
            if (_subscription != null)
            {
                _subscription.Stop(TimeSpan.FromDays(1));
                _subscription = null;
            }

            try
            {
                CreateSubscription(connection);
                _subscription = connection.ConnectToPersistentSubscriptionAsync(_streamName, Group, EventAppeared, SubscriptionDropped).Result;

            }
            catch (Exception e) when (e is ConnectionClosedException || e is ObjectDisposedException || e.InnerException is ConnectionClosedException || e.InnerException is ObjectDisposedException)
            {
                SubscriptionDropped(null, SubscriptionDropReason.ConnectionClosed, e);
            }
        }

        private void SubscriptionDropped(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase, SubscriptionDropReason dropReason, Exception ex)
        {
            SubscriptionUpToDate = false;
            //Console.WriteLine($"Subscription to {supscription.StreamId} dropped! Reason: {dropReason.ToString()}");

            //if (ex != null)
                Console.WriteLine($"Persistant: SubscriptionDropped, Exception: {ex}");

            void ResubscribeOnConnected(IEventStoreConnection connection)
            {
                SubscribeToStream(connection);
                Console.WriteLine($"Persistant: Resubscribeing asynchronously to {_streamName} from {_checkpoint}");
                Program.EsConnectedEvent -= ResubscribeOnConnected;
            }

            Program.EsConnectedEvent += ResubscribeOnConnected;
        }
    }
}
