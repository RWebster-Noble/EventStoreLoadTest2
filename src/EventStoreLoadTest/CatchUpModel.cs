using EventStore.ClientAPI;
using System;
using System.Threading;

namespace EventStoreLoadTestConsumer
{    
    public class CatchUpModel
    {
        private readonly string _streamName;
        private int EventCount { get; set; }
        public static int Total;
        private EventWaitHandle _waitHandle;
        const string Group = "G1";

        private long? _checkpoint;
        private EventStoreStreamCatchUpSubscription _subscription;

        public bool SubscriptionUpToDate { get; private set; }


        public CatchUpModel(string streamName, IEventStoreConnection connection)
        {
            _streamName = streamName;
            //CatchUpAndSubscribeToStream();
            SubscribeToStream(connection);
        }

        private void EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(eventStoreCatchUpSubscription.GetHashCode());
            EventCount++;
            Interlocked.Increment(ref Total);

            //if (Total % 10000 == 0)
            //Encoding.ASCII.GetString(x.Event.Data)

            _checkpoint = resolvedEvent.Event.EventNumber;


            Console.WriteLine($"CatchUp: Event #{resolvedEvent.OriginalEventNumber},{resolvedEvent.Event.CreatedEpoch} from Stream {resolvedEvent.Event.EventStreamId} processed, Total: {Total}, {DateTime.Now}");
        }

        private void CatchUpAndSubscribeToStream(IEventStoreConnection connection)
        {
            //CatchUp();
            //SubscribeToStream();

            using (_waitHandle = new EventWaitHandle(false, EventResetMode.AutoReset))
            {
                SubscribeToStream(connection);
                _waitHandle.WaitOne();
            }
        }

        private void SubscribeToStream(IEventStoreConnection connection)
        {
            if (_subscription != null)
            {
                _subscription.Stop();
                _subscription = null;
            }

            _subscription = connection.SubscribeToStreamFrom(_streamName, _checkpoint,
                CatchUpSubscriptionSettings.Default,
                EventAppeared, LiveProcessingStarted, SubscriptionDropped);
        }

        //private void CatchUp()
        //{
        //    //var streamEvents = new List<ResolvedEvent>();

        //    StreamEventsSlice currentSlice;
        //    var nextSliceStart = _checkpoint ?? 0;
        //    do
        //    {
        //        currentSlice = Program.Connection.ReadStreamEventsForwardAsync(_streamName, nextSliceStart, 200, false).Result;
        //        nextSliceStart = currentSlice.NextEventNumber;

        //        foreach (var evt in currentSlice.Events)
        //        {
        //            EventAppeared(null, evt);
        //        }

        //        //streamEvents.AddRange(currentSlice.Events);
        //    } while (!currentSlice.IsEndOfStream);
        //}

        private void LiveProcessingStarted(EventStoreCatchUpSubscription obj)
        {
            Console.WriteLine("CatchUp: Caught up");

            if (_waitHandle != null && !_waitHandle.GetSafeWaitHandle().IsClosed)
                _waitHandle.Set();

            SubscriptionUpToDate = true;
        }

        private void SubscriptionDropped(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, SubscriptionDropReason dropReason, Exception ex)
        {
            SubscriptionUpToDate = false;
            //Console.WriteLine($"Subscription to {supscription.StreamId} dropped! Reason: {dropReason.ToString()}");

            //if (ex != null)
                Console.WriteLine($"CatchUp: SubscriptionDropped, Exception: {ex}");

            void ResubscribeOnConnected(IEventStoreConnection connection)
            {
                Program.EsConnectedEvent -= ResubscribeOnConnected;
                CatchUpAndSubscribeToStream(connection);
                Console.WriteLine($"CatchUp: Resubscribeing asynchronously to {_streamName} from {_checkpoint}");                
            }

            Program.EsConnectedEvent += ResubscribeOnConnected;

            if (_waitHandle != null && !_waitHandle.GetSafeWaitHandle().IsClosed)
                _waitHandle.Set();
        }
    }
}
