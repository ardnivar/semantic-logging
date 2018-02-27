using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;

namespace MGS.InfluxDbMetrics
{
  public sealed class InfluxDbSink : IObserver<EventEntry>, IDisposable
  {
    private readonly int waitWriteTime;
    private const int MaxBufferSize = 10 * 1024;
    private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

    private readonly BufferedEventPublisher<EventEntry> _bufferedPublisher;
    private readonly InfluxDb influxDb;

    public InfluxDbSink(string url, string databaseName, string precision, int bufferInterval, int bufferingCount, int waitWriteTimeInMilliseconds)
    {
      DebugLogging.Log("Starting up in ctor");
      this.waitWriteTime = waitWriteTimeInMilliseconds;
      var timespan = new TimeSpan(0, 0, 0, 0, bufferInterval);

      var sinkId = "InfluxDb-" + Guid.NewGuid().ToString("N");
      _bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(sinkId, PublishEventsAsync, timespan, bufferingCount, MaxBufferSize, cancellationTokenSource.Token);

      influxDb = new InfluxDb(url, databaseName, precision);
      DebugLogging.Log("Leaving ctor");
    }

    public void OnCompleted()
    {
      DebugLogging.Log("In OnCompleted");
      _bufferedPublisher.FlushAsync().Wait(waitWriteTime);
      Dispose();
      DebugLogging.Log("Leaving OnCompleted");
    }

    public void OnError(Exception error)
    {
      DebugLogging.Log("Entering OnError : " + error.Message);
      Dispose();
      DebugLogging.Log("Leaving OnError");
    }

    public void OnNext(EventEntry value)
    {
      try
      {
        DebugLogging.Log("Entering OnNext");
        if (value == null)
        {
          DebugLogging.Log("Leaving abnormally OnNext");
          return;
        }

        if (!_bufferedPublisher.TryPost(value))
        {
          SemanticLoggingEventSource.Log.CustomSinkUnhandledFault("InfluxDbMetrics : Unable to post to BufferedEventPublisher.");
        }
        DebugLogging.Log("Leaving normally OnNext");
      }
      catch (Exception e)
      {
        DebugLogging.Log("OnNext : " + e.Message);
        throw;
      }
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
      cancellationTokenSource.Cancel();
      _bufferedPublisher.Dispose();
    }

    internal async Task<int> PublishEventsAsync(IList<EventEntry> collection)
    {
      try
      {
        DebugLogging.Log("Entering PublishEventsAsync");
        var events = CreateInfluxDbEventList(collection);

        DebugLogging.Log("Entering BulkWriteAsync");
        await influxDb.BulkWriteAsync(events);
      }
      catch (Exception ex)
      {
        DebugLogging.Log($"Exception PublishEventsAsync {ex.Message}");
        SemanticLoggingEventSource.Log.CustomSinkUnhandledFault(ex.Message);
      }

      return 0;
    }

    private IReadOnlyCollection<InfluxDbEvent> CreateInfluxDbEventList(IList<EventEntry> collection)
    {
      var events = new List<InfluxDbEvent>(collection.Count);

      foreach (var eventEntry in collection)
      {
        var influxEvent = new InfluxDbEvent
        {
          Tags = new Dictionary<string, string>()
        };

        for (var i = 0; i < eventEntry.Schema.Payload.Length; i++)
        {
          var payloadName = eventEntry.Schema.Payload[i];
          var payloadValue = eventEntry.Payload[i];

          switch (payloadName.ToLower())
          {
            case "metrictype":
              influxEvent.Measurement = payloadValue.ToString();
              break;
            case "value":
              influxEvent.Value = (long)payloadValue;
              break;
            case "currenttime":
              influxEvent.DateTime = payloadValue.ToString();
              break;
            default:  // Add all other data as tags. We don't support lists of fields. 
              influxEvent.Tags.Add(payloadName, payloadValue.ToString());
              break;
          }
        }

        events.Add(influxEvent);
      }

      return events;
    }
  }
}
