using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Models;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;

namespace MGS.InfluxDbMetrics
{
  public sealed class InfluxDbSink : IObserver<EventEntry>, IDisposable
  {
    private const int MaxBufferSize = 10 * 1024;
    private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

    private readonly BufferedEventPublisher<EventEntry> _bufferedPublisher;

    private readonly InfluxDbClient _influxDbClient;
    private readonly string _databaseName;
    private readonly string _precision;

    public InfluxDbSink(string url, string databaseName, string precision, int bufferInterval, int bufferingCount, string username, string password)
    {
      DebugLogging.Log("Starting up in ctor");
      var timespan = TimeSpan.FromMilliseconds(bufferInterval);

      var sinkId = "InfluxDb-" + Guid.NewGuid().ToString("N");
      _bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(sinkId, PublishEventsAsync, timespan, bufferingCount, MaxBufferSize, cancellationTokenSource.Token);

      _databaseName = databaseName;
      _precision = precision;

      _influxDbClient = new InfluxDbClient(url, username, password, InfluxDbVersion.Latest);

      DebugLogging.Log("Leaving ctor");
    }

    public void OnCompleted()
    {
      DebugLogging.Log("In OnCompleted");
      //      _bufferedPublisher.FlushAsync().Wait(waitWriteTime);
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
      DebugLogging.Log("Publishing Collection Count " + collection.Count);

      try
      {
        var points = CreatePoints(collection as IReadOnlyCollection<EventEntry>);

        var response = await _influxDbClient.Client.WriteAsync(points, _databaseName, null, _precision);

        if (!response.Success)
        {
          DebugLogging.Log($"Error : Response from InfluxDb {response.StatusCode} : Body {response.Body}");
          return 0;
        }

        return collection.Count;
      }
      catch (Exception ex)
      {
        DebugLogging.Log($"Exception PublishEventsAsync {ex.Message}");
        SemanticLoggingEventSource.Log.CustomSinkUnhandledFault(ex.Message);
      }

      return 0;
    }

    private IEnumerable<Point> CreatePoints(IReadOnlyCollection<EventEntry> collection)
    {
      var points = new List<Point>();

      foreach (var eventEntry in collection)
      {
        var point = new Point
        {
          Tags = new Dictionary<string, object>(),
          Fields = new Dictionary<string, object>()
        };

        for (var i = 0; i < eventEntry.Schema.Payload.Length; i++)
        {
          var payloadName = eventEntry.Schema.Payload[i];
          var payloadValue = eventEntry.Payload[i];

          switch (payloadName.ToLower())
          {
            case "metrictype":
              point.Name = payloadValue.ToString();
              break;
            case "value":
              point.Fields.Add("value", (long)payloadValue);
              break;
            case "currenttime":
              point.Timestamp = DateTime.Parse(payloadValue.ToString(), null, DateTimeStyles.AssumeUniversal);
              break;
            default:  // Add all other data as tags. We don't support lists of fields. 
              point.Tags.Add(payloadName, payloadValue.ToString());
              break;
          }
        }

        points.Add(point);
      }

      return points;
    }
  }
}
