using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Polly;

namespace MGS.InfluxDbMetrics
{
  public class InfluxDbEvent
  {
    public string Measurement { get; set; }
    public IDictionary<string, string> Tags { get; set; }
    public long Value { get; set; }
    public string DateTime { get; set; }
  }

  public class InfluxDb
  {
    private readonly string _url;

    private static readonly DateTime Epoch0 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly HttpClient _client = new HttpClient();
    private const int StringBuilderInitialCapacity = 512;
    private const string BulkLineTerminator = "\n";
    private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

    /// <summary>
    /// Constructs the class.
    /// </summary>
    /// <param name="influxDbUrl">Url to InfluxDb.</param>
    /// <param name="influxDbName">Name of the database to write to.</param>
    /// <param name="precision">The precision for the time. Default in milliseconds.</param>
    public InfluxDb(string influxDbUrl, string influxDbName, string precision = "ms")
    {
      _url = $"{influxDbUrl}/write?db={influxDbName}&precision={precision}";
    }

    /// <summary>
    /// Writes a single event to InfluxDB with a single field.
    /// </summary>
    /// <param name="measurement">The measurement name. InfluxDB accepts one measurement per point.</param>
    /// <param name="tags">All tag key-field pairs for the point.Tag keys and tag fields are both strings.</param>
    /// <param name="fieldValue">Required. Points must have at least one field.Field keys are strings. Field values can be floats, integers, strings, or booleans.</param>
    /// <param name="timestamp">The UTC time in milliseconds. If the value is 0, then the current time is calculated and used.</param>
    public async Task WriteAsync(string measurement, IReadOnlyDictionary<string, string> tags, long fieldValue, long timestamp = 0)
    {
      if (String.IsNullOrWhiteSpace(measurement))
      {
        throw new ArgumentNullException(nameof(measurement));
      }

      var payload = BuildLinePrototcol(measurement, tags, fieldValue, timestamp);
      await SendAsync(payload);
    }

    /// <summary>
    /// Writes multiple events to InfluxDb in a single call.
    /// </summary>
    /// <param name="events" cref="InfluxDbEvent">List of events to send to InfluxDb.</param>
    public async Task BulkWriteAsync(IReadOnlyCollection<InfluxDbEvent> events)
    {
      if (events == null || events.Count == 0)
      {
        return;
      }

      var sb = new StringBuilder(StringBuilderInitialCapacity);
      foreach (var ev in events)
      {
        if (ev.Measurement == null)
        {
          continue;
        }

        DateTime timestamp;
        if (!DateTime.TryParse(ev.DateTime, out timestamp))
        {
          timestamp = DateTime.UtcNow;
        }

        var newTags = ev.Tags as IReadOnlyDictionary<string, string>;
        var payload = BuildLinePrototcol(ev.Measurement, newTags, ev.Value, GetTimeInMilliseconds(timestamp));
        sb.Append($"{payload}{BulkLineTerminator}");
      }

      var policy = Policy
        .Handle<WebException>()
        .Or<HttpRequestException>()
        .Or<TaskCanceledException>()
        .WaitAndRetryAsync(new[]
        {
          TimeSpan.FromMilliseconds(200),
          TimeSpan.FromMilliseconds(400),
          TimeSpan.FromMilliseconds(600)
        }, (exception, span) => { Debug.Write($"InfluxDb http error : {exception.Message}"); });

      DebugLogging.Log("SendAsync");
      await policy.ExecuteAsync(() => SendAsync(sb.ToString()));
    }

    /*
      2xx: If your write request received HTTP 204 No Content, it was a success!
      4xx: InfluxDB could not understand the request.
      5xx: The system is overloaded or significantly impaired.         
     */
    private async Task SendAsync(string payload)
    {
      using (var content = new StringContent(payload))
      {
        using (var responseInfo = await _client.PostAsync(_url, content, cancellationTokenSource.Token).ConfigureAwait(false))
        {
          if (!responseInfo.IsSuccessStatusCode)
          {
            var response = await responseInfo.Content.ReadAsStringAsync().ConfigureAwait(false);
            Debug.Write($"InfluxDbSink : Error in http request HttpStatusCode : {responseInfo.StatusCode} | Response : {response}");
          }
        }
      }
    }

    // See the url below for the documentation for the line protocol. At the moment, the latest version of Influxdb is 1.3
    // https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial/
    private string BuildLinePrototcol(string measurement, IReadOnlyDictionary<string, string> tags, float value, long timestamp)
    {
      var sortedTags = SortTags(tags);

      var sb = new StringBuilder(StringBuilderInitialCapacity);
      sb.Append($"{measurement.Trim()}");

      if (sortedTags.Count > 0)
      {
        sb.Append(",");
      }

      // Add the tags.
      foreach (var tag in sortedTags)
      {
        sb.Append($"{tag.Key.Trim()}={tag.Value.Trim()},");
      }

      // Remove the last ,
      sb.Remove(sb.Length - 1, 1);

      if (timestamp == 0)
      {
        timestamp = GetTimeInMilliseconds(DateTime.UtcNow);
      }

      // Add the fields and timestamp
      sb.Append($" value={value * 1.0} {timestamp}");

      return sb.ToString().ToLowerInvariant();
    }

    // For best performance, tags should be sorted before they are sent to Influxdb.
    private IReadOnlyDictionary<string, string> SortTags(IReadOnlyDictionary<string, string> tags)
    {
      if (tags == null || tags.Count < 2)
      {
        return tags;
      }

      return tags.OrderBy(pair => pair.Key)
                 .ToDictionary(pair => pair.Key, pair => pair.Value);
    }

    private long GetTimeInMilliseconds(DateTime timestamp)
    {
      return (long)(timestamp - Epoch0).TotalMilliseconds;
    }
  }
}
