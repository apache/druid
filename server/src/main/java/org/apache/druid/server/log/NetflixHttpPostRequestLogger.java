package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.server.RequestLogLine;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class NetflixHttpPostRequestLogger implements RequestLogger
{
  private AsyncHttpClient client;
  private static final String HOST_NAME = System.getenv("HOST_NAME");
  private static final String NETFLIX_APP = System.getenv("NETFLIX_APP");
  private static final String NETFLIX_STACK = System.getenv("NETFLIX_STACK");
  private static final String NETFLIX_DETAIL = System.getenv("NETFLIX_DETAIL");
  private static final String EC2_REGION = System.getenv("EC2_REGION");
  private static final String DRUID_LOG_STREAM = "druidLogs";
  private static final String URL = "ksgateway-"
                                    + EC2_REGION
                                    + NETFLIX_STACK
                                    + ".netflix.net/REST/v1/stream/"
                                    + DRUID_LOG_STREAM;


  private static final Logger log = new Logger(NetflixHttpPostRequestLogger.class);

  @LifecycleStart
  public void start()
  {
    client = new DefaultAsyncHttpClient();
  }

  @LifecycleStop
  public void shutdown() throws IOException
  {
    client.close();
  }

  @Override
  public void log(RequestLogLine requestLogLine)
  {
    try {
      ObjectMapper mapper = new DefaultObjectMapper();
      String body = mapper.writeValueAsString(new KeyStoneGatewayRequest(
          NETFLIX_APP,
          HOST_NAME,
          false,
          requestLogLine
      ));
      final RequestBuilder request = new RequestBuilder("POST");
      request.setUrl(URL);
      request.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
      request.setBody(body);
      System.out.println("HTTP post request body: " + body);
//      client.executeRequest(request, new AsyncCompletionHandler<Response>()
//      {
//        @Override
//        public Response onCompleted(Response response)
//        {
//          return response;
//        }
//
//        @Override
//        public void onThrowable(Throwable t)
//        {
//          log.error(t, "Error while making the post request");
//        }
//      });
    }
    catch (Throwable e) {
      // Swallow the error and log it so the caller doesn't fail.
      log.error(e, "Error while building and executing the post request");
    }
  }

  @JsonTypeName("event")
  private static class Event
  {
    private final Payload payload;
    private final String uuid;

    @JsonProperty
    public String getUuid()
    {
      return uuid;
    }

    @JsonProperty
    public Payload getPayload()
    {
      return payload;
    }


    public Event(RequestLogLine logLine)
    {
      this.payload = new Payload(logLine);
      uuid = UUID.randomUUID().toString();
    }
  }

  @VisibleForTesting
  enum QueryStatsKey
  {
    QUERY_TIME("query/time"),
    QUERY_BYTES("query/bytes"),
    SUCCESS("success"),
    ERROR_STACKTRACE("exception"),
    INTERRUPTED("interrupted"),
    INTERRUPTION_REASON("reason");

    private final String key;

    QueryStatsKey(String key)
    {
      this.key = key;
    }

    @Override
    public String toString()
    {
      return key;
    }
  }

  @JsonTypeName("payload")
  @VisibleForTesting
  static class Payload
  {
    private final String queryId;
    private final String datasource;
    private final String queryType;
    private final String remoteAddress;
    private final boolean isDescending;
    private final boolean hasFilters;
    private final boolean querySuccessful;
    private final Long queryTime;
    private final Long queryBytes;
    private final String errorStackTrace;
    private final boolean wasInterrupted;
    private final String interruptionReason;
    private final String druidHostType;

    Payload(RequestLogLine logLine)
    {
      Query query = logLine.getQuery();
      queryId = query.getId();
      datasource = query.getDataSource().toString();
      queryType = query.getType();
      isDescending = query.isDescending();
      hasFilters = query.hasFilters();
      remoteAddress = logLine.getRemoteAddr();
      Map<String, Object> queryStats = logLine.getQueryStats().getStats();
      querySuccessful = (Boolean) queryStats.get(QueryStatsKey.SUCCESS.key);
      queryTime = (Long) queryStats.get(QueryStatsKey.QUERY_TIME.key);
      queryBytes = (Long) queryStats.get(QueryStatsKey.QUERY_BYTES.key);
      errorStackTrace = (String) queryStats.get(QueryStatsKey.ERROR_STACKTRACE.key);
      wasInterrupted = queryStats.get(QueryStatsKey.INTERRUPTED.key) != null;
      interruptionReason = (String) queryStats.get(QueryStatsKey.INTERRUPTION_REASON.key);
      druidHostType = NETFLIX_DETAIL;
    }

    @JsonProperty
    public String getDatasource()
    {
      return datasource;
    }

    @JsonProperty
    public String getQueryType()
    {
      return queryType;
    }

    @JsonProperty
    public String getRemoteAddress()
    {
      return remoteAddress;
    }

    @JsonProperty
    public boolean isDescending()
    {
      return isDescending;
    }

    @JsonProperty
    public boolean isHasFilters()
    {
      return hasFilters;
    }

    @JsonProperty
    public boolean isQuerySuccessful()
    {
      return querySuccessful;
    }

    @JsonProperty
    public Long getQueryTime()
    {
      return queryTime;
    }

    @JsonProperty
    public Long getQueryBytes()
    {
      return queryBytes;
    }

    @JsonProperty
    public String getErrorStackTrace()
    {
      return errorStackTrace;
    }

    @JsonProperty
    public boolean isWasInterrupted()
    {
      return wasInterrupted;
    }

    @JsonProperty
    public String getInterruptionReason()
    {
      return interruptionReason;
    }

    @JsonProperty
    public String getQueryId()
    {
      return queryId;
    }

    @JsonProperty
    public String getDruidHostType()
    {
      return druidHostType;
    }

  }

  @JsonTypeName("request")
  @VisibleForTesting
  static class KeyStoneGatewayRequest
  {
    @JsonProperty
    public String getAppName()
    {
      return appName;
    }

    @JsonProperty
    public String getHostName()
    {
      return hostName;
    }

    @JsonProperty
    public boolean isAck()
    {
      return ack;
    }

    @JsonProperty
    public Event getEvent()
    {
      return event;
    }

    private final String appName;
    private final String hostName;
    private final boolean ack;
    private final Event event;

    KeyStoneGatewayRequest(String appName, String hostName, boolean ack, RequestLogLine requestLogLine)
    {
      this.appName = appName;
      this.hostName = hostName;
      this.ack = ack;
      this.event = new Event(requestLogLine);
    }

    @VisibleForTesting
    Payload getPayload()
    {
      return event.getPayload();
    }
  }

}
