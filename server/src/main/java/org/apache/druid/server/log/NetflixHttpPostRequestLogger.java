/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.HashedWheelTimer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.server.RequestLogLine;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.joda.time.Interval;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Http post request logger that sends druid's request log lines to keystone
 * gateway for eventual persistence to the hive table default.druid_logs
 */
public class NetflixHttpPostRequestLogger implements RequestLogger
{
  private AsyncHttpClient client;
  private static final String HOST_NAME = System.getenv("EC2_LOCAL_IPV4");
  private static final String NETFLIX_APP = System.getenv("NETFLIX_APP");
  private static final String NETFLIX_STACK = System.getenv("NETFLIX_STACK");
  private static final String NETFLIX_DETAIL = System.getenv("NETFLIX_DETAIL");
  private static final String EC2_REGION = System.getenv("EC2_REGION");
  private static final String NETFLIX_ASG = System.getenv("NETFLIX_AUTO_SCALE_GROUP");
  private static final String DRUID_LOG_STREAM = "druid_logs";
  private static final String URL = "http://ksgateway-"
                                    + EC2_REGION
                                    + "."
                                    + "prod"
                                    + "."
                                    + "netflix.net/REST/v1/stream/"
                                    + DRUID_LOG_STREAM;
  private static final String BDP_QUERY_ID = "bdp_query_uuid";

  private static final Logger log = new Logger(NetflixHttpPostRequestLogger.class);

  public NetflixHttpPostRequestLogger()
  {
    DefaultAsyncHttpClientConfig.Builder asyncClientConfigBuilder = new DefaultAsyncHttpClientConfig.Builder();
    asyncClientConfigBuilder.setThreadFactory(Execs.makeThreadFactory("nflx-druid-ksgateway-asynchttpclient-%d"));
    asyncClientConfigBuilder.setNettyTimer(new HashedWheelTimer(Execs.makeThreadFactory(
        "nflx-druid-ksgateway-asynchttpclient-timer-%d")));
    client = new DefaultAsyncHttpClient(asyncClientConfigBuilder.build());
  }

  public static String intervalsToString(List<Interval> intervals)
  {
    if (intervals == null || intervals.isEmpty()) {
      return "";
    }
    List<Interval> condenseIntervals = JodaUtils.condenseIntervals(intervals);
    StringBuilder builder = new StringBuilder();
    Iterator<Interval> itr = condenseIntervals.iterator();
    while (itr.hasNext()) {
      builder.append(itr.next());
      if (itr.hasNext()) {
        builder.append(";");
      }
    }
    return builder.toString();
  }

  public static String aggregators(Query query)
  {
    if (query instanceof GroupByQuery) {
      return asString(((GroupByQuery) query).getAggregatorSpecs());
    }
    if (query instanceof TimeseriesQuery) {
      return asString(((TimeseriesQuery) query).getAggregatorSpecs());
    }
    if (query instanceof TopNQuery) {
      return asString(((TopNQuery) query).getAggregatorSpecs());
    }
    return "";
  }

  public static String postAggregators(Query query)
  {
    if (query instanceof GroupByQuery) {
      return asString(((GroupByQuery) query).getPostAggregatorSpecs());
    }
    if (query instanceof TimeseriesQuery) {
      return asString(((TimeseriesQuery) query).getPostAggregatorSpecs());
    }
    if (query instanceof TopNQuery) {
      return asString(((TopNQuery) query).getPostAggregatorSpecs());
    }
    return "";
  }

  public static String asString(List<? extends Cacheable> list)
  {
    if (list == null || list.isEmpty()) {
      return "";
    }
    List<Object> sortedList = new ArrayList<>(list);
    Collections.sort(sortedList, Comparator.comparing(Object::toString));
    StringBuilder sb = new StringBuilder();
    sortedList.stream().forEach(postAggregator -> sb.append(postAggregator.toString()).append(";;"));
    return sb.toString();
  }

  public static String dimensionsProjectedAndFiltered(Query query)
  {
    if (query instanceof GroupByQuery) {
      return dimensionsString(((GroupByQuery) query).getDimensions(), ((GroupByQuery) query).getDimFilter());
    }
    if (query instanceof TopNQuery) {
      return dimensionsString(Collections.singletonList(((TopNQuery) query).getDimensionSpec()), query.getFilter());
    }
    return dimensionsString(Collections.emptyList(), query.getFilter());
  }

  public static String dimensionsString(List<DimensionSpec> dims, DimFilter filter)
  {
    if (dims == null && filter == null) {
      return "";
    }
    Set<String> dimensionNames = new TreeSet<>();
    if (dims != null) {
      dims.stream().forEach(dimensionSpec -> dimensionNames.add(dimensionSpec.getDimension()));
    }
    if (filter != null) {
      dimensionNames.addAll(filter.getRequiredColumns());
    }
    StringBuilder sb = new StringBuilder();
    dimensionNames.stream().forEach(dimensioName -> sb.append(dimensioName + ";;"));
    return sb.toString();
  }

  @Override
  public void stop()
  {
    CloseQuietly.close(client);
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine)
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
      client.executeRequest(request, new AsyncCompletionHandler<Response>()
      {
        @Override
        public Response onCompleted(Response response)
        {
          return response;
        }

        @Override
        public void onThrowable(Throwable t)
        {
          log.error(t, "Error while executing the post request");
        }
      });
    }
    catch (Throwable e) {
      // Swallow the error and log it so the caller doesn't fail.
      log.error(e, "Error while building and executing the post request");
    }
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine)
  {
    // no-op
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
  enum QueryStats
  {
    QUERY_TIME("query/time"),
    QUERY_BYTES("query/bytes"),
    SUCCESS("success"),
    ERROR_STACKTRACE("exception"),
    INTERRUPTED("interrupted"),
    INTERRUPTION_REASON("reason");

    private final String key;

    QueryStats(String key)
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
    private final String queryHash;

    // Used for identifying queries executed by bdp_tests framework
    private final String bdpQueryId;

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
    private final String queryString;
    private final String intervals;
    private final String filter;
    private final String aggregators;
    private final String postAggregators;
    private final String dimensionsUsed;

    // Netflix druid cluster details
    private final String druidStackName;
    private final String druidAsg;

    Payload(RequestLogLine logLine)
    {
      Query query = logLine.getQuery();
      queryString = query.toString();
      queryHash = Hashing.goodFastHash(32).hashString(queryString, Charset.defaultCharset()).toString();
      queryId = query.getId();
      bdpQueryId = (String) query.getContext().getOrDefault(BDP_QUERY_ID, "");
      datasource = query.getDataSource().toString();
      queryType = query.getType();
      isDescending = query.isDescending();
      hasFilters = query.hasFilters();
      intervals = intervalsToString(query.getIntervals());
      filter = query.getFilter() != null ? query.getFilter().toString() : "";
      aggregators = aggregators(query);
      postAggregators = postAggregators(query);
      dimensionsUsed = dimensionsProjectedAndFiltered(query);
      remoteAddress = logLine.getRemoteAddr();
      Map<String, Object> queryStats = logLine.getQueryStats().getStats();
      querySuccessful = (Boolean) queryStats.get(QueryStats.SUCCESS.key);
      queryTime = (Long) queryStats.get(QueryStats.QUERY_TIME.key);
      queryBytes = (Long) queryStats.get(QueryStats.QUERY_BYTES.key);
      errorStackTrace = (String) queryStats.get(QueryStats.ERROR_STACKTRACE.key);
      wasInterrupted = queryStats.get(QueryStats.INTERRUPTED.key) != null;
      interruptionReason = (String) queryStats.get(QueryStats.INTERRUPTION_REASON.key);
      druidHostType = NETFLIX_DETAIL;
      druidStackName = NETFLIX_STACK;
      druidAsg = NETFLIX_ASG;
    }

    @JsonProperty
    public String getDruidStackName()
    {
      return druidStackName;
    }

    @JsonProperty
    public String getDruidAsg()
    {
      return druidAsg;
    }

    @JsonProperty
    public String getQueryString()
    {
      return queryString;
    }

    @JsonProperty
    public String getQueryHash()
    {
      return queryHash;
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
    public String getBdpQueryId()
    {
      return bdpQueryId;
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

    @JsonProperty
    public String getIntervals()
    {
      return intervals;
    }

    @JsonProperty
    public String getFilter()
    {
      return filter;
    }

    @JsonProperty
    public String getAggregators()
    {
      return aggregators;
    }

    @JsonProperty
    public String getPostAggregators()
    {
      return postAggregators;
    }

    @JsonProperty
    public String getDimensionsUsed()
    {
      return dimensionsUsed;
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
    public String getHostname()
    {
      return hostname;
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
    private final String hostname;
    private final boolean ack;
    private final Event event;

    KeyStoneGatewayRequest(String appName, String hn, boolean ack, RequestLogLine requestLogLine)
    {
      this.appName = appName;
      this.hostname = hn;
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
