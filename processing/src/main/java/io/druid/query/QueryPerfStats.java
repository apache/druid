/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryPerfStats
{
  public static final String KEY_CTX = "queryPerfStats";

  private long queryResultTime;
  private long queryResultBytes;

  //holders for historical/realtime stats as reported from DirectDruidClient
  private Map<String, ServerPerfStats> serverPerf;

  //holders for per segment stats on historical/realtime node
  private List<MetricsEmittingQueryRunnerStats> metricsEmittingQueryRunnerStats;

  //limit maximum number of per segment stats, to keep context size bounded
  private final int maxNumMetricsEmittingQueryRunnerStats;

  public QueryPerfStats(int maxNumMetricsEmittingQueryRunnerStats)
  {
    this.maxNumMetricsEmittingQueryRunnerStats = maxNumMetricsEmittingQueryRunnerStats;
  }

  @JsonProperty
  public long getQueryResultTime()
  {
    return queryResultTime;
  }

  @JsonProperty
  public long getQueryResultBytes()
  {
    return queryResultBytes;
  }

  @JsonProperty
  public Map<String, ServerPerfStats> getServerPerf()
  {
    return serverPerf;
  }

  @JsonProperty
  public List<MetricsEmittingQueryRunnerStats> getMetricsEmittingQueryRunnerStats()
  {
    return metricsEmittingQueryRunnerStats;
  }

  public void updateQueryResultTime(long queryTime) {
    this.queryResultTime = queryTime;
  }

  public void updateQueryResultBytes(long queryBytes) {
    this.queryResultBytes = queryBytes;
  }

  public void updateServerTime(String host, long timeTaken)
  {
    getServerPerfStats(host).setQueryNodeTime(timeTaken);
  }

  public void updateServerBytes(String host, long l)
  {
    getServerPerfStats(host).setQueryNodeBytes(l);
  }

  public void updateServerTTFB(String host, long timeTaken)
  {
    getServerPerfStats(host).setQueryNodeTTFB(timeTaken);
  }

  public void addMetricsEmittingQueryRunnerStats(MetricsEmittingQueryRunnerStats stats)
  {
    if (metricsEmittingQueryRunnerStats == null) {
      metricsEmittingQueryRunnerStats = new ArrayList<>();
    }

    //its possible for multiple threads to end up putting more entries than maxNumMetricsEmittingQueryRunnerStats
    //but that is OK, limit does not have to be so strict.
    if (metricsEmittingQueryRunnerStats.size() < maxNumMetricsEmittingQueryRunnerStats) {
      metricsEmittingQueryRunnerStats.add(stats);
    }
  }

  private ServerPerfStats getServerPerfStats(String host)
  {
    //This is only called in single thread, so concurrent Map is not necessary.
    if (serverPerf == null) {
      serverPerf = new HashMap<>();
    }

    ServerPerfStats sps = serverPerf.get(host);
    if (sps == null) {
      sps = new ServerPerfStats();
      serverPerf.put(host, sps);
    }
    return sps;
  }
}

class ServerPerfStats
{
  private long queryNodeTTFB;
  private long queryNodeTime;
  private long queryNodeBytes;

  @JsonProperty
  public long getQueryNodeTTFB()
  {
    return queryNodeTTFB;
  }

  public void setQueryNodeTTFB(long queryNodeTTFB)
  {
    this.queryNodeTTFB = queryNodeTTFB;
  }

  @JsonProperty
  public long getQueryNodeTime()
  {
    return queryNodeTime;
  }

  public void setQueryNodeTime(long queryNodeTime)
  {
    this.queryNodeTime = queryNodeTime;
  }

  @JsonProperty
  public long getQueryNodeBytes()
  {
    return queryNodeBytes;
  }

  public void setQueryNodeBytes(long queryNodeBytes)
  {
    this.queryNodeBytes = queryNodeBytes;
  }
}

class MetricsEmittingQueryRunnerStats
{
  private final Map<String, String> dimensions;
  private final Map<String, Object> metrics = new HashMap<>();

  @JsonProperty
  public Map<String, String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Map<String, Object> getMetrics()
  {
    return metrics;
  }

  public MetricsEmittingQueryRunnerStats(Map<String, String> dimensions)
  {
    this.dimensions = dimensions;
  }

  public void addMetric(String metricName, Object value)
  {
    metrics.put(metricName, value);
  }
}
