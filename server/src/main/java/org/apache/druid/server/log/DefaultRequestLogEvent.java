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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.Query;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * The default implementation of {@link RequestLogEvent}. This class is annotated {@link PublicApi} because it's getters
 * could be used in proprietary {@link org.apache.druid.java.util.emitter.core.Emitter} implementations.
 */
@PublicApi
public final class DefaultRequestLogEvent implements RequestLogEvent
{
  private final ImmutableMap<String, String> serviceDimensions;
  private final String feed;
  private final RequestLogLine request;

  DefaultRequestLogEvent(ImmutableMap<String, String> serviceDimensions, String feed, RequestLogLine request)
  {
    this.serviceDimensions = serviceDimensions;
    this.request = request;
    this.feed = feed;
  }

  /**
   * Override {@link JsonValue} serialization, instead use annotations to include type information for polymorphic
   * {@link Query} objects.
   */
  @JsonValue(value = false)
  @Override
  public Map<String, Object> toMap()
  {
    return ImmutableMap.of();
  }

  @Override
  @JsonProperty("feed")
  public String getFeed()
  {
    return feed;
  }

  @JsonProperty("timestamp")
  public DateTime getCreatedTime()
  {
    return request.getTimestamp();
  }

  @JsonProperty("service")
  public String getService()
  {
    return serviceDimensions.get("service");
  }

  @JsonProperty("host")
  public String getHost()
  {
    return serviceDimensions.get("host");
  }

  @JsonProperty("query")
  public Query getQuery()
  {
    return request.getQuery();
  }

  @JsonProperty("sql")
  public String getSql()
  {
    return request.getSql();
  }

  @JsonProperty("remoteAddr")
  public String getRemoteAddr()
  {
    return request.getRemoteAddr();
  }

  @JsonProperty("queryStats")
  public QueryStats getQueryStats()
  {
    return request.getQueryStats();
  }
}
