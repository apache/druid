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

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import io.druid.query.Query;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;

public class EmittingRequestLogger implements RequestLogger
{
  final ServiceEmitter emitter;
  final String feed;

  public EmittingRequestLogger(ServiceEmitter emitter, String feed)
  {
    this.emitter = emitter;
    this.feed = feed;
  }

  @Override
  public void log(final RequestLogLine requestLogLine) throws IOException
  {
    emitter.emit(new RequestLogEventBuilder(feed, requestLogLine));
  }

  public static class RequestLogEvent implements Event
  {
    final String service;
    final String host;
    final String feed;
    final RequestLogLine request;

    public RequestLogEvent(String service, String host, String feed, RequestLogLine request)
    {
      this.service = service;
      this.host = host;
      this.request = request;
      this.feed = feed;
    }

    @Override
    // override JsonValue serialization, instead use annotations
    // to include type information for polymorphic Query objects
    @JsonValue(value=false)
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

    @Override
    @JsonProperty("timestamp")
    public DateTime getCreatedTime()
    {
      return request.getTimestamp();
    }

    @JsonProperty("service")
    public String getService()
    {
      return service;
    }

    @JsonProperty("host")
    public String getHost()
    {
      return host;
    }

    @JsonProperty("query")
    public Query getQuery()
    {
      return request.getQuery();
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

    @Override
    public boolean isSafeToBuffer()
    {
      return true;
    }
  }

  private static class RequestLogEventBuilder implements ServiceEventBuilder
  {
    private final String feed;
    private final RequestLogLine requestLogLine;

    public RequestLogEventBuilder(
        String feed,
        RequestLogLine requestLogLine
    )
    {
      this.feed = feed;
      this.requestLogLine = requestLogLine;
    }


    @Override
    public Event build(String service, String host)
    {
      return new RequestLogEvent(service, host, feed, requestLogLine);
    }
  }
}
