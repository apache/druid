/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
  public void log(final RequestLogLine requestLogLine) throws Exception
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

  private class RequestLogEventBuilder implements ServiceEventBuilder
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
