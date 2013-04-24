/*
 * Druid - a distributed column store.
 * Copyright (C) 2013  Metamarkets Group Inc.
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

package com.metamx.druid.http;

import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import org.joda.time.DateTime;

import java.util.Map;

public class EmittingRequestLogger implements RequestLogger
{

  final String service;
  final String host;
  final Emitter emitter;
  final String feed;

  public EmittingRequestLogger(String service, String host, Emitter emitter, String feed)
  {
    this.emitter = emitter;
    this.host = host;
    this.service = service;
    this.feed = feed;
  }

  @Override
  public void log(final RequestLogLine requestLogLine) throws Exception
  {
    emitter.emit(new RequestLogEvent(service, host, feed, requestLogLine));
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
    public Map<String, Object> toMap()
    {
      return ImmutableMap.<String, Object>builder()
              .put("feed", getFeed())
              .put("timestamp", request.getTimestamp())
              .put("service", service)
              .put("host", host)
              .put("query", request.getQuery())
              .put("remoteAddr", request.getRemoteAddr())
              .build();
    }

    @Override
    public String getFeed()
    {
      return feed;
    }

    @Override
    public DateTime getCreatedTime()
    {
      return request.getTimestamp();
    }

    @Override
    public boolean isSafeToBuffer()
    {
      return true;
    }
  }
}
