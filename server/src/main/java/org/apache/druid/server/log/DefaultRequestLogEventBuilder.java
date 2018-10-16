package org.apache.druid.server.log;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.RequestLogLine;

final class DefaultRequestLogEventBuilder extends ServiceEventBuilder<RequestLogEvent>
{
  private final String feed;
  private final RequestLogLine requestLogLine;

  DefaultRequestLogEventBuilder(String feed, RequestLogLine requestLogLine)
  {
    this.feed = feed;
    this.requestLogLine = requestLogLine;
  }

  @Override
  public RequestLogEvent build(ImmutableMap<String, String> serviceDimensions)
  {
    return new DefaultRequestLogEvent(serviceDimensions, feed, requestLogLine);
  }
}
