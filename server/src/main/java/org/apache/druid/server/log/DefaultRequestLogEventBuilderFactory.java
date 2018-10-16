package org.apache.druid.server.log;

import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.RequestLogLine;

public final class DefaultRequestLogEventBuilderFactory implements RequestLogEventBuilderFactory
{
  @Override
  public ServiceEventBuilder<RequestLogEvent> createRequestLogEventBuilder(String feed, RequestLogLine requestLogLine)
  {
    return new DefaultRequestLogEventBuilder(feed, requestLogLine);
  }
}
