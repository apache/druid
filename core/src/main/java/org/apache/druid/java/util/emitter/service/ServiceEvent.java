package org.apache.druid.java.util.emitter.service;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import java.util.Map;

public class ServiceEvent extends AlertEvent
{
  public ServiceEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDimensions,
      Severity severity,
      String description,
      Map<String, Object> dataMap
  )
  {
    super(createdTime, serviceDimensions, severity, description, dataMap);
  }

  public ServiceEvent(
      DateTime createdTime,
      String service,
      String host,
      Severity severity,
      String description,
      Map<String, Object> dataMap
  )
  {
    super(createdTime, service, host, severity, description, dataMap);
  }

  public ServiceEvent(String service, String host, Severity severity, String description, Map<String, Object> dataMap)
  {
    super(service, host, severity, description, dataMap);
  }

  public ServiceEvent(String service, String host, String description, Map<String, Object> dataMap)
  {
    super(service, host, description, dataMap);
  }

  public ServiceEvent(String service, String host, String description)
  {
    super(service, host, description);
  }

  @Override
  public String getFeed()
  {
    return "feed";
  }
}
