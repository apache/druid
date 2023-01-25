package org.apache.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

@PublicApi
public class ServiceEvent implements Event
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final DateTime createdTime;
  private final ImmutableMap<String, String> serviceDims;
  private final Map<String, Object> userDims;
  private final String feed;

  private ServiceEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDims,
      Map<String, Object> userDims,
      String feed
  )
  {
    this.createdTime = createdTime != null ? createdTime : DateTimes.nowUtc();
    this.serviceDims = serviceDims;
    this.userDims = userDims;
    this.feed = feed;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public String getFeed()
  {
    return feed;
  }

  public String getService()
  {
    return serviceDims.get("service");
  }

  public String getHost()
  {
    return serviceDims.get("host");
  }

  public Map<String, Object> getUserDims()
  {
    return ImmutableMap.copyOf(userDims);
  }

  @Override
  @JsonValue
  public EventMap toMap()
  {
    return EventMap
        .builder()
        .put("feed", getFeed())
        .put("timestamp", createdTime.toString())
        .putAll(serviceDims)
        .putAll(
            Maps.filterEntries(
                userDims,
                new Predicate<Map.Entry<String, Object>>()
                {
                  @Override
                  public boolean apply(Map.Entry<String, Object> input)
                  {
                    return input.getKey() != null;
                  }
                }
            )
        )
        .build();
  }

  public static class Builder
  {
    private final Map<String, Object> userDims = new TreeMap<>();
    private String feed = "serviceEvent";

    public Builder setFeed(String feed)
    {
      this.feed = feed;
      return this;
    }

    public Builder setDimension(String dim, String[] values)
    {
      userDims.put(dim, Arrays.asList(values));
      return this;
    }

    public Builder setDimension(String dim, Object value)
    {
      userDims.put(dim, value);
      return this;
    }

    public Object getDimension(String dim)
    {
      return userDims.get(dim);
    }

    public ServiceEventBuilder<ServiceEvent> build(
    )
    {
      return build(null);
    }

    public ServiceEventBuilder<ServiceEvent> build(
        final DateTime createdTime
    )
    {

      return new ServiceEventBuilder<ServiceEvent>()
      {
        @Override
        public ServiceEvent build(ImmutableMap<String, String> serviceDimensions)
        {
          return new ServiceEvent(
              createdTime,
              serviceDimensions,
              userDims,
              feed
          );
        }
      };
    }
  }
}
