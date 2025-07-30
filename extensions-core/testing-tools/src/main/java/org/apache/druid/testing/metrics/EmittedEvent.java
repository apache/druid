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

package org.apache.druid.testing.metrics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Serialized variant of {@link Event} emitted by {@code HttpPostEmitter}.
 * <p>
 * This can later be merged with the {@link Event} interface itself so that
 * events can always be deserialized from an event map.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "feed")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "metrics", value = EmittedEvent.Metric.class),
    @JsonSubTypes.Type(name = "alerts", value = EmittedEvent.Alert.class),
    @JsonSubTypes.Type(name = "segment_metadata", value = EmittedEvent.SegmentMetadata.class)
})
public abstract class EmittedEvent extends LinkedHashMap<String, Object>
{
  private static final Set<String> SYSTEM_DIMENSIONS = Set.of(
      Event.FEED,
      Event.TIMESTAMP,
      Event.SERVICE,
      Event.HOST,
      Event.METRIC,
      Event.VALUE
  );

  abstract Event toEvent();

  String getStringValue(String key)
  {
    return (String) get(key);
  }

  public static class Alert extends EmittedEvent
  {
    @Override
    public AlertEvent toEvent()
    {
      return new AlertEvent(
          DateTimes.of(getStringValue(Event.TIMESTAMP)),
          getStringValue(Event.SERVICE),
          getStringValue(Event.HOST),
          AlertEvent.Severity.valueOf(getStringValue("severity")),
          getStringValue("description"),
          Map.of()
      );
    }
  }

  public static class Metric extends EmittedEvent
  {
    @Override
    public ServiceMetricEvent toEvent()
    {
      final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
      for (String key : keySet()) {
        if (!SYSTEM_DIMENSIONS.contains(key)) {
          builder.setDimension(key, get(key));
        }
      }

      return builder
          .setFeed("metrics")
          .setCreatedTime(DateTimes.of(getStringValue(Event.TIMESTAMP)))
          .setMetric(getStringValue(Event.METRIC), (Number) get(Event.VALUE))
          .build(getStringValue(Event.SERVICE), getStringValue(Event.HOST));
    }
  }

  public static class SegmentMetadata extends EmittedEvent
  {
    @Override
    public SegmentMetadataEvent toEvent()
    {
      return new SegmentMetadataEvent(
          getStringValue(SegmentMetadataEvent.DATASOURCE),
          DateTimes.of(getStringValue(SegmentMetadataEvent.CREATED_TIME)),
          DateTimes.of(getStringValue(SegmentMetadataEvent.START_TIME)),
          DateTimes.of(getStringValue(SegmentMetadataEvent.END_TIME)),
          getStringValue(SegmentMetadataEvent.VERSION),
          (Boolean) get(SegmentMetadataEvent.IS_COMPACTED)
      );
    }
  }
}
