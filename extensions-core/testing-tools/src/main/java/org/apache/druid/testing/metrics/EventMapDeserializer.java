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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.Map;
import java.util.Set;

/**
 * Deserializer used to convert {@link EventMap} to an {@link Event}.
 */
public class EventMapDeserializer
{
  private static final Set<String> SYSTEM_DIMENSIONS = Set.of(
      Event.FEED,
      Event.TIMESTAMP,
      Event.SERVICE,
      Event.HOST,
      Event.METRIC,
      Event.VALUE
  );

  private final EventMap map;

  EventMapDeserializer(EventMap map)
  {
    this.map = map;
  }

  Event asEvent()
  {
    final String feed = getStringValue(Event.FEED);
    switch (feed) {
      case "alerts":
        return asAlertEvent();
      case "segment_metadata":
        return asSegmentMetadataEvent();
      default:
        return asMetricEvent();
    }
  }

  String getService()
  {
    return getStringValue(Event.SERVICE);
  }

  String getHost()
  {
    return getStringValue(Event.HOST);
  }

  private String getStringValue(String key)
  {
    return (String) map.get(key);
  }

  @SuppressWarnings("unchecked")
  private AlertEvent asAlertEvent()
  {
    return new AlertEvent(
        DateTimes.of(getStringValue(Event.TIMESTAMP)),
        getService(),
        getHost(),
        AlertEvent.Severity.fromString(getStringValue("severity")),
        getStringValue("description"),
        (Map<String, Object>) map.get("data")
    );
  }

  private ServiceMetricEvent asMetricEvent()
  {
    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    for (String key : map.keySet()) {
      if (!SYSTEM_DIMENSIONS.contains(key)) {
        builder.setDimension(key, map.get(key));
      }
    }

    return builder
        .setFeed(getStringValue(Event.FEED))
        .setCreatedTime(DateTimes.of(getStringValue(Event.TIMESTAMP)))
        .setMetric(getStringValue(Event.METRIC), (Number) map.get(Event.VALUE))
        .build(getService(), getHost());
  }

  private SegmentMetadataEvent asSegmentMetadataEvent()
  {
    return new SegmentMetadataEvent(
        getStringValue(SegmentMetadataEvent.DATASOURCE),
        DateTimes.of(getStringValue(SegmentMetadataEvent.CREATED_TIME)),
        DateTimes.of(getStringValue(SegmentMetadataEvent.START_TIME)),
        DateTimes.of(getStringValue(SegmentMetadataEvent.END_TIME)),
        getStringValue(SegmentMetadataEvent.VERSION),
        (Boolean) map.get(SegmentMetadataEvent.IS_COMPACTED)
    );
  }
}
