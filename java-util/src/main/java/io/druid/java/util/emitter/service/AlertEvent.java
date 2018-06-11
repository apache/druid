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

package io.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import io.druid.guice.annotations.PublicApi;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.emitter.core.Event;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Map;

/**
 */
@PublicApi
public class AlertEvent implements Event
{
  private final ImmutableMap<String, String> serviceDimensions;
  private final Severity severity;
  private final String description;
  private final DateTime createdTime;

  private final Map<String, Object> dataMap;

  public AlertEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDimensions,
      Severity severity,
      String description,
      Map<String, Object> dataMap
  )
  {
    this.createdTime = createdTime;
    this.serviceDimensions = serviceDimensions;
    this.severity = severity;
    this.description = description;
    this.dataMap = dataMap;
  }

  public AlertEvent(
      DateTime createdTime,
      String service,
      String host,
      Severity severity,
      String description,
      Map<String, Object> dataMap
  )
  {
    this(createdTime, ImmutableMap.of("service", service, "host", host), severity, description, dataMap);
  }

  public AlertEvent(
      String service,
      String host,
      Severity severity,
      String description,
      Map<String, Object> dataMap
  )
  {
    this(DateTimes.nowUtc(), service, host, severity, description, dataMap);
  }

  public AlertEvent(
      String service,
      String host,
      String description,
      Map<String, Object> dataMap
  )
  {
    this(DateTimes.nowUtc(), service, host, Severity.DEFAULT, description, dataMap);
  }

  public AlertEvent(
      String service,
      String host,
      String description
  )
  {
    this(DateTimes.nowUtc(), service, host, Severity.DEFAULT, description, ImmutableMap.<String, Object>of());
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public String getFeed()
  {
    return "alerts";
  }

  /*
   * This method is used in certain proprietary emitter extensions
   */
  @SuppressWarnings("unused")
  public String getService()
  {
    return serviceDimensions.get("service");
  }

  /*
   * This method is used in certain proprietary emitter extensions
   */
  @SuppressWarnings("unused")
  public String getHost()
  {
    return serviceDimensions.get("host");
  }

  public Severity getSeverity()
  {
    return severity;
  }

  public String getDescription()
  {
    return description;
  }

  /*
   * This method is used in certain proprietary emitter extensions
   */
  @SuppressWarnings("unused")
  public Map<String, Object> getDataMap()
  {
    return Collections.unmodifiableMap(dataMap);
  }

  @Override
  @JsonValue
  public Map<String, Object> toMap()
  {
    return ImmutableMap.<String, Object>builder()
        .put("feed", getFeed())
        .put("timestamp", createdTime.toString())
        .putAll(serviceDimensions)
        .put("severity", severity.toString())
        .put("description", description)
        .put("data", dataMap)
        .build();
  }

  public enum Severity
  {
    ANOMALY {
      @Override
      public String toString()
      {
        return "anomaly";
      }
    },

    COMPONENT_FAILURE {
      @Override
      public String toString()
      {
        return "component-failure";
      }
    },

    SERVICE_FAILURE {
      @Override
      public String toString()
      {
        return "service-failure";
      }
    };

    public static final Severity DEFAULT = COMPONENT_FAILURE;
  }
}
