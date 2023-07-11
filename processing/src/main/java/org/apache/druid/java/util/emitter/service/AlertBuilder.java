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

package org.apache.druid.java.util.emitter.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Map;

/**
*/
public class AlertBuilder extends ServiceEventBuilder<AlertEvent>
{
  protected final Map<String, Object> dataMap = Maps.newLinkedHashMap();
  protected final String description;
  protected final ServiceEmitter emitter;

  protected AlertEvent.Severity severity = AlertEvent.Severity.DEFAULT;

  public static AlertBuilder create(String descriptionFormat, Object... objects)
  {
    return AlertBuilder.createEmittable(null, descriptionFormat, objects);
  }

  public static AlertBuilder createEmittable(ServiceEmitter emitter, String descriptionFormat, Object... objects)
  {
    return new AlertBuilder(StringUtils.format(descriptionFormat, objects), emitter);
  }

  protected AlertBuilder(
      String description,
      ServiceEmitter emitter
  )
  {
    this.description = description;
    this.emitter = emitter;
  }

  public AlertBuilder addData(String identifier, Object value)
  {
    dataMap.put(identifier, value);
    return this;
  }

  public AlertBuilder addData(Map<String, Object> data)
  {
    dataMap.putAll(data);
    return this;
  }

  public AlertBuilder severity(AlertEvent.Severity severity)
  {
    this.severity = severity;
    return this;
  }

  @Override
  public AlertEvent build(ImmutableMap<String, String> serviceDimensions)
  {
    return new AlertEvent(DateTimes.nowUtc(), serviceDimensions, severity, description, dataMap);
  }

  public void emit()
  {
    if (emitter == null) {
      throw new UnsupportedOperationException("Emitter is null, cannot emit.");
    }

    emitter.emit(this);
  }
}
