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

package org.apache.druid.emitter.opentelemetry;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implementation of a text-based approach to read the W3C Trace Context from the metric query context.
 * <a href="https://opentelemetry.io/docs/java/manual_instrumentation/#context-propagation">Context propagation</a>
 * <a href="https://www.w3.org/TR/trace-context/">W3C Trace Context</a>
 */
public class DruidContextTextMapGetter implements TextMapGetter<ServiceMetricEvent>
{

  @SuppressWarnings("unchecked")
  private Map<String, Object> getContext(ServiceMetricEvent event)
  {
    Object context = event.getUserDims().get("context");
    if (context instanceof Map) {
      return (Map<String, Object>) context;
    }
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public String get(ServiceMetricEvent event, String key)
  {
    return Optional.ofNullable(getContext(event).get(key)).map(Objects::toString).orElse(null);
  }

  @Override
  public Iterable<String> keys(ServiceMetricEvent event)
  {
    return getContext(event).keySet();
  }
}
