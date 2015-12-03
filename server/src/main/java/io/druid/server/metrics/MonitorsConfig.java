/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.metrics.Monitor;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class MonitorsConfig
{
  public final static String METRIC_DIMENSION_PREFIX = "druid.metrics.emitter.dimension.";

  @JsonProperty("monitors")
  @NotNull
  private List<Class<? extends Monitor>> monitors = Lists.newArrayList();

  public List<Class<? extends Monitor>> getMonitors()
  {
    return monitors;
  }

  @Override
  public String toString()
  {
    return "MonitorsConfig{" +
           "monitors=" + monitors +
           '}';
  }
}
