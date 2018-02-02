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

package io.druid.emitter.statsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.SortedSet;

/**
 */
public class StatsDMetric
{
  public final SortedSet<String> dimensions;
  public final Type type;
  public final boolean convertRange;
  public final double multiplier;

  // multiplier is to take care of cases where unit has to be converted, e.g.StatsDClient#time takes time
  // value in milliseconds.
  @JsonCreator
  public StatsDMetric(
      @JsonProperty("dimensions") SortedSet<String> dimensions,
      @JsonProperty("type") Type type,
      @JsonProperty("convertRange") boolean convertRange,
      @JsonProperty("multiplier") Double multiplier
  )
  {
    this.dimensions = dimensions;
    this.type = type;
    this.convertRange = convertRange;
    this.multiplier = (multiplier == null) ? 1 : multiplier;
  }
  public enum Type
  {
    count, gauge, timer
  }
}
