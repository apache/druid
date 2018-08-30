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

package org.apache.druid.emitter.statsd;

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

  @JsonCreator
  public StatsDMetric(
      @JsonProperty("dimensions") SortedSet<String> dimensions,
      @JsonProperty("type") Type type,
      @JsonProperty("convertRange") boolean convertRange
  )
  {
    this.dimensions = dimensions;
    this.type = type;
    this.convertRange = convertRange;
  }

  public enum Type
  {
    count, gauge, timer
  }
}
