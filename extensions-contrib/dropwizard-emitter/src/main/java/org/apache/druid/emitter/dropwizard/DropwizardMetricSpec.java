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

package org.apache.druid.emitter.dropwizard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DropwizardMetricSpec
{
  @JsonProperty("dimensions")
  private final List<String> dimensions;
  @JsonProperty("type")
  private final Type type;
  @JsonProperty("timeUnit")
  private final TimeUnit timeUnit;

  @JsonCreator
  DropwizardMetricSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("type") Type type,
      @JsonProperty("timeUnit") TimeUnit timeUnit
  )
  {
    this.dimensions = dimensions;
    this.type = type;
    this.timeUnit = timeUnit;
  }

  @JsonProperty
  public Type getType()
  {
    return type;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  public enum Type
  {
    histogram, timer, meter, counter, gauge
  }

}
