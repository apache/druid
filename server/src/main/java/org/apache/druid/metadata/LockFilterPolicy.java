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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Specifies a policy to filter active locks held by a datasource
 */
public class LockFilterPolicy
{
  private final String datasource;
  private final int priority;
  private final List<Interval> intervals;
  private final Map<String, Object> context;

  @JsonCreator
  public LockFilterPolicy(
      @JsonProperty("datasource") String datasource,
      @JsonProperty("priority") int priority,
      @JsonProperty("intervals") @Nullable List<Interval> intervals,
      @JsonProperty("context") @Nullable Map<String, Object> context
  )
  {
    this.datasource = datasource;
    this.priority = priority;
    this.intervals = intervals;
    this.context = context == null ? Collections.emptyMap() : context;
  }

  @JsonProperty
  public String getDatasource()
  {
    return datasource;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Nullable
  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }
}
