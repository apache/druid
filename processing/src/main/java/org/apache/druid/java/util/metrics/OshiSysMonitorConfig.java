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

package org.apache.druid.java.util.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OshiSysMonitorConfig
{
  public static final String PREFIX = "druid.monitoring.sys";

  @JsonProperty("categories")
  @NotNull
  private Set<String> categories;

  public OshiSysMonitorConfig(@JsonProperty("categories") List<String> categories)
  {
    this.categories = categories == null ? new HashSet<>() : ImmutableSet.copyOf(categories);
  }

  public boolean shouldEmitMetricCategory(String category)
  {
    return categories.isEmpty() || categories.contains(category);
  }
}
