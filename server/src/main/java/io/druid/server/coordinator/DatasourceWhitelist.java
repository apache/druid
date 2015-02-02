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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 */
public class DatasourceWhitelist
{
  public static final String CONFIG_KEY = "coordinator.whitelist";

  private final Set<String> dataSources;

  @JsonCreator
  public DatasourceWhitelist(Set<String> dataSources)
  {
    this.dataSources = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
    this.dataSources.addAll(dataSources);
  }

  @JsonValue
  public Set<String> getDataSources()
  {
    return dataSources;
  }

  public boolean contains(String val)
  {
    return dataSources.contains(val);
  }
}
