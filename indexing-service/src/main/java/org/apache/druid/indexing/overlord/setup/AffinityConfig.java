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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class AffinityConfig
{
  // key:Datasource, value:[nodeHostNames]
  private final Map<String, Set<String>> affinity;
  private final boolean strong;

  // Cache of the names of workers that have affinity for any dataSource.
  // Not part of the serialized JSON or equals/hashCode.
  private final Set<String> affinityWorkers;

  @JsonCreator
  public AffinityConfig(
      @JsonProperty("affinity") Map<String, Set<String>> affinity,
      @JsonProperty("strong") boolean strong
  )
  {
    this.affinity = affinity;
    this.strong = strong;
    this.affinityWorkers = affinity.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
  }

  @JsonProperty
  public Map<String, Set<String>> getAffinity()
  {
    return affinity;
  }

  @JsonProperty
  public boolean isStrong()
  {
    return strong;
  }

  public Set<String> getAffinityWorkers()
  {
    return affinityWorkers;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AffinityConfig that = (AffinityConfig) o;
    return strong == that.strong &&
           Objects.equals(affinity, that.affinity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(affinity, strong);
  }

  @Override
  public String toString()
  {
    return "AffinityConfig{" +
           "affinity=" + affinity +
           ", strong=" + strong +
           '}';
  }
}
