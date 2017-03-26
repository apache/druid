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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 */
public class AffinityConfig
{
  // key:Datasource, value:[nodeHostNames]
  private Map<String, List<String>> affinity = Maps.newHashMap();

  @JsonCreator
  public AffinityConfig(
      @JsonProperty("affinity") Map<String, List<String>> affinity
  )
  {
    this.affinity = affinity;
  }

  @JsonProperty
  public Map<String, List<String>> getAffinity()
  {
    return affinity;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AffinityConfig that = (AffinityConfig) o;

    if (affinity != null
        ? !Maps.difference(affinity, that.affinity).entriesDiffering().isEmpty()
        : that.affinity != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return affinity != null ? affinity.hashCode() : 0;
  }
}
