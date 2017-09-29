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

import com.google.common.collect.Maps;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class LimitConfig
{
  private Map<String, Integer> limit = Maps.newHashMap();

  @JsonCreator
  public LimitConfig(
      @JsonProperty("limit") Map<String, Integer> limit
  )
  {
    this.limit = limit;
  }

  @JsonProperty
  public Map<String, Integer> getLimit()
  {
    return limit;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    LimitConfig that = (LimitConfig) o;

    if (limit != null
        ? !Maps.difference(limit, that.limit).entriesDiffering().isEmpty()
        : that.limit != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return limit != null ? limit.hashCode() : 0;
  }
}

