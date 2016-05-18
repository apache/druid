/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.lookup.namespace;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

import java.util.Map;

@JsonTypeName(StaticMapExtractionNamespace.TYPE_NAME)
public class StaticMapExtractionNamespace implements ExtractionNamespace
{
  static final String TYPE_NAME = "staticMap";
  private final Map<String, String> map;
  private final long pollMs;

  @JsonCreator
  public StaticMapExtractionNamespace(
      @JsonProperty("pollMs") long pollMs,
      @JsonProperty("map") Map<String, String> map
  )
  {
    this.pollMs = pollMs;
    this.map = Preconditions.checkNotNull(map, "`map` required");
  }

  @JsonProperty
  public Map<String, String> getMap()
  {
    return map;
  }

  @Override
  @JsonProperty
  public long getPollMs()
  {
    return pollMs;
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

    StaticMapExtractionNamespace that = (StaticMapExtractionNamespace) o;

    if (getPollMs() != that.getPollMs()) {
      return false;
    }
    return getMap().equals(that.getMap());

  }

  @Override
  public int hashCode()
  {
    int result = getMap().hashCode();
    result = 31 * result + (int) (getPollMs() ^ (getPollMs() >>> 32));
    return result;
  }
}
