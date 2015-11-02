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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class Metadata
{
  // container is used for arbitrary key-value pairs in segment metadata e.g.
  // kafka firehose uses it to store commit offset
  @JsonProperty
  private final Map<String, Object> container;

  public Metadata()
  {
    container = new HashMap<>();
  }

  public Metadata addAll(Metadata other)
  {
    if (other != null) {
      container.putAll(other.container);
    }
    return this;
  }

  public Object get(String key)
  {
    return container.get(key);
  }

  public Metadata put(String key, Object value)
  {
    if (value != null) {
      container.put(key, value);
    }
    return this;
  }

  public boolean isEmpty()
  {
    return container.isEmpty();
  }

  public static Metadata merge(List<Metadata> metadataList)
  {
    Metadata result = new Metadata();
    for (Metadata md : metadataList) {
      if (md != null) {
        result.container.putAll(md.container);
      }
    }
    return result;
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

    Metadata metadata = (Metadata) o;

    return container.equals(metadata.container);

  }

  @Override
  public int hashCode()
  {
    return container.hashCode();
  }

  @Override
  public String toString()
  {
    return "Metadata{" +
           "container=" + container +
           '}';
  }
}
