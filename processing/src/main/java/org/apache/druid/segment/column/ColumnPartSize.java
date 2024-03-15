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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnPartSize
{
  public static final ColumnPartSize NO_DATA = new ColumnPartSize("unavailable", 0L, Collections.emptyMap());

  public static ColumnPartSize simple(String name, long size)
  {
    return new ColumnPartSize(
        name,
        size,
        Collections.emptyMap()
    );
  }

  public static ColumnPartSize indexedComponent(String details, long cardinality, long size)
  {
    return new ColumnPartSize(
        StringUtils.format("[%s] with [%s] values", details, cardinality),
        size,
        Collections.emptyMap()
    );
  }

  private final String description;
  private final long size;
  private final Map<String, ColumnPartSize> components;

  @JsonCreator
  public ColumnPartSize(
      @JsonProperty("description") String description,
      @JsonProperty("size") long size,
      @JsonProperty("components") Map<String, ColumnPartSize> components)
  {
    this.description = description;
    this.size = size;
    this.components = components;
  }

  @JsonProperty
  public String getDescription()
  {
    return description;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, ColumnPartSize> getComponents()
  {
    return components;
  }

  public long getTotalSize()
  {
    return size + components.values().stream().mapToLong(ColumnPartSize::getTotalSize).sum();
  }

  public ColumnPartSize merge(ColumnPartSize other)
  {
    long newSize = size + other.size;
    LinkedHashMap<String, ColumnPartSize> combined = new LinkedHashMap<>(components);
    for (Map.Entry<String, ColumnPartSize> sizes : other.getComponents().entrySet()) {
      combined.compute(sizes.getKey(), (k, componentSize) -> {
        if (componentSize == null) {
          return sizes.getValue();
        }
        return componentSize.merge(sizes.getValue());
      });
    }
    return new ColumnPartSize(
        description,
        newSize,
        combined
    );
  }

  @Override
  public String toString()
  {
    return "ColumnPartSize{" +
           "name='" + description + '\'' +
           ", size=" + size +
           ", components=" + components +
           '}';
  }
}
