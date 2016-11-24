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

package io.druid.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.List;

public class KeyValueMap
{
  private final String mapName;
  private final String keyColumn;
  private final String valueColumn;

  public static String DEFAULT_MAPNAME = "__default";

  @JsonCreator
  public KeyValueMap(
      @JsonProperty("mapName") String mapName,
      @JsonProperty("keyColumn") String keyColumn,
      @JsonProperty("valueColumn") String valueColumn
  )
  {
    this.mapName = mapName;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
  }

  @JsonProperty
  public String getMapName()
  {
    return mapName;
  }

  @JsonProperty
  public String getKeyColumn()
  {
    return keyColumn;
  }

  @JsonProperty
  public String getValueColumn()
  {
    return valueColumn;
  }

  @Override
  public String toString()
  {
    return "KeyValueMap{" +
        "mapName='" + mapName + '\'' +
        ", keyColumn='" + keyColumn + '\'' +
        ", valueColumn='" + valueColumn + '\'' +
        '}';
  }

  @Override
  public int hashCode()
  {
    int result = mapName.hashCode();
    result = 31 * result + keyColumn.hashCode();
    result = 31 * result + valueColumn.hashCode();

    return result;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null) {
      return false;
    }
    if (!(o instanceof KeyValueMap)) {
      return false;
    }
    KeyValueMap that = (KeyValueMap)o;

    if (!getMapName().equals(that.getMapName())) {
      return false;
    }
    if (!getKeyColumn().equals(that.getKeyColumn())) {
      return false;
    }
    if (!getValueColumn().equals(that.getValueColumn())) {
      return false;
    }

    return true;
  }

  public static List<String> getRequiredFields(List<KeyValueMap> keyValueMaps)
  {
    ImmutableSet<String> keySet = FluentIterable.from(keyValueMaps)
                                                .transform(
                                                    new Function<KeyValueMap, String>() {
                                                      @Nullable
                                                      @Override
                                                      public String apply(@Nullable KeyValueMap input) {
                                                        return input.keyColumn;
                                                      }
                                                    })
                                                .toSet();
    ImmutableSet<String> valueSet = FluentIterable.from(keyValueMaps)
                                                  .transform(
                                                      new Function<KeyValueMap, String>() {
                                                        @Override
                                                        public String apply(KeyValueMap input) {
                                                          return input.valueColumn;
                                                        }
                                                      })
                                                  .toSet();

    return new ImmutableSet.Builder<String>().addAll(keySet).addAll(valueSet).build().asList();
  }
}
