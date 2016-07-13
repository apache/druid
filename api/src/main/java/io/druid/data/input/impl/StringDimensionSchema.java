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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StringDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public static StringDimensionSchema create(String name) {
    int index = name.indexOf("?");
    int compareCacheEntry = -1;
    if (index > 0) {
      try {
        compareCacheEntry = Integer.parseInt(name.substring(index + 1));
        name = name.substring(0, index);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return new StringDimensionSchema(name, compareCacheEntry);
  }

  private final int compareCacheEntry;

  @JsonCreator
  public StringDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("compareCacheEntry") int compareCacheEntry
  )
  {
    super(name);
    this.compareCacheEntry = compareCacheEntry;
  }

  @Override
  public String getTypeName()
  {
    return DimensionSchema.STRING_TYPE_NAME;
  }

  @Override
  @JsonIgnore
  public ValueType getValueType()
  {
    return ValueType.STRING;
  }

  @JsonProperty
  public int getCompareCacheEntry()
  {
    return compareCacheEntry;
  }
}
