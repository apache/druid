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

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnSize
{
  public static ColumnSize NO_DATA = new ColumnSize(-1L, new LinkedHashMap<>(), "No size information available");

  public static final String DATA_SECTION = "data";
  public static final String LONG_COLUMN_PART = "longValuesColumn";
  public static final String DOUBLE_COLUMN_PART = "doubleValuesColumn";
  public static final String ENCODED_VALUE_COLUMN_PART = "encodedValueColumn";
  public static final String STRING_VALUE_DICTIONARY_COLUMN_PART = "stringValueDictionary";
  public static final String LONG_VALUE_DICTIONARY_COLUMN_PART = "longValueDictionary";
  public static final String DOUBLE_VALUE_DICTIONARY_COLUMN_PART = "doubleValueDictionary";
  public static final String ARRAY_VALUE_DICTIONARY_COLUMN_PART = "arrayValueDictionary";
  public static final String NULL_VALUE_INDEX_COLUMN_PART = "nullValueIndex";
  public static final String BITMAP_VALUE_INDEX_COLUMN_PART = "bitmapValueIndexes";
  public static final String BITMAP_ARRAY_ELEMENT_INDEX_COLUMN_PART = "bitmapArrayElementIndexes";

  private final long size;
  private final LinkedHashMap<String, ColumnPartSize> components;
  @Nullable
  private final String errorMessage;

  @JsonCreator
  public ColumnSize(
      @JsonProperty("size") long size,
      @JsonProperty("components") LinkedHashMap<String, ColumnPartSize> components,
      @JsonProperty("errorMessage") @Nullable String errorMessage
  )
  {
    this.size = size;
    this.components = components;
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public LinkedHashMap<String, ColumnPartSize> getComponents()
  {
    return components;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMessage()
  {
    return errorMessage;
  }

  public ColumnSize merge(@Nullable ColumnSize other)
  {
    if (other == null) {
      return this;
    }
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
    String newErrorMessage;
    if (other.errorMessage != null) {
      newErrorMessage = errorMessage == null ? other.errorMessage : errorMessage + ", " + other.errorMessage;
    } else {
      newErrorMessage = errorMessage;
    }
    return new ColumnSize(newErrorMessage == null ? newSize : -1L, combined, newErrorMessage);
  }

  @Override
  public String toString()
  {
    return "ColumnSize{" +
           "size=" + size +
           ", components=" + components +
           ", errorMessage=" + errorMessage +
           '}';
  }
}
