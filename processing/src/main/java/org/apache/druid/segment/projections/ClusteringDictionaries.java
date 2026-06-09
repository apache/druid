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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Interner;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Per-type sorted-nulls-first dictionaries for a clustered base table's clustering values. Each
 * {@link TableClusterGroupSpec#getClusteringValueIds()} entry at position {@code i} indexes into the dictionary
 * for the column type at position {@code i}; columns of the same type share a dictionary.
 *
 * <p>String entries are interned via {@link DataSegment#stringInterner()} on construction since clustering
 * strings repeat heavily across cached segments. Numeric types aren't interned.
 */
public class ClusteringDictionaries
{
  public static final ClusteringDictionaries EMPTY = new ClusteringDictionaries(null, null, null, null);

  private final List<String> stringDictionary;
  private final List<Long> longDictionary;
  private final List<Double> doubleDictionary;
  private final List<Float> floatDictionary;

  @JsonCreator
  public ClusteringDictionaries(
      @JsonProperty("string") @Nullable List<String> stringDictionary,
      @JsonProperty("long") @Nullable List<Long> longDictionary,
      @JsonProperty("double") @Nullable List<Double> doubleDictionary,
      @JsonProperty("float") @Nullable List<Float> floatDictionary
  )
  {
    this.stringDictionary = internStringDictionary(stringDictionary);
    this.longDictionary = longDictionary == null ? List.of() : Collections.unmodifiableList(longDictionary);
    this.doubleDictionary = doubleDictionary == null ? List.of() : Collections.unmodifiableList(doubleDictionary);
    this.floatDictionary = floatDictionary == null ? List.of() : Collections.unmodifiableList(floatDictionary);
  }

  @JsonProperty("string")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getStringDictionary()
  {
    return stringDictionary;
  }

  @JsonProperty("long")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Long> getLongDictionary()
  {
    return longDictionary;
  }

  @JsonProperty("double")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Double> getDoubleDictionary()
  {
    return doubleDictionary;
  }

  @JsonProperty("float")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Float> getFloatDictionary()
  {
    return floatDictionary;
  }

  /**
   * Look up the typed value at position {@code id} in the dictionary for {@code type}. Throws on out-of-range
   * {@code id} or unsupported {@code type}.
   */
  @Nullable
  public Object lookupValue(ColumnType type, int id)
  {
    final List<?> dict = dictionaryForType(type);
    if (id < 0 || id >= dict.size()) {
      throw DruidException.defensive(
          "dictionary id [%s] is out of range for clustering type [%s] (size [%s])",
          id,
          type,
          dict.size()
      );
    }
    return dict.get(id);
  }

  public List<?> dictionaryForType(ColumnType type)
  {
    if (type == null) {
      throw DruidException.defensive("clustering type must not be null");
    }
    if (type.is(ValueType.STRING)) {
      return stringDictionary;
    }
    if (type.is(ValueType.LONG)) {
      return longDictionary;
    }
    if (type.is(ValueType.DOUBLE)) {
      return doubleDictionary;
    }
    if (type.is(ValueType.FLOAT)) {
      return floatDictionary;
    }
    throw DruidException.defensive("unsupported clustering type [%s]", type);
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
    ClusteringDictionaries that = (ClusteringDictionaries) o;
    return Objects.equals(stringDictionary, that.stringDictionary)
           && Objects.equals(longDictionary, that.longDictionary)
           && Objects.equals(doubleDictionary, that.doubleDictionary)
           && Objects.equals(floatDictionary, that.floatDictionary);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stringDictionary, longDictionary, doubleDictionary, floatDictionary);
  }

  @Override
  public String toString()
  {
    return "ClusteringDictionaries{" +
           "string=" + stringDictionary +
           ", long=" + longDictionary +
           ", double=" + doubleDictionary +
           ", float=" + floatDictionary +
           '}';
  }

  private static List<String> internStringDictionary(@Nullable List<String> dict)
  {
    if (dict == null || dict.isEmpty()) {
      return List.of();
    }
    final Interner<String> interner = DataSegment.stringInterner();
    final List<String> out = new ArrayList<>(dict.size());
    for (String s : dict) {
      out.add(s == null ? null : interner.intern(s));
    }
    return Collections.unmodifiableList(out);
  }
}
