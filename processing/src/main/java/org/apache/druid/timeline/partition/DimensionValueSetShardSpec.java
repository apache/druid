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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link NumberedShardSpec} that additionally declares, per dimension, the set of values a streaming segment
 * contains ({@link #partitionDimensionValues}), letting the broker prune segments whose values cannot match a query filter
 * before compaction. A dimension absent from {@link #partitionDimensionValues} is not pruned on.
 */
public class DimensionValueSetShardSpec extends NumberedShardSpec
{
  /**
   * Maps dimension name → exhaustive list of values that can appear in this shard for that dimension.
   * An absent dimension means "all values possible" (no pruning on that dimension).
   */
  private final Map<String, List<String>> partitionDimensionValues;

  /**
   * Maps dimension name → the {@link ColumnType} of the values stamped in {@link #partitionDimensionValues} for that
   * dimension. Only populated for types safe for typed value-set pruning (currently {@link ColumnType#LONG}); a
   * dimension absent from this map is pruned only via the string/range channel ({@link #possibleInDomain(Map)}),
   * never via {@link #possibleInValueDomain(Map)}.
   *
   * <p>This is the safety gate for numeric pruning: {@link #possibleInValueDomain(Map)} may only prune a dimension
   * when the query filter's match type equals the type recorded here, because value stringification only agrees
   * across ingest and query for identical types.
   */
  private final Map<String, ColumnType> dimensionColumnTypes;

  @JsonCreator
  public DimensionValueSetShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionDimensionValues") @Nullable Map<String, List<String>> partitionDimensionValues,
      @JsonProperty("dimensionColumnTypes") @Nullable Map<String, ColumnType> dimensionColumnTypes
  )
  {
    super(partitionNum, partitions);
    this.partitionDimensionValues = partitionDimensionValues == null ? Collections.emptyMap() : partitionDimensionValues;
    this.dimensionColumnTypes = dimensionColumnTypes == null ? Collections.emptyMap() : dimensionColumnTypes;
  }

  /**
   * Convenience constructor for callers that carry no per-dimension type information (e.g. non-numeric streaming,
   * the append→replace upgrade fallback, or tests). A null {@code dimensionColumnTypes} disables the typed
   * {@link #possibleInValueDomain(Map)} numeric-pruning channel but leaves the string/range channel
   * ({@link #possibleInDomain(Map)}) fully functional.
   */
  public DimensionValueSetShardSpec(
      int partitionNum,
      int partitions,
      @Nullable Map<String, List<String>> partitionDimensionValues
  )
  {
    this(partitionNum, partitions, partitionDimensionValues, null);
  }

  @JsonProperty("partitionDimensionValues")
  public Map<String, List<String>> getPartitionDimensionValues()
  {
    return partitionDimensionValues;
  }

  @JsonProperty("dimensionColumnTypes")
  public Map<String, ColumnType> getDimensionColumnTypes()
  {
    return dimensionColumnTypes;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return ImmutableList.copyOf(partitionDimensionValues.keySet());
  }

  /**
   * Returns false only when the query filter explicitly constrains a dimension that this shard declares,
   * and none of this shard's allowed values for that dimension fall within the filter domain.
   *
   * <p>A null entry in a dimension's allowed-values list denotes a row whose value was null/missing. Druid encodes a
   * null match in the query domain as the range {@code (-inf, "")} (see e.g. {@code NullFilter}), so a declared null
   * is tested against the domain as {@link Range#lessThan} {@code ""} rather than as a point value. Every other value
   * (including the empty string {@code ""}) is tested as a singleton point, keeping null and {@code ""} distinct.
   *
   * @return true if segment needs to be considered for query, false if it can be pruned
   */
  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (partitionDimensionValues.isEmpty()) {
      return true;
    }

    for (Map.Entry<String, List<String>> entry : partitionDimensionValues.entrySet()) {
      final String dimension = entry.getKey();
      final List<String> allowedValues = entry.getValue();

      if (dimensionColumnTypes.containsKey(dimension)) {
        // Canonicalized numeric dimension: pruned only via possibleInValueDomain, not the literal string range.
        continue;
      }

      final RangeSet<String> domainRangeSet = domain.get(dimension);
      if (domainRangeSet == null || domainRangeSet.isEmpty()) {
        // Query doesn't constrain this dimension — cannot prune on it.
        continue;
      }

      boolean anyMatch = false;
      for (String value : allowedValues) {
        // Null is represented in the domain as the range (-inf, ""); any other value as a singleton point.
        final Range<String> valueRange = value == null ? Range.lessThan("") : Range.singleton(value);
        if (!domainRangeSet.subRangeSet(valueRange).isEmpty()) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch) {
        return false;
      }
    }

    return true;
  }

  /**
   * Type-aware, set-membership pruning for numeric ({@code LONG}) equality/IN filters. For each dimension the query
   * constrains to a {@link TypedValueSet}, this shard can be pruned only when:
   * <ul>
   *   <li>this shard declares values for that dimension ({@link #partitionDimensionValues} contains it), and</li>
   *   <li>this shard has a stamped type for that dimension ({@link #dimensionColumnTypes}) that <em>equals</em> the
   *       filter's match type, and</li>
   *   <li>none of this shard's declared values for the dimension is in the filter's value set.</li>
   * </ul>
   *
   * <p>The type-equality check is the data-safety gate: if the stamped type is missing or differs from the filter's
   * type (e.g. a LONG filter against a DOUBLE-stamped dimension), stringified values may not agree ({@code "1"} vs
   * {@code "1.0"}), so this method must not prune and keeps the segment. Filters only produce LONG value sets today.
   *
   * @return true if the segment must be considered for the query, false if it can be pruned
   */
  @Override
  public boolean possibleInValueDomain(Map<String, TypedValueSet> valueDomain)
  {
    if (partitionDimensionValues.isEmpty()) {
      return true;
    }

    for (Map.Entry<String, TypedValueSet> entry : valueDomain.entrySet()) {
      final String dimension = entry.getKey();
      final TypedValueSet filterValues = entry.getValue();

      final List<String> allowedValues = partitionDimensionValues.get(dimension);
      if (allowedValues == null) {
        // This shard declares no values for this dimension, so it cannot be pruned on it.
        continue;
      }

      final ColumnType stampedType = dimensionColumnTypes.get(dimension);
      if (stampedType == null || !stampedType.equals(filterValues.getType())) {
        // No stamped type or a type mismatch: stringified values may not agree, so it is not safe to prune.
        continue;
      }

      boolean anyMatch = false;
      for (String value : allowedValues) {
        if (filterValues.contains(value)) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String getType()
  {
    return Type.DIM_VALUE_SET;
  }

  @Override
  public ShardSpec withPartitionNum(int partitionNum)
  {
    return new DimensionValueSetShardSpec(partitionNum, getNumCorePartitions(), partitionDimensionValues, dimensionColumnTypes);
  }

  @Override
  public ShardSpec withCorePartitions(int partitions)
  {
    return new DimensionValueSetShardSpec(getPartitionNum(), partitions, partitionDimensionValues, dimensionColumnTypes);
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
    if (!super.equals(o)) {
      return false;
    }
    DimensionValueSetShardSpec that = (DimensionValueSetShardSpec) o;
    return Objects.equals(partitionDimensionValues, that.partitionDimensionValues) &&
           Objects.equals(dimensionColumnTypes, that.dimensionColumnTypes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), partitionDimensionValues, dimensionColumnTypes);
  }

  @Override
  public String toString()
  {
    return "DimensionValueSetShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getNumCorePartitions() +
           ", partitionDimensionValues=" + partitionDimensionValues +
           ", dimensionColumnTypes=" + dimensionColumnTypes +
           '}';
  }
}
