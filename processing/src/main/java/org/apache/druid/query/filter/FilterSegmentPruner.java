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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.RangeSet;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
  * Uses a {@link DimFilter} to check the {@link DimFilter#getDimensionRangeSet(String)} against
 * {@link ShardSpec#possibleInDomain(Map)} in order to 'prune' a set of segments whose rows would never match a filter
 * and avoid processing those segments in the first place.
  */
public class FilterSegmentPruner implements SegmentPruner
{
  private final DimFilter filter;
  private final Set<String> filterFields;
  private final Map<String, Optional<RangeSet<String>>> rangeCache;

  @JsonCreator
  public FilterSegmentPruner(
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("filterFields") @Nullable Set<String> filterFields
  )
  {
    this.filter = filter;
    this.filterFields = filterFields == null ? filter.getRequiredColumns() : filterFields;
    this.rangeCache = new HashMap<>();
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public Set<String> getFilterFields()
  {
    return filterFields;
  }

   /**
    * Filter the given iterable of objects by removing any object whose ShardSpec, obtained from the converter function,
    * does not fit in the RangeSet of the dimFilter {@link DimFilter#getDimensionRangeSet(String)}. The returned set
    * contains the filtered objects in the same order as they appear in input.
    *
    * {@link #rangeCache} stores the RangeSets of different dimensions for the filter, so it can be re-used between
    * calls to save redundant evaluation of {@link DimFilter#getDimensionRangeSet(String)} on the same columns.
    *
    * @param input               The iterable of objects to be filtered
    * @param converter           The function to convert T to ShardSpec that can be filtered by
    * @param <T>                 This can be any type, as long as transform function is provided to extract a ShardSpec
    *
    * @return The set of pruned object, in the same order as input
    */
  @Override
  public <T> Collection<T> prune(Iterable<T> input, Function<T, DataSegment> converter)
  {
    // LinkedHashSet retains order from "input".
    final Set<T> retSet = new LinkedHashSet<>();

    for (T obj : input) {
      final DataSegment segment = converter.apply(obj);
      if (segment == null) {
        continue;
      }
      final ShardSpec shard = segment.getShardSpec();
      boolean include = true;

      if (shard != null) {
        Map<String, RangeSet<String>> filterDomain = new HashMap<>();
        List<String> dimensions = shard.getDomainDimensions();
        for (String dimension : dimensions) {
          if (filterFields == null || filterFields.contains(dimension)) {
            Optional<RangeSet<String>> optFilterRangeSet = rangeCache
                .computeIfAbsent(dimension, d -> Optional.ofNullable(filter.getDimensionRangeSet(d)));

            if (optFilterRangeSet.isPresent()) {
              filterDomain.put(dimension, optFilterRangeSet.get());
            }
          }
        }
        if (!filterDomain.isEmpty() && !shard.possibleInDomain(filterDomain)) {
          include = false;
        }
      }

      if (include) {
        retSet.add(obj);
      }
    }
    return retSet;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterSegmentPruner that = (FilterSegmentPruner) o;
    return Objects.equals(filter, that.filter) && Objects.equals(filterFields, that.filterFields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(filter, filterFields);
  }

  @Override
  public String toString()
  {
    return "FilterSegmentPruner{" +
           "filter=" + filter +
           ", filterFields=" + filterFields +
           ", rangeCache=" + rangeCache +
           '}';
  }
}
