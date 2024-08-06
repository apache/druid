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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
@PublicApi
public class Metadata
{
  /**
   * Return value for {@link #getSortOrder()} when a segment is sorted by {@link ColumnHolder#TIME_COLUMN_NAME} only.
   */
  public static final List<String> SORTED_BY_TIME_ONLY = Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME);

  // container is used for arbitrary key-value pairs in segment metadata e.g.
  // kafka input reader uses it to store commit offset
  private final Map<String, Object> container;
  @Nullable
  private final AggregatorFactory[] aggregators;
  @Nullable
  private final TimestampSpec timestampSpec;
  @Nullable
  private final Granularity queryGranularity;
  @Nullable
  private final Boolean rollup;
  @Nullable
  private final List<String> sortOrder;

  public Metadata(
      @JsonProperty("container") @Nullable Map<String, Object> container,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("queryGranularity") @Nullable Granularity queryGranularity,
      @JsonProperty("rollup") @Nullable Boolean rollup,
      @JsonProperty("sortOrder") @Nullable List<String> sortOrder
  )
  {
    this.container = container == null ? new ConcurrentHashMap<>() : container;
    this.aggregators = aggregators;
    this.timestampSpec = timestampSpec;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
    this.sortOrder = sortOrder;
  }

  @JsonProperty
  public Map<String, Object> getContainer()
  {
    return container;
  }

  @JsonProperty
  @Nullable
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  @Nullable
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  @Nullable
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  @Nullable
  public Boolean isRollup()
  {
    return rollup;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSortOrder()
  {
    return sortOrder;
  }

  public Metadata putAll(@Nullable Map<String, Object> other)
  {
    if (other != null) {
      container.putAll(other);
    }
    return this;
  }

  // arbitrary key-value pairs from the metadata just follow the semantics of last one wins if same
  // key exists in multiple input Metadata containers
  // for others e.g. Aggregators, appropriate merging is done
  @Nullable
  public static Metadata merge(
      @Nullable List<Metadata> toBeMerged,
      @Nullable AggregatorFactory[] overrideMergedAggregators
  )
  {
    if (toBeMerged == null || toBeMerged.size() == 0) {
      return null;
    }

    boolean foundSomeMetadata = false;
    Map<String, Object> mergedContainer = new HashMap<>();
    List<AggregatorFactory[]> aggregatorsToMerge = overrideMergedAggregators == null
                                                   ? new ArrayList<>()
                                                   : null;

    List<TimestampSpec> timestampSpecsToMerge = new ArrayList<>();
    List<Granularity> gransToMerge = new ArrayList<>();
    List<Boolean> rollupToMerge = new ArrayList<>();
    List<List<String>> sortOrdersToMerge = new ArrayList<>();

    for (Metadata metadata : toBeMerged) {
      if (metadata != null) {
        foundSomeMetadata = true;
        if (aggregatorsToMerge != null) {
          aggregatorsToMerge.add(metadata.getAggregators());
        }

        if (timestampSpecsToMerge != null && metadata.getTimestampSpec() != null) {
          timestampSpecsToMerge.add(metadata.getTimestampSpec());
        }

        if (gransToMerge != null) {
          gransToMerge.add(metadata.getQueryGranularity());
        }

        if (rollupToMerge != null) {
          rollupToMerge.add(metadata.isRollup());
        }

        sortOrdersToMerge.add(metadata.getSortOrder());
        mergedContainer.putAll(metadata.container);
      } else {
        //if metadata and hence aggregators and queryGranularity for some segment being merged are unknown then
        //final merged segment should not have same in metadata
        aggregatorsToMerge = null;
        timestampSpecsToMerge = null;
        gransToMerge = null;
        rollupToMerge = null;
      }
    }

    if (!foundSomeMetadata) {
      return null;
    }

    final AggregatorFactory[] mergedAggregators = aggregatorsToMerge == null ?
                                                  overrideMergedAggregators :
                                                  AggregatorFactory.mergeAggregators(aggregatorsToMerge);

    final TimestampSpec mergedTimestampSpec = timestampSpecsToMerge == null ?
                                              null :
                                              TimestampSpec.mergeTimestampSpec(timestampSpecsToMerge);

    final Granularity mergedGranularity = gransToMerge == null ?
                                          null :
                                          Granularity.mergeGranularities(gransToMerge);

    final List<String> mergedSortOrder = mergeSortOrders(sortOrdersToMerge);

    Boolean rollup = null;
    if (rollupToMerge != null && !rollupToMerge.isEmpty()) {
      rollup = rollupToMerge.get(0);
      for (Boolean r : rollupToMerge) {
        if (r == null) {
          rollup = null;
          break;
        } else if (!r.equals(rollup)) {
          rollup = null;
          break;
        } else {
          rollup = r;
        }
      }
    }

    return new Metadata(
        mergedContainer,
        mergedAggregators,
        mergedTimestampSpec,
        mergedGranularity,
        rollup,
        mergedSortOrder
    );
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
    final Metadata metadata = (Metadata) o;
    return Objects.equals(container, metadata.container) &&
           Arrays.equals(aggregators, metadata.aggregators) &&
           Objects.equals(timestampSpec, metadata.timestampSpec) &&
           Objects.equals(queryGranularity, metadata.queryGranularity) &&
           Objects.equals(rollup, metadata.rollup) &&
           Objects.equals(sortOrder, metadata.sortOrder);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, Arrays.hashCode(aggregators), timestampSpec, queryGranularity, rollup);
  }

  @Override
  public String toString()
  {
    return "Metadata{" +
           "container=" + container +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", timestampSpec=" + timestampSpec +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           ", sortOrder=" + sortOrder +
           '}';
  }

  @Nullable
  private static List<String> mergeSortOrders(List<List<String>> sortOrdersToMerge)
  {
    if (sortOrdersToMerge.isEmpty()) {
      return null;
    }

    if (sortOrdersToMerge.stream().anyMatch(Objects::isNull)) {
      return null;
    }

    final List<String> mergedSortOrder = new ArrayList<>();

    while (true) {
      String column = null;

      for (final List<String> sortOrder : sortOrdersToMerge) {
        if (mergedSortOrder.size() >= sortOrder.size()) {
          return mergedSortOrder;
        }

        if (column == null) {
          column = sortOrder.get(mergedSortOrder.size());
        } else if (!column.equals(sortOrder.get(mergedSortOrder.size()))) {
          return mergedSortOrder;
        }
      }

      mergedSortOrder.add(column);
    }
  }
}
