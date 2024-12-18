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
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@PublicApi
public class Metadata
{
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
  private final List<OrderBy> ordering;
  @Nullable
  private final List<AggregateProjectionMetadata> projections;

  public Metadata(
      @JsonProperty("container") @Nullable Map<String, Object> container,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("queryGranularity") @Nullable Granularity queryGranularity,
      @JsonProperty("rollup") @Nullable Boolean rollup,
      @JsonProperty("ordering") @Nullable List<OrderBy> ordering,
      @JsonProperty("projections") @Nullable List<AggregateProjectionMetadata> projections
  )
  {
    this.container = container == null ? new ConcurrentHashMap<>() : container;
    this.aggregators = aggregators;
    this.timestampSpec = timestampSpec;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
    this.ordering = ordering;
    this.projections = projections;
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

  /**
   * Ordering for the segment associated with this object. Nonnull for segments written in current versions of Druid,
   * but would null for older segments. Null may be interpreted as {@link Cursors#ascendingTimeOrder()}, since prior
   * to this field being added, segments were always time-ordered.
   *
   * When dealing with {@link QueryableIndex}, it is generally better to use {@link QueryableIndex#getOrdering()}, since
   * that method never returns null.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<AggregateProjectionMetadata> getProjections()
  {
    return projections;
  }

  public Metadata withProjections(List<AggregateProjectionMetadata> projections)
  {
    return new Metadata(
        container,
        aggregators,
        timestampSpec,
        queryGranularity,
        rollup,
        ordering,
        projections
    );
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
    List<List<OrderBy>> orderingsToMerge = new ArrayList<>();
    List<List<AggregateProjectionMetadata>> projectionsToMerge = new ArrayList<>();

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

        orderingsToMerge.add(metadata.getOrdering());
        projectionsToMerge.add(metadata.getProjections());
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

    final List<OrderBy> mergedOrdering = mergeOrderings(orderingsToMerge);
    validateProjections(projectionsToMerge);

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
        mergedOrdering,
        projectionsToMerge.get(0) // we're going to replace this later with updated rowcount
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
           Objects.equals(ordering, metadata.ordering) &&
           Objects.equals(projections, metadata.projections);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, Arrays.hashCode(aggregators), timestampSpec, queryGranularity, rollup, ordering, projections);
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
           ", ordering=" + ordering +
           ", projections=" + projections +
           '}';
  }

  /**
   * Merge {@link #getOrdering()} from different metadatas.
   *
   * When an input sort order is null, we assume it is {@link Cursors#ascendingTimeOrder()}, as this was the only
   * sort order possible prior to the introduction of the "ordering" field.
   */
  public static List<OrderBy> mergeOrderings(List<List<OrderBy>> orderingsToMerge)
  {
    if (orderingsToMerge.isEmpty()) {
      throw new IAE("orderingsToMerge is empty");
    }

    final List<OrderBy> mergedOrdering = new ArrayList<>();

    while (true) {
      final int position = mergedOrdering.size();
      OrderBy orderBy = null;

      // Iterate through each sort order, check that the columns at "position" are all the same. If not, return
      // the mergedOrdering as-is.
      for (List<OrderBy> ordering : orderingsToMerge) {
        if (ordering == null) {
          // null ordering is treated as [__time].
          ordering = Cursors.ascendingTimeOrder();
        }

        if (position < ordering.size()) {
          if (orderBy == null) {
            orderBy = ordering.get(position);
          } else if (!orderBy.equals(ordering.get(position))) {
            return mergedOrdering;
          }
        } else {
          return mergedOrdering;
        }
      }

      mergedOrdering.add(orderBy);
    }
  }

  public static void validateProjections(List<List<AggregateProjectionMetadata>> projectionsToMerge)
  {
    final Map<String, AggregateProjectionMetadata> projectionsMap = new HashMap<>();
    // dedupe by name, fail if somehow incompatible projections are defined
    int nullCount = 0;
    int expectedSize = -1;
    for (List<AggregateProjectionMetadata> projections : projectionsToMerge) {
      if (projections == null) {
        nullCount++;
        continue;
      }
      if (expectedSize < 0) {
        expectedSize = projections.size();
      } else if (projections.size() != expectedSize) {
        throw DruidException.defensive("Unable to merge projections: mismatched projections count");
      }
      for (AggregateProjectionMetadata projection : projections) {
        AggregateProjectionMetadata prev = projectionsMap.putIfAbsent(projection.getSchema().getName(), projection);
        if (prev != null && !prev.getSchema().equals(projection.getSchema())) {
          throw DruidException.defensive("Unable to merge projections: mismatched projections [%s] and [%s]", prev, projection);
        }
      }
    }
    if (nullCount > 0) {
      if (nullCount != projectionsToMerge.size()) {
        throw DruidException.defensive("Unable to merge projections: some projections were null");
      }
    }
  }
}
