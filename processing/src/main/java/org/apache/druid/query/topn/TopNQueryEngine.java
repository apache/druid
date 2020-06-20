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

package org.apache.druid.query.topn;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class TopNQueryEngine
{

  private final NonBlockingPool<ByteBuffer> bufferPool;

  public TopNQueryEngine(NonBlockingPool<ByteBuffer> bufferPool)
  {
    this.bufferPool = bufferPool;
  }

  /**
   * Do the thing - process a {@link StorageAdapter} into a {@link Sequence} of {@link TopNResultValue}, with one of the
   * fine {@link TopNAlgorithm} available chosen based on the type of column being aggregated. The algorithm provides a
   * mapping function to process rows from the adapter {@link org.apache.druid.segment.Cursor} to apply
   * {@link AggregatorFactory} and create or update {@link TopNResultValue}
   */
  public Sequence<Result<TopNResultValue>> query(
      final TopNQuery query,
      final StorageAdapter adapter,
      final @Nullable TopNQueryMetrics queryMetrics
  )
  {
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> queryIntervals = query.getQuerySegmentSpec().getIntervals();
    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));
    final Granularity granularity = query.getGranularity();
    final TopNMapFn mapFn = getMapFn(query, adapter, queryMetrics);

    Preconditions.checkArgument(
        queryIntervals.size() == 1,
        "Can only handle a single interval, got[%s]",
        queryIntervals
    );

    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(
                filter,
                queryIntervals.get(0),
                query.getVirtualColumns(),
                granularity,
                query.isDescending(),
                queryMetrics
            ),
            input -> {
              if (queryMetrics != null) {
                queryMetrics.cursor(input);
              }
              return mapFn.apply(input, queryMetrics);
            }
        ),
        Predicates.notNull()
    );
  }

  /**
   * Choose the best {@link TopNAlgorithm} for the given query.
   */
  private TopNMapFn getMapFn(
      final TopNQuery query,
      final StorageAdapter adapter,
      final @Nullable TopNQueryMetrics queryMetrics
  )
  {
    final String dimension = query.getDimensionSpec().getDimension();
    final int cardinality = adapter.getDimensionCardinality(dimension);
    if (queryMetrics != null) {
      queryMetrics.dimensionCardinality(cardinality);
    }

    int numBytesPerRecord = 0;
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      numBytesPerRecord += aggregatorFactory.getMaxIntermediateSizeWithNulls();
    }

    final TopNAlgorithmSelector selector = new TopNAlgorithmSelector(cardinality, numBytesPerRecord);
    query.initTopNAlgorithmSelector(selector);

    final ColumnCapabilities columnCapabilities = query.getVirtualColumns()
                                                       .getColumnCapabilitiesWithFallback(adapter, dimension);


    final TopNAlgorithm<?, ?> topNAlgorithm;
    if (canUsePooledAlgorithm(selector, query, columnCapabilities)) {
      // pool based algorithm selection, if we can
      if (selector.isAggregateAllMetrics()) {
        // if sorted by dimension we should aggregate all metrics in a single pass, use the regular pooled algorithm for
        // this
        topNAlgorithm = new PooledTopNAlgorithm(adapter, query, bufferPool);
      } else if (selector.isAggregateTopNMetricFirst() || query.getContextBoolean("doAggregateTopNMetricFirst", false)) {
        // for high cardinality dimensions with larger result sets we aggregate with only the ordering aggregation to
        // compute the first 'n' values, and then for the rest of the metrics but for only the 'n' values
        topNAlgorithm = new AggregateTopNMetricFirstAlgorithm(adapter, query, bufferPool);
      } else {
        // anything else, use the regular pooled algorithm
        topNAlgorithm = new PooledTopNAlgorithm(adapter, query, bufferPool);
      }
    } else {
      // heap based algorithm selection, if we must
      if (selector.isHasExtractionFn() && dimension.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        // TimeExtractionTopNAlgorithm can work on any single-value dimension of type long.
        // We might be able to use this for any long column with an extraction function, that is
        //  ValueType.LONG.equals(columnCapabilities.getType())
        // but this needs investigation to ensure that it is an improvement over HeapBasedTopNAlgorithm

        // A special TimeExtractionTopNAlgorithm is required since HeapBasedTopNAlgorithm
        // currently relies on the dimension cardinality to support lexicographic sorting
        topNAlgorithm = new TimeExtractionTopNAlgorithm(adapter, query);
      } else {
        topNAlgorithm = new HeapBasedTopNAlgorithm(adapter, query);
      }
    }
    if (queryMetrics != null) {
      queryMetrics.algorithm(topNAlgorithm);
    }

    return new TopNMapFn(query, topNAlgorithm);
  }

  /**
   * {@link PooledTopNAlgorithm} (and {@link AggregateTopNMetricFirstAlgorithm} which utilizes the pooled
   * algorithm) are optimized off-heap algorithms for aggregating dictionary encoded string columns. These algorithms
   * rely on dictionary ids being unique so to aggregate on the dictionary ids directly and defer
   * {@link org.apache.druid.segment.DimensionSelector#lookupName(int)} until as late as possible in query processing.
   *
   * When these conditions are not true, we have an on-heap fall-back algorithm, the {@link HeapBasedTopNAlgorithm}
   * (and {@link TimeExtractionTopNAlgorithm} for a specialized form for long columns) which aggregates on values of
   * selectors.
   */
  private static boolean canUsePooledAlgorithm(
      final TopNAlgorithmSelector selector,
      final TopNQuery query,
      final ColumnCapabilities capabilities
  )
  {
    if (selector.isHasExtractionFn()) {
      // extraction functions can have a many to one mapping, and should use a heap algorithm
      return false;
    }

    if (query.getDimensionSpec().getOutputType() != ValueType.STRING) {
      // non-string output cannot use the pooled algorith, even if the underlying selector supports it
      return false;
    }
    if (capabilities != null && capabilities.getType() == ValueType.STRING) {
      // string columns must use the on heap algorithm unless they have the following capabilites
      return capabilities.isDictionaryEncoded() && capabilities.areDictionaryValuesUnique().isTrue();
    } else {
      // non-strings are not eligible to use the pooled algorithm, and should use a heap algorithm
      return false;
    }
  }

  /**
   * {@link ExtractionFn} which are one to one may have their execution deferred until as late as possible, since the
   * which value is used as the grouping key itself doesn't particularly matter. For top-n, this method allows the
   * query to be transformed in {@link TopNQueryQueryToolChest#preMergeQueryDecoration} to strip off the
   * {@link ExtractionFn} on the broker, so that a more optimized algorithm (e.g. {@link PooledTopNAlgorithm}) can be
   * chosen for processing segments, and then added back and evaluated against the final merged result sets on the
   * broker via {@link TopNQueryQueryToolChest#postMergeQueryDecoration}.
   */
  public static boolean canApplyExtractionInPost(TopNQuery query)
  {
    return query.getDimensionSpec() != null
           && query.getDimensionSpec().getExtractionFn() != null
           && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(query.getDimensionSpec()
                                                                 .getExtractionFn()
                                                                 .getExtractionType())
           && query.getTopNMetricSpec().canBeOptimizedUnordered();
  }
}
