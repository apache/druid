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
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.CursorGranularizer;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.topn.types.TopNColumnAggregatesProcessor;
import org.apache.druid.query.topn.types.TopNColumnAggregatesProcessorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.TopNOptimizationInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;

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
   * Do the thing - process a {@link Segment} into a {@link Sequence} of {@link TopNResultValue}, with one of the
   * fine {@link TopNAlgorithm} available chosen based on the type of column being aggregated. The algorithm provides a
   * mapping function to process rows from the adapter {@link Cursor} to apply {@link AggregatorFactory} and create or
   * update {@link TopNResultValue}
   */
  public Sequence<Result<TopNResultValue>> query(
      TopNQuery query,
      final Segment segment,
      @Nullable final TopNQueryMetrics queryMetrics
  )
  {
    final CursorFactory cursorFactory = segment.asCursorFactory();
    if (cursorFactory == null) {
      throw new SegmentMissingException(
          "Null cursor factory found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final CursorBuildSpec buildSpec = makeCursorBuildSpec(query, queryMetrics);
    final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec);
    if (cursorHolder.isPreAggregated()) {
      query = query.withAggregatorSpecs(Preconditions.checkNotNull(cursorHolder.getAggregatorsForPreAggregated()));
    }
    final Cursor cursor = cursorHolder.asCursor();
    if (cursor == null) {
      return Sequences.withBaggage(Sequences.empty(), cursorHolder);
    }

    final TimeBoundaryInspector timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);

    final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

    final ColumnSelectorPlus<TopNColumnAggregatesProcessor<?>> selectorPlus =
        DimensionHandlerUtils.createColumnSelectorPlus(
            new TopNColumnAggregatesProcessorFactory(query.getDimensionSpec().getOutputType()),
            query.getDimensionSpec(),
            factory
        );

    final int cardinality;
    if (selectorPlus.getSelector() instanceof DimensionDictionarySelector) {
      cardinality = ((DimensionDictionarySelector) selectorPlus.getSelector()).getValueCardinality();
    } else {
      cardinality = DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }
    final TopNCursorInspector cursorInspector = new TopNCursorInspector(
        factory,
        segment.as(TopNOptimizationInspector.class),
        segment.getDataInterval(),
        cardinality
    );

    final CursorGranularizer granularizer = CursorGranularizer.create(
        cursor,
        timeBoundaryInspector,
        cursorHolder.getTimeOrder(),
        query.getGranularity(),
        buildSpec.getInterval()
    );
    if (granularizer == null || selectorPlus.getSelector() == null) {
      return Sequences.withBaggage(Sequences.empty(), cursorHolder);
    }

    if (queryMetrics != null) {
      queryMetrics.cursor(cursor);
    }
    final TopNMapFn mapFn = getMapFn(query, cursorInspector, queryMetrics);
    return Sequences.filter(
        Sequences.simple(granularizer.getBucketIterable())
                 .map(bucketInterval -> {
                   granularizer.advanceToBucket(bucketInterval);
                   return mapFn.apply(cursor, selectorPlus, granularizer, queryMetrics);
                 }),
                 Predicates.notNull()
    ).withBaggage(cursorHolder);
  }

  /**
   * Choose the best {@link TopNAlgorithm} for the given query.
   */
  private TopNMapFn getMapFn(
      final TopNQuery query,
      final TopNCursorInspector cursorInspector,
      final @Nullable TopNQueryMetrics queryMetrics
  )
  {
    final String dimension = query.getDimensionSpec().getDimension();


    if (queryMetrics != null) {
      queryMetrics.dimensionCardinality(cursorInspector.getDimensionCardinality());
    }

    int numBytesPerRecord = 0;
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      numBytesPerRecord += aggregatorFactory.getMaxIntermediateSizeWithNulls();
    }

    final TopNAlgorithmSelector selector = new TopNAlgorithmSelector(
        cursorInspector.getDimensionCardinality(),
        numBytesPerRecord
    );
    query.initTopNAlgorithmSelector(selector);

    final ColumnCapabilities columnCapabilities = query.getVirtualColumns().getColumnCapabilitiesWithFallback(
        cursorInspector.getColumnInspector(),
        dimension
    );

    final TopNAlgorithm<?, ?> topNAlgorithm;
    if (canUsePooledAlgorithm(selector, query, columnCapabilities, bufferPool, cursorInspector.getDimensionCardinality(), numBytesPerRecord)) {
      // pool based algorithm selection, if we can
      if (selector.isAggregateAllMetrics()) {
        // if sorted by dimension we should aggregate all metrics in a single pass, use the regular pooled algorithm for
        // this
        topNAlgorithm = new PooledTopNAlgorithm(
            query,
            cursorInspector,
            bufferPool
        );
      } else if (shouldUseAggregateMetricFirstAlgorithm(query, selector)) {
        // for high cardinality dimensions with larger result sets we aggregate with only the ordering aggregation to
        // compute the first 'n' values, and then for the rest of the metrics but for only the 'n' values
        topNAlgorithm = new AggregateTopNMetricFirstAlgorithm(
            query,
            cursorInspector,
            bufferPool
        );
      } else {
        // anything else, use the regular pooled algorithm
        topNAlgorithm = new PooledTopNAlgorithm(
            query,
            cursorInspector,
            bufferPool
        );
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
        topNAlgorithm = new TimeExtractionTopNAlgorithm(query, cursorInspector);
      } else {
        topNAlgorithm = new HeapBasedTopNAlgorithm(query, cursorInspector);
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
   * <p>
   * When these conditions are not true, we have an on-heap fall-back algorithm, the {@link HeapBasedTopNAlgorithm}
   * (and {@link TimeExtractionTopNAlgorithm} for a specialized form for long columns) which aggregates on values of
   * selectors.
   */
  private static boolean canUsePooledAlgorithm(
      final TopNAlgorithmSelector selector,
      final TopNQuery query,
      final ColumnCapabilities capabilities,
      final NonBlockingPool<ByteBuffer> bufferPool,
      final int cardinality,
      final int numBytesPerRecord
  )
  {
    if (cardinality < 0) {
      // unknown cardinality doesn't work with the pooled algorithm which requires an exact count of dictionary ids
      return false;
    }

    if (selector.isHasExtractionFn()) {
      // extraction functions can have a many to one mapping, and should use a heap algorithm
      return false;
    }

    if (!query.getDimensionSpec().getOutputType().is(ValueType.STRING)) {
      // non-string output cannot use the pooled algorith, even if the underlying selector supports it
      return false;
    }

    if (!Types.is(capabilities, ValueType.STRING)) {
      // non-strings are not eligible to use the pooled algorithm, and should use a heap algorithm
      return false;
    }

    if (!capabilities.isDictionaryEncoded().isTrue() || !capabilities.areDictionaryValuesUnique().isTrue()) {
      // string columns must use the on heap algorithm unless they have the following capabilites
      return false;
    }

    // num values per pass must be greater than 0 or else the pooled algorithm cannot progress
    try (final ResourceHolder<ByteBuffer> resultsBufHolder = bufferPool.take()) {
      final ByteBuffer resultsBuf = resultsBufHolder.get();

      final int numBytesToWorkWith = resultsBuf.capacity();
      final int numValuesPerPass = numBytesPerRecord > 0 ? numBytesToWorkWith / numBytesPerRecord : cardinality;

      final boolean allowMultiPassPooled = query.context().getBoolean(
          QueryContexts.TOPN_USE_MULTI_PASS_POOLED_QUERY_GRANULARITY,
          false
      );
      if (Granularities.ALL.equals(query.getGranularity()) || allowMultiPassPooled) {
        return numValuesPerPass > 0;
      }

      // if not using multi-pass for pooled + query granularity other than 'ALL', we must check that all values can fit
      // in a single pass
      return numValuesPerPass >= cardinality;
    }
  }

  private static boolean shouldUseAggregateMetricFirstAlgorithm(TopNQuery query, TopNAlgorithmSelector selector)
  {
    // must be using ALL granularity since it makes multiple passes and must reset the cursor
    if (Granularities.ALL.equals(query.getGranularity())) {
      return selector.isAggregateTopNMetricFirst() || query.context().getBoolean("doAggregateTopNMetricFirst", false);
    }
    return false;
  }

  public static CursorBuildSpec makeCursorBuildSpec(TopNQuery query, @Nullable QueryMetrics<?> queryMetrics)
  {
    return Granularities.decorateCursorBuildSpec(
        query,
        CursorBuildSpec.builder()
                       .setInterval(query.getSingleInterval())
                       .setFilter(Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter())))
                       .setGroupingColumns(Collections.singletonList(query.getDimensionSpec().getDimension()))
                       .setVirtualColumns(query.getVirtualColumns())
                       .setPhysicalColumns(query.getRequiredColumns())
                       .setAggregators(query.getAggregatorSpecs())
                       .setQueryContext(query.context())
                       .setQueryMetrics(queryMetrics)
                       .build()
    );
  }

  /**
   * {@link ExtractionFn} which are one to one may have their execution deferred until as late as possible, since
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
