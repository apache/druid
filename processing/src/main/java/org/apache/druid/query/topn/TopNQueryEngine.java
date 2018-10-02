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

import com.google.common.base.Function;
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
import org.apache.druid.segment.Cursor;
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
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
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
            new Function<Cursor, Result<TopNResultValue>>()
            {
              @Override
              public Result<TopNResultValue> apply(Cursor input)
              {
                if (queryMetrics != null) {
                  queryMetrics.cursor(input);
                }
                return mapFn.apply(input, queryMetrics);
              }
            }
        ),
        Predicates.notNull()
    );
  }

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

    final TopNAlgorithm topNAlgorithm;
    if (
        selector.isHasExtractionFn() &&
        // TimeExtractionTopNAlgorithm can work on any single-value dimension of type long.
        // Once we have arbitrary dimension types following check should be replaced by checking
        // that the column is of type long and single-value.
        dimension.equals(ColumnHolder.TIME_COLUMN_NAME)
        ) {
      // A special TimeExtractionTopNAlgorithm is required, since DimExtractionTopNAlgorithm
      // currently relies on the dimension cardinality to support lexicographic sorting
      topNAlgorithm = new TimeExtractionTopNAlgorithm(adapter, query);
    } else if (selector.isHasExtractionFn()) {
      topNAlgorithm = new DimExtractionTopNAlgorithm(adapter, query);
    } else if (columnCapabilities != null && !(columnCapabilities.getType() == ValueType.STRING
                                               && columnCapabilities.isDictionaryEncoded())) {
      // Use DimExtraction for non-Strings and for non-dictionary-encoded Strings.
      topNAlgorithm = new DimExtractionTopNAlgorithm(adapter, query);
    } else if (query.getDimensionSpec().getOutputType() != ValueType.STRING) {
      // Use DimExtraction when the dimension output type is a non-String. (It's like an extractionFn: there can be
      // a many-to-one mapping, since numeric types can't represent all possible values of other types.)
      topNAlgorithm = new DimExtractionTopNAlgorithm(adapter, query);
    } else if (selector.isAggregateAllMetrics()) {
      topNAlgorithm = new PooledTopNAlgorithm(adapter, query, bufferPool);
    } else if (selector.isAggregateTopNMetricFirst() || query.getContextBoolean("doAggregateTopNMetricFirst", false)) {
      topNAlgorithm = new AggregateTopNMetricFirstAlgorithm(adapter, query, bufferPool);
    } else {
      topNAlgorithm = new PooledTopNAlgorithm(adapter, query, bufferPool);
    }
    if (queryMetrics != null) {
      queryMetrics.algorithm(topNAlgorithm);
    }

    return new TopNMapFn(query, topNAlgorithm);
  }

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
