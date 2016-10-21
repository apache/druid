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

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import io.druid.collections.StupidPool;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class TopNQueryEngine
{
  private static final Logger log = new Logger(TopNQueryEngine.class);

  private final StupidPool<ByteBuffer> bufferPool;

  public TopNQueryEngine(StupidPool<ByteBuffer> bufferPool)
  {
    this.bufferPool = bufferPool;
  }

  public Sequence<Result<TopNResultValue>> query(final TopNQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> queryIntervals = query.getQuerySegmentSpec().getIntervals();
    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));
    final QueryGranularity granularity = query.getGranularity();
    final Function<Cursor, Result<TopNResultValue>> mapFn = getMapFn(query, adapter);

    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(filter, queryIntervals.get(0), granularity, query.isDescending()),
            new Function<Cursor, Result<TopNResultValue>>()
            {
              @Override
              public Result<TopNResultValue> apply(Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return mapFn.apply(input);
              }
            }
        ),
        Predicates.<Result<TopNResultValue>>notNull()
    );
  }

  private Function<Cursor, Result<TopNResultValue>> getMapFn(TopNQuery query, final StorageAdapter adapter)
  {
    final Capabilities capabilities = adapter.getCapabilities();
    final String dimension = query.getDimensionSpec().getDimension();

    final int cardinality = adapter.getDimensionCardinality(dimension);

    int numBytesPerRecord = 0;
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      numBytesPerRecord += aggregatorFactory.getMaxIntermediateSize();
    }

    final TopNAlgorithmSelector selector = new TopNAlgorithmSelector(cardinality, numBytesPerRecord);
    query.initTopNAlgorithmSelector(selector);

    final TopNAlgorithm topNAlgorithm;
    if (
        selector.isHasExtractionFn() &&
        // TimeExtractionTopNAlgorithm can work on any single-value dimension of type long.
        // Once we have arbitrary dimension types following check should be replaced by checking
        // that the column is of type long and single-value.
        dimension.equals(Column.TIME_COLUMN_NAME)
        ) {
      // A special TimeExtractionTopNAlgorithm is required, since DimExtractionTopNAlgorithm
      // currently relies on the dimension cardinality to support lexicographic sorting
      topNAlgorithm = new TimeExtractionTopNAlgorithm(capabilities, query);
    } else if (selector.isHasExtractionFn()) {
      topNAlgorithm = new DimExtractionTopNAlgorithm(capabilities, query);
    } else if (selector.isAggregateAllMetrics()) {
      topNAlgorithm = new PooledTopNAlgorithm(capabilities, query, bufferPool);
    } else if (selector.isAggregateTopNMetricFirst() || query.getContextBoolean("doAggregateTopNMetricFirst", false)) {
      topNAlgorithm = new AggregateTopNMetricFirstAlgorithm(capabilities, query, bufferPool);
    } else {
      topNAlgorithm = new PooledTopNAlgorithm(capabilities, query, bufferPool);
    }

    return new TopNMapFn(query, topNAlgorithm);
  }

  public static boolean canApplyExtractionInPost(TopNQuery query)
  {
    return query.getDimensionSpec() != null
           && query.getDimensionSpec().getExtractionFn() != null
           && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(query.getDimensionSpec().getExtractionFn().getExtractionType())
           && query.getTopNMetricSpec().canBeOptimizedUnordered();
  }
}
