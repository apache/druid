/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.collections.StupidPool;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.Filter;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
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

  public Iterable<Result<TopNResultValue>> query(final TopNQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> queryIntervals = query.getQuerySegmentSpec().getIntervals();
    final Filter filter = Filters.convertDimensionFilters(query.getDimensionsFilter());
    final QueryGranularity granularity = query.getGranularity();
    final Function<Cursor, Result<TopNResultValue>> mapFn = getMapFn(query, adapter);

    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return FunctionalIterable
        .create(adapter.makeCursors(filter, queryIntervals.get(0), granularity))
        .transform(
            new Function<Cursor, Cursor>()
            {
              @Override
              public Cursor apply(Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return input;
              }
            }
        )
        .keep(mapFn);
  }

  private Function<Cursor, Result<TopNResultValue>> getMapFn(TopNQuery query, final StorageAdapter adapter)
  {
    final Capabilities capabilities = adapter.getCapabilities();
    final int cardinality = adapter.getDimensionCardinality(query.getDimensionSpec().getDimension());
    int numBytesPerRecord = 0;
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      numBytesPerRecord += aggregatorFactory.getMaxIntermediateSize();
    }

    final TopNAlgorithmSelector selector = new TopNAlgorithmSelector(cardinality, numBytesPerRecord);
    query.initTopNAlgorithmSelector(selector);

    TopNAlgorithm topNAlgorithm = null;
    if (selector.isHasDimExtractionFn()) {
      topNAlgorithm = new DimExtractionTopNAlgorithm(capabilities, query);
    } else if (selector.isAggregateAllMetrics()) {
      topNAlgorithm = new PooledTopNAlgorithm(capabilities, query, bufferPool);
    } else if (selector.isAggregateTopNMetricFirst()) {
      topNAlgorithm = new AggregateTopNMetricFirstAlgorithm(capabilities, query, bufferPool);
    } else {
      topNAlgorithm = new PooledTopNAlgorithm(capabilities, query, bufferPool);
    }

    return new TopNMapFn(query, topNAlgorithm);
  }
}
