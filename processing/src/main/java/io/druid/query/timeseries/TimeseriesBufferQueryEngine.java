/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.KernelAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.BufferCursor;
import io.druid.segment.Cursor;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;
import java.util.List;

public class TimeseriesBufferQueryEngine
{
  public Sequence<Result<TimeseriesResultValue>> process(final TimeseriesQuery query, final StorageAdapter adapter)
  {
    return new BaseSequence<Result<TimeseriesResultValue>, Iterator<Result<TimeseriesResultValue>>>(
        new BaseSequence.IteratorMaker<Result<TimeseriesResultValue>, Iterator<Result<TimeseriesResultValue>>>()
        {
          @Override
          public Iterator<Result<TimeseriesResultValue>> make()
          {
            List<Interval> queryIntervals = query.getQuerySegmentSpec().getIntervals();
            Preconditions.checkArgument(
                queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
            );


            QueryableIndexStorageAdapter bufferAdapter = (QueryableIndexStorageAdapter) adapter;
            BufferCursor cursor = bufferAdapter.makeBufferCursor(queryIntervals.get(0), query.getGranularity());

            final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

            Preconditions.checkArgument(aggregatorSpecs.size() == 1);
            AggregatorFactory kernelFactory = aggregatorSpecs.get(0);

            KernelAggregator aggregator = (KernelAggregator) kernelFactory.factorizeBuffered(cursor);

            ByteBuffer buffer = ByteBuffer.allocateDirect(kernelFactory.getMaxIntermediateSize());
            int pos = 0;

            while (!cursor.isDone()) {
              aggregator.copyBuffer();
              cursor.advance();
            }
            aggregator.run(buffer, pos);

            Pair<LongBuffer, IntBuffer> bucketOffsets = cursor.makeBucketOffsets();

            // do something here

//            TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime(time));
//
//            bob.addMetric(kernelFactory.getName(), aggregator.get(buffer, pos));
//
//
//            Result<TimeseriesResultValue> retVal = bob.build();
//                      aggregator.close();

            throw new UnsupportedOperationException();
          }

          @Override
          public void cleanup(Iterator<Result<TimeseriesResultValue>> toClean)
          {
            // https://github.com/metamx/druid/issues/128
            while (toClean.hasNext()) {
              toClean.next();
            }
          }
        }
    );
  }
}
