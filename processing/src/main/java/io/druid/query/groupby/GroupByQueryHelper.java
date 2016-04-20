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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupByQueryHelper
{
  private static final String CTX_KEY_MAX_RESULTS = "maxResults";
  final static String CTX_KEY_SORT_RESULTS = "sortResults";
  final static String CTX_KEY_LIMIT_DURING_MERGE = "limitDuringMerge";
  final static String CTX_KEY_DISABLE_LIMIT_DURING_MERGE = "disableLimitDuringMerge";

  public static <T> Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> createIndexAccumulatorPair(
      final GroupByQuery query,
      final GroupByQueryConfig config,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    final QueryGranularity gran = query.getGranularity();
    final long timeStart = query.getIntervals().get(0).getStartMillis();

    // use gran.iterable instead of gran.truncate so that
    // AllGranularity returns timeStart instead of Long.MIN_VALUE
    final long granTimeStart = gran.iterable(timeStart, timeStart + 1).iterator().next();

    final List<AggregatorFactory> aggs = Lists.transform(
        query.getAggregatorSpecs(),
        new Function<AggregatorFactory, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(AggregatorFactory input)
          {
            return input.getCombiningFactory();
          }
        }
    );
    final List<String> dimensions = Lists.transform(
        query.getDimensions(),
        new Function<DimensionSpec, String>()
        {
          @Override
          public String apply(DimensionSpec input)
          {
            return input.getOutputName();
          }
        }
    );
    final IncrementalIndex index;

    final int configuredMaxResults = Math.min(
        query.getContextValue(CTX_KEY_MAX_RESULTS, config.getMaxResults()),
        config.getMaxResults()
    );
    final int limitDuringMerge = query.getContextValue(CTX_KEY_LIMIT_DURING_MERGE, 0);
    final boolean disableLimitDuringMerge = query.getContextBoolean(CTX_KEY_DISABLE_LIMIT_DURING_MERGE, false);
    final int actualMaxResults;
    final boolean sortResults;
    final IncrementalIndex.OverflowAction overflowAction;

    // use granularity truncated min timestamp
    // since incoming truncated timestamps may precede timeStart
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(granTimeStart)
        .withQueryGranularity(gran)
        .withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()]))
        .build();


    if (disableLimitDuringMerge || limitDuringMerge == 0 || Math.abs(limitDuringMerge) > configuredMaxResults) {
      sortResults = query.getContextBoolean(CTX_KEY_SORT_RESULTS, true);
      actualMaxResults = configuredMaxResults;
      overflowAction = IncrementalIndex.OverflowAction.FAIL;
    } else {
      sortResults = true;
      actualMaxResults = Math.abs(limitDuringMerge);
      overflowAction = limitDuringMerge > 0
                       ? IncrementalIndex.OverflowAction.DROP_HIGH
                       : IncrementalIndex.OverflowAction.DROP_LOW;
    }

    if (query.getContextBoolean("useOffheap", false)) {
      index = new OffheapIncrementalIndex(
          schema,
          false,
          true,
          sortResults,
          overflowAction,
          actualMaxResults,
          bufferPool
      );
    } else {
      index = new OnheapIncrementalIndex(
          schema,
          false,
          true,
          sortResults,
          overflowAction,
          actualMaxResults
      );
    }

    Accumulator<IncrementalIndex, T> accumulator = new Accumulator<IncrementalIndex, T>()
    {
      @Override
      public IncrementalIndex accumulate(IncrementalIndex accumulated, T in)
      {

        if (in instanceof MapBasedRow) {
          try {
            MapBasedRow row = (MapBasedRow) in;
            accumulated.add(
                new MapBasedInputRow(
                    row.getTimestamp(),
                    dimensions,
                    row.getEvent()
                )
            );
          }
          catch (IndexSizeExceededException e) {
            throw new ISE(e.getMessage());
          }
        } else {
          throw new ISE("Unable to accumulate something of type [%s]", in.getClass());
        }

        return accumulated;
      }
    };
    return new Pair<>(index, accumulator);
  }

  public static <T> Pair<Queue, Accumulator<Queue, T>> createBySegmentAccumulatorPair()
  {
    // In parallel query runner multiple threads add to this queue concurrently
    Queue init = new ConcurrentLinkedQueue<>();
    Accumulator<Queue, T> accumulator = new Accumulator<Queue, T>()
    {
      @Override
      public Queue accumulate(Queue accumulated, T in)
      {
        if (in == null) {
          throw new ISE("Cannot have null result");
        }
        accumulated.offer(in);
        return accumulated;
      }
    };
    return new Pair<>(init, accumulator);
  }
}
