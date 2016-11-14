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
import com.google.common.collect.Sets;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupByQueryHelper
{
  public final static String CTX_KEY_SORT_RESULTS = "sortResults";

  public static <T> Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> createIndexAccumulatorPair(
      final GroupByQuery query,
      final GroupByQueryConfig config,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
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

    final boolean sortResults = query.getContextValue(CTX_KEY_SORT_RESULTS, true);

    // All groupBy dimensions are strings, for now, as long as they don't conflict with any non-dimensions.
    // This should get cleaned up if/when https://github.com/druid-io/druid/pull/3686 makes name conflicts impossible.
    final Set<String> otherNames = Sets.newHashSet();
    for (AggregatorFactory agg : aggs) {
      otherNames.add(agg.getName());
    }
    for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
      otherNames.add(postAggregator.getName());
    }
    final List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
    for (DimensionSpec dimension : query.getDimensions()) {
      if (!otherNames.contains(dimension.getOutputName())) {
        dimensionSchemas.add(new StringDimensionSchema(dimension.getOutputName()));
      }
    }

    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(dimensionSchemas, null, null))
        .withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()]))
        .withQueryGranularity(gran)
        .withMinTimestamp(granTimeStart)
        .build();

    if (query.getContextValue("useOffheap", false)) {
      index = new OffheapIncrementalIndex(
          indexSchema,
          false,
          true,
          sortResults,
          querySpecificConfig.getMaxResults(),
          bufferPool
      );
    } else {
      index = new OnheapIncrementalIndex(
          indexSchema,
          false,
          true,
          sortResults,
          querySpecificConfig.getMaxResults()
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
            throw new ResourceLimitExceededException(e.getMessage());
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

  // Used by GroupByStrategyV1
  public static IncrementalIndex makeIncrementalIndex(
      GroupByQuery query,
      GroupByQueryConfig config,
      StupidPool<ByteBuffer> bufferPool,
      Sequence<Row> rows
  )
  {
    Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        config,
        bufferPool
    );

    return rows.accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
  }

  // Used by GroupByStrategyV1
  public static Sequence<Row> postAggregate(final GroupByQuery query, IncrementalIndex index)
  {
    return Sequences.map(
        Sequences.simple(index.iterableWithPostAggregations(query.getPostAggregatorSpecs(), query.isDescending())),
        new Function<Row, Row>()
        {
          @Override
          public Row apply(Row input)
          {
            final MapBasedRow row = (MapBasedRow) input;
            return new MapBasedRow(
                query.getGranularity()
                     .toDateTime(row.getTimestampFromEpoch()),
                row.getEvent()
            );
          }
        }
    );
  }
}
