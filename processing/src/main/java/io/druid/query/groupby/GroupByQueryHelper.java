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

import com.google.common.base.Enums;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupByQueryHelper
{
  public final static String CTX_KEY_SORT_RESULTS = "sortResults";

  public static <T> Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> createIndexAccumulatorPair(
      final GroupByQuery query,
      final GroupByQueryConfig config,
      StupidPool<ByteBuffer> bufferPool,
      final boolean combine
  )
  {
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final QueryGranularity gran = query.getGranularity();
    final long timeStart = query.getIntervals().get(0).getStartMillis();

    // use gran.iterable instead of gran.truncate so that
    // AllGranularity returns timeStart instead of Long.MIN_VALUE
    final long granTimeStart = gran.iterable(timeStart, timeStart + 1).iterator().next();

    final List<AggregatorFactory> aggs;
    if (combine) {
      aggs = Lists.transform(
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
    } else {
      aggs = query.getAggregatorSpecs();
    }

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

    // All groupBy dimensions are strings, for now.
    final List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
    for (DimensionSpec dimension : query.getDimensions()) {
      dimensionSchemas.add(new StringDimensionSchema(dimension.getOutputName()));
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
      Sequence<Row> rows,
      boolean combine
  )
  {
    Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        config,
        bufferPool,
        combine
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

  /**
   * Returns types for fields that will appear in the Rows output from "query". Useful for feeding them into
   * {@link RowBasedColumnSelectorFactory}.
   *
   * @param query groupBy query
   *
   * @return row types
   */
  public static Map<String, ValueType> rowSignatureFor(final GroupByQuery query)
  {
    final ImmutableMap.Builder<String, ValueType> types = ImmutableMap.builder();

    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      types.put(dimensionSpec.getOutputName(), ValueType.STRING);
    }

    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      final String typeName = aggregatorFactory.getTypeName();
      final ValueType valueType = typeName != null
                                  ? Enums.getIfPresent(ValueType.class, typeName.toUpperCase()).orNull()
                                  : null;
      if (valueType != null) {
        types.put(aggregatorFactory.getName(), valueType);
      }
    }

    // Don't include post-aggregators since we don't know what types they are.
    return types.build();
  }
}
