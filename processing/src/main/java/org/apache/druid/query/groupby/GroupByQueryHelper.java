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

package org.apache.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupByQueryHelper
{
  public static final String CTX_KEY_SORT_RESULTS = "sortResults";

  public static <T> Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> createIndexAccumulatorPair(
      final GroupByQuery query,
      final GroupByQueryConfig config,
      NonBlockingPool<ByteBuffer> bufferPool,
      final boolean combine
  )
  {
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final Granularity gran = query.getGranularity();
    final DateTime timeStart = query.getIntervals().get(0).getStart();

    DateTime granTimeStart = timeStart;
    if (!(Granularities.ALL.equals(gran))) {
      granTimeStart = gran.bucketStart(timeStart);
    }

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
        .withMetrics(aggs.toArray(new AggregatorFactory[0]))
        .withQueryGranularity(gran)
        .withMinTimestamp(granTimeStart.getMillis())
        .build();

    if (query.getContextValue("useOffheap", false)) {
      index = new IncrementalIndex.Builder()
          .setIndexSchema(indexSchema)
          .setDeserializeComplexMetrics(false)
          .setConcurrentEventAdd(true)
          .setSortFacts(sortResults)
          .setMaxRowCount(querySpecificConfig.getMaxResults())
          .buildOffheap(bufferPool);
    } else {
      index = new IncrementalIndex.Builder()
          .setIndexSchema(indexSchema)
          .setDeserializeComplexMetrics(false)
          .setConcurrentEventAdd(true)
          .setSortFacts(sortResults)
          .setMaxRowCount(querySpecificConfig.getMaxResults())
          .buildOnheap();
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
      NonBlockingPool<ByteBuffer> bufferPool,
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
      types.put(dimensionSpec.getOutputName(), dimensionSpec.getOutputType());
    }

    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      final String typeName = aggregatorFactory.getTypeName();
      final ValueType valueType;
      if (typeName != null) {
        valueType = GuavaUtils.getEnumIfPresent(ValueType.class, StringUtils.toUpperCase(typeName));
      } else {
        valueType = null;
      }
      if (valueType != null) {
        types.put(aggregatorFactory.getName(), valueType);
      }
    }

    // Don't include post-aggregators since we don't know what types they are.
    return types.build();
  }
}
