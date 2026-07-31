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

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class TopNQueryEngineTest
{
  private static final String DIMENSION = "dim";
  private static final String METRIC = "metric";
  private static final Interval INTERVAL = Intervals.of("2000/2001");

  @Test
  public void testUsesReducedPooledTopNSizeForPooledVsHeapSelection()
  {
    final TopNAlgorithm algorithm = selectAlgorithm(
        10,
        80,
        metricCapabilities(false),
        ImmutableList.of(new DoubleSumAggregatorFactory("sum", METRIC)),
        new NumericTopNMetricSpec("sum"),
        Granularities.DAY
    );

    Assertions.assertInstanceOf(PooledTopNAlgorithm.class, algorithm);
  }

  @Test
  public void testUsesNullableSizeForUnknownNullabilityInPooledVsHeapSelection()
  {
    final TopNAlgorithm algorithm = selectAlgorithm(
        10,
        80,
        metricCapabilitiesUnknownNullability(),
        ImmutableList.of(new DoubleSumAggregatorFactory("sum", METRIC)),
        new NumericTopNMetricSpec("sum"),
        Granularities.DAY
    );

    Assertions.assertInstanceOf(HeapBasedTopNAlgorithm.class, algorithm);
  }

  @Test
  public void testReducedPooledTopNSizeAvoidsAggregateMetricFirstThreshold()
  {
    final TopNAlgorithm algorithm = selectAlgorithm(
        400001,
        96,
        metricCapabilities(false),
        doubleSumAggregators(12),
        new NumericTopNMetricSpec("sum0"),
        Granularities.ALL
    );

    Assertions.assertEquals(PooledTopNAlgorithm.class, algorithm.getClass());
  }

  @Test
  public void testUnknownNullabilityStillUsesAggregateMetricFirstThreshold()
  {
    final TopNAlgorithm algorithm = selectAlgorithm(
        400001,
        108,
        metricCapabilitiesUnknownNullability(),
        doubleSumAggregators(12),
        new NumericTopNMetricSpec("sum0"),
        Granularities.ALL
    );

    Assertions.assertInstanceOf(AggregateTopNMetricFirstAlgorithm.class, algorithm);
  }

  private static TopNAlgorithm selectAlgorithm(
      final int dimensionCardinality,
      final int bufferSize,
      final ColumnCapabilities metricCapabilities,
      final List<AggregatorFactory> aggregatorFactories,
      final TopNMetricSpec metricSpec,
      final Granularity granularity
  )
  {
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("test")
        .dimension(new DefaultDimensionSpec(DIMENSION, DIMENSION))
        .metric(metricSpec)
        .threshold(10)
        .intervals(new LegacySegmentSpec(INTERVAL))
        .granularity(granularity)
        .aggregators(aggregatorFactories)
        .build();
    final CapturingTopNQueryMetrics queryMetrics = new CapturingTopNQueryMetrics();
    final TopNQueryEngine queryEngine = new TopNQueryEngine(new TestBufferPool(bufferSize));

    queryEngine.getMapFn(
        query,
        new TopNCursorInspector(
            new TestColumnInspector(metricCapabilities),
            null,
            INTERVAL,
            dimensionCardinality
        ),
        queryMetrics
    );

    return queryMetrics.algorithm;
  }

  private static List<AggregatorFactory> doubleSumAggregators(final int count)
  {
    final ImmutableList.Builder<AggregatorFactory> aggregators = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      aggregators.add(new DoubleSumAggregatorFactory("sum" + i, METRIC));
    }
    return aggregators.build();
  }

  private static ColumnCapabilities metricCapabilities(final boolean hasNulls)
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE).setHasNulls(hasNulls);
  }

  private static ColumnCapabilities metricCapabilitiesUnknownNullability()
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
  }

  private static ColumnCapabilities dimensionCapabilities()
  {
    return new ColumnCapabilitiesImpl()
        .setType(ColumnType.STRING)
        .setHasMultipleValues(false)
        .setHasBitmapIndexes(false)
        .setDictionaryEncoded(true)
        .setDictionaryValuesUnique(true);
  }

  private static class TestColumnInspector implements ColumnInspector
  {
    private final ColumnCapabilities metricCapabilities;

    private TestColumnInspector(final ColumnCapabilities metricCapabilities)
    {
      this.metricCapabilities = metricCapabilities;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      if (DIMENSION.equals(column)) {
        return dimensionCapabilities();
      }
      if (METRIC.equals(column)) {
        return metricCapabilities;
      }
      return null;
    }
  }

  private static class TestBufferPool implements NonBlockingPool<ByteBuffer>
  {
    private final int bufferSize;

    private TestBufferPool(final int bufferSize)
    {
      this.bufferSize = bufferSize;
    }

    @Override
    public ResourceHolder<ByteBuffer> take()
    {
      return new ResourceHolder<>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocate(bufferSize);
        }

        @Override
        public void close()
        {
          // Nothing to release.
        }
      };
    }
  }

  private static class CapturingTopNQueryMetrics extends DefaultTopNQueryMetrics
  {
    private TopNAlgorithm algorithm;

    @Override
    public void algorithm(final TopNAlgorithm algorithm)
    {
      this.algorithm = algorithm;
    }
  }
}
