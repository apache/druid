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

package org.apache.druid.query.aggregation.cardinality;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.cardinality.vector.DoubleCardinalityVectorProcessor;
import org.apache.druid.query.aggregation.cardinality.vector.FloatCardinalityVectorProcessor;
import org.apache.druid.query.aggregation.cardinality.vector.LongCardinalityVectorProcessor;
import org.apache.druid.query.aggregation.cardinality.vector.MultiValueStringCardinalityVectorProcessor;
import org.apache.druid.query.aggregation.cardinality.vector.SingleValueStringCardinalityVectorProcessor;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.BaseFloatVectorValueSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;

public class CardinalityVectorAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void testAggregateLong()
  {
    final long[] values = {1, 2, 2, 3, 3, 3, 0};
    final boolean[] nulls = NullHandling.replaceWithDefault()
                            ? null
                            : new boolean[]{false, false, false, false, false, false, true};

    final CardinalityVectorAggregator aggregator = new CardinalityVectorAggregator(
        Collections.singletonList(
            new LongCardinalityVectorProcessor(
                new BaseLongVectorValueSelector(new NoFilterVectorOffset(values.length, 0, values.length))
                {
                  @Override
                  public long[] getLongVector()
                  {
                    return values;
                  }

                  @Nullable
                  @Override
                  public boolean[] getNullVector()
                  {
                    return nulls;
                  }
                }
            )
        )
    );

    testAggregate(aggregator, values.length, NullHandling.replaceWithDefault() ? 4 : 3);
  }

  @Test
  public void testAggregateDouble()
  {
    final double[] values = {1, 2, 2, 3, 3, 3, 0};
    final boolean[] nulls = NullHandling.replaceWithDefault()
                            ? null
                            : new boolean[]{false, false, false, false, false, false, true};

    final CardinalityVectorAggregator aggregator = new CardinalityVectorAggregator(
        Collections.singletonList(
            new DoubleCardinalityVectorProcessor(
                new BaseDoubleVectorValueSelector(new NoFilterVectorOffset(values.length, 0, values.length))
                {
                  @Override
                  public double[] getDoubleVector()
                  {
                    return values;
                  }

                  @Nullable
                  @Override
                  public boolean[] getNullVector()
                  {
                    return nulls;
                  }
                }
            )
        )
    );

    testAggregate(aggregator, values.length, NullHandling.replaceWithDefault() ? 4 : 3);
  }

  @Test
  public void testAggregateFloat()
  {
    final float[] values = {1, 2, 2, 3, 3, 3, 0};
    final boolean[] nulls = NullHandling.replaceWithDefault()
                            ? null
                            : new boolean[]{false, false, false, false, false, false, true};

    final CardinalityVectorAggregator aggregator = new CardinalityVectorAggregator(
        Collections.singletonList(
            new FloatCardinalityVectorProcessor(
                new BaseFloatVectorValueSelector(new NoFilterVectorOffset(values.length, 0, values.length))
                {
                  @Override
                  public float[] getFloatVector()
                  {
                    return values;
                  }

                  @Nullable
                  @Override
                  public boolean[] getNullVector()
                  {
                    return nulls;
                  }
                }
            )
        )
    );

    testAggregate(aggregator, values.length, NullHandling.replaceWithDefault() ? 4 : 3);
  }

  @Test
  public void testAggregateSingleValueString()
  {
    final int[] ids = {1, 2, 2, 3, 3, 3, 0};
    final String[] dict = {null, "abc", "def", "foo"};

    final CardinalityVectorAggregator aggregator = new CardinalityVectorAggregator(
        Collections.singletonList(
            new SingleValueStringCardinalityVectorProcessor(
                new SingleValueDimensionVectorSelector()
                {
                  @Override
                  public int[] getRowVector()
                  {
                    return ids;
                  }

                  @Override
                  public int getValueCardinality()
                  {
                    return dict.length;
                  }

                  @Nullable
                  @Override
                  public String lookupName(int id)
                  {
                    return dict[id];
                  }

                  @Override
                  public boolean nameLookupPossibleInAdvance()
                  {
                    return true;
                  }

                  @Nullable
                  @Override
                  public IdLookup idLookup()
                  {
                    return null;
                  }

                  @Override
                  public int getMaxVectorSize()
                  {
                    return ids.length;
                  }

                  @Override
                  public int getCurrentVectorSize()
                  {
                    return ids.length;
                  }
                }
            )
        )
    );

    testAggregate(aggregator, ids.length, NullHandling.replaceWithDefault() ? 4 : 3);
  }

  @Test
  public void testAggregateMultiValueString()
  {
    final IndexedInts[] ids = {
        new ArrayBasedIndexedInts(new int[]{1, 2}),
        new ArrayBasedIndexedInts(new int[]{2, 3}),
        new ArrayBasedIndexedInts(new int[]{3, 3}),
        new ArrayBasedIndexedInts(new int[]{0})
    };

    final String[] dict = {null, "abc", "def", "foo"};

    final CardinalityVectorAggregator aggregator = new CardinalityVectorAggregator(
        Collections.singletonList(
            new MultiValueStringCardinalityVectorProcessor(
                new MultiValueDimensionVectorSelector()
                {
                  @Override
                  public IndexedInts[] getRowVector()
                  {
                    return ids;
                  }

                  @Override
                  public int getValueCardinality()
                  {
                    return dict.length;
                  }

                  @Nullable
                  @Override
                  public String lookupName(int id)
                  {
                    return dict[id];
                  }

                  @Override
                  public boolean nameLookupPossibleInAdvance()
                  {
                    return true;
                  }

                  @Nullable
                  @Override
                  public IdLookup idLookup()
                  {
                    return null;
                  }

                  @Override
                  public int getMaxVectorSize()
                  {
                    return ids.length;
                  }

                  @Override
                  public int getCurrentVectorSize()
                  {
                    return ids.length;
                  }
                }
            )
        )
    );

    testAggregate(aggregator, ids.length, NullHandling.replaceWithDefault() ? 4 : 3);
  }

  private static void testAggregate(
      final CardinalityVectorAggregator aggregator,
      final int numRows,
      final double expectedResult
  )
  {
    testAggregateStyle1(aggregator, numRows, expectedResult);
    testAggregateStyle2(aggregator, numRows, expectedResult);
  }

  private static void testAggregateStyle1(
      final CardinalityVectorAggregator aggregator,
      final int numRows,
      final double expectedResult
  )
  {
    final int position = 1;
    final ByteBuffer buf = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage() + position);
    aggregator.init(buf, position);
    aggregator.aggregate(buf, position, 0, numRows);

    Assert.assertEquals(
        "style1",
        expectedResult,
        ((HyperLogLogCollector) aggregator.get(buf, position)).estimateCardinality(),
        0.01
    );
  }

  private static void testAggregateStyle2(
      final CardinalityVectorAggregator aggregator,
      final int numRows,
      final double expectedResult
  )
  {
    final int positionOffset = 1;

    final int aggregatorSize = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    final ByteBuffer buf = ByteBuffer.allocate(positionOffset + 2 * aggregatorSize);
    aggregator.init(buf, positionOffset);
    aggregator.init(buf, positionOffset + aggregatorSize);

    final int[] positions = new int[numRows];
    final int[] rows = new int[numRows];

    for (int i = 0; i < numRows; i++) {
      positions[i] = (i % 2) * aggregatorSize;
      rows[i] = (i + 1) % numRows;
    }

    aggregator.aggregate(buf, numRows, positions, rows, positionOffset);

    Assert.assertEquals(
        "style2",
        expectedResult,
        ((HyperLogLogCollector) aggregator.get(buf, positionOffset))
            .fold((HyperLogLogCollector) aggregator.get(buf, positionOffset + aggregatorSize))
            .estimateCardinality(),
        0.01
    );
  }
}
