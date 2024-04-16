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

package org.apache.druid.query.aggregation.firstlast.first;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;


public class StringFirstVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final String[] VALUES = new String[]{"a", "b", null, "c"};
  private static final long[] LONG_VALUES = new long[]{1L, 2L, 3L, 4L};
  private static final String[] STRING_VALUES = new String[]{"1", "2", "3", "4"};
  private static final float[] FLOAT_VALUES = new float[]{1.0f, 2.0f, 3.0f, 4.0f};
  private static final double[] DOUBLE_VALUES = new double[]{1.0, 2.0, 3.0, 4.0};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String FIELD_NAME_LONG = "LONG_NAME";
  private static final String TIME_COL = "__time";
  private final long[] times = {2436, 6879, 7888, 8224};
  private final long[] timesSame = {2436, 2436};
  private final SerializablePairLongString[] pairs = {
      new SerializablePairLongString(2345001L, "first"),
      new SerializablePairLongString(2345100L, "notFirst")
  };

  @Mock
  private VectorObjectSelector selector;
  @Mock
  private VectorObjectSelector selectorForPairs;
  @Mock
  private BaseLongVectorValueSelector timeSelector;
  @Mock
  private BaseLongVectorValueSelector timeSelectorForPairs;
  private ByteBuffer buf;
  private StringFirstVectorAggregator target;
  private StringFirstVectorAggregator targetWithPairs;

  private StringFirstAggregatorFactory stringFirstAggregatorFactory;
  private StringFirstAggregatorFactory stringFirstAggregatorFactory1;

  private VectorColumnSelectorFactory selectorFactory;
  private VectorValueSelector nonStringValueSelector;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);

    timeSelector = new BaseLongVectorValueSelector(new NoFilterVectorOffset(times.length, 0, times.length))
    {
      @Override
      public long[] getLongVector()
      {
        return times;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        return null;
      }
    };

    selector = new VectorObjectSelector()
    {
      @Override
      public Object[] getObjectVector()
      {
        return VALUES;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 4;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 0;
      }
    };

    timeSelectorForPairs = new BaseLongVectorValueSelector(new NoFilterVectorOffset(
        timesSame.length,
        0,
        timesSame.length
    ))
    {
      @Override
      public long[] getLongVector()
      {
        return timesSame;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        return null;
      }
    };
    selectorForPairs = new VectorObjectSelector()
    {
      @Override
      public Object[] getObjectVector()
      {
        return pairs;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 2;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 0;
      }
    };

    nonStringValueSelector = new BaseLongVectorValueSelector(new NoFilterVectorOffset(
        LONG_VALUES.length,
        0,
        LONG_VALUES.length
    ))
    {
      @Override
      public long[] getLongVector()
      {
        return LONG_VALUES;
      }

      @Override
      public float[] getFloatVector()
      {
        return FLOAT_VALUES;
      }

      @Override
      public double[] getDoubleVector()
      {
        return DOUBLE_VALUES;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        return NULLS;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 4;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 4;
      }
    };

    selectorFactory = new VectorColumnSelectorFactory()
    {
      @Override
      public ReadableVectorInspector getReadableVectorInspector()
      {
        return new NoFilterVectorOffset(VALUES.length, 0, VALUES.length);
      }

      @Override
      public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
      {
        return null;
      }

      @Override
      public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
      {
        return null;
      }

      @Override
      public VectorValueSelector makeValueSelector(String column)
      {
        if (TIME_COL.equals(column)) {
          return timeSelector;
        } else if (FIELD_NAME_LONG.equals(column)) {
          return nonStringValueSelector;
        }
        return null;
      }

      @Override
      public VectorObjectSelector makeObjectSelector(String column)
      {
        if (FIELD_NAME.equals(column)) {
          return selector;
        } else {
          return null;
        }
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (FIELD_NAME.equals(column)) {
          return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
        } else if (FIELD_NAME_LONG.equals(column)) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
        }
        return null;
      }
    };

    target = new StringFirstVectorAggregator(timeSelector, selector, 10);
    targetWithPairs = new StringFirstVectorAggregator(timeSelectorForPairs, selectorForPairs, 10);
    clearBufferForPositions(0, 0);


    stringFirstAggregatorFactory = new StringFirstAggregatorFactory(NAME, FIELD_NAME, TIME_COL, 10);
    stringFirstAggregatorFactory1 = new StringFirstAggregatorFactory(NAME, FIELD_NAME_LONG, TIME_COL, 10);
  }

  @Test
  public void testAggregateWithPairs()
  {
    targetWithPairs.aggregate(buf, 0, 0, pairs.length);
    Pair<Long, String> result = (Pair<Long, String>) targetWithPairs.get(buf, 0);
    //Should come 0 as the last value as the left of the pair is greater
    Assert.assertEquals(pairs[0].lhs.longValue(), result.lhs.longValue());
    Assert.assertEquals(pairs[0].rhs, result.rhs);
  }

  @Test
  public void testFactory()
  {
    Assert.assertTrue(stringFirstAggregatorFactory.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = stringFirstAggregatorFactory.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(StringFirstVectorAggregator.class, vectorAggregator.getClass());
  }

  @Test
  public void testInit()
  {
    target.init(buf, 0);
    long initVal = buf.getLong(0);
    Assert.assertEquals(DateTimes.MAX.getMillis(), initVal);
  }

  @Test
  public void testAggregate()
  {
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, String> result = (Pair<Long, String>) target.get(buf, 0);
    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(VALUES[0], result.rhs);
  }

  @Test
  public void testStringEarliestOnNonStringColumns()
  {
    Assert.assertTrue(stringFirstAggregatorFactory1.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = stringFirstAggregatorFactory1.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(StringFirstVectorAggregator.class, vectorAggregator.getClass());
    vectorAggregator.aggregate(buf, 0, 0, LONG_VALUES.length);
    Pair<Long, String> result = (Pair<Long, String>) vectorAggregator.get(buf, 0);
    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(STRING_VALUES[0], result.rhs);
  }

  @Test
  public void testAggregateBatchWithoutRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[i], result.lhs.longValue());
      Assert.assertEquals(NullHandling.nullToEmptyIfNeeded(VALUES[i]), result.rhs);
    }
  }

  @Test
  public void testAggregateBatchWithRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int[] rows = new int[]{3, 2, 0};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[rows[i]], result.lhs.longValue());
      Assert.assertEquals(NullHandling.nullToEmptyIfNeeded(VALUES[rows[i]]), result.rhs);
    }
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      target.init(buf, offset + position);
    }
  }
}
