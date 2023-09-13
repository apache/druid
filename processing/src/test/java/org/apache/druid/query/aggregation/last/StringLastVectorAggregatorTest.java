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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.IdLookup;
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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;


public class StringLastVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final String[] VALUES = new String[]{"a", "b", null, "c"};
  private static final int[] DICT_VALUES = new int[]{1, 2, 0, 3};
  private static final long[] LONG_VALUES = new long[]{1L, 2L, 3L, 4L};
  private static final String[] STRING_VALUES = new String[]{"1", "2", "3", "4"};
  private static final float[] FLOAT_VALUES = new float[]{1.0f, 2.0f, 3.0f, 4.0f};
  private static final double[] DOUBLE_VALUES = new double[]{1.0, 2.0, 3.0, 4.0};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private static final boolean[] NULLS1 = new boolean[]{false, false};
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String FIELD_NAME_LONG = "LONG_NAME";
  private static final String TIME_COL = "__time";
  private final long[] times = {2436, 6879, 7888, 8224};
  private final long[] timesSame = {2436, 2436};
  private final SerializablePairLongString[] pairs = {
      new SerializablePairLongString(2345100L, "last"),
      new SerializablePairLongString(2345001L, "notLast")
  };

  private VectorObjectSelector selector;
  private BaseLongVectorValueSelector timeSelector;
  private VectorValueSelector nonStringValueSelector;
  private ByteBuffer buf;
  private StringLastVectorAggregator target;
  private StringLastVectorAggregator targetWithPairs;

  private StringLastAggregatorFactory stringLastAggregatorFactory;
  private StringLastAggregatorFactory stringLastAggregatorFactory1;
  private SingleStringLastDimensionVectorAggregator targetSingleDim;

  private VectorColumnSelectorFactory selectorFactory;


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
        return 0;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 0;
      }
    };
    BaseLongVectorValueSelector timeSelectorForPairs = new BaseLongVectorValueSelector(new NoFilterVectorOffset(
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
        return NULLS1;
      }
    };
    VectorObjectSelector selectorForPairs = new VectorObjectSelector()
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
        return 2;
      }
    };
    selectorFactory = new VectorColumnSelectorFactory()
    {
      @Override
      public ReadableVectorInspector getReadableVectorInspector()
      {
        return new NoFilterVectorOffset(LONG_VALUES.length, 0, LONG_VALUES.length);
      }

      @Override
      public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
      {
        return new SingleValueDimensionVectorSelector()
        {
          @Override
          public int[] getRowVector()
          {
            return DICT_VALUES;
          }

          @Override
          public int getValueCardinality()
          {
            return DICT_VALUES.length;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            switch (id) {
              case 1:
                return "a";
              case 2:
                return "b";
              case 3:
                return "c";
              default:
                return null;
            }
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            return false;
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
            return DICT_VALUES.length;
          }

          @Override
          public int getCurrentVectorSize()
          {
            return DICT_VALUES.length;
          }
        };
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

    target = new StringLastVectorAggregator(timeSelector, selector, 10);
    targetWithPairs = new StringLastVectorAggregator(timeSelectorForPairs, selectorForPairs, 10);
    targetSingleDim = new SingleStringLastDimensionVectorAggregator(timeSelector, selectorFactory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(FIELD_NAME)), 10);
    clearBufferForPositions(0, 0);


    stringLastAggregatorFactory = new StringLastAggregatorFactory(NAME, FIELD_NAME, TIME_COL, 10);
    stringLastAggregatorFactory1 = new StringLastAggregatorFactory(NAME, FIELD_NAME_LONG, TIME_COL, 10);
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
    Assert.assertTrue(stringLastAggregatorFactory.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = stringLastAggregatorFactory.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(StringLastVectorAggregator.class, vectorAggregator.getClass());
  }

  @Test
  public void testStringLastOnNonStringColumns()
  {
    Assert.assertTrue(stringLastAggregatorFactory1.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = stringLastAggregatorFactory1.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(StringLastVectorAggregator.class, vectorAggregator.getClass());
    vectorAggregator.aggregate(buf, 0, 0, LONG_VALUES.length);
    Pair<Long, String> result = (Pair<Long, String>) vectorAggregator.get(buf, 0);
    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(STRING_VALUES[3], result.rhs);
  }

  @Test
  public void initValueShouldBeMinDate()
  {
    target.init(buf, 0);
    long initVal = buf.getLong(0);
    Assert.assertEquals(DateTimes.MIN.getMillis(), initVal);
  }

  @Test
  public void aggregate()
  {
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, String> result = (Pair<Long, String>) target.get(buf, 0);
    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(VALUES[3], result.rhs);
  }

  @Test
  public void aggregateNoOp()
  {
    // Test that aggregates run just fine when the input field does not exist
    StringLastVectorAggregator aggregator = new StringLastVectorAggregator(null, selector, 10);
    aggregator.aggregate(buf, 0, 0, VALUES.length);
  }

  @Test
  public void aggregateBatchNoOp()
  {
    // Test that aggregates run just fine when the input field does not exist
    StringLastVectorAggregator aggregator = new StringLastVectorAggregator(null, selector, 10);
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    aggregator.aggregate(buf, 3, positions, null, positionOffset);
  }

  @Test
  public void aggregateBatchWithoutRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[i], result.lhs.longValue());
      Assert.assertEquals(VALUES[i], result.rhs);
    }
  }

  @Test
  public void aggregateBatchWithRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int[] rows = new int[]{3, 2, 0};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[rows[i]], result.lhs.longValue());
      Assert.assertEquals(VALUES[rows[i]], result.rhs);
    }
  }

  @Test
  public void aggregateSingleDim()
  {
    targetSingleDim.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, String> result = (Pair<Long, String>) targetSingleDim.get(buf, 0);
    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(VALUES[3], result.rhs);
  }

  @Test
  public void aggregateBatchWithoutRowsSingleDim()
  {
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    targetSingleDim.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) targetSingleDim.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[i], result.lhs.longValue());
      Assert.assertEquals(VALUES[i], result.rhs);
    }
  }

  @Test
  public void aggregateBatchWithRowsSingleDim()
  {
    int[] positions = new int[]{0, 43, 70};
    int[] rows = new int[]{3, 2, 0};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    targetSingleDim.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, String> result = (Pair<Long, String>) targetSingleDim.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[rows[i]], result.lhs.longValue());
      Assert.assertEquals(VALUES[rows[i]], result.rhs);
    }
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      target.init(buf, offset + position);
    }
  }
}
