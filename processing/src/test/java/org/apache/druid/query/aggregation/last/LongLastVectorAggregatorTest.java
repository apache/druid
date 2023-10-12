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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class LongLastVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final long[] VALUES = new long[]{7, 15, 2, 150};
  private static final long[] LONG_VALUES = new long[]{1L, 2L, 3L, 4L};
  private static final float[] FLOAT_VALUES = new float[]{1.0f, 2.0f, 3.0f, 4.0f};
  private static final double[] DOUBLE_VALUES = new double[]{1.0, 2.0, 3.0, 4.0};
  private static final boolean[] NULLS = new boolean[]{false, false, false, false};
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String FIELD_NAME_LONG = "LONG_NAME";
  private static final String TIME_COL = "__time";
  private final long[] times = {2345001L, 2345100L, 2345200L, 2345300L};
  private final SerializablePairLongLong[] pairs = {
      new SerializablePairLongLong(2345001L, 1L),
      new SerializablePairLongLong(2345100L, 2L),
      new SerializablePairLongLong(2345200L, 3L),
      new SerializablePairLongLong(2345300L, 4L)
  };

  private VectorObjectSelector selector;
  private BaseLongVectorValueSelector timeSelector;
  private ByteBuffer buf;
  private LongLastVectorAggregator target;

  private LongLastAggregatorFactory longLastAggregatorFactory;
  private VectorColumnSelectorFactory selectorFactory;
  private VectorValueSelector nonLongValueSelector;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    timeSelector = new BaseLongVectorValueSelector(new NoFilterVectorOffset(times.length, 0, times.length)
    {
    })
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
        return pairs;
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

    nonLongValueSelector = new BaseLongVectorValueSelector(new NoFilterVectorOffset(
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
        return null;
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
          return nonLongValueSelector;
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
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
        } else if (FIELD_NAME_LONG.equals(column)) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
        }
        return null;
      }
    };

    target = new LongLastVectorAggregator(timeSelector, selector);
    clearBufferForPositions(0, 0);

    longLastAggregatorFactory = new LongLastAggregatorFactory(NAME, FIELD_NAME, TIME_COL);
  }

  @Test
  public void testFactory()
  {
    Assert.assertTrue(longLastAggregatorFactory.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = longLastAggregatorFactory.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(LongLastVectorAggregator.class, vectorAggregator.getClass());
  }

  @Test
  public void initValueShouldInitZero()
  {
    target.initValue(buf, 0);
    long initVal = buf.getLong(0);
    Assert.assertEquals(0, initVal);
  }

  @Test
  public void aggregate()
  {
    target.aggregate(buf, 0, 0, pairs.length);
    Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, 0);
    Assert.assertEquals(pairs[3].lhs.longValue(), result.lhs.longValue());
    Assert.assertEquals(pairs[3].rhs, result.rhs, EPSILON);
  }

  @Test
  public void aggregateWithNulls()
  {
    target.aggregate(buf, 0, 0, pairs.length);
    Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, 0);
    Assert.assertEquals(pairs[3].lhs.longValue(), result.lhs.longValue());
    Assert.assertEquals(pairs[3].rhs, result.rhs, EPSILON);
  }

  @Test
  public void aggregateBatchWithoutRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(pairs[i].getLhs().longValue(), result.lhs.longValue());
      if (!NullHandling.replaceWithDefault() && NULLS[i]) {
        Assert.assertNull(result.rhs);
      } else {
        Assert.assertEquals(pairs[i].rhs, result.rhs, EPSILON);
      }
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
      Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, positions[i] + positionOffset);
      Assert.assertEquals(times[rows[i]], result.lhs.longValue());
      if (!NullHandling.replaceWithDefault() && NULLS[rows[i]]) {
        Assert.assertNull(result.rhs);
      } else {
        Assert.assertEquals(pairs[rows[i]].rhs, result.rhs, EPSILON);
      }
    }
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      target.init(buf, offset + position);
    }
  }
}
