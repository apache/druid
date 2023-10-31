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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
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


public class LongFirstVectorAggregationTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final long[] VALUES = new long[]{7, 15, 2, 150};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String TIME_COL = "__time";
  private long[] times = {2436, 6879, 7888, 8224};
  private VectorValueSelector selector;
  private BaseLongVectorValueSelector timeSelector;
  private ByteBuffer buf;
  private LongFirstVectorAggregator target;

  private LongFirstAggregatorFactory longFirstAggregatorFactory;
  private VectorColumnSelectorFactory selectorFactory;

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
        return NULLS;
      }
    };
    selector = new BaseLongVectorValueSelector(new NoFilterVectorOffset(VALUES.length, 0, VALUES.length)
    {

    })
    {
      @Override
      public long[] getLongVector()
      {
        return VALUES;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        if (!NullHandling.replaceWithDefault()) {
          return NULLS;
        }
        return null;
      }
    };

    target = new LongFirstVectorAggregator(timeSelector, selector);
    clearBufferForPositions(0, 0);

    selectorFactory = new VectorColumnSelectorFactory()
    {
      @Override
      public ReadableVectorInspector getReadableVectorInspector()
      {
        return null;
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
        } else if (FIELD_NAME.equals(column)) {
          return selector;
        } else {
          return null;
        }
      }

      @Override
      public VectorObjectSelector makeObjectSelector(String column)
      {
        return null;
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (FIELD_NAME.equals(column)) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
        }
        return null;
      }
    };
    longFirstAggregatorFactory = new LongFirstAggregatorFactory(NAME, FIELD_NAME, TIME_COL);
  }

  @Test
  public void testFactory()
  {
    Assert.assertTrue(longFirstAggregatorFactory.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = longFirstAggregatorFactory.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(LongFirstVectorAggregator.class, vectorAggregator.getClass());
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
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, 0);
    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(VALUES[0], result.rhs, EPSILON);
  }

  @Test
  public void aggregateWithNulls()
  {
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, 0);
    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(VALUES[0], result.rhs, EPSILON);
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
      Assert.assertEquals(times[i], result.lhs.longValue());
      if (!NullHandling.replaceWithDefault() && NULLS[i]) {
        Assert.assertNull(result.rhs);
      } else {
        Assert.assertEquals(VALUES[i], result.rhs, EPSILON);
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
        Assert.assertEquals(VALUES[rows[i]], result.rhs, EPSILON);
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
