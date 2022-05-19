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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.druid.query.aggregation.any.NumericAnyVectorAggregator.BYTE_FLAG_FOUND_MASK;

@RunWith(MockitoJUnitRunner.class)
public class NumericAnyVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int NULL_POSITION = 10;
  private static final int BUFFER_SIZE = 128;
  private static final long FOUND_OBJECT = 23;
  private static final int POSITION = 2;
  private static final boolean[] NULLS = new boolean[] {false, false, true, false};

  private ByteBuffer buf;
  @Mock
  private VectorValueSelector selector;

  private NumericAnyVectorAggregator target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(NULLS).when(selector).getNullVector();
    target = Mockito.spy(new NumericAnyVectorAggregator(selector)
    {
      @Override
      void initValue(ByteBuffer buf, int position)
      {
        /* Do nothing. */
      }

      @Override
      boolean putAnyValueFromRow(ByteBuffer buf, int position, int startRow, int endRow)
      {
        boolean isRowsWithinIndex = startRow < endRow && startRow < NULLS.length;
        if (isRowsWithinIndex) {
          buf.putLong(position, startRow);
        }
        return isRowsWithinIndex;
      }

      @Override
      Object getNonNullObject(ByteBuffer buf, int position)
      {
        if (position == POSITION + 1) {
          return FOUND_OBJECT;
        }
        return -1;
      }

    });
    byte[] randomBuffer = new byte[BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(randomBuffer);
    buf = ByteBuffer.wrap(randomBuffer);
    clearBufferForPositions(0, POSITION);
    buf.put(NULL_POSITION, (byte) 0x01);
  }

  @Test
  public void initShouldSetDoubleAfterPositionToZero()
  {
    target.init(buf, POSITION);
    Assert.assertEquals(0, buf.get(POSITION) & BYTE_FLAG_FOUND_MASK);
    Assert.assertEquals(
        NullHandling.sqlCompatible() ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE,
        buf.get(POSITION)
    );
  }

  @Test
  public void aggregateNotFoundAndHasNullsShouldPutNull()
  {
    target.aggregate(buf, POSITION, 0, 3);
    if (NullHandling.sqlCompatible()) {
      Assert.assertEquals(BYTE_FLAG_FOUND_MASK | NullHandling.IS_NULL_BYTE, buf.get(POSITION));
    } else {
      Assert.assertEquals(BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE, buf.get(POSITION));
    }
  }

  @Test
  public void aggregateNotFoundAndHasNullsOutsideRangeShouldPutValue()
  {
    target.aggregate(buf, POSITION, 0, 1);
    Assert.assertEquals(BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE, buf.get(POSITION));
  }

  @Test
  public void aggregateNotFoundAndNoNullsShouldPutValue()
  {
    Mockito.doReturn(null).when(selector).getNullVector();
    target.aggregate(buf, POSITION, 0, 3);
    Assert.assertEquals(BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE, buf.get(POSITION));
  }

  @Test
  public void aggregateNotFoundNoNullsAndValuesOutsideRangeShouldNotPutValue()
  {
    Mockito.doReturn(null).when(selector).getNullVector();
    target.aggregate(buf, POSITION, NULLS.length, NULLS.length + 1);
    Assert.assertNotEquals(BYTE_FLAG_FOUND_MASK, (buf.get(POSITION) & BYTE_FLAG_FOUND_MASK));
  }

  @Test
  public void aggregateBatchNoRowsShouldAggregateAllRows()
  {
    Mockito.doReturn(null).when(selector).getNullVector();
    int[] positions = new int[] {0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      int position = positions[i] + positionOffset;
      Assert.assertEquals(
          BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE,
          buf.get(position) & (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE)
      );
      Assert.assertEquals(i, buf.getLong(position + 1));
    }
  }

  @Test
  public void aggregateBatchWithRowsShouldAggregateAllRows()
  {
    Mockito.doReturn(null).when(selector).getNullVector();
    int[] positions = new int[] {0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    int[] rows = new int[] {3, 2, 0};
    target.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      int position = positions[i] + positionOffset;
      Assert.assertEquals(
          BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE,
          buf.get(position) & (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE)
      );
      Assert.assertEquals(rows[i], buf.getLong(position + 1));
    }
  }

  @Test
  public void aggregateFoundShouldDoNothing()
  {
    long previous = buf.getLong(POSITION + 1);
    buf.put(POSITION, BYTE_FLAG_FOUND_MASK);
    target.aggregate(buf, POSITION, 0, 3);
    Assert.assertEquals(previous, buf.getLong(POSITION + 1));
  }

  @Test
  public void getNullShouldReturnNull()
  {
    Assert.assertNull(target.get(buf, NULL_POSITION));
  }

  @Test
  public void getNotNullShouldReturnValue()
  {
    Assert.assertEquals(FOUND_OBJECT, target.get(buf, POSITION));
  }

  @Test
  public void isValueNull()
  {
    buf.put(POSITION, (byte) 4);
    Assert.assertFalse(target.isValueNull(buf, POSITION));
    buf.put(POSITION, (byte) 3);
    Assert.assertTrue(target.isValueNull(buf, POSITION));
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      buf.put(position + offset, (byte) 0);
    }
  }
}
