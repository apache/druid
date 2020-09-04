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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.druid.query.aggregation.any.NumericAnyVectorAggregator.BYTE_FLAG_FOUND_MASK;

@RunWith(MockitoJUnitRunner.class)
public class NumericAnyVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int NULL_POSITION = 10;
  private static final long FOUND_OBJECT = 23;
  private static final int BUFFER_SIZE = 128;
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
      @Nullable
      @Override
      public Object get(ByteBuffer buf, int position)
      {
        if (position == NULL_POSITION) {
          return null;
        } else {
          return FOUND_OBJECT;
        }
      }

      @Override
      void initValue(ByteBuffer buf, int position)
      {
        /* Do nothing. */
      }

      @Override
      void putValue(ByteBuffer buf, int position, int row)
      {
        buf.putLong(position, row);
      }

      @Override
      void putNonNullValue(ByteBuffer buf, int position, Object value)
      {
        buf.putLong(position, FOUND_OBJECT);
      }
    });
    byte[] randomBuffer = new byte[BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(randomBuffer);
    buf = ByteBuffer.wrap(randomBuffer);
    buf.put(POSITION, (byte) 0);
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
  public void aggregateFoundShouldDoNothing()
  {
    long previous = buf.getLong(POSITION + 1);
    buf.put(POSITION, BYTE_FLAG_FOUND_MASK);
    target.aggregate(buf, POSITION, 0, 3);
    Assert.assertEquals(previous, buf.getLong(POSITION + 1));
  }

  @Test
  public void isValueNull()
  {
    buf.put(POSITION, (byte) 4);
    Assert.assertFalse(target.isValueNull(buf, POSITION));
    buf.put(POSITION, (byte) 3);
    Assert.assertTrue(target.isValueNull(buf, POSITION));
  }
}
