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

  private ByteBuffer buf;
  @Mock
  private VectorValueSelector selector;

  private NumericAnyVectorAggregator target;

  @Before
  public void setUp()
  {
    target = new NumericAnyVectorAggregator(selector)
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
        /* Do nothing. */
      }

      @Override
      void putNonNullValue(ByteBuffer buf, int position, Object value)
      {

      }
    };
    byte[] randomBuffer = new byte[BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(randomBuffer);
    buf = ByteBuffer.wrap(randomBuffer);
  }

  @Test
  public void initShouldSetDoubleAfterPositionToZero()
  {
    int position = 2;
    target.init(buf, position);
    Assert.assertEquals(0, buf.get(position) & BYTE_FLAG_FOUND_MASK);
    Assert.assertEquals(
        NullHandling.sqlCompatible() ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE,
        buf.get(position)
    );
  }

}
