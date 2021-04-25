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

import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class DoubleAnyVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int NULL_POSITION = 32;
  private static final int POSITION = 2;
  private static final double EPSILON = 1e-15;
  private static final double[] VALUES = new double[]{7.8d, 11, 23.67, 60, 123};

  private ByteBuffer buf;
  @Mock
  private VectorValueSelector selector;

  private DoubleAnyVectorAggregator target;

  @Before
  public void setUp()
  {
    byte[] randomBytes = new byte[128];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    Mockito.doReturn(VALUES).when(selector).getDoubleVector();

    target = spy(new DoubleAnyVectorAggregator(selector));
    Mockito.when(target.isValueNull(buf, NULL_POSITION)).thenReturn(true);
    Mockito.when(target.isValueNull(buf, POSITION)).thenReturn(false);
  }

  @Test
  public void initValueShouldInitZero()
  {
    target.initValue(buf, POSITION);
    Assert.assertEquals(0, buf.getDouble(POSITION), EPSILON);
  }

  @Test
  public void getAtPositionIsNullShouldReturnNull()
  {
    Assert.assertNull(target.get(buf, NULL_POSITION));
  }

  @Test
  public void getAtPositionShouldReturnValue()
  {
    buf.putDouble(POSITION + 1, VALUES[3]);
    Assert.assertEquals(VALUES[3], (double) target.get(buf, POSITION), EPSILON);
  }

  @Test
  public void putValueShouldAddToBuffer()
  {
    Assert.assertTrue(target.putAnyValueFromRow(buf, POSITION, 2, 3));
    Assert.assertEquals(VALUES[2], buf.getDouble(POSITION), EPSILON);
  }

  @Test
  public void putValueStartAfterEndShouldNotAddToBuffer()
  {
    Assert.assertFalse(target.putAnyValueFromRow(buf, POSITION, 2, 2));
    Assert.assertNotEquals(VALUES[2], buf.getDouble(POSITION), EPSILON);
  }

  @Test
  public void putValueStartOutsideRangeShouldNotAddToBuffer()
  {
    Assert.assertFalse(target.putAnyValueFromRow(buf, POSITION, 5, 6));
    Assert.assertNotEquals(VALUES[2], buf.getDouble(POSITION), EPSILON);
  }
}
