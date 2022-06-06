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

@RunWith(MockitoJUnitRunner.class)
public class LongLastVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final long[] VALUES = new long[]{7, 15, 2, 150};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private long[] times = {2436, 6879, 7888, 8224};

  @Mock
  private VectorValueSelector selector;
  @Mock
  private VectorValueSelector timeSelector;
  private ByteBuffer buf;

  private LongLastVectorAggregator target;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    Mockito.doReturn(VALUES).when(selector).getLongVector();
    Mockito.doReturn(times).when(timeSelector).getLongVector();
    target = new LongLastVectorAggregator(timeSelector, selector);
    clearBufferForPositions(0, 0);
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
    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(VALUES[3], result.rhs, EPSILON);
  }

  @Test
  public void aggregateWithNulls()
  {
    mockNullsVector();
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, Long> result = (Pair<Long, Long>) target.get(buf, 0);
    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(VALUES[3], result.rhs, EPSILON);
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
      Assert.assertEquals(VALUES[i], result.rhs, EPSILON);
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
      Assert.assertEquals(VALUES[rows[i]], result.rhs, EPSILON);
    }
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      target.init(buf, offset + position);
    }
  }

  private void mockNullsVector()
  {
    if (!NullHandling.replaceWithDefault()) {
      Mockito.doReturn(NULLS).when(selector).getNullVector();
    }
  }
}
