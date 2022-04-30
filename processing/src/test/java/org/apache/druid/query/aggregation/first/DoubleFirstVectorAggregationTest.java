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
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(MockitoJUnitRunner.class)
public class DoubleFirstVectorAggregationTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final double[] VALUES = new double[]{7.8d, 11, 23.67, 60};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private long[] times = {2436, 6879, 7888, 8224};

  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String TIME_COL = "__time";

  @Mock
  private VectorValueSelector selector;
  @Mock
  private BaseLongVectorValueSelector timeSelector;
  private ByteBuffer buf;

  private DoubleFirstVectorAggregator target;

  private DoubleFirstAggregatorFactory doubleFirstAggregatorFactory;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private VectorColumnSelectorFactory selectorFactory;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    Mockito.doReturn(VALUES).when(selector).getDoubleVector();
    Mockito.doReturn(times).when(timeSelector).getLongVector();
    target = new DoubleFirstVectorAggregator(timeSelector, selector);
    clearBufferForPositions(0, 0);

    Mockito.doReturn(null).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(selector).when(selectorFactory).makeValueSelector(FIELD_NAME);
    Mockito.doReturn(timeSelector).when(selectorFactory).makeValueSelector(TIME_COL);
    doubleFirstAggregatorFactory = new DoubleFirstAggregatorFactory(NAME, FIELD_NAME, TIME_COL);
  }

  @Test
  public void testFactory()
  {
    Assert.assertTrue(doubleFirstAggregatorFactory.canVectorize(selectorFactory));
    VectorAggregator vectorAggregator = doubleFirstAggregatorFactory.factorizeVector(selectorFactory);
    Assert.assertNotNull(vectorAggregator);
    Assert.assertEquals(DoubleFirstVectorAggregator.class, vectorAggregator.getClass());
  }

  @Test
  public void initValueShouldInitZero()
  {
    target.initValue(buf, 0);
    double initVal = buf.getDouble(0);
    Assert.assertEquals(0, initVal, EPSILON);
  }

  @Test
  public void aggregate()
  {
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, Double> result = (Pair<Long, Double>) target.get(buf, 0);
    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(VALUES[0], result.rhs, EPSILON);
  }

  @Test
  public void aggregateWithNulls()
  {
    mockNullsVector();
    target.aggregate(buf, 0, 0, VALUES.length);
    Pair<Long, Double> result = (Pair<Long, Double>) target.get(buf, 0);
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
      Pair<Long, Double> result = (Pair<Long, Double>) target.get(buf, positions[i] + positionOffset);
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
      Pair<Long, Double> result = (Pair<Long, Double>) target.get(buf, positions[i] + positionOffset);
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
