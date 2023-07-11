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
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
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
public class StringLastVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final double EPSILON = 1e-5;
  private static final String[] VALUES = new String[]{"a", "b", null, "c"};
  private static final boolean[] NULLS = new boolean[]{false, false, true, false};
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String TIME_COL = "__time";
  private long[] times = {2436, 6879, 7888, 8224};
  private long[] timesSame = {2436, 2436};
  private SerializablePairLongString[] pairs = {
      new SerializablePairLongString(2345100L, "last"),
      new SerializablePairLongString(2345001L, "notLast")
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
  private StringLastVectorAggregator target;
  private StringLastVectorAggregator targetWithPairs;

  private StringLastAggregatorFactory stringLastAggregatorFactory;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private VectorColumnSelectorFactory selectorFactory;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    Mockito.doReturn(VALUES).when(selector).getObjectVector();
    Mockito.doReturn(times).when(timeSelector).getLongVector();
    Mockito.doReturn(timesSame).when(timeSelectorForPairs).getLongVector();
    Mockito.doReturn(pairs).when(selectorForPairs).getObjectVector();
    target = new StringLastVectorAggregator(timeSelector, selector, 10);
    targetWithPairs = new StringLastVectorAggregator(timeSelectorForPairs, selectorForPairs, 10);
    clearBufferForPositions(0, 0);


    Mockito.doReturn(selector).when(selectorFactory).makeObjectSelector(FIELD_NAME);
    Mockito.doReturn(timeSelector).when(selectorFactory).makeValueSelector(TIME_COL);
    stringLastAggregatorFactory = new StringLastAggregatorFactory(NAME, FIELD_NAME, TIME_COL, 10);

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

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      target.init(buf, offset + position);
    }
  }
}
