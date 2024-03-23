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

import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.druid.query.aggregation.any.StringAnyVectorAggregator.NOT_FOUND_FLAG_VALUE;
import static org.mockito.ArgumentMatchers.anyInt;

@RunWith(MockitoJUnitRunner.class)
public class StringAnyVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int MAX_STRING_BYTES = 32;
  private static final int BUFFER_SIZE = 1024;
  private static final int POSITION = 2;
  private static final IndexedInts[] MULTI_VALUE_ROWS = new IndexedInts[]{
      new ArrayBasedIndexedInts(new int[]{1, 0}),
      new ArrayBasedIndexedInts(new int[]{1}),
      new ArrayBasedIndexedInts(),
      new ArrayBasedIndexedInts(new int[]{2})
  };
  private static final int[] SINGLE_VALUE_ROWS = new int[]{1, 1, 3, 2};
  private static final String[] DICTIONARY = new String[]{"Zero", "One", "TwoThisStringIsLongerThanThirtyTwoBytes"};

  private ByteBuffer buf;
  @Mock
  private SingleValueDimensionVectorSelector singleValueSelector;
  @Mock
  private MultiValueDimensionVectorSelector multiValueSelector;

  private StringAnyVectorAggregator singleValueTarget;
  private StringAnyVectorAggregator multiValueTarget;
  private StringAnyVectorAggregator customMultiValueTarget;

  @Before
  public void setUp()
  {
    Mockito.doReturn(MULTI_VALUE_ROWS).when(multiValueSelector).getRowVector();
    Mockito.doAnswer(invocation -> DICTIONARY[(int) invocation.getArgument(0)])
           .when(multiValueSelector).lookupName(anyInt());
    Mockito.doReturn(SINGLE_VALUE_ROWS).when(singleValueSelector).getRowVector();
    Mockito.doAnswer(invocation -> {
      int index = invocation.getArgument(0);
      return index >= DICTIONARY.length ? null : DICTIONARY[index];
    }).when(singleValueSelector).lookupName(anyInt());
    initializeRandomBuffer();
    singleValueTarget = new StringAnyVectorAggregator(singleValueSelector, null, MAX_STRING_BYTES, true);
    multiValueTarget = new StringAnyVectorAggregator(null, multiValueSelector, MAX_STRING_BYTES, true);
    // customMultiValueTarget aggregates to only single value in case of MVDs
    customMultiValueTarget = new StringAnyVectorAggregator(null, multiValueSelector, MAX_STRING_BYTES, false);
  }

  @Test(expected = IllegalStateException.class)
  public void initWithBothSingleAndMultiValueSelectorShouldThrowException()
  {
    new StringAnyVectorAggregator(singleValueSelector, multiValueSelector, MAX_STRING_BYTES, true);
  }

  @Test(expected = IllegalStateException.class)
  public void initWithNeitherSingleNorMultiValueSelectorShouldThrowException()
  {
    new StringAnyVectorAggregator(null, null, MAX_STRING_BYTES, true);
  }

  @Test
  public void initSingleValueTargetShouldMarkPositionAsNotFound()
  {
    singleValueTarget.init(buf, POSITION + 1);
    Assert.assertEquals(NOT_FOUND_FLAG_VALUE, buf.getInt(POSITION + 1));
  }

  @Test
  public void initMultiValueTargetShouldMarkPositionAsNotFound()
  {
    multiValueTarget.init(buf, POSITION + 1);
    Assert.assertEquals(NOT_FOUND_FLAG_VALUE, buf.getInt(POSITION + 1));
  }

  @Test
  public void aggregatePositionNotFoundShouldPutFirstValue()
  {
    singleValueTarget.aggregate(buf, POSITION, 0, 2);
    Assert.assertEquals(DICTIONARY[1], singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void aggregateEmptyShouldPutNull()
  {
    singleValueTarget.aggregate(buf, POSITION, 2, 3);
    Assert.assertNull(singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void aggregateMultiValuePositionNotFoundShouldPutFirstValue()
  {
    multiValueTarget.aggregate(buf, POSITION, 0, 2);
    Assert.assertEquals("[One, Zero]", multiValueTarget.get(buf, POSITION));
  }

  @Test
  public void aggregateMultiValueEmptyShouldPutNull()
  {
    multiValueTarget.aggregate(buf, POSITION, 2, 3);
    Assert.assertNull(multiValueTarget.get(buf, POSITION));
  }

  @Test
  public void aggregateValueLongerThanLimitShouldPutTruncatedValue()
  {
    singleValueTarget.aggregate(buf, POSITION, 3, 4);
    Assert.assertEquals(DICTIONARY[2].substring(0, 32), singleValueTarget.get(buf, POSITION));
  }

  @Test
  public void aggregateBatchNoRowsShouldAggregateAllRows()
  {
    int[] positions = new int[] {0, 43, 100};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    singleValueTarget.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      int position = positions[i] + positionOffset;
      Assert.assertEquals(singleValueSelector.lookupName(SINGLE_VALUE_ROWS[i]), singleValueTarget.get(buf, position));
    }
  }

  @Test
  public void aggregateBatchWithRowsShouldAggregateAllRows()
  {
    int[] positions = new int[]{0, 43, 100};
    int positionOffset = 2;
    int[] rows = new int[]{2, 1, 0};
    clearBufferForPositions(positionOffset, positions);
    multiValueTarget.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      int position = positions[i] + positionOffset;
      int row = rows[i];
      IndexedInts rowIndex = MULTI_VALUE_ROWS[row];
      if (rowIndex.size() == 0) {
        Assert.assertNull(multiValueTarget.get(buf, position));
      } else if (rowIndex.size() == 1) {
        Assert.assertEquals(multiValueSelector.lookupName(rowIndex.get(0)), multiValueTarget.get(buf, position));
      } else {
        List<String> res = new ArrayList<>();
        rowIndex.forEach(index -> res.add(multiValueSelector.lookupName(index)));
        Assert.assertEquals(res.toString(), multiValueTarget.get(buf, position));
      }
    }
  }

  @Test
  public void aggregateBatchWithRowsShouldAggregateAllRowsWithAggregateMVDFalse()
  {
    int[] positions = new int[]{0, 43, 100};
    int positionOffset = 2;
    int[] rows = new int[]{2, 1, 0};
    clearBufferForPositions(positionOffset, positions);
    customMultiValueTarget.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      int position = positions[i] + positionOffset;
      int row = rows[i];
      IndexedInts rowIndex = MULTI_VALUE_ROWS[row];
      if (rowIndex.size() == 0) {
        Assert.assertNull(customMultiValueTarget.get(buf, position));
      } else {
        Assert.assertEquals(multiValueSelector.lookupName(rowIndex.get(0)), customMultiValueTarget.get(buf, position));
      }
    }
  }

  private void initializeRandomBuffer()
  {
    byte[] randomBuffer = new byte[BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(randomBuffer);
    buf = ByteBuffer.wrap(randomBuffer);
    clearBufferForPositions(0, POSITION);
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      buf.putInt(position + offset, NOT_FOUND_FLAG_VALUE);
    }
  }
}
