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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.segment.vector.VectorObjectSelector;
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
public class VarianceObjectVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int START_ROW = 1;
  private static final int POSITION = 2;
  private static final int UNINIT_POSITION = 512;
  private static final double EPSILON = 1e-10;
  private static final VarianceAggregatorCollector[] VALUES = new VarianceAggregatorCollector[]{
      new VarianceAggregatorCollector(1, 7.8, 0),
      new VarianceAggregatorCollector(1, 11, 0),
      new VarianceAggregatorCollector(1, 23.67, 0),
      null,
      new VarianceAggregatorCollector(2, 183, 1984.5)
  };
  /**
   *   this variable is same as VALUES but with a different type.
   *   This is used in *compatability tests to verify that VarianceObjectVectorAggregator can handle object arrays as well
   */
  private static final Object[] OBJECTS = new Object[]{
      new VarianceAggregatorCollector(1, 7.8, 0),
      new VarianceAggregatorCollector(1, 11, 0),
      new VarianceAggregatorCollector(1, 23.67, 0),
      null,
      new VarianceAggregatorCollector(2, 183, 1984.5)
  };
  private static final boolean[] NULLS = new boolean[]{false, false, true, true, false};

  @Mock
  private VectorObjectSelector selector;
  private ByteBuffer buf;

  private VarianceObjectVectorAggregator target;

  @Before
  public void setup()
  {
    byte[] randomBytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    buf = ByteBuffer.wrap(randomBytes);
    Mockito.doReturn(VALUES).when(selector).getObjectVector();
    target = new VarianceObjectVectorAggregator(selector);
    clearBufferForPositions(0, POSITION);
  }

  @Test
  public void initValueShouldInitZero()
  {
    target.init(buf, UNINIT_POSITION);
    VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(buf, UNINIT_POSITION);
    Assert.assertEquals(0, collector.count);
    Assert.assertEquals(0, collector.sum, EPSILON);
    Assert.assertEquals(0, collector.nvariance, EPSILON);
  }

  @Test
  public void aggregate()
  {
    target.aggregate(buf, POSITION, START_ROW, VALUES.length);
    VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(buf, POSITION);
    Assert.assertEquals(4, collector.count);
    Assert.assertEquals(217.67, collector.sum, EPSILON);
    Assert.assertEquals(7565.211675, collector.nvariance, EPSILON);
  }

  @Test
  public void aggregateCompatibilityTest()
  {
    this.resetSelectorObjectVectorWithObjects();
    target.aggregate(buf, POSITION, START_ROW, VALUES.length);
    VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(buf, POSITION);
    Assert.assertEquals(4, collector.count);
    Assert.assertEquals(217.67, collector.sum, EPSILON);
    Assert.assertEquals(7565.211675, collector.nvariance, EPSILON);
  }

  @Test
  public void aggregateBatchWithoutRows()
  {
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(
          buf,
          positions[i] + positionOffset
      );
      Assert.assertEquals(VALUES[i], collector);
    }
  }

  @Test
  public void aggregateBatchWithoutRowsCompatibilityTest()
  {
    this.resetSelectorObjectVectorWithObjects();
    int[] positions = new int[]{0, 43, 70};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, null, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(
              buf,
              positions[i] + positionOffset
      );
      Assert.assertEquals(VALUES[i], collector);
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
      VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(
          buf,
          positions[i] + positionOffset
      );
      VarianceAggregatorCollector expectedCollector = VALUES[rows[i]];
      Assert.assertEquals(expectedCollector == null ? new VarianceAggregatorCollector() : expectedCollector, collector);
    }
  }

  @Test
  public void aggregateBatchWithRowsCompatibilityTest()
  {
    this.resetSelectorObjectVectorWithObjects();
    int[] positions = new int[]{0, 43, 70};
    int[] rows = new int[]{3, 2, 0};
    int positionOffset = 2;
    clearBufferForPositions(positionOffset, positions);
    target.aggregate(buf, 3, positions, rows, positionOffset);
    for (int i = 0; i < positions.length; i++) {
      VarianceAggregatorCollector collector = VarianceBufferAggregator.getVarianceCollector(
              buf,
              positions[i] + positionOffset
      );
      VarianceAggregatorCollector expectedCollector = VALUES[rows[i]];
      Assert.assertEquals(expectedCollector == null ? new VarianceAggregatorCollector() : expectedCollector, collector);
    }
  }

  @Test
  public void getShouldReturnAllZeros()
  {
    VarianceAggregatorCollector collector = target.get(buf, POSITION);
    Assert.assertEquals(0, collector.count);
    Assert.assertEquals(0, collector.sum, EPSILON);
    Assert.assertEquals(0, collector.nvariance, EPSILON);
  }

  private void clearBufferForPositions(int offset, int... positions)
  {
    for (int position : positions) {
      VarianceBufferAggregator.doInit(buf, offset + position);
    }
  }

  private void resetSelectorObjectVectorWithObjects()
  {
    Mockito.doReturn(OBJECTS).when(selector).getObjectVector();
  }
}
