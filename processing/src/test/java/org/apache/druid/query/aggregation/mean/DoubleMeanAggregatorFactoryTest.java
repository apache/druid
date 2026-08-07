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

package org.apache.druid.query.aggregation.mean;

import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DoubleMeanAggregatorFactoryTest
{
  @Test
  public void testMaxIntermediateSize()
  {
    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    Assertions.assertEquals(Double.BYTES + Long.BYTES, factory.getMaxIntermediateSize());
    Assertions.assertEquals(Double.BYTES + Long.BYTES, factory.getMaxIntermediateSizeWithNulls());
  }

  @Test
  public void testDeserialyze()
  {
    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    DoubleMeanHolder expectedHolder = new DoubleMeanHolder(50.0, 10L);

    DoubleMeanHolder actualHolder = (DoubleMeanHolder) factory.deserialize(expectedHolder);
    Assertions.assertEquals(expectedHolder, actualHolder);

    actualHolder = (DoubleMeanHolder) factory.deserialize(expectedHolder.toBytes());
    Assertions.assertEquals(expectedHolder, actualHolder);
  }

  @Test
  public void testFinalizeComputation()
  {
    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    double sum = 50.0;
    long count = 10L;
    double expecterMean = sum / count;
    DoubleMeanHolder holder = new DoubleMeanHolder(sum, count);

    double actualMean = (Double) factory.finalizeComputation(holder);
    Assertions.assertEquals(expecterMean, actualMean, 1e-6);

    actualMean = (Double) factory.finalizeComputation(holder.toBytes());
    Assertions.assertEquals(expecterMean, actualMean, 1e-6);

    Assertions.assertNull(factory.finalizeComputation(null));
  }

  @Test
  public void testFactorizeWithSize()
  {
    ColumnSelectorFactory colSelectorFactory = EasyMock.mock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(EasyMock.anyString()))
            .andReturn(EasyMock.createMock(ColumnValueSelector.class)).anyTimes();
    EasyMock.replay(colSelectorFactory);

    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    AggregatorAndSize aggregatorAndSize = factory.factorizeWithSize(colSelectorFactory);
    Assertions.assertEquals(DoubleMeanHolder.MAX_INTERMEDIATE_SIZE, aggregatorAndSize.getInitialSizeBytes());
    Assertions.assertTrue(aggregatorAndSize.getAggregator() instanceof DoubleMeanAggregator);
  }
}
