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

import org.junit.Assert;
import org.junit.Test;

public class DoubleMeanAggregatorFactoryTest
{
  @Test
  public void testMaxIntermediateSize()
  {
    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    Assert.assertEquals(Double.BYTES + Long.BYTES, factory.getMaxIntermediateSize());
    Assert.assertEquals(Double.BYTES + Long.BYTES, factory.getMaxIntermediateSizeWithNulls());
  }

  @Test
  public void testDeserialyze()
  {
    DoubleMeanAggregatorFactory factory = new DoubleMeanAggregatorFactory("name", "fieldName");
    DoubleMeanHolder expectedHolder = new DoubleMeanHolder(50.0, 10L);

    DoubleMeanHolder actualHolder = (DoubleMeanHolder) factory.deserialize(expectedHolder);
    Assert.assertEquals(expectedHolder, actualHolder);

    actualHolder = (DoubleMeanHolder) factory.deserialize(expectedHolder.toBytes());
    Assert.assertEquals(expectedHolder, actualHolder);
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
    Assert.assertEquals("", expecterMean, actualMean, 1e-6);

    actualMean = (Double) factory.finalizeComputation(holder.toBytes());
    Assert.assertEquals("", expecterMean, actualMean, 1e-6);

    Assert.assertNull(factory.finalizeComputation(null));
  }
}
