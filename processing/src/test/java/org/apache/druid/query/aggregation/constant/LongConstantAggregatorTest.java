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

package org.apache.druid.query.aggregation.constant;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongConstantAggregatorTest
{
  private long randomVal;
  private LongConstantAggregator aggregator;

  @Before
  public void setup()
  {
    randomVal = RandomUtils.nextLong();
    aggregator = new LongConstantAggregator(randomVal);
  }

  @Test
  public void testLong()
  {
    Assert.assertEquals(randomVal, aggregator.getLong());
  }

  @Test
  public void testAggregate()
  {
    aggregator.aggregate();
    Assert.assertEquals(randomVal, aggregator.getLong());
  }

  @Test
  public void testFloat()
  {
    Assert.assertEquals((float) randomVal, aggregator.getFloat(), 0.0001f);
  }

  @Test
  public void testGet()
  {
    Assert.assertEquals(randomVal, aggregator.get());
  }
}
