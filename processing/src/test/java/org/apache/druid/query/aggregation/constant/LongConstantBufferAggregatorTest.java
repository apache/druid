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

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class LongConstantBufferAggregatorTest
{
  private long randomVal;
  private LongConstantBufferAggregator aggregator;
  private ByteBuffer byteBuffer;

  @BeforeEach
  public void setup()
  {
    randomVal = RandomUtils.nextLong();
    aggregator = new LongConstantBufferAggregator(randomVal);
    // mark byteBuffer null to verify no methods ever get called on it.
    byteBuffer = null;
  }

  @Test
  public void testLong()
  {
    Assertions.assertEquals(randomVal, aggregator.getLong(byteBuffer, 0));
  }

  @Test
  public void testAggregate()
  {
    aggregator.aggregate(byteBuffer, 0);
    Assertions.assertEquals(randomVal, aggregator.getLong(byteBuffer, 0));
  }

  @Test
  public void testFloat()
  {
    Assertions.assertEquals(randomVal, aggregator.getFloat(byteBuffer, 0), 0.0001f);
  }

  @Test
  public void testGet()
  {
    Assertions.assertEquals(randomVal, aggregator.get(byteBuffer, 0));
  }
}
