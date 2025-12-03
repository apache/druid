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

package org.apache.druid.collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DummyBlockingPoolTest
{
  @Test
  public void testSingletonInstance()
  {
    final BlockingPool<Object> p1 = DummyBlockingPool.instance();
    final BlockingPool<Integer> p2 = DummyBlockingPool.instance();
    Assertions.assertSame(p1, p2);
  }

  @Test
  public void testMaxSizeAndMetricsAreZero()
  {
    final BlockingPool<Integer> pool = DummyBlockingPool.instance();
    Assertions.assertEquals(0, pool.maxSize());
    Assertions.assertEquals(0, pool.getPendingRequests());
    Assertions.assertEquals(0, pool.getUsedResourcesCount());
  }

  @Test
  public void testTakeBatchThrows()
  {
    final BlockingPool<Integer> pool = DummyBlockingPool.instance();
    Assertions.assertThrows(UnsupportedOperationException.class, () -> pool.takeBatch(1));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> pool.takeBatch(1, 10L));
  }
}
