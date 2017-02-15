/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections;

import com.google.common.base.Suppliers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BlockingPoolTest
{
  private static final BlockingPool<Integer> POOL = new BlockingPool<>(Suppliers.ofInstance(1), 10);
  private static final BlockingPool<Integer> EMPTY_POOL = new BlockingPool<>(Suppliers.ofInstance(1), 0);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTakeFromEmptyPool() throws InterruptedException
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Pool was initialized with limit = 0, there are no objects to take.");
    EMPTY_POOL.take(0);
  }

  @Test
  public void testDrainFromEmptyPool()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Pool was initialized with limit = 0, there are no objects to take.");
    EMPTY_POOL.drain(1);
  }

  @Test(timeout = 1000)
  public void testTake() throws InterruptedException
  {
    final ReferenceCountingResourceHolder<Integer> holder = POOL.take(100);
    assertNotNull(holder);
    assertEquals(9, POOL.getPoolSize());
    holder.close();
    assertEquals(10, POOL.getPoolSize());
  }

  @Test(timeout = 1000)
  public void testTakeTimeout() throws InterruptedException
  {
    final ReferenceCountingResourceHolder<List<Integer>> batchHolder = POOL.drain(10);
    final ReferenceCountingResourceHolder<Integer> holder = POOL.take(100);
    assertNull(holder);
    batchHolder.close();
  }

  @Test
  public void testDrain()
  {
    final ReferenceCountingResourceHolder<List<Integer>> holder = POOL.drain(6);
    assertNotNull(holder);
    assertEquals(6, holder.get().size());
    assertEquals(4, POOL.getPoolSize());
    holder.close();
    assertEquals(10, POOL.getPoolSize());
  }

  @Test
  public void testDrainTooManyObjects()
  {
    final ReferenceCountingResourceHolder<List<Integer>> holder = POOL.drain(100);
    assertNotNull(holder);
    assertEquals(10, holder.get().size());
    assertEquals(0, POOL.getPoolSize());
    holder.close();
    assertEquals(10, POOL.getPoolSize());
  }
}
