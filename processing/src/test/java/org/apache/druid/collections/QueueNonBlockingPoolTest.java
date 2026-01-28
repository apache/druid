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

import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueueNonBlockingPoolTest
{
  @Test
  public void testTakeAllTwice()
  {
    final BlockingQueue<String> queue = new ArrayBlockingQueue<>(2);
    queue.add("foo");
    queue.add("bar");

    final QueueNonBlockingPool<String> pool = new QueueNonBlockingPool<>(queue);

    // Take everything from pool
    final ResourceHolder<String> obj1 = pool.take();
    Assert.assertEquals("foo", obj1.get());
    Assert.assertEquals(1, queue.size());

    final ResourceHolder<String> obj2 = pool.take();
    Assert.assertEquals("bar", obj2.get());
    Assert.assertEquals(0, queue.size());

    Assert.assertThrows(
        NoSuchElementException.class,
        pool::take
    );

    // Re-fill pool in reverse order
    obj2.close();
    Assert.assertEquals(1, queue.size());

    obj1.close();
    Assert.assertEquals(2, queue.size());

    // Re-take everything from pool

    final ResourceHolder<String> obj1b = pool.take();
    Assert.assertEquals("bar", obj1b.get());
    Assert.assertEquals(1, queue.size());

    final ResourceHolder<String> obj2b = pool.take();
    Assert.assertEquals("foo", obj2b.get());
    Assert.assertEquals(0, queue.size());

    Assert.assertThrows(
        NoSuchElementException.class,
        pool::take
    );
  }
}
