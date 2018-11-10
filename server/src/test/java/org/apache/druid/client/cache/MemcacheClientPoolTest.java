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

package org.apache.druid.client.cache;

import com.google.common.base.Suppliers;
import net.spy.memcached.MemcachedClientIF;
import org.junit.Assert;
import org.junit.Test;

public class MemcacheClientPoolTest
{

  @Test
  public void testSimpleUsage()
  {
    MemcacheClientPool pool = new MemcacheClientPool(3, Suppliers.ofInstance((MemcachedClientIF) null));
    // First round
    MemcacheClientPool.IdempotentCloseableHolder first = pool.get();
    Assert.assertEquals(1, first.count());
    MemcacheClientPool.IdempotentCloseableHolder second = pool.get();
    Assert.assertEquals(1, second.count());
    MemcacheClientPool.IdempotentCloseableHolder third = pool.get();
    Assert.assertEquals(1, third.count());
    // Second round
    MemcacheClientPool.IdempotentCloseableHolder firstClientSecondRound = pool.get();
    Assert.assertEquals(2, firstClientSecondRound.count());
    MemcacheClientPool.IdempotentCloseableHolder secondClientSecondRound = pool.get();
    Assert.assertEquals(2, secondClientSecondRound.count());
    first.close();
    firstClientSecondRound.close();
    MemcacheClientPool.IdempotentCloseableHolder firstAgain = pool.get();
    Assert.assertEquals(1, firstAgain.count());
    
    firstAgain.close();
    second.close();
    third.close();
    secondClientSecondRound.close();
  }

  @Test
  public void testClientLeakDetected() throws InterruptedException
  {
    long initialLeakedClients = MemcacheClientPool.leakedClients();
    createDanglingClient();
    // Wait until Closer runs
    for (int i = 0; i < 6000 && MemcacheClientPool.leakedClients() == initialLeakedClients; i++) {
      System.gc();
      byte[] garbage = new byte[10_000_000];
      Thread.sleep(10);
    }
    Assert.assertEquals(initialLeakedClients + 1, MemcacheClientPool.leakedClients());
  }

  private void createDanglingClient()
  {
    MemcacheClientPool pool = new MemcacheClientPool(1, Suppliers.ofInstance((MemcachedClientIF) null));
    pool.get();
  }
}
