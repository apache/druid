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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SemaphoreResourceGroupSchedulerTest
{
  private final String lane1 = "lane1";
  private final String lane2 = "lane2";
  private final int capacity = 10;
  private final Map<String, Integer> config = ImmutableMap.of(lane1, 5, lane2, 1);
  private final ResourceGroupScheduler resourceGroupScheduler = new SemaphoreResourceGroupScheduler(
      capacity,
      config,
      false
  );

  private final ResourceGroupScheduler dummyResourceGroupScheduler = new SemaphoreResourceGroupScheduler(
      capacity,
      ImmutableMap.of(),
      true
  );


  @Test
  public void testGetGroupAvailableCapacity() throws InterruptedException
  {
    Assert.assertEquals(capacity, resourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(5, resourceGroupScheduler.getGroupAvailableCapacity(lane1));
    Assert.assertEquals(1, resourceGroupScheduler.getGroupAvailableCapacity(lane2));

    resourceGroupScheduler.accquire(lane1, 3);
    Assert.assertEquals(capacity, resourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(2, resourceGroupScheduler.getGroupAvailableCapacity(lane1));
    Assert.assertEquals(1, resourceGroupScheduler.getGroupAvailableCapacity(lane2));

    resourceGroupScheduler.release(lane1, 3);
    Assert.assertEquals(capacity, resourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(5, resourceGroupScheduler.getGroupAvailableCapacity(lane1));
    Assert.assertEquals(1, resourceGroupScheduler.getGroupAvailableCapacity(lane2));

    resourceGroupScheduler.tryAcquire(lane2, 1, 1, TimeUnit.MILLISECONDS);
    Assert.assertEquals(5, resourceGroupScheduler.getGroupAvailableCapacity(lane1));
    Assert.assertEquals(0, resourceGroupScheduler.getGroupAvailableCapacity(lane2));

    Assert.assertFalse(resourceGroupScheduler.tryAcquire(lane2));
    Assert.assertFalse(resourceGroupScheduler.tryAcquire(lane2, 1));


    resourceGroupScheduler.release(lane2, 1);
    Assert.assertEquals(5, resourceGroupScheduler.getGroupAvailableCapacity(lane1));
    Assert.assertEquals(1, resourceGroupScheduler.getGroupAvailableCapacity(lane2));
  }

  @Test
  public void testGetGroupCapacity()
  {
    Assert.assertEquals(capacity, resourceGroupScheduler.getGroupCapacity(null));
    Assert.assertEquals(5, resourceGroupScheduler.getGroupCapacity(lane1));
    Assert.assertEquals(1, resourceGroupScheduler.getGroupCapacity(lane2));

    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupCapacity(null));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupCapacity(""));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupCapacity("lane"));
  }

  @Test
  public void testGetTotalCapacity()
  {
    Assert.assertEquals(capacity, resourceGroupScheduler.getTotalCapacity());
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getTotalCapacity());
  }

  @Test
  public void testGetAllGroup()
  {
    Assert.assertEquals(config.keySet(), resourceGroupScheduler.getAllGroup());
    Assert.assertTrue(dummyResourceGroupScheduler.getAllGroup().isEmpty());
  }

  @Test
  public void testDummyGetGroupAvailableCapacity() throws InterruptedException
  {
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(""));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity("lane"));

    dummyResourceGroupScheduler.tryAcquire("lane", 1, 1, TimeUnit.MILLISECONDS);
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(""));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity("lane"));

    dummyResourceGroupScheduler.release("lane", 1);
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(null));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity(""));
    Assert.assertEquals(capacity, dummyResourceGroupScheduler.getGroupAvailableCapacity("lane"));
  }
}
