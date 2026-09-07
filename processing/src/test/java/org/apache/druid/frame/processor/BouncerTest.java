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

package org.apache.druid.frame.processor;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

/**
 * Simple single-threaded tests for {@link Bouncer}.
 *
 * For multithreaded tests, see {@link RunAllFullyWidgetTest}.
 */
public class BouncerTest
{
  @Test
  public void testBouncerWithoutParent() throws ExecutionException, InterruptedException
  {
    final Bouncer bouncer = new Bouncer(2);

    Assertions.assertEquals(2, bouncer.getMaxCount());
    Assertions.assertEquals(0, bouncer.getCurrentCount());

    // First ticket should be immediately available since count is 0 < maxCount
    final ListenableFuture<Bouncer.Ticket> future1 = bouncer.ticket();
    Assertions.assertTrue(future1.isDone());
    final Bouncer.Ticket ticket1 = future1.get();
    Assertions.assertEquals(1, bouncer.getCurrentCount());

    // Second ticket should be immediately available since count is 1 < maxCount
    final ListenableFuture<Bouncer.Ticket> future2 = bouncer.ticket();
    Assertions.assertTrue(future2.isDone());
    final Bouncer.Ticket ticket2 = future2.get();
    Assertions.assertEquals(2, bouncer.getCurrentCount());

    // Third ticket should not be ready yet because maxCount is reached
    final ListenableFuture<Bouncer.Ticket> future3 = bouncer.ticket();
    Assertions.assertFalse(future3.isDone());
    Assertions.assertEquals(2, bouncer.getCurrentCount());

    // Giving back ticket1 should make future3 ready and transfer the slot
    ticket1.giveBack();
    Assertions.assertEquals(2, bouncer.getCurrentCount());
    Assertions.assertTrue(future3.isDone());
    final Bouncer.Ticket ticket3 = future3.get();

    // Giving back ticket2 should decrease count since no waiters remain
    ticket2.giveBack();
    Assertions.assertEquals(1, bouncer.getCurrentCount());

    // Giving back ticket3 should decrease count to 0 since no waiters remain
    ticket3.giveBack();
    Assertions.assertEquals(0, bouncer.getCurrentCount());
  }

  @Test
  public void testBouncerWithParentMaxCountOne() throws ExecutionException, InterruptedException
  {
    final Bouncer parentBouncer = new Bouncer(1);
    final Bouncer bouncer = new Bouncer(2, parentBouncer);

    Assertions.assertEquals(1, bouncer.getMaxCount());
    Assertions.assertEquals(0, bouncer.getCurrentCount());
    Assertions.assertEquals(0, parentBouncer.getCurrentCount());

    // First ticket should be immediately available, requiring both child and parent slots
    final ListenableFuture<Bouncer.Ticket> future1 = bouncer.ticket();
    Assertions.assertTrue(future1.isDone());
    final Bouncer.Ticket ticket1 = future1.get();
    Assertions.assertEquals(1, bouncer.getCurrentCount());
    Assertions.assertEquals(1, parentBouncer.getCurrentCount());

    // Second ticket should not be ready yet because parent maxCount is reached
    final ListenableFuture<Bouncer.Ticket> future2 = bouncer.ticket();
    Assertions.assertFalse(future2.isDone());
    Assertions.assertEquals(2, bouncer.getCurrentCount());
    Assertions.assertEquals(1, parentBouncer.getCurrentCount());

    // Giving back ticket1 should make future2 ready and reuse the parent slot
    ticket1.giveBack();
    Assertions.assertEquals(1, bouncer.getCurrentCount());
    Assertions.assertEquals(1, parentBouncer.getCurrentCount());
    Assertions.assertTrue(future2.isDone());
    final Bouncer.Ticket ticket2 = future2.get();

    // Giving back ticket2 should return both child and parent slots to 0
    ticket2.giveBack();
    Assertions.assertEquals(0, bouncer.getCurrentCount());
    Assertions.assertEquals(0, parentBouncer.getCurrentCount());
  }

  @Test
  public void testBouncerWithParentMaxCountThree() throws ExecutionException, InterruptedException
  {
    final Bouncer parentBouncer = new Bouncer(3);
    final Bouncer bouncer = new Bouncer(2, parentBouncer);

    Assertions.assertEquals(2, bouncer.getMaxCount());
    Assertions.assertEquals(0, bouncer.getCurrentCount());
    Assertions.assertEquals(0, parentBouncer.getCurrentCount());

    // First ticket should be immediately available, child is limiting factor
    final ListenableFuture<Bouncer.Ticket> future1 = bouncer.ticket();
    Assertions.assertTrue(future1.isDone());
    final Bouncer.Ticket ticket1 = future1.get();
    Assertions.assertEquals(1, bouncer.getCurrentCount());
    Assertions.assertEquals(1, parentBouncer.getCurrentCount());

    // Second ticket should be immediately available, child is limiting factor
    final ListenableFuture<Bouncer.Ticket> future2 = bouncer.ticket();
    Assertions.assertTrue(future2.isDone());
    final Bouncer.Ticket ticket2 = future2.get();
    Assertions.assertEquals(2, bouncer.getCurrentCount());
    Assertions.assertEquals(2, parentBouncer.getCurrentCount());

    // Third ticket should not be ready yet because child maxCount is reached
    final ListenableFuture<Bouncer.Ticket> future3 = bouncer.ticket();
    Assertions.assertFalse(future3.isDone());
    Assertions.assertEquals(2, bouncer.getCurrentCount());
    Assertions.assertEquals(2, parentBouncer.getCurrentCount());

    // Giving back ticket1 should make future3 ready and transfer the slot
    ticket1.giveBack();
    Assertions.assertEquals(2, bouncer.getCurrentCount());
    Assertions.assertEquals(2, parentBouncer.getCurrentCount());
    Assertions.assertTrue(future3.isDone());
    final Bouncer.Ticket ticket3 = future3.get();

    // Giving back ticket2 should decrease counts since no waiters remain
    ticket2.giveBack();
    Assertions.assertEquals(1, bouncer.getCurrentCount());
    Assertions.assertEquals(1, parentBouncer.getCurrentCount());

    // Giving back ticket3 should return both counts to 0 since no waiters remain
    ticket3.giveBack();
    Assertions.assertEquals(0, bouncer.getCurrentCount());
    Assertions.assertEquals(0, parentBouncer.getCurrentCount());
  }
}
