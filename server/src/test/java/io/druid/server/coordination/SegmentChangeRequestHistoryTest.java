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

package io.druid.server.coordination;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class SegmentChangeRequestHistoryTest
{
  @Test
  public void testSimple() throws Exception
  {
    SegmentChangeRequestHistory history = new SegmentChangeRequestHistory();
    Assert.assertEquals(0, history.getLastCounter().getCounter());

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    Assert.assertEquals(1, history.getLastCounter().getCounter());

    SegmentChangeRequestsSnapshot snapshot = history.getRequestsSince(SegmentChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(1, snapshot.getCounter().getCounter());

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    Assert.assertEquals(2, history.getLastCounter().getCounter());

    snapshot = history.getRequestsSince(snapshot.getCounter()).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    snapshot = history.getRequestsSince(SegmentChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());
  }

  @Test
  public void testTruncatedHistory() throws Exception
  {
    SegmentChangeRequestHistory history = new SegmentChangeRequestHistory(2);

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter one = history.getLastCounter();

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter two = history.getLastCounter();

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter three = history.getLastCounter();

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter four = history.getLastCounter();

    Assert.assertTrue(history.getRequestsSince(SegmentChangeRequestHistory.Counter.ZERO).get().isResetCounter());
    Assert.assertTrue(history.getRequestsSince(one).get().isResetCounter());
    Assert.assertTrue(history.getRequestsSince(two).get().isResetCounter());

    SegmentChangeRequestsSnapshot snapshot = history.getRequestsSince(three).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());
  }

  @Test
  public void testCounterHashMismatch() throws Exception
  {
    SegmentChangeRequestHistory history = new SegmentChangeRequestHistory(3);

    try {
      history.getRequestsSince(new SegmentChangeRequestHistory.Counter(0, 1234)).get();
      Assert.fail();
    }
    catch (ExecutionException ex) {
      Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter one = history.getLastCounter();

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter two = history.getLastCounter();

    try {
      history.getRequestsSince(new SegmentChangeRequestHistory.Counter(0, 1234)).get();
      Assert.fail();
    }
    catch (ExecutionException ex) {
      Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    SegmentChangeRequestsSnapshot snapshot = history.getRequestsSince(one).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    try {
      history.getRequestsSince(new SegmentChangeRequestHistory.Counter(1, 1234)).get();
      Assert.fail();
    }
    catch (ExecutionException ex) {
      Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter three = history.getLastCounter();

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());
    SegmentChangeRequestHistory.Counter four = history.getLastCounter();

    snapshot = history.getRequestsSince(two).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());

    try {
      history.getRequestsSince(new SegmentChangeRequestHistory.Counter(2, 1234)).get();
      Assert.fail();
    }
    catch (ExecutionException ex) {
      Assert.assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testCancel() throws Exception
  {
    final SegmentChangeRequestHistory history = new SegmentChangeRequestHistory();

    ListenableFuture<SegmentChangeRequestsSnapshot> future = history.getRequestsSince(
        SegmentChangeRequestHistory.Counter.ZERO
    );
    Assert.assertEquals(1, history.waitingFutures.size());

    final AtomicBoolean callbackExcecuted = new AtomicBoolean(false);
    Futures.addCallback(
        future,
        new FutureCallback<SegmentChangeRequestsSnapshot>()
        {
          @Override
          public void onSuccess(SegmentChangeRequestsSnapshot result)
          {
            callbackExcecuted.set(true);
          }

          @Override
          public void onFailure(Throwable t)
          {
            callbackExcecuted.set(true);
          }
        }
    );

    future.cancel(true);
    Assert.assertEquals(0, history.waitingFutures.size());
    Assert.assertFalse(callbackExcecuted.get());
  }

  @Test
  public void testNonImmediateFuture() throws Exception
  {
    final SegmentChangeRequestHistory history = new SegmentChangeRequestHistory();

    Future<SegmentChangeRequestsSnapshot> future = history.getRequestsSince(
        SegmentChangeRequestHistory.Counter.ZERO
    );

    Assert.assertFalse(future.isDone());

    history.addSegmentChangeRequest(new SegmentChangeRequestNoop());

    SegmentChangeRequestsSnapshot snapshot = future.get(1, TimeUnit.MINUTES);
    Assert.assertEquals(1, snapshot.getCounter().getCounter());
    Assert.assertEquals(1, snapshot.getRequests().size());
  }

  @Test
  public void testCircularBuffer() throws Exception
  {
    SegmentChangeRequestHistory.CircularBuffer<Integer> circularBuffer = new SegmentChangeRequestHistory.CircularBuffer<>(
        3);

    circularBuffer.add(1);
    Assert.assertEquals(1, circularBuffer.size());
    Assert.assertEquals(1, (int) circularBuffer.get(0));

    circularBuffer.add(2);
    Assert.assertEquals(2, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+1, (int) circularBuffer.get(i));
    }

    circularBuffer.add(3);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+1, (int) circularBuffer.get(i));
    }

    circularBuffer.add(4);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+2, (int) circularBuffer.get(i));
    }

    circularBuffer.add(5);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+3, (int) circularBuffer.get(i));
    }

    circularBuffer.add(6);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+4, (int) circularBuffer.get(i));
    }

    circularBuffer.add(7);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+5, (int) circularBuffer.get(i));
    }

    circularBuffer.add(8);
    Assert.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assert.assertEquals(i+6, (int) circularBuffer.get(i));
    }
  }
}
