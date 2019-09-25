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

package org.apache.druid.server.coordination;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ChangeRequestHistoryTest
{
  @Test
  public void testSimple() throws Exception
  {
    ChangeRequestHistory<DataSegmentChangeRequest> history = new ChangeRequestHistory();
    Assert.assertEquals(0, history.getLastCounter().getCounter());

    history.addChangeRequest(new SegmentChangeRequestNoop());
    Assert.assertEquals(1, history.getLastCounter().getCounter());

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = history.getRequestsSince(ChangeRequestHistory.Counter.ZERO)
                                                                       .get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(1, snapshot.getCounter().getCounter());

    history.addChangeRequest(new SegmentChangeRequestNoop());
    Assert.assertEquals(2, history.getLastCounter().getCounter());

    snapshot = history.getRequestsSince(snapshot.getCounter()).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    snapshot = history.getRequestsSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());
  }

  @Test
  public void testTruncatedHistory() throws Exception
  {
    ChangeRequestHistory<DataSegmentChangeRequest> history = new ChangeRequestHistory(2);

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter one = history.getLastCounter();

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter two = history.getLastCounter();

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter three = history.getLastCounter();

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter four = history.getLastCounter();

    Assert.assertTrue(history.getRequestsSince(ChangeRequestHistory.Counter.ZERO).get().isResetCounter());
    Assert.assertTrue(history.getRequestsSince(one).get().isResetCounter());
    Assert.assertTrue(history.getRequestsSince(two).get().isResetCounter());

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = history.getRequestsSince(three).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());
  }

  @Test
  public void testCounterHashMismatch() throws Exception
  {
    ChangeRequestHistory<DataSegmentChangeRequest> history = new ChangeRequestHistory(3);

    Assert.assertTrue(history.getRequestsSince(new ChangeRequestHistory.Counter(0, 1234)).get().isResetCounter());

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter one = history.getLastCounter();

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter two = history.getLastCounter();

    Assert.assertTrue(history.getRequestsSince(new ChangeRequestHistory.Counter(0, 1234)).get().isResetCounter());

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = history.getRequestsSince(one).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    Assert.assertTrue(history.getRequestsSince(new ChangeRequestHistory.Counter(1, 1234)).get().isResetCounter());

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter three = history.getLastCounter();

    history.addChangeRequest(new SegmentChangeRequestNoop());
    ChangeRequestHistory.Counter four = history.getLastCounter();

    snapshot = history.getRequestsSince(two).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());

    Assert.assertTrue(history.getRequestsSince(new ChangeRequestHistory.Counter(2, 1234)).get().isResetCounter());
  }

  @Test
  public void testCancel()
  {
    final ChangeRequestHistory<DataSegmentChangeRequest> history = new ChangeRequestHistory();

    ListenableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> future = history.getRequestsSince(
        ChangeRequestHistory.Counter.ZERO
    );
    Assert.assertEquals(1, history.waitingFutures.size());

    final AtomicBoolean callbackExcecuted = new AtomicBoolean(false);
    Futures.addCallback(
        future,
        new FutureCallback<ChangeRequestsSnapshot<DataSegmentChangeRequest>>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot result)
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
    final ChangeRequestHistory history = new ChangeRequestHistory();

    Future<ChangeRequestsSnapshot> future = history.getRequestsSince(
        ChangeRequestHistory.Counter.ZERO
    );

    Assert.assertFalse(future.isDone());

    history.addChangeRequest(new SegmentChangeRequestNoop());

    ChangeRequestsSnapshot snapshot = future.get(1, TimeUnit.MINUTES);
    Assert.assertEquals(1, snapshot.getCounter().getCounter());
    Assert.assertEquals(1, snapshot.getRequests().size());
  }
}
