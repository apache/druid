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

package org.apache.druid.server.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.druid.server.http.AsyncRequest.AsyncResponse;
import org.apache.druid.server.mocks.MockAsyncContext;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncRequestTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.JSON_MAPPER;

  private MockHttpServletResponse response;
  private MockHttpServletRequest request;
  private CapturingAsyncContext asyncContext;

  @BeforeEach
  public void setup()
  {
    response = new MockHttpServletResponse();
    request = new MockHttpServletRequest();
    asyncContext = new CapturingAsyncContext();
    asyncContext.request = request;
    asyncContext.response = response;
    request.asyncContextSupplier = () -> asyncContext;
  }

  @Test
  public void test_completed_writesStatusAndBody_whenResponseHasResult()
  {
    AsyncRequest.completed(request, JSON_MAPPER, AsyncResponse.of(200, Map.of("hello", "world")));

    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(Map.of("hello", "world"), readJson(new TypeReference<Map<String, String>>() {}));
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completed_writesStatusWithoutBody_whenResponseHasNoResult()
  {
    AsyncRequest.completed(request, JSON_MAPPER, AsyncResponse.status(204));

    Assertions.assertEquals(204, response.getStatus());
    Assertions.assertEquals(0, response.baos.size());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completed_sendsErrorWithMessage_whenResponseIsError()
  {
    AsyncRequest.completed(request, JSON_MAPPER, AsyncResponse.error(400, "bad request"));

    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals("bad request", response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completed_sendsErrorWithoutMessage_whenErrorMessageIsNull()
  {
    AsyncRequest.completed(request, JSON_MAPPER, AsyncResponse.error(500, null));

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertNull(response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_start_invokesOnSuccess_whenFutureSucceeds()
  {
    AsyncRequest.create(request, Futures.immediateFuture("the-result"), 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals("the-result", readJson(new TypeReference<String>() {}));
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_start_invokesOnFailure_whenFutureFails()
  {
    final RuntimeException cause = new RuntimeException("boom");

    AsyncRequest.create(request, Futures.immediateFailedFuture(cause), 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals("boom", response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_create_throwsException_whenTimeoutMillisIsZero()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, Futures.immediateVoidFuture(), 0L)
    );
  }

  @Test
  public void test_create_throwsException_whenTimeoutMillisIsNegative()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, Futures.immediateVoidFuture(), -1L)
    );
  }

  @Test
  public void test_start_throwsException_whenContentTypeIsMissing()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, Futures.immediateVoidFuture(), 10L)
                           .onSuccess(result -> AsyncResponse.status(200))
                           .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                           .start()
    );
  }

  @Test
  public void test_start_throwsException_whenOnSuccessIsMissing()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, Futures.immediateVoidFuture(), 10L)
                           .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                           .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                           .start()
    );
  }

  @Test
  public void test_start_throwsException_whenOnFailureIsMissing()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, Futures.immediateVoidFuture(), 10L)
                           .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                           .onSuccess(result -> AsyncResponse.status(200))
                           .start()
    );
  }

  @Test
  public void test_start_completesWithTimeoutStatusAndCancelsFuture_whenFutureTimesOut() throws Exception
  {
    final SettableFuture<String> future = SettableFuture.create();

    AsyncRequest.create(request, future, 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onTimeout(204)
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertEquals(10L, asyncContext.getTimeout());
    Assertions.assertNotNull(asyncContext.capturedListener);

    asyncContext.capturedListener.onTimeout(new AsyncEvent(asyncContext));

    Assertions.assertEquals(204, response.getStatus());
    Assertions.assertTrue(asyncContext.isCompleted());
    Assertions.assertTrue(future.isCancelled());
  }

  @Test
  public void test_start_doesNotOverwriteResponse_whenFutureCompletesAfterTimeout() throws Exception
  {
    final SettableFuture<String> future = SettableFuture.create();

    AsyncRequest.create(request, future, 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onTimeout(204)
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    asyncContext.capturedListener.onTimeout(new AsyncEvent(asyncContext));
    Assertions.assertEquals(204, response.getStatus());

    // The future is cancelled by the timeout, but completing it again (as a real future might race to do)
    // must not be allowed to overwrite the response that was already sent.
    future.set("too-late");

    Assertions.assertEquals(204, response.getStatus());
    Assertions.assertEquals(0, response.baos.size());
  }

  @Test
  public void test_start_completesRequestOnTimeout_whenFutureRespondedButCouldNotComplete() throws Exception
  {
    final SettableFuture<String> future = SettableFuture.create();

    AsyncRequest.create(request, future, 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onTimeout(204)
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    // The future completes just as the request starts timing out. The response is written, but the container
    // rejects complete() since it is not called from the thread running the timeout listener.
    asyncContext.rejectComplete = true;
    future.set("the-result");

    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertFalse(asyncContext.isCompleted());

    asyncContext.rejectComplete = false;
    asyncContext.capturedListener.onTimeout(new AsyncEvent(asyncContext));

    // The timeout listener must complete the request, without overwriting the response that was already written.
    Assertions.assertTrue(asyncContext.isCompleted());
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals("the-result", readJson(new TypeReference<String>() {}));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void test_start_timeoutWaitsForResponseBeingWrittenByAnotherThread() throws Exception
  {
    final SettableFuture<String> future = SettableFuture.create();
    final CountDownLatch writeStarted = new CountDownLatch(1);
    final CountDownLatch finishWrite = new CountDownLatch(1);

    AsyncRequest.create(request, future, 10L)
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onTimeout(204)
                .onSuccess(result -> {
                  writeStarted.countDown();
                  Uninterruptibles.awaitUninterruptibly(finishWrite);
                  return AsyncResponse.of(200, result);
                })
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    final Thread writer = new Thread(() -> future.set("the-result"));

    // Times out the request while the response is still being written by the writer thread
    final Thread timeout = new Thread(() -> {
      try {
        asyncContext.capturedListener.onTimeout(new AsyncEvent(asyncContext));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    try {
      writer.start();
      Assertions.assertTrue(writeStarted.await(10, TimeUnit.SECONDS));
      timeout.start();

      // The timeout listener must block until the write has finished, rather than complete the request mid-write.
      // Wait for it to either block or finish, to avoid relying on a sleep.
      final long startNanos = System.nanoTime();
      while (timeout.isAlive()
             && timeout.getState() != Thread.State.BLOCKED
             && System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(10)) {
        Thread.sleep(1);
      }
      Assertions.assertEquals(Thread.State.BLOCKED, timeout.getState());
      Assertions.assertFalse(asyncContext.isCompleted());
    }
    finally {
      finishWrite.countDown();
    }

    writer.join();
    timeout.join();

    Assertions.assertTrue(asyncContext.isCompleted());
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals("the-result", readJson(new TypeReference<String>() {}));
  }

  @Test
  public void test_onSuccess_preservesPolymorphicTypeInfo_whenResultTypeIsGenericAndErased()
  {
    final ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = ChangeRequestsSnapshot.success(
        new ChangeRequestHistory.Counter(1, 1),
        List.of(new SegmentChangeRequestNoop())
    );

    AsyncRequest.create(
                    request,
                    Futures.<ChangeRequestsSnapshot<DataSegmentChangeRequest>>immediateFuture(snapshot),
                    10L
                )
                .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                .onSuccess(
                    result -> AsyncResponse.of(200, result, new TypeReference<>() {})
                )
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertEquals(200, response.getStatus());

    final ChangeRequestsSnapshot<DataSegmentChangeRequest> readBack = readJson(
        new TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>() {}
    );
    Assertions.assertEquals(1, readBack.getRequests().size());
    Assertions.assertTrue(readBack.getRequests().get(0) instanceof SegmentChangeRequestNoop);
  }

  private <T> T readJson(TypeReference<T> type)
  {
    try {
      return JSON_MAPPER.readValue(response.baos.toByteArray(), type);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A {@link MockAsyncContext} that captures the single listener added to it, so tests can manually fire a
   * timeout event. If {@link #rejectComplete} is set, {@link #complete()} throws, like a servlet container does
   * when a request that is timing out is completed from a thread other than the one running the timeout listener.
   */
  private static class CapturingAsyncContext extends MockAsyncContext
  {
    private AsyncListener capturedListener;
    private boolean rejectComplete;

    @Override
    public void addListener(AsyncListener listener)
    {
      capturedListener = listener;
    }

    @Override
    public void complete()
    {
      if (rejectComplete) {
        throw new IllegalStateException("Request is timing out");
      }
      super.complete();
    }
  }
}
