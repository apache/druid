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

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import java.util.List;
import java.util.Map;

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
  public void test_completeNow_writesStatusAndBody_whenResponseHasResult()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .completeNow(AsyncResponse.of(200, Map.of("hello", "world")));

    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(Map.of("hello", "world"), readJson(new TypeReference<Map<String, String>>() {}));
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completeNow_writesStatusWithoutBody_whenResponseHasNoResult()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .completeNow(AsyncResponse.status(204));

    Assertions.assertEquals(204, response.getStatus());
    Assertions.assertEquals(0, response.baos.size());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completeNow_sendsErrorWithMessage_whenResponseIsError()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .completeNow(AsyncResponse.error(400, "bad request"));

    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals("bad request", response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completeNow_sendsErrorWithoutMessage_whenErrorMessageIsNull()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .completeNow(AsyncResponse.error(500, null));

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertNull(response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_completeNow_throwsException_whenFutureIsAlsoSpecified()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, JSON_MAPPER)
                           .future(Futures.immediateVoidFuture())
                           .completeNow(AsyncResponse.status(200))
    );
  }

  @Test
  public void test_start_invokesOnSuccess_whenFutureSucceeds()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .future(Futures.immediateFuture("the-result"))
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

    AsyncRequest.create(request, JSON_MAPPER)
                .future(Futures.immediateFailedFuture(cause))
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals("boom", response.getErrorMessage());
    Assertions.assertTrue(asyncContext.isCompleted());
  }

  @Test
  public void test_start_throwsException_whenOnSuccessIsMissing()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, JSON_MAPPER)
                           .future(Futures.immediateVoidFuture())
                           .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                           .start()
    );
  }

  @Test
  public void test_start_throwsException_whenOnFailureIsMissing()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AsyncRequest.create(request, JSON_MAPPER)
                           .future(Futures.immediateVoidFuture())
                           .onSuccess(result -> AsyncResponse.status(200))
                           .start()
    );
  }

  @Test
  public void test_start_completesWithTimeoutStatusAndCancelsFuture_whenFutureTimesOut() throws Exception
  {
    final SettableFuture<String> future = SettableFuture.create();

    AsyncRequest.create(request, JSON_MAPPER)
                .future(future)
                .timeoutMillis(10L)
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

    AsyncRequest.create(request, JSON_MAPPER)
                .future(future)
                .timeoutMillis(10L)
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
  public void test_start_doesNotRegisterTimeoutListener_whenTimeoutMillisIsNotSet()
  {
    AsyncRequest.create(request, JSON_MAPPER)
                .future(Futures.immediateFuture("result"))
                .onSuccess(result -> AsyncResponse.of(200, result))
                .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                .start();

    Assertions.assertNull(asyncContext.capturedListener);
  }

  @Test
  public void test_onSuccess_preservesPolymorphicTypeInfo_whenResultTypeIsGenericAndErased()
  {
    final ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = ChangeRequestsSnapshot.success(
        new ChangeRequestHistory.Counter(1, 1),
        List.of(new SegmentChangeRequestNoop())
    );

    AsyncRequest.create(request, JSON_MAPPER)
                .future(Futures.<ChangeRequestsSnapshot<DataSegmentChangeRequest>>immediateFuture(snapshot))
                .onSuccess((ChangeRequestsSnapshot<DataSegmentChangeRequest> result) -> AsyncResponse.of(
                    200,
                    result,
                    JSON_MAPPER,
                    new TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>() {}
                ))
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
   * timeout event.
   */
  private static class CapturingAsyncContext extends MockAsyncContext
  {
    private AsyncListener capturedListener;

    @Override
    public void addListener(AsyncListener listener)
    {
      capturedListener = listener;
    }
  }
}
