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

package org.apache.druid.grpc.server;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.security.AuthenticationResult;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryServiceTest
{
  private QueryDriver mockDriver;
  private StreamObserver<QueryResponse> mockObserver;
  private QueryService service;
  private Context.CancellableContext testContext;

  @BeforeEach
  public void setup()
  {
    mockDriver = EasyMock.createMock(QueryDriver.class);
    mockObserver = EasyMock.createMock(StreamObserver.class);
    service = new QueryService(mockDriver);
    testContext = Context.current().withCancellation();
  }

  @AfterEach
  public void tearDown()
  {
    testContext.cancel(null);
  }

  @Test
  public void testCancelledQueryReturnsGrpcCancelled()
  {
    QueryInterruptedException cancelException = new QueryInterruptedException(
        new CancellationException("Query was cancelled")
    );

    expect(mockDriver.submitQuery(
        anyObject(QueryRequest.class),
        anyObject(AuthenticationResult.class),
        anyObject(AtomicReference.class)
    )).andThrow(cancelException);

    Capture<Throwable> errorCapture = EasyMock.newCapture();
    mockObserver.onError(capture(errorCapture));
    expectLastCall();

    replay(mockDriver, mockObserver);

    testContext.run(() -> service.submitQuery(QueryRequest.getDefaultInstance(), mockObserver));

    verify(mockDriver, mockObserver);

    StatusRuntimeException statusException = (StatusRuntimeException) errorCapture.getValue();
    assertEquals(Status.Code.CANCELLED, statusException.getStatus().getCode());
  }

  @Test
  public void testInterruptedQueryReturnsGrpcInternal()
  {
    QueryInterruptedException interruptException = new QueryInterruptedException(
        QueryException.QUERY_INTERRUPTED_ERROR_CODE,
        "Query was interrupted",
        null,
        null
    );

    expect(mockDriver.submitQuery(
        anyObject(QueryRequest.class),
        anyObject(AuthenticationResult.class),
        anyObject(AtomicReference.class)
    )).andThrow(interruptException);

    Capture<Throwable> errorCapture = EasyMock.newCapture();
    mockObserver.onError(capture(errorCapture));
    expectLastCall();

    replay(mockDriver, mockObserver);

    testContext.run(() -> service.submitQuery(QueryRequest.getDefaultInstance(), mockObserver));

    verify(mockDriver, mockObserver);

    StatusRuntimeException statusException = (StatusRuntimeException) errorCapture.getValue();
    assertEquals(Status.Code.INTERNAL, statusException.getStatus().getCode());
  }

  @Test
  public void testSuccessfulQueryCompletes()
  {
    expect(mockDriver.submitQuery(
        anyObject(QueryRequest.class),
        anyObject(AuthenticationResult.class),
        anyObject(AtomicReference.class)
    )).andReturn(QueryResponse.getDefaultInstance());

    mockObserver.onNext(anyObject(QueryResponse.class));
    expectLastCall();
    mockObserver.onCompleted();
    expectLastCall();

    replay(mockDriver, mockObserver);

    testContext.run(() -> service.submitQuery(QueryRequest.getDefaultInstance(), mockObserver));

    verify(mockDriver, mockObserver);
  }

  @Test
  public void testContextCancellationInvokesCancelCallback() throws Exception
  {
    AtomicBoolean callbackInvoked = new AtomicBoolean(false);
    Capture<AtomicReference<Runnable>> callbackCapture = EasyMock.newCapture();

    expect(mockDriver.submitQuery(
        anyObject(QueryRequest.class),
        anyObject(AuthenticationResult.class),
        capture(callbackCapture)
    )).andAnswer(() -> {
      // Register a callback that tracks invocation
      callbackCapture.getValue().set(() -> callbackInvoked.set(true));
      return QueryResponse.getDefaultInstance();
    });

    mockObserver.onNext(anyObject(QueryResponse.class));
    expectLastCall();
    mockObserver.onCompleted();
    expectLastCall();

    replay(mockDriver, mockObserver);

    testContext.run(() -> service.submitQuery(QueryRequest.getDefaultInstance(), mockObserver));

    // Now cancel the context - listener should fire and invoke our callback
    testContext.cancel(null);

    assertTrue(callbackInvoked.get(), "Cancel callback should have been invoked");
    verify(mockDriver, mockObserver);
  }

  @Test
  public void testMidExecutionCancellationInvokesCallback() throws Exception
  {
    CountDownLatch queryStarted = new CountDownLatch(1);
    CountDownLatch contextCancelled = new CountDownLatch(1);
    AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    Capture<AtomicReference<Runnable>> callbackCapture = EasyMock.newCapture();

    expect(mockDriver.submitQuery(
        anyObject(QueryRequest.class),
        anyObject(AuthenticationResult.class),
        capture(callbackCapture)
    )).andAnswer(() -> {
      callbackCapture.getValue().set(() -> callbackInvoked.set(true));
      queryStarted.countDown();
      contextCancelled.await(1, TimeUnit.SECONDS);
      throw new QueryInterruptedException(new CancellationException("Cancelled"));
    });

    Capture<Throwable> errorCapture = EasyMock.newCapture();
    mockObserver.onError(capture(errorCapture));
    expectLastCall();

    replay(mockDriver, mockObserver);

    Thread queryThread = new Thread(() ->
        testContext.run(() -> service.submitQuery(QueryRequest.getDefaultInstance(), mockObserver))
    );
    queryThread.start();

    assertTrue(queryStarted.await(1, TimeUnit.SECONDS), "Query should start");
    testContext.cancel(null);
    contextCancelled.countDown();

    queryThread.join(1000);

    assertTrue(callbackInvoked.get(), "Cancel callback should have been invoked");
    verify(mockDriver, mockObserver);

    StatusRuntimeException statusException = (StatusRuntimeException) errorCapture.getValue();
    assertEquals(Status.Code.CANCELLED, statusException.getStatus().getCode());
  }
}
