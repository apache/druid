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

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.security.ForbiddenException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the gRPC Query service. Provides a single method
 * to run a query using the "driver" that holds the actual Druid SQL
 * logic.
 */
class QueryService extends QueryGrpc.QueryImplBase
{
  private final QueryDriver driver;

  public QueryService(QueryDriver driver)
  {
    this.driver = driver;
  }

  @Override
  public void submitQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver)
  {
    final AtomicReference<Runnable> cancelCallback = new AtomicReference<>(() -> {});

    // getAndSet ensures the callback runs at most once
    final Runnable cancelOnce = () -> cancelCallback.getAndSet(() -> {}).run();

    Context.current().addListener(context -> cancelOnce.run(), MoreExecutors.directExecutor());

    try {
      QueryResponse reply = driver.submitQuery(request, QueryServer.AUTH_KEY.get(), cancelCallback);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
    catch (ForbiddenException e) {
      // This block mimics the Servlet pattern of throwing ForbiddenException for
      // all access denied cases rather than handling permissions in each message
      // handler.
      responseObserver.onError(new StatusRuntimeException(Status.PERMISSION_DENIED));
    }
    catch (QueryInterruptedException e) {
      if (QueryException.QUERY_CANCELED_ERROR_CODE.equals(e.getErrorCode())) {
        responseObserver.onError(new StatusRuntimeException(
            Status.CANCELLED.withDescription(e.getMessage())
        ));
      } else {
        responseObserver.onError(new StatusRuntimeException(
            Status.INTERNAL.withDescription(e.getMessage())
        ));
      }
    }
    finally {
      // Handle race where context was cancelled before cancelCallback was set.
      // The listener would have invoked the no-op; now that the real callback
      // is registered, we check again and invoke if needed.
      if (Context.current().isCancelled()) {
        cancelOnce.run();
      }
    }
  }
}
