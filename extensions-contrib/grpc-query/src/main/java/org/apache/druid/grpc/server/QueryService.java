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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.server.security.ForbiddenException;

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
    try {
      QueryResponse reply = driver.submitQuery(request, QueryServer.AUTH_KEY.get());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
    catch (ForbiddenException e) {
      // This block mimics the Servlet pattern of throwing ForbiddenException for
      // all access denied cases rather than handling permissions in each message
      // handler.
      responseObserver.onError(new StatusRuntimeException(Status.PERMISSION_DENIED));
    }
  }
}
