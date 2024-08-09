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
import io.grpc.stub.StreamObserver;
import org.apache.druid.grpc.proto.HealthGrpc;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckRequest;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckResponse;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckResponse.ServingStatus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation of grpc health service. Provides {@code check(HealthCheckRequest, StreamObserver(HealthCheckResponse))}
 * method to get health of a specific service or the overall server health.
 * <p>
 * A client can call the {@code watch(HealthCheckRequest, StreamObserver(HealthCheckResponse))} method
 * to perform a streaming health-check.
 * The server will immediately send back a message indicating the current serving status.
 * It will then subsequently send a new message whenever the service's serving status changes.
 */
class HealthService extends HealthGrpc.HealthImplBase
{
  private final ConcurrentMap<String, ServingStatus> serviceStatusMap;
  private final ConcurrentMap<String, Context.CancellableContext> cancellationContexts;
  private final ConcurrentMap<String, CountDownLatch> statusChangeLatchMap;

  public HealthService()
  {
    this.serviceStatusMap = new ConcurrentHashMap<>();
    this.cancellationContexts = new ConcurrentHashMap<>();
    this.statusChangeLatchMap = new ConcurrentHashMap<>();
  }

  @Override
  public void check(
      HealthCheckRequest request,
      StreamObserver<HealthCheckResponse> responseObserver
  )
  {
    String serviceName = request.getService();
    ServingStatus status = getServiceStatus(serviceName);
    HealthCheckResponse response = buildHealthCheckResponse(status);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void watch(
      HealthCheckRequest request,
      StreamObserver<HealthCheckResponse> responseObserver
  )
  {
    String serviceName = request.getService();

    Context.CancellableContext existingContext = cancellationContexts.get(serviceName);
    if (existingContext != null) {
      // Another request is already watching the same service
      responseObserver.onError(Status.ALREADY_EXISTS.withDescription(
          "Another watch request is already in progress for the same service").asRuntimeException());
      return;
    }

    Context.CancellableContext cancellableContext = Context.current().withCancellation();
    cancellationContexts.put(serviceName, cancellableContext);

    // Attach a cancellation listener to the context
    cancellableContext.addListener((context) -> {
      // If the context is cancelled, remove the observer from the map
      cancellationContexts.remove(serviceName);
    }, MoreExecutors.directExecutor());


    // Send an initial response with the current serving status
    ServingStatus servingStatus = getServiceStatus(serviceName);
    HealthCheckResponse initialResponse = buildHealthCheckResponse(servingStatus);
    responseObserver.onNext(initialResponse);

    // Continuously listen for service status changes
    while (!cancellableContext.isCancelled()) {
      // Wait for the service status to change
      // Update the serving status and send a new response
      servingStatus = waitForServiceStatusChange(serviceName);
      HealthCheckResponse updatedResponse = buildHealthCheckResponse(servingStatus);
      responseObserver.onNext(updatedResponse);
    }

    cancellationContexts.remove(serviceName);
    responseObserver.onCompleted();
  }

  private HealthCheckResponse buildHealthCheckResponse(ServingStatus status)
  {
    return HealthCheckResponse
        .newBuilder()
        .setStatus(status)
        .build();
  }

  // Method to register a new service with its initial serving status
  public void registerService(String serviceName, ServingStatus servingStatus)
  {
    setServiceStatus(serviceName, servingStatus);
  }

  // Method to unregister a service
  public void unregisterService(String serviceName)
  {
    setServiceStatus(serviceName, ServingStatus.NOT_SERVING);
  }

  private void setServiceStatus(String serviceName, ServingStatus newStatus)
  {
    ServingStatus currentStatus = getServiceStatus(serviceName);
    if (currentStatus != newStatus) {
      serviceStatusMap.put(serviceName, newStatus);

      // Notify the waiting threads
      CountDownLatch statusChangeLatch = statusChangeLatchMap.get(serviceName);
      if (statusChangeLatch != null) {
        statusChangeLatch.countDown();
      }
    }
  }

  public ServingStatus getServiceStatus(String serviceName)
  {
    return serviceStatusMap.getOrDefault(serviceName, ServingStatus.UNKNOWN);
  }

  public ServingStatus waitForServiceStatusChange(String serviceName)
  {
    CountDownLatch statusChangeLatch = new CountDownLatch(1);
    statusChangeLatchMap.put(serviceName, statusChangeLatch);

    // Wait for the status change or until the thread is interrupted
    try {
      statusChangeLatch.await();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    statusChangeLatchMap.remove(serviceName);

    return getServiceStatus(serviceName);
  }
}
