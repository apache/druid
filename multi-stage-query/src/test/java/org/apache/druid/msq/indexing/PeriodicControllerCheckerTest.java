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

package org.apache.druid.msq.indexing;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PeriodicControllerCheckerTest
{
  private static final String CONTROLLER_ID = "controller-123";
  private static final String WORKER_ID = "worker-456";

  @Mock
  private ServiceLocator controllerLocator;

  private ExecutorService testExecutor;
  private AutoCloseable mockCloser;
  private CountDownLatch workerCanceled;

  @Before
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    testExecutor = Executors.newSingleThreadExecutor();
    workerCanceled = new CountDownLatch(1);
  }

  @After
  public void tearDown() throws Exception
  {
    testExecutor.shutdownNow();
    if (mockCloser != null) {
      mockCloser.close();
    }
  }

  @Test
  public void testControllerCheckerExitsOnlyWhenClosed() throws Exception
  {
    // Set up sequence of service location responses
    final ServiceLocation validLocation = new ServiceLocation("localhost", 8080, -1, "/");
    final ServiceLocation anotherValidLocation = new ServiceLocation("localhost", 8081, -1, "/");

    // With this sequence, the checker should close on the 4th call to locate()
    Mockito.when(controllerLocator.locate()).thenReturn(
        Futures.immediateFuture(ServiceLocations.forLocation(validLocation)),
        Futures.immediateFuture(ServiceLocations.forLocation(anotherValidLocation)),
        Futures.immediateFuture(ServiceLocations.forLocations(Collections.emptySet())),
        Futures.immediateFuture(ServiceLocations.closed())
    );

    final PeriodicControllerChecker checker = new PeriodicControllerChecker(
        CONTROLLER_ID,
        WORKER_ID,
        controllerLocator,
        () -> workerCanceled.countDown(),
        testExecutor
    );

    checker.start();

    // Wait for checker to finish
    Assert.assertTrue(workerCanceled.await(1, TimeUnit.MINUTES));

    // Verify ServiceLocator was called exactly four times
    Mockito.verify(controllerLocator, Mockito.times(4)).locate();

    checker.close();
  }
}
