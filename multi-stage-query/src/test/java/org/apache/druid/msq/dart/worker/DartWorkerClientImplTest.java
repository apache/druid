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

package org.apache.druid.msq.dart.worker;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactoryImpl;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

public class DartWorkerClientImplTest
{
  private static final String QUERY_ID = "abc-123";
  private static final WorkerId WORKER_ID = new WorkerId("http", "localhost:8100", QUERY_ID);

  private ScheduledExecutorService connectExec;
  private DartWorkerClientImpl workerClient;

  @BeforeEach
  public void setUp()
  {
    connectExec = Execs.scheduledSingleThreaded("DartWorkerClientImplTest-%s");
    workerClient = new DartWorkerClientImpl(
        QUERY_ID,
        new ServiceClientFactoryImpl(Mockito.mock(HttpClient.class), connectExec),
        TestHelper.makeSmileMapper(),
        "localhost:8080"
    );
  }

  @AfterEach
  public void tearDown()
  {
    workerClient.close();
    connectExec.shutdownNow();
  }

  @Test
  public void test_getClient_isCachedPerWorker()
  {
    final ServiceClient client = workerClient.getClient(WORKER_ID.toString());
    Assertions.assertSame(client, workerClient.getClient(WORKER_ID.toString()));
  }

  @Test
  public void test_getClient_wrongQueryId()
  {
    final WorkerId otherWorkerId = new WorkerId("http", "localhost:8100", "other-query");
    Assertions.assertThrows(
        DruidException.class,
        () -> workerClient.getClient(otherWorkerId.toString())
    );
  }

  @Test
  public void test_closeClient_staysClosed()
  {
    final ServiceClient client = workerClient.getClient(WORKER_ID.toString());
    workerClient.closeClient(WORKER_ID.toString());

    // The closed client is retained, rather than being replaced by a fresh one that would contact the worker again.
    Assertions.assertSame(client, workerClient.getClient(WORKER_ID.toString()));
    assertRequestFailsAsClosed();
  }

  @Test
  public void test_closeClient_beforeGetClient()
  {
    // Closing a worker we never contacted still prevents it from being contacted later.
    workerClient.closeClient(WORKER_ID.toString());
    assertRequestFailsAsClosed();
  }

  @Test
  public void test_closeClient_afterClose_isNoop()
  {
    workerClient.close();
    Assertions.assertDoesNotThrow(() -> workerClient.closeClient(WORKER_ID.toString()));
    Assertions.assertThrows(DruidException.class, () -> workerClient.getClient(WORKER_ID.toString()));
  }

  /**
   * Verify that a request to {@link #WORKER_ID} fails immediately, rather than retrying, due to its client
   * being closed.
   */
  private void assertRequestFailsAsClosed()
  {
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> workerClient.stopWorker(WORKER_ID.toString()).get()
    );

    Assertions.assertInstanceOf(ServiceClosedException.class, e.getCause());
  }
}
