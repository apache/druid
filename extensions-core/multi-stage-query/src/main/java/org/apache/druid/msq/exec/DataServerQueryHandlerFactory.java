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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.rpc.ServiceClientFactory;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates new instances of {@link DataServerQueryHandler} and manages the cancellation threadpool.
 */
public class DataServerQueryHandlerFactory implements Closeable
{
  private static final Logger log = new Logger(DataServerQueryHandlerFactory.class);
  private static final int DEFAULT_THREAD_COUNT = 4;
  private final CoordinatorClient coordinatorClient;
  private final ServiceClientFactory serviceClientFactory;
  private final ObjectMapper objectMapper;
  private final QueryToolChestWarehouse warehouse;
  private final ScheduledExecutorService queryCancellationExecutor;

  public DataServerQueryHandlerFactory(
      CoordinatorClient coordinatorClient,
      ServiceClientFactory serviceClientFactory,
      ObjectMapper objectMapper,
      QueryToolChestWarehouse warehouse
  )
  {
    this.coordinatorClient = coordinatorClient;
    this.serviceClientFactory = serviceClientFactory;
    this.objectMapper = objectMapper;
    this.warehouse = warehouse;
    this.queryCancellationExecutor = ScheduledExecutors.fixed(DEFAULT_THREAD_COUNT, "query-cancellation-executor");
  }

  public DataServerQueryHandler createDataServerQueryHandler(
      String dataSource,
      ChannelCounters channelCounters,
      DataServerRequestDescriptor dataServerRequestDescriptor
  )
  {
    return new DataServerQueryHandler(
        dataSource,
        channelCounters,
        serviceClientFactory,
        coordinatorClient,
        objectMapper,
        warehouse,
        queryCancellationExecutor,
        dataServerRequestDescriptor
    );
  }

  @Override
  public void close()
  {
    // Wait for all query cancellations to be complete.
    log.info("Waiting for any data server queries to be canceled.");
    queryCancellationExecutor.shutdown();
    try {
      if (!queryCancellationExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        log.error("Unable to cancel all ongoing queries.");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RE(e);
    }
  }
}
