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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.utils.JvmUtils;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for building {@link DirectDruidClient}
 */
@LazySingleton
public class DirectDruidClientFactory
{
  private final ServiceEmitter emitter;
  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService queryCancellationExecutor;

  @Inject
  public DirectDruidClientFactory(
      final ServiceEmitter emitter,
      final QueryToolChestWarehouse warehouse,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient
  )
  {
    this.emitter = emitter;
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;

    int threadCount = Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 2);
    this.queryCancellationExecutor = ScheduledExecutors.fixed(threadCount, "query-cancellation-executor");
  }

  public DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getScheme(),
        server.getHost(),
        emitter,
        queryCancellationExecutor
    );
  }
}
