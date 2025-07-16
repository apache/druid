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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.rpc.ServiceClientFactory;

/**
 * Creates new instances of {@link IndexerDataServerQueryHandler}.
 */
public class IndexerDataServerQueryHandlerFactory implements DataServerQueryHandlerFactory
{
  private final CoordinatorClient coordinatorClient;
  private final ServiceClientFactory serviceClientFactory;
  private final ObjectMapper objectMapper;
  private final QueryToolChestWarehouse warehouse;

  public IndexerDataServerQueryHandlerFactory(
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
  }

  @Override
  public IndexerDataServerQueryHandler createDataServerQueryHandler(
      int inputNumber,
      String dataSource,
      ChannelCounters channelCounters,
      DataServerRequestDescriptor requestDescriptor
  )
  {
    return new IndexerDataServerQueryHandler(
        dataSource,
        channelCounters,
        serviceClientFactory,
        coordinatorClient,
        objectMapper,
        warehouse,
        requestDescriptor
    );
  }
}
