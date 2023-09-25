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
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class LoadedSegmentDataProvider
{
  private final RichSegmentDescriptor segmentDescriptor;
  private final String dataSource;
  private final ChannelCounters channelCounters;
  private final ServiceClientFactory serviceClientFactory;
  private final CoordinatorClient coordinatorClient;
  private final ObjectMapper objectMapper;
  private final ObjectMapper smileMapper;

  public LoadedSegmentDataProvider(
      RichSegmentDescriptor segmentDescriptor,
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      CoordinatorClient coordinatorClient,
      ObjectMapper objectMapper,
      ObjectMapper smileMapper
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.coordinatorClient = coordinatorClient;
    this.objectMapper = objectMapper;
    this.smileMapper = smileMapper;
  }

  public  <ReturnType, QueryReturn> Sequence<ReturnType> fetchServedSegmentInternal(
      Query<QueryReturn> query,
      Function<Sequence<QueryReturn>, Sequence<ReturnType>> mappingFunction,
      Closer closer
  )
  {
    query = query.withDataSource(new TableDataSource(dataSource));
    query = Queries.withSpecificSegments(
        query,
        ImmutableList.of(segmentDescriptor)
    );

    DataServerClient<QueryReturn> dataServerClient = new DataServerClient<>(
        serviceClientFactory,
        new FixedSetServiceLocator(segmentDescriptor.getServers()),
        objectMapper,
        smileMapper
    );
    DefaultResponseContext responseContext = new DefaultResponseContext();
    ResourceHolder<Sequence<QueryReturn>> clientResponseResourceHolder = dataServerClient.run(query, responseContext);
    closer.register(clientResponseResourceHolder);

    Sequence<QueryReturn> queryReturnSequence = clientResponseResourceHolder.get();

    AtomicInteger rowCount = new AtomicInteger(0);
    Sequence<ReturnType> parsedResponse = mappingFunction.apply(queryReturnSequence).map(row -> {
      rowCount.incrementAndGet();
      return row;
    });
    closer.register(() -> channelCounters.addFile(rowCount.get(), 0));
    return parsedResponse;
  }
}
