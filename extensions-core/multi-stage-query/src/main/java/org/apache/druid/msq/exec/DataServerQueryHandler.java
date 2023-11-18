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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.msq.input.table.DataServerSelector;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.server.coordination.DruidServerMetadata;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class responsible for querying dataservers and retriving results for a given query. Also queries the coordinator
 * to check if a segment has been handed off.
 */
public class DataServerQueryHandler
{
  private static final Logger log = new Logger(DataServerQueryHandler.class);
  private static final int DEFAULT_NUM_TRIES = 3;
  private final String dataSource;
  private final ChannelCounters channelCounters;
  private final ServiceClientFactory serviceClientFactory;
  private final CoordinatorClient coordinatorClient;
  private final ObjectMapper objectMapper;
  private final QueryToolChestWarehouse warehouse;
  private final ScheduledExecutorService queryCancellationExecutor;
  private final DataServerRequestDescriptor dataServerRequestDescriptor;

  public DataServerQueryHandler(
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      CoordinatorClient coordinatorClient,
      ObjectMapper objectMapper,
      QueryToolChestWarehouse warehouse,
      ScheduledExecutorService queryCancellationExecutor,
      DataServerRequestDescriptor dataServerRequestDescriptor
  )
  {
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.coordinatorClient = coordinatorClient;
    this.objectMapper = objectMapper;
    this.warehouse = warehouse;
    this.queryCancellationExecutor = queryCancellationExecutor;
    this.dataServerRequestDescriptor = dataServerRequestDescriptor;
  }

  @VisibleForTesting
  DataServerClient makeDataServerClient(ServiceLocation serviceLocation)
  {
    return new DataServerClient(serviceClientFactory, serviceLocation, objectMapper, queryCancellationExecutor);
  }

  /**
   * Performs some necessary transforms to the query, so that the dataserver is able to understand it first.
   * - Changing the datasource to a {@link TableDataSource}
   * - Limiting the query to the required segments with {@link Queries#withSpecificSegments(Query, List)}
   * <br>
   * Then queries a data server and returns a {@link Yielder} for the results, retrying if needed. If a dataserver
   * indicates that some segments were not found, checks with the coordinator to see if the segment was handed off.
   * - If all the segments were handed off, returns a {@link DataServerQueryResult} with the yielder and list of handed
   * off segments.
   * - If some segments were not handed off, checks with the coordinator fetch an updated list of servers. This step is
   * repeated up to {@link #DEFAULT_NUM_TRIES} times.
   * - If the servers could not be found, checks if the segment was handed-off. If it was, returns a
   * {@link DataServerQueryResult} with the yielder and list of handed off segments. Otherwise, throws an exception.
   * <br>
   * Also applies {@link QueryToolChest#makePreComputeManipulatorFn(Query, MetricManipulationFn)} and reports channel
   * metrics on the returned results.
   *
   * @param <QueryType> result return type for the query from the data server
   * @param <RowType> type of the result rows after parsing from QueryType object
   */
  public <RowType, QueryType> DataServerQueryResult<RowType> fetchRowsFromDataServer(
      Query<QueryType> query,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Closer closer
  )
  {
    final Query<QueryType> preparedQuery = query.withDataSource(new TableDataSource(dataSource));
    final QueryToolChest<QueryType, Query<QueryType>> toolChest = warehouse.getToolChest(preparedQuery);
    final Function<QueryType, QueryType> preComputeManipulatorFn =
        toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing());
    final JavaType queryResultType = toolChest.getBaseResultType();
    final int numRetriesOnMissingSegments = preparedQuery.context().getNumRetriesOnMissingSegments(DEFAULT_NUM_TRIES);

    final SegmentSource includeSegmentSource = MultiStageQueryContext.getSegmentSources(query.context());

    final Queue<DataServerRequestDescriptor> dataServerRequestDescriptorQueue = new ArrayDeque<>();
    // Add the initial request to the queue.
    dataServerRequestDescriptorQueue.add(dataServerRequestDescriptor);

    Sequence<QueryType> returnSequence = Sequences.empty();
    final List<RichSegmentDescriptor> handedOffSegments = new ArrayList<>();

    int retryCount = 0;
    while (retryCount < numRetriesOnMissingSegments) {
      while (!dataServerRequestDescriptorQueue.isEmpty()) {
        DataServerRequestDescriptor requestDescriptor = dataServerRequestDescriptorQueue.remove();

        log.debug(
            "Querying server[%s] for segments[%s], retry:[%d]/[%d]",
            requestDescriptor.getServerMetadata(),
            requestDescriptor.getSegments(),
            retryCount,
            numRetriesOnMissingSegments
        );

        final ServiceLocation serviceLocation = ServiceLocation.fromDruidServerMetadata(requestDescriptor.getServerMetadata());
        final DataServerClient dataServerClient = makeDataServerClient(serviceLocation);

        Sequence<QueryType> sequence = null;
        List<RichSegmentDescriptor> missingSegments;

        try {
          final ResponseContext responseContext = new DefaultResponseContext();
          sequence =
              RetryUtils.retry(
                  () -> dataServerClient.run(
                      Queries.withSpecificSegments(
                          preparedQuery,
                          requestDescriptor.getSegments().stream().map(RichSegmentDescriptor::toSegmentDescritor).collect(Collectors.toList())
                      ), responseContext, queryResultType, closer).map(preComputeManipulatorFn),
                  throwable -> !(throwable instanceof QueryInterruptedException
                                 && throwable.getCause() instanceof InterruptedException),
                  5
              );
          missingSegments = getMissingSegments(responseContext);
        }
        catch (QueryInterruptedException e) {
          if (e.getCause() instanceof RpcException) {
            // In the case that all the realtime servers for a segment are gone (for example, if they were scaled down),
            // we would also be unable to fetch the segment.
            missingSegments = requestDescriptor.getSegments();
          } else {
            throw new RuntimeException(e); // TODO: better exception
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Add results
        if (sequence != null) {
          returnSequence = Sequences.concat(returnSequence, sequence);
        }

        if (missingSegments.isEmpty()) {
          continue;
        }

        List<RichSegmentDescriptor> notHandedOffSegments = findNonHandedOffSegments(missingSegments);
        for (RichSegmentDescriptor descriptor : missingSegments) {
          if (!notHandedOffSegments.contains(descriptor)) {
            handedOffSegments.add(descriptor);
          }
        }

        dataServerRequestDescriptorQueue.addAll(createWeightedSegmentSet(notHandedOffSegments, includeSegmentSource));
      }
      retryCount++;
    }

    return new DataServerQueryResult<>(closer.register(createYielder(returnSequence, mappingFunction)), handedOffSegments, dataSource);
  }

  private <RowType, QueryType> Yielder<RowType> createYielder(
      Sequence<QueryType> sequence,
      Function<Sequence<QueryType>,
      Sequence<RowType>> mappingFunction
  )
  {
    return Yielders.each(
        mappingFunction.apply(sequence)
                       .map(row -> {
                         channelCounters.incrementRowCount();
                         return row;
                       })
    );
  }

  private List<DataServerRequestDescriptor> createWeightedSegmentSet(List<RichSegmentDescriptor> segmentDescriptors, SegmentSource includeSegmentSource)
  {
    List<DataServerRequestDescriptor> requestDescriptors = new ArrayList<>();
    final Map<DruidServerMetadata, Set<RichSegmentDescriptor>> serverVsSegmentsMap = new HashMap<>();
    Iterable<ImmutableSegmentLoadInfo> immutableSegmentLoadInfos
        = coordinatorClient.fetchServerViewSegments(dataSource,
                                                    segmentDescriptors.stream()
                                                                      .map(SegmentDescriptor::getInterval)
                                                                      .collect(Collectors.toList()));

    for (ImmutableSegmentLoadInfo segmentLoadInfo : immutableSegmentLoadInfos) {
      Set<DruidServerMetadata> collect = segmentLoadInfo.getServers()
                                                        .stream()
                                                        .filter(druidServerMetadata -> includeSegmentSource.getUsedServerTypes()
                                                                                                           .contains(
                                                                                                               druidServerMetadata.getType()))
                                                        .collect(Collectors.toSet());
      if (collect.isEmpty()) {
        throw new RE("Segment not found");
      }

      DruidServerMetadata druidServerMetadata = DataServerSelector.RANDOM.getSelectServerFunction().apply(collect);
      serverVsSegmentsMap.computeIfAbsent(druidServerMetadata, ignored -> new HashSet<>());
      serverVsSegmentsMap.get(druidServerMetadata).add(new RichSegmentDescriptor(segmentLoadInfo.getSegment().toDescriptor(), null));
    }

    for (Map.Entry<DruidServerMetadata, Set<RichSegmentDescriptor>> druidServerMetadataSetEntry : serverVsSegmentsMap.entrySet()) {
      DataServerRequestDescriptor dataServerRequest = new DataServerRequestDescriptor(
          druidServerMetadataSetEntry.getKey(),
          ImmutableList.copyOf(druidServerMetadataSetEntry.getValue())
      );
      requestDescriptors.add(dataServerRequest);
    }

    return requestDescriptors;
  }

  /**
   * Retreives the list of missing segments from the response context.
   */
  private static List<RichSegmentDescriptor> getMissingSegments(final ResponseContext responseContext)
  {
    List<SegmentDescriptor> missingSegments = responseContext.getMissingSegments();
    if (missingSegments == null) {
      return ImmutableList.of();
    }
    return missingSegments.stream().map(segment -> new RichSegmentDescriptor(segment, null)).collect(Collectors.toList());
  }

  /**
   * Queries the coordinator to check if a segment has been handed off.
   * <br>
   * See {@link  org.apache.druid.server.http.DataSourcesResource#isHandOffComplete(String, String, int, String)}
   */
  private List<RichSegmentDescriptor> findNonHandedOffSegments(List<RichSegmentDescriptor> segmentDescriptors)
  {
    try {
      List<RichSegmentDescriptor> missingSegments = new ArrayList<>();

      for (RichSegmentDescriptor segmentDescriptor : segmentDescriptors) {
        Boolean wasHandedOff = FutureUtils.get(
            coordinatorClient.isHandoffComplete(dataSource, segmentDescriptor),
            true
        );
        if (!Boolean.TRUE.equals(wasHandedOff)) {
          missingSegments.add(segmentDescriptor);
        }
      }
      return missingSegments;
    }
    catch (Exception e) {
      throw new RE(e, "Could not contact coordinator");
    }
  }
}
