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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
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
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Class responsible for querying dataservers and retriving results for a given query. Also queries the coordinator
 * to check if a segment has been handed off.
 */
public class LoadedSegmentDataProvider
{
  private static final Logger log = new Logger(LoadedSegmentDataProvider.class);
  private static final int DEFAULT_NUM_TRIES = 5;
  private final String dataSource;
  private final ChannelCounters channelCounters;
  private final ServiceClientFactory serviceClientFactory;
  private final CoordinatorClient coordinatorClient;
  private final ObjectMapper objectMapper;
  private final QueryToolChestWarehouse warehouse;
  private final ScheduledExecutorService queryCancellationExecutor;

  public LoadedSegmentDataProvider(
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      CoordinatorClient coordinatorClient,
      ObjectMapper objectMapper,
      QueryToolChestWarehouse warehouse,
      ScheduledExecutorService queryCancellationExecutor
  )
  {
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.coordinatorClient = coordinatorClient;
    this.objectMapper = objectMapper;
    this.warehouse = warehouse;
    this.queryCancellationExecutor = queryCancellationExecutor;
  }

  @VisibleForTesting
  DataServerClient makeDataServerClient(ServiceLocation serviceLocation)
  {
    return new DataServerClient(serviceClientFactory, serviceLocation, objectMapper, queryCancellationExecutor);
  }

  /**
   * Performs some necessary transforms to the query, so that the dataserver is able to understand it first.
   * - Changing the datasource to a {@link TableDataSource}
   * - Limiting the query to a single required segment with {@link Queries#withSpecificSegments(Query, List)}
   * <br>
   * Then queries a data server and returns a {@link Yielder} for the results, retrying if needed. If a dataserver
   * indicates that the segment was not found, checks with the coordinator to see if the segment was handed off.
   * - If the segment was handed off, returns with a {@link DataServerQueryStatus#HANDOFF} status.
   * - If the segment was not handed off, retries with the known list of servers and throws an exception if the retry
   * count is exceeded.
   * - If the servers could not be found, checks if the segment was handed-off. If it was, returns with a
   * {@link DataServerQueryStatus#HANDOFF} status. Otherwise, throws an exception.
   * <br>
   * Also applies {@link QueryToolChest#makePreComputeManipulatorFn(Query, MetricManipulationFn)} and reports channel
   * metrics on the returned results.
   *
   * @param <QueryType> result return type for the query from the data server
   * @param <RowType> type of the result rows after parsing from QueryType object
   */
  public <RowType, QueryType> Pair<DataServerQueryStatus, Yielder<RowType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      RichSegmentDescriptor segmentDescriptor,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Closer closer
  ) throws IOException
  {
    final Query<QueryType> preparedQuery = Queries.withSpecificSegments(
        query.withDataSource(new TableDataSource(dataSource)),
        ImmutableList.of(segmentDescriptor)
    );

    final Set<DruidServerMetadata> servers = segmentDescriptor.getServers();
    final FixedSetServiceLocator fixedSetServiceLocator = FixedSetServiceLocator.forDruidServerMetadata(servers);
    final QueryToolChest<QueryType, Query<QueryType>> toolChest = warehouse.getToolChest(query);
    final Function<QueryType, QueryType> preComputeManipulatorFn =
        toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing());

    final JavaType queryResultType = toolChest.getBaseResultType();
    final int numRetriesOnMissingSegments = preparedQuery.context().getNumRetriesOnMissingSegments(DEFAULT_NUM_TRIES);

    log.debug("Querying severs[%s] for segment[%s], retries:[%d]", servers, segmentDescriptor, numRetriesOnMissingSegments);
    final ResponseContext responseContext = new DefaultResponseContext();

    Pair<DataServerQueryStatus, Yielder<RowType>> statusSequencePair;
    try {
      // We need to check for handoff to decide if we need to retry. Therefore, we handle it here instead of inside
      // the client.
      statusSequencePair = RetryUtils.retry(
          () -> {
            ServiceLocation serviceLocation = CollectionUtils.getOnlyElement(
                fixedSetServiceLocator.locate().get().getLocations(),
                serviceLocations -> {
                  throw DruidException.defensive("Should only have one location");
                }
            );
            DataServerClient dataServerClient = makeDataServerClient(serviceLocation);
            Sequence<QueryType> sequence = dataServerClient.run(preparedQuery, responseContext, queryResultType, closer)
                                                           .map(preComputeManipulatorFn);
            final List<SegmentDescriptor> missingSegments = getMissingSegments(responseContext);
            // Only one segment is fetched, so this should be empty if it was fetched
            if (missingSegments.isEmpty()) {
              log.debug("Successfully fetched rows from server for segment[%s]", segmentDescriptor);
              // Segment was found
              Yielder<RowType> yielder = closer.register(
                  Yielders.each(mappingFunction.apply(sequence)
                                               .map(row -> {
                                                 channelCounters.incrementRowCount();
                                                 return row;
                                               }))
              );
              return Pair.of(DataServerQueryStatus.SUCCESS, yielder);
            } else {
              Boolean wasHandedOff = checkSegmentHandoff(coordinatorClient, dataSource, segmentDescriptor);
              if (Boolean.TRUE.equals(wasHandedOff)) {
                log.debug("Segment[%s] was handed off.", segmentDescriptor);
                return Pair.of(DataServerQueryStatus.HANDOFF, null);
              } else {
                log.error("Segment[%s] could not be found on data server, but segment was not handed off.", segmentDescriptor);
                throw new IOE(
                    "Segment[%s] could not be found on data server, but segment was not handed off.",
                    segmentDescriptor
                );
              }
            }
          },
          throwable -> !(throwable instanceof QueryInterruptedException && throwable.getCause() instanceof InterruptedException),
          numRetriesOnMissingSegments
      );

      return statusSequencePair;
    }
    catch (QueryInterruptedException e) {
      if (e.getCause() instanceof RpcException) {
        // In the case that all the realtime servers for a segment are gone (for example, if they were scaled down),
        // we would also be unable to fetch the segment. Check if the segment was handed off, just in case, instead of
        // failing the query.
        boolean wasHandedOff = checkSegmentHandoff(coordinatorClient, dataSource, segmentDescriptor);
        if (wasHandedOff) {
          log.debug("Segment[%s] was handed off.", segmentDescriptor);
          return Pair.of(DataServerQueryStatus.HANDOFF, null);
        }
      }
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Exception while fetching rows for query from dataservers[%s]", servers);
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e, IOE.class);
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Exception while fetching rows for query from dataservers[%s]", servers);
    }
  }

  /**
   * Retreives the list of missing segments from the response context.
   */
  private static List<SegmentDescriptor> getMissingSegments(final ResponseContext responseContext)
  {
    List<SegmentDescriptor> missingSegments = responseContext.getMissingSegments();
    if (missingSegments == null) {
      return ImmutableList.of();
    }
    return missingSegments;
  }

  /**
   * Queries the coordinator to check if a segment has been handed off.
   * <br>
   * See {@link  org.apache.druid.server.http.DataSourcesResource#isHandOffComplete(String, String, int, String)}
   */
  private static boolean checkSegmentHandoff(
      CoordinatorClient coordinatorClient,
      String dataSource,
      SegmentDescriptor segmentDescriptor
  ) throws IOE
  {
    Boolean wasHandedOff;
    try {
      wasHandedOff = FutureUtils.get(coordinatorClient.isHandoffComplete(dataSource, segmentDescriptor), true);
    }
    catch (Exception e) {
      throw new IOE(e, "Could not contact coordinator for segment[%s]", segmentDescriptor);
    }
    return Boolean.TRUE.equals(wasHandedOff);
  }

  /**
   * Represents the status of fetching a segment from a data server
   */
  public enum DataServerQueryStatus
  {
    /**
     * Segment was found on the data server and fetched successfully.
     */
    SUCCESS,
    /**
     * Segment was not found on the realtime server as it has been handed off to a historical. Only returned while
     * querying a realtime server.
     */
    HANDOFF
  }
}
