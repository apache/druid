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
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Set;
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

  public LoadedSegmentDataProvider(
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      CoordinatorClient coordinatorClient,
      ObjectMapper objectMapper
  )
  {
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.coordinatorClient = coordinatorClient;
    this.objectMapper = objectMapper;
  }

  @VisibleForTesting
  DataServerClient makeDataServerClient(FixedSetServiceLocator serviceLocator)
  {
    return new DataServerClient(serviceClientFactory, serviceLocator, objectMapper);
  }

  /**
   * Queries a data server and returns a {@link Yielder} for the results, retrying if needed. If a dataserver indicates
   * that the segment was not found, checks with the coordinator to see if the segment was handed off.
   * - If the segment was handed off, returns with a {@link DataServerQueryStatus#HANDOFF} status.
   * - If the segment was not handed off, retries with the known list of servers and throws an exception if the retry
   * count is exceeded.
   *
   * @param <QueryType> result return type for the query from the data server
   * @param <RowType> type of the result rows after parsing from QueryType object
   */
  public <RowType, QueryType> Pair<DataServerQueryStatus, Yielder<RowType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      RichSegmentDescriptor segmentDescriptor,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Class<QueryType> resultClass,
      Closer closer
  ) throws IOException
  {
    final Query<QueryType> preparedQuery = Queries.withSpecificSegments(
        query.withDataSource(new TableDataSource(dataSource)),
        ImmutableList.of(segmentDescriptor)
    );

    Set<DruidServerMetadata> servers = segmentDescriptor.getServers();
    DataServerClient dataServerClient = makeDataServerClient(FixedSetServiceLocator.forDruidServerMetadata(servers));

    final JavaType queryResultType = objectMapper.getTypeFactory().constructType(resultClass);
    final int numRetriesOnMissingSegments = preparedQuery.context().getNumRetriesOnMissingSegments(DEFAULT_NUM_TRIES);

    log.debug("Querying severs[%s] for segment[%s], retries:[%d]", servers, segmentDescriptor, numRetriesOnMissingSegments);
    final ResponseContext responseContext = new DefaultResponseContext();

    Pair<DataServerQueryStatus, Yielder<RowType>> statusSequencePair;
    try {
      statusSequencePair = RetryUtils.retry(
          () -> {
            Sequence<QueryType> sequence = dataServerClient.run(preparedQuery, responseContext, queryResultType, closer);
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
        // we would also be unable to fetch the segment. Check if the segment was handed off, just in case.
        boolean wasHandedOff = checkSegmentHandoff(coordinatorClient, dataSource, segmentDescriptor);
        if (wasHandedOff) {
          log.debug("Segment[%s] was handed off.", segmentDescriptor);
          return Pair.of(DataServerQueryStatus.HANDOFF, null);
        }
      }
      throw new IOE(e, "Exception while fetching rows for query from dataservers[%s]", servers);
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e, IOE.class);
      throw new IOE(e, "Exception while fetching rows for query from dataservers[%s]", servers);
    }
  }

  private static List<SegmentDescriptor> getMissingSegments(final ResponseContext responseContext)
  {
    List<SegmentDescriptor> missingSegments = responseContext.getMissingSegments();
    if (missingSegments == null) {
      return ImmutableList.of();
    }
    return missingSegments;
  }

  private static boolean checkSegmentHandoff(
      CoordinatorClient coordinatorClient,
      String dataSource,
      SegmentDescriptor segmentDescriptor
  ) throws IOE
  {
    Boolean wasHandedOff;
    try {
      wasHandedOff = RetryUtils.retry(
          () -> FutureUtils.get(coordinatorClient.isHandoffComplete(dataSource, segmentDescriptor), true),
          input -> true,
          DEFAULT_NUM_TRIES
      );
    }
    catch (Exception e) {
      throw new IOE(e, "Could not contact coordinator");
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
