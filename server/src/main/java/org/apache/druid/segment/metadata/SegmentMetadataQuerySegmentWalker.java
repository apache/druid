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

package org.apache.druid.segment.metadata;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.SegmentLoadInfo;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.FluentQueryRunner;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * This {@link QuerySegmentWalker} implementation is specific to SegmentMetadata queries
 * executed by {@link CoordinatorSegmentMetadataCache} and is in parity with {@link CachingClusteredClient}.
 */
public class SegmentMetadataQuerySegmentWalker implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(SegmentMetadataQuerySegmentWalker.class);
  private final CoordinatorServerView serverView;
  private final DruidHttpClientConfig httpClientConfig;
  private final QueryToolChestWarehouse warehouse;
  private final ServerConfig serverConfig;
  private final ServiceEmitter emitter;

  @Inject
  public SegmentMetadataQuerySegmentWalker(
      final CoordinatorServerView serverView,
      final DruidHttpClientConfig httpClientConfig,
      final QueryToolChestWarehouse warehouse,
      final ServerConfig serverConfig,
      final ServiceEmitter emitter
  )
  {
    this.serverView = serverView;
    this.httpClientConfig = httpClientConfig;
    this.warehouse = warehouse;
    this.emitter = emitter;
    this.serverConfig = serverConfig;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return decorateRunner(query, new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return SegmentMetadataQuerySegmentWalker.this.run(
            queryPlus,
            responseContext,
            new CachingClusteredClient.TimelineConverter<>(specs)
        );
      }
    });
  }

  private <T> QueryRunner<T> decorateRunner(Query<T> query, QueryRunner<T> baseClusterRunner)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    final SetAndVerifyContextQueryRunner<T> baseRunner = new SetAndVerifyContextQueryRunner<>(
        serverConfig,
        baseClusterRunner
    );

    return FluentQueryRunner
        .create(baseRunner, toolChest)
        .emitCPUTimeMetric(emitter);
  }

  private <T> Sequence<T> run(
      QueryPlus<T> queryPlus,
      final ResponseContext responseContext,
      final UnaryOperator<TimelineLookup<String, SegmentLoadInfo>> timelineConverter
  )
  {
    final Query<T> query = queryPlus.getQuery();

    final TimelineLookup<String, SegmentLoadInfo> timeline = serverView.getTimeline(
        query.getDataSource()
    );
    if (timeline == null) {
      return Sequences.empty();
    }

    final TimelineLookup<String, SegmentLoadInfo> timelineLookup = timelineConverter.apply(timeline);

    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    Set<Pair<SegmentDescriptor, SegmentLoadInfo>> segmentAndServers = computeSegmentsToQuery(timelineLookup, query, toolChest);

    queryPlus = queryPlus.withQueryMetrics(toolChest);
    queryPlus.getQueryMetrics().reportQueriedSegmentCount(segmentAndServers.size()).emit(emitter);

    final SortedMap<String, List<SegmentDescriptor>> serverSegments = groupSegmentsByServer(segmentAndServers, query);

    final List<Sequence<T>> listOfSequences = new ArrayList<>(serverSegments.size());

    QueryPlus<T> finalQueryPlus = queryPlus;
    serverSegments.forEach((server, segmentsOfServer) -> {
      final QueryRunner serverRunner = serverView.getQueryRunner(server);

      if (serverRunner == null) {
        log.error("Server [%s] doesn't have a query runner", server);
        return;
      }

      // Divide user-provided maxQueuedBytes by the number of servers, and limit each server to that much.
      final long maxQueuedBytes = httpClientConfig.getMaxQueuedBytes();
      final long maxQueuedBytesPerServer = maxQueuedBytes / serverSegments.size();
      final Sequence<T> serverResults = getServerResults(
          serverRunner,
          finalQueryPlus,
          responseContext,
          maxQueuedBytesPerServer,
          segmentsOfServer
      );
      listOfSequences.add(serverResults);
    });

    return merge(queryPlus.getQuery(), listOfSequences);
  }

  <T> Sequence<T> getServerResults(
      QueryRunner serverRunner,
      QueryPlus<T> queryPlus,
      ResponseContext responseContext,
      long maxQueuedBytesPerServer,
      List<SegmentDescriptor> segmentDescriptors)
  {
    return serverRunner.run(
        queryPlus.withQuery(
            Queries.withSpecificSegments(
                queryPlus.getQuery(),
                segmentDescriptors
            )
        ).withMaxQueuedBytes(maxQueuedBytesPerServer),
        responseContext
    );
  }

  private <T> Set<Pair<SegmentDescriptor, SegmentLoadInfo>> computeSegmentsToQuery(
      TimelineLookup<String, SegmentLoadInfo> timeline,
      Query<T> query,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    final Function<Interval, List<TimelineObjectHolder<String, SegmentLoadInfo>>> lookupFn
        = timeline::lookupWithIncompletePartitions;

    final List<Interval> intervals = query.getIntervals();
    List<TimelineObjectHolder<String, SegmentLoadInfo>> timelineObjectHolders =
        intervals.stream().flatMap(i -> lookupFn.apply(i).stream()).collect(Collectors.toList());

    final List<TimelineObjectHolder<String, SegmentLoadInfo>> serversLookup = toolChest.filterSegments(query, timelineObjectHolders);

    Set<Pair<SegmentDescriptor, SegmentLoadInfo>> segmentAndServers = new HashSet<>();
    for (TimelineObjectHolder<String, SegmentLoadInfo> holder : serversLookup) {
      final Set<PartitionChunk<SegmentLoadInfo>> filteredChunks = Sets.newHashSet(holder.getObject());
      for (PartitionChunk<SegmentLoadInfo> chunk : filteredChunks) {
        SegmentLoadInfo server = chunk.getObject();
        final SegmentDescriptor segment = new SegmentDescriptor(
            holder.getInterval(),
            holder.getVersion(),
            chunk.getChunkNumber()
        );
        segmentAndServers.add(new Pair<>(segment, server));
      }
    }

    return segmentAndServers;
  }

  private <T> SortedMap<String, List<SegmentDescriptor>> groupSegmentsByServer(
      Set<Pair<SegmentDescriptor, SegmentLoadInfo>> segmentAndServers,
      Query<T> query
  )
  {
    final SortedMap<String, List<SegmentDescriptor>> serverSegments = new TreeMap<>();

    for (Pair<SegmentDescriptor, SegmentLoadInfo> segmentAndServer : segmentAndServers) {
      final DruidServerMetadata druidServerMetadata = segmentAndServer.rhs.pickOne();

      if (druidServerMetadata == null) {
        log.makeAlert(
            "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
            segmentAndServer.lhs,
            query.getDataSource()
        ).emit();
      } else {
        serverSegments.computeIfAbsent(druidServerMetadata.getName(), s -> new ArrayList<>()).add(segmentAndServer.lhs);
      }
    }

    return serverSegments;
  }

  private <T> Sequence<T> merge(Query<T> query, List<Sequence<T>> sequencesByInterval)
  {
    return Sequences
          .simple(sequencesByInterval)
          .flatMerge(seq -> seq, query.getResultOrdering());
  }
}
