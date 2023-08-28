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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.segment.metadata.DatasourceSchema;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;




/**
 * This class polls the Coordinator in background to keep the latest published segments.
 * Provides {@link #getSegmentMetadata()} for others to get segments in metadata store.
 *
 * The difference between this class and {@link SegmentsMetadataManager} is that this class resides
 * in Broker's memory, while {@link SegmentsMetadataManager} resides in Coordinator's memory. In
 * fact, this class polls the data from {@link SegmentsMetadataManager} object in the memory of the
 * currently leading Coordinator via HTTP queries.
 */
@ManageLifecycle
public class BrokerSegmentMetadataView
{
  private static final EmittingLogger log = new EmittingLogger(BrokerSegmentMetadataView.class);

  private final CoordinatorClient coordinatorClient;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper objectMapper;

  private final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder;

  private final boolean isMetadataSegmentCacheEnabled;

  private final boolean useSegmentMetadataCache;

  /**
   * Use {@link ImmutableSortedSet} so that the order of segments is deterministic and
   * sys.segments queries return the segments in sorted order based on segmentId.
   *
   * Volatile since this reference is reassigned in {@code poll()} and then read in {@code getPublishedSegments()}
   * from other threads.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedSet<SegmentStatusInCluster> segmentMetadata = null;

  private volatile Map<String, DatasourceTable.PhysicalDatasourceMetadata> datasourceSchemaMap = null;

  private final BrokerServerView brokerServerView;

  private final BrokerSegmentMetadataCache segmentMetadataCache;

  /**
   * Caches the replication factor for segment IDs. In case of coordinator restarts or leadership re-elections,
   * the coordinator API returns `null` replication factor until load rules are evaluated.
   * The cache can be used during these periods to continue serving the previously fetched values.
   */
  private final Cache<SegmentId, Integer> segmentIdToReplicationFactor;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodInMS;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CountDownLatch segmentMetadataCachePopulated = new CountDownLatch(1);

  @Inject
  public BrokerSegmentMetadataView(
      final @Coordinator DruidLeaderClient druidLeaderClient,
      final ObjectMapper objectMapper,
      final CoordinatorClient coordinatorClient,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final BrokerSegmentMetadataCacheConfig config,
      final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder,
      final BrokerServerView brokerServerView,
      final BrokerSegmentMetadataCache segmentMetadataCache
      )
  {
    Preconditions.checkNotNull(config, "SegmentMetadataCacheConfig");
    this.druidLeaderClient = druidLeaderClient;
    this.objectMapper = objectMapper;
    this.coordinatorClient = coordinatorClient;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.isMetadataSegmentCacheEnabled = config.isMetadataSegmentCacheEnable();
    this.pollPeriodInMS = config.getMetadataSegmentPollPeriod();
    this.scheduledExec = Execs.scheduledSingleThreaded("MetadataSegmentView-Cache--%d");
    this.segmentIdToReplicationFactor = CacheBuilder.newBuilder()
                                                    .expireAfterAccess(10, TimeUnit.MINUTES)
                                                    .build();
    this.physicalDatasourceMetadataBuilder = physicalDatasourceMetadataBuilder;
    this.useSegmentMetadataCache = config.isUseSegmentMetadataCache();
    this.brokerServerView = brokerServerView;
    this.segmentMetadataCache = segmentMetadataCache;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      if (isMetadataSegmentCacheEnabled || !useSegmentMetadataCache) {
        scheduledExec.schedule(new PollTask(), pollPeriodInMS, TimeUnit.MILLISECONDS);
      }
      lifecycleLock.started();
      log.info("MetadataSegmentView Started. Configs isMetadataSegmentCacheEnabled [%s], useSegmentMetadataCache [%s]",
               isMetadataSegmentCacheEnabled, useSegmentMetadataCache);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }
    log.info("MetadataSegmentView is stopping.");
    if (isMetadataSegmentCacheEnabled) {
      scheduledExec.shutdown();
    }
    log.info("MetadataSegmentView Stopped.");
  }

  private void pollSegmentMetadata()
  {
    log.info("Polling segment metadata from coordinator");

    segmentMetadata = fetchSegmentMetadata();
    segmentMetadataCachePopulated.countDown();
  }

  private ImmutableSortedSet<SegmentStatusInCluster> fetchSegmentMetadata()
  {
    final Iterator<SegmentStatusInCluster> metadataSegments =
        querySegmentMetadata(segmentWatcherConfig.getWatchedDataSources());

    final ImmutableSortedSet.Builder<SegmentStatusInCluster> builder = ImmutableSortedSet.naturalOrder();
    while (metadataSegments.hasNext()) {
      final SegmentStatusInCluster segment = metadataSegments.next();
      final DataSegment interned = DataSegmentInterner.intern(segment.getDataSegment());
      Integer replicationFactor = segment.getReplicationFactor();
      if (replicationFactor == null) {
        replicationFactor = segmentIdToReplicationFactor.getIfPresent(segment.getDataSegment().getId());
      } else {
        segmentIdToReplicationFactor.put(segment.getDataSegment().getId(), segment.getReplicationFactor());
      }
      final SegmentStatusInCluster segmentStatusInCluster = new SegmentStatusInCluster(
          interned,
          segment.isOvershadowed(),
          replicationFactor,
          segment.getNumRows(),
          true,
          segment.isRealtime()
      );
      builder.add(segmentStatusInCluster);
    }
    return builder.build();
  }

  private void pollDatasourceSchema()
  {
    log.info("Polling datasource schema from coordinator.");
    Set<String> datasources = useSegmentMetadataCache ? segmentMetadataCache.getDatasourceNames() : brokerServerView.getDatasourceNames();

    Map<String, DatasourceTable.PhysicalDatasourceMetadata> physicalDatasourceMetadataMap = new HashMap<>();

    for (List<String> partition : Iterables.partition(datasources, 100)) {
      // retain watched datasources
      List<DatasourceSchema> datasourceSchemas = FutureUtils.getUnchecked(coordinatorClient.fetchDatasourceSchema(
          partition), true);

      for (DatasourceSchema datasourceSchema : datasourceSchemas) {
        physicalDatasourceMetadataMap.put(
            datasourceSchema.getDatasource(),
            physicalDatasourceMetadataBuilder.build(datasourceSchema.getDatasource(), datasourceSchema.getRowSignature()));
      }
    }

    this.datasourceSchemaMap = physicalDatasourceMetadataMap;
  }

  ImmutableSortedSet<SegmentStatusInCluster> getSegmentMetadata()
  {
    if (isMetadataSegmentCacheEnabled) {
      Uninterruptibles.awaitUninterruptibly(segmentMetadataCachePopulated);
      return segmentMetadata;
    } else {
      return fetchSegmentMetadata();
    }
  }

  protected SystemSchema.SegmentsTable.SegmentTableView getSegmentTableView() {
    ImmutableSortedSet<SegmentStatusInCluster> allSegmentMetadata = getSegmentMetadata();

    log.info("logging polled segments from coordinator %s", allSegmentMetadata);
    final ImmutableSortedSet.Builder<SegmentStatusInCluster> publishedSegmentBuilder = ImmutableSortedSet.naturalOrder();

    Map<SegmentId, AvailableSegmentMetadata> availableSegmentMetadataMap;

    if (useSegmentMetadataCache) {
      availableSegmentMetadataMap = segmentMetadataCache.getSegmentMetadataSnapshot();

      for (SegmentStatusInCluster segmentStatusInCluster : allSegmentMetadata) {
        if (segmentStatusInCluster.isPublished()) {
          publishedSegmentBuilder.add(segmentStatusInCluster);
        }
      }
    } else {
      // build available segment metadata map by combining stuff from brokerServerView and numRows from data returned from coordinator
      availableSegmentMetadataMap = new HashMap<>();
      Map<SegmentId, ServerSelector> brokerSegmentMetadata = brokerServerView.getSegmentMetadata();
      // only look at watched ds, confirm if brokerServerView is also looking for watched ds
      for (SegmentStatusInCluster segmentStatusInCluster : allSegmentMetadata) {
        if (segmentStatusInCluster.isPublished()) {
          publishedSegmentBuilder.add(segmentStatusInCluster);
        }
        SegmentId segmentId = segmentStatusInCluster.getDataSegment().getId();
        if (!brokerSegmentMetadata.containsKey(segmentId)) {
          // log and count ignored segments
          continue;
        }
        ServerSelector serverSelector = brokerSegmentMetadata.get(segmentId);

        AvailableSegmentMetadata availableSegmentMetadata =
            AvailableSegmentMetadata.builder(
                                        segmentStatusInCluster.getDataSegment(),
                                        segmentStatusInCluster.isRealtime(),
                                        Sets.newHashSet(serverSelector.getAllServers()),
                                        null,
                                        segmentStatusInCluster.getNumRows() == null ? -1 : segmentStatusInCluster.getNumRows()
                                    )
                                    .build();

        availableSegmentMetadataMap.put(segmentId, availableSegmentMetadata);

      }
    }

    log.info("Logging Segment table view. availableSmMap [%s], published segments [%s]", availableSegmentMetadataMap, publishedSegmentBuilder);
    return new SystemSchema.SegmentsTable.SegmentTableView(availableSegmentMetadataMap, publishedSegmentBuilder.build());
  }

  protected DatasourceTable.PhysicalDatasourceMetadata getDatasource(String name)
  {
    return useSegmentMetadataCache ? segmentMetadataCache.getPhysicalDatasourceMetadata(name) : datasourceSchemaMap.get(name);
  }

  protected Set<String> getDatasourceNames()
  {
    return useSegmentMetadataCache ? segmentMetadataCache.getDatasourceNames() : datasourceSchemaMap.keySet();
  }

  // Note that coordinator must be up to get segments
  private JsonParserIterator<SegmentStatusInCluster> querySegmentMetadata(
      Set<String> watchedDataSources
  )
  {
    String query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus";
    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      log.debug(
          "filtering datasources in published segments based on broker's watchedDataSources[%s]", watchedDataSources);
      final StringBuilder sb = new StringBuilder();
      for (String ds : watchedDataSources) {
        sb.append("datasources=").append(ds).append("&");
      }
      sb.setLength(sb.length() - 1);
      query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&" + sb;
    }

    return SystemSchema.getThingsFromLeaderNode(
        query,
        new TypeReference<SegmentStatusInCluster>()
        {
        },
        druidLeaderClient,
        objectMapper
    );
  }

  private class PollTask implements Runnable
  {
    @Override
    public void run()
    {
      long delayMS = pollPeriodInMS;
      try {
        final long pollStartTime = System.nanoTime();
        if (isMetadataSegmentCacheEnabled) {
          pollSegmentMetadata();
        }
        if (!useSegmentMetadataCache) {
          pollDatasourceSchema();
        }
        final long pollEndTime = System.nanoTime();
        final long pollTimeNS = pollEndTime - pollStartTime;
        final long pollTimeMS = TimeUnit.NANOSECONDS.toMillis(pollTimeNS);
        delayMS = Math.max(pollPeriodInMS - pollTimeMS, 0);
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling Coordinator.").emit();
      }
      finally {
        if (!Thread.currentThread().isInterrupted()) {
          scheduledExec.schedule(new PollTask(), delayMS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }
}
