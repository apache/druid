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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.InventoryView;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.SegmentsTableConfig;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.base.AbstractDateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class SystemSchema extends AbstractSchema
{
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SERVER_SEGMENTS_TABLE = "server_segments";
  private static final String TASKS_TABLE = "tasks";
  private static final String SUPERVISOR_TABLE = "supervisors";

  private static final Function<DataSegment, Iterable<ResourceAction>> SEGMENT_RA_GENERATOR =
      segment -> Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
          segment.getDataSource())
      );

  static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ValueType.STRING)
      .add("datasource", ValueType.STRING)
      .add("start", ValueType.STRING)
      .add("end", ValueType.STRING)
      .add("size", ValueType.LONG)
      .add("version", ValueType.STRING)
      .add("partition_num", ValueType.LONG)
      .add("num_replicas", ValueType.LONG)
      .add("num_rows", ValueType.LONG)
      .add("is_published", ValueType.LONG)
      .add("is_available", ValueType.LONG)
      .add("is_realtime", ValueType.LONG)
      .add("is_overshadowed", ValueType.LONG)
      .add("shard_spec", ValueType.STRING)
      .add("dimensions", ValueType.STRING)
      .add("metrics", ValueType.STRING)
      .add("last_compaction_state", ValueType.STRING)
      .build();

  static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueType.STRING)
      .add("host", ValueType.STRING)
      .add("plaintext_port", ValueType.LONG)
      .add("tls_port", ValueType.LONG)
      .add("server_type", ValueType.STRING)
      .add("tier", ValueType.STRING)
      .add("curr_size", ValueType.LONG)
      .add("max_size", ValueType.LONG)
      .add("is_leader", ValueType.LONG)
      .build();

  static final RowSignature SERVER_SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueType.STRING)
      .add("segment_id", ValueType.STRING)
      .build();

  static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ValueType.STRING)
      .add("group_id", ValueType.STRING)
      .add("type", ValueType.STRING)
      .add("datasource", ValueType.STRING)
      .add("created_time", ValueType.STRING)
      .add("queue_insertion_time", ValueType.STRING)
      .add("status", ValueType.STRING)
      .add("runner_status", ValueType.STRING)
      .add("duration", ValueType.LONG)
      .add("location", ValueType.STRING)
      .add("host", ValueType.STRING)
      .add("plaintext_port", ValueType.LONG)
      .add("tls_port", ValueType.LONG)
      .add("error_msg", ValueType.STRING)
      .build();

  static final RowSignature SUPERVISOR_SIGNATURE = RowSignature
      .builder()
      .add("supervisor_id", ValueType.STRING)
      .add("state", ValueType.STRING)
      .add("detailed_state", ValueType.STRING)
      .add("healthy", ValueType.LONG)
      .add("type", ValueType.STRING)
      .add("source", ValueType.STRING)
      .add("suspended", ValueType.LONG)
      .add("spec", ValueType.STRING)
      .build();

  private final Map<String, Table> tableMap;

  @Inject
  public SystemSchema(
      final SegmentsTableConfig segmentsTableConfig,
      final DruidSchema druidSchema,
      final MetadataSegmentView metadataView,
      final TimelineServerView serverView,
      final InventoryView serverInventoryView,
      final AuthorizerMapper authorizerMapper,
      final @Coordinator DruidLeaderClient coordinatorDruidLeaderClient,
      final @IndexingService DruidLeaderClient overlordDruidLeaderClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE, new SegmentsTable(segmentsTableConfig, druidSchema, metadataView, jsonMapper, authorizerMapper),
        SERVERS_TABLE, new ServersTable(
            druidNodeDiscoveryProvider,
            serverInventoryView,
            authorizerMapper,
            overlordDruidLeaderClient,
            coordinatorDruidLeaderClient
        ),
        SERVER_SEGMENTS_TABLE, new ServerSegmentsTable(serverView, authorizerMapper),
        TASKS_TABLE, new TasksTable(overlordDruidLeaderClient, jsonMapper, authorizerMapper),
        SUPERVISOR_TABLE, new SupervisorsTable(overlordDruidLeaderClient, jsonMapper, authorizerMapper)
    );
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  /**
   * This table contains row per segment from metadata store as well as served segments.
   */
  static class SegmentsTable extends AbstractTable implements ScannableTable
  {
    private final SegmentsTableConfig config;
    private final DruidSchema druidSchema;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final MetadataSegmentView metadataView;

    SegmentsTable(
        SegmentsTableConfig config,
        DruidSchema druidSchemna,
        MetadataSegmentView metadataView,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper
    )
    {
      this.config = config;
      this.druidSchema = druidSchemna;
      this.metadataView = metadataView;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SEGMENTS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      if (metadataView.isCacheEnabled() && !config.isForceHashBasedMerge()) {
        return mergeSortedSegments(root);
      } else {
        return hashBasedMergeSegments(root);
      }
    }

    /**
     * When {@link PlannerConfig#isMetadataSegmentCacheEnable()} is set, the published segments and available segments
     * are sorted in the same order to facilitate merging them using the merge sorted algorithm.
     */
    private Enumerable<Object[]> mergeSortedSegments(DataContext root)
    {
      final PeekingIterator<SegmentsTableRow> sortedPublishedSegments = Iterators.peekingIterator(
          FluentIterable
              .from(() -> getAuthorizedSegments(
                  metadataView.getSortedPublishedSegments().iterator(),
                  root
              ))
              .iterator()
      );
      final PeekingIterator<SegmentsTableRow> sortedAvailableSegments = Iterators.peekingIterator(
          FluentIterable
              .from(() -> getAuthorizedAvailableSegments(
                  druidSchema.getSortedAvailableSegmentMetadataIterator(),
                  root
              ))
              .transform(entry -> SegmentsTableRow.from(entry.getValue()))
              .iterator()
      );

      // We use a specialized iterator to merge sorted iterators for performance
      // instead of Iterables.mergeSorted() and CombiningIterator.
      final Iterator<SegmentsTableRow> combiningIterator = new Iterator<SegmentsTableRow>()
      {
        @Override
        public boolean hasNext()
        {
          return sortedAvailableSegments.hasNext() || sortedPublishedSegments.hasNext();
        }

        @Override
        public SegmentsTableRow next()
        {
          if (sortedAvailableSegments.hasNext() && sortedPublishedSegments.hasNext()) {
            SegmentsTableRow r1 = sortedAvailableSegments.peek();
            SegmentsTableRow r2 = sortedPublishedSegments.peek();

            int compare = MetadataSegmentView.SEGMENT_ORDER.compare(r1, r2);
            if (compare > 0) {
              return sortedPublishedSegments.next();
            } else if (compare < 0) {
              return sortedAvailableSegments.next();
            } else {
              return sortedPublishedSegments.next().merge(sortedAvailableSegments.next());
            }
          } else if (sortedAvailableSegments.hasNext()) {
            return sortedAvailableSegments.next();
          } else if (sortedPublishedSegments.hasNext()) {
            return sortedPublishedSegments.next();
          } else {
            throw new NoSuchElementException();
          }
        }
      };
      return Linq4j.asEnumerable(transformRows(jsonMapper, () -> combiningIterator));
    }

    /**
     * This method uses a hash-based merge algorithm to merge the published segments and available segments.
     */
    private Enumerable<Object[]> hashBasedMergeSegments(DataContext root)
    {
      final Set<SegmentId> segmentsAlreadySeen = Sets.newHashSetWithExpectedSize(druidSchema.getTotalSegments());
      // We create a snapshot of available segments in druidSchema. This is because the available segments
      // are stored in a ConcurrentHashMap and the direct lookup on ConcurrentHashMap is much more expensive
      // than the lookup on HashMap.
      final Map<SegmentId, AvailableSegmentMetadata> availableSegments = Maps.newHashMapWithExpectedSize(
          druidSchema.getTotalSegments()
      );
      final Iterator<Entry<SegmentId, AvailableSegmentMetadata>> iterator = getAuthorizedAvailableSegments(
          druidSchema.getAvailableSegmentMetadataIterator(),
          root
      );
      while (iterator.hasNext()) {
        Entry<SegmentId, AvailableSegmentMetadata> entry = iterator.next();
        availableSegments.put(entry.getKey(), entry.getValue());
      }

      final FluentIterable<SegmentsTableRow> publishedSegments = FluentIterable
          .from(() -> getAuthorizedSegments(metadataView.getPublishedSegmentsIterator(), root))
          .transform(row -> {
            final SegmentId segmentId = row.getSegmentId();
            segmentsAlreadySeen.add(segmentId);
            final AvailableSegmentMetadata metadata = availableSegments.get(segmentId);
            if (metadata != null) {
              return row.merge(metadata);
            } else {
              return row;
            }
          });

      final FluentIterable<SegmentsTableRow> unpublishedAvailableSegments = FluentIterable
          .from(availableSegments.entrySet())
          .filter(entry -> !segmentsAlreadySeen.contains(entry.getKey()))
          .transform(val -> SegmentsTableRow.from(val.getValue()));

      return Linq4j.asEnumerable(
          transformRows(jsonMapper, Iterables.concat(publishedSegments, unpublishedAvailableSegments))
      );
    }

    private Iterable<Object[]> transformRows(
        ObjectMapper jsonMapper,
        Iterable<SegmentsTableRow> rows
    )
    {
      // Serializing objects to string is expensive, so we use caches to avoid serializing the same object
      // over and over again.
      CaffeineObjectStringCache<DateTime> timestampStringCache = createCache(AbstractDateTime::toString);
      CaffeineObjectStringCache<List<String>> dimensionsStringCache = createCache(
          dimensions -> SegmentsTableRow.toJsonString(jsonMapper, dimensions)
      );
      CaffeineObjectStringCache<List<String>> metricsStringCache = createCache(
          metrics -> SegmentsTableRow.toJsonString(jsonMapper, metrics)
      );
      CaffeineObjectStringCache<CompactionState> compactionStateStringCache = createCache(
          state -> SegmentsTableRow.toJsonString(jsonMapper, state)
      );

      return FluentIterable
          .from(rows)
          .transform(row -> row.toObjectArray(
              jsonMapper,
              timestampStringCache,
              dimensionsStringCache,
              metricsStringCache,
              compactionStateStringCache
          ));
    }

    private <K> CaffeineObjectStringCache<K> createCache(java.util.function.Function<K, String> serializer)
    {
      return new CaffeineObjectStringCache<>(
          serializer,
          config.getStringCacheSizeBytes()
      );
    }

    private Iterator<Entry<SegmentId, AvailableSegmentMetadata>> getAuthorizedAvailableSegments(
        Iterator<Entry<SegmentId, AvailableSegmentMetadata>> availableSegmentEntries,
        DataContext root
    )
    {
      return filterAuthorized(
          root,
          availableSegmentEntries,
          segment ->
              Collections.singletonList(
                  AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getKey().getDataSource())
              )
      );
    }

    private Iterator<SegmentsTableRow> getAuthorizedSegments(
        Iterator<SegmentsTableRow> segmentRows,
        DataContext root
    )
    {
      return filterAuthorized(
          root,
          segmentRows,
          segment ->
              Collections.singletonList(
                  AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getSegmentId().getDataSource())
              )
      );
    }

    private <T> Iterator<T> filterAuthorized(
        DataContext root,
        Iterator<T> iterator,
        Function<T, Iterable<ResourceAction>> resourceActionGenerator
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      final Iterable<T> authorizedSegments =
          AuthorizationUtils.filterAuthorizedResources(
              authenticationResult,
              () -> iterator,
              resourceActionGenerator,
              authorizerMapper
          );

      return authorizedSegments.iterator();
    }
  }

  /**
   * This table contains row per server. It contains all the discovered servers in Druid cluster.
   * Some columns like tier and size are only applicable to historical nodes which contain segments.
   */
  static class ServersTable extends AbstractTable implements ScannableTable
  {
    // This is used for maxSize and currentSize when they are unknown.
    // The unknown size doesn't have to be 0, it's better to be null.
    // However, this table is returning 0 for them for some reason and we keep the behavior for backwards compatibility.
    // Maybe we can remove this and return nulls instead when we remove the bindable query path which is currently
    // used to query system tables.
    private static final long UNKNOWN_SIZE = 0L;

    private final AuthorizerMapper authorizerMapper;
    private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
    private final InventoryView serverInventoryView;
    private final DruidLeaderClient overlordLeaderClient;
    private final DruidLeaderClient coordinatorLeaderClient;

    public ServersTable(
        DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        InventoryView serverInventoryView,
        AuthorizerMapper authorizerMapper,
        DruidLeaderClient overlordLeaderClient,
        DruidLeaderClient coordinatorLeaderClient
    )
    {
      this.authorizerMapper = authorizerMapper;
      this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
      this.serverInventoryView = serverInventoryView;
      this.overlordLeaderClient = overlordLeaderClient;
      this.coordinatorLeaderClient = coordinatorLeaderClient;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SERVERS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final Iterator<DiscoveryDruidNode> druidServers = getDruidServers(druidNodeDiscoveryProvider);
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      String tmpCoordinatorLeader = "";
      String tmpOverlordLeader = "";
      try {
        tmpCoordinatorLeader = coordinatorLeaderClient.findCurrentLeader();
        tmpOverlordLeader = overlordLeaderClient.findCurrentLeader();
      }
      catch (ISE ignored) {
        // no reason to kill the results if something is sad and there are no leaders
      }
      final String coordinatorLeader = tmpCoordinatorLeader;
      final String overlordLeader = tmpOverlordLeader;

      final FluentIterable<Object[]> results = FluentIterable
          .from(() -> druidServers)
          .transform((DiscoveryDruidNode discoveryDruidNode) -> {
            //noinspection ConstantConditions
            final boolean isDiscoverableDataServer = isDiscoverableDataServer(discoveryDruidNode);
            final NodeRole serverRole = discoveryDruidNode.getNodeRole();

            if (isDiscoverableDataServer) {
              final DruidServer druidServer = serverInventoryView.getInventoryValue(
                  discoveryDruidNode.getDruidNode().getHostAndPortToUse()
              );
              if (druidServer != null || NodeRole.HISTORICAL.equals(serverRole)) {
                // Build a row for the data server if that server is in the server view, or the node type is historical.
                // The historicals are usually supposed to be found in the server view. If some historicals are
                // missing, it could mean that there are some problems in them to announce themselves. We just fill
                // their status with nulls in this case.
                return buildRowForDiscoverableDataServer(discoveryDruidNode, druidServer);
              } else {
                return buildRowForNonDataServer(discoveryDruidNode);
              }
            } else if (NodeRole.COORDINATOR.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  coordinatorLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else if (NodeRole.OVERLORD.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  overlordLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else {
              return buildRowForNonDataServer(discoveryDruidNode);
            }
          });
      return Linq4j.asEnumerable(results);
    }


    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private static Object[] buildRowForNonDataServer(DiscoveryDruidNode discoveryDruidNode)
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          NullHandling.defaultLongValue()
      };
    }

    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private static Object[] buildRowForNonDataServerWithLeadership(DiscoveryDruidNode discoveryDruidNode, boolean isLeader)
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          isLeader ? 1L : 0L
      };
    }

    /**
     * Returns a row for discoverable data server. This method prefers the information from
     * {@code serverFromInventoryView} if available which is the current state of the server. Otherwise, it
     * will get the information from {@code discoveryDruidNode} which has only static configurations.
     */
    private static Object[] buildRowForDiscoverableDataServer(
        DiscoveryDruidNode discoveryDruidNode,
        @Nullable DruidServer serverFromInventoryView
    )
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      final DruidServer druidServerToUse = serverFromInventoryView == null
                                           ? toDruidServer(discoveryDruidNode)
                                           : serverFromInventoryView;
      final long currentSize;
      if (serverFromInventoryView == null) {
        // If server is missing in serverInventoryView, the currentSize should be unknown
        currentSize = UNKNOWN_SIZE;
      } else {
        currentSize = serverFromInventoryView.getCurrSize();
      }
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          druidServerToUse.getTier(),
          currentSize,
          druidServerToUse.getMaxSize(),
          NullHandling.defaultLongValue()
      };
    }

    private static boolean isDiscoverableDataServer(DiscoveryDruidNode druidNode)
    {
      final DruidService druidService = druidNode.getServices().get(DataNodeService.DISCOVERY_SERVICE_KEY);
      if (druidService == null) {
        return false;
      }
      final DataNodeService dataNodeService = (DataNodeService) druidService;
      return dataNodeService.isDiscoverable();
    }

    private static DruidServer toDruidServer(DiscoveryDruidNode discoveryDruidNode)
    {
      if (isDiscoverableDataServer(discoveryDruidNode)) {
        final DruidNode druidNode = discoveryDruidNode.getDruidNode();
        final DataNodeService dataNodeService = (DataNodeService) discoveryDruidNode
            .getServices()
            .get(DataNodeService.DISCOVERY_SERVICE_KEY);
        return new DruidServer(
            druidNode.getHostAndPortToUse(),
            druidNode.getHostAndPort(),
            druidNode.getHostAndTlsPort(),
            dataNodeService.getMaxSize(),
            dataNodeService.getType(),
            dataNodeService.getTier(),
            dataNodeService.getPriority()
        );
      } else {
        throw new ISE("[%s] is not a discoverable data server", discoveryDruidNode);
      }
    }

    private static Iterator<DiscoveryDruidNode> getDruidServers(DruidNodeDiscoveryProvider druidNodeDiscoveryProvider)
    {
      return Arrays.stream(NodeRole.values())
                   .flatMap(nodeRole -> druidNodeDiscoveryProvider.getForNodeRole(nodeRole).getAllNodes().stream())
                   .collect(Collectors.toList())
                   .iterator();
    }
  }

  /**
   * This table contains row per segment per server.
   */
  static class ServerSegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;
    final AuthorizerMapper authorizerMapper;

    public ServerSegmentsTable(TimelineServerView serverView, AuthorizerMapper authorizerMapper)
    {
      this.serverView = serverView;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SERVER_SEGMENTS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final int serverSegmentsTableSize = SERVER_SEGMENTS_SIGNATURE.size();
      for (ImmutableDruidServer druidServer : druidServers) {
        final Iterable<DataSegment> authorizedServerSegments = AuthorizationUtils.filterAuthorizedResources(
            authenticationResult,
            druidServer.iterateAllSegments(),
            SEGMENT_RA_GENERATOR,
            authorizerMapper
        );

        for (DataSegment segment : authorizedServerSegments) {
          Object[] row = new Object[serverSegmentsTableSize];
          row[0] = druidServer.getHost();
          row[1] = segment.getId();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * This table contains row per task.
   */
  static class TasksTable extends AbstractTable implements ScannableTable
  {
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;

    public TasksTable(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(TASKS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class TasksEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<TaskStatusPlus> it;

        public TasksEnumerable(JsonParserIterator<TaskStatusPlus> tasks)
        {
          this.it = getAuthorizedTasks(tasks, root);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<Object[]>()
          {
            @Override
            public Object[] current()
            {
              final TaskStatusPlus task = it.next();
              @Nullable final String host = task.getLocation().getHost();
              @Nullable final String hostAndPort;

              if (host == null) {
                hostAndPort = null;
              } else {
                final int port;
                if (task.getLocation().getTlsPort() >= 0) {
                  port = task.getLocation().getTlsPort();
                } else {
                  port = task.getLocation().getPort();
                }

                hostAndPort = HostAndPort.fromParts(host, port).toString();
              }
              return new Object[]{
                  task.getId(),
                  task.getGroupId(),
                  task.getType(),
                  task.getDataSource(),
                  toStringOrNull(task.getCreatedTime()),
                  toStringOrNull(task.getQueueInsertionTime()),
                  toStringOrNull(task.getStatusCode()),
                  toStringOrNull(task.getRunnerStatusCode()),
                  task.getDuration() == null ? 0L : task.getDuration(),
                  hostAndPort,
                  host,
                  (long) task.getLocation().getPort(),
                  (long) task.getLocation().getTlsPort(),
                  task.getErrorMsg()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new TasksEnumerable(getTasks(druidLeaderClient, jsonMapper));
    }

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(
        JsonParserIterator<TaskStatusPlus> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<TaskStatusPlus, Iterable<ResourceAction>> raGenerator = task -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(task.getDataSource()));

      final Iterable<TaskStatusPlus> authorizedTasks = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedTasks.iterator(), it);
    }

  }

  //Note that overlord must be up to get tasks
  private static JsonParserIterator<TaskStatusPlus> getTasks(
      DruidLeaderClient indexingServiceClient,
      ObjectMapper jsonMapper
  )
  {
    return getThingsFromLeaderNode(
        "/druid/indexer/v1/tasks",
        new TypeReference<TaskStatusPlus>()
        {
        },
        indexingServiceClient,
        jsonMapper
    );
  }

  /**
   * This table contains a row per supervisor task.
   */
  static class SupervisorsTable extends AbstractTable implements ScannableTable
  {
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;

    public SupervisorsTable(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SUPERVISOR_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class SupervisorsEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<SupervisorStatus> it;

        public SupervisorsEnumerable(JsonParserIterator<SupervisorStatus> tasks)
        {
          this.it = getAuthorizedSupervisors(tasks, root);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<Object[]>()
          {
            @Override
            public Object[] current()
            {
              final SupervisorStatus supervisor = it.next();
              return new Object[]{
                  supervisor.getId(),
                  supervisor.getState(),
                  supervisor.getDetailedState(),
                  supervisor.isHealthy() ? 1L : 0L,
                  supervisor.getType(),
                  supervisor.getSource(),
                  supervisor.isSuspended() ? 1L : 0L,
                  supervisor.getSpecString()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new SupervisorsEnumerable(getSupervisors(druidLeaderClient, jsonMapper));
    }

    private CloseableIterator<SupervisorStatus> getAuthorizedSupervisors(
        JsonParserIterator<SupervisorStatus> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<SupervisorStatus, Iterable<ResourceAction>> raGenerator = supervisor -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(supervisor.getSource()));

      final Iterable<SupervisorStatus> authorizedSupervisors = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedSupervisors.iterator(), it);
    }
  }

  // Note that overlord must be up to get supervisor tasks, otherwise queries to sys.supervisors table
  // will fail with internal server error (HTTP 500)
  private static JsonParserIterator<SupervisorStatus> getSupervisors(
      DruidLeaderClient indexingServiceClient,
      ObjectMapper jsonMapper
  )
  {
    return getThingsFromLeaderNode(
        "/druid/indexer/v1/supervisor?system",
        new TypeReference<SupervisorStatus>()
        {
        },
        indexingServiceClient,
        jsonMapper
    );
  }

  public static <T> JsonParserIterator<T> getThingsFromLeaderNode(
      String query,
      TypeReference<T> typeRef,
      DruidLeaderClient leaderClient,
      ObjectMapper jsonMapper
  )
  {
    Request request;
    InputStreamFullResponseHolder responseHolder;
    try {
      request = leaderClient.makeRequest(
          HttpMethod.GET,
          query
      );

      responseHolder = leaderClient.go(
          request,
          new InputStreamFullResponseHandler()
      );

      if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
        throw new RE(
            "Failed to talk to leader node at [%s]. Error code[%d], description[%s].",
            query,
            responseHolder.getStatus().getCode(),
            responseHolder.getStatus().getReasonPhrase()
        );
      }
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    final JavaType javaType = jsonMapper.getTypeFactory().constructType(typeRef);
    return new JsonParserIterator<>(
        javaType,
        Futures.immediateFuture(responseHolder.getContent()),
        request.getUrl().toString(),
        null,
        request.getUrl().getHost(),
        jsonMapper
    );
  }

  private static <T> CloseableIterator<T> wrap(Iterator<T> iterator, JsonParserIterator<T> it)
  {
    return new CloseableIterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          try {
            it.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return hasNext;
      }

      @Override
      public T next()
      {
        return iterator.next();
      }

      @Override
      public void close() throws IOException
      {
        it.close();
      }
    };
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }

    return object.toString();
  }

  /**
   * Checks if an authenticated user has the STATE READ permissions needed to view server information.
   */
  private static void checkStateReadAccessForServers(
      AuthenticationResult authenticationResult,
      AuthorizerMapper authorizerMapper
  )
  {
    final Access stateAccess = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!stateAccess.isAllowed()) {
      throw new ForbiddenException("Insufficient permission to view servers : " + stateAccess);
    }
  }
}
