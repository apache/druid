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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class SystemSchema extends AbstractSchema
{
  public static final String NAME = "sys";
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SERVER_SEGMENTS_TABLE = "server_segments";
  private static final String TASKS_TABLE = "tasks";

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
      .add("payload", ValueType.STRING)
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
      .build();

  static final RowSignature SERVER_SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueType.STRING)
      .add("segment_id", ValueType.STRING)
      .build();

  static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ValueType.STRING)
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

  private final Map<String, Table> tableMap;

  @Inject
  public SystemSchema(
      final DruidSchema druidSchema,
      final TimelineServerView serverView,
      final AuthorizerMapper authorizerMapper,
      final @Coordinator DruidLeaderClient coordinatorDruidLeaderClient,
      final @IndexingService DruidLeaderClient overlordDruidLeaderClient,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
    SegmentsTable segmentsTable = new SegmentsTable(
        druidSchema,
        coordinatorDruidLeaderClient,
        jsonMapper,
        responseHandler,
        authorizerMapper
    );
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE, segmentsTable,
        SERVERS_TABLE, new ServersTable(serverView, authorizerMapper),
        SERVER_SEGMENTS_TABLE, new ServerSegmentsTable(serverView, authorizerMapper),
        TASKS_TABLE, new TasksTable(overlordDruidLeaderClient, jsonMapper, responseHandler, authorizerMapper)
    );
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  static class SegmentsTable extends AbstractTable implements ScannableTable
  {
    private final DruidSchema druidSchema;
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final BytesAccumulatingResponseHandler responseHandler;
    private final AuthorizerMapper authorizerMapper;

    public SegmentsTable(
        DruidSchema druidSchemna,
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler,
        AuthorizerMapper authorizerMapper
    )
    {
      this.druidSchema = druidSchemna;
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.responseHandler = responseHandler;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SEGMENTS_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      //get available segments from druidSchema
      final Map<DataSegment, SegmentMetadataHolder> availableSegmentMetadata = druidSchema.getSegmentMetadata();
      final Iterator<Entry<DataSegment, SegmentMetadataHolder>> availableSegmentEntries =
          availableSegmentMetadata.entrySet().iterator();

      // in memory map to store segment data from available segments
      final Map<SegmentId, PartialSegmentData> partialSegmentDataMap =
          Maps.newHashMapWithExpectedSize(druidSchema.getTotalSegments());
      for (SegmentMetadataHolder h : availableSegmentMetadata.values()) {
        PartialSegmentData partialSegmentData =
            new PartialSegmentData(h.isAvailable(), h.isRealtime(), h.getNumReplicas(h.getSegmentId()), h.getNumRows());
        partialSegmentDataMap.put(h.getSegmentId(), partialSegmentData);
      }

      //get published segments from coordinator
      final JsonParserIterator<DataSegment> metadataSegments = getMetadataSegments(
          druidLeaderClient,
          jsonMapper,
          responseHandler
      );

      final Set<SegmentId> segmentsAlreadySeen = new HashSet<>();

      final FluentIterable<Object[]> publishedSegments = FluentIterable
          .from(() -> getAuthorizedPublishedSegments(
              metadataSegments,
              root
          ))
          .transform((DataSegment val) -> {
            try {
              segmentsAlreadySeen.add(val.getId());
              final PartialSegmentData partialSegmentData = partialSegmentDataMap.get(val.getId());
              long numReplicas = 0L, numRows = 0L, isRealtime = 0L, isAvailable = 0L;
              if (partialSegmentData != null) {
                numReplicas = partialSegmentData.getNumReplicas();
                numRows = partialSegmentData.getNumRows();
                isAvailable = partialSegmentData.isAvailable();
                isRealtime = partialSegmentData.isRealtime();
              }
              return new Object[]{
                  val.getId(),
                  val.getDataSource(),
                  val.getInterval().getStart().toString(),
                  val.getInterval().getEnd().toString(),
                  val.getSize(),
                  val.getVersion(),
                  Long.valueOf(val.getShardSpec().getPartitionNum()),
                  numReplicas,
                  numRows,
                  1L, //is_published is true for published segments
                  isAvailable,
                  isRealtime,
                  jsonMapper.writeValueAsString(val)
              };
            }
            catch (JsonProcessingException e) {
              throw new RE(e, "Error getting segment payload for segment %s", val.getId());
            }
          });

      final FluentIterable<Object[]> availableSegments = FluentIterable
          .from(() -> getAuthorizedAvailableSegments(
              availableSegmentEntries,
              root
          ))
          .transform(val -> {
            try {
              if (segmentsAlreadySeen.contains(val.getKey().getId())) {
                return null;
              }
              final PartialSegmentData partialSegmentData = partialSegmentDataMap.get(val.getKey().getId());
              final long numReplicas = partialSegmentData == null ? 0L : partialSegmentData.getNumReplicas();
              return new Object[]{
                  val.getKey().getId(),
                  val.getKey().getDataSource(),
                  val.getKey().getInterval().getStart().toString(),
                  val.getKey().getInterval().getEnd().toString(),
                  val.getKey().getSize(),
                  val.getKey().getVersion(),
                  Long.valueOf(val.getKey().getShardSpec().getPartitionNum()),
                  numReplicas,
                  val.getValue().getNumRows(),
                  val.getValue().isPublished(),
                  val.getValue().isAvailable(),
                  val.getValue().isRealtime(),
                  jsonMapper.writeValueAsString(val.getKey())
              };
            }
            catch (JsonProcessingException e) {
              throw new RE(e, "Error getting segment payload for segment %s", val.getKey().getId());
            }
          });

      final Iterable<Object[]> allSegments = Iterables.unmodifiableIterable(
          Iterables.concat(publishedSegments, availableSegments)
      );

      return Linq4j.asEnumerable(allSegments).where(Objects::nonNull);

    }

    private Iterator<Entry<DataSegment, SegmentMetadataHolder>> getAuthorizedAvailableSegments(
        Iterator<Entry<DataSegment, SegmentMetadataHolder>> availableSegmentEntries,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult =
          (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);

      Function<Entry<DataSegment, SegmentMetadataHolder>, Iterable<ResourceAction>> raGenerator = segment -> Collections
          .singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getKey().getDataSource()));

      final Iterable<Entry<DataSegment, SegmentMetadataHolder>> authorizedSegments =
          AuthorizationUtils.filterAuthorizedResources(
              authenticationResult,
              () -> availableSegmentEntries,
              raGenerator,
              authorizerMapper
          );

      return authorizedSegments.iterator();
    }

    private CloseableIterator<DataSegment> getAuthorizedPublishedSegments(
        JsonParserIterator<DataSegment> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult =
          (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);

      Function<DataSegment, Iterable<ResourceAction>> raGenerator = segment -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSource()));

      final Iterable<DataSegment> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedSegments.iterator(), it);
    }

    private static class PartialSegmentData
    {
      private final long isAvailable;
      private final long isRealtime;
      private final long numReplicas;
      private final long numRows;

      public PartialSegmentData(
          final long isAvailable,
          final long isRealtime,
          final long numReplicas,
          final long numRows
      )

      {
        this.isAvailable = isAvailable;
        this.isRealtime = isRealtime;
        this.numReplicas = numReplicas;
        this.numRows = numRows;
      }

      public long isAvailable()
      {
        return isAvailable;
      }

      public long isRealtime()
      {
        return isRealtime;
      }

      public long getNumReplicas()
      {
        return numReplicas;
      }

      public long getNumRows()
      {
        return numRows;
      }
    }
  }

  // Note that coordinator must be up to get segments
  private static JsonParserIterator<DataSegment> getMetadataSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler
  )
  {

    Request request;
    try {
      request = coordinatorClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format("/druid/coordinator/v1/metadata/segments"),
          false
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    ListenableFuture<InputStream> future = coordinatorClient.goAsync(
        request,
        responseHandler
    );

    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<DataSegment>()
    {
    });
    return new JsonParserIterator<>(
        typeRef,
        future,
        request.getUrl().toString(),
        null,
        request.getUrl().getHost(),
        jsonMapper,
        responseHandler
    );
  }

  static class ServersTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;
    private final AuthorizerMapper authorizerMapper;

    public ServersTable(TimelineServerView serverView, AuthorizerMapper authorizerMapper)
    {
      this.serverView = serverView;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SERVERS_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final AuthenticationResult authenticationResult =
          (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);
      final Access access = AuthorizationUtils.authorizeAllResourceActions(
          authenticationResult,
          Collections.singletonList(new ResourceAction(new Resource("STATE", ResourceType.STATE), Action.READ)),
          authorizerMapper
      );
      if (!access.isAllowed()) {
        throw new ForbiddenException("Insufficient permission to view servers :" + access);
      }
      final FluentIterable<Object[]> results = FluentIterable
          .from(druidServers)
          .transform(val -> new Object[]{
              val.getHost(),
              extractHost(val.getHost()),
              (long) extractPort(val.getHostAndPort()),
              (long) extractPort(val.getHostAndTlsPort()),
              toStringOrNull(val.getType()),
              val.getTier(),
              val.getCurrSize(),
              val.getMaxSize()
          });
      return Linq4j.asEnumerable(results);
    }
  }

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
      return SERVER_SEGMENTS_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final int serverSegmentsTableSize = SERVER_SEGMENTS_SIGNATURE.getRowOrder().size();
      for (ImmutableDruidServer druidServer : druidServers) {
        for (DataSegment segment : druidServer.getSegments()) {
          Object[] row = new Object[serverSegmentsTableSize];
          row[0] = druidServer.getHost();
          row[1] = segment.getId();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  static class TasksTable extends AbstractTable implements ScannableTable
  {
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final BytesAccumulatingResponseHandler responseHandler;
    private final AuthorizerMapper authorizerMapper;

    public TasksTable(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler,
        AuthorizerMapper authorizerMapper
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.responseHandler = responseHandler;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return TASKS_SIGNATURE.getRelDataType(typeFactory);
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
              final String hostAndPort;

              if (task.getLocation().getHost() == null) {
                hostAndPort = null;
              } else {
                final int port;
                if (task.getLocation().getTlsPort() >= 0) {
                  port = task.getLocation().getTlsPort();
                } else {
                  port = task.getLocation().getPort();
                }

                hostAndPort = HostAndPort.fromParts(task.getLocation().getHost(), port).toString();
              }
              return new Object[]{
                  task.getId(),
                  task.getType(),
                  task.getDataSource(),
                  toStringOrNull(task.getCreatedTime()),
                  toStringOrNull(task.getQueueInsertionTime()),
                  toStringOrNull(task.getStatusCode()),
                  toStringOrNull(task.getRunnerStatusCode()),
                  task.getDuration() == null ? 0L : task.getDuration(),
                  hostAndPort,
                  task.getLocation().getHost(),
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

      return new TasksEnumerable(getTasks(druidLeaderClient, jsonMapper, responseHandler));
    }

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(
        JsonParserIterator<TaskStatusPlus> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult =
          (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);

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
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler
  )
  {
    Request request;
    try {
      request = indexingServiceClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format("/druid/indexer/v1/tasks"),
          false
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    ListenableFuture<InputStream> future = indexingServiceClient.goAsync(
        request,
        responseHandler
    );

    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<TaskStatusPlus>()
    {
    });
    return new JsonParserIterator<>(
        typeRef,
        future,
        request.getUrl().toString(),
        null,
        request.getUrl().getHost(),
        jsonMapper,
        responseHandler
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
  private static String extractHost(@Nullable final String hostAndPort)
  {
    if (hostAndPort == null) {
      return null;
    }

    return HostAndPort.fromString(hostAndPort).getHostText();
  }

  private static int extractPort(@Nullable final String hostAndPort)
  {
    if (hostAndPort == null) {
      return -1;
    }

    return HostAndPort.fromString(hostAndPort).getPortOrDefault(-1);
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }

    return object.toString();
  }
}
