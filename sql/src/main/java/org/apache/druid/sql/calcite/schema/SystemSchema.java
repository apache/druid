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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;

public class SystemSchema extends AbstractSchema
{
  private static final Logger log = new Logger(SystemSchema.class);

  public static final String NAME = "sys";
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SEGMENT_SERVERS_TABLE = "segment_servers";
  private static final String TASKS_TABLE = "tasks";
  private static final int SEGMENT_SERVERS_TABLE_SIZE;

  private static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ValueType.STRING)
      .add("datasource", ValueType.STRING)
      .add("start", ValueType.STRING)
      .add("end", ValueType.STRING)
      .add("size", ValueType.LONG)
      .add("version", ValueType.STRING)
      .add("partition_num", ValueType.STRING)
      .add("num_replicas", ValueType.LONG)
      .add("num_rows", ValueType.LONG)
      .add("is_published", ValueType.LONG)
      .add("is_available", ValueType.LONG)
      .add("is_realtime", ValueType.LONG)
      .add("payload", ValueType.STRING)
      .build();

  private static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueType.STRING)
      .add("scheme", ValueType.STRING)
      .add("server_type", ValueType.STRING)
      .add("tier", ValueType.STRING)
      .add("curr_size", ValueType.LONG)
      .add("max_size", ValueType.LONG)
      .build();

  private static final RowSignature SERVERSEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueType.STRING)
      .add("segment_id", ValueType.STRING)
      .build();

  private static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ValueType.STRING)
      .add("type", ValueType.STRING)
      .add("datasource", ValueType.STRING)
      .add("created_time", ValueType.STRING)
      .add("queue_insertion_time", ValueType.STRING)
      .add("status", ValueType.STRING)
      .add("runner_status", ValueType.STRING)
      .add("duration", ValueType.STRING)
      .add("location", ValueType.STRING)
      .add("error_msg", ValueType.STRING)
      .build();

  private final Map<String, Table> tableMap;

  static {
    SEGMENT_SERVERS_TABLE_SIZE = SERVERSEGMENTS_SIGNATURE.getRowOrder().size();
  }

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
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE, new SegmentsTable(druidSchema, coordinatorDruidLeaderClient, jsonMapper),
        SERVERS_TABLE, new ServersTable(serverView),
        SEGMENT_SERVERS_TABLE, new ServerSegmentsTable(serverView),
        TASKS_TABLE, new TasksTable(overlordDruidLeaderClient, jsonMapper, new BytesAccumulatingResponseHandler())
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

    public SegmentsTable(
        DruidSchema druidSchemna,
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper
    )
    {
      this.druidSchema = druidSchemna;
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
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
      Map<String, ConcurrentSkipListMap<DataSegment, SegmentMetadataHolder>> getSegmentMetadataInfo = druidSchema.getSegmentMetadataInfo();
      final Map<DataSegment, SegmentMetadataHolder> availableSegmentMetadata = new HashMap<>();
      for (ConcurrentSkipListMap<DataSegment, SegmentMetadataHolder> val : getSegmentMetadataInfo.values()) {
        availableSegmentMetadata.putAll(val);
      }
      final Iterator<Entry<DataSegment, SegmentMetadataHolder>> availableSegmentEntries = availableSegmentMetadata.entrySet()
                                                                                                                  .iterator();

      //get published segments from coordinator
      final JsonParserIterator<DataSegment> metadataSegments = getMetadataSegments(
          druidLeaderClient,
          jsonMapper
      );

      Set<String> availableSegmentIds = new HashSet<>();
      final FluentIterable<Object[]> availableSegments = FluentIterable
          .from(() -> availableSegmentEntries)
          .transform(val -> {
            try {
              if (!availableSegmentIds.contains(val.getKey().getIdentifier())) {
                availableSegmentIds.add(val.getKey().getIdentifier());
              }
              return new Object[]{
                  val.getKey().getIdentifier(),
                  val.getKey().getDataSource(),
                  val.getKey().getInterval().getStart(),
                  val.getKey().getInterval().getEnd(),
                  val.getKey().getSize(),
                  val.getKey().getVersion(),
                  val.getKey().getShardSpec().getPartitionNum(),
                  val.getValue().getNumReplicas(),
                  val.getValue().getNumRows(),
                  val.getValue().isPublished(),
                  val.getValue().isAvailable(),
                  val.getValue().isRealtime(),
                  jsonMapper.writeValueAsString(val.getKey())
              };
            }
            catch (JsonProcessingException e) {
              log.error(e, "Error getting segment payload for segment %s", val.getKey().getIdentifier());
              throw new RuntimeException(e);
            }
          });

      final FluentIterable<Object[]> publishedSegments = FluentIterable
          .from(() -> metadataSegments)
          .transform(val -> {
            try {
              if (availableSegmentIds.contains(val.getIdentifier())) {
                return null;
              }
              return new Object[]{
                  val.getIdentifier(),
                  val.getDataSource(),
                  val.getInterval().getStart(),
                  val.getInterval().getEnd(),
                  val.getSize(),
                  val.getVersion(),
                  val.getShardSpec().getPartitionNum(),
                  0,
                  -1,
                  1,
                  0,
                  0,
                  jsonMapper.writeValueAsString(val)
              };
            }
            catch (JsonProcessingException e) {
              log.error(e, "Error getting segment payload for segment %s", val.getIdentifier());
              throw new RuntimeException(e);
            }
          });

      Iterable<Object[]> allSegments = Iterables.unmodifiableIterable(
          Iterables.concat(availableSegments, publishedSegments));

      return Linq4j.asEnumerable(allSegments).where(t -> t != null);

    }

    // Note that coordinator must be up to get segments
    JsonParserIterator<DataSegment> getMetadataSegments(
        DruidLeaderClient coordinatorClient,
        ObjectMapper jsonMapper
    )
    {

      Request request;
      try {
        request = coordinatorClient.makeRequest(
            HttpMethod.GET,
            StringUtils.format("/druid/coordinator/v1/metadata/segments")
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
      ListenableFuture<InputStream> future = coordinatorClient.goStream(
          request,
          responseHandler
      );
      try {
        future.get();
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      if (responseHandler.status != HttpServletResponse.SC_OK) {
        throw new ISE(
            "Error while fetching metadata segments status[%s] description[%s]",
            responseHandler.status,
            responseHandler.description
        );
      }
      final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<DataSegment>()
      {
      });
      JsonParserIterator<DataSegment> iterator = new JsonParserIterator<>(
          typeRef,
          future,
          request.getUrl().toString(),
          null,
          request.getUrl().getHost(),
          jsonMapper
      );
      return iterator;
    }
  }

  static class ServersTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;

    public ServersTable(TimelineServerView serverView)
    {
      this.serverView = serverView;
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
      Map<String, QueryableDruidServer> serverViewClients = serverView.getClients();
      final FluentIterable<Object[]> results = FluentIterable
          .from(serverViewClients.values())
          .transform(val -> new Object[]{
              val.getServer().getHost(),
              val.getServer().getScheme(),
              val.getServer().getType(),
              val.getServer().getTier(),
              val.getServer().getCurrSize(),
              val.getServer().getMaxSize()
          });
      return Linq4j.asEnumerable(results);
    }
  }

  private static class ServerSegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;

    public ServerSegmentsTable(TimelineServerView serverView)
    {
      this.serverView = serverView;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SERVERSEGMENTS_SIGNATURE.getRelDataType(typeFactory);
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
      final Map<String, QueryableDruidServer> serverViewClients = serverView.getClients();
      for (QueryableDruidServer queryableDruidServer : serverViewClients.values()) {
        final DruidServer druidServer = queryableDruidServer.getServer();
        final Map<String, DataSegment> segmentMap = druidServer.getSegments();
        for (DataSegment segment : segmentMap.values()) {
          Object[] row = new Object[SEGMENT_SERVERS_TABLE_SIZE];
          row[0] = druidServer.getHost();
          row[1] = segment.getIdentifier();
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

    public TasksTable(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.responseHandler = responseHandler;
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
        private final JsonParserIterator<TaskStatusPlus> it;

        public TasksEnumerable(JsonParserIterator<TaskStatusPlus> tasks)
        {
          this.it = tasks;
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
              TaskStatusPlus task = it.next();
              return new Object[]{task.getId(), task.getType(),
                                  task.getDataSource(),
                                  task.getCreatedTime(),
                                  task.getQueueInsertionTime(),
                                  task.getState(),
                                  task.getRunnerTaskState(),
                                  task.getDuration(),
                                  task.getLocation() != null
                                  ? task.getLocation().getHost() + ":" + (task.getLocation().getTlsPort()
                                                                          == -1
                                                                          ? task.getLocation()
                                                                                .getPort()
                                                                          : task.getLocation().getTlsPort())
                                  : null,
                                  task.getErrorMsg()};
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

    //Note that overlord must be up to get tasks
    protected JsonParserIterator<TaskStatusPlus> getTasks(
        DruidLeaderClient indexingServiceClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler
    )
    {

      Request request;
      try {
        request = indexingServiceClient.makeRequest(
            HttpMethod.GET,
            StringUtils.format("/druid/indexer/v1/tasks")
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      ListenableFuture<InputStream> future = indexingServiceClient.goStream(
          request,
          responseHandler
      );
      try {
        future.get();
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      if (responseHandler.status != HttpServletResponse.SC_OK) {
        throw new ISE(
            "Error while fetching tasks status[%s] description[%s]",
            responseHandler.status,
            responseHandler.description
        );
      }
      final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<TaskStatusPlus>()
      {
      });
      JsonParserIterator<TaskStatusPlus> iterator = new JsonParserIterator<>(
          typeRef,
          future,
          request.getUrl().toString(),
          null,
          request.getUrl().getHost(),
          jsonMapper
      );
      return iterator;
    }
  }

  static class BytesAccumulatingResponseHandler extends InputStreamResponseHandler
  {
    protected int status;
    private String description;

    @Override
    public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
    {
      status = response.getStatus().getCode();
      description = response.getStatus().getReasonPhrase();
      return ClientResponse.unfinished(super.handleResponse(response, trafficCop).getObj());
    }
  }
}
