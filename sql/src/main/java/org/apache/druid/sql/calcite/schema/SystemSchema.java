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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SystemSchema extends AbstractSchema
{
  private static final Logger log = new Logger(SystemSchema.class);

  public static final String NAME = "sys";
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SEGMENT_SERVERS_TABLE = "segment_servers";
  private static final String TASKS_TABLE = "tasks";
  private static final int SEGMENTS_TABLE_SIZE;
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
    SEGMENTS_TABLE_SIZE = SEGMENTS_SIGNATURE.getRowOrder().size();
    SEGMENT_SERVERS_TABLE_SIZE = SERVERSEGMENTS_SIGNATURE.getRowOrder().size();
  }

  @Inject
  public SystemSchema(
      final TimelineServerView serverView,
      final AuthorizerMapper authorizerMapper,
      final @Coordinator DruidLeaderClient coordinatorDruidLeaderClient,
      final @IndexingService DruidLeaderClient overlordDruidLeaderClient,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE, new SegmentsTable(serverView, coordinatorDruidLeaderClient, jsonMapper),
        SERVERS_TABLE, new ServersTable(serverView),
        SEGMENT_SERVERS_TABLE, new ServerSegmentsTable(serverView),
        TASKS_TABLE, new TasksTable(overlordDruidLeaderClient, jsonMapper)
    );
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  static class SegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;

    public SegmentsTable(
        TimelineServerView serverView,
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper
    )
    {
      this.serverView = serverView;
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
      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidDataSource> druidDataSourceList = getMetadataSegments(druidLeaderClient, jsonMapper);
      final List<DataSegment> metadataSegments = druidDataSourceList
          .stream()
          .flatMap(t -> t.getSegments().stream())
          .collect(Collectors.toList());
      final Map<String, DataSegment> publishedSegments = metadataSegments
          .stream()
          .collect(Collectors.toMap(
              DataSegment::getIdentifier,
              Function.identity()
          ));
      final Map<String, DataSegment> availableSegments = new HashMap<>();
      final Map<String, QueryableDruidServer> serverViewClients = serverView.getClients();
      for (QueryableDruidServer queryableDruidServer : serverViewClients.values()) {
        final DruidServer druidServer = queryableDruidServer.getServer();
        final ServerType type = druidServer.getType();
        final Map<String, DataSegment> segments = new HashMap<>(druidServer.getSegments());
        final long isRealtime = druidServer.segmentReplicatable() ? 0 : 1;
        for (Map.Entry<String, DataSegment> segmentEntry : segments.entrySet()) {
          String segmentId = segmentEntry.getKey();
          DataSegment segment = segmentEntry.getValue();
          int numReplicas = 1;
          if (availableSegments.containsKey(segmentId)) {
            //do not create new row if a segmentId has been seen previously
            // but increment the replica count and update row
            numReplicas++;
            updateRow(segmentId, numReplicas, rows);
            continue;
          }
          availableSegments.putIfAbsent(segmentId, segment);
          long isAvailable = 0;
          final long isPublished = publishedSegments.containsKey(segmentId) ? 1 : 0;
          if (type.toString().equals(ServerType.HISTORICAL.toString())
              || type.toString().equals(ServerType.REALTIME.toString())
              || type.toString().equals(ServerType.INDEXER_EXECUTOR.toString())) {
            isAvailable = 1;
          }
          String payload;
          try {
            payload = jsonMapper.writeValueAsString(segment);
          }
          catch (JsonProcessingException e) {
            log.error(e, "Error getting segment payload for segment %s", segmentId);
            throw new RuntimeException(e);
          }
          final Object[] row = createRow(
              segment.getIdentifier(),
              segment.getDataSource(),
              segment.getInterval().getStart(),
              segment.getInterval().getEnd(),
              segment.getSize(),
              segment.getVersion(),
              segment.getShardSpec().getPartitionNum(),
              numReplicas,
              isPublished,
              isAvailable,
              isRealtime,
              payload
          );
          rows.add(row);
        }
      }
      //process publishedSegments
      for (Map.Entry<String, DataSegment> segmentEntry : publishedSegments.entrySet()) {
        String segmentId = segmentEntry.getKey();
        //skip the published segments which are already processed
        if (availableSegments.containsKey(segmentId)) {
          continue;
        }
        DataSegment segment = segmentEntry.getValue();
        String payload;
        try {
          payload = jsonMapper.writeValueAsString(segment);
        }
        catch (JsonProcessingException e) {
          log.error(e, "Error getting segment payload for segment %s", segmentId);
          throw new RuntimeException(e);
        }
        final Object[] row = createRow(
            segment.getIdentifier(),
            segment.getDataSource(),
            segment.getInterval().getStart(),
            segment.getInterval().getEnd(),
            segment.getSize(),
            segment.getVersion(),
            segment.getShardSpec().getPartitionNum(),
            0,
            1,
            0,
            0,
            payload
        );
        rows.add(row);
      }
      return Linq4j.asEnumerable(rows);
    }

    private void updateRow(String segmentId, int replicas, List<Object[]> rows)
    {
      Object[] oldRow = null;
      Object[] newRow = null;
      for (Object[] row : rows) {
        if (row[0].equals(segmentId)) {
          oldRow = row;
          row[7] = replicas;
          newRow = row;
          break;
        }
      }
      if (oldRow == null || newRow == null) {
        log.error("Cannot update row if the segment[%s] is not present in the existing rows", segmentId);
        throw new RuntimeException("No row exists with segmentId " + segmentId);
      }
      rows.remove(oldRow);
      rows.add(newRow);
    }

    private Object[] createRow(
        String identifier,
        String dataSource,
        DateTime start,
        DateTime end,
        long size,
        String version,
        int partitionNum,
        int numReplicas,
        long isPublished,
        long isAvailable,
        long isRealtime,
        String payload
    )
    {
      final Object[] row = new Object[SEGMENTS_TABLE_SIZE];
      row[0] = identifier;
      row[1] = dataSource;
      row[2] = start;
      row[3] = end;
      row[4] = size;
      row[5] = version;
      row[6] = partitionNum;
      row[7] = numReplicas;
      row[8] = isPublished;
      row[9] = isAvailable;
      row[10] = isRealtime;
      row[11] = payload;
      return row;
    }

    // Note that coordinator must be up to get segments
    List<ImmutableDruidDataSource> getMetadataSegments(
        DruidLeaderClient coordinatorClient,
        ObjectMapper jsonMapper
    )
    {
      try {
        FullResponseHolder response = coordinatorClient.go(
            coordinatorClient.makeRequest(
                HttpMethod.GET,
                StringUtils.format(
                    "/druid/coordinator/v1/datasources?full"
                )
            )
        );

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          throw new ISE(
              "Error while fetching metadata segments status[%s] content[%s]",
              response.getStatus(),
              response.getContent()
          );
        }
        return jsonMapper.readValue(
            response.getContent(), new TypeReference<List<ImmutableDruidDataSource>>()
            {
            }
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
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

    public TasksTable(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
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
      final List<TaskStatusPlus> tasks = getTasks(druidLeaderClient, jsonMapper);
      final FluentIterable<Object[]> results = FluentIterable
          .from(tasks)
          .transform(
              task -> new Object[]{
                  task.getId(),
                  task.getType(),
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
                  task.getErrorMsg()
              }
          );

      return Linq4j.asEnumerable(results);
    }

    //Note that overlord must be up to get tasks
    private List<TaskStatusPlus> getTasks(
        DruidLeaderClient indexingServiceClient,
        ObjectMapper jsonMapper
    )
    {
      try {
        final FullResponseHolder response = indexingServiceClient.go(
            indexingServiceClient.makeRequest(HttpMethod.GET, StringUtils.format("/druid/indexer/v1/tasks"))
        );

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          throw new ISE(
              "Error while fetching tasks status[%s] content[%s]",
              response.getStatus(),
              response.getContent()
          );
        }

        return jsonMapper.readValue(
            response.getContent(),
            new TypeReference<List<TaskStatusPlus>>()
            {
            }
        );
      }
      catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
