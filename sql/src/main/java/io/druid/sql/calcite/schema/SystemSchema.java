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
package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.client.BrokerServerView;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.coordinator.Coordinator;
import io.druid.client.indexing.IndexingService;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.discovery.DruidLeaderClient;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.segment.column.ValueType;
import io.druid.server.coordination.ServerType;
import io.druid.server.security.AuthorizerMapper;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.timeline.DataSegment;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SystemSchema extends AbstractSchema
{
  private static final Logger log = new Logger(SystemSchema.class);

  public static final String NAME = "SYS";
  private static final String SEGMENTS_TABLE = "SEGMENTS";
  private static final String SERVERS_TABLE = "SERVERS";
  private static final String SERVERSEGMENTS_TABLE = "SEGMENTSERVERS";
  private static final String TASKS_TABLE = "TASKS";
  private static final int SEGMENTS_TABLE_SIZE;
  private static final int SERVERSEGMENTS_TABLE_SIZE;

  private static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("SEGMENT_ID", ValueType.STRING)
      .add("DATASOURCE", ValueType.STRING)
      .add("START", ValueType.STRING)
      .add("END", ValueType.STRING)
      .add("IS_PUBLISHED", ValueType.STRING)
      .add("IS_AVAILABLE", ValueType.STRING)
      .add("IS_REALTIME", ValueType.STRING)
      .add("PAYLOAD", ValueType.STRING)
      .build();

  private static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("SERVER", ValueType.STRING)
      .add("SERVER_TYPE", ValueType.STRING)
      .add("TIER", ValueType.STRING)
      .add("CURR_SIZE", ValueType.STRING)
      .add("MAX_SIZE", ValueType.STRING)
      .build();

  private static final RowSignature SERVERSEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("SERVER", ValueType.STRING)
      .add("SEGMENT_ID", ValueType.STRING)
      .build();

  private static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("TASK_ID", ValueType.STRING)
      .add("TYPE", ValueType.STRING)
      .add("DATASOURCE", ValueType.STRING)
      .add("CREATED_TIME", ValueType.STRING)
      .add("QUEUE_INSERTION_TIME", ValueType.STRING)
      .add("STATUS", ValueType.STRING)
      .add("RUNNER_STATUS", ValueType.STRING)
      .add("DURATION", ValueType.STRING)
      .add("LOCATION", ValueType.STRING)
      .add("ERROR_MSG", ValueType.STRING)
      .build();

  private final Map<String, Table> tableMap;
  private final BrokerServerView serverView;

  static {
    SEGMENTS_TABLE_SIZE = SEGMENTS_SIGNATURE.getRowOrder().size();
    SERVERSEGMENTS_TABLE_SIZE = SERVERSEGMENTS_SIGNATURE.getRowOrder().size();
  }

  @Inject
  public SystemSchema(
      final BrokerServerView serverView,
      final AuthorizerMapper authorizerMapper,
      final @Coordinator DruidLeaderClient coordinatorDruidLeaderClient,
      final @IndexingService DruidLeaderClient overlordDruidLeaderClient,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    this.serverView = serverView;
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE, new SegmentsTable(coordinatorDruidLeaderClient, jsonMapper),
        SERVERS_TABLE, new ServersTable(),
        SERVERSEGMENTS_TABLE, new ServerSegmentsTable(),
        TASKS_TABLE, new TasksTable(overlordDruidLeaderClient, jsonMapper)
    );
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  private class SegmentsTable extends AbstractTable implements ScannableTable
  {
    private final DruidLeaderClient druidLeaderClient;
    private final ObjectMapper jsonMapper;

    public SegmentsTable(
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
      final List<DataSegment> matadataSegments = druidDataSourceList
          .stream()
          .flatMap(t -> t.getSegments().stream())
          .collect(Collectors.toList());
      final Map<String, DataSegment> publishedSegments = matadataSegments
          .stream()
          .collect(Collectors.toMap(
              DataSegment::getIdentifier,
              Function.identity()
          ));
      final Set<String> publishedSegmentIds = matadataSegments
          .stream()
          .map(DataSegment::getIdentifier)
          .collect(Collectors.toSet());

      final Map<String, QueryableDruidServer> serverViewClients = serverView.getClients();
      for (QueryableDruidServer queryableDruidServer : serverViewClients.values()) {
        final Object[] row = new Object[SEGMENTS_TABLE_SIZE];
        final DruidServer druidServer = queryableDruidServer.getServer();
        final ServerType type = druidServer.getType();
        final boolean isSegmentReplicatable = druidServer.segmentReplicatable();
        final Map<String, DataSegment> segments = new HashMap<>(druidServer.getSegments());
        segments.putAll(publishedSegments);  // add published segments map
        for (Map.Entry<String, DataSegment> segmentEntry : segments.entrySet()) {
          String segmentId = segmentEntry.getKey();
          DataSegment segment = segmentEntry.getValue();
          row[0] = segment.getIdentifier();
          row[1] = segment.getDataSource();
          row[2] = segment.getInterval().getStart();
          row[3] = segment.getInterval().getEnd();
          boolean is_available = false;
          boolean is_published = false;
          boolean is_realtime = false;
          if (publishedSegmentIds.contains(segmentId)) {
            is_published = true;
          }
          if (type.toString().equals(ServerType.HISTORICAL.toString())) {
            is_available = true;
            is_published = true;
          } else if (type.toString().equals(ServerType.REALTIME.toString())) {
            is_available = true;
            is_realtime = true;
          } else if (type.toString().equals(ServerType.INDEXER_EXECUTOR.toString())) {
            is_available = true;
            is_published = true;
            is_realtime = true;
          }
          if (!isSegmentReplicatable) {
            is_realtime = true;
          }
          row[4] = is_published;
          row[5] = is_available;
          row[6] = is_realtime;
          try {
            String payload = jsonMapper.writeValueAsString(segment);
            row[7] = payload;
          }
          catch (JsonProcessingException e) {
            log.error(e, "Error parsing segment payload ");
          }
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }

    private List<ImmutableDruidDataSource> getMetadataSegments(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper
    )
    {
      try {
        FullResponseHolder response = druidLeaderClient.go(
            druidLeaderClient.makeRequest(
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

  private class ServersTable extends AbstractTable implements ScannableTable
  {

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
              val.getServer().getType(),
              val.getServer().getTier(),
              val.getServer().getCurrSize(),
              val.getServer().getMaxSize()
          });
      return Linq4j.asEnumerable(results);
    }
  }

  private class ServerSegmentsTable extends AbstractTable implements ScannableTable
  {

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
        Object[] row = new Object[SERVERSEGMENTS_TABLE_SIZE];
        final DruidServer druidServer = queryableDruidServer.getServer();
        final Map<String, DataSegment> segmentMap = druidServer.getSegments();
        for (DataSegment segment : segmentMap.values()) {
          row[0] = druidServer.getHost();
          row[1] = segment.getIdentifier();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  private static class TasksTable extends AbstractTable implements ScannableTable
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

    private List<TaskStatusPlus> getTasks(
        DruidLeaderClient druidLeaderClient,
        ObjectMapper jsonMapper
    )
    {
      try {
        final FullResponseHolder response = druidLeaderClient.go(
            druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format("/druid/indexer/v1/tasks"))
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
