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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.StackTraceCollector;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * System schema table {@code sys.stack_trace} that contains a live Java thread-stack snapshot for
 * explicitly selected Druid servers.
 */
public class SystemStackTraceTable extends AbstractTable implements ProjectableFilterableTable
{
  private static final Logger log = new Logger(SystemStackTraceTable.class);

  public static final String TABLE_NAME = "stack_trace";

  static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("service_name", ColumnType.STRING)
      .add("node_roles", ColumnType.STRING)
      .add("collected_at", ColumnType.STRING)
      .add("thread_id", ColumnType.LONG)
      .add("thread_name", ColumnType.STRING)
      .add("thread_state", ColumnType.STRING)
      .add("daemon", ColumnType.LONG)
      .add("priority", ColumnType.LONG)
      .add("cpu_time_ns", ColumnType.LONG)
      .add("user_cpu_time_ns", ColumnType.LONG)
      .add("lock_name", ColumnType.STRING)
      .add("lock_owner_id", ColumnType.LONG)
      .add("lock_owner_name", ColumnType.STRING)
      .add("is_deadlocked", ColumnType.LONG)
      .add("stack", ColumnType.STRING)
      .add("error_message", ColumnType.STRING)
      .build();

  private static final int SERVER_INDEX = ROW_SIGNATURE.indexOf("server");
  private static final int SERVICE_NAME_INDEX = ROW_SIGNATURE.indexOf("service_name");
  private static final int NODE_ROLES_INDEX = ROW_SIGNATURE.indexOf("node_roles");
  private static final int COLLECTED_AT_INDEX = ROW_SIGNATURE.indexOf("collected_at");
  private static final int THREAD_ID_INDEX = ROW_SIGNATURE.indexOf("thread_id");
  private static final int THREAD_NAME_INDEX = ROW_SIGNATURE.indexOf("thread_name");
  private static final int THREAD_STATE_INDEX = ROW_SIGNATURE.indexOf("thread_state");
  private static final int DAEMON_INDEX = ROW_SIGNATURE.indexOf("daemon");
  private static final int PRIORITY_INDEX = ROW_SIGNATURE.indexOf("priority");
  private static final int CPU_TIME_NS_INDEX = ROW_SIGNATURE.indexOf("cpu_time_ns");
  private static final int USER_CPU_TIME_NS_INDEX = ROW_SIGNATURE.indexOf("user_cpu_time_ns");
  private static final int LOCK_NAME_INDEX = ROW_SIGNATURE.indexOf("lock_name");
  private static final int LOCK_OWNER_ID_INDEX = ROW_SIGNATURE.indexOf("lock_owner_id");
  private static final int LOCK_OWNER_NAME_INDEX = ROW_SIGNATURE.indexOf("lock_owner_name");
  private static final int IS_DEADLOCKED_INDEX = ROW_SIGNATURE.indexOf("is_deadlocked");
  private static final int STACK_INDEX = ROW_SIGNATURE.indexOf("stack");
  private static final int ERROR_MESSAGE_INDEX = ROW_SIGNATURE.indexOf("error_message");

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final AuthorizerMapper authorizerMapper;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  public SystemStackTraceTable(
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final AuthorizerMapper authorizerMapper,
      final HttpClient httpClient,
      final ObjectMapper jsonMapper
  )
  {
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.authorizerMapper = authorizerMapper;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public Enumerable<Object[]> scan(
      final DataContext root,
      final List<RexNode> filters,
      @Nullable final int[] projects
  )
  {
    final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
        root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
        "authenticationResult in dataContext"
    );
    SystemSchema.checkStateReadAccessForServers(authenticationResult, authorizerMapper);

    final Set<String> serverFilter = SystemSchemaFilters.extractColumnValues(filters, SERVER_INDEX);
    InvalidInput.conditionalException(
        serverFilter != null,
        "sys.stack_trace requires a filter on the server column using '=' or 'IN'"
    );
    final int maxStackTraceFrameDepth = getMaxStackTraceFrameDepth(
        root.get(StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY)
    );

    final Iterator<DiscoveryDruidNode> druidServers = SystemSchema.getDruidServers(druidNodeDiscoveryProvider);
    final Map<String, ServerStackTraceTarget> serverToTargetMap = new HashMap<>();
    druidServers.forEachRemaining(discoveryDruidNode -> {
      final DruidNode druidNode = discoveryDruidNode.getDruidNode();
      final String server = druidNode.getHostAndPortToUse();
      if (!serverFilter.contains(server)) {
        return;
      }

      final String nodeRole = discoveryDruidNode.getNodeRole().getJsonName();
      final ServerStackTraceTarget target = serverToTargetMap.get(server);
      if (target == null) {
        serverToTargetMap.put(
            server,
            new ServerStackTraceTarget(
                server,
                druidNode.getServiceName(),
                new ArrayList<>(Collections.singletonList(nodeRole)),
                druidNode
            )
        );
      } else {
        target.addNodeRole(nodeRole);
      }
    });

    final List<Object[]> rows = new ArrayList<>();
    for (final ServerStackTraceTarget target : serverToTargetMap.values()) {
      rows.addAll(target.buildRows(this, projects, maxStackTraceFrameDepth));
    }
    return Linq4j.asEnumerable(rows);
  }

  static int getMaxStackTraceFrameDepth(@Nullable final Object value)
  {
    return StackTraceCollector.validateMaxStackTraceFrameDepth(
        QueryContexts.getAsLong(
            StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY,
            value,
            StackTraceCollector.DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH
        )
    );
  }

  private static Object[] projectRow(final Object[] row, @Nullable final int[] projects)
  {
    if (projects == null) {
      return row;
    }
    final Object[] projectedRow = new Object[projects.length];
    for (int i = 0; i < projects.length; i++) {
      projectedRow[i] = row[projects[i]];
    }
    return projectedRow;
  }

  private StackTraceResult getStackTrace(
      final DruidNode druidNode,
      final int maxStackTraceFrameDepth
  )
  {
    final String url = druidNode.getUriToUse().resolve(
        StringUtils.format(
            "/status/stack?%s=%d",
            StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY,
            maxStackTraceFrameDepth
        )
    ).toString();
    try {
      final Request request = new Request(HttpMethod.GET, new URL(url));
      final StringFullResponseHolder response = httpClient
          .go(request, new StringFullResponseHandler(StandardCharsets.UTF_8))
          .get();

      if (response.getStatus().getCode() != HttpServletResponse.SC_OK) {
        final String errorMessage = StringUtils.format(
            "HTTP %d: %s",
            response.getStatus().getCode(),
            response.getStatus().getReasonPhrase()
        );
        log.warn("Failed to get stack trace from node[%s]: error[%s]", url, errorMessage);
        return new StackTraceResult(null, errorMessage);
      }

      final StackTraceCollector.ThreadStackTraceResponse stackTraceResponse =
          jsonMapper.readValue(response.getContent(), StackTraceCollector.ThreadStackTraceResponse.class);
      return stackTraceResponse == null
             ? new StackTraceResult(null, "Empty stack trace response")
             : new StackTraceResult(stackTraceResponse, null);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(StringUtils.format("Interrupted while fetching stack trace from node[%s]", url), e);
    }
    catch (Exception e) {
      final String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      log.warn(e, "Failed to get stack trace from node[%s]", url);
      return new StackTraceResult(null, errorMessage);
    }
  }

  private static class StackTraceResult
  {
    @Nullable
    final StackTraceCollector.ThreadStackTraceResponse response;
    @Nullable
    final String error;

    StackTraceResult(
        @Nullable final StackTraceCollector.ThreadStackTraceResponse response,
        @Nullable final String error
    )
    {
      this.response = response;
      this.error = error;
    }
  }

  private static class ServerStackTraceTarget
  {
    final String server;
    final String serviceName;
    final List<String> nodeRoles;
    final DruidNode druidNode;

    ServerStackTraceTarget(
        final String server,
        final String serviceName,
        final List<String> nodeRoles,
        final DruidNode druidNode
    )
    {
      this.server = server;
      this.serviceName = serviceName;
      this.nodeRoles = nodeRoles;
      this.druidNode = druidNode;
    }

    void addNodeRole(final String nodeRole)
    {
      if (!nodeRoles.contains(nodeRole)) {
        nodeRoles.add(nodeRole);
      }
    }

    String nodeRolesString()
    {
      return nodeRoles.stream().sorted().collect(Collectors.joining(","));
    }

    List<Object[]> buildRows(
        final SystemStackTraceTable table,
        @Nullable final int[] projects,
        final int maxStackTraceFrameDepth
    )
    {
      final StackTraceResult result = table.getStackTrace(druidNode, maxStackTraceFrameDepth);
      if (result.error != null || result.response == null) {
        return Collections.singletonList(table.buildErrorRow(this, result.error, projects));
      }

      return result.response.getThreads()
          .stream()
          .map(thread -> {
            final Object[] row = table.buildThreadRow(this, result.response, thread);
            return projectRow(row, projects);
          })
          .collect(Collectors.toList());
    }
  }

  private Object[] buildThreadRow(
      final ServerStackTraceTarget target,
      final StackTraceCollector.ThreadStackTraceResponse response,
      final StackTraceCollector.ThreadStackTrace thread
  )
  {
    final Object[] row = new Object[ROW_SIGNATURE.size()];
    row[SERVER_INDEX] = target.server;
    row[SERVICE_NAME_INDEX] = target.serviceName;
    row[NODE_ROLES_INDEX] = target.nodeRolesString();
    row[COLLECTED_AT_INDEX] = response.getCollectedAt();
    row[THREAD_ID_INDEX] = thread.getThreadId();
    row[THREAD_NAME_INDEX] = thread.getThreadName();
    row[THREAD_STATE_INDEX] = thread.getThreadState();
    row[DAEMON_INDEX] = thread.isDaemon() ? 1L : 0L;
    row[PRIORITY_INDEX] = (long) thread.getPriority();
    row[CPU_TIME_NS_INDEX] = thread.getCpuTimeNs();
    row[USER_CPU_TIME_NS_INDEX] = thread.getUserCpuTimeNs();
    row[LOCK_NAME_INDEX] = thread.getLockName();
    row[LOCK_OWNER_ID_INDEX] = thread.getLockOwnerId();
    row[LOCK_OWNER_NAME_INDEX] = thread.getLockOwnerName();
    row[IS_DEADLOCKED_INDEX] = thread.isDeadlocked() ? 1L : 0L;
    row[STACK_INDEX] = thread.getStackTrace();
    row[ERROR_MESSAGE_INDEX] = null;
    return row;
  }

  private Object[] buildErrorRow(
      final ServerStackTraceTarget target,
      @Nullable final String errorMessage,
      @Nullable final int[] projects
  )
  {
    final Object[] row = new Object[ROW_SIGNATURE.size()];
    row[SERVER_INDEX] = target.server;
    row[SERVICE_NAME_INDEX] = target.serviceName;
    row[NODE_ROLES_INDEX] = target.nodeRolesString();
    row[ERROR_MESSAGE_INDEX] = errorMessage;
    return projectRow(row, projects);
  }
}
