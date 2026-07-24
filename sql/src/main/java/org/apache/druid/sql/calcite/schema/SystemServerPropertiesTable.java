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
import io.netty.handler.codec.http.HttpMethod;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * System schema table {@code sys.server_properties} that contains the properties of all Druid servers.
 * Each row contains the value of a single property. If a server has multiple node roles, all the rows for
 * that server would have multiple values in the column {@code node_roles} rather than duplicating all the
 * rows.
 */
public class SystemServerPropertiesTable extends AbstractTable implements ProjectableFilterableTable
{
  private static final Logger log = new Logger(SystemServerPropertiesTable.class);

  public static final String TABLE_NAME = "server_properties";

  static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("service_name", ColumnType.STRING)
      .add("node_roles", ColumnType.STRING)
      .add("property", ColumnType.STRING)
      .add("value", ColumnType.STRING)
      .add("error_message", ColumnType.STRING)
      .build();

  private static final int SERVER_INDEX = ROW_SIGNATURE.indexOf("server");
  private static final int SERVICE_NAME_INDEX = ROW_SIGNATURE.indexOf("service_name");
  private static final int NODE_ROLES_INDEX = ROW_SIGNATURE.indexOf("node_roles");
  private static final int PROPERTY_INDEX = ROW_SIGNATURE.indexOf("property");
  private static final int VALUE_INDEX = ROW_SIGNATURE.indexOf("value");
  private static final int ERROR_MESSAGE_INDEX = ROW_SIGNATURE.indexOf("error_message");

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final AuthorizerMapper authorizerMapper;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  public SystemServerPropertiesTable(
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      AuthorizerMapper authorizerMapper,
      HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.authorizerMapper = authorizerMapper;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
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

    // Extract server/service_name constraints to skip fetching properties from non-matching servers.
    final Map<Integer, Set<String>> columnFilters =
        SystemSchemaFilters.extractColumnValues(filters, SERVER_INDEX, SERVICE_NAME_INDEX);
    final Set<String> serverFilter = columnFilters.get(SERVER_INDEX);
    final Set<String> serviceNameFilter = columnFilters.get(SERVICE_NAME_INDEX);

    final Iterator<DiscoveryDruidNode> druidServers = SystemSchema.getDruidServers(druidNodeDiscoveryProvider);

    final Map<String, ServerProperties> serverToPropertiesMap = new HashMap<>();
    druidServers.forEachRemaining(discoveryDruidNode -> {
      final DruidNode druidNode = discoveryDruidNode.getDruidNode();
      final String nodeRole = discoveryDruidNode.getNodeRole().getJsonName();

      final String serverKey = druidNode.getHostAndPortToUse();

      if (serverFilter != null && !serverFilter.contains(serverKey)) {
        return;
      }
      if (serviceNameFilter != null && !serviceNameFilter.contains(druidNode.getServiceName())) {
        return;
      }

      final ServerProperties serverProperties = serverToPropertiesMap.get(serverKey);
      if (serverProperties != null) {
        serverProperties.addNodeRole(nodeRole);
      } else {
        serverToPropertiesMap.put(
            serverKey,
            new ServerProperties(
                druidNode.getServiceName(),
                serverKey,
                new ArrayList<>(Arrays.asList(nodeRole)),
                druidNode
            )
        );
      }
    });

    final List<Object[]> rows = new ArrayList<>();
    for (ServerProperties serverProperties : serverToPropertiesMap.values()) {
      rows.addAll(serverProperties.buildRows(this, projects));
    }
    return Linq4j.asEnumerable(rows);
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

  private PropertiesResult getProperties(DruidNode druidNode)
  {
    final String url = druidNode.getUriToUse().resolve("/status/properties").toString();
    try {
      final Request request = new Request(HttpMethod.GET, new URL(url));
      final StringFullResponseHolder response;
      response = httpClient
          .go(request, new StringFullResponseHandler(StandardCharsets.UTF_8))
          .get();

      if (response.getStatus().code() != HttpServletResponse.SC_OK) {
        final String errorMsg = StringUtils.format(
            "HTTP %d: %s",
            response.getStatus().code(),
            response.getStatus().reasonPhrase()
        );
        log.warn("Failed to get properties from node[%s]: error[%s]", url, errorMsg);
        return new PropertiesResult(new HashMap<>(), errorMsg);
      }
      return new PropertiesResult(
          jsonMapper.readValue(response.getContent(), new TypeReference<>(){}),
          null
      );
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(StringUtils.format("Interrupted while fetching properties from node[%s]", url), e);
    }
    catch (Exception e) {
      final String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      log.warn(e, "Failed to get properties from node[%s]", url);
      return new PropertiesResult(new HashMap<>(), errorMsg);
    }
  }

  private static class PropertiesResult
  {
    final Map<String, String> properties;
    @Nullable
    final String error;

    PropertiesResult(Map<String, String> properties, @Nullable String error)
    {
      this.properties = properties;
      this.error = error;
    }
  }

  private static class ServerProperties
  {
    final String serviceName;
    final String server;
    final List<String> nodeRoles;
    final DruidNode druidNode;

    public ServerProperties(
        String serviceName,
        String server,
        List<String> nodeRoles,
        DruidNode druidNode
    )
    {
      this.serviceName = serviceName;
      this.server = server;
      this.nodeRoles = nodeRoles;
      this.druidNode = druidNode;
    }

    public void addNodeRole(String nodeRole)
    {
      nodeRoles.add(nodeRole);
    }

    private List<Object[]> buildRows(
        final SystemServerPropertiesTable table,
        @Nullable final int[] projects
    )
    {
      final String nodeRolesString = nodeRoles.toString();
      final PropertiesResult result = table.getProperties(druidNode);
      final Map<String, String> properties = result.properties;
      final String error = result.error;

      if (properties.isEmpty()) {
        final Object[] row = new Object[ROW_SIGNATURE.size()];
        row[SERVER_INDEX] = server;
        row[SERVICE_NAME_INDEX] = serviceName;
        row[NODE_ROLES_INDEX] = nodeRolesString;
        row[PROPERTY_INDEX] = null;
        row[VALUE_INDEX] = null;
        row[ERROR_MESSAGE_INDEX] = error;
        return Collections.singletonList(projectRow(row, projects));
      }

      return properties.entrySet().stream()
          .map(entry -> {
            final Object[] row = new Object[ROW_SIGNATURE.size()];
            row[SERVER_INDEX] = server;
            row[SERVICE_NAME_INDEX] = serviceName;
            row[NODE_ROLES_INDEX] = nodeRolesString;
            row[PROPERTY_INDEX] = entry.getKey();
            row[VALUE_INDEX] = entry.getValue();
            row[ERROR_MESSAGE_INDEX] = error;
            return projectRow(row, projects);
          })
          .collect(Collectors.toList());
    }
  }
}
