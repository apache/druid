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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.http.SqlEngineRegistry;

import java.util.List;
import java.util.Set;

public class SystemSchemaProvider implements SchemaProvider
{
  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final MetadataSegmentView metadataView;
  private final TimelineServerView serverView;
  private final FilteredServerInventoryView serverInventoryView;
  private final AuthorizerMapper authorizerMapper;
  private final CoordinatorClient coordinatorClient;
  private final OverlordClient overlordClient;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final Provider<SqlEngineRegistry> sqlEngineRegistryProvider;
  private final PlannerConfig plannerConfig;
  private final Set<String> allTableNames;

  @Inject
  public SystemSchemaProvider(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final MetadataSegmentView metadataView,
      final TimelineServerView serverView,
      final FilteredServerInventoryView serverInventoryView,
      final AuthorizerMapper authorizerMapper,
      final CoordinatorClient coordinatorClient,
      final OverlordClient overlordClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ObjectMapper jsonMapper,
      @EscalatedClient final HttpClient httpClient,
      final Provider<SqlEngineRegistry> sqlEngineRegistryProvider,
      final PlannerConfig plannerConfig
  )
  {
    this.segmentMetadataCache = segmentMetadataCache;
    this.metadataView = metadataView;
    this.serverView = serverView;
    this.serverInventoryView = serverInventoryView;
    this.authorizerMapper = authorizerMapper;
    this.coordinatorClient = coordinatorClient;
    this.overlordClient = overlordClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.sqlEngineRegistryProvider = sqlEngineRegistryProvider;
    this.plannerConfig = plannerConfig;
    this.allTableNames = computeAllTableNames(plannerConfig);
  }

  /**
   * Compute the list of tables configured to exist on this server.
   */
  public static Set<String> computeAllTableNames(final PlannerConfig plannerConfig)
  {
    final ImmutableSet.Builder<String> allTableNames = ImmutableSet.builder();
    allTableNames.add(SystemSchema.SEGMENTS_TABLE);
    allTableNames.add(SystemSchema.SERVERS_TABLE);
    allTableNames.add(SystemSchema.SERVER_SEGMENTS_TABLE);
    allTableNames.add(TaskTableDescriptor.TABLE_NAME);
    allTableNames.add(SystemSchema.SUPERVISOR_TABLE);
    allTableNames.add(SystemServerPropertiesTable.TABLE_NAME);

    if (plannerConfig.isEnableSysQueriesTable()) {
      allTableNames.add(SystemSchema.QUERIES_TABLE);
    }

    return allTableNames.build();
  }

  @Override
  public List<NamedSchema> getSchemas(AuthenticationResult authenticationResult)
  {
    final SystemSchema systemSchema = new SystemSchema(
        segmentMetadataCache,
        metadataView,
        serverView,
        serverInventoryView,
        authorizerMapper,
        coordinatorClient,
        overlordClient,
        druidNodeDiscoveryProvider,
        jsonMapper,
        httpClient,
        sqlEngineRegistryProvider,
        plannerConfig,
        authenticationResult,
        allTableNames
    );

    return List.of(new NamedSystemSchema(plannerConfig, systemSchema));
  }
}
