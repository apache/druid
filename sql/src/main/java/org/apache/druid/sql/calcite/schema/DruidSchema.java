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

import com.google.common.base.Preconditions;
import org.apache.calcite.schema.Table;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.Set;

public class DruidSchema extends AbstractTableSchema
{
  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final DruidSchemaManager druidSchemaManager;
  private final CatalogResolver catalogResolver;
  private final AuthorizerMapper authorizerMapper;
  private final AuthenticationResult authenticationResult;
  private final boolean authorizeTableVisibility;

  public DruidSchema(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final DruidSchemaManager druidSchemaManager,
      final CatalogResolver catalogResolver,
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final boolean authorizeTableVisibility
  )
  {
    this.segmentMetadataCache = segmentMetadataCache;
    this.catalogResolver = catalogResolver;
    if (druidSchemaManager != null && !(druidSchemaManager instanceof NoopDruidSchemaManager)) {
      this.druidSchemaManager = druidSchemaManager;
    } else {
      this.druidSchemaManager = null;
    }
    this.authorizerMapper = authorizerMapper;
    this.authenticationResult = Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    this.authorizeTableVisibility = authorizeTableVisibility;
  }

  @Override
  public Table getTable(String name)
  {
    if (authorizeTableVisibility
        && !SchemaUtils.isTableVisible(authorizerMapper, authenticationResult, name, _ -> ResourceType.DATASOURCE)) {
      // Do not return tables that are not supposed to be visible in this schema.
      return null;
    }

    DruidTable schemaMgrTable = null;
    DruidTable catalogTable = catalogResolver.resolveDatasource(name, null);
    if (catalogTable == null && druidSchemaManager != null) {
      schemaMgrTable = druidSchemaManager.getTable(name, segmentMetadataCache);
    }
    if (schemaMgrTable == null) {
      DatasourceTable.PhysicalDatasourceMetadata dsMetadata = segmentMetadataCache.getDatasource(name);
      return catalogResolver.resolveDatasource(name, dsMetadata);
    } else {
      return schemaMgrTable;
    }
  }

  @Override
  public Set<String> getTableNames()
  {
    final Set<String> allTableNames;
    if (druidSchemaManager != null) {
      allTableNames = druidSchemaManager.getTableNames(segmentMetadataCache);
    } else {
      allTableNames = catalogResolver.getTableNames(segmentMetadataCache.getDatasourceNames());
    }

    if (authorizeTableVisibility) {
      return SchemaUtils.filterVisibleTables(
          authorizerMapper,
          authenticationResult,
          allTableNames,
          _ -> ResourceType.DATASOURCE
      );
    } else {
      return allTableNames;
    }
  }
}
