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

import org.apache.calcite.schema.Table;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.inject.Inject;
import java.util.Set;

public class DruidSchema extends AbstractTableSchema
{
  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final DruidSchemaManager druidSchemaManager;
  private final CatalogResolver catalogResolver;

  @Inject
  public DruidSchema(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final DruidSchemaManager druidSchemaManager,
      final CatalogResolver catalogResolver
  )
  {
    this.segmentMetadataCache = segmentMetadataCache;
    this.catalogResolver = catalogResolver;
    if (druidSchemaManager != null && !(druidSchemaManager instanceof NoopDruidSchemaManager)) {
      this.druidSchemaManager = druidSchemaManager;
    } else {
      this.druidSchemaManager = null;
    }
  }

  protected BrokerSegmentMetadataCache cache()
  {
    return segmentMetadataCache;
  }

  @Override
  public Table getTable(String name)
  {
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
    if (druidSchemaManager != null) {
      return druidSchemaManager.getTableNames();
    } else {
      return catalogResolver.getTableNames(segmentMetadataCache.getDatasourceNames());
    }
  }
}
