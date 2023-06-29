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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.Set;

/**
 * Facade onto the (optional) Druid catalog. Configured in Guice to be
 * the {@link CatalogResolver.NullCatalogResolver} or to an actual catalog.
 */
public interface CatalogResolver
{
  CatalogResolver NULL_RESOLVER = new NullCatalogResolver();

  /**
   * Catalog resolver for when the catalog is not available.
   */
  class NullCatalogResolver implements CatalogResolver
  {
    @Override
    public boolean ingestRequiresExistingTable()
    {
      return false;
    }

    @Override
    public DruidTable resolveDatasource(
        final String tableName,
        final DatasourceTable.PhysicalDatasourceMetadata dsMetadata
    )
    {
      return dsMetadata == null ? null : new DatasourceTable(dsMetadata);
    }

    @Override
    public Set<String> getTableNames(Set<String> datasourceNames)
    {
      return datasourceNames;
    }
  }

  /**
   * Global option to determine whether ingest requires an existing datasource, or
   * can automatically create a new datasource.
   */
  boolean ingestRequiresExistingTable();

  DruidTable resolveDatasource(
      String tableName,
      DatasourceTable.PhysicalDatasourceMetadata dsMetadata
  );

  Set<String> getTableNames(Set<String> datasourceNames);
}
