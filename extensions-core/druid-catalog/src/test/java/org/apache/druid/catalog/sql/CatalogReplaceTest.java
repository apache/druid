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

package org.apache.druid.catalog.sql;

import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule5;
import org.apache.druid.sql.calcite.CalciteCatalogReplaceTest;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.Assert.fail;

/**
 * Test the use of catalog specs to drive MSQ ingestion.
 */
public class CatalogReplaceTest extends CalciteCatalogReplaceTest
{
  @RegisterExtension
  public static final DerbyConnectorRule5 DERBY_CONNECTION_RULE = new DerbyConnectorRule5();
  private static CatalogStorage storage;

  @Override
  public CatalogResolver createCatalogResolver()
  {
    CatalogTests.DbFixture dbFixture = new CatalogTests.DbFixture(DERBY_CONNECTION_RULE);
    storage = dbFixture.storage;
    MetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        storage.jsonMapper()
    );
    return new LiveCatalogResolver(catalog);
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    super.finalizeTestFramework(sqlTestFramework);
    buildDatasources();
  }

  public void buildDatasources()
  {
    resolvedTables.forEach((datasourceName, datasourceTable) -> {
      DatasourceFacade catalogMetadata = ((DatasourceTable) datasourceTable).effectiveMetadata().catalogMetadata();
      TableBuilder tableBuilder = TableBuilder.datasource(datasourceName, catalogMetadata.segmentGranularityString());
      catalogMetadata.columnFacades().forEach(
          columnFacade -> {
            tableBuilder.column(columnFacade.spec().name(), columnFacade.spec().dataType());
          }
      );

      if (catalogMetadata.hiddenColumns() != null && !catalogMetadata.hiddenColumns().isEmpty()) {
        tableBuilder.hiddenColumns(catalogMetadata.hiddenColumns());
      }

      if (catalogMetadata.isSealed()) {
        tableBuilder.sealed(true);
      }

      if (catalogMetadata.clusterKeys() != null && !catalogMetadata.clusterKeys().isEmpty()) {
        tableBuilder.clusterColumns(catalogMetadata.clusterKeys().toArray(new ClusterKeySpec[0]));
      }

      createTableMetadata(tableBuilder.build());
    });
    DatasourceFacade catalogMetadata =
        ((DatasourceTable) resolvedTables.get("foo")).effectiveMetadata().catalogMetadata();
    TableBuilder tableBuilder = TableBuilder.datasource("foo", catalogMetadata.segmentGranularityString());
    catalogMetadata.columnFacades().forEach(
        columnFacade -> {
          tableBuilder.column(columnFacade.spec().name(), columnFacade.spec().dataType());
        }
    );
  }

  private void createTableMetadata(TableMetadata table)
  {
    try {
      storage.tables().create(table);
    }
    catch (CatalogException e) {
      fail(e.getMessage());
    }
  }
}
