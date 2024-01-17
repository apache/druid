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
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.CachedMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlSchema;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.fail;

public class CatalogQueryTest extends BaseCalciteQueryTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;

  @Test
  public void testCatalogSchema()
  {
    SqlSchema schema = SqlSchema.builder()
        .column("__time", "TIMESTAMP(3) NOT NULL")
        .column("extra1", "VARCHAR")
        .column("dim2", "VARCHAR")
        .column("dim1", "VARCHAR")
        .column("cnt", "BIGINT NOT NULL")
        .column("m1", "DOUBLE NOT NULL")
        .column("extra2", "BIGINT NOT NULL")
        .column("extra3", "VARCHAR")
        .column("m2", "DOUBLE NOT NULL")
        .build();
    testBuilder()
        .sql("SELECT * FROM foo ORDER BY __time LIMIT 1")
        .expectedResources(Collections.singletonList(dataSourceRead("foo")))
        //.expectedSqlSchema(schema)
        .run();
  }

  @After
  public void catalogTearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  @Override
  public CatalogResolver createCatalogResolver()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
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
    buildFooDatasource();
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

  public void buildFooDatasource()
  {
    TableMetadata spec = TableBuilder.datasource("foo", "ALL")
        .timeColumn()
        .column("extra1", null)
        .column("dim2", null)
        .column("dim1", null)
        .column("cnt", null)
        .column("m1", Columns.DOUBLE)
        .column("extra2", Columns.LONG)
        .column("extra3", Columns.STRING)
        .hiddenColumns(Arrays.asList("dim3", "unique_dim1"))
        .build();
    createTableMetadata(spec);
  }
}
