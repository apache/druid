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

package org.apache.druid.testsEx.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.testsEx.categories.Catalog;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.config.Initializer;
import org.apache.druid.testsEx.indexer.AbstractIndexerTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * Tests the full integration of the catalog and MSQ ingestion.
 * <ul>
 * <li>Create a catalog entry for the input table (up to the file name).</li>
 * <li>Create a catalog entry for the target table.</li>
 * <li>Verify that the target table appears in system tables even if no data.</li>
 * <li>Use MSQ to load the target table from the input.</li>
 * <li>Verify that the target table appears unchanged in system tables.</li>
 * <li>Verify some data.</li>
 * </ul>
 *
 * This test acknowledges that detailed functional testing is done in myriad
 * unit tests. Here we're just ensuring that the functionality works end-to-end.
 * Hence, we don't test multiple variations, errors, and so on.
 */
@RunWith(DruidTestRunner.class)
@Category(Catalog.class)
public class ITCatalogIngestTest
{
  private static final String SCHEMA_QUERY_RESOURCE = Initializer.queryFile(Catalog.class, "schema.json");

  @Inject
  private DruidClusterClient clusterClient;

  @Inject
  private SqlTestQueryHelper queryHelper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  @Test
  public void testIngestSanity() throws IOException, Exception
  {
    //cleanUp();

    //defineExternalTable();
    //defineTargetTable();
    verifySchema();
    //    loadDataViaMsq();
    //    verifySchema();
    //    verifyTargetTableData();
  }

  private void cleanUp()
  {
    // Drop the external table
    CatalogClient client = new CatalogClient(clusterClient);
    client.dropTableIfExists(TableId.external("sampleWiki"));

    // Clear up the datasource from the previous runs
    coordinatorClient.unloadSegmentsForDataSource("testWiki");
  }

  private void defineExternalTable()
  {
    CatalogClient client = new CatalogClient(clusterClient);
    TableMetadata table = TableBuilder.external("sampleWiki")
        .inputSource(ImmutableMap.of(
            "type", "local",
            // Path to data files within the indexer container
            "baseDir", "/resources/data/batch_index/json"
         ))
        .inputFormat(ImmutableMap.of("type", "json"))
        .column("timestamp", Columns.VARCHAR)
        .column("isRobot", Columns.VARCHAR)
        .column("diffUrl", Columns.VARCHAR)
        .column("added", Columns.BIGINT)
        .column("countryIsoCode", Columns.VARCHAR)
        .column("regionName", Columns.VARCHAR)
        .column("channel", Columns.VARCHAR)
        .column("flags", Columns.VARCHAR)
        .column("delta", Columns.BIGINT)
        .column("isUnpatrolled", Columns.VARCHAR)
        .column("isNew", Columns.VARCHAR)
        .column("deltaBucket", Columns.DOUBLE)
        .column("isMinor", Columns.VARCHAR)
        .column("isAnonymous", Columns.VARCHAR)
        .column("deleted", Columns.BIGINT)
        .column("cityName", Columns.VARCHAR)
        .column("metroCode", Columns.BIGINT)
        .column("namespace", Columns.VARCHAR)
        .column("comment", Columns.VARCHAR)
        .column("page", Columns.VARCHAR)
        .column("commentLength", Columns.BIGINT)
        .column("countryName", Columns.VARCHAR)
        .column("user", Columns.VARCHAR)
        .column("regionIsoCode", Columns.VARCHAR)
        .build();
    client.createTable(table, true);
  }

  private void defineTargetTable()
  {
    CatalogClient client = new CatalogClient(clusterClient);
    TableMetadata table = TableBuilder.datasource("testWiki", "P1D")
        .clusterColumns(
            new ClusterKeySpec("namespace", false),
            new ClusterKeySpec("page", false)
         )
        .timeColumn()
        .column("namespace", Columns.VARCHAR)
        .column("page", Columns.VARCHAR)
        .column("channel", Columns.VARCHAR)
        .column("isUnpatrolled", Columns.BIGINT) // 0 = false, 1 = true
        .column("isAnonymous", Columns.BIGINT) // 0 = false, 1 = true
        .column("user", Columns.VARCHAR)
        .column("countryName", Columns.VARCHAR)
        .column("isRobot", Columns.BIGINT) // 0 = false, 1 = true
        .column("diffUrl", Columns.VARCHAR)
        .column("added", Columns.BIGINT)
        .column("delta", Columns.DOUBLE) // Silly, just to test
        .column("isNew", Columns.BIGINT) // 0 = false, 1 = true
        .column("deltaBucket", Columns.DOUBLE)
        .column("isMinor", Columns.BIGINT) // 0 = false, 1 = true
        .column("deleted", Columns.BIGINT)
        .column("comment", Columns.VARCHAR)
        .column("commentLength", Columns.BIGINT)
        .build();
    client.createTable(table, true);
  }

  private void verifySchema() throws IOException, Exception
  {
    queryHelper.testQueriesFromString(AbstractIndexerTest.getResourceAsString(SCHEMA_QUERY_RESOURCE));
  }

  private void loadDataViaMsq()
  {
    // TODO Auto-generated method stub

  }

  private void verifyTargetTableData()
  {
    // TODO Auto-generated method stub

  }
}
