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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.testsEx.categories.Catalog;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.config.Initializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.fail;

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
  @Inject
  private DruidClusterClient clusterClient;

  @Inject
  private SqlTestQueryHelper queryHelper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  @Test
  public void testIngestSanity() throws IOException, Exception
  {
    cleanUp();

    defineExternalTable();
    defineTargetTable();
    verifySchema();
    loadDataViaMsq();
    verifySchema();
    verifyTargetTableData();
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
        .column("timestamp", Columns.STRING)
        .column("isRobot", Columns.STRING)
        .column("diffUrl", Columns.STRING)
        .column("added", Columns.LONG)
        .column("countryIsoCode", Columns.STRING)
        .column("regionName", Columns.STRING)
        .column("channel", Columns.STRING)
        .column("flags", Columns.STRING)
        .column("delta", Columns.LONG)
        .column("isUnpatrolled", Columns.STRING)
        .column("isNew", Columns.STRING)
        .column("deltaBucket", Columns.DOUBLE)
        .column("isMinor", Columns.STRING)
        .column("isAnonymous", Columns.STRING)
        .column("deleted", Columns.LONG)
        .column("cityName", Columns.STRING)
        .column("metroCode", Columns.LONG)
        .column("namespace", Columns.STRING)
        .column("comment", Columns.STRING)
        .column("page", Columns.STRING)
        .column("commentLength", Columns.LONG)
        .column("countryName", Columns.STRING)
        .column("user", Columns.STRING)
        .column("regionIsoCode", Columns.STRING)
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
        .column("namespace", Columns.STRING)
        .column("page", Columns.STRING)
        .column("channel", Columns.STRING)
        .column("user", Columns.STRING)
        .column("countryName", Columns.STRING)
        .column("isRobot", Columns.LONG) // 0 = false, 1 = true
        .column("added", Columns.LONG)
        .column("delta", Columns.DOUBLE) // Silly, just to test
        .column("isNew", Columns.LONG) // 0 = false, 1 = true
        .column("deltaBucket", Columns.DOUBLE)
        .column("deleted", Columns.LONG)
        .build();
    client.createTable(table, true);
  }

  private void verifySchema() throws IOException, Exception
  {
    queryHelper.testQueriesFromString(
        StringUtils.getResource(
            this,
            Initializer.queryFile(Catalog.class, "schema.json")
        )
    );
  }

  private void loadDataViaMsq()
  {
    submitMSQTask(
        "testWiki",
        StringUtils.getResource(
            this,
            Initializer.queryFile(Catalog.class, "ingestWiki.sql")
        ),
        ImmutableList.of(
            new SqlParameter(
                SqlType.ARRAY,
                ImmutableList.of(
                    "wikipedia_index_data1.json",
                    "wikipedia_index_data2.json",
                    "wikipedia_index_data3.json"
                )
            )
        )
    );
  }

  private void verifyTargetTableData() throws IOException, Exception
  {
    queryHelper.testQueriesFromString(
        StringUtils.getResource(
            this,
            Initializer.queryFile(Catalog.class, "check_wiki.json")
        )
    );
  }

  /**
   * Submits a sqlTask, waits for task completion.
   */
  protected void submitMSQTask(
      String datasource,
      String sql,
      List<SqlParameter> parameters
  )
  {
    SqlQuery query = new SqlQuery(sql, null, false, false, false, null, parameters);
    try {
      SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTask(query);
      msqHelper.waitForCompletion(sqlTaskStatus);
    }
    catch (Exception e) {
      fail(e.getMessage());
    }

    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);
  }
}
