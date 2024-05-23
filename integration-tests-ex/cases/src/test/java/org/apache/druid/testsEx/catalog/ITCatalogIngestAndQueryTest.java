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
import com.google.inject.Inject;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that expect succesfully ingestng data into catalog defined tables and querying the data
 * gives expected results.
 */
public abstract class ITCatalogIngestAndQueryTest
{
  public static final Logger LOG = new Logger(ITCatalogIngestAndQueryTest.class);

  @Inject
  private MsqTestQueryHelper msqHelper;
  @Inject
  private DataLoaderHelper dataLoaderHelper;
  @Inject
  private DruidClusterClient clusterClient;
  private CatalogClient client;

  private final String operationName;
  private final String dmlPrefixPattern;

  public ITCatalogIngestAndQueryTest()
  {
    this.operationName = getOperationName();
    this.dmlPrefixPattern = getDmlPrefixPattern();
  }

  public abstract String getOperationName();
  public abstract String getDmlPrefixPattern();

  @Before
  public void initializeClient()
  {
    client = new CatalogClient(clusterClient);
  }

  /**
   * Create table with columns:
   * <p>
   * __time      LONG
   * double_col1 DOUBLE
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', the BIGINT type column should
   * be implicitly coherced into type DOUBLE when inserted into the table, since the column being
   * written into is type DOUBLE.
   * <p>
   * __time, bigint_col1
   * 2022-12-26T12:34:56,8.0
   * 2022-12-26T12:34:56,8.0
   * 2022-12-26T12:34:56,9.0
   * 2022-12-26T12:34:56,10.0
   *
   */
  @Test
  public void testInsertImplicitCast() throws Exception
  {
    String queryFile = "/catalog/implicitCast_select.sql";
    String tableName = "testImplicitCast" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("double_col1", "DOUBLE")
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  c AS double_col1\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\\n2022-12-26T12:34:56,extra,9,\\\"30\\\",2.0,foo\\n2022-12-26T12:34:56,extra,8,\\\"40\\\",2.0,foq\\n2022-12-26T12:34:56,extra,8,\\\"50\\\",2.0,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n";

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryInline);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(tableName);

    msqHelper.testQueriesFromFile(queryFile, tableName);
  }

  /**
   * Create table with columns:
   * <p>
   * __time      LONG
   * double_col1 DOUBLE
   * <p>
   * and clustering columns defined in catalog as
   * <p>
   * bigInt_col1
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', because of the clustering
   * defined on the table, the data should be reordered to:
   * <p>
   * __time, bigint_col1
   * 2022-12-26T12:34:56,8
   * 2022-12-26T12:34:56,8
   * 2022-12-26T12:34:56,9
   * 2022-12-26T12:34:56,10
   *
   */
  @Test
  public void testInsertWithClusteringFromCatalog() throws Exception
  {
    String queryFile = "/catalog/clustering_select.sql";
    String tableName = "testWithClusteringFromCatalog" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("bigint_col1", "BIGINT")
        .property(
            DatasourceDefn.CLUSTER_KEYS_PROPERTY,
            ImmutableList.of(new ClusterKeySpec("bigint_col1", false))
        )
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  c AS bigint_col1\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\\n2022-12-26T12:34:56,extra,9,\\\"30\\\",2.0,foo\\n2022-12-26T12:34:56,extra,8,\\\"40\\\",2.0,foq\\n2022-12-26T12:34:56,extra,8,\\\"50\\\",2.0,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n";

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryInline);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(tableName);

    msqHelper.testQueriesFromFile(queryFile, tableName);
  }

  /**
   * Create table with columns:
   * <p>
   * __time      LONG
   * double_col1 DOUBLE
   * <p>
   * and clustering columns defined in query as
   * <p>
   * bigInt_col1
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', because of the clustering
   * defined on the table, the data should be reordered to:
   * <p>
   * __time, bigint_col1
   * 2022-12-26T12:34:56,8
   * 2022-12-26T12:34:56,8
   * 2022-12-26T12:34:56,9
   * 2022-12-26T12:34:56,10
   *
   */
  @Test
  public void testInsertWithClusteringFromQuery() throws Exception
  {
    String queryFile = "/catalog/clustering_select.sql";
    String tableName = "testWithClusteringFromQuery" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("bigint_col1", "BIGINT")
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  c AS bigint_col1\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\\n2022-12-26T12:34:56,extra,9,\\\"30\\\",2.0,foo\\n2022-12-26T12:34:56,extra,8,\\\"40\\\",2.0,foq\\n2022-12-26T12:34:56,extra,8,\\\"50\\\",2.0,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
        + "CLUSTERED BY \"bigint_col1\"\n";

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryInline);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(tableName);

    msqHelper.testQueriesFromFile(queryFile, tableName);
  }

  /**
   * Create table with columns:
   * <p>
   * __time       LONG
   * varchar_col1 VARCHAR
   * bigint_col1  BIGINT
   * float_col1  FLOAT
   * varchar_col2 VARCHAR
   * <p>
   * and multiple clustering columns defined in catalog as
   * <p>
   * bigInt_col1, varchar_col2
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', because of the clustering
   * defined on the table, the data should be reordered to:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   *
   */
  @Test
  public void testInsertWithMultiClusteringFromCatalog() throws Exception
  {
    String queryFile = "/catalog/multiClustering_select.sql";
    String tableName = "testWithMultiClusteringFromCatalog" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col1", "VARCHAR")
        .column("bigint_col1", "BIGINT")
        .column("float_col1", "FLOAT")
        .column("varchar_col2", "VARCHAR")
        .property(
            DatasourceDefn.CLUSTER_KEYS_PROPERTY,
            ImmutableList.of(new ClusterKeySpec("bigint_col1", false), new ClusterKeySpec("varchar_col2", false))
        )
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  b AS varchar_col1,\n"
        + "  c AS bigint_col1,\n"
        + "  e AS float_col1,\n"
        + "  f AS varchar_col2\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\\n2022-12-26T12:34:56,extra,9,\\\"30\\\",2.0,foo\\n2022-12-26T12:34:56,extra,8,\\\"40\\\",2.0,foq\\n2022-12-26T12:34:56,extra,8,\\\"50\\\",2.0,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n";

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryInline);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(tableName);

    msqHelper.testQueriesFromFile(queryFile, tableName);
  }

  /**
   * Create table with columns:
   * <p>
   * __time       LONG
   * varchar_col1 VARCHAR
   * bigint_col1  BIGINT
   * float_col1  FLOAT
   * varchar_col2 VARCHAR
   * <p>
   * and multiple clustering columns defined in query as
   * <p>
   * bigInt_col1, varchar_col2
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', because of the clustering
   * defined on the query, the data should be reordered to:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,8,"50",2.0,fop
   * 2022-12-26T12:34:56,extra,8,"40",2.0,foq
   * 2022-12-26T12:34:56,extra,9,"30",2.0,foo
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   *
   */
  @Test
  public void testInsertWithMultiClusteringFromQuery() throws Exception
  {
    String queryFile = "/catalog/multiClustering_select.sql";
    String tableName = "testWithMultiClusteringFromQuery" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col1", "VARCHAR")
        .column("bigint_col1", "BIGINT")
        .column("float_col1", "FLOAT")
        .column("varchar_col2", "VARCHAR")
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  b AS varchar_col1,\n"
        + "  c AS bigint_col1,\n"
        + "  e AS float_col1,\n"
        + "  f AS varchar_col2\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\\n2022-12-26T12:34:56,extra,9,\\\"30\\\",2.0,foo\\n2022-12-26T12:34:56,extra,8,\\\"40\\\",2.0,foq\\n2022-12-26T12:34:56,extra,8,\\\"50\\\",2.0,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
        + "CLUSTERED BY \"bigint_col1\", \"varchar_col2\"\n";

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryInline);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(tableName);

    msqHelper.testQueriesFromFile(queryFile, tableName);
  }
}
