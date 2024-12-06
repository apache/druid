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
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

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
    TableMetadata table = TableBuilder.datasource(tableName, "DAY")
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
    TableMetadata table = TableBuilder.datasource(tableName, "ALL")
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

  /**
   * Adding a new column during ingestion that is not defined in a sealed table, should fail with
   * proper validation error. Disabling catalog validation, through context parameter, and issuing ingest
   * query again, should succeed.
   */
  @Test
  public void testInsertNonDefinedColumnIntoSealedCatalogTableWithValidationDisabled() throws Exception
  {
    String queryFile = "/catalog/sealedWithValidationDisabled_select.sql";
    String tableName = "testInsertNonDefinedColumnIntoSealedCatalogTableWithValidationDisabled" + operationName;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(dmlPrefixPattern, tableName) + "\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  f AS extra\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
        + "  )\n"
        + ") "
        + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
        + "PARTITIONED BY DAY\n";

    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(
        sqlQueryFromString(
            queryInline,
            ImmutableMap.of()
        ),
        null,
        null,
        HttpResponseStatus.BAD_REQUEST
    );
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            StringUtils.format("Column [extra] is not defined in the target table [druid.%s] strict schema", tableName))
    );

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(
        queryInline,
        ImmutableMap.of(QueryContexts.CATALOG_VALIDATION_ENABLED, false)
    );

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
   * Assigning a column during ingestion, to an input type that is not compatible with the defined type of the
   * column, should result in a proper validation error. Disabling catalog validation, through context parameter, and
   * issuing ingest query again, should succeed.
   *
   * In this test we define the table as
   * <p>
   * __time      LONG
   * double_col  DOUBLE
   * <p>
   * And insert the following data:
   * <p>
   * __time, varchar_col1, bigint_col1, float_col1, varchar_col2
   * 2022-12-26T12:34:56,extra,10,"20",2.0,foo
   * <p>
   * even though the data is written
   * as
   * <p>
   * 2022-12-26T12:34:56,extra
   * <p>
   * When querying the table with query: 'SELECT * from ##tableName', the data is returned as:
   * <p>
   * __time, double_col
   * 2022-12-26T12:34:56,0.0
   * <p>
   * because the broker knows the double_col column to be a DOUBLE, and so converts to null (0.0) at query time.
   */
  @Test
  public void testInsertWithIncompatibleTypeAssignmentWithValidationDisabled() throws Exception
  {
    String tableName = "testInsertWithIncompatibleTypeAssignmentWithValidationDisabled" + operationName;
    String queryFile = "/catalog/incompatibleTypeAssignmentWithValidationDisabled_select.sql";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("double_col", "DOUBLE")
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
    LOG.info("table created:\n%s", client.readTable(table.id()));
    String queryInline =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(a) AS __time,\n"
            + "  b AS double_col\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
            + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
            + "  )\n"
            + ") "
            + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            tableName
        );

    LOG.info("Running query:\n%s", queryInline);
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(
        sqlQueryFromString(
            queryInline,
            ImmutableMap.of()
        ),
        null,
        null,
        HttpResponseStatus.BAD_REQUEST
    );
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            "Cannot assign to target field 'double_col' of type DOUBLE from source field 'double_col' of type VARCHAR (line [4], column [3])")
    );

    // Submit the task and wait for the datasource to get loaded
    LOG.info("Running query:\n%s", queryInline);
    sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(
        queryInline,
        ImmutableMap.of(QueryContexts.CATALOG_VALIDATION_ENABLED, false)
    );

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

  private static SqlQuery sqlQueryFromString(String queryString, Map<String, Object> context)
  {
    return new SqlQuery(queryString, null, false, false, false, context, null);
  }
}
