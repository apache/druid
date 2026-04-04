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

package org.apache.druid.testing.embedded.catalog;

import com.google.common.collect.ImmutableList;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests that expect succesfully ingestng data into catalog defined tables and querying the data
 * gives expected results.
 */
public abstract class CatalogIngestAndQueryTest extends CatalogTestBase
{
  private final String dmlPrefixPattern;

  public CatalogIngestAndQueryTest()
  {
    this.dmlPrefixPattern = getDmlPrefixPattern();
  }

  public abstract String getDmlPrefixPattern();

  private TestCatalogClient client;
  private EmbeddedMSQApis msqApis;

  @BeforeAll
  public void initializeClient()
  {
    client = new TestCatalogClient(cluster);
    msqApis = new EmbeddedMSQApis(cluster, overlord);
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
  public void testInsertImplicitCast()
  {
    final String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "DAY")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("double_col1", "DOUBLE")
        .build();

    client.createTable(table, true);
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
    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(queryInline);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        dataSource,
        "2022-12-26T12:34:56.000Z,8.0\n"
        + "2022-12-26T12:34:56.000Z,8.0\n"
        + "2022-12-26T12:34:56.000Z,9.0\n"
        + "2022-12-26T12:34:56.000Z,10.0"
    );
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
  public void testInsertWithClusteringFromCatalog()
  {
    String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "ALL")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("bigint_col1", "BIGINT")
        .property(
            DatasourceDefn.CLUSTER_KEYS_PROPERTY,
            ImmutableList.of(new ClusterKeySpec("bigint_col1", false))
        )
        .build();

    client.createTable(table, true);
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
    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(queryInline);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        dataSource,
        "2022-12-26T12:34:56.000Z,8\n"
        + "2022-12-26T12:34:56.000Z,8\n"
        + "2022-12-26T12:34:56.000Z,9\n"
        + "2022-12-26T12:34:56.000Z,10"
    );
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
  public void testInsertWithClusteringFromQuery()
  {
    String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("bigint_col1", "BIGINT")
        .build();

    client.createTable(table, true);
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
    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(queryInline);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        dataSource,
        "2022-12-26T12:34:56.000Z,8\n"
        + "2022-12-26T12:34:56.000Z,8\n"
        + "2022-12-26T12:34:56.000Z,9\n"
        + "2022-12-26T12:34:56.000Z,10"
    );
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
  public void testInsertWithMultiClusteringFromCatalog()
  {
    String tableName = dataSource;
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
    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(queryInline);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        dataSource,
        "2022-12-26T12:34:56.000Z,extra,8,2.0,fop\n"
        + "2022-12-26T12:34:56.000Z,extra,8,2.0,foq\n"
        + "2022-12-26T12:34:56.000Z,extra,9,2.0,foo\n"
        + "2022-12-26T12:34:56.000Z,extra,10,2.0,foo"
    );
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
  public void testInsertWithMultiClusteringFromQuery()
  {
    String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col1", "VARCHAR")
        .column("bigint_col1", "BIGINT")
        .column("float_col1", "FLOAT")
        .column("varchar_col2", "VARCHAR")
        .build();

    client.createTable(table, true);
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
    SqlTaskStatus sqlTaskStatus = msqApis.submitTaskSql(queryInline);
    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        dataSource,
        "2022-12-26T12:34:56.000Z,extra,8,2.0,fop\n"
        + "2022-12-26T12:34:56.000Z,extra,8,2.0,foq\n"
        + "2022-12-26T12:34:56.000Z,extra,9,2.0,foo\n"
        + "2022-12-26T12:34:56.000Z,extra,10,2.0,foo"
    );
  }

  /**
   * Adding a new column during ingestion that is not defined in a sealed table, should fail with
   * proper validation error. Disabling catalog validation, through context parameter, and issuing ingest
   * query again, should succeed.
   */
  @Test
  public void testInsertNonDefinedColumnIntoSealedCatalogTableWithValidationDisabled()
  {
    final String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
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

    verifySubmitSqlTaskFailsWith400BadRequest(
        queryInline,
        StringUtils.format("Column [extra] is not defined in the target table [druid.%s] strict schema", tableName)
    );

    // Submit the task and wait for the datasource to get loaded
    final ClientSqlQuery sqlQuery = new ClientSqlQuery(
        queryInline,
        null,
        false,
        false,
        false,
        Map.of(QueryContexts.CATALOG_VALIDATION_ENABLED, false),
        null
    );
    SqlTaskStatus sqlTaskStatus = cluster.callApi().onAnyBroker(b -> b.submitSqlTask(sqlQuery));

    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery("SELECT * FROM %s", dataSource, "2022-12-26T12:34:56.000Z,foo");
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
  public void testInsertWithIncompatibleTypeAssignmentWithValidationDisabled()
  {
    String tableName = dataSource;
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("double_col", "DOUBLE")
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
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

    verifySubmitSqlTaskFailsWith400BadRequest(
        queryInline,
        "Cannot assign to target field 'double_col' of type DOUBLE from source field 'double_col' of type VARCHAR (line [4], column [3])"
    );

    // Submit the task and wait for the datasource to get loaded
    final ClientSqlQuery sqlQuery = new ClientSqlQuery(
        queryInline,
        null,
        false,
        false,
        false,
        Map.of(QueryContexts.CATALOG_VALIDATION_ENABLED, false),
        null
    );
    SqlTaskStatus sqlTaskStatus = cluster.callApi().onAnyBroker(b -> b.submitSqlTask(sqlQuery));

    cluster.callApi().waitForTaskToSucceed(sqlTaskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery("SELECT * FROM %s", dataSource, "2022-12-26T12:34:56.000Z,");
  }
}
