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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of catalog DDL: define a table with SQL, ingest into it, and query it. This is what proves the
 * Broker-to-Coordinator write path and the cache refresh actually work against a running cluster, rather than against
 * a recording stub.
 */
public class CatalogDdlAndIngestTest extends CatalogTestBase
{
  private TestCatalogClient client;
  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    // Catalog DDL is opt-in.
    broker.addProperty("druid.sql.planner.enableCatalogDdl", "true");
    return super.createCluster();
  }

  @BeforeAll
  public void initializeClient()
  {
    client = new TestCatalogClient(cluster);
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void testCreateTableThenIngestAndQuery()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" (\n"
        + "  __time TIMESTAMP,\n"
        + "  varchar_col1 VARCHAR,\n"
        + "  double_col1 DOUBLE\n"
        + ")\n"
        + "PARTITIONED BY DAY\n"
        + "CLUSTERED BY varchar_col1",
        tableName
    );

    // The spec reached the Coordinator, in the declared column order.
    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    assertEquals(
        List.of("__time", "varchar_col1", "double_col1"),
        columnNames(table)
    );
    assertEquals(
        List.of("TIMESTAMP", "VARCHAR", "DOUBLE"),
        table.spec().columns().stream().map(ColumnSpec::dataType).collect(Collectors.toList())
    );
    assertEquals("P1D", table.spec().properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY));

    // An INSERT that omits PARTITIONED BY picks it up from the catalog entry the DDL just wrote, which only works
    // if this Broker's catalog cache saw the write.
    ingest(
        "INSERT INTO \"%s\"\n"
        + "SELECT\n"
        + "  TIME_PARSE(a) AS __time,\n"
        + "  b AS varchar_col1,\n"
        + "  c AS double_col1\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,foo,10\\n2022-12-26T12:34:56,bar,20\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\"]}'\n"
        + "  )\n"
        + ") EXTEND (a VARCHAR, b VARCHAR, c BIGINT)\n",
        tableName
    );

    // The BIGINT source column is coerced to the DOUBLE declared by the DDL, and rows come back in clustered order.
    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        tableName,
        "2022-12-26T12:34:56.000Z,bar,20.0\n"
        + "2022-12-26T12:34:56.000Z,foo,10.0"
    );
  }

  @Test
  public void testAlterTableAddColumnThenIngest()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR) PARTITIONED BY DAY", tableName);
    cluster.callApi().runSql("ALTER TABLE \"%s\" ADD COLUMN b BIGINT", tableName);

    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    assertEquals(List.of("__time", "a", "b"), columnNames(table));

    ingest(
        "INSERT INTO \"%s\"\n"
        + "SELECT TIME_PARSE(x) AS __time, y AS a, z AS b\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,hello,7\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"x\",\"y\",\"z\"]}'\n"
        + "  )\n"
        + ") EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n",
        tableName
    );

    cluster.callApi().verifySqlQuery("SELECT * FROM %s", tableName, "2022-12-26T12:34:56.000Z,hello,7");
  }

  @Test
  public void testAlterTableColumnAndProperties()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR, b VARCHAR)", tableName);

    cluster.callApi().runSql("ALTER TABLE \"%s\" ALTER COLUMN b SET DATA TYPE BIGINT", tableName);
    cluster.callApi().runSql("ALTER TABLE \"%s\" DROP COLUMN a", tableName);
    cluster.callApi().runSql(
        "ALTER TABLE \"%s\" SET PROPERTIES (segmentGranularity = 'P1D', sealed = TRUE)",
        tableName
    );

    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    assertEquals(List.of("__time", "b"), columnNames(table));
    assertEquals("BIGINT", table.spec().columns().get(1).dataType());
    assertEquals("P1D", table.spec().properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY));
    assertEquals(true, table.spec().properties().get(DatasourceDefn.SEALED_PROPERTY));

    // A null value removes a property.
    cluster.callApi().runSql("ALTER TABLE \"%s\" SET PROPERTIES (sealed = NULL)", tableName);
    assertNull(
        client.readTable(TableId.datasource(tableName)).spec().properties().get(DatasourceDefn.SEALED_PROPERTY)
    );
  }

  @Test
  public void testCreateOrReplaceAndIfNotExists()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR) CLUSTERED BY a", tableName);

    // IF NOT EXISTS leaves the original definition alone.
    cluster.callApi().runSql("CREATE TABLE IF NOT EXISTS \"%s\" (__time TIMESTAMP, zzz VARCHAR)", tableName);
    assertEquals(List.of("__time", "a"), columnNames(client.readTable(TableId.datasource(tableName))));

    // OR REPLACE swaps the whole spec, including dropping the clustering the first statement set.
    cluster.callApi().runSql("CREATE OR REPLACE TABLE \"%s\" (__time TIMESTAMP, b BIGINT)", tableName);
    final TableMetadata replaced = client.readTable(TableId.datasource(tableName));
    assertEquals(List.of("__time", "b"), columnNames(replaced));
    assertNull(replaced.spec().properties().get(DatasourceDefn.CLUSTER_KEYS_PROPERTY));
  }

  @Test
  public void testCreateTableClusterKeys()
  {
    final String tableName = dataSource;
    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR, b BIGINT) CLUSTERED BY a, b", tableName);

    // Catalog properties round trip as untyped JSON, so decode before asserting.
    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    final List<ClusterKeySpec> keys = TestHelper.JSON_MAPPER.convertValue(
        table.spec().properties().get(DatasourceDefn.CLUSTER_KEYS_PROPERTY),
        ClusterKeySpec.CLUSTER_KEY_LIST_TYPE_REF
    );
    assertEquals(List.of("a", "b"), keys.stream().map(ClusterKeySpec::expr).collect(Collectors.toList()));
    assertTrue(keys.stream().noneMatch(ClusterKeySpec::desc));
  }

  /**
   * A rejection from the Coordinator's own validation must reach the SQL user as the Coordinator worded it, not as
   * a generic remote-call failure. Segment granularity is validated only on the Coordinator, so it exercises the
   * whole round trip.
   */
  @Test
  public void testCoordinatorValidationErrorSurfacesToSqlUser()
  {
    final String tableName = dataSource;
    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR)", tableName);

    final Exception e = assertThrows(
        Exception.class,
        () -> cluster.callApi().runSql(
            "ALTER TABLE \"%s\" SET PROPERTIES (segmentGranularity = 'not_a_granularity')",
            tableName
        )
    );
    assertTrue(e.getMessage().contains("granularity"), e.getMessage());
  }

  @Test
  public void testCreateTableAlreadyExistsFails()
  {
    final String tableName = dataSource;
    cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR)", tableName);

    final Exception e = assertThrows(
        Exception.class,
        () -> cluster.callApi().runSql("CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR)", tableName)
    );
    assertTrue(e.getMessage().contains("duplicate table"), e.getMessage());
  }

  /**
   * A projection defined in SQL must actually be used at query time, which is the whole point of storing one: the
   * specification the translator produces has to match what the planner generates for the equivalent query.
   */
  @Test
  public void testCreateTableWithProjectionThenQuery()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" (\n"
        + "  __time TIMESTAMP,\n"
        + "  varchar_col1 VARCHAR,\n"
        + "  bigint_col1 BIGINT,\n"
        + "  PROJECTION by_varchar AS (\n"
        + "    SELECT varchar_col1, SUM(bigint_col1) AS sum_bigint_col1\n"
        + "    GROUP BY varchar_col1\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        tableName
    );

    ingest(
        "INSERT INTO \"%s\"\n"
        + "SELECT TIME_PARSE(a) AS __time, b AS varchar_col1, c AS bigint_col1\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,foo,10\\n2022-12-26T12:34:56,foo,9"
        + "\\n2022-12-26T12:34:56,bar,8\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\"]}'\n"
        + "  )\n"
        + ") EXTEND (a VARCHAR, b VARCHAR, c BIGINT)\n",
        tableName
    );

    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.flush();

    cluster.callApi().verifySqlQuery(
        "SELECT varchar_col1, SUM(bigint_col1) FROM %s GROUP BY 1 ORDER BY 1",
        tableName,
        "bar,8\nfoo,19"
    );

    // The segment-scan metrics name the projection that served the query.
    emitter.waitForEvent(
        event -> event.hasMetricName("query/segment/time").hasDimension("projection", "by_varchar")
    );
  }

  @Test
  public void testAlterTableAddAndDropProjection()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" (__time TIMESTAMP, a VARCHAR, b BIGINT) PARTITIONED BY DAY",
        tableName
    );
    cluster.callApi().runSql(
        "ALTER TABLE \"%s\" ADD PROJECTION p AS (SELECT a, SUM(b) AS sum_b GROUP BY a)",
        tableName
    );
    assertEquals(1, projectionsOf(tableName).size());
    assertEquals("p", projectionsOf(tableName).get(0).getSpec().getName());

    // Adding it again is an error, but IF NOT EXISTS tolerates it.
    assertThrows(
        Exception.class,
        () -> cluster.callApi().runSql(
            "ALTER TABLE \"%s\" ADD PROJECTION p AS (SELECT a, SUM(b) AS sum_b GROUP BY a)",
            tableName
        )
    );
    cluster.callApi().runSql(
        "ALTER TABLE \"%s\" ADD IF NOT EXISTS PROJECTION p AS (SELECT a, SUM(b) AS sum_b GROUP BY a)",
        tableName
    );
    assertEquals(1, projectionsOf(tableName).size());

    cluster.callApi().runSql("ALTER TABLE \"%s\" DROP PROJECTION p", tableName);
    assertNull(
        client.readTable(TableId.datasource(tableName))
              .spec().properties().get(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY)
    );
    cluster.callApi().runSql("ALTER TABLE \"%s\" DROP PROJECTION IF EXISTS p", tableName);
  }

  /**
   * A clustered table defined entirely in SQL. The declared column order is the physical segment order, so rows come
   * back grouped by the clustering column, and a computed column is materialized at ingest time from the expression
   * the __base projection gives it.
   */
  @Test
  public void testCreateClusteredBaseTableThenIngestAndQuery()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" SEALED (\n"
        + "  varchar_col2 VARCHAR,\n"
        + "  __time TIMESTAMP,\n"
        + "  varchar_col1 VARCHAR,\n"
        + "  bigint_col1 BIGINT,\n"
        + "  doubled BIGINT,\n"
        + "  PROJECTION __base AS (\n"
        + "    SELECT varchar_col2, __time, varchar_col1, bigint_col1, bigint_col1 * 2 AS doubled\n"
        + "    CLUSTERED BY varchar_col2\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        tableName
    );

    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    assertEquals(true, table.spec().properties().get(DatasourceDefn.SEALED_PROPERTY));
    assertNotNull(table.spec().properties().get(DatasourceDefn.BASE_TABLE_PROPERTY));

    // 'doubled' is computed by the base table, so the INSERT supplies only its input column.
    ingest(
        "INSERT INTO \"%s\"\n"
        + "SELECT TIME_PARSE(a) AS __time, b AS varchar_col1, c AS bigint_col1, f AS varchar_col2\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,foo"
        + "\\n2022-12-26T12:34:56,extra,9,foo\\n2022-12-26T12:34:56,extra,8,foq"
        + "\\n2022-12-26T12:34:56,extra,8,fop\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"f\"]}'\n"
        + "  )\n"
        + ") EXTEND (a VARCHAR, b VARCHAR, c BIGINT, f VARCHAR)\n",
        tableName
    );

    // Columns come back in declared order, rows in clustering-value order, and 'doubled' was materialized at ingest.
    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        tableName,
        "foo,2022-12-26T12:34:56.000Z,extra,9,18\n"
        + "foo,2022-12-26T12:34:56.000Z,extra,10,20\n"
        + "fop,2022-12-26T12:34:56.000Z,extra,8,16\n"
        + "foq,2022-12-26T12:34:56.000Z,extra,8,16"
    );
  }

  /**
   * The same layout without SEALED: a column the query produces but the table does not declare is stored after the
   * declared layout rather than rejected or dropped, and the clustering the table declared is unchanged.
   */
  @Test
  public void testCreateNonSealedClusteredBaseTableThenIngestExtraColumn()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" (\n"
        + "  tenant VARCHAR,\n"
        + "  __time TIMESTAMP,\n"
        + "  v BIGINT,\n"
        + "  PROJECTION __base AS (SELECT tenant, __time, v CLUSTERED BY tenant)\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        tableName
    );

    final TableMetadata table = client.readTable(TableId.datasource(tableName));
    assertNull(table.spec().properties().get(DatasourceDefn.SEALED_PROPERTY));
    assertNotNull(table.spec().properties().get(DatasourceDefn.BASE_TABLE_PROPERTY));

    // 'extra_col' is not declared by the table.
    ingest(
        "INSERT INTO \"%s\"\n"
        + "SELECT TIME_PARSE(a) AS __time, b AS tenant, c AS v, d AS extra_col\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,bbb,1,x"
        + "\\n2022-12-26T12:34:56,aaa,2,y\\n2022-12-26T12:34:56,aaa,3,z\"}',\n"
        + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\"]}'\n"
        + "  )\n"
        + ") EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR)\n",
        tableName
    );

    // Declared columns first, the appended extra last; rows still grouped by the clustering column.
    cluster.callApi().verifySqlQuery(
        "SELECT * FROM %s",
        tableName,
        "aaa,2022-12-26T12:34:56.000Z,2,y\n"
        + "aaa,2022-12-26T12:34:56.000Z,3,z\n"
        + "bbb,2022-12-26T12:34:56.000Z,1,x"
    );
  }

  /**
   * A column the base table computes cannot be written directly: the catalog rejects the INSERT and says to supply
   * the expression's inputs instead.
   */
  @Test
  public void testInsertIntoComputedColumnIsRejected()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" SEALED (t VARCHAR, __time TIMESTAMP, v BIGINT, doubled BIGINT,"
        + " PROJECTION __base AS (SELECT t, __time, v, v * 2 AS doubled CLUSTERED BY t))"
        + " PARTITIONED BY DAY",
        tableName
    );

    verifySubmitSqlTaskFailsWith400BadRequest(
        StringUtils.format(
            "INSERT INTO \"%s\" SELECT TIME_PARSE(a) AS __time, b AS t, 1 AS v, 2 AS doubled"
            + " FROM TABLE(EXTERN('{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,x\"}',"
            + " '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\"]}'))"
            + " EXTEND (a VARCHAR, b VARCHAR) PARTITIONED BY DAY",
            tableName
        ),
        "computed by a virtual column"
    );
  }

  @Test
  public void testAlterTableAddAndDropBaseProjection()
  {
    final String tableName = dataSource;

    cluster.callApi().runSql(
        "CREATE TABLE \"%s\" SEALED (t VARCHAR, __time TIMESTAMP, v BIGINT) PARTITIONED BY DAY",
        tableName
    );
    cluster.callApi().runSql(
        "ALTER TABLE \"%s\" ADD PROJECTION __base AS (SELECT t, __time, v CLUSTERED BY t)",
        tableName
    );
    assertNotNull(
        client.readTable(TableId.datasource(tableName))
              .spec().properties().get(DatasourceDefn.BASE_TABLE_PROPERTY)
    );

    cluster.callApi().runSql("ALTER TABLE \"%s\" DROP PROJECTION __base", tableName);
    assertNull(
        client.readTable(TableId.datasource(tableName))
              .spec().properties().get(DatasourceDefn.BASE_TABLE_PROPERTY)
    );
  }

  private List<DatasourceProjectionMetadata> projectionsOf(String tableName)
  {
    return TestHelper.JSON_MAPPER.convertValue(
        client.readTable(TableId.datasource(tableName))
              .spec().properties().get(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY),
        new TypeReference<List<DatasourceProjectionMetadata>>() {}
    );
  }

  private void ingest(String sqlPattern, String tableName)
  {
    final SqlTaskStatus status = msqApis.submitTaskSql(StringUtils.format(sqlPattern, tableName));
    cluster.callApi().waitForTaskToSucceed(status.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName, coordinator, broker);
  }

  private static List<String> columnNames(TableMetadata table)
  {
    return table.spec().columns().stream().map(ColumnSpec::name).collect(Collectors.toList());
  }
}
