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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ClusteredValueGroupsBaseTableMetadata;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.DatasourceBaseTableMetadata;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.CalciteCatalogDdlTest.CatalogDdlComponentSupplier;
import org.apache.druid.sql.calcite.planner.CatalogTableWriter;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that catalog DDL statements plan into the catalog operations they claim to, using a writer that records
 * calls instead of contacting a Coordinator.
 */
@SqlTestFrameworkConfig.ComponentSupplier(CatalogDdlComponentSupplier.class)
public class CalciteCatalogDdlTest extends BaseCalciteQueryTest
{
  private static final RecordingCatalogTableWriter WRITER = new RecordingCatalogTableWriter();

  public static class CatalogDdlComponentSupplier extends StandardComponentSupplier
  {
    public CatalogDdlComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public CatalogTableWriter createCatalogTableWriter()
    {
      return WRITER;
    }
  }

  @BeforeEach
  public void resetWriter()
  {
    WRITER.reset();
  }

  @Test
  public void testCreateTable()
  {
    execute("CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT)");

    assertEquals(1, WRITER.calls.size());
    final RecordingCatalogTableWriter.Call call = WRITER.calls.get(0);
    assertEquals("createTable", call.operation);
    assertEquals(TableId.datasource("tbl"), call.tableId);
    assertEquals(DatasourceDefn.TABLE_TYPE, call.spec.type());
    assertEquals(ImmutableMap.of(), call.spec.properties());
    assertEquals(
        ImmutableList.of(
            new ColumnSpec("__time", "TIMESTAMP", null),
            new ColumnSpec("page", "VARCHAR", null),
            new ColumnSpec("cnt", "BIGINT", null)
        ),
        call.spec.columns()
    );
    assertFalse(call.ifNotExists);
    assertFalse(call.replace);
  }

  @Test
  public void testCreateTableWithPartitioningAndClustering()
  {
    execute("CREATE TABLE tbl (page VARCHAR, cnt BIGINT) PARTITIONED BY DAY CLUSTERED BY page, cnt");

    final TableSpec spec = WRITER.calls.get(0).spec;
    assertEquals("P1D", spec.properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY));
    assertEquals(
        ImmutableList.of(new ClusterKeySpec("page", false), new ClusterKeySpec("cnt", false)),
        spec.properties().get(DatasourceDefn.CLUSTER_KEYS_PROPERTY)
    );
  }

  @Test
  public void testCreateTablePartitionedByAll()
  {
    execute("CREATE TABLE tbl (page VARCHAR) PARTITIONED BY ALL TIME");
    assertEquals("ALL", WRITER.calls.get(0).spec.properties().get(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY));
  }

  @Test
  public void testCreateTableTypeCanonicalization()
  {
    execute(
        "CREATE TABLE tbl (a CHAR, b INTEGER, c REAL, d DOUBLE, e VARCHAR ARRAY, f TYPE('complex<json>'))"
    );
    assertEquals(
        ImmutableList.of(
            new ColumnSpec("a", "VARCHAR", null),
            new ColumnSpec("b", "BIGINT", null),
            new ColumnSpec("c", "FLOAT", null),
            new ColumnSpec("d", "DOUBLE", null),
            new ColumnSpec("e", "VARCHAR ARRAY", null),
            new ColumnSpec("f", "COMPLEX<json>", null)
        ),
        WRITER.calls.get(0).spec.columns()
    );
  }

  @Test
  public void testCreateTableFlags()
  {
    execute("CREATE OR REPLACE TABLE tbl (a VARCHAR)");
    assertTrue(WRITER.calls.get(0).replace);

    WRITER.reset();
    execute("CREATE TABLE IF NOT EXISTS tbl (a VARCHAR)");
    assertTrue(WRITER.calls.get(0).ifNotExists);
  }

  @Test
  public void testCreateTableInDruidSchema()
  {
    execute("CREATE TABLE druid.tbl (a VARCHAR)");
    assertEquals(TableId.datasource("tbl"), WRITER.calls.get(0).tableId);
  }

  @Test
  public void testResourceActionIsDatasourceWrite()
  {
    final DirectStatement stmt = statement("CREATE TABLE tbl (a VARCHAR)");
    stmt.execute();
    assertEquals(
        Collections.singleton(new ResourceAction(new Resource("tbl", ResourceType.DATASOURCE), Action.WRITE)),
        stmt.resources()
    );
  }

  @Test
  public void testDdlReturnsNoRows()
  {
    final DirectStatement stmt = statement("CREATE TABLE tbl (a VARCHAR)");
    final List<Object[]> results = stmt.execute().getResults().toList();
    assertEquals(ImmutableList.of(), results);
  }

  @Test
  public void testAlterTableAddColumn()
  {
    // ADD and ALTER differ only in which existence outcome is an error, and that rule is enforced inside the
    // Coordinator's update transaction, so all the statement does is pick the verb. EditorTest covers the rules.
    execute("ALTER TABLE tbl ADD COLUMN b BIGINT");

    final RecordingCatalogTableWriter.Call call = WRITER.lastCall("addColumns");
    assertEquals(ImmutableList.of(new ColumnSpec("b", "BIGINT", null)), call.columns);
  }

  @Test
  public void testAlterTableDropColumn()
  {
    execute("ALTER TABLE tbl DROP COLUMN gone");
    assertEquals(ImmutableList.of("gone"), WRITER.lastCall("dropColumns").droppedColumns);
  }

  @Test
  public void testAlterTableAlterColumn()
  {
    execute("ALTER TABLE tbl ALTER COLUMN cnt SET DATA TYPE DOUBLE");
    assertEquals(
        ImmutableList.of(new ColumnSpec("cnt", "DOUBLE", null)),
        WRITER.lastCall("alterColumns").columns
    );
  }

  @Test
  public void testAlterTableSetProperties()
  {
    execute("ALTER TABLE tbl SET PROPERTIES (targetSegmentRows = 3000000, sealed = TRUE, description = 'hi')");

    final Map<String, Object> properties = WRITER.lastCall("updateProperties").properties;
    assertEquals(3000000L, properties.get("targetSegmentRows"));
    assertEquals(true, properties.get("sealed"));
    assertEquals("hi", properties.get("description"));
  }

  @Test
  public void testAlterTableSetPropertyToNullRemovesIt()
  {
    execute("ALTER TABLE tbl SET PROPERTIES (description = NULL)");
    final Map<String, Object> properties = WRITER.lastCall("updateProperties").properties;
    assertTrue(properties.containsKey("description"));
    assertNull(properties.get("description"));
  }

  @Test
  public void testCreateTableRejectsDuplicateColumn()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, a BIGINT)")
    );
    assertTrue(e.getMessage().contains("Column [a] is declared more than once"));
  }

  @Test
  public void testCreateTableRejectsUnsupportedType()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a TYPE('NOT_A_TYPE'))")
    );
    assertTrue(e.getMessage().contains("unsupported type"));
  }

  /**
   * Any spelling that resolves to a LONG is accepted for the time column, and is stored as written.
   */
  @Test
  public void testCreateTableTimeColumnSpellings()
  {
    execute("CREATE TABLE tbl (__time BIGINT)");
    assertEquals(ImmutableList.of(new ColumnSpec("__time", "BIGINT", null)), WRITER.calls.get(0).spec.columns());

    WRITER.reset();
    execute("CREATE TABLE tbl (__time TYPE('LONG'))");
    assertEquals(ImmutableList.of(new ColumnSpec("__time", "LONG", null)), WRITER.calls.get(0).spec.columns());
  }

  @Test
  public void testCreateTableRejectsNonLongTimeColumn()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (__time VARCHAR)")
    );
    assertTrue(e.getMessage().contains("Column [__time] must have type"));
  }

  @Test
  public void testCreateTableRejectsNonDruidSchema()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE lookup.tbl (a VARCHAR)")
    );
    assertTrue(e.getMessage().contains("is not a Druid datasource"));
  }

  @Test
  public void testCreateTableRejectsBothReplaceAndIfNotExists()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE OR REPLACE TABLE IF NOT EXISTS tbl (a VARCHAR)")
    );
    assertTrue(e.getMessage().contains("Cannot specify both OR REPLACE and IF NOT EXISTS"));
  }

  @Test
  public void testCreateTableRejectsClusteringExpression()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR) CLUSTERED BY a DESC")
    );
    assertTrue(e.getMessage().contains("must be a column name"));
  }

  /**
   * The feature is off unless an operator turns it on, so that upgrading a cluster does not silently widen what a
   * datasource WRITE permission allows.
   */
  @Test
  public void testDdlIsDisabledByDefault()
  {
    final DirectStatement stmt = getSqlStatementFactory(PlannerConfig.builder().build(), new AuthConfig())
        .directStatement(
            SqlQueryPlus.builder("CREATE TABLE tbl (a VARCHAR)")
                        .auth(CalciteTests.SUPER_USER_AUTH_RESULT)
                        .build()
        );
    final DruidException e = assertThrows(DruidException.class, stmt::execute);
    assertTrue(e.getMessage().contains("druid.sql.planner.enableCatalogDdl"), e.getMessage());
    assertEquals(ImmutableList.of(), WRITER.calls);
  }

  /**
   * The stored specification must be the one the planner would produce for the equivalent query, since that is what
   * makes a projection match at query time. Pinned as JSON so a change in planner output is visible here.
   */
  @Test
  public void testCreateTableWithProjection() throws Exception
  {
    execute(
        "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION daily AS (SELECT TIME_FLOOR(__time, 'P1D'), page, SUM(cnt) AS total GROUP BY 1, 2))"
    );

    assertEquals(
        "[{\"spec\":{\"type\":\"aggregate\",\"name\":\"daily\","
        + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\","
        + "\"expression\":\"timestamp_floor(\\\"__time\\\",'P1D',null,'UTC')\",\"outputType\":\"LONG\"}],"
        + "\"groupingColumns\":[{\"type\":\"long\",\"name\":\"v0\",\"multiValueHandling\":\"SORTED_ARRAY\","
        + "\"createBitmapIndex\":false},{\"type\":\"string\",\"name\":\"page\","
        + "\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true}],"
        + "\"aggregators\":[{\"type\":\"longSum\",\"name\":\"total\",\"fieldName\":\"cnt\"}],"
        + "\"ordering\":[{\"columnName\":\"v0\",\"order\":\"ascending\"},"
        + "{\"columnName\":\"page\",\"order\":\"ascending\"}]}}]",
        projectionsJson()
    );
  }

  /**
   * A projection body is planned under the statement's own context, so a SET clause that changes how the equivalent
   * query would plan changes the stored definition the same way. Here the session time zone reaches the TIME_FLOOR.
   */
  @Test
  public void testProjectionBodyHonorsStatementContext() throws Exception
  {
    execute(
        "SET sqlTimeZone = 'America/Los_Angeles';\n"
        + "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION daily AS (SELECT TIME_FLOOR(__time, 'P1D'), page, SUM(cnt) AS total GROUP BY 1, 2))"
    );

    assertTrue(
        projectionsJson().contains("timestamp_floor(\\\"__time\\\",'P1D',null,'America/Los_Angeles')"),
        projectionsJson()
    );
  }

  /**
   * The overrides the lift depends on are applied on top of the statement's context, so a SET clause cannot put the
   * planner into a shape the lift does not understand.
   */
  @Test
  public void testProjectionBodyContextCannotOverrideDeterministicOverrides() throws Exception
  {
    execute(
        "SET sqlUseGranularity = TRUE;\n"
        + "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION daily AS (SELECT TIME_FLOOR(__time, 'P1D'), page, SUM(cnt) AS total GROUP BY 1, 2))"
    );

    // Still lifted as an ordinary grouping column rather than a query granularity, exactly as without the SET.
    assertTrue(
        projectionsJson().contains("timestamp_floor(\\\"__time\\\",'P1D',null,'UTC')"),
        projectionsJson()
    );
  }

  /**
   * A projection defined with TIME_FLOOR must carry a granularity the segment layer can recover, which is how the
   * projection gets matched to time-grouped queries.
   */
  @Test
  public void testProjectionGranularityIsRecoverable()
  {
    execute(
        "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION hourly AS (SELECT TIME_FLOOR(__time, 'PT1H'), page, SUM(cnt) AS total GROUP BY 1, 2))"
    );

    final AggregateProjectionSpec spec = projection(0).getSpec();
    final String timeColumn = spec.toMetadataSchema().getTimeColumnName();
    assertEquals("v0", timeColumn);
    assertEquals(
        Granularities.HOUR,
        Granularities.fromVirtualColumn(spec.getVirtualColumns().getVirtualColumn(timeColumn))
    );
  }

  @Test
  public void testProjectionWithFilter()
  {
    execute(
        "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION filtered AS (SELECT page, SUM(cnt) AS total WHERE page <> 'skip' GROUP BY page))"
    );
    assertEquals("!page = skip", projection(0).getSpec().getFilter().toString());
  }

  /**
   * A time bound written in the body is moved into the query's intervals during planning, and has to be put back:
   * a projection stores a filter, not an interval.
   */
  @Test
  public void testProjectionWithTimeFilter()
  {
    execute(
        "CREATE TABLE tbl (__time TIMESTAMP, page VARCHAR, cnt BIGINT,"
        + " PROJECTION recent AS (SELECT page, SUM(cnt) AS total"
        + " WHERE __time >= TIMESTAMP '2020-01-01 00:00:00' GROUP BY page))"
    );
    assertNotNull(projection(0).getSpec().getFilter(), "time filter must survive as a filter");
    assertTrue(projection(0).getSpec().getFilter().getRequiredColumns().contains("__time"));
  }

  @Test
  public void testProjectionSelectDistinct()
  {
    execute("CREATE TABLE tbl (a VARCHAR, PROJECTION d AS (SELECT DISTINCT a))");
    final AggregateProjectionSpec spec = projection(0).getSpec();
    assertEquals(1, spec.getGroupingColumns().size());
    assertEquals("a", spec.getGroupingColumns().get(0).getName());
    assertEquals(0, spec.getAggregators().length);
  }

  @Test
  public void testMultipleProjections()
  {
    execute(
        "CREATE TABLE tbl (a VARCHAR, b BIGINT,"
        + " PROJECTION p1 AS (SELECT a, SUM(b) AS s GROUP BY a),"
        + " PROJECTION p2 AS (SELECT b, COUNT(*) AS c GROUP BY b))"
    );
    assertEquals(List.of("p1", "p2"), List.of(projection(0).getSpec().getName(), projection(1).getSpec().getName()));
  }

  @Test
  public void testAlterTableAddProjection()
  {
    WRITER.existing.put(TableId.datasource("tbl"), tableWithColumns("a"));
    execute("ALTER TABLE tbl ADD PROJECTION p AS (SELECT a, COUNT(*) AS c GROUP BY a)");

    final RecordingCatalogTableWriter.Call call = WRITER.lastCall("addProjection");
    assertEquals("p", call.projection.getSpec().getName());
    assertFalse(call.ifNotExists);
  }

  @Test
  public void testAlterTableAddProjectionIfNotExists()
  {
    WRITER.existing.put(TableId.datasource("tbl"), tableWithColumns("a"));
    execute("ALTER TABLE tbl ADD IF NOT EXISTS PROJECTION p AS (SELECT a GROUP BY a)");
    assertTrue(WRITER.lastCall("addProjection").ifNotExists);
  }

  @Test
  public void testAlterTableDropProjection()
  {
    execute("ALTER TABLE tbl DROP PROJECTION p");
    final RecordingCatalogTableWriter.Call call = WRITER.lastCall("dropProjection");
    assertEquals("p", call.projectionName);
    assertFalse(call.ifExists);

    WRITER.reset();
    execute("ALTER TABLE tbl DROP PROJECTION IF EXISTS p");
    assertTrue(WRITER.lastCall("dropProjection").ifExists);
  }

  @Test
  public void testProjectionRejectsUnaliasedAggregate()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, b BIGINT, PROJECTION p AS (SELECT a, SUM(b) GROUP BY a))")
    );
    assertTrue(e.getMessage().contains("no name"), e.getMessage());
  }

  @Test
  public void testProjectionRejectsPostAggregation()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, b BIGINT, PROJECTION p AS (SELECT a, AVG(b) AS m GROUP BY a))")
    );
    assertTrue(e.getMessage().contains("expression over aggregates"), e.getMessage());
  }

  @Test
  public void testProjectionRejectsUnknownColumn()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, PROJECTION p AS (SELECT nope GROUP BY nope))")
    );
    assertTrue(e.getMessage().contains("nope"), e.getMessage());
  }

  @Test
  public void testProjectionRejectsNonAggregatingBody()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, PROJECTION p AS (SELECT a))")
    );
    assertTrue(e.getMessage().contains("does not aggregate"), e.getMessage());
  }

  /**
   * {@code __base} names the table's own layout and is handled separately; every other name beginning with the
   * reserved prefix stays unavailable.
   */
  @Test
  public void testProjectionRejectsOtherReservedNames()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("CREATE TABLE tbl (a VARCHAR, PROJECTION __other AS (SELECT a GROUP BY a))")
    );
    assertTrue(e.getMessage().contains("reserved name"), e.getMessage());
  }

  @Test
  public void testProjectionRejectsDuplicateName()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (a VARCHAR, PROJECTION p AS (SELECT a GROUP BY a),"
            + " PROJECTION p AS (SELECT a GROUP BY a))"
        )
    );
    assertTrue(e.getMessage().contains("declared more than once"), e.getMessage());
  }

  @SuppressWarnings("unchecked")
  private DatasourceProjectionMetadata projection(int index)
  {
    return ((List<DatasourceProjectionMetadata>) WRITER.calls.get(0).spec.properties().get("projections")).get(index);
  }

  private String projectionsJson() throws Exception
  {
    return queryFramework().queryJsonMapper()
                           .writeValueAsString(WRITER.calls.get(0).spec.properties().get("projections"));
  }

  /**
   * The reserved {@code __base} projection describes the table's own layout, so it becomes the baseTable property
   * rather than one of the projections. A computed column becomes a virtual column materializing the declared column
   * it fills.
   */
  @Test
  public void testCreateTableWithBaseProjection() throws Exception
  {
    execute(
        "CREATE TABLE tbl ("
        + " tenant VARCHAR,"
        + " bucket BIGINT,"
        + " __time TIMESTAMP,"
        + " user_id BIGINT,"
        + " PROJECTION __base AS ("
        + "   SELECT tenant, ABS(user_id) AS bucket, __time, user_id"
        + "   CLUSTERED BY tenant, bucket"
        + " )"
        + ") PARTITIONED BY DAY SEALED"
    );

    final TableSpec spec = WRITER.calls.get(0).spec;
    assertEquals(true, spec.properties().get(DatasourceDefn.SEALED_PROPERTY));
    assertNull(spec.properties().get(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY));
    assertEquals(
        "{\"clusteringColumns\":[\"tenant\",\"bucket\"],"
        + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"bucket\","
        + "\"expression\":\"abs(\\\"user_id\\\")\",\"outputType\":\"LONG\"}],"
        + "\"type\":\"clusteredValueGroups\"}",
        queryFramework().queryJsonMapper()
                        .writeValueAsString(spec.properties().get(DatasourceDefn.BASE_TABLE_PROPERTY))
    );
  }

  /**
   * A base table column is stored under the name it declares, so a body item that renames another column is rejected:
   * only a virtual column materializes a name the body did not select, and a bare reference produces none.
   * <p>
   * Selecting the same column twice also makes the scan's deduplicated column list shorter than the select list, so
   * the source of each item is taken from the query's output signature. Reading the scan's columns positionally used
   * to run off the end of that list and fail as an internal error.
   */
  @Test
  public void testBaseProjectionRenamedColumnFails()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (id BIGINT, copy BIGINT, __time TIMESTAMP,"
            + " PROJECTION __base AS (SELECT id, id AS copy, __time CLUSTERED BY id)) SEALED"
        )
    );
    assertTrue(
        e.getMessage().contains("its column 2 selects [id] but declares it as [copy]"),
        e.getMessage()
    );
  }

  @Test
  public void testBaseProjectionWithoutComputedColumns()
  {
    execute(
        "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, v BIGINT,"
        + " PROJECTION __base AS (SELECT tenant, __time, v CLUSTERED BY tenant)) SEALED"
    );
    final ClusteredValueGroupsBaseTableMetadata baseTable = baseTable();
    assertEquals(List.of("tenant"), baseTable.getClusteringColumns());
    assertEquals(0, baseTable.getVirtualColumns().getVirtualColumns().length);
  }

  /**
   * A base table and aggregate projections are different catalog entities and may coexist.
   */
  @Test
  public void testBaseProjectionAlongsideAggregateProjection()
  {
    execute(
        "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, v BIGINT,"
        + " PROJECTION __base AS (SELECT tenant, __time, v CLUSTERED BY tenant),"
        + " PROJECTION by_tenant AS (SELECT tenant, SUM(v) AS sum_v GROUP BY tenant)) SEALED"
    );
    final TableSpec spec = WRITER.calls.get(0).spec;
    assertNotNull(spec.properties().get(DatasourceDefn.BASE_TABLE_PROPERTY));
    assertEquals(1, ((List<?>) spec.properties().get(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY)).size());
  }

  @Test
  public void testBaseProjectionRequiresSealed()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP,"
            + " PROJECTION __base AS (SELECT tenant, __time CLUSTERED BY tenant))"
        )
    );
    assertTrue(e.getMessage().contains("must be declared SEALED"), e.getMessage());
  }

  /**
   * The body lists the columns in the order segments store them, so it must match the declaration exactly.
   */
  @Test
  public void testBaseProjectionColumnOrderMustMatch()
  {
    final DruidException wrongOrder = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, v BIGINT,"
            + " PROJECTION __base AS (SELECT __time, tenant, v CLUSTERED BY tenant)) SEALED"
        )
    );
    assertTrue(wrongOrder.getMessage().contains("the table declares"), wrongOrder.getMessage());

    final DruidException missing = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, v BIGINT,"
            + " PROJECTION __base AS (SELECT tenant, __time CLUSTERED BY tenant)) SEALED"
        )
    );
    assertTrue(missing.getMessage().contains("must name every declared column"), missing.getMessage());
  }

  /**
   * Clustering columns must lead the declared column list, since the declared order is the physical order. The
   * catalog enforces this on write; catching it here names the statement that caused it.
   */
  @Test
  public void testBaseProjectionClusteringMustBeLeadingPrefix()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, v BIGINT,"
            + " PROJECTION __base AS (SELECT tenant, __time, v CLUSTERED BY v)) SEALED"
        )
    );
    assertTrue(e.getMessage().contains("__base"), e.getMessage());
  }

  @Test
  public void testBaseProjectionRejectsFilterOrGrouping()
  {
    for (String body : new String[]{
        "SELECT tenant, __time WHERE tenant <> 'x'",
        "SELECT tenant, __time GROUP BY tenant, __time"
    }) {
      final DruidException e = assertThrows(
          DruidException.class,
          () -> execute(
              "CREATE TABLE tbl (tenant VARCHAR, __time TIMESTAMP, PROJECTION __base AS (" + body + ")) SEALED"
          ),
          body
      );
      assertTrue(e.getMessage().contains("filters or groups"), e.getMessage());
    }
  }

  /**
   * Only the base projection chooses a clustering; an aggregate projection is ordered by its grouping columns.
   */
  @Test
  public void testAggregateProjectionRejectsClusteredBy()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute(
            "CREATE TABLE tbl (a VARCHAR, b BIGINT,"
            + " PROJECTION p AS (SELECT a, SUM(b) AS s GROUP BY a CLUSTERED BY a))"
        )
    );
    assertTrue(e.getMessage().contains("cannot use CLUSTERED BY"), e.getMessage());
  }

  @Test
  public void testAlterTableAddBaseProjection()
  {
    WRITER.existing.put(
        TableId.datasource("tbl"),
        TableMetadata.newTable(
            TableId.datasource("tbl"),
            new TableSpec(
                DatasourceDefn.TABLE_TYPE,
                ImmutableMap.of(DatasourceDefn.SEALED_PROPERTY, true),
                List.of(
                    new ColumnSpec("tenant", "VARCHAR", null),
                    new ColumnSpec("__time", "TIMESTAMP", null)
                )
            )
        )
    );
    execute("ALTER TABLE tbl ADD PROJECTION __base AS (SELECT tenant, __time CLUSTERED BY tenant)");

    // Whether a layout already exists is the Coordinator's call, inside its update transaction, so the statement only
    // hands over the translated layout and the IF NOT EXISTS flag. EditorTest covers the rule itself.
    final RecordingCatalogTableWriter.Call call = WRITER.lastCall("setBaseTable");
    assertNotNull(call.baseTable);
    assertFalse(call.ifNotExists);

    execute("ALTER TABLE tbl ADD IF NOT EXISTS PROJECTION __base AS (SELECT tenant, __time CLUSTERED BY tenant)");
    assertTrue(WRITER.lastCall("setBaseTable").ifNotExists);
  }

  @Test
  public void testAlterTableDropBaseProjection()
  {
    WRITER.existing.put(
        TableId.datasource("tbl"),
        TableMetadata.newTable(
            TableId.datasource("tbl"),
            new TableSpec(
                DatasourceDefn.TABLE_TYPE,
                ImmutableMap.of(DatasourceDefn.BASE_TABLE_PROPERTY, ImmutableMap.of("type", "clusteredValueGroups")),
                List.of(new ColumnSpec("tenant", "VARCHAR", null))
            )
        )
    );
    execute("ALTER TABLE tbl DROP PROJECTION __base");
    assertFalse(WRITER.lastCall("dropBaseTable").ifExists);

    // Whether there is one to drop is likewise the Coordinator's call; the statement only carries IF EXISTS.
    execute("ALTER TABLE tbl DROP PROJECTION IF EXISTS __base");
    assertTrue(WRITER.lastCall("dropBaseTable").ifExists);
  }

  private ClusteredValueGroupsBaseTableMetadata baseTable()
  {
    return (ClusteredValueGroupsBaseTableMetadata)
        WRITER.calls.get(0).spec.properties().get(DatasourceDefn.BASE_TABLE_PROPERTY);
  }

  private void execute(String sql)
  {
    statement(sql).execute();
  }

  private DirectStatement statement(String sql)
  {
    return getSqlStatementFactory(PlannerConfig.builder().enableCatalogDdl(true).build(), new AuthConfig())
        .directStatement(
        SqlQueryPlus.builder(sql).auth(CalciteTests.SUPER_USER_AUTH_RESULT).build()
    );
  }

  private static TableMetadata tableWithColumns(String... names)
  {
    final List<ColumnSpec> columns = new ArrayList<>();
    for (String name : names) {
      columns.add(new ColumnSpec(name, "VARCHAR", null));
    }
    return TableMetadata.newTable(
        TableId.datasource("tbl"),
        new TableSpec(DatasourceDefn.TABLE_TYPE, ImmutableMap.of(), columns)
    );
  }

  /**
   * Records what a DDL statement asked the catalog to do, so tests can assert on the resulting operation rather
   * than on a Coordinator round trip.
   */
  private static class RecordingCatalogTableWriter implements CatalogTableWriter
  {
    static class Call
    {
      String operation;
      TableId tableId;
      TableSpec spec;
      boolean ifNotExists;
      boolean replace;
      List<ColumnSpec> columns;
      List<String> droppedColumns;
      Map<String, Object> properties;
      DatasourceProjectionMetadata projection;
      String projectionName;
      boolean ifExists;
      DatasourceBaseTableMetadata baseTable;
    }

    final List<Call> calls = new ArrayList<>();
    final Map<TableId, TableMetadata> existing = new HashMap<>();

    void reset()
    {
      calls.clear();
      existing.clear();
    }

    Call lastCall(String operation)
    {
      for (int i = calls.size() - 1; i >= 0; i--) {
        if (operation.equals(calls.get(i).operation)) {
          return calls.get(i);
        }
      }
      throw new AssertionError("No call to [" + operation + "] in " + calls);
    }

    @Override
    public void createTable(TableId tableId, TableSpec spec, boolean ifNotExists, boolean replace)
    {
      final Call call = record("createTable", tableId);
      call.spec = spec;
      call.ifNotExists = ifNotExists;
      call.replace = replace;
    }

    @Override
    public void addColumns(TableId tableId, List<ColumnSpec> columns)
    {
      record("addColumns", tableId).columns = columns;
    }

    @Override
    public void alterColumns(TableId tableId, List<ColumnSpec> columns)
    {
      record("alterColumns", tableId).columns = columns;
    }

    @Override
    public void dropColumns(TableId tableId, List<String> columns)
    {
      record("dropColumns", tableId).droppedColumns = columns;
    }

    @Override
    public void updateProperties(TableId tableId, Map<String, Object> properties)
    {
      record("updateProperties", tableId).properties = properties;
    }

    @Override
    public void addProjection(TableId tableId, DatasourceProjectionMetadata projection, boolean ifNotExists)
    {
      final Call call = record("addProjection", tableId);
      call.projection = projection;
      call.ifNotExists = ifNotExists;
    }

    @Override
    public void dropProjection(TableId tableId, String projectionName, boolean ifExists)
    {
      final Call call = record("dropProjection", tableId);
      call.projectionName = projectionName;
      call.ifExists = ifExists;
    }

    @Override
    public void setBaseTable(TableId tableId, DatasourceBaseTableMetadata baseTable, boolean ifNotExists)
    {
      final Call call = record("setBaseTable", tableId);
      call.baseTable = baseTable;
      call.ifNotExists = ifNotExists;
    }

    @Override
    public void dropBaseTable(TableId tableId, boolean ifExists)
    {
      record("dropBaseTable", tableId).ifExists = ifExists;
    }

    @Nullable
    @Override
    public TableMetadata readTable(TableId tableId)
    {
      return existing.get(tableId);
    }

    private Call record(String operation, TableId tableId)
    {
      final Call call = new Call();
      call.operation = operation;
      call.tableId = tableId;
      calls.add(call);
      return call;
    }
  }
}
