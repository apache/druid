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
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.error.DruidException;
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
    WRITER.existing.put(TableId.datasource("tbl"), tableWithColumns("a"));
    execute("ALTER TABLE tbl ADD COLUMN b BIGINT");

    final RecordingCatalogTableWriter.Call call = WRITER.lastCall("updateColumns");
    assertEquals(ImmutableList.of(new ColumnSpec("b", "BIGINT", null)), call.columns);
  }

  @Test
  public void testAlterTableAddExistingColumnFails()
  {
    WRITER.existing.put(TableId.datasource("tbl"), tableWithColumns("a"));
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("ALTER TABLE tbl ADD COLUMN a BIGINT")
    );
    assertTrue(e.getMessage().contains("Column [a] already exists"));
  }

  @Test
  public void testAlterTableAddColumnToMissingTableFails()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> execute("ALTER TABLE tbl ADD COLUMN a BIGINT")
    );
    assertTrue(e.getMessage().contains("does not have a catalog entry"));
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
    // Unlike ADD COLUMN, changing a type does not require the column to be absent, so no read is needed.
    execute("ALTER TABLE tbl ALTER COLUMN cnt SET DATA TYPE DOUBLE");
    assertEquals(
        ImmutableList.of(new ColumnSpec("cnt", "DOUBLE", null)),
        WRITER.lastCall("updateColumns").columns
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
    public void updateColumns(TableId tableId, List<ColumnSpec> columns)
    {
      record("updateColumns", tableId).columns = columns;
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
