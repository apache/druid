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

package org.apache.druid.catalog;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.http.CatalogResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

import static org.apache.druid.catalog.DummyRequest.deleteBy;
import static org.apache.druid.catalog.DummyRequest.getBy;
import static org.apache.druid.catalog.DummyRequest.postBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of REST API operations for the table catalog.
 */
public class CatalogResourceTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogResource resource;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    resource = new CatalogResource(dbFixture.storage);
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private static long getVersion(Response resp)
  {
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) resp.getEntity();
    return (Long) result.get("version");
  }

  @Test
  public void testCreate()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();

    // Missing schema name: infer the schema.
    String tableName = "create";
    TableMetadata table = TableMetadata.newTable(
        null,
        "create1",
        defn);
    Response resp = resource.createTable(table, false, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Blank schema name: infer the schema.
    table = TableMetadata.newTable(
        "",
        "create2",
        defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Missing table name
    table = TableMetadata.newTable(TableId.DRUID_SCHEMA, null, defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    table = TableMetadata.newTable("bogus", tableName, defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    table = TableMetadata.newTable(TableId.CATALOG_SCHEMA, tableName, defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Wrong definition type.
    table = TableMetadata.newTable(TableId.INPUT_SCHEMA, tableName, defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.DENY_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // No permissions
    table = TableMetadata.newTable(TableId.DRUID_SCHEMA, tableName, defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.DENY_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Read permission
    resp = resource.createTable(table, false, postBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Write permission
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > 0);

    // Duplicate
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Duplicate, "if not exists"
    resp = resource.createTable(table, true, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(0, getVersion(resp));

    // Input source
    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = CatalogTests.csvFormat();
    InputTableSpec inputDefn = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();
    table = TableMetadata.newTable(TableId.INPUT_SCHEMA, "input", inputDefn);
    resp = resource.createTable(table, true, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testUpdate()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();

    // Missing schema name
    String tableName = "update";
    Response resp = resource.updateTableDefn("", tableName, defn, 0, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, null, defn, 0, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.updateTableDefn("bogus", tableName, defn, 0, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.updateTableDefn(TableId.CATALOG_SCHEMA, tableName, defn, 0, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // TODO: Wrong definition type.

    // Does not exist
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, tableName, defn, 0, postBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    TableMetadata table = TableMetadata.newTable(
        TableId.DRUID_SCHEMA,
        "update",
        defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // No update permission
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, tableName, defn, 0, postBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Out-of-date version
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, tableName, defn, 10, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Valid version
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, tableName, defn, version, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // Overwrite
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, tableName, defn, 0, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
  }

  @Test
  public void testRead()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();

    // Missing schema name
    String tableName = "read";
    Response resp = resource.getTable("", tableName, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.getTable(TableId.DRUID_SCHEMA, null, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.getTable("bogus", tableName, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    TableMetadata table = TableMetadata.newTable(
        TableId.DRUID_SCHEMA,
        tableName,
        defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    table = table.fromInsert(TableId.DRUID_SCHEMA, getVersion(resp));

    // No read permission
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(DummyRequest.DENY_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Valid
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(table, read);

    // Internal sync API
    resp = resource.syncTable(TableId.DRUID_SCHEMA, tableName, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    read = (TableMetadata) resp.getEntity();
    assertEquals(table, read);
  }

  @SuppressWarnings("unchecked")
  private List<TableId> getTableIdList(Response resp)
  {
    return (List<TableId>) resp.getEntity();
  }

  @SuppressWarnings("unchecked")
  private List<String> getTableList(Response resp)
  {
    return (List<String>) resp.getEntity();
  }

  @SuppressWarnings("unchecked")
  private List<TableMetadata> getDetailsList(Response resp)
  {
    return (List<TableMetadata>) resp.getEntity();
  }

  @Test
  public void testList()
  {
    // No entries
    Response resp = resource.listTables(getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableId> tableIds = getTableIdList(resp);
    assertTrue(tableIds.isEmpty());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<String> tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Missing schema
    resp = resource.listTables(null, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Invalid schema
    resp = resource.listTables("bogus", getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create a table
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();
    TableMetadata table = TableMetadata.newTable(TableId.DRUID_SCHEMA, "list", defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No read access
    resp = resource.listTables(getBy(DummyRequest.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertTrue(tableIds.isEmpty());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Read access
    resp = resource.listTables(getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertEquals(1, tableIds.size());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(1, tables.size());

    resp = resource.listTables(TableId.SYSTEM_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Internal sync schema API
    resp = resource.syncSchema(TableId.SYSTEM_SCHEMA, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    resp = resource.syncSchema(TableId.DRUID_SCHEMA, getBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableMetadata> details = getDetailsList(resp);
    assertEquals(1, details.size());
  }

  @Test
  public void testDelete()
  {
    // Missing schema name
    String tableName = "delete";
    Response resp = resource.deleteTable("", tableName, false, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, null, false, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.deleteTable("bogus", tableName, false, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.deleteTable(TableId.CATALOG_SCHEMA, tableName, false, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, true, deleteBy(DummyRequest.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Create the table
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.DRUID_SCHEMA,
        tableName,
        defn);
    resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No write permission
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Write permission
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, true, deleteBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testLifecycle()
  {
    // Operations for one table - create
    String table1Name = "lifecycle1";
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();
    TableMetadata table = TableMetadata.newTable(TableId.DRUID_SCHEMA, table1Name, defn);
    Response resp = resource.createTable(table, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);
    table = table.fromInsert(TableId.DRUID_SCHEMA, version);

    // read
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, postBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(table, read);

    // list
    resp = resource.listTables(getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableId> tableIds = getTableIdList(resp);
    assertEquals(1, tableIds.size());
    assertEquals(table.id(), tableIds.get(0));

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<String> tables = getTableList(resp);
    assertEquals(1, tables.size());
    assertEquals(table.name(), tables.get(0));

    // update
    DatasourceSpec defn2 = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .build();
    resp = resource.updateTableDefn(TableId.DRUID_SCHEMA, table1Name, defn2, version, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // verify update
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    read = (TableMetadata) resp.getEntity();
    assertEquals(table.creationTime(), read.creationTime());
    assertEquals(version, read.updateTime());
    assertEquals(defn2, read.spec());

    // add second table
    String table2Name = "lifecycle2";
    TableMetadata table2 = TableMetadata.newTable(TableId.DRUID_SCHEMA, table2Name, defn);
    resp = resource.createTable(table2, false, postBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // verify lists
    resp = resource.listTables(getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertEquals(2, tableIds.size());
    assertEquals(table.id(), tableIds.get(0));
    assertEquals(table2.id(), tableIds.get(1));

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(2, tables.size());
    assertEquals(table.name(), tables.get(0));
    assertEquals(table2.name(), tables.get(1));

    // delete and verify
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table1Name, false, deleteBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(1, tables.size());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table2Name, false, deleteBy(DummyRequest.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(DummyRequest.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(0, tables.size());
  }
}
