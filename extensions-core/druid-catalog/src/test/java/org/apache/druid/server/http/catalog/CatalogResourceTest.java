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

package org.apache.druid.server.http.catalog;

import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.http.TableEditRequest.DropColumns;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.BaseExternTableTest;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test of REST API operations for the table catalog.
 */
public class CatalogResourceTest
{
  public static final String GET = "GET";
  public static final String POST = "POST";
  public static final String DELETE = "DELETE";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogResource resource;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    resource = new CatalogResource(dbFixture.storage, CatalogTests.AUTH_MAPPER);
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
    return CatalogUtils.getLong(result, "version");
  }

  @Test
  public void testCreate()
  {
    final String tableName = "create";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();

    // Blank schema name: infer the schema.
    Response resp = resource.postTable("", tableName, dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.postTable(TableId.DRUID_SCHEMA, "", dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Invalid table name
    resp = resource.postTable(TableId.DRUID_SCHEMA, " bogus ", dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.postTable("bogus", tableName, dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.postTable(TableId.CATALOG_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Wrong definition type.
    resp = resource.postTable(TableId.EXTERNAL_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // No permissions
    assertThrows(
        ForbiddenException.class,
        () -> resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.DENY_USER))
    );

    // Read permission
    assertThrows(
        ForbiddenException.class,
        () -> resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.READER_USER))
    );

    // Write permission
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > 0);

    // Duplicate
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Inline input source
    TableSpec inputSpec = TableBuilder.external("inline")
        .inputSource(toMap(new InlineInputSource("a,b,1\nc,d,2\n")))
        .inputFormat(BaseExternTableTest.CSV_FORMAT)
        .column("a", Columns.STRING)
        .column("b", Columns.STRING)
        .column("c", Columns.LONG)
        .buildSpec();
    resp = resource.postTable(TableId.EXTERNAL_SCHEMA, "inline", inputSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Wrong spec type
    resp = resource.postTable(TableId.DRUID_SCHEMA, "invalid", inputSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
  }

  private Map<String, Object> toMap(Object obj)
  {
    try {
      return dbFixture.storage.jsonMapper().convertValue(obj, ExternalTableDefn.MAP_TYPE_REF);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
  }

  @Test
  public void testUpdate()
  {
    final String tableName = "update";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();

    // Does not exist
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 10, false, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // No update permission
    assertThrows(
        ForbiddenException.class,
        () -> resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.READER_USER))
    );

    // Out-of-date version
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 10, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Valid version
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, version, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // Overwrite
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, true, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
  }

  @Test
  public void testRead()
  {
    final String tableName = "read";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();

    // Missing schema name
    Response resp = resource.getTable("", tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.getTable(TableId.DRUID_SCHEMA, null, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.getTable("bogus", tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // No read permission
    assertThrows(
        ForbiddenException.class,
        () -> resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.DENY_USER))
    );

    // Valid
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(version, read.updateTime());
    assertEquals(dsSpec, read.spec());

    // Internal sync API
    resp = resource.syncTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    read = (TableMetadata) resp.getEntity();
    assertEquals(version, read.updateTime());
    assertEquals(dsSpec, read.spec());
  }

  @SuppressWarnings("unchecked")
  private Set<String> getSchemaSet(Response resp)
  {
    return (Set<String>) resp.getEntity();
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
  public void testGetSchemas()
  {
    // Invalid format
    Response resp = resource.getSchemas("bogus", getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Schema names (default)
    resp = resource.getSchemas(null, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getSchemaSet(resp).contains("druid"));

    resp = resource.getSchemas("", getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getSchemaSet(resp).contains("druid"));

    // Schema names
    resp = resource.getSchemas(CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getSchemaSet(resp).contains("druid"));

    // Table paths - no entries
    resp = resource.getSchemas(CatalogResource.PATH_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getTableIdList(resp).isEmpty());

    // Table metadata - no entries
    resp = resource.getSchemas(CatalogResource.METADATA_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getTableIdList(resp).isEmpty());

    // Create a table
    final String tableName = "list";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();
    resp = resource.postTable(TableId.DRUID_SCHEMA, "list", dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Table paths - no read access
    resp = resource.getSchemas(CatalogResource.PATH_FORMAT, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getTableIdList(resp).isEmpty());

    // Table metadata - no read access
    resp = resource.getSchemas(CatalogResource.METADATA_FORMAT, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    // Table paths - read access
    resp = resource.getSchemas(CatalogResource.PATH_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(1, getTableIdList(resp).size());
    assertEquals(tableName, getTableIdList(resp).get(0).name());

    // Table metadata - read access
    resp = resource.getSchemas(CatalogResource.METADATA_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableMetadata> tables = getDetailsList(resp);
    assertEquals(1, tables.size());
    assertEquals(tableName, tables.get(0).id().name());
    assertEquals(1, tables.get(0).spec().properties().size());
  }

  @Test
  public void testGetSchemaTables()
  {
    // No entries
    Response resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getTableList(resp).isEmpty());

    // Missing schema
    resp = resource.getSchemaTables(null, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Invalid schema
    resp = resource.getSchemaTables("bogus", CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create a table
    final String tableName = "list";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();
    resp = resource.postTable(TableId.DRUID_SCHEMA, "list", dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No read access - name
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getTableIdList(resp).isEmpty());

    // No read access - metadata
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.METADATA_FORMAT, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    // No read access - status
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.STATUS_FORMAT, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    // Read access - name
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(Collections.singletonList(tableName), getTableList(resp));

    // Read access - metadata
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.METADATA_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(1, getDetailsList(resp).size());
    TableMetadata table = getDetailsList(resp).get(0);
    assertEquals(TableId.datasource(tableName), table.id());
    assertEquals(1, table.spec().properties().size());

    // Read access - status
    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.STATUS_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(1, getDetailsList(resp).size());
    table = getDetailsList(resp).get(0);
    assertEquals(TableId.datasource(tableName), table.id());
    assertEquals(DatasourceDefn.TABLE_TYPE, table.spec().type());
    assertTrue(table.spec().properties().isEmpty());
  }

  @Test
  public void testSync()
  {
    final String tableName = "sync";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, "list", dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Internal sync schema API
    resp = resource.syncSchema(TableId.SYSTEM_SCHEMA, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    resp = resource.syncSchema(TableId.DRUID_SCHEMA, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableMetadata> details = getDetailsList(resp);
    assertEquals(1, details.size());
  }

  @Test
  public void testDelete()
  {
    // Missing schema name
    String tableName = "delete";
    Response resp = resource.deleteTable("", tableName, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, null, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.deleteTable("bogus", tableName, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.deleteTable(TableId.CATALOG_SCHEMA, tableName, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D").buildSpec();
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No write permission
    assertThrows(
        ForbiddenException.class,
        () -> resource.deleteTable(TableId.DRUID_SCHEMA, tableName, deleteBy(CatalogTests.READER_USER))
    );

    // Write permission
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testLifecycle()
  {
    // Operations for one table - create
    String table1Name = "lifecycle1";
    TableSpec dsSpec = TableBuilder.datasource(table1Name, "P1D").buildSpec();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, table1Name, dsSpec, 0, true, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // read
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read1 = (TableMetadata) resp.getEntity();
    TableId id1 = TableId.of(TableId.DRUID_SCHEMA, table1Name);
    assertEquals(id1, read1.id());
    assertEquals(version, read1.updateTime());
    assertEquals(dsSpec, read1.spec());

    // list
    resp = resource.getSchemas(CatalogResource.PATH_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableId> tableIds = getTableIdList(resp);
    assertEquals(1, tableIds.size());
    assertEquals(id1, tableIds.get(0));

    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<String> tables = getTableList(resp);
    assertEquals(1, tables.size());
    assertEquals(id1.name(), tables.get(0));

    // update
    TableSpec table2Spec = TableBuilder.datasource(table1Name, "PT1H").buildSpec();
    resp = resource.postTable(TableId.DRUID_SCHEMA, table1Name, table2Spec, version, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // verify update
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(read1.creationTime(), read.creationTime());
    assertEquals(version, read.updateTime());
    assertEquals(table2Spec, read.spec());

    // add second table
    String table2Name = "lifecycle2";
    resp = resource.postTable(TableId.DRUID_SCHEMA, table2Name, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableId id2 = TableId.of(TableId.DRUID_SCHEMA, table2Name);

    // verify lists
    resp = resource.getSchemas(CatalogResource.PATH_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertEquals(2, tableIds.size());
    assertEquals(id1, tableIds.get(0));
    assertEquals(id2, tableIds.get(1));

    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(2, tables.size());
    assertEquals(id1.name(), tables.get(0));
    assertEquals(id2.name(), tables.get(1));

    // delete and verify
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table1Name, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(1, tables.size());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table2Name, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getSchemaTables(TableId.DRUID_SCHEMA, CatalogResource.NAME_FORMAT, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(0, tables.size());
  }

  @Test
  public void testMoveColumn()
  {
    String tableName = "move";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .buildSpec();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    MoveColumn cmd = new MoveColumn("foo", MoveColumn.Position.FIRST, null);
    resp = resource.editTable("bogus", tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Bad table
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // No target column
    cmd = new MoveColumn(null, MoveColumn.Position.FIRST, null);
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // No anchor column
    cmd = new MoveColumn("a", MoveColumn.Position.BEFORE, null);
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Move first
    cmd = new MoveColumn("c", MoveColumn.Position.FIRST, null);
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(
        Arrays.asList("c", "a", "b"),
        CatalogUtils.columnNames(read.spec().columns())
    );
  }

  @Test
  public void testHideColumns()
  {
    String tableName = "hide";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D")
         .buildSpec();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    TableEditRequest cmd = new HideColumns(null);
    resp = resource.editTable("bogus", tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Bad table. OK because there is nothing to do.
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Nothing to do
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertNull(read.spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY));

    // Hide
    cmd = new HideColumns(Arrays.asList("a", "b"));
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertEquals(
        Arrays.asList("a", "b"),
        read.spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
    assertTrue(read.updateTime() > version);

    // Unhide
    cmd = new UnhideColumns(Arrays.asList("a", "e"));
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertEquals(
        Collections.singletonList("b"),
        read.spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );
    assertTrue(read.updateTime() > version);
  }

  @Test
  public void testDropColumns()
  {
    String tableName = "drop";
    TableSpec dsSpec = TableBuilder.datasource(tableName, "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .buildSpec();

    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, 0, false, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    DropColumns cmd = new DropColumns(Collections.emptyList());
    resp = resource.editTable("bogus", tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Nothing to do
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    cmd = new DropColumns(null);
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(
        CatalogUtils.columnNames(dsSpec.columns()),
        CatalogUtils.columnNames(read.spec().columns())
    );

    // Bad table
    cmd = new DropColumns(Arrays.asList("a", "c"));
    resp = resource.editTable(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Drop
    resp = resource.editTable(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertTrue(read.updateTime() > version);
    assertEquals(
        Collections.singletonList("b"),
        CatalogUtils.columnNames(read.spec().columns())
    );
  }

  private static MockHttpServletRequest makeRequest(String method, String user, String contentType)
  {
    final MockHttpServletRequest retVal = new MockHttpServletRequest();
    retVal.method = method;
    retVal.attributes.put(
        AuthConfig.DRUID_AUTHENTICATION_RESULT,
        new AuthenticationResult(user, CatalogTests.TEST_AUTHORITY, null, null
        )
    );
    retVal.contentType = contentType;
    return retVal;
  }

  private static MockHttpServletRequest postBy(String user)
  {
    return makeRequest(POST, user, null);
  }

  private static MockHttpServletRequest getBy(String user)
  {
    return makeRequest(GET, user, null);
  }

  private static MockHttpServletRequest deleteBy(String user)
  {
    return makeRequest(DELETE, user, null);
  }
}
