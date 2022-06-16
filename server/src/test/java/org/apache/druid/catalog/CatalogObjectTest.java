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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class CatalogObjectTest
{
  @Test
  public void testMinimalTable()
  {
    TableMetadata table = new TableMetadata(
        TableId.DRUID_SCHEMA,
        "foo",
        "bob",
        10,
        20,
        TableState.ACTIVE,
        null);
    table.validate();
    assertEquals(TableId.DRUID_SCHEMA, table.dbSchema());
    assertEquals("foo", table.name());
    assertEquals("bob", table.owner());
    assertEquals(10, table.creationTime());
    assertEquals(20, table.updateTime());
    assertEquals(TableState.ACTIVE, table.state());
    assertNull(table.defn());

    try {
      table = new TableMetadata(
          null,
          "foo",
          "bob",
          10,
          20,
          TableState.ACTIVE,
          null);
      table.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      table = new TableMetadata(
          TableId.DRUID_SCHEMA,
          null,
          "bob",
          10,
          20,
          TableState.ACTIVE,
          null);
      table.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testDefn()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();
    TableMetadata table = new TableMetadata(
        TableId.DRUID_SCHEMA,
        "foo",
        "bob",
        10,
        20,
        TableState.ACTIVE,
        defn);
    table.validate();
    assertSame(defn, table.defn());

    try {
      table = new TableMetadata(
          "wrong",
          "foo",
          "bob",
          10,
          20,
          TableState.ACTIVE,
          defn);
      table.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testConversions()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();
    TableMetadata table = TableMetadata.newSegmentTable(
        "ds",
        defn);
    assertEquals(TableId.datasource("ds"), table.id());
    assertEquals(TableState.ACTIVE, table.state());
    assertEquals(0, table.updateTime());
    assertSame(defn, table.defn());

    TableMetadata table2 = TableMetadata.newSegmentTable("ds", defn);
    assertEquals(table, table2);

    TableMetadata table3 = table2.asUpdate(20);
    assertEquals(20, table3.updateTime());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableMetadata.class)
                  .usingGetClass()
                  .verify();
  }
}
