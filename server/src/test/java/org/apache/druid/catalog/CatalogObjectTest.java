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
    TableSpec table = new TableSpec(
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
      table = new TableSpec(
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
      table = new TableSpec(
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
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .build();
    TableSpec table = new TableSpec(
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
      table = new TableSpec(
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
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .build();
    TableSpec table = TableSpec.newSegmentTable(
        "ds",
        defn);
    assertEquals(TableId.datasource("ds"), table.id());
    assertEquals(TableState.ACTIVE, table.state());
    assertEquals(0, table.updateTime());
    assertSame(defn, table.defn());

    TableSpec table2 = TableSpec.newSegmentTable("ds", defn);
    assertEquals(table, table2);

    TableSpec table3 = table2.asUpdate(20);
    assertEquals(20, table3.updateTime());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
