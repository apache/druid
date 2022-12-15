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

package org.apache.druid.catalog.model;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.TableMetadata.TableState;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

@Category(CatalogTest.class)
public class TableMetadataTest
{
  @Test
  public void testId()
  {
    TableId id1 = new TableId("schema", "table");
    assertEquals(id1, id1);
    assertEquals("schema", id1.schema());
    assertEquals("table", id1.name());
    assertEquals("\"schema\".\"table\"", id1.sqlName());
    assertEquals(id1.sqlName(), id1.toString());

    TableId id2 = TableId.datasource("ds");
    assertEquals(TableId.DRUID_SCHEMA, id2.schema());
    assertEquals("ds", id2.name());
  }

  @Test
  public void testIdEquals()
  {
    EqualsVerifier.forClass(TableId.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testTableMetadata()
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    {
      TableMetadata table = new TableMetadata(
          TableId.datasource("foo"),
          10,
          20,
          TableState.ACTIVE,
          spec
      );
      table.validate();
      assertEquals(TableId.DRUID_SCHEMA, table.id().schema());
      assertEquals("foo", table.id().name());
      assertEquals(10, table.creationTime());
      assertEquals(20, table.updateTime());
      assertEquals(TableState.ACTIVE, table.state());
      assertNotNull(table.spec());
    }

    {
      TableMetadata table = TableMetadata.newTable(
          TableId.of(null, "foo"),
          spec
      );
      assertThrows(IAE.class, () -> table.validate());
    }

    {
      TableMetadata table = TableMetadata.newTable(
          TableId.of(TableId.DRUID_SCHEMA, null),
          spec
      );
      assertThrows(IAE.class, () -> table.validate());
    }
  }
  @Test
  public void testConversions()
  {
    Map<String, Object> props = ImmutableMap.of(
        AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(
        TableId.datasource("ds"),
        spec
    );
    assertEquals(TableId.datasource("ds"), table.id());
    assertEquals(TableState.ACTIVE, table.state());
    assertEquals(0, table.updateTime());
    assertSame(spec, table.spec());

    TableMetadata table2 = TableMetadata.newTable(
        TableId.datasource("ds"),
        spec
    );
    assertEquals(table, table2);

    TableMetadata table3 = table2.fromInsert(10);
    assertEquals(10, table3.creationTime());
    assertEquals(10, table3.updateTime());

    table3 = table3.asUpdate(20);
    assertEquals(10, table3.creationTime());
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
