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
import org.apache.druid.catalog.model.TableMetadata.TableState;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;


@Tag("CatalogTest")
public class TableMetadataTest
{
  @Test
  public void testId()
  {
    TableId id1 = new TableId("schema", "table");
    Assertions.assertEquals("schema", id1.schema());
    Assertions.assertEquals("table", id1.name());
    Assertions.assertEquals("\"schema\".\"table\"", id1.sqlName());
    Assertions.assertEquals(id1.sqlName(), id1.toString());

    TableId id2 = TableId.datasource("ds");
    Assertions.assertEquals(TableId.DRUID_SCHEMA, id2.schema());
    Assertions.assertEquals("ds", id2.name());
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
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
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
      Assertions.assertEquals(TableId.DRUID_SCHEMA, table.id().schema());
      Assertions.assertEquals("foo", table.id().name());
      Assertions.assertEquals(10, table.creationTime());
      Assertions.assertEquals(20, table.updateTime());
      Assertions.assertEquals(TableState.ACTIVE, table.state());
      Assertions.assertNotNull(table.spec());
    }

    {
      // Missing schema
      TableMetadata table = TableMetadata.newTable(
          TableId.of(null, "foo"),
          spec
      );
      Assertions.assertThrows(IAE.class, () -> table.validate());
    }

    {
      // Missing table name
      TableMetadata table = TableMetadata.newTable(
          TableId.of(TableId.DRUID_SCHEMA, null),
          spec
      );
      Assertions.assertThrows(IAE.class, () -> table.validate());
    }
  }
  @Test
  public void testConversions()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.TABLE_TYPE, props, null);
    TableMetadata table = TableMetadata.newTable(
        TableId.datasource("ds"),
        spec
    );
    Assertions.assertEquals(TableId.datasource("ds"), table.id());
    Assertions.assertEquals(TableState.ACTIVE, table.state());
    Assertions.assertEquals(0, table.updateTime());
    Assertions.assertSame(spec, table.spec());

    TableMetadata table2 = TableMetadata.newTable(
        TableId.datasource("ds"),
        spec
    );
    Assertions.assertEquals(table, table2);

    TableMetadata table3 = table2.fromInsert(10);
    Assertions.assertEquals(10, table3.creationTime());
    Assertions.assertEquals(10, table3.updateTime());

    table3 = table3.asUpdate(20);
    Assertions.assertEquals(10, table3.creationTime());
    Assertions.assertEquals(20, table3.updateTime());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableMetadata.class)
                  .usingGetClass()
                  .verify();
  }
}
