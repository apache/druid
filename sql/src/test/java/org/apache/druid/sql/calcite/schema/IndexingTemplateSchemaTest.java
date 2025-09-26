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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.catalog.MapMetadataCatalog;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.IndexingTemplateDefn;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.table.InlineTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexingTemplateSchemaTest
{
  private IndexingTemplateSchema schema;
  private MapMetadataCatalog catalog;

  @BeforeEach
  public void setUp()
  {
    catalog = new MapMetadataCatalog(TestHelper.JSON_MAPPER);
    schema = new IndexingTemplateSchema(catalog);
  }

  @Test
  public void test_getTableNames_returnsAllKnownTemplateIds()
  {
    Assertions.assertTrue(schema.getTableNames().isEmpty());

    final String msqTemplate = "msq_1";
    final String compactTemplate = "compact_2";
    catalog.addSpec(
        TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, msqTemplate),
        new TableSpec(IndexingTemplateDefn.TYPE, null, null)
    );
    catalog.addSpec(
        TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, compactTemplate),
        new TableSpec(IndexingTemplateDefn.TYPE, null, null)
    );

    final Set<String> tableNames = schema.getTableNames();
    Assertions.assertEquals(Set.of(msqTemplate, compactTemplate), tableNames);
  }

  @Test
  public void test_getTable_returnsNull_forUnknownTable()
  {
    Assertions.assertNull(schema.getTable("msq"));
  }

  @Test
  public void test_getTable_returnsTable()
  {
    // Add a template to the catalog
    final String msqTemplate = "msq_1";
    catalog.addSpec(
        TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, msqTemplate),
        new TableSpec(
            IndexingTemplateDefn.TYPE,
            Map.of("payload", Map.of("type", "msq", "granularity", "DAY")),
            null
        )
    );

    final InlineTable inlineTable = Assertions.assertInstanceOf(
        InlineTable.class,
        schema.getTable(msqTemplate)
    );
    Assertions.assertNotNull(inlineTable);

    final InlineDataSource dataSource = Assertions.assertInstanceOf(
        InlineDataSource.class,
        inlineTable.getDataSource()
    );
    Assertions.assertEquals(
        List.of("type", "payload"),
        dataSource.getColumnNames()
    );
    Assertions.assertEquals(
        List.of(ColumnType.STRING, ColumnType.UNKNOWN_COMPLEX),
        dataSource.getColumnTypes()
    );

    final List<Object[]> rows = dataSource.getRowsAsList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(
        rows.get(0),
        new Object[]{"msq", Map.of("type", "msq", "granularity", "DAY")}
    );
  }
}
