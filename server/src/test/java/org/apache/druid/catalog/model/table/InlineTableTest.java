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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@Category(CatalogTest.class)
public class InlineTableTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final InlineTableDefn tableDefn = new InlineTableDefn();
  private final TableBuilder baseBuilder = TableBuilder.of(tableDefn)
      .description("inline input")
      .format(InputFormats.CSV_FORMAT_TYPE)
      .column("x", Columns.VARCHAR)
      .column("y", Columns.BIGINT);

  @Test
  public void testEmptyData()
  {
    ResolvedTable table = baseBuilder.buildResolved(mapper);

    // Check validation
    assertThrows(IAE.class, () -> table.validate());
  }

  @Test
  public void testValidData()
  {
    ResolvedTable table = baseBuilder.copy()
        .data("a,b", "c,d")
        .buildResolved(mapper);

    // Check validation
    table.validate();

    // Check registry
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    assertNotNull(registry.resolve(table.spec()));

    // Convert to an external spec
    ExternalTableSpec externSpec = tableDefn.convertToExtern(table);

    InlineInputSource inlineSpec = (InlineInputSource) externSpec.inputSource();
    assertEquals("a,b\nc,d\n", inlineSpec.getData());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat();
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature();
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }
}
