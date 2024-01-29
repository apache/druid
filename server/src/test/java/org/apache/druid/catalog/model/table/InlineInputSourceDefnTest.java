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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.model.table.InputFormats.FlatTextFormatDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InlineInputSourceDefnTest extends BaseExternTableTest
{
  @Test
  public void testValidateEmptyInputSource()
  {
    // No data property: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", InlineInputSource.TYPE_KEY))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateNoFormat()
  {
    // No format: not valid. For inline, format must be provided to match data
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .column("x", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateNoColumns()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(CSV_FORMAT)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateGood()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testFullTableFnBasics()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.adHocTableFn();
    assertNotNull(fn);
    assertTrue(hasParam(fn, InlineInputSourceDefn.DATA_PROPERTY));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
    assertTrue(hasParam(fn, FlatTextFormatDefn.LIST_DELIMITER_PARAMETER));
  }

  @Test
  public void testMissingArgs()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.adHocTableFn();
    assertThrows(IAE.class, () -> fn.apply("x", new HashMap<>(), Collections.emptyList(), mapper));
  }

  @Test
  public void testMissingFormat()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(InlineInputSourceDefn.DATA_PROPERTY, "a");
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testValidAdHocFn()
  {
    // Simulate the information obtained from an SQL table function
    final InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    final Map<String, Object> args = new HashMap<>();
    args.put(InlineInputSourceDefn.DATA_PROPERTY, Arrays.asList("a,b", "c,d"));
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    final List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("a", Columns.STRING, null),
        new ColumnSpec("b", Columns.STRING, null)
    );

    final TableFunction fn = defn.adHocTableFn();
    ExternalTableSpec extern = fn.apply("x", args, columns, mapper);

    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat format = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), format.getColumns());
    assertEquals(2, extern.signature.size());
    assertEquals(Collections.singleton(InlineInputSourceDefn.TYPE_KEY), extern.inputSourceTypesSupplier.get());

    // Fails if no columns are provided.
    assertThrows(IAE.class, () -> fn.apply("x", new HashMap<>(), Collections.emptyList(), mapper));
  }

  @Test
  public void testPartialTable()
  {
    // Define an inline table
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a,b\nc,d\n")))
        .inputFormat(CSV_FORMAT)
        .column("a", Columns.STRING)
        .column("b", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    // Get the partial table function
    TableFunction fn = ((ExternalTableDefn) resolved.defn()).tableFn(resolved);

    // Inline is always fully defined: no arguments needed
    assertTrue(fn.parameters().isEmpty());

    // Verify the conversion
    ExternalTableSpec extern = fn.apply("x", new HashMap<>(), Collections.emptyList(), mapper);

    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat actualFormat = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), actualFormat.getColumns());
    assertEquals(2, extern.signature.size());
    assertEquals(Collections.singleton(InlineInputSourceDefn.TYPE_KEY), extern.inputSourceTypesSupplier.get());

    // Cannot supply columns with the function
    List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec("a", Columns.STRING, null),
        new ColumnSpec("b", Columns.STRING, null)
    );
    assertThrows(IAE.class, () -> fn.apply("x", new HashMap<>(), columns, mapper));
  }

  @Test
  public void testDefinedTable()
  {
    // Define an inline table
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a,b\nc,d")))
        .inputFormat(formatToMap(format))
        .column("a", Columns.STRING)
        .column("b", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    // Inline is always fully defined: can directly convert to a table
    ExternalTableSpec extern = ((ExternalTableDefn) resolved.defn()).convert(resolved);

    // Verify the conversion
    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat actualFormat = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), actualFormat.getColumns());
    assertEquals(2, extern.signature.size());
    assertEquals(Collections.singleton(InlineInputSourceDefn.TYPE_KEY), extern.inputSourceTypesSupplier.get());
  }
}
