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
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LocalInputSourceDefnTest extends BaseExternTableTest
{
  private static final Map<String, Object> BASE_DIR_ONLY =
      ImmutableMap.of("type", LocalInputSource.TYPE_KEY, "baseDir", "/tmp");

  private final InputSourceDefn localDefn = registry.inputSourceDefnFor(LocalInputSourceDefn.TYPE_KEY);

  @Test
  public void testValidateEmptyInputSource()
  {
    // No data property: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", LocalInputSource.TYPE_KEY))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateNoFormat()
  {
    // No format: Not valid. If columns are given, a format is required.
    LocalInputSource inputSource = new LocalInputSource(new File("/tmp"), "*");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateNoColumns()
  {
    // No columns: Not valid. If a format is given, then columns required.
    LocalInputSource inputSource = new LocalInputSource(new File("/tmp"), "*");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateConnection()
  {
    // Valid if neither columns nor format are provided. This is a "connection"
    // to some local directory.
    LocalInputSource inputSource = new LocalInputSource(new File("/tmp"), "*");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateConnectionNoFilter()
  {
    // Valid to provide an input source without a filter: we expect the filter
    // to be provided later.
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(BASE_DIR_ONLY)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateBaseDirWithFormat()
  {
    // Valid if neither columns nor format are provided. This is a "connection"
    // to some local directory.
    LocalInputSource inputSource = new LocalInputSource(new File("/tmp"), "*");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateFilesWithFormat()
  {
    // Valid if neither columns nor format are provided. This is a "connection"
    // to some local directory.
    LocalInputSource inputSource = new LocalInputSource(
        null,
        null,
        Collections.singletonList(new File("/tmp/myFile.csv")),
        null
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testBaseDirAndFiles()
  {
    Map<String, Object> source = ImmutableMap.of(
        InputSource.TYPE_PROPERTY, LocalInputSource.TYPE_KEY,
        LocalInputSourceDefn.BASE_DIR_FIELD, "/tmp",
        LocalInputSourceDefn.FILTER_FIELD, "*.csv",
        LocalInputSourceDefn.FILES_FIELD, Collections.singletonList("foo.csv")
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(source)
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testAdHocParameters()
  {
    TableFunction fn = localDefn.adHocTableFn();
    assertTrue(hasParam(fn, LocalInputSourceDefn.BASE_DIR_PARAMETER));
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILES_PARAMETER));
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILTER_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
  }

  @Test
  public void testAdHocBaseDir()
  {
    TableFunction fn = localDefn.adHocTableFn();

    Map<String, Object> args = new HashMap<>();
    args.put(LocalInputSourceDefn.BASE_DIR_PARAMETER, "/tmp");
    args.put(LocalInputSourceDefn.FILTER_PARAMETER, "*.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertEquals(new File("/tmp"), sourceSpec.getBaseDir());
    assertEquals("*.csv", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
    validateFormat(externSpec);

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testAdHocBaseDirOnly()
  {
    TableFunction fn = localDefn.adHocTableFn();

    Map<String, Object> args = new HashMap<>();
    args.put(LocalInputSourceDefn.BASE_DIR_PARAMETER, "/tmp");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertEquals(new File("/tmp"), sourceSpec.getBaseDir());
    assertEquals("*", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
    validateFormat(externSpec);

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testAdHocFiles()
  {
    TableFunction fn = localDefn.adHocTableFn();

    Map<String, Object> args = new HashMap<>();
    args.put(LocalInputSourceDefn.FILES_PARAMETER, Arrays.asList("/tmp/foo.csv", "/tmp/bar.csv"));
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertNull(sourceSpec.getBaseDir());
    assertNull(sourceSpec.getFilter());
    assertEquals(
        Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")),
        sourceSpec.getFiles()
    );
    validateFormat(externSpec);

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testAdHocBaseDirAndFiles()
  {
    TableFunction fn = localDefn.adHocTableFn();

    Map<String, Object> args = new HashMap<>();
    args.put(LocalInputSourceDefn.BASE_DIR_PARAMETER, "/tmp");
    args.put(LocalInputSourceDefn.FILES_PARAMETER, Arrays.asList("foo.csv", "bar.csv"));
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertNull(sourceSpec.getBaseDir());
    assertNull(sourceSpec.getFilter());
    assertEquals(
        Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")),
        sourceSpec.getFiles()
    );
    validateFormat(externSpec);

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testAdHocErrors()
  {
    TableFunction fn = localDefn.adHocTableFn();

    {
      // Empty arguments: not valid
      Map<String, Object> args = new HashMap<>();
      assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
    }

    {
      // Base dir without filter: not valid.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.BASE_DIR_PARAMETER, "/tmp");
      assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
    }

    {
      // Filter without base dir: not valid
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.FILTER_PARAMETER, "*.csv");
      assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
    }

    {
      // Cannot provide both a filter and a list of files.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.BASE_DIR_PARAMETER, "/tmp");
      args.put(LocalInputSourceDefn.FILES_PARAMETER, "/tmp/foo.csv, /tmp/bar.csv");
      args.put(LocalInputSourceDefn.FILTER_PARAMETER, "*.csv");
      assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
    }
  }

  @Test
  public void testFullyDefinedBaseDirAndPattern()
  {
    LocalInputSource inputSource = new LocalInputSource(
        new File("/tmp"),
        "*.csv",
        null,
        null
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Check registry
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    assertNotNull(registry.resolve(table.spec()));

    // Convert to an external spec
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*.csv", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
    validateFormat(externSpec);

    // Get the partial table function. Since table is fully defined,
    // no parameters available.
    TableFunction fn = externDefn.tableFn(resolved);
    assertEquals(0, fn.parameters().size());

    // Apply the function with no arguments and no columns (since columns are already defined.)
    externSpec = fn.apply("x", Collections.emptyMap(), Collections.emptyList(), mapper);
    sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*.csv", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
    validateFormat(externSpec);

    // Fails if columns are provided.
    assertThrows(IAE.class, () -> fn.apply("x", Collections.emptyMap(), COLUMNS, mapper));
  }

  @Test
  public void testFullyDefinedFiles()
  {
    List<File> files = Collections.singletonList(new File("/tmp/my.csv"));
    LocalInputSource inputSource = new LocalInputSource(
        null,
        null,
        files,
        null
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Check registry
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    assertNotNull(registry.resolve(table.spec()));

    // Convert to an external spec
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertNull(sourceSpec.getBaseDir());
    assertNull(sourceSpec.getFilter());
    assertEquals(files, sourceSpec.getFiles());
    validateFormat(externSpec);

    // Get the partial table function. Since table is fully defined,
    // no parameters available.
    TableFunction fn = externDefn.tableFn(resolved);
    assertEquals(0, fn.parameters().size());

    // Apply the function with no arguments and no columns (since columns are already defined.)
    externSpec = fn.apply("x", Collections.emptyMap(), Collections.emptyList(), mapper);
    sourceSpec = (LocalInputSource) externSpec.inputSource;
    assertNull(sourceSpec.getBaseDir());
    assertNull(sourceSpec.getFilter());
    assertEquals(files, sourceSpec.getFiles());
    validateFormat(externSpec);

    // Fails if columns are provided.
    assertThrows(IAE.class, () -> fn.apply("x", Collections.emptyMap(), COLUMNS, mapper));
  }

  @Test
  public void testBaseDirAndFormat()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(BASE_DIR_ONLY)
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec. Fails because the table is partial.
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function.
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILES_PARAMETER));
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILTER_PARAMETER));
    assertFalse(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));

    // Must provide an additional parameter.
    assertThrows(IAE.class, () -> fn.apply("x", Collections.emptyMap(), Collections.emptyList(), mapper));

    {
      // Create a table with a file pattern.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.FILTER_PARAMETER, "*.csv");
      // Apply the function with no arguments and no columns (since columns are already defined.)
      ExternalTableSpec externSpec = fn.apply("x", args, Collections.emptyList(), mapper);
      LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
      assertEquals("/tmp", sourceSpec.getBaseDir().toString());
      assertEquals("*.csv", sourceSpec.getFilter());
      validateFormat(externSpec);
    }

    {
      // Create a table with a file list.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.FILES_PARAMETER, Arrays.asList("foo.csv", "bar.csv"));
      ExternalTableSpec externSpec = fn.apply("x", args, Collections.emptyList(), mapper);
      LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
      assertNull(sourceSpec.getBaseDir());
      assertNull(sourceSpec.getFilter());
      assertEquals(Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")), sourceSpec.getFiles());
      validateFormat(externSpec);

      // Fails if columns are provided.
      assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
    }
  }

  @Test
  public void testBaseDirOnly()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(BASE_DIR_ONLY)
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec. Fails because the table is partial.
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function.
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILES_PARAMETER));
    assertTrue(hasParam(fn, LocalInputSourceDefn.FILTER_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));

    // Must provide an additional parameter.
    assertThrows(IAE.class, () -> fn.apply("x", Collections.emptyMap(), Collections.emptyList(), mapper));

    {
      // Create a table with a file pattern and format.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.FILTER_PARAMETER, "*.csv");
      args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

      // Function fails without columns, since the table has none.
      assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));

      // Apply the function with no arguments and columns
      ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);
      LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
      assertEquals("/tmp", sourceSpec.getBaseDir().toString());
      assertEquals("*.csv", sourceSpec.getFilter());
      validateFormat(externSpec);
    }

    {
      // Create a table with a file list.
      Map<String, Object> args = new HashMap<>();
      args.put(LocalInputSourceDefn.FILES_PARAMETER, Arrays.asList("foo.csv", "bar.csv"));
      args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

      // Function fails without columns, since the table has none.
      assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));

      // Provide format and columns.
      ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);
      LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource;
      assertNull(sourceSpec.getBaseDir());
      assertNull(sourceSpec.getFilter());
      assertEquals(Arrays.asList(new File("/tmp/foo.csv"), new File("/tmp/bar.csv")), sourceSpec.getFiles());
      validateFormat(externSpec);
    }
  }

  private void validateFormat(ExternalTableSpec externSpec)
  {
    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat;
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature;
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
    assertEquals(Collections.singleton(LocalInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }
}
