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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Tests the CSV export of {@link MetadataCsvExporter} against Derby, exercising the generic JDBC path which is used
 * for every metadata store.
 */
public class MetadataCsvExporterTest
{
  private static final MetadataCsvExporter.ValueConverter KEEP_VALUE = (kind, value) -> value;

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = TemporaryFolderExtension.testCaseScoped();

  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  @Test
  public void testCsvEscape()
  {
    Assertions.assertEquals("hello", MetadataCsvExporter.csvEscape("hello"));
    Assertions.assertEquals("\"value,with,commas\"", MetadataCsvExporter.csvEscape("value,with,commas"));
    Assertions.assertEquals("\"value\"\"with\"\"quotes\"", MetadataCsvExporter.csvEscape("value\"with\"quotes"));
    Assertions.assertEquals("\"line1\nline2\"", MetadataCsvExporter.csvEscape("line1\nline2"));
    Assertions.assertEquals("\"line1\rline2\"", MetadataCsvExporter.csvEscape("line1\rline2"));
    // A backslash is an ordinary character
    Assertions.assertEquals("back\\slash", MetadataCsvExporter.csvEscape("back\\slash"));
    // Derby strips the boundary spaces of an unquoted field, so such a value is quoted
    Assertions.assertEquals("\" leading\"", MetadataCsvExporter.csvEscape(" leading"));
    Assertions.assertEquals("\"trailing \"", MetadataCsvExporter.csvEscape("trailing "));
    Assertions.assertEquals("no  boundary  space", MetadataCsvExporter.csvEscape("no  boundary  space"));
    // A NULL is an empty field, while an empty string is a quoted empty field, so that the two stay distinct
    Assertions.assertEquals("", MetadataCsvExporter.csvEscape(null));
    Assertions.assertEquals("\"\"", MetadataCsvExporter.csvEscape(""));
  }

  @Test
  public void testExportTable() throws IOException
  {
    createTable(
        "test_export",
        "name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, active BOOLEAN NOT NULL, PRIMARY KEY(name)"
    );
    insert("test_export", "key1", StringUtils.toUtf8("{\"type\":\"test\"}"), true);
    insert("test_export", "key2", StringUtils.toUtf8("{\"value\":42}"), false);

    final List<String> lines = export("TEST_EXPORT", ImmutableList.of("NAME", "PAYLOAD", "ACTIVE"));

    // Binary values are hex-encoded and booleans are written as true/false
    Assertions.assertEquals(
        ImmutableList.of(
            "key1," + BaseEncoding.base16().encode(StringUtils.toUtf8("{\"type\":\"test\"}")) + ",true",
            "key2," + BaseEncoding.base16().encode(StringUtils.toUtf8("{\"value\":42}")) + ",false"
        ),
        lines
    );
  }

  @Test
  public void testExportTableEscapesSpecialCharacters() throws IOException
  {
    createTable(
        "test_special",
        "name VARCHAR(255) NOT NULL, description VARCHAR(1024), PRIMARY KEY(name)"
    );
    insert("test_special", "commas", "value,with,commas");
    insert("test_special", "quotes", "value\"with\"quotes");
    insert("test_special", "backslash", "back\\slash");
    insert("test_special", "simple", "plain_value");

    final List<String> lines = export("TEST_SPECIAL", ImmutableList.of("NAME", "DESCRIPTION"));

    Assertions.assertEquals(
        ImmutableList.of(
            "backslash,back\\slash",
            "commas,\"value,with,commas\"",
            "quotes,\"value\"\"with\"\"quotes\"",
            "simple,plain_value"
        ),
        lines
    );
  }

  @Test
  public void testExportTableDistinguishesNullFromEmptyString() throws IOException
  {
    createTable(
        "test_nulls",
        "name VARCHAR(255) NOT NULL, payload BLOB, description VARCHAR(255), PRIMARY KEY(name)"
    );
    execute("INSERT INTO test_nulls (name) VALUES ('all_null')");
    execute("INSERT INTO test_nulls (name, description) VALUES ('empty_string', '')");

    final List<String> lines = export("TEST_NULLS", ImmutableList.of("NAME", "PAYLOAD", "DESCRIPTION"));

    // A NULL is an unquoted empty field, an empty string a quoted one
    Assertions.assertEquals(
        ImmutableList.of("all_null,,", "empty_string,,\"\""),
        lines
    );
  }

  @Test
  public void testExportTableWithMultilineValues() throws IOException
  {
    createTable(
        "test_multiline",
        "name VARCHAR(255) NOT NULL, description VARCHAR(1024), PRIMARY KEY(name)"
    );
    insert("test_multiline", "breaks", "line1\nline2\rline3");

    final Path outputFile = exportToFile("TEST_MULTILINE", ImmutableList.of("NAME", "DESCRIPTION"), KEEP_VALUE);

    // A value containing a line break is quoted, so that the record can be read back as one record
    Assertions.assertEquals(
        "breaks,\"line1\nline2\rline3\"\n",
        new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8)
    );
  }

  @Test
  public void testExportTableInGivenColumnOrder() throws IOException
  {
    // "end" is a reserved word, so it must be quoted in the export query
    createTable(
        "test_column_order",
        "id VARCHAR(255) NOT NULL, used_status_last_updated VARCHAR(255), \"END\" VARCHAR(255), "
        + "used BOOLEAN NOT NULL, PRIMARY KEY(id)"
    );
    insert("test_column_order", "seg1", "2024-01-01", "2024-01-02", true);

    final List<String> lines =
        export("TEST_COLUMN_ORDER", ImmutableList.of("ID", "END", "USED", "USED_STATUS_LAST_UPDATED"));

    Assertions.assertEquals(ImmutableList.of("seg1,2024-01-02,true,2024-01-01"), lines);
  }

  @Test
  public void testExportTableAppliesValueConverter() throws IOException
  {
    createTable(
        "test_converter",
        "name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, active BOOLEAN NOT NULL, PRIMARY KEY(name)"
    );
    insert("test_converter", "key1", StringUtils.toUtf8("{\"type\":\"test\"}"), true);

    final Path outputFile = exportToFile(
        "TEST_CONVERTER",
        ImmutableList.of("NAME", "PAYLOAD", "ACTIVE"),
        (kind, value) -> {
          switch (kind) {
            case BINARY:
              return StringUtils.fromUtf8(BaseEncoding.base16().decode(value));
            case BOOLEAN:
              return "true".equals(value) ? "1" : "0";
            default:
              return value;
          }
        }
    );

    Assertions.assertEquals(
        ImmutableList.of("key1,\"{\"\"type\"\":\"\"test\"\"}\",1"),
        Files.readAllLines(outputFile, StandardCharsets.UTF_8)
    );
  }

  private void createTable(final String tableName, final String columns)
  {
    execute(StringUtils.format("CREATE TABLE %s (%s)", tableName, columns));
  }

  private void execute(final String sql)
  {
    derbyConnectorRule.getConnector().getDBI().withHandle(handle -> {
      handle.execute(sql);
      return null;
    });
  }

  private void insert(final String tableName, final Object... values)
  {
    final StringBuilder placeholders = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      placeholders.append(i == 0 ? "?" : ",?");
    }
    derbyConnectorRule.getConnector().getDBI().withHandle(handle -> {
      handle.execute(StringUtils.format("INSERT INTO %s VALUES (%s)", tableName, placeholders), values);
      return null;
    });
  }

  /**
   * Exports the given table and returns its records, sorted so that the assertions do not depend on the order in
   * which the database returns the rows.
   */
  private List<String> export(final String tableName, final List<String> columns) throws IOException
  {
    final List<String> lines =
        Files.readAllLines(exportToFile(tableName, columns, KEEP_VALUE), StandardCharsets.UTF_8);
    lines.sort(String::compareTo);
    return lines;
  }

  private Path exportToFile(
      final String tableName,
      final List<String> columns,
      final MetadataCsvExporter.ValueConverter converter
  ) throws IOException
  {
    final Path outputFile = tempFolder.newFolder(tableName).toPath().resolve(tableName + ".csv");
    new MetadataCsvExporter(derbyConnectorRule.getConnector())
        .exportTable(tableName, columns, outputFile, converter);
    return outputFile;
  }
}
