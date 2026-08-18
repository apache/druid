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
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ExportMetadataTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  @Test
  public void testOrderSegmentsColumns()
  {
    // Columns as reported by a table where the newer columns were added by ALTER TABLE in arbitrary order
    Assert.assertEquals(
        ImmutableList.of(
            "id", "dataSource", "created_date", "start", "end", "partitioned", "version", "used", "payload",
            "used_status_last_updated", "indexing_state_fingerprint", "upgraded_from_segment_id",
            "schema_fingerprint", "num_rows"
        ),
        ExportMetadata.orderSegmentsColumns(ImmutableList.of(
            "id", "dataSource", "created_date", "start", "end", "partitioned", "version", "used", "payload",
            "upgraded_from_segment_id", "num_rows", "used_status_last_updated", "schema_fingerprint",
            "indexing_state_fingerprint"
        ))
    );

    // Ordering ignores case, and columns missing from the source table are skipped
    Assert.assertEquals(
        ImmutableList.of("ID", "DATASOURCE", "CREATED_DATE", "START", "END", "PARTITIONED", "VERSION", "USED",
                        "PAYLOAD"),
        ExportMetadata.orderSegmentsColumns(ImmutableList.of(
            "PAYLOAD", "USED", "ID", "DATASOURCE", "CREATED_DATE", "START", "END", "PARTITIONED", "VERSION"
        ))
    );

    // Unknown columns are appended at the end, in the order reported by the database
    Assert.assertEquals(
        ImmutableList.of("id", "payload", "custom_col", "another_col"),
        ExportMetadata.orderSegmentsColumns(ImmutableList.of("custom_col", "id", "payload", "another_col"))
    );
  }

  @Test
  public void testCsvEscape()
  {
    Assert.assertEquals("hello", SQLMetadataConnector.csvEscape("hello"));
    Assert.assertEquals("\"value,with,commas\"", SQLMetadataConnector.csvEscape("value,with,commas"));
    Assert.assertEquals("\"value\"\"with\"\"quotes\"", SQLMetadataConnector.csvEscape("value\"with\"quotes"));
    Assert.assertEquals("\"line1\nline2\"", SQLMetadataConnector.csvEscape("line1\nline2"));
    Assert.assertEquals("\"line1\rline2\"", SQLMetadataConnector.csvEscape("line1\rline2"));
    Assert.assertEquals("", SQLMetadataConnector.csvEscape(null));
    Assert.assertEquals("", SQLMetadataConnector.csvEscape(""));
  }

  @Test
  public void testRewriteSegmentsExport_preservesAllColumns() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_export");
    final String tableName = "druid_segments";

    // Build a raw CSV with 12 columns matching the current segments table schema:
    // id, dataSource, created_date, start, end, partitioned, version, used, payload,
    // used_status_last_updated, indexing_state_fingerprint, upgraded_from_segment_id
    final String payloadJson = "{\"type\":\"test\"}";
    final String payloadHex = BaseEncoding.base16().encode(StringUtils.toUtf8(payloadJson));

    final String rawLine = String.join(",",
        "seg_id_1",
        "my_datasource",
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        "true",
        "v1",
        "true",
        payloadHex,
        "2024-06-01T00:00:00.000Z",
        "fp_abc123",
        "upgraded_seg_0"
    );

    final File rawFile = new File(outputDir, tableName + "_raw.csv");
    try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(rawFile), StandardCharsets.UTF_8)) {
      writer.write(rawLine + "\n");
    }

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    exporter.rewriteSegmentsExport(tableName);

    final File outFile = new File(outputDir, tableName + ".csv");
    Assert.assertTrue("Output CSV must exist", outFile.exists());

    final List<String> lines = Files.readAllLines(outFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(1, lines.size());

    // Parse the output with opencsv to verify field count and values
    final ICSVParser parser = new RFC4180ParserBuilder().build();
    final String[] fields = parser.parseLine(lines.get(0));

    // Must have all 12 columns
    Assert.assertEquals("All 12 columns must be preserved", 12, fields.length);

    Assert.assertEquals("seg_id_1", fields[0]);
    Assert.assertEquals("my_datasource", fields[1]);
    Assert.assertEquals("2024-01-15", fields[2]);
    Assert.assertEquals("2024-01-01", fields[3]);
    Assert.assertEquals("2024-01-02", fields[4]);
    Assert.assertEquals("1", fields[5]); // partitioned: true -> 1
    Assert.assertEquals("v1", fields[6]);
    Assert.assertEquals("1", fields[7]); // used: true -> 1

    // payload should be escaped JSON, not hex
    Assert.assertEquals(payloadJson, fields[8]);

    // Additional columns preserved
    Assert.assertEquals("2024-06-01T00:00:00.000Z", fields[9]);
    Assert.assertEquals("fp_abc123", fields[10]);
    Assert.assertEquals("upgraded_seg_0", fields[11]);
  }

  @Test
  public void testRewriteSegmentsExport_withSpecialCharsInFields() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_special");
    final String tableName = "druid_segments";

    final String payloadJson = "{\"type\":\"test\"}";
    final String payloadHex = BaseEncoding.base16().encode(StringUtils.toUtf8(payloadJson));

    // datasource with comma, version with quotes — these need proper CSV escaping
    final String datasource = "ds,with,commas";
    final String version = "v\"quoted\"";

    // Column order: id, dataSource, created_date, start, end, partitioned, version, used, payload, ...
    final String rawLineCorrected = SQLMetadataConnector.csvEscape("seg,id,1") + ","
                                    + SQLMetadataConnector.csvEscape(datasource) + ","
                                    + "2024-01-15,"
                                    + "2024-01-01,"
                                    + "2024-01-02,"
                                    + "true,"
                                    + SQLMetadataConnector.csvEscape(version) + ","
                                    + "false,"
                                    + payloadHex + ","
                                    + "2024-06-01";

    final File rawFile = new File(outputDir, tableName + "_raw.csv");
    try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(rawFile), StandardCharsets.UTF_8)) {
      writer.write(rawLineCorrected + "\n");
    }

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    exporter.rewriteSegmentsExport(tableName);

    final File outFile = new File(outputDir, tableName + ".csv");
    final List<String> lines = Files.readAllLines(outFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(1, lines.size());

    // Parse output and verify special characters survived the round-trip
    final ICSVParser parser = new RFC4180ParserBuilder().build();
    final String[] fields = parser.parseLine(lines.get(0));

    Assert.assertEquals(10, fields.length);
    Assert.assertEquals("seg,id,1", fields[0]);
    Assert.assertEquals(datasource, fields[1]);
    Assert.assertEquals(version, fields[6]);
    Assert.assertEquals("2024-06-01", fields[9]);
  }

  @Test
  public void testRewriteSegmentsExport_with9ColumnsOnly() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_9cols");
    final String tableName = "druid_segments";

    // Simulate an older segments table that only has 9 columns (no used_status_last_updated, etc.)
    final String payloadJson = "{\"type\":\"old\"}";
    final String payloadHex = BaseEncoding.base16().encode(StringUtils.toUtf8(payloadJson));

    final String rawLine = String.join(",",
        "old_seg",
        "old_ds",
        "2020-01-01",
        "2020-01-01",
        "2020-01-02",
        "false",
        "v0",
        "true",
        payloadHex
    );

    final File rawFile = new File(outputDir, tableName + "_raw.csv");
    try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(rawFile), StandardCharsets.UTF_8)) {
      writer.write(rawLine + "\n");
    }

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    exporter.rewriteSegmentsExport(tableName);

    final File outFile = new File(outputDir, tableName + ".csv");
    final List<String> lines = Files.readAllLines(outFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(1, lines.size());

    final ICSVParser parser = new RFC4180ParserBuilder().build();
    final String[] fields = parser.parseLine(lines.get(0));

    // Should still work with only 9 columns
    Assert.assertEquals(9, fields.length);
    Assert.assertEquals("old_seg", fields[0]);
    Assert.assertEquals(payloadJson, fields[8]);
  }

  @Test
  public void testRewriteSegmentsExport_preservesBackslashes() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_backslash");
    final String tableName = "druid_segments";

    // Backslashes are valid in segment ids and datasource names, and must not be treated as CSV escapes
    final String id = "foo\\bar_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1";
    final String datasource = "foo\\bar";
    final String version = "v\\1";
    final String upgradedFrom = "back\\slash,and\"quote";

    final String payloadJson = "{\"type\":\"test\",\"path\":\"C:\\\\druid\\\\segments\"}";
    final String payloadHex = BaseEncoding.base16().encode(StringUtils.toUtf8(payloadJson));

    final String rawLine = SQLMetadataConnector.csvEscape(id) + ","
                           + SQLMetadataConnector.csvEscape(datasource) + ","
                           + "2024-01-15,"
                           + "2024-01-01,"
                           + "2024-01-02,"
                           + "true,"
                           + SQLMetadataConnector.csvEscape(version) + ","
                           + "true,"
                           + payloadHex + ","
                           + "2024-06-01,"
                           + "fp\\123,"
                           + SQLMetadataConnector.csvEscape(upgradedFrom);

    final File rawFile = new File(outputDir, tableName + "_raw.csv");
    try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(rawFile), StandardCharsets.UTF_8)) {
      writer.write(rawLine + "\n");
    }

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    exporter.rewriteSegmentsExport(tableName);

    final File outFile = new File(outputDir, tableName + ".csv");
    final List<String> lines = Files.readAllLines(outFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(1, lines.size());

    final ICSVParser parser = new RFC4180ParserBuilder().build();
    final String[] fields = parser.parseLine(lines.get(0));

    Assert.assertEquals(12, fields.length);
    Assert.assertEquals(id, fields[0]);
    Assert.assertEquals(datasource, fields[1]);
    Assert.assertEquals(version, fields[6]);
    Assert.assertEquals(payloadJson, fields[8]);
    Assert.assertEquals("fp\\123", fields[10]);
    Assert.assertEquals(upgradedFrom, fields[11]);
  }

  /**
   * End-to-end test: export a segments table containing values with embedded newlines and carriage
   * returns via the generic JDBC export, then rewrite the raw CSV and verify that every record and
   * field survived. A record with an embedded newline spans multiple physical lines, so the rewrite
   * must read records rather than lines.
   */
  @Test
  public void testExportAndRewriteSegments_withMultilineFields() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_multiline_e2e");
    final String tableName = "druid_segments";

    final String multilineDatasource = "ds\nwith\nnewlines";
    final String crDatasource = "ds\rwith\rcarriage";
    final String multilineFingerprint = "line1\nline2,line3";
    final String payloadJson = "{\"type\":\"test\",\"desc\":\"has\\na newline\"}";

    final TestDerbyConnector connector = derbyConnectorRule.getConnector();
    connector.getDBI().withHandle(
        handle -> {
          handle.execute(
              StringUtils.format(
                  "CREATE TABLE %s ("
                  + "id VARCHAR(255) NOT NULL, "
                  + "dataSource VARCHAR(255) NOT NULL, "
                  + "created_date VARCHAR(255) NOT NULL, "
                  + "start VARCHAR(255) NOT NULL, "
                  + "\"END\" VARCHAR(255) NOT NULL, "
                  + "partitioned BOOLEAN NOT NULL, "
                  + "version VARCHAR(255) NOT NULL, "
                  + "used BOOLEAN NOT NULL, "
                  + "payload BLOB NOT NULL, "
                  + "used_status_last_updated VARCHAR(255), "
                  + "indexing_state_fingerprint VARCHAR(255), "
                  + "PRIMARY KEY(id))",
                  tableName
              )
          );
          final String insert = StringUtils.format(
              "INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              tableName
          );
          handle.execute(
              insert,
              "seg1",
              multilineDatasource,
              "2024-01-15",
              "2024-01-01",
              "2024-01-02",
              true,
              "v1",
              true,
              StringUtils.toUtf8(payloadJson),
              "2024-06-01",
              multilineFingerprint
          );
          // indexing_state_fingerprint is left NULL for this row
          handle.execute(
              StringUtils.format(
                  "INSERT INTO %s (id, dataSource, created_date, start, \"END\", partitioned, version, used, payload, "
                  + "used_status_last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  tableName
              ),
              "seg2",
              crDatasource,
              "2024-01-16",
              "2024-01-03",
              "2024-01-04",
              false,
              "v2",
              false,
              StringUtils.toUtf8(payloadJson),
              "2024-06-02"
          );
          return null;
        }
    );

    connector.exportTableGeneric(
        StringUtils.toUpperCase(tableName),
        new File(outputDir, tableName + "_raw.csv").getAbsolutePath(),
        ImmutableList.of(
            "ID",
            "DATASOURCE",
            "CREATED_DATE",
            "START",
            "END",
            "PARTITIONED",
            "VERSION",
            "USED",
            "PAYLOAD",
            "USED_STATUS_LAST_UPDATED",
            "INDEXING_STATE_FINGERPRINT"
        )
    );

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    exporter.rewriteSegmentsExport(tableName);

    final File outFile = new File(outputDir, tableName + ".csv");
    final List<String[]> records = new ArrayList<>();
    try (CSVReader reader = new CSVReaderBuilder(
        Files.newBufferedReader(outFile.toPath(), StandardCharsets.UTF_8))
        .withCSVParser(new RFC4180ParserBuilder().build())
        .withKeepCarriageReturn(true)
        .build()) {
      String[] record;
      while ((record = reader.readNext()) != null) {
        records.add(record);
      }
    }

    records.sort(Comparator.comparing(record -> record[0]));
    Assert.assertEquals(2, records.size());

    final String[] seg1 = records.get(0);
    Assert.assertEquals(11, seg1.length);
    Assert.assertEquals("seg1", seg1[0]);
    Assert.assertEquals(multilineDatasource, seg1[1]);
    Assert.assertEquals("1", seg1[5]);
    Assert.assertEquals("1", seg1[7]);
    Assert.assertEquals(payloadJson, seg1[8]);
    Assert.assertEquals("2024-06-01", seg1[9]);
    Assert.assertEquals(multilineFingerprint, seg1[10]);

    final String[] seg2 = records.get(1);
    Assert.assertEquals(11, seg2.length);
    Assert.assertEquals("seg2", seg2[0]);
    Assert.assertEquals(crDatasource, seg2[1]);
    Assert.assertEquals("0", seg2[5]);
    Assert.assertEquals("0", seg2[7]);
    Assert.assertEquals(payloadJson, seg2[8]);
    Assert.assertEquals("", seg2[10]);
  }

  @Test
  public void testRewriteSegmentsExport_failsOnTruncatedRow() throws IOException
  {
    final File outputDir = tempFolder.newFolder("segments_truncated");
    final String tableName = "druid_segments";

    final File rawFile = new File(outputDir, tableName + "_raw.csv");
    try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(rawFile), StandardCharsets.UTF_8)) {
      writer.write("only_id,only_datasource\n");
    }

    final ExportMetadata exporter = new ExportMetadata();
    exporter.outputPath = outputDir.getAbsolutePath();
    exporter.useHexBlobs = false;
    exporter.booleansAsStrings = false;

    final ISE e = Assert.assertThrows(ISE.class, () -> exporter.rewriteSegmentsExport(tableName));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("has [2] fields, expected at least [9]"));
  }
}
