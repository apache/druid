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

import com.google.common.io.BaseEncoding;
import com.opencsv.CSVParser;
import org.apache.druid.java.util.common.StringUtils;
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
import java.util.List;

public class ExportMetadataTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testCsvEscapeField_plainValue()
  {
    Assert.assertEquals("hello", ExportMetadata.csvEscapeField("hello"));
  }

  @Test
  public void testCsvEscapeField_withComma()
  {
    Assert.assertEquals("\"value,with,commas\"", ExportMetadata.csvEscapeField("value,with,commas"));
  }

  @Test
  public void testCsvEscapeField_withDoubleQuote()
  {
    Assert.assertEquals("\"value\"\"with\"\"quotes\"", ExportMetadata.csvEscapeField("value\"with\"quotes"));
  }

  @Test
  public void testCsvEscapeField_withNewline()
  {
    Assert.assertEquals("\"line1\nline2\"", ExportMetadata.csvEscapeField("line1\nline2"));
  }

  @Test
  public void testCsvEscapeField_withCarriageReturn()
  {
    Assert.assertEquals("\"line1\rline2\"", ExportMetadata.csvEscapeField("line1\rline2"));
  }

  @Test
  public void testCsvEscapeField_null()
  {
    Assert.assertEquals("", ExportMetadata.csvEscapeField(null));
  }

  @Test
  public void testCsvEscapeField_empty()
  {
    Assert.assertEquals("", ExportMetadata.csvEscapeField(""));
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
    final CSVParser parser = new CSVParser();
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

    // The raw CSV file from exportTableWithJdbc would have properly escaped these
    final String rawLine = csvEscapeField(datasource) + ","
                           + csvEscapeField("seg,id,1") + ","  // swapped: id first in raw but we're writing fields in schema order
                           + "2024-01-15,"
                           + "2024-01-01,"
                           + "2024-01-02,"
                           + "true,"
                           + csvEscapeField(version) + ","
                           + "false,"
                           + payloadHex + ","
                           + "2024-06-01";

    // Actually let's be precise about column order: id, dataSource, created_date, start, end, partitioned, version, used, payload, ...
    final String rawLineCorrected = csvEscapeField("seg,id,1") + ","
                                    + csvEscapeField(datasource) + ","
                                    + "2024-01-15,"
                                    + "2024-01-01,"
                                    + "2024-01-02,"
                                    + "true,"
                                    + csvEscapeField(version) + ","
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
    final CSVParser parser = new CSVParser();
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

    final CSVParser parser = new CSVParser();
    final String[] fields = parser.parseLine(lines.get(0));

    // Should still work with only 9 columns
    Assert.assertEquals(9, fields.length);
    Assert.assertEquals("old_seg", fields[0]);
    Assert.assertEquals(payloadJson, fields[8]);
  }

  /**
   * Local helper matching ExportMetadata.csvEscapeField for building test input.
   */
  private static String csvEscapeField(String value)
  {
    return ExportMetadata.csvEscapeField(value);
  }
}
