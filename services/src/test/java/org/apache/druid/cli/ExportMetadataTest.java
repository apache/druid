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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Tests the export of the segments table, which the {@code export-metadata} tool reads from the database and writes
 * as CSV in a single pass.
 */
public class ExportMetadataTest
{
  private static final String SEGMENTS_TABLE = "druid_segments";
  private static final String INDEXING_STATES_TABLE = "druid_indexingStates";
  private static final String PAYLOAD_JSON = "{\"type\":\"test\",\"path\":\"C:\\\\druid\\\\segments\"}";
  /** The nine columns of a segments table of any Druid version. {@code end} is a reserved word in Derby. */
  private static final String SEGMENTS_BASE_COLUMNS =
      "id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date VARCHAR(255) NOT NULL, "
      + "start VARCHAR(255) NOT NULL, \"END\" VARCHAR(255) NOT NULL, partitioned BOOLEAN NOT NULL, "
      + "version VARCHAR(255) NOT NULL, used BOOLEAN NOT NULL, payload BLOB NOT NULL";
  private static final String INSERT_LEGACY_SEGMENT = "INSERT INTO druid_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final List<String> SEGMENTS_COLUMN_ORDER = ExportMetadata.SEGMENTS.columnOrder;

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = TemporaryFolderExtension.testCaseScoped();

  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  private ExportMetadata tool;
  private File outputDir;

  @BeforeEach
  public void setUp() throws IOException
  {
    ExportMetadata.configureJsonMapper();
    outputDir = tempFolder.newFolder("export");
    tool = new ExportMetadata();
    tool.outputPath = outputDir.getAbsolutePath();
  }

  @Test
  public void testOrderColumns()
  {
    // Columns as reported by a table where the newer columns were added by ALTER TABLE in arbitrary order
    Assertions.assertEquals(
        ImmutableList.of(
            "id", "dataSource", "created_date", "start", "end", "partitioned", "version", "used", "payload",
            "used_status_last_updated", "indexing_state_fingerprint", "upgraded_from_segment_id",
            "schema_fingerprint", "num_rows"
        ),
        ExportMetadata.orderColumns(SEGMENTS_COLUMN_ORDER, ImmutableList.of(
            "id", "dataSource", "created_date", "start", "end", "partitioned", "version", "used", "payload",
            "upgraded_from_segment_id", "num_rows", "used_status_last_updated", "schema_fingerprint",
            "indexing_state_fingerprint"
        ), ImmutableSet.of())
    );

    // Ordering ignores case, and columns missing from the source table are skipped
    Assertions.assertEquals(
        ImmutableList.of("ID", "DATASOURCE", "CREATED_DATE", "START", "END", "PARTITIONED", "VERSION", "USED",
                        "PAYLOAD"),
        ExportMetadata.orderColumns(SEGMENTS_COLUMN_ORDER, ImmutableList.of(
            "PAYLOAD", "USED", "ID", "DATASOURCE", "CREATED_DATE", "START", "END", "PARTITIONED", "VERSION"
        ), ImmutableSet.of())
    );

    // Unknown columns are appended at the end, in the order reported by the database
    Assertions.assertEquals(
        ImmutableList.of("id", "payload", "custom_col", "another_col"),
        ExportMetadata.orderColumns(
            SEGMENTS_COLUMN_ORDER,
            ImmutableList.of("custom_col", "id", "payload", "another_col"),
            ImmutableSet.of()
        )
    );

    // Excluded columns are left out, whether they are listed in the canonical order or not
    Assertions.assertEquals(
        ImmutableList.of("payload", "custom_col"),
        ExportMetadata.orderColumns(
            SEGMENTS_COLUMN_ORDER,
            ImmutableList.of("custom_col", "ID", "payload", "another_col"),
            ImmutableSet.of("id", "another_col")
        )
    );
  }

  /**
   * The segments table is exported in the canonical column order, with all of its columns, whatever the physical
   * column order of the source table is.
   */
  @Test
  public void testExportSegments() throws IOException, CsvValidationException
  {
    // The optional columns are declared in an order which differs from the canonical one
    createSegmentsTable(
        "upgraded_from_segment_id VARCHAR(255)",
        "num_rows BIGINT",
        "used_status_last_updated VARCHAR(255)",
        "indexing_state_fingerprint VARCHAR(255)"
    );
    insert(
        "INSERT INTO druid_segments (id, dataSource, created_date, start, \"END\", partitioned, version, used, "
        + "payload, upgraded_from_segment_id, num_rows, used_status_last_updated, indexing_state_fingerprint) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "seg1",
        "my_datasource",
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        true,
        "v1",
        true,
        StringUtils.toUtf8(PAYLOAD_JSON),
        "upgraded_seg_0",
        42L,
        "2024-06-01T00:00:00.000Z",
        "fp_abc123"
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertEquals(13, record.length);
    Assertions.assertEquals("seg1", record[0]);
    Assertions.assertEquals("my_datasource", record[1]);
    Assertions.assertEquals("2024-01-15", record[2]);
    Assertions.assertEquals("2024-01-01", record[3]);
    Assertions.assertEquals("2024-01-02", record[4]);
    // Booleans are written as 1 and 0
    Assertions.assertEquals("1", record[5]);
    Assertions.assertEquals("v1", record[6]);
    Assertions.assertEquals("1", record[7]);
    // The payload is written as the JSON it holds, with its backslashes preserved
    Assertions.assertEquals(PAYLOAD_JSON, record[8]);
    // The optional columns follow in the canonical order, not in the order of the source table
    Assertions.assertEquals("2024-06-01T00:00:00.000Z", record[9]);
    Assertions.assertEquals("fp_abc123", record[10]);
    Assertions.assertEquals("upgraded_seg_0", record[11]);
    Assertions.assertEquals("42", record[12]);
  }

  /**
   * A table with only the nine columns of an older Druid version is exported with those nine columns.
   */
  @Test
  public void testExportSegmentsWithLegacyColumns() throws IOException, CsvValidationException
  {
    createSegmentsTable();
    insert(
        INSERT_LEGACY_SEGMENT,
        "old_seg",
        "old_ds",
        "2020-01-01",
        "2020-01-01",
        "2020-01-02",
        false,
        "v0",
        true,
        StringUtils.toUtf8(PAYLOAD_JSON)
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertEquals(9, record.length);
    Assertions.assertEquals("old_seg", record[0]);
    Assertions.assertEquals("0", record[5]);
    Assertions.assertEquals("1", record[7]);
    Assertions.assertEquals(PAYLOAD_JSON, record[8]);
  }

  /**
   * Values containing commas, double quotes, backslashes and line breaks are escaped as per RFC 4180, and a record
   * spanning several lines is written as a single record.
   */
  @Test
  public void testExportSegmentsWithSpecialCharacters() throws IOException, CsvValidationException
  {
    final String id = "seg,id\\1";
    final String datasource = "ds\nwith\nnewlines";
    final String version = "v\"quoted\"";
    final String fingerprint = "line1\rline2,line3";

    createSegmentsTable("indexing_state_fingerprint VARCHAR(255)");
    insert(
        "INSERT INTO druid_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        id,
        datasource,
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        true,
        version,
        true,
        StringUtils.toUtf8(PAYLOAD_JSON),
        fingerprint
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertEquals(10, record.length);
    Assertions.assertEquals(id, record[0]);
    Assertions.assertEquals(datasource, record[1]);
    Assertions.assertEquals(version, record[6]);
    Assertions.assertEquals(PAYLOAD_JSON, record[8]);
    Assertions.assertEquals(fingerprint, record[9]);
  }

  /**
   * A NULL is written as an unquoted empty field and an empty string as a quoted one, so that a nullable column such
   * as {@code num_rows} is imported as a NULL rather than as an empty string.
   */
  @Test
  public void testExportSegmentsDistinguishesNullFromEmptyString() throws IOException, CsvValidationException
  {
    createSegmentsTable(
        "used_status_last_updated VARCHAR(255)",
        "indexing_state_fingerprint VARCHAR(255)",
        "num_rows BIGINT"
    );
    insert(
        "INSERT INTO druid_segments (id, dataSource, created_date, start, \"END\", partitioned, version, used, "
        + "payload, used_status_last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "seg1",
        "my_datasource",
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        true,
        "v1",
        true,
        StringUtils.toUtf8(PAYLOAD_JSON),
        ""
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertEquals(12, record.length);
    Assertions.assertEquals("", record[9]);
    Assertions.assertNull(record[10]);
    Assertions.assertNull(record[11]);
  }

  /**
   * With {@code --use-hex-blobs} and {@code --booleans-as-strings}, the payload stays hex-encoded and the booleans
   * are written as true/false.
   */
  @Test
  public void testExportSegmentsWithHexBlobsAndBooleanStrings() throws IOException, CsvValidationException
  {
    tool.useHexBlobs = true;
    tool.booleansAsStrings = true;

    createSegmentsTable();
    insert(
        INSERT_LEGACY_SEGMENT,
        "seg1",
        "my_datasource",
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        true,
        "v1",
        false,
        StringUtils.toUtf8(PAYLOAD_JSON)
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertEquals("true", record[5]);
    Assertions.assertEquals("false", record[7]);
    Assertions.assertEquals(BaseEncoding.base16().encode(StringUtils.toUtf8(PAYLOAD_JSON)), record[8]);
  }

  /**
   * With a deep storage migration option, the load spec of every segment payload is rewritten.
   */
  @Test
  public void testExportSegmentsRewritesLoadSpec() throws IOException, CsvValidationException
  {
    tool.newLocalPath = "/new/local/path";

    final String payload = "{\"dataSource\":\"my_datasource\","
                           + "\"interval\":\"2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z\","
                           + "\"version\":\"v1\","
                           + "\"loadSpec\":{\"type\":\"local\",\"path\":\"/old/path/index.zip\"},"
                           + "\"dimensions\":\"\",\"metrics\":\"\",\"shardSpec\":{\"type\":\"numbered\","
                           + "\"partitionNum\":0,\"partitions\":1},\"binaryVersion\":9,\"size\":100,"
                           + "\"identifier\":\"seg1\"}";

    createSegmentsTable();
    insert(
        INSERT_LEGACY_SEGMENT,
        "seg1",
        "my_datasource",
        "2024-01-15",
        "2024-01-01",
        "2024-01-02",
        true,
        "v1",
        true,
        StringUtils.toUtf8(payload)
    );

    final String[] record = exportSegments().get(0);

    Assertions.assertTrue(
        record[8].contains("\"loadSpec\":{\"type\":\"local\",\"path\":\"/new/local/path/"),
        record[8]
    );
  }

  /**
   * The table backing {@code schema_fingerprint} is exported, without its generated {@code id}, which the target
   * database assigns on import.
   */
  @Test
  public void testExportSegmentSchemas() throws IOException, CsvValidationException
  {
    createTable(
        "druid_segmentSchemas",
        "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL, "
        + "created_date VARCHAR(255) NOT NULL, datasource VARCHAR(255) NOT NULL, "
        + "fingerprint VARCHAR(255) NOT NULL, payload BLOB NOT NULL, used BOOLEAN NOT NULL, "
        + "used_status_last_updated VARCHAR(255) NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (id)"
    );
    insert(
        "INSERT INTO druid_segmentSchemas (created_date, datasource, fingerprint, payload, used, "
        + "used_status_last_updated, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
        "2024-01-15",
        "my_datasource",
        "fp_schema_1",
        StringUtils.toUtf8(PAYLOAD_JSON),
        true,
        "2024-06-01",
        1
    );

    exportTable("druid_segmentSchemas", ExportMetadata.SEGMENT_SCHEMAS, tool::convertValue);
    final String[] record = readRecords("druid_segmentSchemas").get(0);

    // The generated id is not exported, and the remaining columns are written in the canonical order
    Assertions.assertEquals(7, record.length);
    Assertions.assertEquals("fp_schema_1", record[0]);
    Assertions.assertEquals("2024-01-15", record[1]);
    Assertions.assertEquals("my_datasource", record[2]);
    Assertions.assertEquals(PAYLOAD_JSON, record[3]);
    Assertions.assertEquals("1", record[4]);
    Assertions.assertEquals("2024-06-01", record[5]);
    Assertions.assertEquals("1", record[6]);
  }

  /**
   * The table backing {@code indexing_state_fingerprint} is exported.
   */
  @Test
  public void testExportIndexingStates() throws IOException, CsvValidationException
  {
    createTable(
        INDEXING_STATES_TABLE,
        "created_date VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, "
        + "fingerprint VARCHAR(255) NOT NULL, payload BLOB NOT NULL, used BOOLEAN NOT NULL, "
        + "pending BOOLEAN NOT NULL, used_status_last_updated VARCHAR(255) NOT NULL, PRIMARY KEY (fingerprint)"
    );
    insert(
        "INSERT INTO druid_indexingStates VALUES (?, ?, ?, ?, ?, ?, ?)",
        "2024-01-15",
        "my_datasource",
        "fp_state_1",
        StringUtils.toUtf8(PAYLOAD_JSON),
        true,
        false,
        "2024-06-01"
    );

    exportTable(INDEXING_STATES_TABLE, ExportMetadata.INDEXING_STATES, tool::convertValue);
    final String[] record = readRecords(INDEXING_STATES_TABLE).get(0);

    Assertions.assertEquals(7, record.length);
    Assertions.assertEquals("fp_state_1", record[0]);
    Assertions.assertEquals("2024-01-15", record[1]);
    Assertions.assertEquals("my_datasource", record[2]);
    Assertions.assertEquals(PAYLOAD_JSON, record[3]);
    Assertions.assertEquals("1", record[4]);
    Assertions.assertEquals("0", record[5]);
    Assertions.assertEquals("2024-06-01", record[6]);
  }

  /**
   * A table which the metadata store does not have, such as the segment schemas of a store without centralized
   * datasource schema, is skipped instead of failing the export, and the file of an earlier export into the same
   * directory is removed rather than left behind as stale data.
   */
  @Test
  public void testExportSkipsMissingOptionalTable() throws IOException
  {
    final Path staleFile = outputDir.toPath().resolve("DRUID_SEGMENTSCHEMAS.csv");
    Files.write(staleFile, StringUtils.toUtf8("stale,data\n"));

    exportTable("druid_segmentSchemas", ExportMetadata.SEGMENT_SCHEMAS, tool::convertValue);

    Assertions.assertFalse(Files.exists(staleFile));
  }

  /**
   * A failed column lookup must not look like a table the metadata store does not have, which would silently skip
   * an optional table.
   */
  @Test
  public void testExportFailsWhenColumnLookupFails()
  {
    createTable(INDEXING_STATES_TABLE, "fingerprint VARCHAR(255) NOT NULL, payload BLOB NOT NULL");

    final TestDerbyConnector connector = new TestDerbyConnector(
        derbyConnectorRule.getMetadataConnectorConfig(),
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector().getJdbcUri(),
        CentralizedDatasourceSchemaConfig.create()
    )
    {
      @Override
      public String getMetadataTableSchema(final Connection connection) throws SQLException
      {
        throw new SQLException("Could not read the database metadata");
      }
    };

    final Exception e = Assertions.assertThrows(
        Exception.class,
        () -> tool.exportTable(
            new MetadataCsvExporter(connector),
            connector,
            StringUtils.toUpperCase(INDEXING_STATES_TABLE),
            ExportMetadata.INDEXING_STATES,
            tool::convertValue
        )
    );
    Assertions.assertInstanceOf(SQLException.class, Throwables.getRootCause(e));
  }

  @Test
  public void testExportFailsWhenTableIsMissing()
  {
    final ISE e = Assertions.assertThrows(
        ISE.class,
        this::exportSegmentsTable
    );
    Assertions.assertTrue(e.getMessage().contains("does not exist in this metadata store"), e.getMessage());
  }

  /**
   * Creates the segments table with the nine columns every version has, followed by the given optional columns.
   */
  private void createSegmentsTable(final String... optionalColumns)
  {
    final StringBuilder columns = new StringBuilder(SEGMENTS_BASE_COLUMNS);
    for (String column : optionalColumns) {
      columns.append(", ").append(column);
    }
    createTable(SEGMENTS_TABLE, columns.append(", PRIMARY KEY(id)").toString());
  }

  private void createTable(final String tableName, final String columns)
  {
    derbyConnectorRule.getConnector().getDBI().withHandle(handle -> {
      handle.execute(StringUtils.format("CREATE TABLE %s (%s)", tableName, columns));
      return null;
    });
  }

  private void insert(final String sql, final Object... values)
  {
    derbyConnectorRule.getConnector().getDBI().withHandle(handle -> {
      handle.execute(sql, values);
      return null;
    });
  }

  private void exportSegmentsTable()
  {
    exportTable(SEGMENTS_TABLE, ExportMetadata.SEGMENTS, tool::convertSegmentValue);
  }

  private void exportTable(
      final String tableName,
      final ExportMetadata.TableSpec spec,
      final MetadataCsvExporter.ValueConverter converter
  )
  {
    final TestDerbyConnector connector = derbyConnectorRule.getConnector();
    tool.exportTable(
        new MetadataCsvExporter(connector),
        connector,
        StringUtils.toUpperCase(tableName),
        spec,
        converter
    );
  }

  /**
   * Exports the segments table and reads the written CSV back as records, so that a record spanning several lines is
   * read as one record and a NULL stays distinguishable from an empty string.
   */
  private List<String[]> exportSegments() throws IOException, CsvValidationException
  {
    exportSegmentsTable();
    return readRecords(SEGMENTS_TABLE);
  }

  private List<String[]> readRecords(final String tableName) throws IOException, CsvValidationException
  {
    final Path outputFile = outputDir.toPath().resolve(StringUtils.toUpperCase(tableName) + ".csv");
    final List<String[]> records = new ArrayList<>();
    try (
        CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(outputFile, StandardCharsets.UTF_8))
            .withCSVParser(
                new RFC4180ParserBuilder().withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).build()
            )
            .withKeepCarriageReturn(true)
            .build()
    ) {
      String[] record;
      while ((record = reader.readNext()) != null) {
        records.add(record);
      }
    }
    records.sort(Comparator.comparing(record -> record[0]));
    return records;
  }
}
