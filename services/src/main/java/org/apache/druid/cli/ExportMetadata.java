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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.server.DruidNode;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;

import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Command(
    name = "export-metadata",
    description = "Exports the contents of a Druid metadata store (Derby or PostgreSQL) to CSV files to assist with cluster migration. This tool also provides the ability to rewrite segment locations in the metadata to assist with deep storage migration."
)
public class ExportMetadata extends GuiceRunnable
{
  @Option(name = "--connectURI", description = "Database JDBC connection string")
  @Required
  private String connectURI;

  @Option(name = "--user", description = "Database username")
  private String user = null;

  @Option(name = "--password", description = "Database password")
  private String password = null;

  @Option(name = "--base", description = "Base table name")
  private String base = "druid";

  @Option(
      name = {"-b", "--s3bucket"},
      title = "s3bucket",
      description = "S3 bucket of the migrated segments")
  public String s3Bucket = null;

  @Option(
      name = {"-k", "--s3baseKey"},
      title = "s3baseKey",
      description = "S3 baseKey of the migrated segments")
  public String s3baseKey = null;

  @Option(
      name = {"-h", "--hadoopStorageDirectory"},
      title = "hadoopStorageDirectory",
      description = "hadoopStorageDirectory of the migrated segments")
  public String hadoopStorageDirectory = null;

  @Option(
      name = {"-n", "--newLocalPath"},
      title = "newLocalPath",
      description = "newLocalPath of the migrated segments")
  public String newLocalPath = null;

  @Option(
      name = {"-o", "--output-path"},
      title = "output-path",
      description = "CSV output path")
  public String outputPath = null;

  @Option(
      name = {"-x", "--use-hex-blobs"},
      title = "use-hex-blobs",
      description = "Write BLOB payloads as hex strings")
  public boolean useHexBlobs = false;

  @Option(
      name = {"-t", "--booleans-as-strings"},
      title = "booleans-as-strings",
      description = "Write boolean values as true/false strings instead of 1/0")
  public boolean booleansAsStrings = false;

  /**
   * Canonical order in which the columns of the segments table are exported, matching the import
   * commands documented in {@code docs/operations/export-metadata.md}. Columns which do not exist in
   * the source table are skipped, and any column not listed here is appended after these, in the
   * order reported by the source database.
   */
  private static final List<String> SEGMENTS_COLUMN_ORDER = ImmutableList.of(
      "id",
      "dataSource",
      "created_date",
      "start",
      "end",
      "partitioned",
      "version",
      "used",
      "payload",
      "used_status_last_updated",
      "indexing_state_fingerprint",
      "upgraded_from_segment_id",
      "schema_fingerprint",
      "num_rows"
  );

  private static final Logger log = new Logger(ExportMetadata.class);

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public ExportMetadata()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        // This area is copied from CreateTables.
        // It's unknown why those modules are required in CreateTables, and if all of those modules are required or not.
        // Maybe some of those modules could be removed.
        // See https://github.com/apache/druid/pull/4429#discussion_r123602930
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> {
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(MetadataStorageConnectorConfig.class),
              new MetadataStorageConnectorConfig()
              {
                @Override
                public String getConnectURI()
                {
                  return connectURI;
                }

                @Override
                public String getUser()
                {
                  return user;
                }

                @Override
                public String getPassword()
                {
                  return password;
                }
              }
          );
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(MetadataStorageTablesConfig.class),
              MetadataStorageTablesConfig.fromBase(base)
          );
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(DruidNode.class, Self.class),
              new DruidNode("tools", "localhost", false, -1, null, true, false)
          );
        }
    );
  }

  @Override
  public void run()
  {
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, JSON_MAPPER);
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    JSON_MAPPER.setInjectableValues(injectableValues);

    if (hadoopStorageDirectory != null && newLocalPath != null) {
      throw new IllegalArgumentException(
          "Only one of s3Bucket, hadoopStorageDirectory, and newLocalPath can be set."
      );
    }

    if (s3Bucket != null && (hadoopStorageDirectory != null || newLocalPath != null)) {
      throw new IllegalArgumentException(
          "Only one of s3Bucket, hadoopStorageDirectory, and newLocalPath can be set."
      );
    }

    if (s3Bucket != null && s3baseKey == null) {
      throw new IllegalArgumentException("s3baseKey must be set if s3Bucket is set.");
    }

    final Injector injector = makeInjector();
    SQLMetadataConnector dbConnector = injector.getInstance(SQLMetadataConnector.class);
    MetadataStorageTablesConfig metadataStorageTablesConfig = injector.getInstance(MetadataStorageTablesConfig.class);
    
    // We export a raw CSV first, and then apply some conversions for easier imports:
    // Boolean strings are rewritten as 1 and 0
    // hexadecimal BLOB columns are rewritten with rewriteHexPayloadAsEscapedJson()
    log.info("Exporting datasource table: " + metadataStorageTablesConfig.getDataSourceTable());
    exportTable(dbConnector, metadataStorageTablesConfig.getDataSourceTable(), true);
    rewriteDatasourceExport(metadataStorageTablesConfig.getDataSourceTable());

    log.info("Exporting segments table: " + metadataStorageTablesConfig.getSegmentsTable());
    exportSegmentsTable(dbConnector, metadataStorageTablesConfig.getSegmentsTable());
    rewriteSegmentsExport(metadataStorageTablesConfig.getSegmentsTable());

    log.info("Exporting rules table: " + metadataStorageTablesConfig.getRulesTable());
    exportTable(dbConnector, metadataStorageTablesConfig.getRulesTable(), true);
    rewriteRulesExport(metadataStorageTablesConfig.getRulesTable());

    log.info("Exporting config table: " + metadataStorageTablesConfig.getConfigTable());
    exportTable(dbConnector, metadataStorageTablesConfig.getConfigTable(), true);
    rewriteConfigExport(metadataStorageTablesConfig.getConfigTable());

    log.info("Exporting supervisor table: " + metadataStorageTablesConfig.getSupervisorTable());
    exportTable(dbConnector, metadataStorageTablesConfig.getSupervisorTable(), true);
    rewriteSupervisorExport(metadataStorageTablesConfig.getSupervisorTable());
  }

  private void exportTable(
      SQLMetadataConnector dbConnector,
      String tableName,
      boolean withRawFilename
  )
  {
    String pathFormatString;
    if (withRawFilename) {
      pathFormatString = "%s/%s_raw.csv";
    } else {
      pathFormatString = "%s/%s.csv";
    }
    final String exportTableName = isDerby() ? StringUtils.toUpperCase(tableName) : tableName;
    dbConnector.exportTable(
        exportTableName,
        StringUtils.format(pathFormatString, outputPath, tableName)
    );
  }

  /**
   * Exports the segments table with the columns emitted in {@link #SEGMENTS_COLUMN_ORDER}, so that the
   * output does not depend on the physical column order of the source table.
   */
  private void exportSegmentsTable(
      SQLMetadataConnector dbConnector,
      String tableName
  )
  {
    final String exportTableName = isDerby() ? StringUtils.toUpperCase(tableName) : tableName;
    final List<String> columns = orderSegmentsColumns(dbConnector.getTableColumns(exportTableName));
    if (columns.isEmpty()) {
      throw new ISE(
          "Could not read the columns of table[%s]. Cannot export the segments table in a stable column order.",
          exportTableName
      );
    }

    dbConnector.exportTable(
        exportTableName,
        StringUtils.format("%s/%s_raw.csv", outputPath, tableName),
        columns
    );
  }

  /**
   * Orders the given actual column names of the segments table by {@link #SEGMENTS_COLUMN_ORDER},
   * appending any unknown columns at the end in their original order.
   */
  static List<String> orderSegmentsColumns(List<String> actualColumns)
  {
    final Map<String, String> remaining = new LinkedHashMap<>();
    for (String column : actualColumns) {
      remaining.put(StringUtils.toLowerCase(column), column);
    }

    final List<String> ordered = new ArrayList<>(actualColumns.size());
    for (String column : SEGMENTS_COLUMN_ORDER) {
      final String actualColumn = remaining.remove(StringUtils.toLowerCase(column));
      if (actualColumn != null) {
        ordered.add(actualColumn);
      }
    }
    ordered.addAll(remaining.values());
    return ordered;
  }

  private boolean isDerby()
  {
    return connectURI != null && connectURI.startsWith("jdbc:derby");
  }

  private void rewriteDatasourceExport(
      String datasourceTableName
  )
  {
    String inFile = StringUtils.format(("%s/%s_raw.csv"), outputPath, datasourceTableName);
    String outFile = StringUtils.format("%s/%s.csv", outputPath, datasourceTableName);
    try (
        CSVReader reader = openCsvReader(inFile);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)
    ) {
      String[] parsed;
      while ((parsed = readRecord(reader, inFile, 4)) != null) {
        String newLine = SQLMetadataConnector.csvEscape(parsed[0]) + "," //dataSource
                         + SQLMetadataConnector.csvEscape(parsed[1]) + "," //created_date
                         + rewriteHexPayloadAsEscapedJson(parsed[2]) + "," //commit_metadata_payload
                         + SQLMetadataConnector.csvEscape(parsed[3]) //commit_metadata_sha1
                         + "\n";
        writer.write(newLine);

      }
    }
    catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  private void rewriteRulesExport(
      String rulesTableName
  )
  {
    String inFile = StringUtils.format(("%s/%s_raw.csv"), outputPath, rulesTableName);
    String outFile = StringUtils.format("%s/%s.csv", outputPath, rulesTableName);
    try (
        CSVReader reader = openCsvReader(inFile);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)
    ) {
      String[] parsed;
      while ((parsed = readRecord(reader, inFile, 4)) != null) {
        String newLine = SQLMetadataConnector.csvEscape(parsed[0]) + "," //id
                         + SQLMetadataConnector.csvEscape(parsed[1]) + "," //dataSource
                         + SQLMetadataConnector.csvEscape(parsed[2]) + "," //version
                         + rewriteHexPayloadAsEscapedJson(parsed[3]) //payload
                         + "\n";
        writer.write(newLine);

      }
    }
    catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  private void rewriteConfigExport(
      String configTableName
  )
  {
    String inFile = StringUtils.format(("%s/%s_raw.csv"), outputPath, configTableName);
    String outFile = StringUtils.format("%s/%s.csv", outputPath, configTableName);
    try (
        CSVReader reader = openCsvReader(inFile);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)
    ) {
      String[] parsed;
      while ((parsed = readRecord(reader, inFile, 2)) != null) {
        String newLine = SQLMetadataConnector.csvEscape(parsed[0]) + "," //name
                         + rewriteHexPayloadAsEscapedJson(parsed[1]) //payload
                         + "\n";
        writer.write(newLine);

      }
    }
    catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  private void rewriteSupervisorExport(
      String supervisorTableName
  )
  {
    String inFile = StringUtils.format(("%s/%s_raw.csv"), outputPath, supervisorTableName);
    String outFile = StringUtils.format("%s/%s.csv", outputPath, supervisorTableName);
    try (
        CSVReader reader = openCsvReader(inFile);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)
    ) {
      String[] parsed;
      while ((parsed = readRecord(reader, inFile, 4)) != null) {
        String newLine = SQLMetadataConnector.csvEscape(parsed[0]) + "," //id
                         + SQLMetadataConnector.csvEscape(parsed[1]) + "," //spec_id
                         + SQLMetadataConnector.csvEscape(parsed[2]) + "," //created_date
                         + rewriteHexPayloadAsEscapedJson(parsed[3]) //payload
                         + "\n";
        writer.write(newLine);

      }
    }
    catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }


  void rewriteSegmentsExport(
      String segmentsTableName
  )
  {
    String inFile = StringUtils.format(("%s/%s_raw.csv"), outputPath, segmentsTableName);
    String outFile = StringUtils.format("%s/%s.csv", outputPath, segmentsTableName);
    try (
        CSVReader reader = openCsvReader(inFile);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outFile), StandardCharsets.UTF_8)
    ) {
      String[] parsed;
      while ((parsed = readRecord(reader, inFile, 9)) != null) {
        StringBuilder newLineBuilder = new StringBuilder();
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[0])).append(","); //id
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[1])).append(","); //dataSource
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[2])).append(","); //created_date
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[3])).append(","); //start
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[4])).append(","); //end
        newLineBuilder.append(convertBooleanString(parsed[5])).append(","); //partitioned
        newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[6])).append(","); //version
        newLineBuilder.append(convertBooleanString(parsed[7])).append(","); //used

        if (s3Bucket != null || hadoopStorageDirectory != null || newLocalPath != null) {
          newLineBuilder.append(makePayloadWithConvertedLoadSpec(parsed[8]));
        } else {
          newLineBuilder.append(rewriteHexPayloadAsEscapedJson(parsed[8])); //payload
        }

        // Preserve any additional columns after payload (e.g. used_status_last_updated,
        // indexing_state_fingerprint, upgraded_from_segment_id, schema_fingerprint, num_rows)
        for (int i = 9; i < parsed.length; i++) {
          newLineBuilder.append(",");
          newLineBuilder.append(SQLMetadataConnector.csvEscape(parsed[i]));
        }
        newLineBuilder.append("\n");
        writer.write(newLineBuilder.toString());

      }
    }
    catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  /**
   * Opens a record-aware CSV reader on the given file. A single CSV record may span multiple physical
   * lines if a field value contains a newline, so records must be read with
   * {@link CSVReader#readNext()} rather than by reading one line at a time.
   *
   * The parser follows RFC 4180, matching the output written by the export stage: quotes are doubled and
   * backslashes are not escape characters. Carriage returns are kept, so that a carriage return which is
   * part of a value is preserved rather than silently dropped. This relies on the raw CSV using LF line
   * endings, which is the case for the files written by the export stage.
   */
  private static CSVReader openCsvReader(String inFile) throws IOException
  {
    return new CSVReaderBuilder(
        new BufferedReader(new InputStreamReader(new FileInputStream(inFile), StandardCharsets.UTF_8))
    )
        .withCSVParser(new RFC4180ParserBuilder().build())
        .withKeepCarriageReturn(true)
        .build();
  }

  /**
   * Reads the next record of the given reader, or returns null at the end of the file.
   *
   * @param minFields minimum number of fields the record must have, to fail with a useful message
   *                  instead of an {@link ArrayIndexOutOfBoundsException} on a malformed file
   */
  private static String[] readRecord(CSVReader reader, String inFile, int minFields) throws IOException
  {
    final String[] record = reader.readNext();
    if (record == null) {
      return null;
    }
    if (record.length < minFields) {
      throw new ISE(
          "Row[%d] of file[%s] has [%d] fields, expected at least [%d].",
          reader.getLinesRead(),
          inFile,
          record.length,
          minFields
      );
    }
    return record;
  }

  /**
   * Returns a new load spec in escaped JSON form, with the new deep storage location if configured.
   */
  private String makePayloadWithConvertedLoadSpec(
      String payload
  ) throws IOException
  {
    DataSegment segment = JSON_MAPPER.readValue(DatatypeConverter.parseHexBinary(payload), DataSegment.class);
    String uniqueId = getUniqueIDFromLocalLoadSpec(segment.getLoadSpec());
    String segmentPath = DataSegmentPusher.getDefaultStorageDirWithExistingUniquePath(segment, uniqueId);

    Map<String, Object> newLoadSpec = null;
    if (s3Bucket != null) {
      newLoadSpec = makeS3LoadSpec(segmentPath);
    } else if (hadoopStorageDirectory != null) {
      newLoadSpec = makeHDFSLoadSpec(segmentPath);
    } else if (newLocalPath != null) {
      newLoadSpec = makeLocalLoadSpec(segmentPath);
    }

    if (newLoadSpec != null) {
      segment = DataSegment.builder(segment).loadSpec(newLoadSpec).build();
    }

    String serialized = JSON_MAPPER.writeValueAsString(segment);
    if (useHexBlobs) {
      return DatatypeConverter.printHexBinary(StringUtils.toUtf8(serialized));
    } else {
      return escapeJSONForCSV(serialized);
    }
  }

  /**
   * Derby's export tool writes BLOB columns as a hexadecimal string:
   * https://db.apache.org/derby/docs/10.9/adminguide/cadminimportlobs.html
   *
   * Decodes the hex string and escapes the decoded JSON.
   */
  private String rewriteHexPayloadAsEscapedJson(
      String payload
  )
  {
    if (useHexBlobs) {
      return payload;
    }
    String json = StringUtils.fromUtf8(DatatypeConverter.parseHexBinary(payload));
    return escapeJSONForCSV(json);
  }

  private String convertBooleanString(String booleanString)
  {
    if (booleansAsStrings) {
      return booleanString;
    } else {
      return "true".equals(booleanString) ? "1" : "0";
    }
  }

  private String escapeJSONForCSV(String json)
  {
    return "\"" + StringUtils.replace(json, "\"", "\"\"") + "\"";
  }

  private Map<String, Object> makeS3LoadSpec(
      String segmentPath
  )
  {
    return ImmutableMap.of(
        "type", "s3_zip",
        "bucket", s3Bucket,
        "key", StringUtils.format("%s/%s/index.zip", s3baseKey, segmentPath)
    );
  }

  /**
   * Makes an HDFS spec, replacing colons with underscores. HDFS doesn't support colons in filenames.
   */
  private Map<String, Object> makeHDFSLoadSpec(
      String segmentPath
  )
  {
    return ImmutableMap.of(
        "type", "hdfs",
        "path", StringUtils.format("%s/%s/index.zip", hadoopStorageDirectory, segmentPath.replace(':', '_'))
    );
  }

  private Map<String, Object> makeLocalLoadSpec(
      String segmentPath
  )
  {
    return ImmutableMap.of(
        "type", "local",
        "path", StringUtils.format("%s/%s/index.zip", newLocalPath, segmentPath)
    );
  }

  /**
   * Looks for an optional unique path component in the segment path.
   * The unique path is used for segments created by realtime indexing tasks like Kafka.
   */
  @Nullable
  private String getUniqueIDFromLocalLoadSpec(
      Map<String, Object> localLoadSpec
  )
  {
    String[] splits = ((String) localLoadSpec.get("path")).split("/");
    if (splits.length < 2) {
      return null;
    }
    String maybeUUID = splits[splits.length - 2];

    try {
      UUID.fromString(maybeUUID);
      return maybeUUID;
    }
    catch (IllegalArgumentException iae) {
      return null;
    }
  }
}
