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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.cli.MetadataCsvExporter.ColumnKind;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * How one metadata table is exported: the canonical order in which its columns are written, matching the import
   * commands documented in {@code docs/operations/export-metadata.md}, the columns which are not exported (in lower
   * case), and whether the table may be absent from the metadata store, in which case it is skipped instead of
   * failing the export.
   */
  static final class TableSpec
  {
    final List<String> columnOrder;
    final Set<String> excludedColumns;
    final boolean optional;

    TableSpec(final List<String> columnOrder)
    {
      this(columnOrder, ImmutableSet.of(), false);
    }

    TableSpec(final List<String> columnOrder, final Set<String> excludedColumns, final boolean optional)
    {
      this.columnOrder = columnOrder;
      this.excludedColumns = excludedColumns;
      this.optional = optional;
    }
  }

  private static final TableSpec DATASOURCE = new TableSpec(
      ImmutableList.of("dataSource", "created_date", "commit_metadata_payload", "commit_metadata_sha1")
  );

  private static final TableSpec RULES = new TableSpec(
      ImmutableList.of("id", "dataSource", "version", "payload")
  );

  private static final TableSpec CONFIG = new TableSpec(
      ImmutableList.of("name", "payload")
  );

  private static final TableSpec SUPERVISORS = new TableSpec(
      ImmutableList.of("id", "spec_id", "created_date", "payload")
  );

  static final TableSpec SEGMENTS = new TableSpec(
      ImmutableList.of(
          "id", "dataSource", "created_date", "start", "end", "partitioned", "version", "used", "payload",
          "used_status_last_updated", "indexing_state_fingerprint", "upgraded_from_segment_id",
          "schema_fingerprint", "num_rows"
      )
  );

  /**
   * The table holding the schemas that the {@code schema_fingerprint} of a segment refers to. It only exists if
   * centralized datasource schema is enabled, and its {@code id} is a generated identity column which nothing
   * refers to, so it is not exported: importing a value into such a column is rejected outright by Derby and needs
   * database-specific handling elsewhere, while letting the target database generate the ids works everywhere.
   */
  static final TableSpec SEGMENT_SCHEMAS = new TableSpec(
      ImmutableList.of(
          "fingerprint", "created_date", "datasource", "payload", "used", "used_status_last_updated", "version"
      ),
      ImmutableSet.of("id"),
      true
  );

  /**
   * The table holding the indexing states that the {@code indexing_state_fingerprint} of a segment refers to. It
   * does not exist in metadata stores written by older Druid versions.
   */
  static final TableSpec INDEXING_STATES = new TableSpec(
      ImmutableList.of(
          "fingerprint", "created_date", "dataSource", "payload", "used", "pending", "used_status_last_updated"
      ),
      ImmutableSet.of(),
      true
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
    configureJsonMapper();

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
    final SQLMetadataConnector dbConnector = injector.getInstance(SQLMetadataConnector.class);
    final MetadataStorageTablesConfig tablesConfig = injector.getInstance(MetadataStorageTablesConfig.class);
    final MetadataCsvExporter exporter = new MetadataCsvExporter(dbConnector);

    exportTable(exporter, dbConnector, tablesConfig.getDataSourceTable(), DATASOURCE, this::convertValue);
    exportTable(exporter, dbConnector, tablesConfig.getSegmentsTable(), SEGMENTS, this::convertSegmentValue);
    exportTable(exporter, dbConnector, tablesConfig.getRulesTable(), RULES, this::convertValue);
    exportTable(exporter, dbConnector, tablesConfig.getConfigTable(), CONFIG, this::convertValue);
    exportTable(exporter, dbConnector, tablesConfig.getSupervisorTable(), SUPERVISORS, this::convertValue);
    // The tables the fingerprints of a segment refer to. Without them, an imported segment refers to a schema and an
    // indexing state which the target metadata store does not have.
    exportTable(exporter, dbConnector, tablesConfig.getSegmentSchemasTable(), SEGMENT_SCHEMAS, this::convertValue);
    exportTable(exporter, dbConnector, tablesConfig.getIndexingStatesTable(), INDEXING_STATES, this::convertValue);
  }

  static void configureJsonMapper()
  {
    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, JSON_MAPPER);
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    JSON_MAPPER.setInjectableValues(injectableValues);
  }

  /**
   * Exports one metadata table to {@code <outputPath>/<tableName>.csv}, writing the columns of the source table in
   * the canonical order of the given spec.
   */
  void exportTable(
      final MetadataCsvExporter exporter,
      final SQLMetadataConnector dbConnector,
      final String tableName,
      final TableSpec spec,
      final MetadataCsvExporter.ValueConverter converter
  )
  {
    // Derby folds unquoted identifiers to upper case, so its tables are named in upper case, while PostgreSQL folds
    // them to lower case and uses the table names as configured.
    final String sourceTableName = isDerby() ? StringUtils.toUpperCase(tableName) : tableName;
    final List<String> columns =
        orderColumns(spec.columnOrder, dbConnector.getTableColumns(sourceTableName), spec.excludedColumns);
    // An empty column list means the table does not exist: a failed lookup throws instead, so that a table which
    // cannot be read is never mistaken for one the metadata store does not have.
    if (columns.isEmpty()) {
      if (spec.optional) {
        // Remove the file of an earlier export into the same directory, which would otherwise be imported as this
        // table's data.
        deleteOutputFile(tableName);
        log.info("Skipping table[%s], which this metadata store does not have.", tableName);
        return;
      }
      throw new ISE("Table[%s] does not exist in this metadata store.", sourceTableName);
    }

    log.info("Exporting table[%s].", tableName);
    exporter.exportTable(sourceTableName, columns, makeOutputFile(tableName), converter);
  }

  private Path makeOutputFile(final String tableName)
  {
    return Paths.get(outputPath, StringUtils.format("%s.csv", tableName));
  }

  private void deleteOutputFile(final String tableName)
  {
    final Path outputFile = makeOutputFile(tableName);
    try {
      Files.deleteIfExists(outputFile);
    }
    catch (IOException e) {
      throw new ISE(e, "Could not delete stale file[%s] of skipped table[%s].", outputFile, tableName);
    }
  }

  /**
   * Orders the given actual column names of a table by the given canonical order, ignoring case, skipping canonical
   * columns which the table does not have and appending any unknown columns at the end in their original order.
   * Columns in {@code excludedColumns} are left out. Exporting this explicit column list keeps the output
   * independent of the physical column order, which depends on the order in which {@code ALTER TABLE} added the
   * newer columns.
   */
  static List<String> orderColumns(
      final List<String> columnOrder,
      final List<String> actualColumns,
      final Set<String> excludedColumns
  )
  {
    final Map<String, String> remaining = new LinkedHashMap<>();
    for (String column : actualColumns) {
      final String lowerCaseColumn = StringUtils.toLowerCase(column);
      if (!excludedColumns.contains(lowerCaseColumn)) {
        remaining.put(lowerCaseColumn, column);
      }
    }

    final List<String> ordered = new ArrayList<>(actualColumns.size());
    for (String column : columnOrder) {
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

  /**
   * Converts one exported value: BLOB payloads are decoded as JSON unless {@code --use-hex-blobs} was given, and
   * booleans are written as 1 and 0 unless {@code --booleans-as-strings} was given. A NULL stays a NULL.
   */
  @Nullable
  String convertValue(final ColumnKind kind, @Nullable final String value)
  {
    if (value == null) {
      return null;
    }
    switch (kind) {
      case BINARY:
        return convertPayload(value);
      case BOOLEAN:
        return convertBooleanString(value);
      default:
        return value;
    }
  }

  /**
   * Converts one exported value of the segments table, rewriting the deep storage location of the segment payload
   * if one of {@code --s3bucket}, {@code --hadoopStorageDirectory} or {@code --newLocalPath} was given, and
   * otherwise behaving like {@link #convertValue}.
   */
  @Nullable
  String convertSegmentValue(final ColumnKind kind, @Nullable final String value) throws IOException
  {
    if (kind == ColumnKind.BINARY && value != null && isDeepStorageMigration()) {
      return makePayloadWithConvertedLoadSpec(value);
    }
    return convertValue(kind, value);
  }

  private boolean isDeepStorageMigration()
  {
    return s3Bucket != null || hadoopStorageDirectory != null || newLocalPath != null;
  }

  /**
   * Returns the segment payload in JSON form, with the new deep storage location if configured.
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
      return serialized;
    }
  }

  /**
   * Converts a BLOB value, which the exporter reads as a hexadecimal string, to the JSON it holds, unless hex blobs
   * were requested.
   */
  private String convertPayload(final String payload)
  {
    if (useHexBlobs) {
      return payload;
    }
    return StringUtils.fromUtf8(DatatypeConverter.parseHexBinary(payload));
  }

  private String convertBooleanString(final String booleanString)
  {
    if (booleansAsStrings) {
      return booleanString;
    } else {
      return "true".equals(booleanString) ? "1" : "0";
    }
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
