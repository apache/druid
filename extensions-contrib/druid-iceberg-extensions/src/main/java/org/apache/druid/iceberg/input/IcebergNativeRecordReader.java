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

package org.apache.druid.iceberg.input;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@link InputSourceReader} that reads one Iceberg data file and applies any associated
 * position-delete and equality-delete files before converting records to Druid {@link InputRow}s.
 *
 * <p>The table schema is deserialized from a JSON string (captured on the coordinator at
 * planning time) — no catalog interaction is needed for schema resolution on the worker.
 *
 * <p>File I/O is handled by {@link WarehouseFileIO} for local filesystem paths.  For non-local
 * paths (S3, HDFS) a fallback is made to the catalog's own {@link FileIO} (Phase 1 limitation;
 * see {@code WarehouseFileIO} Javadoc for details).
 *
 * <p>This reader is constructed by {@link IcebergFileTaskInputSource#reader} and handles the
 * Iceberg V2 delete semantics in a straightforward way:
 * <ol>
 *   <li>Deserialize the table schema from the embedded JSON string.</li>
 *   <li>Resolve the {@link FileIO} to use for this data file's path.</li>
 *   <li>Load position delete files and build a set of row positions to skip.</li>
 *   <li>Load equality delete files and build sets of field-value maps to skip.</li>
 *   <li>Stream through the data file, filtering out any deleted rows.</li>
 *   <li>Convert surviving {@link Record}s to {@link MapBasedInputRow} via
 *       {@link IcebergRecordConverter}.</li>
 * </ol>
 *
 * <p>Only Parquet format is supported for the initial implementation.
 */
public class IcebergNativeRecordReader implements InputSourceReader
{
  private static final Logger log = new Logger(IcebergNativeRecordReader.class);

  private static final WarehouseFileIO WAREHOUSE_FILE_IO = new WarehouseFileIO();

  private final String dataFilePath;
  private final String fileFormat;
  private final long dataFileSizeInBytes;
  private final long dataFileRecordCount;
  private final List<IcebergFileTaskInputSource.DeleteFileInfo> deleteFiles;
  private final String tableSchemaJson;
  private final String tableNamespace;
  private final String tableName;
  @Nullable
  private final IcebergCatalog icebergCatalog;
  @Nullable
  private final InputSourceFactory warehouseSource;
  private final InputRowSchema inputRowSchema;

  public IcebergNativeRecordReader(
      String dataFilePath,
      String fileFormat,
      long dataFileSizeInBytes,
      long dataFileRecordCount,
      List<IcebergFileTaskInputSource.DeleteFileInfo> deleteFiles,
      String tableSchemaJson,
      String tableNamespace,
      String tableName,
      @Nullable IcebergCatalog icebergCatalog,
      @Nullable InputSourceFactory warehouseSource,
      InputRowSchema inputRowSchema
  )
  {
    this.dataFilePath = dataFilePath;
    this.fileFormat = fileFormat;
    this.dataFileSizeInBytes = dataFileSizeInBytes;
    this.dataFileRecordCount = dataFileRecordCount;
    this.deleteFiles = deleteFiles;
    this.tableSchemaJson = tableSchemaJson;
    this.tableNamespace = tableNamespace;
    this.tableName = tableName;
    this.icebergCatalog = icebergCatalog;
    this.warehouseSource = warehouseSource;
    this.inputRowSchema = inputRowSchema;
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
  {
    return streamRows(inputStats);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    CloseableIterator<InputRow> delegate = streamRows(null);
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        InputRow row = delegate.next();
        return InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent());
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  /**
   * Loads delete metadata upfront (delete files are small), then opens the data file and returns
   * a lazy {@link CloseableIterator} that skips deleted rows and converts surviving records
   * one at a time — no full materialization into a list.
   */
  private CloseableIterator<InputRow> streamRows(InputStats inputStats) throws IOException
  {
    Schema tableSchema = SchemaParser.fromJson(tableSchemaJson);
    FileIO io = resolveFileIO(dataFilePath);

    Set<Long> deletedPositions = new HashSet<>();
    List<EqualityDeleteSet> equalityDeleteSets = new ArrayList<>();
    for (IcebergFileTaskInputSource.DeleteFileInfo deleteFile : deleteFiles) {
      FileIO deleteIO = resolveFileIO(deleteFile.getPath());
      if (deleteFile.isPositionDelete()) {
        collectPositionDeletes(deleteIO, deleteFile.getPath(), deletedPositions);
      } else if (deleteFile.isEqualityDelete()) {
        collectEqualityDeletes(deleteIO, tableSchema, deleteFile, equalityDeleteSets);
      }
    }

    if (!"PARQUET".equalsIgnoreCase(fileFormat)) {
      throw new UnsupportedOperationException(
          "IcebergNativeRecordReader currently supports PARQUET format only, got: " + fileFormat
      );
    }

    if (inputStats != null) {
      inputStats.incrementProcessedBytes(dataFileSizeInBytes);
    }

    final CloseableIterable<Record> dataRecords = Parquet.read(io.newInputFile(dataFilePath))
                                                         .project(tableSchema)
                                                         .createReaderFunc(
                                                             fileSchema -> GenericParquetReaders.buildReader(
                                                                 tableSchema,
                                                                 fileSchema
                                                             ))
                                                         .build();
    final Iterator<Record> rawIter = dataRecords.iterator();

    return new CloseableIterator<InputRow>()
    {
      private long position = 0;
      private InputRow pending = null;
      private boolean exhausted = false;

      @Override
      public boolean hasNext()
      {
        if (pending != null) {
          return true;
        }
        if (exhausted) {
          return false;
        }
        advance();
        return pending != null;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        InputRow row = pending;
        pending = null;
        return row;
      }

      private void advance()
      {
        while (rawIter.hasNext()) {
          Record record = rawIter.next();
          long pos = position++;
          if (deletedPositions.contains(pos)) {
            continue;
          }
          boolean eqDeleted = false;
          for (EqualityDeleteSet eqSet : equalityDeleteSets) {
            if (eqSet.matches(record)) {
              eqDeleted = true;
              break;
            }
          }
          if (eqDeleted) {
            continue;
          }
          Map<String, Object> rowMap = IcebergRecordConverter.convertToMap(record, tableSchema);
          pending = IcebergRecordConverter.toInputRow(rowMap, inputRowSchema);
          return;
        }
        exhausted = true;
      }

      @Override
      public void close() throws IOException
      {
        dataRecords.close();
      }
    };
  }

  /**
   * Returns a {@link FileIO} appropriate for the given path.
   *
   * <ul>
   *   <li>Local filesystem paths: {@link WarehouseFileIO} (Iceberg built-in local file access,
   *       no catalog interaction).</li>
   *   <li>Non-local paths (S3, HDFS): falls back to the catalog's {@link FileIO}. Requires
   *       {@code icebergCatalog} to be non-null. This is the Phase 1 fallback; full
   *       {@code WarehouseFileIO} support for non-local storage is deferred to Phase 2.</li>
   * </ul>
   */
  private FileIO resolveFileIO(String path)
  {
    try {
      // WarehouseFileIO handles local paths; throws UnsupportedOperationException for others.
      WAREHOUSE_FILE_IO.newInputFile(path);
      return WAREHOUSE_FILE_IO;
    }
    catch (UnsupportedOperationException e) {
      // Non-local path — fall back to catalog FileIO.
      if (icebergCatalog == null) {
        throw new IAE(
            "Cannot open non-local path [%s]: icebergCatalog is required as a FileIO fallback "
            + "for S3/HDFS paths in Phase 1, but no catalog was provided.",
            path
        );
      }
      log.debug(
          "Path [%s] is non-local; falling back to catalog FileIO (Phase 1 limitation).", path
      );
      return getCatalogFileIO();
    }
  }

  /**
   * Loads the table from the catalog to obtain its {@link FileIO}.
   * Called only for non-local paths as a Phase 1 fallback.
   */
  private FileIO getCatalogFileIO()
  {
    Catalog catalog = icebergCatalog.retrieveCatalog();
    try {
      TableIdentifier id = TableIdentifier.of(Namespace.of(tableNamespace), tableName);
      return catalog.loadTable(id).io();
    }
    catch (Exception e) {
      throw new RE(e, "Failed to obtain catalog FileIO for path [%s]", dataFilePath);
    }
  }

  // ---- position deletes ----

  private void collectPositionDeletes(FileIO io, String deleteFilePath, Set<Long> deletedPositions)
      throws IOException
  {
    Schema posDelSchema = DeleteSchemaUtil.pathPosSchema();

    try (CloseableIterable<Record> records = Parquet.read(io.newInputFile(deleteFilePath))
                                                    .project(posDelSchema)
                                                    .createReaderFunc(
                                                        fileSchema -> GenericParquetReaders.buildReader(
                                                            posDelSchema,
                                                            fileSchema
                                                        ))
                                                    .build()) {
      for (Record posDelRecord : records) {
        String filePath = (String) posDelRecord.getField("file_path");
        if (dataFilePath.equals(filePath)) {
          deletedPositions.add((Long) posDelRecord.getField("pos"));
        }
      }
    }
    log.debug("Collected [%d] position deletes for file [%s]", deletedPositions.size(), dataFilePath);
  }

  // ---- equality deletes ----

  private void collectEqualityDeletes(
      FileIO io,
      Schema tableSchema,
      IcebergFileTaskInputSource.DeleteFileInfo deleteFile,
      List<EqualityDeleteSet> equalityDeleteSets
  ) throws IOException
  {
    List<Integer> fieldIds = deleteFile.getEqualityFieldIds();
    List<String> fieldNames = fieldIds.stream()
                                      .map(tableSchema::findColumnName)
                                      .filter(name -> name != null)
                                      .collect(Collectors.toList());

    if (fieldNames.isEmpty()) {
      log.warn(
          "Equality delete file [%s] references field IDs %s that are not found in table schema — skipping",
          deleteFile.getPath(), fieldIds
      );
      return;
    }

    Schema eqSchema = tableSchema.select(fieldNames.toArray(new String[0]));
    Set<Map<String, Object>> keys = new HashSet<>();

    try (CloseableIterable<Record> records = Parquet.read(io.newInputFile(deleteFile.getPath()))
                                                    .project(eqSchema)
                                                    .createReaderFunc(
                                                        fileSchema -> GenericParquetReaders.buildReader(
                                                            eqSchema,
                                                            fileSchema
                                                        ))
                                                    .build()) {
      for (Record eqRecord : records) {
        Map<String, Object> key = new HashMap<>();
        for (Types.NestedField field : eqSchema.columns()) {
          key.put(field.name(), eqRecord.getField(field.name()));
        }
        keys.add(key);
      }
    }

    equalityDeleteSets.add(new EqualityDeleteSet(fieldNames, keys));
    log.debug(
        "Collected [%d] equality delete keys from file [%s]",
        keys.size(), deleteFile.getPath()
    );
  }

  // ---- helper ----

  /**
   * Holds the equality field names and a set of key maps read from one equality-delete file.
   */
  private static class EqualityDeleteSet
  {
    private final List<String> fieldNames;
    private final Set<Map<String, Object>> keys;

    EqualityDeleteSet(List<String> fieldNames, Set<Map<String, Object>> keys)
    {
      this.fieldNames = fieldNames;
      this.keys = keys;
    }

    boolean matches(Record dataRecord)
    {
      Map<String, Object> key = new HashMap<>();
      for (String fieldName : fieldNames) {
        key.put(fieldName, dataRecord.getField(fieldName));
      }
      return keys.contains(key);
    }
  }
}
