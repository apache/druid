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
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An {@link InputSourceReader} that reads an Iceberg data file and applies
 * associated positional and equality delete files before converting records
 * to Druid {@link InputRow} objects.
 *
 * Delete application follows the Iceberg v2 spec:
 * <ol>
 *   <li>Positional deletes: read (file_path, pos) pairs, filter by current data file,
 *       build a Set of deleted positions</li>
 *   <li>Equality deletes: read key tuples from equality delete files, build Sets
 *       of deleted key values per equality field set</li>
 *   <li>Stream data file: for each record, skip if position-deleted or equality-deleted</li>
 * </ol>
 *
 * All reads use Iceberg's Parquet reader with {@link GenericParquetReaders} for
 * schema-aware reading. Files are accessed via Hadoop {@link Configuration}.
 */
public class IcebergNativeRecordReader implements InputSourceReader
{
  private static final Logger log = new Logger(IcebergNativeRecordReader.class);

  private final String dataFilePath;
  private final List<DeleteFileInfo> deleteFiles;
  private final String tableSchemaJson;
  private final InputSourceFactory warehouseSource;
  private final InputRowSchema inputRowSchema;
  private final Configuration hadoopConf;
  private final FileIO fileIO;

  public IcebergNativeRecordReader(
      final String dataFilePath,
      final List<DeleteFileInfo> deleteFiles,
      final String tableSchemaJson,
      final InputSourceFactory warehouseSource,
      final InputRowSchema inputRowSchema,
      @Nullable final String fileIOImpl,
      @Nullable final Map<String, String> fileIOProperties
  )
  {
    this.dataFilePath = dataFilePath;
    this.deleteFiles = deleteFiles;
    this.tableSchemaJson = tableSchemaJson;
    this.warehouseSource = warehouseSource;
    this.inputRowSchema = inputRowSchema;
    this.hadoopConf = new Configuration();
    this.fileIO = buildFileIO(fileIOImpl, fileIOProperties, hadoopConf);
  }

  private static FileIO buildFileIO(
      @Nullable final String fileIOImpl,
      @Nullable final Map<String, String> fileIOProperties,
      final Configuration hadoopConf
  )
  {
    final Map<String, String> props = fileIOProperties == null ? Collections.emptyMap() : fileIOProperties;
    if (fileIOImpl == null || fileIOImpl.isEmpty()) {
      return new HadoopFileIO(hadoopConf);
    }
    return CatalogUtil.loadFileIO(fileIOImpl, props, hadoopConf);
  }

  @Override
  public CloseableIterator<InputRow> read(final InputStats inputStats) throws IOException
  {
    final Schema tableSchema = SchemaParser.fromJson(tableSchemaJson);

    // Step 1: Collect positional deletes
    final Set<Long> deletedPositions = collectPositionalDeletes();

    // Step 2: Collect equality deletes
    final List<EqualityDeleteSet> equalityDeleteSets = collectEqualityDeletes(tableSchema);

    // Step 3: Stream data file with delete application
    final InputFile dataInputFile = fileIO.newInputFile(dataFilePath);
    final CloseableIterable<Record> records = Parquet.read(dataInputFile)
                                                     .project(tableSchema)
                                                     .createReaderFunc(
                                                         fileSchema -> GenericParquetReaders.buildReader(
                                                             tableSchema,
                                                             fileSchema
                                                         )
                                                     )
                                                     .build();

    final IcebergRecordConverter converter = new IcebergRecordConverter(tableSchema);

    return new CloseableIterator<InputRow>()
    {
      private final Iterator<Record> delegate = records.iterator();
      private long position = 0;
      private InputRow nextRow = null;

      @Override
      public boolean hasNext()
      {
        while (nextRow == null && delegate.hasNext()) {
          final Record record = delegate.next();
          final long currentPos = position++;

          // Step 4: Apply positional deletes
          if (!deletedPositions.isEmpty() && deletedPositions.contains(currentPos)) {
            continue;
          }

          // Step 5: Apply equality deletes
          if (isEqualityDeleted(record, equalityDeleteSets)) {
            continue;
          }

          // Step 6: Convert surviving record
          final Map<String, Object> map = converter.convert(record);
          nextRow = MapInputRowParser.parse(inputRowSchema, map);
        }
        return nextRow != null;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final InputRow row = nextRow;
        nextRow = null;
        return row;
      }

      @Override
      public void close() throws IOException
      {
        records.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final CloseableIterator<InputRow> reader = read(null);
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public boolean hasNext()
      {
        return reader.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        final InputRow row = reader.next();
        return InputRowListPlusRawValues.of(row, Collections.emptyMap());
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }
    };
  }

  /**
   * Reads all positional delete files and collects positions that apply to the
   * current data file.
   */
  private Set<Long> collectPositionalDeletes() throws IOException
  {
    final Set<Long> deletedPositions = new HashSet<>();
    final Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();

    for (final DeleteFileInfo deleteFileInfo : deleteFiles) {
      if (deleteFileInfo.getContentType() != DeleteFileInfo.ContentType.POSITION) {
        continue;
      }

      final InputFile deleteInputFile = fileIO.newInputFile(deleteFileInfo.getPath());

      try (CloseableIterable<Record> deleteRecords = Parquet.read(deleteInputFile)
                                                            .project(posDeleteSchema)
                                                            .createReaderFunc(
                                                                fileSchema -> GenericParquetReaders.buildReader(
                                                                    posDeleteSchema,
                                                                    fileSchema
                                                                )
                                                            )
                                                            .build()) {
        for (final Record deleteRecord : deleteRecords) {
          final String filePath = deleteRecord.getField("file_path").toString();
          if (dataFilePath.equals(filePath)) {
            final long pos = (Long) deleteRecord.getField("pos");
            deletedPositions.add(pos);
          }
        }
      }
    }

    if (!deletedPositions.isEmpty()) {
      log.info("Collected [%d] positional deletes for data file [%s]", deletedPositions.size(), dataFilePath);
    }

    return deletedPositions;
  }

  /**
   * Reads all equality delete files and builds sets of deleted key tuples.
   */
  private List<EqualityDeleteSet> collectEqualityDeletes(final Schema tableSchema) throws IOException
  {
    final List<EqualityDeleteSet> result = new ArrayList<>();

    for (final DeleteFileInfo deleteFileInfo : deleteFiles) {
      if (deleteFileInfo.getContentType() != DeleteFileInfo.ContentType.EQUALITY) {
        continue;
      }

      // Build projected schema from equality field IDs
      final List<Types.NestedField> equalityFields = new ArrayList<>();
      final List<String> fieldNames = new ArrayList<>();
      for (final int fieldId : deleteFileInfo.getEqualityFieldIds()) {
        final Types.NestedField field = tableSchema.findField(fieldId);
        if (field != null) {
          equalityFields.add(field);
          fieldNames.add(field.name());
        }
      }

      if (equalityFields.isEmpty()) {
        continue;
      }

      final Schema deleteSchema = new Schema(equalityFields);
      final InputFile deleteInputFile = fileIO.newInputFile(deleteFileInfo.getPath());

      final Set<List<Object>> deletedKeys = new HashSet<>();
      try (CloseableIterable<Record> deleteRecords = Parquet.read(deleteInputFile)
                                                            .project(deleteSchema)
                                                            .createReaderFunc(
                                                                fileSchema -> GenericParquetReaders.buildReader(
                                                                    deleteSchema,
                                                                    fileSchema
                                                                )
                                                            )
                                                            .build()) {
        for (final Record deleteRecord : deleteRecords) {
          final List<Object> key = new ArrayList<>(fieldNames.size());
          for (final String fieldName : fieldNames) {
            final Object value = deleteRecord.getField(fieldName);
            key.add(value);
          }
          deletedKeys.add(key);
        }
      }

      if (!deletedKeys.isEmpty()) {
        result.add(new EqualityDeleteSet(fieldNames, deletedKeys));
        log.info(
            "Collected [%d] equality deletes on fields %s for data file [%s]",
            deletedKeys.size(),
            fieldNames,
            dataFilePath
        );
      }
    }

    return result;
  }

  /**
   * Checks whether a record matches any equality delete set.
   */
  private static boolean isEqualityDeleted(final Record record, final List<EqualityDeleteSet> equalityDeleteSets)
  {
    for (final EqualityDeleteSet deleteSet : equalityDeleteSets) {
      final List<Object> key = new ArrayList<>(deleteSet.fieldNames.size());
      for (final String fieldName : deleteSet.fieldNames) {
        key.add(record.getField(fieldName));
      }
      if (deleteSet.deletedKeys.contains(key)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Holds a set of deleted key tuples for a single equality delete file.
   */
  private static class EqualityDeleteSet
  {
    final List<String> fieldNames;
    final Set<List<Object>> deletedKeys;

    EqualityDeleteSet(final List<String> fieldNames, final Set<List<Object>> deletedKeys)
    {
      this.fieldNames = fieldNames;
      this.deletedKeys = deletedKeys;
    }
  }
}
