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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.SplittableInputSource;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A {@link SplittableInputSource} representing a single Iceberg v2 data file with its
 * associated delete files. Created internally by {@link IcebergInputSource} when v2
 * delete files are detected. Reports itself as non-splittable (one file = one split)
 * so MSQ routes it to a single worker without further splitting.
 *
 * This input source is JSON-serializable, carrying all metadata needed for a worker
 * to read a data file and apply deletes without catalog access:
 * <ul>
 *   <li>Data file path</li>
 *   <li>Delete file metadata (paths, types, equality field IDs)</li>
 *   <li>Serialized Iceberg table schema (JSON)</li>
 *   <li>warehouseSource for file I/O</li>
 * </ul>
 */
public class IcebergFileTaskInputSource implements SplittableInputSource<List<String>>
{
  public static final String TYPE_KEY = "icebergFileTask";

  private final String dataFilePath;
  private final List<DeleteFileInfo> deleteFiles;
  private final String tableSchemaJson;
  private final InputSourceFactory warehouseSource;
  private final String fileIOImpl;
  private final Map<String, String> fileIOProperties;
  // TODO https://github.com/apache/druid/issues/19472: extend to ORC/AVRO once iceberg-orc/iceberg-avro deps are added
  private final String fileFormat;

  @JsonCreator
  public IcebergFileTaskInputSource(
      @JsonProperty("dataFilePath") final String dataFilePath,
      @JsonProperty("deleteFiles") final List<DeleteFileInfo> deleteFiles,
      @JsonProperty("tableSchemaJson") final String tableSchemaJson,
      @JsonProperty("warehouseSource") final InputSourceFactory warehouseSource,
      @JsonProperty("fileIOImpl") @Nullable final String fileIOImpl,
      @JsonProperty("fileIOProperties") @Nullable final Map<String, String> fileIOProperties,
      @JsonProperty("fileFormat") @Nullable final String fileFormat
  )
  {
    this.dataFilePath = dataFilePath;
    this.deleteFiles = deleteFiles;
    this.tableSchemaJson = tableSchemaJson;
    this.warehouseSource = warehouseSource;
    this.fileIOImpl = fileIOImpl;
    this.fileIOProperties = fileIOProperties == null ? Collections.emptyMap() : fileIOProperties;
    this.fileFormat = fileFormat != null ? fileFormat : "PARQUET";
  }

  @JsonProperty
  public String getDataFilePath()
  {
    return dataFilePath;
  }

  @JsonProperty
  public List<DeleteFileInfo> getDeleteFiles()
  {
    return deleteFiles;
  }

  @JsonProperty
  public String getTableSchemaJson()
  {
    return tableSchemaJson;
  }

  @JsonProperty
  public InputSourceFactory getWarehouseSource()
  {
    return warehouseSource;
  }

  @JsonProperty
  @Nullable
  public String getFileIOImpl()
  {
    return fileIOImpl;
  }

  @JsonProperty
  public Map<String, String> getFileIOProperties()
  {
    return fileIOProperties;
  }

  @JsonProperty
  public String getFileFormat()
  {
    return fileFormat;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public Stream<InputSplit<List<String>>> createSplits(
      final InputFormat inputFormat,
      @Nullable final SplitHintSpec splitHintSpec
  )
  {
    return Stream.of(new InputSplit<>(Collections.singletonList(dataFilePath)));
  }

  @Override
  public int estimateNumSplits(final InputFormat inputFormat, @Nullable final SplitHintSpec splitHintSpec)
  {
    return 1;
  }

  @Override
  public InputSource withSplit(final InputSplit<List<String>> split)
  {
    return this;
  }

  @Override
  public boolean needsFormat()
  {
    // Native Iceberg reader handles format internally
    return false;
  }

  @Override
  public InputSourceReader reader(
      final InputRowSchema inputRowSchema,
      @Nullable final InputFormat inputFormat,
      final File temporaryDirectory
  )
  {
    return new IcebergNativeRecordReader(
        dataFilePath,
        deleteFiles,
        tableSchemaJson,
        warehouseSource,
        inputRowSchema,
        fileIOImpl,
        fileIOProperties,
        fileFormat
    );
  }
}
