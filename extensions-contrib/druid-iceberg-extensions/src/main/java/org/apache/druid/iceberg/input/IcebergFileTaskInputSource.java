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
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.iceberg.DeleteFile;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * A single-split {@link InputSource} representing one Iceberg V2 file scan task (one data file
 * plus its associated delete files).  Instances of this class are created by
 * {@link IcebergInputSource#withSplit(InputSplit)} when the split carries a {@code "v2"} marker,
 * and they are embedded in per-task specs that are distributed to Druid workers.
 *
 * <p>The table schema is carried as a JSON string (serialized on the coordinator at planning
 * time via {@code SchemaParser.toJson}). Workers deserialize it without contacting the catalog.
 *
 * <p>The {@code icebergCatalog} field is retained as a fallback source of {@link org.apache.iceberg.io.FileIO}
 * for non-local storage paths (S3, HDFS) in Phase 1. For local filesystem paths
 * {@link WarehouseFileIO} is used instead.
 *
 */
@JsonTypeName(IcebergFileTaskInputSource.TYPE_KEY)
public class IcebergFileTaskInputSource implements InputSource
{
  public static final String TYPE_KEY = "icebergFileTask";

  @JsonProperty
  private final String dataFilePath;

  @JsonProperty
  private final String fileFormat;

  @JsonProperty
  private final long dataFileSizeInBytes;

  @JsonProperty
  private final long dataFileRecordCount;

  @JsonProperty
  private final List<DeleteFileInfo> deleteFiles;

  /** JSON-serialized Iceberg Schema captured at coordinator planning time. */
  @JsonProperty
  private final String tableSchemaJson;

  @JsonProperty
  private final String tableNamespace;

  @JsonProperty
  private final String tableName;

  /**
   * Catalog retained as a fallback FileIO source for non-local storage paths (S3, HDFS).
   * For local filesystem paths {@link WarehouseFileIO} is used instead.
   */
  @JsonProperty
  private final IcebergCatalog icebergCatalog;

  @JsonProperty
  private final InputSourceFactory warehouseSource;

  @JsonCreator
  public IcebergFileTaskInputSource(
      @JsonProperty("dataFilePath") String dataFilePath,
      @JsonProperty("fileFormat") String fileFormat,
      @JsonProperty("dataFileSizeInBytes") long dataFileSizeInBytes,
      @JsonProperty("dataFileRecordCount") long dataFileRecordCount,
      @JsonProperty("deleteFiles") List<DeleteFileInfo> deleteFiles,
      @JsonProperty("tableSchemaJson") String tableSchemaJson,
      @JsonProperty("tableNamespace") String tableNamespace,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("warehouseSource") InputSourceFactory warehouseSource
  )
  {
    this.dataFilePath = dataFilePath;
    this.fileFormat = fileFormat;
    this.dataFileSizeInBytes = dataFileSizeInBytes;
    this.dataFileRecordCount = dataFileRecordCount;
    this.deleteFiles = deleteFiles != null ? deleteFiles : Collections.emptyList();
    this.tableSchemaJson = tableSchemaJson;
    this.tableNamespace = tableNamespace;
    this.tableName = tableName;
    this.icebergCatalog = icebergCatalog;
    this.warehouseSource = warehouseSource;
  }

  @Override
  public boolean needsFormat()
  {
    // Handles its own reading through Iceberg's native Parquet/ORC reader
    return false;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    return new IcebergNativeRecordReader(
        dataFilePath,
        fileFormat,
        dataFileSizeInBytes,
        dataFileRecordCount,
        deleteFiles,
        tableSchemaJson,
        tableNamespace,
        tableName,
        icebergCatalog,
        warehouseSource,
        inputRowSchema
    );
  }

  public String getDataFilePath()
  {
    return dataFilePath;
  }

  public String getFileFormat()
  {
    return fileFormat;
  }

  public long getDataFileSizeInBytes()
  {
    return dataFileSizeInBytes;
  }

  public long getDataFileRecordCount()
  {
    return dataFileRecordCount;
  }

  public List<DeleteFileInfo> getDeleteFiles()
  {
    return deleteFiles;
  }

  public String getTableSchemaJson()
  {
    return tableSchemaJson;
  }

  public String getTableNamespace()
  {
    return tableNamespace;
  }

  public String getTableName()
  {
    return tableName;
  }

  /**
   * Metadata describing one delete file associated with a data file.
   */
  public static class DeleteFileInfo
  {
    /** Fully-qualified path to the delete file (same format as stored in Iceberg metadata). */
    @JsonProperty
    private final String path;

    /**
     * Delete file content type as a string: {@code "POSITION_DELETES"} or
     * {@code "EQUALITY_DELETES"} (mirrors {@link org.apache.iceberg.FileContent}).
     */
    @JsonProperty
    private final String content;

    /** Field IDs used for equality matching (non-null only for EQUALITY_DELETES). */
    @JsonProperty
    private final List<Integer> equalityFieldIds;

    @JsonProperty
    private final long fileSizeInBytes;

    @JsonProperty
    private final long recordCount;

    @JsonCreator
    public DeleteFileInfo(
        @JsonProperty("path") String path,
        @JsonProperty("content") String content,
        @JsonProperty("equalityFieldIds") @Nullable List<Integer> equalityFieldIds,
        @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
        @JsonProperty("recordCount") long recordCount
    )
    {
      this.path = path;
      this.content = content;
      this.equalityFieldIds = equalityFieldIds != null ? equalityFieldIds : Collections.emptyList();
      this.fileSizeInBytes = fileSizeInBytes;
      this.recordCount = recordCount;
    }

    /** Create a {@link DeleteFileInfo} from an Iceberg {@link DeleteFile} object. */
    public static DeleteFileInfo fromDeleteFile(DeleteFile deleteFile)
    {
      List<Integer> fieldIds = Collections.emptyList();
      if (deleteFile.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
          && deleteFile.equalityFieldIds() != null) {
        fieldIds = deleteFile.equalityFieldIds();
      }
      return new DeleteFileInfo(
          deleteFile.path().toString(),
          deleteFile.content().name(),
          fieldIds,
          deleteFile.fileSizeInBytes(),
          deleteFile.recordCount()
      );
    }

    public String getPath()
    {
      return path;
    }

    public String getContent()
    {
      return content;
    }

    public List<Integer> getEqualityFieldIds()
    {
      return equalityFieldIds;
    }

    public long getFileSizeInBytes()
    {
      return fileSizeInBytes;
    }

    public long getRecordCount()
    {
      return recordCount;
    }

    public boolean isPositionDelete()
    {
      return "POSITION_DELETES".equals(content);
    }

    public boolean isEqualityDelete()
    {
      return "EQUALITY_DELETES".equals(content);
    }
  }
}
