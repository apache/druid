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
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.FilePerSplitHintSpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SchemaParser;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Input source for ingesting data from Iceberg tables.
 *
 * Connects to a configured Iceberg catalog, executes partition filters, and retrieves
 * data file paths. For Iceberg v1 tables (no delete files), file paths are delegated
 * to {@code warehouseSource}. For v2 tables with active delete files, an
 * {@link IcebergFileTaskInputSource} is created per task to apply deletes at read time.
 *
 * V2 detection is automatic -- no user configuration needed.
 */
public class IcebergInputSource implements SplittableInputSource<List<String>>
{
  public static final String TYPE_KEY = "iceberg";
  private static final Logger log = new Logger(IcebergInputSource.class);

  @JsonProperty
  private final String tableName;

  @JsonProperty
  private final String namespace;

  @JsonProperty
  private IcebergCatalog icebergCatalog;

  @JsonProperty
  private IcebergFilter icebergFilter;

  @JsonProperty
  private InputSourceFactory warehouseSource;

  @JsonProperty
  private final DateTime snapshotTime;

  @JsonProperty
  private final ResidualFilterMode residualFilterMode;

  private boolean isLoaded = false;

  private SplittableInputSource delegateInputSource;

  /**
   * When v2 delete files are detected, this holds the per-task input sources
   * for the native reader path.
   */
  @Nullable
  private List<IcebergFileTaskInputSource> v2TaskInputSources;

  @JsonCreator
  public IcebergInputSource(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("icebergFilter") @Nullable IcebergFilter icebergFilter,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("warehouseSource") InputSourceFactory warehouseSource,
      @JsonProperty("snapshotTime") @Nullable DateTime snapshotTime,
      @JsonProperty("residualFilterMode") @Nullable ResidualFilterMode residualFilterMode
  )
  {
    this.tableName = Preconditions.checkNotNull(tableName, "tableName cannot be null");
    this.namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
    this.icebergCatalog = Preconditions.checkNotNull(icebergCatalog, "icebergCatalog cannot be null");
    this.icebergFilter = icebergFilter;
    this.warehouseSource = Preconditions.checkNotNull(warehouseSource, "warehouseSource cannot be null");
    this.snapshotTime = snapshotTime;
    this.residualFilterMode = Configs.valueOrDefault(residualFilterMode, ResidualFilterMode.IGNORE);
  }

  @Override
  public boolean needsFormat()
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    // V2 path uses native Iceberg readers and does not need an InputFormat
    if (v2TaskInputSources != null) {
      return false;
    }
    return getDelegateInputSource().needsFormat();
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }

    // V2 path: use native Iceberg reader with delete application
    if (v2TaskInputSources != null && !v2TaskInputSources.isEmpty()) {
      return new CompositeInputSourceReader(v2TaskInputSources, inputRowSchema, inputFormat, temporaryDirectory);
    }

    return getDelegateInputSource().reader(inputRowSchema, inputFormat, temporaryDirectory);
  }

  @Override
  public Stream<InputSplit<List<String>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  ) throws IOException
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    if (v2TaskInputSources != null) {
      return v2TaskInputSources.stream()
                               .map(task -> new InputSplit<>(Collections.singletonList(task.getDataFilePath())));
    }
    return getDelegateInputSource().createSplits(inputFormat, splitHintSpec);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    if (v2TaskInputSources != null) {
      return v2TaskInputSources.size();
    }
    return getDelegateInputSource().estimateNumSplits(inputFormat, splitHintSpec);
  }

  @Override
  public InputSource withSplit(InputSplit<List<String>> inputSplit)
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    if (v2TaskInputSources != null) {
      final String dataFilePath = inputSplit.get().get(0);
      for (final IcebergFileTaskInputSource task : v2TaskInputSources) {
        if (dataFilePath.equals(task.getDataFilePath())) {
          return task;
        }
      }
      throw new ISE("No v2 IcebergFileTaskInputSource found for data file path [%s]", dataFilePath);
    }
    return getDelegateInputSource().withSplit(inputSplit);
  }

  @Override
  public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    if (v2TaskInputSources != null) {
      return splitHintSpec == null ? FilePerSplitHintSpec.INSTANCE : splitHintSpec;
    }
    return getDelegateInputSource().getSplitHintSpecOrDefault(splitHintSpec);
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getNamespace()
  {
    return namespace;
  }

  @JsonProperty
  public IcebergFilter getIcebergFilter()
  {
    return icebergFilter;
  }

  @JsonProperty
  public IcebergCatalog getIcebergCatalog()
  {
    return icebergCatalog;
  }

  @JsonProperty
  public InputSourceFactory getWarehouseSource()
  {
    return warehouseSource;
  }

  @JsonProperty
  public DateTime getSnapshotTime()
  {
    return snapshotTime;
  }

  @JsonProperty
  public ResidualFilterMode getResidualFilterMode()
  {
    return residualFilterMode;
  }

  protected SplittableInputSource getDelegateInputSource()
  {
    if (delegateInputSource == null) {
      delegateInputSource = new EmptyInputSource();
    }
    return delegateInputSource;
  }

  /**
   * Scans the Iceberg table and routes to V1 or V2 path based on delete file presence.
   *
   * V1 path (no deletes): extracts file paths, delegates to warehouseSource.
   * V2 path (deletes detected): creates IcebergFileTaskInputSource per FileScanTask
   * carrying data file path, delete file metadata, and serialized table schema.
   */
  protected void retrieveIcebergDatafiles()
  {
    final IcebergCatalog.FileScanResult scanResult = icebergCatalog.extractFileScanTasks(
        getNamespace(),
        getTableName(),
        getIcebergFilter(),
        getSnapshotTime(),
        getResidualFilterMode()
    );

    if (scanResult.getFileScanTasks().isEmpty()) {
      delegateInputSource = new EmptyInputSource();
      isLoaded = true;
      return;
    }

    if (scanResult.hasDeleteFiles()) {
      // V2 path: create per-task input sources with delete file metadata
      final String schemaJson = SchemaParser.toJson(scanResult.getTable().schema());
      v2TaskInputSources = new ArrayList<>();

      for (final FileScanTask task : scanResult.getFileScanTasks()) {
        final String dataFilePath = task.file().location();
        final List<DeleteFileInfo> deleteFileInfos = new ArrayList<>();

        for (final DeleteFile deleteFile : task.deletes()) {
          deleteFileInfos.add(toDeleteFileInfo(deleteFile));
        }

        v2TaskInputSources.add(new IcebergFileTaskInputSource(
            dataFilePath,
            deleteFileInfos,
            schemaJson,
            warehouseSource,
            scanResult.getFileIOImpl(),
            scanResult.getFileIOProperties(),
            scanResult.getHadoopConfigOverrides(),
            task.file().format().name()
        ));
      }

      // Set a delegate for createSplits/estimateNumSplits compatibility
      delegateInputSource = new EmptyInputSource();

      log.info(
          "Iceberg v2 delete files detected for table [%s.%s]. Using native reader for [%d] tasks.",
          getNamespace(),
          getTableName(),
          v2TaskInputSources.size()
      );
    } else {
      // V1 path: extract file paths, delegate to warehouseSource
      final List<String> dataFilePaths = new ArrayList<>();
      for (final FileScanTask task : scanResult.getFileScanTasks()) {
        dataFilePaths.add(task.file().location());
      }
      delegateInputSource = warehouseSource.create(dataFilePaths);
    }

    isLoaded = true;
  }

  /**
   * Composes readers from multiple {@link IcebergFileTaskInputSource} instances,
   * iterating through tasks sequentially. Each task's reader is opened lazily
   * and closed before opening the next.
   */
  static DeleteFileInfo toDeleteFileInfo(final DeleteFile deleteFile)
  {
    final FileContent content = deleteFile.content();
    switch (content) {
      case EQUALITY_DELETES:
        return new DeleteFileInfo(
            deleteFile.location(),
            DeleteFileInfo.ContentType.EQUALITY,
            deleteFile.equalityFieldIds(),
            deleteFile.format().name()
        );
      case POSITION_DELETES:
        return new DeleteFileInfo(
            deleteFile.location(),
            DeleteFileInfo.ContentType.POSITION,
            Collections.emptyList(),
            deleteFile.format().name()
        );
      default:
        throw new UOE(
            "Iceberg delete file content [%s] is not supported. Only EQUALITY_DELETES and POSITION_DELETES are supported. "
            + "Deletion vectors (Iceberg v3) are not yet implemented in druid-iceberg-extensions.",
            content
        );
    }
  }

  private static class CompositeInputSourceReader implements InputSourceReader
  {
    private final List<IcebergFileTaskInputSource> taskSources;
    private final InputRowSchema inputRowSchema;
    private final InputFormat inputFormat;
    private final File temporaryDirectory;

    CompositeInputSourceReader(
        final List<IcebergFileTaskInputSource> taskSources,
        final InputRowSchema inputRowSchema,
        final InputFormat inputFormat,
        final File temporaryDirectory
    )
    {
      this.taskSources = taskSources;
      this.inputRowSchema = inputRowSchema;
      this.inputFormat = inputFormat;
      this.temporaryDirectory = temporaryDirectory;
    }

    @Override
    public CloseableIterator<InputRow> read(final InputStats inputStats) throws IOException
    {
      final Iterator<IcebergFileTaskInputSource> taskIterator = taskSources.iterator();
      return new CloseableIterator<InputRow>()
      {
        private CloseableIterator<InputRow> current = null;

        @Override
        public boolean hasNext()
        {
          while ((current == null || !current.hasNext()) && taskIterator.hasNext()) {
            closeCurrent();
            try {
              current = taskIterator.next()
                                    .reader(inputRowSchema, inputFormat, temporaryDirectory)
                                    .read(inputStats);
            }
            catch (IOException e) {
              throw new RuntimeException("Failed to open Iceberg task reader", e);
            }
          }
          return current != null && current.hasNext();
        }

        @Override
        public InputRow next()
        {
          if (!hasNext()) {
            throw new java.util.NoSuchElementException();
          }
          return current.next();
        }

        @Override
        public void close()
        {
          closeCurrent();
        }

        private void closeCurrent()
        {
          if (current != null) {
            try {
              current.close();
            }
            catch (IOException e) {
              throw new RuntimeException("Failed to close Iceberg task reader", e);
            }
            current = null;
          }
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
  }

  /**
   * Placeholder input source used when there are no input file paths.
   */
  static class EmptyInputSource implements SplittableInputSource
  {
    @Override
    public boolean needsFormat()
    {
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
      return new InputSourceReader()
      {
        @Override
        public CloseableIterator<InputRow> read(InputStats inputStats)
        {
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {
          });
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample()
        {
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {
          });
        }
      };
    }

    @Override
    public Stream<InputSplit> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    )
    {
      return Stream.empty();
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return 0;
    }

    @Override
    public InputSource withSplit(InputSplit split)
    {
      throw new ISE("withSplit called on EmptyInputSource; createSplits returns no splits so this is unreachable in valid flow");
    }
  }
}
