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
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Inputsource to ingest data managed by the Iceberg table format.
 * This inputsource talks to the configured catalog, executes any configured filters and retrieves the data file paths upto the latest snapshot associated with the iceberg table.
 * The data file paths are then provided to a native {@link SplittableInputSource} implementation depending on the warehouse source defined.
 *
 * <p>When the table contains Iceberg V2 delete files (position deletes or equality deletes), this
 * source switches to a native reader path via {@link IcebergFileTaskInputSource} and
 * {@link IcebergNativeRecordReader} that applies the deletes before producing rows.
 */
public class IcebergInputSource implements SplittableInputSource<List<String>>
{
  public static final String TYPE_KEY = "iceberg";

  /** Marker placed at index 0 of a V2-encoded split list. */
  private static final String V2_MARKER = "v2";

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

  // ---- mutable state (populated on first access) ----

  private boolean isLoaded = false;
  private SplittableInputSource delegateInputSource;

  /** True when the planned tasks contain at least one delete file (Iceberg V2). */
  private boolean hasDeleteFiles = false;

  /** FileScanTask list cached during planning when delete files are present. */
  private List<FileScanTask> v2Tasks;

  /** JSON-serialized Iceberg Schema, captured at planning time, embedded in V2 splits. */
  private String tableSchemaJson;

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
    return true;
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
    if (hasDeleteFiles) {
      // V2 path: create a combined reader across all file-scan tasks.
      final List<IcebergNativeRecordReader> taskReaders = v2Tasks.stream()
                                                                 .map(task -> taskToNativeReader(task, inputRowSchema))
                                                                 .collect(Collectors.toList());
      return new InputSourceReader()
      {
        @Override
        public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
        {
          List<CloseableIterator<InputRow>> iters = new ArrayList<>();
          for (IcebergNativeRecordReader r : taskReaders) {
            iters.add(r.read(inputStats));
          }
          return CloseableIterators.concat(iters);
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
        {
          List<CloseableIterator<InputRowListPlusRawValues>> iters = new ArrayList<>();
          for (IcebergNativeRecordReader r : taskReaders) {
            iters.add(r.sample());
          }
          return CloseableIterators.concat(iters);
        }
      };
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
    if (hasDeleteFiles) {
      return v2Tasks.stream().map(this::taskToV2Split);
    }
    return getDelegateInputSource().createSplits(inputFormat, splitHintSpec);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
      throws IOException
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    if (hasDeleteFiles) {
      return v2Tasks.size();
    }
    return getDelegateInputSource().estimateNumSplits(inputFormat, splitHintSpec);
  }

  /**
   * Returns an {@link InputSource} for exactly the data described by {@code inputSplit}.
   *
   * <ul>
   *   <li>V2 split (first element is {@value #V2_MARKER}): decoded into an
   *       {@link IcebergFileTaskInputSource} that uses the native Iceberg reader.</li>
   *   <li>V1 split (no marker): delegated to the warehouse {@link SplittableInputSource} as
   *       before—fully backward-compatible.</li>
   * </ul>
   */
  @Override
  public InputSource withSplit(InputSplit<List<String>> inputSplit)
  {
    List<String> parts = inputSplit.get();
    if (!parts.isEmpty() && V2_MARKER.equals(parts.get(0))) {
      return decodeV2Split(parts);
    }
    return getDelegateInputSource().withSplit(inputSplit);
  }

  @Override
  public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
  {
    if (delegateInputSource != null) {
      return getDelegateInputSource().getSplitHintSpecOrDefault(splitHintSpec);
    }
    // V2 path or not yet loaded — use interface default
    return splitHintSpec == null ? SplittableInputSource.DEFAULT_SPLIT_HINT_SPEC : splitHintSpec;
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
  public IcebergCatalog getIcebergCatalog()
  {
    return icebergCatalog;
  }

  @JsonProperty
  public IcebergFilter getIcebergFilter()
  {
    return icebergFilter;
  }

  @Nullable
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

  public SplittableInputSource getDelegateInputSource()
  {
    return delegateInputSource;
  }

  protected void retrieveIcebergDatafiles()
  {
    IcebergCatalog.FileScanResult result = icebergCatalog.extractFileScanTasksWithSchema(
        getNamespace(),
        getTableName(),
        getIcebergFilter(),
        getSnapshotTime(),
        getResidualFilterMode()
    );

    List<FileScanTask> tasks = result.getTasks();
    boolean anyDeletes = tasks.stream().anyMatch(t -> !t.deletes().isEmpty());

    if (anyDeletes) {
      hasDeleteFiles = true;
      v2Tasks = tasks;
      tableSchemaJson = result.getTableSchemaJson();
      // delegateInputSource remains null; the V2 path handles reading directly.
    } else {
      // V1 path: extract file paths and delegate to the warehouse input source.
      List<String> paths = tasks.stream()
                                .map(t -> t.file().path().toString())
                                .collect(Collectors.toList());
      delegateInputSource = paths.isEmpty() ? new EmptyInputSource() : warehouseSource.create(paths);
    }
    isLoaded = true;
  }

  // ---- V2 split encoding / decoding ----

  /**
   * Encodes a {@link FileScanTask} as a V2 split.
   *
   * <pre>
   * parts[0] = "v2"
   * parts[1] = data file path
   * parts[2] = file format name ("PARQUET" | "ORC")
   * parts[3] = data file size in bytes
   * parts[4] = data file record count
   * parts[5] = table schema as JSON (from SchemaParser.toJson)
   * parts[6+] = delete file entries:
   *   position delete  → "POS:<size>:<count>:<path>"
   *   equality delete  → "EQ:<fieldIds>:<size>:<count>:<path>"
   * </pre>
   *
   * The path is always the last component so that paths containing colons (e.g. {@code s3://…})
   * are captured correctly by {@code split(":", N)} with a limit.
   */
  private InputSplit<List<String>> taskToV2Split(FileScanTask task)
  {
    List<String> parts = new ArrayList<>();
    parts.add(V2_MARKER);
    parts.add(task.file().path().toString());
    parts.add(task.file().format().name());
    parts.add(String.valueOf(task.file().fileSizeInBytes()));
    parts.add(String.valueOf(task.file().recordCount()));
    parts.add(tableSchemaJson);

    for (DeleteFile deleteFile : task.deletes()) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        parts.add("POS:" + deleteFile.fileSizeInBytes()
                  + ":" + deleteFile.recordCount()
                  + ":" + deleteFile.path());
      } else {
        // EQUALITY_DELETES
        String fieldIds = deleteFile.equalityFieldIds() == null
                          ? ""
                          : deleteFile.equalityFieldIds().stream()
                                      .map(String::valueOf)
                                      .collect(Collectors.joining(","));
        parts.add("EQ:" + fieldIds
                  + ":" + deleteFile.fileSizeInBytes()
                  + ":" + deleteFile.recordCount()
                  + ":" + deleteFile.path());
      }
    }
    return new InputSplit<>(parts);
  }

  /**
   * Parses a V2-encoded split back into an {@link IcebergFileTaskInputSource}.
   */
  private IcebergFileTaskInputSource decodeV2Split(List<String> parts)
  {
    // parts[0] = "v2", parts[1] = dataFilePath, parts[2] = format,
    // parts[3] = dataFileSize, parts[4] = dataFileRecordCount,
    // parts[5] = tableSchemaJson, parts[6+] = delete file tokens
    String dataFilePath = parts.get(1);
    String fileFormat = parts.get(2);
    long dataFileSize = Long.parseLong(parts.get(3));
    long dataFileCount = Long.parseLong(parts.get(4));
    String schemaJson = parts.get(5);

    List<IcebergFileTaskInputSource.DeleteFileInfo> deleteFiles = new ArrayList<>();
    for (int i = 6; i < parts.size(); i++) {
      String token = parts.get(i);
      if (token.startsWith("POS:")) {
        // "POS:<size>:<count>:<path>"  — split on at most 4 colons (path is last)
        String[] toks = token.split(":", 4);
        long size = Long.parseLong(toks[1]);
        long count = Long.parseLong(toks[2]);
        String path = toks[3];
        deleteFiles.add(
            new IcebergFileTaskInputSource.DeleteFileInfo(
                path,
                "POSITION_DELETES",
                null,
                size,
                count));
      } else if (token.startsWith("EQ:")) {
        // "EQ:<fieldIds>:<size>:<count>:<path>"
        String[] toks = token.split(":", 5);
        String fieldIdsStr = toks[1];
        long size = Long.parseLong(toks[2]);
        long count = Long.parseLong(toks[3]);
        String path = toks[4];
        List<Integer> fieldIds = fieldIdsStr.isEmpty()
                                 ? Collections.emptyList()
                                 : Arrays.stream(fieldIdsStr.split(","))
                                         .map(Integer::parseInt)
                                         .collect(Collectors.toList());
        deleteFiles.add(
            new IcebergFileTaskInputSource.DeleteFileInfo(
            path,
            "EQUALITY_DELETES",
            fieldIds,
            size,
            count));
      }
    }

    return new IcebergFileTaskInputSource(
        dataFilePath,
        fileFormat,
        dataFileSize,
        dataFileCount,
        deleteFiles,
        schemaJson,
        namespace,
        tableName,
        icebergCatalog,
        warehouseSource
    );
  }

  /** Build an {@link IcebergNativeRecordReader} from a live {@link FileScanTask}. */
  private IcebergNativeRecordReader taskToNativeReader(FileScanTask task, InputRowSchema inputRowSchema)
  {
    List<IcebergFileTaskInputSource.DeleteFileInfo> deleteInfos = task.deletes().stream()
                                                                      .map(IcebergFileTaskInputSource.DeleteFileInfo::fromDeleteFile)
                                                                      .collect(Collectors.toList());
    return new IcebergNativeRecordReader(
        task.file().path().toString(),
        task.file().format().name(),
        task.file().fileSizeInBytes(),
        task.file().recordCount(),
        deleteInfos,
        tableSchemaJson,
        namespace,
        tableName,
        icebergCatalog,
        warehouseSource,
        inputRowSchema
    );
  }

  // ---- inner empty source ----

  /**
   * This input source is used in place of a delegate input source if there are no input file paths.
   * Certain input sources cannot be instantiated with an empty input file list and so composing input sources such as IcebergInputSource
   * may use this input source as delegate in such cases.
   */
  private static class EmptyInputSource implements SplittableInputSource
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
      return null;
    }
  }
}
