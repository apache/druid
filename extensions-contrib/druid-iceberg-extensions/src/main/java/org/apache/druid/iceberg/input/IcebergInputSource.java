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
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.Table;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Inputsource to ingest data managed by the Iceberg table format.
 * This inputsource talks to the configured catalog, executes any configured filters and retrieves the data file paths upto the latest snapshot associated with the iceberg table.
 * The data file paths are then provided to a native {@link SplittableInputSource} implementation depending on the warehouse source defined.
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

  @JsonProperty
  private final V2DeleteHandling v2DeleteHandling;

  private boolean isLoaded = false;

  private SplittableInputSource delegateInputSource;

  /**
   * When v2DeleteHandling is APPLY and delete files are detected, this holds
   * the scan result needed to construct the native Iceberg reader.
   */
  @Nullable
  private IcebergCatalog.FileScanResult nativeReaderResult;

  @JsonCreator
  public IcebergInputSource(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("icebergFilter") @Nullable IcebergFilter icebergFilter,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("warehouseSource") InputSourceFactory warehouseSource,
      @JsonProperty("snapshotTime") @Nullable DateTime snapshotTime,
      @JsonProperty("residualFilterMode") @Nullable ResidualFilterMode residualFilterMode,
      @JsonProperty("v2DeleteHandling") @Nullable V2DeleteHandling v2DeleteHandling
  )
  {
    this.tableName = Preconditions.checkNotNull(tableName, "tableName cannot be null");
    this.namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
    this.icebergCatalog = Preconditions.checkNotNull(icebergCatalog, "icebergCatalog cannot be null");
    this.icebergFilter = icebergFilter;
    this.warehouseSource = Preconditions.checkNotNull(warehouseSource, "warehouseSource cannot be null");
    this.snapshotTime = snapshotTime;
    this.residualFilterMode = Configs.valueOrDefault(residualFilterMode, ResidualFilterMode.IGNORE);
    this.v2DeleteHandling = Configs.valueOrDefault(v2DeleteHandling, V2DeleteHandling.SKIP);
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

    // When native reader is required (v2 APPLY mode with delete files),
    // bypass warehouseSource and use Iceberg's own reader stack.
    if (nativeReaderResult != null) {
      final Table table = nativeReaderResult.getTable();
      return new IcebergNativeRecordReader(
          table.io(),
          table.schema(),
          table.schema(),
          nativeReaderResult.getFileScanTasks(),
          inputRowSchema
      );
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
    return getDelegateInputSource().createSplits(inputFormat, splitHintSpec);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    if (!isLoaded) {
      retrieveIcebergDatafiles();
    }
    return getDelegateInputSource().estimateNumSplits(inputFormat, splitHintSpec);
  }

  @Override
  public InputSource withSplit(InputSplit<List<String>> inputSplit)
  {
    return getDelegateInputSource().withSplit(inputSplit);
  }

  @Override
  public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
  {
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

  @JsonProperty
  public V2DeleteHandling getV2DeleteHandling()
  {
    return v2DeleteHandling;
  }

  public SplittableInputSource getDelegateInputSource()
  {
    return delegateInputSource;
  }

  protected void retrieveIcebergDatafiles()
  {
    if (v2DeleteHandling == V2DeleteHandling.APPLY || v2DeleteHandling == V2DeleteHandling.FAIL) {
      final IcebergCatalog.FileScanResult scanResult = icebergCatalog.extractFileScanTasks(
          getNamespace(),
          getTableName(),
          getIcebergFilter(),
          getSnapshotTime(),
          getResidualFilterMode()
      );

      if (scanResult.hasDeleteFiles()) {
        if (v2DeleteHandling == V2DeleteHandling.FAIL) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.INVALID_VALUE)
                              .build(
                                  "Iceberg table [%s.%s] contains v2 delete files. "
                                  + "Set v2DeleteHandling to 'apply' to correctly handle deletes, "
                                  + "or 'skip' to ignore them (deleted rows will be ingested).",
                                  getNamespace(),
                                  getTableName()
                              );
        }

        // APPLY mode: use native Iceberg reader that applies deletes
        if (scanResult.getFileScanTasks().isEmpty()) {
          delegateInputSource = new EmptyInputSource();
        } else {
          nativeReaderResult = scanResult;
          // Set a dummy delegate so createSplits/estimateNumSplits don't NPE.
          // The reader() method will bypass this and use nativeReaderResult instead.
          delegateInputSource = new EmptyInputSource();
        }
        log.info(
            "Iceberg v2 delete files detected for table [%s.%s]. Using native Iceberg reader with delete application.",
            getNamespace(),
            getTableName()
        );
      } else {
        // No delete files: fall through to the standard warehouseSource path
        final List<String> dataFilePaths = new java.util.ArrayList<>();
        for (final org.apache.iceberg.FileScanTask task : scanResult.getFileScanTasks()) {
          dataFilePaths.add(task.file().location());
        }
        if (dataFilePaths.isEmpty()) {
          delegateInputSource = new EmptyInputSource();
        } else {
          delegateInputSource = warehouseSource.create(dataFilePaths);
        }
      }
    } else {
      // SKIP mode: original v1-compatible behavior
      final List<String> snapshotDataFiles = icebergCatalog.extractSnapshotDataFiles(
          getNamespace(),
          getTableName(),
          getIcebergFilter(),
          getSnapshotTime(),
          getResidualFilterMode()
      );
      if (snapshotDataFiles.isEmpty()) {
        delegateInputSource = new EmptyInputSource();
      } else {
        delegateInputSource = warehouseSource.create(snapshotDataFiles);
      }
    }
    isLoaded = true;
  }

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
