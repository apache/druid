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
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Reads an Iceberg table. Two reader modes sit behind this single type:
 * the default resolves the snapshot to data file paths and hands them to {@code warehouseSource},
 * while {@code useArrowReader} scans the table directly through Iceberg's vectorized Arrow reader.
 */
public class IcebergInputSource implements SplittableInputSource<List<String>>
{
  public static final String TYPE_KEY = "iceberg";

  private final String tableName;
  private final String namespace;
  private final IcebergCatalog icebergCatalog;
  private final IcebergFilter icebergFilter;
  private final DateTime snapshotTime;
  private final ResidualFilterMode residualFilterMode;
  private final boolean useArrowReader;
  private final int arrowBatchSize;

  @Nullable
  private final InputSourceFactory warehouseSource;

  private final InputSourceDelegate delegate;

  @JsonCreator
  public IcebergInputSource(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("icebergFilter") @Nullable IcebergFilter icebergFilter,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("warehouseSource") @Nullable InputSourceFactory warehouseSource,
      @JsonProperty("snapshotTime") @Nullable DateTime snapshotTime,
      @JsonProperty("residualFilterMode") @Nullable ResidualFilterMode residualFilterMode,
      @JsonProperty("useArrowReader") @Nullable Boolean useArrowReader,
      @JsonProperty("arrowBatchSize") @Nullable Integer arrowBatchSize
  )
  {
    this.tableName = Preconditions.checkNotNull(tableName, "tableName cannot be null");
    this.namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
    this.icebergCatalog = Preconditions.checkNotNull(icebergCatalog, "icebergCatalog cannot be null");
    this.icebergFilter = icebergFilter;
    this.snapshotTime = snapshotTime;
    this.residualFilterMode = Configs.valueOrDefault(residualFilterMode, ResidualFilterMode.IGNORE);
    this.useArrowReader = Boolean.TRUE.equals(useArrowReader);
    this.arrowBatchSize = arrowBatchSize != null && arrowBatchSize > 0
                          ? arrowBatchSize
                          : IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE;
    this.warehouseSource = warehouseSource;

    this.delegate = this.useArrowReader
                    ? new ArrowDelegate()
                    : new StandardDelegate(
                        Preconditions.checkNotNull(
                            warehouseSource,
                            "warehouseSource cannot be null unless useArrowReader is true"
                        )
                    );
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

  @Nullable
  @JsonProperty("warehouseSource")
  public InputSourceFactory getWarehouseSource()
  {
    return warehouseSource;
  }

  @JsonProperty
  public boolean isUseArrowReader()
  {
    return useArrowReader;
  }

  @JsonProperty
  public int getArrowBatchSize()
  {
    return arrowBatchSize;
  }

  @Override
  public boolean needsFormat()
  {
    return delegate.needsFormat();
  }

  @Override
  public boolean isSplittable()
  {
    return delegate.isSplittable();
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    return delegate.reader(inputRowSchema, inputFormat, temporaryDirectory);
  }

  @Override
  public Stream<InputSplit<List<String>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  ) throws IOException
  {
    return delegate.createSplits(inputFormat, splitHintSpec);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    return delegate.estimateNumSplits(inputFormat, splitHintSpec);
  }

  @Override
  public InputSource withSplit(InputSplit<List<String>> inputSplit)
  {
    return delegate.withSplit(inputSplit);
  }

  @Override
  public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
  {
    return delegate.getSplitHintSpecOrDefault(splitHintSpec);
  }

  private Table retrieveTable()
  {
    return icebergCatalog.retrieveTable(namespace, tableName);
  }

  /**
   * Mode-specific behavior. The two modes differ on more than how rows are read: they disagree on whether
   * an {@link InputFormat} is needed and whether the source can be split across tasks.
   */
  private interface InputSourceDelegate
  {
    boolean needsFormat();

    boolean isSplittable();

    InputSourceReader reader(
        InputRowSchema inputRowSchema,
        @Nullable InputFormat inputFormat,
        File temporaryDirectory
    );

    Stream<InputSplit<List<String>>> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    ) throws IOException;

    int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException;

    InputSource withSplit(InputSplit<List<String>> inputSplit);

    SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec);
  }

  /**
   * Resolves the snapshot to a list of data file paths and defers reading to the warehouse input source.
   */
  private class StandardDelegate implements InputSourceDelegate
  {
    private final InputSourceFactory warehouseSource;

    private boolean isLoaded = false;
    private SplittableInputSource delegateInputSource;

    StandardDelegate(final InputSourceFactory warehouseSource)
    {
      this.warehouseSource = warehouseSource;
    }

    @Override
    public boolean needsFormat()
    {
      return true;
    }

    @Override
    public boolean isSplittable()
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
      return warehouseInputSource().reader(inputRowSchema, inputFormat, temporaryDirectory);
    }

    @Override
    public Stream<InputSplit<List<String>>> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    ) throws IOException
    {
      return warehouseInputSource().createSplits(inputFormat, splitHintSpec);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
    {
      return warehouseInputSource().estimateNumSplits(inputFormat, splitHintSpec);
    }

    @Override
    public InputSource withSplit(InputSplit<List<String>> inputSplit)
    {
      return warehouseInputSource().withSplit(inputSplit);
    }

    @Override
    public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
    {
      return warehouseInputSource().getSplitHintSpecOrDefault(splitHintSpec);
    }

    private SplittableInputSource warehouseInputSource()
    {
      if (!isLoaded) {
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
        isLoaded = true;
      }
      return delegateInputSource;
    }
  }

  /**
   * Scans the table through Iceberg's vectorized Arrow reader. Parquet only, and not splittable:
   * there is no data file list to hand out, so all rows are read by a single task.
   */
  private class ArrowDelegate implements InputSourceDelegate
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
      final Table table = retrieveTable();
      TableScan scan = table.newScan().caseSensitive(icebergCatalog.isCaseSensitive());
      if (icebergFilter != null) {
        scan = icebergFilter.filter(scan);
      }
      if (snapshotTime != null) {
        scan = scan.asOfTime(snapshotTime.getMillis());
      }
      if (icebergFilter != null) {
        icebergCatalog.enforceResidualMode(scan, residualFilterMode);
      }
      validateParquetOnly(scan);

      return new IcebergArrowInputSourceReader(
          table,
          icebergFilter,
          snapshotTime,
          icebergCatalog.isCaseSensitive(),
          inputRowSchema,
          arrowBatchSize
      );
    }

    @Override
    public Stream<InputSplit<List<String>>> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    )
    {
      return Stream.of(new InputSplit<>(Collections.emptyList()));
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return 1;
    }

    @Override
    public InputSource withSplit(InputSplit<List<String>> inputSplit)
    {
      return IcebergInputSource.this;
    }

    @Override
    public SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
    {
      return splitHintSpec == null ? SplittableInputSource.DEFAULT_SPLIT_HINT_SPEC : splitHintSpec;
    }

    /**
     * Iceberg's Arrow reader only supports Parquet. Checked against table metadata so a mixed-format or
     * ORC/Avro table fails with a clear message instead of an obscure error inside Arrow.
     */
    private void validateParquetOnly(final TableScan scan)
    {
      try (CloseableIterable<FileScanTask> fileTasks = scan.planFiles()) {
        for (final FileScanTask fileTask : fileTasks) {
          final FileFormat format = fileTask.file().format();
          if (format != FileFormat.PARQUET) {
            throw DruidException.forPersona(DruidException.Persona.USER)
                                .ofCategory(DruidException.Category.UNSUPPORTED)
                                .build(
                                    "Arrow reader supports only Parquet data files, but table[%s.%s] has a"
                                    + " data file in format[%s]. Set useArrowReader to false for this table.",
                                    namespace,
                                    tableName,
                                    format
                                );
          }
        }
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

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
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample()
        {
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
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
      return this;
    }
  }
}
