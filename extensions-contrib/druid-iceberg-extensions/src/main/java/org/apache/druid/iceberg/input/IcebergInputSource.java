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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class IcebergInputSource extends AbstractIcebergInputSource
    implements SplittableInputSource<List<String>>
{
  public static final String TYPE_KEY = "iceberg";
  private static final Logger log = new Logger(IcebergInputSource.class);

  @JsonProperty
  private final InputSourceFactory warehouseSource;

  private boolean isLoaded = false;
  private SplittableInputSource delegateInputSource;

  @JsonCreator
  public IcebergInputSource(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("icebergFilter") @Nullable IcebergFilter icebergFilter,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("warehouseSource") InputSourceFactory warehouseSource,
      @JsonProperty("snapshotTime") @Nullable DateTime snapshotTime,
      @JsonProperty("residualFilterMode") @Nullable ResidualFilterMode residualFilterMode,
      // deprecated: use type "iceberg_arrow" instead. retained for spec back-compat.
      @JsonProperty("useArrowReader") @Nullable Boolean useArrowReader,
      @JsonProperty("arrowBatchSize") @Nullable Integer arrowBatchSize
  )
  {
    super(tableName, namespace, icebergFilter, icebergCatalog, snapshotTime, residualFilterMode);
    this.warehouseSource = Preconditions.checkNotNull(warehouseSource, "warehouseSource cannot be null");
    if (useArrowReader != null && useArrowReader) {
      log.warn(
          "useArrowReader on type[iceberg] is deprecated and ignored; "
          + "switch to type[%s] for Arrow vectorized reads",
          IcebergArrowInputSource.TYPE_KEY
      );
    }
  }

  @JsonProperty("warehouseSource")
  public InputSourceFactory getWarehouseSource()
  {
    return warehouseSource;
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

  public SplittableInputSource getDelegateInputSource()
  {
    return delegateInputSource;
  }

  protected void retrieveIcebergDatafiles()
  {
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
