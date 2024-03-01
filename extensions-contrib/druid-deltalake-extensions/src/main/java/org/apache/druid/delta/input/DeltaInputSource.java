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

package org.apache.druid.delta.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.utils.Streams;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Input source to ingest data from a Delta Lake.
 * This input source reads the latest snapshot from a Delta table specified by {@code tablePath} parameter.
 * We leverage the Delta Kernel APIs to interact with a Delta table. The Kernel API abstracts away the
 * complexities of the Delta protocol itself.
 * Note: currently, the Kernel table API only supports reading from the latest snapshot.
 */
public class DeltaInputSource implements SplittableInputSource<DeltaSplit>
{
  public static final String TYPE_KEY = "delta";

  @JsonProperty
  private final String tablePath;

  @JsonProperty
  @Nullable
  private final DeltaSplit deltaSplit;

  @JsonCreator
  public DeltaInputSource(
      @JsonProperty("tablePath") String tablePath,
      @JsonProperty("deltaSplit") @Nullable DeltaSplit deltaSplit
  )
  {
    if (tablePath == null) {
      throw InvalidInput.exception("tablePath cannot be null.");
    }
    this.tablePath = tablePath;
    this.deltaSplit = deltaSplit;
  }

  @Override
  public boolean needsFormat()
  {
    // Only support Parquet
    return false;
  }

  /**
   * Instantiates a {@link DeltaInputSourceReader} to read the Delta table rows. If a {@link DeltaSplit} is supplied,
   * the Delta files and schema are obtained from it to instantiate the reader. Otherwise, a Delta table client is
   * instantiated with the supplied configuration to read the table.
   *
   * @param inputRowSchema     schema for {@link org.apache.druid.data.input.InputRow}
   * @param inputFormat        unused parameter. The input format is always parquet
   * @param temporaryDirectory unused parameter
   */
  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    final TableClient tableClient = createTableClient();
    try {
      final List<CloseableIterator<FilteredColumnarBatch>> scanFileDataIters = new ArrayList<>();

      if (deltaSplit != null) {
        final Row scanState = deserialize(tableClient, deltaSplit.getStateRow());
        final StructType physicalReadSchema =
            ScanStateRow.getPhysicalDataReadSchema(tableClient, scanState);

        for (String file : deltaSplit.getFiles()) {
          final Row scanFile = deserialize(tableClient, file);
          scanFileDataIters.add(
              getTransformedDataIterator(tableClient, scanState, scanFile, physicalReadSchema)
          );
        }
      } else {
        final Table table = Table.forPath(tableClient, tablePath);
        final Snapshot latestSnapshot = table.getLatestSnapshot(tableClient);
        final StructType prunedSchema = pruneSchema(
            latestSnapshot.getSchema(tableClient),
            inputRowSchema.getColumnsFilter()
        );
        final Scan scan = latestSnapshot.getScanBuilder(tableClient).withReadSchema(tableClient, prunedSchema).build();
        final CloseableIterator<FilteredColumnarBatch> scanFilesIter = scan.getScanFiles(tableClient);
        final Row scanState = scan.getScanState(tableClient);

        final StructType physicalReadSchema =
            ScanStateRow.getPhysicalDataReadSchema(tableClient, scanState);

        while (scanFilesIter.hasNext()) {
          final FilteredColumnarBatch scanFileBatch = scanFilesIter.next();
          final CloseableIterator<Row> scanFileRows = scanFileBatch.getRows();

          while (scanFileRows.hasNext()) {
            final Row scanFile = scanFileRows.next();
            scanFileDataIters.add(
                getTransformedDataIterator(tableClient, scanState, scanFile, physicalReadSchema)
            );
          }
        }
      }

      return new DeltaInputSourceReader(
        scanFileDataIters.iterator(),
        inputRowSchema
      );
    }
    catch (TableNotFoundException e) {
      throw InvalidInput.exception(e, "tablePath[%s] not found.", tablePath);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<InputSplit<DeltaSplit>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (deltaSplit != null) {
      // can't split a split
      return Stream.of(new InputSplit<>(deltaSplit));
    }

    final TableClient tableClient = createTableClient();
    final Snapshot latestSnapshot;
    try {
      final Table table = Table.forPath(tableClient, tablePath);
      latestSnapshot = table.getLatestSnapshot(tableClient);
    }
    catch (TableNotFoundException e) {
      throw InvalidInput.exception(e, "tablePath[%s] not found.", tablePath);
    }
    final Scan scan = latestSnapshot.getScanBuilder(tableClient).build();
    // scan files iterator for the current snapshot
    final CloseableIterator<FilteredColumnarBatch> scanFilesIterator = scan.getScanFiles(tableClient);

    final Row scanState = scan.getScanState(tableClient);
    final String scanStateStr = RowSerde.serializeRowToJson(scanState);

    Iterator<DeltaSplit> deltaSplitIterator = Iterators.transform(
        scanFilesIterator,
        scanFile -> {
          final CloseableIterator<Row> rows = scanFile.getRows();
          final List<String> fileRows = new ArrayList<>();
          while (rows.hasNext()) {
            fileRows.add(RowSerde.serializeRowToJson(rows.next()));
          }
          return new DeltaSplit(scanStateStr, fileRows);
        }
    );

    return Streams.sequentialStreamFrom(deltaSplitIterator).map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return Ints.checkedCast(createSplits(inputFormat, splitHintSpec).count());
  }

  @Override
  public InputSource withSplit(InputSplit<DeltaSplit> split)
  {
    return new DeltaInputSource(
        tablePath,
        split.get()
    );
  }

  private Row deserialize(TableClient tableClient, String row)
  {
    return RowSerde.deserializeRowFromJson(tableClient, row);
  }

  /**
   * Utility method to return a pruned schema that contains the given {@code columns} from
   * {@code baseSchema} applied by {@code columnsFilter}. This will serve as an optimization
   * for table scans if we're interested in reading only a subset of columns from the Delta Lake table.
   */
  private StructType pruneSchema(final StructType baseSchema, final ColumnsFilter columnsFilter)
  {
    final List<String> columnNames = baseSchema.fieldNames();
    final List<String> fiteredColumnNames = columnNames
        .stream()
        .filter(columnsFilter::apply)
        .collect(Collectors.toList());

    if (fiteredColumnNames.equals(columnNames)) {
      return baseSchema;
    }
    final List<StructField> selectedFields = fiteredColumnNames
        .stream()
        .map(baseSchema::get)
        .collect(Collectors.toList());
    return new StructType(selectedFields);
  }

  /**
   * @return a table client where the client is initialized with {@link Configuration} class that uses the class's
   * class loader instead of the context classloader. The latter by default doesn't know about the extension classes,
   * so the table client cannot load runtime classes resulting in {@link ClassNotFoundException}.
   */
  private TableClient createTableClient()
  {
    final ClassLoader currCtxClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      final Configuration conf = new Configuration();
      return DefaultTableClient.create(conf);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxClassloader);
    }
  }

  /**
   * Utility to get the transformed data iterator from the scan file. Code borrowed and modified from
   * <a href="https://github.com/delta-io/delta/blob/master/kernel/examples/table-reader/src/main/java/io/delta/kernel/examples/SingleThreadedTableReader.java">
   * SingleThreadedTableReader.java</a>.
   */
  private CloseableIterator<FilteredColumnarBatch> getTransformedDataIterator(
      final TableClient tableClient,
      final Row scanState,
      final Row scanFile,
      final StructType physicalReadSchema
  ) throws IOException
  {
    final FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);

    final CloseableIterator<ColumnarBatch> physicalDataIter = tableClient.getParquetHandler().readParquetFiles(
        Utils.singletonCloseableIterator(fileStatus),
        physicalReadSchema,
        Optional.empty()
    );
    return Scan.transformPhysicalData(
        tableClient,
        scanState,
        scanFile,
        physicalDataIter
    );
  }
}
