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
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
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
 * Note: the kernel table API only supports reading from the latest snapshot.
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
      throw InvalidInput.exception("tablePath cannot be null");
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

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    Configuration hadoopConf = new Configuration();
    TableClient tableClient = DefaultTableClient.create(hadoopConf);
    try {
      final Row scanState;
      final List<Row> scanRowList;

      if (deltaSplit != null) {
        scanState = deserialize(tableClient, deltaSplit.getStateRow());
        scanRowList = deltaSplit.getFiles()
                                .stream()
                                .map(row -> deserialize(tableClient, row))
                                .collect(Collectors.toList());
      } else {
        Table table = Table.forPath(tableClient, tablePath);
        Snapshot latestSnapshot = table.getLatestSnapshot(tableClient);

        Scan scan = latestSnapshot.getScanBuilder(tableClient).build();
        scanState = scan.getScanState(tableClient);
        CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(tableClient);
        scanRowList = new ArrayList<>();

        while (scanFiles.hasNext()) {
          FilteredColumnarBatch scanFileBatch = scanFiles.next();
          CloseableIterator<Row> scanFileRows = scanFileBatch.getRows();
          scanFileRows.forEachRemaining(scanRowList::add);
        }
      }
      return new DeltaInputSourceReader(
          Scan.readData(
              tableClient,
              scanState,
              Utils.toCloseableIterator(scanRowList.iterator()),
              Optional.empty()
          ),
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
      throws IOException
  {
    if (null != deltaSplit) {
      // can't split a split
      return Stream.of(new InputSplit<>(deltaSplit));
    }

    TableClient tableClient = DefaultTableClient.create(new Configuration());
    final Snapshot latestSnapshot;
    final Table table;
    try {
      table = Table.forPath(tableClient, tablePath);
      latestSnapshot = table.getLatestSnapshot(tableClient);
    }
    catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    Scan scan = latestSnapshot.getScanBuilder(tableClient).build();
    // scan files iterator for the current snapshot
    CloseableIterator<FilteredColumnarBatch> scanFilesIterator = scan.getScanFiles(tableClient);

    Row scanState = scan.getScanState(tableClient);
    String scanStateStr = RowSerde.serializeRowToJson(scanState);

    Iterator<DeltaSplit> deltaSplitIterator = Iterators.transform(
        scanFilesIterator,
        scanFile -> {
          CloseableIterator<Row> rows = scanFile.getRows();
          List<String> fileRows = new ArrayList<>();
          while (rows.hasNext()) {
            fileRows.add(RowSerde.serializeRowToJson(rows.next()));
          }
          return new DeltaSplit(scanStateStr, fileRows);
        }
    );

    return Streams.sequentialStreamFrom(deltaSplitIterator).map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
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
}
