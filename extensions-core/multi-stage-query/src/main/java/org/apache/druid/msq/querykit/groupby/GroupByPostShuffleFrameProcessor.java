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

package org.apache.druid.msq.querykit.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.groupby.having.AlwaysHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GroupByPostShuffleFrameProcessor implements FrameProcessor<Object>
{
  private final GroupByQuery query;
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameWriterFactory frameWriterFactory;
  private final FrameReader frameReader;
  private final ColumnSelectorFactory columnSelectorFactoryForFrameWriter;
  private final Comparator<ResultRow> compareFn;
  private final BinaryOperator<ResultRow> mergeFn;
  private final Consumer<ResultRow> finalizeFn;

  @Nullable
  private final HavingSpec havingSpec;

  private final SettableLongVirtualColumn partitionBoostVirtualColumn;

  private Cursor frameCursor = null;
  private Supplier<ResultRow> rowSupplierFromFrameCursor;
  private ResultRow outputRow = null;
  private FrameWriter frameWriter = null;

  public GroupByPostShuffleFrameProcessor(
      final GroupByQuery query,
      final GroupingEngine groupingEngine,
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameWriterFactory frameWriterFactory,
      final FrameReader frameReader,
      final ObjectMapper jsonMapper
  )
  {
    this.query = query;
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.frameWriterFactory = frameWriterFactory;
    this.compareFn = groupingEngine.createResultComparator(query);
    this.mergeFn = groupingEngine.createMergeFn(query);
    this.finalizeFn = makeFinalizeFn(query);
    this.havingSpec = cloneHavingSpec(query);
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);
    this.columnSelectorFactoryForFrameWriter =
        makeVirtualColumnsForFrameWriter(partitionBoostVirtualColumn, jsonMapper, query).wrap(
            RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
                query,
                () -> outputRow,
                RowSignature.Finalization.YES
            )
        );
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Object> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (frameCursor == null || frameCursor.isDone()) {
      // Keep reading through the input channel.
      if (readableInputs.isEmpty()) {
        return ReturnOrAwait.awaitAll(1);
      } else if (inputChannel.isFinished()) {
        if (outputRow != null && writeOutputRow()) {
          return ReturnOrAwait.runAgain();
        }

        writeCurrentFrameIfNeeded();
        return ReturnOrAwait.returnObject(Unit.instance());
      } else {
        final Frame frame = inputChannel.read();
        frameCursor = FrameProcessors.makeCursor(frame, frameReader);
        final ColumnSelectorFactory frameColumnSelectorFactory = frameCursor.getColumnSelectorFactory();

        //noinspection unchecked
        final Supplier<Object>[] fieldSuppliers = new Supplier[query.getResultRowSizeWithoutPostAggregators()];
        for (int i = 0; i < fieldSuppliers.length; i++) {
          final ColumnValueSelector<?> selector =
              frameColumnSelectorFactory.makeColumnValueSelector(frameReader.signature().getColumnName(i));
          fieldSuppliers[i] = selector::getObject;
        }

        final int fullRowSize = query.getResultRowSignature().size();
        rowSupplierFromFrameCursor = () -> {
          final ResultRow row = ResultRow.create(fullRowSize);
          for (int i = 0; i < fieldSuppliers.length; i++) {
            row.set(i, fieldSuppliers[i].get());
          }

          for (int i = fieldSuppliers.length; i < fullRowSize; i++) {
            // Post-aggregators.
            row.set(i, null);
          }

          return row;
        };
      }
    }

    setUpFrameWriterIfNeeded();

    while (!frameCursor.isDone()) {
      final ResultRow currentRow = rowSupplierFromFrameCursor.get();

      if (outputRow == null) {
        outputRow = currentRow.copy();
      } else if (compareFn.compare(outputRow, currentRow) == 0) {
        outputRow = mergeFn.apply(outputRow, currentRow);
      } else {
        if (writeOutputRow()) {
          return ReturnOrAwait.runAgain();
        }

        outputRow = currentRow.copy();
      }

      frameCursor.advance();
    }

    return ReturnOrAwait.runAgain();
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), frameWriter);
  }

  /**
   * Writes the current {@link #outputRow} to a pending frame, if it matches the query's havingSpec. Either way,
   * the {@link #outputRow} is cleared.
   *
   * If needed, writes the previously pending frame to the output channel.
   *
   * @return whether the previously pending frame was flushed
   */
  private boolean writeOutputRow() throws IOException
  {
    final int resultRowSize = query.getResultRowSignature().size();

    if (outputRow.length() < resultRowSize) {
      final Object[] newArray = new Object[resultRowSize];
      System.arraycopy(outputRow.getArray(), 0, newArray, 0, outputRow.length());
      outputRow = ResultRow.of(newArray);
    }

    // Apply post-aggregators.
    final Map<String, Object> outputRowAsMap = outputRow.toMap(query);

    for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
      final PostAggregator postAggregator = query.getPostAggregatorSpecs().get(i);
      final Object value = postAggregator.compute(outputRowAsMap);
      outputRow.set(query.getResultRowPostAggregatorStart() + i, value);
      outputRowAsMap.put(postAggregator.getName(), value);
    }

    if (havingSpec != null && !havingSpec.eval(outputRow)) {
      // Didn't match HAVING.
      outputRow = null;
      return false;
    }

    // Finalize aggregators after checking if they are passing the havingSpec, because havingSpec expects the
    // unfinalized row (and finalizes it internally after making a copy of it)
    finalizeFn.accept(outputRow);

    if (frameWriter.addSelection()) {
      incrementBoostColumn();
      outputRow = null;
      return false;
    } else if (frameWriter.getNumRows() > 0) {
      writeCurrentFrameIfNeeded();
      setUpFrameWriterIfNeeded();

      if (frameWriter.addSelection()) {
        incrementBoostColumn();
        outputRow = null;
        return true;
      } else {
        throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
      }
    } else {
      throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
    }
  }

  private void writeCurrentFrameIfNeeded() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
    }
  }

  private void setUpFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactoryForFrameWriter);
    }
  }

  private static Consumer<ResultRow> makeFinalizeFn(final GroupByQuery query)
  {
    if (GroupByQueryKit.isFinalize(query)) {
      final int startIndex = query.getResultRowAggregatorStart();
      final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();

      return row -> {
        for (int i = 0; i < aggregators.size(); i++) {
          row.set(startIndex + i, aggregators.get(i).finalizeComputation(row.get(startIndex + i)));
        }
      };
    } else {
      return row -> {};
    }
  }

  @Nullable
  private static HavingSpec cloneHavingSpec(final GroupByQuery query)
  {
    if (query.getHavingSpec() == null || query.getHavingSpec() instanceof AlwaysHavingSpec) {
      return null;
    } else if (query.getHavingSpec() instanceof DimFilterHavingSpec) {
      final DimFilterHavingSpec dimFilterHavingSpec = (DimFilterHavingSpec) query.getHavingSpec();
      final DimFilterHavingSpec clonedHavingSpec = new DimFilterHavingSpec(
          dimFilterHavingSpec.getDimFilter(),
          dimFilterHavingSpec.isFinalize()
      );
      clonedHavingSpec.setQuery(query);
      return clonedHavingSpec;
    } else {
      throw new UnsupportedOperationException("Must use 'filter' or 'always' havingSpec");
    }
  }

  /**
   * Create virtual columns containing "bonus" fields that should be attached to the {@link FrameWriter} for
   * this processor. Kept in sync with the signature generated by {@link GroupByQueryKit}.
   */
  private static VirtualColumns makeVirtualColumnsForFrameWriter(
      @Nullable final VirtualColumn partitionBoostVirtualColumn,
      final ObjectMapper jsonMapper,
      final GroupByQuery query
  )
  {
    List<VirtualColumn> virtualColumns = new ArrayList<>();

    virtualColumns.add(partitionBoostVirtualColumn);
    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query);
    if (segmentGranularityVirtualColumn != null) {
      virtualColumns.add(segmentGranularityVirtualColumn);
    }

    return VirtualColumns.create(virtualColumns);
  }

  /**
   * Increments the value of the partition boosting column. It should be called once the row value has been written
   * to the frame
   */
  private void incrementBoostColumn()
  {
    partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
  }
}
