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

package org.apache.druid.msq.querykit.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.util.SequenceUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExportResultsFrameProcessor implements FrameProcessor<Object>
{
  private final ReadableFrameChannel inputChannel;
  private final ResultFormat exportFormat;
  private final FrameReader frameReader;
  private final StorageConnector storageConnector;
  private final ObjectMapper jsonMapper;
  private final ChannelCounters channelCounter;
  final String exportFilePath;
  private final Object2IntMap<String> outputColumnNameToFrameColumnNumberMap;
  private final RowSignature exportRowSignature;

  public ExportResultsFrameProcessor(
      final ReadableFrameChannel inputChannel,
      final ResultFormat exportFormat,
      final FrameReader frameReader,
      final StorageConnector storageConnector,
      final ObjectMapper jsonMapper,
      final ChannelCounters channelCounter,
      final String exportFilePath,
      final ColumnMappings columnMappings
  )
  {
    this.inputChannel = inputChannel;
    this.exportFormat = exportFormat;
    this.frameReader = frameReader;
    this.storageConnector = storageConnector;
    this.jsonMapper = jsonMapper;
    this.channelCounter = channelCounter;
    this.exportFilePath = exportFilePath;
    this.outputColumnNameToFrameColumnNumberMap = new Object2IntOpenHashMap<>();
    final RowSignature inputRowSignature = frameReader.signature();

    if (columnMappings == null) {
      // If the column mappings wasn't sent, fail the query to avoid inconsistency in file format.
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Received null columnMappings from controller. This might be due to an upgrade.");
    }
    for (final ColumnMapping columnMapping : columnMappings.getMappings()) {
      this.outputColumnNameToFrameColumnNumberMap.put(
          columnMapping.getOutputColumn(),
          frameReader.signature().indexOf(columnMapping.getQueryColumn())
      );
    }
    final RowSignature.Builder exportRowSignatureBuilder = RowSignature.builder();

    for (String outputColumn : columnMappings.getOutputColumnNames()) {
      exportRowSignatureBuilder.add(
          outputColumn,
          inputRowSignature.getColumnType(outputColumnNameToFrameColumnNumberMap.getInt(outputColumn)).orElse(null)
      );
    }
    this.exportRowSignature = exportRowSignatureBuilder.build();
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      exportFrame(inputChannel.read());
      return ReturnOrAwait.awaitAll(1);
    }
  }

  private void exportFrame(final Frame frame) throws IOException
  {
    final Sequence<Cursor> cursorSequence =
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null);

    // Add headers if we are writing to a new file.
    final boolean writeHeader = !storageConnector.pathExists(exportFilePath);

    try (OutputStream stream = storageConnector.write(exportFilePath)) {
      ResultFormat.Writer formatter = exportFormat.createFormatter(stream, jsonMapper);
      formatter.writeResponseStart();

      if (writeHeader) {
        formatter.writeHeaderFromRowSignature(exportRowSignature, false);
      }

      SequenceUtils.forEach(
          cursorSequence,
          cursor -> {
            try {
              final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

              //noinspection rawtypes
              @SuppressWarnings("rawtypes")
              final List<BaseObjectColumnValueSelector> selectors =
                  frameReader.signature()
                             .getColumnNames()
                             .stream()
                             .map(columnSelectorFactory::makeColumnValueSelector)
                             .collect(Collectors.toList());

              while (!cursor.isDone()) {
                formatter.writeRowStart();
                for (int j = 0; j < exportRowSignature.size(); j++) {
                  String columnName = exportRowSignature.getColumnName(j);
                  BaseObjectColumnValueSelector<?> selector = selectors.get(outputColumnNameToFrameColumnNumberMap.getInt(columnName));
                  formatter.writeRowField(columnName, selector.getObject());
                }
                channelCounter.incrementRowCount();
                formatter.writeRowEnd();
                cursor.advance();
              }
            }
            catch (IOException e) {
              throw DruidException.forPersona(DruidException.Persona.USER)
                                  .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                  .build(e, "Exception occurred while writing file to the export location [%s].", exportFilePath);
            }
          }
      );
      formatter.writeResponseEnd();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
