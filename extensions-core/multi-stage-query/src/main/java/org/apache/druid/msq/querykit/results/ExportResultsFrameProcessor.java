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
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.run.SqlResults;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.timeline.SegmentId;

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
  private final String exportFilePath;
  private final Object2IntMap<String> outputColumnNameToFrameColumnNumberMap;
  private final RowSignature exportRowSignature;
  private final ResultsContext resultsContext;
  private final int partitionNum;

  private volatile ResultFormat.Writer exportWriter;

  public ExportResultsFrameProcessor(
      final ReadableFrameChannel inputChannel,
      final ResultFormat exportFormat,
      final FrameReader frameReader,
      final StorageConnector storageConnector,
      final ObjectMapper jsonMapper,
      final ChannelCounters channelCounter,
      final String exportFilePath,
      final ColumnMappings columnMappings,
      final ResultsContext resultsContext,
      final int partitionNum
  )
  {
    this.inputChannel = inputChannel;
    this.exportFormat = exportFormat;
    this.frameReader = frameReader;
    this.storageConnector = storageConnector;
    this.jsonMapper = jsonMapper;
    this.channelCounter = channelCounter;
    this.exportFilePath = exportFilePath;
    this.resultsContext = resultsContext;
    this.partitionNum = partitionNum;
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

    if (exportWriter == null) {
      createExportWriter();
    }
    if (inputChannel.isFinished()) {
      exportWriter.writeResponseEnd();
      return ReturnOrAwait.returnObject(exportFilePath);
    } else {
      exportFrame(inputChannel.read());
      return ReturnOrAwait.awaitAll(1);
    }
  }

  private void exportFrame(final Frame frame)
  {
    final Segment segment = new FrameSegment(frame, frameReader, SegmentId.dummy("test"));
    try (final CursorHolder cursorHolder = segment.asCursorFactory().makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      if (cursor == null) {
        exportWriter.writeRowEnd();
        return;
      }
      final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

      //noinspection rawtypes
      final List<BaseObjectColumnValueSelector> selectors =
          frameReader.signature()
                     .getColumnNames()
                     .stream()
                     .map(columnSelectorFactory::makeColumnValueSelector)
                     .collect(Collectors.toList());

      while (!cursor.isDone()) {
        exportWriter.writeRowStart();
        for (int j = 0; j < exportRowSignature.size(); j++) {
          String columnName = exportRowSignature.getColumnName(j);
          BaseObjectColumnValueSelector<?> selector = selectors.get(outputColumnNameToFrameColumnNumberMap.getInt(columnName));
          if (resultsContext == null) {
            throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build("Received null resultsContext from the controller. This is due to a version mismatch between the controller and the worker. Please ensure that the worker and the controller are on the same version before retrying the query.");
          }
          exportWriter.writeRowField(
              columnName,
              SqlResults.coerce(
                  jsonMapper,
                  resultsContext.getSqlResultsContext(),
                  selector.getObject(),
                  resultsContext.getSqlTypeNames().get(j),
                  columnName
              )
          );
        }
        channelCounter.incrementRowCount(partitionNum);
        exportWriter.writeRowEnd();
        cursor.advance();
      }
    }
    catch (IOException e) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              e,
                              "Exception occurred while writing file to the export location [%s].",
                              exportFilePath
                          );
    }
  }

  private void createExportWriter() throws IOException
  {
    OutputStream stream = null;
    try {
      stream = storageConnector.write(exportFilePath);
      exportWriter = exportFormat.createFormatter(stream, jsonMapper);
      exportWriter.writeResponseStart();
      exportWriter.writeHeaderFromRowSignature(exportRowSignature, false);
    }
    catch (IOException e) {
      if (exportWriter != null) {
        exportWriter.close();
      }
      if (stream != null) {
        stream.close();
      }
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Exception occurred while opening a stream to the export location [%s].", exportFilePath);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());

    if (exportWriter != null) {
      exportWriter.close();
    }
  }
}
