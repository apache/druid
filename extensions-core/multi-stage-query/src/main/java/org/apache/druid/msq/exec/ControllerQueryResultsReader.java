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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.utils.CloseableUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Used by {@link ControllerImpl} to read query results and hand them to a {@link QueryListener}.
 */
public class ControllerQueryResultsReader implements FrameProcessor<Void>
{
  private static final Logger log = new Logger(ControllerQueryResultsReader.class);

  private final ReadableFrameChannel in;
  private final FrameReader frameReader;
  private final ColumnMappings columnMappings;
  private final ResultsContext resultsContext;
  private final ObjectMapper jsonMapper;
  private final QueryListener queryListener;

  private boolean wroteResultsStart;

  ControllerQueryResultsReader(
      final ReadableFrameChannel in,
      final FrameReader frameReader,
      final ColumnMappings columnMappings,
      final ResultsContext resultsContext,
      final ObjectMapper jsonMapper,
      final QueryListener queryListener
  )
  {
    this.in = in;
    this.frameReader = frameReader;
    this.columnMappings = columnMappings;
    this.resultsContext = resultsContext;
    this.jsonMapper = jsonMapper;
    this.queryListener = queryListener;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(in);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<Void> runIncrementally(final IntSet readableInputs)
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }

    if (!wroteResultsStart) {
      final RowSignature querySignature = frameReader.signature();
      final ImmutableList.Builder<MSQResultsReport.ColumnAndType> mappedSignature = ImmutableList.builder();

      for (final ColumnMapping mapping : columnMappings.getMappings()) {
        mappedSignature.add(
            new MSQResultsReport.ColumnAndType(
                mapping.getOutputColumn(),
                querySignature.getColumnType(mapping.getQueryColumn()).orElse(null)
            )
        );
      }

      queryListener.onResultsStart(
          mappedSignature.build(),
          resultsContext.getSqlTypeNames()
      );

      wroteResultsStart = true;
    }

    // Read from query results channel, if it's open.
    if (in.isFinished()) {
      queryListener.onResultsComplete();
      return ReturnOrAwait.returnObject(null);
    } else {
      final Frame frame = in.read();
      Yielder<Object[]> rowYielder = Yielders.each(
          SqlStatementResourceHelper.getResultSequence(
              frame,
              frameReader,
              columnMappings,
              resultsContext,
              jsonMapper
          )
      );

      try {
        while (!rowYielder.isDone()) {
          if (queryListener.onResultRow(rowYielder.get())) {
            rowYielder = rowYielder.next(null);
          } else {
            // Caller wanted to stop reading.
            return ReturnOrAwait.returnObject(null);
          }
        }
      }
      finally {
        CloseableUtils.closeAndSuppressExceptions(rowYielder, e -> log.warn(e, "Failed to close frame yielder"));
      }

      return ReturnOrAwait.awaitAll(inputChannels().size());
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
