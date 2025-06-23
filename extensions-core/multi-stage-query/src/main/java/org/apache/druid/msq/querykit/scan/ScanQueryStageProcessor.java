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

package org.apache.druid.msq.querykit.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.querykit.BaseLeafStageProcessor;
import org.apache.druid.query.Druids;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@JsonTypeName("scan")
public class ScanQueryStageProcessor extends BaseLeafStageProcessor
{
  private static final String IRRELEVANT = "irrelevant";

  private final ScanQuery query;

  /**
   * Instantiated and passed to all the {@link ScanQueryFrameProcessor}s created from this factory to keep a track
   * of the number of rows processed so far in case the query is limited (without any order by) because one doesn't need
   * to scan through all the rows in that case.
   * This is not ideal because nothing really guarantees this factory is used for a single makeWorkers call
   */
  @Nullable
  private final AtomicLong runningCountForLimit;

  @JsonCreator
  public ScanQueryStageProcessor(@JsonProperty("query") ScanQuery query)
  {
    super(query);
    this.query = Preconditions.checkNotNull(query, "query");
    this.runningCountForLimit = query.isLimited() && query.getOrderBys().isEmpty() ? new AtomicLong() : null;
  }

  public static ScanQueryStageProcessor makeScanStageProcessor(
      VirtualColumns virtualColumns,
      RowSignature signature,
      DimFilter dimFilter)
  {
    ScanQuery scanQuery = Druids.newScanQueryBuilder()
        .dataSource(IRRELEVANT)
        .intervals(QuerySegmentSpec.ETERNITY)
        .filters(dimFilter)
        .virtualColumns(virtualColumns)
        .columns(signature.getColumnNames())
        .columnTypes(signature.getColumnTypes())
        .build();
    return new ScanQueryStageProcessor(scanQuery);
  }

  @JsonProperty
  public ScanQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Object> makeProcessor(
      ReadableInput baseInput,
      SegmentMapFunction segmentMapFn,
      ResourceHolder<WritableFrameChannel> outputChannelHolder,
      ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      FrameContext frameContext
  )
  {
    return new ScanQueryFrameProcessor(
        query,
        runningCountForLimit,
        frameContext.jsonMapper(),
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
  }

  @Override
  public boolean usesProcessingBuffers()
  {
    return false;
  }
}
