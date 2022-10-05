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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.querykit.BaseLeafFrameProcessorFactory;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@JsonTypeName("scan")
public class ScanQueryFrameProcessorFactory extends BaseLeafFrameProcessorFactory
{
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
  public ScanQueryFrameProcessorFactory(
      @JsonProperty("query") ScanQuery query
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.runningCountForLimit =
        query.isLimited() && query.getOrderBys().isEmpty() ? new AtomicLong() : null;
  }

  @JsonProperty
  public ScanQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Long> makeProcessor(
      ReadableInput baseInput,
      Int2ObjectMap<ReadableInput> sideChannels,
      ResourceHolder<WritableFrameChannel> outputChannelHolder,
      ResourceHolder<MemoryAllocator> allocatorHolder,
      RowSignature signature,
      ClusterBy clusterBy,
      FrameContext frameContext
  )
  {
    return new ScanQueryFrameProcessor(
        query,
        baseInput,
        sideChannels,
        new JoinableFactoryWrapper(frameContext.joinableFactory()),
        outputChannelHolder,
        new LazyResourceHolder<>(() -> Pair.of(
            FrameWriters.makeFrameWriterFactory(
                FrameType.ROW_BASED,
                allocatorHolder.get(),
                signature,
                clusterBy.getColumns()
            ),
            allocatorHolder
        )),
        runningCountForLimit,
        frameContext.memoryParameters().getBroadcastJoinMemory()
    );
  }
}
