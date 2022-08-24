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

import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.msq.rpc.CoordinatorServiceClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Production implementation of {@link DataSegmentProvider} using Coordinator APIs.
 */
public class TaskDataSegmentProvider implements DataSegmentProvider
{
  private final CoordinatorServiceClient coordinatorClient;
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;

  public TaskDataSegmentProvider(
      CoordinatorServiceClient coordinatorClient,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    this.coordinatorClient = coordinatorClient;
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
  }

  @Override
  public LazyResourceHolder<Segment> fetchSegment(
      final SegmentId segmentId,
      final ChannelCounters channelCounters
  )
  {
    try {
      // Use LazyResourceHolder so Coordinator call and segment downloads happen in processing threads,
      // rather than the main thread.
      return new LazyResourceHolder<>(
          () -> {
            final DataSegment dataSegment;
            try {
              dataSegment = FutureUtils.get(
                  coordinatorClient.fetchUsedSegment(
                      segmentId.getDataSource(),
                      segmentId.toString()
                  ),
                  true
              );
            }
            catch (InterruptedException | ExecutionException e) {
              throw new RE(e, "Failed to fetch segment details from Coordinator for [%s]", segmentId);
            }

            final Closer closer = Closer.create();
            try {
              final File segmentDir = segmentCacheManager.getSegmentFiles(dataSegment);
              closer.register(() -> FileUtils.deleteDirectory(segmentDir));

              final QueryableIndex index = indexIO.loadIndex(segmentDir);
              final int numRows = index.getNumRows();
              final long size = dataSegment.getSize();
              closer.register(() -> channelCounters.addFile(numRows, size));
              closer.register(index);
              return Pair.of(new QueryableIndexSegment(index, dataSegment.getId()), closer);
            }
            catch (IOException | SegmentLoadingException e) {
              throw CloseableUtils.closeInCatch(
                  new RE(e, "Failed to download segment [%s]", segmentId),
                  closer
              );
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
