/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.appenderator;

import com.google.common.base.Preconditions;
import io.druid.data.input.InputRow;
import io.druid.indexing.common.actions.CountingSegmentAllocateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

/**
 * Segment allocator based on {@link CountingSegmentAllocateAction}.
 */
public class CountingActionBasedSegmentAllocator implements SegmentAllocator
{
  private final TaskActionClient taskActionClient;
  private final String dataSource;
  private final GranularitySpec granularitySpec;
  private final Map<Interval, String> versions;

  public CountingActionBasedSegmentAllocator(
      TaskActionClient taskActionClient,
      String dataSource,
      GranularitySpec granularitySpec,
      Map<Interval, String> versions
  )
  {
    this.taskActionClient = Preconditions.checkNotNull(taskActionClient, "taskActionClient");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    this.versions = Preconditions.checkNotNull(versions, "versions");
  }

  @Override
  public SegmentIdentifier allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return taskActionClient.submit(
        new CountingSegmentAllocateAction(
            dataSource,
            row.getTimestamp(),
            granularitySpec,
            versions
        )
    );
  }
}
