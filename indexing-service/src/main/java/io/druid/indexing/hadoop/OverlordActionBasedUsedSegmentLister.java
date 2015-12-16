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

package io.druid.indexing.hadoop;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.indexer.path.UsedSegmentLister;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

/**
 */
public class OverlordActionBasedUsedSegmentLister implements UsedSegmentLister
{
  private final TaskToolbox toolbox;

  @Inject
  public OverlordActionBasedUsedSegmentLister(TaskToolbox toolbox)
  {
    this.toolbox = Preconditions.checkNotNull(toolbox, "null task toolbox");
  }

  @Override
  public List<DataSegment> getUsedSegmentsForIntervals(
      String dataSource, List<Interval> intervals
  ) throws IOException
  {
    return toolbox
        .getTaskActionClient()
        .submit(new SegmentListUsedAction(dataSource, null, intervals));
  }
}
