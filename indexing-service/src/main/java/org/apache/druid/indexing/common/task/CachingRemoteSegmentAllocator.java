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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentBulkAllocateAction;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CachingRemoteSegmentAllocator extends CachingSegmentAllocator
{
  public CachingRemoteSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec
  ) throws IOException
  {
    super(toolbox, taskId, allocateSpec);
  }

  @Override
  Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds() throws IOException
  {
    return getToolbox().getTaskActionClient().submit(
        new SegmentBulkAllocateAction(
            getAllocateSpec(),
            getTaskId()
        )
    );
  }
}
