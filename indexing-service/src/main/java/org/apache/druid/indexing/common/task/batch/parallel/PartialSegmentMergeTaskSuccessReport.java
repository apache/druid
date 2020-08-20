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

package org.apache.druid.indexing.common.task.batch.parallel;

import org.apache.druid.timeline.DataSegment;

import java.util.Set;

public class PartialSegmentMergeTaskSuccessReport
{
  private final String taskId;
  private final Set<DataSegment> oldSegments;
  private final Set<DataSegment> newSegments;

  // TODO: Total input/output bytes/rows
  // TODO: Num of disk spills, spill time (for tertiary partitioning)

  // TODO: class for fetching. probably need more details than just fetchTime for fetches

  public PartialSegmentMergeTaskSuccessReport(
      String taskId,
      Set<DataSegment> oldSegments,
      Set<DataSegment> newSegments
  )
  {
    this.taskId = taskId;
    this.oldSegments = oldSegments;
    this.newSegments = newSegments;
  }
}
