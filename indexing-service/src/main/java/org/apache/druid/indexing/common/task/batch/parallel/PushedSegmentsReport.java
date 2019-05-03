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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;

import java.util.Set;

/**
 * This class is used in native parallel batch indexing, currently only in {@link SinglePhaseParallelIndexTaskRunner}.
 * In native parallel batch indexing, each subTask generates and pushes segments and sends a report to the
 * supervisorTask. Once the supervisorTask collects all reports, it publishes all the pushed segments at once.
 */
public class PushedSegmentsReport
{
  private final String taskId;
  private final Set<DataSegment> oldSegments;
  private final Set<DataSegment> newSegments;

  @JsonCreator
  public PushedSegmentsReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty("oldSegments") Set<DataSegment> oldSegments,
      @JsonProperty("segments") Set<DataSegment> newSegments
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.oldSegments = Preconditions.checkNotNull(oldSegments, "oldSegments");
    this.newSegments = Preconditions.checkNotNull(newSegments, "newSegments");
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public Set<DataSegment> getOldSegments()
  {
    return oldSegments;
  }

  @JsonProperty("segments")
  public Set<DataSegment> getNewSegments()
  {
    return newSegments;
  }
}
