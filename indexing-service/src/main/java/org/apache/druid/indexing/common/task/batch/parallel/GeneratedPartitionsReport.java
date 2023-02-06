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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.TaskReport;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Report containing the {@link PartitionStat}s created by a {@link PartialSegmentGenerateTask}.
 * This report is collected by {@link ParallelIndexSupervisorTask} and
 * used to generate {@link PartialSegmentMergeIOConfig}.
 */
public class GeneratedPartitionsReport implements SubTaskReport
{
  private final String taskId;
  private final List<PartitionStat> partitionStats;
  private final Map<String, TaskReport> taskReport;

  GeneratedPartitionsReport(String taskId, List<PartitionStat> partitionStats, Map<String, TaskReport> taskReport)
  {
    this.taskId = taskId;
    this.partitionStats = partitionStats;
    this.taskReport = taskReport;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public Map<String, TaskReport> getTaskReport()
  {
    return taskReport;
  }

  @JsonProperty
  public List<PartitionStat> getPartitionStats()
  {
    return partitionStats;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeneratedPartitionsReport that = (GeneratedPartitionsReport) o;
    return Objects.equals(taskId, that.taskId) &&
           Objects.equals(partitionStats, that.partitionStats) &&
           Objects.equals(taskReport, that.taskReport);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, partitionStats, taskReport);
  }

  @Override
  public String toString()
  {
    return "GeneratedPartitionsReport{" +
        "taskId='" + taskId + '\'' +
        ", partitionStats=" + partitionStats +
        ", taskReport=" + taskReport +
        '}';
  }
}
