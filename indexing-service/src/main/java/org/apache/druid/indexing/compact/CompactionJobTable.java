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

package org.apache.druid.indexing.compact;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.Table;

import java.util.List;

public class CompactionJobTable extends Table
{
  public static CompactionJobTable create(List<CompactionJobStatus> jobs, int maxRows)
  {
    final CompactionJobTable table = new CompactionJobTable(
        List.of("Interval", "Task Id", "Compacted segments", "Compacted bytes", "Position in job queue")
    );

    for (int i = 0; i < maxRows && i < jobs.size(); ++i) {
      table.addJobRow(jobs.get(i));
    }
    return table;
  }

  private CompactionJobTable(List<String> columnNames)
  {
    super(columnNames, null);
  }

  private void addJobRow(CompactionJobStatus jobStatus)
  {
    final CompactionJob job = jobStatus.getJob();
    final CompactionCandidate candidate = jobStatus.getJob().getCandidate();
    final CompactionStatistics compactedStats = candidate.getCompactedStats();

    final List<Object> values = List.of(
        candidate.getCompactionInterval(),
        job.isMsq() ? "" : job.getNonNullTask().getId(),
        compactedStats == null
        ? ""
        : StringUtils.format("%d/%d", compactedStats.getNumSegments(), candidate.numSegments()),
        compactedStats == null
        ? ""
        : StringUtils.format("%d/%d", compactedStats.getTotalBytes(), candidate.getTotalBytes()),
        jobStatus.getPositionInQueue()
    );

    addRow(values.toArray(new Object[0]));
  }
}
