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

package org.apache.druid.indexing.appenderator;

import com.google.common.collect.Iterables;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.UsedSegmentChecker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ActionBasedUsedSegmentChecker implements UsedSegmentChecker
{
  private final TaskActionClient taskActionClient;

  public ActionBasedUsedSegmentChecker(TaskActionClient taskActionClient)
  {
    this.taskActionClient = taskActionClient;
  }

  @Override
  public Set<DataSegment> findUsedSegments(Set<SegmentIdWithShardSpec> segmentIds) throws IOException
  {
    // Group by dataSource
    final Map<String, Set<SegmentId>> idsByDataSource = new TreeMap<>();
    for (SegmentIdWithShardSpec segmentId : segmentIds) {
      idsByDataSource.computeIfAbsent(segmentId.getDataSource(), i -> new HashSet<>()).add(segmentId.asSegmentId());
    }

    final Set<DataSegment> usedSegments = new HashSet<>();

    for (Map.Entry<String, Set<SegmentId>> entry : idsByDataSource.entrySet()) {
      String dataSource = entry.getKey();
      Set<SegmentId> segmentIdsInDataSource = entry.getValue();
      final List<Interval> intervals = JodaUtils.condenseIntervals(
          Iterables.transform(segmentIdsInDataSource, SegmentId::getInterval)
      );

      final Collection<DataSegment> usedSegmentsForIntervals = taskActionClient
          .submit(new RetrieveUsedSegmentsAction(dataSource, intervals));

      for (DataSegment segment : usedSegmentsForIntervals) {
        if (segmentIdsInDataSource.contains(segment.getId())) {
          usedSegments.add(segment);
        }
      }
    }

    return usedSegments;
  }
}
