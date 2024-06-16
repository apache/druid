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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.realtime.appenderator.PublishedSegmentRetriever;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ActionBasedPublishedSegmentRetriever implements PublishedSegmentRetriever
{
  private final TaskActionClient taskActionClient;

  public ActionBasedPublishedSegmentRetriever(TaskActionClient taskActionClient)
  {
    this.taskActionClient = taskActionClient;
  }

  @Override
  public Set<DataSegment> findPublishedSegments(Set<SegmentIdWithShardSpec> segmentIds) throws IOException
  {
    // Validate that all segments belong to the same datasource
    final String dataSource = segmentIds.iterator().next().getDataSource();
    final Set<SegmentId> segmentIdsToFind = new HashSet<>();
    for (SegmentIdWithShardSpec segmentId : segmentIds) {
      segmentIdsToFind.add(segmentId.asSegmentId());
      if (!segmentId.getDataSource().equals(dataSource)) {
        throw InvalidInput.exception(
            "Published segment IDs to find cannot belong to multiple datasources[%s, %s].",
            dataSource, segmentId.getDataSource()
        );
      }
    }

    final Set<DataSegment> publishedSegments = new HashSet<>();

    // Search for the required segments in the "used" set
    final List<Interval> usedSearchIntervals = JodaUtils.condenseIntervals(
        Iterables.transform(segmentIdsToFind, SegmentId::getInterval)
    );
    final Collection<DataSegment> foundUsedSegments = taskActionClient.submit(
        new RetrieveUsedSegmentsAction(dataSource, null, usedSearchIntervals, Segments.INCLUDING_OVERSHADOWED)
    );
    for (DataSegment segment : foundUsedSegments) {
      if (segmentIdsToFind.contains(segment.getId())) {
        publishedSegments.add(segment);
        segmentIdsToFind.remove(segment.getId());
      }
    }

    if (segmentIdsToFind.isEmpty()) {
      return publishedSegments;
    }

    // Search for the remaining segments in the "unused" set
    final List<String> versions = segmentIdsToFind.stream().map(SegmentId::getVersion).collect(Collectors.toList());
    final List<Interval> unusedSearchIntervals = JodaUtils.condenseIntervals(
        Iterables.transform(segmentIdsToFind, SegmentId::getInterval)
    );
    for (Interval searchInterval : unusedSearchIntervals) {
      final Collection<DataSegment> foundUnusedSegments = taskActionClient
          .submit(new RetrieveUnusedSegmentsAction(dataSource, searchInterval, versions, null, null));
      for (DataSegment segment : foundUnusedSegments) {
        if (segmentIdsToFind.contains(segment.getId())) {
          publishedSegments.add(segment);
          segmentIdsToFind.remove(segment.getId());
        }
      }
    }

    return publishedSegments;
  }
}
