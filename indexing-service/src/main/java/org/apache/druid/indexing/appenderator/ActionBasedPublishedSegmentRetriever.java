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
import org.apache.druid.indexing.common.actions.RetrieveSegmentsByIdAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.PublishedSegmentRetriever;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ActionBasedPublishedSegmentRetriever implements PublishedSegmentRetriever
{
  private static final Logger log = new Logger(ActionBasedPublishedSegmentRetriever.class);

  private final TaskActionClient taskActionClient;

  public ActionBasedPublishedSegmentRetriever(TaskActionClient taskActionClient)
  {
    this.taskActionClient = taskActionClient;
  }

  @Override
  public Set<DataSegment> findPublishedSegments(Set<SegmentId> segmentIds) throws IOException
  {
    if (segmentIds == null || segmentIds.isEmpty()) {
      return Collections.emptySet();
    }

    // Validate that all segments belong to the same datasource
    final String dataSource = segmentIds.iterator().next().getDataSource();
    for (SegmentId segmentId : segmentIds) {
      if (!segmentId.getDataSource().equals(dataSource)) {
        throw InvalidInput.exception(
            "Published segment IDs to find cannot belong to multiple datasources[%s, %s].",
            dataSource, segmentId.getDataSource()
        );
      }
    }

    // Try to retrieve segments using new task action
    final Set<String> serializedSegmentIds = segmentIds.stream()
                                                       .map(SegmentId::toString)
                                                       .collect(Collectors.toSet());
    try {
      return taskActionClient.submit(new RetrieveSegmentsByIdAction(dataSource, serializedSegmentIds));
    }
    catch (Exception e) {
      log.warn(
          e,
          "Could not retrieve published segment IDs[%s] using task action[retrieveSegmentsById]."
          + " Overlord maybe on an older version, retrying with action[segmentListUsed]."
          + " This task may fail to publish segments if there is a concurrent replace happening.",
          serializedSegmentIds
      );
    }

    // Fall back to using old task action if Overlord is still on an older version
    final Set<DataSegment> publishedSegments = new HashSet<>();
    final List<Interval> usedSearchIntervals = JodaUtils.condenseIntervals(
        Iterables.transform(segmentIds, SegmentId::getInterval)
    );
    final Collection<DataSegment> foundUsedSegments = taskActionClient.submit(
        new RetrieveUsedSegmentsAction(dataSource, usedSearchIntervals, Segments.INCLUDING_OVERSHADOWED)
    );
    for (DataSegment segment : foundUsedSegments) {
      if (segmentIds.contains(segment.getId())) {
        publishedSegments.add(segment);
      }
    }

    return publishedSegments;
  }
}
