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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.utils.JodaUtils;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.UsedSegmentChecker;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ActionBasedUsedSegmentChecker implements UsedSegmentChecker
{
  private final TaskActionClient taskActionClient;

  public ActionBasedUsedSegmentChecker(TaskActionClient taskActionClient)
  {
    this.taskActionClient = taskActionClient;
  }

  @Override
  public Set<DataSegment> findUsedSegments(Set<SegmentIdentifier> identifiers) throws IOException
  {
    // Group by dataSource
    final Map<String, Set<SegmentIdentifier>> identifiersByDataSource = Maps.newTreeMap();
    for (SegmentIdentifier identifier : identifiers) {
      if (!identifiersByDataSource.containsKey(identifier.getDataSource())) {
        identifiersByDataSource.put(identifier.getDataSource(), Sets.<SegmentIdentifier>newHashSet());
      }
      identifiersByDataSource.get(identifier.getDataSource()).add(identifier);
    }

    final Set<DataSegment> retVal = Sets.newHashSet();

    for (Map.Entry<String, Set<SegmentIdentifier>> entry : identifiersByDataSource.entrySet()) {
      final List<Interval> intervals = JodaUtils.condenseIntervals(
          Iterables.transform(entry.getValue(), new Function<SegmentIdentifier, Interval>()
          {
            @Override
            public Interval apply(SegmentIdentifier input)
            {
              return input.getInterval();
            }
          })
      );

      final List<DataSegment> usedSegmentsForIntervals = taskActionClient.submit(
          new SegmentListUsedAction(entry.getKey(), null, intervals)
      );

      for (DataSegment segment : usedSegmentsForIntervals) {
        if (identifiers.contains(SegmentIdentifier.fromDataSegment(segment))) {
          retVal.add(segment);
        }
      }
    }

    return retVal;
  }
}
