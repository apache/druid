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

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TombstoneHelper
{

  private final IngestionSpec ingestionSpec;
  private final TaskActionClient taskActionClient;
  private final Collection<DataSegment> pushedSegments;

  public TombstoneHelper(
      Collection<DataSegment> pushedSegments,
      IngestionSpec ingestionSpec,
      TaskActionClient taskActionClient
  )
  {
    this.ingestionSpec = ingestionSpec;
    this.taskActionClient = taskActionClient;
    this.pushedSegments = pushedSegments;
  }

  public String getDataSource()
  {
    return ingestionSpec.getDataSchema().getDataSource();
  }

  public GranularitySpec getGranularitySpec()
  {
    return ingestionSpec.getDataSchema().getGranularitySpec();
  }

  public List<Interval> getInputIntervals()
  {
    return getGranularitySpec().inputIntervals();
  }

  private List<Interval> getPushedSegmentsIntervals()
  {
    List<Interval> pushedSegmentsIntervals = new ArrayList<>();
    for (DataSegment pushedSegment : pushedSegments) {
      pushedSegmentsIntervals.add(pushedSegment.getInterval());
    }
    return JodaUtils.condenseIntervals(pushedSegmentsIntervals);
  }

  public Set<DataSegment> computeTombstones() throws IOException
  {

    Set<DataSegment> retVal = new HashSet<>();

    if (pushedSegments.isEmpty()) {
      return retVal;
    }

    // pick an element of the set to be the prototype for building the
    // tombstones... they will share many similar properties
    DataSegment pushedSegmentPrototype = null;
    for (DataSegment pushedSegment : pushedSegments) {
      pushedSegmentPrototype = pushedSegment;
      break;
    }

    GranularitySpec granularitySpec = getGranularitySpec();
    List<Interval> pushedSegmentsIntervals = getPushedSegmentsIntervals();

    // create tombstones for the empty time chunks
    List<Interval> intervalsForUsedSegments = getUsedIntervals();
    for (Interval timeChunk : granularitySpec.sortedBucketIntervals()) {
      // is time chunk empty?
      boolean isEmpty = true;
      for (Interval pushedSegmentCondensedInterval : pushedSegmentsIntervals) {
        if (timeChunk.overlaps(pushedSegmentCondensedInterval)) {
          isEmpty = false;
          break;
        }
      }

      if (isEmpty) {
        boolean buildTombstone = false;
        // only build tombstone that covers an existing used segment (avoid
        // unecessary tombstones... this is a space optimization)
        for (Interval usedSegmentInterval : intervalsForUsedSegments) {
          if (timeChunk.overlaps(usedSegmentInterval)) {
            buildTombstone = true;
            break;
          }
        }
        if (buildTombstone) {
          // create tombstone
          DataSegment tombstone = createTombstoneForTimeChunk(pushedSegmentPrototype, timeChunk);
          retVal.add(tombstone);
        }
      }

    }

    return retVal;
  }

  private DataSegment createTombstoneForTimeChunk(DataSegment prototype, Interval timeChunk)
  {

    // most of the fields will be the same as prototype...
    DataSegment.Builder dataSegmentBuilder =
        DataSegment.builder(prototype);

    // interval is different:
    dataSegmentBuilder.interval(timeChunk).size(0);

    // and the loadSpec is different too:
    Preconditions.checkNotNull(prototype.getLoadSpec());
    Map<String, Object> tombstoneLoadSpec = new HashMap<>(prototype.getLoadSpec());
    // since loadspec comes from prototype it is guaranteed to be non-null

    tombstoneLoadSpec.put("type", DataSegment.TOMBSTONE_LOADSPEC_TYPE);
    tombstoneLoadSpec.put("path", null); // tombstones do not have any backing file
    dataSegmentBuilder.loadSpec(tombstoneLoadSpec);

    return dataSegmentBuilder.build();

  }


  /**
   * Helper method to prune required tombstones. Only tombstones that cover used intervals will be created
   * since those that not cover used intervals will be redundant.
   * @return Intervals corresponding to used segments that overlap with any of the spec's input intervals
   * @throws IOException
   */
  public List<Interval> getUsedIntervals() throws IOException
  {
    List<Interval> retVal = new ArrayList<>();

    List<Interval> condensedInputIntervals = JodaUtils.condenseIntervals(getInputIntervals());
    if (!condensedInputIntervals.isEmpty()) {
      Collection<DataSegment> usedSegmentsInInputInterval =
          taskActionClient.submit(new RetrieveUsedSegmentsAction(getDataSource(), null,
                                                                 condensedInputIntervals,
                                                                 Segments.ONLY_VISIBLE
          ));
      for (DataSegment usedSegment : usedSegmentsInInputInterval) {
        for (Interval condensedInputInterval : condensedInputIntervals) {
          if (condensedInputInterval.overlaps(usedSegment.getInterval())) {
            retVal.add(usedSegment.getInterval());
            break;
          }
        }
      }
    }

    return retVal;
  }

}
