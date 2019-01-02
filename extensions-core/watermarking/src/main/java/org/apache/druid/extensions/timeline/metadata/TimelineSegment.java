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

package org.apache.druid.extensions.timeline.metadata;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/***
 * Computed wrapper for a segment from a TimelineServerView which provides convenience data for
 * TimelineMetadataCursor and TimelineMetadataCollector to operate on
 */
public class TimelineSegment
{
  private final Supplier<TimelineSegmentDetails> details;
  private final Interval interval;
  private final TimelineObjectHolder<String, ServerSelector> segment;

  public TimelineSegment(TimelineObjectHolder<String, ServerSelector> segment)
  {
    this.segment = segment;
    this.interval = segment.getInterval();
    this.details = Suppliers.memoize(() -> new TimelineSegmentDetails(segment));
  }

  /***
   * check segment has a ShardSpec that is not 'Linear'
   * @return
   */
  public boolean getIsBatchIndexed()
  {
    return details.get().isBatchIndexed;
  }

  /***
   * check segment is not actively being indexed and served by a realtime node
   * @return
   */
  public boolean getIsSegmentImmutable()
  {
    return details.get().isSegmentImmutable;
  }

  /***
   * get underlying timeline segment object
   * @return
   */
  public TimelineObjectHolder<String, ServerSelector> getSegment()
  {
    return segment;
  }

  /***
   * get segment interval
   * @return
   */
  public Interval getInterval()
  {
    return interval;
  }

  /***
   * check if segment start is equal to 'time' (or 'time' is null)
   * @param time time to see if segment start is equivalent to
   * @return
   */
  public boolean isContiguous(DateTime time)
  {
    return time == null || getInterval().getStart().isEqual(time);
  }

  /***
   * lazy computed details about indexing type and segment immutability
   */
  private class TimelineSegmentDetails
  {
    private final TimelineObjectHolder<String, ServerSelector> segment;
    private boolean isBatchIndexed;
    private boolean isSegmentImmutable;

    TimelineSegmentDetails(TimelineObjectHolder<String, ServerSelector> segment)
    {
      this.segment = segment;
      // todo: not entirely sure this group is homogeneous, if not, not a legit approach to determining if segment is
      // batch indexed or served by a historical, investigating further
      for (final ServerSelector selector : segment.getObject().payloads()) {
        QueryableDruidServer server = selector.pick();
        if (server != null) {
          DataSegment ds = selector.getSegment();
          ShardSpec ss = ds.getShardSpec();
          isBatchIndexed |= !(ss instanceof LinearShardSpec);
          isSegmentImmutable |= server.getServer().getType().isSegmentReplicationTarget();
        }
      }
    }
  }
}
