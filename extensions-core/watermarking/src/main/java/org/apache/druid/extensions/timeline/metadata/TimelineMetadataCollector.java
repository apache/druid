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

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/***
 * The TimelineMetadataCollector is a mechanism that performs a fold of a set of TimelineMetadataCursors
 * with the same return type across a timeline provided by a TimelineServerView. Cursors walk
 * the timeline in chronological order, and results are aggregated into a list.
 * @param <TResult>
 */
public class TimelineMetadataCollector<TResult>
{
  private static final Logger log = new Logger(TimelineMetadataCollector.class);
  private final TimelineServerView serverView;
  private final List<? extends TimelineMetadataCursorFactory<TResult>> cursorFactories;

  public TimelineMetadataCollector(
      TimelineServerView serverView,
      List<? extends TimelineMetadataCursorFactory<TResult>> cursorFactories
  )
  {
    this.serverView = serverView;
    this.cursorFactories = cursorFactories;
  }

  public List<TimelineMetadata<TResult>> fold(
      String dataSource,
      Interval interval,
      boolean full
  )
  {

    TimelineLookup<String, ServerSelector> timeline =
        serverView.getTimeline(new TableDataSource(dataSource));

    if (timeline != null) {
      List<TimelineObjectHolder<String, ServerSelector>> segments =
          ImmutableList.copyOf(timeline.lookup(interval));

      if (segments.size() > 0) {
        log.debug(
            "Analyzing %d segments for '%s' for interval %s to %s",
            segments.size(),
            dataSource,
            interval.getStart(),
            interval.getEnd()
        );

        List<TimelineMetadataCursor<TResult>> timelineCursors = getCursors(
            dataSource,
            interval,
            segments,
            full
        );

        // fold list of cursors across timeline, removing completed cursors from the accumulator as they complete
        for (TimelineObjectHolder<String, ServerSelector> segment : segments) {
          List<TimelineMetadataCursor<TResult>> completed = advanceCursors(timelineCursors, segment);
          timelineCursors.removeAll(completed);
        }

        // complete any cursors which did not finish during fold, collect results
        List<TimelineMetadata<TResult>> results = new ArrayList<>();
        for (TimelineMetadataCursor<TResult> cursor : timelineCursors) {
          cursor.complete();
          results.add(cursor.getResult());
        }

        return results;
      } else {
        log.debug(
            "No segments for '%s' for interval %s to %s",
            dataSource,
            interval.getStart(),
            interval.getEnd()
        );
      }
    } else {
      log.warn("No timeline data for '%s'", dataSource);
    }
    return Collections.emptyList();
  }

  private List<TimelineMetadataCursor<TResult>> advanceCursors(
      List<TimelineMetadataCursor<TResult>> timelineCursors,
      TimelineObjectHolder<String, ServerSelector> segment
  )
  {
    List<TimelineMetadataCursor<TResult>> completed = new ArrayList<>();
    for (TimelineMetadataCursor<TResult> cursor : timelineCursors) {
      if (!cursor.advance(new TimelineSegment(segment))) {
        completed.add(cursor);
      }
    }
    return completed;
  }

  private List<TimelineMetadataCursor<TResult>> getCursors(
      String dataSource,
      Interval traceInterval,
      List<TimelineObjectHolder<String, ServerSelector>> segments,
      boolean isCompleteTimeline
  )
  {
    List<TimelineMetadataCursor<TResult>> cursors = new ArrayList<>();
    for (TimelineMetadataCursorFactory<TResult> factory : cursorFactories) {
      TimelineMetadataCursor<TResult> cursor = factory.build(dataSource, traceInterval, segments, isCompleteTimeline);
      if (cursor != null) {
        cursors.add(cursor);
      }
    }
    return cursors;
  }
}
