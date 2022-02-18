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

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.map.LinkedMap;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ComplementaryNamespacedVersionedIntervalTimeline<VersionType, ObjectType extends Overshadowable<ObjectType>>
    extends NamespacedVersionedIntervalTimeline<VersionType, ObjectType>
{

  private final LinkedMap<String, NamespacedVersionedIntervalTimeline<VersionType, ObjectType>> supportTimelinesByDataSource;

  private final String dataSource;

  public ComplementaryNamespacedVersionedIntervalTimeline(
          String dataSource,
          Map<String, NamespacedVersionedIntervalTimeline<VersionType, ObjectType>> supportTimelinesByDataSource,
          List<String> supportDataSourceQueryOrder
  )
  {
    this.dataSource = dataSource;
  }

    this.supportTimelinesByDataSource =
            new LinkedMap<>(supportDataSourceQueryOrder.size() + 1);
    this.supportTimelinesByDataSource.put(dataSource, this);
    supportDataSourceQueryOrder.forEach(ds -> this.supportTimelinesByDataSource.put(ds, supportTimelinesByDataSource.get(ds)));
  }

  @Override
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval)
  {
    return lookup(ImmutableList.of(interval), (timeline, in) ->
            timeline.lookup(in)).values().stream().flatMap(List::stream).collect(Collectors.toList());
  }

  @Override
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookupWithIncompletePartitions(
      Interval interval
  )
  {
    return lookup(ImmutableList.of(interval), (timeline, in) ->
        timeline.lookupWithIncompletePartitions(in)).values()
        .stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public ImmutableMap<String, List<TimelineObjectHolder<VersionType, ObjectType>>> lookupWithComplementary(
      List<Interval> intervals
  )
  {
    return lookup(intervals, (timeline, in) -> timeline.lookup(in));
  }

  private ImmutableMap<String, List<TimelineObjectHolder<VersionType, ObjectType>>> lookup(
      List<Interval> intervals,
      Function2<VersionedIntervalTimeline<VersionType, ObjectType>, Interval, List<TimelineObjectHolder<VersionType, ObjectType>>> converter
  )
  {
    try {
      supportTimelinesByDataSource.values().forEach(supportTimeline -> supportTimeline.lock.readLock().lock());

      ImmutableMap.Builder<String, List<TimelineObjectHolder<VersionType, ObjectType>>> ret = ImmutableMap.builder();
      
      Map<String, Map<String, Map<Interval, List<TimelineObjectHolder<VersionType, ObjectType>>>>> entriesForIntervalByDataSourceAndNamespace =
              new HashMap<>();

      // We assume here that the last timeline will be a superset of all other timelines. Every namespace and interval
      // should be covered by this base timeline
      NamespacedVersionedIntervalTimeline<VersionType, ObjectType> baseTimeline =
              supportTimelinesByDataSource.get(supportTimelinesByDataSource.lastKey());

      Map<String, List<Interval>> namespaceToRemainingInterval = baseTimeline.getNamespaces().stream()
              .map(ComplementaryNamespacedVersionedIntervalTimeline::getRootNamespace)
              .collect(Collectors.toMap(namespace -> namespace, namespace -> intervals));

      for (String dataSource : supportTimelinesByDataSource.keySet()) {
        if (namespaceToRemainingInterval.values().stream().anyMatch(remainingInterval -> !remainingInterval.isEmpty())) {
          // We have remaining segments to find
          entriesForIntervalByDataSourceAndNamespace.putIfAbsent(dataSource, new HashMap<>());
          NamespacedVersionedIntervalTimeline<VersionType, ObjectType> timeline = supportTimelinesByDataSource.get(dataSource);
          for (String ns : timeline.getNamespaces()) {
            String rootNamespace = getRootNamespace(ns);
            entriesForIntervalByDataSourceAndNamespace.get(dataSource).putIfAbsent(rootNamespace, new HashMap<>());
            for (Interval i : namespaceToRemainingInterval.getOrDefault(rootNamespace, new ArrayList<>())) {
              List<TimelineObjectHolder<VersionType, ObjectType>> supportEntry = timeline.lookup(ns, i);
              // For all but the base dataSource we want to filter intervals with end times past the requested interval
              // end time to prevent returning segments convering time intervals not requested
              if (!dataSource.equals(supportTimelinesByDataSource.lastKey())) {
                supportEntry = supportEntry.stream().filter(t -> !t.getTrueInterval().getEnd().isAfter(i.getEnd()))
                        .collect(Collectors.toList());
            }
              if (!supportEntry.isEmpty()) {
                List<TimelineObjectHolder<VersionType, ObjectType>> enteries =
                        entriesForIntervalByDataSourceAndNamespace.get(dataSource).get(rootNamespace).getOrDefault(i, new ArrayList<>());
                enteries.addAll(supportEntry);
                entriesForIntervalByDataSourceAndNamespace.get(dataSource).get(rootNamespace).put(i, enteries);
          }
        }
          }
        } else {
          // If there are no remaining segments to find then we're done
          break;
      }
        //Recompute remaining intervals by filtering out those that we just added
        Map<String, List<Interval>> tempNamespaceToRemainingInterval = new HashMap<>();
        for (String namespace : namespaceToRemainingInterval.keySet()) {
          List<Interval> remainingIntervals = namespaceToRemainingInterval.get(namespace).stream().map(remainingInterval -> {
            if (entriesForIntervalByDataSourceAndNamespace.containsKey(dataSource) &&
                    entriesForIntervalByDataSourceAndNamespace.get(dataSource).containsKey(namespace) &&
                    entriesForIntervalByDataSourceAndNamespace.get(dataSource).get(namespace).containsKey(remainingInterval)) {
              return filterIntervals(remainingInterval,
                      entriesForIntervalByDataSourceAndNamespace.get(dataSource).get(namespace).get(remainingInterval).stream()
                              .map(TimelineObjectHolder::getInterval).collect(Collectors.toList()));
      }
            return Collections.singletonList(remainingInterval);
          }).flatMap(List::stream).collect(Collectors.toList());
          tempNamespaceToRemainingInterval.put(namespace, remainingIntervals);
        }
        namespaceToRemainingInterval = tempNamespaceToRemainingInterval;
      }

      for (String dataSource : entriesForIntervalByDataSourceAndNamespace.keySet()) {
        List<TimelineObjectHolder<VersionType, ObjectType>> timelines = new ArrayList<>();
        Map<String, Map<Interval, List<TimelineObjectHolder<VersionType, ObjectType>>>> entriesForIntervalByNamespace =
                entriesForIntervalByDataSourceAndNamespace.get(dataSource);
        for (Map<Interval, List<TimelineObjectHolder<VersionType, ObjectType>>> entriesForInterval : entriesForIntervalByNamespace.values()) {
          for (List<TimelineObjectHolder<VersionType, ObjectType>> timelineObjectHolders : entriesForInterval.values()) {
            timelines.addAll(timelineObjectHolders);
          }
        }
        if (!timelines.isEmpty()) {
          ret.put(dataSource, timelines);
        }
      }

      return ret.build();
    }
    finally {
      supportTimelinesByDataSource.values().forEach(supportTimeline -> supportTimeline.lock.readLock().unlock());

    }
  }

  private List<Interval> filterIntervals(Interval interval, List<Interval> sortedSkipIntervals)
  {
    ImmutableList.Builder<Interval> filteredIntervals = ImmutableList.builder();
    DateTime remainingStart = interval.getStart();
    DateTime remainingEnd = interval.getEnd();
    for (Interval skipInterval : sortedSkipIntervals) {
      if (skipInterval.getStart().isBefore(remainingStart) && skipInterval.getEnd().isAfter(remainingStart)) {
        remainingStart = skipInterval.getEnd();
      } else if (skipInterval.getStart().isBefore(remainingEnd) && skipInterval.getEnd().isAfter(remainingEnd)) {
        remainingEnd = skipInterval.getStart();
      } else if (!remainingStart.isAfter(skipInterval.getStart()) && !remainingEnd.isBefore(skipInterval.getEnd())) {
        if (!remainingStart.equals(skipInterval.getStart())) {
        filteredIntervals.add(new Interval(remainingStart, skipInterval.getStart()));
        }
        remainingStart = skipInterval.getEnd();
      }
      if (!remainingStart.isBefore(remainingEnd)) {
        break;
      }
    }
    if (!remainingStart.equals(remainingEnd)) {
      filteredIntervals.add(new Interval(remainingStart, remainingEnd));
    }
    return filteredIntervals.build();
  }

  private static String getRootNamespace(String namespace)
  {
    int index = namespace.indexOf('_');
    if (index <= 0) {
      return namespace;
    }
    return namespace.substring(0, index);
  }

  interface Function2<F1, F2, T>
  {
    T apply(F1 var1, F2 var2);
  }
}
