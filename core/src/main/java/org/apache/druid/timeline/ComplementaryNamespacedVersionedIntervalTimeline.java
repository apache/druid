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
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ComplementaryNamespacedVersionedIntervalTimeline<VersionType, ObjectType extends Overshadowable<ObjectType>>
    extends NamespacedVersionedIntervalTimeline<VersionType, ObjectType>
{

  private final NamespacedVersionedIntervalTimeline<VersionType, ObjectType> supportTimeline;

  private final String supportDatasource;

  private final String dataSource;

  public ComplementaryNamespacedVersionedIntervalTimeline(
      String dataSource,
      NamespacedVersionedIntervalTimeline<VersionType, ObjectType> supportTimeline,
      String supportDatasource
  )
  {
    this.dataSource = dataSource;
    this.supportTimeline = supportTimeline;
    this.supportDatasource = supportDatasource;
  }

  public String getSupportDatasource()
  {
    return supportDatasource;
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
      lock.readLock().lock();
      supportTimeline.lock.readLock().lock();
      ImmutableMap.Builder<String, List<TimelineObjectHolder<VersionType, ObjectType>>> ret = ImmutableMap.builder();
      List<TimelineObjectHolder<VersionType, ObjectType>> entries = new ArrayList<>();
      List<TimelineObjectHolder<VersionType, ObjectType>> supportEntries = new ArrayList<>();

      intervals.forEach(interval -> {
        Set<String> uncoveredNamespaces = new HashSet<>(supportTimeline.getNamespaces());
        for (String namespace : timelines.keySet()) {
          VersionedIntervalTimeline<VersionType, ObjectType> timeline = timelines.get(namespace);
          List<TimelineObjectHolder<VersionType, ObjectType>> entry = converter
              .apply(timeline, interval);
          entries.addAll(entry);
          List<Interval> remainingIntervals = filterIntervals(
              interval,
              entry.stream().map(e -> e.getInterval()).collect(
                  Collectors.toList())
          );
          String rootNamespace = getRootNamespace(namespace);
          for (String ns : supportTimeline.getNamespaces()) {
            if (getRootNamespace(ns).equals(rootNamespace)) {
              remainingIntervals.forEach(i -> supportEntries.addAll(supportTimeline.lookup(ns, i)));
              uncoveredNamespaces.remove(ns);
            }
          }
        }
        for (String ns : uncoveredNamespaces) {
          supportEntries.addAll(supportTimeline.lookup(ns, interval));
        }
      });
      if (!entries.isEmpty()) {
        ret.put(dataSource, entries);
      }
      if (!supportEntries.isEmpty()) {
        ret.put(supportDatasource, supportEntries);
      }
      return ret.build();
    }
    finally {
      supportTimeline.lock.readLock().unlock();
      lock.readLock().unlock();
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
        filteredIntervals.add(new Interval(remainingStart, skipInterval.getStart()));
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
