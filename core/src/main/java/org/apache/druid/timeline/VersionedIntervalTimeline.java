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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.timeline.partition.ImmutablePartitionHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.StreamSupport;

/**
 * VersionedIntervalTimeline is a data structure that manages objects on a specific timeline.
 *
 * It associates a jodatime Interval and a generically-typed version with the object that is being stored.
 *
 * In the event of overlapping timeline entries, timeline intervals may be chunked. The underlying data associated
 * with a timeline entry remains unchanged when chunking occurs.
 *
 * After loading objects via the add() method, the lookup(Interval) method can be used to get the list of the most
 * recent objects (according to the version) that match the given interval.  The intent is that objects represent
 * a certain time period and when you do a lookup(), you are asking for all of the objects that you need to look
 * at in order to get a correct answer about that time period.
 *
 * The findFullyOvershadowed() method returns a list of objects that will never be returned by a call to lookup() because
 * they are overshadowed by some other object.  This can be used in conjunction with the add() and remove() methods
 * to achieve "atomic" updates.  First add new items, then check if those items caused anything to be overshadowed, if
 * so, remove the overshadowed elements and you have effectively updated your data set without any user impact.
 */
public class VersionedIntervalTimeline<VersionType, ObjectType extends Overshadowable<ObjectType>> implements TimelineLookup<VersionType, ObjectType>
{
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  // Below timelines stores only *visible* timelineEntries
  // adjusted interval -> timelineEntry
  private final NavigableMap<Interval, TimelineEntry> completePartitionsTimeline = new TreeMap<>(
      Comparators.intervalsByStartThenEnd()
  );
  // IncompletePartitionsTimeline also includes completePartitionsTimeline
  // adjusted interval -> timelineEntry
  @VisibleForTesting
  final NavigableMap<Interval, TimelineEntry> incompletePartitionsTimeline = new TreeMap<>(
      Comparators.intervalsByStartThenEnd()
  );
  // true interval -> version -> timelineEntry
  private final Map<Interval, TreeMap<VersionType, TimelineEntry>> allTimelineEntries = new HashMap<>();
  private final AtomicInteger numObjects = new AtomicInteger();

  private final Comparator<? super VersionType> versionComparator;

  public VersionedIntervalTimeline(Comparator<? super VersionType> versionComparator)
  {
    this.versionComparator = versionComparator;
  }

  public static VersionedIntervalTimeline<String, DataSegment> forSegments(Iterable<DataSegment> segments)
  {
    return forSegments(segments.iterator());
  }

  public static VersionedIntervalTimeline<String, DataSegment> forSegments(Iterator<DataSegment> segments)
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline =
        new VersionedIntervalTimeline<>(Comparator.naturalOrder());
    addSegments(timeline, segments);
    return timeline;
  }

  public static void addSegments(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Iterator<DataSegment> segments
  )
  {
    timeline.addAll(
        Iterators.transform(segments, segment -> segment.getShardSpec().createChunk(segment)),
        DataSegment::getInterval,
        DataSegment::getVersion
    );
  }

  public Map<Interval, TreeMap<VersionType, TimelineEntry>> getAllTimelineEntries()
  {
    return allTimelineEntries;
  }

  /**
   * Returns a lazy collection with all objects (including overshadowed, see {@link #findFullyOvershadowed}) in this
   * VersionedIntervalTimeline to be used for iteration or {@link Collection#stream()} transformation. The order of
   * objects in this collection is unspecified.
   *
   * Note: iteration over the returned collection may not be as trivially cheap as, for example, iteration over an
   * ArrayList. Try (to some reasonable extent) to organize the code so that it iterates the returned collection only
   * once rather than several times.
   */
  public Collection<ObjectType> iterateAllObjects()
  {
    return CollectionUtils.createLazyCollectionFromStream(
        () -> allTimelineEntries
            .values()
            .stream()
            .flatMap((TreeMap<VersionType, TimelineEntry> entryMap) -> entryMap.values().stream())
            .flatMap((TimelineEntry entry) -> StreamSupport.stream(entry.getPartitionHolder().spliterator(), false))
            .map(PartitionChunk::getObject),
        numObjects.get()
    );
  }

  public void add(final Interval interval, VersionType version, PartitionChunk<ObjectType> object)
  {
    addAll(Iterators.singletonIterator(object), o -> interval, o -> version);
  }

  private void addAll(
      final Iterator<PartitionChunk<ObjectType>> objects,
      final Function<ObjectType, Interval> intervalFunction,
      final Function<ObjectType, VersionType> versionFunction
  )
  {
    lock.writeLock().lock();

    try {
      final IdentityHashMap<TimelineEntry, Interval> allEntries = new IdentityHashMap<>();

      while (objects.hasNext()) {
        PartitionChunk<ObjectType> object = objects.next();
        Interval interval = intervalFunction.apply(object.getObject());
        VersionType version = versionFunction.apply(object.getObject());
        Map<VersionType, TimelineEntry> exists = allTimelineEntries.get(interval);
        TimelineEntry entry;

        if (exists == null) {
          entry = new TimelineEntry(interval, version, new PartitionHolder<>(object));
          TreeMap<VersionType, TimelineEntry> versionEntry = new TreeMap<>(versionComparator);
          versionEntry.put(version, entry);
          allTimelineEntries.put(interval, versionEntry);
          numObjects.incrementAndGet();
        } else {
          entry = exists.get(version);

          if (entry == null) {
            entry = new TimelineEntry(interval, version, new PartitionHolder<>(object));
            exists.put(version, entry);
            numObjects.incrementAndGet();
          } else {
            PartitionHolder<ObjectType> partitionHolder = entry.getPartitionHolder();
            if (partitionHolder.add(object)) {
              numObjects.incrementAndGet();
            }
          }
        }

        allEntries.put(entry, interval);
      }

      // "isComplete" is O(objects in holder) so defer it to the end of addAll.
      for (Entry<TimelineEntry, Interval> entry : allEntries.entrySet()) {
        Interval interval = entry.getValue();

        if (entry.getKey().getPartitionHolder().isComplete()) {
          add(completePartitionsTimeline, interval, entry.getKey());
        }

        add(incompletePartitionsTimeline, interval, entry.getKey());
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  @Nullable
  public PartitionChunk<ObjectType> remove(Interval interval, VersionType version, PartitionChunk<ObjectType> chunk)
  {
    lock.writeLock().lock();
    try {
      Map<VersionType, TimelineEntry> versionEntries = allTimelineEntries.get(interval);
      if (versionEntries == null) {
        return null;
      }

      TimelineEntry entry = versionEntries.get(version);
      if (entry == null) {
        return null;
      }

      PartitionChunk<ObjectType> removedChunk = entry.getPartitionHolder().remove(chunk);
      if (removedChunk == null) {
        return null;
      }
      numObjects.decrementAndGet();
      if (entry.getPartitionHolder().isEmpty()) {
        versionEntries.remove(version);
        if (versionEntries.isEmpty()) {
          allTimelineEntries.remove(interval);
        }

        remove(incompletePartitionsTimeline, interval, entry, true);
      }

      remove(completePartitionsTimeline, interval, entry, false);

      return removedChunk;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public @Nullable PartitionHolder<ObjectType> findEntry(Interval interval, VersionType version)
  {
    lock.readLock().lock();
    try {
      for (Entry<Interval, TreeMap<VersionType, TimelineEntry>> entry : allTimelineEntries.entrySet()) {
        if (entry.getKey().equals(interval) || entry.getKey().contains(interval)) {
          TimelineEntry foundEntry = entry.getValue().get(version);
          if (foundEntry != null) {
            return new ImmutablePartitionHolder<>(foundEntry.getPartitionHolder());
          }
        }
      }

      return null;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Does a lookup for the objects representing the given time interval.  Will *only* return
   * PartitionHolders that are complete.
   *
   * @param interval interval to find objects for
   *
   * @return Holders representing the interval that the objects exist for, PartitionHolders
   * are guaranteed to be complete
   */
  @Override
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval)
  {
    lock.readLock().lock();
    try {
      return lookup(interval, false);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookupWithIncompletePartitions(Interval interval)
  {
    lock.readLock().lock();
    try {
      return lookup(interval, true);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public boolean isEmpty()
  {
    lock.readLock().lock();
    try {
      return completePartitionsTimeline.isEmpty();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public TimelineObjectHolder<VersionType, ObjectType> first()
  {
    lock.readLock().lock();
    try {
      return timelineEntryToObjectHolder(completePartitionsTimeline.firstEntry().getValue());
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public TimelineObjectHolder<VersionType, ObjectType> last()
  {
    lock.readLock().lock();
    try {
      return timelineEntryToObjectHolder(completePartitionsTimeline.lastEntry().getValue());
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private TimelineObjectHolder<VersionType, ObjectType> timelineEntryToObjectHolder(TimelineEntry entry)
  {
    return new TimelineObjectHolder<>(
        entry.getTrueInterval(),
        entry.getTrueInterval(),
        entry.getVersion(),
        new PartitionHolder<>(entry.getPartitionHolder())
    );
  }

  /**
   * This method should be deduplicated with DataSourcesSnapshot.determineOvershadowedSegments(): see
   * https://github.com/apache/incubator-druid/issues/8070.
   */
  public Set<TimelineObjectHolder<VersionType, ObjectType>> findFullyOvershadowed()
  {
    lock.readLock().lock();
    try {
      // 1. Put all timelineEntries and remove all visible entries to find out only non-visible timelineEntries.
      final Map<Interval, Map<VersionType, TimelineEntry>> overShadowed = new HashMap<>();
      for (Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
        @SuppressWarnings("unchecked")
        Map<VersionType, TimelineEntry> versionCopy = (TreeMap) versionEntry.getValue().clone();
        overShadowed.put(versionEntry.getKey(), versionCopy);
      }

      for (Entry<Interval, TimelineEntry> entry : completePartitionsTimeline.entrySet()) {
        Map<VersionType, TimelineEntry> versionEntry = overShadowed.get(entry.getValue().getTrueInterval());
        if (versionEntry != null) {
          versionEntry.remove(entry.getValue().getVersion());
          if (versionEntry.isEmpty()) {
            overShadowed.remove(entry.getValue().getTrueInterval());
          }
        }
      }

      for (Entry<Interval, TimelineEntry> entry : incompletePartitionsTimeline.entrySet()) {
        Map<VersionType, TimelineEntry> versionEntry = overShadowed.get(entry.getValue().getTrueInterval());
        if (versionEntry != null) {
          versionEntry.remove(entry.getValue().getVersion());
          if (versionEntry.isEmpty()) {
            overShadowed.remove(entry.getValue().getTrueInterval());
          }
        }
      }

      final Set<TimelineObjectHolder<VersionType, ObjectType>> retVal = new HashSet<>();
      for (Entry<Interval, Map<VersionType, TimelineEntry>> versionEntry : overShadowed.entrySet()) {
        for (Entry<VersionType, TimelineEntry> entry : versionEntry.getValue().entrySet()) {
          final TimelineEntry timelineEntry = entry.getValue();
          retVal.add(timelineEntryToObjectHolder(timelineEntry));
        }
      }

      // 2. Visible timelineEntries can also have overshadowed segments. Add them to the result too.
      for (TimelineEntry entry : incompletePartitionsTimeline.values()) {
        final List<PartitionChunk<ObjectType>> entryOvershadowed = entry.partitionHolder.getOvershadowed();
        if (!entryOvershadowed.isEmpty()) {
          retVal.add(
              new TimelineObjectHolder<>(
                  entry.trueInterval,
                  entry.version,
                  new PartitionHolder<>(entryOvershadowed)
              )
          );
        }
      }

      return retVal;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public boolean isOvershadowed(Interval interval, VersionType version, ObjectType object)
  {
    lock.readLock().lock();
    try {
      TimelineEntry entry = completePartitionsTimeline.get(interval);
      if (entry != null) {
        final int majorVersionCompare = versionComparator.compare(version, entry.getVersion());
        if (majorVersionCompare == 0) {
          for (PartitionChunk<ObjectType> chunk : entry.partitionHolder) {
            if (chunk.getObject().overshadows(object)) {
              return true;
            }
          }
          return false;
        } else {
          return majorVersionCompare < 0;
        }
      }

      Interval lower = completePartitionsTimeline.floorKey(
          new Interval(interval.getStart(), DateTimes.MAX)
      );

      if (lower == null || !lower.overlaps(interval)) {
        return false;
      }

      Interval prev = null;
      Interval curr = lower;

      do {
        if (curr == null ||  //no further keys
            (prev != null && curr.getStartMillis() > prev.getEndMillis()) //a discontinuity
        ) {
          return false;
        }

        final TimelineEntry timelineEntry = completePartitionsTimeline.get(curr);
        final int versionCompare = versionComparator.compare(version, timelineEntry.getVersion());

        //lower or same version
        if (versionCompare > 0) {
          return false;
        } else if (versionCompare == 0) {
          if (timelineEntry.partitionHolder.stream().noneMatch(chunk -> chunk.getObject().overshadows(object))) {
            return false;
          }
        }

        prev = curr;
        curr = completePartitionsTimeline.higherKey(curr);

      } while (interval.getEndMillis() > prev.getEndMillis());

      return true;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private void add(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      TimelineEntry entry
  )
  {
    TimelineEntry existsInTimeline = timeline.get(interval);

    if (existsInTimeline != null) {
      int compare = versionComparator.compare(entry.getVersion(), existsInTimeline.getVersion());
      if (compare > 0) {
        addIntervalToTimeline(interval, entry, timeline);
      }
      return;
    }

    Interval lowerKey = timeline.lowerKey(interval);

    if (lowerKey != null) {
      if (addAtKey(timeline, lowerKey, entry)) {
        return;
      }
    }

    Interval higherKey = timeline.higherKey(interval);

    if (higherKey != null) {
      if (addAtKey(timeline, higherKey, entry)) {
        return;
      }
    }

    addIntervalToTimeline(interval, entry, timeline);
  }

  /**
   * @param timeline
   * @param key
   * @param entry
   *
   * @return boolean flag indicating whether or not we inserted or discarded something
   */
  private boolean addAtKey(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval key,
      TimelineEntry entry
  )
  {
    boolean retVal = false;
    Interval currKey = key;
    Interval entryInterval = entry.getTrueInterval();

    if (!currKey.overlaps(entryInterval)) {
      return false;
    }

    while (entryInterval != null && currKey != null && currKey.overlaps(entryInterval)) {
      final Interval nextKey = timeline.higherKey(currKey);

      final int versionCompare = versionComparator.compare(
          entry.getVersion(),
          timeline.get(currKey).getVersion()
      );

      if (versionCompare < 0) {
        // since the entry version is lower than the existing one, the existing one overwrites the given entry
        // if overlapped.
        if (currKey.contains(entryInterval)) {
          // the version of the entry of currKey is larger than that of the given entry. Discard it
          return true;
        } else if (currKey.getStart().isBefore(entryInterval.getStart())) {
          //       | entry |
          //     | cur |
          // =>        |new|
          entryInterval = new Interval(currKey.getEnd(), entryInterval.getEnd());
        } else {
          //     | entry |
          //         | cur |
          // =>  |new|
          addIntervalToTimeline(new Interval(entryInterval.getStart(), currKey.getStart()), entry, timeline);

          //     |   entry   |
          //       | cur |
          // =>          |new|
          if (entryInterval.getEnd().isAfter(currKey.getEnd())) {
            entryInterval = new Interval(currKey.getEnd(), entryInterval.getEnd());
          } else {
            // Discard this entry since there is no portion of the entry interval that goes past the end of the curr
            // key interval.
            entryInterval = null;
          }
        }
      } else if (versionCompare > 0) {
        // since the entry version is greater than the existing one, the given entry overwrites the existing one
        // if overlapped.
        final TimelineEntry oldEntry = timeline.remove(currKey);

        if (currKey.contains(entryInterval)) {
          //     |      cur      |
          //         | entry |
          // =>  |old|  new  |old|
          addIntervalToTimeline(new Interval(currKey.getStart(), entryInterval.getStart()), oldEntry, timeline);
          addIntervalToTimeline(new Interval(entryInterval.getEnd(), currKey.getEnd()), oldEntry, timeline);
          addIntervalToTimeline(entryInterval, entry, timeline);

          return true;
        } else if (currKey.getStart().isBefore(entryInterval.getStart())) {
          //     |   cur  |
          //         |   entry   |
          // =>  |old|
          addIntervalToTimeline(new Interval(currKey.getStart(), entryInterval.getStart()), oldEntry, timeline);
        } else if (entryInterval.getEnd().isBefore(currKey.getEnd())) {
          //            |   cur  |
          //     |   entry   |
          // =>              |old|
          addIntervalToTimeline(new Interval(entryInterval.getEnd(), currKey.getEnd()), oldEntry, timeline);
        }
      } else {
        if (timeline.get(currKey).equals(entry)) {
          // This occurs when restoring segments
          timeline.remove(currKey);
        } else {
          throw new UOE(
              "Cannot add overlapping segments [%s and %s] with the same version [%s]",
              currKey,
              entryInterval,
              entry.getVersion()
          );
        }
      }

      currKey = nextKey;
      retVal = true;
    }

    addIntervalToTimeline(entryInterval, entry, timeline);

    return retVal;
  }

  private void addIntervalToTimeline(
      Interval interval,
      TimelineEntry entry,
      NavigableMap<Interval, TimelineEntry> timeline
  )
  {
    if (interval != null && interval.toDurationMillis() > 0) {
      timeline.put(interval, entry);
    }
  }

  private void remove(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      TimelineEntry entry,
      boolean incompleteOk
  )
  {
    List<Interval> intervalsToRemove = new ArrayList<>();
    TimelineEntry removed = timeline.get(interval);

    if (removed == null) {
      Iterator<Entry<Interval, TimelineEntry>> iter = timeline.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<Interval, TimelineEntry> timelineEntry = iter.next();
        if (timelineEntry.getValue() == entry) {
          intervalsToRemove.add(timelineEntry.getKey());
        }
      }
    } else {
      intervalsToRemove.add(interval);
    }

    for (Interval i : intervalsToRemove) {
      remove(timeline, i, incompleteOk);
    }
  }

  private void remove(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      boolean incompleteOk
  )
  {
    timeline.remove(interval);

    for (Entry<Interval, TreeMap<VersionType, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
      if (versionEntry.getKey().overlap(interval) != null) {
        if (incompleteOk) {
          add(timeline, versionEntry.getKey(), versionEntry.getValue().lastEntry().getValue());
        } else {
          for (VersionType ver : versionEntry.getValue().descendingKeySet()) {
            TimelineEntry timelineEntry = versionEntry.getValue().get(ver);
            if (timelineEntry.getPartitionHolder().isComplete()) {
              add(timeline, versionEntry.getKey(), timelineEntry);
              break;
            }
          }
        }
      }
    }
  }

  private List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval, boolean incompleteOk)
  {
    List<TimelineObjectHolder<VersionType, ObjectType>> retVal = new ArrayList<>();
    NavigableMap<Interval, TimelineEntry> timeline = (incompleteOk)
                                                     ? incompletePartitionsTimeline
                                                     : completePartitionsTimeline;

    for (Entry<Interval, TimelineEntry> entry : timeline.entrySet()) {
      Interval timelineInterval = entry.getKey();
      TimelineEntry val = entry.getValue();

      if (timelineInterval.overlaps(interval)) {
        retVal.add(
            new TimelineObjectHolder<>(
                timelineInterval,
                val.getTrueInterval(),
                val.getVersion(),
                new PartitionHolder<>(val.getPartitionHolder())
            )
        );
      }
    }

    if (retVal.isEmpty()) {
      return retVal;
    }

    TimelineObjectHolder<VersionType, ObjectType> firstEntry = retVal.get(0);
    if (interval.overlaps(firstEntry.getInterval()) && interval.getStart()
                                                               .isAfter(firstEntry.getInterval().getStart())) {
      retVal.set(
          0,
          new TimelineObjectHolder<>(
              new Interval(interval.getStart(), firstEntry.getInterval().getEnd()),
              firstEntry.getTrueInterval(),
              firstEntry.getVersion(),
              firstEntry.getObject()
          )
      );
    }

    TimelineObjectHolder<VersionType, ObjectType> lastEntry = retVal.get(retVal.size() - 1);
    if (interval.overlaps(lastEntry.getInterval()) && interval.getEnd().isBefore(lastEntry.getInterval().getEnd())) {
      retVal.set(
          retVal.size() - 1,
          new TimelineObjectHolder<>(
              new Interval(lastEntry.getInterval().getStart(), interval.getEnd()),
              lastEntry.getTrueInterval(),
              lastEntry.getVersion(),
              lastEntry.getObject()
          )
      );
    }

    return retVal;
  }

  public class TimelineEntry
  {
    private final Interval trueInterval;
    private final VersionType version;
    private final PartitionHolder<ObjectType> partitionHolder;

    TimelineEntry(Interval trueInterval, VersionType version, PartitionHolder<ObjectType> partitionHolder)
    {
      this.trueInterval = Preconditions.checkNotNull(trueInterval);
      this.version = Preconditions.checkNotNull(version);
      this.partitionHolder = Preconditions.checkNotNull(partitionHolder);
    }

    Interval getTrueInterval()
    {
      return trueInterval;
    }

    public VersionType getVersion()
    {
      return version;
    }

    public PartitionHolder<ObjectType> getPartitionHolder()
    {
      return partitionHolder;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final TimelineEntry that = (TimelineEntry) o;

      if (!this.trueInterval.equals(that.trueInterval)) {
        return false;
      }

      if (!this.version.equals(that.version)) {
        return false;
      }

      if (!this.partitionHolder.equals(that.partitionHolder)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(trueInterval, version, partitionHolder);
    }
  }
}
