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

package org.apache.druid.timeline.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSets;
import it.unimi.dsi.fastutil.shorts.AbstractShort2ObjectSortedMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectSortedMap;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import it.unimi.dsi.fastutil.shorts.ShortComparators;
import it.unimi.dsi.fastutil.shorts.ShortSortedSet;
import it.unimi.dsi.fastutil.shorts.ShortSortedSets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;

/**
 * OvershadowableManager manages the state of {@link AtomicUpdateGroup}. See the below {@link State} for details of
 * the possible state.
 * Note that an AtomicUpdateGroup can consist of {@link Overshadowable}s of the same majorVersion, minorVersion,
 * rootPartition range, and atomicUpdateGroupSize.
 * In {@link org.apache.druid.timeline.VersionedIntervalTimeline}, this class is used to manage segments in the same
 * timeChunk.
 *
 * This class is not thread-safe.
 */
class OvershadowableManager<T extends Overshadowable<T>>
{
  private enum State
  {
    STANDBY, // have atomicUpdateGroup of higher versions than visible
    VISIBLE, // have a single fully available atomicUpdateGroup of highest version
    OVERSHADOWED // have atomicUpdateGroup of lower versions than visible
  }

  private final Map<Integer, PartitionChunk<T>> knownPartitionChunks; // served segments

  // (start partitionId, end partitionId) -> minorVersion -> atomicUpdateGroup
  private final TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> standbyGroups;
  private final TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> visibleGroup;
  private final TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> overshadowedGroups;

  OvershadowableManager()
  {
    this.knownPartitionChunks = new HashMap<>();
    this.standbyGroups = new TreeMap<>();
    this.visibleGroup = new TreeMap<>();
    this.overshadowedGroups = new TreeMap<>();
  }

  OvershadowableManager(OvershadowableManager<T> other)
  {
    this.knownPartitionChunks = new HashMap<>(other.knownPartitionChunks);
    this.standbyGroups = new TreeMap<>(other.standbyGroups);
    this.visibleGroup = new TreeMap<>(other.visibleGroup);
    this.overshadowedGroups = new TreeMap<>(other.overshadowedGroups);
  }

  private TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> getStateMap(State state)
  {
    switch (state) {
      case STANDBY:
        return standbyGroups;
      case VISIBLE:
        return visibleGroup;
      case OVERSHADOWED:
        return overshadowedGroups;
      default:
        throw new ISE("Unknown state[%s]", state);
    }
  }

  private Short2ObjectSortedMap<AtomicUpdateGroup<T>> createMinorVersionToAugMap(State state)
  {
    switch (state) {
      case STANDBY:
      case OVERSHADOWED:
        return new Short2ObjectRBTreeMap<>();
      case VISIBLE:
        return new SingleEntryShort2ObjectSortedMap<>();
      default:
        throw new ISE("Unknown state[%s]", state);
    }
  }

  private void transitAtomicUpdateGroupState(AtomicUpdateGroup<T> atomicUpdateGroup, State from, State to)
  {
    Preconditions.checkNotNull(atomicUpdateGroup, "atomicUpdateGroup");
    Preconditions.checkArgument(!atomicUpdateGroup.isEmpty(), "empty atomicUpdateGroup");

    removeFrom(atomicUpdateGroup, from);
    addAtomicUpdateGroupWithState(atomicUpdateGroup, to);
  }

  /**
   * Find the {@link AtomicUpdateGroup} of the given state which has the same {@link RootPartitionRange} and
   * minorVersion with {@link PartitionChunk}.
   */
  @Nullable
  private AtomicUpdateGroup<T> findAtomicUpdateGroupWith(PartitionChunk<T> chunk, State state)
  {
    final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = getStateMap(state).get(
        RootPartitionRange.of(chunk)
    );
    if (versionToGroup != null) {
      final AtomicUpdateGroup<T> atomicUpdateGroup = versionToGroup.get(chunk.getObject().getMinorVersion());
      if (atomicUpdateGroup != null) {
        return atomicUpdateGroup;
      }
    }
    return null;
  }

  /**
   * Returns null if atomicUpdateGroup is not found for the state.
   * Can return an empty atomicUpdateGroup.
   */
  @Nullable
  private AtomicUpdateGroup<T> tryRemoveChunkFromGroupWithState(PartitionChunk<T> chunk, State state)
  {
    final RootPartitionRange rangeKey = RootPartitionRange.of(chunk);
    final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = getStateMap(state).get(rangeKey);
    if (versionToGroup != null) {
      final AtomicUpdateGroup<T> atomicUpdateGroup = versionToGroup.get(chunk.getObject().getMinorVersion());
      if (atomicUpdateGroup != null) {
        atomicUpdateGroup.remove(chunk);
        if (atomicUpdateGroup.isEmpty()) {
          versionToGroup.remove(chunk.getObject().getMinorVersion());
          if (versionToGroup.isEmpty()) {
            getStateMap(state).remove(rangeKey);
          }
        }

        determineVisibleGroupAfterRemove(
            atomicUpdateGroup,
            RootPartitionRange.of(chunk),
            chunk.getObject().getMinorVersion(),
            state
        );
        return atomicUpdateGroup;
      }
    }
    return null;
  }

  private List<Short2ObjectMap.Entry<AtomicUpdateGroup<T>>> findOvershadowedBy(
      AtomicUpdateGroup<T> aug,
      State fromState
  )
  {
    final RootPartitionRange rangeKeyOfGivenAug = RootPartitionRange.of(aug);
    return findOvershadowedBy(rangeKeyOfGivenAug, aug.getMinorVersion(), fromState);
  }

  private List<Short2ObjectMap.Entry<AtomicUpdateGroup<T>>> findOvershadowedBy(
      RootPartitionRange rangeOfAug,
      short minorVersion,
      State fromState
  )
  {
    Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> current = getStateMap(fromState)
        .floorEntry(rangeOfAug);

    if (current == null) {
      return Collections.emptyList();
    }

    // Find the first key for searching for overshadowed atomicUpdateGroup
    while (true) {
      final Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> lowerEntry = getStateMap(fromState)
          .lowerEntry(current.getKey());
      if (lowerEntry != null && lowerEntry.getKey().startPartitionId == rangeOfAug.startPartitionId) {
        current = lowerEntry;
      } else {
        break;
      }
    }

    final List<Short2ObjectMap.Entry<AtomicUpdateGroup<T>>> found = new ArrayList<>();
    while (current != null && rangeOfAug.contains(current.getKey())) {
      final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = current.getValue();
      found.addAll(versionToGroup.subMap(versionToGroup.firstShortKey(), minorVersion).short2ObjectEntrySet());
      current = getStateMap(fromState).higherEntry(current.getKey());
    }
    return found;
  }

  /**
   * Handles addition of the atomicUpdateGroup to the given state
   */
  private void transitionStandbyGroupIfFull(AtomicUpdateGroup<T> aug, State stateOfAug)
  {
    if (stateOfAug == State.STANDBY) {
      // A standby atomicUpdateGroup becomes visible when its all segments are available.
      if (aug.isFull()) {
        // A visible atomicUpdateGroup becomes overshadowed when a fully available standby atomicUpdateGroup becomes
        // visible which overshadows the current visible one.
        findOvershadowedBy(aug, State.VISIBLE)
            .forEach(entry -> transitAtomicUpdateGroupState(entry.getValue(), State.VISIBLE, State.OVERSHADOWED));
        findOvershadowedBy(aug, State.STANDBY)
            .forEach(entry -> transitAtomicUpdateGroupState(entry.getValue(), State.STANDBY, State.OVERSHADOWED));
        transitAtomicUpdateGroupState(aug, State.STANDBY, State.VISIBLE);
      }
    }
  }

  private void addAtomicUpdateGroupWithState(AtomicUpdateGroup<T> aug, State state)
  {
    final AtomicUpdateGroup<T> existing = getStateMap(state)
        .computeIfAbsent(RootPartitionRange.of(aug), k -> createMinorVersionToAugMap(state))
        .put(aug.getMinorVersion(), aug);

    if (existing != null) {
      throw new ISE("AtomicUpdateGroup[%s] is already in state[%s]", aug, state);
    }

    transitionStandbyGroupIfFull(aug, state);
  }

  public boolean addChunk(PartitionChunk<T> chunk)
  {
    // Sanity check. ExistingChunk should be usually null.
    final PartitionChunk<T> existingChunk = knownPartitionChunks.put(chunk.getChunkNumber(), chunk);
    if (existingChunk != null) {
      if (!existingChunk.equals(chunk)) {
        throw new ISE(
            "existingChunk[%s] is different from newChunk[%s] for partitionId[%d]",
            existingChunk,
            chunk,
            chunk.getChunkNumber()
        );
      } else {
        // A new chunk of the same major version and partitionId can be added in segment handoff
        // from stream ingestion tasks to historicals
        return false;
      }
    }

    // Find atomicUpdateGroup of the new chunk
    AtomicUpdateGroup<T> atomicUpdateGroup = findAtomicUpdateGroupWith(chunk, State.OVERSHADOWED);

    if (atomicUpdateGroup != null) {
      atomicUpdateGroup.add(chunk);
    } else {
      atomicUpdateGroup = findAtomicUpdateGroupWith(chunk, State.STANDBY);

      if (atomicUpdateGroup != null) {
        atomicUpdateGroup.add(chunk);
        transitionStandbyGroupIfFull(atomicUpdateGroup, State.STANDBY);
      } else {
        atomicUpdateGroup = findAtomicUpdateGroupWith(chunk, State.VISIBLE);

        if (atomicUpdateGroup != null) {
          atomicUpdateGroup.add(chunk);
        } else {
          final AtomicUpdateGroup<T> newAtomicUpdateGroup = new AtomicUpdateGroup<>(chunk);

          // Decide the initial state of the new atomicUpdateGroup
          final boolean overshadowed = visibleGroup
              .values()
              .stream()
              .flatMap(map -> map.values().stream())
              .anyMatch(group -> group.isOvershadow(newAtomicUpdateGroup));

          if (overshadowed) {
            addAtomicUpdateGroupWithState(newAtomicUpdateGroup, State.OVERSHADOWED);
          } else {
            addAtomicUpdateGroupWithState(newAtomicUpdateGroup, State.STANDBY);
          }
        }
      }
    }
    return true;
  }

  /**
   * Handles of removal of an empty atomicUpdateGroup from a state.
   */
  private void determineVisibleGroupAfterRemove(
      AtomicUpdateGroup<T> augOfRemovedChunk,
      RootPartitionRange rangeOfAug,
      short minorVersion,
      State stateOfRemovedAug
  )
  {
    // If an atomicUpdateGroup is overshadowed by another non-visible atomicUpdateGroup, there must be another visible
    // atomicUpdateGroup which also overshadows the same atomicUpdateGroup.
    // As a result, the state of overshadowed atomicUpdateGroup should be updated only when a visible atomicUpdateGroup
    // is removed.

    if (stateOfRemovedAug == State.VISIBLE) {
      // All segments in the visible atomicUpdateGroup which overshadows this atomicUpdateGroup is removed.
      // Fall back if there is a fully available overshadowed atomicUpdateGroup

      final List<AtomicUpdateGroup<T>> latestFullAugs = findLatestFullyAvailableOvershadowedAtomicUpdateGroup(
          rangeOfAug,
          minorVersion
      );

      // If there is no fully available fallback group, then the existing VISIBLE group remains VISIBLE.
      // Otherwise, the latest fully available group becomes VISIBLE.
      if (!latestFullAugs.isEmpty()) {
        // Move the atomicUpdateGroup to standby
        // and move the fully available overshadowed atomicUpdateGroup to visible
        if (!augOfRemovedChunk.isEmpty()) {
          transitAtomicUpdateGroupState(augOfRemovedChunk, State.VISIBLE, State.STANDBY);
        }
        latestFullAugs.forEach(group -> transitAtomicUpdateGroupState(group, State.OVERSHADOWED, State.VISIBLE));
      }
    }
  }

  private List<AtomicUpdateGroup<T>> findLatestFullyAvailableOvershadowedAtomicUpdateGroup(
      RootPartitionRange rangeOfAug,
      short minorVersion
  )
  {
    final List<Short2ObjectMap.Entry<AtomicUpdateGroup<T>>> overshadowedGroups = findOvershadowedBy(
        rangeOfAug,
        minorVersion,
        State.OVERSHADOWED
    );
    if (overshadowedGroups.isEmpty()) {
      return Collections.emptyList();
    }

    final OvershadowableManager<T> manager = new OvershadowableManager<>();
    for (Short2ObjectMap.Entry<AtomicUpdateGroup<T>> entry : overshadowedGroups) {
      for (PartitionChunk<T> chunk : entry.getValue().getChunks()) {
        manager.addChunk(chunk);
      }
    }

    final List<AtomicUpdateGroup<T>> visibles = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> map : manager.visibleGroup.values()) {
      visibles.addAll(map.values());
    }
    return visibles;
  }

  private void removeFrom(AtomicUpdateGroup<T> aug, State state)
  {
    final RootPartitionRange rangeKey = RootPartitionRange.of(aug);
    final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = getStateMap(state).get(rangeKey);
    if (versionToGroup == null) {
      throw new ISE("Unknown atomicUpdateGroup[%s] in state[%s]", aug, state);
    }

    final AtomicUpdateGroup<T> removed = versionToGroup.remove(aug.getMinorVersion());
    if (removed == null) {
      throw new ISE("Unknown atomicUpdateGroup[%s] in state[%s]", aug, state);
    }

    if (!removed.equals(aug)) {
      throw new ISE(
          "WTH? actually removed atomicUpdateGroup[%s] is different from the one which is supposed to be[%s]",
          removed,
          aug
      );
    }

    if (versionToGroup.isEmpty()) {
      getStateMap(state).remove(rangeKey);
    }
  }

  @Nullable
  public PartitionChunk<T> removeChunk(PartitionChunk<T> partitionChunk)
  {
    final PartitionChunk<T> knownChunk = knownPartitionChunks.get(partitionChunk.getChunkNumber());
    if (knownChunk == null) {
      return null;
    }

    if (!knownChunk.equals(partitionChunk)) {
      throw new ISE(
          "WTH? Same partitionId[%d], but known partition[%s] is different from the input partition[%s]",
          partitionChunk.getChunkNumber(),
          knownChunk,
          partitionChunk
      );
    }

    AtomicUpdateGroup<T> augOfRemovedChunk = tryRemoveChunkFromGroupWithState(partitionChunk, State.STANDBY);

    if (augOfRemovedChunk == null) {
      augOfRemovedChunk = tryRemoveChunkFromGroupWithState(partitionChunk, State.VISIBLE);
      if (augOfRemovedChunk == null) {
        augOfRemovedChunk = tryRemoveChunkFromGroupWithState(partitionChunk, State.OVERSHADOWED);
        if (augOfRemovedChunk == null) {
          throw new ISE("Can't find atomicUpdateGroup for partitionChunk[%s]", partitionChunk);
        }
      }
    }

    return knownPartitionChunks.remove(partitionChunk.getChunkNumber());
  }

  public boolean isEmpty()
  {
    return visibleGroup.isEmpty();
  }

  public boolean isComplete()
  {
    return visibleGroup.values().stream().allMatch(map -> Iterables.getOnlyElement(map.values()).isFull());
  }

  @Nullable
  public PartitionChunk<T> getChunk(int partitionId)
  {
    final PartitionChunk<T> chunk = knownPartitionChunks.get(partitionId);
    if (chunk == null) {
      return null;
    }
    final AtomicUpdateGroup<T> aug = findAtomicUpdateGroupWith(chunk, State.VISIBLE);
    if (aug == null) {
      return null;
    } else {
      return Preconditions.checkNotNull(
          aug.findChunk(partitionId),
          "Can't find partitionChunk for partitionId[%s] in atomicUpdateGroup[%s]",
          partitionId,
          aug
      );
    }
  }

  public List<PartitionChunk<T>> getVisibles()
  {
    final List<PartitionChunk<T>> visibles = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> treeMap : visibleGroup.values()) {
      for (AtomicUpdateGroup<T> aug : treeMap.values()) {
        visibles.addAll(aug.getChunks());
      }
    }
    return visibles;
  }

  public List<PartitionChunk<T>> getOvershadowed()
  {
    final List<PartitionChunk<T>> overshadowed = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> treeMap : overshadowedGroups.values()) {
      for (AtomicUpdateGroup<T> aug : treeMap.values()) {
        overshadowed.addAll(aug.getChunks());
      }
    }
    return overshadowed;
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
    OvershadowableManager<?> that = (OvershadowableManager<?>) o;
    return Objects.equals(knownPartitionChunks, that.knownPartitionChunks) &&
           Objects.equals(standbyGroups, that.standbyGroups) &&
           Objects.equals(visibleGroup, that.visibleGroup) &&
           Objects.equals(overshadowedGroups, that.overshadowedGroups);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(knownPartitionChunks, standbyGroups, visibleGroup, overshadowedGroups);
  }

  @Override
  public String toString()
  {
    return "OvershadowableManager{" +
           "knownPartitionChunks=" + knownPartitionChunks +
           ", standbyGroups=" + standbyGroups +
           ", visibleGroup=" + visibleGroup +
           ", overshadowedGroups=" + overshadowedGroups +
           '}';
  }

  private static class RootPartitionRange implements Comparable<RootPartitionRange>
  {
    private final short startPartitionId;
    private final short endPartitionId;

    private static <T extends Overshadowable<T>> RootPartitionRange of(PartitionChunk<T> chunk)
    {
      return of(chunk.getObject().getStartRootPartitionId(), chunk.getObject().getEndRootPartitionId());
    }

    private static <T extends Overshadowable<T>> RootPartitionRange of(AtomicUpdateGroup<T> aug)
    {
      return of(aug.getStartRootPartitionId(), aug.getEndRootPartitionId());
    }

    private static RootPartitionRange of(int startPartitionId, int endPartitionId)
    {
      return new RootPartitionRange((short) startPartitionId, (short) endPartitionId);
    }

    private RootPartitionRange(short startPartitionId, short endPartitionId)
    {
      this.startPartitionId = startPartitionId;
      this.endPartitionId = endPartitionId;
    }

    public boolean contains(RootPartitionRange that)
    {
      return this.startPartitionId <= that.startPartitionId && this.endPartitionId >= that.endPartitionId;
    }

    @Override
    public int compareTo(RootPartitionRange o)
    {
      if (startPartitionId != o.startPartitionId) {
        return Integer.compare(Short.toUnsignedInt(startPartitionId), Short.toUnsignedInt(o.startPartitionId));
      } else {
        return Integer.compare(Short.toUnsignedInt(endPartitionId), Short.toUnsignedInt(o.endPartitionId));
      }
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
      RootPartitionRange that = (RootPartitionRange) o;
      return startPartitionId == that.startPartitionId &&
             endPartitionId == that.endPartitionId;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(startPartitionId, endPartitionId);
    }

    @Override
    public String toString()
    {
      return "RootPartitionRange{" +
             "startPartitionId=" + startPartitionId +
             ", endPartitionId=" + endPartitionId +
             '}';
    }
  }

  /**
   * Map can store at most a single entry.
   * Comparing to{@link it.unimi.dsi.fastutil.shorts.Short2ObjectSortedMaps.Singleton}, it's different from the
   * perspective of that this class supports update.
   */
  private static class SingleEntryShort2ObjectSortedMap<V> extends AbstractShort2ObjectSortedMap<V>
  {
    private short key;
    private V val;

    private SingleEntryShort2ObjectSortedMap()
    {
      key = -1;
      val = null;
    }

    @Override
    public Short2ObjectSortedMap<V> subMap(short fromKey, short toKey)
    {
      if (fromKey <= key && toKey > key) {
        return this;
      } else {
        throw new IAE("fromKey: %s, toKey: %s, key: %s", fromKey, toKey, key);
      }
    }

    @Override
    public Short2ObjectSortedMap<V> headMap(short toKey)
    {
      if (toKey > key) {
        return this;
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Short2ObjectSortedMap<V> tailMap(short fromKey)
    {
      if (fromKey <= key) {
        return this;
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public short firstShortKey()
    {
      if (key < 0) {
        throw new NoSuchElementException();
      }
      return key;
    }

    @Override
    public short lastShortKey()
    {
      if (key < 0) {
        throw new NoSuchElementException();
      }
      return key;
    }

    @Override
    public ObjectSortedSet<Short2ObjectMap.Entry<V>> short2ObjectEntrySet()
    {
      return isEmpty() ? ObjectSortedSets.EMPTY_SET : ObjectSortedSets.singleton(new BasicEntry<>(key, val));
    }

    @Override
    public ShortSortedSet keySet()
    {
      return isEmpty() ? ShortSortedSets.EMPTY_SET : ShortSortedSets.singleton(key);
    }

    @Override
    public ObjectCollection<V> values()
    {
      return new AbstractObjectCollection<V>()
      {
        @Override
        public ObjectIterator<V> iterator()
        {
          return size() > 0 ? ObjectIterators.singleton(val) : ObjectIterators.emptyIterator();
        }

        @Override
        public int size()
        {
          return key < 0 ? 0 : 1;
        }
      };
    }

    @Override
    public V put(final short key, final V value)
    {
      if (isEmpty()) {
        this.key = key;
        this.val = value;
        return null;
      } else {
        if (this.key == key) {
          final V existing = this.val;
          this.val = value;
          return existing;
        } else {
          throw new ISE(
              "Can't add [%d, %s] to non-empty SingleEntryShort2ObjectSortedMap[%d, %s]",
              key,
              value,
              this.key,
              this.val
          );
        }
      }
    }

    @Override
    public V get(short key)
    {
      return this.key == key ? val : null;
    }

    @Override
    public V remove(final short key)
    {
      if (this.key == key) {
        this.key = -1;
        return val;
      } else {
        return null;
      }
    }

    @Override
    public boolean containsKey(short key)
    {
      return this.key == key;
    }

    @Override
    public ShortComparator comparator()
    {
      return ShortComparators.NATURAL_COMPARATOR;
    }

    @Override
    public int size()
    {
      return key < 0 ? 0 : 1;
    }

    @Override
    public void defaultReturnValue(V rv)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public V defaultReturnValue()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty()
    {
      return key < 0;
    }

    @Override
    public boolean containsValue(Object value)
    {
      if (key < 0) {
        return false;
      } else {
        return Objects.equals(val, value);
      }
    }

    @Override
    public void putAll(Map<? extends Short, ? extends V> m)
    {
      if (!m.isEmpty()) {
        if (m.size() == 1) {
          final Map.Entry<? extends Short, ? extends V> entry = m.entrySet().iterator().next();
          this.key = entry.getKey();
          this.val = entry.getValue();
        } else {
          throw new IllegalArgumentException();
        }
      }
    }
  }
}
