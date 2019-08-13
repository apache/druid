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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * OvershadowableManager manages the state of {@link AtomicUpdateGroup}. See the below {@link State} for details.
 * Note that an AtomicUpdateGroup can consist of {@link Overshadowable}s of the same majorVersion, minorVersion,
 * rootPartition range, and atomicUpdateGroupSize.
 * In {@link org.apache.druid.timeline.VersionedIntervalTimeline}, this class is used to manage segments in the same
 * timeChunk.
 *
 * This class is not thread-safe.
 */
class OvershadowableManager<T extends Overshadowable<T>>
{
  /**
   * There are 3 states for atomicUpdateGroups.
   * There could be at most one visible atomicUpdateGroup at any time in a non-empty overshadowableManager.
   *
   * - Visible: fully available atomicUpdateGroup of the highest version if any.
   *            If there's no fully available atomicUpdateGroup, the standby atomicUpdateGroup of the highest version
   *            becomes visible.
   * - Standby: all atomicUpdateGroups of higher versions than that of the visible atomicUpdateGroup.
   * - Overshadowed: all atomicUpdateGroups of lower versions than that of the visible atomicUpdateGroup.
   */
  @VisibleForTesting
  enum State
  {
    STANDBY,
    VISIBLE,
    OVERSHADOWED
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

  private OvershadowableManager(List<AtomicUpdateGroup<T>> groups)
  {
    this();
    for (AtomicUpdateGroup<T> entry : groups) {
      for (PartitionChunk<T> chunk : entry.getChunks()) {
        addChunk(chunk);
      }
    }
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
    addAtomicUpdateGroupWithState(atomicUpdateGroup, to, false);
  }

  /**
   * Replace the oldVisibleGroups with the newVisibleGroups.
   * This method first removes the oldVisibleGroups from the visibles map,
   * moves the newVisibleGroups from its old state map to the visibles map,
   * and finally add the oldVisibleGroups to its new state map.
   */
  private void replaceVisibleWith(
      Collection<AtomicUpdateGroup<T>> oldVisibleGroups,
      State newStateOfOldVisibleGroup,
      List<AtomicUpdateGroup<T>> newVisibleGroups,
      State oldStateOfNewVisibleGroups
  )
  {
    oldVisibleGroups.forEach(
        group -> {
          if (!group.isEmpty()) {
            removeFrom(group, State.VISIBLE);
          }
        }
    );
    newVisibleGroups.forEach(
        entry -> transitAtomicUpdateGroupState(entry, oldStateOfNewVisibleGroups, State.VISIBLE)
    );
    oldVisibleGroups.forEach(
        group -> {
          if (!group.isEmpty()) {
            addAtomicUpdateGroupWithState(group, newStateOfOldVisibleGroup, false);
          }
        }
    );
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

  private List<AtomicUpdateGroup<T>> findOvershadowedBy(
      AtomicUpdateGroup<T> aug,
      State fromState
  )
  {
    final RootPartitionRange rangeKeyOfGivenAug = RootPartitionRange.of(aug);
    return findOvershadowedBy(rangeKeyOfGivenAug, aug.getMinorVersion(), fromState);
  }

  /**
   * Find all atomicUpdateGroups of the given state overshadowed by the minorVersion in the given rootPartitionRange.
   * The atomicUpdateGroup of a higher minorVersion can have a wider RootPartitionRange.
   * To find all atomicUpdateGroups overshadowed by the given rootPartitionRange and minorVersion,
   * we first need to find the first key contained by the given rootPartitionRange.
   * Once we find such key, then we go through the entire map until we see an atomicUpdateGroup of which
   * rootRangePartition is not contained by the given rootPartitionRange.
   *
   * @param rangeOfAug   the partition range to search for overshadowed groups.
   * @param minorVersion the minor version to check overshadow relation. The found groups will have lower minor versions
   *                     than this.
   * @param fromState    the state to search for overshadowed groups.
   *
   * @return a list of found atomicUpdateGroups. It could be empty if no groups are found.
   */
  @VisibleForTesting
  List<AtomicUpdateGroup<T>> findOvershadowedBy(RootPartitionRange rangeOfAug, short minorVersion, State fromState)
  {
    final TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> stateMap = getStateMap(fromState);
    Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> current = findLowestOverlappingEntry(
        rangeOfAug,
        stateMap,
        true
    );

    if (current == null) {
      return Collections.emptyList();
    }

    // Going through the map to find all entries of the RootPartitionRange contained by the given rangeOfAug.
    // Note that RootPartitionRange of entries are always consecutive.
    final List<AtomicUpdateGroup<T>> found = new ArrayList<>();
    while (current != null && rangeOfAug.overlaps(current.getKey())) {
      if (rangeOfAug.contains(current.getKey())) {
        // versionToGroup is sorted by minorVersion.
        // versionToGroup.headMap(minorVersion) below returns a map containing all entries of lower minorVersions
        // than the given minorVersion.
        final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = current.getValue();
        // Short2ObjectRBTreeMap.SubMap.short2ObjectEntrySet() implementation, especially size(), is not optimized.
        // Note that size() is indirectly called in ArrayList.addAll() when ObjectSortedSet.toArray() is called.
        // See AbstractObjectCollection.toArray().
        // If you see performance degradation here, probably we need to improve the below line.
        if (versionToGroup.firstShortKey() < minorVersion) {
          found.addAll(versionToGroup.headMap(minorVersion).values());
        }
      }
      current = stateMap.higherEntry(current.getKey());
    }
    return found;
  }

  private List<AtomicUpdateGroup<T>> findOvershadows(AtomicUpdateGroup<T> aug, State fromState)
  {
    return findOvershadows(RootPartitionRange.of(aug), aug.getMinorVersion(), fromState);
  }

  /**
   * Find all atomicUpdateGroups which overshadow others of the given minorVersion in the given rootPartitionRange.
   * Similar to {@link #findOvershadowedBy}.
   *
   * Note that one atomicUpdateGroup can overshadow multiple other groups. If you're finding overshadowing
   * atomicUpdateGroups by calling this method in a loop, the results of this method can contain duplicate groups.
   *
   * @param rangeOfAug   the partition range to search for overshadowing groups.
   * @param minorVersion the minor version to check overshadow relation. The found groups will have higher minor
   *                     versions than this.
   * @param fromState    the state to search for overshadowed groups.
   *
   * @return a list of found atomicUpdateGroups. It could be empty if no groups are found.
   */
  @VisibleForTesting
  List<AtomicUpdateGroup<T>> findOvershadows(RootPartitionRange rangeOfAug, short minorVersion, State fromState)
  {
    final TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> stateMap = getStateMap(fromState);
    Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> current = findLowestOverlappingEntry(
        rangeOfAug,
        stateMap,
        false
    );

    if (current == null) {
      return Collections.emptyList();
    }

    // Going through the map to find all entries of the RootPartitionRange contains the given rangeOfAug.
    // Note that RootPartitionRange of entries are always consecutive.
    final List<AtomicUpdateGroup<T>> found = new ArrayList<>();
    while (current != null && current.getKey().overlaps(rangeOfAug)) {
      if (current.getKey().contains(rangeOfAug)) {
        // versionToGroup is sorted by minorVersion.
        // versionToGroup.tailMap(minorVersion) below returns a map containing all entries of equal to or higher
        // minorVersions than the given minorVersion.
        final Short2ObjectSortedMap<AtomicUpdateGroup<T>> versionToGroup = current.getValue();
        // Short2ObjectRBTreeMap.SubMap.short2ObjectEntrySet() implementation, especially size(), is not optimized.
        // Note that size() is indirectly called in ArrayList.addAll() when ObjectSortedSet.toArray() is called.
        // See AbstractObjectCollection.toArray().
        // If you see performance degradation here, probably we need to improve the below line.
        if (versionToGroup.lastShortKey() > minorVersion) {
          found.addAll(versionToGroup.tailMap(minorVersion).values());
        }
      }
      current = stateMap.higherEntry(current.getKey());
    }
    return found;
  }

  /**
   * Finds the lowest entry overlapping with the given root partition range.
   * It first searches the entries lower than or equal to the given range.
   * If there's no such entry lower than the given range, then it searches the entries higher than the given range.
   *
   * @return an entry of the lowest key overlapping with the given range. Otherwise null.
   */
  @Nullable
  private Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> findLowestOverlappingEntry(
      RootPartitionRange rangeOfAug,
      TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> stateMap,
      boolean strictSameStartId
  )
  {
    // Searches the entries lower than or equal to the given range.
    Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> current = stateMap.floorEntry(rangeOfAug);

    if (current == null) {
      // Searches the entries higher than then given range.
      current = stateMap.higherEntry(rangeOfAug);
    }

    if (current == null) {
      return null;
    }

    // floorEntry() can return the greatest key less than rangeOfAug. We need to skip non-overlapping keys.
    while (current != null && !current.getKey().overlaps(rangeOfAug)) {
      current = stateMap.higherEntry(current.getKey());
    }

    final BiPredicate<RootPartitionRange, RootPartitionRange> predicate;
    if (strictSameStartId) {
      predicate = (entryRange, groupRange) -> entryRange.startPartitionId == groupRange.startPartitionId;
    } else {
      predicate = RootPartitionRange::overlaps;
    }

    // There could be multiple entries of the same startPartitionId but different endPartitionId.
    // Find the first key of the same startPartitionId which has the lowest endPartitionId.
    while (current != null) {
      final Entry<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> lowerEntry = stateMap.lowerEntry(
          current.getKey()
      );
      if (lowerEntry != null && predicate.test(lowerEntry.getKey(), rangeOfAug)) {
        current = lowerEntry;
      } else {
        break;
      }
    }

    return current;
  }

  /**
   * Determine the visible group after a new chunk is added.
   */
  private void determineVisibleGroupAfterAdd(AtomicUpdateGroup<T> aug, State stateOfAug)
  {
    if (stateOfAug == State.STANDBY) {
      moveNewStandbyToVisibleIfNecessary(aug, stateOfAug);
    } else if (stateOfAug == State.OVERSHADOWED) {
      checkVisibleIsFullyAvailableAndTryToMoveOvershadowedToVisible(aug, stateOfAug);
    }
  }

  /**
   * This method is called in {@link #determineVisibleGroupAfterAdd}.
   * The given standby group can be visible in the below two cases:
   *
   * - The standby group is full. Since every standby group has a higher version than the current visible group,
   *   it should become visible immediately when it's full.
   * - The standby group is not full but not empty and the current visible is not full. If there's no fully available
   *   group, the group of the highest version should be the visible.
   */
  private void moveNewStandbyToVisibleIfNecessary(AtomicUpdateGroup<T> standbyGroup, State stateOfGroup)
  {
    assert stateOfGroup == State.STANDBY;

    // A standby atomicUpdateGroup becomes visible when its all segments are available.
    if (standbyGroup.isFull()) {
      // A current visible atomicUpdateGroup becomes overshadowed when a fully available standby atomicUpdateGroup
      // becomes visible.
      replaceVisibleWith(
          findOvershadowedBy(standbyGroup, State.VISIBLE),
          State.OVERSHADOWED,
          Collections.singletonList(standbyGroup),
          State.STANDBY
      );
      findOvershadowedBy(standbyGroup, State.STANDBY)
          .forEach(entry -> transitAtomicUpdateGroupState(entry, State.STANDBY, State.OVERSHADOWED));
    } else {
      // The given atomicUpdateGroup is in the standby state which means it's not overshadowed by the visible group.
      // If the visible group is not fully available, then the new standby group should be visible since it has a
      // higher minor version.
      if (!standbyGroup.isEmpty()) {
        // Check there are visible atomicUpdateGroups overshadowed by the given atomicUpdateGroup.
        final List<AtomicUpdateGroup<T>> overshadowedVisibles = findOvershadowedBy(
            standbyGroup,
            State.VISIBLE
        );
        if (overshadowedVisibles.isEmpty()) {
          // There is no visible atomicUpdateGroup for the rootPartitionRange of the given aug.
          // The given aug should be visible.
          transitAtomicUpdateGroupState(standbyGroup, State.STANDBY, State.VISIBLE);
          findOvershadowedBy(standbyGroup, State.STANDBY)
              .forEach(entry -> transitAtomicUpdateGroupState(entry, State.STANDBY, State.OVERSHADOWED));
        } else {
          // Check there is any missing chunk in the current visible groups.
          // If the current visible groups don't cover the partitino range of the given standby group,
          // the given standby group should be visible.
          final boolean fullyCoverAugRange = doGroupsFullyCoverPartitionRange(
              overshadowedVisibles,
              standbyGroup.getStartRootPartitionId(),
              standbyGroup.getEndRootPartitionId()
          );
          if (!fullyCoverAugRange) {
            replaceVisibleWith(
                overshadowedVisibles,
                State.OVERSHADOWED,
                Collections.singletonList(standbyGroup),
                State.STANDBY
            );
            findOvershadowedBy(standbyGroup, State.STANDBY)
                .forEach(entry -> transitAtomicUpdateGroupState(entry, State.STANDBY, State.OVERSHADOWED));

          }
          // If all visible atomicUpdateGroups are full, then the given atomicUpdateGroup should stay in the standby
          // state.
        }
      }
    }
  }

  /**
   * This method is called in {@link #determineVisibleGroupAfterAdd}. It first checks the current visible group is
   * fully available. If not, it checks there are overshadowed groups which can cover the rootPartitionRange of
   * the visible groups and are fully available. If it finds such groups, they become visible.
   */
  private void checkVisibleIsFullyAvailableAndTryToMoveOvershadowedToVisible(
      AtomicUpdateGroup<T> group,
      State stateOfGroup
  )
  {
    assert stateOfGroup == State.OVERSHADOWED;
    if (group.isFull()) {
      // Since this atomicUpdateGroup is full, it could be changed to visible if the current visible group is not
      // fully available. To check this, we first check the current visible is fully available.
      // And if not, we check the overshadowed groups are fully available and can cover the partition range of
      // the atomicUpdateGroups overshadow the given overshadowed group.

      // Visible or standby groups overshadowing the given group.
      // Used to both 1) check fully available visible group and
      // 2) get the partition range which the fully available overshadowed groups should cover to become visible.
      final List<AtomicUpdateGroup<T>> groupsOvershadowingAug;
      final boolean isOvershadowingGroupsFull;

      final List<AtomicUpdateGroup<T>> overshadowingVisibles = findOvershadows(group, State.VISIBLE);
      if (overshadowingVisibles.isEmpty()) {
        final List<AtomicUpdateGroup<T>> overshadowingStandbys = findLatestNonFullyAvailableAtomicUpdateGroups(
            findOvershadows(group, State.STANDBY)
        );
        if (overshadowingStandbys.isEmpty()) {
          throw new ISE("WTH? atomicUpdateGroup[%s] is in overshadowed state, but no one overshadows it?", group);
        }
        groupsOvershadowingAug = overshadowingStandbys;
        isOvershadowingGroupsFull = false;
      } else {
        groupsOvershadowingAug = overshadowingVisibles;
        isOvershadowingGroupsFull = doGroupsFullyCoverPartitionRange(
            groupsOvershadowingAug,
            groupsOvershadowingAug.get(0).getStartRootPartitionId(),
            groupsOvershadowingAug.get(groupsOvershadowingAug.size() - 1).getEndRootPartitionId()
        );
      }

      // If groupsOvershadowingAug is the standby groups, isOvershadowingGroupsFull is always false.
      // If groupsOvershadowingAug is the visible groups, isOvershadowingGroupsFull indicates the visible group is
      // fully available or not.
      if (!isOvershadowingGroupsFull) {
        // Let's check the overshadowed groups can cover the partition range of groupsOvershadowingAug
        // and are fully available.
        final List<AtomicUpdateGroup<T>> latestFullGroups = groupsOvershadowingAug
            .stream()
            .flatMap(eachFullgroup -> findLatestFullyAvailableOvershadowedAtomicUpdateGroups(
                RootPartitionRange.of(eachFullgroup),
                eachFullgroup.getMinorVersion()).stream()
            )
            .collect(Collectors.toList());

        if (!latestFullGroups.isEmpty()) {
          final boolean isOvershadowedGroupsFull = doGroupsFullyCoverPartitionRange(
              latestFullGroups,
              groupsOvershadowingAug.get(0).getStartRootPartitionId(),
              groupsOvershadowingAug.get(groupsOvershadowingAug.size() - 1).getEndRootPartitionId()
          );

          if (isOvershadowedGroupsFull) {
            replaceVisibleWith(overshadowingVisibles, State.STANDBY, latestFullGroups, State.OVERSHADOWED);
          }
        }
      }
    }
  }

  /**
   * Checks if the given groups fully cover the given partition range. To fully cover the range, the given groups
   * should satisfy the below:
   *
   * - All groups must be full.
   * - All groups must be adjacent.
   * - The lowest startPartitionId and the highest endPartitionId must be same with the given startPartitionId and
   *   the given endPartitionId, respectively.
   *
   * @param groups               atomicUpdateGroups sorted by their rootPartitionRange
   * @param startRootPartitionId the start partitionId of the root partition range to check the coverage
   * @param endRootPartitionId   the end partitionId of the root partition range to check the coverage
   *
   * @return true if the given groups fully cover the given partition range.
   */
  private boolean doGroupsFullyCoverPartitionRange(
      List<AtomicUpdateGroup<T>> groups,
      int startRootPartitionId,
      int endRootPartitionId
  )
  {
    final int startRootPartitionIdOfOvershadowed = groups.get(0).getStartRootPartitionId();
    final int endRootPartitionIdOfOvershadowed = groups.get(groups.size() - 1).getEndRootPartitionId();
    if (startRootPartitionId != startRootPartitionIdOfOvershadowed
        || endRootPartitionId != endRootPartitionIdOfOvershadowed) {
      return false;
    } else {
      int prevEndPartitionId = groups.get(0).getStartRootPartitionId();
      for (AtomicUpdateGroup<T> group : groups) {
        if (!group.isFull() || prevEndPartitionId != group.getStartRootPartitionId()) {
          // If any visible atomicUpdateGroup overshadowed by the given standby atomicUpdateGroup is not full,
          // then the given atomicUpdateGroup should be visible since it has a higher version.
          return false;
        }
        prevEndPartitionId = group.getEndRootPartitionId();
      }
    }
    return true;
  }

  private void addAtomicUpdateGroupWithState(AtomicUpdateGroup<T> aug, State state, boolean determineVisible)
  {
    final AtomicUpdateGroup<T> existing = getStateMap(state)
        .computeIfAbsent(RootPartitionRange.of(aug), k -> createMinorVersionToAugMap(state))
        .put(aug.getMinorVersion(), aug);

    if (existing != null) {
      throw new ISE("AtomicUpdateGroup[%s] is already in state[%s]", existing, state);
    }

    if (determineVisible) {
      determineVisibleGroupAfterAdd(aug, state);
    }
  }

  boolean addChunk(PartitionChunk<T> chunk)
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
      // If overshadowed atomicUpdateGroup is full and visible atomicUpdateGroup is not full,
      // move overshadowed one to visible.
      determineVisibleGroupAfterAdd(atomicUpdateGroup, State.OVERSHADOWED);
    } else {
      atomicUpdateGroup = findAtomicUpdateGroupWith(chunk, State.STANDBY);

      if (atomicUpdateGroup != null) {
        atomicUpdateGroup.add(chunk);
        determineVisibleGroupAfterAdd(atomicUpdateGroup, State.STANDBY);
      } else {
        atomicUpdateGroup = findAtomicUpdateGroupWith(chunk, State.VISIBLE);

        if (atomicUpdateGroup != null) {
          if (atomicUpdateGroup.findChunk(chunk.getChunkNumber()) == null) {
            // If this chunk is not in the atomicUpdateGroup, then we add the chunk to it if it's not full.
            if (!atomicUpdateGroup.isFull()) {
              atomicUpdateGroup.add(chunk);
            } else {
              throw new ISE("Can't add chunk[%s] to a full atomicUpdateGroup[%s]", chunk, atomicUpdateGroup);
            }
          } else {
            // If this chunk is already in the atomicUpdateGroup, it should be in knownPartitionChunks
            // and this code must not be executed.
            throw new ISE(
                "WTH? chunk[%s] is in the atomicUpdateGroup[%s] but not in knownPartitionChunks[%s]?",
                chunk,
                atomicUpdateGroup,
                knownPartitionChunks
            );
          }
        } else {
          final AtomicUpdateGroup<T> newAtomicUpdateGroup = new AtomicUpdateGroup<>(chunk);

          // Decide the initial state of the new atomicUpdateGroup
          final boolean overshadowed = visibleGroup
              .values()
              .stream()
              .flatMap(map -> map.values().stream())
              .anyMatch(group -> group.overshadows(newAtomicUpdateGroup));

          if (overshadowed) {
            addAtomicUpdateGroupWithState(newAtomicUpdateGroup, State.OVERSHADOWED, true);
          } else {
            addAtomicUpdateGroupWithState(newAtomicUpdateGroup, State.STANDBY, true);
          }
        }
      }
    }
    return true;
  }

  /**
   * Handles the removal of an empty atomicUpdateGroup from a state.
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
      // A chunk is removed from the current visible group.
      // Fall back to
      //   1) the latest fully available overshadowed group if any
      //   2) the latest standby group if any
      //   3) the latest overshadowed group if any

      // Check there is a fully available latest overshadowed atomicUpdateGroup.
      final List<AtomicUpdateGroup<T>> latestFullAugs = findLatestFullyAvailableOvershadowedAtomicUpdateGroups(
          rangeOfAug,
          minorVersion
      );

      // If there are fully available overshadowed groups, then the latest one becomes visible.
      if (!latestFullAugs.isEmpty()) {
        // The current visible atomicUpdateGroup becomes standby
        // and the fully available overshadowed atomicUpdateGroups become visible
        final Set<AtomicUpdateGroup<T>> overshadowsLatestFullAugsInVisible = latestFullAugs
            .stream()
            .flatMap(group -> findOvershadows(group, State.VISIBLE).stream())
            .collect(Collectors.toSet());
        replaceVisibleWith(
            overshadowsLatestFullAugsInVisible,
            State.STANDBY,
            latestFullAugs,
            State.OVERSHADOWED
        );
        latestFullAugs
            .stream()
            .flatMap(group -> findOvershadows(group, State.OVERSHADOWED).stream())
            .collect(Collectors.toSet())
            .forEach(group -> transitAtomicUpdateGroupState(group, State.OVERSHADOWED, State.STANDBY));
      } else {
        // Find the latest non-fully available atomicUpdateGroups
        final List<AtomicUpdateGroup<T>> latestStandby = findLatestNonFullyAvailableAtomicUpdateGroups(
            findOvershadows(rangeOfAug, minorVersion, State.STANDBY)
        );
        if (!latestStandby.isEmpty()) {
          final List<AtomicUpdateGroup<T>> overshadowedByLatestStandby = latestStandby
              .stream()
              .flatMap(group -> findOvershadowedBy(group, State.VISIBLE).stream())
              .collect(Collectors.toList());
          replaceVisibleWith(overshadowedByLatestStandby, State.OVERSHADOWED, latestStandby, State.STANDBY);

          // All standby groups overshadowed by the new visible group should be moved to overshadowed
          latestStandby
              .stream()
              .flatMap(group -> findOvershadowedBy(group, State.STANDBY).stream())
              .collect(Collectors.toSet())
              .forEach(aug -> transitAtomicUpdateGroupState(aug, State.STANDBY, State.OVERSHADOWED));
        } else if (augOfRemovedChunk.isEmpty()) {
          // Visible is empty. Move the latest overshadowed to visible.
          final List<AtomicUpdateGroup<T>> latestOvershadowed = findLatestNonFullyAvailableAtomicUpdateGroups(
              findOvershadowedBy(rangeOfAug, minorVersion, State.OVERSHADOWED)
          );
          if (!latestOvershadowed.isEmpty()) {
            latestOvershadowed.forEach(aug -> transitAtomicUpdateGroupState(aug, State.OVERSHADOWED, State.VISIBLE));
          }
        }
      }
    }
  }

  /**
   * Find the latest NON-FULLY available atomicUpdateGroups from the given groups.
   *
   * This method MUST be called only when there is no fully available ones in the given groups. If the given groups
   * are in the overshadowed state, calls {@link #findLatestFullyAvailableOvershadowedAtomicUpdateGroups} first
   * to check there is any fully available group.
   * If the given groups are in the standby state, you can freely call this method because there should be no fully
   * available one in the standby groups at any time.
   */
  private List<AtomicUpdateGroup<T>> findLatestNonFullyAvailableAtomicUpdateGroups(List<AtomicUpdateGroup<T>> groups)
  {
    if (groups.isEmpty()) {
      return Collections.emptyList();
    }

    final OvershadowableManager<T> manager = new OvershadowableManager<>(groups);
    if (!manager.standbyGroups.isEmpty()) {
      throw new ISE("This method should be called only when there is no fully available group in the given state.");
    }

    final List<AtomicUpdateGroup<T>> visibles = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> map : manager.visibleGroup.values()) {
      visibles.addAll(map.values());
    }
    return visibles;
  }

  private List<AtomicUpdateGroup<T>> findLatestFullyAvailableOvershadowedAtomicUpdateGroups(
      RootPartitionRange rangeOfAug,
      short minorVersion
  )
  {
    final List<AtomicUpdateGroup<T>> overshadowedGroups = findOvershadowedBy(
        rangeOfAug,
        minorVersion,
        State.OVERSHADOWED
    );
    if (overshadowedGroups.isEmpty()) {
      return Collections.emptyList();
    }

    final OvershadowableManager<T> manager = new OvershadowableManager<>(overshadowedGroups);
    final List<AtomicUpdateGroup<T>> visibles = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> map : manager.visibleGroup.values()) {
      for (AtomicUpdateGroup<T> atomicUpdateGroup : map.values()) {
        if (!atomicUpdateGroup.isFull()) {
          return Collections.emptyList();
        }
        visibles.add(atomicUpdateGroup);
      }
    }
    final RootPartitionRange foundRange = RootPartitionRange.of(
        visibles.get(0).getStartRootPartitionId(),
        visibles.get(visibles.size() - 1).getEndRootPartitionId()
    );
    if (!rangeOfAug.equals(foundRange)) {
      return Collections.emptyList();
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
  PartitionChunk<T> removeChunk(PartitionChunk<T> partitionChunk)
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
  PartitionChunk<T> getChunk(int partitionId)
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

  List<PartitionChunk<T>> getVisibleChunks()
  {
    return getAllChunks(visibleGroup);
  }

  List<PartitionChunk<T>> getOvershadowedChunks()
  {
    return getAllChunks(overshadowedGroups);
  }

  @VisibleForTesting
  List<PartitionChunk<T>> getStandbyChunks()
  {
    return getAllChunks(standbyGroups);
  }

  private List<PartitionChunk<T>> getAllChunks(
      TreeMap<RootPartitionRange, Short2ObjectSortedMap<AtomicUpdateGroup<T>>> stateMap
  )
  {
    final List<PartitionChunk<T>> allChunks = new ArrayList<>();
    for (Short2ObjectSortedMap<AtomicUpdateGroup<T>> treeMap : stateMap.values()) {
      for (AtomicUpdateGroup<T> aug : treeMap.values()) {
        allChunks.addAll(aug.getChunks());
      }
    }
    return allChunks;
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

  @VisibleForTesting
  static class RootPartitionRange implements Comparable<RootPartitionRange>
  {
    private final short startPartitionId;
    private final short endPartitionId;

    @VisibleForTesting
    static RootPartitionRange of(int startPartitionId, int endPartitionId)
    {
      return new RootPartitionRange((short) startPartitionId, (short) endPartitionId);
    }

    private static <T extends Overshadowable<T>> RootPartitionRange of(PartitionChunk<T> chunk)
    {
      return of(chunk.getObject().getStartRootPartitionId(), chunk.getObject().getEndRootPartitionId());
    }

    private static <T extends Overshadowable<T>> RootPartitionRange of(AtomicUpdateGroup<T> aug)
    {
      return of(aug.getStartRootPartitionId(), aug.getEndRootPartitionId());
    }

    private RootPartitionRange(short startPartitionId, short endPartitionId)
    {
      this.startPartitionId = startPartitionId;
      this.endPartitionId = endPartitionId;
    }

    public boolean contains(RootPartitionRange that)
    {
      return Short.toUnsignedInt(startPartitionId) <= Short.toUnsignedInt(that.startPartitionId)
             && Short.toUnsignedInt(this.endPartitionId) >= Short.toUnsignedInt(that.endPartitionId);
    }

    public boolean overlaps(RootPartitionRange that)
    {
      return Short.toUnsignedInt(startPartitionId) <= Short.toUnsignedInt(that.startPartitionId)
          && Short.toUnsignedInt(endPartitionId) > Short.toUnsignedInt(that.startPartitionId)
          || Short.toUnsignedInt(startPartitionId) >= Short.toUnsignedInt(that.startPartitionId)
          && Short.toUnsignedInt(startPartitionId) < Short.toUnsignedInt(that.endPartitionId);
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
        throw new IAE("toKey: %s, key: %s", toKey, key);
      }
    }

    @Override
    public Short2ObjectSortedMap<V> tailMap(short fromKey)
    {
      if (fromKey <= key) {
        return this;
      } else {
        throw new IAE("fromKey: %s, key: %s", fromKey, key);
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
