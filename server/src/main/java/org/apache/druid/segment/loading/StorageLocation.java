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

package org.apache.druid.segment.loading;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Logical representation of a local disk path to store {@link CacheEntry}, controlling that the total size of stored
 * files doesn't exceed the {@link #maxSizeBytes} and available space is always kept smaller than
 * {@link #freeSpaceToKeep}.
 * <p>
 * {@link CacheEntry} can be stored in two manners in a storage location. The first is to store the entry indefinitely
 * with {@link #reserve(CacheEntry)}, where the space of the entry is accounted for and the storage space will not be
 * recovered until {@link #release(CacheEntry)} is called. These entries are stored in {@link #staticCacheEntries}.
 * <p>
 * The second way is to store as a transient cache item with one of {@link #reserveWeak(CacheEntry)},
 * {@link #addWeakReservation(CacheEntry)}, or {@link #addWeakReservationIfExists(CacheEntry)}. {@link CacheEntry}
 * stored in this manner will exist on disk in this location until the point that another new reservation needs more
 * space than remains available in the location, at which point {@link #reclaim(long)} will be called to try to call
 * {@link CacheEntry#unmount()} on any eligible entries until enough space is available to store the new item.
 *
 * Items are chosen for eviction using an algorithm based on
 * (<a href="https://junchengyang.com/publication/nsdi24-SIEVE.pdf">SIEVE</a>) with additional mechanisms to place
 * temporary holds on cache entries. This holds required to support cases such as if a group of them are taking part
 * in a query and must all be loaded simultaneously.
 *
 * Implementation-wise, {@link CacheEntry} are wrapped in {@link WeakCacheEntry} to form a doubly linked list
 * functioning as a queue which can be interacted with via 3 fields, {@link #head}, {@link #tail}, and {@link #hand}.
 * Head and tail expectedly mark the beginning and end of the queue, while the hand is the interesting bit and is used
 * as the position of the current item to consider for eviction the next time {@link #reclaim(long)} needs to be called.
 * Entries are also stored in a map, {@link #weakCacheEntries}, for fast retrieval. Using a weak cache entry sets
 * {@link WeakCacheEntry#visited} to true, and {@link #addWeakReservation(CacheEntry)} and
 * {@link #addWeakReservationIfExists(CacheEntry)} additionally will increment {@link WeakCacheEntry#holds} until
 * {@link #finishWeakReservationHold(Iterable)} is called.
 * <p>
 * When it is time to reclaim space, first the {@link #hand} is checked for holds - if any exist then the hand is moved
 * to {@link WeakCacheEntry#prev} immediately and we try again. If no holds are present, then it is checked if it has
 * been marked as {@link WeakCacheEntry#visited} - if so then it is unmarked as visited, and the hand moves to
 * {@link WeakCacheEntry#prev} (allowing this entry to be reclaimed the next time we pass if it has not been visited
 * again). Lastly, if neither under a hold or marked, the entry will be unlinked from the queue AND unmounted from the
 * storage location (deleting the files from disk) with {@link #unlinkAndUnmountWeakEntry(WeakCacheEntry)}. This process
 * is repeated until either a sufficient amount of space has been reclaimed, or no additional space is able to be
 * reclaimed, in which case the new reservation fails.
 * <p>
 * This class is thread-safe, so that multiple threads can update its state at the same time.
 * One example usage is that a historical can use multiple threads to load different segments in parallel
 * from deep storage.
 */
public class StorageLocation
{
  private static final EmittingLogger log = new EmittingLogger(StorageLocation.class);

  private final File path;
  private final long maxSizeBytes;
  private final long freeSpaceToKeep;

  @GuardedBy("this")
  private final Map<CacheEntryIdentifier, CacheEntry> staticCacheEntries = new HashMap<>();

  @GuardedBy("this")
  private final Map<CacheEntryIdentifier, WeakCacheEntry> weakCacheEntries = new HashMap<>();

  @GuardedBy("this")
  private WeakCacheEntry head;
  @GuardedBy("this")
  private WeakCacheEntry tail;
  @GuardedBy("this")
  private WeakCacheEntry hand;
  /**
   * Current total size of files in bytes, including weak entries.
   */
  @GuardedBy("this")
  private long currSizeBytes = 0;

  @GuardedBy("this")
  private long currWeakSizeBytes = 0;

  public StorageLocation(File path, long maxSizeBytes, @Nullable Double freeSpacePercent)
  {
    this.path = path;
    this.maxSizeBytes = maxSizeBytes;

    if (freeSpacePercent != null) {
      long totalSpaceInPartition = path.getTotalSpace();
      this.freeSpaceToKeep = (long) ((freeSpacePercent * totalSpaceInPartition) / 100);
      log.info(
          "SegmentLocation[%s] will try and maintain [%d:%d] free space while loading segments.",
          path,
          freeSpaceToKeep,
          totalSpaceInPartition
      );
    } else {
      this.freeSpaceToKeep = 0;
    }
  }

  public File getPath()
  {
    return path;
  }

  public synchronized <T extends CacheEntry> T getCacheEntry(CacheEntryIdentifier entryId)
  {
    if (staticCacheEntries.containsKey(entryId)) {
      return (T) staticCacheEntries.get(entryId);
    }
    if (weakCacheEntries.containsKey(entryId)) {
      final WeakCacheEntry weakCacheEntry = weakCacheEntries.get(entryId);
      weakCacheEntry.visited = true;
      return (T) weakCacheEntry.cacheEntry;
    }
    return null;
  }

  public synchronized boolean isReserved(CacheEntry entry)
  {
    return staticCacheEntries.containsKey(entry.getId());
  }

  public synchronized boolean isWeakReserved(CacheEntry entry)
  {
    return weakCacheEntries.containsKey(entry.getId());
  }

  /**
   * Reserves space to store the given {@link CacheEntry}.
   * If it succeeds, it returns a file path to mount the given entry in this storage location.
   * Returns null otherwise.
   */
  @Nullable
  public synchronized boolean reserve(CacheEntry entry)
  {
    if (staticCacheEntries.containsKey(entry.getId())) {
      return false;
    }
    if (canHandle(entry)) {
      staticCacheEntries.put(entry.getId(), entry);
      currSizeBytes += entry.getSize();
      return true;
    }
    return false;
  }

  public synchronized boolean reserveWeak(CacheEntry entry)
  {
    if (staticCacheEntries.containsKey(entry.getId())) {
      return true;
    }
    if (weakCacheEntries.containsKey(entry.getId())) {
      weakCacheEntries.get(entry.getId()).visited = true;
      return true;
    }
    if (canHandle(entry)) {
      linkNewWeakEntry(new WeakCacheEntry(entry));
      return true;
    }
    return false;
  }

  @Nullable
  public synchronized <T extends CacheEntry> T addWeakReservationIfExists(CacheEntry entry)
  {
    final CacheEntryIdentifier entryId = entry.getId();
    if (staticCacheEntries.containsKey(entryId)) {
      return (T) staticCacheEntries.get(entryId);
    }
    if (weakCacheEntries.containsKey(entryId)) {
      WeakCacheEntry existingEntry = weakCacheEntries.get(entryId);
      existingEntry.visited = true;
      existingEntry.holds++;
      return (T) existingEntry.cacheEntry;
    }
    return null;
  }

  @Nullable
  public synchronized <T extends CacheEntry> T addWeakReservation(CacheEntry entry)
  {
    final CacheEntry existingEntry = addWeakReservationIfExists(entry);
    if (existingEntry != null) {
      return (T) existingEntry;
    }
    if (canHandle(entry)) {
      final WeakCacheEntry newEntry = new WeakCacheEntry(entry);
      newEntry.holds++;
      linkNewWeakEntry(newEntry);
      return (T) entry;
    }
    return null;
  }

  public synchronized void finishWeakReservationHold(Iterable<CacheEntry> reservations)
  {
    for (CacheEntry entry : reservations) {
      final WeakCacheEntry reservation = weakCacheEntries.get(entry.getId());
      if (reservation != null && reservation.holds > 0) {
        reservation.holds--;
      }
    }
  }

  public synchronized boolean release(CacheEntry entry)
  {
    if (staticCacheEntries.containsKey(entry.getId())) {
      final CacheEntry toRemove = staticCacheEntries.get(entry.getId());
      toRemove.unmount();
      currSizeBytes -= entry.getSize();
      staticCacheEntries.remove(entry.getId());
      return true;
    } else if (weakCacheEntries.containsKey(entry.getId())) {
      final WeakCacheEntry toRemove = weakCacheEntries.get(entry.getId());
      unlinkAndUnmountWeakEntry(toRemove);
      return true;
    }
    log.warn("Entry[%s] is not found under this location[%s]", entry.getId(), path);
    return false;
  }

  /**
   * Inserts a new
   */
  @GuardedBy("this")
  private void linkNewWeakEntry(WeakCacheEntry newWeakEntry)
  {
    if (head != null) {
      newWeakEntry.next = head;
      head.prev = newWeakEntry;
    } else {
      // first item
      tail = newWeakEntry;
      hand = newWeakEntry;
    }
    head = newWeakEntry;
    weakCacheEntries.put(newWeakEntry.cacheEntry.getId(), newWeakEntry);
    currWeakSizeBytes += newWeakEntry.cacheEntry.getSize();
    currSizeBytes += newWeakEntry.cacheEntry.getSize();
  }

  /**
   * Removes a {@link WeakCacheEntry} from the queue, calling {@link CacheEntry#unmount()}
   */
  @GuardedBy("this")
  private void unlinkAndUnmountWeakEntry(WeakCacheEntry toRemove)
  {
    if (hand == toRemove) {
      hand = toRemove.prev != null ? toRemove.prev : tail;
    }
    if (toRemove.prev != null) {
      toRemove.prev.next = toRemove.next;
      if (tail == toRemove) {
        tail = toRemove.prev;
      }
    }
    if (toRemove.next != null) {
      toRemove.next.prev = toRemove.prev;
      if (head == toRemove) {
        head = toRemove.next;
      }
    }
    toRemove.cacheEntry.unmount();
    weakCacheEntries.remove(toRemove.cacheEntry.getId());
    currSizeBytes -= toRemove.cacheEntry.getSize();
    currWeakSizeBytes -= toRemove.cacheEntry.getSize();
  }

  /**
   * This method is only package-private to use it in unit tests. Production code must not call this method directly.
   * Use {@link #reserve} instead.
   */
  @VisibleForTesting
  @GuardedBy("this")
  boolean canHandle(CacheEntry entry)
  {
    if (availableSizeBytes() < entry.getSize()) {
      if (reclaim(entry.getSize())) {
        return canHandle(entry);
      }
      log.warn(
          "Cache entry[%s:%,d] too large for storage[%s:%,d]. Check your druid.segmentCache.locations maxSize param",
          entry.getId(),
          entry.getSize(),
          getPath(),
          availableSizeBytes()
      );
      return false;
    }

    if (freeSpaceToKeep > 0) {
      long currFreeSpace = path.getFreeSpace();
      if ((freeSpaceToKeep + entry.getSize()) > currFreeSpace) {
        if (reclaim(freeSpaceToKeep + entry.getSize())) {
          return canHandle(entry);
        }
        log.warn(
            "Cache entry[%s:%,d] too large for storage[%s:%,d] to maintain suggested freeSpace[%d], current freeSpace is [%d].",
            entry.getId(),
            entry.getSize(),
            getPath(),
            availableSizeBytes(),
            freeSpaceToKeep,
            currFreeSpace
        );
        return false;
      }
    }

    return true;
  }

  @GuardedBy("this")
  private boolean reclaim(long sizeToReclaim)
  {
    if (head == null) {
      return false;
    }
    long sizeFreed = 0;
    WeakCacheEntry startEntry = null;
    boolean unmarked = false;
    while (hand != null && sizeFreed < sizeToReclaim && startEntry != hand) {
      if (startEntry == null) {
        startEntry = hand;
      }
      if (hand.holds > 0) {
        // dont touch bulk reservations
        hand = hand.prev;
      } else if (hand.visited) {
        unmarked = true;
        hand.visited = false;
        hand = hand.prev;
      } else {
        WeakCacheEntry toRemove = hand;
        unlinkAndUnmountWeakEntry(toRemove);
        sizeFreed += toRemove.cacheEntry.getSize();
      }

      // loop around
      if (hand == null) {
        hand = tail;
      }
    }
    // if we unmarked stuff, loop again
    if (unmarked && sizeFreed < sizeToReclaim) {
      return reclaim(sizeToReclaim - sizeFreed);
    }
    return sizeFreed >= sizeToReclaim;
  }

  public synchronized long availableSizeBytes()
  {
    return maxSizeBytes - currSizeBytes;
  }

  public synchronized long currSizeBytes()
  {
    return currSizeBytes;
  }

  private static final class WeakCacheEntry
  {
    private final CacheEntry cacheEntry;
    private WeakCacheEntry prev;
    private WeakCacheEntry next;
    private boolean visited;
    private int holds = 0;

    private WeakCacheEntry(CacheEntry cacheEntry)
    {
      this.cacheEntry = cacheEntry;
    }
  }
}
