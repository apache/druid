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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
 * {@link #addWeakReservationHold(CacheEntryIdentifier, Supplier)}, or
 * {@link #addWeakReservationHoldIfExists(CacheEntryIdentifier)}. {@link CacheEntry} stored in this manner will exist on
 * disk in this location until the point that another new reservation needs more space than remains available in the
 * location, at which point {@link #reclaim(long)} will be called to try to call {@link CacheEntry#unmount()} on any
 * eligible entries until enough space is available to store the new item.
 * <p>
 * Items are chosen for eviction using an algorithm based on
 * <a href="https://www.usenix.org/system/files/nsdi24spring_prepub_zhang-yazhuo.pdf">SIEVE</a> with additional
 * mechanisms to place temporary holds on cache entries. The holds are required to support cases such as if a group of
 * cache entries are taking part in a query and must all be loaded simultaneously before processing can continue.
 * <p>
 * Implementation-wise, {@link CacheEntry} are wrapped in {@link WeakCacheEntry} to form a doubly linked list
 * functioning as a queue which can be interacted with via 3 fields, {@link #head}, {@link #tail}, and {@link #hand}.
 * Head and tail expectedly mark the beginning and end of the queue, while the hand is the interesting bit and is used
 * as the position of the current item to consider for eviction the next time {@link #reclaim(long)} needs to be called.
 * Entries are also stored in a map, {@link #weakCacheEntries}, for fast retrieval. Using a weak cache entry sets
 * {@link WeakCacheEntry#visited} to true, and {@link #addWeakReservationHold(CacheEntryIdentifier, Supplier)} and
 * {@link #addWeakReservationHoldIfExists(CacheEntryIdentifier)} additionally will call
 * {@link WeakCacheEntry#hold()} until {@link ReservationHold#close()} is called which will call
 * {@link WeakCacheEntry#release()}.
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
@ThreadSafe
public class StorageLocation
{
  private static final EmittingLogger log = new EmittingLogger(StorageLocation.class);

  private final File path;
  private final long maxSizeBytes;
  private final long freeSpaceToKeep;

  private final ConcurrentHashMap<CacheEntryIdentifier, CacheEntry> staticCacheEntries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<CacheEntryIdentifier, WeakCacheEntry> weakCacheEntries = new ConcurrentHashMap<>();

  @GuardedBy("lock")
  private WeakCacheEntry head;
  @GuardedBy("lock")
  private WeakCacheEntry tail;
  @GuardedBy("lock")
  private WeakCacheEntry hand;

  /**
   * Current total size of files in bytes, including weak entries.
   */
  private final AtomicLong currSizeBytes = new AtomicLong(0);

  @GuardedBy("lock")
  private long currWeakSizeBytes = 0;

  private final Object lock = new Object();


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

  /**
   * If a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in either {@link #staticCacheEntries} or
   * {@link #weakCacheEntries}, this method will return it. Additionally, {@link WeakCacheEntry} will be marked as
   * visited to reduce the chance that they are evicted during future calls to {@link #reclaim(long)}.
   */
  @Nullable
  public <T extends CacheEntry> T getCacheEntry(CacheEntryIdentifier entryId)
  {
    final CacheEntry entry = staticCacheEntries.get(entryId);
    if (entry != null) {
      return (T) entry;
    }
    final WeakCacheEntry weakCacheEntry = weakCacheEntries.computeIfPresent(
        entryId,
        (identifier, weakCacheEntry1) -> {
          weakCacheEntry1.visited = true;
          return weakCacheEntry1;
        }
    );
    if (weakCacheEntry != null) {
      return (T) weakCacheEntry.cacheEntry;
    }
    return null;
  }

  /**
   * Returns true if a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in {@link #staticCacheEntries}
   */
  public boolean isReserved(CacheEntryIdentifier identifier)
  {
    return staticCacheEntries.containsKey(identifier);
  }

  /**
   * Returns true if a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in {@link #weakCacheEntries}
   */
  public boolean isWeakReserved(CacheEntryIdentifier identifier)
  {
    return weakCacheEntries.containsKey(identifier);
  }

  /**
   * Reserves space to store the given {@link CacheEntry}, returning true if sucessful and false if already reserved,
   * or unable to be reserved.
   */
  public boolean reserve(CacheEntry entry)
  {
    if (staticCacheEntries.containsKey(entry.getId())) {
      return false;
    }

    synchronized (lock) {
      if (canHandle(entry)) {
        staticCacheEntries.put(entry.getId(), entry);
        currSizeBytes.getAndAdd(entry.getSize());
        return true;
      }
      return false;
    }
  }

  /**
   * Reserves space to store a 'weak' reservation for a given {@link CacheEntry}. Returns true if already reserved or
   * was able to be successfully reserved, or false if unable to be reserved.
   */
  public boolean reserveWeak(CacheEntry entry)
  {
    if (staticCacheEntries.containsKey(entry.getId())) {
      return true;
    }
    WeakCacheEntry existingEntry = weakCacheEntries.computeIfPresent(
        entry.getId(),
        (identifier, weakCacheEntry) -> {
          weakCacheEntry.visited = true;
          return weakCacheEntry;
        }
    );
    if (existingEntry != null) {
      return true;
    }

    synchronized (lock) {
      if (canHandle(entry)) {

        weakCacheEntries.computeIfAbsent(
            entry.getId(),
            identifier -> {
              final WeakCacheEntry newEntry = new WeakCacheEntry(entry);
              linkNewWeakEntry(newEntry);
              return newEntry;
            }
        );

        return true;
      }
      return false;
    }
  }

  @Nullable
  public <T extends CacheEntry> ReservationHold<T> addWeakReservationHoldIfExists(CacheEntryIdentifier entryId)
  {
    final CacheEntry entry = staticCacheEntries.get(entryId);
    if (entry != null) {
      return new ReservationHold<>((T) entry, () -> {});
    }

    final WeakCacheEntry existingEntry = weakCacheEntries.computeIfPresent(
        entryId,
        (identifier, weakCacheEntry) -> {
          if (weakCacheEntry.hold()) {
            weakCacheEntry.visited = true;
            return weakCacheEntry;
          }
          return null;
        }
    );
    if (existingEntry != null) {
      return new ReservationHold<>((T) existingEntry.cacheEntry, existingEntry::release);
    }
    return null;
  }

  @Nullable
  public <T extends CacheEntry> ReservationHold<T> addWeakReservationHold(
      CacheEntryIdentifier entryId,
      Supplier<? extends CacheEntry> entrySupplier
  )
  {
    final ReservationHold<T> existingEntry = addWeakReservationHoldIfExists(entryId);
    if (existingEntry != null) {
      return existingEntry;
    }

    final CacheEntry newEntry = entrySupplier.get();
    synchronized (lock) {
      if (canHandle(newEntry)) {
        final WeakCacheEntry newWeakEntry = new WeakCacheEntry(newEntry);
        newWeakEntry.hold();
        weakCacheEntries.put(newEntry.getId(), newWeakEntry);
        linkNewWeakEntry(newWeakEntry);
        return new ReservationHold<>((T) newEntry, () -> {
          newWeakEntry.release();
          // if we never successfully mounted, go ahead and remove so we don't have a dead entry
          if (!newEntry.isMounted()) {
            synchronized (lock) {
              weakCacheEntries.remove(newEntry.getId());
              unlinkAndUnmountWeakEntry(newWeakEntry);
            }
          }
        });
      }
      return null;
    }
  }

  public void release(CacheEntry entry)
  {
    final boolean present = staticCacheEntries.containsKey(entry.getId());
    staticCacheEntries.computeIfPresent(
        entry.getId(),
        (identifier, cacheEntry) -> {
          cacheEntry.unmount();
          currSizeBytes.getAndAdd(-entry.getSize());
          return null;
        }
    );
    if (present) {
      return;
    }

    synchronized (lock) {
      weakCacheEntries.computeIfPresent(
          entry.getId(),
          (identifier, weakCacheEntry) -> {
            unlinkAndUnmountWeakEntry(weakCacheEntry);
            return null;
          }
      );
    }
  }

  /**
   * Inserts a new
   */
  @GuardedBy("lock")
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
    currWeakSizeBytes += newWeakEntry.cacheEntry.getSize();
    currSizeBytes.getAndAdd(newWeakEntry.cacheEntry.getSize());
  }

  /**
   * Removes a {@link WeakCacheEntry} from the queue, calling {@link CacheEntry#unmount()}
   */
  @GuardedBy("lock")
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
    toRemove.unmount();
    currSizeBytes.getAndAdd(-toRemove.cacheEntry.getSize());
    currWeakSizeBytes -= toRemove.cacheEntry.getSize();
  }

  /**
   * This method is only package-private to use it in unit tests. Production code must not call this method directly.
   * Use {@link #reserve} instead.
   */
  @VisibleForTesting
  @GuardedBy("lock")
  boolean canHandle(CacheEntry entry)
  {
    if (availableSizeBytes() < entry.getSize()) {
      if (reclaim(entry.getSize() - availableSizeBytes())) {
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

  @GuardedBy("lock")
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
      if (hand.isHeld()) {
        // dont touch bulk reservations
        hand = hand.prev;
      } else if (hand.visited) {
        unmarked = true;
        hand.visited = false;
        hand = hand.prev;
      } else {
        WeakCacheEntry toRemove = hand;
        hand = hand.prev;
        weakCacheEntries.remove(toRemove.cacheEntry.getId());
        unlinkAndUnmountWeakEntry(toRemove);
        sizeFreed += toRemove.cacheEntry.getSize();
        startEntry = null;
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

  public long availableSizeBytes()
  {
    return maxSizeBytes - currSizeBytes.get();
  }

  public long currSizeBytes()
  {
    return currSizeBytes.get();
  }

  private static final class WeakCacheEntry
  {
    private final CacheEntry cacheEntry;
    private final Phaser holdReferents = new Phaser(1)
    {
      @Override
      protected boolean onAdvance(int phase, int registeredParties)
      {
        // Ensure that onAdvance() doesn't throw exception, otherwise termination won't happen
        if (registeredParties != 0) {
          log.error("registeredParties[%s] is not 0", registeredParties);
        }
        try {
          cacheEntry.unmount();
        }
        catch (Exception e) {
          try {
            log.error(e, "Exception while closing reference counted object[%s]", cacheEntry.getId());
          }
          catch (Exception e2) {
            // ignore
          }
        }
        // Always terminate.
        return true;
      }
    };

    private WeakCacheEntry prev;
    private WeakCacheEntry next;
    private volatile boolean visited;

    private WeakCacheEntry(CacheEntry cacheEntry)
    {
      this.cacheEntry = cacheEntry;
    }

    private boolean isHeld()
    {
      return holdReferents.getRegisteredParties() > 1;
    }

    private boolean hold()
    {
      return holdReferents.register() >= 0;
    }

    private int release()
    {
      return holdReferents.arriveAndDeregister();
    }

    private void unmount()
    {
      holdReferents.arriveAndDeregister();
    }
  }

  public static class ReservationHold<TEntry extends CacheEntry> implements Closeable
  {
    private final TEntry entry;
    private final Runnable releaseHold;

    public ReservationHold(TEntry entry, Runnable releaseHold)
    {
      this.entry = entry;
      this.releaseHold = releaseHold;
    }

    public TEntry getEntry()
    {
      return entry;
    }

    @Override
    public void close()
    {
      releaseHold.run();
    }
  }
}
