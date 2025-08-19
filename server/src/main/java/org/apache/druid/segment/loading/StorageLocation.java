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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
 * storage location (deleting the files from disk) with {@link #unlinkWeakEntry(WeakCacheEntry)}. This process
 * is repeated until either a sufficient amount of space has been reclaimed, or no additional space is able to be
 * reclaimed, in which case the new reservation fails.
 * <p>
 * This class is thread-safe, so that multiple threads can update its state at the same time.
 * One example usage is that a historical server can use multiple threads to load different segments in parallel
 * from deep storage.
 */
@ThreadSafe
public class StorageLocation
{
  private static final EmittingLogger log = new EmittingLogger(StorageLocation.class);

  private final File path;
  private final long maxSizeBytes;
  private final long freeSpaceToKeep;


  @GuardedBy("lock")
  private final Map<CacheEntryIdentifier, CacheEntry> staticCacheEntries = new HashMap<>();

  @GuardedBy("lock")
  private final Map<CacheEntryIdentifier, WeakCacheEntry> weakCacheEntries = new HashMap<>();

  @GuardedBy("lock")
  private WeakCacheEntry head;
  @GuardedBy("lock")
  private WeakCacheEntry tail;
  @GuardedBy("lock")
  private WeakCacheEntry hand;

  /**
   * Current total size of files in bytes, including weak entries.
   */

  /**
   * Current total size of files in bytes, including weak entries.
   */
  private final AtomicLong currSizeBytes = new AtomicLong(0);

  private final AtomicLong currWeakSizeBytes = new AtomicLong(0);

  private final AtomicReference<Stats>  stats = new AtomicReference<>();

  /**
   * A {@link ReentrantReadWriteLock.ReadLock} may be used for any operations to access {@link #staticCacheEntries} or
   * {@link #weakCacheEntries}, including visiting and placing holds on weak entries.
   * <p>
   * A {@link ReentrantReadWriteLock.WriteLock} must be acquired for all operations to insert or remove items, including
   * any operations which traverse or modify the {@link WeakCacheEntry} double linked list such as
   * {@link #linkNewWeakEntry(WeakCacheEntry)} or {@link #unlinkWeakEntry(WeakCacheEntry)}, including calling
   * {@link #canHandle(CacheEntry)} which can unlink entries
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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
    resetStats();
  }

  /**
   * Exposes the {@link #lock} used by the {@link StorageLocation} to allow {@link CacheEntry#mount(StorageLocation)}
   * and {@link CacheEntry#unmount()} to synchronize operations with this location. Callers MUST pay attention to the
   * type of lock they are using if planning to call any other methods of this class to ensure deadlocks are not
   * possible.
   */
  public ReadWriteLock getLock()
  {
    return lock;
  }

  /**
   * The place where the files are stored
   */
  public File getPath()
  {
    return path;
  }

  public <T extends CacheEntry> T getStaticCacheEntry(CacheEntryIdentifier entryId)
  {
    lock.readLock().lock();
    try {
      if (staticCacheEntries.containsKey(entryId)) {
        return (T) staticCacheEntries.get(entryId);
      }
      return null;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * If a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in either {@link #staticCacheEntries} or
   * {@link #weakCacheEntries}, this method will return it. Additionally, {@link WeakCacheEntry} will be marked as
   * visited to reduce the chance that they are evicted during future calls to {@link #reclaim(long)}.
   */
  @Nullable
  public <T extends CacheEntry> T getCacheEntry(CacheEntryIdentifier entryId)
  {
    lock.readLock().lock();
    try {
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
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns true if a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in {@link #staticCacheEntries}
   */
  public boolean isReserved(CacheEntryIdentifier identifier)
  {
    lock.readLock().lock();
    try {
      return staticCacheEntries.containsKey(identifier);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns true if a {@link CacheEntry} for a {@link CacheEntryIdentifier} exists in {@link #weakCacheEntries}
   */
  public boolean isWeakReserved(CacheEntryIdentifier identifier)
  {
    lock.readLock().lock();
    try {
      return weakCacheEntries.containsKey(identifier);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Reserves space to store the given {@link CacheEntry}, returning true if sucessful and false if already reserved,
   * or unable to be reserved.
   */
  public boolean reserve(CacheEntry entry)
  {
    lock.readLock().lock();
    try {
      if (staticCacheEntries.containsKey(entry.getId())) {
        return false;
      }
    }
    finally {
      lock.readLock().unlock();
    }

    lock.writeLock().lock();
    try {
      final ReclaimResult reclaimResult = canHandle(entry);
      unmountReclaimed(reclaimResult);
      if (reclaimResult.isSuccess()) {
        staticCacheEntries.put(entry.getId(), entry);
        currSizeBytes.getAndAdd(entry.getSize());
      }
      return reclaimResult.isSuccess();
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Reserves space to store a 'weak' reservation for a given {@link CacheEntry}. Returns true if already reserved or
   * was able to be successfully reserved, or false if unable to be reserved. This method is intended for use during
   * 'bootstrapping'. To use weak cache entries in a query engine use
   * {@link #addWeakReservationHold(CacheEntryIdentifier, Supplier)} or
   * {@link #addWeakReservationHoldIfExists(CacheEntryIdentifier)}, which places a hold on cache entries to prevent
   * eviction until the hold is released.
   */
  public boolean reserveWeak(CacheEntry entry)
  {
    lock.readLock().lock();
    try {
      if (staticCacheEntries.containsKey(entry.getId())) {
        return true;
      }
      if (weakCacheEntries.containsKey(entry.getId())) {
        weakCacheEntries.get(entry.getId()).visited = true;
        return true;
      }
    }
    finally {
      lock.readLock().unlock();
    }

    lock.writeLock().lock();
    try {
      if (staticCacheEntries.containsKey(entry.getId())) {
        return true;
      }
      if (weakCacheEntries.containsKey(entry.getId())) {
        weakCacheEntries.get(entry.getId()).visited = true;
        return true;
      }
      final ReclaimResult reclaimResult = canHandleWeak(entry);
      unmountReclaimed(reclaimResult);
      if (reclaimResult.isSuccess()) {
        final WeakCacheEntry newEntry = new WeakCacheEntry(entry);
        linkNewWeakEntry(newEntry);
        weakCacheEntries.put(entry.getId(), newEntry);
        stats.get().load();
      }
      return reclaimResult.isSuccess();
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns a {@link ReservationHold} of a {@link CacheEntry} with a 'hold' placed on it, preventing it from being
   * automatically removed by {@link #reclaim(long)} if the {@link CacheEntry} is one of {@link #weakCacheEntries} until
   * the hold is released by {@link ReservationHold#close()}. Callers must call close on the returned object.
   * <p>
   * This method only returns already existing entries, if callers want to insert a new entry if it doesn't already
   * exist, use {@link #addWeakReservationHold(CacheEntryIdentifier, Supplier)}
   */
  @Nullable
  public <T extends CacheEntry> ReservationHold<T> addWeakReservationHoldIfExists(CacheEntryIdentifier entryId)
  {
    lock.readLock().lock();
    try {
      if (staticCacheEntries.containsKey(entryId)) {
        return new ReservationHold<>((T) staticCacheEntries.get(entryId), () -> {});
      }

      WeakCacheEntry existingEntry = weakCacheEntries.get(entryId);
      if (existingEntry != null && existingEntry.hold()) {
        existingEntry.visited = true;
        stats.get().hit();
        return new ReservationHold<>((T) existingEntry.cacheEntry, existingEntry::release);
      }
      return null;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns a {@link ReservationHold} of a {@link CacheEntry} with a 'hold' placed on it, preventing it from being
   * automatically removed by {@link #reclaim(long)} if the {@link CacheEntry} is one of {@link #weakCacheEntries} until
   * the hold is released by {@link ReservationHold#close()}. Callers must call close on the returned object.
   * <p>
   * If the entry already exists, this method will return it, else it will create a new entry if there is space
   * available.
   */
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

    lock.writeLock().lock();
    try {
      WeakCacheEntry retryExistingEntry = weakCacheEntries.get(entryId);
      if (retryExistingEntry != null && retryExistingEntry.hold()) {
        retryExistingEntry.visited = true;
        stats.get().hit();
        return new ReservationHold<>((T) retryExistingEntry.cacheEntry, retryExistingEntry::release);
      }
      final CacheEntry newEntry = entrySupplier.get();
      final ReclaimResult reclaimResult = canHandleWeak(newEntry);
      unmountReclaimed(reclaimResult);
      final ReservationHold<T> hold;
      if (reclaimResult.isSuccess()) {
        final WeakCacheEntry newWeakEntry = new WeakCacheEntry(newEntry);
        newWeakEntry.hold();
        linkNewWeakEntry(newWeakEntry);
        weakCacheEntries.put(newEntry.getId(), newWeakEntry);
        stats.get().load();
        hold = new ReservationHold<>(
            (T) newEntry,
            () -> {
              newWeakEntry.release();
              lock.writeLock().lock();
              try {
                weakCacheEntries.computeIfPresent(
                    newEntry.getId(),
                    (cacheEntryIdentifier, weakCacheEntry) -> {
                      if (!weakCacheEntry.cacheEntry.isMounted()) {
                        // if we never successfully mounted, go ahead and remove so we don't have a dead entry
                        unlinkWeakEntry(weakCacheEntry);
                        // we call unmount anyway to terminate the phaser
                        weakCacheEntry.unmount();
                        return null;
                      }
                      return weakCacheEntry;
                    }
                );
              }
              finally {
                lock.writeLock().unlock();
              }
            }
        );
      } else {
        stats.get().reject();
        hold = null;
      }
      return hold;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes an item from {@link #staticCacheEntries} or {@link #weakCacheEntries}, reducing {@link #currSizeBytes}
   * by {@link CacheEntry#getSize()}
   */
  public void release(CacheEntry entry)
  {
    lock.writeLock().lock();
    try {

      if (staticCacheEntries.containsKey(entry.getId())) {
        final CacheEntry toRemove = staticCacheEntries.remove(entry.getId());
        toRemove.unmount();
        currSizeBytes.getAndAdd(-entry.getSize());
      } else if (weakCacheEntries.containsKey(entry.getId())) {
        final WeakCacheEntry toRemove = weakCacheEntries.remove(entry.getId());
        unlinkWeakEntry(toRemove);
        toRemove.unmount();
        stats.get().unmount();
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Inserts a new {@link WeakCacheEntry}, inserting it as {@link #head} (or both {@link #head} and {@link #tail} if it
   * is the only entry), tracking size in {@link #currSizeBytes} and {@link #currWeakSizeBytes}
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
    currWeakSizeBytes.getAndAdd(newWeakEntry.cacheEntry.getSize());
    currSizeBytes.getAndAdd(newWeakEntry.cacheEntry.getSize());
  }

  /**
   * Removes a {@link WeakCacheEntry} from the queue
   */
  @GuardedBy("lock")
  private void unlinkWeakEntry(WeakCacheEntry toRemove)
  {
    if (head == toRemove) {
      head = toRemove.next;
    }
    if (tail == toRemove) {
      tail = toRemove.prev;
    }
    if (hand == toRemove) {
      hand = toRemove.prev != null ? toRemove.prev : tail;
    }
    if (toRemove.prev != null) {
      toRemove.prev.next = toRemove.next;
    }
    if (toRemove.next != null) {
      toRemove.next.prev = toRemove.prev;
    }
    toRemove.prev = null;
    toRemove.next = null;
    currSizeBytes.getAndAdd(-toRemove.cacheEntry.getSize());
    currWeakSizeBytes.getAndAdd(-toRemove.cacheEntry.getSize());
  }

  /**
   * Checks if this location can store a new {@link CacheEntry}, calling {@link #reclaim(long)} to drop
   * {@link #weakCacheEntries} if possible.
   * <p>
   * This method is only package-private to use it in unit tests. Production code must not call this method directly.
   * Use {@link #reserve} instead.
   */
  @VisibleForTesting
  @GuardedBy("lock")
  ReclaimResult canHandle(CacheEntry entry)
  {
    return canHandle(entry, false);
  }


  /**
   * Checks if this location can store a new {@link WeakCacheEntry}, calling {@link #reclaim(long)} to drop
   * {@link #weakCacheEntries} if possible.
   */
  @GuardedBy("lock")
  private ReclaimResult canHandleWeak(CacheEntry entry)
  {
    return canHandle(entry, true);
  }

  @GuardedBy("lock")
  private ReclaimResult canHandle(CacheEntry entry, boolean weak)
  {
    List<WeakCacheEntry> evicted = new ArrayList<>();
    long bytesReclaimed = 0;
    if (availableSizeBytes() < entry.getSize()) {
      long sizeToReclaim = entry.getSize() - availableSizeBytes();
      final ReclaimResult result = reclaim(sizeToReclaim);
      if (!result.isSuccess()) {
        final String msg = StringUtils.format(
            "Cache entry[%s:%,d] too large for storage[%s:%,d/%,d]",
            entry.getId(),
            entry.getSize(),
            getPath(),
            availableSizeBytes(),
            maxSizeBytes
        );
        if (weak) {
          log.debug(msg);
        } else {
          log.warn(msg);
        }
        return ReclaimResult.failed(sizeToReclaim);
      }
      bytesReclaimed += result.bytesReclaimed;
      evicted.addAll(result.getEvictions());
    }

    if (freeSpaceToKeep > 0) {
      long currFreeSpace = path.getFreeSpace();
      if ((freeSpaceToKeep + entry.getSize()) > currFreeSpace) {
        final ReclaimResult result = reclaim(freeSpaceToKeep + entry.getSize());
        if (!result.isSuccess()) {
          final String msg = StringUtils.format(
              "Cache entry[%s:%,d] too large for storage[%s:%,d/%,d] to maintain suggested freeSpace[%d], current freeSpace is [%d].",
              entry.getId(),
              entry.getSize(),
              getPath(),
              availableSizeBytes(),
              maxSizeBytes,
              freeSpaceToKeep,
              currFreeSpace
          );
          if (weak) {
            log.debug(msg);
          } else {
            log.warn(msg);
          }
          return ReclaimResult.failed(freeSpaceToKeep + entry.getSize());
        }
        bytesReclaimed += result.bytesReclaimed;
        evicted.addAll(result.getEvictions());
      }
    }

    return new ReclaimResult(true, entry.getSize(), bytesReclaimed, evicted);
  }

  @GuardedBy("lock")
  private void unmountReclaimed(ReclaimResult reclaimResult)
  {
    if (reclaimResult != null) {
      for (WeakCacheEntry removed : reclaimResult.getEvictions()) {
        weakCacheEntries.computeIfAbsent(
            removed.cacheEntry.getId(),
            cacheEntryIdentifier -> {
              if (!staticCacheEntries.containsKey(cacheEntryIdentifier)) {
                removed.unmount();
                stats.get().unmount();
              }
              return null;
            }
        );
      }
    }
  }

  public int getWeakEntryCount()
  {
    lock.readLock().lock();
    try {
      return weakCacheEntries.size();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @VisibleForTesting
  public long getActiveWeakHolds()
  {
    lock.readLock().lock();
    try {
      return weakCacheEntries.values().stream().filter(WeakCacheEntry::isHeld).count();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public Stats getStats()
  {
    return stats.get();
  }

  public void resetStats()
  {
    stats.set(new Stats());
  }

  /**
   * Tries to reclaim the specified number of bytes by removing {@link WeakCacheEntry}, starting from {@link #hand}.
   * <p>
   * If {@link WeakCacheEntry#visited} is set, the entry is skipped moving {@link #hand} to {@link WeakCacheEntry#prev}
   * and setting {@link WeakCacheEntry#visited} to false so we can consider it for removal the next time it is the
   * {@link #hand}.
   * <p>
   * If {@link WeakCacheEntry#isHeld()}, it is also skipped, moving {@link #hand} to {@link WeakCacheEntry#prev}.
   *
   * Otherwise, this method will remove entries until either it frees up enough space or runs out of entries to remove
   * (either because there are no more entries or all remaining entries are under a hold).
   */
  @GuardedBy("lock")
  private ReclaimResult reclaim(long sizeToReclaim)
  {
    return reclaimHelper(sizeToReclaim, new ArrayList<>());
  }

  @GuardedBy("lock")
  private ReclaimResult reclaimHelper(long sizeToReclaim, List<WeakCacheEntry> droppedEntries)
  {
    if (head == null) {
      return ReclaimResult.failed(sizeToReclaim);
    }
    long sizeFreed = 0;
    // keep track of where we start so if we get back to the same entry we know when to stop
    WeakCacheEntry startEntry = null;
    // keep track of if we unset visited from any entries, allowing us to try again if we reach startEntry but didn't
    // reclaim enough space yet and know there will likely still be candidates if we check again
    boolean unmarked = false;
    // run until we reclaim enough space, end up where we started, or remove everything
    while (hand != null && sizeFreed < sizeToReclaim && startEntry != hand) {
      if (startEntry == null) {
        startEntry = hand;
      }
      if (hand.isHeld()) {
        // item has one or more holds - move along
        hand = hand.prev;
      } else if (hand.visited) {
        // item is visited, unmark so we can consider it the next time it is hand
        unmarked = true;
        hand.visited = false;
        hand = hand.prev;
      } else {
        // item is valid to remove
        final WeakCacheEntry toRemove = hand;
        hand = hand.prev;
        final WeakCacheEntry removed = weakCacheEntries.remove(toRemove.cacheEntry.getId());
        if (removed == null) {
          throw DruidException.defensive(
              "Weakly held cache entry[%s] already removed from map, how can this be?",
              toRemove.cacheEntry.getId()
          );
        }
        unlinkWeakEntry(removed);
        stats.get().evict();
        toRemove.next = null;
        toRemove.prev = null;
        droppedEntries.add(toRemove);
        sizeFreed += toRemove.cacheEntry.getSize();
        startEntry = null;
      }

      // if we reach the end of the queue, loop around
      if (hand == null) {
        hand = tail;
      }
    }
    // if we unmarked visited stuff, try again
    if (unmarked && sizeFreed < sizeToReclaim) {
      return reclaimHelper(sizeToReclaim - sizeFreed, droppedEntries);
    }
    if (sizeFreed >= sizeToReclaim) {
      return new  ReclaimResult(true, sizeToReclaim, sizeFreed, droppedEntries);
    }
    // if we didn't free up enough space, return everything we removed to the cache
    for (WeakCacheEntry entry : droppedEntries) {
      linkNewWeakEntry(new WeakCacheEntry(entry.cacheEntry));
      weakCacheEntries.put(entry.cacheEntry.getId(), entry);
    }
    return ReclaimResult.failed(sizeToReclaim);
  }

  public long availableSizeBytes()
  {
    return maxSizeBytes - currSizeBytes.get();
  }

  public long currentSizeBytes()
  {
    return currSizeBytes.get();
  }

  public long currentWeakSizeBytes()
  {
    return currWeakSizeBytes.get();
  }

  public static final class ReclaimResult
  {
    public static ReclaimResult failed(long spaceRequired)
    {
      return new ReclaimResult(false, spaceRequired, 0, List.of());
    }

    private final boolean success;
    private final long spaceRequired;
    private final long bytesReclaimed;
    private final List<WeakCacheEntry> evictions;

    ReclaimResult(
        boolean success,
        long spaceRequired,
        long bytesReclaimed,
        List<WeakCacheEntry> evictions
    )
    {
      this.success = success;
      this.spaceRequired = spaceRequired;
      this.bytesReclaimed = bytesReclaimed;
      this.evictions = evictions;
    }

    public boolean isSuccess()
    {
      return success;
    }

    public List<WeakCacheEntry> getEvictions()
    {
      return evictions;
    }
  }
  /**
   * Wrapper for a {@link CacheEntry} which can be reclaimed if there is not enough space available to add some other
   * {@link CacheEntry} later.
   */
  static final class WeakCacheEntry
  {
    private final CacheEntry cacheEntry;
    /**
     * Phaser that allows callers to place a hold on an entry, which will prevent {@link #reclaim(long)} from being able
     * to remove this entry until the hold is released. When the phaser arrives at 0, calls {@link CacheEntry#unmount()}
     */
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

    /**
     * Set to true when an entry is used by a caller, which will cause {@link #reclaim(long)} to skip this entry
     * (and set to false). this is volatile because we allow setting it to true without holding {@link #lock}
     */
    private volatile boolean visited;

    private WeakCacheEntry(CacheEntry cacheEntry)
    {
      this.cacheEntry = cacheEntry;
    }

    /**
     * Returns true if there is 1 or more {@link #hold()} currently placed on this entry
     */
    boolean isHeld()
    {
      return holdReferents.getRegisteredParties() > 1;
    }

    /**
     * Place a hold on this entry to prevent it from being dropped by {@link #reclaim(long)}
     */
    private boolean hold()
    {
      return holdReferents.register() >= 0;
    }

    /**
     * Release a {@link #hold()}
     */
    private int release()
    {
      return holdReferents.arriveAndDeregister();
    }

    /**
     * Call {@link CacheEntry#unmount()} after all holds are released ({@link #holdReferents})
     */
    private void unmount()
    {
      holdReferents.arriveAndDeregister();
    }
  }

  /**
   * A {@link Closeable} {@link CacheEntry} wrapper representing both the entry and a 'hold' that is placed on it to
   * prevent the entry from being dropped by {@link #reclaim(long)} at least until the wrapper is closed.
   * <p>
   * In practice, if the entry is {@link #weakCacheEntries} the entry contained in this object was placed under a hold
   * with {@link WeakCacheEntry#hold()}, and {@link #close()} with then call {@link WeakCacheEntry#release()} to finish
   * the hold. If the entry is instead {@link #staticCacheEntries}, a hold is not necessary since these entries will
   * not be removed automatically during {@link #reclaim(long)}, and instead are only ever being dropped if
   * {@link #release(CacheEntry)} is explicitly called. Close is a no-op for these entries.
   * <p>
   * Callers MUST be sure to close this object when finished to release the hold
   */
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

  public static final class Stats
  {
    private final AtomicLong loadCount = new AtomicLong(0);
    private final AtomicLong rejectionCount = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong unmountCount = new AtomicLong(0);

    public void hit()
    {
      hitCount.getAndIncrement();
    }

    public long getHitCount()
    {
      return hitCount.get();
    }

    public void load()
    {
      loadCount.getAndIncrement();
    }

    public long getLoadCount()
    {
      return loadCount.get();
    }

    public void evict()
    {
      evictionCount.getAndIncrement();
    }

    public long getEvictionCount()
    {
      return evictionCount.get();
    }

    public void unmount()
    {
      unmountCount.getAndIncrement();
    }

    public long getUnmountCount()
    {
      return unmountCount.get();
    }

    public void reject()
    {
      rejectionCount.getAndIncrement();
    }

    public long getRejectCount()
    {
      return rejectionCount.get();
    }
  }
}
