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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

class StorageLocationTest
{
  @TempDir
  File tempDir;

  ExecutorService executorService;

  @BeforeEach
  public void setup()
  {
    executorService = Execs.multiThreaded(
        10,
        "storage-location-test-%d"
    );
  }

  @Test
  public void testWeakReserveAndReclaim()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 25);
    CacheEntry entry2 = new TestCacheEntry("2", 25);
    CacheEntry entry3 = new TestCacheEntry("3", 25);
    CacheEntry entry4 = new TestCacheEntry("4", 25);
    CacheEntry entry5 = new TestCacheEntry("5", 25);

    location.reserveWeak(entry1);
    location.reserveWeak(entry2);
    location.reserveWeak(entry3);
    location.reserveWeak(entry4);
    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    location.reserveWeak(entry5);
    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertFalse(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));
    Assertions.assertEquals(100, location.currentWeakSizeBytes());
  }

  @Test
  public void testRemoveFromHead()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 25);
    CacheEntry entry2 = new TestCacheEntry("2", 25);
    CacheEntry entry3 = new TestCacheEntry("3", 25);
    CacheEntry entry4 = new TestCacheEntry("4", 25);

    location.reserveWeak(entry1);
    location.reserveWeak(entry2);
    location.reserveWeak(entry3);
    location.reserveWeak(entry4);
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    location.release(entry1);
    location.release(entry2);
    location.release(entry3);
    location.release(entry4);
    // release does not remove weak entries
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
  }

  @Test
  public void testRemoveFromTail()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 25);
    CacheEntry entry2 = new TestCacheEntry("2", 25);
    CacheEntry entry3 = new TestCacheEntry("3", 25);
    CacheEntry entry4 = new TestCacheEntry("4", 25);

    location.reserveWeak(entry1);
    location.reserveWeak(entry2);
    location.reserveWeak(entry3);
    location.reserveWeak(entry4);
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    location.release(entry4);
    location.release(entry3);
    location.release(entry2);
    location.release(entry1);
    // release does not remove weak entries
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
  }

  @Test
  public void testRemoveRandom()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 25);
    CacheEntry entry2 = new TestCacheEntry("2", 25);
    CacheEntry entry3 = new TestCacheEntry("3", 25);
    CacheEntry entry4 = new TestCacheEntry("4", 25);
    List<CacheEntry> entries = new ArrayList<>();
    entries.add(entry1);
    entries.add(entry2);
    entries.add(entry3);
    entries.add(entry4);

    location.reserveWeak(entry1);
    location.reserveWeak(entry2);
    location.reserveWeak(entry3);
    location.reserveWeak(entry4);
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    while (!entries.isEmpty()) {
      int toRemove = ThreadLocalRandom.current().nextInt(entries.size());
      CacheEntry entry = entries.get(toRemove);
      location.release(entry);
      entries.remove(toRemove);
    }
    // release does not remove weak entries
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    Assertions.assertEquals(100, location.currentSizeBytes());
    Assertions.assertEquals(100, location.currentWeakSizeBytes());
  }

  @Test
  public void testBulkReservation()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 25);
    CacheEntry entry2 = new TestCacheEntry("2", 25);
    CacheEntry entry3 = new TestCacheEntry("3", 25);
    CacheEntry entry4 = new TestCacheEntry("4", 25);
    CacheEntry entry5 = new TestCacheEntry("5", 25);
    CacheEntry entry6 = new TestCacheEntry("6", 25);
    CacheEntry entry7 = new TestCacheEntry("7", 25);
    CacheEntry entry8 = new TestCacheEntry("8", 25);

    final Closer closer = Closer.create();
    Assertions.assertNotNull(closer.register(location.addWeakReservationHold(entry1.getId(), () -> entry1)));
    Assertions.assertNotNull(closer.register(location.addWeakReservationHold(entry2.getId(), () -> entry2)));
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(50, location.getWeakStats().getHoldBytes());
    Assertions.assertTrue(location.reserveWeak(entry3));
    Assertions.assertTrue(location.reserveWeak(entry4));

    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(50, location.getWeakStats().getHoldBytes());
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));

    Assertions.assertNotNull(closer.register(location.addWeakReservationHold(entry5.getId(), () -> entry5)));

    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertEquals(3, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(75, location.getWeakStats().getHoldBytes());


    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));

    Assertions.assertTrue(location.reserveWeak(entry6));

    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry6.getId()));

    Assertions.assertEquals(3, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(75, location.getWeakStats().getHoldBytes());

    Assertions.assertNotNull(closer.register(location.addWeakReservationHold(entry7.getId(), () -> entry7)));
    Assertions.assertEquals(4, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(100, location.getWeakStats().getHoldBytes());

    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry6.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry7.getId()));

    // all storage is held, cannot reserve
    Assertions.assertFalse(location.reserveWeak(entry8));

    // release holds
    CloseableUtils.closeAndWrapExceptions(closer);
    Assertions.assertTrue(location.reserveWeak(entry8));

    Assertions.assertFalse(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry6.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry7.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry8.getId()));
    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(0, location.getWeakStats().getHoldBytes());
  }

  @Test
  @SuppressWarnings({"GuardedBy", "FieldAccessNotGuarded"})
  public void testStorageLocationFreePercent()
  {
    // free space ignored only maxSize matters
    StorageLocation locationPlain = fakeLocation(100_000, 5_000, 10_000, null);
    Assertions.assertTrue(locationPlain.canHandle(makeSegmentEntry("2012/2013", 9_000)).isSuccess());
    Assertions.assertFalse(locationPlain.canHandle(makeSegmentEntry("2012/2013", 11_000)).isSuccess());

    // enough space available maxSize is the limit
    StorageLocation locationFree = fakeLocation(100_000, 25_000, 10_000, 10.0);
    Assertions.assertTrue(locationFree.canHandle(makeSegmentEntry("2012/2013", 9_000)).isSuccess());
    Assertions.assertFalse(locationFree.canHandle(makeSegmentEntry("2012/2013", 11_000)).isSuccess());

    // disk almost full percentage is the limit
    StorageLocation locationFull = fakeLocation(100_000, 15_000, 10_000, 10.0);
    Assertions.assertTrue(locationFull.canHandle(makeSegmentEntry("2012/2013", 4_000)).isSuccess());
    Assertions.assertFalse(locationFull.canHandle(makeSegmentEntry("2012/2013", 6_000)).isSuccess());
  }

  @Test
  @SuppressWarnings({"GuardedBy", "FieldAccessNotGuarded"})
  public void testStorageLocationRealFileSystem()
  {
    StorageLocation location = new StorageLocation(tempDir, 10_000, 100.0d);
    Assertions.assertFalse(location.canHandle(makeSegmentEntry("2012/2013", 5_000)).isSuccess());

    location = new StorageLocation(tempDir, 10_000, 0.0001d);
    Assertions.assertTrue(location.canHandle(makeSegmentEntry("2012/2013", 1)).isSuccess());
  }


  @Test
  public void testStorageLocation()
  {
    long expectedAvail = 1000L;
    StorageLocation loc = new StorageLocation(tempDir, expectedAvail, null);

    verifyLoc(expectedAvail, loc);

    final CacheEntry entry1 = makeSegmentEntry("2012-01-01/2012-01-02", 10);
    final CacheEntry entry2 = makeSegmentEntry("2012-01-01/2012-01-02", 10);
    final CacheEntry entry3 = makeSegmentEntry("2012-01-02/2012-01-03", 23);

    loc.reserve(entry1);
    expectedAvail -= 10;
    verifyLoc(expectedAvail, loc);

    loc.reserve(entry2);
    verifyLoc(expectedAvail, loc);

    loc.reserve(entry3);
    expectedAvail -= 23;
    verifyLoc(expectedAvail, loc);

    loc.release(entry1);
    expectedAvail += 10;
    verifyLoc(expectedAvail, loc);

    loc.release(makeSegmentEntry("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.release(entry3);
    expectedAvail += 23;
    verifyLoc(expectedAvail, loc);
  }

  @Test
  public void testReserveAndRelease()
  {
    StorageLocation loc = new StorageLocation(tempDir, 1000L, null);
    CacheEntry entry1 = new TestCacheEntry("testPath", 100L);
    CacheEntry entry2 = new TestCacheEntry("testPath", 100L);

    Assertions.assertTrue(loc.reserve(entry1));

    Assertions.assertEquals(900L, loc.availableSizeBytes());
    Assertions.assertTrue(loc.isReserved(entry1.getId()));

    Assertions.assertFalse(loc.reserve(entry2));

    loc.release(entry1);
    Assertions.assertEquals(1000L, loc.availableSizeBytes());
    Assertions.assertFalse(loc.isReserved(entry2.getId()));

    loc.release(entry2);
  }

  @Test
  public void testReserveWeakExistsConcurrency() throws ExecutionException, InterruptedException
  {
    StorageLocation loc = new StorageLocation(tempDir, 1000L, null);
    final TestSegmentCacheEntry entry = makeSegmentEntry("2024/2025", 10);
    loc.reserveWeak(entry);
    entry.mount(loc);

    for (int i = 0; i < 1000; i++) {
      final List<Future<Boolean>> futures = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        futures.add(
            executorService.submit(() -> {
              try {
                StorageLocation.ReservationHold<TestSegmentCacheEntry> hold =
                    loc.addWeakReservationHoldIfExists(entry.getId());
                Assertions.assertNotNull(hold);
                hold.close();
                return true;
              }
              catch (Throwable t) {
                return false;
              }
            })
        );
      }

      for (Future<Boolean> future : futures) {
        Assertions.assertTrue(future.get());
      }
    }

    Assertions.assertEquals(0, loc.getWeakStats().getHoldCount());
  }

  @Test
  public void testReclaimRestoreDoesNotCreateZombieEntries()
  {
    StorageLocation location = new StorageLocation(tempDir, 100L, null);
    CacheEntry entry1 = new TestCacheEntry("1", 10);
    CacheEntry entry2 = new TestCacheEntry("2", 90);
    CacheEntry entry3 = new TestCacheEntry("3", 20);

    location.reserveWeak(entry1);
    // hold entry2 so it cannot be evicted by reclaim
    StorageLocation.ReservationHold<?> hold2 = location.addWeakReservationHold(
        entry2.getId(),
        () -> entry2
    );

    // must free 20 bytes but can only evict entry1 (10). Fails and restores entry1
    // where the bug was a mismatch caused by creating a new entry in the list but re-using the old entry for the map.
    Assertions.assertFalse(location.reserveWeak(entry3));

    // the hand pointer reaches the new entry1, removes the old entry1 from the map which is a zombie, then wraps around
    // to the same zombie entry1 again since its head — at which point the map no longer contains the ID and the defensive exception was
    // thrown.
    Assertions.assertFalse(location.reserveWeak(entry3));

    hold2.close();
  }

  @Test
  public void testRemoveUnheldWeakEntry()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final UnmountTrackingCacheEntry entry = new UnmountTrackingCacheEntry("a", 30);

    // an unheld weak entry is removed: unlinked from the queue, unmounted, and its size reclaimed
    Assertions.assertTrue(location.reserveWeak(entry));
    Assertions.assertTrue(location.isWeakReserved(entry.getId()));
    Assertions.assertEquals(30, location.currentSizeBytes());

    location.removeUnheldWeakEntry(entry.getId());
    Assertions.assertFalse(location.isWeakReserved(entry.getId()));
    Assertions.assertEquals(0, location.getWeakEntryCount());
    Assertions.assertEquals(0, location.currentSizeBytes());
    Assertions.assertTrue(entry.unmountCalled, "removeUnheldWeakEntry must unmount the entry");

    // absent id is a no-op
    location.removeUnheldWeakEntry(new StringCacheIdentifier("missing"));

    // a held weak entry is left in place: its holder's release runnable is responsible for cleanup
    final UnmountTrackingCacheEntry held = new UnmountTrackingCacheEntry("b", 40);
    final StorageLocation.ReservationHold<?> hold = location.addWeakReservationHold(held.getId(), () -> held);
    Assertions.assertNotNull(hold);
    location.removeUnheldWeakEntry(held.getId());
    Assertions.assertTrue(location.isWeakReserved(held.getId()), "a held weak entry must not be removed");
    Assertions.assertFalse(held.unmountCalled, "a held weak entry must not be unmounted");
    hold.close();
  }

  @Test
  public void testAdjustReservationStaticEntry()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 50);
    Assertions.assertTrue(location.reserve(entry));
    Assertions.assertEquals(50, location.currentSizeBytes());
    Assertions.assertEquals(50, location.availableSizeBytes());

    location.adjustReservation(entry.getId(), 10);
    Assertions.assertEquals(10, entry.getSize());
    Assertions.assertEquals(10, location.currentSizeBytes());
    Assertions.assertEquals(90, location.availableSizeBytes());

    // after shrink, location can host new entries that wouldn't have fit at the original size
    final TestResizableCacheEntry entry2 = new TestResizableCacheEntry("b", 80);
    Assertions.assertTrue(location.reserve(entry2));

    // release accounting still uses the (post-shrink) size
    location.release(entry);
    Assertions.assertEquals(80, location.currentSizeBytes());
  }

  @Test
  public void testAdjustReservationWeakEntry()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 80);
    Assertions.assertTrue(location.reserveWeak(entry));
    Assertions.assertEquals(80, location.currentWeakSizeBytes());

    location.adjustReservation(entry.getId(), 30);
    Assertions.assertEquals(30, entry.getSize());
    Assertions.assertEquals(30, location.currentWeakSizeBytes());
    Assertions.assertEquals(30, location.currentSizeBytes());
  }

  @Test
  public void testAdjustReservationGrowThrows()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 30);
    Assertions.assertTrue(location.reserve(entry));

    Assertions.assertThrows(
        DruidException.class,
        () -> location.adjustReservation(entry.getId(), 60)
    );
    // entry size and location accounting unchanged
    Assertions.assertEquals(30, entry.getSize());
    Assertions.assertEquals(30, location.currentSizeBytes());
  }

  @Test
  public void testAdjustReservationUnknownEntryThrows()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    Assertions.assertThrows(
        DruidException.class,
        () -> location.adjustReservation(new StringCacheIdentifier("nope"), 10)
    );
  }

  @Test
  public void testAdjustReservationNonResizableEntryThrows()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final CacheEntry entry = new TestCacheEntry("a", 30);
    Assertions.assertTrue(location.reserve(entry));

    Assertions.assertThrows(
        DruidException.class,
        () -> location.adjustReservation(entry.getId(), 10)
    );
  }

  @Test
  public void testAdjustReservationToSameSizeIsNoOp()
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 50);
    Assertions.assertTrue(location.reserve(entry));

    location.adjustReservation(entry.getId(), 50);
    Assertions.assertEquals(50, entry.getSize());
    Assertions.assertEquals(50, location.currentSizeBytes());
  }

  @Test
  public void testAdjustReservationWeakEntryShrinksHeldBytes() throws IOException
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 80);
    Assertions.assertTrue(location.reserveWeak(entry));

    // Acquire a hold BEFORE shrinking. trackWeakHold records 80 bytes against currHoldBytes.
    final StorageLocation.ReservationHold<?> hold = location.addWeakReservationHold(entry.getId(), () -> entry);
    Assertions.assertNotNull(hold);
    Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(80, location.getWeakStats().getHoldBytes());

    // Shrink to 30: hold-bytes contribution from the active hold must shrink in lockstep so the eventual
    // trackWeakRelease (which subtracts the new smaller size) leaves currHoldBytes at 0.
    location.adjustReservation(entry.getId(), 30);
    Assertions.assertEquals(30, entry.getSize());
    Assertions.assertEquals(30, location.currentWeakSizeBytes());
    Assertions.assertEquals(30, location.getWeakStats().getHoldBytes());

    hold.close();
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(0, location.getWeakStats().getHoldBytes());
  }

  @Test
  public void testAdjustReservationWeakEntryShrinksHeldBytesWithMultipleHolds() throws IOException
  {
    final StorageLocation location = new StorageLocation(tempDir, 100L, null);
    final TestResizableCacheEntry entry = new TestResizableCacheEntry("a", 50);
    Assertions.assertTrue(location.reserveWeak(entry));

    // Two concurrent holds: trackWeakHold fires twice, so currHoldBytes = 2 * 50 = 100.
    final StorageLocation.ReservationHold<?> hold1 = location.addWeakReservationHold(entry.getId(), () -> entry);
    final StorageLocation.ReservationHold<?> hold2 = location.addWeakReservationHold(entry.getId(), () -> entry);
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(100, location.getWeakStats().getHoldBytes());

    // Shrink by 30 (50 → 20): each of the two active holds contributes -30, so currHoldBytes drops by 60.
    location.adjustReservation(entry.getId(), 20);
    Assertions.assertEquals(40, location.getWeakStats().getHoldBytes());

    hold1.close();
    Assertions.assertEquals(20, location.getWeakStats().getHoldBytes());
    hold2.close();
    Assertions.assertEquals(0, location.getWeakStats().getHoldBytes());
  }

  @SuppressWarnings({"GuardedBy", "FieldAccessNotGuarded"})
  private void verifyLoc(long maxSize, StorageLocation loc)
  {
    Assertions.assertEquals(maxSize, loc.availableSizeBytes());
    for (int i = 0; i <= maxSize; ++i) {
      Assertions.assertTrue(loc.canHandle(makeSegmentEntry("2013/2014", i)).isSuccess(), String.valueOf(i));
    }
  }

  private StorageLocation fakeLocation(long total, long free, long max, Double percent)
  {
    File file = EasyMock.mock(File.class);
    EasyMock.expect(file.getTotalSpace()).andReturn(total).anyTimes();
    EasyMock.expect(file.getFreeSpace()).andReturn(free).anyTimes();
    EasyMock.replay(file);
    return new StorageLocation(file, max, percent);
  }

  private static DataSegment makeSegment(String intervalString, long size)
  {
    return new DataSegment(
        "test",
        Intervals.of(intervalString),
        "1",
        ImmutableMap.of(),
        Collections.singletonList("d"),
        Collections.singletonList("m"),
        null,
        null,
        size
    );
  }

  private SegmentId newSegmentId(String intervalString)
  {
    return SegmentId.of("test", Intervals.of(intervalString), "1", 0);
  }

  private static TestSegmentCacheEntry makeSegmentEntry(String intervalString, long size)
  {
    return new TestSegmentCacheEntry(intervalString, size);
  }

  public static final class TestSegmentCacheEntry implements CacheEntry
  {
    private final SegmentCacheEntryIdentifier identifier;
    private final DataSegment segment;
    private boolean isMounted = false;

    public TestSegmentCacheEntry(String intervalString, long size)
    {
      this.segment = makeSegment(intervalString, size);
      this.identifier = new SegmentCacheEntryIdentifier(segment.getId());
    }

    @Override
    public SegmentCacheEntryIdentifier getId()
    {
      return identifier;
    }

    @Override
    public long getSize()
    {
      return segment.getSize();
    }

    @Override
    public boolean isMounted()
    {
      return isMounted;
    }

    @Override
    public void mount(StorageLocation location)
    {
      isMounted = true;
    }

    @Override
    public void unmount()
    {
      isMounted = false;
    }
  }

  private static final class TestCacheEntry implements CacheEntry
  {
    private final StringCacheIdentifier id;
    private final long size;
    private boolean isMounted = false;

    private TestCacheEntry(String id, long size)
    {
      this.id = new StringCacheIdentifier(id);
      this.size = size;
    }

    @Override
    public StringCacheIdentifier getId()
    {
      return id;
    }

    @Override
    public long getSize()
    {
      return size;
    }

    @Override
    public boolean isMounted()
    {
      return true;
    }

    @Override
    public void mount(StorageLocation location)
    {
      // do nothing
    }

    @Override
    public void unmount()
    {
      // do nothing
    }
  }

  /**
   * A {@link CacheEntry} that tracks mount/unmount so tests can assert that lifecycle hooks fired.
   */
  private static final class UnmountTrackingCacheEntry implements CacheEntry
  {
    private final StringCacheIdentifier id;
    private final long size;
    private boolean mounted = false;
    private boolean unmountCalled = false;

    private UnmountTrackingCacheEntry(String id, long size)
    {
      this.id = new StringCacheIdentifier(id);
      this.size = size;
    }

    @Override
    public StringCacheIdentifier getId()
    {
      return id;
    }

    @Override
    public long getSize()
    {
      return size;
    }

    @Override
    public boolean isMounted()
    {
      return mounted;
    }

    @Override
    public void mount(StorageLocation location)
    {
      mounted = true;
    }

    @Override
    public void unmount()
    {
      unmountCalled = true;
      mounted = false;
    }
  }

  private static final class TestResizableCacheEntry implements ResizableCacheEntry
  {
    private final StringCacheIdentifier id;
    private long size;
    private boolean isMounted = false;

    private TestResizableCacheEntry(String id, long size)
    {
      this.id = new StringCacheIdentifier(id);
      this.size = size;
    }

    @Override
    public StringCacheIdentifier getId()
    {
      return id;
    }

    @Override
    public long getSize()
    {
      return size;
    }

    @Override
    public boolean isMounted()
    {
      return isMounted;
    }

    @Override
    public void mount(StorageLocation location)
    {
      isMounted = true;
    }

    @Override
    public void unmount()
    {
      isMounted = false;
    }

    @Override
    public void resizeReservation(long newSize)
    {
      this.size = newSize;
    }
  }

  public static final class StringCacheIdentifier implements CacheEntryIdentifier
  {
    private final String string;

    public StringCacheIdentifier(String string)
    {
      this.string = string;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringCacheIdentifier that = (StringCacheIdentifier) o;
      return Objects.equals(string, that.string);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(string);
    }
  }
}
