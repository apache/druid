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
    Assertions.assertFalse(location.isWeakReserved(entry1.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
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
    Assertions.assertFalse(location.isWeakReserved(entry1.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
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
    Assertions.assertFalse(location.isWeakReserved(entry1.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
    Assertions.assertEquals(0, location.currentSizeBytes());
    Assertions.assertEquals(0, location.currentWeakSizeBytes());
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
    Assertions.assertTrue(location.reserveWeak(entry3));
    Assertions.assertTrue(location.reserveWeak(entry4));

    Assertions.assertEquals(100, location.currentWeakSizeBytes());
    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry3.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry4.getId()));

    Assertions.assertNotNull(closer.register(location.addWeakReservationHold(entry5.getId(), () -> entry5)));

    Assertions.assertEquals(100, location.currentWeakSizeBytes());

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

    Assertions.assertTrue(location.reserveWeak(entry7));

    Assertions.assertTrue(location.isWeakReserved(entry1.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry2.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry3.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry4.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry5.getId()));
    Assertions.assertFalse(location.isWeakReserved(entry6.getId()));
    Assertions.assertTrue(location.isWeakReserved(entry7.getId()));

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
    entry.mount(loc.getPath());

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

    Assertions.assertEquals(0, loc.getActiveWeakHolds());
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
    public void mount(File location)
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
    public void mount(File location)
    {
      // do nothing
    }

    @Override
    public void unmount()
    {
      // do nothing
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
