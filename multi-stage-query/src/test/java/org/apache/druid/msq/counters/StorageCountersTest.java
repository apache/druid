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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageCountersTest
{
  @Test
  public void testSnapshotWithNoByteTracker()
  {
    final StorageCounters counters = new StorageCounters(null);
    final StorageCounters.Snapshot snapshot = (StorageCounters.Snapshot) counters.snapshot();

    Assertions.assertNull(snapshot.getLocalBytesMax());
    Assertions.assertEquals(0, snapshot.getLocalBytesReserved());
    Assertions.assertEquals(0, snapshot.getLocalFilesWritten());
    Assertions.assertEquals(0, snapshot.getLocalBytesWritten());
    Assertions.assertEquals(0, snapshot.getDurableFileCount());
    Assertions.assertEquals(0, snapshot.getDurableBytesWritten());
  }

  @Test
  public void testSnapshotWithByteTracker()
  {
    final ByteTracker tracker = new ByteTracker(1000);
    tracker.reserve(300);

    final StorageCounters counters = new StorageCounters(tracker);

    final StorageCounters.Snapshot snapshot = (StorageCounters.Snapshot) counters.snapshot();
    Assertions.assertEquals(1000L, (long) snapshot.getLocalBytesMax());
    Assertions.assertEquals(300, snapshot.getLocalBytesReserved());
  }

  @Test
  public void testSnapshotWithByteTrackerReflectsCurrentState()
  {
    final ByteTracker tracker = new ByteTracker(1000);
    tracker.reserve(300);

    final StorageCounters counters = new StorageCounters(tracker);

    // Take first snapshot
    final StorageCounters.Snapshot snapshot1 = (StorageCounters.Snapshot) counters.snapshot();
    Assertions.assertEquals(300, snapshot1.getLocalBytesReserved());

    // Change tracker state
    tracker.reserve(200);

    // Second snapshot reflects new state
    final StorageCounters.Snapshot snapshot2 = (StorageCounters.Snapshot) counters.snapshot();
    Assertions.assertEquals(500, snapshot2.getLocalBytesReserved());
  }

  @Test
  public void testIncrementLocalFiles()
  {
    final StorageCounters counters = new StorageCounters(null);
    counters.incrementLocalFiles();
    counters.incrementLocalBytes(500);
    counters.incrementLocalFiles();
    counters.incrementLocalBytes(300);

    final StorageCounters.Snapshot snapshot = (StorageCounters.Snapshot) counters.snapshot();
    Assertions.assertEquals(2, snapshot.getLocalFilesWritten());
    Assertions.assertEquals(800, snapshot.getLocalBytesWritten());
    Assertions.assertEquals(0, snapshot.getDurableFileCount());
    Assertions.assertEquals(0, snapshot.getDurableBytesWritten());
  }

  @Test
  public void testIncrementDurableFiles()
  {
    final StorageCounters counters = new StorageCounters(null);
    counters.incrementDurableFiles();
    counters.incrementDurableBytes(1000);
    counters.incrementDurableFiles();
    counters.incrementDurableBytes(2000);

    final StorageCounters.Snapshot snapshot = (StorageCounters.Snapshot) counters.snapshot();
    Assertions.assertEquals(0, snapshot.getLocalFilesWritten());
    Assertions.assertEquals(0, snapshot.getLocalBytesWritten());
    Assertions.assertEquals(2, snapshot.getDurableFileCount());
    Assertions.assertEquals(3000, snapshot.getDurableBytesWritten());
  }

  @Test
  public void testSnapshotSerde() throws Exception
  {
    final ObjectMapper mapper =
        TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

    final StorageCounters.Snapshot snapshot = new StorageCounters.Snapshot(1000L, 300, 5, 2500, 3, 1500);

    final String json = mapper.writeValueAsString(snapshot);
    final StorageCounters.Snapshot deserialized = mapper.readValue(json, StorageCounters.Snapshot.class);

    Assertions.assertEquals(snapshot, deserialized);
  }

  @Test
  public void testSnapshotSerdeViaCounterSnapshots() throws Exception
  {
    final ObjectMapper mapper =
        TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

    final StorageCounters.Snapshot snapshot = new StorageCounters.Snapshot(1000L, 300, 5, 2500, 3, 1500);
    final CounterSnapshotsTree tree = new CounterSnapshotsTree();
    tree.put(0, 0, new CounterSnapshots(ImmutableMap.of(CounterNames.storage(), snapshot)));

    final String json = mapper.writeValueAsString(tree);
    final CounterSnapshotsTree deserialized = mapper.readValue(json, CounterSnapshotsTree.class);

    Assertions.assertEquals(
        snapshot,
        deserialized.copyMap().get(0).get(0).getMap().get(CounterNames.storage())
    );
  }

  @Test
  public void testCounterTrackerDefensiveCheck()
  {
    final ByteTracker tracker1 = new ByteTracker(1000);
    final ByteTracker tracker2 = new ByteTracker(2000);

    final CounterTracker counterTracker = new CounterTracker(true);

    // First call creates the counter
    final StorageCounters counters = counterTracker.storage(tracker1);
    Assertions.assertSame(tracker1, counters.getLocalByteTracker());

    // Second call with same tracker succeeds
    Assertions.assertSame(counters, counterTracker.storage(tracker1));

    // Call with different tracker fails
    Assertions.assertThrows(IllegalStateException.class, () -> counterTracker.storage(tracker2));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StorageCounters.Snapshot.class)
                  .usingGetClass()
                  .verify();
  }
}
