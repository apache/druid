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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.timeline.partition.OvershadowableManager.RootPartitionRange;
import org.apache.druid.timeline.partition.OvershadowableManager.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OvershadowableManagerTest
{
  private static final String MAJOR_VERSION = "version";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private OvershadowableManager<OvershadowableInteger> manager;
  private int nextRootPartitionId;
  private int nextNonRootPartitionId;
  private List<PartitionChunk<OvershadowableInteger>> expectedVisibleChunks;
  private List<PartitionChunk<OvershadowableInteger>> expectedOvershadowedChunks;
  private List<PartitionChunk<OvershadowableInteger>> expectedStandbyChunks;

  @Before
  public void setup()
  {
    manager = new OvershadowableManager<>();
    nextRootPartitionId = PartitionIds.ROOT_GEN_START_PARTITION_ID;
    nextNonRootPartitionId = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID;
    expectedVisibleChunks = new ArrayList<>();
    expectedOvershadowedChunks = new ArrayList<>();
    expectedStandbyChunks = new ArrayList<>();
  }

  @Test
  public void testCopyVisible()
  {
    // chunks of partition id 0 and 1
    manager.addChunk(newRootChunk());
    manager.addChunk(newRootChunk());

    // chunks to overshadow the partition id range [0, 2)
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));

    // chunks of partition id 3 and 4
    manager.addChunk(newRootChunk());
    manager.addChunk(newRootChunk());

    // standby chunk
    manager.addChunk(newNonRootChunk(2, 4, 1, 3));

    OvershadowableManager<OvershadowableInteger> copy = OvershadowableManager.copyVisible(manager);
    Assert.assertTrue(copy.getOvershadowedChunks().isEmpty());
    Assert.assertTrue(copy.getStandbyChunks().isEmpty());
    Assert.assertEquals(
        Lists.newArrayList(manager.visibleChunksIterator()),
        Lists.newArrayList(copy.visibleChunksIterator())
    );
  }

  @Test
  public void testDeepCopy()
  {
    // chunks of partition id 0 and 1
    manager.addChunk(newRootChunk());
    manager.addChunk(newRootChunk());

    // chunks to overshadow the partition id range [0, 2)
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));
    manager.addChunk(newNonRootChunk(0, 2, 1, 3));

    // chunks of partition id 3 and 4
    manager.addChunk(newRootChunk());
    manager.addChunk(newRootChunk());

    // standby chunk
    manager.addChunk(newNonRootChunk(2, 4, 1, 3));

    OvershadowableManager<OvershadowableInteger> copy = OvershadowableManager.deepCopy(manager);
    Assert.assertEquals(manager, copy);
  }

  @Test
  public void testEqualAndHashCodeContract()
  {
    EqualsVerifier.forClass(OvershadowableManager.class).usingGetClass().verify();
  }

  @Test
  public void testFindOvershadowedBy()
  {
    final List<PartitionChunk<OvershadowableInteger>> expectedOvershadowedChunks = new ArrayList<>();

    // All chunks except the last one are in the overshadowed state
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 2, 1, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(0, 3, 2, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(0, 5, 3, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(5, 8, 1, 1);
    expectedOvershadowedChunks.add(chunk);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(8, 11, 2, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(5, 11, 3, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(0, 12, 5, 1);
    manager.addChunk(chunk);

    List<AtomicUpdateGroup<OvershadowableInteger>> overshadowedGroups = manager.findOvershadowedBy(
        RootPartitionRange.of(2, 10),
        (short) 10,
        State.OVERSHADOWED
    );
    Assert.assertEquals(
        expectedOvershadowedChunks.stream().map(AtomicUpdateGroup::new).collect(Collectors.toList()),
        overshadowedGroups
    );

    overshadowedGroups = manager.findOvershadowedBy(
        RootPartitionRange.of(2, 10),
        (short) 10,
        State.VISIBLE
    );
    Assert.assertEquals(
        Collections.emptyList(),
        overshadowedGroups
    );
  }

  @Test
  public void testFindOvershadows()
  {
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(2, 6, 3, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(6, 8, 3, 1);
    manager.addChunk(chunk);
    chunk = newNonRootChunk(1, 8, 4, 1);
    final PartitionChunk<OvershadowableInteger> visibleChunk = chunk;
    manager.addChunk(chunk);

    List<AtomicUpdateGroup<OvershadowableInteger>> overshadowingGroups = manager.findOvershadows(
        RootPartitionRange.of(1, 3),
        (short) 1,
        State.OVERSHADOWED
    );
    Assert.assertEquals(
        Collections.emptyList(),
        overshadowingGroups
    );
    overshadowingGroups = manager.findOvershadows(
        RootPartitionRange.of(1, 3),
        (short) 1,
        State.VISIBLE
    );
    Assert.assertEquals(
        ImmutableList.of(new AtomicUpdateGroup<>(visibleChunk)),
        overshadowingGroups
    );

    overshadowingGroups = manager.findOvershadows(
        RootPartitionRange.of(4, 7),
        (short) 1,
        State.OVERSHADOWED
    );
    Assert.assertEquals(
        Collections.emptyList(),
        overshadowingGroups
    );
    overshadowingGroups = manager.findOvershadows(
        RootPartitionRange.of(4, 7),
        (short) 1,
        State.VISIBLE
    );
    Assert.assertEquals(
        ImmutableList.of(new AtomicUpdateGroup<>(visibleChunk)),
        overshadowingGroups
    );
  }

  @Test
  public void testAddRootChunkToEmptyManager()
  {
    Assert.assertTrue(manager.isEmpty());
    // Add a new one
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    Assert.assertTrue(manager.isComplete());
    // Add a duplicate
    Assert.assertFalse(manager.addChunk(chunk));
    // Add a new one
    chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    Assert.assertTrue(manager.isComplete());
  }

  @Test
  public void testAddNonRootChunkToEmptyManager()
  {
    Assert.assertTrue(manager.isEmpty());
    // Add a new one, atomicUpdateGroup is not full
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(10, 12, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    Assert.assertFalse(manager.isComplete());
    // Add a new one, atomicUpdateGroup is still not full
    chunk = newNonRootChunk(10, 12, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    Assert.assertFalse(manager.isComplete());
    // Add a new one, now atomicUpdateGroup is full
    chunk = newNonRootChunk(10, 12, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    Assert.assertTrue(manager.isComplete());

    // Add a new one to the full group
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Can't add chunk");
    chunk = newNonRootChunk(10, 12, 1, 3);
    addVisibleToManager(chunk);
  }

  @Test
  public void testRemoveFromEmptyManager()
  {
    Assert.assertTrue(manager.isEmpty());
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertNull(manager.removeChunk(chunk));
  }

  @Test
  public void testAddOvershadowedChunkToCompletePartition()
  {
    // Start with a non-root incomplete partitionChunk
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 3, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a visible root chunk, now this group is complete
    chunk = newNonRootChunk(0, 3, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add an overshadowed chunk
    nextRootPartitionId = 1;
    chunk = newRootChunk();
    Assert.assertTrue(manager.addChunk(chunk));
    expectedOvershadowedChunks.add(chunk);
    assertManagerState();
  }

  @Test
  public void testAddOvershadowedChunkToIncompletePartition()
  {
    // Start with a non-root partitionChunk. This group is incomplete.
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 3, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add an overshadowed chunk
    nextRootPartitionId = 1;
    chunk = newRootChunk();
    expectedOvershadowedChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testAddStandbyChunksToCompletePartition()
  {
    // Add complete chunks
    PartitionChunk<OvershadowableInteger> chunk;
    for (int i = 0; i < 3; i++) {
      chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
      assertManagerState();
    }

    // Add a chunk of an incomplete group
    chunk = newNonRootChunk(0, 3, 1, 2);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    // This group is now full
    chunk = newNonRootChunk(0, 3, 1, 2);
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    expectedVisibleChunks.addAll(expectedStandbyChunks);
    expectedStandbyChunks.clear();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
  }

  @Test
  public void testAddStandbyChunksToIncompletePartition()
  {
    // Add a chunk of an incomplete group
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 3, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add chunks of an incomplete group overshadowing the previous one
    chunk = newNonRootChunk(0, 3, 2, 3);
    expectedOvershadowedChunks.add(expectedVisibleChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    chunk = newNonRootChunk(0, 3, 2, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveUnknownChunk()
  {
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    chunk = newRootChunk();
    Assert.assertNull(manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveChunksUntilEmpty()
  {
    PartitionChunk<OvershadowableInteger> chunk;
    for (int i = 0; i < 10; i++) {
      chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
      assertManagerState();
    }

    while (expectedVisibleChunks.size() > 0) {
      chunk = expectedVisibleChunks.remove(ThreadLocalRandom.current().nextInt(expectedVisibleChunks.size()));
      Assert.assertEquals(chunk, manager.removeChunk(chunk));
      assertManagerState();
    }

    Assert.assertTrue(manager.isEmpty());
  }

  @Test
  public void testRemoveStandbyChunk()
  {
    // Add complete groups
    PartitionChunk<OvershadowableInteger> chunk;
    for (int i = 0; i < 3; i++) {
      chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
      assertManagerState();
    }

    // Add two chunks of an incomplete group overshadowing the previous one
    chunk = newNonRootChunk(0, 3, 1, 3);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
    chunk = newNonRootChunk(0, 3, 1, 3);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    // Remove one standby chunk
    chunk = expectedStandbyChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveVisibleChunkAndFallBackToStandby()
  {
    // Add two complete groups
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add two chunks of an incomplete group
    chunk = newNonRootChunk(0, 2, 1, 3);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
    chunk = newNonRootChunk(0, 2, 1, 3);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    // Remove a chunk of the incomplete group
    chunk = expectedVisibleChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    expectedVisibleChunks.addAll(expectedStandbyChunks);
    expectedStandbyChunks.clear();
    assertManagerState();
  }

  @Test
  public void testAddCompleteOvershadowedToInCompletePartition()
  {
    // Add an incomplete group
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 2, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    chunk = newNonRootChunk(0, 2, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a complete overshadowed group
    chunk = newRootChunk();
    expectedOvershadowedChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
    chunk = newRootChunk();
    expectedStandbyChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    expectedVisibleChunks.add(expectedOvershadowedChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
  }

  @Test
  public void testAddCompleteOvershadowedToCompletePartition()
  {
    // Add a complete group
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 2, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    chunk = newNonRootChunk(0, 2, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a complete overshadowed group
    chunk = newRootChunk();
    expectedOvershadowedChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
    chunk = newRootChunk();
    expectedOvershadowedChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveChunkFromOvershadowd()
  {
    // Add a complete group
    nextRootPartitionId = 1;
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add an incomplete group of a larger partition range
    chunk = newNonRootChunk(0, 2, 1, 2);
    expectedOvershadowedChunks.add(expectedVisibleChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Remove an overshadowed chunk
    chunk = expectedOvershadowedChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveChunkFromCompleteParition()
  {
    // Add a complete group
    nextRootPartitionId = 1;
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a complete group overshadowing the previous
    chunk = newNonRootChunk(0, 2, 1, 2);
    expectedOvershadowedChunks.add(expectedVisibleChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    chunk = newNonRootChunk(0, 2, 1, 2);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Remove a chunk from the visible group
    chunk = expectedVisibleChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();

    // Remove another chunk from the visible group. Now the overshadowed group should be visible.
    chunk = expectedVisibleChunks.remove(0);
    expectedVisibleChunks.addAll(expectedOvershadowedChunks);
    expectedOvershadowedChunks.clear();
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testRemoveChunkFromCompletePartitionFallBackToOvershadowed()
  {
    // Add complete groups
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
    chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a complete group overshadowing the previous
    chunk = newNonRootChunk(0, 2, 1, 2);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    chunk = newNonRootChunk(0, 2, 1, 2);
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    expectedVisibleChunks.add(expectedStandbyChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Remove a visible chunk. Should fall back to the complete overshadowed group.
    chunk = expectedVisibleChunks.remove(0);
    expectedStandbyChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    expectedVisibleChunks.addAll(expectedOvershadowedChunks);
    expectedOvershadowedChunks.clear();
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testAddCompleteOvershadowedToCompletePartition2()
  {
    // Add overshadowed incomplete groups
    List<PartitionChunk<OvershadowableInteger>> chunks = newNonRootChunks(2, 0, 2, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 2, 5, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 5, 8, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 8, 10, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 5, 2, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 8, 3, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Add a visible complete group
    chunks = newNonRootChunks(2, 0, 10, 4, 2);
    expectedVisibleChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Add a standby incomplete group
    chunks = newNonRootChunks(1, 0, 10, 5, 2);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    assertManagerState();

    // Add a chunk to complete the second overshadowed group
    chunks = newNonRootChunks(1, 0, 5, 2, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(1, 5, 8, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(1, 8, 10, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    assertManagerState();

    // Remove a chunk from the visible group
    PartitionChunk<OvershadowableInteger> chunkToRemove = expectedVisibleChunks.remove(0);
    expectedStandbyChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    Iterator<PartitionChunk<OvershadowableInteger>> iterator = expectedOvershadowedChunks.iterator();
    while (iterator.hasNext()) {
      final PartitionChunk<OvershadowableInteger> chunk = iterator.next();
      if (chunk.getObject().getStartRootPartitionId() == 0 && chunk.getObject().getMinorVersion() == 2
          || chunk.getObject().getStartRootPartitionId() == 5 && chunk.getObject().getMinorVersion() == 1
          || chunk.getObject().getStartRootPartitionId() == 8 && chunk.getObject().getMinorVersion() == 1) {
        expectedVisibleChunks.add(chunk);
        iterator.remove();
      } else if (chunk.getObject().getStartRootPartitionId() == 0 && chunk.getObject().getMinorVersion() > 2
                 || chunk.getObject().getStartRootPartitionId() == 5 && chunk.getObject().getMinorVersion() > 1
                 || chunk.getObject().getStartRootPartitionId() == 8 && chunk.getObject().getMinorVersion() > 1) {
        expectedStandbyChunks.add(chunk);
        iterator.remove();
      }
    }
    Assert.assertEquals(chunkToRemove, manager.removeChunk(chunkToRemove));
    assertManagerState();
  }

  @Test
  public void testAddCompleteStandbyToCompletePartition()
  {
    // Add overshadowed groups
    List<PartitionChunk<OvershadowableInteger>> chunks = newNonRootChunks(2, 0, 2, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 2, 5, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 5, 8, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 8, 10, 1, 2);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Visible group for [0, 5)
    chunks = newNonRootChunks(2, 0, 5, 2, 2);
    expectedVisibleChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    // Visible group for [5, 10)
    chunks = newNonRootChunks(2, 5, 10, 2, 2);
    expectedVisibleChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Standby groups
    chunks = newNonRootChunks(2, 0, 5, 3, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);
    chunks = newNonRootChunks(2, 5, 10, 3, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 5, 4, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 10, 5, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    assertManagerState();

    // Add a chunk to complete the second standby group
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    chunks = newNonRootChunks(1, 0, 5, 4, 3);
    chunks.forEach(this::addVisibleToManager);
    chunks = newNonRootChunks(1, 5, 10, 3, 3);
    chunks.forEach(this::addVisibleToManager);

    Iterator<PartitionChunk<OvershadowableInteger>> iterator = expectedStandbyChunks.iterator();
    while (iterator.hasNext()) {
      final PartitionChunk<OvershadowableInteger> chunk = iterator.next();
      if (chunk.getObject().getStartRootPartitionId() == 0 && chunk.getObject().getMinorVersion() == 4
          || chunk.getObject().getStartRootPartitionId() == 5 && chunk.getObject().getMinorVersion() == 3) {
        expectedVisibleChunks.add(chunk);
        iterator.remove();
      } else if (chunk.getObject().getStartRootPartitionId() == 0 && chunk.getObject().getMinorVersion() < 4
                 || chunk.getObject().getStartRootPartitionId() == 5 && chunk.getObject().getMinorVersion() < 3) {
        expectedOvershadowedChunks.add(chunk);
        iterator.remove();
      }
    }

    assertManagerState();
  }

  @Test
  public void testFallBackToStandby2()
  {
    // Add an overshadowed incomplete group
    List<PartitionChunk<OvershadowableInteger>> chunks = newNonRootChunks(2, 0, 2, 1, 3);
    expectedOvershadowedChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Add a visible complete group
    chunks = newNonRootChunks(2, 0, 2, 2, 2);
    expectedVisibleChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    // Add three standby incomplete groups
    chunks = newNonRootChunks(2, 0, 2, 3, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 2, 4, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    chunks = newNonRootChunks(2, 0, 2, 5, 3);
    expectedStandbyChunks.addAll(chunks);
    chunks.forEach(manager::addChunk);

    assertManagerState();

    // Remove a visible chunk. The latest standby group should be visible.
    PartitionChunk<OvershadowableInteger> chunkToRemove = expectedVisibleChunks.remove(0);
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    Iterator<PartitionChunk<OvershadowableInteger>> iterator = expectedStandbyChunks.iterator();
    while (iterator.hasNext()) {
      final PartitionChunk<OvershadowableInteger> chunk = iterator.next();
      if (chunk.getObject().getMinorVersion() == 5) {
        expectedVisibleChunks.add(chunk);
        iterator.remove();
      } else {
        expectedOvershadowedChunks.add(chunk);
        iterator.remove();
      }
    }

    Assert.assertEquals(chunkToRemove, manager.removeChunk(chunkToRemove));
    assertManagerState();
  }

  @Test
  public void testAddAndOverwriteAndAdd()
  {
    // Start with root partitionChunks
    for (int i = 0; i < 5; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    assertManagerState();

    // Overwrite some partitionChunks with a higher minor version
    final int rootStartPartitionIdToOverwrite = expectedVisibleChunks.get(1).getChunkNumber();
    final int rootEndPartitionIdToOverwrite = expectedVisibleChunks.get(3).getChunkNumber();
    for (int i = 0; i < 2; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(
          rootStartPartitionIdToOverwrite,
          rootEndPartitionIdToOverwrite,
          3,
          2
      );
      Assert.assertTrue(manager.addChunk(chunk));
      if (i == 0) {
        expectedStandbyChunks.add(chunk);
      }
      if (i == 1) {
        expectedOvershadowedChunks.addAll(expectedVisibleChunks.subList(1, 3));
        expectedVisibleChunks.subList(1, 3).clear();
        expectedVisibleChunks.addAll(expectedStandbyChunks);
        expectedVisibleChunks.add(chunk);
        expectedStandbyChunks.clear();
      }
      assertManagerState();
    }

    // Append new visible chunks
    for (int i = 0; i < 3; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    assertManagerState();

    // Append complete overshadowed chunks
    for (int i = 0; i < 2; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(
          rootStartPartitionIdToOverwrite,
          rootEndPartitionIdToOverwrite,
          2,
          2
      );
      expectedOvershadowedChunks.add(chunk);
      Assert.assertTrue(manager.addChunk(chunk));
      assertManagerState();
    }
  }

  @Test
  public void testRemoveOvershadowed()
  {
    // Start with root partitionChunks
    for (int i = 0; i < 5; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
    }

    // Overwrite some partitionChunks with a higher minor version
    final int rootStartPartitionIdToOverwrite = expectedVisibleChunks.get(1).getChunkNumber();
    final int rootEndPartitionIdToOverwrite = expectedVisibleChunks.get(3).getChunkNumber();
    for (int i = 0; i < 2; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(
          rootStartPartitionIdToOverwrite,
          rootEndPartitionIdToOverwrite,
          1,
          2
      );
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    expectedOvershadowedChunks.addAll(expectedVisibleChunks.subList(1, 3));
    IntStream.range(0, 2).forEach(i -> expectedVisibleChunks.remove(1));
    assertManagerState();

    // Remove an overshadowed chunk
    PartitionChunk<OvershadowableInteger> chunk = expectedOvershadowedChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();

    // Remove a chunk overshadows others
    for (PartitionChunk<OvershadowableInteger> visibleChunk : expectedVisibleChunks) {
      if (visibleChunk.getChunkNumber() >= PartitionIds.NON_ROOT_GEN_START_PARTITION_ID) {
        Assert.assertEquals(visibleChunk, removeVisibleFromManager(visibleChunk));
        break;
      }
    }
    assertManagerState();
  }

  @Test
  public void testRemoveOvershadowingVisible()
  {
    // Start with root partitionChunks
    for (int i = 0; i < 5; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
      Assert.assertTrue(addVisibleToManager(chunk));
    }

    // Overwrite some partitionChunks with a higher minor version
    final int rootStartPartitionIdToOverwrite = expectedVisibleChunks.get(1).getChunkNumber();
    final int rootEndPartitionIdToOverwrite = expectedVisibleChunks.get(3).getChunkNumber();
    for (int i = 0; i < 2; i++) {
      PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(
          rootStartPartitionIdToOverwrite,
          rootEndPartitionIdToOverwrite,
          1,
          2
      );
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    expectedOvershadowedChunks.addAll(expectedVisibleChunks.subList(1, 3));
    IntStream.range(0, 2).forEach(i -> expectedVisibleChunks.remove(1));
    assertManagerState();

    // Remove a chunk overshadows others
    boolean removed = false;
    final Iterator<PartitionChunk<OvershadowableInteger>> iterator = expectedVisibleChunks.iterator();
    while (iterator.hasNext()) {
      final PartitionChunk<OvershadowableInteger> visibleChunk = iterator.next();
      if (visibleChunk.getChunkNumber() >= PartitionIds.NON_ROOT_GEN_START_PARTITION_ID) {
        iterator.remove();
        if (!removed) {
          manager.removeChunk(visibleChunk);
          removed = true;
        } else {
          expectedStandbyChunks.add(visibleChunk);
        }
      }
    }
    expectedVisibleChunks.addAll(expectedOvershadowedChunks);
    expectedOvershadowedChunks.clear();
    assertManagerState();
  }

  @Test
  public void testFallBackToStandbyOnRemove()
  {
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a chunk of an incomplete atomicUpdateGroup
    chunk = newNonRootChunk(0, 1, 1, 3);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    // Add a chunk of an incomplete atomicUpdateGroup which overshadows the previous one
    chunk = newNonRootChunk(0, 1, 2, 2);
    expectedStandbyChunks.add(chunk);
    Assert.assertTrue(manager.addChunk(chunk));
    assertManagerState();

    // Remove the visible chunk
    chunk = expectedVisibleChunks.remove(0);
    expectedVisibleChunks.add(expectedStandbyChunks.remove(1));
    expectedOvershadowedChunks.add(expectedStandbyChunks.remove(0));
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testFallBackToOvershadowedOnRemove()
  {
    PartitionChunk<OvershadowableInteger> chunk;
    // Add incomplete non-root group
    for (int i = 0; i < 2; i++) {
      chunk = newNonRootChunk(10, 20, 5, 3);
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    assertManagerState();

    // Add incomplete non-root group overshadowed by the previous one
    for (int i = 0; i < 2; i++) {
      chunk = newNonRootChunk(10, 20, 4, 3);
      expectedOvershadowedChunks.add(chunk);
      Assert.assertTrue(manager.addChunk(chunk));
      chunk = newNonRootChunk(10, 20, 3, 3);
      expectedOvershadowedChunks.add(chunk);
      Assert.assertTrue(manager.addChunk(chunk));
    }
    assertManagerState();

    // Remove the visible group one by one
    chunk = expectedVisibleChunks.remove(0);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();

    chunk = expectedVisibleChunks.remove(0);
    expectedOvershadowedChunks
        .stream()
        .filter(c -> c.getObject().getMinorVersion() == 4)
        .forEach(c -> expectedVisibleChunks.add(c));
    expectedOvershadowedChunks.removeAll(expectedVisibleChunks);
    Assert.assertEquals(chunk, manager.removeChunk(chunk));
    assertManagerState();
  }

  @Test
  public void testAddIncompleteAtomicUpdateGroups()
  {
    // Add an incomplete chunk
    PartitionChunk<OvershadowableInteger> chunk = newNonRootChunk(0, 1, 1, 3);
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add an incomplete chunk overshadowing the previous one. The atomicUpdateGroup of this chunk
    // will be complete later in this test.
    chunk = newNonRootChunk(0, 1, 2, 2);
    expectedOvershadowedChunks.add(expectedVisibleChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add an incomplete chunk overshadowing the previous one
    chunk = newNonRootChunk(0, 1, 3, 5);
    expectedOvershadowedChunks.add(expectedVisibleChunks.remove(0));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a chunk to complete the second atomicUpdateGroup overshadowed by the previous one
    chunk = newNonRootChunk(0, 1, 2, 2);
    expectedStandbyChunks.add(expectedVisibleChunks.remove(0));
    expectedVisibleChunks.add(expectedOvershadowedChunks.remove(expectedOvershadowedChunks.size() - 1));
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();
  }

  @Test
  public void testMissingStartRootPartitionId()
  {
    // Simulate the first two chunks are missing at the root level
    nextRootPartitionId = 2;
    PartitionChunk<OvershadowableInteger> chunk = newRootChunk();
    Assert.assertTrue(addVisibleToManager(chunk));
    assertManagerState();

    // Add a new group overshadows the previous one
    expectedOvershadowedChunks.addAll(expectedVisibleChunks);
    expectedVisibleChunks.clear();
    for (int i = 0; i < 2; i++) {
      chunk = newNonRootChunk(0, 3, 1, 2);
      Assert.assertTrue(addVisibleToManager(chunk));
    }
    assertManagerState();

    // Remove the visible group
    for (int i = 0; i < 2; i++) {
      chunk = expectedVisibleChunks.remove(0);
      Assert.assertEquals(chunk, manager.removeChunk(chunk));
    }
    expectedVisibleChunks.addAll(expectedOvershadowedChunks);
    expectedOvershadowedChunks.clear();
    assertManagerState();
  }

  private boolean addVisibleToManager(PartitionChunk<OvershadowableInteger> chunk)
  {
    expectedVisibleChunks.add(chunk);
    return manager.addChunk(chunk);
  }

  private PartitionChunk<OvershadowableInteger> removeVisibleFromManager(PartitionChunk<OvershadowableInteger> chunk)
  {
    expectedVisibleChunks.remove(chunk);
    return manager.removeChunk(chunk);
  }

  private void assertManagerState()
  {
    Assert.assertEquals(
        "Mismatched visible chunks",
        new HashSet<>(expectedVisibleChunks),
        Sets.newHashSet(manager.visibleChunksIterator())
    );
    Assert.assertEquals(
        "Mismatched overshadowed chunks",
        new HashSet<>(expectedOvershadowedChunks),
        new HashSet<>(manager.getOvershadowedChunks())
    );
    Assert.assertEquals(
        "Mismatched standby chunks",
        new HashSet<>(expectedStandbyChunks),
        new HashSet<>(manager.getStandbyChunks())
    );
  }

  private List<PartitionChunk<OvershadowableInteger>> newNonRootChunks(
      int n,
      int startPartitionId,
      int endPartitionId,
      int minorVersion,
      int atomicUpdateGroupSize
  )
  {
    return IntStream
        .range(0, n)
        .mapToObj(i -> newNonRootChunk(startPartitionId, endPartitionId, minorVersion, atomicUpdateGroupSize))
        .collect(Collectors.toList());
  }

  private NumberedPartitionChunk<OvershadowableInteger> newRootChunk()
  {
    final int partitionId = nextRootPartitionId();
    return new NumberedPartitionChunk<>(partitionId, 0, new OvershadowableInteger(MAJOR_VERSION, partitionId, 0));
  }

  private NumberedOverwritingPartitionChunk<OvershadowableInteger> newNonRootChunk(
      int startRootPartitionId,
      int endRootPartitionId,
      int minorVersion,
      int atomicUpdateGroupSize
  )
  {
    final int partitionId = nextNonRootPartitionId();
    return new NumberedOverwritingPartitionChunk<>(
        partitionId,
        new OvershadowableInteger(
            MAJOR_VERSION,
            partitionId,
            0,
            startRootPartitionId,
            endRootPartitionId,
            minorVersion,
            atomicUpdateGroupSize
        )
    );
  }

  private int nextRootPartitionId()
  {
    return nextRootPartitionId++;
  }

  private int nextNonRootPartitionId()
  {
    return nextNonRootPartitionId++;
  }
}
