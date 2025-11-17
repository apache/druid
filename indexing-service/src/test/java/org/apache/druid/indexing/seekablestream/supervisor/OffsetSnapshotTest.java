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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OffsetSnapshotTest
{
  @Test
  public void testOffsetSnapshot_emptyInputReturnsEmptyMap()
  {
    OffsetSnapshot<String, Long> snapshot = OffsetSnapshot.of(
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    Assert.assertTrue(snapshot.getHighestIngestedOffsets().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getHighestIngestedOffsets());
    Assert.assertTrue(snapshot.getLatestOffsetsFromStream().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getLatestOffsetsFromStream());
  }

  @Test
  public void testOffsetSnapshot_nullInputsReturnEmptyMaps()
  {
    OffsetSnapshot<Integer, Long> snapshot = OffsetSnapshot.of(null, null);

    Assert.assertTrue(snapshot.getHighestIngestedOffsets().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getHighestIngestedOffsets());
    Assert.assertTrue(snapshot.getLatestOffsetsFromStream().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getLatestOffsetsFromStream());
  }

  @Test
  public void testOffsetSnapshot_nullCurrentOffsetsReturnsEmptyCurrentMap()
  {
    Map<Integer, Long> endOffsets = ImmutableMap.of(0, 100L, 1, 200L);

    OffsetSnapshot<Integer, Long> snapshot = OffsetSnapshot.of(null, endOffsets);

    Assert.assertTrue(snapshot.getHighestIngestedOffsets().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getHighestIngestedOffsets());
    Assert.assertEquals(endOffsets, snapshot.getLatestOffsetsFromStream());
  }

  @Test
  public void testOffsetSnapshot_nullEndOffsetsReturnsEmptyEndMap()
  {
    Map<Integer, Long> currentOffsets = ImmutableMap.of(0, 50L, 1, 150L);

    OffsetSnapshot<Integer, Long> snapshot = OffsetSnapshot.of(currentOffsets, null);

    Assert.assertEquals(currentOffsets, snapshot.getHighestIngestedOffsets());
    Assert.assertTrue(snapshot.getLatestOffsetsFromStream().isEmpty());
    Assert.assertSame(ImmutableMap.of(), snapshot.getLatestOffsetsFromStream());
  }

  @Test
  public void testOffsetSnapshot_copiesInputMapsAndReturnsImmutableCopies()
  {
    Map<String, String> current = new HashMap<>();
    current.put("p0", "100");
    current.put("p1", "200");

    Map<String, String> end = new HashMap<>();
    end.put("p0", "150");
    end.put("p2", "300");

    OffsetSnapshot<String, String> snapshot = OffsetSnapshot.of(current, end);

    Assert.assertEquals(ImmutableMap.of("p0", "100", "p1", "200"), snapshot.getHighestIngestedOffsets());
    Assert.assertEquals(ImmutableMap.of("p0", "150", "p2", "300"), snapshot.getLatestOffsetsFromStream());

    // Returned maps must be immutable
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> snapshot.getHighestIngestedOffsets().put("x", "x")
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> snapshot.getLatestOffsetsFromStream().put("x", "x")
    );
  }

  @Test
  public void testOffsetSnapshot_filtersOutNullValues()
  {
    Map<Integer, Long> current = new HashMap<>();
    current.put(0, 100L);
    current.put(1, null);   // should be filtered
    current.put(2, 200L);

    Map<Integer, Long> end = new HashMap<>();
    end.put(0, null);       // should be filtered
    end.put(1, 300L);

    OffsetSnapshot<Integer, Long> snapshot = OffsetSnapshot.of(current, end);

    Assert.assertEquals(ImmutableMap.of(0, 100L, 2, 200L), snapshot.getHighestIngestedOffsets());
    Assert.assertEquals(2, snapshot.getHighestIngestedOffsets().size());

    Assert.assertEquals(ImmutableMap.of(1, 300L), snapshot.getLatestOffsetsFromStream());
    Assert.assertEquals(1, snapshot.getLatestOffsetsFromStream().size());
  }

  @Test
  public void testOffsetSnapshot_snapshotIndependentFromInputMapMutations()
  {
    Map<Integer, Long> inputCurrent = new HashMap<>();
    inputCurrent.put(1, 10L);
    Map<Integer, Long> inputEnd = new HashMap<>();
    inputEnd.put(2, 20L);

    OffsetSnapshot<Integer, Long> snapshot = OffsetSnapshot.of(inputCurrent, inputEnd);

    // Mutate inputs after snapshot creation
    inputCurrent.clear();
    inputEnd.put(999, 999L);

    Assert.assertEquals(ImmutableMap.of(1, 10L), snapshot.getHighestIngestedOffsets());
    Assert.assertEquals(ImmutableMap.of(2, 20L), snapshot.getLatestOffsetsFromStream());
  }
}
