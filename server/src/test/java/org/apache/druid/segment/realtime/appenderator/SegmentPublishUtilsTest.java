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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SegmentPublishUtilsTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of(
      "datasource",
      new Interval("2020-01-01/2020-01-02"),
      "version",
      0
  );

  @Test
  public void testAdoptedGenerationIsRetained()
  {
    final DataSegment uploaded = segment("uploaded");
    final List<DataSegment> killed = new ArrayList<>();
    final DataSegmentKiller killer = killer(killed);
    Assertions.assertDoesNotThrow(
        () -> SegmentPublishUtils.cleanupSegmentsNotAdoptedBySuccessfulPublish(
            Set.of(uploaded),
            Set.of(uploaded),
            ids -> {
              throw new AssertionError("authoritative metadata must not be queried for an adopted generation");
            },
            killer
        )
    );
    Assertions.assertTrue(killed.isEmpty());
  }

  @Test
  public void testDifferentTransactionGenerationIsDeletedOnlyWhenAuthoritativeWinnerExists()
  {
    final DataSegment uploaded = segment("uploaded");
    final DataSegment winner = segment("winner");
    Assertions.assertEquals(List.of(uploaded), cleanup(Set.of(uploaded), Set.of(winner), Set.of(winner)));
  }

  @Test
  public void testDifferentTransactionGenerationWithoutAuthoritativeSegmentIsRetained()
  {
    final DataSegment uploaded = segment("uploaded");
    final DataSegment transactionWinner = segment("transaction-winner");
    Assertions.assertTrue(
        cleanup(Set.of(uploaded), Set.of(transactionWinner), Collections.emptySet()).isEmpty()
    );
  }

  @Test
  public void testMissingAuthoritativeSegmentIsRetained()
  {
    final DataSegment uploaded = segment("uploaded");
    Assertions.assertTrue(cleanup(Set.of(uploaded), Collections.emptySet(), Collections.emptySet()).isEmpty());
  }

  @Test
  public void testMetadataLookupFailureRetainsUploadedGeneration()
  {
    final DataSegment uploaded = segment("uploaded");
    final List<DataSegment> killed = new ArrayList<>();
    final DataSegmentKiller killer = new DataSegmentKiller()
    {
      @Override
      public void kill(DataSegment segment)
      {
        killed.add(segment);
      }

      @Override
      public void killAll()
      {
      }
    };
    Assertions.assertDoesNotThrow(
        () -> SegmentPublishUtils.cleanupSegmentsNotAdoptedBySuccessfulPublish(
            Set.of(uploaded),
            Collections.emptySet(),
            ids -> {
              throw new IOException("metadata unavailable");
            },
            killer
        )
    );
    Assertions.assertTrue(killed.isEmpty());
  }

  @Test
  public void testMultipleGenerationsOnlyDeleteNonAuthoritativeGeneration()
  {
    final DataSegment authoritative = segment("authoritative");
    final DataSegment other = segment("other");
    Assertions.assertEquals(List.of(other), cleanup(List.of(authoritative, other), List.of(authoritative), Set.of(authoritative)));
  }

  private static List<DataSegment> cleanup(
      Collection<DataSegment> uploaded,
      Collection<DataSegment> transactionResult,
      Set<DataSegment> authoritative
  )
  {
    final List<DataSegment> killed = new ArrayList<>();
    final DataSegmentKiller killer = killer(killed);
    SegmentPublishUtils.cleanupSegmentsNotAdoptedBySuccessfulPublish(
        uploaded,
        transactionResult,
        ids -> authoritative,
        killer
    );
    return killed;
  }

  private static DataSegmentKiller killer(List<DataSegment> killed)
  {
    return new DataSegmentKiller()
    {
      @Override
      public void kill(DataSegment segment)
      {
        killed.add(segment);
      }

      @Override
      public void killAll()
      {
      }
    };
  }

  private static DataSegment segment(String generation)
  {
    return DataSegment.builder(SEGMENT_ID)
                      .shardSpec(new NumberedShardSpec(0, 1))
                      .loadSpec(ImmutableMap.of("generation", generation))
                      .size(1)
                      .build();
  }
}
