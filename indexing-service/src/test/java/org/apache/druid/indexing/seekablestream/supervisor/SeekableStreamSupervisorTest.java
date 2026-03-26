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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SeekableStreamSupervisorTest
{
  @Test
  public void testCalculateBackfillRange_withAllOffsetsPresent()
  {
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    Map<String, Long> startOffsets = ImmutableMap.of(
        "partition0", 100L,
        "partition1", 200L,
        "partition2", 300L
    );

    Map<String, Long> endOffsets = ImmutableMap.of(
        "partition0", 150L,
        "partition1", 250L,
        "partition2", 350L
    );

    Map<String, Object> result = supervisor.calculateBackfillRange(startOffsets, endOffsets);

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(
        ImmutableMap.of("start", 100L, "end", 150L),
        result.get("partition0")
    );
    Assert.assertEquals(
        ImmutableMap.of("start", 200L, "end", 250L),
        result.get("partition1")
    );
    Assert.assertEquals(
        ImmutableMap.of("start", 300L, "end", 350L),
        result.get("partition2")
    );
  }

  @Test
  public void testCalculateBackfillRange_withMissingStartOffsets()
  {
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    // Only partition0 has a checkpoint
    Map<String, Long> startOffsets = ImmutableMap.of(
        "partition0", 100L
    );

    Map<String, Long> endOffsets = ImmutableMap.of(
        "partition0", 150L,
        "partition1", 250L,
        "partition2", 350L
    );

    Map<String, Object> result = supervisor.calculateBackfillRange(startOffsets, endOffsets);

    Assert.assertEquals(3, result.size());

    // Partition 0 has both offsets
    Assert.assertEquals(
        ImmutableMap.of("start", 100L, "end", 150L),
        result.get("partition0")
    );

    // Partition 1 and 2 have no checkpoint
    Map<?, ?> partition1Result = (Map<?, ?>) result.get("partition1");
    Assert.assertEquals("none", partition1Result.get("start"));
    Assert.assertEquals(250L, partition1Result.get("end"));
    Assert.assertEquals("No committed offset found for this partition", partition1Result.get("note"));

    Map<?, ?> partition2Result = (Map<?, ?>) result.get("partition2");
    Assert.assertEquals("none", partition2Result.get("start"));
    Assert.assertEquals(350L, partition2Result.get("end"));
    Assert.assertEquals("No committed offset found for this partition", partition2Result.get("note"));
  }

  @Test
  public void testCalculateBackfillRange_withNullStartOffsets()
  {
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();

    Map<String, Long> endOffsets = ImmutableMap.of(
        "partition0", 150L,
        "partition1", 250L,
        "partition2", 350L
    );

    Map<String, Object> result = supervisor.calculateBackfillRange(null, endOffsets);

    Assert.assertEquals(3, result.size());
    for (Map.Entry<String, Long> entry : endOffsets.entrySet()) {
      Map<?, ?> partitionResult = (Map<?, ?>) result.get(entry.getKey());
      Assert.assertEquals("none", partitionResult.get("start"));
      Assert.assertEquals(entry.getValue(), partitionResult.get("end"));
      Assert.assertEquals("No committed offset found for this partition", partitionResult.get("note"));
    }
  }

  /**
   * Minimal test implementation of SeekableStreamSupervisor to test calculateBackfillRange
   */
  private static class TestSeekableStreamSupervisor extends SeekableStreamSupervisor<String, Long, Object>
  {
    public TestSeekableStreamSupervisor()
    {
      super(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          false
      );
    }

    @Override
    protected int getTaskGroupIdForPartition(String partition)
    {
      return 0;
    }

    @Override
    protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
    {
      return false;
    }

    @Override
    protected boolean doesTaskTypeMatchSupervisor(Task task)
    {
      return false;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, Long> createDataSourceMetaDataForReset(
        String stream,
        Map<String, Long> map
    )
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
    {
      return null;
    }

    @Override
    protected Long getNotSetMarker()
    {
      return null;
    }

    @Override
    protected Long getEndOfPartitionMarker()
    {
      return null;
    }

    @Override
    protected boolean isEndOfShard(Long seqNum)
    {
      return false;
    }

    @Override
    protected boolean isShardExpirationMarker(Long seqNum)
    {
      return false;
    }

    @Override
    protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
    {
      return false;
    }
  }
}
