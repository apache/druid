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

import java.util.Map;

public class SeekableStreamSupervisorTest extends SeekableStreamSupervisorTestBase
{
  @Test
  public void testCalculateBackfillRange_withAllOffsetsPresent()
  {
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);

    Map<String, String> startOffsets = ImmutableMap.of(
        "0", "100",
        "1", "200",
        "2", "300"
    );

    Map<String, String> endOffsets = ImmutableMap.of(
        "0", "150",
        "1", "250",
        "2", "350"
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
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);

    Map<String, String> startOffsets = ImmutableMap.of(
        "0", "100"
    );

    Map<String, String> endOffsets = ImmutableMap.of(
        "0", "150",
        "1", "250",
        "2", "350"
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
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);

    Map<String, String> endOffsets = ImmutableMap.of(
        "0", "150",
        "1", "250",
        "2", "350"
    );

    Map<String, Object> result = supervisor.calculateBackfillRange(null, endOffsets);

    Assert.assertEquals(3, result.size());
    for (Map.Entry<String, String> entry : endOffsets.entrySet()) {
      Map<?, ?> partitionResult = (Map<?, ?>) result.get(entry.getKey());
      Assert.assertEquals("none", partitionResult.get("start"));
      Assert.assertEquals(entry.getValue(), partitionResult.get("end"));
      Assert.assertEquals("No committed offset found for this partition", partitionResult.get("note"));
    }
  }
}
