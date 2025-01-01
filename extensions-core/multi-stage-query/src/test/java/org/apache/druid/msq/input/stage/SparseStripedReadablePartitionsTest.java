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

package org.apache.druid.msq.input.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class SparseStripedReadablePartitionsTest
{
  @Test
  public void testPartitionNumbers()
  {
    final SparseStripedReadablePartitions partitions =
        (SparseStripedReadablePartitions) ReadablePartitions.striped(1, new IntAVLTreeSet(new int[]{1, 3}), 3);
    Assert.assertEquals(ImmutableSet.of(0, 1, 2), partitions.getPartitionNumbers());
  }

  @Test
  public void testWorkers()
  {
    final SparseStripedReadablePartitions partitions =
        (SparseStripedReadablePartitions) ReadablePartitions.striped(1, new IntAVLTreeSet(new int[]{1, 3}), 3);
    Assert.assertEquals(IntSet.of(1, 3), partitions.getWorkers());
  }

  @Test
  public void testStageNumber()
  {
    final SparseStripedReadablePartitions partitions =
        (SparseStripedReadablePartitions) ReadablePartitions.striped(1, new IntAVLTreeSet(new int[]{1, 3}), 3);
    Assert.assertEquals(1, partitions.getStageNumber());
  }

  @Test
  public void testSplit()
  {
    final IntAVLTreeSet workers = new IntAVLTreeSet(new int[]{1, 3});
    final SparseStripedReadablePartitions partitions =
        (SparseStripedReadablePartitions) ReadablePartitions.striped(1, workers, 3);

    Assert.assertEquals(
        ImmutableList.of(
            new SparseStripedReadablePartitions(1, workers, new IntAVLTreeSet(new int[]{0, 2})),
            new SparseStripedReadablePartitions(1, workers, new IntAVLTreeSet(new int[]{1}))
        ),
        partitions.split(2)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    final IntAVLTreeSet workers = new IntAVLTreeSet(new int[]{1, 3});
    final ReadablePartitions partitions = ReadablePartitions.striped(1, workers, 3);

    Assert.assertEquals(
        partitions,
        mapper.readValue(
            mapper.writeValueAsString(partitions),
            ReadablePartitions.class
        )
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SparseStripedReadablePartitions.class).usingGetClass().verify();
  }
}
