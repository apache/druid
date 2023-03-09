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

package org.apache.druid.msq.exec;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.junit.Assert;
import org.junit.Test;

public class WorkerMemoryParametersTest
{
  @Test
  public void test_oneWorkerInJvm_alone()
  {
    Assert.assertEquals(params(1, 41, 224_785_000, 100_650_000, 75_000_000), create(1_000_000_000, 1, 1, 1, 0, 0));
    Assert.assertEquals(params(2, 13, 149_410_000, 66_900_000, 75_000_000), create(1_000_000_000, 1, 2, 1, 0, 0));
    Assert.assertEquals(params(4, 3, 89_110_000, 39_900_000, 75_000_000), create(1_000_000_000, 1, 4, 1, 0, 0));
    Assert.assertEquals(params(3, 2, 48_910_000, 21_900_000, 75_000_000), create(1_000_000_000, 1, 8, 1, 0, 0));
    Assert.assertEquals(params(2, 2, 33_448_460, 14_976_922, 75_000_000), create(1_000_000_000, 1, 12, 1, 0, 0));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> create(1_000_000_000, 1, 32, 1, 0, 0)
    );
    Assert.assertEquals(new NotEnoughMemoryFault(1_588_044_000, 1_000_000_000, 750_000_000, 1, 32), e.getFault());

    final MSQFault fault = Assert.assertThrows(MSQException.class, () -> create(1_000_000_000, 2, 32, 1, 0, 0))
                                 .getFault();

    Assert.assertEquals(new NotEnoughMemoryFault(2024045333, 1_000_000_000, 750_000_000, 2, 32), fault);

  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(params(1, 83, 317_580_000, 142_200_000, 150_000_000), create(2_000_000_000, 1, 1, 200, 0, 0));
    Assert.assertEquals(params(2, 27, 166_830_000, 74_700_000, 150_000_000), create(2_000_000_000, 1, 2, 200, 0, 0));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> create(1_000_000_000, 1, 4, 200, 0, 0)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 109), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(params(1, 150, 679_380_000, 304_200_000, 168_750_000), create(9_000_000_000L, 4, 1, 200, 0, 0));
    Assert.assertEquals(params(2, 62, 543_705_000, 243_450_000, 168_750_000), create(9_000_000_000L, 4, 2, 200, 0, 0));
    Assert.assertEquals(params(4, 22, 374_111_250, 167_512_500, 168_750_000), create(9_000_000_000L, 4, 4, 200, 0, 0));
    Assert.assertEquals(params(4, 14, 204_517_500, 91_575_000, 168_750_000), create(9_000_000_000L, 4, 8, 200, 0, 0));
    Assert.assertEquals(params(4, 8, 68_842_500, 30_825_000, 168_750_000), create(9_000_000_000L, 4, 16, 200, 0, 0));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> create(8_000_000_000L, 4, 32, 200, 0, 0)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 124), e.getFault());

    // Make sure 124 actually works, and 125 doesn't. (Verify the error message above.)
    Assert.assertEquals(params(4, 3, 16_750_000, 7_500_000, 150_000_000), create(8_000_000_000L, 4, 32, 124, 0, 0));

    final MSQException e2 = Assert.assertThrows(
        MSQException.class,
        () -> create(8_000_000_000L, 4, 32, 125, 0, 0)
    );

    Assert.assertEquals(new TooManyWorkersFault(125, 124), e2.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster_hashPartitions()
  {
    Assert.assertEquals(
        params(1, 150, 545_380_000, 244_200_000, 168_750_000), create(9_000_000_000L, 4, 1, 200, 200, 0));
    Assert.assertEquals(
        params(2, 62, 409_705_000, 183_450_000, 168_750_000), create(9_000_000_000L, 4, 2, 200, 200, 0));
    Assert.assertEquals(
        params(4, 22, 240_111_250, 107_512_500, 168_750_000), create(9_000_000_000L, 4, 4, 200, 200, 0));
    Assert.assertEquals(
        params(4, 14, 70_517_500, 31_575_000, 168_750_000), create(9_000_000_000L, 4, 8, 200, 200, 0));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> create(9_000_000_000L, 4, 16, 200, 200, 0)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 138), e.getFault());

    // Make sure 138 actually works, and 139 doesn't. (Verify the error message above.)
    Assert.assertEquals(params(4, 8, 17_922_500, 8_025_000, 168_750_000), create(9_000_000_000L, 4, 16, 138, 138, 0));

    final MSQException e2 = Assert.assertThrows(
        MSQException.class,
        () -> create(9_000_000_000L, 4, 16, 139, 139, 0)
    );

    Assert.assertEquals(new TooManyWorkersFault(139, 138), e2.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_oneByteUsableMemory()
  {
    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> WorkerMemoryParameters.createInstance(1, 1, 1, 32, 1, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(554669334, 1, 1, 1, 1), e.getFault());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerMemoryParameters.class).usingGetClass().verify();
  }

  private static WorkerMemoryParameters params(
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final long appenderatorMemory,
      final long broadcastJoinMemory,
      final int partitionStatisticsMaxRetainedBytes
  )
  {
    return new WorkerMemoryParameters(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        appenderatorMemory,
        broadcastJoinMemory,
        partitionStatisticsMaxRetainedBytes
    );
  }

  private static WorkerMemoryParameters create(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers,
      final int numHashOutputPartitions,
      final int totalLookUpFootprint
  )
  {
    return WorkerMemoryParameters.createInstance(
        maxMemoryInJvm,
        numWorkersInJvm,
        numProcessingThreadsInJvm,
        numInputWorkers,
        numHashOutputPartitions,
        totalLookUpFootprint
    );
  }
}
