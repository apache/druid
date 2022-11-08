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
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.junit.Assert;
import org.junit.Test;

public class WorkerMemoryParametersTest
{
  @Test
  public void test_oneWorkerInJvm_alone()
  {
    Assert.assertEquals(parameters(1, 41, 224_785_000, 100_650_000, 75_000_000), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 13, 149_410_000, 66_900_000, 75_000_000), compute(1_000_000_000, 1, 2, 1));
    Assert.assertEquals(parameters(4, 3, 89_110_000, 39_900_000, 75_000_000), compute(1_000_000_000, 1, 4, 1));
    Assert.assertEquals(parameters(3, 2, 48_910_000, 21_900_000, 75_000_000), compute(1_000_000_000, 1, 8, 1));
    Assert.assertEquals(parameters(2, 2, 33_448_460, 14_976_922, 75_000_000), compute(1_000_000_000, 1, 12, 1));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(1_000_000_000, 750_000_000, 1, 32), e.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 83, 317_580_000, 142_200_000, 150_000_000), compute(2_000_000_000, 1, 1, 200));
    Assert.assertEquals(parameters(2, 27, 166_830_000, 74_700_000, 150_000_000), compute(2_000_000_000, 1, 2, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(1_000_000_000, 1, 4, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 109), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 150, 679_380_000, 304_200_000, 168_750_000), compute(9_000_000_000L, 4, 1, 200));
    Assert.assertEquals(parameters(2, 62, 543_705_000, 243_450_000, 168_750_000), compute(9_000_000_000L, 4, 2, 200));
    Assert.assertEquals(parameters(4, 22, 374_111_250, 167_512_500, 168_750_000), compute(9_000_000_000L, 4, 4, 200));
    Assert.assertEquals(parameters(4, 14, 204_517_500, 91_575_000, 168_750_000), compute(9_000_000_000L, 4, 8, 200));
    Assert.assertEquals(parameters(4, 8, 68_842_500, 30_825_000, 168_750_000), compute(9_000_000_000L, 4, 16, 200));

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> compute(8_000_000_000L, 4, 32, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 124), e.getFault());

    // Make sure 107 actually works. (Verify the error message above.)
    Assert.assertEquals(parameters(4, 3, 28_140_000, 12_600_000, 150_000_000), compute(8_000_000_000L, 4, 32, 107));
  }

  @Test
  public void test_oneWorkerInJvm_negativeUsableMemory()
  {
    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> WorkerMemoryParameters.createInstance(100, -50, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(100, -50, 1, 32), e.getFault());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerMemoryParameters.class).usingGetClass().verify();
  }

  private static WorkerMemoryParameters parameters(
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

  private static WorkerMemoryParameters compute(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers
  )
  {
    return WorkerMemoryParameters.createInstance(
        maxMemoryInJvm,
        (long) (maxMemoryInJvm * WorkerMemoryParameters.USABLE_MEMORY_FRACTION),
        numWorkersInJvm,
        numProcessingThreadsInJvm,
        numInputWorkers
    );
  }
}
